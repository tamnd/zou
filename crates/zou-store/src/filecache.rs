//! Local file cache over a store: layers on NVMe instead of a round
//! trip to S3 (spec 03 read tiers).
//!
//! The cache only ever holds immutable objects. Those are content
//! addressed, a layer's name pins its bytes, so a cached copy can
//! never be stale and a restart can trust whatever the previous
//! process left on disk: the startup scan rebuilds the index from the
//! directory and cold attach starts with a warm cache. The two mutable
//! objects per tenant, the manifest and the per shard manifests, pass
//! straight through on every call. Caching those would break the CAS
//! loops that depend on reading the current version.
//!
//! A miss fetches the whole object and keeps it, including a range
//! miss: the read path asks for footers and individual blocks, and the
//! first touch of a layer is the signal that its neighbors are coming.
//! After the fill every range is a local pread. Writes are cached on
//! the way through for free, the flusher and the reader are the same
//! process in embedded mode and the layer just built is the layer the
//! next read wants.
//!
//! Eviction is LRU over a byte budget. The order lives in memory and
//! is rebuilt from file modification times on restart, approximate but
//! only recency is lost, never bytes. A cache file that fails to parse
//! is treated as a miss and refetched, so a torn write costs one round
//! trip, not an error.
//!
//! On disk an entry is the key's path under the root, one file,
//! written to a temp name and renamed in: a small header carrying the
//! inner store's version, then the bytes. Concurrent fills of the same
//! key race to identical content, the last rename wins.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::cas::{CasError, CasStore, Version};

const MAGIC: &[u8; 8] = b"zoucach1";

/// Whether a key may be cached: everything under a tenant prefix
/// except the two mutable manifest objects. Mirrors
/// [`crate::layout::TenantLayout::is_immutable`] without needing to
/// know which tenant a key belongs to.
pub fn cacheable(key: &str) -> bool {
    key.starts_with("tenants/") && !key.ends_with("/MANIFEST") && !key.ends_with("/SHARD")
}

/// Hit and miss counters, all monotone, for the bench and for spotting
/// a cache that is too small to hold its working set.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub fills: u64,
    pub evictions: u64,
}

struct Index {
    /// Key to (bytes on disk, recency tick).
    entries: HashMap<String, (u64, u64)>,
    total: u64,
    tick: u64,
}

/// The caching wrapper. Reads of cacheable keys are served from
/// `root` when present, filled from the inner store when not; every
/// other operation forwards.
pub struct FileCache {
    inner: Box<dyn CasStore>,
    root: PathBuf,
    cap: u64,
    index: Mutex<Index>,
    hits: AtomicU64,
    misses: AtomicU64,
    fills: AtomicU64,
    evictions: AtomicU64,
}

impl FileCache {
    /// Open a cache at `root` over `inner`, holding at most `cap`
    /// bytes of object data. Whatever a previous process left under
    /// `root` is adopted, ordered by file modification time.
    pub fn new(
        inner: Box<dyn CasStore>,
        root: impl Into<PathBuf>,
        cap: u64,
    ) -> std::io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        let mut found: Vec<(String, u64, std::time::SystemTime)> = Vec::new();
        scan(&root, &root, &mut found)?;
        found.sort_by_key(|(_, _, mtime)| *mtime);
        let mut index = Index {
            entries: HashMap::new(),
            total: 0,
            tick: 0,
        };
        for (key, size, _) in found {
            index.tick += 1;
            let tick = index.tick;
            index.entries.insert(key, (size, tick));
            index.total += size;
        }
        let cache = FileCache {
            inner,
            root,
            cap,
            index: Mutex::new(index),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            fills: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        };
        cache.evict_to_cap();
        Ok(cache)
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            fills: self.fills.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    fn path_for(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    /// The cached copy, or `None` on any kind of miss: absent, torn,
    /// or unreadable. A bad file is dropped so the refetch can land.
    fn read_cached(&self, key: &str) -> Option<(Vec<u8>, Version)> {
        {
            let mut index = self.index.lock().expect("cache index lock");
            index.tick += 1;
            let now = index.tick;
            let (_, tick) = index.entries.get_mut(key)?;
            *tick = now;
        }
        match parse_entry(&self.path_for(key)) {
            Some(entry) => Some(entry),
            None => {
                self.drop_entry(key);
                None
            }
        }
    }

    /// Keep a copy of `data`, evicting whatever the budget demands.
    /// Cache write failures are swallowed: the caller has the bytes,
    /// a cache that cannot write is just a cache that never hits.
    fn fill(&self, key: &str, data: &[u8], version: &Version) {
        if data.len() as u64 > self.cap {
            return;
        }
        let path = self.path_for(key);
        if write_entry(&self.root, &path, data, version).is_err() {
            return;
        }
        let size = entry_size(data, version);
        {
            let mut index = self.index.lock().expect("cache index lock");
            index.tick += 1;
            let tick = index.tick;
            if let Some((old, _)) = index.entries.insert(key.to_string(), (size, tick)) {
                index.total -= old;
            }
            index.total += size;
        }
        self.fills.fetch_add(1, Ordering::Relaxed);
        self.evict_to_cap();
    }

    fn drop_entry(&self, key: &str) {
        let removed = {
            let mut index = self.index.lock().expect("cache index lock");
            match index.entries.remove(key) {
                Some((size, _)) => {
                    index.total -= size;
                    true
                }
                None => false,
            }
        };
        if removed {
            let _ = fs::remove_file(self.path_for(key));
        }
    }

    fn evict_to_cap(&self) {
        loop {
            let victim = {
                let index = self.index.lock().expect("cache index lock");
                if index.total <= self.cap {
                    return;
                }
                index
                    .entries
                    .iter()
                    .min_by_key(|(_, (_, tick))| *tick)
                    .map(|(key, _)| key.clone())
            };
            let Some(key) = victim else { return };
            self.drop_entry(&key);
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Fetch through the inner store and keep the copy.
    fn fetch(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        self.misses.fetch_add(1, Ordering::Relaxed);
        let Some((data, version)) = self.inner.get(key)? else {
            return Ok(None);
        };
        self.fill(key, &data, &version);
        Ok(Some((data, version)))
    }
}

impl CasStore for FileCache {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        if !cacheable(key) {
            return self.inner.get(key);
        }
        if let Some(entry) = self.read_cached(key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(entry));
        }
        self.fetch(key)
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
        if !cacheable(key) {
            return self.inner.get_range(key, offset, len);
        }
        let (data, _) = match self.read_cached(key) {
            Some(entry) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                entry
            }
            None => match self.fetch(key)? {
                Some(entry) => entry,
                None => return Ok(None),
            },
        };
        let start = (offset as usize).min(data.len());
        let end = (offset.saturating_add(len) as usize).min(data.len());
        Ok(Some(data[start..end].to_vec()))
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        let version = self.inner.put_if_match(key, data, expected)?;
        if cacheable(key) {
            self.fill(key, data, &version);
        }
        Ok(version)
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        self.inner.delete(key)?;
        if cacheable(key) {
            self.drop_entry(key);
        }
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        self.inner.list(prefix)
    }
}

fn entry_size(data: &[u8], version: &Version) -> u64 {
    (MAGIC.len() + 4 + version.as_str().len() + data.len()) as u64
}

fn write_entry(root: &Path, path: &Path, data: &[u8], version: &Version) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let tmp_dir = root.join("tmp");
    fs::create_dir_all(&tmp_dir)?;
    let tmp = tmp_dir.join(format!(
        "{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(MAGIC)?;
        let v = version.as_str().as_bytes();
        file.write_all(&(v.len() as u32).to_le_bytes())?;
        file.write_all(v)?;
        file.write_all(data)?;
        file.sync_data()?;
        fs::rename(&tmp, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn parse_entry(path: &Path) -> Option<(Vec<u8>, Version)> {
    let mut file = fs::File::open(path).ok()?;
    let mut header = [0u8; 12];
    file.read_exact(&mut header).ok()?;
    if &header[..8] != MAGIC {
        return None;
    }
    let vlen = u32::from_le_bytes(header[8..12].try_into().expect("checked length")) as usize;
    if vlen > 1024 {
        return None;
    }
    let mut v = vec![0u8; vlen];
    file.read_exact(&mut v).ok()?;
    let version = Version::from_backend(String::from_utf8(v).ok()?);
    let mut data = Vec::new();
    file.read_to_end(&mut data).ok()?;
    Some((data, version))
}

/// Walk the cache directory, reporting (key, entry size, mtime) for
/// every regular file. Keys are the paths relative to the root.
fn scan(
    root: &Path,
    dir: &Path,
    found: &mut Vec<(String, u64, std::time::SystemTime)>,
) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_dir() {
            scan(root, &entry.path(), found)?;
        } else if meta.is_file() {
            let rel = entry
                .path()
                .strip_prefix(root)
                .expect("under the root")
                .to_string_lossy()
                .into_owned();
            // Temp files from a crashed fill are garbage, not entries.
            if cacheable(&rel) {
                found.push((rel, meta.len(), meta.modified()?));
            } else {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::MemStore;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    /// Counts reads that reach the store behind the cache.
    struct Counting {
        inner: MemStore,
        reads: Arc<AtomicUsize>,
    }

    impl CasStore for Counting {
        fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.inner.get(key)
        }
        fn put_if_match(
            &self,
            key: &str,
            data: &[u8],
            expected: Option<&Version>,
        ) -> Result<Version, CasError> {
            self.inner.put_if_match(key, data, expected)
        }
        fn delete(&self, key: &str) -> Result<(), CasError> {
            self.inner.delete(key)
        }
        fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
            self.inner.list(prefix)
        }
    }

    fn harness(cap: u64) -> (FileCache, Arc<AtomicUsize>, tempfile::TempDir) {
        let reads = Arc::new(AtomicUsize::new(0));
        let inner = Counting {
            inner: MemStore::default(),
            reads: reads.clone(),
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = FileCache::new(Box::new(inner), dir.path().join("cache"), cap).expect("cache");
        (cache, reads, dir)
    }

    const LAYER: &str = "tenants/acme/shards/0000/some-layer";

    #[test]
    fn the_second_read_never_leaves_the_machine() {
        let (cache, reads, _dir) = harness(1 << 20);
        cache.inner.put_if_absent(LAYER, b"bytes").unwrap();
        let (a, va) = cache.get(LAYER).unwrap().expect("exists");
        let after_first = reads.load(Ordering::Relaxed);
        let (b, vb) = cache.get(LAYER).unwrap().expect("exists");
        assert_eq!(a, b"bytes");
        assert_eq!(a, b);
        assert_eq!(va, vb, "the cached version is the store's version");
        assert_eq!(
            reads.load(Ordering::Relaxed),
            after_first,
            "hit stayed local"
        );
        assert_eq!(cache.stats().hits, 1);
        assert_eq!(cache.stats().misses, 1);
    }

    #[test]
    fn a_range_miss_fills_the_whole_object() {
        let (cache, reads, _dir) = harness(1 << 20);
        cache.inner.put_if_absent(LAYER, b"0123456789").unwrap();
        assert_eq!(
            cache.get_range(LAYER, 2, 3).unwrap().expect("exists"),
            b"234"
        );
        let after_first = reads.load(Ordering::Relaxed);
        assert_eq!(
            cache.get_range(LAYER, 8, 100).unwrap().expect("exists"),
            b"89",
            "ranges clamp like the trait promises"
        );
        assert_eq!(
            reads.load(Ordering::Relaxed),
            after_first,
            "the first range fetched everything"
        );
    }

    #[test]
    fn mutable_manifests_pass_through_every_time() {
        let (cache, reads, _dir) = harness(1 << 20);
        for key in ["tenants/acme/MANIFEST", "tenants/acme/shards/0000/SHARD"] {
            cache.inner.put_if_absent(key, b"v1").unwrap();
            cache.get(key).unwrap().expect("exists");
            let before = reads.load(Ordering::Relaxed);
            let (data, version) = cache.get(key).unwrap().expect("exists");
            assert_eq!(data, b"v1");
            assert!(
                reads.load(Ordering::Relaxed) > before,
                "no caching for {key}"
            );
            // The version must be live, CAS loops depend on it.
            cache.put_if_match(key, b"v2", Some(&version)).unwrap();
            assert_eq!(cache.get(key).unwrap().expect("exists").0, b"v2");
        }
        assert_eq!(cache.stats().hits + cache.stats().misses, 0);
    }

    #[test]
    fn a_restart_adopts_what_the_last_process_cached() {
        let reads = Arc::new(AtomicUsize::new(0));
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("cache");
        {
            let inner = Counting {
                inner: MemStore::default(),
                reads: reads.clone(),
            };
            inner.inner.put_if_absent(LAYER, b"survives").unwrap();
            let cache = FileCache::new(Box::new(inner), &root, 1 << 20).expect("cache");
            cache.get(LAYER).unwrap().expect("exists");
        }
        // The new process sits over an empty store: only the disk copy
        // can answer.
        let cache = FileCache::new(
            Box::new(Counting {
                inner: MemStore::default(),
                reads: reads.clone(),
            }),
            &root,
            1 << 20,
        )
        .expect("cache");
        let before = reads.load(Ordering::Relaxed);
        let (data, _) = cache.get(LAYER).unwrap().expect("served from disk");
        assert_eq!(data, b"survives");
        assert_eq!(reads.load(Ordering::Relaxed), before);
    }

    #[test]
    fn a_torn_cache_file_is_a_miss_not_an_error() {
        let (cache, _reads, _dir) = harness(1 << 20);
        cache.inner.put_if_absent(LAYER, b"good bytes").unwrap();
        cache.get(LAYER).unwrap();
        std::fs::write(cache.path_for(LAYER), b"torn").expect("overwrite");
        let (data, _) = cache.get(LAYER).unwrap().expect("refetched");
        assert_eq!(data, b"good bytes");
        let (data, _) = cache.get(LAYER).unwrap().expect("cached again");
        assert_eq!(data, b"good bytes");
    }

    #[test]
    fn eviction_drops_the_coldest_and_a_hit_is_warmth() {
        let entry = {
            let probe = MemStore::default();
            probe.put_if_absent("k", &[0u8; 100]).unwrap();
            let (data, version) = probe.get("k").unwrap().expect("exists");
            entry_size(&data, &version)
        };
        let (cache, _reads, _dir) = harness(3 * entry);
        let key = |i: usize| format!("tenants/acme/shards/0000/layer-{i}");
        for i in 0..3 {
            cache.inner.put_if_absent(&key(i), &[i as u8; 100]).unwrap();
            cache.get(&key(i)).unwrap();
        }
        // Touch the oldest so the second oldest is now coldest.
        cache.get(&key(0)).unwrap();
        cache.inner.put_if_absent(&key(3), &[3u8; 100]).unwrap();
        cache.get(&key(3)).unwrap();
        assert_eq!(cache.stats().evictions, 1);
        assert!(!cache.path_for(&key(1)).exists(), "the coldest went");
        assert!(cache.path_for(&key(0)).exists(), "the touched one stayed");
        let total = cache.index.lock().unwrap().total;
        assert!(total <= 3 * entry, "budget holds");
    }

    #[test]
    fn writes_warm_the_cache_on_the_way_through() {
        let (cache, reads, _dir) = harness(1 << 20);
        cache.put_if_absent(LAYER, b"fresh layer").unwrap();
        let before = reads.load(Ordering::Relaxed);
        let (data, _) = cache.get(LAYER).unwrap().expect("exists");
        assert_eq!(data, b"fresh layer");
        assert_eq!(reads.load(Ordering::Relaxed), before, "no fetch needed");
    }

    #[test]
    fn delete_takes_the_cached_copy_with_it() {
        let (cache, _reads, _dir) = harness(1 << 20);
        cache.inner.put_if_absent(LAYER, b"bytes").unwrap();
        cache.get(LAYER).unwrap();
        cache.delete(LAYER).unwrap();
        assert!(!cache.path_for(LAYER).exists());
        assert!(cache.get(LAYER).unwrap().is_none());
    }

    #[test]
    fn an_object_bigger_than_the_budget_is_never_kept() {
        let (cache, _reads, _dir) = harness(64);
        cache.inner.put_if_absent(LAYER, &[7u8; 200]).unwrap();
        cache.get(LAYER).unwrap().expect("served");
        assert!(!cache.path_for(LAYER).exists(), "too big to keep");
        assert_eq!(cache.stats().fills, 0);
    }
}
