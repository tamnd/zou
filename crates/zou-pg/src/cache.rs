//! RAM and NVMe slab cache in front of checkpoint run range reads.
//!
//! Keys are content addressed: a slab is named by its checkpoint id,
//! run number, and byte offset, and run objects are immutable once
//! written, put_new refuses overwrites and a fold never rewrites an
//! existing run. A cached slab can therefore never go stale and there
//! is no invalidation path at all, eviction is purely about space.
//!
//! The RAM tier is a strict least recently used map with a byte
//! budget. The disk tier is a directory of slab files shared by every
//! backend process of the cluster, sized by its own budget; each
//! process tracks the files it knows about and evicts oldest first,
//! and a file another process removed first is simply a miss. Disk
//! writes go through a temp file and rename so a reader never sees a
//! half written slab.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

/// Log a hit rate line this often, in lookups. Backends also log a
/// summary on exit through Drop.
const LOG_EVERY: u64 = 65536;

pub struct CacheConfig {
    pub ram_bytes: usize,
    pub disk_dir: Option<PathBuf>,
    pub disk_bytes: u64,
}

impl CacheConfig {
    /// `ZOU_READ_CACHE_RAM_MB` sizes the RAM tier, default 64. The
    /// disk tier is off unless `ZOU_READ_CACHE_DIR` points somewhere,
    /// sized by `ZOU_READ_CACHE_DISK_MB`, default 1024.
    pub fn from_env() -> Self {
        let mb = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(default)
        };
        Self {
            ram_bytes: (mb("ZOU_READ_CACHE_RAM_MB", 64) << 20) as usize,
            disk_dir: std::env::var("ZOU_READ_CACHE_DIR").ok().map(PathBuf::from),
            disk_bytes: mb("ZOU_READ_CACHE_DISK_MB", 1024) << 20,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheMetrics {
    pub ram_hits: u64,
    pub disk_hits: u64,
    pub misses: u64,
}

impl CacheMetrics {
    pub fn lookups(&self) -> u64 {
        self.ram_hits + self.disk_hits + self.misses
    }

    fn line(&self) -> String {
        let pct = |n: u64| 100.0 * n as f64 / self.lookups() as f64;
        format!(
            "zou read cache: {} lookups, {:.1}% ram, {:.1}% disk, {:.1}% miss",
            self.lookups(),
            pct(self.ram_hits),
            pct(self.disk_hits),
            pct(self.misses)
        )
    }
}

/// One process's view of the shared disk tier.
struct Disk {
    dir: PathBuf,
    budget: u64,
    total: u64,
    /// Eviction order, oldest first. Values are file name and size.
    order: BTreeMap<u64, (String, u64)>,
    index: HashMap<String, u64>,
    seq: u64,
}

impl Disk {
    /// Adopt whatever slabs already sit in the directory, oldest
    /// modification first, so restarts inherit a warm tier and its
    /// size counts against the budget from the start.
    fn open(dir: PathBuf, budget: u64) -> Option<Self> {
        std::fs::create_dir_all(&dir).ok()?;
        let mut existing = Vec::new();
        for entry in std::fs::read_dir(&dir).ok()?.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if !meta.is_file() {
                continue;
            }
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            if name.ends_with(".tmp") {
                let _ = std::fs::remove_file(entry.path());
                continue;
            }
            let modified = meta.modified().ok();
            existing.push((modified, name, meta.len()));
        }
        existing.sort();
        let mut disk = Self {
            dir,
            budget,
            total: 0,
            order: BTreeMap::new(),
            index: HashMap::new(),
            seq: 0,
        };
        for (_, name, len) in existing {
            disk.track(name, len);
        }
        disk.evict_to_budget();
        Some(disk)
    }

    fn track(&mut self, name: String, len: u64) {
        if let Some(seq) = self.index.remove(&name)
            && let Some((_, old)) = self.order.remove(&seq)
        {
            self.total -= old;
        }
        self.seq += 1;
        self.total += len;
        self.order.insert(self.seq, (name.clone(), len));
        self.index.insert(name, self.seq);
    }

    fn evict_to_budget(&mut self) {
        while self.total > self.budget {
            let Some((_, (name, len))) = self.order.pop_first() else {
                return;
            };
            self.index.remove(&name);
            self.total -= len;
            let _ = std::fs::remove_file(self.dir.join(&name));
        }
    }

    fn get(&mut self, key: &str) -> Option<Vec<u8>> {
        let data = match std::fs::read(self.dir.join(key)) {
            Ok(data) => data,
            Err(_) => {
                // Another process may have evicted it, forget it.
                if let Some(seq) = self.index.remove(key)
                    && let Some((_, len)) = self.order.remove(&seq)
                {
                    self.total -= len;
                }
                return None;
            }
        };
        self.track(key.to_string(), data.len() as u64);
        Some(data)
    }

    fn put(&mut self, key: &str, data: &[u8]) {
        let tmp = self.dir.join(format!("{key}.{}.tmp", std::process::id()));
        if std::fs::write(&tmp, data).is_err() {
            let _ = std::fs::remove_file(&tmp);
            return;
        }
        if std::fs::rename(&tmp, self.dir.join(key)).is_err() {
            let _ = std::fs::remove_file(&tmp);
            return;
        }
        self.track(key.to_string(), data.len() as u64);
        self.evict_to_budget();
    }
}

pub struct SlabCache {
    ram_budget: usize,
    ram_total: usize,
    /// Key to last use sequence and slab bytes.
    ram: HashMap<String, (u64, Vec<u8>)>,
    /// Least recently used first.
    order: BTreeMap<u64, String>,
    seq: u64,
    disk: Option<Disk>,
    metrics: CacheMetrics,
}

impl SlabCache {
    pub fn new(config: CacheConfig) -> Self {
        Self {
            ram_budget: config.ram_bytes,
            ram_total: 0,
            ram: HashMap::new(),
            order: BTreeMap::new(),
            seq: 0,
            disk: config
                .disk_dir
                .and_then(|dir| Disk::open(dir, config.disk_bytes)),
            metrics: CacheMetrics::default(),
        }
    }

    pub fn metrics(&self) -> CacheMetrics {
        self.metrics
    }

    /// Return the slab for `key`, loading and caching it on a miss.
    /// The RAM tier answers first, then the disk tier promoting into
    /// RAM, then the loader filling both.
    pub fn get_or_load(
        &mut self,
        key: &str,
        load: impl FnOnce() -> Result<Vec<u8>, String>,
    ) -> Result<&[u8], String> {
        if self.ram.contains_key(key) {
            self.metrics.ram_hits += 1;
            self.log_maybe();
            self.touch(key);
            return Ok(&self.ram[key].1);
        }
        if let Some(data) = self.disk.as_mut().and_then(|d| d.get(key)) {
            self.metrics.disk_hits += 1;
            self.log_maybe();
            return Ok(self.insert_ram(key, data));
        }
        self.metrics.misses += 1;
        self.log_maybe();
        let data = load()?;
        if let Some(disk) = &mut self.disk {
            disk.put(key, &data);
        }
        Ok(self.insert_ram(key, data))
    }

    fn touch(&mut self, key: &str) {
        let entry = self.ram.get_mut(key).expect("touch of a present key");
        self.order.remove(&entry.0);
        self.seq += 1;
        entry.0 = self.seq;
        self.order.insert(self.seq, key.to_string());
    }

    fn insert_ram(&mut self, key: &str, data: Vec<u8>) -> &[u8] {
        self.ram_total += data.len();
        while self.ram_total > self.ram_budget {
            let Some((_, victim)) = self.order.pop_first() else {
                break;
            };
            if let Some((_, old)) = self.ram.remove(&victim) {
                self.ram_total -= old.len();
            }
        }
        self.seq += 1;
        self.order.insert(self.seq, key.to_string());
        self.ram.insert(key.to_string(), (self.seq, data));
        &self.ram[key].1
    }

    fn log_maybe(&self) {
        if self.metrics.lookups().is_multiple_of(LOG_EVERY) {
            log::info!("{}", self.metrics.line());
        }
    }

    /// Emit the hit rate summary. The shim calls this from an atexit
    /// hook because the static it holds the cache in never drops when
    /// a backend exits through C's exit().
    pub fn log_summary(&self) {
        if self.metrics.lookups() > 0 {
            log::info!("{}", self.metrics.line());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ram_only(budget: usize) -> SlabCache {
        SlabCache::new(CacheConfig {
            ram_bytes: budget,
            disk_dir: None,
            disk_bytes: 0,
        })
    }

    fn slab(cache: &mut SlabCache, key: &str, fill: u8, len: usize) -> Vec<u8> {
        cache
            .get_or_load(key, || Ok(vec![fill; len]))
            .unwrap()
            .to_vec()
    }

    #[test]
    fn hits_do_not_reload_and_metrics_count() {
        let mut cache = ram_only(1 << 20);
        assert_eq!(slab(&mut cache, "a", 0x11, 100), vec![0x11; 100]);
        let hit = cache
            .get_or_load("a", || Err("must not reload".into()))
            .unwrap();
        assert_eq!(hit, vec![0x11; 100]);
        assert_eq!(
            cache.metrics(),
            CacheMetrics {
                ram_hits: 1,
                disk_hits: 0,
                misses: 1
            }
        );
    }

    #[test]
    fn the_least_recently_used_slab_is_evicted_first() {
        let mut cache = ram_only(250);
        slab(&mut cache, "a", 0x11, 100);
        slab(&mut cache, "b", 0x22, 100);
        // Touch a so b is the oldest when c overflows the budget.
        cache.get_or_load("a", || Err("hit".into())).unwrap();
        slab(&mut cache, "c", 0x33, 100);
        cache.get_or_load("a", || Err("a stays".into())).unwrap();
        cache.get_or_load("c", || Err("c stays".into())).unwrap();
        assert_eq!(
            slab(&mut cache, "b", 0x44, 100),
            vec![0x44; 100],
            "b was evicted and reloads"
        );
    }

    #[test]
    fn a_loader_error_caches_nothing() {
        let mut cache = ram_only(1 << 20);
        assert!(cache.get_or_load("a", || Err("boom".into())).is_err());
        assert_eq!(slab(&mut cache, "a", 0x55, 10), vec![0x55; 10]);
        assert_eq!(cache.metrics().misses, 2);
    }

    #[test]
    fn the_disk_tier_survives_a_new_cache_instance() {
        let dir = tempfile::tempdir().unwrap();
        let config = || CacheConfig {
            ram_bytes: 1 << 20,
            disk_dir: Some(dir.path().to_path_buf()),
            disk_bytes: 1 << 20,
        };
        let mut first = SlabCache::new(config());
        slab(&mut first, "a", 0x77, 4096);
        drop(first);

        // A fresh process finds the slab on disk, not through its RAM.
        let mut second = SlabCache::new(config());
        assert_eq!(
            second
                .get_or_load("a", || Err("disk must answer".into()))
                .unwrap(),
            vec![0x77; 4096]
        );
        assert_eq!(second.metrics().disk_hits, 1);
        // And the promoted copy now answers from RAM.
        second
            .get_or_load("a", || Err("ram must answer".into()))
            .unwrap();
        assert_eq!(second.metrics().ram_hits, 1);
    }

    #[test]
    fn the_disk_tier_evicts_oldest_first_under_its_budget() {
        let dir = tempfile::tempdir().unwrap();
        let mut cache = SlabCache::new(CacheConfig {
            // A tiny RAM tier so disk is the only place slabs survive.
            ram_bytes: 0,
            disk_dir: Some(dir.path().to_path_buf()),
            disk_bytes: 250,
        });
        slab(&mut cache, "a", 0x11, 100);
        slab(&mut cache, "b", 0x22, 100);
        slab(&mut cache, "c", 0x33, 100);
        assert!(!dir.path().join("a").exists(), "a was the oldest");
        assert!(dir.path().join("b").exists());
        assert!(dir.path().join("c").exists());
    }

    #[test]
    fn a_slab_evicted_by_another_process_reads_as_a_miss() {
        let dir = tempfile::tempdir().unwrap();
        let mut cache = SlabCache::new(CacheConfig {
            ram_bytes: 0,
            disk_dir: Some(dir.path().to_path_buf()),
            disk_bytes: 1 << 20,
        });
        slab(&mut cache, "a", 0x11, 100);
        // A zero budget RAM tier still holds the newest slab, push a
        // out with b so the disk tier is the only place left.
        slab(&mut cache, "b", 0x22, 100);
        std::fs::remove_file(dir.path().join("a")).unwrap();
        assert_eq!(slab(&mut cache, "a", 0x99, 100), vec![0x99; 100]);
        assert_eq!(cache.metrics().misses, 3);
    }
}
