//! Store op counters shared across every process touching one store.
//!
//! `ZOU_STORE_STATS` names a small counter file. [`StatsStore`] wraps the
//! opened backend, maps that file, and bumps fixed-slot atomics in place
//! on every op: count and bytes per op kind and key class, a power of two
//! microsecond latency histogram per op kind, io errors, and CAS
//! conflicts. The file is plain shared memory, so zou dev, initdb, and
//! every postgres backend all add into the same totals and nobody has to
//! flush anything on exit. [`Snapshot::read`] turns the file into json
//! for the benchmark harness, `zou stats <file>` is the cli for it.
//!
//! The costs this feeds are per-op: a put is a put whether it carried 8K
//! or 8M, which is exactly how S3 bills, so counts and bytes are kept
//! separately and never merged into one number here.

use std::fs::{self, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::cas::{CasError, CasStore, Version};

/// One slot per counter, all u64, native endian. The header pins the
/// layout so a dump from a stale binary fails loudly instead of reading
/// garbage.
const MAGIC: u64 = u64::from_ne_bytes(*b"ZOUSTATS");
const FORMAT: u64 = 1;

pub const OP_NAMES: [&str; 6] = ["get", "get_range", "put_if_match", "put", "delete", "list"];
pub const CLASS_NAMES: [&str; 6] = ["manifest", "wal", "chk", "page", "file", "other"];

const KINDS: usize = OP_NAMES.len();
const CLASSES: usize = CLASS_NAMES.len();
const BUCKETS: usize = 32;

const HEADER: usize = 2;
const BUCKET_BASE: usize = HEADER + KINDS * CLASSES * 2;
const ERROR_BASE: usize = BUCKET_BASE + KINDS * BUCKETS;
const CONFLICT_SLOT: usize = ERROR_BASE + KINDS;
const SLOTS: usize = CONFLICT_SLOT + 1;

#[derive(Clone, Copy)]
enum Op {
    Get,
    GetRange,
    PutIfMatch,
    Put,
    Delete,
    List,
}

const fn count_slot(kind: usize, class: usize) -> usize {
    HEADER + (kind * CLASSES + class) * 2
}

/// Which layout region a key belongs to, as an index into
/// [`CLASS_NAMES`]. Keys arrive with the tenants/ prefix still on, so
/// substring checks against the layout's directory names are enough.
fn classify(key: &str) -> usize {
    if key.ends_with("MANIFEST") || key.contains("/manifests/") {
        0
    } else if key.contains("/wal/") {
        1
    } else if key.contains("/chk/") {
        2
    } else if key.contains("/pg/") {
        3
    } else if key.contains("/files/") {
        4
    } else {
        5
    }
}

/// Bucket b holds latencies in [2^b, 2^(b+1)) microseconds, sub
/// microsecond ops land in bucket 0 and the top bucket is open ended.
fn bucket(elapsed: Duration) -> usize {
    let us = (elapsed.as_micros() as u64).max(1);
    (us.ilog2() as usize).min(BUCKETS - 1)
}

/// The mapped counter file. Opening creates and sizes it if needed,
/// several processes map the same file concurrently and that is the
/// point.
struct Counters {
    map: memmap2::MmapMut,
}

impl Counters {
    fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.set_len((SLOTS * 8) as u64)?;
        let map = unsafe { memmap2::MmapMut::map_mut(&file)? };
        let this = Self { map };
        this.slot(0).store(MAGIC, Ordering::Relaxed);
        this.slot(1).store(FORMAT, Ordering::Relaxed);
        Ok(this)
    }

    /// The mapping is page aligned and every slot offset is a multiple
    /// of 8, so the cast below is aligned by construction.
    fn slot(&self, i: usize) -> &AtomicU64 {
        debug_assert!(i < SLOTS);
        unsafe { &*self.map.as_ptr().cast::<AtomicU64>().add(i) }
    }

    fn add(&self, i: usize, by: u64) {
        self.slot(i).fetch_add(by, Ordering::Relaxed);
    }

    fn op(&self, op: Op, key: &str, bytes: u64, elapsed: Duration, err: Option<&CasError>) {
        let kind = op as usize;
        let class = classify(key);
        self.add(count_slot(kind, class), 1);
        if bytes > 0 {
            self.add(count_slot(kind, class) + 1, bytes);
        }
        self.add(BUCKET_BASE + kind * BUCKETS + bucket(elapsed), 1);
        match err {
            Some(CasError::Conflict { .. }) => self.add(CONFLICT_SLOT, 1),
            Some(_) => self.add(ERROR_BASE + kind, 1),
            None => {}
        }
    }
}

/// A [`CasStore`] that counts every op into a shared counter file and
/// passes it through untouched. Wraps outermost, after any simulated
/// delay, so the latency it records is the latency callers paid.
pub struct StatsStore {
    inner: Box<dyn CasStore>,
    counters: Counters,
}

impl StatsStore {
    pub fn new(inner: Box<dyn CasStore>, path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            inner,
            counters: Counters::open(path)?,
        })
    }
}

impl CasStore for StatsStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        let start = Instant::now();
        let out = self.inner.get(key);
        let bytes = match &out {
            Ok(Some((data, _))) => data.len() as u64,
            _ => 0,
        };
        self.counters
            .op(Op::Get, key, bytes, start.elapsed(), out.as_ref().err());
        out
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
        let start = Instant::now();
        let out = self.inner.get_range(key, offset, len);
        let bytes = match &out {
            Ok(Some(data)) => data.len() as u64,
            _ => 0,
        };
        self.counters.op(
            Op::GetRange,
            key,
            bytes,
            start.elapsed(),
            out.as_ref().err(),
        );
        out
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        let start = Instant::now();
        let out = self.inner.put_if_match(key, data, expected);
        self.counters.op(
            Op::PutIfMatch,
            key,
            data.len() as u64,
            start.elapsed(),
            out.as_ref().err(),
        );
        out
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        let start = Instant::now();
        let out = self.inner.put(key, data);
        self.counters.op(
            Op::Put,
            key,
            data.len() as u64,
            start.elapsed(),
            out.as_ref().err(),
        );
        out
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        let start = Instant::now();
        let out = self.inner.delete(key);
        self.counters
            .op(Op::Delete, key, 0, start.elapsed(), out.as_ref().err());
        out
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        let start = Instant::now();
        let out = self.inner.list(prefix);
        self.counters
            .op(Op::List, prefix, 0, start.elapsed(), out.as_ref().err());
        out
    }
}

/// Counters for one op kind, decoded from the file.
#[derive(Debug, serde::Serialize)]
pub struct OpSnapshot {
    pub op: &'static str,
    pub count: u64,
    pub bytes: u64,
    pub errors: u64,
    /// Latency percentiles as bucket upper bounds in microseconds, so
    /// p50_us 512 reads as "half the ops finished under 512 us".
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
    pub by_class: Vec<ClassSnapshot>,
}

#[derive(Debug, serde::Serialize)]
pub struct ClassSnapshot {
    pub class: &'static str,
    pub count: u64,
    pub bytes: u64,
}

/// The whole counter file, decoded. This reads the file cold rather
/// than mapping it, so dumping never touches the counters.
#[derive(Debug, serde::Serialize)]
pub struct Snapshot {
    pub conflicts: u64,
    pub ops: Vec<OpSnapshot>,
}

fn percentile(buckets: &[u64], total: u64, q: f64) -> u64 {
    if total == 0 {
        return 0;
    }
    let want = (total as f64 * q).ceil() as u64;
    let mut seen = 0u64;
    for (b, n) in buckets.iter().enumerate() {
        seen += n;
        if seen >= want {
            return 1u64 << (b + 1);
        }
    }
    1u64 << BUCKETS
}

impl Snapshot {
    pub fn read(path: &Path) -> Result<Self, String> {
        let data = fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
        if data.len() < SLOTS * 8 {
            return Err(format!("{} is not a counter file", path.display()));
        }
        let slot = |i: usize| u64::from_ne_bytes(data[i * 8..i * 8 + 8].try_into().unwrap());
        if slot(0) != MAGIC || slot(1) != FORMAT {
            return Err(format!(
                "{} is not a format {FORMAT} counter file",
                path.display()
            ));
        }
        let mut ops = Vec::with_capacity(KINDS);
        for (kind, op) in OP_NAMES.iter().copied().enumerate() {
            let mut by_class = Vec::new();
            let (mut count, mut bytes) = (0u64, 0u64);
            for (class, name) in CLASS_NAMES.iter().copied().enumerate() {
                let (c, b) = (
                    slot(count_slot(kind, class)),
                    slot(count_slot(kind, class) + 1),
                );
                count += c;
                bytes += b;
                if c > 0 {
                    by_class.push(ClassSnapshot {
                        class: name,
                        count: c,
                        bytes: b,
                    });
                }
            }
            let buckets: Vec<u64> = (0..BUCKETS)
                .map(|b| slot(BUCKET_BASE + kind * BUCKETS + b))
                .collect();
            let max_us = buckets
                .iter()
                .rposition(|&n| n > 0)
                .map_or(0, |b| 1u64 << (b + 1));
            ops.push(OpSnapshot {
                op,
                count,
                bytes,
                errors: slot(ERROR_BASE + kind),
                p50_us: percentile(&buckets, count, 0.50),
                p95_us: percentile(&buckets, count, 0.95),
                p99_us: percentile(&buckets, count, 0.99),
                max_us,
                by_class,
            });
        }
        Ok(Self {
            conflicts: slot(CONFLICT_SLOT),
            ops,
        })
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("snapshot serializes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::LocalFsStore;

    fn wrap(dir: &Path, counters: &Path) -> StatsStore {
        StatsStore::new(Box::new(LocalFsStore::new(dir.join("store"))), counters).unwrap()
    }

    #[test]
    fn keys_classify_by_layout_region() {
        assert_eq!(classify("tenants/a/MANIFEST"), 0);
        assert_eq!(classify("tenants/a/manifests/0-0.json"), 0);
        assert_eq!(classify("tenants/a/wal/0000000000000001/00.wal"), 1);
        assert_eq!(classify("tenants/a/chk/chk-1/INDEX"), 2);
        assert_eq!(classify("tenants/a/pg/1663/5/16384/0/00000001"), 3);
        assert_eq!(classify("tenants/a/files/avatars/pic.png"), 4);
        assert_eq!(classify("something-else"), 5);
    }

    #[test]
    fn ops_land_in_their_kind_and_class_counters() {
        let dir = tempfile::tempdir().unwrap();
        let counters = dir.path().join("stats");
        let store = wrap(dir.path(), &counters);
        store
            .put("tenants/a/pg/1/2/3/0/00000000", &[7u8; 100])
            .unwrap();
        store
            .put("tenants/a/pg/1/2/3/0/00000001", &[7u8; 100])
            .unwrap();
        store.get("tenants/a/pg/1/2/3/0/00000000").unwrap();
        store.get("tenants/a/pg/1/2/3/0/missing").unwrap();
        store
            .put_if_absent("tenants/a/wal/0000000000000001/00.wal", b"frame")
            .unwrap();
        store.list("tenants/a/wal/").unwrap();
        store.delete("tenants/a/pg/1/2/3/0/00000001").unwrap();

        let snap = Snapshot::read(&counters).unwrap();
        let op = |name: &str| snap.ops.iter().find(|o| o.op == name).unwrap();
        assert_eq!(op("put").count, 2);
        assert_eq!(op("put").bytes, 200);
        assert_eq!(op("put").by_class[0].class, "page");
        assert_eq!(op("get").count, 2);
        assert_eq!(op("get").bytes, 100);
        assert_eq!(op("put_if_match").count, 1);
        assert_eq!(op("put_if_match").by_class[0].class, "wal");
        assert_eq!(op("list").count, 1);
        assert_eq!(op("delete").count, 1);
        assert_eq!(op("get").errors, 0);
        assert_eq!(snap.conflicts, 0);
        assert!(op("put").p50_us > 0);
        assert!(op("put").max_us >= op("put").p50_us);
    }

    #[test]
    fn conflicts_count_separately_from_errors() {
        let dir = tempfile::tempdir().unwrap();
        let counters = dir.path().join("stats");
        let store = wrap(dir.path(), &counters);
        store
            .put_if_match("tenants/a/MANIFEST", b"v1", None)
            .unwrap();
        let err = store.put_if_match("tenants/a/MANIFEST", b"v2", None);
        assert!(matches!(err, Err(CasError::Conflict { .. })));
        let snap = Snapshot::read(&counters).unwrap();
        let pim = snap.ops.iter().find(|o| o.op == "put_if_match").unwrap();
        assert_eq!(pim.count, 2);
        assert_eq!(pim.errors, 0);
        assert_eq!(snap.conflicts, 1);
    }

    #[test]
    fn two_handles_on_one_file_share_totals() {
        let dir = tempfile::tempdir().unwrap();
        let counters = dir.path().join("stats");
        let a = wrap(dir.path(), &counters);
        let b = wrap(dir.path(), &counters);
        a.put("tenants/a/pg/1/2/3/0/00000000", b"x").unwrap();
        b.put("tenants/a/pg/1/2/3/0/00000001", b"y").unwrap();
        let snap = Snapshot::read(&counters).unwrap();
        let put = snap.ops.iter().find(|o| o.op == "put").unwrap();
        assert_eq!(put.count, 2);
    }

    #[test]
    fn a_snapshot_rejects_files_that_are_not_counters() {
        let dir = tempfile::tempdir().unwrap();
        let bogus = dir.path().join("bogus");
        fs::write(&bogus, b"hello").unwrap();
        assert!(Snapshot::read(&bogus).is_err());
        assert!(Snapshot::read(&dir.path().join("absent")).is_err());
        fs::write(&bogus, vec![0u8; SLOTS * 8]).unwrap();
        assert!(Snapshot::read(&bogus).is_err());
    }
}
