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
//!
//! The same file carries the smgr read tier counters: every page read a
//! postgres backend makes lands in exactly one tier, cache when the
//! local page cache answered, local when reconstruction ran entirely
//! against local bytes, store when the read paid at least one store
//! round trip, service when the page service answered it. The read path
//! reports through [`note_read_pages`] and [`note_read_call`], which
//! open the `ZOU_STORE_STATS` file on first use and no-op when the
//! variable is unset, so a server that nobody is measuring pays two
//! branches per read and nothing else.
//!
//! The service tier is where the read leaves the backend, so it says
//! nothing about what the wait was made of. The page service reports
//! its own side through [`note_phase`]: how long a request sat parked
//! waiting for ingest to reach the LSN it asked for, how long the read
//! itself took once it ran, and how long the driver spent in ingest
//! rather than answering anybody. One serve loop does all three, so
//! ingest time is read latency for every request queued behind it and
//! the only way to see that is to measure it.
//!
//! The commit path reports the same way through [`note_commit`], and
//! for the same reason: a commit that takes half a second is a number
//! with six places it could have come from, and a run that only knows
//! the total can only guess which. The six add up along one chunk of
//! WAL: `push` is the pusher's own loop between two appends, so it is
//! how late the bytes were handed over in the first place, `stage` is
//! the append call itself, encoding included, `window` is how long the
//! batch that chunk joined stayed open, `dispatch` is from that window
//! closing to its PUT starting, which is the inflight bound when it
//! binds, `put` is the store, and `ack` is what the window waited on
//! its predecessors after its own PUT returned, because the chain acks
//! in order. `durable` is the whole of it from the append onwards, the
//! number a committing backend is actually waiting for.

use std::cell::Cell;
use std::fs::{self, OpenOptions};
use std::path::Path;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::cas::{CasError, CasStore, Version};

/// One slot per counter, all u64, native endian. The header pins the
/// layout so a dump from a stale binary fails loudly instead of reading
/// garbage.
const MAGIC: u64 = u64::from_ne_bytes(*b"ZOUSTATS");
const FORMAT: u64 = 4;

pub const OP_NAMES: [&str; 6] = ["get", "get_range", "put_if_match", "put", "delete", "list"];
pub const CLASS_NAMES: [&str; 7] = ["manifest", "wal", "chk", "shards", "page", "file", "other"];
pub const TIER_NAMES: [&str; 4] = ["cache", "local", "store", "service"];
pub const PHASE_NAMES: [&str; 3] = ["park", "read", "ingest"];
pub const COMMIT_NAMES: [&str; 7] = [
    "push", "stage", "window", "dispatch", "put", "ack", "durable",
];

const KINDS: usize = OP_NAMES.len();
const CLASSES: usize = CLASS_NAMES.len();
const TIERS: usize = TIER_NAMES.len();
const PHASES: usize = PHASE_NAMES.len();
const STEPS: usize = COMMIT_NAMES.len();
const BUCKETS: usize = 32;

const HEADER: usize = 2;
const BUCKET_BASE: usize = HEADER + KINDS * CLASSES * 2;
const ERROR_BASE: usize = BUCKET_BASE + KINDS * BUCKETS;
const CONFLICT_SLOT: usize = ERROR_BASE + KINDS;
const TIER_BASE: usize = CONFLICT_SLOT + 1;
const PHASE_BASE: usize = TIER_BASE + TIERS * (2 + BUCKETS);
const STEP_BASE: usize = PHASE_BASE + PHASES * (1 + BUCKETS);
const SLOTS: usize = STEP_BASE + STEPS * (1 + BUCKETS);

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

const fn tier_slot(tier: usize) -> usize {
    TIER_BASE + tier * (2 + BUCKETS)
}

const fn phase_slot(phase: usize) -> usize {
    PHASE_BASE + phase * (1 + BUCKETS)
}

const fn step_slot(step: usize) -> usize {
    STEP_BASE + step * (1 + BUCKETS)
}

/// Which layout region a key belongs to, as an index into
/// [`CLASS_NAMES`]. Keys arrive with the tenants/ prefix still on, so
/// substring checks against the layout's directory names are enough.
/// The SHARD manifests classify with their layer objects, both live
/// under shards/ and both are page service traffic.
fn classify(key: &str) -> usize {
    if key.ends_with("MANIFEST") || key.contains("/manifests/") {
        0
    } else if key.contains("/wal/") {
        1
    } else if key.contains("/chk/") {
        2
    } else if key.contains("/shards/") {
        3
    } else if key.contains("/pg/") {
        4
    } else if key.contains("/files/") {
        5
    } else {
        6
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
        // An upgraded binary is the ordinary way to meet a file from
        // another layout, because the path is the same across restarts
        // and nothing deletes it. Stamping the new header over the old
        // counts would leave every slot meaning something it does not
        // hold, and the reader's header check would pass, because the
        // header it checks is the one just written. Zero it instead: a
        // process loses the counts of the process before it, which it
        // was never adding to anyway.
        let fresh = this.slot(0).load(Ordering::Relaxed) != MAGIC
            || this.slot(1).load(Ordering::Relaxed) != FORMAT;
        if fresh {
            for i in 0..SLOTS {
                this.slot(i).store(0, Ordering::Relaxed);
            }
        }
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
        THREAD_OPS.with(|c| c.set(c.get() + 1));
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

/// Which tier answered one smgr read, [`TIER_NAMES`] in enum form.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReadTier {
    /// The local page cache had the page, no reconstruction ran.
    Cache,
    /// Reconstruction ran entirely against local bytes, slab cache
    /// hits and local redo.
    Local,
    /// At least one store round trip happened inside the read.
    Store,
    /// The page service answered, timed at the backend from the send
    /// to the reply, so it carries the socket, the queue behind the
    /// serve loop and the read itself.
    Service,
}

/// What the page service driver was doing, [`PHASE_NAMES`] in enum
/// form. These are the driver's own clock, not any one backend's.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    /// A request waited for ingest to reach the LSN it asked for.
    /// Sampled once, when the request finally goes through.
    Park,
    /// One request planned and read, from ready to answered.
    Read,
    /// One ingest poll: the durable end, the catch up, any flush.
    /// The serve loop does this instead of answering, so it is
    /// somebody's read latency.
    Ingest,
}

/// One step of the commit path, [`COMMIT_NAMES`] in enum form. Each is
/// sampled once per whatever it measures: `push` and `stage` once per
/// chunk of WAL, `window`, `dispatch`, `put` and `ack` once per batch,
/// and `durable` once per chunk again, since that is the one a backend
/// is waiting on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Step {
    /// The pusher's own loop, from one append returning to the next
    /// starting. WAL that has been flushed locally sits here.
    Push,
    /// The append call: encode, admission, staging.
    Stage,
    /// How long the batch stayed open before the flusher closed it.
    Window,
    /// From the window closing to its PUT starting: the segment build,
    /// the queue, and the inflight bound when that is what binds.
    Dispatch,
    /// The store call that makes the window durable.
    Put,
    /// After that call returned, what the window waited on the windows
    /// before it, because a segment behind a hole is not durable.
    Ack,
    /// The whole of it from the append: window remainder, dispatch,
    /// put and ack together.
    Durable,
}

thread_local! {
    static THREAD_OPS: Cell<u64> = const { Cell::new(0) };
}

/// Store ops this thread has made through any [`StatsStore`], ever.
/// A read path samples it before and after to learn whether serving a
/// page left the process. Worker threads a read fans out to are not
/// covered, callers classify those paths by construction instead.
pub fn thread_ops() -> u64 {
    THREAD_OPS.with(|c| c.get())
}

/// The counter file for direct tier reporting, opened once per process
/// from `ZOU_STORE_STATS`. None when the variable is unset or the file
/// cannot be mapped, and every report is then a no-op.
fn global() -> Option<&'static Counters> {
    static GLOBAL: OnceLock<Option<Counters>> = OnceLock::new();
    GLOBAL
        .get_or_init(|| {
            let path = std::env::var("ZOU_STORE_STATS")
                .ok()
                .filter(|p| !p.is_empty())?;
            Counters::open(Path::new(&path)).ok()
        })
        .as_ref()
}

/// Count pages served by a tier. Split from the call sample so a
/// vectored read can attribute every page to the tier that served it
/// while its latency lands once, under the slowest tier it touched.
pub fn note_read_pages(tier: ReadTier, pages: u64) {
    if pages > 0
        && let Some(c) = global()
    {
        c.add(tier_slot(tier as usize) + 1, pages);
    }
}

/// Record one smgr read call under the tier that bounded it.
pub fn note_read_call(tier: ReadTier, elapsed: Duration) {
    if let Some(c) = global() {
        let t = tier as usize;
        c.add(tier_slot(t), 1);
        c.add(tier_slot(t) + 2 + bucket(elapsed), 1);
    }
}

/// Record one page service phase. Called from the driver's own
/// thread, which maps the same counter file as the backends it serves.
pub fn note_phase(phase: Phase, elapsed: Duration) {
    if let Some(c) = global() {
        let p = phase as usize;
        c.add(phase_slot(p), 1);
        c.add(phase_slot(p) + 1 + bucket(elapsed), 1);
    }
}

/// Record one step of the commit path. Called from the pusher, the
/// sequencer's flusher and its put workers, all of them in the
/// postmaster's process, so they all map the file the backends do.
pub fn note_commit(step: Step, elapsed: Duration) {
    if let Some(c) = global() {
        let s = step as usize;
        c.add(step_slot(s), 1);
        c.add(step_slot(s) + 1 + bucket(elapsed), 1);
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

    /// Not counted, because nothing happened: signing a url is
    /// arithmetic and the request it enables is made by somebody else,
    /// against the backend, without passing through here at all. That
    /// is the whole point of it, and it is also what makes the op
    /// counters stop being the whole story once passthrough is on.
    fn presigned_get(
        &self,
        key: &str,
        ttl: Duration,
        response: &[(&str, &str)],
    ) -> Result<Option<String>, CasError> {
        self.inner.presigned_get(key, ttl, response)
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
    /// The raw latency histogram, one slot per power of two, where slot
    /// b counted the ops that finished under 2^(b+1) microseconds. Kept
    /// out of the json because `zou stats` is read by a person and 32
    /// numbers per op is not, and kept in the struct because a scrape
    /// wants the buckets rather than percentiles somebody else has
    /// already averaged for it.
    #[serde(skip)]
    pub buckets: Vec<u64>,
}

#[derive(Debug, serde::Serialize)]
pub struct ClassSnapshot {
    pub class: &'static str,
    pub count: u64,
    pub bytes: u64,
}

/// One read tier, decoded from the file. Calls are smgr reads, pages
/// can exceed calls because vectored reads serve many pages per call.
#[derive(Debug, serde::Serialize)]
pub struct TierSnapshot {
    pub tier: &'static str,
    pub calls: u64,
    pub pages: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

/// One page service phase, decoded. Calls are samples, not requests:
/// a request that never parked is counted under read and not under
/// park, and an ingest poll is counted whether or not anybody was
/// waiting on it.
#[derive(Debug, serde::Serialize)]
pub struct PhaseSnapshot {
    pub phase: &'static str,
    pub calls: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

/// The whole counter file, decoded. This reads the file cold rather
/// than mapping it, so dumping never touches the counters.
#[derive(Debug, serde::Serialize)]
pub struct Snapshot {
    pub conflicts: u64,
    pub ops: Vec<OpSnapshot>,
    pub reads: Vec<TierSnapshot>,
    pub pagesvc: Vec<PhaseSnapshot>,
    pub commit: Vec<StepSnapshot>,
}

/// One step of the commit path, decoded. Samples rather than commits:
/// a batch carrying forty chunks reports one `window` and forty
/// `durable`, which is what makes the two comparable at all.
#[derive(Debug, serde::Serialize)]
pub struct StepSnapshot {
    pub step: &'static str,
    pub samples: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
    /// The raw histogram, kept for the same reason [`OpSnapshot`] keeps
    /// its own and left out of the json for the same reason too.
    #[serde(skip)]
    pub buckets: Vec<u64>,
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
        let data = fs::read(path).map_err(|e| {
            format!(
                "read {}: {e}, a counter file exists only where ZOU_STORE_STATS pointed a running node, and `zou dev` logs the path it used on boot",
                path.display()
            )
        })?;
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
                buckets,
            });
        }
        let mut reads = Vec::with_capacity(TIERS);
        for (tier, name) in TIER_NAMES.iter().copied().enumerate() {
            let calls = slot(tier_slot(tier));
            let buckets: Vec<u64> = (0..BUCKETS)
                .map(|b| slot(tier_slot(tier) + 2 + b))
                .collect();
            reads.push(TierSnapshot {
                tier: name,
                calls,
                pages: slot(tier_slot(tier) + 1),
                p50_us: percentile(&buckets, calls, 0.50),
                p95_us: percentile(&buckets, calls, 0.95),
                p99_us: percentile(&buckets, calls, 0.99),
                max_us: buckets
                    .iter()
                    .rposition(|&n| n > 0)
                    .map_or(0, |b| 1u64 << (b + 1)),
            });
        }
        let mut pagesvc = Vec::with_capacity(PHASES);
        for (phase, name) in PHASE_NAMES.iter().copied().enumerate() {
            let calls = slot(phase_slot(phase));
            let buckets: Vec<u64> = (0..BUCKETS)
                .map(|b| slot(phase_slot(phase) + 1 + b))
                .collect();
            pagesvc.push(PhaseSnapshot {
                phase: name,
                calls,
                p50_us: percentile(&buckets, calls, 0.50),
                p95_us: percentile(&buckets, calls, 0.95),
                p99_us: percentile(&buckets, calls, 0.99),
                max_us: buckets
                    .iter()
                    .rposition(|&n| n > 0)
                    .map_or(0, |b| 1u64 << (b + 1)),
            });
        }
        let mut commit = Vec::with_capacity(STEPS);
        for (step, name) in COMMIT_NAMES.iter().copied().enumerate() {
            let samples = slot(step_slot(step));
            let buckets: Vec<u64> = (0..BUCKETS)
                .map(|b| slot(step_slot(step) + 1 + b))
                .collect();
            commit.push(StepSnapshot {
                step: name,
                samples,
                p50_us: percentile(&buckets, samples, 0.50),
                p95_us: percentile(&buckets, samples, 0.95),
                p99_us: percentile(&buckets, samples, 0.99),
                max_us: buckets
                    .iter()
                    .rposition(|&n| n > 0)
                    .map_or(0, |b| 1u64 << (b + 1)),
                buckets,
            });
        }
        Ok(Self {
            conflicts: slot(CONFLICT_SLOT),
            ops,
            reads,
            pagesvc,
            commit,
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

    /// The counter file is the one thing a binary upgrade meets in
    /// place: same path, nothing deletes it, and the slot a number sat
    /// in under the last layout is a different number under this one.
    /// A process that stamped its own header over the old counts would
    /// then pass its own header check and serve them.
    #[test]
    fn a_counter_file_from_another_layout_starts_over_rather_than_being_reread() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stats");
        {
            let counters = Counters::open(&path).unwrap();
            counters.add(count_slot(0, 0), 7);
            counters.slot(1).store(FORMAT - 1, Ordering::Relaxed);
        }
        let counters = Counters::open(&path).unwrap();
        assert_eq!(
            counters.slot(count_slot(0, 0)).load(Ordering::Relaxed),
            0,
            "a count from another layout was kept"
        );
        assert_eq!(counters.slot(1).load(Ordering::Relaxed), FORMAT);

        // And the ordinary case, a restart at the same format, keeps
        // what the file holds: this is not a reason to lose counts.
        counters.add(count_slot(0, 0), 9);
        drop(counters);
        let again = Counters::open(&path).unwrap();
        assert_eq!(again.slot(count_slot(0, 0)).load(Ordering::Relaxed), 9);
    }

    #[test]
    fn keys_classify_by_layout_region() {
        assert_eq!(classify("tenants/a/MANIFEST"), 0);
        assert_eq!(classify("tenants/a/manifests/0-0.json"), 0);
        assert_eq!(classify("tenants/a/wal/0000000000000001/00.wal"), 1);
        assert_eq!(classify("tenants/a/chk/chk-1/INDEX"), 2);
        assert_eq!(classify("tenants/a/shards/0000/SHARD"), 3);
        assert_eq!(
            classify("tenants/a/shards/0000/i-00-ff-0000000000000001.il"),
            3
        );
        assert_eq!(classify("tenants/a/pg/1663/5/16384/0/00000001"), 4);
        assert_eq!(classify("tenants/a/files/avatars/pic.png"), 5);
        assert_eq!(classify("something-else"), 6);
    }

    #[test]
    fn read_tiers_round_trip_through_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let counters = Counters::open(&dir.path().join("stats")).unwrap();
        for _ in 0..3 {
            counters.add(tier_slot(ReadTier::Cache as usize), 1);
            counters.add(tier_slot(ReadTier::Cache as usize) + 1, 1);
            counters.add(
                tier_slot(ReadTier::Cache as usize) + 2 + bucket(Duration::from_micros(40)),
                1,
            );
        }
        counters.add(tier_slot(ReadTier::Store as usize), 1);
        counters.add(tier_slot(ReadTier::Store as usize) + 1, 128);
        counters.add(
            tier_slot(ReadTier::Store as usize) + 2 + bucket(Duration::from_millis(30)),
            1,
        );
        let snap = Snapshot::read(&dir.path().join("stats")).unwrap();
        let tier = |name: &str| snap.reads.iter().find(|t| t.tier == name).unwrap();
        assert_eq!(tier("cache").calls, 3);
        assert_eq!(tier("cache").pages, 3);
        assert!(tier("cache").p50_us >= 40 && tier("cache").p50_us < 128);
        assert_eq!(tier("local").calls, 0);
        assert_eq!(tier("store").calls, 1);
        assert_eq!(tier("store").pages, 128);
        assert!(tier("store").p50_us >= 30_000);
    }

    /// The phases are what the driver was doing, and a scrape has to
    /// be able to tell a request that waited on ingest from one that
    /// waited on the read, so they land in separate counters and both
    /// keep their own histogram.
    #[test]
    fn page_service_phases_round_trip_through_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let counters = Counters::open(&dir.path().join("stats")).unwrap();
        for _ in 0..4 {
            counters.add(phase_slot(Phase::Read as usize), 1);
            counters.add(
                phase_slot(Phase::Read as usize) + 1 + bucket(Duration::from_micros(90)),
                1,
            );
        }
        counters.add(phase_slot(Phase::Park as usize), 1);
        counters.add(
            phase_slot(Phase::Park as usize) + 1 + bucket(Duration::from_millis(200)),
            1,
        );
        let snap = Snapshot::read(&dir.path().join("stats")).unwrap();
        let phase = |name: &str| snap.pagesvc.iter().find(|p| p.phase == name).unwrap();
        assert_eq!(phase("read").calls, 4);
        assert!(phase("read").p50_us >= 90 && phase("read").p50_us < 256);
        assert_eq!(phase("park").calls, 1);
        assert!(phase("park").p50_us >= 200_000);
        assert_eq!(phase("ingest").calls, 0);
        assert_eq!(phase("ingest").max_us, 0);
    }

    /// The steps share a file with everything else, and a step written
    /// into the wrong region would land on a read tier or a page
    /// service phase and be believed. So the check is both ways: the
    /// steps come back, and nothing that was not written shows up.
    #[test]
    fn commit_steps_round_trip_through_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let counters = Counters::open(&dir.path().join("stats")).unwrap();
        for _ in 0..7 {
            counters.add(step_slot(Step::Durable as usize), 1);
            counters.add(
                step_slot(Step::Durable as usize) + 1 + bucket(Duration::from_millis(600)),
                1,
            );
        }
        counters.add(step_slot(Step::Put as usize), 1);
        counters.add(
            step_slot(Step::Put as usize) + 1 + bucket(Duration::from_micros(700)),
            1,
        );
        let snap = Snapshot::read(&dir.path().join("stats")).unwrap();
        let step = |name: &str| snap.commit.iter().find(|s| s.step == name).unwrap();
        assert_eq!(step("durable").samples, 7);
        assert!(step("durable").p50_us >= 600_000);
        assert_eq!(step("put").samples, 1);
        assert!(step("put").p50_us >= 700 && step("put").p50_us < 2048);
        for quiet in ["push", "stage", "window", "dispatch", "ack"] {
            assert_eq!(step(quiet).samples, 0);
            assert_eq!(step(quiet).max_us, 0);
        }
        assert!(snap.pagesvc.iter().all(|p| p.calls == 0));
        assert!(snap.reads.iter().all(|t| t.calls == 0));
    }

    #[test]
    fn thread_ops_count_every_wrapped_op() {
        let dir = tempfile::tempdir().unwrap();
        let store = wrap(dir.path(), &dir.path().join("stats"));
        let before = thread_ops();
        store.put("tenants/a/pg/1/2/3/0/00000000", b"x").unwrap();
        store.get("tenants/a/pg/1/2/3/0/00000000").unwrap();
        assert_eq!(thread_ops(), before + 2);
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
