//! Store op counters shared across every process touching one store.
//!
//! `ZOU_STORE_STATS` names a small counter file. [`StatsStore`] wraps the
//! opened backend, maps that file, and bumps fixed-slot atomics in place
//! on every op: count and bytes per op kind and key class, a power of two
//! microsecond latency histogram per op kind, io errors, and CAS
//! conflicts. The file is plain shared memory, so zou dev, initdb, and
//! every postgres backend all add into the same totals and nobody has to
//! flush anything on exit. [`Snapshot::read`] turns the file into json
//! for the benchmark harness, `zou stats <file>` is the cli for it, and
//! [`Snapshot::read_since`] takes the difference against a copy of the
//! file from earlier in the run, which is how a phase of a benchmark
//! reports what it cost rather than what everything before it cost too.
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
//! waiting for ingest to reach the LSN it asked for, how long it then
//! queued for a reader, how long the read itself took once it ran, and
//! how long the driver spent in ingest rather than answering anybody.
//! Ingest and the reads share a driver, so ingest time is read latency
//! for every request behind it and the only way to see that is to
//! measure it.
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
//!
//! Park time is the largest of the three phases on a loaded node and
//! the least self explanatory, so it has two counters of its own.
//! [`note_park_gap`] records how much WAL a parked read was ahead of
//! ingest when it parked, which separates a service that is behind
//! from one that is idle and being asked for a position nothing wrote.
//! [`note_park_cause`] records whether the WAL it waited through wrote
//! the pages it asked for at all, because a read waiting on somebody
//! else's writes is waiting for nothing and wants a different read
//! position rather than a faster ingest.

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
const FORMAT: u64 = 7;

pub const OP_NAMES: [&str; 6] = ["get", "get_range", "put_if_match", "put", "delete", "list"];
pub const CLASS_NAMES: [&str; 7] = ["manifest", "wal", "chk", "shards", "page", "file", "other"];
pub const TIER_NAMES: [&str; 4] = ["cache", "local", "store", "service"];
pub const PHASE_NAMES: [&str; 4] = ["park", "read", "ingest", "queue"];
pub const CAUSE_NAMES: [&str; 3] = ["touched", "untouched", "unclear"];
pub const COMMIT_NAMES: [&str; 7] = [
    "push", "stage", "window", "dispatch", "put", "ack", "durable",
];
/// Why a request was sent again, plus the one that was not sent again.
/// `throttle` is a 503 SlowDown or a 429, which is the bucket asking for
/// less traffic, `server` is any other 5xx, `transport` is a connection
/// that died under an idempotent request, and `exhausted` is the op that
/// used its whole attempt budget and failed anyway.
pub const RETRY_NAMES: [&str; 4] = ["throttle", "server", "transport", "exhausted"];

const KINDS: usize = OP_NAMES.len();
const CLASSES: usize = CLASS_NAMES.len();
const TIERS: usize = TIER_NAMES.len();
const PHASES: usize = PHASE_NAMES.len();
const STEPS: usize = COMMIT_NAMES.len();
const CAUSES: usize = CAUSE_NAMES.len();
const RETRIES: usize = RETRY_NAMES.len();
const BUCKETS: usize = 32;

const HEADER: usize = 2;
const BUCKET_BASE: usize = HEADER + KINDS * CLASSES * 2;
const ERROR_BASE: usize = BUCKET_BASE + KINDS * BUCKETS;
const CONFLICT_SLOT: usize = ERROR_BASE + KINDS;
const TIER_BASE: usize = CONFLICT_SLOT + 1;
const PHASE_BASE: usize = TIER_BASE + TIERS * (2 + BUCKETS);
const STEP_BASE: usize = PHASE_BASE + PHASES * (1 + BUCKETS);
const CAUSE_BASE: usize = STEP_BASE + STEPS * (1 + BUCKETS);
const GAP_BASE: usize = CAUSE_BASE + CAUSES;
const RETRY_BASE: usize = GAP_BASE + 1 + BUCKETS;
const SLOTS: usize = RETRY_BASE + RETRIES;

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
/// under shards/ and both are page service traffic. The wal is under
/// `log/`, which is where the cell chain, its sealed segments and its
/// round indexes all live, and `wal/` is the older layout that a store
/// written before the rename still has in it.
fn classify(key: &str) -> usize {
    if key.ends_with("MANIFEST") || key.contains("/manifests/") {
        0
    } else if key.contains("/log/") || key.contains("/wal/") {
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

/// The same buckets, for a plain quantity rather than a duration:
/// bucket b holds [2^b, 2^(b+1)), and zero lands in bucket 0.
fn magnitude(n: u64) -> usize {
    (n.max(1).ilog2() as usize).min(BUCKETS - 1)
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
    /// From the driver handing a request to the readers to a reader
    /// picking it up. Zero when the driver serves reads itself, and
    /// otherwise the thing to look at when reads are slow and none of
    /// them is: it is the pool being too small for the offered rate.
    Queue,
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

/// What a read that parked was actually waiting for, [`CAUSE_NAMES`]
/// in enum form.
///
/// A backend asks for a page at the lsn its WAL pusher has made
/// durable, which is a position for the whole tenant rather than for
/// the page in hand. So a park is one of two things, and they want
/// opposite fixes: WAL that wrote the pages asked for, which is a read
/// waiting for its own writes and can only be made faster by ingesting
/// faster, or WAL that wrote something else entirely, which is a read
/// waiting for nothing and wants a per block position instead.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ParkCause {
    /// The wal applied during the wait wrote at least one of the pages
    /// the request asked for.
    Touched,
    /// It wrote none of them.
    Untouched,
    /// A flush emptied the memtable while the request waited, so the
    /// evidence either way went to a layer and the wait is not
    /// classified rather than guessed at.
    Unclear,
}

/// Record what one park was waiting for. Called by the page service
/// driver when a parked request finally runs.
pub fn note_park_cause(cause: ParkCause) {
    if let Some(c) = global() {
        c.add(CAUSE_BASE + cause as usize, 1);
    }
}

/// Record how far ahead of ingest a read was when it parked, in wal
/// bytes, sampled once per request the first time it has to wait.
///
/// This is the other half of the park histogram. Park says how long
/// the wait was, this says how much wal the wait was for, and the two
/// together tell a service that is behind from a service that is idle
/// and being asked for a position nothing has reached.
pub fn note_park_gap(bytes: u64) {
    if let Some(c) = global() {
        c.add(GAP_BASE, 1);
        c.add(GAP_BASE + 1 + magnitude(bytes), 1);
    }
}

/// Why a request went out again, [`RETRY_NAMES`] in enum form.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Retry {
    /// 503 SlowDown or 429, the bucket asking for less traffic.
    Throttle,
    /// Any other 5xx.
    Server,
    /// The connection died under an idempotent request.
    Transport,
    /// Not a retry: the op spent its whole attempt budget and failed.
    Exhausted,
}

/// Record one retry, or one op that ran out of attempts.
///
/// Counted where the retry happens, inside the backend, rather than in
/// [`StatsStore`], which wraps outermost and sees one op taking a long
/// time. A phase whose puts averaged two seconds because the bucket
/// asked for less traffic and a phase whose puts averaged two seconds
/// because the objects were large are the same latency histogram and
/// two different problems, and this is the counter that separates them.
pub fn note_retry(kind: Retry) {
    if let Some(c) = global() {
        c.add(RETRY_BASE + kind as usize, 1);
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
    pub park_cause: Vec<CauseSnapshot>,
    pub park_gap: GapSnapshot,
    pub retries: Vec<RetrySnapshot>,
}

/// How many requests went out a second time, by what made them.
#[derive(Debug, serde::Serialize)]
pub struct RetrySnapshot {
    pub kind: &'static str,
    pub count: u64,
}

/// How many parked reads were waiting for wal that wrote the pages
/// they asked for, and how many were not.
#[derive(Debug, serde::Serialize)]
pub struct CauseSnapshot {
    pub cause: &'static str,
    pub parks: u64,
}

/// How far ahead of ingest the reads that parked were, in wal bytes.
#[derive(Debug, serde::Serialize)]
pub struct GapSnapshot {
    pub samples: u64,
    pub p50_bytes: u64,
    pub p95_bytes: u64,
    pub p99_bytes: u64,
    pub max_bytes: u64,
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

/// The counter slots of one file, header checked.
fn slots(path: &Path) -> Result<Vec<u64>, String> {
    let data = fs::read(path).map_err(|e| {
        format!(
            "read {}: {e}, a counter file exists only where ZOU_STORE_STATS pointed a running node, and `zou dev` logs the path it used on boot",
            path.display()
        )
    })?;
    if data.len() < SLOTS * 8 {
        return Err(format!("{} is not a counter file", path.display()));
    }
    let slots: Vec<u64> = (0..SLOTS)
        .map(|i| u64::from_ne_bytes(data[i * 8..i * 8 + 8].try_into().unwrap()))
        .collect();
    if slots[0] != MAGIC || slots[1] != FORMAT {
        return Err(format!(
            "{} is not a format {FORMAT} counter file",
            path.display()
        ));
    }
    Ok(slots)
}

impl Snapshot {
    pub fn read(path: &Path) -> Result<Self, String> {
        Ok(Self::decode(&slots(path)?))
    }

    /// The same file minus a copy of it taken earlier, which is what a
    /// run needs to say what one phase cost rather than what the whole
    /// run cost. Every slot in the file only ever goes up, so the
    /// subtraction is the counters of the window between the two, right
    /// down to the histograms: percentiles come out of the difference of
    /// the buckets rather than of two sets of percentiles, which is the
    /// only way to get them at all.
    pub fn read_since(path: &Path, earlier: &Path) -> Result<Self, String> {
        let now = slots(path)?;
        let then = slots(earlier)?;
        let mut delta = Vec::with_capacity(SLOTS);
        for (i, (a, b)) in now.iter().zip(then.iter()).enumerate() {
            if i < HEADER {
                delta.push(*a);
                continue;
            }
            let Some(d) = a.checked_sub(*b) else {
                return Err(format!(
                    "{} counted less than {} did, so they are not the same file at two times and the difference would be nonsense",
                    path.display(),
                    earlier.display()
                ));
            };
            delta.push(d);
        }
        Ok(Self::decode(&delta))
    }

    fn decode(slots: &[u64]) -> Self {
        let slot = |i: usize| slots[i];
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
        let park_cause = CAUSE_NAMES
            .iter()
            .copied()
            .enumerate()
            .map(|(cause, name)| CauseSnapshot {
                cause: name,
                parks: slot(CAUSE_BASE + cause),
            })
            .collect();
        let gaps: Vec<u64> = (0..BUCKETS).map(|b| slot(GAP_BASE + 1 + b)).collect();
        let samples = slot(GAP_BASE);
        let park_gap = GapSnapshot {
            samples,
            p50_bytes: percentile(&gaps, samples, 0.50),
            p95_bytes: percentile(&gaps, samples, 0.95),
            p99_bytes: percentile(&gaps, samples, 0.99),
            max_bytes: gaps
                .iter()
                .rposition(|&n| n > 0)
                .map_or(0, |b| 1u64 << (b + 1)),
        };
        let retries = RETRY_NAMES
            .iter()
            .copied()
            .enumerate()
            .map(|(kind, name)| RetrySnapshot {
                kind: name,
                count: slot(RETRY_BASE + kind),
            })
            .collect();
        Self {
            conflicts: slot(CONFLICT_SLOT),
            ops,
            reads,
            pagesvc,
            commit,
            park_cause,
            park_gap,
            retries,
        }
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
        assert_eq!(classify("tenants/a/log/cellwal/0000/0000000000000ccb"), 1);
        assert_eq!(
            classify("tenants/a/log/cellwal-sealed/0000/0000000000000001-0000000000000064.seg"),
            1
        );
        assert_eq!(
            classify("tenants/a/log/cellwal-rounds/0000/00000001.idx"),
            1
        );
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

    /// The park counters are the last two regions of the file, so they
    /// are the ones a slot arithmetic mistake lands past the end of or
    /// on top of the commit steps. The gap side also has to keep a park
    /// with nothing outstanding apart from a park behind three
    /// megabytes of wal, because those are the two answers the
    /// histogram exists to tell apart.
    #[test]
    fn park_gaps_and_causes_round_trip_through_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let counters = Counters::open(&dir.path().join("stats")).unwrap();
        for _ in 0..9 {
            counters.add(CAUSE_BASE + ParkCause::Untouched as usize, 1);
        }
        counters.add(CAUSE_BASE + ParkCause::Touched as usize, 1);
        counters.add(GAP_BASE, 2);
        counters.add(GAP_BASE + 1 + magnitude(0), 1);
        counters.add(GAP_BASE + 1 + magnitude(3 << 20), 1);
        let snap = Snapshot::read(&dir.path().join("stats")).unwrap();
        let cause = |name: &str| snap.park_cause.iter().find(|c| c.cause == name).unwrap();
        assert_eq!(cause("untouched").parks, 9);
        assert_eq!(cause("touched").parks, 1);
        assert_eq!(cause("unclear").parks, 0);
        assert_eq!(snap.park_gap.samples, 2);
        assert_eq!(snap.park_gap.p50_bytes, 2);
        assert_eq!(snap.park_gap.p99_bytes, 4 << 20);
        assert_eq!(snap.park_gap.max_bytes, 4 << 20);
        assert!(snap.commit.iter().all(|s| s.samples == 0));
        assert!(snap.pagesvc.iter().all(|p| p.calls == 0));
    }

    /// The retries are the newest region and so the one a slot
    /// arithmetic mistake writes past the end of the file for. They
    /// also have to stay apart from each other: a bucket asking for
    /// less traffic and a network that dropped the connection are the
    /// same delay to a caller and two different things to fix.
    #[test]
    fn retries_round_trip_through_the_file_and_stay_apart() {
        let dir = tempfile::tempdir().unwrap();
        let counters = Counters::open(&dir.path().join("stats")).unwrap();
        for _ in 0..5 {
            counters.add(RETRY_BASE + Retry::Throttle as usize, 1);
        }
        counters.add(RETRY_BASE + Retry::Transport as usize, 1);
        counters.add(RETRY_BASE + Retry::Exhausted as usize, 1);
        let snap = Snapshot::read(&dir.path().join("stats")).unwrap();
        let retry = |name: &str| snap.retries.iter().find(|r| r.kind == name).unwrap();
        assert_eq!(retry("throttle").count, 5);
        assert_eq!(retry("transport").count, 1);
        assert_eq!(retry("exhausted").count, 1);
        assert_eq!(retry("server").count, 0);
        assert!(snap.park_cause.iter().all(|c| c.parks == 0));
        assert!(snap.commit.iter().all(|s| s.samples == 0));
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

    /// One phase of a run is the counters at its end minus the counters
    /// at its start, and the reason to take that difference in the
    /// buckets rather than between two sets of percentiles is that a
    /// fast phase after a slow one has to read as fast.
    #[test]
    fn a_snapshot_since_an_earlier_copy_is_the_window_between_them() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stats");
        let counters = Counters::open(&path).unwrap();
        let put = count_slot(Op::Put as usize, 4);
        let hist = BUCKET_BASE + (Op::Put as usize) * BUCKETS;
        for _ in 0..9 {
            counters.add(put, 1);
            counters.add(put + 1, 8192);
            counters.add(hist + bucket(Duration::from_millis(50)), 1);
        }
        let mark = dir.path().join("mark");
        fs::copy(&path, &mark).unwrap();
        counters.add(put, 1);
        counters.add(put + 1, 100);
        counters.add(hist + bucket(Duration::from_micros(30)), 1);

        let ops = |s: &Snapshot| {
            let o = s.ops.iter().find(|o| o.op == "put").unwrap();
            (o.count, o.bytes, o.p50_us)
        };
        let (count, bytes, p50) = ops(&Snapshot::read(&path).unwrap());
        assert_eq!((count, bytes), (10, 73828));
        assert!(
            p50 >= 50_000,
            "the whole run is nine slow puts and one fast"
        );

        let (count, bytes, p50) = ops(&Snapshot::read_since(&path, &mark).unwrap());
        assert_eq!((count, bytes), (1, 100));
        assert!(p50 < 64, "the window after the mark is the fast put alone");

        // The other way round is an earlier file that counted more than
        // the later one, which happens when the file was reset between
        // the two and means the subtraction would wrap rather than say
        // anything, so it is an error and not a number.
        assert!(Snapshot::read_since(&mark, &path).is_err());
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
