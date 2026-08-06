//! C ABI shim between Postgres and zou-store.
//!
//! The zou storage manager patch (patches/0001) routes relation page I/O
//! to these functions. Pages live as objects under the tenant's pg/
//! prefix: one object per block plus a SIZE marker per fork whose
//! presence means the fork exists and whose content is the block count.
//! A block object missing below the size reads as zeros, which mirrors
//! the file hole semantics of the stock md storage manager.
//!
//! This v0 page store is mutable derived data. The durable truth becomes
//! WAL plus checkpoints as the milestone progresses, at which point reads
//! route through checkpoint objects instead. The FFI surface stays.
//!
//! Three things keep the round trips off the query path: skipFsync
//! pages buffer in this process and drain in parallel when durability
//! comes due (see the pending module), the vectored entry points fan
//! independent gets, puts, and deletes across a small thread pool, and
//! every page written to or read from the store also lands in a local
//! write-through cache that answers reads first (see the pagecache
//! module), so a warm working set never leaves the machine.
//!
//! Every function returns 0 for success or a negative ZOU_ERR code, and
//! never unwinds into C. Postgres turns nonzero into ereport(ERROR).

pub mod bootstrap;
pub mod cache;
pub mod capture;
pub mod fold;
pub mod gc;
pub mod getpage;
pub mod ingest;
pub mod pagecache;
pub mod pending;
pub mod reader;
pub mod redo;
pub mod restore;
pub mod walscan;

/// A real shard sequencer over any store for the writer side of tests:
/// the same take_over plus resume wiring open_wal_pipe does, minus the
/// lease machinery, so fold, reader, and restore tests push tenant WAL
/// through the actual chain instead of a mock.
#[cfg(test)]
pub(crate) mod testv2 {
    use std::sync::Arc;

    use zou_log::{MediaSink, Sequencer, SequencerConfig, WalMedia, take_over};
    use zou_store::{CasStore, Frame2, Lsn, tenant_id};

    use crate::WAL_SHARD;

    pub struct V2Wal {
        pub media: Arc<WalMedia>,
        pub seq: Sequencer,
        pub tenant: u128,
        pub epoch: u32,
        first_appended: bool,
    }

    impl V2Wal {
        pub fn open(store: Arc<dyn CasStore>, tenant_ref: &str, epoch: u32) -> Self {
            let media = Arc::new(WalMedia::single(store));
            let takeover = take_over(&media, WAL_SHARD, "testv2").unwrap();
            let sink = Arc::new(MediaSink::new(Arc::clone(&media), WAL_SHARD));
            let seq = Sequencer::resume(
                WAL_SHARD,
                sink,
                SequencerConfig::default(),
                takeover.next_seq,
                takeover.prev_digest,
            );
            Self {
                media,
                seq,
                tenant: tenant_id(tenant_ref),
                epoch,
                first_appended: false,
            }
        }

        /// Append one chunk of tenant WAL at `pg_lsn` and wait until it
        /// is durable on the chain.
        pub fn push(&mut self, pg_lsn: u64, bytes: &[u8]) {
            let frame = Frame2 {
                tenant: self.tenant,
                writer_epoch: self.epoch,
                start_lsn: Lsn(pg_lsn),
                end_lsn: Lsn(pg_lsn + bytes.len() as u64),
                contains_commit: true,
                first_of_epoch: !self.first_appended,
                hints: Vec::new(),
                payload: bytes.to_vec(),
            };
            self.seq.append(vec![frame]).unwrap().wait().unwrap();
            self.first_appended = true;
        }

        pub fn close(self) {
            self.seq.close().unwrap();
        }
    }
}

use std::ffi::{CStr, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::time::{Duration, Instant, SystemTime};

use zou_log::{
    AppendError, AppendTicket, MediaSink, Sequencer, WalMedia, consolidate, gc_landing, stream_end,
    take_over,
};
use zou_store::heartbeat::Heartbeat;
use zou_store::layout::TenantLayout;
use zou_store::lease;
use zou_store::stats;
use zou_store::{CasStore, Frame2, HeldLease, Lsn, Manifest, open_store, tenant_id};

/// Postgres BLCKSZ. The patch checks this against its own BLCKSZ at init.
pub const ZOU_PAGE_SIZE: usize = 8192;

pub const ZOU_OK: i32 = 0;
pub const ZOU_ERR_STORE: i32 = -1;
pub const ZOU_ERR_NOT_INITIALIZED: i32 = -2;
pub const ZOU_ERR_PANIC: i32 = -3;
pub const ZOU_ERR_BAD_ARGUMENT: i32 = -4;
pub const ZOU_ERR_LEASE_HELD: i32 = -5;
pub const ZOU_ERR_LEASE_LOST: i32 = -6;

/// [`zou_wal_fold_poll`] answers, and [`zou_wal_fold_start`] borrows
/// RUNNING for a refused second start. Errors stay negative.
pub const ZOU_FOLD_IDLE: i32 = 0;
pub const ZOU_FOLD_RUNNING: i32 = 1;
pub const ZOU_FOLD_DONE_DELTA: i32 = 2;
pub const ZOU_FOLD_DONE_FULL: i32 = 3;

/// The chain reader behind the smgr read path. Unset until the first
/// read, which attaches lazily so init stays cheap and a store with no
/// checkpoints yet costs nothing. Off means every read goes to pg/,
/// either by choice, by attach failure, or because there is no chain
/// to serve. Fatal means the tenant is a branch whose inherited state
/// cannot be served, pg/ would answer zeros where the parent's pages
/// belong, so every read errors instead of lying. Backends are single
/// threaded, the mutex sees no contention.
enum ReaderSlot {
    Unset,
    Off,
    Fatal,
    On(Box<reader::ChainReader>),
}

struct Shim {
    store: Arc<dyn CasStore>,
    layout: TenantLayout,
    reader: Mutex<ReaderSlot>,
    /// skipFsync pages waiting for their drain, see the pending module.
    /// Locked after neither of the other locks, drains release it
    /// before touching the store.
    pending: Mutex<pending::Pending>,
    /// Write-through local page cache, `None` when ZOU_PAGE_CACHE is
    /// unset. Plain files, no lock, cross process safety comes from
    /// bufmgr never running IO on one block from two processes.
    cache: Option<pagecache::PageCache>,
}

static SHIM: OnceLock<Shim> = OnceLock::new();

/// Serve one page from the checkpoint chain, `None` when pg/ must
/// answer. `ZOU_CHAIN_READER=0` is the escape hatch that pins every
/// read to pg/.
/// atexit hook logging the cache hit rate summary. The reader lives in
/// a static, and statics never drop when a Postgres process leaves
/// through C's exit(), so Drop cannot do this.
extern "C" fn log_cache_summary_at_exit() {
    if let Some(shim) = SHIM.get()
        && let Ok(slot) = shim.reader.lock()
        && let ReaderSlot::On(rd) = &*slot
    {
        rd.log_cache_summary();
    }
}

/// Run `f` against the attached reader. `Ok(None)` means there is no
/// reader and pg/ answers alone, `Err(())` means the attach failed
/// fatally, a branch whose inherited pages nothing can serve, and the
/// caller must error instead of touching pg/.
fn with_reader<R>(
    shim: &Shim,
    f: impl FnOnce(&mut reader::ChainReader) -> R,
) -> Result<Option<R>, ()> {
    unsafe extern "C" {
        fn atexit(cb: extern "C" fn()) -> i32;
    }
    let mut slot = shim.reader.lock().map_err(|_| ())?;
    if matches!(*slot, ReaderSlot::Unset) {
        *slot = if std::env::var("ZOU_CHAIN_READER").is_ok_and(|v| v == "0") {
            ReaderSlot::Off
        } else {
            match reader::ChainReader::attach(&shim.store, &shim.layout) {
                Ok(Some(rd)) => {
                    unsafe { atexit(log_cache_summary_at_exit) };
                    ReaderSlot::On(Box::new(rd))
                }
                Ok(None) => ReaderSlot::Off,
                Err(e) if e.fatal => {
                    log::error!("zou chain reader cannot serve this branch: {}", e.why);
                    ReaderSlot::Fatal
                }
                Err(e) => {
                    log::warn!(
                        "zou chain reader attach failed, reads stay on pg/: {}",
                        e.why
                    );
                    ReaderSlot::Off
                }
            }
        };
    }
    match &mut *slot {
        ReaderSlot::On(rd) => Ok(Some(f(rd))),
        ReaderSlot::Fatal => Err(()),
        _ => Ok(None),
    }
}

fn chain_read(shim: &Shim, r: walscan::BlockRef, durable: u64) -> Result<Option<Vec<u8>>, ()> {
    with_reader(shim, |rd| rd.read(&*shim.store, r, durable)).map(Option::flatten)
}

/// Tell an attached reader this process wrote a page, see
/// [`reader::ChainReader::note_write`].
fn note_write(shim: &Shim, r: walscan::BlockRef) {
    if let Ok(mut slot) = shim.reader.lock()
        && let ReaderSlot::On(rd) = &mut *slot
    {
        rd.note_write(r);
    }
}

/// Tell an attached reader a whole relation changed shape locally.
fn note_rel(shim: &Shim, spc: u32, db: u32, rel: u32) {
    if let Ok(mut slot) = shim.reader.lock()
        && let ReaderSlot::On(rd) = &mut *slot
    {
        rd.note_rel(walscan::RelTag { spc, db, rel });
    }
}

/// Tell an attached reader this process truncated a fork. A main fork
/// truncate keeps blocks below the cutoff serving from the chain,
/// which a branch needs, its pg/ prefix has no copy of inherited
/// survivors. Truncating any other fork leaves the main fork alone.
fn note_truncate(shim: &Shim, spc: u32, db: u32, rel: u32, fork: u32, nblocks: u32) {
    if let Ok(mut slot) = shim.reader.lock()
        && let ReaderSlot::On(rd) = &mut *slot
    {
        let cut = if fork == 0 { nblocks } else { u32::MAX };
        rd.note_truncate(walscan::RelTag { spc, db, rel }, cut);
    }
}

fn wrap(f: impl FnOnce() -> i32) -> i32 {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or(ZOU_ERR_PANIC)
}

/// A copy of the page this process buffered for the block, if any.
fn pending_page(shim: &Shim, fork: pending::ForkId, blk: u32) -> Option<Vec<u8>> {
    let slot = shim.pending.lock().ok()?;
    slot.page(fork, blk).map(<[u8]>::to_vec)
}

/// Drain one fork's buffered pages to the store, or every fork when
/// `fork` is None. The buffer empties under the lock, the store work
/// happens outside it. A failed flush surfaces as ZOU_ERR_STORE and
/// aborts the transaction, which drops the relation the pages were
/// building.
fn drain_pending(shim: &Shim, fork: Option<pending::ForkId>) -> i32 {
    let drained = {
        let Ok(mut slot) = shim.pending.lock() else {
            return ZOU_ERR_STORE;
        };
        match fork {
            Some(id) => slot
                .take_fork(id)
                .map(|(pages, size)| vec![(id, pages, size)])
                .unwrap_or_default(),
            None => slot.take_all(),
        }
    };
    for (id, pages, size) in drained {
        if pending::flush_fork(
            &shim.store,
            &shim.layout,
            shim.cache.as_ref(),
            id,
            &pages,
            size,
        )
        .is_err()
        {
            return ZOU_ERR_STORE;
        }
    }
    ZOU_OK
}

/// Install the env_logger backend once per process. Output goes to
/// stderr, which the server collects into its own log, so Rust lines
/// land next to Postgres's. `RUST_LOG` in the server environment
/// filters them and info is the default.
fn init_logging() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .try_init();
    });
}

fn with_shim(f: impl FnOnce(&'static Shim) -> i32) -> i32 {
    wrap(|| match SHIM.get() {
        Some(shim) => f(shim),
        None => ZOU_ERR_NOT_INITIALIZED,
    })
}

fn read_size(shim: &Shim, spc: u32, db: u32, rel: u32, fork: u32) -> Result<Option<u32>, ()> {
    if let Some(cache) = &shim.cache
        && let Some(n) = cache.load_size((spc, db, rel, fork))
    {
        return Ok(Some(n));
    }
    let key = shim.layout.pg_size(spc, db, rel, fork);
    match shim.store.get(&key) {
        Ok(Some((data, _))) => {
            let bytes: [u8; 4] = data.as_slice().try_into().map_err(|_| ())?;
            let n = u32::from_le_bytes(bytes);
            if let Some(cache) = &shim.cache {
                cache.save_size((spc, db, rel, fork), n);
            }
            Ok(Some(n))
        }
        Ok(None) => Ok(None),
        Err(_) => Err(()),
    }
}

/// The fork size for the read side: the tenant's own SIZE object when
/// one exists, an own extend, truncate, or create always writes it
/// eagerly, and otherwise the chain's folded sizes, but only on a
/// branch. A branch inherits forks that have no SIZE under its own
/// prefix, the s lines of the owner's folds are the only place their
/// lengths live. An unbranched tenant's own prefix is complete, its
/// absent SIZE means the fork does not exist and the chain would
/// resurrect just dropped relations until the next fold names them.
fn read_size_chained(
    shim: &Shim,
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
) -> Result<Option<u32>, ()> {
    let pending = shim
        .pending
        .lock()
        .map_err(|_| ())?
        .size((spc, db, rel, fork));
    if let Some(n) = read_size(shim, spc, db, rel, fork)? {
        return Ok(Some(pending.map_or(n, |p| p.max(n))));
    }
    let chained = with_reader(shim, |rd| {
        if rd.branched() {
            rd.fork_size(spc, db, rel, fork)
        } else {
            None
        }
    })
    .map(Option::flatten)?;
    Ok(match (chained, pending) {
        (Some(n), Some(p)) => Some(n.max(p)),
        (n, p) => n.or(p),
    })
}

fn write_size(shim: &Shim, spc: u32, db: u32, rel: u32, fork: u32, nblocks: u32) -> Result<(), ()> {
    let key = shim.layout.pg_size(spc, db, rel, fork);
    shim.store
        .put(&key, &nblocks.to_le_bytes())
        .map_err(|_| ())?;
    // Local copy only after the store accepted, so a cached size is
    // never newer than the durable one.
    if let Some(cache) = &shim.cache {
        cache.save_size((spc, db, rel, fork), nblocks);
    }
    Ok(())
}

/// Block index of a pg/ object key, parsed from the 8 hex digit tail.
/// SIZE and anything unparseable return None.
fn block_index(key: &str) -> Option<u32> {
    let tail = key.rsplit('/').next()?;
    if tail.len() != 8 {
        return None;
    }
    u32::from_str_radix(tail, 16).ok()
}

/// Open the store for this process. Idempotent, every Postgres process
/// (postmaster, backends, checkpointer) calls it on startup. The target
/// is a local directory or an object store URL like `s3://bucket/prefix`,
/// see `zou_store::open_store` for the environment the URL forms read.
///
/// # Safety
/// `target` must be a valid NUL terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_pg_init(target: *const c_char) -> i32 {
    wrap(|| {
        init_logging();
        if target.is_null() {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let Ok(target) = unsafe { CStr::from_ptr(target) }.to_str() else {
            return ZOU_ERR_BAD_ARGUMENT;
        };
        let store = match open_store(target) {
            Ok(store) => store,
            Err(e) => {
                log::error!("zou_pg_init: {e}");
                return ZOU_ERR_BAD_ARGUMENT;
            }
        };
        // A restored branch server writes under its own tenant prefix,
        // ZOU_TENANT names it and single node setups stay on local.
        let tenant = std::env::var("ZOU_TENANT").unwrap_or_else(|_| "local".to_string());
        let _ = SHIM.set(Shim {
            store: Arc::from(store),
            layout: TenantLayout::new(&tenant),
            reader: Mutex::new(ReaderSlot::Unset),
            pending: Mutex::new(pending::Pending::default()),
            cache: pagecache::PageCache::from_env(),
        });
        ZOU_OK
    })
}

/// Create a fork: write SIZE=0 unless it already exists.
#[unsafe(no_mangle)]
pub extern "C" fn zou_smgr_create(spc: u32, db: u32, rel: u32, fork: u32) -> i32 {
    with_shim(|shim| {
        // A create can reuse a relfilenode a dropped relation once
        // held, run images of the old incarnation must not serve.
        note_rel(shim, spc, db, rel);
        match read_size(shim, spc, db, rel, fork) {
            Ok(Some(_)) => ZOU_OK,
            Ok(None) => match write_size(shim, spc, db, rel, fork, 0) {
                Ok(()) => ZOU_OK,
                Err(()) => ZOU_ERR_STORE,
            },
            Err(()) => ZOU_ERR_STORE,
        }
    })
}

/// Does the fork exist? Writes 1 or 0 into `out`.
///
/// # Safety
/// `out` must be a valid pointer to an i32.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_smgr_exists(
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    out: *mut i32,
) -> i32 {
    with_shim(|shim| {
        if out.is_null() {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        match read_size_chained(shim, spc, db, rel, fork) {
            Ok(size) => {
                unsafe { *out = size.is_some() as i32 };
                ZOU_OK
            }
            Err(()) => ZOU_ERR_STORE,
        }
    })
}

/// Block count of a fork, into `out`. Missing fork is an error, Postgres
/// only asks for forks it believes exist.
///
/// # Safety
/// `out` must be a valid pointer to a u32.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_smgr_nblocks(
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    out: *mut u32,
) -> i32 {
    with_shim(|shim| {
        if out.is_null() {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        match read_size_chained(shim, spc, db, rel, fork) {
            Ok(Some(n)) => {
                unsafe { *out = n };
                ZOU_OK
            }
            Ok(None) => ZOU_ERR_STORE,
            Err(()) => ZOU_ERR_STORE,
        }
    })
}

/// One page served by one tier: the page count and the call sample
/// together, the single block read shape.
fn note_read(tier: stats::ReadTier, pages: u64, started: Instant) {
    stats::note_read_pages(tier, pages);
    stats::note_read_call(tier, started.elapsed());
}

/// Read one page into `buf`. An absent block object reads as zeros,
/// matching md's file hole semantics for blocks extended but not yet
/// written. `durable_lsn` is the wal pusher's published durable LSN at
/// the moment of the call, zero when none is published; the chain
/// reader uses it to skip its freshness barrier when the mirrored
/// stream has not advanced.
///
/// # Safety
/// `buf` must be a valid pointer to ZOU_PAGE_SIZE writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_smgr_read(
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    blk: u32,
    buf: *mut u8,
    durable_lsn: u64,
) -> i32 {
    with_shim(|shim| {
        if buf.is_null() {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let out = unsafe { std::slice::from_raw_parts_mut(buf, ZOU_PAGE_SIZE) };
        let started = Instant::now();
        let ops_before = stats::thread_ops();
        if let Some(page) = pending_page(shim, (spc, db, rel, fork), blk) {
            out.copy_from_slice(&page);
            note_read(stats::ReadTier::Cache, 1, started);
            return ZOU_OK;
        }
        if let Some(cache) = &shim.cache
            && let Some(page) = cache.load((spc, db, rel, fork), blk)
        {
            out.copy_from_slice(&page);
            note_read(stats::ReadTier::Cache, 1, started);
            return ZOU_OK;
        }
        let r = walscan::BlockRef {
            spc,
            db,
            rel,
            fork,
            blk,
        };
        match chain_read(shim, r, durable_lsn) {
            Ok(Some(page)) => {
                out.copy_from_slice(&page);
                if let Some(cache) = &shim.cache {
                    cache.save((spc, db, rel, fork), blk, &page);
                }
                // Slab misses and barrier probes both leave the
                // process, the op delta is what tells a local
                // reconstruction from one that paid a round trip.
                let tier = if stats::thread_ops() > ops_before {
                    stats::ReadTier::Store
                } else {
                    stats::ReadTier::Local
                };
                note_read(tier, 1, started);
                return ZOU_OK;
            }
            Ok(None) => {}
            Err(()) => return ZOU_ERR_STORE,
        }
        match shim
            .store
            .get(&shim.layout.pg_block(spc, db, rel, fork, blk))
        {
            Ok(Some((data, _))) if data.len() == ZOU_PAGE_SIZE => {
                out.copy_from_slice(&data);
                if let Some(cache) = &shim.cache {
                    cache.save((spc, db, rel, fork), blk, &data);
                }
                note_read(stats::ReadTier::Store, 1, started);
                ZOU_OK
            }
            Ok(Some(_)) => ZOU_ERR_STORE,
            Ok(None) => {
                // Absent blocks read as zeros and stay out of the
                // cache, absence is already a local answer once SIZE
                // is known and zeros cached across a truncate and
                // re-extend would need their own invalidation.
                out.fill(0);
                note_read(stats::ReadTier::Store, 1, started);
                ZOU_OK
            }
            Err(_) => ZOU_ERR_STORE,
        }
    })
}

/// Read a run of `nblocks` pages starting at `blk` into `bufs`. Each
/// page resolves like zou_smgr_read, buffered pages and the chain
/// answer first, and whatever is left goes to pg/ as one batch of
/// parallel gets.
///
/// # Safety
/// `bufs` must point to `nblocks` valid pointers, each to
/// ZOU_PAGE_SIZE writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_smgr_readv(
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    blk: u32,
    bufs: *const *mut u8,
    nblocks: u32,
    durable_lsn: u64,
) -> i32 {
    with_shim(|shim| {
        if bufs.is_null() || nblocks == 0 {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let ptrs = unsafe { std::slice::from_raw_parts(bufs, nblocks as usize) };
        if ptrs.iter().any(|p| p.is_null()) {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let started = Instant::now();
        let ops_before = stats::thread_ops();
        let mut cache_pages = 0u64;
        let mut chain_pages = 0u64;
        let mut misses: Vec<(usize, u32)> = Vec::new();
        for (i, ptr) in ptrs.iter().enumerate() {
            let out = unsafe { std::slice::from_raw_parts_mut(*ptr, ZOU_PAGE_SIZE) };
            let b = blk + i as u32;
            if let Some(page) = pending_page(shim, (spc, db, rel, fork), b) {
                out.copy_from_slice(&page);
                cache_pages += 1;
                continue;
            }
            if let Some(cache) = &shim.cache
                && let Some(page) = cache.load((spc, db, rel, fork), b)
            {
                out.copy_from_slice(&page);
                cache_pages += 1;
                continue;
            }
            let r = walscan::BlockRef {
                spc,
                db,
                rel,
                fork,
                blk: b,
            };
            match chain_read(shim, r, durable_lsn) {
                Ok(Some(page)) => {
                    out.copy_from_slice(&page);
                    if let Some(cache) = &shim.cache {
                        cache.save((spc, db, rel, fork), b, &page);
                    }
                    chain_pages += 1;
                }
                Ok(None) => misses.push((i, b)),
                Err(()) => return ZOU_ERR_STORE,
            }
        }
        // The remaining blocks all live under pg/ and their gets are
        // independent, so they fan out across the shim thread pool.
        // Each job owns a distinct output buffer, so worker threads
        // never write the same bytes.
        struct Job(u32, *mut u8);
        unsafe impl Send for Job {}
        unsafe impl Sync for Job {}
        let jobs: Vec<Job> = misses.iter().map(|(i, b)| Job(*b, ptrs[*i])).collect();
        let ok = pending::for_each_parallel(&jobs, |Job(b, ptr)| {
            let out = unsafe { std::slice::from_raw_parts_mut(*ptr, ZOU_PAGE_SIZE) };
            match shim
                .store
                .get(&shim.layout.pg_block(spc, db, rel, fork, *b))
            {
                Ok(Some((data, _))) if data.len() == ZOU_PAGE_SIZE => {
                    out.copy_from_slice(&data);
                    if let Some(cache) = &shim.cache {
                        cache.save((spc, db, rel, fork), *b, &data);
                    }
                    true
                }
                Ok(Some(_)) | Err(_) => false,
                Ok(None) => {
                    out.fill(0);
                    true
                }
            }
        });
        if !ok {
            return ZOU_ERR_STORE;
        }
        // Pages count where they were served, the call lands once
        // under the slowest tier it touched. The parallel gets run on
        // pool threads the op delta cannot see, but a nonempty miss
        // list is a store trip by construction.
        stats::note_read_pages(stats::ReadTier::Cache, cache_pages);
        stats::note_read_pages(stats::ReadTier::Local, chain_pages);
        stats::note_read_pages(stats::ReadTier::Store, misses.len() as u64);
        let tier = if !misses.is_empty() || stats::thread_ops() > ops_before {
            stats::ReadTier::Store
        } else if chain_pages > 0 {
            stats::ReadTier::Local
        } else {
            stats::ReadTier::Cache
        };
        stats::note_read_call(tier, started.elapsed());
        ZOU_OK
    })
}

/// Write one page. The block must already be within the fork size,
/// Postgres extends before it writes.
///
/// # Safety
/// `buf` must be a valid pointer to ZOU_PAGE_SIZE readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_smgr_write(
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    blk: u32,
    buf: *const u8,
) -> i32 {
    with_shim(|shim| {
        if buf.is_null() {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let data = unsafe { std::slice::from_raw_parts(buf, ZOU_PAGE_SIZE) };
        match shim
            .store
            .put(&shim.layout.pg_block(spc, db, rel, fork, blk), data)
        {
            Ok(_) => {
                if let Ok(mut slot) = shim.pending.lock() {
                    slot.refresh((spc, db, rel, fork), blk, data);
                }
                if let Some(cache) = &shim.cache {
                    cache.save((spc, db, rel, fork), blk, data);
                }
                note_write(
                    shim,
                    walscan::BlockRef {
                        spc,
                        db,
                        rel,
                        fork,
                        blk,
                    },
                );
                ZOU_OK
            }
            Err(_) => ZOU_ERR_STORE,
        }
    })
}

/// Buffer one skipFsync page and drain everything when the buffer
/// tips over its cap. Zero when buffering is disabled and the caller
/// must go to the store eagerly.
fn buffer_page(
    shim: &Shim,
    fork: pending::ForkId,
    blk: u32,
    page: &[u8],
    grow: bool,
) -> Option<i32> {
    let full = {
        let Ok(mut slot) = shim.pending.lock() else {
            return Some(ZOU_ERR_STORE);
        };
        if !slot.enabled() {
            return None;
        }
        slot.push(fork, blk, page, grow.then_some(blk + 1))
    };
    // The cache copy lands now rather than at drain time, so a page
    // that outlives its pending stay reads locally either way.
    if let Some(cache) = &shim.cache {
        cache.save(fork, blk, page);
    }
    let (spc, db, rel, fk) = fork;
    note_write(
        shim,
        walscan::BlockRef {
            spc,
            db,
            rel,
            fork: fk,
            blk,
        },
    );
    if full {
        return Some(drain_pending(shim, None));
    }
    Some(ZOU_OK)
}

/// Write a page at `blk` and grow the fork size to cover it. Postgres
/// serializes extension per relation, so the read-modify-write on SIZE
/// has a single writer.
///
/// With `skip_fsync` set the caller owns durability and settles it
/// later through zou_smgr_sync, so the page only lands in the pending
/// buffer. Postgres uses this for bulk fills of relfilenodes no other
/// backend can see yet, which is what makes a process local buffer
/// coherent. Such a page can reach the store after WAL that mentions
/// it, or never, and both are fine: nothing references the relation
/// until a commit record whose own mirror wait fences everything
/// before it.
///
/// # Safety
/// `buf` must be a valid pointer to ZOU_PAGE_SIZE readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_smgr_extend(
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    blk: u32,
    buf: *const u8,
    skip_fsync: i32,
) -> i32 {
    if skip_fsync != 0 {
        let rc = with_shim(|shim| {
            let page = unsafe { std::slice::from_raw_parts(buf, ZOU_PAGE_SIZE) };
            buffer_page(shim, (spc, db, rel, fork), blk, page, true)
                .unwrap_or(ZOU_ERR_NOT_INITIALIZED)
        });
        if rc != ZOU_ERR_NOT_INITIALIZED {
            return rc;
        }
        // Buffering is disabled, fall through to the eager path.
    }
    let rc = unsafe { zou_smgr_write(spc, db, rel, fork, blk, buf) };
    if rc != ZOU_OK {
        return rc;
    }
    with_shim(|shim| match read_size(shim, spc, db, rel, fork) {
        Ok(size) => {
            let current = size.unwrap_or(0);
            if blk + 1 > current && write_size(shim, spc, db, rel, fork, blk + 1).is_err() {
                return ZOU_ERR_STORE;
            }
            ZOU_OK
        }
        Err(()) => ZOU_ERR_STORE,
    })
}

/// Write a run of `nblocks` pages starting at `blk`. skipFsync runs go
/// to the pending buffer like zou_smgr_extend, everything else is one
/// batch of parallel durable puts. The C shim settles the WAL mirror
/// barrier for the whole run before calling, so per page waits are
/// gone by the time the pages get here.
///
/// # Safety
/// `bufs` must point to `nblocks` valid pointers, each to
/// ZOU_PAGE_SIZE readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_smgr_writev(
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    blk: u32,
    bufs: *const *const u8,
    nblocks: u32,
    skip_fsync: i32,
) -> i32 {
    with_shim(|shim| {
        if bufs.is_null() || nblocks == 0 {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let ptrs = unsafe { std::slice::from_raw_parts(bufs, nblocks as usize) };
        if ptrs.iter().any(|p| p.is_null()) {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        if skip_fsync != 0 {
            let mut buffered = true;
            for (i, ptr) in ptrs.iter().enumerate() {
                let page = unsafe { std::slice::from_raw_parts(*ptr, ZOU_PAGE_SIZE) };
                match buffer_page(shim, (spc, db, rel, fork), blk + i as u32, page, false) {
                    Some(ZOU_OK) => {}
                    Some(rc) => return rc,
                    None => {
                        buffered = false;
                        break;
                    }
                }
            }
            if buffered {
                return ZOU_OK;
            }
        }
        struct Job(u32, *const u8);
        unsafe impl Send for Job {}
        unsafe impl Sync for Job {}
        let jobs: Vec<Job> = ptrs
            .iter()
            .enumerate()
            .map(|(i, p)| Job(blk + i as u32, *p))
            .collect();
        let ok = pending::for_each_parallel(&jobs, |Job(b, ptr)| {
            let page = unsafe { std::slice::from_raw_parts(*ptr, ZOU_PAGE_SIZE) };
            shim.store
                .put(&shim.layout.pg_block(spc, db, rel, fork, *b), page)
                .is_ok()
        });
        if !ok {
            return ZOU_ERR_STORE;
        }
        for (i, ptr) in ptrs.iter().enumerate() {
            let page = unsafe { std::slice::from_raw_parts(*ptr, ZOU_PAGE_SIZE) };
            if let Ok(mut slot) = shim.pending.lock() {
                slot.refresh((spc, db, rel, fork), blk + i as u32, page);
            }
            if let Some(cache) = &shim.cache {
                cache.save((spc, db, rel, fork), blk + i as u32, page);
            }
            note_write(
                shim,
                walscan::BlockRef {
                    spc,
                    db,
                    rel,
                    fork,
                    blk: blk + i as u32,
                },
            );
        }
        ZOU_OK
    })
}

/// Settle durability for a fork: drain its buffered pages to the
/// store. The C shim calls this from smgrregistersync and
/// smgrimmedsync, the two points where a skipFsync caller hands
/// durability back.
#[unsafe(no_mangle)]
pub extern "C" fn zou_smgr_sync(spc: u32, db: u32, rel: u32, fork: u32) -> i32 {
    with_shim(|shim| drain_pending(shim, Some((spc, db, rel, fork))))
}

/// Grow the fork by `count` zero pages starting at `blk`. Zero pages are
/// represented as absent block objects, reads fill zeros.
#[unsafe(no_mangle)]
pub extern "C" fn zou_smgr_zeroextend(
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    blk: u32,
    count: u32,
) -> i32 {
    with_shim(|shim| {
        let Some(new_size) = blk.checked_add(count) else {
            return ZOU_ERR_BAD_ARGUMENT;
        };
        match write_size(shim, spc, db, rel, fork, new_size) {
            Ok(()) => {
                // The new blocks are zeros with no pg/ objects and, for
                // unWALed cases, no records either, so run images of a
                // previous incarnation must never answer for them.
                for b in blk..new_size {
                    note_write(
                        shim,
                        walscan::BlockRef {
                            spc,
                            db,
                            rel,
                            fork,
                            blk: b,
                        },
                    );
                }
                ZOU_OK
            }
            Err(()) => ZOU_ERR_STORE,
        }
    })
}

/// Shrink the fork to `nblocks`, deleting block objects past the new end
/// so a later re-extend cannot resurrect stale pages.
#[unsafe(no_mangle)]
pub extern "C" fn zou_smgr_truncate(spc: u32, db: u32, rel: u32, fork: u32, nblocks: u32) -> i32 {
    with_shim(|shim| {
        if let Ok(mut slot) = shim.pending.lock() {
            slot.truncate((spc, db, rel, fork), nblocks);
        } else {
            return ZOU_ERR_STORE;
        }
        if let Some(cache) = &shim.cache {
            cache.truncate((spc, db, rel, fork), nblocks);
        }
        note_truncate(shim, spc, db, rel, fork, nblocks);
        if write_size(shim, spc, db, rel, fork, nblocks).is_err() {
            return ZOU_ERR_STORE;
        }
        let prefix = format!("{}/", shim.layout.pg_fork_prefix(spc, db, rel, fork));
        let Ok(keys) = shim.store.list(&prefix) else {
            return ZOU_ERR_STORE;
        };
        let doomed: Vec<String> = keys
            .into_iter()
            .filter(|key| block_index(key).is_some_and(|idx| idx >= nblocks))
            .collect();
        if pending::for_each_parallel(&doomed, |key| shim.store.delete(key).is_ok()) {
            ZOU_OK
        } else {
            ZOU_ERR_STORE
        }
    })
}

/// Remove a fork entirely: SIZE marker and every block object.
#[unsafe(no_mangle)]
pub extern "C" fn zou_smgr_unlink(spc: u32, db: u32, rel: u32, fork: u32) -> i32 {
    with_shim(|shim| {
        if let Ok(mut slot) = shim.pending.lock() {
            slot.forget((spc, db, rel, fork));
        } else {
            return ZOU_ERR_STORE;
        }
        note_rel(shim, spc, db, rel);
        // SIZE goes first so the fork stops existing even if a block
        // delete fails midway, leaving only unreachable garbage. The
        // cache forgets after the store delete, the other order would
        // let a racing read_size replant a size the store just lost.
        if shim
            .store
            .delete(&shim.layout.pg_size(spc, db, rel, fork))
            .is_err()
        {
            return ZOU_ERR_STORE;
        }
        if let Some(cache) = &shim.cache {
            cache.forget((spc, db, rel, fork));
        }
        let prefix = format!("{}/", shim.layout.pg_fork_prefix(spc, db, rel, fork));
        let Ok(keys) = shim.store.list(&prefix) else {
            return ZOU_ERR_STORE;
        };
        if pending::for_each_parallel(&keys, |key| shim.store.delete(key).is_ok()) {
            ZOU_OK
        } else {
            ZOU_ERR_STORE
        }
    })
}

/// The WAL pipeline. One process owns it, the zou wal pusher background
/// worker, which is the sole holder of the writer lease. Backends never
/// touch this, they wait on the durable LSN the worker publishes into
/// Postgres shared memory.
///
/// In this release the append rpc is a function call: the pusher hosts
/// the shard 0 sequencer of the shared WAL in process and every chunk
/// becomes one frame on the fenced chain. The wire rpc replaces the
/// call later without touching the C surface.
/// The waiter behind the staged append path. [`zou_wal_append`] hands
/// each ticket here and returns, one thread resolves them in submission
/// order and advances the durable watermark [`zou_wal_durable`] reports.
/// FIFO is enough because the sequencer resolves tickets in submission
/// order too, so waiting on the head never holds up a resolved
/// successor.
struct WalWaiter {
    /// Staged tickets with the end Postgres LSN their chunk covers.
    /// `None` once the close drained the queue and dropped the sender.
    tx: Option<mpsc::SyncSender<(AppendTicket, u64)>>,
    /// End Postgres LSN of the last chunk the store acknowledged.
    durable: Arc<AtomicU64>,
    /// First staged append failure as a ZOU_ERR code, sticky, zero
    /// while the pipeline is healthy.
    failed: Arc<AtomicI32>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl WalWaiter {
    /// The queue bound is the pipeline depth: this many chunks staged
    /// but not yet durable. A full queue blocks the next append, the
    /// backpressure that keeps a stalled store from buffering unbounded
    /// WAL in this process.
    const DEPTH: usize = 64;

    fn spawn(resume: u64) -> Self {
        let (tx, rx) = mpsc::sync_channel::<(AppendTicket, u64)>(Self::DEPTH);
        let durable = Arc::new(AtomicU64::new(resume));
        let failed = Arc::new(AtomicI32::new(0));
        let (thread_durable, thread_failed) = (Arc::clone(&durable), Arc::clone(&failed));
        let handle = std::thread::Builder::new()
            .name("zou-wal-waiter".into())
            .spawn(move || {
                while let Ok((ticket, end)) = rx.recv() {
                    match ticket.wait() {
                        Ok(_) => thread_durable.store(end, Ordering::Release),
                        Err(e) => {
                            let rc = match e {
                                AppendError::WrongEpoch { .. } => ZOU_ERR_LEASE_LOST,
                                _ => ZOU_ERR_STORE,
                            };
                            thread_failed.store(rc, Ordering::Release);
                            return;
                        }
                    }
                }
            })
            .expect("spawn zou-wal-waiter");
        WalWaiter {
            tx: Some(tx),
            durable,
            failed,
            handle: Some(handle),
        }
    }

    /// Drop the queue and join the thread, then report the sticky
    /// failure. Only safe once every staged ticket can resolve, which
    /// [`Sequencer::close`] guarantees.
    fn drain(&mut self) -> i32 {
        drop(self.tx.take());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        self.failed.load(Ordering::Acquire)
    }
}

struct WalPipe {
    seq: Option<Sequencer>,
    media: Arc<WalMedia>,
    heartbeat: Option<Heartbeat>,
    held: Arc<Mutex<HeldLease>>,
    store: Arc<dyn CasStore>,
    layout: TenantLayout,
    tenant: u128,
    writer_epoch: u32,
    waiter: WalWaiter,
    /// Whether this session has appended yet, so the first frame can
    /// carry `first_of_epoch` and mark the takeover boundary in the
    /// stream.
    first_appended: bool,
}

static WAL: OnceLock<Mutex<WalPipe>> = OnceLock::new();

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

const WAL_LEASE_TTL_SECS: u64 = 15;

/// The WAL shard the single tenant of a self hosted store pins to. The
/// pusher hosts this one shard's sequencer, a cell with many shards
/// spreads tenants across them later.
pub(crate) const WAL_SHARD: u32 = 0;

/// The lease, heartbeat, and pipeline setup behind [`zou_wal_open`],
/// separated so tests can run several writer sessions in one process.
/// Returns the pipe plus the Postgres LSN to resume pushing from, zero
/// when the store holds no WAL yet.
/// The sequencer defaults, with two knobs for bench work. The window
/// trades commit latency against request count and ZOU_WAL_INFLIGHT
/// caps how many landing PUTs ride the wire at once; both fall back
/// to the library defaults when unset or unparsable.
fn sequencer_config_from_env() -> zou_log::SequencerConfig {
    let mut config = zou_log::SequencerConfig::default();
    if let Some(ms) = std::env::var("ZOU_WAL_WINDOW_MS")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        && ms > 0.0
    {
        config.window = std::time::Duration::from_secs_f64(ms / 1000.0);
    }
    if let Some(n) = std::env::var("ZOU_WAL_INFLIGHT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
    {
        config.inflight = n.clamp(1, zou_log::MAX_INFLIGHT);
    }
    config
}

fn open_wal_pipe(target: &str, _flush_lsn: u64) -> Result<(WalPipe, u64), i32> {
    init_logging();
    let store: Arc<dyn CasStore> = match open_store(target) {
        Ok(store) => Arc::from(store),
        Err(e) => {
            log::error!("zou_wal_open: {e}");
            return Err(ZOU_ERR_BAD_ARGUMENT);
        }
    };
    // The same tenant the smgr shim attaches as: a restored branch
    // server pushes under its own prefix and stream, not the parent's.
    let tenant_ref = std::env::var("ZOU_TENANT").unwrap_or_else(|_| "local".to_string());
    let layout = TenantLayout::new(&tenant_ref);
    let manifest_key = layout.manifest();
    match store.get(&manifest_key) {
        Ok(Some(_)) => {}
        Ok(None) => {
            let genesis = Manifest::new(&tenant_ref, 18);
            // A racing genesis from another process is fine, someone won.
            let _ = store.put_if_match(&manifest_key, &genesis.to_json(), None);
        }
        Err(_) => return Err(ZOU_ERR_STORE),
    }
    let holder = format!("pg-wal-{}", std::process::id());
    let held = match lease::acquire(&*store, &layout, &holder, WAL_LEASE_TTL_SECS, now_unix()) {
        Ok(held) => held,
        Err(lease::LeaseError::Held { .. }) => return Err(ZOU_ERR_LEASE_HELD),
        Err(_) => return Err(ZOU_ERR_STORE),
    };
    // The lease epoch fences frames in the shared log too: the sequencer
    // rejects appends from an epoch below the newest it has admitted.
    let Ok(writer_epoch) = u32::try_from(held.epoch) else {
        log::error!("zou_wal_open: lease epoch {} does not fit u32", held.epoch);
        return Err(ZOU_ERR_STORE);
    };
    let held = Arc::new(Mutex::new(held));
    let heartbeat = Heartbeat::spawn(
        Arc::clone(&store),
        layout.clone(),
        Arc::clone(&held),
        WAL_LEASE_TTL_SECS,
    );
    // The landing chain hedges its creation PUTs: past an adaptive
    // delay a second identical attempt races the first and the fastest
    // success wins, which cuts the store's latency tail out of the
    // commit path. With ordered acks one slow PUT otherwise stalls
    // every window behind it. ZOU_WAL_HEDGE=off keeps the raw store.
    let landing: Arc<dyn CasStore> = match std::env::var("ZOU_WAL_HEDGE").as_deref() {
        Ok("off") => Arc::clone(&store),
        _ => Arc::new(zou_store::HedgedStore::new(Arc::clone(&store))),
    };
    let media = Arc::new(WalMedia::single(landing));
    let takeover = match take_over(&media, WAL_SHARD, &holder) {
        Ok(takeover) => takeover,
        Err(e) => {
            log::error!("zou_wal_open: takeover: {e}");
            return Err(ZOU_ERR_STORE);
        }
    };
    // The resume point comes from the chain after the takeover sealed
    // it, so no rival can still be appending below it. It is byte exact:
    // frames carry the chunk's Postgres LSN range, and the consolidator
    // treats a gap or an overlap not starting at the watermark as
    // corruption, so the pusher must continue from exactly here.
    let tenant = tenant_id(layout.tenant_ref());
    let resume = match stream_end(&media, WAL_SHARD, tenant) {
        Ok(end) => end.map_or(0, |lsn| lsn.0),
        Err(e) => {
            log::error!("zou_wal_open: stream end: {e}");
            return Err(ZOU_ERR_STORE);
        }
    };
    let sink = Arc::new(MediaSink::new(Arc::clone(&media), WAL_SHARD));
    let seq = Sequencer::resume(
        WAL_SHARD,
        sink,
        sequencer_config_from_env(),
        takeover.next_seq,
        takeover.prev_digest,
    );
    Ok((
        WalPipe {
            seq: Some(seq),
            media,
            heartbeat: Some(heartbeat),
            held,
            store,
            layout,
            tenant,
            writer_epoch,
            // The watermark starts at the resume point: everything the
            // chain already holds is durable by definition.
            waiter: WalWaiter::spawn(resume),
            first_appended: false,
        },
        resume,
    ))
}

fn close_wal_pipe(pipe: &mut WalPipe) -> i32 {
    let mut rc = ZOU_OK;
    if let Some(seq) = pipe.seq.take() {
        if seq.close().is_err() {
            rc = ZOU_ERR_STORE;
        }
        // The close resolved every staged ticket, so the waiter can
        // finish its queue and exit. Its sticky failure is part of the
        // close verdict: a chunk that died between staging and here
        // must not read as a clean shutdown.
        let failed = pipe.waiter.drain();
        if rc == ZOU_OK && failed != 0 {
            rc = failed;
        }
        if rc == ZOU_OK {
            // Best effort: fold the landing chain into a sealed round so
            // the next start probes a short tail. Failure costs nothing,
            // the next fold thread runs the same pass.
            if let Err(e) = consolidate(&pipe.media, WAL_SHARD) {
                log::info!("zou_wal_close: consolidate: {e}");
            }
            if let Err(e) = gc_landing(&pipe.media, WAL_SHARD, Duration::from_secs(600)) {
                log::info!("zou_wal_close: gc landing: {e}");
            }
        }
    }
    if let Some(heartbeat) = pipe.heartbeat.take()
        && heartbeat.detach().is_err()
    {
        rc = ZOU_ERR_LEASE_LOST;
    }
    rc
}

/// Open the WAL pipeline: genesis manifest if the store is empty, writer
/// lease, heartbeat renewal, shared log takeover, and the shard sequencer
/// resumed onto the fenced chain.
///
/// Each appended chunk becomes one frame whose lsn range is the chunk's
/// Postgres LSN range, which makes the stream self describing for the
/// recovery path. When the store already holds WAL, `out_resume` receives
/// the Postgres LSN right after its last frame and the caller must push
/// from there, re-reading its local pg_wal, so bytes flushed after the
/// previous pusher died are not skipped. It stays zero when the store is
/// empty and pushing starts at `flush_lsn`.
///
/// Returns `ZOU_ERR_LEASE_HELD` while a previous holder's lease has not
/// expired, the caller retries until the TTL passes.
///
/// # Safety
/// `target` must be a valid NUL terminated C string and `out_resume` a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_wal_open(
    target: *const c_char,
    flush_lsn: u64,
    out_resume: *mut u64,
) -> i32 {
    wrap(|| {
        if target.is_null() || out_resume.is_null() {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let Ok(target) = unsafe { CStr::from_ptr(target) }.to_str() else {
            return ZOU_ERR_BAD_ARGUMENT;
        };
        if WAL.get().is_some() {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        match open_wal_pipe(target, flush_lsn) {
            Ok((pipe, resume)) => {
                unsafe { *out_resume = resume };
                let _ = WAL.set(Mutex::new(pipe));
                ZOU_OK
            }
            Err(rc) => rc,
        }
    })
}

/// Stage one chunk of WAL starting at Postgres LSN `pg_lsn` into the
/// group commit pipeline and return without waiting on the store.
/// Durability is a separate question answered by [`zou_wal_durable`]:
/// its watermark reaches `pg_lsn + len` once this chunk's batch lands.
/// `out_durable` receives the watermark as of this call, diagnostics
/// only. A full pipeline, sixty four staged chunks, blocks here until
/// the store drains, and a failure from any earlier staged chunk
/// surfaces on this and every later call.
///
/// # Safety
/// `data` must point to `len` readable bytes and `out_durable` must be a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_wal_append(
    data: *const u8,
    len: usize,
    pg_lsn: u64,
    out_durable: *mut u64,
) -> i32 {
    wrap(|| {
        if data.is_null() || out_durable.is_null() || len == 0 {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let Some(pipe) = WAL.get() else {
            return ZOU_ERR_NOT_INITIALIZED;
        };
        let mut pipe = pipe.lock().expect("wal pipe mutex poisoned");
        if pipe.heartbeat.as_ref().is_some_and(Heartbeat::lost) {
            return ZOU_ERR_LEASE_LOST;
        }
        let rc = pipe.waiter.failed.load(Ordering::Acquire);
        if rc != 0 {
            return rc;
        }
        let Some(seq) = pipe.seq.as_ref() else {
            return ZOU_ERR_NOT_INITIALIZED;
        };
        let chunk = unsafe { std::slice::from_raw_parts(data, len) };
        let frame = Frame2 {
            tenant: pipe.tenant,
            writer_epoch: pipe.writer_epoch,
            start_lsn: Lsn(pg_lsn),
            end_lsn: Lsn(pg_lsn + len as u64),
            contains_commit: true,
            first_of_epoch: !pipe.first_appended,
            hints: Vec::new(),
            payload: chunk.to_vec(),
        };
        let ticket = match seq.append(vec![frame]) {
            Ok(ticket) => ticket,
            Err(AppendError::WrongEpoch { .. }) => return ZOU_ERR_LEASE_LOST,
            Err(_) => return ZOU_ERR_STORE,
        };
        pipe.first_appended = true;
        let tx = pipe.waiter.tx.as_ref().expect("append after close");
        if tx.send((ticket, pg_lsn + len as u64)).is_err() {
            // The waiter exited on a failed ticket, report why.
            let rc = pipe.waiter.failed.load(Ordering::Acquire);
            return if rc != 0 { rc } else { ZOU_ERR_STORE };
        }
        unsafe { *out_durable = pipe.waiter.durable.load(Ordering::Acquire) };
        ZOU_OK
    })
}

/// The durable watermark of the append pipeline: the end Postgres LSN of
/// the last chunk the store acknowledged, in submission order, so every
/// byte below it is durable. Starts at the resume point [`zou_wal_open`]
/// reported. A failed append turns this and every later poll into its
/// error code, the pusher treats that as fatal.
///
/// # Safety
/// `out_pg_lsn` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_wal_durable(out_pg_lsn: *mut u64) -> i32 {
    wrap(|| {
        if out_pg_lsn.is_null() {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let Some(pipe) = WAL.get() else {
            return ZOU_ERR_NOT_INITIALIZED;
        };
        let pipe = pipe.lock().expect("wal pipe mutex poisoned");
        let rc = pipe.waiter.failed.load(Ordering::Acquire);
        if rc != 0 {
            return rc;
        }
        unsafe { *out_pg_lsn = pipe.waiter.durable.load(Ordering::Acquire) };
        ZOU_OK
    })
}

/// How a fold thread ended: published, fenced out, or failed. The
/// thread carries the whole lifecycle, capture, publish, and the
/// shared log consolidation, so the pusher loop never runs a store
/// call on a fold's behalf.
enum FoldEnd {
    Done {
        kind: zou_store::manifest::CheckpointKind,
        dropped: u32,
    },
    LeaseLost,
    Failed(String),
}

/// The fold in flight, at most one per process. The thread runs
/// [`fold::prepare`] and then [`fold::publish`] on its own, never on
/// the pusher loop, so appending and folding overlap end to end and a
/// shutdown can abandon it without a join: the capture is idempotent
/// per redo and the publish is one CAS swap that either landed whole
/// or left no manifest edit, so a retry at the same redo is safe and
/// abandoned objects are gc food.
struct FoldTask {
    redo: u64,
    handle: std::thread::JoinHandle<FoldEnd>,
}

static FOLD_TASK: Mutex<Option<FoldTask>> = Mutex::new(None);

/// Start the whole fold lifecycle for the completed checkpoint at
/// `redo` on a background thread: capture with [`fold::prepare`],
/// publish with [`fold::publish`], then consolidate the shared log.
/// Called by the pusher from the data directory once its pushed
/// position covers `redo`, so relative paths resolve inside PGDATA and
/// the checkpoint record the capture names is already durable in the
/// stream. Returns `ZOU_FOLD_RUNNING` without starting anything while
/// an earlier fold is still in flight, its result must be polled
/// first.
#[unsafe(no_mangle)]
pub extern "C" fn zou_wal_fold_start(redo: u64) -> i32 {
    wrap(|| {
        let Some(pipe) = WAL.get() else {
            return ZOU_ERR_NOT_INITIALIZED;
        };
        let (store, layout, media, tenant, held) = {
            let pipe = pipe.lock().expect("wal pipe mutex poisoned");
            if pipe.heartbeat.as_ref().is_some_and(Heartbeat::lost) {
                return ZOU_ERR_LEASE_LOST;
            }
            if pipe.seq.is_none() {
                return ZOU_ERR_NOT_INITIALIZED;
            }
            (
                Arc::clone(&pipe.store),
                pipe.layout.clone(),
                Arc::clone(&pipe.media),
                pipe.tenant,
                Arc::clone(&pipe.held),
            )
        };
        let mut slot = FOLD_TASK.lock().expect("fold slot mutex poisoned");
        if slot.is_some() {
            return ZOU_FOLD_RUNNING;
        }
        let handle = std::thread::Builder::new()
            .name("zou-fold".into())
            .spawn(move || {
                let outcome =
                    match fold::prepare(&*store, &layout, &media, tenant, Path::new("."), redo) {
                        Ok(outcome) => outcome,
                        Err(e) => return FoldEnd::Failed(format!("capture: {e}")),
                    };
                {
                    let mut held = held.lock().expect("lease mutex poisoned");
                    match fold::publish(
                        &*store,
                        &layout,
                        &mut held,
                        &outcome.checkpoint,
                        redo,
                        now_unix(),
                    ) {
                        Ok(()) => {}
                        Err(lease::LeaseError::Lost { .. }) => return FoldEnd::LeaseLost,
                        Err(e) => return FoldEnd::Failed(format!("publish: {e}")),
                    }
                }
                // A fold marks the natural moment to fold the shared
                // log too: landing segments consolidate into a sealed
                // round and old landing objects past the safety window
                // drop.
                let dropped = match consolidate(&media, WAL_SHARD) {
                    Ok(_) => match gc_landing(&media, WAL_SHARD, Duration::from_secs(600)) {
                        Ok(dropped) => dropped,
                        Err(e) => {
                            log::info!("zou fold: gc landing: {e}");
                            0
                        }
                    },
                    Err(e) => {
                        log::info!("zou fold: consolidate: {e}");
                        0
                    }
                };
                FoldEnd::Done {
                    kind: outcome.stats.kind,
                    dropped: dropped as u32,
                }
            });
        match handle {
            Ok(handle) => {
                *slot = Some(FoldTask { redo, handle });
                ZOU_OK
            }
            Err(e) => {
                log::error!("zou_wal_fold_start: spawn: {e}");
                ZOU_ERR_STORE
            }
        }
    })
}

/// Collect the fold the pusher started. Returns `ZOU_FOLD_IDLE` when
/// nothing is in flight, `ZOU_FOLD_RUNNING` while the thread works, and
/// once the thread has published its checkpoint and consolidated the
/// shared log on its own, writes the fold's redo and the count of
/// dropped landing objects through the out pointers and answers
/// `ZOU_FOLD_DONE_DELTA` or `ZOU_FOLD_DONE_FULL`. Negative means the
/// fold failed or the publish did; either way the slot is clear, the
/// error is in the log, and a retry at the same redo is safe because
/// the fold is idempotent. The poll itself never touches the store, so
/// calling it between appends costs a mutex peek and nothing more.
///
/// # Safety
/// `out_redo` and `out_dropped` must be valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_wal_fold_poll(out_redo: *mut u64, out_dropped: *mut u32) -> i32 {
    wrap(|| {
        if out_redo.is_null() || out_dropped.is_null() {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let task = {
            let mut slot = FOLD_TASK.lock().expect("fold slot mutex poisoned");
            match slot.as_ref() {
                None => return ZOU_FOLD_IDLE,
                Some(task) if !task.handle.is_finished() => return ZOU_FOLD_RUNNING,
                Some(_) => slot.take().expect("checked some"),
            }
        };
        unsafe { *out_redo = task.redo };
        let end = match task.handle.join() {
            Ok(end) => end,
            Err(_) => {
                log::error!("zou_wal_fold_poll: fold thread panicked");
                return ZOU_ERR_STORE;
            }
        };
        let (kind, dropped) = match end {
            FoldEnd::Done { kind, dropped } => (kind, dropped),
            FoldEnd::LeaseLost => return ZOU_ERR_LEASE_LOST,
            FoldEnd::Failed(e) => {
                log::error!("zou_wal_fold_poll: fold at {:#X}: {e}", task.redo);
                return ZOU_ERR_STORE;
            }
        };
        unsafe { *out_dropped = dropped };
        match kind {
            zou_store::manifest::CheckpointKind::Full => ZOU_FOLD_DONE_FULL,
            zou_store::manifest::CheckpointKind::Delta => ZOU_FOLD_DONE_DELTA,
        }
    })
}

/// Flush the open batch, stop the sequencer, and release the lease.
/// Called when the pusher worker exits so a clean shutdown leaves a
/// quiet chain and the next start acquires the lease immediately.
#[unsafe(no_mangle)]
pub extern "C" fn zou_wal_close() -> i32 {
    wrap(|| {
        // A fold still in flight is abandoned, not joined: it holds its
        // own store handle and lease clone, its publish is one CAS swap
        // that either landed whole or left no manifest edit, and waiting
        // out a full capture here would hold up shutdown for minutes.
        if FOLD_TASK.lock().map(|s| s.is_some()).unwrap_or(false) {
            log::info!("zou_wal_close: a fold is still running, abandoning it mid flight");
        }
        let Some(pipe) = WAL.get() else {
            return ZOU_ERR_NOT_INITIALIZED;
        };
        let mut pipe = pipe.lock().expect("wal pipe mutex poisoned");
        close_wal_pipe(&mut pipe)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use zou_store::LocalFsStore;

    /// One test drives the whole C ABI lifecycle, because the shim is a
    /// process global and tests share the process.
    #[test]
    fn the_c_abi_lifecycle_works_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        unsafe { std::env::set_var("ZOU_PAGE_CACHE", cache_dir.path()) };
        let target = CString::new(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(unsafe { zou_pg_init(target.as_ptr()) }, ZOU_OK);
        unsafe { std::env::remove_var("ZOU_PAGE_CACHE") };
        // Idempotent: a second init from the same process is fine.
        assert_eq!(unsafe { zou_pg_init(target.as_ptr()) }, ZOU_OK);

        let (spc, db, rel, fork) = (1663, 5, 16384, 0);
        let mut flag = -1i32;
        assert_eq!(
            unsafe { zou_smgr_exists(spc, db, rel, fork, &mut flag) },
            ZOU_OK
        );
        assert_eq!(flag, 0);

        assert_eq!(zou_smgr_create(spc, db, rel, fork), ZOU_OK);
        assert_eq!(
            unsafe { zou_smgr_exists(spc, db, rel, fork, &mut flag) },
            ZOU_OK
        );
        assert_eq!(flag, 1);

        // Extend three pages with distinct contents.
        for blk in 0u32..3 {
            let page = [blk as u8 + 1; ZOU_PAGE_SIZE];
            assert_eq!(
                unsafe { zou_smgr_extend(spc, db, rel, fork, blk, page.as_ptr(), 0) },
                ZOU_OK
            );
        }
        let mut n = 0u32;
        assert_eq!(
            unsafe { zou_smgr_nblocks(spc, db, rel, fork, &mut n) },
            ZOU_OK
        );
        assert_eq!(n, 3);

        // The size serves locally now: with the store copy deleted out
        // from under it, nblocks still answers from the cache, which is
        // the planner's round trip gone. The next size write puts the
        // store copy back.
        let shim = SHIM.get().unwrap();
        shim.store
            .delete(&shim.layout.pg_size(spc, db, rel, fork))
            .unwrap();
        assert_eq!(
            unsafe { zou_smgr_nblocks(spc, db, rel, fork, &mut n) },
            ZOU_OK
        );
        assert_eq!(n, 3);

        // Overwrite block 1 through the write path and read everything back.
        let page = [0xAB; ZOU_PAGE_SIZE];
        assert_eq!(
            unsafe { zou_smgr_write(spc, db, rel, fork, 1, page.as_ptr()) },
            ZOU_OK
        );
        let mut buf = [0u8; ZOU_PAGE_SIZE];
        for (blk, expect) in [(0u32, 1u8), (1, 0xAB), (2, 3)] {
            assert_eq!(
                unsafe { zou_smgr_read(spc, db, rel, fork, blk, buf.as_mut_ptr(), 0) },
                ZOU_OK
            );
            assert!(buf.iter().all(|b| *b == expect), "block {blk}");
        }

        // Zero extension: size grows, the new blocks read as zeros.
        assert_eq!(zou_smgr_zeroextend(spc, db, rel, fork, 3, 2), ZOU_OK);
        assert_eq!(
            unsafe { zou_smgr_nblocks(spc, db, rel, fork, &mut n) },
            ZOU_OK
        );
        assert_eq!(n, 5);
        assert_eq!(
            unsafe { zou_smgr_read(spc, db, rel, fork, 4, buf.as_mut_ptr(), 0) },
            ZOU_OK
        );
        assert!(buf.iter().all(|b| *b == 0));

        // Truncate drops the tail blocks for real.
        assert_eq!(zou_smgr_truncate(spc, db, rel, fork, 1), ZOU_OK);
        assert_eq!(
            unsafe { zou_smgr_nblocks(spc, db, rel, fork, &mut n) },
            ZOU_OK
        );
        assert_eq!(n, 1);
        assert_eq!(
            unsafe { zou_smgr_read(spc, db, rel, fork, 0, buf.as_mut_ptr(), 0) },
            ZOU_OK
        );
        assert!(buf.iter().all(|b| *b == 1));

        // Unlink removes the fork and its objects.
        assert_eq!(zou_smgr_unlink(spc, db, rel, fork), ZOU_OK);
        assert_eq!(
            unsafe { zou_smgr_exists(spc, db, rel, fork, &mut flag) },
            ZOU_OK
        );
        assert_eq!(flag, 0);

        // The skipFsync path on a second fork: extends buffer in
        // process, reads and nblocks see them before any block object
        // exists, and zou_smgr_sync drains everything.
        let (rel2, shim) = (16400, SHIM.get().unwrap());
        assert_eq!(zou_smgr_create(spc, db, rel2, fork), ZOU_OK);
        for blk in 0u32..4 {
            let page = [0x40 + blk as u8; ZOU_PAGE_SIZE];
            assert_eq!(
                unsafe { zou_smgr_extend(spc, db, rel2, fork, blk, page.as_ptr(), 1) },
                ZOU_OK
            );
        }
        assert_eq!(
            unsafe { zou_smgr_nblocks(spc, db, rel2, fork, &mut n) },
            ZOU_OK
        );
        assert_eq!(n, 4);
        assert_eq!(
            unsafe { zou_smgr_read(spc, db, rel2, fork, 2, buf.as_mut_ptr(), 0) },
            ZOU_OK
        );
        assert!(buf.iter().all(|b| *b == 0x42));
        let prefix = format!("{}/", shim.layout.pg_fork_prefix(spc, db, rel2, fork));
        let before = shim.store.list(&prefix).unwrap();
        assert!(
            before.iter().all(|k| k.ends_with("/SIZE")),
            "no block objects before the drain: {before:?}"
        );

        // An eager write to a buffered block must not be shadowed by
        // the older buffered copy afterwards.
        let page = [0xEE; ZOU_PAGE_SIZE];
        assert_eq!(
            unsafe { zou_smgr_write(spc, db, rel2, fork, 1, page.as_ptr()) },
            ZOU_OK
        );
        assert_eq!(
            unsafe { zou_smgr_read(spc, db, rel2, fork, 1, buf.as_mut_ptr(), 0) },
            ZOU_OK
        );
        assert!(buf.iter().all(|b| *b == 0xEE));

        assert_eq!(zou_smgr_sync(spc, db, rel2, fork), ZOU_OK);
        let after = shim.store.list(&prefix).unwrap();
        assert_eq!(after.len(), 5, "four blocks plus SIZE: {after:?}");
        assert_eq!(
            unsafe { zou_smgr_nblocks(spc, db, rel2, fork, &mut n) },
            ZOU_OK
        );
        assert_eq!(n, 4);
        for (blk, expect) in [(0u32, 0x40u8), (1, 0xEE), (2, 0x42), (3, 0x43)] {
            assert_eq!(
                unsafe { zou_smgr_read(spc, db, rel2, fork, blk, buf.as_mut_ptr(), 0) },
                ZOU_OK
            );
            assert!(buf.iter().all(|b| *b == expect), "block {blk}");
        }

        // The vectored entry points agree with the scalar ones.
        let mut out = [[0u8; ZOU_PAGE_SIZE]; 4];
        let mut outp: Vec<*mut u8> = out.iter_mut().map(|p| p.as_mut_ptr()).collect();
        assert_eq!(
            unsafe { zou_smgr_readv(spc, db, rel2, fork, 0, outp.as_mut_ptr(), 4, 0) },
            ZOU_OK
        );
        for (blk, expect) in [(0usize, 0x40u8), (1, 0xEE), (2, 0x42), (3, 0x43)] {
            assert!(out[blk].iter().all(|b| *b == expect), "readv block {blk}");
        }
        let w0 = [0x50; ZOU_PAGE_SIZE];
        let w1 = [0x51; ZOU_PAGE_SIZE];
        let inp: Vec<*const u8> = vec![w0.as_ptr(), w1.as_ptr()];
        assert_eq!(
            unsafe { zou_smgr_writev(spc, db, rel2, fork, 2, inp.as_ptr(), 2, 0) },
            ZOU_OK
        );
        for (blk, expect) in [(2u32, 0x50u8), (3, 0x51)] {
            assert_eq!(
                unsafe { zou_smgr_read(spc, db, rel2, fork, blk, buf.as_mut_ptr(), 0) },
                ZOU_OK
            );
            assert!(buf.iter().all(|b| *b == expect), "block {blk}");
        }

        // The write-through cache answers reads on its own: delete a
        // block object behind the shim's back and the page still comes
        // back, no store get involved.
        shim.store
            .delete(&shim.layout.pg_block(spc, db, rel2, fork, 2))
            .unwrap();
        assert_eq!(
            unsafe { zou_smgr_read(spc, db, rel2, fork, 2, buf.as_mut_ptr(), 0) },
            ZOU_OK
        );
        assert!(
            buf.iter().all(|b| *b == 0x50),
            "cache serves the deleted block"
        );

        assert_eq!(zou_smgr_unlink(spc, db, rel2, fork), ZOU_OK);
        // Unlink dropped the cached fork too, the read now zero fills
        // instead of resurrecting cached pages.
        assert_eq!(
            unsafe { zou_smgr_read(spc, db, rel2, fork, 2, buf.as_mut_ptr(), 0) },
            ZOU_OK
        );
        assert!(buf.iter().all(|b| *b == 0), "no cache after unlink");
    }

    /// One test for the WAL side too, the pipe is its own process global.
    #[test]
    fn the_wal_pipeline_gates_on_durability() {
        let dir = tempfile::tempdir().unwrap();
        let target = CString::new(dir.path().to_str().unwrap()).unwrap();
        let start = 0x0100_0000_u64;
        let mut resume = u64::MAX;
        assert_eq!(
            unsafe { zou_wal_open(target.as_ptr(), start, &mut resume) },
            ZOU_OK
        );
        assert_eq!(resume, 0, "an empty store has nothing to resume from");
        // A second open in the same process is a bug in the caller.
        assert_eq!(
            unsafe { zou_wal_open(target.as_ptr(), start, &mut resume) },
            ZOU_ERR_BAD_ARGUMENT
        );

        // Appends stage and return, durability comes back through the
        // poll: the watermark climbs to the end lsn of the last staged
        // chunk once its batch lands, and never past it.
        let chunk = vec![7u8; 4096];
        let mut durable = 0u64;
        for i in 0..8 {
            let pg_lsn = start + i * 4096;
            assert_eq!(
                unsafe { zou_wal_append(chunk.as_ptr(), chunk.len(), pg_lsn, &mut durable) },
                ZOU_OK
            );
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            assert_eq!(unsafe { zou_wal_durable(&mut durable) }, ZOU_OK);
            assert!(durable <= start + 8 * 4096, "watermark never overshoots");
            if durable == start + 8 * 4096 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "durability stuck at {durable:#x}"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        // The fold pair: nothing in flight polls idle, a started fold
        // polls running then surfaces its result. The capture fails
        // here, the test's cwd is no PGDATA, but the lifecycle is the
        // point: the error comes back through poll, the slot clears,
        // and the pusher's retry story holds.
        let (mut fredo, mut fdropped) = (0u64, 0u32);
        assert_eq!(
            unsafe { zou_wal_fold_poll(&mut fredo, &mut fdropped) },
            ZOU_FOLD_IDLE
        );
        assert_eq!(zou_wal_fold_start(0x0100_4000), ZOU_OK);
        let rc = loop {
            match unsafe { zou_wal_fold_poll(&mut fredo, &mut fdropped) } {
                ZOU_FOLD_RUNNING => std::thread::sleep(std::time::Duration::from_millis(10)),
                rc => break rc,
            }
        };
        assert_eq!(rc, ZOU_ERR_STORE, "no pg_control in cwd fails the fold");
        assert_eq!(fredo, 0x0100_4000, "the failed fold names its redo");
        assert_eq!(
            unsafe { zou_wal_fold_poll(&mut fredo, &mut fdropped) },
            ZOU_FOLD_IDLE,
            "a collected fold clears the slot"
        );

        assert_eq!(zou_wal_close(), ZOU_OK);

        // A later session, as after a server restart, resumes exactly
        // where the stream ended, not at the caller's flush pointer.
        let target_str = dir.path().to_str().unwrap();
        let (mut pipe, resumed) = open_wal_pipe(target_str, start + 999).unwrap();
        assert_eq!(resumed, start + 8 * 4096);
        {
            let seq = pipe.seq.as_ref().unwrap();
            let frame = Frame2 {
                tenant: pipe.tenant,
                writer_epoch: pipe.writer_epoch,
                start_lsn: Lsn(resumed),
                end_lsn: Lsn(resumed + 128),
                contains_commit: true,
                first_of_epoch: true,
                hints: Vec::new(),
                payload: vec![9u8; 128],
            };
            seq.append(vec![frame]).unwrap().wait().unwrap();
        }
        assert_eq!(close_wal_pipe(&mut pipe), ZOU_OK);

        // Both sessions chain in the shared log: a catch up read hands
        // back every frame in lsn order, byte exact, whether it sits in
        // a sealed round or on the landing tail, and each session's
        // first frame carries the takeover marker.
        let store = LocalFsStore::new(dir.path());
        let layout = TenantLayout::new("local");
        let (data, _) = store.get(&layout.manifest()).unwrap().unwrap();
        let manifest = Manifest::from_json(&data).unwrap();
        assert!(manifest.lease.is_none(), "lease released on close");

        let media = WalMedia::single(Arc::new(LocalFsStore::new(dir.path())) as Arc<dyn CasStore>);
        let frames = zou_log::catch_up(
            &media,
            WAL_SHARD,
            &zou_log::TeeFilter::Tenant(tenant_id("local")),
            Lsn(0),
        )
        .unwrap();
        assert_eq!(frames.len(), 9);
        let epochs: std::collections::BTreeSet<u32> =
            frames.iter().map(|f| f.writer_epoch).collect();
        assert_eq!(epochs.len(), 2, "two writer sessions in one stream");
        for (i, frame) in frames.iter().take(8).enumerate() {
            assert_eq!(frame.start_lsn.0, start + i as u64 * 4096);
            assert_eq!(frame.payload.len(), 4096);
            assert!(frame.payload.iter().all(|b| *b == 7));
            assert_eq!(frame.first_of_epoch, i == 0, "frame {i}");
        }
        let last = frames.last().unwrap();
        assert_eq!(last.start_lsn.0, resumed);
        assert_eq!(last.payload.len(), 128);
        assert!(last.first_of_epoch, "a new session marks its takeover");
    }
}
