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
//! Every function returns 0 for success or a negative ZOU_ERR code, and
//! never unwinds into C. Postgres turns nonzero into ereport(ERROR).

pub mod cache;
pub mod capture;
pub mod fold;
pub mod reader;
pub mod restore;
pub mod walscan;

use std::ffi::{CStr, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::SystemTime;

use zou_store::heartbeat::Heartbeat;
use zou_store::layout::TenantLayout;
use zou_store::lease;
use zou_store::{
    CasStore, GroupCommit, GroupCommitConfig, LocalFsStore, Lsn, Manifest, TailConfig,
};

/// Postgres BLCKSZ. The patch checks this against its own BLCKSZ at init.
pub const ZOU_PAGE_SIZE: usize = 8192;

pub const ZOU_OK: i32 = 0;
pub const ZOU_ERR_STORE: i32 = -1;
pub const ZOU_ERR_NOT_INITIALIZED: i32 = -2;
pub const ZOU_ERR_PANIC: i32 = -3;
pub const ZOU_ERR_BAD_ARGUMENT: i32 = -4;
pub const ZOU_ERR_LEASE_HELD: i32 = -5;
pub const ZOU_ERR_LEASE_LOST: i32 = -6;

/// The chain reader behind the smgr read path. Unset until the first
/// read, which attaches lazily so init stays cheap and a store with no
/// checkpoints yet costs nothing. Off means every read goes to pg/,
/// either by choice, by attach failure, or because there is no chain
/// to serve. Backends are single threaded, the mutex sees no
/// contention.
enum ReaderSlot {
    Unset,
    Off,
    On(Box<reader::ChainReader>),
}

struct Shim {
    store: LocalFsStore,
    layout: TenantLayout,
    reader: Mutex<ReaderSlot>,
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

fn chain_read(shim: &Shim, r: walscan::BlockRef, durable: u64) -> Option<Vec<u8>> {
    unsafe extern "C" {
        fn atexit(cb: extern "C" fn()) -> i32;
    }
    let mut slot = shim.reader.lock().ok()?;
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
                Err(e) => {
                    eprintln!("zou chain reader attach failed, reads stay on pg/: {e}");
                    ReaderSlot::Off
                }
            }
        };
    }
    match &mut *slot {
        ReaderSlot::On(rd) => rd.read(&shim.store, &shim.layout, r, durable),
        _ => None,
    }
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

fn wrap(f: impl FnOnce() -> i32) -> i32 {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or(ZOU_ERR_PANIC)
}

fn with_shim(f: impl FnOnce(&'static Shim) -> i32) -> i32 {
    wrap(|| match SHIM.get() {
        Some(shim) => f(shim),
        None => ZOU_ERR_NOT_INITIALIZED,
    })
}

fn read_size(shim: &Shim, spc: u32, db: u32, rel: u32, fork: u32) -> Result<Option<u32>, ()> {
    let key = shim.layout.pg_size(spc, db, rel, fork);
    match shim.store.get(&key) {
        Ok(Some((data, _))) => {
            let bytes: [u8; 4] = data.as_slice().try_into().map_err(|_| ())?;
            Ok(Some(u32::from_le_bytes(bytes)))
        }
        Ok(None) => Ok(None),
        Err(_) => Err(()),
    }
}

fn write_size(shim: &Shim, spc: u32, db: u32, rel: u32, fork: u32, nblocks: u32) -> Result<(), ()> {
    let key = shim.layout.pg_size(spc, db, rel, fork);
    shim.store
        .put(&key, &nblocks.to_le_bytes())
        .map(|_| ())
        .map_err(|_| ())
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
/// is a local directory in v0, object store URLs arrive with the CLI.
///
/// # Safety
/// `target` must be a valid NUL terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_pg_init(target: *const c_char) -> i32 {
    wrap(|| {
        if target.is_null() {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let Ok(target) = unsafe { CStr::from_ptr(target) }.to_str() else {
            return ZOU_ERR_BAD_ARGUMENT;
        };
        if target.contains("://") {
            // Object store URLs need config (region, creds) that the CLI
            // will own. Refuse loudly instead of guessing.
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let _ = SHIM.set(Shim {
            store: LocalFsStore::new(target),
            layout: TenantLayout::new("local"),
            reader: Mutex::new(ReaderSlot::Unset),
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
        match read_size(shim, spc, db, rel, fork) {
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
        match read_size(shim, spc, db, rel, fork) {
            Ok(Some(n)) => {
                unsafe { *out = n };
                ZOU_OK
            }
            Ok(None) => ZOU_ERR_STORE,
            Err(()) => ZOU_ERR_STORE,
        }
    })
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
        let r = walscan::BlockRef {
            spc,
            db,
            rel,
            fork,
            blk,
        };
        if let Some(page) = chain_read(shim, r, durable_lsn) {
            out.copy_from_slice(&page);
            return ZOU_OK;
        }
        match shim
            .store
            .get(&shim.layout.pg_block(spc, db, rel, fork, blk))
        {
            Ok(Some((data, _))) if data.len() == ZOU_PAGE_SIZE => {
                out.copy_from_slice(&data);
                ZOU_OK
            }
            Ok(Some(_)) => ZOU_ERR_STORE,
            Ok(None) => {
                out.fill(0);
                ZOU_OK
            }
            Err(_) => ZOU_ERR_STORE,
        }
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

/// Write a page at `blk` and grow the fork size to cover it. Postgres
/// serializes extension per relation, so the read-modify-write on SIZE
/// has a single writer.
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
) -> i32 {
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
        note_rel(shim, spc, db, rel);
        if write_size(shim, spc, db, rel, fork, nblocks).is_err() {
            return ZOU_ERR_STORE;
        }
        let prefix = format!("{}/", shim.layout.pg_fork_prefix(spc, db, rel, fork));
        let Ok(keys) = shim.store.list(&prefix) else {
            return ZOU_ERR_STORE;
        };
        for key in keys {
            if let Some(idx) = block_index(&key)
                && idx >= nblocks
                && shim.store.delete(&key).is_err()
            {
                return ZOU_ERR_STORE;
            }
        }
        ZOU_OK
    })
}

/// Remove a fork entirely: SIZE marker and every block object.
#[unsafe(no_mangle)]
pub extern "C" fn zou_smgr_unlink(spc: u32, db: u32, rel: u32, fork: u32) -> i32 {
    with_shim(|shim| {
        note_rel(shim, spc, db, rel);
        // SIZE goes first so the fork stops existing even if a block
        // delete fails midway, leaving only unreachable garbage.
        if shim
            .store
            .delete(&shim.layout.pg_size(spc, db, rel, fork))
            .is_err()
        {
            return ZOU_ERR_STORE;
        }
        let prefix = format!("{}/", shim.layout.pg_fork_prefix(spc, db, rel, fork));
        let Ok(keys) = shim.store.list(&prefix) else {
            return ZOU_ERR_STORE;
        };
        for key in keys {
            if shim.store.delete(&key).is_err() {
                return ZOU_ERR_STORE;
            }
        }
        ZOU_OK
    })
}

/// The WAL pipeline. One process owns it, the zou wal pusher background
/// worker, which is the sole holder of the writer lease. Backends never
/// touch this, they wait on the durable LSN the worker publishes into
/// Postgres shared memory.
struct WalPipe {
    commit: Option<GroupCommit>,
    heartbeat: Option<Heartbeat>,
    store: Arc<dyn CasStore>,
    layout: TenantLayout,
}

static WAL: OnceLock<Mutex<WalPipe>> = OnceLock::new();

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

const WAL_LEASE_TTL_SECS: u64 = 15;

/// Where a resuming pusher picks up: the Postgres LSN right after the last
/// record in the store, and the zou stream position the next frame starts
/// at. Computed by reading the newest segment back.
struct ResumePoint {
    pg_lsn: u64,
    stream_lsn: u64,
}

/// Read the last segment of the reconciled tail and derive the resume
/// point from its last record's Postgres LSN header. None means the store
/// holds no WAL yet.
fn resume_point(
    store: &dyn CasStore,
    layout: &TenantLayout,
    tail: Option<&zou_store::manifest::WalTail>,
) -> Result<Option<ResumePoint>, i32> {
    let Some(last) = tail.and_then(|t| t.segments.last()) else {
        return Ok(None);
    };
    let epoch = zou_store::commit::segment_epoch(last).ok_or(ZOU_ERR_STORE)?;
    let key = layout.wal_segment_path(last);
    let (bytes, _) = store
        .get(&key)
        .map_err(|_| ZOU_ERR_STORE)?
        .ok_or(ZOU_ERR_STORE)?;
    // Segments are one frame uploaded atomically, but read them all and
    // keep the newest anyway, torn history must fail loudly here.
    let mut newest = None;
    for frame in zou_store::SegmentReader::new(&bytes, epoch) {
        newest = Some(frame.map_err(|_| ZOU_ERR_STORE)?);
    }
    let frame = newest.ok_or(ZOU_ERR_STORE)?;
    let records = zou_store::commit::split_records(&frame.payload).ok_or(ZOU_ERR_STORE)?;
    let record = records.last().ok_or(ZOU_ERR_STORE)?;
    if record.len() < 8 {
        return Err(ZOU_ERR_STORE);
    }
    let start = u64::from_le_bytes(record[..8].try_into().expect("checked length"));
    Ok(Some(ResumePoint {
        pg_lsn: start + (record.len() as u64 - 8),
        stream_lsn: frame.end_lsn.0,
    }))
}

/// The lease, heartbeat, and pipeline setup behind [`zou_wal_open`],
/// separated so tests can run several writer sessions in one process.
/// Returns the pipe plus the Postgres LSN to resume pushing from, zero
/// when the store holds no WAL and pushing starts at `flush_lsn`.
fn open_wal_pipe(target: &str, flush_lsn: u64) -> Result<(WalPipe, u64), i32> {
    let store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(target));
    let layout = TenantLayout::new("local");
    let manifest_key = layout.manifest();
    match store.get(&manifest_key) {
        Ok(Some(_)) => {}
        Ok(None) => {
            let genesis = Manifest::new("local", 18);
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
    // The manifest tail can lag reality: frames become durable, and acked,
    // on upload, and sessions can die before publishing. The scan is the
    // truth, and seeding the pipeline with it makes the next publish carry
    // the whole history forward.
    let tail = zou_store::commit::reconcile_tail(&*store, &layout, held.manifest())
        .map_err(|_| ZOU_ERR_STORE)?;
    let resume = resume_point(&*store, &layout, tail.as_ref())?;
    let (resume_pg, stream_start) = match &resume {
        Some(point) => (point.pg_lsn, point.stream_lsn),
        None => (0, flush_lsn),
    };
    let held = Arc::new(Mutex::new(held));
    let heartbeat = Heartbeat::spawn(
        Arc::clone(&store),
        layout.clone(),
        Arc::clone(&held),
        WAL_LEASE_TTL_SECS,
    );
    let mut builder = GroupCommit::builder(Arc::clone(&store), layout.clone())
        .lease(held)
        .start_lsn(Lsn(stream_start))
        .config(GroupCommitConfig::default())
        .tail_config(TailConfig::default());
    if let Some(tail) = tail {
        builder = builder.initial_tail(tail);
    }
    let commit = builder.build();
    Ok((
        WalPipe {
            commit: Some(commit),
            heartbeat: Some(heartbeat),
            store,
            layout,
        },
        resume_pg,
    ))
}

fn close_wal_pipe(pipe: &mut WalPipe) -> i32 {
    let mut rc = ZOU_OK;
    if let Some(commit) = pipe.commit.take()
        && commit.close().is_err()
    {
        rc = ZOU_ERR_STORE;
    }
    if let Some(heartbeat) = pipe.heartbeat.take()
        && heartbeat.detach().is_err()
    {
        rc = ZOU_ERR_LEASE_LOST;
    }
    rc
}

/// Open the WAL pipeline: genesis manifest if the store is empty, writer
/// lease, heartbeat renewal, group commit chained onto the WAL already in
/// the store.
///
/// Each appended record is a contiguous chunk of WAL prefixed with its
/// Postgres start LSN, which makes the stream self describing for the
/// recovery path. When the store already holds WAL, `out_resume` receives
/// the Postgres LSN right after its last record and the caller must push
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
        if target.contains("://") {
            return ZOU_ERR_BAD_ARGUMENT;
        }
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

/// Append one chunk of WAL starting at Postgres LSN `pg_lsn` and block
/// until it is durable on the store. On success writes the durable zou
/// stream position through `out_durable`, which is diagnostics only, the
/// caller's durability cursor is `pg_lsn + len`.
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
        let pipe = pipe.lock().expect("wal pipe mutex poisoned");
        if pipe.heartbeat.as_ref().is_some_and(Heartbeat::lost) {
            return ZOU_ERR_LEASE_LOST;
        }
        let Some(commit) = pipe.commit.as_ref() else {
            return ZOU_ERR_NOT_INITIALIZED;
        };
        let chunk = unsafe { std::slice::from_raw_parts(data, len) };
        let mut record = Vec::with_capacity(8 + len);
        record.extend_from_slice(&pg_lsn.to_le_bytes());
        record.extend_from_slice(chunk);
        let ticket = match commit.append(&record) {
            Ok(ticket) => ticket,
            Err(_) => return ZOU_ERR_STORE,
        };
        match ticket.wait() {
            Ok(durable) => {
                unsafe { *out_durable = durable.0 };
                ZOU_OK
            }
            Err(_) => ZOU_ERR_STORE,
        }
    })
}

/// Fold the completed checkpoint at `redo` into a checkpoint object and
/// truncate the mirrored tail, see [`fold::fold`]. Called by the pusher
/// from the data directory when it is fully caught up, so relative paths
/// resolve inside PGDATA. Writes the count of dropped sealed segments
/// through `out_dropped`. Returns 0 for a delta, 1 when the fold down
/// policy promoted the capture to a full, negative on error. Errors are
/// transient, the caller retries after a backoff.
///
/// # Safety
/// `out_dropped` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_wal_fold(redo: u64, out_dropped: *mut u32) -> i32 {
    wrap(|| {
        if out_dropped.is_null() {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let Some(pipe) = WAL.get() else {
            return ZOU_ERR_NOT_INITIALIZED;
        };
        let pipe = pipe.lock().expect("wal pipe mutex poisoned");
        if pipe.heartbeat.as_ref().is_some_and(Heartbeat::lost) {
            return ZOU_ERR_LEASE_LOST;
        }
        let Some(commit) = pipe.commit.as_ref() else {
            return ZOU_ERR_NOT_INITIALIZED;
        };
        match fold::fold(&*pipe.store, &pipe.layout, commit, Path::new("."), redo) {
            Ok(stats) => {
                unsafe { *out_dropped = stats.dropped as u32 };
                // 1 tells the pusher the fold down policy promoted this
                // fold to a full capture, 0 is the everyday delta.
                match stats.kind {
                    zou_store::manifest::CheckpointKind::Full => 1,
                    zou_store::manifest::CheckpointKind::Delta => ZOU_OK,
                }
            }
            Err(e) => {
                // The bgworker's stderr lands in the server log, which is
                // the only channel this shim has for the error detail.
                eprintln!("zou_wal_fold: {e}");
                ZOU_ERR_STORE
            }
        }
    })
}

/// Seal the open segment, publish wal_tail, and release the lease.
/// Called when the pusher worker exits so a clean shutdown leaves an
/// exact manifest and the next start acquires the lease immediately.
#[unsafe(no_mangle)]
pub extern "C" fn zou_wal_close() -> i32 {
    wrap(|| {
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

    /// One test drives the whole C ABI lifecycle, because the shim is a
    /// process global and tests share the process.
    #[test]
    fn the_c_abi_lifecycle_works_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let target = CString::new(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(unsafe { zou_pg_init(target.as_ptr()) }, ZOU_OK);
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
                unsafe { zou_smgr_extend(spc, db, rel, fork, blk, page.as_ptr()) },
                ZOU_OK
            );
        }
        let mut n = 0u32;
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

        // Each append blocks until durable, the stream position advances
        // past the payload plus its pg LSN header every time.
        let chunk = vec![7u8; 4096];
        let mut durable = 0u64;
        let mut last = start;
        for i in 0..8 {
            let pg_lsn = start + i * 4096;
            assert_eq!(
                unsafe { zou_wal_append(chunk.as_ptr(), chunk.len(), pg_lsn, &mut durable) },
                ZOU_OK
            );
            assert!(durable > last + 4096, "durable past payload each round");
            last = durable;
        }

        assert_eq!(zou_wal_close(), ZOU_OK);

        // A later session, as after a server restart, resumes exactly
        // where the stream ended, not at the caller's flush pointer.
        let target_str = dir.path().to_str().unwrap();
        let (mut pipe, resumed) = open_wal_pipe(target_str, start + 999).unwrap();
        assert_eq!(resumed, start + 8 * 4096);
        {
            let commit = pipe.commit.as_ref().unwrap();
            let mut record = resumed.to_le_bytes().to_vec();
            record.extend_from_slice(&[9u8; 128]);
            commit.append(&record).unwrap().wait().unwrap();
        }
        assert_eq!(close_wal_pipe(&mut pipe), ZOU_OK);

        // The manifest tail chains both sessions, and every record round
        // trips with its pg LSN header.
        let store = LocalFsStore::new(dir.path());
        let layout = TenantLayout::new("local");
        let (data, _) = store.get(&layout.manifest()).unwrap().unwrap();
        let manifest = Manifest::from_json(&data).unwrap();
        let tail = manifest.wal_tail.expect("tail published");
        assert!(!tail.segments.is_empty());
        assert!(manifest.lease.is_none(), "lease released on close");
        let epochs: std::collections::BTreeSet<u64> = tail
            .segments
            .iter()
            .map(|s| zou_store::commit::segment_epoch(s).unwrap())
            .collect();
        assert_eq!(epochs.len(), 2, "two writer sessions in one tail");

        let mut records = Vec::new();
        for name in &tail.segments {
            let epoch = zou_store::commit::segment_epoch(name).unwrap();
            let (bytes, _) = store.get(&layout.wal_segment_path(name)).unwrap().unwrap();
            for frame in zou_store::SegmentReader::new(&bytes, epoch) {
                let frame = frame.expect("well formed frame");
                records.extend(
                    zou_store::commit::split_records(&frame.payload).expect("well formed batch"),
                );
            }
        }
        assert_eq!(records.len(), 9);
        for (i, record) in records.iter().take(8).enumerate() {
            let lsn = u64::from_le_bytes(record[..8].try_into().unwrap());
            assert_eq!(lsn, start + i as u64 * 4096);
            assert_eq!(record.len(), 8 + 4096);
        }
        let last = records.last().unwrap();
        assert_eq!(u64::from_le_bytes(last[..8].try_into().unwrap()), resumed);
        assert_eq!(last.len(), 8 + 128);
    }
}
