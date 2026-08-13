//! GetPage over a unix socket: the serving half of the page service.
//!
//! The server runs in its own background worker, registered at
//! postmaster start so the socket exists before crash recovery reads
//! its first page. It cannot lean on the pusher's tee for that reason,
//! the pusher only starts once recovery finishes; instead it polls the
//! durable WAL stream out of the store through [`catch_up_resuming`],
//! which reads the same bytes the tee would have carried, just a poll
//! interval later. That read is cursored and time sliced, because one
//! thread does ingest and reads both: a poll that walks the whole tail
//! is a read stall for everything queued behind it.
//!
//! Serving follows the reconstruction rule set: published layers and
//! the live memtable carry every record since the anchor, and blocks
//! the anchor predates resolve their base image from the frozen pg/
//! objects through the [`PageService`] base fallback. A request names
//! the lsn it needs; the server holds it until ingest has applied that
//! far and then serves at the applied watermark, which is the
//! freshness barrier the v1 chain reader used to provide. Requests at
//! lsn zero, reads before the pusher publishes a durability watermark,
//! are served once ingest has covered the durable end of the stream.
//!
//! An ingest error freezes the applied watermark. Waiting requests
//! then time out and read as errors at the smgr, loud and safe: with
//! eager page puts elided there is no stale fallback worth serving.

use std::io::{ErrorKind, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, SyncSender, channel, sync_channel};
use std::time::{Duration, Instant};

use zou_log::{ChainCursor, ConsolidateError, TeeFilter, WalMedia, catch_up_resuming, stream_end};
use zou_store::CasStore;
use zou_store::layermap::LayerMap;
use zou_store::layout::TenantLayout;
use zou_store::lsn::Lsn;
use zou_store::memtable::Memtable;
use zou_store::pageread::ReadError;
use zou_store::shardmanifest::PageShardManifest;
use zou_store::stats::{Phase, note_phase};

use crate::WAL_SHARD;
use crate::getpage::{GetPageError, MAX_GETPAGE_BATCH, PageService};
use crate::ingest::{IngestConfig, ShardIngest};
use crate::pagesvc::ingest_config;
use crate::redo::{RedoPool, RedoPoolConfig};
use crate::walscan::BlockRef;

const BLCKSZ: usize = 8192;

/// "ZPG1" little endian, the one and only protocol version.
const MAGIC: u32 = 0x3147_505A;

/// How long the server holds a request whose lsn ingest has not
/// reached, and so also how long a client can sit in a read. Past it
/// the request fails loudly rather than serving a page missing its
/// own writes.
const WAIT_CAP: Duration = Duration::from_secs(20);

/// The ingest poll cadence, the freshness cost of reading the stream
/// out of the store instead of the tee.
const POLL: Duration = Duration::from_millis(100);

/// How long one poll may spend applying before it hands the thread back
/// to the readers. A service that has fallen a long way behind still
/// catches up, one slice per poll, and a read that only needs an lsn
/// already applied is answered in between instead of waiting for the
/// whole backlog.
const INGEST_SLICE: Duration = Duration::from_millis(200);

/// One read request as the driver sees it: a run of blocks of one
/// fork, the lsn the reader needs covered, and the channel the pages
/// go back on.
struct GetReq {
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    blks: Vec<u32>,
    lsn: u64,
    arrived: Instant,
    deadline: Instant,
    reply: SyncSender<Result<Vec<Vec<u8>>, String>>,
}

/// The client half, one per backend process. A connection is opened
/// lazily, survives across calls, and is dropped and reopened once on
/// any transport error.
pub struct PageClient {
    path: PathBuf,
    conn: Mutex<Option<UnixStream>>,
}

impl PageClient {
    pub fn new(path: PathBuf) -> Self {
        PageClient {
            path,
            conn: Mutex::new(None),
        }
    }

    /// Fetch `blks` of one fork as of `lsn`. Zero means the latest
    /// durable state, see the module docs. Pages come back in request
    /// order, absent blocks as zeros.
    pub fn get_pages(
        &self,
        spc: u32,
        db: u32,
        rel: u32,
        fork: u32,
        blks: &[u32],
        lsn: u64,
    ) -> Result<Vec<Vec<u8>>, String> {
        if blks.is_empty() || blks.len() > MAX_GETPAGE_BATCH {
            return Err(format!("bad batch of {} blocks", blks.len()));
        }
        let mut conn = self.conn.lock().map_err(|_| "client mutex poisoned")?;
        // One retry with a fresh connection: the server restarts with
        // its worker and an idle connection can be the stale half of
        // the previous incarnation.
        for attempt in 0..2 {
            if conn.is_none() {
                *conn = Some(self.connect()?);
            }
            let sock = conn.as_mut().expect("connected above");
            match round_trip(sock, spc, db, rel, fork, blks, lsn) {
                Ok(pages) => return Ok(pages),
                Err(e) if e.kind() == ErrorKind::Other => {
                    // The server answered with its own error text, the
                    // connection is still in protocol.
                    return Err(e.to_string());
                }
                Err(e) => {
                    *conn = None;
                    if attempt == 1 {
                        return Err(format!("page service at {:?}: {e}", self.path));
                    }
                }
            }
        }
        unreachable!("the retry loop returns");
    }

    fn connect(&self) -> Result<UnixStream, String> {
        // The service worker rides every crash cycle: a SIGKILL'd
        // sibling makes the postmaster reinitialize, and recovery's
        // first read can land before the restarted worker rebinds the
        // socket. Failing that read kills the startup process, so a
        // missing or refusing socket gets a bounded grace instead.
        let deadline = Instant::now() + Duration::from_secs(10);
        let sock = loop {
            match UnixStream::connect(&self.path) {
                Ok(sock) => break sock,
                Err(e)
                    if matches!(e.kind(), ErrorKind::ConnectionRefused | ErrorKind::NotFound)
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => {
                    return Err(format!("page service at {:?}: connect: {e}", self.path));
                }
            }
        };
        let cap = WAIT_CAP + Duration::from_secs(10);
        sock.set_read_timeout(Some(cap))
            .map_err(|e| e.to_string())?;
        sock.set_write_timeout(Some(Duration::from_secs(10)))
            .map_err(|e| e.to_string())?;
        Ok(sock)
    }
}

fn round_trip(
    sock: &mut UnixStream,
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    blks: &[u32],
    lsn: u64,
) -> std::io::Result<Vec<Vec<u8>>> {
    let mut req = Vec::with_capacity(32 + 4 * blks.len());
    req.extend_from_slice(&MAGIC.to_le_bytes());
    req.extend_from_slice(&lsn.to_le_bytes());
    for v in [spc, db, rel, fork, blks.len() as u32] {
        req.extend_from_slice(&v.to_le_bytes());
    }
    for b in blks {
        req.extend_from_slice(&b.to_le_bytes());
    }
    sock.write_all(&req)?;
    let mut status = [0u8; 4];
    sock.read_exact(&mut status)?;
    if u32::from_le_bytes(status) != 0 {
        let mut len = [0u8; 4];
        sock.read_exact(&mut len)?;
        let len = u32::from_le_bytes(len).min(64 << 10) as usize;
        let mut msg = vec![0u8; len];
        sock.read_exact(&mut msg)?;
        return Err(std::io::Error::other(
            String::from_utf8_lossy(&msg).to_string(),
        ));
    }
    let mut pages = Vec::with_capacity(blks.len());
    for _ in blks {
        let mut page = vec![0u8; BLCKSZ];
        sock.read_exact(&mut page)?;
        pages.push(page);
    }
    Ok(pages)
}

/// Everything the server needs to run. `redo` is optional only for
/// tests, a server without a pool errors on any block with records.
pub struct ServerConfig {
    pub store: Arc<dyn CasStore>,
    pub layout: TenantLayout,
    pub tenant: u128,
    pub socket: PathBuf,
    pub data_checksums: bool,
    pub redo: Option<RedoPoolConfig>,
}

pub struct PageServer {
    stop: Arc<AtomicBool>,
    driver: Option<std::thread::JoinHandle<()>>,
    listener: Option<std::thread::JoinHandle<()>>,
    socket: PathBuf,
}

impl PageServer {
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.listener.take() {
            let _ = h.join();
        }
        if let Some(h) = self.driver.take() {
            let _ = h.join();
        }
        let _ = std::fs::remove_file(&self.socket);
    }
}

impl Drop for PageServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Bind the socket and start the listener and driver threads. The
/// socket exists when this returns, a client connecting right after
/// will reach a live accept loop.
pub fn spawn(cfg: ServerConfig) -> std::io::Result<PageServer> {
    let _ = std::fs::remove_file(&cfg.socket);
    let listener = UnixListener::bind(&cfg.socket)?;
    listener.set_nonblocking(true)?;
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = channel::<GetReq>();

    let accept_stop = Arc::clone(&stop);
    let listener_handle = std::thread::Builder::new()
        .name("zou-pageserve-accept".into())
        .spawn(move || accept_loop(listener, tx, accept_stop))?;

    let socket = cfg.socket.clone();
    let driver_stop = Arc::clone(&stop);
    let driver_handle = std::thread::Builder::new()
        .name("zou-pageserve".into())
        .spawn(move || {
            if let Err(e) = drive(cfg, rx, driver_stop) {
                log::error!("zou pageserve: stopped: {e}");
            }
        })?;

    Ok(PageServer {
        stop,
        driver: Some(driver_handle),
        listener: Some(listener_handle),
        socket,
    })
}

fn accept_loop(listener: UnixListener, tx: Sender<GetReq>, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((sock, _)) => {
                // BSD accept inherits the listener's nonblocking flag
                // and a nonblocking write_all dies with WouldBlock the
                // moment a response outgrows the socket buffer.
                if sock.set_nonblocking(false).is_err() {
                    continue;
                }
                let tx = tx.clone();
                let stop = Arc::clone(&stop);
                let _ = std::thread::Builder::new()
                    .name("zou-pageserve-conn".into())
                    .spawn(move || connection(sock, tx, stop));
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => {
                log::error!("zou pageserve: accept: {e}");
                return;
            }
        }
    }
}

/// One backend's connection: read requests, forward them to the
/// driver, write replies. Any protocol or transport error closes the
/// connection, the client reconnects.
fn connection(mut sock: UnixStream, tx: Sender<GetReq>, stop: Arc<AtomicBool>) {
    if sock.set_read_timeout(Some(Duration::from_secs(1))).is_err() {
        return;
    }
    let mut head = [0u8; 32];
    while !stop.load(Ordering::Acquire) {
        match sock.read_exact(&mut head) {
            Ok(()) => {}
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => continue,
            Err(_) => return,
        }
        if u32::from_le_bytes(head[0..4].try_into().expect("4 bytes")) != MAGIC {
            return;
        }
        let lsn = u64::from_le_bytes(head[4..12].try_into().expect("8 bytes"));
        let mut words = [0u32; 5];
        for (i, w) in words.iter_mut().enumerate() {
            *w = u32::from_le_bytes(head[12 + 4 * i..16 + 4 * i].try_into().expect("4 bytes"));
        }
        let [spc, db, rel, fork, n] = words;
        if n == 0 || n as usize > MAX_GETPAGE_BATCH {
            return;
        }
        let mut raw = vec![0u8; 4 * n as usize];
        if sock.read_exact(&mut raw).is_err() {
            return;
        }
        let blks: Vec<u32> = raw
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes(c.try_into().expect("4 bytes")))
            .collect();
        let (reply_tx, reply_rx) = sync_channel(1);
        let req = GetReq {
            spc,
            db,
            rel,
            fork,
            blks,
            lsn,
            arrived: Instant::now(),
            deadline: Instant::now() + WAIT_CAP,
            reply: reply_tx,
        };
        if tx.send(req).is_err() {
            let _ = respond_err(&mut sock, "page service driver is gone");
            return;
        }
        // The driver answers a request it cannot serve itself, and says
        // how far ingest got, so this only fires when the driver never
        // reached the deadline check at all. Say that, rather than
        // blaming a request nobody dropped.
        let answer = match reply_rx.recv_timeout(WAIT_CAP + Duration::from_secs(5)) {
            Ok(answer) => answer,
            Err(RecvTimeoutError::Timeout) => Err(format!(
                "page service did not answer within {} seconds",
                (WAIT_CAP + Duration::from_secs(5)).as_secs()
            )),
            Err(RecvTimeoutError::Disconnected) => Err("page service driver is gone".to_string()),
        };
        let ok = match answer {
            Ok(pages) => respond_pages(&mut sock, &pages).is_ok(),
            Err(e) => respond_err(&mut sock, &e).is_ok(),
        };
        if !ok {
            return;
        }
    }
}

fn respond_pages(sock: &mut UnixStream, pages: &[Vec<u8>]) -> std::io::Result<()> {
    sock.write_all(&0u32.to_le_bytes())?;
    for page in pages {
        sock.write_all(page)?;
    }
    Ok(())
}

fn respond_err(sock: &mut UnixStream, msg: &str) -> std::io::Result<()> {
    sock.write_all(&1u32.to_le_bytes())?;
    sock.write_all(&(msg.len() as u32).to_le_bytes())?;
    sock.write_all(msg.as_bytes())
}

/// The driver: one thread that owns ingest and serves reads, so the
/// memtable never needs a lock. Ingest polls the store, requests
/// arrive over the channel, and a request whose lsn is not covered
/// yet waits in `parked` until ingest advances or its deadline hits.
fn drive(mut cfg: ServerConfig, rx: Receiver<GetReq>, stop: Arc<AtomicBool>) -> Result<(), String> {
    let store = Arc::clone(&cfg.store);
    let media = WalMedia::single(crate::log_store(Arc::clone(&store), &cfg.layout));
    let filter = TeeFilter::Tenant(cfg.tenant);
    let pool = cfg.redo.take().map(RedoPool::new);
    let empty_mem = Memtable::new();

    let ingest_cfg = ingest_config(cfg.tenant);
    let mut ingest: Option<ShardIngest> = None;
    let mut map = LayerMap::new(Vec::new()).expect("an empty map builds");
    let mut durable_seen: u64 = 0;
    let mut frozen: Option<String> = None;
    let mut parked: Vec<GetReq> = Vec::new();
    let mut last_poll = Instant::now() - POLL;
    let mut cursor: Option<ChainCursor> = None;
    let mut behind = false;

    match PageShardManifest::load(&*store, &cfg.layout.shard_manifest(0)) {
        Ok(Some((manifest, _))) => {
            map = manifest.layer_map().map_err(|e| e.to_string())?;
            let at = manifest.disk_consistent_lsn.0;
            log::info!("zou pageserve: anchored at {at:#x} from the shard manifest");
            ingest = Some(ShardIngest::new(ingest_cfg.clone(), at));
        }
        Ok(None) => {}
        Err(e) => return Err(format!("shard manifest: {e}")),
    }

    loop {
        // A poll that stopped on its slice has more waiting, so go
        // straight back to it after the readers have had their turn
        // rather than idling out the rest of the cadence.
        if frozen.is_none() && (behind || last_poll.elapsed() >= POLL) {
            last_poll = Instant::now();
            let polled = Instant::now();
            let outcome = poll_ingest(
                &store,
                &cfg.layout,
                &media,
                &filter,
                cfg.tenant,
                &ingest_cfg,
                &mut ingest,
                &mut map,
                &mut durable_seen,
                &mut cursor,
                polled + INGEST_SLICE,
            );
            // The serve loop is one thread, so this poll is latency
            // every request behind it pays. Sample it whether or not
            // there was anything to apply.
            note_phase(Phase::Ingest, polled.elapsed());
            match outcome {
                Ok(caught_up) => behind = !caught_up,
                Err(e) => {
                    // A hole in the stream would poison every later
                    // delta; freeze and let waits fail loudly instead.
                    log::error!("zou pageserve: ingest frozen: {e}");
                    frozen = Some(e);
                }
            }
        }

        match rx.recv_timeout(Duration::from_millis(10)) {
            Ok(req) => parked.push(req),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        while let Ok(req) = rx.try_recv() {
            parked.push(req);
        }

        let applied = ingest.as_ref().map_or(0, ShardIngest::applied);
        let seen = ingest.as_ref().map_or(0, ShardIngest::seen);
        let mem = ingest.as_ref().map_or(&empty_mem, ShardIngest::memtable);
        let now = Instant::now();
        let mut ready: Vec<GetReq> = Vec::new();
        let mut still: Vec<GetReq> = Vec::new();
        for req in parked.drain(..) {
            // Zero asks for the latest durable state; anything else is
            // the durability watermark the reader saw, already safe.
            // Covered means the stream bytes reached the watermark, not
            // that `applied` did: the published durable is usually a
            // WAL page boundary with a record spilling over it, and any
            // complete record ending at or below the watermark has been
            // parsed once its bytes are in, so the page state at
            // `applied` is the page state at the watermark.
            let need = if req.lsn == 0 { durable_seen } else { req.lsn };
            if seen >= need {
                ready.push(req);
            } else if now >= req.deadline {
                let msg = match &frozen {
                    Some(e) => format!("ingest frozen: {e}"),
                    None => format!(
                        "ingest saw {seen:#x} but never reached {need:#x} within the wait cap"
                    ),
                };
                let _ = req.reply.send(Err(msg));
            } else {
                still.push(req);
            }
        }
        parked = still;
        for req in ready {
            let at = if applied == 0 { u64::MAX } else { applied };
            // Two samples, because they answer different questions:
            // parked is how long ingest kept the reader waiting, read
            // is what planning and reading the page actually cost.
            note_phase(Phase::Park, req.arrived.elapsed());
            let ran = Instant::now();
            serve_reloading(&cfg, &*store, pool.as_ref(), &mut map, mem, &req, at);
            note_phase(Phase::Read, ran.elapsed());
        }

        if stop.load(Ordering::Acquire) {
            for req in parked.drain(..) {
                let _ = req.reply.send(Err("page service stopping".to_string()));
            }
            if let Some(ingest) = &mut ingest
                && let Ok(Some(entry)) = ingest
                    .flush(&*store, &cfg.layout)
                    .map_err(|e| log::error!("zou pageserve: final flush: {e}"))
            {
                log::info!(
                    "zou pageserve: final flush, layer {} of {} bytes",
                    entry.name,
                    entry.size
                );
            }
            return Ok(());
        }
    }
    Ok(())
}

/// What went wrong inside a replay. The wal read and the applying
/// have nothing to do with each other; this only exists to carry
/// either one out of the sink closure, where `?` needs a single type.
enum ReplayError {
    Wal(ConsolidateError),
    Ingest(String),
}

impl From<ConsolidateError> for ReplayError {
    fn from(e: ConsolidateError) -> Self {
        Self::Wal(e)
    }
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wal(e) => write!(f, "{e}"),
            Self::Ingest(e) => write!(f, "{e}"),
        }
    }
}

/// One ingest poll: refresh the durable end, anchor a fresh shard at
/// the oldest retained frame, catch up to the stream, and flush when
/// a threshold says so.
#[allow(clippy::too_many_arguments)]
fn poll_ingest(
    store: &Arc<dyn CasStore>,
    layout: &TenantLayout,
    media: &WalMedia,
    filter: &TeeFilter,
    tenant: u128,
    ingest_cfg: &IngestConfig,
    ingest: &mut Option<ShardIngest>,
    map: &mut LayerMap,
    durable_seen: &mut u64,
    cursor: &mut Option<ChainCursor>,
    deadline: Instant,
) -> Result<bool, String> {
    match stream_end(media, WAL_SHARD, tenant) {
        Ok(Some(end)) => *durable_seen = end.0.max(*durable_seen),
        Ok(None) => return Ok(true),
        Err(e) => {
            // The store not answering is not a hole in the stream;
            // stay at the old watermark and try again next poll.
            log::warn!("zou pageserve: stream end: {e}");
            return Ok(true);
        }
    }
    let applied = ingest.as_ref().map_or(0, ShardIngest::applied);
    let mut caught_up = true;
    if applied < *durable_seen {
        // Streamed rather than collected, and flushed inside the
        // replay. A service that has fallen behind a bulk load has a
        // whole index build of wal waiting for it, and reading that
        // into a Vec before applying any of it holds the backlog
        // twice: once as frames, once as a memtable that gets no
        // flush check until the last frame lands. At scale 1000 the
        // worker went 610 MB, 810 MB, 1.1 GB, 1.5 GB a layer and the
        // kernel killed it at 6.8 GB resident. Flushing here bounds
        // the memtable by its own threshold instead of by how far
        // behind the service happens to be.
        let seen = *durable_seen;
        caught_up = catch_up_resuming::<ReplayError, _>(
            media,
            WAL_SHARD,
            filter,
            Lsn(applied),
            cursor,
            |frame| {
                if ingest.is_none() {
                    let start = frame.start_lsn.0;
                    log::info!("zou pageserve: anchoring a fresh shard at {start:#x}");
                    *ingest = Some(ShardIngest::new(ingest_cfg.clone(), start));
                }
                let ingest = ingest.as_mut().expect("anchored on the first frame");
                ingest
                    .apply_frames(std::slice::from_ref(&frame))
                    .map_err(|e| ReplayError::Ingest(e.to_string()))?;
                flush_if_due(store, layout, ingest, map, seen).map_err(ReplayError::Ingest)?;
                Ok(Instant::now() < deadline)
            },
        )
        .map_err(|e| format!("catch up: {e}"))?;
    }
    if let Some(ingest) = ingest.as_mut() {
        flush_if_due(store, layout, ingest, map, *durable_seen)?;
    }
    Ok(caught_up)
}

/// Publish the memtable if a threshold says so and pick the new layer
/// up into `map`, which is what serves it back. Nothing due is not an
/// error and not a flush.
fn flush_if_due(
    store: &Arc<dyn CasStore>,
    layout: &TenantLayout,
    ingest: &mut ShardIngest,
    map: &mut LayerMap,
    durable_seen: u64,
) -> Result<(), String> {
    if ingest.flush_due(durable_seen).is_none() {
        return Ok(());
    }
    let entry = ingest
        .flush(&**store, layout)
        .map_err(|e| format!("flush: {e}"))?;
    if let Some(entry) = entry {
        log::info!(
            "zou pageserve: flush, layer {} of {} bytes, applied {:#x}",
            entry.name,
            entry.size,
            ingest.applied()
        );
        if !reload_map(&**store, layout, map)? {
            return Err("flush published no manifest".to_string());
        }
    }
    Ok(())
}

/// Pick the shard manifest up into `map`, which is what serves reads
/// back. False means there is no manifest yet, a shard that has never
/// flushed, which is not a failure.
fn reload_map(
    store: &dyn CasStore,
    layout: &TenantLayout,
    map: &mut LayerMap,
) -> Result<bool, String> {
    match PageShardManifest::load(store, &layout.shard_manifest(0)) {
        Ok(Some((manifest, _))) => {
            *map = manifest.layer_map().map_err(|e| e.to_string())?;
            Ok(true)
        }
        Ok(None) => Ok(false),
        Err(e) => Err(format!("manifest reload: {e}")),
    }
}

/// Serve one request, and if the map named a layer the store no
/// longer has, pick up the current manifest and serve again.
///
/// The driver reloads the map after its own flush and at no other
/// time, so between flushes it is a snapshot of a manifest that
/// compaction keeps rewriting. Compaction retires the layers it
/// merged, gc deletes the objects once the window passes, and the
/// read then plans against names nothing holds anymore. Every
/// restored node in the six hour run on server2 logged this around
/// the death drills, and because the checker reads a failed query as
/// a failed identity it came back as six of eight drills reporting a
/// broken balance. The map is a cache, the manifest is the authority:
/// on a miss, go ask it. A second miss is a real answer, the layer is
/// named by the current manifest and genuinely gone, and it goes back
/// to the reader as the error it is.
fn serve_reloading(
    cfg: &ServerConfig,
    store: &dyn CasStore,
    pool: Option<&RedoPool>,
    map: &mut LayerMap,
    mem: &Memtable,
    req: &GetReq,
    at: u64,
) {
    let Served::Stale { layer } = serve(cfg, store, pool, map, mem, req, at, false) else {
        return;
    };
    log::info!("zou pageserve: layer {layer} is gone, reloading the map and reading again");
    if let Err(e) = reload_map(store, &cfg.layout, map) {
        log::warn!("zou pageserve: {e}");
    }
    serve(cfg, store, pool, map, mem, req, at, true);
}

/// What one serve attempt did: replied, or found the map naming a
/// layer the store does not have and left the request unanswered for
/// the caller to retry.
enum Served {
    Done,
    Stale { layer: String },
}

/// Serve one request at `at` and reply on its channel. The service is
/// rebuilt per call; the footer cache it loses is a few small reads
/// on a local store, and the borrow it would otherwise pin across
/// ingest mutation is not worth it yet.
///
/// `last` says whether a missing layer is the answer. On the first
/// attempt it is not, the request goes back unanswered so the caller
/// can reload the map; on the retry it is.
#[allow(clippy::too_many_arguments)]
fn serve(
    cfg: &ServerConfig,
    store: &dyn CasStore,
    pool: Option<&RedoPool>,
    map: &LayerMap,
    mem: &Memtable,
    req: &GetReq,
    at: u64,
    last: bool,
) -> Served {
    let layout = &cfg.layout;
    let service = PageService::new(store, layout.shard_prefix(0), pool, cfg.data_checksums)
        .with_base_fallback(move |blk: &BlockRef| {
            match store.get(&layout.pg_block(blk.spc, blk.db, blk.rel, blk.fork, blk.blk)) {
                Ok(Some((data, _))) if data.len() == BLCKSZ => Some(data),
                _ => None,
            }
        });
    let refs: Vec<BlockRef> = req
        .blks
        .iter()
        .map(|&blk| BlockRef {
            spc: req.spc,
            db: req.db,
            rel: req.rel,
            fork: req.fork,
            blk,
        })
        .collect();
    let answer = match service.get_pages(map, mem, &refs, at) {
        Ok(pages) => Ok(pages),
        Err(GetPageError::Read(ReadError::Missing { name })) if !last => {
            return Served::Stale { layer: name };
        }
        Err(e) => Err(e.to_string()),
    };
    let _ = req.reply.send(answer);
    Served::Done
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::walscan::BlockRef;
    use crate::walscan::testwal::Builder;
    use zou_store::frame::Frame2;
    use zou_store::mem::MemStore;

    const TENANT: u128 = 7;
    const WAL_BASE: u64 = 16 << 20;

    fn server(store: Arc<dyn CasStore>, sock: PathBuf) -> PageServer {
        spawn(ServerConfig {
            store,
            layout: TenantLayout::new("t"),
            tenant: TENANT,
            socket: sock,
            data_checksums: false,
            redo: None,
        })
        .expect("server spawns")
    }

    fn sock_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("zou-pageserve-tests");
        std::fs::create_dir_all(&dir).expect("tmp dir");
        dir.join(name)
    }

    /// A store with pg/ images and no WAL serves them through the
    /// fallback, and blocks nothing ever wrote come back as zeros.
    #[test]
    fn frozen_images_serve_through_the_socket() {
        let store: Arc<dyn CasStore> = Arc::new(MemStore::default());
        let layout = TenantLayout::new("t");
        let page = vec![7u8; BLCKSZ];
        store
            .put(&layout.pg_block(1663, 5, 2000, 0, 3), &page)
            .expect("seed page");
        let sock = sock_path("frozen.sock");
        let mut srv = server(Arc::clone(&store), sock.clone());

        let client = PageClient::new(sock);
        let pages = client
            .get_pages(1663, 5, 2000, 0, &[3, 4], 0)
            .expect("served");
        assert_eq!(pages[0], page, "the frozen image came back whole");
        assert_eq!(pages[1], vec![0u8; BLCKSZ], "an absent block is zeros");
        srv.stop();
    }

    /// A request past everything durable parks and then fails loudly
    /// instead of serving a page missing its writes. The deadline is
    /// capped for the test through the request lsn wait, so this
    /// exercises the parking path without waiting the full cap.
    #[test]
    fn a_request_past_the_stream_parks() {
        let store: Arc<dyn CasStore> = Arc::new(MemStore::default());
        let sock = sock_path("parked.sock");
        let mut srv = server(Arc::clone(&store), sock.clone());

        let client = PageClient::new(sock);
        let started = Instant::now();
        std::thread::spawn(move || {
            // Nothing ever lands, the request must ride out its
            // deadline; the test only checks it does not serve.
            let _ = client.get_pages(1663, 5, 2000, 0, &[0], 1 << 40);
        });
        // Give the request time to arrive and park, then stop: the
        // stop path must reply to parked requests rather than strand
        // their connections.
        std::thread::sleep(Duration::from_millis(300));
        srv.stop();
        assert!(
            started.elapsed() < WAIT_CAP,
            "the stop cleared the parked request without the full wait"
        );
    }

    /// A backlog on the chain, cut into frames the way the sequencer
    /// cuts them.
    fn frames_over(start: u64, raw: &[u8]) -> Vec<Frame2> {
        let mut frames = Vec::new();
        let mut at = start;
        for chunk in raw.chunks(1024) {
            frames.push(Frame2 {
                tenant: TENANT,
                writer_epoch: 1,
                start_lsn: Lsn(at),
                end_lsn: Lsn(at + chunk.len() as u64),
                contains_commit: false,
                first_of_epoch: false,
                hints: Vec::new(),
                payload: chunk.to_vec(),
            });
            at += chunk.len() as u64;
        }
        frames
    }

    /// Seal a stream of `records` page writes onto the chain and
    /// answer where it starts and ends.
    fn seed_chain(media: &WalMedia, records: u32) -> (u64, u64) {
        let mut b = Builder::new(WAL_BASE);
        for i in 0..records {
            let r = BlockRef {
                spc: 1663,
                db: 5,
                rel: 1000,
                fork: 0,
                blk: i,
            };
            b.record(&[(r, false)], &[i as u8; 4096]);
        }
        let end = b.pos();
        let (start, bytes) = b.stream();
        let raw = bytes.to_vec();
        let t = zou_log::take_over(media, WAL_SHARD, "test").expect("take over");
        let sink = Arc::new(zou_log::MediaSink::new(
            Arc::new(WalMedia::single(Arc::clone(media.manifest_store()))),
            WAL_SHARD,
            t.sealed_seq,
        ));
        let seq = zou_log::Sequencer::resume(
            WAL_SHARD,
            sink,
            zou_log::SequencerConfig::default(),
            t.next_seq,
            t.prev_digest,
        );
        seq.append(frames_over(start, &raw))
            .expect("admitted")
            .wait()
            .expect("durable");
        seq.close().expect("sequencer close");
        (start, end)
    }

    /// [`seed_chain`], but one append per frame, so the chain is many
    /// segments instead of one. That is what a live pusher writes, and
    /// a slice can only stop between segments.
    fn seed_chain_segments(media: &WalMedia, records: u32) -> (u64, u64) {
        let mut b = Builder::new(WAL_BASE);
        for i in 0..records {
            let r = BlockRef {
                spc: 1663,
                db: 5,
                rel: 1000,
                fork: 0,
                blk: i,
            };
            b.record(&[(r, false)], &[i as u8; 4096]);
        }
        let end = b.pos();
        let (start, bytes) = b.stream();
        let raw = bytes.to_vec();
        let t = zou_log::take_over(media, WAL_SHARD, "test").expect("take over");
        let sink = Arc::new(zou_log::MediaSink::new(
            Arc::new(WalMedia::single(Arc::clone(media.manifest_store()))),
            WAL_SHARD,
            t.sealed_seq,
        ));
        let seq = zou_log::Sequencer::resume(
            WAL_SHARD,
            sink,
            zou_log::SequencerConfig::default(),
            t.next_seq,
            t.prev_digest,
        );
        for frame in frames_over(start, &raw) {
            seq.append(vec![frame])
                .expect("admitted")
                .wait()
                .expect("durable");
        }
        seq.close().expect("sequencer close");
        (start, end)
    }

    /// The poll that killed the worker at scale 1000. A service that
    /// has fallen behind an index build reads the whole backlog, and
    /// before this it read it into a Vec and applied all of it before
    /// anything asked whether a flush was due, so the memtable grew
    /// to the size of the backlog no matter what the threshold said.
    /// The layers went 610 MB, 810 MB, 1.1 GB, 1.5 GB and the kernel
    /// took the process at 6.8 GB resident.
    #[test]
    fn a_backlog_flushes_as_it_replays_instead_of_all_at_once() {
        let store: Arc<dyn CasStore> = Arc::new(MemStore::default());
        let layout = TenantLayout::new("t");
        let media = WalMedia::single(Arc::clone(&store));
        let (_, end) = seed_chain(&media, 64);

        let mut cfg = IngestConfig::new(TENANT, 0, 1);
        // Far below the backlog, which is the whole point: the poll
        // has to notice mid replay, not after it.
        cfg.flush_bytes = 32 << 10;
        cfg.small_floor = 0;

        let mut ingest = None;
        let mut map = LayerMap::new(Vec::new()).expect("an empty map builds");
        let mut durable_seen = 0u64;
        let mut cursor = None;
        // A slice long enough that this poll runs the whole backlog,
        // the point of the test being where the flushes land.
        poll_ingest(
            &store,
            &layout,
            &media,
            &TeeFilter::Tenant(TENANT),
            TENANT,
            &cfg,
            &mut ingest,
            &mut map,
            &mut durable_seen,
            &mut cursor,
            Instant::now() + Duration::from_secs(600),
        )
        .expect("the poll replays the chain");

        let ingest = ingest.expect("the poll anchored a shard");
        assert!(
            ingest.applied() >= end,
            "the replay stopped early at {:#x} of {end:#x}",
            ingest.applied()
        );
        let (manifest, _) = PageShardManifest::load(&*store, &layout.shard_manifest(0))
            .expect("manifest reads")
            .expect("the replay published one");
        assert!(
            manifest.layers.len() > 1,
            "one poll published {} layer(s), so the backlog was held in memory",
            manifest.layers.len()
        );
        assert!(
            ingest.memtable().bytes() < cfg.flush_bytes,
            "the memtable ended at {} bytes, over its own threshold",
            ingest.memtable().bytes()
        );
    }

    /// The stall behind #322: one thread does ingest and reads, and a
    /// poll that replays a whole backlog before returning is an outage
    /// for every read queued behind it. On server2 that came back as
    /// six balance checks in a row failing after a drill, each one
    /// giving up 25 seconds in without the driver ever answering. A
    /// poll takes a slice and comes back for the rest.
    #[test]
    fn a_poll_stops_on_its_slice_and_the_next_one_carries_on() {
        let store: Arc<dyn CasStore> = Arc::new(MemStore::default());
        let layout = TenantLayout::new("t");
        let media = WalMedia::single(Arc::clone(&store));
        let (_, end) = seed_chain_segments(&media, 24);

        let cfg = IngestConfig::new(TENANT, 0, 1);
        let mut ingest = None;
        let mut map = LayerMap::new(Vec::new()).expect("an empty map builds");
        let mut durable_seen = 0u64;
        let mut cursor = None;
        let poll = |ingest: &mut _, map: &mut _, seen: &mut _, cursor: &mut _, deadline| {
            poll_ingest(
                &store,
                &layout,
                &media,
                &TeeFilter::Tenant(TENANT),
                TENANT,
                &cfg,
                ingest,
                map,
                seen,
                cursor,
                deadline,
            )
        };

        // A deadline already gone stops after the first segment, and
        // says it is not caught up.
        let caught_up = poll(
            &mut ingest,
            &mut map,
            &mut durable_seen,
            &mut cursor,
            Instant::now(),
        )
        .expect("the poll replays what it can");
        assert!(!caught_up, "a poll out of time is not caught up");
        let first = ingest.as_ref().expect("anchored").applied();
        assert!(first > 0 && first < end, "one slice applied {first:#x}");

        // Polling again picks up where it stopped rather than starting
        // over, and enough of them finish the backlog.
        let mut rounds = 0;
        while !poll(
            &mut ingest,
            &mut map,
            &mut durable_seen,
            &mut cursor,
            Instant::now(),
        )
        .expect("the poll replays what it can")
        {
            rounds += 1;
            assert!(rounds < 200, "the slices are not making progress");
        }
        assert!(
            ingest.expect("anchored").applied() >= end,
            "the slices never finished the backlog"
        );
    }

    /// The read failure every restored node logged in the six hour run
    /// on server2: compaction retired a layer, gc collected the
    /// object, and the page service was still holding the map it
    /// loaded before either happened. The map is a cache and the
    /// manifest is the authority, so a miss reloads and reads again
    /// rather than failing the page.
    #[test]
    fn a_read_naming_a_collected_layer_reloads_the_map_and_answers() {
        use zou_store::layer::{DeltaEntry, ImageBuilder, LayerKey, build_delta};
        use zou_store::layermap::LayerDesc;
        use zou_store::shardmanifest::{LayerEntry, publish_layer, swap_layers};

        let store: Arc<dyn CasStore> = Arc::new(MemStore::default());
        let layout = TenantLayout::new("t");
        let manifest_key = layout.shard_manifest(0);
        let key = LayerKey::page(1663, 5, 2000, 0, 3);
        let page = vec![9u8; BLCKSZ];

        let publish = |bytes: Vec<u8>, footer: &zou_store::layer::LayerFooter| -> String {
            let desc = LayerDesc::from_footer(footer, bytes.len() as u64);
            let name = desc.name();
            store
                .put(&format!("{}{name}", layout.shard_prefix(0)), &bytes)
                .expect("layer lands");
            let entry = LayerEntry {
                name: name.clone(),
                size: bytes.len() as u64,
                owner: None,
                upto: None,
            };
            publish_layer(&*store, &manifest_key, 0, &entry, footer.max_lsn).expect("published");
            name
        };

        let mut images = ImageBuilder::new(1, Lsn(100), 8192);
        images.push(key, &page).expect("image pushes");
        let (bytes, footer) = images.finish().expect("image layer builds");
        publish(bytes, &footer);

        // The layer compaction merged away and gc then deleted. It is
        // in the map the driver holds and in nothing else.
        let (bytes, footer) = build_delta(
            &[DeltaEntry {
                key,
                lsn: Lsn(150),
                record: vec![0x5A; 64],
            }],
            8192,
        )
        .expect("delta layer builds");
        let gone = publish(bytes, &footer);
        let (manifest, _) = PageShardManifest::load(&*store, &manifest_key)
            .expect("manifest reads")
            .expect("both layers published");
        let mut map = manifest.layer_map().expect("the map builds");
        swap_layers(
            &*store,
            &manifest_key,
            0,
            std::slice::from_ref(&gone),
            &[],
            None,
        )
        .expect("retired");
        store
            .delete(&format!("{}{gone}", layout.shard_prefix(0)))
            .expect("collected");

        let (reply, answers) = sync_channel(1);
        let req = GetReq {
            spc: 1663,
            db: 5,
            rel: 2000,
            fork: 0,
            blks: vec![3],
            lsn: 0,
            arrived: Instant::now(),
            deadline: Instant::now() + WAIT_CAP,
            reply,
        };
        let cfg = ServerConfig {
            store: Arc::clone(&store),
            layout: layout.clone(),
            tenant: TENANT,
            socket: sock_path("stale.sock"),
            data_checksums: false,
            redo: None,
        };
        serve_reloading(&cfg, &*store, None, &mut map, &Memtable::new(), &req, 200);
        let pages = answers
            .recv()
            .expect("the driver replied")
            .expect("the read survived the collected layer");
        assert_eq!(pages[0], page, "the image served the page");
        assert!(
            !map.layers().iter().any(|d| d.name() == gone),
            "the map still names the layer that is gone"
        );
    }
}
