//! GetPage over a unix socket: the serving half of the page service.
//!
//! The server runs in its own background worker, registered at
//! postmaster start so the socket exists before crash recovery reads
//! its first page. It cannot lean on the pusher's tee for that reason,
//! the pusher only starts once recovery finishes; instead it polls the
//! durable WAL stream out of the store through [`catch_up_resuming`],
//! which reads the same bytes the tee would have carried, just a poll
//! interval later. That read is cursored and time sliced, because the
//! driver owns ingest and the dispatch of every read: a poll that
//! walks the whole tail is a read stall for everything queued behind
//! it.
//!
//! Doing the read is somebody else's job. Serving one is a layer
//! fetch and a redo, hundreds of microseconds at the median and
//! milliseconds at the tail, and a driver that does them one after
//! another is a queue rather than a service: at a few thousand reads a
//! second that one thread is the p99 (zou #671). So a request found
//! covered goes to a small pool instead, carrying the map it plans
//! against and the records above that map for the keys it names, both
//! taken here. Because the drain and the map reload a flush does also
//! happen here, no read is ever handed the moment between them, where
//! the records are in neither.
//!
//! Serving follows the reconstruction rule set: published layers and
//! the live memtable carry every record since the anchor, and blocks
//! the anchor predates resolve their base image from the frozen pg/
//! objects through the [`PageService`] base fallback. A request names
//! the lsn it needs; the server holds it until ingest has applied that
//! far and then serves at the applied watermark, which is the
//! freshness barrier the v1 chain reader used to provide. Requests at
//! lsn zero, reads before the pusher publishes a durability watermark,
//! are served once ingest has covered the durable end of the stream,
//! and a service that has only just anchored does not know where that
//! end is, so they wait for the first walk that reaches it. Recovery
//! reads name the replay position instead, since the startup process
//! knows the lsn it needs better than any watermark does.
//!
//! A request asks for a run of blocks of one fork, or, with a block
//! count of zero, for the fork's length. Zero used to be a protocol
//! error that dropped the connection, so no client in the field sends
//! it and the two shapes can share one request header. A length comes
//! back as four bytes behind the same ok status, and since a client
//! knows what it asked for the two answers never have to be told
//! apart on the wire.
//!
//! An ingest error freezes the applied watermark. Waiting requests
//! then time out and read as errors at the smgr, loud and safe: with
//! eager page puts elided there is no stale fallback worth serving.

use std::collections::VecDeque;
use std::io::{ErrorKind, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, SyncSender, channel, sync_channel};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use zou_log::{CatchUp, CatchUpCursor, ConsolidateError, TeeFilter, WalMedia, catch_up_resuming};
use zou_store::CasStore;
use zou_store::layer::{LayerKey, LayerKind};
use zou_store::layermap::LayerMap;
use zou_store::layout::TenantLayout;
use zou_store::lsn::Lsn;
use zou_store::memtable::Memtable;
use zou_store::pageread::ReadError;
use zou_store::shardmanifest::PageShardManifest;
use zou_store::stats::{ParkCause, Phase, note_park_cause, note_park_gap, note_phase};

use crate::WAL_SHARD;
use crate::getpage::{GetPageError, MAX_GETPAGE_BATCH, PageService};
use crate::ingest::{IngestConfig, ShardIngest};
use crate::pagesvc::ingest_config;
use crate::redo::{RedoPool, RedoPoolConfig};
use crate::walscan::BlockRef;

const BLCKSZ: usize = 8192;

/// The block count that means the layers hold nothing about the fork.
/// This is postgres's InvalidBlockNumber, which no real length can
/// be, so the wire does not need a second field to carry silence.
const SIZE_UNKNOWN: u32 = u32::MAX;

/// "ZPG2" little endian. Version 1 carried one lsn per request, this
/// one carries the block position beside it, and the two headers are
/// different lengths, so a client and a server that disagree have to
/// find out from the first word rather than from the block count.
const MAGIC: u32 = 0x3247_505A;

/// The request header: the magic, the two lsns, and five words of
/// fork and count.
const HEAD: usize = 40;

/// How long the server holds a request whose lsn ingest has not
/// reached, once ingest has stopped moving. A request fails loudly
/// rather than serving a page missing its own writes, but only when
/// waiting longer would not have helped: a service catching up from
/// the store is on its way to the lsn the reader asked for, and
/// failing that read kills the recovery that is doing the reading
/// (zou #336).
const WAIT_CAP: Duration = Duration::from_secs(20);

/// The backstop on the client side of the socket. The driver decides
/// when a wait is hopeless and says so, so this only fires when the
/// driver never got to the decision at all, which is a bug in the
/// driver rather than a slow catch up.
const ANSWER_CAP: Duration = Duration::from_secs(300);

/// The ingest poll cadence, the freshness cost of reading the stream
/// out of the store instead of the tee.
const POLL: Duration = Duration::from_millis(100);

/// The cadence while somebody is parked.
///
/// A park is a read waiting for wal it has been told is durable, and
/// the measurement on #671 says what it is waiting for: 17277 parks in
/// a five minute run, none of them for a record that touched the page
/// in hand, and a median gap of 4096 bytes between the lsn asked for
/// and the lsn ingest held. Half a wal page. The wait was not volume
/// and it was not the reader's own writes, it was the hundred
/// millisecond poll, which is why park p99 was 131 ms and its max was
/// the poll interval to the byte.
///
/// So the first look after a request parks is a millisecond later, and
/// the interval doubles from there to POLL while polls keep coming
/// back empty. A park that ends at the first look costs one extra read
/// of the stream, and one that lasts the old hundred milliseconds
/// costs seven, which is the price of not sleeping through the arrival
/// of four kilobytes.
const PARKED_POLL: Duration = Duration::from_millis(1);

/// How long the loop waits on the request channel with nothing else
/// to do. Also the ceiling on that wait while somebody is parked,
/// since a poll cannot happen sooner than the sleep in front of it.
const CHANNEL_WAIT: Duration = Duration::from_millis(10);

/// What the cadence relaxes to once the stream has stopped moving.
///
/// A poll is a shard manifest and a round index whether or not
/// anything was written, so a project nobody is using read the store
/// twenty one times a second forever: 1883 gets in ninety seconds of
/// an idle laptop node, which on S3 is a bill and a rate limit for
/// nothing. At two seconds the same ninety seconds cost 243. The
/// freshness this spends is bounded by the same two seconds and only
/// on the first read after a quiet spell, since anything that arrives
/// or anybody who waits puts the cadence straight back to POLL.
const IDLE_POLL: Duration = Duration::from_secs(2);

/// How long one poll may spend applying before it hands the thread back
/// to the readers. A service that has fallen a long way behind still
/// catches up, one slice per poll, and a read that only needs an lsn
/// already applied is answered in between instead of waiting for the
/// whole backlog.
const INGEST_SLICE: Duration = Duration::from_millis(200);

/// How often the fold looks at what reads are paying.
const FOLD_EVERY: Duration = Duration::from_secs(60);

/// The least delta worth folding, two flush thresholds. Under this the
/// chain a read walks is short enough that rebuilding the pages the
/// debt touches costs more than reading through it.
const FOLD_DEBT_FLOOR: u64 = 128 << 20;

/// How far back a shard keeps answering, in seconds, unless the
/// deployment says otherwise.
///
/// Zero turns the merge fold off and the store keeps every image and
/// every record forever, which is what it did before there was a
/// horizon at all. It is the escape hatch rather than the setting
/// anybody wants: the disk a soak needs then is the whole write volume
/// of the soak.
const RETENTION: &str = "ZOU_PAGE_RETENTION_SECS";

/// The cadence of the merge fold for a given retention window.
///
/// A shard holds the window plus however long it has been since the
/// last merge, so this is what the overshoot costs: an eighth of the
/// window is at most twelve percent more history than was asked for,
/// which is a cheap price for running the expensive pass eight times
/// per window instead of continuously.
///
/// The floor is the ordinary fold cadence. A merge reads every layer
/// below the horizon to learn its keys, so running it more often than
/// the cheap pass would be backwards. The soak scenario's ten minute
/// window is above the floor and gets one every seventy five seconds.
fn merge_every(retention_secs: u64) -> Option<Duration> {
    match retention_secs {
        0 => None,
        secs => Some(Duration::from_secs((secs / 8).max(FOLD_EVERY.as_secs()))),
    }
}

/// What a request wants of one fork. A batch of blocks is the wire
/// request with a block count on it; a block count of zero is a size
/// request, which used to be a protocol error and so is a shape no
/// client in the field ever sends.
enum Want {
    Pages(Vec<u32>),
    Size,
}

/// What one request gets back, matching what it asked for.
enum Answer {
    Pages(Vec<Vec<u8>>),
    /// `None` is a fork the layers say nothing about, which goes on
    /// the wire as [`SIZE_UNKNOWN`].
    Size(Option<u32>),
}

/// One read request as the driver sees it: one fork, what is wanted of
/// it, the lsn the reader needs covered, and the channel the answer
/// goes back on.
struct GetReq {
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    want: Want,
    lsn: u64,
    /// The newest position any block of this request was written back
    /// at, zero when the backend had nothing to say. Ingest being past
    /// this is enough to answer, whatever `lsn` says, because
    /// everything after it changed some other block.
    since: u64,
    arrived: Instant,
    deadline: Instant,
    /// Where ingest stood when this request first parked, `None` while
    /// it has never parked. A request is looked at once a poll while
    /// it waits, and the wait is one thing however many times it is
    /// looked at, so this is set on the first look and left alone.
    parked_at: Option<ParkedAt>,
    reply: SyncSender<Result<Answer, String>>,
}

/// Where the service stood when a request parked: the lsn ingest had
/// applied, and how many times it had flushed. The wal the request
/// waits through is everything applied after that lsn, and the flush
/// count is what says whether that wal is still in the memtable to be
/// looked at when the wait ends.
#[derive(Clone, Copy)]
struct ParkedAt {
    seen: u64,
    flushes: u64,
}

/// How many times this process has published a layer. A park is
/// classified by looking at the memtable for the wal it waited
/// through, and a flush moves that wal out of the memtable, so the
/// count is what tells a park that can be classified from one that
/// cannot. A static rather than a local because the flush happens two
/// calls deep inside a sink closure that owns the ingest for the
/// length of a poll.
static FLUSHES: AtomicU64 = AtomicU64::new(0);

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
    ///
    /// `since` is the newest position any of these blocks was written
    /// back at, zero when the backend has nothing to say. It is a
    /// smaller ask than `lsn` and usually a much smaller one: ingest
    /// only has to be past it for the answer to be right, where `lsn`
    /// is whatever the busiest writer in the tenant did last.
    #[allow(clippy::too_many_arguments)]
    pub fn get_pages(
        &self,
        spc: u32,
        db: u32,
        rel: u32,
        fork: u32,
        blks: &[u32],
        lsn: u64,
        since: u64,
    ) -> Result<Vec<Vec<u8>>, String> {
        if blks.is_empty() || blks.len() > MAX_GETPAGE_BATCH {
            return Err(format!("bad batch of {} blocks", blks.len()));
        }
        let head = Head {
            spc,
            db,
            rel,
            fork,
            lsn,
            since,
        };
        self.ask(|sock| round_trip(sock, head, blks))
    }

    /// How many blocks long the fork is as of `lsn`, folded out of the
    /// layers. This is the answer `smgr nblocks` needs on a branch,
    /// where the parent's `pg/` prefix says nothing about what the
    /// branch has done since. `None` is silence rather than a length
    /// of zero, see [`PageService::rel_size`].
    pub fn get_size(
        &self,
        spc: u32,
        db: u32,
        rel: u32,
        fork: u32,
        lsn: u64,
    ) -> Result<Option<u32>, String> {
        // A length is a property of the fork rather than of a block,
        // so there is no block position to name and this one waits the
        // way it always did.
        let head = Head {
            spc,
            db,
            rel,
            fork,
            lsn,
            since: 0,
        };
        self.ask(|sock| round_trip_size(sock, head))
    }

    /// Run one exchange on the kept connection, with one retry on a
    /// fresh one: the server restarts with its worker and an idle
    /// connection can be the stale half of the previous incarnation. A
    /// server side error is not a transport error and does not retry.
    fn ask<T>(
        &self,
        exchange: impl Fn(&mut UnixStream) -> std::io::Result<T>,
    ) -> Result<T, String> {
        let mut conn = self.conn.lock().map_err(|_| "client mutex poisoned")?;
        for attempt in 0..2 {
            if conn.is_none() {
                *conn = Some(self.connect()?);
            }
            let sock = conn.as_mut().expect("connected above");
            match exchange(sock) {
                Ok(got) => return Ok(got),
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
        // Long enough to outlast the driver's own backstop, so a read
        // that is going to fail fails with the driver's reason rather
        // than with a timeout on this end.
        let cap = ANSWER_CAP + Duration::from_secs(10);
        sock.set_read_timeout(Some(cap))
            .map_err(|e| e.to_string())?;
        sock.set_write_timeout(Some(Duration::from_secs(10)))
            .map_err(|e| e.to_string())?;
        Ok(sock)
    }
}

/// The fixed part of every request: which fork, and as of when.
#[derive(Clone, Copy)]
struct Head {
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    lsn: u64,
    since: u64,
}

impl Head {
    /// The 40 byte header, with the block count a caller is about to
    /// send that many block numbers for. Zero of them asks for the
    /// fork's length instead.
    fn bytes(&self, n: usize) -> Vec<u8> {
        let mut req = Vec::with_capacity(HEAD + 4 * n);
        req.extend_from_slice(&MAGIC.to_le_bytes());
        req.extend_from_slice(&self.lsn.to_le_bytes());
        req.extend_from_slice(&self.since.to_le_bytes());
        for v in [self.spc, self.db, self.rel, self.fork, n as u32] {
            req.extend_from_slice(&v.to_le_bytes());
        }
        req
    }
}

/// The status word, turning a server side refusal into an error
/// carrying its text. Everything after it is the answer's own shape.
fn read_status(sock: &mut UnixStream) -> std::io::Result<()> {
    let mut status = [0u8; 4];
    sock.read_exact(&mut status)?;
    if u32::from_le_bytes(status) == 0 {
        return Ok(());
    }
    let mut len = [0u8; 4];
    sock.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len).min(64 << 10) as usize;
    let mut msg = vec![0u8; len];
    sock.read_exact(&mut msg)?;
    Err(std::io::Error::other(
        String::from_utf8_lossy(&msg).to_string(),
    ))
}

fn round_trip(sock: &mut UnixStream, head: Head, blks: &[u32]) -> std::io::Result<Vec<Vec<u8>>> {
    let mut req = head.bytes(blks.len());
    for b in blks {
        req.extend_from_slice(&b.to_le_bytes());
    }
    sock.write_all(&req)?;
    read_status(sock)?;
    let mut pages = Vec::with_capacity(blks.len());
    for _ in blks {
        let mut page = vec![0u8; BLCKSZ];
        sock.read_exact(&mut page)?;
        pages.push(page);
    }
    Ok(pages)
}

fn round_trip_size(sock: &mut UnixStream, head: Head) -> std::io::Result<Option<u32>> {
    sock.write_all(&head.bytes(0))?;
    read_status(sock)?;
    let mut n = [0u8; 4];
    sock.read_exact(&mut n)?;
    Ok(Some(u32::from_le_bytes(n)).filter(|&n| n != SIZE_UNKNOWN))
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
    /// Threads that serve reads, `None` for whatever
    /// `ZOU_PAGESERVE_READERS` says. A caller names it so a test can
    /// run both shapes without an environment variable two tests on
    /// two threads would share.
    pub readers: Option<usize>,
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
    let mut head = [0u8; HEAD];
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
        let since = u64::from_le_bytes(head[12..20].try_into().expect("8 bytes"));
        let mut words = [0u32; 5];
        for (i, w) in words.iter_mut().enumerate() {
            *w = u32::from_le_bytes(head[20 + 4 * i..24 + 4 * i].try_into().expect("4 bytes"));
        }
        let [spc, db, rel, fork, n] = words;
        if n as usize > MAX_GETPAGE_BATCH {
            return;
        }
        let want = if n == 0 {
            Want::Size
        } else {
            let mut raw = vec![0u8; 4 * n as usize];
            if sock.read_exact(&mut raw).is_err() {
                return;
            }
            Want::Pages(
                raw.as_chunks::<4>()
                    .0
                    .iter()
                    .map(|c| u32::from_le_bytes(*c))
                    .collect(),
            )
        };
        let (reply_tx, reply_rx) = sync_channel(1);
        let req = GetReq {
            spc,
            db,
            rel,
            fork,
            want,
            lsn,
            since,
            arrived: Instant::now(),
            deadline: Instant::now() + WAIT_CAP,
            parked_at: None,
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
        let answer = match reply_rx.recv_timeout(ANSWER_CAP) {
            Ok(answer) => answer,
            Err(RecvTimeoutError::Timeout) => Err(format!(
                "page service did not answer within {} seconds",
                ANSWER_CAP.as_secs()
            )),
            Err(RecvTimeoutError::Disconnected) => Err("page service driver is gone".to_string()),
        };
        let ok = match answer {
            Ok(Answer::Pages(pages)) => respond_pages(&mut sock, &pages).is_ok(),
            Ok(Answer::Size(n)) => respond_size(&mut sock, n).is_ok(),
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

/// The answer to a size request: the same ok status, then the block
/// count. A client that asked for pages never sees this and a client
/// that asked for a size never sees pages, so the two share a status
/// word without ambiguity.
fn respond_size(sock: &mut UnixStream, n: Option<u32>) -> std::io::Result<()> {
    sock.write_all(&0u32.to_le_bytes())?;
    sock.write_all(&n.unwrap_or(SIZE_UNKNOWN).to_le_bytes())
}

fn respond_err(sock: &mut UnixStream, msg: &str) -> std::io::Result<()> {
    sock.write_all(&1u32.to_le_bytes())?;
    sock.write_all(&(msg.len() as u32).to_le_bytes())?;
    sock.write_all(msg.as_bytes())
}

/// Whether a parked request has waited as long as waiting is worth.
///
/// Not the same question as whether it has waited long enough. A
/// service catching up from the store answers the request the moment
/// it gets there, and the reader is often the recovery whose page this
/// is, which dies of the error and takes the node with it. So a
/// request past its own cap on a watermark that is still moving is
/// early rather than hopeless, and what fails it is the watermark
/// going quiet for a cap of its own. The request cap is still the
/// floor, because an idle service has a watermark that has not moved
/// in hours and a request arriving into that should not fail on the
/// spot.
fn hopeless(past_deadline: bool, stalled: Duration, frozen: bool) -> bool {
    past_deadline && (frozen || stalled >= WAIT_CAP)
}

/// The next gap between polls, given whether this one was worth
/// making.
///
/// Doubling rather than jumping, so that a stream which goes quiet for
/// a fifth of a second and comes back has paid nothing for it and a
/// project nobody has touched since yesterday is at the ceiling.
fn relax(cadence: Duration, worth_it: bool) -> Duration {
    if worth_it {
        POLL
    } else {
        (cadence * 2).min(IDLE_POLL)
    }
}

/// The same doubling for a service with readers waiting on it, between
/// PARKED_POLL and POLL rather than between POLL and IDLE_POLL.
///
/// The ceiling is the old behaviour, so a park that outlives the climb
/// costs what it always cost, and the floor is where every park starts
/// because a reader that has just arrived is waiting for bytes its own
/// backend watched go durable.
fn relax_parked(cadence: Duration, worth_it: bool) -> Duration {
    if worth_it {
        PARKED_POLL
    } else {
        (cadence * 2).min(POLL)
    }
}

/// What the wal a parked request waited through had to do with the
/// pages it asked for. `from` is where the service stood when the
/// request parked and `seen` where it stands now, so the wait was
/// spent applying `(from.seen, seen]` and the question is whether any
/// of that wrote a key the request wants.
///
/// A backend asks for a page at the lsn its own wal pusher has made
/// durable, which is the newest thing anybody in the tenant wrote, so
/// a read of a page nobody touched still waits for every write that
/// landed before it. Counting the two apart is what says whether park
/// time is ingest being slow or the read position being too coarse
/// (zou #671).
fn park_cause(mem: &Memtable, req: &GetReq, from: ParkedAt, seen: u64) -> ParkCause {
    if FLUSHES.load(Ordering::Relaxed) != from.flushes {
        return ParkCause::Unclear;
    }
    let touched = |key: LayerKey| {
        mem.records_for(&key, Lsn(from.seen), Lsn(seen))
            .next()
            .is_some()
    };
    let hit = match &req.want {
        Want::Pages(blks) => blks.iter().any(|&blk| {
            touched(LayerKey::page(
                req.spc,
                req.db,
                req.rel,
                req.fork as u8,
                blk,
            ))
        }),
        Want::Size => touched(LayerKey::relsize(req.spc, req.db, req.rel, req.fork as u8)),
    };
    if hit {
        ParkCause::Touched
    } else {
        ParkCause::Untouched
    }
}

/// Whether a request can be served now.
///
/// `since` is the block's own position and answers this on its own
/// when the backend knew one. The blocks in hand were last written
/// there, so ingest being past it means the wal that has not been
/// applied yet cannot contain a change to any of them, and the page it
/// would build now is the page it would build after applying the rest.
/// This is the whole point of carrying two positions: the other one is
/// a tenant wide watermark, and waiting for it is waiting for somebody
/// else's writes (zou #697).
///
/// Without one, an lsn names the durability watermark the reader
/// already saw, so it is covered once the stream bytes reached it.
/// Covered means the bytes, not `applied`: a watermark is usually a
/// WAL page boundary with a record spilling over it, and any complete
/// record ending at or below the watermark has been parsed once its
/// bytes are in, so the page state at `applied` is the page state at
/// the watermark.
///
/// Zero asks for the latest durable state instead, which is a question
/// nobody can answer before a walk has reached the head of the stream.
/// Answering it anyway hands back whatever ingest happens to hold, and
/// for a service that has only just anchored that is the anchor. That
/// is the first read a restarted node makes, from the startup process,
/// and replaying a record onto a page missing everything since the
/// anchor is a panic rather than an error (zou #329). So a zero waits
/// for the probe.
fn covered(lsn: u64, since: u64, seen: u64, durable_seen: u64, probed: bool) -> bool {
    if since != 0 {
        seen >= since
    } else if lsn == 0 {
        probed && seen >= durable_seen
    } else {
        seen >= lsn
    }
}

/// The layer map as a reader sees it.
///
/// The driver is the only writer and it writes between reads rather
/// than during one, so what a reader needs is a pointer it can copy
/// and hold for as long as its read takes. A mutex around an `Arc` is
/// that: the lock is held for a clone, and the map a reader is working
/// from stays alive under it however many times the driver replaces
/// the published one.
type Published = Mutex<Arc<LayerMap>>;

/// The current map, for a caller that is about to read from it.
fn published(map: &Published) -> Arc<LayerMap> {
    Arc::clone(&map.lock().expect("the published map"))
}

/// How many threads serve reads, `ZOU_PAGESERVE_READERS`.
///
/// Zero is the old shape, where the driver serves each ready request
/// itself before it looks at the channel again. That is what the A/B
/// on this is: one binary, one flag.
fn reader_threads() -> usize {
    zou_store::setting::number_or("ZOU_PAGESERVE_READERS", "a whole number of threads", 4usize)
}

/// One read on its way to a reader thread.
///
/// Everything it needs was taken on the driver thread at the moment
/// the request was found covered: the map it plans against, and the
/// records above that map for the keys it names. So a job carries the
/// view the driver had, and that view stays true however far ingest
/// moves on afterwards, because a record is written once at one lsn
/// and a published layer is never rewritten in place.
///
/// A flush landing between the take and the read is therefore not a
/// hole. The records this job holds are drained into a layer the map
/// it holds does not name, so it reads them out of the memtable
/// subset; a reader that goes on to reload the map for a retired
/// layer may then see the same records twice, once out of the new
/// layer and once out of the subset, and both times at the same lsn,
/// which [`zou_store::pageread::LayerReader::reconstruct`] merges on.
struct Job {
    req: GetReq,
    map: Arc<LayerMap>,
    mem: Memtable,
    at: u64,
    queued: Instant,
}

/// The readers' inbox: one queue for the pool rather than a channel
/// each, because a read is anything from a hundred microseconds to a
/// few milliseconds and round robin puts a long one in front of the
/// short one behind it.
struct Inbox {
    /// `None` once the driver has closed it, which is how a reader
    /// blocked on the condvar learns there is nothing more coming.
    jobs: Mutex<Option<VecDeque<Job>>>,
    waiting: Condvar,
}

impl Inbox {
    fn new() -> Inbox {
        Inbox {
            jobs: Mutex::new(Some(VecDeque::new())),
            waiting: Condvar::new(),
        }
    }

    fn push(&self, job: Job) {
        let mut jobs = self.jobs.lock().expect("the inbox");
        match jobs.as_mut() {
            Some(queue) => queue.push_back(job),
            // Closed, which only happens on the way out. Answer it
            // here rather than dropping it, so the client reading the
            // socket gets an error instead of a hang up.
            None => {
                let _ = job.req.reply.send(Err("page service stopping".to_string()));
                return;
            }
        }
        drop(jobs);
        self.waiting.notify_one();
    }

    /// The next read, waiting for one, or `None` once the inbox is
    /// closed and drained.
    fn take(&self) -> Option<Job> {
        let mut jobs = self.jobs.lock().expect("the inbox");
        loop {
            match jobs.as_mut()?.pop_front() {
                Some(job) => return Some(job),
                None => jobs = self.waiting.wait(jobs).expect("the inbox"),
            }
        }
    }

    /// Stop the readers, answering whatever is still in hand.
    fn close(&self) {
        let left = self.jobs.lock().expect("the inbox").take();
        self.waiting.notify_all();
        for job in left.into_iter().flatten() {
            let _ = job.req.reply.send(Err("page service stopping".to_string()));
        }
    }
}

/// Closes the inbox on the way out of the driver, whichever way the
/// driver leaves. Readers are scoped threads and the scope joins them,
/// so a driver that returned without closing would hang there.
struct CloseOnDrop<'a>(&'a Inbox);

impl Drop for CloseOnDrop<'_> {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// One reader thread: take a read, do it, reply, take the next.
fn read_loop(
    inbox: &Inbox,
    service: &PageService,
    store: &dyn CasStore,
    layout: &TenantLayout,
    published_map: &Published,
) {
    while let Some(job) = inbox.take() {
        // How long the handover took, which is the pool being too
        // small for the offered rate when reads are slow and no one
        // read is.
        note_phase(Phase::Queue, job.queued.elapsed());
        let ran = Instant::now();
        serve_reloading(
            service,
            store,
            layout,
            published_map,
            &job.map,
            &job.mem,
            &job.req,
            job.at,
        );
        note_phase(Phase::Read, ran.elapsed());
    }
}

/// The keys one request names, which is what its reader needs out of
/// the memtable and all it needs.
fn keys_of(req: &GetReq) -> Vec<LayerKey> {
    match &req.want {
        Want::Pages(blks) => blks
            .iter()
            .map(|&blk| LayerKey::page(req.spc, req.db, req.rel, req.fork as u8, blk))
            .collect(),
        Want::Size => vec![
            crate::relsize::ForkRef {
                spc: req.spc,
                db: req.db,
                rel: req.rel,
                fork: req.fork as u8,
            }
            .key(),
        ],
    }
}

/// The driver: one thread that owns ingest and hands reads out, so the
/// memtable never needs a lock. Ingest polls the store, requests
/// arrive over the channel, and a request whose lsn is not covered
/// yet waits in `parked` until ingest advances or its deadline hits.
///
/// A covered request is dispatched to the reader pool rather than
/// served here, because serving it is a layer fetch and a redo and the
/// driver owes the next request its dispatch. Dispatching from this
/// thread is what makes that safe for free: the drain and the map
/// reload a flush does both happen here too, so no reader is ever
/// handed the moment in between, where the records are in neither
/// (zou #671).
fn drive(mut cfg: ServerConfig, rx: Receiver<GetReq>, stop: Arc<AtomicBool>) -> Result<(), String> {
    let store = Arc::clone(&cfg.store);
    let media = WalMedia::single(crate::log_store(Arc::clone(&store), &cfg.layout));
    let filter = TeeFilter::Tenant(cfg.tenant);
    let pool = cfg.redo.take().map(RedoPool::new).map(Arc::new);
    let empty_mem = Memtable::new();

    let ingest_cfg = ingest_config(cfg.tenant);
    let mut ingest: Option<ShardIngest> = None;
    let map: Published = Mutex::new(Arc::new(
        LayerMap::new(Vec::new()).expect("an empty map builds"),
    ));
    let mut durable_seen: u64 = 0;
    let mut frozen: Option<String> = None;
    let mut parked: Vec<GetReq> = Vec::new();
    let mut last_poll = Instant::now() - POLL;
    // How long the driver waits between polls, POLL while the stream
    // is moving and doubling towards IDLE_POLL while it is not.
    let mut cadence = POLL;
    // The same thing for a driver with somebody parked on it, which
    // starts a millisecond after the park and doubles to POLL.
    let mut parked_cadence = PARKED_POLL;
    let mut cursor = CatchUpCursor::default();
    let mut behind = false;
    // Whether a walk has ever reached the head of the stream. Until
    // one has, `durable_seen` is not a low estimate of the durable
    // end, it is no estimate at all, and a request that asks for the
    // latest durable state has to wait rather than take the anchor.
    let mut probed = false;
    let mut progress = Progress::default();
    // The watermark and the last time it moved. A parked request is
    // failed on a stalled ingest, not on a slow one: a service that is
    // still applying frames is on its way to the lsn the request is
    // waiting for, and the reader is often the recovery that dies of
    // the error (zou #336).
    let mut watermark: u64 = 0;
    let mut advanced = Instant::now();

    match PageShardManifest::load(&*store, &cfg.layout.shard_manifest(0)) {
        Ok(Some((manifest, _))) => {
            *map.lock().expect("the published map") =
                Arc::new(manifest.layer_map().map_err(|e| e.to_string())?);
            let at = manifest.disk_consistent_lsn.0;
            log::info!("zou pageserve: anchored at {at:#x} from the shard manifest");
            ingest = Some(ShardIngest::new(ingest_cfg.clone(), at));
        }
        Ok(None) => {}
        Err(e) => return Err(format!("shard manifest: {e}")),
    }

    // The fold runs next to the driver rather than in it, and dies
    // with it whichever way the driver leaves.
    let folding = Arc::new(AtomicBool::new(false));
    let _fold_stops = StopOnDrop(Arc::clone(&folding));
    if let Some(pool) = &pool {
        let store = Arc::clone(&store);
        let pool = Arc::clone(pool);
        let done = Arc::clone(&folding);
        let layout = cfg.layout.clone();
        let data_checksums = cfg.data_checksums;
        std::thread::Builder::new()
            .name("zou-fold".into())
            .spawn(move || fold_loop(store, pool, done, layout, data_checksums))
            .map_err(|e| format!("fold thread: {e}"))?;
    }

    // One service for the life of the driver. Every read plans against
    // layer footers, and a footer is megabytes on a layer of any size,
    // so rebuilding the service per request meant refetching them per
    // request: 34 GB of range reads for 18466 pages in one segment of
    // the gamingpc smoke, 2.4 MB a page (zou #338).
    let service = page_service(&*store, &cfg.layout, pool.as_deref(), cfg.data_checksums);
    let mut footers_held = 0;

    // The readers, and the inbox the driver hands them work on.
    // They are scoped rather than detached because everything they
    // read through is borrowed from this frame: the service and its
    // footer cache, the store behind it, and the published map.
    let inbox = Inbox::new();
    let readers = cfg.readers.unwrap_or_else(reader_threads);
    std::thread::scope(|scope| {
        let _close = CloseOnDrop(&inbox);
        for i in 0..readers {
            let (inbox, service, store, layout, map) =
                (&inbox, &service, &*store, &cfg.layout, &map);
            std::thread::Builder::new()
                .name(format!("zou-pageread-{i}"))
                .spawn_scoped(scope, move || read_loop(inbox, service, store, layout, map))
                .map_err(|e| format!("reader thread: {e}"))?;
        }
        loop {
            // A poll that stopped on its slice has more waiting, so go
            // straight back to it after the readers have had their turn
            // rather than idling out the rest of the cadence. A reader
            // waiting on an lsn holds the cadence down; only a service
            // with nobody waiting and nothing arriving relaxes it.
            let due = if parked.is_empty() {
                cadence
            } else {
                parked_cadence
            };
            if frozen.is_none() && (behind || last_poll.elapsed() >= due) {
                let was = (ingest.as_ref().map_or(0, ShardIngest::seen), durable_seen);
                last_poll = Instant::now();
                let polled = Instant::now();
                let outcome = poll_ingest(
                    &store,
                    &cfg.layout,
                    &media,
                    &filter,
                    &ingest_cfg,
                    &mut ingest,
                    &map,
                    &mut durable_seen,
                    &mut cursor,
                    &mut progress,
                    polled + INGEST_SLICE,
                );
                // The serve loop is one thread, so this poll is latency
                // every request behind it pays. Sample it whether or not
                // there was anything to apply.
                note_phase(Phase::Ingest, polled.elapsed());
                progress.report(
                    ingest.as_ref().map_or(0, ShardIngest::applied),
                    ingest.as_ref().map_or(0, ShardIngest::seen),
                    durable_seen,
                    &cursor,
                );
                let moved = (ingest.as_ref().map_or(0, ShardIngest::seen), durable_seen) != was;
                match outcome {
                    Ok(caught_up) => {
                        behind = !caught_up;
                        probed |= caught_up;
                    }
                    Err(e) => {
                        // A hole in the stream would poison every later
                        // delta; freeze and let waits fail loudly instead.
                        log::error!("zou pageserve: ingest frozen: {e}");
                        frozen = Some(e);
                    }
                }
                cadence = relax(cadence, moved || behind);
                parked_cadence = relax_parked(parked_cadence, moved || behind);
            }

            // The wait on the channel is the loop's own floor, so it comes
            // down with the cadence: a millisecond poll inside a ten
            // millisecond sleep is a ten millisecond poll. It follows the
            // climb back up as well, so a park that is going to be a long
            // one is not a thousand wakeups a second for the length of it.
            let idle = if parked.is_empty() {
                CHANNEL_WAIT
            } else {
                parked_cadence.min(CHANNEL_WAIT)
            };
            match rx.recv_timeout(idle) {
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
            if seen > watermark {
                watermark = seen;
                advanced = now;
            }
            // An idle service has a watermark that has not moved in hours,
            // so the request's own deadline is the floor: every request
            // gets its wait cap, and one waiting on a moving watermark
            // gets as long as the catch up takes.
            let stalled = now.duration_since(advanced);
            let mut ready: Vec<GetReq> = Vec::new();
            let mut still: Vec<GetReq> = Vec::new();
            for mut req in parked.drain(..) {
                let need = match (req.since, req.lsn) {
                    (0, 0) => durable_seen,
                    (0, lsn) => lsn,
                    (since, _) => since,
                };
                if covered(req.lsn, req.since, seen, durable_seen, probed) {
                    ready.push(req);
                } else if hopeless(now >= req.deadline, stalled, frozen.is_some()) {
                    let secs = stalled.as_secs();
                    let msg = match &frozen {
                        Some(e) => format!("ingest frozen: {e}"),
                        None if req.lsn == 0 && !probed => {
                            format!(
                                "ingest has not reached the head of the stream in {secs} seconds"
                            )
                        }
                        None => format!(
                            "ingest saw {seen:#x} but never reached {need:#x}, and has not moved for {secs} seconds"
                        ),
                    };
                    let _ = req.reply.send(Err(msg));
                } else {
                    if req.parked_at.is_none() {
                        note_park_gap(need.saturating_sub(seen));
                        req.parked_at = Some(ParkedAt {
                            seen,
                            flushes: FLUSHES.load(Ordering::Relaxed),
                        });
                    }
                    still.push(req);
                }
            }
            parked = still;
            // Nobody waiting is where the next park starts from, so the
            // climb a previous one made is not inherited by a reader that
            // arrives an hour later.
            if parked.is_empty() {
                parked_cadence = PARKED_POLL;
            }
            for req in ready {
                let at = if applied == 0 { u64::MAX } else { applied };
                // Two samples, because they answer different questions:
                // parked is how long ingest kept the reader waiting, read
                // is what planning and reading the page actually cost.
                if let Some(from) = req.parked_at {
                    note_park_cause(park_cause(mem, &req, from, seen));
                }
                note_phase(Phase::Park, req.arrived.elapsed());
                if readers == 0 {
                    let ran = Instant::now();
                    let current = published(&map);
                    serve_reloading(
                        &service,
                        &*store,
                        &cfg.layout,
                        &map,
                        &current,
                        mem,
                        &req,
                        at,
                    );
                    note_phase(Phase::Read, ran.elapsed());
                    continue;
                }
                // The map and the records above it, taken together on
                // this thread, which is what makes them a view rather
                // than two halves of one. The ceiling is the position
                // the read will be served at, so the subset is a page's
                // worth of records and not a copy of the table.
                let mem = mem.subset(&keys_of(&req), Lsn(at));
                inbox.push(Job {
                    req,
                    map: published(&map),
                    mem,
                    at,
                    queued: Instant::now(),
                });
            }

            // Flush and compaction retire layers under the reader. Holding
            // their footers is holding a bloom filter per retired layer,
            // so let them go once the map has stopped naming them.
            let held = service.forget_unnamed(&published(&map));
            if held != footers_held {
                footers_held = held;
                log::debug!("zou pageserve: {held} layer footers cached");
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
                    FLUSHES.fetch_add(1, Ordering::Relaxed);
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
    })
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

/// How often the driver says where ingest is. Long enough to be quiet
/// on an idle node, short enough that a service which stops applying
/// is visible in the log within a minute instead of being reconstructed
/// afterwards from a store on disk (zou #324).
const PROGRESS_EVERY: Duration = Duration::from_secs(30);

/// The periodic ingest progress line. Everything chasing #324 needed
/// and had to get from an offline reader instead: both watermarks,
/// where the cursor sits, and how much one interval of polling
/// actually read.
#[derive(Default)]
struct Progress {
    last: Option<Instant>,
    applied: u64,
    polls: u64,
    rounds: u64,
    chunks: u64,
    segments: u64,
    frames: u64,
    bytes: u64,
    polling: Duration,
    applying: Duration,
}

impl Progress {
    /// Fold in what one catch up covered, and how its time split
    /// between the store reads and the apply.
    fn poll(&mut self, out: &CatchUp, polling: Duration, applying: Duration, bytes: u64) {
        self.polls += 1;
        self.rounds += u64::from(out.rounds);
        self.chunks += u64::from(out.chunks);
        self.segments += u64::from(out.segments);
        self.frames += out.frames;
        self.bytes += bytes;
        self.polling += polling;
        self.applying += applying;
    }

    /// Log the interval if it is due, and start a new one.
    fn report(&mut self, applied: u64, seen: u64, durable: u64, cursor: &CatchUpCursor) {
        let now = Instant::now();
        let since = *self.last.get_or_insert(now);
        let elapsed = now.duration_since(since);
        if elapsed < PROGRESS_EVERY {
            return;
        }
        let round = cursor.round.unwrap_or(0);
        let chunk = cursor.chunk;
        let seq = cursor.chain.map_or(0, |c| c.seq);
        // A poll that reads and applies nothing is the healthy idle
        // case. A poll that reads frames without the watermark moving
        // is the bug, so say which one this is.
        let stuck = if applied == self.applied && self.frames > 0 {
            ", applied has not moved"
        } else {
            ""
        };
        log::info!(
            "zou pageserve: applied {applied:#x} seen {seen:#x} durable {durable:#x}, cursor round {round} chunk {chunk} seq {seq}, {} polls read {} rounds {} chunks {} segments {} frames {} MB in {:.0}s, {:.1}s polling of which {:.1}s applying{stuck}",
            self.polls,
            self.rounds,
            self.chunks,
            self.segments,
            self.frames,
            self.bytes >> 20,
            elapsed.as_secs_f64(),
            self.polling.as_secs_f64(),
            self.applying.as_secs_f64(),
        );
        self.applied = applied;
        self.polls = 0;
        self.rounds = 0;
        self.chunks = 0;
        self.segments = 0;
        self.frames = 0;
        self.bytes = 0;
        self.polling = Duration::ZERO;
        self.applying = Duration::ZERO;
        self.last = Some(now);
    }
}

/// One ingest poll: catch up to the stream, anchor a fresh shard at
/// the oldest retained frame, refresh the durable end from what the
/// walk saw, and flush when a threshold says so.
#[allow(clippy::too_many_arguments)]
fn poll_ingest(
    store: &Arc<dyn CasStore>,
    layout: &TenantLayout,
    media: &WalMedia,
    filter: &TeeFilter,
    ingest_cfg: &IngestConfig,
    ingest: &mut Option<ShardIngest>,
    map: &Published,
    durable_seen: &mut u64,
    cursor: &mut CatchUpCursor,
    progress: &mut Progress,
    deadline: Instant,
) -> Result<bool, String> {
    let applied = ingest.as_ref().map_or(0, ShardIngest::applied);
    // Streamed rather than collected, and flushed inside the replay. A
    // service that has fallen behind a bulk load has a whole index
    // build of wal waiting for it, and reading that into a Vec before
    // applying any of it holds the backlog twice: once as frames, once
    // as a memtable that gets no flush check until the last frame
    // lands. At scale 1000 the worker went 610 MB, 810 MB, 1.1 GB,
    // 1.5 GB a layer and the kernel killed it at 6.8 GB resident.
    // Flushing here bounds the memtable by its own threshold instead
    // of by how far behind the service happens to be.
    let seen = *durable_seen;
    // The poll splits in two: the store reads that bring frames in, and
    // the apply that puts them in the memtable. A catch up that runs at
    // a megabyte a second is a different bug depending on which half it
    // spends its time in, and from outside the two are one number.
    let started = Instant::now();
    let mut applying = Duration::ZERO;
    let mut bytes: u64 = 0;
    let out = catch_up_resuming::<ReplayError, _, _>(
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
            bytes += frame.payload.len() as u64;
            let at = Instant::now();
            ingest
                .apply_frames(std::slice::from_ref(&frame))
                .map_err(|e| ReplayError::Ingest(e.to_string()))?;
            flush_if_due(store, layout, ingest, map, seen).map_err(ReplayError::Ingest)?;
            applying += at.elapsed();
            Ok(Instant::now() < deadline)
        },
        || Instant::now() < deadline,
    )
    .map_err(|e| format!("catch up: {e}"))?;
    progress.poll(&out, started.elapsed(), applying, bytes);
    // The walk is the freshness probe, which is why there is no
    // separate stream end call here any more. That one ran first on
    // every poll and read the whole landing tail into memory to look
    // at one number per frame, uncursored and outside the slice: at a
    // few thousand segments a minute it becomes the poll, and the
    // watermark stops moving (zou #324). A walk that reached the head
    // has seen the highest end lsn this tenant has durable, so that is
    // the durable end. A walk that stopped on its slice only saw a
    // prefix, and its end is a lower bound, so the watermark from the
    // last complete walk stands.
    if let Some(end) = out.end.filter(|_| out.caught_up) {
        *durable_seen = end.0.max(*durable_seen);
    }
    if let Some(ingest) = ingest.as_mut() {
        flush_if_due(store, layout, ingest, map, *durable_seen)?;
    }
    Ok(out.caught_up)
}

/// Publish the memtable if a threshold says so and pick the new layer
/// up into `map`, which is what serves it back. Nothing due is not an
/// error and not a flush.
fn flush_if_due(
    store: &Arc<dyn CasStore>,
    layout: &TenantLayout,
    ingest: &mut ShardIngest,
    map: &Published,
    durable_seen: u64,
) -> Result<(), String> {
    if ingest.flush_due(durable_seen).is_none() {
        return Ok(());
    }
    let entry = ingest
        .flush(&**store, layout)
        .map_err(|e| format!("flush: {e}"))?;
    if let Some(entry) = entry {
        FLUSHES.fetch_add(1, Ordering::Relaxed);
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

/// Publish the shard manifest as the map reads plan against. False
/// means there is no manifest yet, a shard that has never flushed,
/// which is not a failure.
///
/// The reader that finds a retired layer calls this too, so the lock
/// is held for the swap and not for the load: two threads that both
/// missed publish the same manifest one after the other, which is a
/// wasted load and not a wrong map.
fn reload_map(
    store: &dyn CasStore,
    layout: &TenantLayout,
    map: &Published,
) -> Result<bool, String> {
    match PageShardManifest::load(store, &layout.shard_manifest(0)) {
        Ok(Some((manifest, _))) => {
            let fresh = manifest.layer_map().map_err(|e| e.to_string())?;
            *map.lock().expect("the published map") = Arc::new(fresh);
            Ok(true)
        }
        Ok(None) => Ok(false),
        Err(e) => Err(format!("manifest reload: {e}")),
    }
}

/// Sets a flag when it goes out of scope, whichever way the scope
/// ends. The fold thread outlives the driver's loop otherwise. The
/// driver returns as soon as the server's stop flag is set, so this
/// one flag ends the fold on both the ordered stop and the abrupt
/// ones, the channel hanging up and a panic on the way out.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// The half of compaction that needs a redo pool: cut a fresh image
/// when the delta bytes above the newest image say reads have got
/// expensive (zou #340).
///
/// The sweep a `zou compact` worker runs merges deltas and separates
/// keyspaces, but it has no pool and no way to know whether the
/// cluster it is folding for runs with data checksums, so it cannot
/// materialize a page. The page service has both, which is why this
/// runs here. It runs on its own thread: a fold is minutes of redo on
/// a big shard and the driver loop owes its reads a hundred
/// milliseconds.
///
/// Everything it does commits with one CAS, so being killed anywhere
/// leaves orphan objects for gc and nothing else. A flush that lands
/// first makes the swap retry.
fn fold_loop(
    store: Arc<dyn CasStore>,
    pool: Arc<RedoPool>,
    stop: Arc<AtomicBool>,
    layout: TenantLayout,
    data_checksums: bool,
) {
    let tenant_ref = layout.tenant_ref().to_string();
    let retention = zou_store::setting::number_or(
        RETENTION,
        "a whole number of seconds",
        crate::gc::DEFAULT_RETENTION_SECS,
    );
    let cadence = merge_every(retention);
    match cadence {
        Some(every) => log::info!(
            "zou pageserve: keeping {retention}s of page history, merging every {}s",
            every.as_secs(),
        ),
        None => log::info!(
            "zou pageserve: {RETENTION} is zero, so page history is kept and never retired"
        ),
    }
    let mut due = Instant::now() + FOLD_EVERY;
    let mut merge_due = cadence.map(|every| Instant::now() + every);
    loop {
        std::thread::sleep(Duration::from_millis(200));
        if stop.load(Ordering::Acquire) {
            return;
        }
        if merge_due.is_some_and(|at| Instant::now() >= at) {
            merge_pass(&*store, &tenant_ref, &pool, data_checksums, retention);
            merge_due = cadence.map(|every| Instant::now() + every);
            // A merge has just rewritten everything a fold would have
            // looked at, so the debt it would read is the debt this
            // pass left behind rather than one it has not seen.
            due = Instant::now() + FOLD_EVERY;
            continue;
        }
        if Instant::now() < due {
            continue;
        }
        let (debt, image) = match fold_debt(&*store, &layout, 0) {
            Ok(pair) => pair,
            Err(e) => {
                log::warn!("zou pageserve: fold: {e}");
                due = Instant::now() + FOLD_EVERY;
                continue;
            }
        };
        if !worth_folding(debt) {
            due = Instant::now() + FOLD_EVERY;
            continue;
        }
        log::info!(
            "zou pageserve: folding, reads are walking {} MB of delta above a {} MB image",
            debt >> 20,
            image >> 20
        );
        let started = Instant::now();
        match crate::compact::compact_shard(&*store, &tenant_ref, 0, Some(&pool), data_checksums) {
            Ok(Some(out)) => log::info!(
                "zou pageserve: folded {} layers into {} in {:.0}s, debt {} MB to {} MB, imaged {} pages of which {} off the frozen objects",
                out.retired,
                out.outputs,
                started.elapsed().as_secs_f64(),
                out.debt_before >> 20,
                out.debt_after >> 20,
                out.imaged,
                out.frozen,
            ),
            Ok(None) => log::info!("zou pageserve: fold found nothing to do"),
            Err(e) => log::warn!("zou pageserve: fold: {e}"),
        }
        // From the end of the work, not the start of it: a fold that
        // took longer than the cadence has already had its turn.
        due = Instant::now() + FOLD_EVERY;
    }
}

/// One merge fold: work out where the horizon is right now and buy it.
///
/// The horizon is not the caller's to choose. A branch or a restore
/// names an old lsn through a checkpoint, and a point in time restore
/// names one through a history snapshot gc has not expired, so
/// [`crate::compact::horizon_for`] takes the oldest lsn anything still
/// pins under the same retention window gc uses and that is the
/// ceiling. Retiring above it would leave the operation that named it
/// reading half a chain.
///
/// Every failure here is a warning and a return. The store is exactly
/// as correct after a merge that did not happen as before it, the only
/// cost is disk, and taking a page service down over a pass that could
/// not read a manifest would trade a bill for an outage.
fn merge_pass(
    store: &dyn CasStore,
    tenant_ref: &str,
    pool: &RedoPool,
    data_checksums: bool,
    retention_secs: u64,
) {
    let now_unix = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(since) => since.as_secs(),
        Err(e) => {
            log::warn!("zou pageserve: merge: the clock is before the epoch: {e}");
            return;
        }
    };
    let at = match crate::compact::horizon_for(store, tenant_ref, now_unix, retention_secs) {
        Ok(at) => at,
        Err(e) => {
            log::warn!("zou pageserve: merge: horizon: {e}");
            return;
        }
    };
    let started = Instant::now();
    match crate::compact::merge_to_horizon(store, tenant_ref, 0, at, pool, data_checksums) {
        Ok(Some(out)) => log::info!(
            "zou pageserve: merged to {}, retired {} layers into {} in {:.0}s, {} MB to {} MB, imaged {} pages, left {} keys nobody could base in {} layers",
            out.horizon,
            out.retired,
            out.outputs,
            started.elapsed().as_secs_f64(),
            out.bytes_before >> 20,
            out.bytes_after >> 20,
            out.imaged,
            out.unbased,
            out.pinned,
        ),
        Ok(None) => log::debug!("zou pageserve: merge found nothing below the horizon to retire"),
        Err(e) => log::warn!("zou pageserve: merge: {e}"),
    }
}

/// What the fold decides on: the delta bytes piled above the newest
/// image of `shard`, which is what every read there walks, and the
/// bytes of that image, which is what a fold would rewrite. A shard
/// with no manifest of its own has neither yet.
fn fold_debt(
    store: &dyn CasStore,
    layout: &TenantLayout,
    shard: u16,
) -> Result<(u64, u64), String> {
    let Some((manifest, _)) =
        PageShardManifest::load(store, &layout.shard_manifest(shard)).map_err(|e| e.to_string())?
    else {
        return Ok((0, 0));
    };
    let map = manifest.layer_map().map_err(|e| e.to_string())?;
    let descs = map.layers();
    let floor = descs
        .iter()
        .filter(|d| d.kind == LayerKind::Image)
        .map(|d| d.min_lsn)
        .max();
    let image = match floor {
        Some(at) => descs
            .iter()
            .filter(|d| d.kind == LayerKind::Image && d.min_lsn == at)
            .map(|d| d.size)
            .sum(),
        None => 0,
    };
    Ok((crate::compact::debt(descs), image))
}

/// Whether a shard owing `debt` bytes of delta has earned a fold. The
/// debt alone decides it: a pass costs what it reads, and since zou
/// #356 what it reads is the debt, not the shard. The image underneath
/// used to be rewritten every pass, so a fat one had to buy itself
/// patience by demanding a debt half its size before it would move;
/// now the fold leaves it where it is, and waiting only means reads
/// keep walking delta for no reason.
fn worth_folding(debt: u64) -> bool {
    debt >= FOLD_DEBT_FLOOR
}

/// The service the driver serves every request through, built once so
/// the footer cache survives the request that filled it (zou #338).
///
/// The fallback reads the pg/ image of a block the layers do not
/// cover, the objects frozen at the put elision flag day.
///
/// It attaches by tenant and shard rather than by prefix, because a
/// branch's layer list names its parent's layers and a reader opened
/// on a bare prefix refuses them: it has no way to know whose prefix
/// to fetch an inherited layer from. Every read on a branched tenant
/// went that way, which is a database that comes up and then cannot
/// answer for a single catalog it did not write itself.
fn page_service<'a>(
    store: &'a dyn CasStore,
    layout: &'a TenantLayout,
    pool: Option<&'a RedoPool>,
    data_checksums: bool,
) -> PageService<'a> {
    PageService::for_shard(store, layout.tenant_ref(), 0, pool, data_checksums).with_base_fallback(
        move |blk: &BlockRef| match store
            .get(&layout.pg_block(blk.spc, blk.db, blk.rel, blk.fork, blk.blk))
        {
            Ok(Some((data, _))) if data.len() == BLCKSZ => Some(data),
            _ => None,
        },
    )
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
#[allow(clippy::too_many_arguments)]
fn serve_reloading(
    service: &PageService,
    store: &dyn CasStore,
    layout: &TenantLayout,
    published_map: &Published,
    map: &LayerMap,
    mem: &Memtable,
    req: &GetReq,
    at: u64,
) {
    let Served::Stale { layer } = serve(service, map, mem, req, at, false) else {
        return;
    };
    log::info!("zou pageserve: layer {layer} is gone, reloading the map and reading again");
    if let Err(e) = reload_map(store, layout, published_map) {
        log::warn!("zou pageserve: {e}");
    }
    // Against the published map rather than the one this read was
    // handed, since the point of the reload is to get past a name the
    // handed one still carries. The records come from the same subset
    // either way: a flush that put them in a layer this map does name
    // wrote them at the lsn they already have, and reconstruct merges
    // on lsn.
    serve(service, &published(published_map), mem, req, at, true);
}

/// What one serve attempt did: replied, or found the map naming a
/// layer the store does not have and left the request unanswered for
/// the caller to retry.
enum Served {
    Done,
    Stale { layer: String },
}

/// Serve one request at `at` and reply on its channel.
///
/// `last` says whether a missing layer is the answer. On the first
/// attempt it is not, the request goes back unanswered so the caller
/// can reload the map; on the retry it is.
fn serve(
    service: &PageService,
    map: &LayerMap,
    mem: &Memtable,
    req: &GetReq,
    at: u64,
    last: bool,
) -> Served {
    let got = match &req.want {
        Want::Pages(blks) => {
            let refs: Vec<BlockRef> = blks
                .iter()
                .map(|&blk| BlockRef {
                    spc: req.spc,
                    db: req.db,
                    rel: req.rel,
                    fork: req.fork,
                    blk,
                })
                .collect();
            service.get_pages(map, mem, &refs, at).map(Answer::Pages)
        }
        Want::Size => {
            let fork = crate::relsize::ForkRef {
                spc: req.spc,
                db: req.db,
                rel: req.rel,
                fork: req.fork as u8,
            };
            service.rel_size(map, mem, fork, at).map(Answer::Size)
        }
    };
    let answer = match got {
        Ok(answer) => Ok(answer),
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
        server_with(store, sock, None)
    }

    fn server_with(store: Arc<dyn CasStore>, sock: PathBuf, readers: Option<usize>) -> PageServer {
        spawn(ServerConfig {
            store,
            layout: TenantLayout::new("t"),
            tenant: TENANT,
            socket: sock,
            data_checksums: false,
            redo: None,
            readers,
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
            .get_pages(1663, 5, 2000, 0, &[3, 4], 0, 0)
            .expect("served");
        assert_eq!(pages[0], page, "the frozen image came back whole");
        assert_eq!(pages[1], vec![0u8; BLCKSZ], "an absent block is zeros");
        srv.stop();
    }

    /// The pool is an A/B on one binary: `ZOU_PAGESERVE_READERS=0`
    /// leaves the driver serving each read itself, which is the shape
    /// everything before #671 had. Both answer the same, which is what
    /// makes the flag a comparison rather than two servers. Several
    /// clients at once, because one at a time never has two reads in
    /// the pool and would say nothing about the handover.
    #[test]
    fn a_read_answers_the_same_whether_the_driver_serves_it_or_a_reader_does() {
        for readers in [0usize, 1, 4] {
            let store: Arc<dyn CasStore> = Arc::new(MemStore::default());
            let layout = TenantLayout::new("t");
            let page = vec![7u8; BLCKSZ];
            store
                .put(&layout.pg_block(1663, 5, 2000, 0, 3), &page)
                .expect("seed page");
            let sock = sock_path(&format!("readers-{readers}.sock"));
            let mut srv = server_with(Arc::clone(&store), sock.clone(), Some(readers));

            std::thread::scope(|scope| {
                for _ in 0..4 {
                    let sock = sock.clone();
                    let page = &page;
                    scope.spawn(move || {
                        let client = PageClient::new(sock);
                        for _ in 0..8 {
                            let pages = client
                                .get_pages(1663, 5, 2000, 0, &[3, 4], 0, 0)
                                .expect("served");
                            assert_eq!(pages[0], *page, "{readers} readers: the frozen image");
                            assert_eq!(pages[1], vec![0u8; BLCKSZ], "{readers} readers: zeros");
                        }
                    });
                }
            });
            srv.stop();
        }
    }

    /// The other request shape: a block count of zero asks how long
    /// the fork is, and the answer folds the relsize keys the layers
    /// carry, the same ones compaction images.
    #[test]
    fn a_size_comes_back_over_the_socket() {
        use zou_store::layer::{ImageBuilder, LayerKey};
        use zou_store::layermap::LayerDesc;
        use zou_store::shardmanifest::{LayerEntry, publish_layer};

        let store: Arc<dyn CasStore> = Arc::new(MemStore::default());
        let layout = TenantLayout::new("t");
        let mut images = ImageBuilder::new(Lsn(100), 8192);
        let mut sized = vec![0u8; BLCKSZ];
        sized[..crate::relsize::REC_LEN]
            .copy_from_slice(&crate::relsize::SizeRec::Set(12).encode());
        images
            .push(LayerKey::page(1663, 5, 2000, 0, 3), &vec![7u8; BLCKSZ])
            .expect("image pushes");
        images
            .push(LayerKey::relsize(1663, 5, 2000, 0), &sized)
            .expect("image pushes");
        let (bytes, footer) = images.finish().expect("image layer builds");
        let desc = LayerDesc::from_footer(&footer, bytes.len() as u64);
        store
            .put(
                &format!("{}{}", layout.shard_prefix(0), desc.name()),
                &bytes,
            )
            .expect("layer lands");
        publish_layer(
            &*store,
            &layout.shard_manifest(0),
            0,
            &LayerEntry {
                name: desc.name(),
                size: bytes.len() as u64,
                owner: None,
                upto: None,
            },
            footer.max_lsn,
        )
        .expect("published");

        let sock = sock_path("size.sock");
        let mut srv = server(Arc::clone(&store), sock.clone());
        let client = PageClient::new(sock);
        assert_eq!(
            client.get_size(1663, 5, 2000, 0, 0).expect("served"),
            Some(12),
            "the imaged length is what the fork is"
        );
        assert_eq!(
            client.get_size(1663, 5, 2000, 1, 0).expect("served"),
            None,
            "a fork the layers hold nothing about is silence, not zero"
        );
        // The connection is still in protocol afterwards, which is
        // the thing a shared status word could have broken.
        let pages = client
            .get_pages(1663, 5, 2000, 0, &[3], 0, 0)
            .expect("served");
        assert_eq!(pages[0], vec![7u8; BLCKSZ]);
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
            let _ = client.get_pages(1663, 5, 2000, 0, &[0], 1 << 40, 0);
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

    /// The availability half of #336. A node that comes back with the
    /// service a memtable behind the stream has recovery reading pages
    /// at lsns catch up has not got to yet, and failing those reads
    /// kills the startup process and then the node.
    #[test]
    fn a_moving_watermark_holds_a_request_past_its_cap() {
        assert!(
            !hopeless(true, Duration::from_secs(1), false),
            "catching up towards the lsn, the answer is coming"
        );
        assert!(
            hopeless(true, WAIT_CAP, false),
            "a watermark that has gone quiet is not going to reach it"
        );
        assert!(
            !hopeless(false, WAIT_CAP * 10, false),
            "an idle service is quiet, a fresh request still gets its own cap"
        );
        assert!(
            hopeless(true, Duration::ZERO, true),
            "frozen ingest never moves again, so it fails on the deadline"
        );
    }

    /// The idle cost behind #457. A project nobody is using polled the
    /// store ten times a second forever, and the two gets that costs
    /// are the whole store bill of a node that is doing nothing.
    #[test]
    fn an_idle_service_relaxes_its_poll_and_snaps_back() {
        let mut cadence = POLL;
        for _ in 0..20 {
            cadence = relax(cadence, false);
        }
        assert_eq!(cadence, IDLE_POLL, "quiet settles at the ceiling");
        assert_eq!(
            relax(cadence, true),
            POLL,
            "a frame arriving pays back the whole backoff at once"
        );
        assert_eq!(
            relax(POLL, false),
            POLL * 2,
            "doubling, so a fifth of a second of quiet costs a fifth of a second"
        );
    }

    /// The park half of #671. A reader waiting on four kilobytes of
    /// wal used to wait out the idle stream's poll for them, so the
    /// climb starts a millisecond after the park and stops where the
    /// old behaviour began.
    #[test]
    fn a_parked_reader_polls_from_a_millisecond() {
        assert_eq!(
            relax_parked(PARKED_POLL, false),
            PARKED_POLL * 2,
            "the same doubling, from a floor a hundred times lower"
        );
        let mut cadence = PARKED_POLL;
        let mut polls = 0;
        while cadence < POLL {
            cadence = relax_parked(cadence, false);
            polls += 1;
        }
        assert_eq!(cadence, POLL, "the climb stops at the old cadence");
        assert_eq!(polls, 7, "and takes seven looks to get there");
        assert_eq!(
            relax_parked(POLL, true),
            PARKED_POLL,
            "wal arriving puts the next park back on the floor"
        );
    }

    /// The panic behind #329. A service that has just anchored has no
    /// idea where the stream ends, and the first read of a restarted
    /// node asks for the end. Answering that out of the anchor gives
    /// recovery a page missing 99 MB of WAL, which redo turns into
    /// "failed to add tuple" and a dead node.
    #[test]
    fn a_zero_waits_for_the_probe_and_an_lsn_does_not() {
        let anchor = 0x69bf_9a58;
        assert!(
            !covered(0, 0, anchor, 0, false),
            "the latest durable state is not the anchor just because that is all there is"
        );
        assert!(
            covered(0, 0, anchor, anchor, true),
            "a walk that reached the head answers the question"
        );
        assert!(
            !covered(0, 0, anchor, anchor + 1, true),
            "and a walk that found more still holds the read"
        );

        // Recovery names the lsn it is replaying, which is a real
        // barrier on its own and needs no probe behind it: ingest
        // cannot have passed an lsn without walking to it.
        let replay = 0x6f65_a108;
        assert!(
            !covered(replay, 0, anchor, 0, false),
            "the anchor is behind it"
        );
        assert!(covered(replay, 0, replay, 0, false), "ingest reached it");
        assert!(covered(replay, 0, replay + 1, 0, false), "and past it");
    }

    /// The point of #697. The durable watermark moves with every other
    /// backend's commits, so a read that waits for it waits for writes
    /// that cannot touch the blocks it asked for. A block position is
    /// an upper bound on the last change to those blocks, so it is the
    /// only thing worth waiting for, and it outranks both the other
    /// arms.
    #[test]
    fn a_block_position_is_what_the_read_waits_for() {
        let durable = 0x8000_0000;
        let block = 0x4000_0000;

        assert!(
            covered(durable, block, block, 0, false),
            "ingest past the block's own writes answers it"
        );
        assert!(
            !covered(durable, block, block - 1, 0, false),
            "and short of them it does not"
        );
        assert!(
            covered(0, block, block, durable, false),
            "a zero beside a block position needs no probe, the block says enough"
        );

        // The bound runs the other way too: a block written after the
        // watermark the reader saw still waits for its own writes.
        let ahead = durable + 0x1000;
        assert!(
            !covered(durable, ahead, durable, 0, false),
            "the watermark being covered says nothing about this block"
        );
        assert!(covered(durable, ahead, ahead, 0, false), "its own does");
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
        let map: Published = Mutex::new(Arc::new(
            LayerMap::new(Vec::new()).expect("an empty map builds"),
        ));
        let mut durable_seen = 0u64;
        let mut cursor = CatchUpCursor::default();
        let mut progress = Progress::default();
        // A slice long enough that this poll runs the whole backlog,
        // the point of the test being where the flushes land.
        poll_ingest(
            &store,
            &layout,
            &media,
            &TeeFilter::Tenant(TENANT),
            &cfg,
            &mut ingest,
            &map,
            &mut durable_seen,
            &mut cursor,
            &mut progress,
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

    /// Delegates everything and counts the object reads, because the
    /// cost of a poll is the whole point here.
    struct CountingStore {
        inner: Arc<dyn CasStore>,
        gets: std::sync::atomic::AtomicUsize,
        ranges: std::sync::atomic::AtomicUsize,
    }

    impl CountingStore {
        fn new(inner: Arc<dyn CasStore>) -> Self {
            Self {
                inner,
                gets: std::sync::atomic::AtomicUsize::new(0),
                ranges: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn gets(&self) -> usize {
            self.gets.load(Ordering::SeqCst)
        }

        fn ranges(&self) -> usize {
            self.ranges.load(Ordering::SeqCst)
        }
    }

    impl CasStore for CountingStore {
        fn get(
            &self,
            key: &str,
        ) -> Result<Option<(Vec<u8>, zou_store::Version)>, zou_store::CasError> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.inner.get(key)
        }
        fn put_if_match(
            &self,
            key: &str,
            data: &[u8],
            expected: Option<&zou_store::Version>,
        ) -> Result<zou_store::Version, zou_store::CasError> {
            self.inner.put_if_match(key, data, expected)
        }
        fn get_range(
            &self,
            key: &str,
            offset: u64,
            len: u64,
        ) -> Result<Option<Vec<u8>>, zou_store::CasError> {
            self.ranges.fetch_add(1, Ordering::SeqCst);
            self.inner.get_range(key, offset, len)
        }
        fn delete(&self, key: &str) -> Result<(), zou_store::CasError> {
            self.inner.delete(key)
        }
        fn list(&self, prefix: &str) -> Result<Vec<String>, zou_store::CasError> {
            self.inner.list(prefix)
        }
    }

    /// The freeze behind #324. Every poll used to ask for the durable
    /// end of the stream first, and that read the whole landing tail
    /// from the consolidated boundary, uncursored and outside the
    /// slice, to look at one number per frame. The tail past a fold is
    /// everything the pusher has written since, so on a box under a
    /// bulk load the poll became the tail: on server2 and server3 the
    /// applied watermark stopped for hours, with the driver still
    /// polling, no error and no warning, and 56 GB of chain reads to
    /// show for it. A poll that is caught up should cost a handful of
    /// gets whatever the tail behind it looks like.
    #[test]
    fn a_caught_up_poll_does_not_reread_the_tail() {
        let inner: Arc<dyn CasStore> = Arc::new(MemStore::default());
        let store = Arc::new(CountingStore::new(inner));
        let counted: Arc<dyn CasStore> = store.clone();
        let layout = TenantLayout::new("t");
        let media = WalMedia::single(Arc::clone(&counted));
        let (_, end) = seed_chain_segments(&media, 40);

        let cfg = IngestConfig::new(TENANT, 0, 1);
        let mut ingest = None;
        let map: Published = Mutex::new(Arc::new(
            LayerMap::new(Vec::new()).expect("an empty map builds"),
        ));
        let mut durable_seen = 0u64;
        let mut cursor = CatchUpCursor::default();
        let mut progress = Progress::default();
        let mut poll = |ingest: &mut _, map: &_, seen: &mut _, cursor: &mut _| {
            poll_ingest(
                &counted,
                &layout,
                &media,
                &TeeFilter::Tenant(TENANT),
                &cfg,
                ingest,
                map,
                seen,
                cursor,
                &mut progress,
                Instant::now() + Duration::from_secs(600),
            )
            .expect("the poll replays the chain")
        };

        assert!(
            poll(&mut ingest, &map, &mut durable_seen, &mut cursor),
            "one long slice catches up"
        );
        assert_eq!(
            ingest.as_ref().expect("anchored").applied(),
            end,
            "the whole chain"
        );
        assert_eq!(durable_seen, end, "the walk is the freshness probe");

        // Nothing has moved: the manifest, the round check and the one
        // miss that says the head is where the cursor left it.
        let before = store.gets();
        assert!(poll(&mut ingest, &map, &mut durable_seen, &mut cursor));
        let cost = store.gets() - before;
        assert!(
            cost <= 4,
            "a caught up poll cost {cost} gets over a 40 segment tail"
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
        let map: Published = Mutex::new(Arc::new(
            LayerMap::new(Vec::new()).expect("an empty map builds"),
        ));
        let mut durable_seen = 0u64;
        let mut cursor = CatchUpCursor::default();
        let mut progress = Progress::default();
        let poll =
            |ingest: &mut _, map: &_, seen: &mut _, cursor: &mut _, progress: &mut _, deadline| {
                poll_ingest(
                    &store,
                    &layout,
                    &media,
                    &TeeFilter::Tenant(TENANT),
                    &cfg,
                    ingest,
                    map,
                    seen,
                    cursor,
                    progress,
                    deadline,
                )
            };

        // A deadline already gone stops on the segment it is in and
        // says it is not caught up. The first segment of a fresh chain
        // carries the epoch marker and no frames, so it takes two of
        // these to get an applied watermark, and two is still nothing
        // like the whole backlog.
        for _ in 0..2 {
            let caught_up = poll(
                &mut ingest,
                &map,
                &mut durable_seen,
                &mut cursor,
                &mut progress,
                Instant::now(),
            )
            .expect("the poll replays what it can");
            assert!(!caught_up, "a poll out of time is not caught up");
        }
        let first = ingest.as_ref().expect("anchored").applied();
        assert!(first > 0 && first < end, "two slices applied {first:#x}");

        // Polling again picks up where it stopped rather than starting
        // over, and enough of them finish the backlog.
        let mut rounds = 0;
        while !poll(
            &mut ingest,
            &map,
            &mut durable_seen,
            &mut cursor,
            &mut progress,
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

    /// [`seed_chain`] with a body the frame codec cannot pack down, so
    /// a fold over it seals more than one chunk for the tenant. A
    /// constant byte compresses to nothing and puts the whole round in
    /// one chunk, which is not the shape this is about.
    fn seed_chain_noisy(media: &WalMedia, records: u32) -> (u64, u64) {
        let mut b = Builder::new(WAL_BASE);
        let mut x = 0x2545_F491_4F6C_DD1Du64;
        for i in 0..records {
            let r = BlockRef {
                spc: 1663,
                db: 5,
                rel: 1000,
                fork: 0,
                blk: i,
            };
            let body: Vec<u8> = (0..4096)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x as u8
                })
                .collect();
            b.record(&[(r, false)], &body);
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

    /// The freeze after a kill drill on server2, #327. A restarted
    /// service comes up behind the sealed rounds rather than behind the
    /// landing tail, and reading a tenant's frames out of a round is a
    /// range GET per chunk. A poll whose slice is spent by that GET
    /// used to hand over the first frame, stop, and leave the cursor on
    /// the round, so the next poll paid for the same chunk again. The
    /// frames were ones the ingest buffer already held, waiting on a
    /// record the next chunk completes, so applied never moved: 40
    /// minutes of polling, hundreds of megabytes a minute of reads, and
    /// the watermark exactly where the restart left it. Every poll has
    /// to end further along than it started.
    #[test]
    fn a_poll_over_sealed_rounds_advances_on_every_slice() {
        let store: Arc<dyn CasStore> = Arc::new(MemStore::default());
        let layout = TenantLayout::new("t");
        let media = WalMedia::single(Arc::clone(&store));
        let (_, end) = seed_chain_noisy(&media, 400);
        zou_log::consolidate(&media, WAL_SHARD)
            .expect("the fold runs")
            .expect("it had something to fold");

        let cfg = IngestConfig::new(TENANT, 0, 1);
        let mut ingest = None;
        let map: Published = Mutex::new(Arc::new(
            LayerMap::new(Vec::new()).expect("an empty map builds"),
        ));
        let mut durable_seen = 0u64;
        let mut cursor = CatchUpCursor::default();
        let mut progress = Progress::default();
        let mut applied = 0;
        let mut polls = 0;
        loop {
            let was = cursor;
            // Spent before it starts, which is what a poll behind a
            // fetch always is.
            let caught_up = poll_ingest(
                &store,
                &layout,
                &media,
                &TeeFilter::Tenant(TENANT),
                &cfg,
                &mut ingest,
                &map,
                &mut durable_seen,
                &mut cursor,
                &mut progress,
                Instant::now(),
            )
            .expect("the poll replays what it can");
            let now = ingest.as_ref().map_or(0, ShardIngest::applied);
            // Either the watermark moved or the cursor did. A poll
            // where neither did is one that read something and threw
            // it away, and every poll after it does the same thing.
            assert!(
                now > applied || cursor != was || caught_up,
                "poll {polls} ended where it started, at {now:#x}"
            );
            applied = now;
            polls += 1;
            assert!(polls < 50, "the slices are not making progress");
            if caught_up {
                break;
            }
        }
        assert!(polls > 2, "a single chunk round is not a test of this");
        assert!(
            applied >= end,
            "the slices stopped at {applied:#x} of {end:#x}"
        );
    }

    /// The cost behind #338. The driver used to build the page service
    /// inside the serve call, so the footer cache lived for exactly one
    /// request and every read planned by refetching the footer of every
    /// layer it touched. A footer is not small: the 350 MB delta layer
    /// the gamingpc smoke ended with carries a 2 MB bloom and an index
    /// row for each of its 3850 blocks, and the segment holding the
    /// death drill paid 34.5 GB of range reads to serve 18466 pages,
    /// 2.4 MB a page.
    ///
    /// Since #465 the block is cached too, so a second read of a page
    /// out of a layer this reader has already read costs nothing at
    /// all on the store.
    #[test]
    fn a_second_read_of_a_layer_refetches_nothing() {
        use zou_store::layer::{ImageBuilder, LayerKey};
        use zou_store::layermap::LayerDesc;
        use zou_store::shardmanifest::{LayerEntry, publish_layer};

        let inner: Arc<dyn CasStore> = Arc::new(MemStore::default());
        let counting = Arc::new(CountingStore::new(inner));
        let store: Arc<dyn CasStore> = counting.clone();
        let layout = TenantLayout::new("t");
        let page = vec![4u8; BLCKSZ];

        let mut images = ImageBuilder::new(Lsn(100), 8192);
        images
            .push(LayerKey::page(1663, 5, 2000, 0, 3), &page)
            .expect("image pushes");
        let (bytes, footer) = images.finish().expect("image layer builds");
        let desc = LayerDesc::from_footer(&footer, bytes.len() as u64);
        store
            .put(
                &format!("{}{}", layout.shard_prefix(0), desc.name()),
                &bytes,
            )
            .expect("layer lands");
        publish_layer(
            &*store,
            &layout.shard_manifest(0),
            0,
            &LayerEntry {
                name: desc.name(),
                size: bytes.len() as u64,
                owner: None,
                upto: None,
            },
            footer.max_lsn,
        )
        .expect("published");

        let sock = sock_path("footers.sock");
        let mut srv = server(Arc::clone(&store), sock.clone());
        let client = PageClient::new(sock);

        assert_eq!(
            client
                .get_pages(1663, 5, 2000, 0, &[3], 0, 0)
                .expect("served")[0],
            page
        );
        let first = counting.ranges();
        assert!(
            first >= 2,
            "the first read fetched the footer, {first} range"
        );

        for _ in 0..4 {
            assert_eq!(
                client
                    .get_pages(1663, 5, 2000, 0, &[3], 0, 0)
                    .expect("served")[0],
                page
            );
        }
        srv.stop();
        assert_eq!(
            counting.ranges() - first,
            0,
            "four more reads of the same page cost {} ranges, the footer and the block are both held",
            counting.ranges() - first
        );
    }

    /// A read is handed its map and its records on the driver thread
    /// and runs on another one, so a flush can land in between: the
    /// records go out of the memtable into a layer, and the map that
    /// names the layer is published, all while the read is on its way.
    /// It reads them out of the subset it was handed. And if it goes
    /// on to reload the map, for a layer retired under it, it sees
    /// them a second time out of the layer the flush wrote, at the
    /// lsn they already had, which is where the two copies merge back
    /// into one (zou #671).
    #[test]
    fn a_read_handed_out_before_a_flush_still_reads_what_the_flush_took() {
        use crate::relsize::{ForkRef, SizeRec};
        use zou_store::layer::build_delta;
        use zou_store::layermap::LayerDesc;
        use zou_store::shardmanifest::{LayerEntry, publish_layer};

        let store: Arc<dyn CasStore> = Arc::new(MemStore::default());
        let layout = TenantLayout::new("t");
        let fork = ForkRef {
            spc: 1663,
            db: 5,
            rel: 2000,
            fork: 0,
        };
        let ask = || {
            let (reply, answers) = sync_channel(1);
            let req = GetReq {
                spc: fork.spc,
                db: fork.db,
                rel: fork.rel,
                fork: fork.fork as u32,
                want: Want::Size,
                lsn: 0,
                since: 0,
                arrived: Instant::now(),
                deadline: Instant::now() + WAIT_CAP,
                parked_at: None,
                reply,
            };
            (req, answers)
        };
        let size = |answers: std::sync::mpsc::Receiver<Result<Answer, String>>| match answers
            .recv()
            .expect("the reader replied")
            .expect("a size")
        {
            Answer::Size(size) => size,
            Answer::Pages(_) => panic!("a size request is answered with a size"),
        };

        let mut mem = Memtable::new();
        for (lsn, blocks) in [(100u64, 10u32), (150, 20)] {
            mem.insert(
                fork.key(),
                Lsn(lsn),
                SizeRec::Grow(blocks).encode().to_vec(),
            );
        }
        let map: Published = Mutex::new(Arc::new(
            LayerMap::new(Vec::new()).expect("an empty map builds"),
        ));

        // What the driver takes for one request, on the thread that
        // owns both: the map it plans against and the records above it
        // for the keys it names.
        let (req, answers) = ask();
        let held = published(&map);
        let taken = mem.subset(&keys_of(&req), Lsn(200));

        // The flush, in full: drained out of the memtable, written as
        // a layer, published as the map every later read plans against.
        let drained = mem.drain_sorted();
        let (bytes, footer) = build_delta(&drained, 8192).expect("a delta layer builds");
        let desc = LayerDesc::from_footer(&footer, bytes.len() as u64);
        store
            .put(
                &format!("{}{}", layout.shard_prefix(0), desc.name()),
                &bytes,
            )
            .expect("the layer lands");
        publish_layer(
            &*store,
            &layout.shard_manifest(0),
            0,
            &LayerEntry {
                name: desc.name(),
                size: bytes.len() as u64,
                owner: None,
                upto: None,
            },
            footer.max_lsn,
        )
        .expect("published");
        reload_map(&*store, &layout, &map).expect("the manifest reads");
        assert!(
            mem.is_empty(),
            "the flush emptied the table it was taken from"
        );

        let service = page_service(&*store, &layout, None, false);
        serve_reloading(&service, &*store, &layout, &map, &held, &taken, &req, 200);
        assert_eq!(
            size(answers),
            Some(20),
            "the read was handed the records before the flush took them"
        );

        // The same records reached this read twice, once out of the
        // layer and once out of the subset. Two copies of one record
        // is one record.
        let (req, answers) = ask();
        let now = published(&map);
        serve(&service, &now, &taken, &req, 200, true);
        assert_eq!(size(answers), Some(20), "the two copies merged on lsn");

        // And a read that arrives after all of it, with nothing in
        // hand, gets the same answer out of the layer alone.
        let (req, answers) = ask();
        serve(&service, &now, &Memtable::new(), &req, 200, true);
        assert_eq!(size(answers), Some(20), "the layer carries them now");
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

        let mut images = ImageBuilder::new(Lsn(100), 8192);
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
        let map: Published = Mutex::new(Arc::new(manifest.layer_map().expect("the map builds")));
        swap_layers(
            &*store,
            &manifest_key,
            0,
            std::slice::from_ref(&gone),
            &[],
            None,
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
            want: Want::Pages(vec![3]),
            lsn: 0,
            since: 0,
            arrived: Instant::now(),
            deadline: Instant::now() + WAIT_CAP,
            parked_at: None,
            reply,
        };
        let service = page_service(&*store, &layout, None, false);
        // The map the reader was handed, taken before the retirement,
        // which is the way a reader holds one.
        let held = published(&map);
        serve_reloading(
            &service,
            &*store,
            &layout,
            &map,
            &held,
            &Memtable::new(),
            &req,
            200,
        );
        let answer = answers
            .recv()
            .expect("the driver replied")
            .expect("the read survived the collected layer");
        let Answer::Pages(pages) = answer else {
            panic!("a page request is answered with pages");
        };
        assert_eq!(pages[0], page, "the image served the page");
        assert!(
            !published(&map).layers().iter().any(|d| d.name() == gone),
            "the map still names the layer that is gone"
        );
    }

    /// A backend asks at the lsn its pusher made durable, so a read of
    /// a quiet page waits behind every write in the tenant. Telling
    /// that wait from a read waiting for its own writes is the whole
    /// point of the counter, so the two cases are checked against the
    /// same memtable, and the case the memtable cannot answer for is
    /// checked to say so rather than to guess.
    #[test]
    fn a_park_says_whether_the_wal_it_waited_for_wrote_the_pages_it_wanted() {
        let mut mem = Memtable::new();
        mem.insert(LayerKey::page(1663, 5, 2000, 0, 7), Lsn(150), vec![1; 8]);
        let ask = |blk: u32| {
            let (reply, _answers) = sync_channel(1);
            GetReq {
                spc: 1663,
                db: 5,
                rel: 2000,
                fork: 0,
                want: Want::Pages(vec![blk]),
                lsn: 200,
                since: 0,
                arrived: Instant::now(),
                deadline: Instant::now() + WAIT_CAP,
                parked_at: None,
                reply,
            }
        };
        let from = ParkedAt {
            seen: 100,
            flushes: FLUSHES.load(Ordering::Relaxed),
        };
        assert_eq!(
            park_cause(&mem, &ask(7), from, 200),
            ParkCause::Touched,
            "the wait was for a write to the page asked for"
        );
        assert_eq!(
            park_cause(&mem, &ask(8), from, 200),
            ParkCause::Untouched,
            "block 8 was never written and still waited"
        );

        // Records at or below where the request parked are not what it
        // waited for, they were already applied when it arrived.
        let early = ParkedAt {
            seen: 150,
            flushes: from.flushes,
        };
        assert_eq!(park_cause(&mem, &ask(7), early, 200), ParkCause::Untouched);

        // And a flush during the wait takes the evidence out of the
        // memtable, which is not the same as there being none.
        let stale = ParkedAt {
            seen: 100,
            flushes: from.flushes.wrapping_sub(1),
        };
        assert_eq!(park_cause(&mem, &ask(7), stale, 200), ParkCause::Unclear);
        assert_eq!(park_cause(&mem, &ask(8), stale, 200), ParkCause::Unclear);
    }

    /// What the fold decides on. Deltas piled on top of an image are
    /// the chain a read walks, so they count; the same deltas under a
    /// newer image are history nobody reads through, so they do not.
    /// A shard the tenant manifest does not name owes nothing.
    #[test]
    fn the_fold_counts_the_delta_a_read_walks_and_nothing_else() {
        use zou_store::layer::{DeltaEntry, ImageBuilder, LayerKey, build_delta};
        use zou_store::layermap::LayerDesc;
        use zou_store::manifest::Manifest;
        use zou_store::shardmanifest::{LayerEntry, publish_layer};

        let store: Arc<dyn CasStore> = Arc::new(MemStore::default());
        let layout = TenantLayout::new("t");
        store
            .put_if_absent(&layout.manifest(), &Manifest::new("t", 18).to_json())
            .expect("tenant manifest lands");
        let manifest_key = layout.shard_manifest(0);
        let key = LayerKey::page(1663, 5, 2000, 0, 3);

        let publish = |bytes: Vec<u8>, footer: &zou_store::layer::LayerFooter| -> u64 {
            let desc = LayerDesc::from_footer(footer, bytes.len() as u64);
            let entry = LayerEntry {
                name: desc.name(),
                size: bytes.len() as u64,
                owner: None,
                upto: None,
            };
            store
                .put(
                    &format!("{}{}", layout.shard_prefix(0), desc.name()),
                    &bytes,
                )
                .expect("layer lands");
            publish_layer(&*store, &manifest_key, 0, &entry, footer.max_lsn).expect("published");
            bytes.len() as u64
        };

        let mut images = ImageBuilder::new(Lsn(100), 8192);
        images.push(key, &vec![9u8; BLCKSZ]).expect("image pushes");
        let (bytes, footer) = images.finish().expect("image layer builds");
        let image_bytes = publish(bytes, &footer);
        assert_eq!(
            fold_debt(&*store, &layout, 0).expect("debt reads"),
            (0, image_bytes),
            "an image alone owes nothing and is the thing a fold would rewrite"
        );

        let (bytes, footer) = build_delta(
            &[DeltaEntry {
                key,
                lsn: Lsn(150),
                record: vec![0x5A; 4096],
            }],
            8192,
        )
        .expect("delta layer builds");
        let delta_bytes = publish(bytes, &footer);
        assert_eq!(
            fold_debt(&*store, &layout, 0).expect("debt reads"),
            (delta_bytes, image_bytes),
            "the delta above the image is what a read walks"
        );

        assert_eq!(
            fold_debt(&*store, &TenantLayout::new("nosuchtenant"), 0).expect("debt reads"),
            (0, 0),
            "a store with no shard manifest yet has nothing to fold"
        );
    }

    /// The gate is the debt and nothing else, because the pass reads
    /// the debt and nothing else. The image it stood on used to be part
    /// of the bill and no longer is (zou #356).
    #[test]
    fn a_fold_waits_for_the_debt_floor_and_nothing_else() {
        assert!(
            !worth_folding(FOLD_DEBT_FLOOR - 1),
            "a short chain is the cheaper read"
        );
        assert!(
            worth_folding(FOLD_DEBT_FLOOR),
            "and then the fold has earned its turn"
        );
        assert!(
            worth_folding(4 * (1u64 << 30)),
            "however big the tenant underneath it is"
        );
    }

    /// The cadence is a fraction of the window, so what a shard holds
    /// over what was asked for is bounded by the fraction rather than
    /// by how long the node has been up.
    #[test]
    fn the_merge_runs_eight_times_a_window_and_never_faster_than_the_fold() {
        let week = 7 * 24 * 60 * 60;
        assert_eq!(
            merge_every(week),
            Some(Duration::from_secs(week / 8)),
            "the default window buys a merge about every twenty one hours"
        );
        assert_eq!(
            merge_every(600),
            Some(Duration::from_secs(75)),
            "and the soak scenario's ten minutes buys one every seventy five seconds"
        );
        assert_eq!(
            merge_every(120),
            Some(FOLD_EVERY),
            "under that the floor holds, since the expensive pass does not \
             run faster than the cheap one"
        );
        assert_eq!(
            merge_every(0),
            None,
            "zero is the escape hatch: keep everything, retire nothing"
        );
    }

    /// The window the pass is given is the window a snapshot is judged
    /// by, which is the whole reason the horizon is not a setting: a
    /// point in time recovery that can still name an old checkpoint
    /// keeps the layers under it, and the same store with a shorter
    /// promise lets them go.
    #[test]
    fn a_merge_leaves_what_a_snapshot_inside_the_window_still_names() {
        use crate::compact::tests::{dead_pool, put_image, seed};
        use zou_store::layer::{ImageEntry, LayerKey, PAGE_IMAGE_LEN};
        use zou_store::manifest::{CheckpointKind, CheckpointRef, Manifest};

        let store = MemStore::default();
        let layout = seed(&store, "t");
        let image = |key, at| {
            put_image(
                &store,
                &layout,
                0,
                &[ImageEntry {
                    key,
                    page: vec![0xAA; PAGE_IMAGE_LEN],
                }],
                at,
            )
        };
        // Two sparse images, which is the pair a merge exists to fold:
        // neither is droppable, each is the only base for its key.
        image(LayerKey::page(1663, 5, 90, 0, 1), 0x100);
        image(LayerKey::page(1663, 5, 90, 0, 2), 0x200);

        // A history snapshot from a hundred seconds ago naming a
        // checkpoint at the older image.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_secs();
        let mut snap = Manifest::new("t", 18);
        snap.checkpoints = vec![CheckpointRef {
            id: "c-1".into(),
            lsn: Lsn(0x100),
            kind: CheckpointKind::Full,
            owner: None,
        }];
        store
            .put(
                &format!("{}0000000001-{}.json", layout.manifests_dir(), now - 100),
                &snap.to_json(),
            )
            .expect("snapshot");

        let pool = dead_pool();
        let horizon = || {
            PageShardManifest::load(&store, &layout.shard_manifest(0))
                .expect("load")
                .expect("a shard that has published")
                .0
                .horizon
        };
        merge_pass(&store, "t", &pool, false, 3600);
        assert_eq!(
            horizon(),
            None,
            "the snapshot is inside the hour and the restore it promises \
             needs the image its checkpoint sits on"
        );

        merge_pass(&store, "t", &pool, false, 10);
        let bought = horizon().expect("the snapshot has fallen out of the window");
        assert!(
            bought > Lsn(0x100) && bought < Lsn(0x200),
            "up to the flush point and a byte under it, {bought}"
        );
    }
}
