//! The sequencer role: group commit for one WAL shard (spec 03 §4, §6).
//!
//! Computes append frames, the sequencer stages them into the open
//! batch, and the batch closes at 3 ms elapsed, 4 MB pending, or 4096
//! frames, whichever comes first. One durable PUT lands the whole
//! window, then every staged append is acked with its durable lsn. The
//! rules the tests hold this to:
//!
//! - Acks only after the durable PUT returns. If the store stalls,
//!   commits stall. There is no timeout that fakes an ack.
//! - Admission is by writer epoch per tenant: a frame with an epoch
//!   below the highest one seen for its tenant is rejected with the
//!   current epoch so the stale compute can self detach.
//! - A failed PUT fails every append staged in that batch and poisons
//!   the sequencer. Landing is fenced by put_if_absent, so a failure
//!   means this node may no longer own the shard, and the honest move
//!   is to step down, not to retry into someone else's chain.
//! - Empty windows PUT nothing. An idle shard costs zero requests.
//!
//! Landing is pipelined: the chain digest links window content, not
//! acks, so window n+1 builds and lands while n is still in flight,
//! up to [`SequencerConfig::inflight`] at once. That takes the commit
//! cycle from the mean PUT latency, which a fat tailed store drags
//! way past its p50, down to the batch window plus one PUT. Acks
//! still resolve strictly in chain order, because a segment behind a
//! hole is unreachable by the digest walk and calling it durable
//! would be a lie. Plain threads and condvars throughout. Frames are
//! encoded on the append path so the flush path only copies bytes.

use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use zou_store::{CasError, Frame2, Lsn};

use crate::segment::{SegmentBuilder, SegmentHeader, SegmentKind, tenants_digest};
use crate::tee::Tee;

/// Where closed batches go to become durable. `put_segment` must return
/// only once the segment survives whatever the sink's durability story
/// is, because the sequencer acks commits on its return. An error is
/// terminal for the shard: with a fenced chain it means another
/// sequencer may own the head now, so the caller poisons itself.
/// Deployments use [`MediaSink`](crate::MediaSink), which lands fenced
/// objects on whatever media the cell's durability mode picked.
pub trait SegmentSink: Send + Sync {
    fn put_segment(&self, seq: u64, segment: &[u8]) -> Result<(), CasError>;
}

/// The hard cap on [`SequencerConfig::inflight`]. Takeover leans on
/// it: a crash leaves stragglers at most this far past the first hole
/// in the chain, so the head search and the straggler sweep both know
/// where to stop looking.
pub const MAX_INFLIGHT: usize = 64;

#[derive(Debug, Clone)]
pub struct SequencerConfig {
    /// Batch window, commit p50 is about half of this plus the PUT p50.
    pub window: Duration,
    /// Close early once this many encoded frame bytes are pending.
    pub batch_bytes: usize,
    /// Close early once this many frames are pending.
    pub batch_frames: usize,
    /// Landing PUTs in flight at once, between 1 and [`MAX_INFLIGHT`].
    /// One is the fully serial chain. More lets the next window land
    /// while its predecessors are still on the wire, which is what
    /// keeps the commit cycle at the window instead of the mean PUT
    /// latency. Acks resolve in chain order no matter the setting.
    pub inflight: usize,
    /// Where durable windows fan out to subscribers (spec 03 section
    /// 8). The tee sees a window only after its PUT returned, and
    /// publishing never blocks or fails the flush: durability does not
    /// depend on it.
    pub tee: Option<Arc<Tee>>,
}

impl Default for SequencerConfig {
    fn default() -> Self {
        Self {
            window: Duration::from_millis(3),
            batch_bytes: 4 * 1024 * 1024,
            batch_frames: 4096,
            inflight: 16,
            tee: None,
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum AppendError {
    /// The tenant has a newer writer. The stale compute self detaches;
    /// nothing from the rejected append was staged.
    #[error("stale writer epoch for tenant {tenant:#x}, current is {current}")]
    WrongEpoch { tenant: u128, current: u32 },
    /// The landing PUT for the batch this append was staged in failed.
    /// Nothing was acked; the compute retries on the successor.
    #[error("landing put failed, the shard needs takeover: {source}")]
    Store {
        #[source]
        source: Arc<CasError>,
    },
    /// An earlier batch already failed and this sequencer stepped down.
    #[error("the sequencer is poisoned by an earlier landing failure")]
    Poisoned,
    /// The sequencer was shut down before this append was staged.
    #[error("the sequencer is closed")]
    Closed,
}

struct TicketInner {
    done: Mutex<Option<Result<Lsn, AppendError>>>,
    cv: Condvar,
}

/// One append's ack. Resolves only after the segment holding the
/// frames is durable, or with the error that prevented that.
pub struct AppendTicket {
    inner: Arc<TicketInner>,
}

impl AppendTicket {
    /// Block until the append is durable or failed.
    pub fn wait(self) -> Result<Lsn, AppendError> {
        let mut done = self.inner.done.lock().unwrap();
        loop {
            if let Some(result) = done.clone() {
                return result;
            }
            done = self.inner.cv.wait(done).unwrap();
        }
    }

    /// Non blocking peek, for callers overlapping work with the window.
    pub fn try_wait(&self) -> Option<Result<Lsn, AppendError>> {
        self.inner.done.lock().unwrap().clone()
    }
}

fn resolve(ticket: &TicketInner, result: Result<Lsn, AppendError>) {
    *ticket.done.lock().unwrap() = Some(result);
    ticket.cv.notify_all();
}

struct StagedFrame {
    /// Kept decoded alongside the wire bytes so the tee can filter by
    /// hints and deliver without a decode on the flush path.
    frame: Frame2,
    wire: Vec<u8>,
}

struct Batch {
    frames: Vec<StagedFrame>,
    bytes: usize,
    opened: Instant,
    /// One entry per append call: the ticket and the lsn it gets acked
    /// with, the highest end lsn of its frames.
    tickets: Vec<(Arc<TicketInner>, Lsn)>,
}

struct State {
    batch: Option<Batch>,
    epochs: HashMap<u128, u32>,
    next_seq: u64,
    prev_digest: u64,
    poisoned: bool,
    shutdown: bool,
}

struct Shared {
    state: Mutex<State>,
    work: Condvar,
}

/// One closed batch on its way to the store.
struct Dispatch {
    seq: u64,
    segment: Vec<u8>,
    /// Decoded frames for the tee, empty when no tee is configured.
    frames: Vec<Frame2>,
    tickets: Vec<(Arc<TicketInner>, Lsn)>,
}

/// Ordered resolution for landed windows. Put workers park their
/// outcome here and only the contiguous prefix resolves, in chain
/// order: a window whose predecessor has not landed is not durable no
/// matter what the store said about the window itself.
///
/// Lock order is lander state before sequencer state, never the other
/// way around.
struct Lander {
    shard: u32,
    tee: Option<Arc<Tee>>,
    state: Mutex<LanderState>,
    moved: Condvar,
}

struct LanderState {
    /// The next seq to resolve; everything below it already resolved.
    next_ack: u64,
    /// Landed out of order, waiting for their predecessors.
    parked: BTreeMap<u64, (Dispatch, Result<(), CasError>)>,
    /// A window failed; everything at or past it resolves as failed.
    failed: bool,
    /// Dispatched but not yet resolved, the flusher's admission gate.
    outstanding: usize,
}

impl Lander {
    fn complete(&self, shared: &Shared, d: Dispatch, outcome: Result<(), CasError>) {
        let mut st = self.state.lock().unwrap();
        st.parked.insert(d.seq, (d, outcome));
        while let Some((&seq, _)) = st.parked.first_key_value() {
            if seq != st.next_ack {
                break;
            }
            let (d, outcome) = st.parked.remove(&seq).unwrap();
            if st.failed {
                // An earlier window failed, so this one sits behind a
                // hole even if its own PUT landed.
                for (ticket, _) in &d.tickets {
                    resolve(ticket, Err(AppendError::Poisoned));
                }
            } else {
                match outcome {
                    Ok(()) => {
                        for (ticket, lsn) in &d.tickets {
                            resolve(ticket, Ok(*lsn));
                        }
                        // The tee sees windows strictly in order and
                        // strictly after the whole prefix is durable.
                        // Fan out is channel sends and never blocks.
                        if let Some(tee) = &self.tee {
                            tee.publish(seq, &d.frames);
                        }
                    }
                    Err(e) => {
                        log::error!("shard {} seq {seq} landing put failed: {e}", self.shard);
                        st.failed = true;
                        shared.state.lock().unwrap().poisoned = true;
                        let source = Arc::new(e);
                        for (ticket, _) in &d.tickets {
                            resolve(
                                ticket,
                                Err(AppendError::Store {
                                    source: Arc::clone(&source),
                                }),
                            );
                        }
                    }
                }
            }
            st.next_ack = seq + 1;
            st.outstanding -= 1;
        }
        drop(st);
        self.moved.notify_all();
    }

    /// Block until fewer than `limit` windows are dispatched and
    /// unresolved, then claim a slot. The bound covers parked windows
    /// too, not just PUTs on the wire, so a crash leaves stragglers at
    /// most [`MAX_INFLIGHT`] past the first hole.
    fn admit(&self, limit: usize) {
        let mut st = self.state.lock().unwrap();
        while st.outstanding >= limit {
            st = self.moved.wait(st).unwrap();
        }
        st.outstanding += 1;
    }

    fn wait_drained(&self) {
        let mut st = self.state.lock().unwrap();
        while st.outstanding > 0 {
            st = self.moved.wait(st).unwrap();
        }
    }
}

fn put_worker(
    rx: &Mutex<Receiver<Dispatch>>,
    sink: &dyn SegmentSink,
    shared: &Shared,
    lander: &Lander,
) {
    loop {
        // Holding the receiver lock across the blocking recv is fine:
        // idle workers queue on the mutex instead of the channel.
        let d = match rx.lock().unwrap().recv() {
            Ok(d) => d,
            Err(_) => return,
        };
        let outcome = sink.put_segment(d.seq, &d.segment);
        lander.complete(shared, d, outcome);
    }
}

/// The role. Owns the flusher and put worker threads; drop or
/// [`Sequencer::close`] drains every staged window before they exit.
pub struct Sequencer {
    shared: Arc<Shared>,
    handles: Vec<JoinHandle<()>>,
}

impl Sequencer {
    /// Start a fresh chain at seq 1. Takeover of an existing chain
    /// resumes with [`Sequencer::resume`].
    pub fn start(shard: u32, sink: Arc<dyn SegmentSink>, config: SequencerConfig) -> Self {
        Self::resume(shard, sink, config, 1, 0)
    }

    /// Start at a given chain position, after a takeover established
    /// the head and the digest of the segment before `next_seq`.
    pub fn resume(
        shard: u32,
        sink: Arc<dyn SegmentSink>,
        config: SequencerConfig,
        next_seq: u64,
        prev_digest: u64,
    ) -> Self {
        assert!(
            (1..=MAX_INFLIGHT).contains(&config.inflight),
            "inflight must be between 1 and {MAX_INFLIGHT}"
        );
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                batch: None,
                epochs: HashMap::new(),
                next_seq,
                prev_digest,
                poisoned: false,
                shutdown: false,
            }),
            work: Condvar::new(),
        });
        let lander = Arc::new(Lander {
            shard,
            tee: config.tee.clone(),
            state: Mutex::new(LanderState {
                next_ack: next_seq,
                parked: BTreeMap::new(),
                failed: false,
                outstanding: 0,
            }),
            moved: Condvar::new(),
        });
        let (tx, rx) = mpsc::sync_channel::<Dispatch>(config.inflight);
        let rx = Arc::new(Mutex::new(rx));
        let mut handles = Vec::with_capacity(config.inflight + 1);
        for _ in 0..config.inflight {
            let rx = Arc::clone(&rx);
            let sink = Arc::clone(&sink);
            let shared = Arc::clone(&shared);
            let lander = Arc::clone(&lander);
            handles.push(std::thread::spawn(move || {
                put_worker(&rx, &*sink, &shared, &lander)
            }));
        }
        {
            let shared = Arc::clone(&shared);
            handles.push(std::thread::spawn(move || {
                flusher_loop(&shared, shard, &config, tx, &lander)
            }));
        }
        Self { shared, handles }
    }

    /// Stage frames for the batch in flight and return the ticket that
    /// resolves once they are durable. The whole append admits or
    /// rejects atomically: one stale epoch and nothing is staged.
    pub fn append(&self, frames: Vec<Frame2>) -> Result<AppendTicket, AppendError> {
        assert!(!frames.is_empty(), "an append carries at least one frame");
        // Encoding is the expensive part, lz4 over the payloads, and it
        // needs nothing from the shared state, so it happens before the
        // lock. A rejected append wastes the work, but rejection is a
        // once per failover event, not a hot path.
        let staged: Vec<StagedFrame> = frames
            .into_iter()
            .map(|f| {
                let wire = f.encode();
                StagedFrame { frame: f, wire }
            })
            .collect();
        let durable_lsn = staged.iter().map(|s| s.frame.end_lsn).max().unwrap();

        let mut state = self.shared.state.lock().unwrap();
        if state.shutdown {
            return Err(AppendError::Closed);
        }
        if state.poisoned {
            return Err(AppendError::Poisoned);
        }
        for s in &staged {
            if let Some(&current) = state.epochs.get(&s.frame.tenant)
                && s.frame.writer_epoch < current
            {
                return Err(AppendError::WrongEpoch {
                    tenant: s.frame.tenant,
                    current,
                });
            }
        }
        for s in &staged {
            let known = state
                .epochs
                .entry(s.frame.tenant)
                .or_insert(s.frame.writer_epoch);
            *known = (*known).max(s.frame.writer_epoch);
        }

        let ticket = Arc::new(TicketInner {
            done: Mutex::new(None),
            cv: Condvar::new(),
        });
        let batch = state.batch.get_or_insert_with(|| Batch {
            frames: Vec::new(),
            bytes: 0,
            opened: Instant::now(),
            tickets: Vec::new(),
        });
        for s in staged {
            batch.bytes += s.wire.len();
            batch.frames.push(s);
        }
        batch.tickets.push((Arc::clone(&ticket), durable_lsn));
        drop(state);
        self.shared.work.notify_all();
        Ok(AppendTicket { inner: ticket })
    }

    /// The writer epoch currently admitted for a tenant, if any frame
    /// of that tenant has been seen.
    pub fn tenant_epoch(&self, tenant: u128) -> Option<u32> {
        self.shared
            .state
            .lock()
            .unwrap()
            .epochs
            .get(&tenant)
            .copied()
    }

    /// Drain the open batch and every window in flight, then stop.
    /// Every staged append resolves, durably or with the failure,
    /// before this returns.
    pub fn close(mut self) -> std::thread::Result<()> {
        self.begin_shutdown();
        let mut result = Ok(());
        for handle in self.handles.drain(..) {
            if let Err(e) = handle.join() {
                result = Err(e);
            }
        }
        result
    }

    fn begin_shutdown(&self) {
        self.shared.state.lock().unwrap().shutdown = true;
        self.shared.work.notify_all();
    }
}

impl Drop for Sequencer {
    fn drop(&mut self) {
        self.begin_shutdown();
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

fn flusher_loop(
    shared: &Shared,
    shard: u32,
    config: &SequencerConfig,
    tx: SyncSender<Dispatch>,
    lander: &Lander,
) {
    let mut state = shared.state.lock().unwrap();
    loop {
        let Some(batch) = state.batch.as_ref() else {
            if state.shutdown {
                break;
            }
            state = shared.work.wait(state).unwrap();
            continue;
        };

        let age = batch.opened.elapsed();
        let close_now = state.shutdown
            || age >= config.window
            || batch.bytes >= config.batch_bytes
            || batch.frames.len() >= config.batch_frames;
        if !close_now {
            let (next, _) = shared
                .work
                .wait_timeout(state, config.window - age)
                .unwrap();
            state = next;
            continue;
        }

        let batch = state.batch.take().unwrap();
        if state.poisoned {
            // A batch staged between the failing PUT and the poison
            // flag propagating: never acked, never PUT.
            for (ticket, _) in &batch.tickets {
                resolve(ticket, Err(AppendError::Poisoned));
            }
            continue;
        }
        let seq = state.next_seq;
        let prev_digest = state.prev_digest;
        drop(state);

        // Build outside the lock so appends keep staging the next
        // window while this one encodes.
        let mut builder = SegmentBuilder::new(SegmentHeader {
            kind: SegmentKind::Landing,
            shard,
            seq,
            prev_digest,
        });
        for f in &batch.frames {
            builder.push_encoded(f.frame.tenant, f.frame.start_lsn, f.frame.end_lsn, &f.wire);
        }
        let (segment, summaries) = builder.finish();

        // The chain links window content, not acks, so the position
        // advances the moment the segment is built and the next window
        // lands behind this one without waiting for its PUT.
        state = shared.state.lock().unwrap();
        state.next_seq = seq + 1;
        state.prev_digest = tenants_digest(&summaries);
        drop(state);

        let frames = if lander.tee.is_some() {
            batch.frames.into_iter().map(|s| s.frame).collect()
        } else {
            Vec::new()
        };
        let dispatch = Dispatch {
            seq,
            segment,
            frames,
            tickets: batch.tickets,
        };
        lander.admit(config.inflight);
        if let Err(mpsc::SendError(d)) = tx.send(dispatch) {
            // The workers are gone, which takes a panic in the sink.
            // Fail the batch loudly instead of hanging its committers.
            for (ticket, _) in &d.tickets {
                resolve(ticket, Err(AppendError::Poisoned));
            }
            let mut st = lander.state.lock().unwrap();
            st.failed = true;
            st.outstanding -= 1;
            drop(st);
            shared.state.lock().unwrap().poisoned = true;
        }
        state = shared.state.lock().unwrap();
    }
    // Shutdown: nothing left to stage. Close the channel so idle
    // workers exit, then wait for every window in flight to resolve.
    drop(state);
    drop(tx);
    lander.wait_drained();
}
