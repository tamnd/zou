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
//! Deliberately synchronous, plain threads and condvars, matching the
//! v1 group commit in zou-store. Frames are encoded on the append path
//! so the flush path only copies bytes.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use zou_store::{CasError, CasStore, Frame2, Lsn};

use crate::chain::segment_key;
use crate::segment::{SegmentBuilder, SegmentHeader, SegmentKind, tenants_digest};

/// Where closed batches go to become durable. `put_segment` must return
/// only once the segment survives whatever the sink's durability story
/// is, because the sequencer acks commits on its return. An error is
/// terminal for the shard: with a fenced chain it means another
/// sequencer may own the head now, so the caller poisons itself.
pub trait SegmentSink: Send + Sync {
    fn put_segment(&self, seq: u64, segment: &[u8]) -> Result<(), CasError>;
}

/// The plain sink: one fenced object per segment on any CAS store,
/// `cellwal/<shard>/<seq:016x>` created with put_if_absent. Losing the
/// creation race surfaces as AlreadyExists, which is exactly the fence:
/// someone else owns that chain position, this sequencer must stop.
pub struct CasSink {
    store: Arc<dyn CasStore>,
    shard: u32,
}

impl CasSink {
    pub fn new(store: Arc<dyn CasStore>, shard: u32) -> Self {
        Self { store, shard }
    }
}

impl SegmentSink for CasSink {
    fn put_segment(&self, seq: u64, segment: &[u8]) -> Result<(), CasError> {
        let key = segment_key(self.shard, seq);
        self.store.put_if_absent(&key, segment).map(|_| ())
    }
}

#[derive(Debug, Clone)]
pub struct SequencerConfig {
    /// Batch window, commit p50 is about half of this plus the PUT p50.
    pub window: Duration,
    /// Close early once this many encoded frame bytes are pending.
    pub batch_bytes: usize,
    /// Close early once this many frames are pending.
    pub batch_frames: usize,
}

impl Default for SequencerConfig {
    fn default() -> Self {
        Self {
            window: Duration::from_millis(3),
            batch_bytes: 4 * 1024 * 1024,
            batch_frames: 4096,
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
    tenant: u128,
    start_lsn: Lsn,
    end_lsn: Lsn,
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

/// The role. Owns the flusher thread; drop or [`Sequencer::close`]
/// drains the open batch before the thread exits.
pub struct Sequencer {
    shared: Arc<Shared>,
    handle: Option<JoinHandle<()>>,
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
        let handle = {
            let shared = Arc::clone(&shared);
            std::thread::spawn(move || flusher_loop(&shared, &*sink, shard, &config))
        };
        Self {
            shared,
            handle: Some(handle),
        }
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
            .iter()
            .map(|f| StagedFrame {
                tenant: f.tenant,
                start_lsn: f.start_lsn,
                end_lsn: f.end_lsn,
                wire: f.encode(),
            })
            .collect();
        let durable_lsn = frames.iter().map(|f| f.end_lsn).max().unwrap();

        let mut state = self.shared.state.lock().unwrap();
        if state.shutdown {
            return Err(AppendError::Closed);
        }
        if state.poisoned {
            return Err(AppendError::Poisoned);
        }
        for f in &frames {
            if let Some(&current) = state.epochs.get(&f.tenant)
                && f.writer_epoch < current
            {
                return Err(AppendError::WrongEpoch {
                    tenant: f.tenant,
                    current,
                });
            }
        }
        for f in &frames {
            let known = state.epochs.entry(f.tenant).or_insert(f.writer_epoch);
            *known = (*known).max(f.writer_epoch);
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

    /// Drain the open batch and stop. Every staged append resolves,
    /// durably or with the failure, before this returns.
    pub fn close(mut self) -> std::thread::Result<()> {
        self.begin_shutdown();
        match self.handle.take() {
            Some(handle) => handle.join(),
            None => Ok(()),
        }
    }

    fn begin_shutdown(&self) {
        self.shared.state.lock().unwrap().shutdown = true;
        self.shared.work.notify_all();
    }
}

impl Drop for Sequencer {
    fn drop(&mut self) {
        self.begin_shutdown();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn flusher_loop(shared: &Shared, sink: &dyn SegmentSink, shard: u32, config: &SequencerConfig) {
    let mut state = shared.state.lock().unwrap();
    loop {
        let Some(batch) = state.batch.as_ref() else {
            if state.shutdown {
                return;
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

        // Build and PUT outside the lock so appends keep staging the
        // next window while this one lands.
        let mut builder = SegmentBuilder::new(SegmentHeader {
            kind: SegmentKind::Landing,
            shard,
            seq,
            prev_digest,
        });
        for f in &batch.frames {
            builder.push_encoded(f.tenant, f.start_lsn, f.end_lsn, &f.wire);
        }
        let (segment, summaries) = builder.finish();
        let outcome = sink.put_segment(seq, &segment);

        state = shared.state.lock().unwrap();
        match outcome {
            Ok(()) => {
                state.next_seq = seq + 1;
                state.prev_digest = tenants_digest(&summaries);
                for (ticket, durable_lsn) in &batch.tickets {
                    resolve(ticket, Ok(*durable_lsn));
                }
            }
            Err(e) => {
                log::error!("shard {shard} seq {seq} landing put failed: {e}");
                state.poisoned = true;
                let source = Arc::new(e);
                for (ticket, _) in &batch.tickets {
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
}
