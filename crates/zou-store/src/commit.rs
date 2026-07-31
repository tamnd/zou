//! Group commit: the write path between "a record is appended" and "the
//! record is durable on the object store".
//!
//! Producers call [`GroupCommit::append`] and get a ticket. A background
//! flusher drains the buffer into WAL frames and uploads each frame as one
//! immutable object, keyed by its start LSN. Tickets resolve only after the
//! upload succeeds, which is the whole contract: an ack means the bytes are
//! on the object store, never sooner. If the store stalls, the buffer fills
//! and appends block, so backpressure reaches producers instead of memory.
//!
//! This module is deliberately synchronous, plain threads and condvars.
//! zou-store is the embedded core and stays runtime free the way SQLite
//! does; async wrappers belong to the server layer above.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::cas::{CasError, CasStore};
use crate::layout::TenantLayout;
use crate::lease::{self, HeldLease, LeaseError};
use crate::lsn::Lsn;
use crate::manifest::{CheckpointKind, CheckpointRef, Manifest, WalTail};
use crate::tier::{PureS3Target, WalTarget};
use crate::wal::{Frame, MAX_BODY_LEN};

/// Per-record overhead inside a frame payload: a u32 length prefix.
const RECORD_PREFIX: usize = 4;

#[derive(Debug, Clone)]
pub struct GroupCommitConfig {
    /// Flush pending records once the oldest has waited this long.
    pub flush_interval: Duration,
    /// Flush immediately once this many pending bytes accumulate.
    pub flush_bytes: usize,
    /// Appends block once pending bytes reach this, which is how a stalled
    /// object store pushes back on producers.
    pub buffer_capacity: usize,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_millis(2),
            flush_bytes: 512 * 1024,
            buffer_capacity: 8 * 1024 * 1024,
        }
    }
}

/// When to fold the accumulated segment list into manifest.wal_tail.
/// Publishing piggybacks on flushes, there is no separate timer thread:
/// an idle pipeline has nothing new to publish, and close always does a
/// final publish so a clean shutdown leaves an exact tail behind.
#[derive(Debug, Clone)]
pub struct TailConfig {
    /// Publish once this many bytes of frames sealed since the last publish.
    pub seal_bytes: u64,
    /// Publish once this much time passed since the last publish.
    pub seal_interval: Duration,
}

impl Default for TailConfig {
    fn default() -> Self {
        Self {
            seal_bytes: 16 * 1024 * 1024,
            seal_interval: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum CommitError {
    #[error("wal pipeline is closed")]
    Closed,
    #[error("record of {len} bytes exceeds the frame body cap")]
    RecordTooLarge { len: usize },
    /// The flusher exhausted its retries against the store. Every ticket
    /// past the durable watermark fails with this, none of them were acked.
    #[error("wal flush failed: {0}")]
    Flush(Arc<CasError>),
    /// The manifest shows another writer took the lease. This node must
    /// stop writing immediately, its epoch is dead.
    #[error("writer lease lost, this node must stop writing")]
    LeaseLost,
    #[error("wal tail publish failed: {reason}")]
    TailPublish { reason: String },
    /// A fold request could not be applied to the live tail. The tail is
    /// unchanged and the pipeline keeps running, the caller retries later.
    #[error("wal fold rejected: {reason}")]
    FoldRejected { reason: String },
}

struct State {
    pending: VecDeque<Vec<u8>>,
    pending_bytes: usize,
    first_pending_at: Option<Instant>,
    /// LSN after everything appended so far. LSNs are byte offsets into the
    /// framed record stream, so they advance by prefix plus payload.
    next_lsn: u64,
    /// LSN up to which records have been handed to the flusher.
    taken_lsn: u64,
    /// LSN up to which records are durable on the store.
    durable_lsn: u64,
    failure: Option<CommitError>,
    shutdown: bool,
    /// A pending checkpoint fold, applied by the flusher between frames.
    fold: Option<FoldRequest>,
}

/// A checkpoint fold: drop a prefix of sealed segments and publish the
/// checkpoint ref atomically with the truncated tail. Serviced on the
/// flusher thread because it owns the live segment list; a manifest CAS
/// from outside would be undone by the next publish.
struct FoldRequest {
    checkpoint: CheckpointRef,
    /// Segment names to drop, a prefix of the live list, oldest first.
    drop: Vec<String>,
    result: Option<Result<(), CommitError>>,
}

struct Shared {
    state: Mutex<State>,
    /// Wakes the flusher when records arrive or shutdown starts.
    work: Condvar,
    /// Wakes ticket waiters and producers blocked on backpressure.
    progress: Condvar,
    config: GroupCommitConfig,
}

/// Proof of an append. `wait` blocks until the record is durable.
#[must_use = "an append is not durable until the ticket resolves"]
pub struct CommitTicket {
    shared: Arc<Shared>,
    end_lsn: u64,
}

impl CommitTicket {
    /// Block until this record's frame is on the object store, returning
    /// the LSN the record ends at. Errors mean it never became durable.
    pub fn wait(self) -> Result<Lsn, CommitError> {
        let mut state = self.shared.state.lock().unwrap();
        loop {
            if state.durable_lsn >= self.end_lsn {
                return Ok(Lsn(self.end_lsn));
            }
            if let Some(failure) = &state.failure {
                return Err(failure.clone());
            }
            state = self.shared.progress.wait(state).unwrap();
        }
    }
}

pub struct GroupCommit {
    shared: Arc<Shared>,
    epoch: u64,
    flusher: Option<JoinHandle<()>>,
}

/// Everything the flusher thread needs beyond the shared buffer state.
struct FlusherParams {
    epoch: u64,
    fence: u64,
    target: Arc<dyn WalTarget>,
    tail: Option<TailCtx>,
}

/// Assembles a [`GroupCommit`]. `session` or `lease` picks where epoch and
/// fence come from, `target` picks the latency tier, defaulting to PureS3
/// on the main store.
pub struct GroupCommitBuilder {
    store: Arc<dyn CasStore>,
    layout: TenantLayout,
    session: (u64, u64),
    lease: Option<Arc<Mutex<HeldLease>>>,
    start_lsn: Lsn,
    config: GroupCommitConfig,
    tail: TailConfig,
    initial_tail: Option<WalTail>,
    target: Option<Arc<dyn WalTarget>>,
}

impl GroupCommitBuilder {
    /// Stamp frames with an explicit epoch and fence, no tail publishing.
    pub fn session(mut self, epoch: u64, fence: u64) -> Self {
        self.session = (epoch, fence);
        self.lease = None;
        self
    }

    /// Bind to a held lease: epoch and fence come from it, and sealed
    /// segments are folded into manifest.wal_tail. Every publish doubles
    /// as an ownership check, a stolen lease poisons the pipeline.
    pub fn lease(mut self, held: Arc<Mutex<HeldLease>>) -> Self {
        self.lease = Some(held);
        self
    }

    pub fn start_lsn(mut self, lsn: Lsn) -> Self {
        self.start_lsn = lsn;
        self
    }

    pub fn config(mut self, config: GroupCommitConfig) -> Self {
        self.config = config;
        self
    }

    pub fn tail_config(mut self, tail: TailConfig) -> Self {
        self.tail = tail;
        self
    }

    /// Chain this session onto the WAL history already in the store,
    /// normally the result of [`reconcile_tail`]. Published tails then
    /// keep the earlier sessions' segments instead of replacing them,
    /// which is what makes recovery across restarts possible.
    pub fn initial_tail(mut self, tail: WalTail) -> Self {
        self.initial_tail = Some(tail);
        self
    }

    /// Route frame uploads through a latency tier other than the default
    /// PureS3 on the main store.
    pub fn target(mut self, target: Arc<dyn WalTarget>) -> Self {
        self.target = Some(target);
        self
    }

    pub fn build(self) -> GroupCommit {
        let target = self
            .target
            .unwrap_or_else(|| Arc::new(PureS3Target::new(Arc::clone(&self.store))));
        let (epoch, fence, tail) = match self.lease {
            Some(held) => {
                let (epoch, fence) = {
                    let held = held.lock().unwrap();
                    (held.epoch, held.fence)
                };
                let (from_lsn, segments) = match self.initial_tail {
                    Some(prior) => (prior.from_lsn, prior.segments),
                    None => (self.start_lsn, Vec::new()),
                };
                let tail = TailCtx {
                    held,
                    config: self.tail,
                    from_lsn,
                    segments,
                    bytes_since_publish: 0,
                    last_publish: Instant::now(),
                };
                (epoch, fence, Some(tail))
            }
            None => (self.session.0, self.session.1, None),
        };
        let params = FlusherParams {
            epoch,
            fence,
            target,
            tail,
        };
        GroupCommit::start(self.store, self.layout, self.start_lsn, self.config, params)
    }
}

impl GroupCommit {
    pub fn builder(store: Arc<dyn CasStore>, layout: TenantLayout) -> GroupCommitBuilder {
        GroupCommitBuilder {
            store,
            layout,
            session: (0, 0),
            lease: None,
            start_lsn: Lsn(0),
            config: GroupCommitConfig::default(),
            tail: TailConfig::default(),
            initial_tail: None,
            target: None,
        }
    }

    /// Start the pipeline for one writer session without tail publishing.
    /// Epoch and fence are stamped into every frame, `start_lsn` is where
    /// this session's WAL stream begins.
    pub fn new(
        store: Arc<dyn CasStore>,
        layout: TenantLayout,
        epoch: u64,
        fence: u64,
        start_lsn: Lsn,
        config: GroupCommitConfig,
    ) -> Self {
        Self::builder(store, layout)
            .session(epoch, fence)
            .start_lsn(start_lsn)
            .config(config)
            .build()
    }

    /// Start the pipeline bound to a held lease. See
    /// [`GroupCommitBuilder::lease`].
    pub fn with_lease(
        store: Arc<dyn CasStore>,
        layout: TenantLayout,
        held: Arc<Mutex<HeldLease>>,
        start_lsn: Lsn,
        config: GroupCommitConfig,
        tail: TailConfig,
    ) -> Self {
        Self::builder(store, layout)
            .lease(held)
            .start_lsn(start_lsn)
            .config(config)
            .tail_config(tail)
            .build()
    }

    fn start(
        store: Arc<dyn CasStore>,
        layout: TenantLayout,
        start_lsn: Lsn,
        config: GroupCommitConfig,
        params: FlusherParams,
    ) -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                pending: VecDeque::new(),
                pending_bytes: 0,
                first_pending_at: None,
                next_lsn: start_lsn.0,
                taken_lsn: start_lsn.0,
                durable_lsn: start_lsn.0,
                failure: None,
                shutdown: false,
                fold: None,
            }),
            work: Condvar::new(),
            progress: Condvar::new(),
            config,
        });
        let epoch = params.epoch;
        let flusher = {
            let shared = Arc::clone(&shared);
            std::thread::spawn(move || flusher_loop(&shared, &*store, &layout, params))
        };
        Self {
            shared,
            epoch,
            flusher: Some(flusher),
        }
    }

    /// The epoch stamped into this session's frames, which names the
    /// `wal/<epoch>/` directory the session writes. Readers use it to
    /// pick up segments uploaded after the last tail publish without
    /// trusting any other epoch's unpublished objects.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Queue one record. Blocks while the buffer is full, errors if the
    /// pipeline has failed or closed. Durability comes from the ticket.
    pub fn append(&self, record: &[u8]) -> Result<CommitTicket, CommitError> {
        let framed_len = RECORD_PREFIX + record.len();
        if framed_len > MAX_BODY_LEN as usize {
            return Err(CommitError::RecordTooLarge { len: record.len() });
        }

        let mut state = self.shared.state.lock().unwrap();
        loop {
            if let Some(failure) = &state.failure {
                return Err(failure.clone());
            }
            if state.shutdown {
                return Err(CommitError::Closed);
            }
            // An oversized record is admitted alone into an empty buffer,
            // otherwise it could never fit and would block forever.
            if state.pending.is_empty()
                || state.pending_bytes + framed_len <= self.shared.config.buffer_capacity
            {
                break;
            }
            state = self.shared.progress.wait(state).unwrap();
        }

        let was_empty = state.pending.is_empty();
        if was_empty {
            state.first_pending_at = Some(Instant::now());
        }
        let mut framed = Vec::with_capacity(framed_len);
        framed.extend_from_slice(&(record.len() as u32).to_le_bytes());
        framed.extend_from_slice(record);
        state.pending.push_back(framed);
        state.pending_bytes += framed_len;
        state.next_lsn += framed_len as u64;
        let end_lsn = state.next_lsn;

        // The first record into an empty buffer must wake the flusher so it
        // arms the interval timer; an untimed wait would otherwise sleep
        // through small appends forever. Later records only wake it when the
        // byte trigger fires.
        if was_empty || state.pending_bytes >= self.shared.config.flush_bytes {
            self.shared.work.notify_one();
        }
        Ok(CommitTicket {
            shared: Arc::clone(&self.shared),
            end_lsn,
        })
    }

    /// Fold a checkpoint into the manifest: drop `drop` sealed segments,
    /// which must be a prefix of the live tail, and publish the truncated
    /// tail together with the checkpoint ref in one manifest update. The
    /// caller decides which segments the checkpoint covers, this method
    /// only guarantees the swap is atomic against concurrent publishes.
    /// Blocks until the flusher applied it. Only meaningful on lease bound
    /// pipelines, session pipelines reject it.
    pub fn fold_tail(
        &self,
        checkpoint: CheckpointRef,
        drop: Vec<String>,
    ) -> Result<(), CommitError> {
        let mut state = self.shared.state.lock().unwrap();
        if let Some(failure) = &state.failure {
            return Err(failure.clone());
        }
        if state.shutdown {
            return Err(CommitError::Closed);
        }
        if state.fold.is_some() {
            return Err(CommitError::FoldRejected {
                reason: "another fold is in flight".into(),
            });
        }
        state.fold = Some(FoldRequest {
            checkpoint,
            drop,
            result: None,
        });
        self.shared.work.notify_one();
        loop {
            if state
                .fold
                .as_ref()
                .is_some_and(|fold| fold.result.is_some())
            {
                let fold = state.fold.take().expect("checked above");
                return fold.result.expect("checked above");
            }
            if let Some(failure) = state.failure.clone() {
                state.fold = None;
                return Err(failure);
            }
            state = self.shared.progress.wait(state).unwrap();
        }
    }

    /// Flush everything pending and stop the flusher. Returns the pipeline
    /// failure if one happened, in which case the unflushed tail is lost
    /// and its tickets already reported that.
    pub fn close(mut self) -> Result<(), CommitError> {
        self.begin_shutdown();
        if let Some(handle) = self.flusher.take() {
            let _ = handle.join();
        }
        let state = self.shared.state.lock().unwrap();
        match &state.failure {
            Some(failure) => Err(failure.clone()),
            None => Ok(()),
        }
    }

    fn begin_shutdown(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;
        drop(state);
        self.shared.work.notify_all();
        self.shared.progress.notify_all();
    }
}

impl Drop for GroupCommit {
    fn drop(&mut self) {
        if let Some(handle) = self.flusher.take() {
            self.begin_shutdown();
            let _ = handle.join();
        }
    }
}

/// Tail publishing state carried by the flusher for lease-bound pipelines.
struct TailCtx {
    held: Arc<Mutex<HeldLease>>,
    config: TailConfig,
    from_lsn: Lsn,
    /// Segment file names written this session, oldest first.
    segments: Vec<String>,
    bytes_since_publish: u64,
    last_publish: Instant,
}

fn flusher_loop(
    shared: &Shared,
    store: &dyn CasStore,
    layout: &TenantLayout,
    params: FlusherParams,
) {
    let FlusherParams {
        epoch,
        fence,
        target,
        mut tail,
    } = params;
    let mut state = shared.state.lock().unwrap();
    loop {
        // A fold outranks everything else: the segment list must not move
        // between the caller's snapshot and the publish, and only this
        // thread appends to it, so applying it here makes it atomic.
        if state
            .fold
            .as_ref()
            .is_some_and(|fold| fold.result.is_none())
        {
            let mut fold = state.fold.take().expect("checked above");
            let result = match tail.as_mut() {
                None => Err(CommitError::FoldRejected {
                    reason: "pipeline is not lease bound".into(),
                }),
                Some(tail) if !tail.segments.starts_with(&fold.drop) => {
                    Err(CommitError::FoldRejected {
                        reason: "drop list is not a prefix of the live tail".into(),
                    })
                }
                Some(tail) if !fold.drop.is_empty() && fold.drop.len() == tail.segments.len() => {
                    Err(CommitError::FoldRejected {
                        reason: "fold would drop the entire tail".into(),
                    })
                }
                Some(tail) => {
                    let kept: Vec<String> = tail.segments[fold.drop.len()..].to_vec();
                    let from_lsn = match kept.first() {
                        Some(first) => segment_start_lsn(first).unwrap_or(tail.from_lsn),
                        None => tail.from_lsn,
                    };
                    let wal_tail = WalTail {
                        epoch_dir: epoch,
                        from_lsn,
                        segments: kept.clone(),
                    };
                    drop(state);
                    let result =
                        publish_manifest(store, layout, tail, wal_tail, Some(&fold.checkpoint));
                    state = shared.state.lock().unwrap();
                    // The in memory tail only truncates once the manifest
                    // carries the checkpoint, otherwise a later routine
                    // publish would drop coverage the store still needs.
                    if result.is_ok() {
                        tail.segments = kept;
                        tail.from_lsn = from_lsn;
                    }
                    result
                }
            };
            match result {
                Err(e @ CommitError::LeaseLost) => {
                    fold.result = Some(Err(e.clone()));
                    state.fold = Some(fold);
                    state.failure = Some(e);
                    shared.progress.notify_all();
                    return;
                }
                result => {
                    fold.result = Some(result);
                    state.fold = Some(fold);
                    shared.progress.notify_all();
                }
            }
            continue;
        }

        if state.pending.is_empty() {
            if state.shutdown {
                // A clean shutdown leaves an exact tail in the manifest.
                if let Some(tail) = tail.as_mut()
                    && tail.bytes_since_publish > 0
                {
                    drop(state);
                    let result = publish_tail(store, layout, tail, epoch);
                    state = shared.state.lock().unwrap();
                    if let Err(e) = result {
                        state.failure = Some(e);
                        shared.progress.notify_all();
                    }
                }
                return;
            }
            state = shared.work.wait(state).unwrap();
            continue;
        }

        let age = state
            .first_pending_at
            .map_or(Duration::ZERO, |t| t.elapsed());
        let due = state.shutdown
            || state.pending_bytes >= shared.config.flush_bytes
            || age >= shared.config.flush_interval;
        if !due {
            let timeout = shared.config.flush_interval - age;
            (state, _) = shared.work.wait_timeout(state, timeout).unwrap();
            continue;
        }

        // Take a prefix of whole records up to the frame body cap.
        let mut payload = Vec::new();
        while let Some(front) = state.pending.front() {
            if !payload.is_empty() && payload.len() + front.len() > MAX_BODY_LEN as usize {
                break;
            }
            let framed = state.pending.pop_front().unwrap();
            state.pending_bytes -= framed.len();
            payload.extend_from_slice(&framed);
        }
        let start_lsn = state.taken_lsn;
        let end_lsn = start_lsn + payload.len() as u64;
        state.taken_lsn = end_lsn;
        state.first_pending_at = if state.pending.is_empty() {
            None
        } else {
            Some(Instant::now())
        };
        drop(state);
        // Buffer space just freed, unblock producers before the upload.
        shared.progress.notify_all();

        let frame = Frame {
            epoch,
            fence,
            start_lsn: Lsn(start_lsn),
            end_lsn: Lsn(end_lsn),
            payload,
        };
        let key = layout.wal_segment(epoch, Lsn(start_lsn));
        let encoded = frame.encode();
        let result = target.put_frame(&key, &encoded);

        // Piggyback tail publishing on a successful flush, outside the
        // state lock so producers keep moving during the manifest CAS.
        let tail_result = match (&result, tail.as_mut()) {
            (Ok(()), Some(tail)) => {
                let name = key.rsplit('/').next().expect("wal keys contain slashes");
                tail.segments.push(format!("{epoch:016}/{name}"));
                tail.bytes_since_publish += encoded.len() as u64;
                let due = tail.bytes_since_publish >= tail.config.seal_bytes
                    || tail.last_publish.elapsed() >= tail.config.seal_interval;
                if due {
                    publish_tail(store, layout, tail, epoch)
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        };

        state = shared.state.lock().unwrap();
        match (result, tail_result) {
            // A lost lease means this epoch is dead and the frame we just
            // uploaded will never be replayed. Acking it would be a lie, so
            // the durable watermark stays put and every waiter errors.
            (Ok(()), Err(e @ CommitError::LeaseLost)) => {
                state.failure = Some(e);
                shared.progress.notify_all();
                return;
            }
            // The frame is durable and the epoch still ours, so the ack is
            // honest even though the pipeline stops over the failed publish.
            (Ok(()), Err(e)) => {
                state.durable_lsn = end_lsn;
                state.failure = Some(e);
                shared.progress.notify_all();
                return;
            }
            (Ok(()), Ok(())) => {
                state.durable_lsn = end_lsn;
                shared.progress.notify_all();
            }
            (Err(e), _) => {
                state.failure = Some(CommitError::Flush(Arc::new(e)));
                shared.progress.notify_all();
                return;
            }
        }
    }
}

/// Fold the accumulated segment list into manifest.wal_tail under the
/// lease. Races with the heartbeat renewer are expected and retried, a
/// lost lease is fatal for this writer.
fn publish_tail(
    store: &dyn CasStore,
    layout: &TenantLayout,
    tail: &mut TailCtx,
    epoch: u64,
) -> Result<(), CommitError> {
    let wal_tail = WalTail {
        epoch_dir: epoch,
        from_lsn: tail.from_lsn,
        segments: tail.segments.clone(),
    };
    publish_manifest(store, layout, tail, wal_tail, None)
}

/// The manifest CAS behind both routine tail publishes and checkpoint
/// folds. Publishes the given tail, and with it the checkpoint ref when
/// one rides along, skipping refs the manifest already carries so a
/// retried fold stays idempotent. A full ref prunes everything before
/// it: restore and the chain walk start at the newest full and never
/// look behind it, so the superseded chain becomes garbage for the gc
/// job the moment the full publishes.
fn publish_manifest(
    store: &dyn CasStore,
    layout: &TenantLayout,
    tail: &mut TailCtx,
    wal_tail: WalTail,
    checkpoint: Option<&CheckpointRef>,
) -> Result<(), CommitError> {
    const ATTEMPTS: u32 = 5;
    let mut last: Option<LeaseError> = None;
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    for _ in 0..ATTEMPTS {
        let mut held = tail.held.lock().unwrap();
        let wal_tail = wal_tail.clone();
        let checkpoint = checkpoint.cloned();
        match lease::update_manifest(store, layout, &mut held, now_unix, move |m| {
            m.wal_tail = Some(wal_tail);
            if let Some(checkpoint) = checkpoint {
                if !m.checkpoints.iter().any(|c| c.id == checkpoint.id) {
                    m.checkpoints.push(checkpoint.clone());
                }
                if checkpoint.kind == CheckpointKind::Full
                    && let Some(pos) = m.checkpoints.iter().rposition(|c| c.id == checkpoint.id)
                {
                    m.checkpoints.drain(..pos);
                }
                // Any fold's scan window covers the frozen parent tail,
                // the assembly errors out before publish if a segment
                // is unreadable, so once a checkpoint lands the child
                // no longer needs the parent's WAL and gc can let go
                // of it.
                m.parent_tail.clear();
            }
        }) {
            Ok(()) => {
                tail.bytes_since_publish = 0;
                tail.last_publish = Instant::now();
                return Ok(());
            }
            Err(LeaseError::Lost { .. }) => return Err(CommitError::LeaseLost),
            Err(e) => last = Some(e),
        }
    }
    Err(CommitError::TailPublish {
        reason: last.expect("loop ran at least once").to_string(),
    })
}

/// Reconstruct the complete WAL tail visible in the store: everything the
/// manifest lists plus segments uploaded after the last publish, including
/// whole sessions that crashed before their first publish. Frames become
/// durable, and acked, on upload, so the manifest list alone would lose
/// the newest commits. Returns None when the store holds no WAL at all.
pub fn reconcile_tail(
    store: &dyn CasStore,
    layout: &TenantLayout,
    manifest: &Manifest,
) -> Result<Option<WalTail>, CasError> {
    let dir = layout.wal_dir();
    let mut segments: Vec<String> = store
        .list(&dir)?
        .into_iter()
        .filter_map(|key| {
            let rel = key.strip_prefix(&dir)?;
            let (epoch, name) = rel.split_once('/')?;
            (epoch.len() == 16
                && epoch.parse::<u64>().is_ok()
                && name.len() == 20
                && name.ends_with(".wal"))
            .then(|| rel.to_string())
        })
        .collect();
    // Epoch and start LSN are fixed width hex, so the path sort is the
    // session order and the stream order within each session.
    segments.sort();
    if segments.is_empty() {
        return Ok(None);
    }
    let from_lsn = match &manifest.wal_tail {
        Some(tail) => tail.from_lsn,
        None => segment_start_lsn(&segments[0]).unwrap_or(Lsn(0)),
    };
    let epoch_dir = segment_epoch(segments.last().expect("nonempty")).unwrap_or(manifest.epoch);
    Ok(Some(WalTail {
        epoch_dir,
        from_lsn,
        segments,
    }))
}

/// Epoch of an epoch qualified segment path. The directory component is
/// zero padded decimal, matching [`TenantLayout::wal_epoch_dir`].
pub fn segment_epoch(qualified: &str) -> Option<u64> {
    let (epoch, _) = qualified.split_once('/')?;
    epoch.parse().ok()
}

/// Stream start LSN encoded in a segment file name.
pub fn segment_start_lsn(qualified: &str) -> Option<Lsn> {
    let name = qualified.rsplit('/').next()?;
    let hex = name.strip_suffix(".wal")?;
    u64::from_str_radix(hex, 16).ok().map(Lsn)
}

/// Split a frame payload back into the records that went in. Returns None
/// on malformed framing, which recovery treats the same as a bad crc.
pub fn split_records(payload: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut records = Vec::new();
    let mut rest = payload;
    while !rest.is_empty() {
        let len_bytes: [u8; RECORD_PREFIX] = rest.get(..RECORD_PREFIX)?.try_into().ok()?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        rest = &rest[RECORD_PREFIX..];
        records.push(rest.get(..len)?.to_vec());
        rest = &rest[len..];
    }
    Some(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::LocalFsStore;
    use crate::wal::SegmentReader;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn setup() -> (tempfile::TempDir, Arc<LocalFsStore>, TenantLayout) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFsStore::new(dir.path()));
        (dir, store, TenantLayout::new("t1"))
    }

    fn stored_records(store: &dyn CasStore, layout: &TenantLayout, epoch: u64) -> Vec<Vec<u8>> {
        let prefix = layout.wal_segment(epoch, Lsn(0));
        let prefix = &prefix[..prefix.rfind('/').unwrap() + 1];
        let mut out = Vec::new();
        for key in store.list(prefix).unwrap() {
            let (data, _) = store.get(&key).unwrap().unwrap();
            for frame in SegmentReader::new(&data, epoch) {
                out.extend(split_records(&frame.unwrap().payload).unwrap());
            }
        }
        out
    }

    #[test]
    fn records_round_trip_and_lsns_increase() {
        let (_d, store, layout) = setup();
        let gc = GroupCommit::new(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            3,
            7,
            Lsn(0),
            GroupCommitConfig::default(),
        );

        let records: Vec<Vec<u8>> = (0u8..5).map(|i| vec![i; 10 + i as usize]).collect();
        let mut last = Lsn(0);
        for r in &records {
            let lsn = gc.append(r).unwrap().wait().unwrap();
            assert!(lsn > last, "LSNs must strictly increase");
            last = lsn;
        }
        gc.close().unwrap();

        assert_eq!(stored_records(&*store, &layout, 3), records);
    }

    #[test]
    fn concurrent_small_appends_coalesce_into_few_frames() {
        let (_d, store, layout) = setup();
        let gc = Arc::new(GroupCommit::new(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            1,
            1,
            Lsn(0),
            GroupCommitConfig {
                flush_interval: Duration::from_millis(50),
                ..GroupCommitConfig::default()
            },
        ));

        std::thread::scope(|s| {
            for t in 0..4u8 {
                let gc = Arc::clone(&gc);
                s.spawn(move || {
                    for i in 0..10u8 {
                        gc.append(&[t, i]).unwrap().wait().unwrap();
                    }
                });
            }
        });

        let objects = store.list("tenants/t1/wal/").unwrap();
        assert!(
            objects.len() < 40,
            "40 rapid appends produced {} objects, no coalescing happened",
            objects.len()
        );
        assert_eq!(stored_records(&*store, &layout, 1).len(), 40);
    }

    #[test]
    fn the_size_trigger_flushes_without_waiting_for_the_interval() {
        let (_d, store, layout) = setup();
        let gc = GroupCommit::new(
            store,
            layout,
            1,
            1,
            Lsn(0),
            GroupCommitConfig {
                // The interval alone would hang the test past its timeout,
                // so a returning wait() proves the byte trigger fired.
                flush_interval: Duration::from_secs(3600),
                flush_bytes: 1024,
                ..GroupCommitConfig::default()
            },
        );
        gc.append(&[9u8; 2048]).unwrap().wait().unwrap();
        gc.close().unwrap();
    }

    #[test]
    fn close_flushes_the_pending_tail() {
        let (_d, store, layout) = setup();
        let gc = GroupCommit::new(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            1,
            1,
            Lsn(0),
            GroupCommitConfig {
                flush_interval: Duration::from_secs(3600),
                ..GroupCommitConfig::default()
            },
        );
        let tickets: Vec<CommitTicket> = (0u8..3).map(|i| gc.append(&[i]).unwrap()).collect();
        gc.close().unwrap();
        for t in tickets {
            t.wait().unwrap();
        }
        assert_eq!(stored_records(&*store, &layout, 1).len(), 3);
    }

    /// Blocks every put until released, simulating a stalled object store.
    struct StallStore {
        inner: LocalFsStore,
        stalled: Mutex<bool>,
        released: Condvar,
    }

    impl StallStore {
        fn release(&self) {
            *self.stalled.lock().unwrap() = false;
            self.released.notify_all();
        }
    }

    impl CasStore for StallStore {
        fn get(&self, key: &str) -> Result<Option<(Vec<u8>, crate::cas::Version)>, CasError> {
            self.inner.get(key)
        }
        fn put_if_match(
            &self,
            key: &str,
            data: &[u8],
            expected: Option<&crate::cas::Version>,
        ) -> Result<crate::cas::Version, CasError> {
            let mut stalled = self.stalled.lock().unwrap();
            while *stalled {
                stalled = self.released.wait(stalled).unwrap();
            }
            drop(stalled);
            self.inner.put_if_match(key, data, expected)
        }
        fn delete(&self, key: &str) -> Result<(), CasError> {
            self.inner.delete(key)
        }
        fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
            self.inner.list(prefix)
        }
    }

    #[test]
    fn a_stalled_store_backpressures_producers_and_acks_after_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(StallStore {
            inner: LocalFsStore::new(dir.path()),
            stalled: Mutex::new(true),
            released: Condvar::new(),
        });
        let layout = TenantLayout::new("t1");
        let gc = Arc::new(GroupCommit::new(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            1,
            1,
            Lsn(0),
            GroupCommitConfig {
                flush_interval: Duration::from_millis(1),
                flush_bytes: 64,
                buffer_capacity: 256,
            },
        ));

        // The flusher takes one batch and stalls inside put. The producer
        // then fills the 256 byte buffer with 36 byte framed records and
        // must block partway through its 20 appends: at most one taken
        // batch plus seven buffered records can exist before backpressure.
        let appended = Arc::new(AtomicUsize::new(0));
        let all_acked = Arc::new(AtomicBool::new(false));
        let producer = {
            let gc = Arc::clone(&gc);
            let appended = Arc::clone(&appended);
            let all_acked = Arc::clone(&all_acked);
            std::thread::spawn(move || {
                let tickets: Vec<CommitTicket> = (0u8..20)
                    .map(|i| {
                        let t = gc.append(&[i; 32]).unwrap();
                        appended.fetch_add(1, Ordering::SeqCst);
                        t
                    })
                    .collect();
                for t in tickets {
                    t.wait().unwrap();
                }
                all_acked.store(true, Ordering::SeqCst);
            })
        };

        std::thread::sleep(Duration::from_millis(100));
        let during_stall = appended.load(Ordering::SeqCst);
        assert!(
            during_stall < 20,
            "all 20 appends went through while the store was stalled"
        );
        assert!(!all_acked.load(Ordering::SeqCst), "acked during a stall");
        assert!(
            store.inner.list("tenants/").unwrap().is_empty(),
            "bytes reached the store while it was stalled"
        );

        store.release();
        producer.join().unwrap();
        assert!(all_acked.load(Ordering::SeqCst));
        assert_eq!(stored_records(&store.inner, &layout, 1).len(), 20);
    }

    /// Fails every put, simulating a dead object store.
    struct DeadStore;

    impl CasStore for DeadStore {
        fn get(&self, _key: &str) -> Result<Option<(Vec<u8>, crate::cas::Version)>, CasError> {
            Ok(None)
        }
        fn put_if_match(
            &self,
            key: &str,
            _data: &[u8],
            _expected: Option<&crate::cas::Version>,
        ) -> Result<crate::cas::Version, CasError> {
            Err(CasError::Io {
                key: key.to_string(),
                source: std::io::Error::other("store is down"),
            })
        }
        fn delete(&self, key: &str) -> Result<(), CasError> {
            Err(CasError::Io {
                key: key.to_string(),
                source: std::io::Error::other("store is down"),
            })
        }
        fn list(&self, _prefix: &str) -> Result<Vec<String>, CasError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn a_dead_store_fails_tickets_and_poisons_the_pipeline() {
        let gc = GroupCommit::new(
            Arc::new(DeadStore),
            TenantLayout::new("t1"),
            1,
            1,
            Lsn(0),
            GroupCommitConfig {
                flush_interval: Duration::from_millis(1),
                ..GroupCommitConfig::default()
            },
        );
        let err = gc.append(b"doomed").unwrap().wait().unwrap_err();
        assert!(matches!(err, CommitError::Flush(_)));

        // The pipeline is poisoned, later appends refuse instead of queueing
        // into a buffer nobody will ever drain.
        loop {
            match gc.append(b"after") {
                Err(CommitError::Flush(_)) => break,
                Ok(_ticket) => std::thread::sleep(Duration::from_millis(5)),
                Err(e) => panic!("unexpected: {e}"),
            }
        }
        assert!(gc.close().is_err());
    }

    #[test]
    fn oversized_records_are_rejected_up_front() {
        let (_d, store, layout) = setup();
        let gc = GroupCommit::new(store, layout, 1, 1, Lsn(0), GroupCommitConfig::default());
        let too_big = vec![0u8; MAX_BODY_LEN as usize + 1];
        assert!(matches!(
            gc.append(&too_big),
            Err(CommitError::RecordTooLarge { .. })
        ));
        gc.close().unwrap();
    }

    fn lease_setup(
        store: &Arc<LocalFsStore>,
        layout: &TenantLayout,
        now: u64,
    ) -> Arc<Mutex<HeldLease>> {
        store
            .put_new(
                &layout.manifest(),
                &crate::manifest::Manifest::new("t1", 18).to_json(),
            )
            .unwrap();
        let held = lease::acquire(&**store, layout, "node-a", 15, now).unwrap();
        Arc::new(Mutex::new(held))
    }

    fn manifest_of(store: &dyn CasStore, layout: &TenantLayout) -> crate::manifest::Manifest {
        let (data, _) = store.get(&layout.manifest()).unwrap().unwrap();
        crate::manifest::Manifest::from_json(&data).unwrap()
    }

    #[test]
    fn sealed_segments_are_published_to_the_manifest_tail() {
        let (_d, store, layout) = setup();
        let held = lease_setup(&store, &layout, 1000);
        let epoch = held.lock().unwrap().epoch;
        let gc = GroupCommit::with_lease(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            held,
            Lsn(0),
            GroupCommitConfig::default(),
            TailConfig {
                seal_bytes: 1,
                ..TailConfig::default()
            },
        );

        let records: Vec<Vec<u8>> = (0u8..4).map(|i| vec![i; 50]).collect();
        for r in &records {
            gc.append(r).unwrap().wait().unwrap();
        }
        gc.close().unwrap();

        let tail = manifest_of(&*store, &layout)
            .wal_tail
            .expect("tail published");
        assert_eq!(tail.epoch_dir, epoch);
        assert_eq!(tail.from_lsn, Lsn(0));

        // The manifest alone must be enough to find and replay the WAL,
        // each entry naming its own epoch directory.
        let mut replayed = Vec::new();
        for name in &tail.segments {
            let seg_epoch = segment_epoch(name).expect("epoch qualified");
            assert_eq!(seg_epoch, epoch);
            let (data, _) = store.get(&layout.wal_segment_path(name)).unwrap().unwrap();
            for frame in SegmentReader::new(&data, seg_epoch) {
                replayed.extend(split_records(&frame.unwrap().payload).unwrap());
            }
        }
        assert_eq!(replayed, records);
    }

    #[test]
    fn a_new_session_chains_onto_the_previous_tail() {
        let (_d, store, layout) = setup();
        let held = lease_setup(&store, &layout, 1000);
        let first_epoch = held.lock().unwrap().epoch;
        let gc = GroupCommit::with_lease(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            held,
            Lsn(0),
            GroupCommitConfig::default(),
            TailConfig::default(),
        );
        gc.append(b"first session").unwrap().wait().unwrap();
        gc.close().unwrap();

        // A later session, as after a server restart. The lease above was
        // never released, so steal it past its TTL.
        let manifest = manifest_of(&*store, &layout);
        let prior = reconcile_tail(&*store, &layout, &manifest)
            .unwrap()
            .expect("first session left segments");
        let held = lease::acquire(&*store, &layout, "node-b", 15, 2000).unwrap();
        let second_epoch = held.epoch;
        let gc = GroupCommit::builder(Arc::clone(&store) as Arc<dyn CasStore>, layout.clone())
            .lease(Arc::new(Mutex::new(held)))
            .start_lsn(Lsn(prior.from_lsn.0 + 1000))
            .initial_tail(prior)
            .build();
        gc.append(b"second session").unwrap().wait().unwrap();
        gc.close().unwrap();

        let tail = manifest_of(&*store, &layout)
            .wal_tail
            .expect("tail published");
        assert_eq!(tail.epoch_dir, second_epoch);
        assert_eq!(tail.from_lsn, Lsn(0), "from_lsn chains, it never resets");
        let epochs: Vec<u64> = tail
            .segments
            .iter()
            .map(|s| segment_epoch(s).unwrap())
            .collect();
        assert_eq!(epochs, vec![first_epoch, second_epoch]);

        let mut replayed = Vec::new();
        for name in &tail.segments {
            let (data, _) = store.get(&layout.wal_segment_path(name)).unwrap().unwrap();
            for frame in SegmentReader::new(&data, segment_epoch(name).unwrap()) {
                replayed.extend(split_records(&frame.unwrap().payload).unwrap());
            }
        }
        assert_eq!(
            replayed,
            vec![b"first session".to_vec(), b"second session".to_vec()]
        );
    }

    #[test]
    fn reconcile_finds_segments_the_manifest_never_learned_about() {
        let (_d, store, layout) = setup();
        store
            .put_new(
                &layout.manifest(),
                &crate::manifest::Manifest::new("t1", 18).to_json(),
            )
            .unwrap();

        // A session that crashes before any tail publish: frames exist in
        // the store, the manifest knows nothing.
        let gc = GroupCommit::new(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            4,
            4,
            Lsn(0x500),
            GroupCommitConfig::default(),
        );
        gc.append(b"acked but unpublished").unwrap().wait().unwrap();
        drop(gc);

        let manifest = manifest_of(&*store, &layout);
        assert!(manifest.wal_tail.is_none());
        let tail = reconcile_tail(&*store, &layout, &manifest)
            .unwrap()
            .expect("scan finds the orphan session");
        assert_eq!(tail.segments.len(), 1);
        assert_eq!(segment_epoch(&tail.segments[0]), Some(4));
        assert_eq!(tail.from_lsn, Lsn(0x500));
        assert_eq!(tail.epoch_dir, 4);
    }

    #[test]
    fn reconcile_of_an_empty_store_is_none() {
        let (_d, store, layout) = setup();
        store
            .put_new(
                &layout.manifest(),
                &crate::manifest::Manifest::new("t1", 18).to_json(),
            )
            .unwrap();
        let manifest = manifest_of(&*store, &layout);
        assert!(
            reconcile_tail(&*store, &layout, &manifest)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn close_publishes_the_final_tail_even_below_thresholds() {
        let (_d, store, layout) = setup();
        let held = lease_setup(&store, &layout, 1000);
        let gc = GroupCommit::with_lease(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            held,
            Lsn(0),
            GroupCommitConfig::default(),
            TailConfig::default(),
        );
        for i in 0u8..3 {
            gc.append(&[i]).unwrap().wait().unwrap();
        }
        // Nothing published yet, both thresholds are far away.
        assert!(manifest_of(&*store, &layout).wal_tail.is_none());
        gc.close().unwrap();
        let tail = manifest_of(&*store, &layout)
            .wal_tail
            .expect("published on close");
        assert!(!tail.segments.is_empty());
    }

    #[test]
    fn a_fold_truncates_the_tail_and_publishes_the_checkpoint_atomically() {
        use crate::manifest::{CheckpointKind, CheckpointRef};

        let (_d, store, layout) = setup();
        let held = lease_setup(&store, &layout, 1000);
        let gc = GroupCommit::with_lease(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            held,
            Lsn(0),
            GroupCommitConfig::default(),
            TailConfig {
                seal_bytes: 1,
                ..TailConfig::default()
            },
        );
        for i in 0u8..4 {
            gc.append(&[i; 50]).unwrap().wait().unwrap();
        }
        let before = manifest_of(&*store, &layout).wal_tail.unwrap();
        assert_eq!(before.segments.len(), 4, "one sealed segment per append");

        let checkpoint = CheckpointRef {
            id: "00000000000000aa".into(),
            lsn: Lsn(0xAA),
            kind: CheckpointKind::Delta,
            owner: None,
        };
        gc.fold_tail(checkpoint.clone(), before.segments[..2].to_vec())
            .unwrap();

        // One manifest swap carries both the truncation and the ref.
        let m = manifest_of(&*store, &layout);
        let tail = m.wal_tail.unwrap();
        assert_eq!(tail.segments, before.segments[2..].to_vec());
        assert_eq!(
            tail.from_lsn,
            segment_start_lsn(&before.segments[2]).unwrap()
        );
        assert_eq!(m.checkpoints, vec![checkpoint.clone()]);

        // A retried fold with the same id publishes no duplicate ref.
        gc.fold_tail(checkpoint.clone(), Vec::new()).unwrap();
        assert_eq!(manifest_of(&*store, &layout).checkpoints.len(), 1);

        // A drop list the tail does not start with is rejected, and so is
        // dropping everything, the tail must keep covering the stream.
        let bogus = vec!["0000000000000099/00000000DEADBEEF.wal".to_string()];
        assert!(matches!(
            gc.fold_tail(checkpoint.clone(), bogus),
            Err(CommitError::FoldRejected { .. })
        ));
        let all = manifest_of(&*store, &layout).wal_tail.unwrap().segments;
        assert!(matches!(
            gc.fold_tail(checkpoint.clone(), all),
            Err(CommitError::FoldRejected { .. })
        ));

        // A full ref prunes everything before it, the superseded chain
        // becomes garbage for the gc job.
        let full = CheckpointRef {
            id: "00000000000000bb".into(),
            lsn: Lsn(0xBB),
            kind: CheckpointKind::Full,
            owner: None,
        };
        gc.fold_tail(full.clone(), Vec::new()).unwrap();
        assert_eq!(manifest_of(&*store, &layout).checkpoints, vec![full]);

        // Later appends chain onto the truncated tail, the dropped
        // segments never resurface.
        gc.append(&[9u8; 50]).unwrap().wait().unwrap();
        gc.close().unwrap();
        let final_tail = manifest_of(&*store, &layout).wal_tail.unwrap();
        assert_eq!(final_tail.segments.len(), 3);
        assert!(!final_tail.segments.contains(&before.segments[0]));
        assert_eq!(
            final_tail.from_lsn,
            segment_start_lsn(&before.segments[2]).unwrap()
        );
    }

    #[test]
    fn a_session_pipeline_rejects_folds() {
        use crate::manifest::{CheckpointKind, CheckpointRef};

        let (_d, store, layout) = setup();
        let gc = GroupCommit::new(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout,
            1,
            1,
            Lsn(0),
            GroupCommitConfig::default(),
        );
        let checkpoint = CheckpointRef {
            id: "x".into(),
            lsn: Lsn(0),
            kind: CheckpointKind::Delta,
            owner: None,
        };
        assert!(matches!(
            gc.fold_tail(checkpoint, Vec::new()),
            Err(CommitError::FoldRejected { .. })
        ));
        gc.close().unwrap();
    }

    #[test]
    fn a_stolen_lease_stops_the_pipeline_without_a_fake_ack() {
        let (_d, store, layout) = setup();
        let held = lease_setup(&store, &layout, 1000);
        let gc = GroupCommit::with_lease(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            held,
            Lsn(0),
            GroupCommitConfig::default(),
            TailConfig {
                seal_bytes: 1,
                ..TailConfig::default()
            },
        );

        // node-b steals after the TTL. Our next publish must detect it,
        // and the record flushed alongside it must not be acked: it lives
        // in a dead epoch no successor will ever replay.
        lease::acquire(&*store, &layout, "node-b", 15, 1015).unwrap();
        let err = gc.append(b"zombie").unwrap().wait().unwrap_err();
        assert!(matches!(err, CommitError::LeaseLost), "got: {err}");
        assert!(matches!(gc.append(b"after"), Err(CommitError::LeaseLost)));
        assert!(gc.close().is_err());
    }

    #[test]
    fn the_builder_routes_frames_through_a_custom_target() {
        let main_dir = tempfile::tempdir().unwrap();
        let fast_dir = tempfile::tempdir().unwrap();
        let main = Arc::new(LocalFsStore::new(main_dir.path()));
        let fast = Arc::new(LocalFsStore::new(fast_dir.path()));
        let layout = TenantLayout::new("t1");

        let gc = GroupCommit::builder(Arc::clone(&main) as Arc<dyn CasStore>, layout.clone())
            .session(1, 1)
            .target(Arc::new(crate::tier::ExpressTarget::new(
                Arc::clone(&fast) as Arc<dyn CasStore>
            )))
            .build();
        gc.append(b"fast lane").unwrap().wait().unwrap();
        gc.close().unwrap();

        assert!(main.list("tenants/t1/wal/").unwrap().is_empty());
        assert_eq!(
            stored_records(&*fast, &layout, 1),
            vec![b"fast lane".to_vec()]
        );
    }

    #[test]
    fn split_records_rejects_malformed_framing() {
        assert_eq!(split_records(&[]), Some(vec![]));
        assert_eq!(split_records(&[5, 0, 0, 0, 1, 2]), None);
        assert_eq!(split_records(&[1, 2]), None);
        let mut good = 3u32.to_le_bytes().to_vec();
        good.extend_from_slice(b"abc");
        assert_eq!(split_records(&good), Some(vec![b"abc".to_vec()]));
    }
}
