//! The tee (spec 03 section 8).
//!
//! The sequencer streams every admitted frame, after durability, to
//! subscribers: page service shards, read replicas, CDC. A
//! subscription carries a filter, either a tenant's full stream or one
//! page shard's stripe of it selected by the frames' block ref hints,
//! so a 64 shard tenant costs each subscriber a 64th of the bandwidth,
//! not 64 copies. Frames without hints fall back to full delivery and
//! the subscriber filters after parsing.
//!
//! The tee is best effort and durability never depends on it. Publish
//! never blocks the flush path: every subscription has a byte budget,
//! and one that falls behind it is cut with a [`TeeEvent::Lagged`]
//! marker instead of stalling commits. A cut subscriber catches up
//! from storage and rejoins.
//!
//! The rejoin protocol that loses nothing: subscribe first, then run
//! [`catch_up`] from your applied lsn, then consume live events
//! dropping anything at or below the watermark you caught up to.
//! Frames are lsn identified, so the overlap between the catch up
//! read and the first live windows dedupes exactly.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};

use zou_store::{Frame2, Lsn};

use crate::chain::{ChainCursor, ChainError, ShardManifest, walk_chain_linked};
use crate::consolidate::{ConsolidateError, RoundIndex, read_round_tenant};
use crate::media::WalMedia;

/// The default subscription budget from the spec: fall more than this
/// many frame bytes behind and the tee cuts you to catch up mode.
pub const DEFAULT_TEE_BUFFER: usize = 64 << 20;

/// What one subscription wants out of the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeeFilter {
    /// Every frame of one tenant: read replicas, CDC.
    Tenant(u128),
    /// One page shard's stripe of a tenant, selected by block ref
    /// hints (spec 04 section 1). A frame without hints matches every
    /// stripe, full stream fallback, and the subscriber filters after
    /// it parses the records.
    PageShard {
        tenant: u128,
        shard: u32,
        shard_count: u32,
    },
}

impl TeeFilter {
    pub fn tenant(&self) -> u128 {
        match *self {
            TeeFilter::Tenant(t) => t,
            TeeFilter::PageShard { tenant, .. } => tenant,
        }
    }

    pub fn matches(&self, frame: &Frame2) -> bool {
        match *self {
            TeeFilter::Tenant(t) => frame.tenant == t,
            TeeFilter::PageShard {
                tenant,
                shard,
                shard_count,
            } => {
                frame.tenant == tenant
                    && (frame.hints.is_empty()
                        || frame
                            .hints
                            .iter()
                            .any(|h| h.page_shard(shard_count) == shard))
            }
        }
    }
}

/// What a subscription receives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeeEvent {
    /// The matching frames of one durable window. Never empty.
    Window { seq: u64, frames: Vec<Frame2> },
    /// The subscription fell past its byte budget and was cut, this is
    /// its last event. `next_seq` is the first chain position it was
    /// not delivered: catch up from storage and resubscribe.
    Lagged { next_seq: u64 },
}

fn event_bytes(frames: &[Frame2]) -> usize {
    frames.iter().map(|f| f.payload.len() + 64).sum()
}

struct SubEntry {
    filter: TeeFilter,
    tx: Sender<TeeEvent>,
    used: Arc<AtomicUsize>,
    budget: usize,
}

/// The fan out point. The sequencer publishes every durable window
/// into it, subscribers take filtered streams out of it.
#[derive(Default)]
pub struct Tee {
    subs: Mutex<Vec<SubEntry>>,
}

impl std::fmt::Debug for Tee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tee")
            .field("subscribers", &self.subscriber_count())
            .finish()
    }
}

impl Tee {
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a subscription. Delivery starts with the next window
    /// published, nothing is replayed: pair with [`catch_up`] for
    /// history.
    pub fn subscribe(&self, filter: TeeFilter, budget: usize) -> TeeSubscription {
        let (tx, rx) = channel();
        let used = Arc::new(AtomicUsize::new(0));
        self.subs.lock().unwrap().push(SubEntry {
            filter,
            tx,
            used: Arc::clone(&used),
            budget,
        });
        TeeSubscription { rx, used }
    }

    pub fn subscriber_count(&self) -> usize {
        self.subs.lock().unwrap().len()
    }

    /// Fan one durable window out. Called by the sequencer after the
    /// landing PUT returns, never before: an unacked frame must not be
    /// observable anywhere. Never blocks: a subscription without room
    /// gets cut with Lagged, and a dropped receiver is detached
    /// silently.
    pub fn publish(&self, seq: u64, frames: &[Frame2]) {
        let mut subs = self.subs.lock().unwrap();
        subs.retain(|sub| {
            let matching: Vec<Frame2> = frames
                .iter()
                .filter(|f| sub.filter.matches(f))
                .cloned()
                .collect();
            if matching.is_empty() {
                return true;
            }
            let bytes = event_bytes(&matching);
            if sub.used.load(Ordering::Acquire) + bytes > sub.budget {
                let _ = sub.tx.send(TeeEvent::Lagged { next_seq: seq });
                return false;
            }
            sub.used.fetch_add(bytes, Ordering::AcqRel);
            sub.tx
                .send(TeeEvent::Window {
                    seq,
                    frames: matching,
                })
                .is_ok()
        });
    }
}

/// The receiving end. Dropping it detaches from the tee on the next
/// publish.
pub struct TeeSubscription {
    rx: Receiver<TeeEvent>,
    used: Arc<AtomicUsize>,
}

impl TeeSubscription {
    /// Block for the next event. None once the subscription was cut or
    /// the tee is gone.
    pub fn recv(&self) -> Option<TeeEvent> {
        let event = self.rx.recv().ok()?;
        if let TeeEvent::Window { frames, .. } = &event {
            self.used.fetch_sub(event_bytes(frames), Ordering::AcqRel);
        }
        Some(event)
    }

    /// Non blocking variant of [`TeeSubscription::recv`].
    pub fn try_recv(&self) -> Option<TeeEvent> {
        let event = self.rx.try_recv().ok()?;
        if let TeeEvent::Window { frames, .. } = &event {
            self.used.fetch_sub(event_bytes(frames), Ordering::AcqRel);
        }
        Some(event)
    }
}

/// Read everything a filter's tenant has durable past `applied` out of
/// storage: the sealed rounds first, planned from round indexes with
/// whole rounds skipped by watermark, then the landing tail past the
/// consolidated boundary. Frames come back in lsn order. This is the
/// catch up path for a subscriber that fell behind or just arrived,
/// and it costs range GETs plus the landing tail, no LIST.
/// The durable end of one tenant's stream: the newest round's watermark
/// for the tenant joined with the landing tail past the consolidated
/// boundary. None when the tenant has never appended. Costs one shard
/// manifest GET, at most one round index GET, and the landing chain,
/// with no LIST and no sealed object reads.
pub fn stream_end(
    media: &WalMedia,
    wal_shard: u32,
    tenant: u128,
) -> Result<Option<Lsn>, ConsolidateError> {
    let store = media.manifest_store();
    let Some((manifest, _)) = ShardManifest::load(store.as_ref(), wal_shard)? else {
        return Ok(None);
    };
    let mut end: Option<Lsn> = None;
    if let Some(rounds) = manifest.rounds {
        // The newest round's index carries every tenant's watermark
        // forward, so one GET answers for the whole sealed history.
        let index = RoundIndex::load(store.as_ref(), wal_shard, rounds.last)?.ok_or(
            ConsolidateError::BadRound {
                shard: wal_shard,
                round: rounds.last,
                reason: "a retained round index is missing".to_string(),
            },
        )?;
        for t in &index.tenants {
            if t.tenant == tenant {
                end = Some(Lsn(t.watermark));
            }
        }
    }
    // Walked rather than collected: the tail past the boundary is
    // every segment written since the last fold, which on a box under
    // a bulk load is tens of thousands of them, and holding all of
    // that parsed in memory to look at one number per frame is how a
    // page service poll turns into gigabytes of resident set.
    walk_chain_linked::<ConsolidateError, _>(
        media,
        wal_shard,
        ChainCursor {
            seq: manifest.consolidated_upto + 1,
            prev_digest: manifest.consolidated_digest,
        },
        |segment| {
            for frame in segment.frames {
                if frame.tenant == tenant && end.is_none_or(|e| frame.end_lsn > e) {
                    end = Some(frame.end_lsn);
                }
            }
            Ok(true)
        },
    )?;
    Ok(end)
}

pub fn catch_up(
    media: &WalMedia,
    wal_shard: u32,
    filter: &TeeFilter,
    applied: Lsn,
) -> Result<Vec<Frame2>, ConsolidateError> {
    let mut out = Vec::new();
    catch_up_with::<ConsolidateError, _>(media, wal_shard, filter, applied, |frame| {
        out.push(frame);
        Ok(())
    })?;
    Ok(out)
}

/// Streaming form of [`catch_up`]: each frame goes to `sink` as it is
/// read, in lsn order, so a caller replaying a long history never holds
/// more than one round in memory. The sink's error type only has to
/// absorb [`ConsolidateError`], so a caller with its own error can pass
/// it straight through.
pub fn catch_up_with<E, F>(
    media: &WalMedia,
    wal_shard: u32,
    filter: &TeeFilter,
    applied: Lsn,
    mut sink: F,
) -> Result<(), E>
where
    E: From<ConsolidateError>,
    F: FnMut(Frame2) -> Result<(), E>,
{
    let mut cursor = CatchUpCursor::default();
    catch_up_resuming(
        media,
        wal_shard,
        filter,
        applied,
        &mut cursor,
        |frame| sink(frame).map(|()| true),
        || true,
    )?;
    Ok(())
}

/// A [`ChainError`] on the way out of the walk, or the caller's own
/// error on the way out of the sink. The walk wants one error type and
/// the caller only promised to absorb a [`ConsolidateError`], so this
/// carries both across and unwraps on the other side.
enum WalkError<E> {
    Chain(ChainError),
    Sink(E),
}

impl<E> From<ChainError> for WalkError<E> {
    fn from(e: ChainError) -> Self {
        Self::Chain(e)
    }
}

/// Where a catch up stopped, so the next one starts there instead of
/// paying again for everything it has already read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CatchUpCursor {
    /// The next sealed round to scan. None starts at the oldest round
    /// the shard still retains.
    pub round: Option<u64>,
    /// Where the landing tail walk stopped.
    pub chain: Option<ChainCursor>,
}

/// What one catch up covered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CatchUp {
    /// True when the walk reached the head of the chain, so everything
    /// the tenant had durable at that moment went to the sink.
    pub caught_up: bool,
    /// The highest end lsn this call saw for the filter's tenant, over
    /// the rounds it read indexes for and the tail it walked. The
    /// durable end of the stream when `caught_up`, a lower bound on it
    /// otherwise.
    pub end: Option<Lsn>,
    /// Round indexes read, tail segments walked, and frames handed to
    /// the sink, for a caller that wants to log its own progress.
    pub rounds: u32,
    pub segments: u32,
    pub frames: u64,
}

/// [`catch_up_with`] that resumes and can stop early.
///
/// Reading starts at the oldest retained round and the consolidated
/// boundary, and neither moves except when a fold runs, so a service
/// polling a live stream reads every round index and every segment
/// written since the last fold on every poll. On a box pushing a few
/// thousand segments a minute that is thousands of GETs per poll for
/// the few that are new. `cursor` fixes it: pass the same one back and
/// the scan picks up at the first round and the first segment nobody
/// has read. A fold that retires a round or moves the boundary past
/// the cursor invalidates it, and this notices and starts again from
/// where the frames it would have missed now live.
///
/// Two ways to stop early. The sink says whether to keep going, and
/// false stops after the frame it is on. `more` is asked between
/// rounds and between segments whatever the sink said, which is what
/// bounds a poll that is reading a long run of rounds and segments
/// holding nothing for this tenant: the sink never sees those, so a
/// deadline that only reaches the walk through the sink does not bind
/// on them at all. `caught_up` false out of this means it stopped with
/// work left, so a caller sharing the thread with something else, like
/// a page service answering reads, can bound one poll and come back
/// for the rest.
pub fn catch_up_resuming<E, F, G>(
    media: &WalMedia,
    wal_shard: u32,
    filter: &TeeFilter,
    applied: Lsn,
    cursor: &mut CatchUpCursor,
    mut sink: F,
    mut more: G,
) -> Result<CatchUp, E>
where
    E: From<ConsolidateError>,
    F: FnMut(Frame2) -> Result<bool, E>,
    G: FnMut() -> bool,
{
    let store = media.manifest_store();
    let mut out = CatchUp::default();
    let Some((manifest, _)) = ShardManifest::load(store.as_ref(), wal_shard)
        .map_err(|e| E::from(ConsolidateError::from(e)))?
    else {
        out.caught_up = true;
        return Ok(out);
    };
    let tenant = filter.tenant();
    let mut keep = true;
    let mut stopped = false;
    if let Some(rounds) = manifest.rounds {
        // A cursor below the oldest retained round is one retention
        // has dropped out from under, so start where the history now
        // starts.
        let mut at = cursor.round.unwrap_or(rounds.first).max(rounds.first);
        while at <= rounds.last {
            let index = RoundIndex::load(store.as_ref(), wal_shard, at)
                .map_err(E::from)?
                .ok_or(ConsolidateError::BadRound {
                    shard: wal_shard,
                    round: at,
                    reason: "a retained round index is missing".to_string(),
                })
                .map_err(E::from)?;
            out.rounds += 1;
            // Every round index carries the tenant watermarks forward,
            // so this is both the freshness test and the sealed half
            // of the durable end, for one small GET and no sealed
            // object reads.
            let watermark = index
                .tenants
                .iter()
                .find(|t| t.tenant == tenant)
                .map(|t| Lsn(t.watermark));
            out.end = out.end.max(watermark);
            if watermark.is_some_and(|w| w > applied) {
                for frame in read_round_tenant(store.as_ref(), &index, tenant).map_err(E::from)? {
                    if frame.end_lsn > applied && filter.matches(&frame) {
                        out.frames += 1;
                        keep &= sink(frame)?;
                    }
                }
                // Stopping mid round leaves the cursor on it: the next
                // call rescans it against a higher `applied` and skips
                // what it has already handed over.
                if !keep {
                    break;
                }
            }
            at += 1;
            // Asked after the round, not before it, so a poll that
            // starts with its slice already spent still does one round
            // of work rather than none at all.
            if !more() {
                stopped = true;
                break;
            }
        }
        cursor.round = Some(at);
        if !keep || stopped {
            return Ok(out);
        }
    }
    // A cursor at or below the boundary is one a fold has read out from
    // under, and everything under it is in the rounds above by now.
    let start = match cursor.chain {
        Some(c) if c.seq > manifest.consolidated_upto => c,
        _ => ChainCursor {
            seq: manifest.consolidated_upto + 1,
            prev_digest: manifest.consolidated_digest,
        },
    };
    let walked = walk_chain_linked::<WalkError<E>, _>(media, wal_shard, start, |segment| {
        out.segments += 1;
        for frame in segment.frames {
            if frame.tenant == tenant {
                out.end = out.end.max(Some(frame.end_lsn));
                if frame.end_lsn > applied && filter.matches(&frame) {
                    out.frames += 1;
                    keep &= sink(frame).map_err(WalkError::Sink)?;
                }
            }
        }
        let go = keep && more();
        stopped |= !go;
        Ok(go)
    });
    match walked {
        Ok(end) => {
            cursor.chain = Some(end);
            out.caught_up = !stopped;
            Ok(out)
        }
        Err(WalkError::Chain(e)) => Err(E::from(ConsolidateError::from(e))),
        Err(WalkError::Sink(e)) => Err(e),
    }
}
