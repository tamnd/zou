//! Durability modes (spec 03 section 4).
//!
//! The mode is a property of the cell, not of the code: every mode
//! shares the same segments, the same sequencer, and the same chain
//! protocol, and only the media a window must reach before an ack
//! changes. `standard` is one PUT to one bucket, the mode for R2, GCS,
//! B2, MinIO, or anyone who does not want an Express dependency.
//! `express-dual` is the low latency mode: PUT to two Express buckets
//! in different AZs in parallel, and if one is still outstanding after
//! the hedge delay, PUT to the Standard bucket too and ack as soon as
//! any two media hold the segment.
//!
//! Express dual is a strict two of three quorum, which is a deliberate
//! tightening of the spec's chain rule. Each medium can award a seq to
//! exactly one writer, put_if_absent sees to that, an ack needs two
//! media, and a seal needs two media, so two owners of one seq would
//! need four wins from three media. Exactly one owner per seq is
//! arithmetic, not protocol. The payoff is that recovery is presence
//! only: a seq is in the chain iff two media hold identical bytes for
//! it, and the spec's "referenced by the next seq" arm never has to
//! break a tie, because no tie can form.
//!
//! The rules per role:
//!
//! - Writer: ack when two media hold the segment. AlreadyExists from
//!   any medium fails the batch immediately, someone is fencing this
//!   sequencer and the honest move is stepping down, not hedging past
//!   the seal. The hedge fires on slowness or an io error, never on a
//!   lost race.
//! - Sealer: a seal that wins two media fences. One that cannot reach
//!   two media lost to a fully landed segment, so the takeover adopts
//!   that segment as the head and seals the next seq instead.
//! - Reader: the owner of a seq is the byte string two media agree on.
//!   A half landed loser sitting on one medium is invisible.

use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use zou_store::{CasError, CasStore};

use crate::chain::segment_key;
use crate::sequencer::SegmentSink;

/// How long the dual writer waits on the second AZ before paying for a
/// third PUT to Standard. Spec 03 section 4 sets this at 15 ms, about
/// two Express p99s past the point where the slow AZ should have
/// answered.
pub const DEFAULT_HEDGE_AFTER: Duration = Duration::from_millis(15);

/// The cell setting. Stored in the cell manifest, so it serializes as
/// the strings the spec uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DurabilityMode {
    Standard,
    ExpressDual,
}

/// What the seal PUT found at a chain position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealOutcome {
    /// The seal owns the seq, the old sequencer is fenced.
    Sealed,
    /// A fully landed segment owns the seq, adopt it and seal the next.
    Occupied,
}

/// The media a shard's WAL lands on, chosen by [`DurabilityMode`].
/// Everything the chain protocol does to the store goes through here,
/// so the sequencer, takeover, and recovery are mode blind.
pub enum WalMedia {
    /// One bucket, one PUT, the `standard` mode.
    Single(Arc<dyn CasStore>),
    /// Two Express buckets in different AZs plus the Standard bucket as
    /// the hedge medium and the manifest home, the `express-dual` mode.
    Dual {
        az1: Arc<dyn CasStore>,
        az2: Arc<dyn CasStore>,
        standard: Arc<dyn CasStore>,
        hedge_after: Duration,
    },
}

impl WalMedia {
    pub fn single(store: Arc<dyn CasStore>) -> Self {
        WalMedia::Single(store)
    }

    pub fn express_dual(
        az1: Arc<dyn CasStore>,
        az2: Arc<dyn CasStore>,
        standard: Arc<dyn CasStore>,
        hedge_after: Duration,
    ) -> Self {
        WalMedia::Dual {
            az1,
            az2,
            standard,
            hedge_after,
        }
    }

    /// Where mutable chain state lives. Express buckets are fast and
    /// expensive, the manifest is small and swapped rarely, so in dual
    /// mode it lives on Standard.
    pub fn manifest_store(&self) -> &Arc<dyn CasStore> {
        match self {
            WalMedia::Single(store) => store,
            WalMedia::Dual { standard, .. } => standard,
        }
    }

    /// The writer rule: return only once the segment is durable under
    /// this mode's ack rule, error out the moment anyone else's fence
    /// shows up.
    pub fn put_segment(&self, shard: u32, seq: u64, segment: &[u8]) -> Result<(), CasError> {
        let key = segment_key(shard, seq);
        match self {
            WalMedia::Single(store) => store.put_if_absent(&key, segment).map(|_| ()),
            WalMedia::Dual {
                az1,
                az2,
                standard,
                hedge_after,
            } => put_quorum(az1, az2, standard, *hedge_after, &key, segment),
        }
    }

    /// The sealer rule: try to own `seq` with `seal`. Sealed means the
    /// fence is up. Occupied means a fully landed segment owns the seq
    /// and the takeover should adopt it and move on.
    pub fn seal(&self, shard: u32, seq: u64, seal: &[u8]) -> Result<SealOutcome, CasError> {
        let key = segment_key(shard, seq);
        match self {
            WalMedia::Single(store) => match store.put_if_absent(&key, seal) {
                Ok(_) => Ok(SealOutcome::Sealed),
                Err(CasError::AlreadyExists { .. }) => Ok(SealOutcome::Occupied),
                Err(e) => Err(e),
            },
            WalMedia::Dual {
                az1, az2, standard, ..
            } => {
                let mut wins = 0;
                for store in [az1, az2] {
                    match store.put_if_absent(&key, seal) {
                        Ok(_) => wins += 1,
                        Err(CasError::AlreadyExists { .. }) => {}
                        Err(e) => return Err(e),
                    }
                }
                match wins {
                    2 => Ok(SealOutcome::Sealed),
                    // One AZ lost to someone. The standard medium is
                    // the tie breaker: whoever takes it holds two of
                    // three and owns the seq.
                    1 => match standard.put_if_absent(&key, seal) {
                        Ok(_) => Ok(SealOutcome::Sealed),
                        Err(CasError::AlreadyExists { .. }) => Ok(SealOutcome::Occupied),
                        Err(e) => Err(e),
                    },
                    _ => Ok(SealOutcome::Occupied),
                }
            }
        }
    }

    /// The reader rule for probes: does an owner hold this seq. Cheap,
    /// header sized range reads only.
    pub fn present(&self, shard: u32, seq: u64) -> Result<bool, CasError> {
        let key = segment_key(shard, seq);
        match self {
            WalMedia::Single(store) => Ok(store.get_range(&key, 0, 1)?.is_some()),
            WalMedia::Dual {
                az1, az2, standard, ..
            } => {
                let a = az1.get_range(&key, 0, 28)?;
                let b = az2.get_range(&key, 0, 28)?;
                match (&a, &b) {
                    (Some(x), Some(y)) if x == y => Ok(true),
                    (None, None) => Ok(false),
                    // One medium, or two that disagree: the standard
                    // medium decides whether either candidate has a
                    // second vote.
                    _ => match standard.get_range(&key, 0, 28)? {
                        Some(s) => Ok(a.as_deref() == Some(&s[..]) || b.as_deref() == Some(&s[..])),
                        None => Ok(false),
                    },
                }
            }
        }
    }

    /// The GC rule: remove a consolidated seq from every medium it
    /// could sit on, half landed losers included. Deleting a missing
    /// key is a no op, so reruns after a crash converge.
    pub fn delete_segment(&self, shard: u32, seq: u64) -> Result<(), CasError> {
        let key = segment_key(shard, seq);
        match self {
            WalMedia::Single(store) => store.delete(&key),
            WalMedia::Dual {
                az1, az2, standard, ..
            } => {
                az1.delete(&key)?;
                az2.delete(&key)?;
                standard.delete(&key)
            }
        }
    }

    /// The reader rule for recovery: the owner's bytes, or None if no
    /// owner holds the seq. A half landed loser on one medium never
    /// comes back from here.
    pub fn fetch(&self, shard: u32, seq: u64) -> Result<Option<Vec<u8>>, CasError> {
        let key = segment_key(shard, seq);
        match self {
            WalMedia::Single(store) => Ok(store.get(&key)?.map(|(bytes, _)| bytes)),
            WalMedia::Dual {
                az1, az2, standard, ..
            } => {
                let a = az1.get(&key)?.map(|(bytes, _)| bytes);
                let b = az2.get(&key)?.map(|(bytes, _)| bytes);
                if let (Some(x), Some(y)) = (&a, &b)
                    && x == y
                {
                    return Ok(a);
                }
                if a.is_none() && b.is_none() {
                    return Ok(None);
                }
                let s = standard.get(&key)?.map(|(bytes, _)| bytes);
                if s.is_some() && s == a {
                    return Ok(a);
                }
                if s.is_some() && s == b {
                    return Ok(b);
                }
                Ok(None)
            }
        }
    }
}

/// The dual writer: both AZ PUTs in parallel, ack on any two media,
/// hedge to Standard after the delay or the first io error, fail fast
/// on any fence.
fn put_quorum(
    az1: &Arc<dyn CasStore>,
    az2: &Arc<dyn CasStore>,
    standard: &Arc<dyn CasStore>,
    hedge_after: Duration,
    key: &str,
    segment: &[u8],
) -> Result<(), CasError> {
    let bytes: Arc<[u8]> = segment.into();
    let key: Arc<str> = key.into();
    let (tx, rx) = mpsc::channel();
    let launch = |store: &Arc<dyn CasStore>| {
        let store = Arc::clone(store);
        let bytes = Arc::clone(&bytes);
        let key = Arc::clone(&key);
        let tx = tx.clone();
        thread::spawn(move || {
            let _ = tx.send(store.put_if_absent(&key, &bytes));
        });
    };
    launch(az1);
    launch(az2);

    let deadline = Instant::now() + hedge_after;
    let mut outstanding = 2;
    let mut successes = 0;
    let mut hedged = false;
    let mut first_err = None;
    while outstanding > 0 {
        let result = if hedged {
            rx.recv().expect("a launched put always reports")
        } else {
            match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(result) => result,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    launch(standard);
                    outstanding += 1;
                    hedged = true;
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    unreachable!("a launched put always reports")
                }
            }
        };
        outstanding -= 1;
        match result {
            Ok(_) => {
                successes += 1;
                if successes >= 2 {
                    return Ok(());
                }
            }
            // Someone else's object at this seq. That is a fence, and
            // hedging around a fence would be writing into a chain
            // this sequencer no longer owns.
            Err(e @ CasError::AlreadyExists { .. }) => return Err(e),
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
                if !hedged {
                    launch(standard);
                    outstanding += 1;
                    hedged = true;
                }
            }
        }
    }
    Err(first_err.expect("the quorum failed, so some put errored"))
}

/// The one sink real deployments use: a [`Sequencer`](crate::Sequencer)
/// over whatever media the cell's durability mode picked.
pub struct MediaSink {
    media: Arc<WalMedia>,
    shard: u32,
    /// The seq of the seal this sink's takeover wrote, which is its
    /// claim on the shard. Zero for a sink with no takeover behind it,
    /// which owns nothing and treats every taken seq as a fence.
    sealed_seq: u64,
}

impl MediaSink {
    pub fn new(media: Arc<WalMedia>, shard: u32, sealed_seq: u64) -> Self {
        Self {
            media,
            shard,
            sealed_seq,
        }
    }
}

impl SegmentSink for MediaSink {
    fn put_segment(&self, seq: u64, segment: &[u8]) -> Result<(), CasError> {
        match self.media.put_segment(self.shard, seq, segment) {
            // A taken seq is usually the fence and the end of this
            // sequencer. It is litter instead when a writer this sink's
            // own takeover fenced was frozen rather than killed, and its
            // in flight PUTs landed after the sweep. Clearing it is the
            // owner's job, since nobody else can know it is litter.
            Err(CasError::AlreadyExists { key }) => {
                match crate::chain::straggler_at(&self.media, self.shard, seq, self.sealed_seq) {
                    Ok(true) => {
                        log::warn!(
                            "shard {} seq {seq} held by a fenced writer's late put, clearing it",
                            self.shard
                        );
                        self.media.delete_segment(self.shard, seq)?;
                        self.media.put_segment(self.shard, seq, segment)
                    }
                    Ok(false) => Err(CasError::AlreadyExists { key }),
                    // Unreadable is not ours to clear, so it fences us
                    // like anything else we did not write.
                    Err(e) => {
                        log::warn!(
                            "shard {} seq {seq} is taken and unreadable: {e}",
                            self.shard
                        );
                        Err(CasError::AlreadyExists { key })
                    }
                }
            }
            other => other,
        }
    }
}
