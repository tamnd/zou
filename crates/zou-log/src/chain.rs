//! The landing segment chain (spec 03 section 5).
//!
//! A shard's WAL is a strictly consecutive chain of fenced objects,
//! `cellwal/<shard>/<seq:016x>`, every one created with put_if_absent.
//! Ownership of the shard is not a lease, it is the chain itself: only
//! one process can ever create seq N, so the head moves under exactly
//! one sequencer no matter what the control plane believes.
//!
//! Takeover is three moves. Probe forward from the manifest's last
//! known position to find the head h, PUT a seal segment at h+1, then
//! CAS the shard manifest to the new epoch and head. The seal is what
//! fences: any in flight PUT the old sequencer still has at h+1 now
//! loses the creation race, comes back AlreadyExists, and poisons that
//! sequencer before it can ack a single byte. The successor resumes at
//! h+2 with the seal's digest as its chain link.
//!
//! The manifest is a hint, never an authority. It CASes so racing
//! successors notice each other, but correctness only ever rests on
//! put_if_absent at the head. A crash between seal and manifest CAS
//! leaves nothing wedged: the next takeover just probes past the seal.

use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use zou_store::{CasError, CasStore, Frame2, Version};

use crate::segment::{
    Footer, SegmentBuilder, SegmentDecodeError, SegmentHeader, SegmentKind, decode_segment,
    read_footer, tenants_digest,
};

pub const SHARD_MANIFEST_FORMAT: u32 = 1;

/// Object key for one chain position.
pub fn segment_key(shard: u32, seq: u64) -> String {
    format!("cellwal/{shard:04x}/{seq:016x}")
}

/// Object key for the shard manifest.
pub fn manifest_key(shard: u32) -> String {
    format!("cellwal/{shard:04x}/manifest")
}

/// Chain state for one shard. This is a recovery hint that bounds the
/// probe, not the source of truth: the chain of fenced objects is the
/// truth, and `head` here may lag it by however long the current
/// sequencer has been running since its takeover.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardManifest {
    pub format: u32,
    pub shard: u32,
    /// Bumped once per takeover. Diagnostic only, the fence is the seal.
    pub chain_epoch: u64,
    /// The seal's seq as of the last takeover.
    pub head: u64,
    /// Everything at or below this seq has been consolidated and may be
    /// deleted, so probes never look at or below it. Zero until the
    /// consolidator exists.
    pub consolidated_upto: u64,
    /// The spec's {sealed_by, node, unix} marker. It rides here instead
    /// of inside the seal object so the seal stays a fixed size segment
    /// and the story has one authoritative copy.
    pub sealed_by: String,
    pub sealed_unix: u64,
}

impl ShardManifest {
    pub fn load(
        store: &dyn CasStore,
        shard: u32,
    ) -> Result<Option<(ShardManifest, Version)>, ChainError> {
        match store.get(&manifest_key(shard))? {
            None => Ok(None),
            Some((bytes, version)) => {
                let manifest: ShardManifest =
                    serde_json::from_slice(&bytes).map_err(|source| ChainError::BadManifest {
                        shard,
                        reason: source.to_string(),
                    })?;
                if manifest.format > SHARD_MANIFEST_FORMAT {
                    return Err(ChainError::BadManifest {
                        shard,
                        reason: format!(
                            "format {} is newer than this zou, upgrade",
                            manifest.format
                        ),
                    });
                }
                Ok(Some((manifest, version)))
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChainError {
    #[error(transparent)]
    Store(#[from] CasError),
    #[error("shard {shard} manifest is unreadable: {reason}")]
    BadManifest { shard: u32, reason: String },
    #[error("segment {seq} on shard {shard} is bad: {source}")]
    Segment {
        shard: u32,
        seq: u64,
        #[source]
        source: SegmentDecodeError,
    },
    #[error("segment {seq} on shard {shard} does not link to its predecessor")]
    BrokenLink { shard: u32, seq: u64 },
    #[error("shard {shard} takeover lost {attempts} races in a row, another node is taking over")]
    Contested { shard: u32, attempts: u32 },
}

/// Where the successor picks up. Feed `next_seq` and `prev_digest`
/// straight into [`crate::Sequencer::resume`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Takeover {
    pub chain_epoch: u64,
    /// The seal's position.
    pub sealed_seq: u64,
    pub next_seq: u64,
    pub prev_digest: u64,
}

fn exists(store: &dyn CasStore, shard: u32, seq: u64) -> Result<bool, CasError> {
    Ok(store.get_range(&segment_key(shard, seq), 0, 1)?.is_some())
}

/// Find the chain head by probing forward from a known floor. The
/// chain is gap free, presence is a prefix, so gallop then binary
/// search: log cost in the distance since the manifest was last
/// written, no LIST anywhere near the hot path.
pub fn chain_head(store: &dyn CasStore, shard: u32, floor: u64) -> Result<u64, ChainError> {
    if !exists(store, shard, floor + 1)? {
        return Ok(floor);
    }
    // Gallop: find a missing seq to bound the search.
    let mut lo = floor + 1; // known present
    let mut step = 1u64;
    let hi = loop {
        let probe = lo.saturating_add(step);
        if !exists(store, shard, probe)? {
            break probe; // known absent
        }
        lo = probe;
        step = step.saturating_mul(2);
    };
    // Binary search the boundary in (lo, hi).
    let (mut lo, mut hi) = (lo, hi);
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if exists(store, shard, mid)? {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn digest_of_segment(store: &dyn CasStore, shard: u32, seq: u64) -> Result<u64, ChainError> {
    if seq == 0 {
        return Ok(0);
    }
    let bytes = store
        .get(&segment_key(shard, seq))?
        .ok_or(ChainError::BrokenLink { shard, seq })?
        .0;
    let (_, footer) =
        read_footer(&bytes).map_err(|source| ChainError::Segment { shard, seq, source })?;
    Ok(tenants_digest(&footer.tenants))
}

/// Take over a shard: fence whoever holds it now and return the
/// position for [`crate::Sequencer::resume`]. Safe to call any time,
/// from anywhere, against any number of rivals. Losing a race to a
/// rival just retries against the new head, and after a few straight
/// losses it returns Contested so a confused control plane backs off
/// instead of dueling forever.
pub fn take_over(store: &dyn CasStore, shard: u32, node: &str) -> Result<Takeover, ChainError> {
    const MAX_ATTEMPTS: u32 = 8;
    for _ in 0..MAX_ATTEMPTS {
        let loaded = ShardManifest::load(store, shard)?;
        let (manifest, version) = match &loaded {
            Some((m, v)) => (Some(m), Some(v)),
            None => (None, None),
        };
        let floor = manifest.map_or(0, |m| m.head.max(m.consolidated_upto));
        let head = chain_head(store, shard, floor)?;

        // The seal links to the head like any other segment, so a
        // reader walking the chain crosses takeovers without a special
        // case.
        let prev_digest = digest_of_segment(store, shard, head)?;
        let sealed_seq = head + 1;
        let builder = SegmentBuilder::new(SegmentHeader {
            kind: SegmentKind::Seal,
            shard,
            seq: sealed_seq,
            prev_digest,
        });
        let (seal, summaries) = builder.finish();
        match store.put_if_absent(&segment_key(shard, sealed_seq), &seal) {
            Ok(_) => {}
            // The head moved, either the incumbent landed another
            // window or a rival sealed first. Probe again.
            Err(CasError::AlreadyExists { .. }) => continue,
            Err(e) => return Err(e.into()),
        }

        let next = ShardManifest {
            format: SHARD_MANIFEST_FORMAT,
            shard,
            chain_epoch: manifest.map_or(0, |m| m.chain_epoch) + 1,
            head: sealed_seq,
            consolidated_upto: manifest.map_or(0, |m| m.consolidated_upto),
            sealed_by: node.to_string(),
            sealed_unix: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
        };
        let body = serde_json::to_vec_pretty(&next).expect("shard manifest serializes");
        match store.put_if_match(&manifest_key(shard), &body, version) {
            Ok(_) => {
                return Ok(Takeover {
                    chain_epoch: next.chain_epoch,
                    sealed_seq,
                    next_seq: sealed_seq + 1,
                    prev_digest: tenants_digest(&summaries),
                });
            }
            // A rival swapped the manifest between our load and here.
            // Our seal still fenced whoever held the chain, but the
            // rival may have sealed after us and own the head now, so
            // start over and fight for the current head.
            Err(CasError::Conflict { .. }) => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Err(ChainError::Contested {
        shard,
        attempts: MAX_ATTEMPTS,
    })
}

/// One decoded chain position.
#[derive(Debug, Clone, PartialEq)]
pub struct ChainSegment {
    pub seq: u64,
    pub header: SegmentHeader,
    pub frames: Vec<Frame2>,
    pub footer: Footer,
}

/// Read the chain from `from` (exclusive) to the current head,
/// verifying every digest link on the way. This is the recovery read:
/// a half landed zombie PUT past the true head fails the link check
/// and is ignored, exactly the chain rule in spec 03 section 5.
pub fn read_chain(
    store: &dyn CasStore,
    shard: u32,
    from: u64,
) -> Result<Vec<ChainSegment>, ChainError> {
    let mut prev_digest = digest_of_segment(store, shard, from)?;
    let mut out = Vec::new();
    let mut seq = from + 1;
    while let Some((bytes, _)) = store.get(&segment_key(shard, seq))? {
        let (header, frames, footer) =
            decode_segment(&bytes).map_err(|source| ChainError::Segment { shard, seq, source })?;
        if header.shard != shard || header.seq != seq || header.prev_digest != prev_digest {
            return Err(ChainError::BrokenLink { shard, seq });
        }
        prev_digest = tenants_digest(&footer.tenants);
        out.push(ChainSegment {
            seq,
            header,
            frames,
            footer,
        });
        seq += 1;
    }
    Ok(out)
}
