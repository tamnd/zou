//! Consolidation (spec 03 section 7).
//!
//! Landing segments are tiny and sit on expensive storage, so a
//! background pass folds everything past the consolidated boundary
//! into one sealed segment on cheap storage, sorted by (tenant, lsn),
//! and publishes a round index saying where every tenant's bytes went.
//! Per tenant WAL access after that is round index to range GET, no
//! per tenant objects and no LIST anywhere.
//!
//! The commit protocol is the manifest CAS, everything before it is
//! create only. A round writes the sealed segment, then the round
//! index, then CASes the shard manifest to move `consolidated_upto`
//! and append the round. Crash anywhere before the CAS and the next
//! run either adopts the finished round object it finds at the next
//! round number or collides harmlessly on identical create only
//! objects. The manifest also records the digest at the consolidated
//! boundary, which is what lets GC delete landing objects underneath
//! it without breaking the chain link for takeover or recovery.
//!
//! The round index carries the full per tenant durable watermark map
//! forward every round, so the newest round answers any tenant's
//! watermark in one GET. Watermarks are also the continuity check: a
//! tenant's frames must resume exactly where the last round left off,
//! frames wholly below the watermark are dropped as writer retry
//! duplicates, and a gap or a partial overlap refuses the round, that
//! is data loss or a sequencer bug and consolidating over it would
//! bake it in.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use zou_store::{CasError, CasStore, Frame2, Frame2Stream};

use crate::chain::{
    ChainError, RoundRange, SHARD_MANIFEST_FORMAT, ShardManifest, chain_head, digest_of_segment,
    read_chain_linked,
};
use crate::media::WalMedia;
use crate::sealed::{SEALED_CHUNK_TARGET, SealedHeader, build_sealed, sealed_key};
use crate::segment::tenants_digest;

pub const ROUND_INDEX_FORMAT: u32 = 1;

/// Object key for one round index.
pub fn round_key(shard: u32, round: u64) -> String {
    format!("cellwal-rounds/{shard:04x}/{round:08x}.idx")
}

/// One consolidation round, the object per tenant readers plan from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoundIndex {
    pub format: u32,
    pub shard: u32,
    pub round: u64,
    /// The landing chain range this round folded, inclusive.
    pub first_seq: u64,
    pub last_seq: u64,
    /// The sealed segment's object key.
    pub sealed: String,
    /// When the round was published. Drives the landing GC window,
    /// nothing in the safety argument reads it.
    pub unix: u64,
    /// Every tenant the shard has ever consolidated, sorted by tenant
    /// ref, each with its durable watermark as of this round. Tenants
    /// idle this round carry no chunks.
    pub tenants: Vec<RoundTenant>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoundTenant {
    pub tenant: u128,
    /// End lsn of the tenant's last durable frame through this round.
    pub watermark: u64,
    /// How many frames this round sealed for the tenant. Zero when the
    /// tenant was idle.
    #[serde(default)]
    pub frames: u32,
    /// Byte ranges of this tenant's frames inside `sealed`, in lsn
    /// order. Empty when the tenant wrote nothing this round.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chunks: Vec<RoundChunk>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoundChunk {
    pub min_lsn: u64,
    pub max_lsn: u64,
    pub offset: u64,
    pub len: u64,
}

impl RoundIndex {
    pub fn load(
        store: &dyn CasStore,
        shard: u32,
        round: u64,
    ) -> Result<Option<RoundIndex>, ConsolidateError> {
        match store.get(&round_key(shard, round))? {
            None => Ok(None),
            Some((bytes, _)) => {
                let index: RoundIndex = serde_json::from_slice(&bytes).map_err(|source| {
                    ConsolidateError::BadRound {
                        shard,
                        round,
                        reason: source.to_string(),
                    }
                })?;
                if index.format > ROUND_INDEX_FORMAT {
                    return Err(ConsolidateError::BadRound {
                        shard,
                        round,
                        reason: format!("format {} is newer than this zou, upgrade", index.format),
                    });
                }
                Ok(Some(index))
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConsolidateError {
    #[error(transparent)]
    Chain(#[from] ChainError),
    #[error(transparent)]
    Store(#[from] CasError),
    #[error("round {round} on shard {shard} is unusable: {reason}")]
    BadRound {
        shard: u32,
        round: u64,
        reason: String,
    },
    #[error(
        "tenant {tenant:x} breaks lsn continuity: the watermark is {expect} but the next frame starts at {found}"
    )]
    Discontinuity {
        tenant: u128,
        expect: u64,
        found: u64,
    },
    #[error("shard {shard} moved mid round, run consolidation again")]
    Raced { shard: u32 },
}

/// Lets a caller whose own error type is a plain message, like the
/// restore path, pass it straight through [`crate::tee::catch_up_with`]
/// without a wrapper enum.
impl From<ConsolidateError> for String {
    fn from(e: ConsolidateError) -> String {
        e.to_string()
    }
}

/// What a round did, for the operator log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsolidateOutcome {
    pub round: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    pub frames: u64,
    pub sealed: String,
    /// True when this run finished a round a crashed predecessor left
    /// fully written but unpublished.
    pub adopted: bool,
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Run one consolidation round on a shard. Returns None when there is
/// nothing past the consolidated boundary. Safe to run from anywhere,
/// any number of times: every object it writes is create only and the
/// manifest CAS is the only commit point, so rivals and reruns either
/// adopt each other's finished work or collide harmlessly.
pub fn consolidate(
    media: &WalMedia,
    shard: u32,
) -> Result<Option<ConsolidateOutcome>, ConsolidateError> {
    let store = media.manifest_store();
    let Some((manifest, version)) = ShardManifest::load(store.as_ref(), shard)? else {
        return Ok(None);
    };

    // A crashed predecessor may have written the next round's objects
    // without publishing them. The round object is complete by
    // construction, publish it instead of consolidating a range that
    // overlaps it.
    let round = manifest.rounds.map_or(1, |r| r.last + 1);
    if let Some(existing) = RoundIndex::load(store.as_ref(), shard, round)? {
        if existing.first_seq != manifest.consolidated_upto + 1 {
            return Err(ConsolidateError::BadRound {
                shard,
                round,
                reason: format!(
                    "covers {}..{} but the shard is consolidated to {}",
                    existing.first_seq, existing.last_seq, manifest.consolidated_upto
                ),
            });
        }
        let digest = digest_of_segment(media, shard, existing.last_seq)?;
        publish(
            store.as_ref(),
            &manifest,
            &version,
            existing.last_seq,
            digest,
            round,
        )?;
        return Ok(Some(ConsolidateOutcome {
            round,
            first_seq: existing.first_seq,
            last_seq: existing.last_seq,
            frames: existing.tenants.iter().map(|t| t.frames as u64).sum(),
            sealed: existing.sealed,
            adopted: true,
        }));
    }

    let from = manifest.consolidated_upto;
    let segments = read_chain_linked(media, shard, from, manifest.consolidated_digest)?;
    let Some(last) = segments.last() else {
        return Ok(None);
    };
    let last_seq = last.seq;
    let last_digest = tenants_digest(&last.footer.tenants);

    // The previous round's watermark map is the continuity baseline
    // and carries forward into this round's index.
    let mut watermarks: BTreeMap<u128, u64> = BTreeMap::new();
    if let Some(rounds) = manifest.rounds {
        let prev = RoundIndex::load(store.as_ref(), shard, rounds.last)?.ok_or(
            ConsolidateError::BadRound {
                shard,
                round: rounds.last,
                reason: "the newest published round index is missing".to_string(),
            },
        )?;
        for t in prev.tenants {
            watermarks.insert(t.tenant, t.watermark);
        }
    }

    let mut frames: Vec<Frame2> = segments.into_iter().flat_map(|s| s.frames).collect();
    frames.sort_by_key(|f| (f.tenant, f.start_lsn, f.end_lsn));
    let mut kept: Vec<Frame2> = Vec::with_capacity(frames.len());
    for frame in frames {
        match watermarks.get(&frame.tenant) {
            // A frame wholly at or below the watermark is a writer
            // retry of something already durable, drop it. Anything
            // else must resume exactly at the watermark.
            Some(&w) if frame.end_lsn.0 <= w => continue,
            Some(&w) if frame.start_lsn.0 != w => {
                return Err(ConsolidateError::Discontinuity {
                    tenant: frame.tenant,
                    expect: w,
                    found: frame.start_lsn.0,
                });
            }
            _ => {}
        }
        watermarks.insert(frame.tenant, frame.end_lsn.0);
        kept.push(frame);
    }

    let header = SealedHeader {
        shard,
        first_seq: from + 1,
        last_seq,
    };
    let (bytes, footer) =
        build_sealed(header, &kept, SEALED_CHUNK_TARGET).expect("frames were sorted two lines up");
    let skey = sealed_key(shard, from + 1, last_seq);
    match store.put_if_absent(&skey, &bytes) {
        // Same name means same range over the same immutable chain,
        // the bytes are identical. A crashed rerun lands here.
        Ok(_) | Err(CasError::AlreadyExists { .. }) => {}
        Err(e) => return Err(e.into()),
    }

    let mut per_tenant: BTreeMap<u128, (u32, Vec<RoundChunk>)> = BTreeMap::new();
    for t in footer.tenants {
        let chunks = t
            .chunks
            .iter()
            .map(|c| RoundChunk {
                min_lsn: c.min_lsn.0,
                max_lsn: c.max_lsn.0,
                offset: c.offset,
                len: c.len,
            })
            .collect();
        per_tenant.insert(t.tenant, (t.frames, chunks));
    }
    let index = RoundIndex {
        format: ROUND_INDEX_FORMAT,
        shard,
        round,
        first_seq: from + 1,
        last_seq,
        sealed: skey.clone(),
        unix: unix_now(),
        tenants: watermarks
            .iter()
            .map(|(&tenant, &watermark)| {
                let (frames, chunks) = per_tenant.remove(&tenant).unwrap_or_default();
                RoundTenant {
                    tenant,
                    watermark,
                    frames,
                    chunks,
                }
            })
            .collect(),
    };
    let body = serde_json::to_vec(&index).expect("round index serializes");
    match store.put_if_absent(&round_key(shard, round), &body) {
        Ok(_) => {}
        // A rival published this round between our manifest load and
        // here. Its range may differ from ours, so adopt nothing now,
        // the next run will.
        Err(CasError::AlreadyExists { .. }) => return Err(ConsolidateError::Raced { shard }),
        Err(e) => return Err(e.into()),
    }

    publish(
        store.as_ref(),
        &manifest,
        &version,
        last_seq,
        last_digest,
        round,
    )?;
    Ok(Some(ConsolidateOutcome {
        round,
        first_seq: from + 1,
        last_seq,
        frames: kept.len() as u64,
        sealed: skey,
        adopted: false,
    }))
}

/// The commit point: CAS the manifest to the new consolidated boundary
/// with the round appended, preserving whatever chain state the
/// sequencer side wrote. A conflict means a takeover or a rival moved
/// the manifest first, and the round objects stay where they are for
/// the next run to adopt.
fn publish(
    store: &dyn CasStore,
    manifest: &ShardManifest,
    version: &zou_store::Version,
    upto: u64,
    digest: u64,
    round: u64,
) -> Result<(), ConsolidateError> {
    let next = ShardManifest {
        format: SHARD_MANIFEST_FORMAT,
        consolidated_upto: upto,
        consolidated_digest: digest,
        rounds: Some(RoundRange {
            first: manifest.rounds.map_or(round, |r| r.first),
            last: round,
        }),
        ..manifest.clone()
    };
    let body = serde_json::to_vec_pretty(&next).expect("shard manifest serializes");
    match store.put_if_match(
        &crate::chain::manifest_key(next.shard),
        &body,
        Some(version),
    ) {
        Ok(_) => Ok(()),
        Err(CasError::Conflict { .. }) => Err(ConsolidateError::Raced { shard: next.shard }),
        Err(e) => Err(e.into()),
    }
}

/// Bytes of landing chain sitting past the consolidated boundary, the
/// consolidation lag gauge of spec 08 section 4. The runner measures
/// this on its cadence and reports it into
/// [`Backpressure`](crate::Backpressure), which bumps the worst shard
/// to the front and, past the bound, throttles the cell's appends.
/// Fetches every unconsolidated segment to size it, so this belongs on
/// the runner's clock, never near the commit path; the segments it
/// reads are the ones the next round reads anyway.
pub fn landing_backlog(media: &WalMedia, shard: u32) -> Result<u64, ConsolidateError> {
    let store = media.manifest_store();
    let Some((manifest, _)) = ShardManifest::load(store.as_ref(), shard)? else {
        return Ok(0);
    };
    let floor = manifest.head.max(manifest.consolidated_upto);
    let head = chain_head(media, shard, floor)?;
    let mut backlog = 0u64;
    for seq in manifest.consolidated_upto + 1..=head {
        if let Some(bytes) = media.fetch(shard, seq)? {
            backlog += bytes.len() as u64;
        }
    }
    Ok(backlog)
}

/// Delete landing segments covered by rounds older than the grace
/// window (spec says 10 minutes: long enough that any zombie PUT still
/// in flight against those seqs has died with its connection).
/// Returns how many chain positions were deleted. Reruns after a crash
/// redelete harmlessly, the manifest's gc hint only moves at the end.
pub fn gc_landing(media: &WalMedia, shard: u32, grace: Duration) -> Result<u64, ConsolidateError> {
    let store = media.manifest_store();
    let Some((manifest, version)) = ShardManifest::load(store.as_ref(), shard)? else {
        return Ok(0);
    };
    let Some(rounds) = manifest.rounds else {
        return Ok(0);
    };
    let now = unix_now();
    let mut deleted = 0u64;
    let mut gc_round = manifest.gc_round;
    for round in (manifest.gc_round + 1).max(rounds.first)..=rounds.last {
        let index =
            RoundIndex::load(store.as_ref(), shard, round)?.ok_or(ConsolidateError::BadRound {
                shard,
                round,
                reason: "a retained round index is missing".to_string(),
            })?;
        if index.unix.saturating_add(grace.as_secs()) > now {
            break;
        }
        for seq in index.first_seq..=index.last_seq {
            media.delete_segment(shard, seq)?;
            deleted += 1;
        }
        gc_round = round;
    }
    if gc_round != manifest.gc_round {
        let next = ShardManifest {
            gc_round,
            ..manifest
        };
        let body = serde_json::to_vec_pretty(&next).expect("shard manifest serializes");
        match store.put_if_match(&crate::chain::manifest_key(shard), &body, Some(&version)) {
            Ok(_) => {}
            // The hint stays behind, the deletions themselves are done
            // and the next pass redeletes nothing that matters.
            Err(CasError::Conflict { .. }) => return Err(ConsolidateError::Raced { shard }),
            Err(e) => return Err(e.into()),
        }
    }
    Ok(deleted)
}

/// The byte ranges one tenant's frames occupy in a round, in lsn
/// order, empty for a tenant the round never saw. Every entry carries
/// its lsn bounds, so a reader that only wants what is above a
/// watermark can drop most of them without a single GET.
pub fn round_chunks(index: &RoundIndex, tenant: u128) -> &[RoundChunk] {
    index
        .tenants
        .iter()
        .find(|t| t.tenant == tenant)
        .map_or(&[], |t| &t.chunks)
}

/// One chunk of one tenant's frames, one range GET. A reader that
/// cannot hold a whole round, or cannot afford to fetch one before it
/// answers anything else, takes them one at a time through this.
pub fn read_round_chunk(
    store: &dyn CasStore,
    index: &RoundIndex,
    chunk: &RoundChunk,
) -> Result<Vec<Frame2>, ConsolidateError> {
    let bytes = store
        .get_range(&index.sealed, chunk.offset, chunk.len)?
        .ok_or(ConsolidateError::BadRound {
            shard: index.shard,
            round: index.round,
            reason: format!("sealed object {} is missing", index.sealed),
        })?;
    let mut out = Vec::new();
    for item in Frame2Stream::new(&bytes) {
        let frame = item.map_err(|source| ConsolidateError::BadRound {
            shard: index.shard,
            round: index.round,
            reason: format!("sealed chunk at {} is corrupt: {source}", chunk.offset),
        })?;
        out.push(frame);
    }
    Ok(out)
}

/// Read one tenant's frames out of a round, in lsn order, using only
/// the round index and range GETs against the sealed object. This is
/// the catch up read path: no footer fetch, no full object fetch.
pub fn read_round_tenant(
    store: &dyn CasStore,
    index: &RoundIndex,
    tenant: u128,
) -> Result<Vec<Frame2>, ConsolidateError> {
    let mut out = Vec::new();
    for chunk in round_chunks(index, tenant) {
        out.extend(read_round_chunk(store, index, chunk)?);
    }
    Ok(out)
}
