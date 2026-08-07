//! The shard function and its metadata only split and merge (spec 04
//! section 1).
//!
//! `shard(key) = hash(relfilenode, block >> 14) mod N`, N a power of
//! two per tenant, computed by [`crate::frame::BlockRef::page_shard`]
//! so the read side and the tee routing can never disagree. A split
//! doubles N and a merge halves it, and both are one CAS on the tenant
//! manifest: no layer moves, no data is copied. The children of a
//! split serve the parent's layers in place, each answering only the
//! keys it owns, and the layers physically separate at the next
//! compaction. A merge is the mirror, the survivor serves both
//! children's layers.
//!
//! What makes the lazy part sound is the split lineage the manifest
//! carries: every count the tenant ever ran at. A shard's serving set
//! is its own manifest plus, for every past era, the ancestor shards
//! whose keyspace overlaps its own; with power of two counts that is
//! `s mod M` for a smaller era M and `s + i * N` for a larger one.
//! Shard numbers are stable identities, the same prefix serves the
//! same number across eras, so consulting an ancestor's manifest after
//! it moved on only adds layers whose key ranges the lookup skips.
//!
//! Each lineage entry records the safe resume floor `at`: the minimum
//! `disk_consistent_lsn` across the pre change era, so every record at
//! or below it, for any key, is already in a published layer. A shard
//! the change created starts ingesting there and may replay records an
//! ancestor already flushed, which is harmless, a rebuilt layer is
//! read amplification, never wrong data.

use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cas::{CasError, CasStore};
use crate::layermap::LayerDesc;
use crate::layout::TenantLayout;
use crate::lsn::Lsn;
use crate::manifest::{Manifest, ManifestError, ShardChange};
use crate::shardmanifest::{PageShardError, PageShardManifest};

/// The most page shards one tenant may have. The spec sizes a 1 PB
/// tenant at 4096 shards of 256 GB; anything past that is a new
/// problem, not a bigger N.
pub const MAX_PAGE_SHARDS: u32 = 4096;

#[derive(Debug, thiserror::Error)]
pub enum ShardError {
    #[error("no manifest at {key}, the tenant does not exist")]
    NoTenant { key: String },
    #[error("tenant already has {MAX_PAGE_SHARDS} shards, the ceiling")]
    AtCeiling,
    #[error("tenant has one shard, nothing to merge")]
    AtFloor,
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error("shard manifest: {0}")]
    Shard(#[from] PageShardError),
    #[error(transparent)]
    Store(#[from] CasError),
}

/// The page shard a key lands on at shard count `count`. Delegates to
/// the frozen tee routing hash, so a record and the reads of the pages
/// it touched always name the same shard. Non page keys hash by their
/// relation with block zero, which parks a fork's relsize next to its
/// first stripe.
pub fn shard_of(key: &crate::layer::LayerKey, count: u32) -> u16 {
    crate::frame::BlockRef {
        relfilenode: key.rel,
        fork: key.fork,
        block: key.block,
    }
    .page_shard(count) as u16
}

/// Double the tenant's shard count. One CAS on the tenant manifest,
/// nothing else moves, see the module doc for why that is enough.
/// Returns the manifest as published.
pub fn split(store: &dyn CasStore, tenant_ref: &str) -> Result<Manifest, ShardError> {
    change(store, tenant_ref, true)
}

/// Halve the tenant's shard count, the mirror of [`split`].
pub fn merge(store: &dyn CasStore, tenant_ref: &str) -> Result<Manifest, ShardError> {
    change(store, tenant_ref, false)
}

fn change(store: &dyn CasStore, tenant_ref: &str, grow: bool) -> Result<Manifest, ShardError> {
    let layout = TenantLayout::new(tenant_ref);
    let key = layout.manifest();
    loop {
        let Some((data, version)) = store.get(&key)? else {
            return Err(ShardError::NoTenant { key });
        };
        let mut manifest = Manifest::from_json(&data)?;
        let from = manifest.shards;
        let to = if grow {
            if from >= MAX_PAGE_SHARDS {
                return Err(ShardError::AtCeiling);
            }
            from * 2
        } else {
            if from < 2 {
                return Err(ShardError::AtFloor);
            }
            from / 2
        };
        let at = era_floor(store, &layout, &manifest)?;
        let unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        manifest
            .shard_history
            .push(ShardChange { from, to, at, unix });
        manifest.shards = to;
        // Sharding needs format 3: a binary that predates it must
        // refuse the whole tenant instead of quietly serving shard
        // zero as if it were everything.
        manifest.format = manifest.format.max(3);
        manifest.published_unix = Some(unix);
        match store.put_if_match(&key, &manifest.to_json(), Some(&version)) {
            Ok(_) => {
                // The history snapshot PITR materializes from. Best
                // effort, same contract as the lease heartbeat's copy:
                // a miss loses this second from history, never data.
                let history = layout.manifest_history(manifest.epoch, unix);
                let mut snapshot = manifest.clone();
                snapshot.lease = None;
                if let Err(e) = store.put_if_absent(&history, &snapshot.to_json())
                    && !matches!(e, CasError::AlreadyExists { .. })
                {
                    log::warn!("history snapshot {history} failed, pitr loses this second: {e}");
                }
                return Ok(manifest);
            }
            // Racing the lease heartbeat or another resize: re-read
            // and rebuild on top of what actually landed.
            Err(CasError::Conflict { .. }) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// The safe resume floor across the current era, what a change stamps
/// into its lineage entry: the minimum coverage over every shard the
/// keyspace maps to today. A shard with no manifest of its own falls
/// back to the floor of the change that created it, so a split chain
/// where a middle era never flushed stays conservative instead of
/// optimistic.
fn era_floor(
    store: &dyn CasStore,
    layout: &TenantLayout,
    manifest: &Manifest,
) -> Result<Lsn, ShardError> {
    let mut floor: Option<Lsn> = None;
    for shard in 0..manifest.shards as u16 {
        let own = PageShardManifest::load(store, &layout.shard_manifest(shard))?
            .map(|(m, _)| m.disk_consistent_lsn);
        let f = resume_floor(manifest, own);
        floor = Some(floor.map_or(f, |lo| lo.min(f)));
    }
    Ok(floor.unwrap_or(Lsn(0)))
}

/// Where a shard's ingest resumes: its own `disk_consistent_lsn` when
/// it has published anything, otherwise the floor of the lineage entry
/// that created the current count, otherwise the start of history.
pub fn resume_floor(manifest: &Manifest, own_disk_consistent: Option<Lsn>) -> Lsn {
    own_disk_consistent
        .or_else(|| manifest.shard_history.last().map(|c| c.at))
        .unwrap_or(Lsn(0))
}

/// Every shard number whose manifest may list layers holding keys of
/// `shard` at the current count: the shard itself, plus for each past
/// era the ancestors its keyspace lived on. Sorted, deduplicated, own
/// shard always present. Counts are powers of two so the era overlap
/// is pure arithmetic, no manifest reads.
pub fn serving_shards(shard: u16, count: u32, history: &[ShardChange]) -> Vec<u16> {
    let mut counts: BTreeSet<u32> = history.iter().flat_map(|c| [c.from, c.to]).collect();
    counts.insert(count);
    let mut out = BTreeSet::new();
    for m in counts {
        if m <= count {
            out.insert(shard % m as u16);
        } else {
            for i in 0..(m / count) {
                out.insert(shard + (i * count) as u16);
            }
        }
    }
    out.into_iter().collect()
}

/// Load the layer descriptors one shard serves: its own manifest plus
/// every lineage ancestor's, foreign entries stamped with their home
/// shard so the reader fetches them in place. Also returns the shard's
/// ingest resume floor. The descriptor list feeds
/// [`crate::layermap::LayerMap::new`].
pub fn load_serving_descs(
    store: &dyn CasStore,
    tenant_ref: &str,
    manifest: &Manifest,
    shard: u16,
) -> Result<(Vec<LayerDesc>, Lsn), ShardError> {
    let layout = TenantLayout::new(tenant_ref);
    let mut descs = Vec::new();
    let mut own = None;
    // A manifest that covers its keyspace at some count keeps covering
    // it at any count at or above, every split since only narrowed the
    // shard. Skip the lineage entirely then; a merge widens the shard
    // past the claim and the ancestors are consulted again.
    if let Some((m, _)) = PageShardManifest::load(store, &layout.shard_manifest(shard))?
        && m.covers.is_some_and(|c| c <= manifest.shards)
    {
        for l in &m.layers {
            let mut d = LayerDesc::parse(&l.name, l.size).map_err(PageShardError::from)?;
            d.owner = l.owner.clone();
            d.upto = l.upto;
            descs.push(d);
        }
        return Ok((descs, m.disk_consistent_lsn));
    }
    for s in serving_shards(shard, manifest.shards, &manifest.shard_history) {
        let Some((m, _)) = PageShardManifest::load(store, &layout.shard_manifest(s))? else {
            continue;
        };
        if s == shard {
            own = Some(m.disk_consistent_lsn);
        }
        for l in &m.layers {
            let mut d = LayerDesc::parse(&l.name, l.size).map_err(PageShardError::from)?;
            d.owner = l.owner.clone();
            d.upto = l.upto;
            if s != shard {
                d.home = Some(s);
            }
            descs.push(d);
        }
    }
    Ok((descs, resume_floor(manifest, own)))
}

/// Retire the split lineage once it stopped mattering: when every
/// shard of the current count covers its keyspace on its own, nobody
/// consults an ancestor era again, and the history can be cleared so
/// the serving set math stays trivial and old eras become gc food.
/// Returns whether the lineage is gone, false when some shard still
/// leans on its ancestors. One CAS on the tenant manifest, retried
/// against the heartbeat like a resize.
pub fn prune_lineage(store: &dyn CasStore, tenant_ref: &str) -> Result<bool, ShardError> {
    let layout = TenantLayout::new(tenant_ref);
    let key = layout.manifest();
    loop {
        let Some((data, version)) = store.get(&key)? else {
            return Err(ShardError::NoTenant { key });
        };
        let mut manifest = Manifest::from_json(&data)?;
        if manifest.shard_history.is_empty() {
            return Ok(true);
        }
        for shard in 0..manifest.shards as u16 {
            let covered = PageShardManifest::load(store, &layout.shard_manifest(shard))?
                .is_some_and(|(m, _)| m.covers.is_some_and(|c| c <= manifest.shards));
            if !covered {
                return Ok(false);
            }
        }
        manifest.shard_history.clear();
        match store.put_if_match(&key, &manifest.to_json(), Some(&version)) {
            Ok(_) => return Ok(true),
            Err(CasError::Conflict { .. }) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::LayerKey;
    use crate::mem::MemStore;
    use crate::shardmanifest::{LayerEntry, publish_layer};

    fn seed(store: &MemStore, tenant_ref: &str) {
        let layout = TenantLayout::new(tenant_ref);
        store
            .put_if_absent(&layout.manifest(), &Manifest::new(tenant_ref, 18).to_json())
            .unwrap();
    }

    fn load(store: &MemStore, tenant_ref: &str) -> Manifest {
        let layout = TenantLayout::new(tenant_ref);
        let (data, _) = store.get(&layout.manifest()).unwrap().unwrap();
        Manifest::from_json(&data).unwrap()
    }

    #[test]
    fn the_shard_function_matches_the_tee_routing() {
        for (rel, block) in [(1663u32, 0u32), (1663, 16384), (16400, 123_456_789)] {
            let key = LayerKey::page(1663, 5, rel, 0, block);
            let hint = crate::frame::BlockRef {
                relfilenode: rel,
                fork: 0,
                block,
            };
            for count in [1u32, 2, 64, 4096] {
                assert_eq!(shard_of(&key, count) as u32, hint.page_shard(count));
            }
        }
    }

    #[test]
    fn splitting_halves_the_keyspace_consistently() {
        // The parent of a key's shard at 2N is always its shard at N:
        // that arithmetic is what lets a child serve the parent's
        // layers, so pin it here.
        for i in 0..500u32 {
            let key = LayerKey::page(1663, 5, 16000 + i, 0, i * 30_000);
            let mut prev = shard_of(&key, 1);
            for count in [2u32, 4, 8, 16, 32] {
                let s = shard_of(&key, count);
                assert_eq!(s % (count / 2) as u16, prev);
                prev = s;
            }
        }
    }

    #[test]
    fn split_and_merge_move_the_count_and_write_lineage() {
        let store = MemStore::default();
        seed(&store, "t");
        let m = split(&store, "t").unwrap();
        assert_eq!((m.shards, m.format), (2, 3));
        let m = split(&store, "t").unwrap();
        assert_eq!(m.shards, 4);
        let m = merge(&store, "t").unwrap();
        assert_eq!(m.shards, 2);
        let history: Vec<(u32, u32)> = m.shard_history.iter().map(|c| (c.from, c.to)).collect();
        assert_eq!(history, vec![(1, 2), (2, 4), (4, 2)]);
        assert_eq!(load(&store, "t"), m);

        // The lineage rides into a history snapshot for PITR.
        let dir = TenantLayout::new("t").manifests_dir();
        assert!(!store.list(&dir).unwrap().is_empty());
    }

    #[test]
    fn merge_refuses_the_floor_and_split_the_ceiling() {
        let store = MemStore::default();
        seed(&store, "t");
        assert!(matches!(
            merge(&store, "t").unwrap_err(),
            ShardError::AtFloor
        ));
        for _ in 0..12 {
            split(&store, "t").unwrap();
        }
        assert_eq!(load(&store, "t").shards, MAX_PAGE_SHARDS);
        assert!(matches!(
            split(&store, "t").unwrap_err(),
            ShardError::AtCeiling
        ));
    }

    #[test]
    fn the_lineage_floor_is_the_least_covered_shard() {
        let store = MemStore::default();
        seed(&store, "t");
        let layout = TenantLayout::new("t");
        let entry = |blocks: &[u32], lsns: &[u64]| {
            let mut entries = Vec::new();
            for &b in blocks {
                for &l in lsns {
                    entries.push(crate::layer::DeltaEntry {
                        key: LayerKey::page(1663, 5, 16384, 0, b),
                        lsn: Lsn(l),
                        record: vec![7; 16],
                    });
                }
            }
            let (bytes, footer) = crate::layer::build_delta(&entries, 4096).unwrap();
            let desc = LayerDesc::from_footer(&footer, bytes.len() as u64);
            LayerEntry {
                name: desc.name(),
                size: bytes.len() as u64,
                owner: None,
                upto: None,
            }
        };
        publish_layer(
            &store,
            &layout.shard_manifest(0),
            0,
            &entry(&[1], &[0x500]),
            Lsn(0x500),
        )
        .unwrap();
        let m = split(&store, "t").unwrap();
        assert_eq!(m.shard_history[0].at, Lsn(0x500));

        // The next change sees a fresh child with no manifest, whose
        // floor is the previous entry's, not zero and not the parent's
        // newer coverage.
        publish_layer(
            &store,
            &layout.shard_manifest(0),
            0,
            &entry(&[2], &[0x900]),
            Lsn(0x900),
        )
        .unwrap();
        let m = split(&store, "t").unwrap();
        assert_eq!(m.shard_history[1].at, Lsn(0x500));
        assert_eq!(resume_floor(&m, None), Lsn(0x500));
        assert_eq!(resume_floor(&m, Some(Lsn(0x900))), Lsn(0x900));
    }

    #[test]
    fn serving_sets_cover_splits_merges_and_chains() {
        let h = |pairs: &[(u32, u32)]| {
            pairs
                .iter()
                .map(|&(from, to)| ShardChange {
                    from,
                    to,
                    at: Lsn(0),
                    unix: 0,
                })
                .collect::<Vec<_>>()
        };
        // Never resized: just yourself.
        assert_eq!(serving_shards(0, 1, &[]), vec![0]);
        // One split: each child consults the parent era.
        assert_eq!(serving_shards(3, 4, &h(&[(1, 2), (2, 4)])), vec![0, 1, 3]);
        // A merge: the survivor consults both children of the wider
        // era, and the count 1 origin.
        assert_eq!(
            serving_shards(1, 2, &h(&[(1, 2), (2, 4), (4, 2)])),
            vec![0, 1, 3]
        );
        // Merged all the way down: everything ever written is in reach.
        assert_eq!(
            serving_shards(0, 1, &h(&[(1, 2), (2, 4), (4, 2), (2, 1)])),
            vec![0, 1, 2, 3]
        );
    }

    #[test]
    fn a_child_serves_the_parents_layers_in_place() {
        let store = MemStore::default();
        seed(&store, "t");
        let layout = TenantLayout::new("t");

        // One layer flushed by shard 0 before any split, holding keys
        // that land on both children of the split.
        let (left, right) = {
            let mut left = None;
            let mut right = None;
            for b in 0..200u32 {
                let key = LayerKey::page(1663, 5, 16384, 0, b * 20_000);
                match shard_of(&key, 2) {
                    0 if left.is_none() => left = Some(key),
                    1 if right.is_none() => right = Some(key),
                    _ => {}
                }
            }
            (left.unwrap(), right.unwrap())
        };
        let mut entries: Vec<_> = [left, right]
            .iter()
            .map(|&key| crate::layer::DeltaEntry {
                key,
                lsn: Lsn(0x100),
                record: vec![9; 16],
            })
            .collect();
        entries.sort_by_key(|e| (e.key, e.lsn));
        let (bytes, footer) = crate::layer::build_delta(&entries, 4096).unwrap();
        let desc = LayerDesc::from_footer(&footer, bytes.len() as u64);
        store
            .put_if_absent(
                &format!("{}{}", layout.shard_prefix(0), desc.name()),
                &bytes,
            )
            .unwrap();
        let entry = LayerEntry {
            name: desc.name(),
            size: bytes.len() as u64,
            owner: None,
            upto: None,
        };
        publish_layer(&store, &layout.shard_manifest(0), 0, &entry, Lsn(0x100)).unwrap();

        let manifest = split(&store, "t").unwrap();

        // Shard 1 has no manifest of its own, yet its serving set
        // reaches the parent's layer, stamped with its home.
        let (descs, floor) = load_serving_descs(&store, "t", &manifest, 1).unwrap();
        assert_eq!(descs.len(), 1);
        assert_eq!(descs[0].home, Some(0));
        assert_eq!(floor, Lsn(0x100));

        // Shard 0 serves the same layer as its own, untagged.
        let (descs, _) = load_serving_descs(&store, "t", &manifest, 0).unwrap();
        assert_eq!(descs.len(), 1);
        assert_eq!(descs[0].home, None);

        // And the map plus reader actually reconstruct the right key
        // from shard 1 through the parent's prefix.
        let map = crate::layermap::LayerMap::new(
            load_serving_descs(&store, "t", &manifest, 1).unwrap().0,
        )
        .unwrap();
        let reader = crate::pageread::LayerReader::for_shard(&store, "t", 1);
        let mem = crate::memtable::Memtable::new();
        let rec = reader.reconstruct(&map, &mem, &right, Lsn(0x200)).unwrap();
        assert_eq!(rec.records, vec![(Lsn(0x100), vec![9; 16])]);
    }

    #[test]
    fn a_coverage_claim_skips_the_lineage_and_lets_the_prune_land() {
        let store = MemStore::default();
        seed(&store, "t");
        let layout = TenantLayout::new("t");
        let entry = |b: u32| {
            let entries = vec![crate::layer::DeltaEntry {
                key: LayerKey::page(1663, 5, 16384, 0, b),
                lsn: Lsn(0x100),
                record: vec![7; 16],
            }];
            let (bytes, footer) = crate::layer::build_delta(&entries, 4096).unwrap();
            LayerEntry {
                name: LayerDesc::from_footer(&footer, bytes.len() as u64).name(),
                size: bytes.len() as u64,
                owner: None,
                upto: None,
            }
        };
        publish_layer(&store, &layout.shard_manifest(0), 0, &entry(1), Lsn(0x100)).unwrap();
        let manifest = split(&store, "t").unwrap();

        // No claims yet: the lineage stays, shard 1 leans on shard 0.
        assert!(!prune_lineage(&store, "t").unwrap());
        let (descs, _) = load_serving_descs(&store, "t", &manifest, 1).unwrap();
        assert_eq!(descs[0].home, Some(0));

        // Shard 1 covers itself. Its serving set is now its own
        // manifest alone, but the prune still refuses for shard 0.
        crate::shardmanifest::swap_layers(
            &store,
            &layout.shard_manifest(1),
            1,
            &[],
            &[entry(2)],
            Some(2),
        )
        .unwrap();
        let (descs, _) = load_serving_descs(&store, "t", &manifest, 1).unwrap();
        assert_eq!(descs.len(), 1);
        assert_eq!(descs[0].home, None);
        assert!(!prune_lineage(&store, "t").unwrap());
        assert!(!load(&store, "t").shard_history.is_empty());

        // Shard 0 covers too: the lineage goes, and stays gone on a
        // rerun.
        crate::shardmanifest::swap_layers(&store, &layout.shard_manifest(0), 0, &[], &[], Some(2))
            .unwrap();
        assert!(prune_lineage(&store, "t").unwrap());
        assert!(load(&store, "t").shard_history.is_empty());
        assert!(prune_lineage(&store, "t").unwrap());

        // A merge widens shard 0 past its claim: covers 2 at count 1
        // no longer holds and the serving math consults the eras
        // again, which is exactly the safe direction.
        let manifest = merge(&store, "t").unwrap();
        let (descs, _) = load_serving_descs(&store, "t", &manifest, 0).unwrap();
        assert_eq!(descs.len(), 2, "shard 0 reads both halves after the merge");
        assert!(!prune_lineage(&store, "t").unwrap());
    }
}
