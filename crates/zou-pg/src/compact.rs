//! Compaction: rewrite a shard's layers so reads stay cheap (spec 04
//! section 6).
//!
//! One pass over one tenant shard reads everything the shard serves,
//! wherever it lives: its own layers, ancestors' layers across split
//! eras, a branch parent's layers. Records that belong to other shards
//! or sit above a branch cut are left behind, everything else comes
//! back out as layers the shard owns: the deltas sort merged into one
//! run, every source image rewritten at its lsn, and, when a redo pool
//! is on hand, a fresh image materialized at `disk_consistent_lsn` so
//! the delta chains above the old images stop being the read path.
//!
//! The commit is one CAS on the shard manifest that retires the inputs
//! and lists the outputs, so a worker can die anywhere: outputs are
//! create only, a half written pass leaves orphans for gc and the
//! manifest never saw it, and a rerun redoes the work idempotently.
//! That is what makes spot capacity safe to run this on.
//!
//! A full pass also stamps the coverage claim: the manifest now covers
//! the shard's keyspace at the current count by itself, readers stop
//! consulting the lineage, and once every shard of the count says so
//! the lineage itself can be pruned. This is where a split's lazy
//! serving debt is finally paid down.
//!
//! Nothing here deletes a record that is this tenant's history: the
//! merge preserves every owned record and every source image, because
//! shard manifests have no history of their own and a branch or PITR
//! read at an old lsn must still find its bases. Only foreign keys and
//! records above a branch cut are dropped, and foreign is judged at
//! the oldest era that still consults this manifest, not the current
//! count: a pre split shard's layers are its descendants' lineage, so
//! their keys stay through the rewrite until every descendant covers
//! its own keyspace and the lineage is pruned. Records above a branch
//! cut were never this tenant's to serve.
//!
//! Scheduling is by debt, the bytes of delta sitting above the newest
//! image: the queue takes the worst shards first and hands them to a
//! pool of workers that stop between jobs when asked, the preemption
//! story again.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use zou_store::cas::{CasError, CasStore};
use zou_store::layer::{
    DeltaBuilder, ImageBuilder, KEY_PAGE, LayerBuildError, LayerDecodeError, LayerKey, LayerKind,
    delta_cursor, image_cursor,
};
use zou_store::layermap::{LayerDesc, LayerMap};
use zou_store::layout::TenantLayout;
use zou_store::lsn::Lsn;
use zou_store::manifest::{Manifest, ManifestError};
use zou_store::memtable::Memtable;
use zou_store::pageread::{LayerReader, ReadError};
use zou_store::shardmanifest::{LayerEntry, PageShardError, PageShardManifest, swap_layers};
use zou_store::shards::{ShardError, load_serving_descs, shard_of};

use crate::getpage::{GetPageError, MAX_GETPAGE_BATCH, PageService};
use crate::redo::RedoPool;

/// Compression block size for rebuilt layers, the format's read unit.
const BLOCK_TARGET: usize = 256 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum CompactError {
    #[error("no manifest at {key}, the tenant does not exist")]
    NoTenant { key: String },
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    Shards(#[from] ShardError),
    #[error("shard manifest: {0}")]
    Shard(#[from] PageShardError),
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error("layer {name}: {source}")]
    Decode {
        name: String,
        source: LayerDecodeError,
    },
    #[error("building output: {0}")]
    Build(#[from] LayerBuildError),
    #[error("materializing images: {0}")]
    Materialize(#[from] GetPageError),
    #[error(transparent)]
    Store(#[from] CasError),
}

/// The read amplification bound (spec 08 section 4): one image plus
/// four delta runs per shard. GetPage never waits on compaction, so
/// the bound is enforced by scheduling, a shard past it jumps the
/// queue no matter whose byte debt is bigger.
pub const READ_AMP_BOUND: usize = 5;

/// Worst case objects a read on this shard touches: every serving
/// delta run plus the image, the number the bound caps.
pub fn read_amp(descs: &[LayerDesc]) -> usize {
    let image = descs.iter().any(|d| d.kind == LayerKind::Image);
    descs.iter().filter(|d| d.kind == LayerKind::Delta).count() + usize::from(image)
}

/// The debt of one shard: bytes of delta above the newest image, what
/// reads pay for on every lookup until compaction erases it. The
/// scheduler orders shards by this.
pub fn debt(descs: &[LayerDesc]) -> u64 {
    let floor = descs
        .iter()
        .filter(|d| d.kind == LayerKind::Image)
        .map(|d| d.min_lsn)
        .max()
        .unwrap_or(Lsn(0));
    descs
        .iter()
        .filter(|d| d.kind == LayerKind::Delta && d.max_lsn > floor)
        .map(|d| d.size)
        .sum()
}

/// What one pass did, for logs and the scoreboard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactOutcome {
    pub retired: usize,
    pub outputs: usize,
    pub debt_before: u64,
    pub debt_after: u64,
}

/// One compaction pass over one shard. `Ok(None)` means there was
/// nothing worth doing: at most one owned delta run, no foreign
/// layers, and no pool to cut a fresh image with.
pub fn compact_shard(
    store: &dyn CasStore,
    tenant_ref: &str,
    shard: u16,
    pool: Option<&RedoPool>,
    data_checksums: bool,
) -> Result<Option<CompactOutcome>, CompactError> {
    let layout = TenantLayout::new(tenant_ref);
    let manifest_key = layout.manifest();
    let Some((data, _)) = store.get(&manifest_key)? else {
        return Err(CompactError::NoTenant { key: manifest_key });
    };
    let manifest = Manifest::from_json(&data)?;
    let (descs, _) = load_serving_descs(store, tenant_ref, &manifest, shard)?;
    if descs.is_empty() {
        return Ok(None);
    }
    let debt_before = debt(&descs);

    let own = PageShardManifest::load(store, &layout.shard_manifest(shard))?.map(|(m, _)| m);
    let dcl = own
        .as_ref()
        .map(|m| m.disk_consistent_lsn)
        .unwrap_or(Lsn(0));
    let retire: Vec<String> = own
        .iter()
        .flat_map(|m| &m.layers)
        .map(|l| l.name.clone())
        .collect();

    let foreign = descs.iter().any(|d| d.home.is_some() || d.owner.is_some());
    let delta_runs = descs.iter().filter(|d| d.kind == LayerKind::Delta).count();
    let covered = own
        .as_ref()
        .is_some_and(|m| m.covers.is_some_and(|c| c <= manifest.shards));
    // A shard with one owned run and nothing foreign still owes a pass
    // while the lineage stands and its coverage claim is missing: the
    // stamp is what lets prune_lineage retire the history.
    if !foreign
        && delta_runs <= 1
        && (pool.is_none() || debt_before == 0)
        && (covered || manifest.shard_history.is_empty())
    {
        return Ok(None);
    }

    // Read every serving layer and keep what is ours, filtered at the
    // oldest era that still consults this manifest, not the current
    // count. A pre split shard's layers are its descendants' lineage:
    // shard 0 compacting at count 2 before shard 1 has covered would
    // otherwise retire the very layers shard 1 still reads through the
    // history. The wide filter keeps those keys alive here until every
    // descendant stands alone; after the prune a later pass tightens
    // to the current count and drops the stragglers.
    let keep_count = manifest
        .shard_history
        .iter()
        .flat_map(|c| [c.from, c.to])
        .chain([manifest.shards])
        .filter(|&c| c > shard as u32)
        .min()
        .expect("the current count always exceeds a valid shard number");
    let keep = |key: &LayerKey| shard_of(key, keep_count) == shard;
    let reader = LayerReader::for_shard(store, tenant_ref, shard);
    // Inputs stay compressed in memory and stream out block by block:
    // the pass holds the fetched objects, one decoded block per input,
    // and the compressed output it is building, never a decoded copy
    // of the whole shard. Deltas keep their fetch order so a duplicate
    // (key, lsn) resolves to the latest source, the same last write
    // wins the map insert used to give.
    let mut delta_srcs: Vec<(String, Option<Lsn>, Vec<u8>)> = Vec::new();
    let mut image_groups: BTreeMap<Lsn, Vec<(String, Vec<u8>)>> = BTreeMap::new();
    for desc in &descs {
        let bytes = reader.fetch(desc)?;
        match desc.kind {
            LayerKind::Delta => delta_srcs.push((desc.name(), desc.upto, bytes)),
            LayerKind::Image => image_groups
                .entry(desc.min_lsn)
                .or_default()
                .push((desc.name(), bytes)),
        }
    }

    // The outputs, create only under the shard's own prefix. Names
    // derive from coverage, so a rerun of the same pass writes the
    // same objects and AlreadyExists is a crashed twin, not a problem.
    let mut add = Vec::new();
    let mut publish = |bytes: Vec<u8>, footer: &zou_store::layer::LayerFooter| {
        let desc = LayerDesc::from_footer(footer, bytes.len() as u64);
        let key = format!("{}{}", layout.shard_prefix(shard), desc.name());
        match store.put_if_absent(&key, &bytes) {
            Ok(_) | Err(CasError::AlreadyExists { .. }) => {}
            Err(e) => return Err(CompactError::from(e)),
        }
        add.push(LayerEntry {
            name: desc.name(),
            size: bytes.len() as u64,
            owner: None,
            upto: None,
        });
        Ok(())
    };

    // Source images rewritten at their lsn, each group of same lsn
    // layers merged by key with the latest source winning duplicates.
    let mut based: BTreeSet<LayerKey> = BTreeSet::new();
    for (at, group) in &image_groups {
        let mut cursors = Vec::with_capacity(group.len());
        let mut expected = 0usize;
        for (name, bytes) in group {
            let cursor = image_cursor(bytes).map_err(|source| CompactError::Decode {
                name: name.clone(),
                source,
            })?;
            expected += cursor.footer().entry_count as usize;
            cursors.push((name.as_str(), cursor.peekable()));
        }
        let mut builder = ImageBuilder::new(expected, *at, BLOCK_TARGET);
        loop {
            let mut best: Option<(usize, LayerKey)> = None;
            for (i, (name, cursor)) in cursors.iter_mut().enumerate() {
                match cursor.peek() {
                    None => {}
                    Some(Err(_)) => {
                        let source = cursor.next().expect("peeked").unwrap_err();
                        return Err(CompactError::Decode {
                            name: name.to_string(),
                            source,
                        });
                    }
                    // A tie goes to the later source, the last writer.
                    Some(Ok(e)) if best.is_none_or(|(_, k)| e.key <= k) => {
                        best = Some((i, e.key));
                    }
                    Some(Ok(_)) => {}
                }
            }
            let Some((take, key)) = best else { break };
            for (i, (_, cursor)) in cursors.iter_mut().enumerate() {
                if cursor
                    .peek()
                    .is_some_and(|r| r.as_ref().is_ok_and(|e| e.key == key))
                {
                    let e = cursor.next().expect("peeked").expect("checked");
                    if i == take && keep(&e.key) {
                        builder.push(e.key, &e.page)?;
                        based.insert(e.key);
                    }
                }
            }
        }
        if !builder.is_empty() {
            let (bytes, footer) = builder.finish()?;
            publish(bytes, &footer)?;
        }
    }

    // The deltas sort merged into one run the same way, and the pass
    // notes what the fresh image cut below will need: every merged
    // key, and whether a key's oldest record initializes its page.
    let mut merged_keys: BTreeSet<LayerKey> = BTreeSet::new();
    let mut first_init: BTreeMap<LayerKey, bool> = BTreeMap::new();
    let mut cursors = Vec::with_capacity(delta_srcs.len());
    let mut expected = 0usize;
    for (name, upto, bytes) in &delta_srcs {
        let cursor = delta_cursor(bytes).map_err(|source| CompactError::Decode {
            name: name.clone(),
            source,
        })?;
        expected += cursor.footer().entry_count as usize;
        cursors.push((name.as_str(), *upto, cursor.peekable()));
    }
    let mut builder = DeltaBuilder::new(expected, BLOCK_TARGET);
    loop {
        let mut best: Option<(usize, (LayerKey, Lsn))> = None;
        for (i, (name, upto, cursor)) in cursors.iter_mut().enumerate() {
            // An inherited layer's records past its cut never compete,
            // so a twin inside another source's cut still survives.
            while cursor
                .peek()
                .is_some_and(|r| r.as_ref().is_ok_and(|e| upto.is_some_and(|u| e.lsn > u)))
            {
                cursor.next();
            }
            match cursor.peek() {
                None => {}
                Some(Err(_)) => {
                    let source = cursor.next().expect("peeked").unwrap_err();
                    return Err(CompactError::Decode {
                        name: name.to_string(),
                        source,
                    });
                }
                Some(Ok(e)) if best.is_none_or(|(_, k)| (e.key, e.lsn) <= k) => {
                    best = Some((i, (e.key, e.lsn)));
                }
                Some(Ok(_)) => {}
            }
        }
        let Some((take, at)) = best else { break };
        for (i, (_, _, cursor)) in cursors.iter_mut().enumerate() {
            if cursor
                .peek()
                .is_some_and(|r| r.as_ref().is_ok_and(|e| (e.key, e.lsn) == at))
            {
                let e = cursor.next().expect("peeked").expect("checked");
                if i != take || !keep(&e.key) {
                    continue;
                }
                if !based.contains(&e.key) {
                    first_init.entry(e.key).or_insert_with(|| {
                        crate::walscan::record_init_refs(&e.record)
                            .unwrap_or_default()
                            .iter()
                            .any(|(b, init)| {
                                *init
                                    && (b.spc, b.db, b.rel, b.fork as u8, b.blk)
                                        == (e.key.spc, e.key.db, e.key.rel, e.key.fork, e.key.block)
                            })
                    });
                }
                merged_keys.insert(e.key);
                builder.push(e.key, e.lsn, &e.record)?;
            }
        }
    }
    if !builder.is_empty() {
        let (bytes, footer) = builder.finish()?;
        publish(bytes, &footer)?;
    }
    drop(cursors);
    drop(delta_srcs);
    drop(image_groups);

    // With a redo pool on hand, cut a fresh image at the flush point
    // so the merged run drops out of the read path for current reads.
    // Only relation pages materialize, and only those redo can build
    // from what the store holds: a base in a source image, or a first
    // record that initializes the page or carries a full image of it.
    // A page whose stored history starts mid chain stays in the delta
    // run; feeding its records a zeroed base would crash the redo
    // worker, not rebuild the page.
    if let Some(pool) = pool
        && dcl > Lsn(0)
    {
        let rebuildable =
            |k: &LayerKey| based.contains(k) || first_init.get(k).copied().unwrap_or(false);
        let keys: BTreeSet<LayerKey> = merged_keys
            .iter()
            .chain(based.iter())
            .copied()
            .filter(|k| k.kind == KEY_PAGE && rebuildable(k))
            .collect();
        let keys: Vec<LayerKey> = keys.into_iter().collect();
        if !keys.is_empty() {
            let map = LayerMap::new(descs.clone()).map_err(PageShardError::from)?;
            let svc = PageService::for_shard(store, tenant_ref, shard, Some(pool), data_checksums);
            let mem = Memtable::new();
            let mut builder = ImageBuilder::new(keys.len(), dcl, BLOCK_TARGET);
            for batch in keys.chunks(MAX_GETPAGE_BATCH) {
                let blocks: Vec<crate::walscan::BlockRef> = batch
                    .iter()
                    .map(|k| crate::walscan::BlockRef {
                        spc: k.spc,
                        db: k.db,
                        rel: k.rel,
                        fork: k.fork as u32,
                        blk: k.block,
                    })
                    .collect();
                let pages = svc.get_pages(&map, &mem, &blocks, dcl.0)?;
                for (key, page) in batch.iter().zip(pages) {
                    builder.push(*key, &page)?;
                }
            }
            let (bytes, footer) = builder.finish()?;
            publish(bytes, &footer)?;
        }
    }

    // The atomic commit. A concurrent flush that lands first makes the
    // CAS retry and its fresh layer survives the retire list, which
    // names only what this pass actually merged.
    let published = swap_layers(
        store,
        &layout.shard_manifest(shard),
        shard,
        &retire,
        &add,
        Some(manifest.shards),
    )?;
    let after: Vec<LayerDesc> = published
        .layers
        .iter()
        .map(|l| LayerDesc::parse(&l.name, l.size))
        .collect::<Result<_, _>>()
        .map_err(PageShardError::from)?;
    Ok(Some(CompactOutcome {
        retired: retire.len(),
        outputs: add.len(),
        debt_before,
        debt_after: debt(&after),
    }))
}

/// One queue entry: a shard, the debt that ranks it, and its read amp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub tenant: String,
    pub shard: u16,
    pub debt: u64,
    pub amp: usize,
}

impl Job {
    /// Queue order: shards past [`READ_AMP_BOUND`] first, they are
    /// blown bounds and not mere cost, then the deepest byte debt.
    fn rank(&self) -> (bool, std::cmp::Reverse<u64>, u16) {
        (
            self.amp <= READ_AMP_BOUND,
            std::cmp::Reverse(self.debt),
            self.shard,
        )
    }
}

/// Every shard of a tenant ranked worst first: the queue the scheduler
/// feeds from.
pub fn debts(store: &dyn CasStore, tenant_ref: &str) -> Result<Vec<Job>, CompactError> {
    let layout = TenantLayout::new(tenant_ref);
    let key = layout.manifest();
    let Some((data, _)) = store.get(&key)? else {
        return Err(CompactError::NoTenant { key });
    };
    let manifest = Manifest::from_json(&data)?;
    let mut jobs = Vec::new();
    for shard in 0..manifest.shards as u16 {
        let (descs, _) = load_serving_descs(store, tenant_ref, &manifest, shard)?;
        jobs.push(Job {
            tenant: tenant_ref.to_string(),
            shard,
            debt: debt(&descs),
            amp: read_amp(&descs),
        });
    }
    jobs.sort_by_key(Job::rank);
    Ok(jobs)
}

/// Run a queue of jobs on `workers` parallel workers, worst debt
/// first. `stop` is the preemption line: workers finish the job in
/// hand and take no more, and because every job commits with one CAS
/// a harder kill only leaves orphan objects behind. Per job results
/// come back in queue order; a failed job fails alone, the queue keeps
/// draining, and a rerun picks up whatever was left.
pub fn run_queue(
    store: &dyn CasStore,
    jobs: Vec<Job>,
    workers: usize,
    stop: &AtomicBool,
    pool: Option<&RedoPool>,
    data_checksums: bool,
) -> Vec<(Job, Result<Option<CompactOutcome>, CompactError>)> {
    let queue = Mutex::new(jobs.into_iter());
    let results = Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..workers.max(1) {
            scope.spawn(|| {
                loop {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let Some(job) = queue.lock().unwrap().next() else {
                        break;
                    };
                    let outcome =
                        compact_shard(store, &job.tenant, job.shard, pool, data_checksums);
                    results.lock().unwrap().push((job, outcome));
                }
            });
        }
    });
    let mut out = results.into_inner().unwrap();
    out.sort_by_key(|(job, _)| job.rank());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use zou_store::cas::Version;
    use zou_store::layer::{DeltaEntry, build_delta};
    use zou_store::mem::MemStore;
    use zou_store::shardmanifest::publish_layer;
    use zou_store::shards::{prune_lineage, split};

    fn seed(store: &dyn CasStore, tenant_ref: &str) -> TenantLayout {
        let layout = TenantLayout::new(tenant_ref);
        store
            .put_if_absent(&layout.manifest(), &Manifest::new(tenant_ref, 18).to_json())
            .unwrap();
        layout
    }

    fn put_delta(
        store: &dyn CasStore,
        layout: &TenantLayout,
        shard: u16,
        entries: &mut [DeltaEntry],
        dcl: u64,
    ) {
        entries.sort_by_key(|e| (e.key, e.lsn));
        let (bytes, footer) = build_delta(entries, 4096).unwrap();
        let desc = LayerDesc::from_footer(&footer, bytes.len() as u64);
        store
            .put_if_absent(
                &format!("{}{}", layout.shard_prefix(shard), desc.name()),
                &bytes,
            )
            .unwrap();
        let entry = LayerEntry {
            name: desc.name(),
            size: bytes.len() as u64,
            owner: None,
            upto: None,
        };
        publish_layer(
            store,
            &layout.shard_manifest(shard),
            shard,
            &entry,
            Lsn(dcl),
        )
        .unwrap();
    }

    fn rec(rel: u32, block: u32, lsn: u64) -> DeltaEntry {
        DeltaEntry {
            key: LayerKey::page(1663, 5, rel, 0, block),
            lsn: Lsn(lsn),
            record: vec![(lsn & 0xFF) as u8; 24],
        }
    }

    /// Blocks of one relation landing on each child of a two way
    /// split, found by probing the frozen shard function.
    fn split_blocks(rel: u32) -> (u32, u32) {
        let mut on0 = None;
        let mut on1 = None;
        for b in 0..200u32 {
            let block = b * 20_000;
            match shard_of(&LayerKey::page(1663, 5, rel, 0, block), 2) {
                0 if on0.is_none() => on0 = Some(block),
                1 if on1.is_none() => on1 = Some(block),
                _ => {}
            }
        }
        (on0.unwrap(), on1.unwrap())
    }

    #[test]
    fn debt_counts_delta_bytes_above_the_newest_image() {
        let k = |b: u32| LayerKey::page(1663, 5, 90, 0, b);
        let image = |lsn: u64| LayerDesc {
            size: 1000,
            ..LayerDesc::image(k(0), k(99), Lsn(lsn))
        };
        let delta = |lo: u64, hi: u64, size: u64| LayerDesc {
            size,
            ..LayerDesc::delta(k(0), k(99), Lsn(lo), Lsn(hi))
        };
        assert_eq!(debt(&[]), 0);
        assert_eq!(debt(&[delta(10, 50, 7)]), 7);
        // Deltas at or below the newest image are folded already.
        assert_eq!(debt(&[image(100), delta(10, 100, 7), delta(90, 150, 3)]), 3);
        assert_eq!(debt(&[image(200), delta(10, 100, 7)]), 0);
    }

    #[test]
    fn a_full_pass_separates_a_split_child_and_retires_the_lineage() {
        let store = MemStore::default();
        let layout = seed(&store, "t");
        let (b0, b1) = split_blocks(90);
        let mut batch1 = vec![rec(90, b0, 0x100), rec(90, b1, 0x110)];
        let mut batch2 = vec![rec(90, b0, 0x200), rec(90, b1, 0x210)];
        put_delta(&store, &layout, 0, &mut batch1, 0x110);
        put_delta(&store, &layout, 0, &mut batch2, 0x210);
        let manifest = split(&store, "t").unwrap();

        // The child leans on the parent era before the pass.
        let (descs, _) = load_serving_descs(&store, "t", &manifest, 1).unwrap();
        assert!(descs.iter().any(|d| d.home == Some(0)));

        let out = compact_shard(&store, "t", 1, None, false)
            .unwrap()
            .expect("foreign layers mean work");
        assert_eq!((out.retired, out.outputs), (0, 1));

        // Afterwards it stands alone: one owned run holding exactly
        // its half, reachable without the lineage.
        let (descs, floor) = load_serving_descs(&store, "t", &manifest, 1).unwrap();
        assert_eq!(descs.len(), 1);
        assert!(descs[0].home.is_none() && descs[0].owner.is_none());
        assert_eq!(floor, Lsn(0), "a child that never flushed has no own dcl");
        let reader = LayerReader::for_shard(&store, "t", 1);
        let map = LayerMap::new(descs).unwrap();
        let mem = Memtable::new();
        let key = LayerKey::page(1663, 5, 90, 0, b1);
        let got = reader.reconstruct(&map, &mem, &key, Lsn(0x300)).unwrap();
        assert_eq!(
            got.records.iter().map(|(l, _)| *l).collect::<Vec<_>>(),
            vec![Lsn(0x110), Lsn(0x210)],
            "both records of the child's half survived the rewrite"
        );
        let other = LayerKey::page(1663, 5, 90, 0, b0);
        let got = reader.reconstruct(&map, &mem, &other, Lsn(0x300)).unwrap();
        assert!(got.records.is_empty(), "the sibling's half stayed behind");

        // The lineage holds until every shard stands alone, then goes.
        assert!(!prune_lineage(&store, "t").unwrap());
        compact_shard(&store, "t", 0, None, false).unwrap().unwrap();
        assert!(prune_lineage(&store, "t").unwrap());
        let (data, _) = store.get(&layout.manifest()).unwrap().unwrap();
        let manifest = Manifest::from_json(&data).unwrap();
        assert!(manifest.shard_history.is_empty());
        assert_eq!(manifest.shards, 2);
        // Serving still works from the pruned lineage.
        let (descs, _) = load_serving_descs(&store, "t", &manifest, 1).unwrap();
        assert_eq!(descs.len(), 1);
    }

    #[test]
    fn merging_own_runs_preserves_every_record() {
        let store = MemStore::default();
        let layout = seed(&store, "t");
        for (lsn, dcl) in [(0x100u64, 0x100u64), (0x200, 0x200), (0x300, 0x300)] {
            let mut batch = vec![rec(90, 1, lsn), rec(90, 2, lsn + 1)];
            put_delta(&store, &layout, 0, &mut batch, dcl);
        }
        let out = compact_shard(&store, "t", 0, None, false)
            .unwrap()
            .expect("three runs merge");
        assert_eq!((out.retired, out.outputs), (3, 1));
        assert!(out.debt_after <= out.debt_before);

        let (m, _) = PageShardManifest::load(&store, &layout.shard_manifest(0))
            .unwrap()
            .unwrap();
        assert_eq!(m.layers.len(), 1);
        assert_eq!(m.covers, Some(1));
        assert_eq!(m.disk_consistent_lsn, Lsn(0x300), "the swap keeps the dcl");
        let reader = LayerReader::new(&store, layout.shard_prefix(0));
        let map = m.layer_map().unwrap();
        let mem = Memtable::new();
        for (block, lsns) in [(1u32, [0x100u64, 0x200, 0x300]), (2, [0x101, 0x201, 0x301])] {
            let key = LayerKey::page(1663, 5, 90, 0, block);
            let got = reader.reconstruct(&map, &mem, &key, Lsn(0x400)).unwrap();
            assert_eq!(
                got.records.iter().map(|(l, _)| l.0).collect::<Vec<_>>(),
                lsns.to_vec()
            );
        }

        // Running again finds one owned run and nothing foreign.
        assert!(
            compact_shard(&store, "t", 0, None, false)
                .unwrap()
                .is_none()
        );
    }

    /// A store that starts refusing writes after a budget, the shape
    /// of a spot worker dying mid pass.
    struct DyingStore {
        inner: MemStore,
        writes_left: AtomicUsize,
    }

    impl CasStore for DyingStore {
        fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
            self.inner.get(key)
        }
        fn put_if_match(
            &self,
            key: &str,
            data: &[u8],
            expected: Option<&Version>,
        ) -> Result<Version, CasError> {
            if self.writes_left.fetch_sub(1, Ordering::SeqCst) == 0 {
                return Err(CasError::Io {
                    key: key.to_string(),
                    source: std::io::Error::other("preempted"),
                });
            }
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
    fn a_preempted_pass_leaves_reads_untouched_and_reruns_clean() {
        let store = MemStore::default();
        let layout = seed(&store, "t");
        for lsn in [0x100u64, 0x200] {
            let mut batch = vec![rec(90, 1, lsn)];
            put_delta(&store, &layout, 0, &mut batch, lsn);
        }
        let (before, _) = PageShardManifest::load(&store, &layout.shard_manifest(0))
            .unwrap()
            .unwrap();

        // Die on the commit CAS: the output object landed, the
        // manifest never heard of it.
        let dying = DyingStore {
            inner: store,
            writes_left: AtomicUsize::new(1),
        };
        compact_shard(&dying, "t", 0, None, false).unwrap_err();
        let store = dying.inner;
        let (after, _) = PageShardManifest::load(&store, &layout.shard_manifest(0))
            .unwrap()
            .unwrap();
        assert_eq!(after, before, "a dead worker never moves the manifest");
        assert_eq!(
            store.list(&layout.shard_prefix(0)).unwrap().len(),
            4,
            "two inputs, the manifest, and one orphan output"
        );

        // The rerun redoes the pass over the orphan, idempotently.
        let out = compact_shard(&store, "t", 0, None, false)
            .unwrap()
            .expect("the work is still there");
        assert_eq!((out.retired, out.outputs), (2, 1));
        let (m, _) = PageShardManifest::load(&store, &layout.shard_manifest(0))
            .unwrap()
            .unwrap();
        assert_eq!(m.layers.len(), 1);
    }

    #[test]
    fn a_blown_amp_bound_outranks_deeper_byte_debt() {
        let store = MemStore::default();
        let layout = seed(&store, "t");
        split(&store, "t").unwrap();
        let (b0, b1) = split_blocks(90);
        // Settle the split first so each child serves only its own
        // runs and the amp counts below are not muddied by lineage.
        for (shard, block) in [(0, b0), (1, b1)] {
            let mut batch = vec![rec(90, block, 0x100)];
            put_delta(&store, &layout, shard, &mut batch, 0x100);
            compact_shard(&store, "t", shard, None, false).unwrap();
        }
        assert!(prune_lineage(&store, "t").unwrap());

        // Shard 0 takes one fat run, the deepest byte debt by far.
        // Shard 1 takes five more tiny runs, blowing the amp bound on
        // a fraction of the bytes.
        let mut fat: Vec<DeltaEntry> = (0..200u64).map(|i| rec(90, b0, 0x200 + i)).collect();
        put_delta(&store, &layout, 0, &mut fat, 0x200 + 199);
        for i in 0..5u64 {
            let mut batch = vec![rec(90, b1, 0x200 + i)];
            put_delta(&store, &layout, 1, &mut batch, 0x200 + i);
        }

        let jobs = debts(&store, "t").unwrap();
        assert_eq!(jobs.len(), 2);
        assert!(
            jobs[1].debt > jobs[0].debt,
            "the byte debt points the other way"
        );
        assert_eq!(jobs[0].shard, 1, "the blown bound jumps the queue");
        assert!(jobs[0].amp > READ_AMP_BOUND, "amp {}", jobs[0].amp);

        // One pass puts the shard back under the bound, and byte debt
        // ranks the queue again.
        compact_shard(&store, "t", 1, None, false).unwrap().unwrap();
        let jobs = debts(&store, "t").unwrap();
        assert!(jobs.iter().all(|j| j.amp <= READ_AMP_BOUND));
        assert_eq!(jobs[0].shard, 0);
    }

    #[test]
    fn the_queue_takes_the_worst_debt_first_and_honors_stop() {
        let store = MemStore::default();
        let layout = seed(&store, "t");
        split(&store, "t").unwrap();
        let (b0, b1) = split_blocks(90);
        // Shard 1 carries more delta runs, so more debt.
        let mut batch = vec![rec(90, b0, 0x100)];
        put_delta(&store, &layout, 0, &mut batch, 0x100);
        for lsn in [0x100u64, 0x200, 0x300] {
            let mut batch = vec![rec(90, b1, lsn)];
            put_delta(&store, &layout, 1, &mut batch, lsn);
        }
        let jobs = debts(&store, "t").unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].shard, 1, "worst debt first");
        assert!(jobs[0].debt > jobs[1].debt);

        // A stop that is already up drains nothing.
        let stopped = AtomicBool::new(true);
        let results = run_queue(&store, jobs.clone(), 2, &stopped, None, false);
        assert!(results.is_empty());

        let stop = AtomicBool::new(false);
        let results = run_queue(&store, jobs, 2, &stop, None, false);
        assert_eq!(results.len(), 2);
        for (_, outcome) in &results {
            outcome.as_ref().unwrap();
        }
        assert!(prune_lineage(&store, "t").unwrap());
    }
}
