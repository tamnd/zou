//! What has to be true of a branch before anybody is told it exists.
//!
//! [`zou_store::branch()`] writes a child manifest and its shard
//! manifests and nothing else, which is the whole point: no data is
//! copied and the call is a handful of round trips. What it cannot
//! know is whether the objects those manifests name are enough to
//! serve a database from. A child has no fallback for a page it
//! cannot find, so a source that has folded nothing down yet yields a
//! child that looks like a database in every listing and fails on the
//! first page read.
//!
//! There are two shapes of enough, one per read path, and the answer
//! is the same sentence either way: fold first. The object path wants
//! a full capture bearing page runs. The page service wants an image
//! layer at or below the branch point on every shard.
//!
//! So the two halves live here rather than in one caller: the check
//! that takes the child back off the store when it would not serve, and
//! the removal itself, which `zou branch delete` needs for a child that
//! did serve and is no longer wanted.

use std::collections::BTreeMap;
use std::sync::Arc;

use zou_log::{TeeFilter, WalMedia, catch_up_with};

use crate::ingest::ShardIngest;
use crate::redo::RedoPool;
use crate::relsize;
use zou_store::layer::{ImageBuilder, LayerKey, LayerKind, PAGE_IMAGE_LEN};
use zou_store::layermap::LayerDesc;
use zou_store::layout::TenantLayout;
use zou_store::lsn::Lsn;
use zou_store::shardmanifest::{LayerEntry, PageShardManifest, publish_layer};
use zou_store::{CasError, CasStore, Manifest, shards};

/// Everything under a ref's prefix, and the count of what went.
///
/// The manifest goes first. Everything else under the prefix is
/// unreachable once it is gone, so a removal interrupted halfway leaves
/// objects nobody can read rather than a tenant whose manifest points
/// at objects that are no longer there.
pub fn discard(store: &dyn CasStore, tenant_ref: &str) -> Result<usize, String> {
    let layout = TenantLayout::new(tenant_ref);
    store
        .delete(&layout.manifest())
        .map_err(|e| format!("store: {e}"))?;
    let prefix = format!("{}/", layout.prefix());
    let keys = store.list(&prefix).map_err(|e| format!("store: {e}"))?;
    let mut deleted = 1;
    for key in &keys {
        store.delete(key).map_err(|e| format!("store: {e}"))?;
        deleted += 1;
    }
    Ok(deleted)
}

/// Which read path a child is going to be served through, and so
/// which of the two rules it has to satisfy.
///
/// This is a caller's answer rather than something read out of the
/// environment here, because the process asking is not always the
/// process serving. The embedded library runs its postmasters with
/// the page service pinned off whatever the ambient setting says, so
/// it asks for [`ReadPath::Objects`] by name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadPath {
    /// Pages come out of the page runs a fold packed into a capture.
    Objects,
    /// Pages come out of the layers the page shards publish.
    Layers,
}

impl ReadPath {
    /// What this process would serve through, which is what the CLI
    /// wants: it branches and serves under one setting.
    pub fn current() -> Self {
        match crate::pageserve_enabled() {
            true => Self::Layers,
            false => Self::Objects,
        }
    }
}

/// Refuse a child that would not serve, and leave nothing of it behind.
///
/// Asking costs one store round trip per inherited checkpoint, or one
/// per page shard on the layer path, which is the cheapest this answer
/// is ever going to be, and it is asked while somebody is still
/// looking at the terminal rather than an hour later on the first page
/// read.
///
/// The two paths need different things from the same manifest. The
/// object path reads inherited pages out of the page runs a fold
/// packed into a capture. The page service reads them out of the
/// layers, and never out of the parent's `pg/`: those objects are the
/// parent's live base images, a truncate deletes them, and a child
/// that leaned on them would lose its floor the first time the parent
/// dropped a table.
pub fn refuse_unservable(
    store: &dyn CasStore,
    src: &str,
    dst: &str,
    manifest: &Manifest,
    path: ReadPath,
) -> Result<(), String> {
    let Some(why) = why_unbranchable(store, &TenantLayout::new(dst), manifest, path)? else {
        return Ok(());
    };
    discard(store, dst)?;
    Err(format!(
        "{src} cannot be branched yet, {why}. A fold packs one down after a few checkpoints of \
         writes, so keep the source running and try again. Nothing of {dst} was left on the store"
    ))
}

/// What a source with nothing published yet is told, the layer path's
/// version of the object path's no chain.
const NO_CHECKPOINT: &str = "there is no checkpoint to branch at yet, \
                             so there is nothing for a child to stand on";

/// Whether a branch off this manifest would serve, and why not when
/// it would not.
///
/// Asked of a child that has just been written, where the branch point
/// is the one it names, and of a source somebody is about to branch,
/// where it is the newest checkpoint, which is where a head branch
/// would cut. Both are the same question about the same objects.
///
/// The rule follows the read path the child will be served through,
/// since the two paths need different things out of one manifest.
pub fn why_unbranchable(
    store: &dyn CasStore,
    layout: &TenantLayout,
    manifest: &Manifest,
    path: ReadPath,
) -> Result<Option<String>, String> {
    match path {
        ReadPath::Layers => why_layerless(store, layout, manifest),
        ReadPath::Objects => crate::reader::why_unservable(store, layout, manifest),
    }
}

/// The page service's version of the same question, asked of the
/// shard manifests.
///
/// A block the child reads resolves to a base image and the records
/// above it. The records are inherited, cut at the branch point, and
/// so is the image, but only if there is one at or below that point:
/// an image taken later is the parent's future and the branch drops
/// it. Below every image the reader falls back to `pg/`, which is the
/// one thing a child does not inherit, so a shard whose oldest cover
/// is a delta is a child that can read whatever postgres extended
/// since the fold and nothing older, which is every catalog block it
/// needs to start.
///
/// So the rule is one image at or below the branch point per shard,
/// which is the layer world's version of the object path's full
/// capture, and a fold is what produces one either way.
fn why_layerless(
    store: &dyn CasStore,
    layout: &TenantLayout,
    manifest: &Manifest,
) -> Result<Option<String>, String> {
    let Some(at) = cut_at(manifest) else {
        return Ok(Some(NO_CHECKPOINT.to_string()));
    };
    for shard in 0..manifest.shards as u16 {
        let key = layout.shard_manifest(shard);
        let Some((shard_manifest, _)) =
            PageShardManifest::load(store, &key).map_err(|e| format!("shard {shard}: {e}"))?
        else {
            return Ok(Some(format!(
                "page shard {shard} has published nothing yet, so there is no image to serve \
                 inherited pages from"
            )));
        };
        if !has_image_at_or_below(&shard_manifest, at)? {
            return Ok(Some(format!(
                "no image layer at or below {:#x} on page shard {shard} to serve inherited pages \
                 from, fold one in the source first",
                at.0
            )));
        }
    }
    Ok(None)
}

/// Where a branch of this manifest would cut, which is the point every
/// shard needs an image at or below.
///
/// A child names its cut. A source has not been cut yet, so the answer
/// is its newest checkpoint, which is where a head branch of it lands.
fn cut_at(manifest: &Manifest) -> Option<Lsn> {
    match manifest.branch_of.as_ref().map(|b| b.at_lsn) {
        Some(at) => Some(at),
        None => manifest.checkpoints.last().map(|c| c.lsn),
    }
}

/// Which shards would leave a child with no floor, worst first.
///
/// The answer to a shard on this list is a fold. Everything else about
/// it is the same question `why_layerless` asks and is answered off
/// the same objects.
pub fn floorless(
    store: &dyn CasStore,
    layout: &TenantLayout,
    manifest: &Manifest,
) -> Result<Vec<u16>, String> {
    let Some(at) = cut_at(manifest) else {
        return Err(NO_CHECKPOINT.to_string());
    };
    let mut want = Vec::new();
    for shard in 0..manifest.shards as u16 {
        let loaded = PageShardManifest::load(store, &layout.shard_manifest(shard))
            .map_err(|e| format!("shard {shard}: {e}"))?;
        match loaded {
            Some((shard_manifest, _)) if has_image_at_or_below(&shard_manifest, at)? => {}
            // A shard that has published nothing has nothing to fold
            // either. It is still floorless and the caller is still
            // told, since a fold that quietly skipped it would leave a
            // branch that fails on its first page read.
            _ => want.push(shard),
        }
    }
    Ok(want)
}

/// The same question `why_layerless` answers, asked by a caller that
/// is going to call [`fold_for_branch`] first.
///
/// A source with no floor is not unbranchable to such a caller, it is a
/// source with a fold to cut, and answering no would have a host
/// refusing to offer branching for a database that branches fine. What
/// is still no is a shard with no floor and nothing of its own to make
/// one out of: a shard that has published nothing at all, or one still
/// riding somebody else's layers, where the fold has nothing to fold.
pub fn why_unbranchable_after_fold(
    store: &dyn CasStore,
    layout: &TenantLayout,
    manifest: &Manifest,
) -> Result<Option<String>, String> {
    if cut_at(manifest).is_none() {
        return Ok(Some(NO_CHECKPOINT.to_string()));
    }
    for shard in floorless(store, layout, manifest)? {
        let own = PageShardManifest::load(store, &layout.shard_manifest(shard))
            .map_err(|e| format!("shard {shard}: {e}"))?;
        let has_own = own.is_some_and(|(m, _)| !m.layers.is_empty());
        if !has_own {
            return Ok(Some(format!(
                "page shard {shard} has published nothing yet, so there is no image to serve \
                 inherited pages from and nothing to fold one out of"
            )));
        }
    }
    Ok(None)
}

/// Block target for the seeded layers, the same one compaction hands
/// its image builder.
const SEED_BLOCK_TARGET: usize = 256 << 10;

/// How much of an image to fill before publishing it and starting
/// another, so a source with a large `pg/` does not build one layer
/// the size of its whole base in memory.
const SEED_TARGET_BYTES: usize = 64 << 20;

/// Where a seeded image sits: under everything the layers hold, so
/// every record above it still applies on top.
///
/// The objects it is built from are the base postgres wrote before
/// the puts were elided, which is exactly what the live service reads
/// as its fallback base today, and redo stamps each page with its
/// record's lsn, so a record the base already contains is skipped
/// rather than applied twice. Putting the seed at the bottom also
/// means a fold's fresher image of the same key wins, which is the
/// order that keeps a stale frozen object from masking a newer one.
const SEED_LSN: Lsn = Lsn(1);

/// What a seeded entry is built from: a page object under `pg/`, or a
/// fork length read out of a SIZE marker.
enum Seed {
    Page(String),
    Size(u32),
}

/// Copy the source's own base into the layers, where a child can read
/// it.
///
/// A branch inherits layers and nothing else. It does not inherit the
/// `pg/` prefix, and it must not: those are the parent's live base
/// images, a truncate deletes them, and a child leaning on them would
/// lose its floor the first time the parent dropped a table. But an
/// initdb writes its catalogs to `pg/` and the WAL that follows only
/// touches a few hundred of those pages, so the layers hold nothing
/// about pg_database or pg_authid and a child comes up unable to find
/// the database it was told to connect to.
///
/// So before a source is branched, everything under its `pg/` is
/// imaged into its layers: the pages as pages, and the SIZE markers as
/// the `Set` records a length reads back out of. The cost is one list
/// and one get per object, paid once by whoever seals the thing that
/// is going to be branched.
///
/// The count is entries written.
pub fn seed_base(
    store: &dyn CasStore,
    tenant_ref: &str,
    manifest: &Manifest,
) -> Result<usize, String> {
    let layout = TenantLayout::new(tenant_ref);
    let shard_count = manifest.shards;
    let mut by_shard: BTreeMap<u16, Vec<(LayerKey, Seed)>> = BTreeMap::new();
    for (key, seed) in pg_objects(store, &layout)? {
        let shard = shards::shard_of(&key, shard_count);
        by_shard.entry(shard).or_default().push((key, seed));
    }
    let mut written = 0;
    for (shard, mut entries) in by_shard {
        entries.sort_by_key(|e| e.0);
        entries.dedup_by(|a, b| a.0 == b.0);
        let mut builder = ImageBuilder::new(SEED_LSN, SEED_BLOCK_TARGET);
        for (key, seed) in &entries {
            let page = match seed {
                // A page that has gone since the listing is one this
                // seed does not need: the object is only the base, and
                // a relation dropped out from under us has no child
                // that wants it.
                Seed::Page(object) => match store.get(object).map_err(|e| format!("store: {e}"))? {
                    Some((page, _)) if page.len() == PAGE_IMAGE_LEN => page,
                    _ => continue,
                },
                Seed::Size(n) => {
                    let mut page = vec![0u8; PAGE_IMAGE_LEN];
                    page[..relsize::REC_LEN].copy_from_slice(&relsize::SizeRec::Set(*n).encode());
                    page
                }
            };
            builder
                .push(*key, &page)
                .map_err(|e| format!("shard {shard}: {e}"))?;
            written += 1;
            if builder.bytes() >= SEED_TARGET_BYTES {
                publish_seed(store, &layout, shard, builder)?;
                builder = ImageBuilder::new(SEED_LSN, SEED_BLOCK_TARGET);
            }
        }
        if !builder.is_empty() {
            publish_seed(store, &layout, shard, builder)?;
        }
    }
    Ok(written)
}

/// Land one seeded image and list it on the shard.
///
/// The bytes go under a name derived from what is in them, so a second
/// attempt after a crash writes the same object and adopting the one
/// already there is the whole of the retry logic. The publish does not
/// move the shard's flush point: this is history being filled in
/// underneath, not anything new arriving.
fn publish_seed(
    store: &dyn CasStore,
    layout: &TenantLayout,
    shard: u16,
    builder: ImageBuilder,
) -> Result<(), String> {
    let (bytes, footer) = builder
        .finish()
        .map_err(|e| format!("shard {shard}: {e}"))?;
    let desc = LayerDesc::from_footer(&footer, bytes.len() as u64);
    let key = format!("{}{}", layout.shard_prefix(shard), desc.name());
    match store.put_if_absent(&key, &bytes) {
        Ok(_) => {}
        Err(CasError::AlreadyExists { .. }) => {}
        Err(e) => return Err(format!("shard {shard}: {e}")),
    }
    publish_layer(
        store,
        &layout.shard_manifest(shard),
        shard,
        &LayerEntry {
            name: desc.name(),
            size: bytes.len() as u64,
            owner: None,
            upto: None,
        },
        SEED_LSN,
    )
    .map_err(|e| format!("shard {shard}: {e}"))?;
    Ok(())
}

/// Everything under the tenant's own `pg/` prefix that a layer can
/// hold: one key per page object and one per SIZE marker.
///
/// One list of the prefix, and the gets happen later, one at a time as
/// the image is filled, so the whole base is never in memory at once.
fn pg_objects(
    store: &dyn CasStore,
    layout: &TenantLayout,
) -> Result<Vec<(LayerKey, Seed)>, String> {
    let prefix = layout.pg_dir();
    let mut out = Vec::new();
    for key in store.list(&prefix).map_err(|e| format!("store: {e}"))? {
        let rest = key.strip_prefix(&prefix).unwrap_or(&key);
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() != 5 {
            continue;
        }
        let (Ok(spc), Ok(db), Ok(rel), Ok(fork)) = (
            parts[0].parse::<u32>(),
            parts[1].parse::<u32>(),
            parts[2].parse::<u32>(),
            parts[3].parse::<u8>(),
        ) else {
            continue;
        };
        if parts[4] == "SIZE" {
            let Some((data, _)) = store.get(&key).map_err(|e| format!("store: {e}"))? else {
                continue;
            };
            let n = zou_store::forksize::ForkSize::decode(&data)
                .map(|fs| fs.nblocks)
                .ok_or_else(|| format!("bad SIZE object at {key}"))?;
            out.push((LayerKey::relsize(spc, db, rel, fork), Seed::Size(n)));
            continue;
        }
        let Ok(blk) = parts[4].parse::<u32>() else {
            continue;
        };
        out.push((LayerKey::page(spc, db, rel, fork, blk), Seed::Page(key)));
    }
    Ok(out)
}

/// Apply whatever sealed WAL the page shard has not ingested yet, so
/// the shard is consistent through the end of the source's own log.
///
/// A branch cut leaves the child standing on
/// `min(parent disk consistent lsn, branch point)` and then hands it a
/// log that starts at the branch point, because that is where the
/// child's recovery starts. When the parent's ingest stopped short of
/// its own last checkpoint, those two numbers differ, and the records
/// in between belong to a log the child has no copy of and cannot ask
/// for. Its ingest reads the hole for what it is, refuses to apply
/// past it, and every page read on the child fails. Closing it here
/// costs the tail of one log, which on a source that has just been
/// sealed is a checkpoint record or two.
///
/// The returned lsn is what the shard is consistent through, which is
/// the anchor a child inherits.
fn catch_up_shard(store: &Arc<dyn CasStore>, tenant_ref: &str) -> Result<u64, String> {
    let layout = TenantLayout::new(tenant_ref);
    let tenant = zou_store::layout::tenant_id(tenant_ref);
    let Some((shard_manifest, _)) =
        PageShardManifest::load(&**store, &layout.shard_manifest(0)).map_err(|e| e.to_string())?
    else {
        // Nothing ingested at all, so there is no anchor to advance
        // and no child that could stand on one. `why_layerless` is the
        // one that says so, in a sentence somebody can act on.
        return Ok(0);
    };
    let mut ingest = ShardIngest::new(
        crate::pagesvc::ingest_config(tenant),
        shard_manifest.disk_consistent_lsn.0,
    );
    let media = WalMedia::single(crate::log_store(Arc::clone(store), &layout));
    let filter = TeeFilter::Tenant(tenant);
    catch_up_with::<String, _>(
        &media,
        crate::WAL_SHARD,
        &filter,
        Lsn(ingest.applied()),
        |frame| {
            ingest
                .apply_frames(std::slice::from_ref(&frame))
                .map_err(|e| format!("catching the shard up: {e}"))?;
            // Bytes is the only reason that fires with a durable end
            // of zero and no age, which is the one this wants: the
            // memtable stays bounded on a long tail without a flush
            // per frame on a short one.
            if ingest.flush_reason(0, std::time::Duration::ZERO).is_some() {
                ingest
                    .flush(&**store, &layout)
                    .map_err(|e| format!("flushing the catch up: {e}"))?;
            }
            Ok(())
        },
    )?;
    ingest
        .flush(&**store, &layout)
        .map_err(|e| format!("flushing the catch up: {e}"))?;
    // And say so even when the tail changed no page, which is the
    // ordinary case for the last records a shutdown writes: a flush
    // with an empty memtable publishes nothing, and the lsn left
    // behind is the one a child would be anchored at.
    zou_store::shardmanifest::advance_consistent(
        &**store,
        &layout.shard_manifest(0),
        Lsn(ingest.applied()),
    )
    .map_err(|e| format!("advancing the shard: {e}"))?;
    Ok(ingest.applied())
}

/// Cut the folds a branch of this source needs, rather than wait for
/// the background one to earn them.
///
/// Compaction cuts an image on its own after enough delta debt, which
/// is the right rule for a database that has been running for a while
/// and no rule at all for a young one. A template is a fresh initdb and
/// a couple of thousand rows: it would never earn a fold, so every
/// branch of it would be refused for want of something the source was
/// never going to produce. This is the same fold asked for by name.
///
/// The cost is one merge per shard that has no image at or below the
/// cut, and it is paid where it belongs: once, by whoever seals the
/// thing that is going to be branched, rather than once per branch.
/// A shard that already has a floor is not touched.
///
/// The count is how many shards were folded, which is zero on a source
/// that was already branchable.
pub fn fold_for_branch(
    store: &Arc<dyn CasStore>,
    tenant_ref: &str,
    manifest: &Manifest,
    pool: &RedoPool,
    data_checksums: bool,
) -> Result<usize, String> {
    let layout = TenantLayout::new(tenant_ref);
    let at = cut_at(manifest).ok_or_else(|| NO_CHECKPOINT.to_string())?;
    // Before anything else, because the merge's horizon is bounded by
    // the shard's own consistent point and a child's floor is bounded
    // by the same number.
    let caught = catch_up_shard(store, tenant_ref)?;
    log::debug!("fold for branch: the shard is consistent through {caught:#x}");
    let store: &dyn CasStore = &**store;
    // Before the merge, so the images it cuts carry the lengths too
    // and the seeded layer is retired along with everything else.
    let seeded = seed_base(store, tenant_ref, manifest)?;
    log::debug!("fold for branch: seeded {seeded} entries from the pg objects");
    let mut folded = 0;
    for shard in floorless(store, &layout, manifest)? {
        // One shard at a time, for the reason the offline fold gives:
        // a merge holds an image in memory while it fills.
        let out =
            crate::compact::merge_to_horizon(store, tenant_ref, shard, at, pool, data_checksums)
                .map_err(|e| format!("shard {shard}: {e}"))?;
        match out {
            Some(out) => {
                log::debug!(
                    "fold for branch: shard {shard} imaged {} pages and {} fork lengths at {}",
                    out.imaged,
                    out.sized,
                    out.horizon
                );
                folded += 1;
            }
            // Nothing below the horizon to fold, which on a shard with
            // no floor means the layers it would have folded are
            // somebody else's. Saying so beats a branch that is refused
            // later for a reason that reads like a bug.
            None => {
                return Err(format!(
                    "page shard {shard} has no image at or below {:#x} and nothing of its own to \
                     fold into one, so a branch of {tenant_ref} would not serve",
                    at.0
                ));
            }
        }
    }
    Ok(folded)
}

/// Whether a shard carries an image the child can stand on, which is
/// one whose lsn is at or below the branch point. The branch has
/// already dropped the ones above it, so in practice this is asking
/// whether anything survived the cut.
fn has_image_at_or_below(manifest: &PageShardManifest, at: Lsn) -> Result<bool, String> {
    for entry in &manifest.layers {
        let desc = LayerDesc::parse(&entry.name, entry.size)
            .map_err(|e| format!("layer {}: {e}", entry.name))?;
        if desc.kind == LayerKind::Image && desc.min_lsn <= at {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zou_store::LocalFsStore;
    use zou_store::layer::LayerKey;
    use zou_store::manifest::{BranchOf, CheckpointKind, CheckpointRef};
    use zou_store::shardmanifest::LayerEntry;

    /// The whole keyspace, which is what a fold's image covers and
    /// what the rule is about; the bounds are not what is under test.
    fn whole() -> (LayerKey, LayerKey) {
        (
            LayerKey::page(0, 0, 0, 0, 0),
            LayerKey::page(u32::MAX, u32::MAX, u32::MAX, u8::MAX, u32::MAX),
        )
    }

    fn entry(desc: LayerDesc) -> LayerEntry {
        LayerEntry {
            name: desc.name(),
            size: 4096,
            owner: None,
            upto: None,
        }
    }

    /// A child manifest cut at `at`, which is the shape the check is
    /// asked of in production: the branch has been written and is
    /// about to be either kept or taken back off the store.
    fn child(at: Lsn) -> Manifest {
        let mut m = Manifest::new("pr-1", 18);
        m.branch_of = Some(BranchOf {
            tenant_ref: "prod".into(),
            at_lsn: at,
        });
        m.checkpoints.push(CheckpointRef {
            id: "c1".into(),
            lsn: at,
            kind: CheckpointKind::Full,
            owner: None,
        });
        m
    }

    fn publish(store: &dyn CasStore, layout: &TenantLayout, layers: Vec<LayerEntry>) {
        let mut shard = PageShardManifest::new(0);
        shard.disk_consistent_lsn = Lsn(0x1000);
        shard.layers = layers;
        store
            .put(&layout.shard_manifest(0), &shard.encode())
            .unwrap();
    }

    #[test]
    fn a_shard_that_has_published_nothing_cannot_be_stood_on() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let layout = TenantLayout::new("pr-1");
        let why = why_layerless(&store, &layout, &child(Lsn(0x100)))
            .unwrap()
            .unwrap();
        assert!(why.contains("page shard 0 has published nothing"), "{why}");
    }

    #[test]
    fn deltas_alone_are_a_child_with_no_floor() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let layout = TenantLayout::new("pr-1");
        let (lo, hi) = whole();
        publish(
            &store,
            &layout,
            vec![entry(LayerDesc::delta(lo, hi, Lsn(0x10), Lsn(0x100)))],
        );
        let why = why_layerless(&store, &layout, &child(Lsn(0x100)))
            .unwrap()
            .unwrap();
        assert!(why.contains("no image layer at or below"), "{why}");
        assert!(why.contains("fold one in the source first"), "{why}");
    }

    /// An image the branch point sits on is a floor. One above it is
    /// the parent's future and the branch has already dropped it, so
    /// the same shard answers differently depending on where the cut
    /// is, which is the only interesting thing this rule does.
    #[test]
    fn the_image_has_to_be_at_or_below_the_cut() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let layout = TenantLayout::new("pr-1");
        let (lo, hi) = whole();
        publish(
            &store,
            &layout,
            vec![
                entry(LayerDesc::delta(lo, hi, Lsn(0x10), Lsn(0x400))),
                entry(LayerDesc::image(lo, hi, Lsn(0x200))),
            ],
        );
        assert!(
            why_layerless(&store, &layout, &child(Lsn(0x200)))
                .unwrap()
                .is_none()
        );
        assert!(
            why_layerless(&store, &layout, &child(Lsn(0x300)))
                .unwrap()
                .is_none()
        );
        let why = why_layerless(&store, &layout, &child(Lsn(0x100)))
            .unwrap()
            .unwrap();
        assert!(why.contains("no image layer at or below 0x100"), "{why}");
    }

    /// The shards a fold has to touch are the ones with no floor, and
    /// the ones that have one are left alone, which is what keeps a
    /// branch of a sealed template free.
    #[test]
    fn only_the_shards_with_no_floor_are_asked_for() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let layout = TenantLayout::new("pr-1");
        let (lo, hi) = whole();
        publish(
            &store,
            &layout,
            vec![entry(LayerDesc::delta(lo, hi, Lsn(0x10), Lsn(0x100)))],
        );
        assert_eq!(floorless(&store, &layout, &child(Lsn(0x100))).unwrap(), [0]);

        publish(
            &store,
            &layout,
            vec![entry(LayerDesc::image(lo, hi, Lsn(0x80)))],
        );
        assert!(
            floorless(&store, &layout, &child(Lsn(0x100)))
                .unwrap()
                .is_empty()
        );
    }

    /// A shard with deltas and no image is a fold waiting to happen, so
    /// a caller that folds is told yes where a caller that does not is
    /// told to fold first. A shard with nothing at all is no to both.
    #[test]
    fn a_shard_with_something_to_fold_is_branchable_to_a_caller_that_folds() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let layout = TenantLayout::new("pr-1");
        let (lo, hi) = whole();

        let why = why_unbranchable_after_fold(&store, &layout, &child(Lsn(0x100)))
            .unwrap()
            .unwrap();
        assert!(why.contains("nothing to fold one out of"), "{why}");

        publish(
            &store,
            &layout,
            vec![entry(LayerDesc::delta(lo, hi, Lsn(0x10), Lsn(0x100)))],
        );
        assert!(
            why_unbranchable_after_fold(&store, &layout, &child(Lsn(0x100)))
                .unwrap()
                .is_none()
        );
        assert!(
            why_layerless(&store, &layout, &child(Lsn(0x100)))
                .unwrap()
                .is_some(),
            "and a caller that does not fold is still told to"
        );
    }

    /// Asked of a source rather than a child, the cut is the newest
    /// checkpoint, because that is where a head branch of it lands.
    #[test]
    fn a_source_is_asked_about_its_newest_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let layout = TenantLayout::new("prod");
        let (lo, hi) = whole();
        publish(
            &store,
            &layout,
            vec![entry(LayerDesc::image(lo, hi, Lsn(0x200)))],
        );

        let mut m = Manifest::new("prod", 18);
        assert_eq!(
            why_layerless(&store, &layout, &m).unwrap().as_deref(),
            Some(NO_CHECKPOINT)
        );
        m.checkpoints.push(CheckpointRef {
            id: "c1".into(),
            lsn: Lsn(0x300),
            kind: CheckpointKind::Full,
            owner: None,
        });
        assert!(why_layerless(&store, &layout, &m).unwrap().is_none());
    }

    /// A catalog initdb wrote and nothing has touched since lives
    /// entirely under `pg/`, which a child does not inherit. Seeding
    /// copies it into the layers, pages and length both, where a
    /// branch can read it.
    #[test]
    fn the_pg_objects_become_layers_a_branch_can_read() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let layout = TenantLayout::new("prod");
        for (rel, n) in [(1260u32, 2u32), (1255, 1)] {
            store
                .put(
                    &layout.pg_size(1663, 5, rel, 0),
                    &zou_store::forksize::ForkSize::plain(n).encode(),
                )
                .unwrap();
            for blk in 0..n {
                store
                    .put(
                        &layout.pg_block(1663, 5, rel, 0, blk),
                        &vec![(rel + blk) as u8; 8192],
                    )
                    .unwrap();
            }
        }
        // Not one of ours: the fs capture writes other objects under
        // the same prefix and the walk has to step over them.
        store
            .put(&format!("{}nonsense", layout.pg_dir()), b"x")
            .unwrap();

        let mut m = Manifest::new("prod", 1);
        m.checkpoints.push(CheckpointRef {
            id: "c1".into(),
            lsn: Lsn(0x400),
            kind: CheckpointKind::Full,
            owner: None,
        });
        assert_eq!(
            seed_base(&store, "prod", &m).unwrap(),
            5,
            "3 pages, 2 sizes"
        );

        let (shard, _) = PageShardManifest::load(&store, &layout.shard_manifest(0))
            .unwrap()
            .unwrap();
        let map = shard.layer_map().unwrap();
        let svc = crate::getpage::PageService::new(&store, layout.shard_prefix(0), None, false);
        let mem = zou_store::memtable::Memtable::new();
        let ask = |rel| {
            let fork = relsize::ForkRef {
                spc: 1663,
                db: 5,
                rel,
                fork: 0,
            };
            svc.rel_size(&map, &mem, fork, u64::MAX).unwrap()
        };
        assert_eq!(ask(1260), Some(2));
        assert_eq!(ask(1255), Some(1));
        assert_eq!(ask(2600), None, "a rel with no marker stays silent");

        let page = svc
            .get_pages(
                &map,
                &mem,
                &[crate::walscan::BlockRef {
                    spc: 1663,
                    db: 5,
                    rel: 1260,
                    fork: 0,
                    blk: 1,
                }],
                u64::MAX,
            )
            .unwrap();
        assert_eq!(
            page[0],
            vec![1261u32 as u8; 8192],
            "the page came along too"
        );

        // And it is a floor: a branch cut here has something to stand
        // on, which is the whole reason the seed happens before one.
        assert_eq!(why_layerless(&store, &layout, &m).unwrap(), None);
    }
}
