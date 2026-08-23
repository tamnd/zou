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

use crate::redo::RedoPool;
use zou_store::layer::LayerKind;
use zou_store::layermap::LayerDesc;
use zou_store::layout::TenantLayout;
use zou_store::lsn::Lsn;
use zou_store::shardmanifest::PageShardManifest;
use zou_store::{CasStore, Manifest};

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
/// it is the same question [`why_layerless`] asks and is answered off
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

/// The same question [`why_layerless`] answers, asked by a caller that
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
    store: &dyn CasStore,
    tenant_ref: &str,
    manifest: &Manifest,
    pool: &RedoPool,
    data_checksums: bool,
) -> Result<usize, String> {
    let layout = TenantLayout::new(tenant_ref);
    let at = cut_at(manifest).ok_or_else(|| NO_CHECKPOINT.to_string())?;
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
                    "fold for branch: shard {shard} imaged {} pages at {}",
                    out.imaged,
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
}
