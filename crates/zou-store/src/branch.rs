//! Branch creation and point in time materialization.
//!
//! A branch is a new tenant prefix whose manifests point into its
//! parent's immutable objects: checkpoint refs and page shard layer
//! entries are copied and tagged with their owning tenant, and the
//! shard entries additionally carry the branch cut so an inherited
//! delta never serves records past the branch point. No data moves,
//! so a branch costs one manifest GET, one GET and PUT per page shard
//! the parent has, and one conditional PUT, whatever the database
//! size.
//!
//! Branch points are checkpoint lsns. The tenant's WAL lives in the
//! shared log, which consolidation rewrites and gc trims on its own
//! schedule, so a child cannot pin unfolded WAL the way it pins a
//! capture. An at_lsn that names a checkpoint pins the chain there,
//! the fold that made it already covers everything before it, and the
//! default branch point is the newest checkpoint. Anything finer
//! means folding first; PITR granularity is the fold cadence.
//!
//! Materializing at a timestamp picks the newest history snapshot at
//! or before it, written by [`crate::lease::update_manifest`] on every
//! state changing publish, and branches from that snapshot the same
//! way. History only survives the gc retention window, so PITR reaches
//! back exactly that far.

use std::collections::BTreeSet;

use crate::cas::{CasError, CasStore};
use crate::layermap::LayerDesc;
use crate::layout::TenantLayout;
use crate::lsn::Lsn;
use crate::manifest::{BranchOf, Manifest, ManifestError};
use crate::shardmanifest::{PageShardError, PageShardManifest};

#[derive(Debug, thiserror::Error)]
pub enum BranchError {
    #[error(
        "no manifest at {key}, the source database does not exist, `zou tenant <target> list` shows what does"
    )]
    NoSource { key: String },
    #[error(
        "destination {tenant_ref} already exists, pick another name, or `zou branch <target> delete {tenant_ref}` first if it is a branch you meant to replace"
    )]
    DestinationExists { tenant_ref: String },
    #[error("source has no checkpoint yet, run zou-bootstrap first")]
    NoCheckpoint,
    #[error(
        "lsn {at_lsn} is not a checkpoint lsn, branch points are checkpoint lsns in this release, fold first"
    )]
    AtLsnUnavailable { at_lsn: Lsn },
    #[error("no history snapshot at or before unix {unix_ts}, the retention window has passed it")]
    NoHistory { unix_ts: u64 },
    #[error(
        "lsn {at_lsn} is below the page retention horizon {horizon} on shard {shard}, the layers that answered reads there were folded into one image at the horizon and collected"
    )]
    BelowHorizon {
        at_lsn: Lsn,
        horizon: Lsn,
        shard: u16,
    },
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error("shard manifest: {0}")]
    Shard(#[from] PageShardError),
    #[error(transparent)]
    Store(#[from] CasError),
}

/// Create `dst_ref` as a branch of `src_ref` at `at_lsn`, or at the
/// last published state when `at_lsn` is `None`. Returns the child
/// manifest as written.
pub fn branch(
    store: &dyn CasStore,
    src_ref: &str,
    dst_ref: &str,
    at_lsn: Option<Lsn>,
    now_unix: u64,
) -> Result<Manifest, BranchError> {
    let src = TenantLayout::new(src_ref);
    let key = src.manifest();
    let Some((data, _)) = store.get(&key)? else {
        return Err(BranchError::NoSource { key });
    };
    let parent = Manifest::from_json(&data)?;
    let child = child_of(&parent, src_ref, dst_ref, at_lsn, now_unix)?;
    branch_shards(store, src_ref, dst_ref, branch_point(&child), parent.shards)?;
    publish_child(store, dst_ref, &child)?;
    Ok(child)
}

/// Create `dst_ref` from the newest history snapshot of `src_ref` at
/// or before `unix_ts`. The snapshot is branched at its newest
/// checkpoint, so the child sees the last fold the source had
/// published at that moment.
pub fn materialize_at(
    store: &dyn CasStore,
    src_ref: &str,
    dst_ref: &str,
    unix_ts: u64,
    now_unix: u64,
) -> Result<Manifest, BranchError> {
    let snapshot = snapshot_at(store, src_ref, unix_ts)?;
    let child = child_of(&snapshot, src_ref, dst_ref, None, now_unix)?;
    branch_shards(
        store,
        src_ref,
        dst_ref,
        branch_point(&child),
        snapshot.shards,
    )?;
    publish_child(store, dst_ref, &child)?;
    Ok(child)
}

/// The tenants a read falls back through, `tenant_ref` first and then
/// its parent, its parent's parent, and so on to the root.
///
/// Pages and layers do not need this: a branch copies the parent's
/// layer entries into the child's own shard manifest and tags each one
/// with the tenant that owns its bytes, so a reader already holds the
/// whole chain flattened in front of it. Storage API files have no such
/// list, since nothing writes an entry per file anywhere, so the chain
/// has to be walked and the walk is what this is.
///
/// A tenant with no manifest is a chain of one. That is not an error:
/// a server pointed at a directory that only ever held object bytes is
/// a legitimate way to run the storage surface on its own, and it is
/// what every conformance run does.
///
/// The walk stops on a ref it has already seen. A cycle cannot be
/// built through [`branch`], which refuses an existing destination, but
/// this is a read of somebody else's mutable objects and a loop here
/// would be an unkillable process rather than a wrong answer.
pub fn lineage(store: &dyn CasStore, tenant_ref: &str) -> Result<Vec<String>, BranchError> {
    let mut chain = vec![tenant_ref.to_string()];
    let mut at = tenant_ref.to_string();
    loop {
        let key = TenantLayout::new(&at).manifest();
        let Some((data, _)) = store.get(&key)? else {
            return Ok(chain);
        };
        let Some(parent) = Manifest::from_json(&data)?.branch_of else {
            return Ok(chain);
        };
        if chain.contains(&parent.tenant_ref) {
            return Ok(chain);
        }
        chain.push(parent.tenant_ref.clone());
        at = parent.tenant_ref;
    }
}

/// The lsn a freshly built child branches at.
fn branch_point(child: &Manifest) -> Lsn {
    child
        .branch_of
        .as_ref()
        .expect("child_of always sets branch_of")
        .at_lsn
}

/// The newest history snapshot of `src_ref` at or before `unix_ts`,
/// exactly what the source had published at that moment. Time travel
/// restore reads one of these directly, nothing in the store changes.
pub fn snapshot_at(
    store: &dyn CasStore,
    src_ref: &str,
    unix_ts: u64,
) -> Result<Manifest, BranchError> {
    let src = TenantLayout::new(src_ref);
    let dir = src.manifests_dir();
    let mut best: Option<(u64, String)> = None;
    for key in store.list(&dir)? {
        let Some(stamp) = history_unix(&dir, &key) else {
            continue;
        };
        if stamp <= unix_ts && best.as_ref().is_none_or(|(b, _)| stamp >= *b) {
            best = Some((stamp, key));
        }
    }
    let Some((_, key)) = best else {
        return Err(BranchError::NoHistory { unix_ts });
    };
    let (data, _) = store.get(&key)?.ok_or(BranchError::NoHistory { unix_ts })?;
    Ok(Manifest::from_json(&data)?)
}

/// The unix stamp inside a history key like
/// `<prefix>/manifests/<epoch>-<unix>.json`.
fn history_unix(dir: &str, key: &str) -> Option<u64> {
    let name = key.strip_prefix(dir)?.strip_suffix(".json")?;
    let (_, unix) = name.split_once('-')?;
    unix.parse().ok()
}

/// Build the child manifest from a parent state, which is the current
/// manifest for branch() and a history snapshot for materialize_at().
fn child_of(
    parent: &Manifest,
    src_ref: &str,
    dst_ref: &str,
    at_lsn: Option<Lsn>,
    now_unix: u64,
) -> Result<Manifest, BranchError> {
    let Some(newest) = parent.checkpoints.last().map(|c| c.lsn) else {
        return Err(BranchError::NoCheckpoint);
    };
    // An at_lsn must name a checkpoint exactly, which pins the chain
    // there; the fold that made it already covers everything before it.
    let (upto, at) = match at_lsn {
        None => (parent.checkpoints.len(), newest),
        Some(at) => match parent.checkpoints.iter().rposition(|c| c.lsn == at) {
            Some(i) => (i + 1, at),
            None => return Err(BranchError::AtLsnUnavailable { at_lsn: at }),
        },
    };

    let mut child = Manifest::new(dst_ref, parent.pg.version);
    child.pg.timeline = parent.pg.timeline;
    // The shard function is part of what the child inherits: its keys
    // land where the parent's did, and the lineage is what lets its
    // shards find the eras the inherited layers were flushed under.
    // Sharded parents hand down their format 3 with it.
    child.shards = parent.shards;
    child.shard_history = parent.shard_history.clone();
    child.format = child.format.max(parent.format);
    child.checkpoints = parent.checkpoints[..upto]
        .iter()
        .cloned()
        .map(|mut c| {
            if c.owner.is_none() {
                c.owner = Some(src_ref.to_string());
            }
            c
        })
        .collect();
    child.branch_of = Some(BranchOf {
        tenant_ref: src_ref.to_string(),
        at_lsn: at,
    });
    child.published_unix = Some(now_unix);
    Ok(child)
}

/// Copy the parent's page shard manifests under the child, cut at the
/// branch point. The child must capture the parent's layer list now:
/// the parent keeps compacting, its manifest stops naming layers the
/// child still needs, and gc pins layers by walking manifests. Entries
/// keep their owner when they already carry one, a grandchild still
/// names the tenant whose prefix holds the bytes, and the branch cut
/// only ever tightens.
///
/// Each child SHARD is a plain put, not a CAS: nothing serves the
/// child until its tenant manifest lands, so a branch that crashed
/// here retries by overwriting its own leftovers. The tenant manifest
/// CAS in [`publish_child`] stays the one commit point.
fn branch_shards(
    store: &dyn CasStore,
    src_ref: &str,
    dst_ref: &str,
    at: Lsn,
    shards: u32,
) -> Result<(), BranchError> {
    let src = TenantLayout::new(src_ref);
    let dst = TenantLayout::new(dst_ref);
    // The listing finds the ancestor shards of past eras, which a
    // shrunk tenant still reads through and which nothing but the
    // prefix knows about. The live shards come from the count in the
    // manifest instead, because no object store promises a listing has
    // caught up with a write that landed a moment ago, and a shard the
    // listing missed would be a child published complete with one of
    // its layer lists silently absent. A key that is genuinely not
    // there loads as None either way.
    let mut keys: BTreeSet<String> = store
        .list(&src.shards_dir())?
        .into_iter()
        .filter(|k| k.ends_with("/SHARD"))
        .collect();
    for shard in 0..shards as u16 {
        keys.insert(src.shard_manifest(shard));
    }
    let keys: Vec<String> = keys.into_iter().collect();
    // Every shard is asked before anything is written. A branch below
    // the horizon cannot be served, and finding that out on the fourth
    // shard after three child manifests have landed would leave a half
    // built tenant behind for a question that could be answered first.
    for key in &keys {
        let Some((parent, _)) = PageShardManifest::load(store, key)? else {
            continue;
        };
        if let Some(horizon) = parent.horizon
            && horizon > at
        {
            return Err(BranchError::BelowHorizon {
                at_lsn: at,
                horizon,
                shard: parent.shard,
            });
        }
    }
    for key in &keys {
        let Some((parent, _)) = PageShardManifest::load(store, key)? else {
            continue;
        };
        let mut child = PageShardManifest::new(parent.shard);
        // Inheritance needs format 2: a binary that predates it must
        // refuse the whole shard instead of quietly fetching inherited
        // layers from the wrong prefix.
        child.format = 2;
        child.disk_consistent_lsn = parent.disk_consistent_lsn.min(at);
        // The inherited layers are the parent's, and the ones below its
        // horizon are gone from under both of them, so the child starts
        // life with the same floor.
        child.horizon = parent.horizon;
        for l in &parent.layers {
            let desc = LayerDesc::parse(&l.name, l.size).map_err(PageShardError::from)?;
            // A layer that starts past the branch point is entirely
            // the parent's future; images taken after the point drop
            // out here too.
            if desc.min_lsn > at {
                continue;
            }
            let mut e = l.clone();
            if e.owner.is_none() {
                e.owner = Some(src_ref.to_string());
            }
            if desc.max_lsn > at || e.upto.is_some() {
                e.upto = Some(e.upto.map_or(at, |u| u.min(at)));
            }
            child.layers.push(e);
        }
        store.put(&dst.shard_manifest(parent.shard), &child.encode())?;
    }
    Ok(())
}

/// Land the child manifest, refusing to clobber an existing tenant.
fn publish_child(store: &dyn CasStore, dst_ref: &str, child: &Manifest) -> Result<(), BranchError> {
    let dst = TenantLayout::new(dst_ref);
    match store.put_if_match(&dst.manifest(), &child.to_json(), None) {
        Ok(_) => Ok(()),
        Err(CasError::Conflict { .. }) | Err(CasError::AlreadyExists { .. }) => {
            Err(BranchError::DestinationExists {
                tenant_ref: dst_ref.to_string(),
            })
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::LocalFsStore;
    use crate::layer::LayerKey;
    use crate::manifest::{CheckpointKind, CheckpointRef};
    use crate::shardmanifest::LayerEntry;

    fn chk(id: &str, lsn: u64, kind: CheckpointKind) -> CheckpointRef {
        CheckpointRef {
            id: id.into(),
            lsn: Lsn(lsn),
            kind,
            owner: None,
        }
    }

    fn parent_manifest() -> Manifest {
        let mut m = Manifest::new("p", 18);
        m.epoch = 3;
        m.checkpoints = vec![
            chk("f1", 0x100, CheckpointKind::Full),
            chk("d2", 0x200, CheckpointKind::Delta),
        ];
        m.folded_upto = Some(Lsn(0x200));
        m
    }

    fn setup() -> (tempfile::TempDir, LocalFsStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        store
            .put_if_absent(
                &TenantLayout::new("p").manifest(),
                &parent_manifest().to_json(),
            )
            .unwrap();
        (dir, store)
    }

    fn k(block: u32) -> LayerKey {
        LayerKey::page(1663, 5, 16384, 0, block)
    }

    fn delta_entry(min: u64, max: u64) -> LayerEntry {
        LayerEntry {
            name: LayerDesc::delta(k(0), k(9), Lsn(min), Lsn(max)).name(),
            size: 1000,
            owner: None,
            upto: None,
        }
    }

    fn image_entry(lsn: u64) -> LayerEntry {
        LayerEntry {
            name: LayerDesc::image(k(0), k(9), Lsn(lsn)).name(),
            size: 1000,
            owner: None,
            upto: None,
        }
    }

    /// The parent's shard 0: an image, a delta before the branch
    /// point, a delta spanning it, and an image past it.
    fn setup_shard(store: &LocalFsStore) {
        let mut m = PageShardManifest::new(0);
        m.disk_consistent_lsn = Lsn(0x300);
        m.layers = vec![
            image_entry(0x100),
            delta_entry(0x101, 0x180),
            delta_entry(0x0F0, 0x300),
            image_entry(0x280),
        ];
        let p = TenantLayout::new("p");
        store.put(&p.shard_manifest(0), &m.encode()).unwrap();
        // A layer object in the prefix must not be mistaken for a
        // shard manifest.
        store
            .put(&format!("{}{}", p.shard_prefix(0), m.layers[0].name), b"x")
            .unwrap();
    }

    fn child_shard(store: &LocalFsStore, tenant_ref: &str) -> PageShardManifest {
        let key = TenantLayout::new(tenant_ref).shard_manifest(0);
        PageShardManifest::load(store, &key).unwrap().unwrap().0
    }

    #[test]
    fn a_branch_inherits_the_shard_function() {
        let (_d, store) = setup();
        setup_shard(&store);
        crate::shards::split(&store, "p").unwrap();
        let child = branch(&store, "p", "c", Some(Lsn(0x200)), 5000).unwrap();
        assert_eq!(
            child.shards, 2,
            "the child's keys land where the parent's did"
        );
        assert_eq!(child.shard_history.len(), 1);
        assert_eq!(child.format, 3, "sharding rides the format gate down");
    }

    #[test]
    fn a_branch_cuts_and_tags_the_shard_manifests() {
        let (_d, store) = setup();
        setup_shard(&store);
        branch(&store, "p", "c", Some(Lsn(0x200)), 5000).unwrap();

        let m = child_shard(&store, "c");
        assert_eq!(m.format, 2, "inheritance needs the format gate");
        assert_eq!(
            m.disk_consistent_lsn,
            Lsn(0x200),
            "the flush watermark cannot claim past the branch point"
        );
        assert_eq!(
            m.layers.len(),
            3,
            "the image past the branch point dropped out"
        );
        assert!(
            m.layers.iter().all(|l| l.owner.as_deref() == Some("p")),
            "every inherited entry names its owner"
        );
        assert_eq!(m.layers[0].upto, None, "an image needs no cut");
        assert_eq!(
            m.layers[1].upto, None,
            "a delta entirely before the point needs no cut"
        );
        assert_eq!(
            m.layers[2].upto,
            Some(Lsn(0x200)),
            "the spanning delta is cut at the branch point"
        );
        // And the map the child attaches with carries all of it.
        let map = m.layer_map().unwrap();
        assert_eq!(map.layers()[2].upto, Some(Lsn(0x200)));
    }

    #[test]
    fn a_branch_below_the_page_horizon_is_refused_and_leaves_nothing_behind() {
        let (_d, store) = setup();
        setup_shard(&store);
        let key = TenantLayout::new("p").shard_manifest(0);
        let (mut m, _) = PageShardManifest::load(&store, &key).unwrap().unwrap();
        m.horizon = Some(Lsn(0x150));
        store.put(&key, &m.encode()).unwrap();

        let err = branch(&store, "p", "c", Some(Lsn(0x100)), 5000).unwrap_err();
        assert!(
            matches!(
                err,
                BranchError::BelowHorizon { at_lsn, horizon, shard }
                    if at_lsn == Lsn(0x100) && horizon == Lsn(0x150) && shard == 0
            ),
            "{err}"
        );
        assert!(
            PageShardManifest::load(&store, &TenantLayout::new("c").shard_manifest(0))
                .unwrap()
                .is_none(),
            "the refusal comes before anything of the child is written"
        );

        // At or above the horizon the branch goes through and the child
        // inherits the same floor, because the layers below it are gone
        // from under both of them.
        branch(&store, "p", "c", Some(Lsn(0x200)), 5000).unwrap();
        assert_eq!(child_shard(&store, "c").horizon, Some(Lsn(0x150)));
    }

    #[test]
    fn branching_a_branch_tightens_the_cut_and_keeps_the_owner() {
        let (_d, store) = setup();
        setup_shard(&store);
        branch(&store, "p", "c", None, 5000).unwrap();
        branch(&store, "c", "g", Some(Lsn(0x100)), 6000).unwrap();

        let m = child_shard(&store, "g");
        assert_eq!(
            m.layers.len(),
            2,
            "deltas starting past the earlier point dropped out"
        );
        assert!(
            m.layers.iter().all(|l| l.owner.as_deref() == Some("p")),
            "the grandchild still names the tenant whose prefix holds the bytes"
        );
        assert_eq!(m.layers[0].upto, None);
        assert_eq!(
            m.layers[1].upto,
            Some(Lsn(0x100)),
            "the cut tightened to the earlier branch point"
        );
        assert_eq!(m.disk_consistent_lsn, Lsn(0x100));
    }

    #[test]
    fn a_crashed_branch_retry_overwrites_its_leftovers() {
        let (_d, store) = setup();
        setup_shard(&store);
        // A first attempt died after writing the shard manifest but
        // before the tenant manifest landed.
        branch_shards(&store, "p", "c", Lsn(0x100), 1).unwrap();
        assert!(
            store
                .get(&TenantLayout::new("c").manifest())
                .unwrap()
                .is_none(),
            "nothing committed yet"
        );
        // The retry picks a different branch point and must win whole.
        branch(&store, "p", "c", Some(Lsn(0x200)), 5000).unwrap();
        let m = child_shard(&store, "c");
        assert_eq!(m.disk_consistent_lsn, Lsn(0x200));
        assert_eq!(m.layers.len(), 3);
    }

    #[test]
    fn a_head_branch_inherits_the_chain_at_the_newest_checkpoint() {
        let (_d, store) = setup();
        let child = branch(&store, "p", "c", None, 5000).unwrap();

        assert_eq!(child.tenant_ref, "c");
        assert_eq!(child.epoch, 0, "the child's own epochs start fresh");
        assert!(child.lease.is_none());
        assert_eq!(child.checkpoints.len(), 2);
        assert!(
            child
                .checkpoints
                .iter()
                .all(|c| c.owner.as_deref() == Some("p")),
            "inherited refs are tagged with their owner"
        );
        assert!(
            child.folded_upto.is_none(),
            "the child's own fold cursor starts fresh"
        );
        let b = child.branch_of.unwrap();
        assert_eq!((b.tenant_ref.as_str(), b.at_lsn), ("p", Lsn(0x200)));
        assert_eq!(child.published_unix, Some(5000));

        // And it landed in the store as the child's MANIFEST.
        let (data, _) = store
            .get(&TenantLayout::new("c").manifest())
            .unwrap()
            .unwrap();
        let stored = Manifest::from_json(&data).unwrap();
        assert_eq!(stored.tenant_ref, "c");
    }

    #[test]
    fn an_at_lsn_naming_a_checkpoint_pins_the_chain_there() {
        let (_d, store) = setup();
        let child = branch(&store, "p", "c", Some(Lsn(0x100)), 5000).unwrap();
        assert_eq!(child.checkpoints.len(), 1);
        assert_eq!(child.checkpoints[0].id, "f1");
        assert_eq!(child.branch_of.unwrap().at_lsn, Lsn(0x100));
    }

    #[test]
    fn an_at_lsn_off_the_checkpoint_grid_is_refused() {
        let (_d, store) = setup();
        // Between two checkpoints, and past the newest one: neither
        // names a fold, so neither is a branch point.
        for lsn in [0x180u64, 0x300] {
            let err = branch(&store, "p", "c", Some(Lsn(lsn)), 5000).unwrap_err();
            assert!(matches!(
                err,
                BranchError::AtLsnUnavailable { at_lsn } if at_lsn == Lsn(lsn)
            ));
        }
    }

    #[test]
    fn branching_a_branch_chains_owners() {
        let (_d, store) = setup();
        branch(&store, "p", "c", None, 5000).unwrap();
        let g = branch(&store, "c", "g", None, 6000).unwrap();
        assert!(
            g.checkpoints
                .iter()
                .all(|c| c.owner.as_deref() == Some("p")),
            "owner tags survive a second hop untouched"
        );
        assert_eq!(g.branch_of.unwrap().tenant_ref, "c");
    }

    #[test]
    fn the_lineage_is_the_chain_of_parents_child_first() {
        let (_d, store) = setup();
        branch(&store, "p", "c", None, 5000).unwrap();
        branch(&store, "c", "g", None, 6000).unwrap();
        assert_eq!(lineage(&store, "g").unwrap(), ["g", "c", "p"]);
        assert_eq!(lineage(&store, "c").unwrap(), ["c", "p"]);
        assert_eq!(lineage(&store, "p").unwrap(), ["p"]);
    }

    #[test]
    fn a_tenant_with_no_manifest_is_a_chain_of_one() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        // A store that only ever held object bytes is a legitimate way
        // to run the storage surface on its own, and it is what every
        // conformance run does.
        assert_eq!(lineage(&store, "local").unwrap(), ["local"]);
    }

    #[test]
    fn a_cycle_in_the_manifests_stops_rather_than_spins() {
        let (_d, store) = setup();
        branch(&store, "p", "c", None, 5000).unwrap();
        // branch() refuses an existing destination, so this cannot be
        // built through the api. It is a read of somebody else's
        // mutable objects, and a loop here would be a process that
        // never comes back rather than a wrong answer.
        let key = TenantLayout::new("p").manifest();
        let (data, version) = store.get(&key).unwrap().unwrap();
        let mut parent = Manifest::from_json(&data).unwrap();
        parent.branch_of = Some(BranchOf {
            tenant_ref: "c".to_string(),
            at_lsn: Lsn(0x100),
        });
        store
            .put_if_match(&key, &parent.to_json(), Some(&version))
            .unwrap();
        assert_eq!(lineage(&store, "c").unwrap(), ["c", "p"]);
    }

    #[test]
    fn an_existing_destination_is_refused() {
        let (_d, store) = setup();
        branch(&store, "p", "c", None, 5000).unwrap();
        let err = branch(&store, "p", "c", None, 5001).unwrap_err();
        assert!(matches!(err, BranchError::DestinationExists { .. }));
    }

    #[test]
    fn a_missing_source_and_an_empty_source_error_clearly() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        assert!(matches!(
            branch(&store, "ghost", "c", None, 5000).unwrap_err(),
            BranchError::NoSource { .. }
        ));
        store
            .put_if_absent(
                &TenantLayout::new("empty").manifest(),
                &Manifest::new("empty", 18).to_json(),
            )
            .unwrap();
        assert!(matches!(
            branch(&store, "empty", "c", None, 5000).unwrap_err(),
            BranchError::NoCheckpoint
        ));
    }

    #[test]
    fn materialize_picks_the_newest_snapshot_at_or_before_the_timestamp() {
        let (_d, store) = setup();
        let p = TenantLayout::new("p");
        // Three snapshots: the middle one is the state to hit, the last
        // one is past the asked timestamp and must not win.
        let mut early = parent_manifest();
        early.checkpoints.truncate(1);
        early.published_unix = Some(1000);
        store
            .put_if_absent(&p.manifest_history(3, 1000), &early.to_json())
            .unwrap();
        let mut middle = parent_manifest();
        middle.published_unix = Some(2000);
        store
            .put_if_absent(&p.manifest_history(3, 2000), &middle.to_json())
            .unwrap();
        let mut late = parent_manifest();
        late.checkpoints
            .push(chk("d3", 0x300, CheckpointKind::Delta));
        late.published_unix = Some(3000);
        store
            .put_if_absent(&p.manifest_history(3, 3000), &late.to_json())
            .unwrap();

        let child = materialize_at(&store, "p", "c", 2500, 5000).unwrap();
        assert_eq!(
            child.checkpoints.len(),
            2,
            "the child sees the chain as of the snapshot, not the head"
        );

        let err = materialize_at(&store, "p", "c2", 999, 5000).unwrap_err();
        assert!(matches!(err, BranchError::NoHistory { unix_ts: 999 }));
    }
}
