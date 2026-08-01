//! Branch creation and point in time materialization.
//!
//! A branch is a new tenant prefix whose manifest points into its
//! parent's immutable objects: checkpoint refs are copied and tagged
//! with their owning tenant, and the parent's published WAL tail is
//! carried along as a parent_tail entry so replay reaches the branch
//! point. Nothing is copied, so a branch is one manifest GET and one
//! conditional PUT whatever the database size.
//!
//! The branch point is the parent's last published state, not its in
//! flight head: an unpublished segment can still be racing an upload
//! and referencing it would tie the child to a write that may lose its
//! lease. An explicit at_lsn either names a checkpoint exactly, which
//! yields the state at that fold with no tail at all, or lands at or
//! past the newest checkpoint, which keeps the tail segments starting
//! below it. Segments are sealed as a whole, so an at_lsn inside a
//! segment rounds up to that segment's end on replay; PITR here is
//! segment grained, finer grains would need record level truncation.
//!
//! Materializing at a timestamp picks the newest history snapshot at
//! or before it, written by [`crate::lease::update_manifest`] on every
//! state changing publish, and branches from that snapshot the same
//! way. History only survives the gc retention window, so PITR reaches
//! back exactly that far.

use crate::cas::{CasError, CasStore};
use crate::layout::TenantLayout;
use crate::lsn::Lsn;
use crate::manifest::{BranchOf, Manifest, ManifestError, ParentTail};

#[derive(Debug, thiserror::Error)]
pub enum BranchError {
    #[error("no manifest at {key}, the source database does not exist")]
    NoSource { key: String },
    #[error("destination {tenant_ref} already exists")]
    DestinationExists { tenant_ref: String },
    #[error("source has no checkpoint yet, run zou-bootstrap first")]
    NoCheckpoint,
    #[error(
        "lsn {at_lsn} is not a checkpoint and lies below the newest checkpoint {newest}, that state was folded away"
    )]
    AtLsnUnavailable { at_lsn: Lsn, newest: Lsn },
    #[error("no history snapshot at or before unix {unix_ts}, the retention window has passed it")]
    NoHistory { unix_ts: u64 },
    #[error(transparent)]
    Manifest(#[from] ManifestError),
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
    publish_child(store, dst_ref, &child)?;
    Ok(child)
}

/// Create `dst_ref` from the newest history snapshot of `src_ref` at
/// or before `unix_ts`. The snapshot is branched at its head, its own
/// wal_tail becoming a parent_tail entry, so the child sees exactly
/// what the source had published at that moment.
pub fn materialize_at(
    store: &dyn CasStore,
    src_ref: &str,
    dst_ref: &str,
    unix_ts: u64,
    now_unix: u64,
) -> Result<Manifest, BranchError> {
    let snapshot = snapshot_at(store, src_ref, unix_ts)?;
    let child = child_of(&snapshot, src_ref, dst_ref, None, now_unix)?;
    publish_child(store, dst_ref, &child)?;
    Ok(child)
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
    // The checkpoint subset and whether the tail rides along. An at_lsn
    // naming a checkpoint pins the chain there and needs no WAL, the
    // fold that made it already covers everything before it.
    let (upto, with_tail, at) = match at_lsn {
        None => (parent.checkpoints.len(), true, newest),
        Some(at) if at >= newest => (parent.checkpoints.len(), true, at),
        Some(at) => match parent.checkpoints.iter().rposition(|c| c.lsn == at) {
            Some(i) => (i + 1, false, at),
            None => return Err(BranchError::AtLsnUnavailable { at_lsn: at, newest }),
        },
    };

    let mut child = Manifest::new(dst_ref, parent.pg.version);
    child.pg.timeline = parent.pg.timeline;
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
    if with_tail {
        // Ancestor tails first, they replay before the parent's own.
        child.parent_tail = parent.parent_tail.clone();
        if let Some(tail) = &parent.wal_tail {
            let segments: Vec<String> = match at_lsn {
                None => tail.segments.clone(),
                Some(at) => tail
                    .segments
                    .iter()
                    .filter(|s| crate::commit::segment_start_lsn(s).is_none_or(|start| start < at))
                    .cloned()
                    .collect(),
            };
            if !segments.is_empty() {
                child.parent_tail.push(ParentTail {
                    tenant_ref: src_ref.to_string(),
                    from_lsn: tail.from_lsn,
                    segments,
                });
            }
        }
    }
    child.branch_of = Some(BranchOf {
        tenant_ref: src_ref.to_string(),
        at_lsn: at,
    });
    child.published_unix = Some(now_unix);
    Ok(child)
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
    use crate::manifest::{CheckpointKind, CheckpointRef, WalTail};

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
        m.wal_tail = Some(WalTail {
            epoch_dir: 3,
            from_lsn: Lsn(0x200),
            segments: vec![
                "0000000000000003/0000000000000200.wal".into(),
                "0000000000000003/0000000000000400.wal".into(),
            ],
        });
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

    #[test]
    fn a_head_branch_inherits_the_chain_and_the_whole_tail() {
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
        assert_eq!(child.parent_tail.len(), 1);
        assert_eq!(child.parent_tail[0].tenant_ref, "p");
        assert_eq!(child.parent_tail[0].segments.len(), 2);
        assert!(child.wal_tail.is_none(), "the child has written nothing");
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
    fn an_at_lsn_naming_a_checkpoint_pins_the_chain_and_drops_the_tail() {
        let (_d, store) = setup();
        let child = branch(&store, "p", "c", Some(Lsn(0x100)), 5000).unwrap();
        assert_eq!(child.checkpoints.len(), 1);
        assert_eq!(child.checkpoints[0].id, "f1");
        assert!(
            child.parent_tail.is_empty(),
            "the fold at 0x100 already covers everything before it"
        );
        assert_eq!(child.branch_of.unwrap().at_lsn, Lsn(0x100));
    }

    #[test]
    fn an_at_lsn_past_the_newest_checkpoint_keeps_segments_below_it() {
        let (_d, store) = setup();
        let child = branch(&store, "p", "c", Some(Lsn(0x300)), 5000).unwrap();
        assert_eq!(child.checkpoints.len(), 2);
        assert_eq!(
            child.parent_tail[0].segments,
            vec!["0000000000000003/0000000000000200.wal".to_string()],
            "the segment starting at 0x400 lies wholly past the branch point"
        );
    }

    #[test]
    fn an_at_lsn_inside_a_folded_window_is_refused() {
        let (_d, store) = setup();
        let err = branch(&store, "p", "c", Some(Lsn(0x180)), 5000).unwrap_err();
        assert!(matches!(
            err,
            BranchError::AtLsnUnavailable {
                at_lsn: Lsn(0x180),
                newest: Lsn(0x200)
            }
        ));
    }

    #[test]
    fn branching_a_branch_chains_owners_and_tails() {
        let (_d, store) = setup();
        branch(&store, "p", "c", None, 5000).unwrap();

        // The middle branch grows its own tail before being branched.
        let c_layout = TenantLayout::new("c");
        let (data, v) = store.get(&c_layout.manifest()).unwrap().unwrap();
        let mut c = Manifest::from_json(&data).unwrap();
        c.epoch = 1;
        c.wal_tail = Some(WalTail {
            epoch_dir: 1,
            from_lsn: Lsn(0x500),
            segments: vec!["0000000000000001/0000000000000500.wal".into()],
        });
        store
            .put_if_match(&c_layout.manifest(), &c.to_json(), Some(&v))
            .unwrap();

        let g = branch(&store, "c", "g", None, 6000).unwrap();
        assert!(
            g.checkpoints
                .iter()
                .all(|c| c.owner.as_deref() == Some("p")),
            "owner tags survive a second hop untouched"
        );
        assert_eq!(g.parent_tail.len(), 2);
        assert_eq!(g.parent_tail[0].tenant_ref, "p");
        assert_eq!(g.parent_tail[1].tenant_ref, "c");
        assert_eq!(g.branch_of.unwrap().tenant_ref, "c");
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
        early.wal_tail = None;
        early.published_unix = Some(1000);
        store
            .put_if_absent(&p.manifest_history(3, 1000), &early.to_json())
            .unwrap();
        let mut middle = parent_manifest();
        middle.wal_tail.as_mut().unwrap().segments.truncate(1);
        middle.published_unix = Some(2000);
        store
            .put_if_absent(&p.manifest_history(3, 2000), &middle.to_json())
            .unwrap();
        let mut late = parent_manifest();
        late.published_unix = Some(3000);
        store
            .put_if_absent(&p.manifest_history(3, 3000), &late.to_json())
            .unwrap();

        let child = materialize_at(&store, "p", "c", 2500, 5000).unwrap();
        assert_eq!(child.checkpoints.len(), 2);
        assert_eq!(
            child.parent_tail[0].segments,
            vec!["0000000000000003/0000000000000200.wal".to_string()],
            "the child sees the tail as of the snapshot, not the head"
        );

        let err = materialize_at(&store, "p", "c2", 999, 5000).unwrap_err();
        assert!(matches!(err, BranchError::NoHistory { unix_ts: 999 }));
    }
}
