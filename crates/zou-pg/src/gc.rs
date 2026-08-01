//! Garbage collection: delete objects no retained manifest references.
//!
//! The store only ever grows on its own: a fold down supersedes a whole
//! checkpoint chain, a failed fold leaves captures behind with no
//! manifest naming them, and segments dropped from the WAL tail stay
//! as objects. The gc job walks every tenant under the store root,
//! pins everything any current manifest references, and deletes the
//! rest through a two phase candidate window.
//!
//! Pinning follows branches: a manifest with `branch_of` references
//! objects that live under its parent's prefix, so its checkpoint ids
//! and tail segments pin keys under both prefixes, and a tenant whose
//! manifest is missing or unreadable contributes nothing and loses
//! nothing. A checkpoint ref carrying an owner tag is sharper, it pins
//! under exactly that owner, which is what keeps a grandparent's
//! capture alive when `branch_of` only names the direct parent. The
//! frozen parent tail segment lists pin by name under the tenant each
//! entry names.
//!
//! PITR retention rides the same pins. Every state change leaves a
//! snapshot under `manifests/`, and a snapshot younger than the
//! retention window pins its references exactly like a live manifest,
//! except it never moves the WAL cut, its own tail segments are pinned
//! by name and need no successor rule. A snapshot past retention is
//! ordinary two phase garbage, and whatever only it referenced follows
//! it out through the same window.
//!
//! WAL is never judged by reference alone. Recovery reconciles the
//! tail from a LIST of the whole wal prefix, so a segment absent from
//! every wal_tail can still carry acked frames from a session that
//! crashed before its first publish. A segment dies only by the fold's
//! own rule: its successor within the epoch starts at or below the
//! cut, where the cut is the minimum newest checkpoint redo over every
//! manifest that can reach this tenant's WAL, rounded down to a
//! Postgres segment. A branch pinned at an old LSN drags that cut down
//! and keeps the WAL it replays from alive, and the last segment of an
//! epoch has no successor to bound it and is never collected.
//!
//! Deletion is two phase. A run stamps each garbage key into the
//! candidates object with the current time, and a later run deletes a
//! key only when its stamp is older than the safety window and the key
//! is still garbage in that run's own scan. A branch created between
//! the two runs republishes a reference, so the deleting run drops the
//! candidate instead of the object. The window must exceed the longest
//! fold upload and the longest gap between reading a manifest and
//! publishing a branch from it, and one gc job runs at a time, the
//! candidates object is swapped without a guard.

use std::collections::{BTreeMap, BTreeSet};

use zou_store::CasStore;
use zou_store::layout::TenantLayout;
use zou_store::manifest::Manifest;

use crate::fold::segment_first_pg_lsn;
use crate::restore::WAL_SEGMENT_SIZE;

/// Where the two phase state lives, next to `tenants/` under the store
/// root. Lines of `<first-seen-unix> <key>`.
pub const CANDIDATES_KEY: &str = "gc/CANDIDATES";

#[derive(Debug, Default, PartialEq, Eq)]
pub struct GcStats {
    /// Tenants with a readable manifest.
    pub tenants: usize,
    /// Keys stamped and waiting out the safety window after this run.
    pub candidates: usize,
    /// Objects deleted by this run.
    pub deleted: usize,
}

/// The stamp encoded in a history key, `<epoch>-<unix>.json`.
fn history_stamp(key: &str) -> Option<u64> {
    let name = key.rsplit('/').next()?;
    let (_, unix) = name.strip_suffix(".json")?.split_once('-')?;
    unix.parse().ok()
}

/// One gc pass over the whole store. `now_unix` is the caller's clock
/// and `window_secs` the safety window, so a run never deletes a key
/// it stamped itself and a window of zero still takes two runs.
/// `retention_secs` is how far back PITR reaches, history snapshots
/// younger than it pin their references.
pub fn run(
    store: &dyn CasStore,
    now_unix: u64,
    window_secs: u64,
    retention_secs: u64,
) -> Result<GcStats, String> {
    let keys = store.list("tenants/").map_err(|e| format!("store: {e}"))?;

    let mut refs = BTreeSet::new();
    for key in &keys {
        if let Some(rest) = key.strip_prefix("tenants/")
            && let Some((r, _)) = rest.split_once('/')
        {
            refs.insert(r.to_string());
        }
    }
    let mut manifests: BTreeMap<String, Manifest> = BTreeMap::new();
    for r in &refs {
        let layout = TenantLayout::new(r);
        if let Some((data, _)) = store
            .get(&layout.manifest())
            .map_err(|e| format!("store: {e}"))?
        {
            let m = Manifest::from_json(&data).map_err(|e| format!("manifest for {r}: {e}"))?;
            manifests.insert(r.clone(), m);
        }
    }

    // History snapshots: within retention they pin like live
    // manifests, past it they are garbage. Only tenants with a live
    // manifest are processed, a ghost prefix stays untouched.
    let mut history: Vec<(String, Manifest)> = Vec::new();
    let mut expired_history: Vec<String> = Vec::new();
    for r in manifests.keys() {
        let prefix = format!("tenants/{r}/manifests/");
        for key in &keys {
            if !key.starts_with(&prefix) {
                continue;
            }
            let Some(stamp) = history_stamp(key) else {
                continue;
            };
            if now_unix.saturating_sub(stamp) >= retention_secs {
                expired_history.push(key.clone());
            } else if let Some((data, _)) = store.get(key).map_err(|e| format!("store: {e}"))? {
                let m = Manifest::from_json(&data).map_err(|e| format!("history {key}: {e}"))?;
                history.push((r.clone(), m));
            }
        }
    }

    // The pins: checkpoint ids and tail segments under every prefix
    // the manifest can reference, and the WAL cut per tenant, the
    // minimum over every live manifest that reaches it. An owner
    // tagged checkpoint ref pins under exactly its owner, an untagged
    // one predates the tags and pins under everything reachable.
    let mut pinned_chk: BTreeSet<(String, String)> = BTreeSet::new();
    let mut pinned_wal: BTreeSet<String> = BTreeSet::new();
    let mut cuts: BTreeMap<String, u64> = BTreeMap::new();
    let mut pin = |r: &str, m: &Manifest, live: bool| {
        let mut owners = vec![r.to_string()];
        if let Some(b) = &m.branch_of {
            owners.push(b.tenant_ref.clone());
        }
        let cut = m
            .checkpoints
            .last()
            .map_or(0, |c| c.lsn.0 & !(WAL_SEGMENT_SIZE - 1));
        for c in &m.checkpoints {
            match &c.owner {
                Some(o) => {
                    pinned_chk.insert((o.clone(), c.id.clone()));
                }
                None => {
                    for owner in &owners {
                        pinned_chk.insert((owner.clone(), c.id.clone()));
                    }
                }
            }
        }
        for owner in &owners {
            let layout = TenantLayout::new(owner);
            if let Some(tail) = &m.wal_tail {
                for s in &tail.segments {
                    pinned_wal.insert(layout.wal_segment_path(s));
                }
            }
            // A history snapshot must not drag the cut down forever,
            // its tail is pinned by name and needs no successor rule.
            if live {
                let entry = cuts.entry(owner.clone()).or_insert(u64::MAX);
                *entry = (*entry).min(cut);
            }
        }
        for pt in &m.parent_tail {
            let layout = TenantLayout::new(&pt.tenant_ref);
            for s in &pt.segments {
                pinned_wal.insert(layout.wal_segment_path(s));
            }
        }
    };
    for (r, m) in &manifests {
        pin(r, m, true);
    }
    for (r, m) in &history {
        pin(r, m, false);
    }

    let mut garbage: BTreeSet<String> = BTreeSet::new();
    garbage.extend(expired_history);
    for r in manifests.keys() {
        let chk_prefix = format!("tenants/{r}/chk/");
        let wal_prefix = format!("tenants/{r}/wal/");
        let mut epochs: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for key in &keys {
            if let Some(rest) = key.strip_prefix(&chk_prefix) {
                if let Some((id, _)) = rest.split_once('/')
                    && !pinned_chk.contains(&(r.clone(), id.to_string()))
                {
                    garbage.insert(key.clone());
                }
            } else if let Some(rest) = key.strip_prefix(&wal_prefix)
                && let Some((epoch, _)) = rest.split_once('/')
            {
                epochs
                    .entry(epoch.to_string())
                    .or_default()
                    .push(key.clone());
            }
        }
        let cut = cuts.get(r).copied().unwrap_or(0);
        if cut == 0 {
            continue;
        }
        let layout = TenantLayout::new(r);
        for segments in epochs.values_mut() {
            segments.sort();
            for pair in segments.windows(2) {
                if pinned_wal.contains(&pair[0]) {
                    continue;
                }
                let qualified = pair[1]
                    .strip_prefix(&wal_prefix)
                    .expect("collected under this prefix");
                if segment_first_pg_lsn(store, &layout, qualified)? <= cut {
                    garbage.insert(pair[0].clone());
                }
            }
        }
    }

    let mut state: BTreeMap<String, u64> = BTreeMap::new();
    if let Some((data, _)) = store
        .get(CANDIDATES_KEY)
        .map_err(|e| format!("store: {e}"))?
    {
        let text =
            String::from_utf8(data).map_err(|_| "candidates state is not utf8".to_string())?;
        for line in text.lines() {
            let parsed = line
                .split_once(' ')
                .and_then(|(ts, key)| Some((ts.parse::<u64>().ok()?, key)));
            let Some((ts, key)) = parsed else {
                return Err(format!("bad candidates line {line:?}"));
            };
            state.insert(key.to_string(), ts);
        }
    }
    let mut next: BTreeMap<String, u64> = BTreeMap::new();
    let mut deleted = 0;
    for key in &garbage {
        match state.get(key) {
            Some(&stamp) if now_unix.saturating_sub(stamp) >= window_secs => {
                store.delete(key).map_err(|e| format!("store: {e}"))?;
                deleted += 1;
            }
            Some(&stamp) => {
                next.insert(key.clone(), stamp);
            }
            None => {
                next.insert(key.clone(), now_unix);
            }
        }
    }
    let mut text = String::new();
    for (key, stamp) in &next {
        text.push_str(&format!("{stamp} {key}\n"));
    }
    store
        .put(CANDIDATES_KEY, text.as_bytes())
        .map_err(|e| format!("store: {e}"))?;
    Ok(GcStats {
        tenants: manifests.len(),
        candidates: next.len(),
        deleted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use zou_store::manifest::{BranchOf, CheckpointKind, CheckpointRef, ParentTail, WalTail};
    use zou_store::{GroupCommit, GroupCommitConfig, LocalFsStore, Lsn, TailConfig, lease};

    fn put_chk(store: &dyn CasStore, r: &str, id: &str) {
        let layout = TenantLayout::new(r);
        store
            .put_if_absent(&layout.chk_index(id), b"f base/one 100\n")
            .unwrap();
        store
            .put_if_absent(&layout.checkpoint_page_index(id), b"runs 1024\n")
            .unwrap();
        store
            .put_if_absent(&layout.checkpoint_pages(id, 0), &[0xAB; 16])
            .unwrap();
    }

    fn chk_present(store: &dyn CasStore, r: &str, id: &str) -> bool {
        let layout = TenantLayout::new(r);
        store.get(&layout.chk_index(id)).unwrap().is_some()
    }

    /// Drop the history snapshots a leased session leaves behind. They
    /// carry real wall clock stamps, which would pin everything under
    /// the synthetic clocks these tests run gc with; retention has its
    /// own test.
    fn purge_history(store: &dyn CasStore, r: &str) {
        for key in store.list(&format!("tenants/{r}/manifests/")).unwrap() {
            store.delete(&key).unwrap();
        }
    }

    fn write_manifest(
        store: &dyn CasStore,
        r: &str,
        checkpoints: &[(&str, u64, CheckpointKind)],
        wal_tail: Option<WalTail>,
        branch_of: Option<(&str, u64)>,
    ) {
        let mut m = Manifest::new(r, 18);
        for (id, lsn, kind) in checkpoints {
            m.checkpoints.push(CheckpointRef {
                id: (*id).to_string(),
                lsn: Lsn(*lsn),
                kind: *kind,
                owner: None,
            });
        }
        m.wal_tail = wal_tail;
        m.branch_of = branch_of.map(|(parent, at)| BranchOf {
            tenant_ref: parent.to_string(),
            at_lsn: Lsn(at),
        });
        store
            .put(&TenantLayout::new(r).manifest(), &m.to_json())
            .unwrap();
    }

    #[test]
    fn an_unreferenced_checkpoint_waits_out_the_window_then_goes() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        put_chk(&store, "p", "aaa");
        put_chk(&store, "p", "bbb");
        write_manifest(
            &store,
            "p",
            &[("bbb", 0x100, CheckpointKind::Full)],
            None,
            None,
        );

        let first = run(&store, 1000, 100, 100_000).unwrap();
        assert_eq!(
            first,
            GcStats {
                tenants: 1,
                candidates: 3,
                deleted: 0
            },
            "the superseded checkpoint is three stamped keys"
        );
        assert!(chk_present(&store, "p", "aaa"));

        let early = run(&store, 1050, 100, 100_000).unwrap();
        assert_eq!(early.deleted, 0, "the window has not passed");
        assert!(chk_present(&store, "p", "aaa"));

        let due = run(&store, 1100, 100, 100_000).unwrap();
        assert_eq!(due.deleted, 3);
        assert_eq!(due.candidates, 0);
        assert!(!chk_present(&store, "p", "aaa"));
        assert!(chk_present(&store, "p", "bbb"));
    }

    #[test]
    fn a_branch_created_between_scans_pins_the_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        put_chk(&store, "p", "aaa");
        put_chk(&store, "p", "bbb");
        write_manifest(
            &store,
            "p",
            &[("bbb", 0x200, CheckpointKind::Full)],
            None,
            None,
        );
        assert_eq!(run(&store, 1000, 100, 100_000).unwrap().candidates, 3);

        // The branch lands after the stamping run and before the
        // deleting one, referencing the parent's superseded full, the
        // exact race the window exists for.
        write_manifest(
            &store,
            "child",
            &[("aaa", 0x100, CheckpointKind::Full)],
            None,
            Some(("p", 0x100)),
        );
        let pinned = run(&store, 2000, 100, 100_000).unwrap();
        assert_eq!(pinned.tenants, 2);
        assert_eq!(pinned.deleted, 0);
        assert_eq!(pinned.candidates, 0, "the branch pin drops the candidate");
        assert!(chk_present(&store, "p", "aaa"));

        // Dropping the branch restarts the clock, the old stamp must
        // not carry over.
        store
            .delete(&TenantLayout::new("child").manifest())
            .unwrap();
        assert_eq!(run(&store, 2000, 100, 100_000).unwrap().candidates, 3);
        assert_eq!(run(&store, 2099, 100, 100_000).unwrap().deleted, 0);
        assert_eq!(run(&store, 2100, 100, 100_000).unwrap().deleted, 3);
        assert!(!chk_present(&store, "p", "aaa"));
        assert!(chk_present(&store, "p", "bbb"));
    }

    #[test]
    fn wal_dies_only_by_the_fold_rule_and_a_branch_holds_the_cut_down() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFsStore::new(dir.path()));
        let layout = TenantLayout::new("p");
        write_manifest(&*store, "p", &[], None, None);

        // Three sealed segments through the real pusher, first record
        // pg LSNs at 0x100, 0x200, and exactly the 16MB cut.
        let held = lease::acquire(&*store, &layout, "test", 15, 1000).unwrap();
        let gc = GroupCommit::with_lease(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            Arc::new(Mutex::new(held)),
            Lsn(0),
            GroupCommitConfig::default(),
            TailConfig {
                seal_bytes: 1,
                ..TailConfig::default()
            },
        );
        for pg_lsn in [0x100u64, 0x200, WAL_SEGMENT_SIZE] {
            let mut record = pg_lsn.to_le_bytes().to_vec();
            record.extend_from_slice(b"payload");
            gc.append(&record).unwrap().wait().unwrap();
        }
        let segments = {
            let (data, _) = store.get(&layout.manifest()).unwrap().unwrap();
            Manifest::from_json(&data)
                .unwrap()
                .wal_tail
                .unwrap()
                .segments
        };
        assert_eq!(segments.len(), 3);
        gc.close().unwrap();
        purge_history(&*store, "p");
        let seg_key = |i: usize| layout.wal_segment_path(&segments[i]);

        // The fold kept the tail from the second segment on, redo just
        // past the cut, and a branch still pinned at the beginning of
        // history holds the cut at zero.
        write_manifest(
            &*store,
            "p",
            &[("ff", WAL_SEGMENT_SIZE + 0x80, CheckpointKind::Full)],
            Some(WalTail {
                epoch_dir: 1,
                from_lsn: Lsn(0x200),
                segments: segments[1..].to_vec(),
            }),
            None,
        );
        write_manifest(
            &*store,
            "child",
            &[("aa", 0x100, CheckpointKind::Full)],
            None,
            Some(("p", 0x100)),
        );
        put_chk(&*store, "p", "ff");
        put_chk(&*store, "p", "aa");
        assert_eq!(run(&*store, 1000, 0, 100_000).unwrap().candidates, 0);
        assert!(store.get(&seg_key(0)).unwrap().is_some());

        // Without the branch the cut is past the first two segments,
        // but only the first dies: the second is pinned by the tail
        // list and the third has no successor to bound it.
        store
            .delete(&TenantLayout::new("child").manifest())
            .unwrap();
        let stamped = run(&*store, 2000, 0, 100_000).unwrap();
        assert_eq!(stamped.deleted, 0, "a window of zero still takes two runs");
        let swept = run(&*store, 2001, 0, 100_000).unwrap();
        assert!(store.get(&seg_key(0)).unwrap().is_none());
        assert!(store.get(&seg_key(1)).unwrap().is_some());
        assert!(store.get(&seg_key(2)).unwrap().is_some());
        // The dropped branch also unpinned the aa capture, so the
        // sweep took it together with the segment.
        assert!(!chk_present(&*store, "p", "aa"));
        assert!(chk_present(&*store, "p", "ff"));
        assert_eq!(swept.deleted, 4);
    }

    #[test]
    fn history_snapshots_pin_within_retention_and_expire_after() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        put_chk(&store, "p", "aaa");
        put_chk(&store, "p", "bbb");
        write_manifest(
            &store,
            "p",
            &[("bbb", 0x200, CheckpointKind::Full)],
            None,
            None,
        );
        // A snapshot from when aaa was still the head, stamped 1000.
        let mut old = Manifest::new("p", 18);
        old.checkpoints.push(CheckpointRef {
            id: "aaa".into(),
            lsn: Lsn(0x100),
            kind: CheckpointKind::Full,
            owner: None,
        });
        let hkey = TenantLayout::new("p").manifest_history(1, 1000);
        store.put_if_absent(&hkey, &old.to_json()).unwrap();

        // Within retention the snapshot pins its checkpoint, a PITR
        // materialize at that second must still find everything.
        assert_eq!(run(&store, 1500, 0, 1000).unwrap().candidates, 0);
        assert!(chk_present(&store, "p", "aaa"));

        // Past retention the snapshot and the capture only it named
        // go out through the ordinary two phase window.
        let stamped = run(&store, 2100, 0, 1000).unwrap();
        assert_eq!(stamped.deleted, 0);
        assert_eq!(
            stamped.candidates, 4,
            "the snapshot plus the three aaa objects"
        );
        let swept = run(&store, 2101, 0, 1000).unwrap();
        assert_eq!(swept.deleted, 4);
        assert!(store.get(&hkey).unwrap().is_none());
        assert!(!chk_present(&store, "p", "aaa"));
        assert!(chk_present(&store, "p", "bbb"));
    }

    #[test]
    fn an_owner_tag_pins_under_the_owner_even_two_hops_down() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        put_chk(&store, "p", "aaa");
        put_chk(&store, "p", "bbb");
        write_manifest(
            &store,
            "p",
            &[("bbb", 0x200, CheckpointKind::Full)],
            None,
            None,
        );
        // A grandchild two hops from p: branch_of names its direct
        // parent c, only the owner tag on the inherited ref reaches p.
        let mut g = Manifest::new("g", 18);
        g.checkpoints.push(CheckpointRef {
            id: "aaa".into(),
            lsn: Lsn(0x100),
            kind: CheckpointKind::Full,
            owner: Some("p".into()),
        });
        g.branch_of = Some(BranchOf {
            tenant_ref: "c".into(),
            at_lsn: Lsn(0x100),
        });
        store
            .put(&TenantLayout::new("g").manifest(), &g.to_json())
            .unwrap();

        assert_eq!(run(&store, 1000, 0, 100_000).unwrap().candidates, 0);
        assert!(chk_present(&store, "p", "aaa"));

        store.delete(&TenantLayout::new("g").manifest()).unwrap();
        assert_eq!(run(&store, 2000, 0, 100_000).unwrap().candidates, 3);
        assert_eq!(run(&store, 2001, 0, 100_000).unwrap().deleted, 3);
        assert!(!chk_present(&store, "p", "aaa"));
        assert!(chk_present(&store, "p", "bbb"));
    }

    #[test]
    fn a_parent_tail_entry_pins_segments_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFsStore::new(dir.path()));
        let layout = TenantLayout::new("p");
        write_manifest(&*store, "p", &[], None, None);

        let held = lease::acquire(&*store, &layout, "test", 15, 1000).unwrap();
        let gc = GroupCommit::with_lease(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            Arc::new(Mutex::new(held)),
            Lsn(0),
            GroupCommitConfig::default(),
            TailConfig {
                seal_bytes: 1,
                ..TailConfig::default()
            },
        );
        for pg_lsn in [0x100u64, 0x200, WAL_SEGMENT_SIZE] {
            let mut record = pg_lsn.to_le_bytes().to_vec();
            record.extend_from_slice(b"payload");
            gc.append(&record).unwrap().wait().unwrap();
        }
        let segments = {
            let (data, _) = store.get(&layout.manifest()).unwrap().unwrap();
            Manifest::from_json(&data)
                .unwrap()
                .wal_tail
                .unwrap()
                .segments
        };
        gc.close().unwrap();
        purge_history(&*store, "p");
        let seg0 = layout.wal_segment_path(&segments[0]);

        // The parent folded past the first segment and dropped it from
        // its tail, so nothing of p's own holds it anymore.
        put_chk(&*store, "p", "ff");
        write_manifest(
            &*store,
            "p",
            &[("ff", WAL_SEGMENT_SIZE + 0x80, CheckpointKind::Full)],
            Some(WalTail {
                epoch_dir: 1,
                from_lsn: Lsn(0x200),
                segments: segments[1..].to_vec(),
            }),
            None,
        );
        // A child whose checkpoints sit past the cut: the only hold on
        // the first segment is its frozen parent tail list.
        let mut c = Manifest::new("c", 18);
        c.checkpoints.push(CheckpointRef {
            id: "ff".into(),
            lsn: Lsn(WAL_SEGMENT_SIZE + 0x80),
            kind: CheckpointKind::Full,
            owner: Some("p".into()),
        });
        c.branch_of = Some(BranchOf {
            tenant_ref: "p".into(),
            at_lsn: Lsn(WAL_SEGMENT_SIZE + 0x80),
        });
        c.parent_tail.push(ParentTail {
            tenant_ref: "p".into(),
            from_lsn: Lsn(0x100),
            segments: vec![segments[0].clone()],
        });
        store
            .put(&TenantLayout::new("c").manifest(), &c.to_json())
            .unwrap();

        assert_eq!(run(&*store, 1000, 0, 100_000).unwrap().candidates, 0);
        assert!(store.get(&seg0).unwrap().is_some());

        // Dropping the child frees the segment through the window.
        store.delete(&TenantLayout::new("c").manifest()).unwrap();
        assert_eq!(run(&*store, 2000, 0, 100_000).unwrap().candidates, 1);
        assert_eq!(run(&*store, 2001, 0, 100_000).unwrap().deleted, 1);
        assert!(store.get(&seg0).unwrap().is_none());
    }

    #[test]
    fn a_tenant_without_a_manifest_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        put_chk(&store, "ghost", "aaa");

        assert_eq!(run(&store, 1000, 0, 100_000).unwrap(), GcStats::default());
        assert_eq!(run(&store, 2000, 0, 100_000).unwrap(), GcStats::default());
        assert!(chk_present(&store, "ghost", "aaa"));
    }
}
