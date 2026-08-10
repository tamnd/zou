//! Garbage collection: delete objects no retained manifest references.
//!
//! The store only ever grows on its own: a fold down supersedes a whole
//! checkpoint chain and a failed fold leaves captures behind with no
//! manifest naming them. The gc job walks every tenant under the store
//! root, pins everything any current manifest references, and deletes
//! the rest through a two phase candidate window.
//!
//! Pinning follows branches: a manifest with `branch_of` references
//! objects that live under its parent's prefix, so its checkpoint ids
//! pin keys under both prefixes, and a tenant whose manifest is
//! missing or unreadable contributes nothing and loses nothing. A
//! checkpoint ref carrying an owner tag is sharper, it pins under
//! exactly that owner, which is what keeps a grandparent's capture
//! alive when `branch_of` only names the direct parent.
//!
//! PITR retention rides the same pins. Every state change leaves a
//! snapshot under `manifests/`, and a snapshot younger than the
//! retention window pins its references exactly like a live manifest.
//! A snapshot past retention is ordinary two phase garbage, and
//! whatever only it referenced follows it out through the same window.
//!
//! WAL is not this job's problem. A tenant's log lives under its own
//! `log/` prefix, consolidation rewrites it and `gc_landing` in zou-log
//! trims the landing chain by its own rules, so this job only ever
//! collects under `chk/` and `manifests/` and never touches a WAL
//! object.
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

    // The pins: checkpoint ids under every prefix the manifest can
    // reference. An owner tagged checkpoint ref pins under exactly its
    // owner, an untagged one predates the tags and pins under
    // everything reachable.
    let mut pinned_chk: BTreeSet<(String, String)> = BTreeSet::new();
    let mut pin = |r: &str, m: &Manifest| {
        let mut owners = vec![r.to_string()];
        if let Some(b) = &m.branch_of {
            owners.push(b.tenant_ref.clone());
        }
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
    };
    for (r, m) in &manifests {
        pin(r, m);
    }
    for (r, m) in &history {
        pin(r, m);
    }

    let mut garbage: BTreeSet<String> = BTreeSet::new();
    garbage.extend(expired_history);
    for r in manifests.keys() {
        let chk_prefix = format!("tenants/{r}/chk/");
        for key in &keys {
            if let Some(rest) = key.strip_prefix(&chk_prefix)
                && let Some((id, _)) = rest.split_once('/')
                && !pinned_chk.contains(&(r.clone(), id.to_string()))
            {
                garbage.insert(key.clone());
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
    use zou_store::LocalFsStore;
    use zou_store::Lsn;
    use zou_store::manifest::{BranchOf, CheckpointKind, CheckpointRef};

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

    fn write_manifest(
        store: &dyn CasStore,
        r: &str,
        checkpoints: &[(&str, u64, CheckpointKind)],
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
        write_manifest(&store, "p", &[("bbb", 0x100, CheckpointKind::Full)], None);

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

    /// A tenant's WAL chain sits under the same `tenants/<ref>/` prefix
    /// this job walks, and no manifest names a landing segment, so a job
    /// that collected everything it could not pin would delete the log
    /// out from under a running pusher. Only `chk/` and `manifests/` are
    /// ever collected here.
    #[test]
    fn the_log_is_never_garbage() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        put_chk(&store, "p", "aaa");
        write_manifest(&store, "p", &[("aaa", 0x100, CheckpointKind::Full)], None);
        let landing = "tenants/p/log/cellwal/0000/0000000000000001";
        store.put_if_absent(landing, b"a window of wal").unwrap();

        run(&store, 1000, 0, 100_000).unwrap();
        let after = run(&store, 2000, 0, 100_000).unwrap();
        assert_eq!(after.deleted, 0, "nothing here was ever garbage");
        assert!(
            store.get(landing).unwrap().is_some(),
            "the log is still there"
        );
    }

    #[test]
    fn a_branch_created_between_scans_pins_the_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        put_chk(&store, "p", "aaa");
        put_chk(&store, "p", "bbb");
        write_manifest(&store, "p", &[("bbb", 0x200, CheckpointKind::Full)], None);
        assert_eq!(run(&store, 1000, 100, 100_000).unwrap().candidates, 3);

        // The branch lands after the stamping run and before the
        // deleting one, referencing the parent's superseded full, the
        // exact race the window exists for.
        write_manifest(
            &store,
            "child",
            &[("aaa", 0x100, CheckpointKind::Full)],
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
    fn history_snapshots_pin_within_retention_and_expire_after() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        put_chk(&store, "p", "aaa");
        put_chk(&store, "p", "bbb");
        write_manifest(&store, "p", &[("bbb", 0x200, CheckpointKind::Full)], None);
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
        write_manifest(&store, "p", &[("bbb", 0x200, CheckpointKind::Full)], None);
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
    fn a_tenant_without_a_manifest_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        put_chk(&store, "ghost", "aaa");

        assert_eq!(run(&store, 1000, 0, 100_000).unwrap(), GcStats::default());
        assert_eq!(run(&store, 2000, 0, 100_000).unwrap(), GcStats::default());
        assert!(chk_present(&store, "ghost", "aaa"));
    }
}
