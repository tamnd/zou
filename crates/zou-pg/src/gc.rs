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
//! publishing a branch from it.
//!
//! One job runs at a time, because the candidates object is swapped
//! without a guard and two runs would each write a view of it that the
//! other's deletions have already made wrong. [`sweep`] is the front
//! door that enforces it: a lock object under the same prefix, taken
//! with the store's own conditional write, held for a TTL and released
//! at the end, so a node that dies mid sweep blocks the next one for
//! the TTL and not forever. [`run`] itself takes no lock, which is what
//! a test wants and not what a deployment does.

use std::collections::{BTreeMap, BTreeSet};

use zou_store::CasStore;
use zou_store::cas::CasError;
use zou_store::layout::TenantLayout;
use zou_store::manifest::Manifest;

/// Where the two phase state lives, next to `tenants/` under the store
/// root. Lines of `<first-seen-unix> <key>`.
pub const CANDIDATES_KEY: &str = "gc/CANDIDATES";

/// Where the one at a time guard lives. `<holder> <expires-unix>`.
pub const LOCK_KEY: &str = "gc/LOCK";

/// The safety window a key waits out between being stamped a candidate
/// and being deleted. A day, which is longer than any fold upload and
/// longer than the gap between reading a manifest and publishing a
/// branch from it.
pub const DEFAULT_WINDOW_SECS: u64 = 24 * 60 * 60;

/// How far back PITR reaches by default. History snapshots younger than
/// this pin what they reference, so it is the real retention promise
/// and the window is only the delay on collecting what fell out of it.
pub const DEFAULT_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;

/// How long a run's claim on the store is good for.
///
/// A sweep of a large store is minutes, not an hour, so this is mostly
/// the answer to how long a crashed run blocks the next one. A store
/// big enough that a sweep runs past it should raise it rather than
/// have a second node start one on top of the first.
pub const DEFAULT_LOCK_TTL_SECS: u64 = 60 * 60;

/// What a run is asked to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    /// Seconds a candidate waits before it is deleted.
    pub window_secs: u64,
    /// Seconds of PITR history that pins what it references.
    pub retention_secs: u64,
    /// Seconds the lock a sweep holds stays valid.
    pub lock_ttl_secs: u64,
    /// Scan and report, write nothing and delete nothing.
    pub dry_run: bool,
    /// Take the lock even if somebody else holds it. For the case where
    /// a run died and the operator knows it, since the TTL is the only
    /// other way out.
    pub force: bool,
}

impl Default for Policy {
    fn default() -> Self {
        Policy {
            window_secs: DEFAULT_WINDOW_SECS,
            retention_secs: DEFAULT_RETENTION_SECS,
            lock_ttl_secs: DEFAULT_LOCK_TTL_SECS,
            dry_run: false,
            force: false,
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct GcStats {
    /// Tenants with a readable manifest.
    pub tenants: usize,
    /// Keys stamped and waiting out the safety window after this run.
    pub candidates: usize,
    /// Objects deleted by this run, or under a dry run the objects it
    /// would have deleted.
    pub deleted: usize,
    /// The keys behind that count, filled in under a dry run only. A
    /// real run has already deleted them and a list of thousands is not
    /// what a log wants; a person asking what would go wants every one.
    pub doomed: Vec<String>,
}

/// What a [`sweep`] did, since not getting to run is an outcome and not
/// a failure.
#[derive(Debug, PartialEq, Eq)]
pub enum Sweep {
    Ran(GcStats),
    /// Somebody else holds the lock. Try again after `until_unix`, or
    /// pass `force` if that holder is known to be gone.
    Busy {
        holder: String,
        until_unix: u64,
    },
}

/// The stamp encoded in a history key, `<epoch>-<unix>.json`.
fn history_stamp(key: &str) -> Option<u64> {
    let name = key.rsplit('/').next()?;
    let (_, unix) = name.strip_suffix(".json")?.split_once('-')?;
    unix.parse().ok()
}

/// One gc pass over the whole store, under the default policy with the
/// window and retention the caller names. `now_unix` is the caller's
/// clock, so a run never deletes a key it stamped itself and a window
/// of zero still takes two runs.
///
/// This takes no lock. [`sweep`] is the one a deployment calls.
pub fn run(
    store: &dyn CasStore,
    now_unix: u64,
    window_secs: u64,
    retention_secs: u64,
) -> Result<GcStats, String> {
    run_with(
        store,
        now_unix,
        Policy {
            window_secs,
            retention_secs,
            ..Policy::default()
        },
    )
}

/// One gc pass over the whole store, told exactly what to do.
///
/// Under a dry run nothing is written: no key is deleted, the
/// candidates object is left as it was, and the stats say what the same
/// run without the flag would have done. That is worth reading twice,
/// because a first real run stamps candidates and a dry run does not,
/// so the two runs after a dry run are still the two runs a deletion
/// takes.
pub fn run_with(store: &dyn CasStore, now_unix: u64, policy: Policy) -> Result<GcStats, String> {
    let window_secs = policy.window_secs;
    let retention_secs = policy.retention_secs;
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
    let mut doomed = Vec::new();
    for key in &garbage {
        match state.get(key) {
            Some(&stamp) if now_unix.saturating_sub(stamp) >= window_secs => {
                if policy.dry_run {
                    doomed.push(key.clone());
                } else {
                    store.delete(key).map_err(|e| format!("store: {e}"))?;
                }
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
    if !policy.dry_run {
        let mut text = String::new();
        for (key, stamp) in &next {
            text.push_str(&format!("{stamp} {key}\n"));
        }
        store
            .put(CANDIDATES_KEY, text.as_bytes())
            .map_err(|e| format!("store: {e}"))?;
    }
    Ok(GcStats {
        tenants: manifests.len(),
        candidates: next.len(),
        deleted,
        doomed,
    })
}

/// A claim on the store, which is the line we wrote and nothing else:
/// releasing checks the object still says that before deleting it, so a
/// run whose lock expired and was taken over does not release the new
/// holder's.
struct Claim {
    line: String,
}

enum Claimed {
    Ours(Claim),
    Theirs { holder: String, until_unix: u64 },
}

/// The one lock line, or `None` if whatever is there is not one.
fn held(data: &[u8]) -> Option<(String, u64)> {
    let text = String::from_utf8(data.to_vec()).ok()?;
    let (holder, until) = text.trim().split_once(' ')?;
    Some((holder.to_string(), until.parse().ok()?))
}

/// Take the store wide gc lock, or say who has it.
///
/// An expired lock is taken over, which is what makes a crashed run
/// cost a TTL rather than an operator. A lock body nobody can parse is
/// treated the same way, since a line no reader understands is a line
/// no releaser will ever remove.
fn lock(
    store: &dyn CasStore,
    holder: &str,
    now_unix: u64,
    policy: Policy,
) -> Result<Claimed, String> {
    let current = store.get(LOCK_KEY).map_err(|e| format!("store: {e}"))?;
    if !policy.force
        && let Some((data, _)) = &current
        && let Some((holder, until_unix)) = held(data)
        && until_unix > now_unix
    {
        return Ok(Claimed::Theirs { holder, until_unix });
    }
    let until_unix = now_unix.saturating_add(policy.lock_ttl_secs);
    let line = format!("{holder} {until_unix}\n");
    match store.put_if_match(LOCK_KEY, line.as_bytes(), current.as_ref().map(|(_, v)| v)) {
        Ok(_) => Ok(Claimed::Ours(Claim { line })),
        // Somebody swapped the lock between our read and our write,
        // which is another run taking it. Theirs, whoever they are.
        Err(CasError::Conflict { .. }) | Err(CasError::AlreadyExists { .. }) => {
            Ok(Claimed::Theirs {
                holder: "another run".to_string(),
                until_unix,
            })
        }
        Err(e) => Err(format!("store: {e}")),
    }
}

/// Give the lock back, if it is still ours to give.
fn unlock(store: &dyn CasStore, claim: &Claim) -> Result<(), String> {
    match store.get(LOCK_KEY).map_err(|e| format!("store: {e}"))? {
        Some((data, _)) if data == claim.line.as_bytes() => {
            store.delete(LOCK_KEY).map_err(|e| format!("store: {e}"))
        }
        _ => Ok(()),
    }
}

/// One gc pass, with the store wide lock that keeps two of them from
/// running at once.
///
/// `holder` names whoever is asking, and it goes in the lock so the run
/// that finds it busy can say who has it. A dry run takes no lock: it
/// writes nothing, and a person asking what would go should not have to
/// wait out somebody's sweep to find out.
///
/// The lock is released whether the pass worked or not, and a release
/// that itself fails is not turned into the answer, since the TTL
/// already covers that case and the pass is what the caller asked
/// about.
pub fn sweep(
    store: &dyn CasStore,
    holder: &str,
    now_unix: u64,
    policy: Policy,
) -> Result<Sweep, String> {
    if policy.dry_run {
        return run_with(store, now_unix, policy).map(Sweep::Ran);
    }
    let claim = match lock(store, holder, now_unix, policy)? {
        Claimed::Ours(claim) => claim,
        Claimed::Theirs { holder, until_unix } => return Ok(Sweep::Busy { holder, until_unix }),
    };
    let out = run_with(store, now_unix, policy);
    if let Err(e) = unlock(store, &claim) {
        log::warn!("gc: the lock was not released: {e}");
    }
    out.map(Sweep::Ran)
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
                deleted: 0,
                doomed: vec![]
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

    fn policy(window_secs: u64) -> Policy {
        Policy {
            window_secs,
            retention_secs: 100_000,
            ..Policy::default()
        }
    }

    /// The point of a dry run is that a person can ask the question
    /// before the answer costs anything, so it has to leave the store
    /// exactly as it found it, candidates object included.
    #[test]
    fn a_dry_run_names_what_would_go_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        put_chk(&store, "p", "aaa");
        put_chk(&store, "p", "bbb");
        write_manifest(&store, "p", &[("bbb", 0x100, CheckpointKind::Full)], None);

        // Nothing is a candidate yet, so a dry run before the first
        // real one has nothing to name, and it does not stamp them.
        let first = run_with(&store, 1000, policy(100).dry(true)).unwrap();
        assert_eq!((first.candidates, first.deleted), (3, 0));
        assert!(first.doomed.is_empty());
        assert!(
            store.get(CANDIDATES_KEY).unwrap().is_none(),
            "a dry run did not create the candidates object"
        );

        run(&store, 1000, 100, 100_000).unwrap();
        let (state, _) = store.get(CANDIDATES_KEY).unwrap().expect("stamped");

        let due = run_with(&store, 1100, policy(100).dry(true)).unwrap();
        assert_eq!(due.deleted, 3);
        assert_eq!(due.doomed.len(), 3, "each one named: {:?}", due.doomed);
        assert!(
            due.doomed
                .iter()
                .all(|key| key.starts_with("tenants/p/chk/"))
        );
        assert!(chk_present(&store, "p", "aaa"), "and still there");
        assert_eq!(
            store.get(CANDIDATES_KEY).unwrap().map(|(data, _)| data),
            Some(state),
            "the stamps are untouched, so the real run still deletes"
        );

        assert_eq!(run(&store, 1100, 100, 100_000).unwrap().deleted, 3);
        assert!(!chk_present(&store, "p", "aaa"));
    }

    #[test]
    fn one_sweep_at_a_time_and_a_dead_holder_costs_a_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        put_chk(&store, "p", "aaa");
        write_manifest(&store, "p", &[("aaa", 0x100, CheckpointKind::Full)], None);
        let policy = Policy {
            lock_ttl_secs: 600,
            ..policy(100)
        };

        // A node that stops holding the lock when it is done leaves
        // nothing behind for the next one to wait on.
        assert!(matches!(
            sweep(&store, "node-a", 1000, policy).unwrap(),
            Sweep::Ran(_)
        ));
        assert!(store.get(LOCK_KEY).unwrap().is_none());

        // A node that does not, because it died, holds it until the
        // TTL, and the run that finds it says who and until when
        // instead of running on top of it.
        store.put(LOCK_KEY, b"node-a 1600\n").unwrap();
        assert_eq!(
            sweep(&store, "node-b", 1100, policy).unwrap(),
            Sweep::Busy {
                holder: "node-a".to_string(),
                until_unix: 1600
            }
        );
        // Asking what would go is still allowed, since it writes
        // nothing that the holder's run could disagree with.
        assert!(matches!(
            sweep(&store, "node-b", 1100, policy.dry(true)).unwrap(),
            Sweep::Ran(_)
        ));

        assert!(matches!(
            sweep(&store, "node-b", 1601, policy).unwrap(),
            Sweep::Ran(_),
        ));
        assert!(store.get(LOCK_KEY).unwrap().is_none(), "and released");

        // Force is for the operator who knows the holder is gone and is
        // not waiting out the rest of the TTL to say so.
        store.put(LOCK_KEY, b"node-a 9000\n").unwrap();
        assert!(matches!(
            sweep(&store, "node-b", 1700, policy).unwrap(),
            Sweep::Busy { .. }
        ));
        assert!(matches!(
            sweep(
                &store,
                "node-b",
                1700,
                Policy {
                    force: true,
                    ..policy
                }
            )
            .unwrap(),
            Sweep::Ran(_)
        ));
    }

    /// A holder that comes back after its lock was taken over must not
    /// release the lock it no longer has, or the run holding it is
    /// suddenly running unguarded.
    #[test]
    fn releasing_a_lock_somebody_else_took_leaves_it_alone() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let mine = Claim {
            line: "node-a 1600\n".to_string(),
        };
        store.put(LOCK_KEY, b"node-b 2600\n").unwrap();

        unlock(&store, &mine).unwrap();
        assert_eq!(
            store.get(LOCK_KEY).unwrap().map(|(data, _)| data),
            Some(b"node-b 2600\n".to_vec())
        );
    }

    /// A lock body nothing can read is a lock nothing will release, so
    /// it has to be takeable rather than a store that never collects
    /// again.
    #[test]
    fn a_lock_nobody_can_read_is_taken_over() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        store.put(LOCK_KEY, b"\x00\x01 not a lock line").unwrap();

        assert!(matches!(
            sweep(&store, "node-a", 1000, Policy::default()).unwrap(),
            Sweep::Ran(_)
        ));
    }

    impl Policy {
        fn dry(self, dry_run: bool) -> Policy {
            Policy { dry_run, ..self }
        }
    }
}
