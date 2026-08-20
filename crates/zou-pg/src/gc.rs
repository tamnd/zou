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
//! Page layers pin the same way, off the shard manifests rather than
//! the tenant one. Compaction commits by retiring its inputs from a
//! shard manifest and listing its outputs in the same CAS, which makes
//! every retired input dead the instant the swap lands: the outputs
//! preserve every record the inputs held, and a pass that died before
//! the swap left objects the manifest never saw. Neither had a
//! collector until now, and a whole shard rewrite every couple of
//! minutes leaves a whole shard behind every couple of minutes. A
//! fifteen minute soak at scale 10 wrote four rewrites of about 280 MB
//! and kept all four, 1.8 GB of store for a 188 MB database, while the
//! amp gauge sat at 2 against a bound of 5, because read amplification
//! is about the layers a read walks and says nothing about the ones
//! nothing walks at all.
//!
//! Every `SHARD` object in the store pins, not only the ones at the
//! current shard count. A split serves lazily: a descendant reads
//! through its ancestors' manifests until it covers its own keyspace,
//! so an older era's layers are live exactly as long as that era's
//! manifest is, and pruning the lineage is what releases them. Layers
//! pin by owner and name, so a branch child pins its parent's layers
//! under the parent's prefix, the way a checkpoint ref already does.
//! A layer a flush has uploaded but not yet published looks exactly
//! like a layer a crash abandoned, and the two phase window is what
//! tells them apart: the second run reads a manifest that names the
//! one and still does not name the other.
//!
//! WAL is not this job's problem. A tenant's log lives under its own
//! `log/` prefix, consolidation rewrites it and `gc_landing` in zou-log
//! trims the landing chain by its own rules, so this job never touches
//! a WAL object.
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
//! A stamp is also thrown away once it is older than the retention.
//! Runs only sample, so between two of them a key can go garbage, live
//! and garbage again, and a stamp taken before the key was ever
//! published would otherwise carry through its whole life and get it
//! deleted the moment it is superseded, window and all. What rules that
//! out inside the retention is that a key which was live in the gap had
//! a manifest published naming it, every publish leaves a snapshot, and
//! a snapshot inside the retention pins, so a run sees the key pinned
//! and drops the stamp. Past the retention that evidence is gone, so
//! the stamp goes with it. Retention therefore has to be longer than
//! the window, or no stamp lives long enough to come of age and nothing
//! is ever collected. The defaults are a day and a week.
//!
//! What none of this covers is a reader that is not a manifest: nothing
//! records that somebody is part way through fetching a checkpoint, so
//! the window is the only grace a superseded object gets, and a fetch
//! slower than the window can still find its bytes gone.
//!
//! Two things this job leaks rather than collects, both deliberately.
//! A prefix with no live manifest is never scanned, so a tenant deleted
//! by removing its `MANIFEST` keeps its captures and its snapshots for
//! good; deleting a tenant means deleting its prefix, and this job is
//! not the thing that does it. That is the same rule that makes branch
//! creation safe, since a branch has no manifest until its last write
//! and would otherwise be racing the collector for its own bytes. And a
//! `manifests/` key whose name does not parse as `<epoch>-<unix>.json`
//! is skipped entirely, so it neither pins nor expires. Both fail
//! towards keeping bytes, which is the direction to fail in.
//!
//! `tests/gc_model.rs` walks the reachable interleavings of publishes,
//! folds, branch creations, crashes and runs against a real store, and
//! checks after every step that nothing a live manifest or an in
//! retention snapshot names has been collected. It also drops each
//! precondition in turn and requires the violation to show up, which is
//! what says the preconditions are load bearing rather than decorative.
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
use zou_store::layermap::LayerDesc;
use zou_store::layout::TenantLayout;
use zou_store::manifest::Manifest;
use zou_store::shardmanifest::PageShardManifest;

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
    // A stamp is only trusted while the snapshots that would contradict
    // it are still around, so a retention shorter than the window means
    // no stamp ever both survives and comes of age, and the job stamps
    // forever without deleting. That is the safe direction to fail in,
    // but silently doing nothing is not a way to fail, so it is said.
    if retention_secs <= window_secs {
        log::warn!(
            "gc: retention {retention_secs}s is not longer than the window {window_secs}s, \
             so nothing will be collected"
        );
    }
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

    // The layer pins, read off every shard manifest in the store. Every
    // one of them counts, including eras below the current shard count,
    // because a split serves through its ancestors until it has
    // rewritten past them. An entry with an owner tag names the prefix
    // its bytes actually live under; an untagged one is this tenant's
    // own, and pins under the branch parent too for the same reason an
    // untagged checkpoint ref does.
    let mut pinned_layer: BTreeSet<(String, String)> = BTreeSet::new();
    for r in &refs {
        let layout = TenantLayout::new(r);
        let shards_dir = layout.shards_dir();
        let mut owners = vec![r.to_string()];
        if let Some(b) = manifests.get(r).and_then(|m| m.branch_of.as_ref()) {
            owners.push(b.tenant_ref.clone());
        }
        for key in &keys {
            if !key.starts_with(&shards_dir) || !key.ends_with("/SHARD") {
                continue;
            }
            let Some((data, _)) = store.get(key).map_err(|e| format!("store: {e}"))? else {
                continue;
            };
            let sm = PageShardManifest::decode(&data).map_err(|e| format!("shard {key}: {e}"))?;
            for e in &sm.layers {
                match &e.owner {
                    Some(o) => {
                        pinned_layer.insert((o.clone(), e.name.clone()));
                    }
                    None => {
                        for owner in &owners {
                            pinned_layer.insert((owner.clone(), e.name.clone()));
                        }
                    }
                }
            }
        }
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
    // Retired layers. Only names that parse as layers are touched, so
    // the `SHARD` manifest itself and anything else a shard prefix
    // grows later are left where they are rather than deleted by a job
    // that does not know what they are.
    for r in manifests.keys() {
        let shards_dir = TenantLayout::new(r).shards_dir();
        for key in &keys {
            let Some(rest) = key.strip_prefix(&shards_dir) else {
                continue;
            };
            let Some((_, name)) = rest.split_once('/') else {
                continue;
            };
            if LayerDesc::parse(name, 0).is_ok()
                && !pinned_layer.contains(&(r.clone(), name.to_string()))
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
        // A stamp says the key was garbage then. What makes it say the
        // key has been garbage *since* then is the snapshot record:
        // anything that named the key in between published a manifest,
        // every published manifest leaves a snapshot, and a snapshot
        // inside the retention pins what it names, which takes the key
        // off this list and restarts the wait. That reasoning runs out
        // exactly when the stamp is older than the retention, because
        // then a life the key lived after being stamped can have left
        // only snapshots that have since expired. Past that the stamp
        // is thrown away and the key waits a fresh window.
        //
        // Without this a key stamped while its upload was still in
        // flight keeps that stamp through however long it then spends
        // published and referenced, and the first run after it is
        // superseded deletes it on the spot with no window at all.
        let stamp = state
            .get(key)
            .copied()
            .filter(|stamp| now_unix.saturating_sub(*stamp) < retention_secs);
        match stamp {
            Some(stamp) if now_unix.saturating_sub(stamp) >= window_secs => {
                if policy.dry_run {
                    doomed.push(key.clone());
                } else {
                    store.delete(key).map_err(|e| format!("store: {e}"))?;
                }
                deleted += 1;
            }
            Some(stamp) => {
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
    use zou_store::layer::LayerKey;
    use zou_store::manifest::{BranchOf, CheckpointKind, CheckpointRef};
    use zou_store::shardmanifest::LayerEntry;

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

    /// Publish a manifest the way the real one does, leaving behind the
    /// history snapshot that is the record of what was referenced when.
    fn publish(
        store: &dyn CasStore,
        r: &str,
        epoch: u64,
        unix: u64,
        checkpoints: &[(&str, u64, CheckpointKind)],
    ) {
        write_manifest(store, r, checkpoints, None);
        let (data, _) = store
            .get(&TenantLayout::new(r).manifest())
            .unwrap()
            .unwrap();
        store
            .put(&TenantLayout::new(r).manifest_history(epoch, unix), &data)
            .unwrap();
    }

    /// Found by the model check in `tests/gc_model.rs`, which is worth
    /// saying because no scenario anybody wrote by hand put a run in
    /// the one place this needs it: after the upload and before the
    /// publish. The stamp taken there is about a checkpoint nothing has
    /// ever referenced, and it used to survive however long the
    /// checkpoint then spent referenced, so the first run after it was
    /// superseded deleted it with none of the window it was owed. A
    /// branch that read the manifest just before that supersede is left
    /// naming bytes that are gone, which is the failure the two phases
    /// exist to prevent.
    ///
    /// Reaching it takes a gap between runs longer than the retention,
    /// because inside the retention the snapshot from the publish is
    /// still there and pinning, and a run that sees the pin drops the
    /// stamp. A week without a sweep is a broken cron, not a fantasy.
    #[test]
    fn a_stamp_taken_before_a_checkpoint_was_ever_published_does_not_count() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        publish(&store, "p", 1, 1_000, &[]);
        put_chk(&store, "p", "aaa");

        // A run while the fold is still uploading. Nothing names aaa.
        let stamping = run(&store, 1_000, 100, 5_000).unwrap();
        assert_eq!(stamping.candidates, 3, "an upload in flight looks garbage");

        // The fold publishes. aaa is live from here, and no sweep runs
        // for longer than the retention, so by the time one does the
        // snapshot that would have shown aaa alive has expired.
        publish(
            &store,
            "p",
            2,
            2_000,
            &[("aaa", 0x100, CheckpointKind::Full)],
        );
        publish(&store, "p", 3, 9_000, &[]);

        let after = run(&store, 9_000, 100, 5_000).unwrap();
        assert_eq!(
            after.deleted, 0,
            "the window starts when aaa became garbage, not when it was uploaded"
        );
        assert!(chk_present(&store, "p", "aaa"));

        assert_eq!(run(&store, 9_099, 100, 5_000).unwrap().deleted, 0);
        // Three keys of checkpoint, and the two snapshots that fell out
        // of retention while nothing was sweeping.
        assert_eq!(run(&store, 9_100, 100, 5_000).unwrap().deleted, 5);
        assert!(!chk_present(&store, "p", "aaa"));
    }

    /// The rule above throws a stamp away once it is older than the
    /// retention, so a retention no longer than the window leaves no
    /// stamp able to both survive and come of age. Failing that way
    /// round is the safe one, and it is checked rather than assumed
    /// because the other way round is deleting early. The command
    /// refuses the policy outright; this is what the library does if
    /// something else hands it one.
    #[test]
    fn a_retention_shorter_than_the_window_collects_nothing_rather_than_early() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        write_manifest(&store, "p", &[], None);
        put_chk(&store, "p", "aaa");

        for now in [1000, 1100, 1200, 1300] {
            let pass = run(&store, now, 100, 50).unwrap();
            assert_eq!(pass.deleted, 0, "nothing is collected at {now}");
            assert_eq!(pass.candidates, 3, "and it is restamped every time");
        }
        assert!(chk_present(&store, "p", "aaa"));
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

    /// A layer object and the name a manifest would list it under. The
    /// lsn range is the only thing that varies, which is enough to make
    /// one layer a rewrite of another.
    fn put_layer(store: &dyn CasStore, r: &str, shard: u16, lo: u64, hi: u64) -> String {
        let layout = TenantLayout::new(r);
        let key = layout.delta_layer(
            shard,
            &LayerKey::page(1663, 5, 16384, 0, 0),
            &LayerKey::page(1663, 5, 16384, 0, 999),
            Lsn(lo),
            Lsn(hi),
        );
        store.put_if_absent(&key, &[0xCD; 32]).unwrap();
        key.rsplit('/').next().unwrap().to_string()
    }

    /// The manifest a shard publishes: whichever layers it still serves,
    /// each with the owner the reader would fetch it from.
    fn write_shard(store: &dyn CasStore, r: &str, shard: u16, layers: &[(&str, Option<&str>)]) {
        let mut sm = PageShardManifest::new(shard);
        sm.disk_consistent_lsn = Lsn(0x400);
        for (name, owner) in layers {
            sm.layers.push(LayerEntry {
                name: (*name).to_string(),
                size: 32,
                owner: owner.map(str::to_string),
                upto: None,
            });
        }
        store
            .put(&TenantLayout::new(r).shard_manifest(shard), &sm.encode())
            .unwrap();
    }

    /// The bug that filled two disks. Compaction retires its inputs from
    /// the shard manifest in the same CAS that lists its outputs, so the
    /// inputs are dead the moment the swap lands, and until now nothing
    /// ever deleted them. A fifteen minute soak kept four whole shard
    /// rewrites of 280 MB each and reported an amplification of 2.
    #[test]
    fn a_layer_compaction_retired_waits_out_the_window_then_goes() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        write_manifest(&store, "p", &[], None);
        let old = put_layer(&store, "p", 0, 0x100, 0x200);
        let new = put_layer(&store, "p", 0, 0x100, 0x400);
        write_shard(&store, "p", 0, &[(&new, None)]);
        let key = |name: &str| format!("tenants/p/shards/0000/{name}");

        let first = run(&store, 1000, 100, 100_000).unwrap();
        assert_eq!(first.deleted, 0, "the first run only stamps");
        assert_eq!(first.candidates, 1, "the retired input and nothing else");
        assert!(store.get(&key(&old)).unwrap().is_some());

        let due = run(&store, 1100, 100, 100_000).unwrap();
        assert_eq!(due.deleted, 1);
        assert!(store.get(&key(&old)).unwrap().is_none());
        assert!(
            store.get(&key(&new)).unwrap().is_some(),
            "the layer the manifest names is what the shard serves"
        );
        assert!(
            store.get("tenants/p/shards/0000/SHARD").unwrap().is_some(),
            "the manifest is not a layer and is never collectible"
        );
    }

    /// A split serves lazily: shard 2 of four reads through shard 0 of
    /// two until it has rewritten past it, so the older era's manifest
    /// is live and so are the layers it names. Collecting by current
    /// shard count alone would take the ground out from under the
    /// descendant.
    #[test]
    fn an_older_era_shard_manifest_still_pins_its_layers() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        write_manifest(&store, "p", &[], None);
        let inherited = put_layer(&store, "p", 0, 0x100, 0x200);
        let own = put_layer(&store, "p", 2, 0x200, 0x400);
        write_shard(&store, "p", 0, &[(&inherited, None)]);
        write_shard(&store, "p", 2, &[(&own, None)]);

        let first = run(&store, 1000, 0, 100_000).unwrap();
        assert_eq!(first.candidates, 0, "every layer is named by some era");
        let second = run(&store, 1000, 0, 100_000).unwrap();
        assert_eq!(second.deleted, 0);
        assert!(
            store
                .get(&format!("tenants/p/shards/0000/{inherited}"))
                .unwrap()
                .is_some()
        );
    }

    /// A branch copies its parent's entries and tags them with the
    /// tenant whose prefix holds the bytes, so the tag is what has to
    /// pin, under the owner and not under the child.
    #[test]
    fn a_branch_pins_the_parent_layers_it_inherited() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let shared = put_layer(&store, "parent", 0, 0x100, 0x200);
        write_manifest(&store, "parent", &[], None);
        write_manifest(&store, "child", &[], Some(("parent", 0x200)));
        // The parent has moved on and no longer names it, the child
        // still reads through it.
        let rewritten = put_layer(&store, "parent", 0, 0x100, 0x400);
        write_shard(&store, "parent", 0, &[(&rewritten, None)]);
        write_shard(&store, "child", 0, &[(&shared, Some("parent"))]);

        run(&store, 1000, 0, 100_000).unwrap();
        let due = run(&store, 1000, 0, 100_000).unwrap();
        assert_eq!(due.deleted, 0, "the child is still reading it");
        assert!(
            store
                .get(&format!("tenants/parent/shards/0000/{shared}"))
                .unwrap()
                .is_some()
        );

        // Once the child stops naming it, the parent's own manifest is
        // the only opinion left and it says gone.
        write_shard(&store, "child", 0, &[]);
        run(&store, 2000, 0, 100_000).unwrap();
        let after = run(&store, 2000, 0, 100_000).unwrap();
        assert_eq!(after.deleted, 1);
        assert!(
            store
                .get(&format!("tenants/parent/shards/0000/{shared}"))
                .unwrap()
                .is_none()
        );
    }

    impl Policy {
        fn dry(self, dry_run: bool) -> Policy {
            Policy { dry_run, ..self }
        }
    }
}
