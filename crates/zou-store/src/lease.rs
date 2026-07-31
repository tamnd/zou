//! The writer lease protocol.
//!
//! Exactly one node may write to a database at a time. The lease lives
//! inside the manifest and is taken, renewed, and released with manifest
//! CAS, so the object store's conditional PUT is the only coordination
//! we depend on. Every acquisition increments the epoch, and WAL is
//! written under epoch directories, which is what makes an expired holder
//! harmless: its uploads land in a dead epoch the live manifest never
//! references.
//!
//! Time is passed in by the caller as unix seconds. That keeps this module
//! deterministic under test and pushes the "how skewed can clocks be"
//! question to one documented place: the TTL must exceed the worst clock
//! skew between nodes plus the longest upload pause, and the default of
//! 15 seconds assumes NTP disciplined hosts.

use crate::cas::{CasError, CasStore, Version};
use crate::layout::TenantLayout;
use crate::manifest::{Lease, Manifest, ManifestError};

/// Default lease TTL in seconds. Renewal should run at a third of this.
pub const DEFAULT_TTL_SECS: u64 = 15;

/// Proof of a successful acquisition. The fence goes into every WAL frame
/// the holder writes, and the version pins the manifest for the next CAS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldLease {
    pub holder: String,
    pub epoch: u64,
    pub fence: u64,
    pub expires_unix: u64,
    manifest: Manifest,
    version: Version,
    /// Second of the last history snapshot this holder wrote, so a busy
    /// publisher skips the extra PUT instead of racing put_new against
    /// its own earlier write within the same second.
    last_history_unix: u64,
}

impl HeldLease {
    /// The manifest as of the last successful swap.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error("no manifest at {key}, the database does not exist")]
    NoManifest { key: String },
    #[error("lease held by {holder} until unix {expires_unix}")]
    Held { holder: String, expires_unix: u64 },
    /// Someone swapped the manifest between our read and our write.
    /// Re-read and decide again.
    #[error("lost a manifest race, re-read and retry")]
    Raced,
    /// We are no longer the holder, a steal happened. The caller must stop
    /// writing immediately.
    #[error("lease lost: manifest now shows {holder} on epoch {epoch}")]
    Lost { holder: String, epoch: u64 },
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    Store(#[from] CasError),
}

/// Try to become the writer for a tenant.
///
/// Succeeds if the lease is absent, expired, or already ours. On success
/// the epoch is incremented and the fence advances, both persisted through
/// CAS before this returns.
pub fn acquire(
    store: &dyn CasStore,
    layout: &TenantLayout,
    holder: &str,
    ttl_secs: u64,
    now_unix: u64,
) -> Result<HeldLease, LeaseError> {
    let key = layout.manifest();
    let Some((data, version)) = store.get(&key)? else {
        return Err(LeaseError::NoManifest { key });
    };
    let mut manifest = Manifest::from_json(&data)?;

    if let Some(lease) = &manifest.lease {
        let expired = lease.expires_unix <= now_unix;
        if !expired && lease.holder != holder {
            return Err(LeaseError::Held {
                holder: lease.holder.clone(),
                expires_unix: lease.expires_unix,
            });
        }
    }

    let fence = manifest.lease.as_ref().map_or(0, |l| l.fence) + 1;
    manifest.epoch += 1;
    manifest.lease = Some(Lease {
        holder: holder.to_string(),
        expires_unix: now_unix + ttl_secs,
        fence,
    });

    let version = swap(store, &key, &manifest, &version)?;
    Ok(HeldLease {
        holder: holder.to_string(),
        epoch: manifest.epoch,
        fence,
        expires_unix: now_unix + ttl_secs,
        manifest,
        version,
        last_history_unix: 0,
    })
}

/// Extend a held lease. Fails with `Lost` if a steal happened, in which
/// case the caller must stop writing immediately.
pub fn renew(
    store: &dyn CasStore,
    layout: &TenantLayout,
    held: &mut HeldLease,
    ttl_secs: u64,
    now_unix: u64,
) -> Result<(), LeaseError> {
    let key = layout.manifest();
    let (mut manifest, version) = reread_checking_ownership(store, &key, held)?;

    let lease = manifest.lease.as_mut().expect("ownership was just checked");
    lease.expires_unix = now_unix + ttl_secs;

    held.version = swap(store, &key, &manifest, &version)?;
    held.expires_unix = now_unix + ttl_secs;
    held.manifest = manifest;
    Ok(())
}

/// Graceful detach: clear the lease so the next writer does not wait out
/// the TTL. The epoch stays, it only moves on acquisition.
pub fn release(
    store: &dyn CasStore,
    layout: &TenantLayout,
    held: HeldLease,
) -> Result<(), LeaseError> {
    let key = layout.manifest();
    let (mut manifest, version) = reread_checking_ownership(store, &key, &held)?;
    manifest.lease = None;
    swap(store, &key, &manifest, &version)?;
    Ok(())
}

/// Mutate the manifest while holding the lease. Re-reads, verifies we are
/// still the holder, applies `mutate`, and swaps. This is how the writer
/// publishes wal_tail and checkpoint updates: every such write doubles as
/// an ownership check, so a stolen lease surfaces as `Lost` here instead
/// of silently corrupting the manifest.
///
/// A swap that changed anything besides the lease also lands a history
/// snapshot under `manifests/`, at most one per second, which is the
/// trail PITR materializes branches from. The snapshot is best effort
/// and written after the swap: the manifest is already current, so a
/// failed history PUT costs a snapshot of granularity, never state.
pub fn update_manifest(
    store: &dyn CasStore,
    layout: &TenantLayout,
    held: &mut HeldLease,
    now_unix: u64,
    mutate: impl FnOnce(&mut Manifest),
) -> Result<(), LeaseError> {
    let key = layout.manifest();
    let (mut manifest, version) = reread_checking_ownership(store, &key, held)?;
    let before = manifest.clone();
    mutate(&mut manifest);
    let changed = {
        let strip = |m: &Manifest| {
            let mut m = m.clone();
            m.lease = None;
            m.published_unix = None;
            m
        };
        strip(&before) != strip(&manifest)
    };
    if changed {
        manifest.published_unix = Some(now_unix);
    }
    held.version = swap(store, &key, &manifest, &version)?;
    if changed && held.last_history_unix != now_unix {
        let mut snapshot = manifest.clone();
        snapshot.lease = None;
        let history = layout.manifest_history(manifest.epoch, now_unix);
        match store.put_new(&history, &snapshot.to_json()) {
            Ok(_) | Err(CasError::AlreadyExists { .. }) => held.last_history_unix = now_unix,
            Err(e) => {
                eprintln!("zou: history snapshot {history} failed, pitr loses this second: {e}")
            }
        }
    }
    held.manifest = manifest;
    Ok(())
}

fn reread_checking_ownership(
    store: &dyn CasStore,
    key: &str,
    held: &HeldLease,
) -> Result<(Manifest, Version), LeaseError> {
    let Some((data, version)) = store.get(key)? else {
        return Err(LeaseError::NoManifest {
            key: key.to_string(),
        });
    };
    let manifest = Manifest::from_json(&data)?;
    let ours = manifest
        .lease
        .as_ref()
        .is_some_and(|l| l.holder == held.holder && l.fence == held.fence)
        && manifest.epoch == held.epoch;
    if !ours {
        return Err(LeaseError::Lost {
            holder: manifest
                .lease
                .as_ref()
                .map_or_else(String::new, |l| l.holder.clone()),
            epoch: manifest.epoch,
        });
    }
    Ok((manifest, version))
}

fn swap(
    store: &dyn CasStore,
    key: &str,
    manifest: &Manifest,
    expected: &Version,
) -> Result<Version, LeaseError> {
    match store.put_if_match(key, &manifest.to_json(), Some(expected)) {
        Ok(v) => Ok(v),
        Err(CasError::Conflict { .. }) => Err(LeaseError::Raced),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::cas::LocalFsStore;

    fn setup() -> (tempfile::TempDir, LocalFsStore, TenantLayout) {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let layout = TenantLayout::new("t1");
        store
            .put_new(&layout.manifest(), &Manifest::new("t1", 18).to_json())
            .unwrap();
        (dir, store, layout)
    }

    #[test]
    fn acquire_bumps_epoch_and_fence() {
        let (_d, store, layout) = setup();
        let held = acquire(&store, &layout, "node-a", 15, 1000).unwrap();
        assert_eq!((held.epoch, held.fence, held.expires_unix), (1, 1, 1015));

        let (data, _) = store.get(&layout.manifest()).unwrap().unwrap();
        let m = Manifest::from_json(&data).unwrap();
        assert_eq!(m.epoch, 1);
        assert_eq!(m.lease.unwrap().holder, "node-a");
    }

    #[test]
    fn a_live_lease_blocks_other_holders() {
        let (_d, store, layout) = setup();
        acquire(&store, &layout, "node-a", 15, 1000).unwrap();
        let err = acquire(&store, &layout, "node-b", 15, 1010).unwrap_err();
        assert!(
            matches!(err, LeaseError::Held { ref holder, expires_unix: 1015 } if holder == "node-a")
        );
    }

    #[test]
    fn an_expired_lease_can_be_stolen_and_epochs_never_repeat() {
        let (_d, store, layout) = setup();
        let a = acquire(&store, &layout, "node-a", 15, 1000).unwrap();
        let b = acquire(&store, &layout, "node-b", 15, 1015).unwrap();
        assert_eq!((a.epoch, a.fence), (1, 1));
        assert_eq!((b.epoch, b.fence), (2, 2));
    }

    #[test]
    fn reacquiring_our_own_lease_is_allowed() {
        let (_d, store, layout) = setup();
        let first = acquire(&store, &layout, "node-a", 15, 1000).unwrap();
        let again = acquire(&store, &layout, "node-a", 15, 1005).unwrap();
        assert_eq!(again.epoch, first.epoch + 1);
        assert_eq!(again.expires_unix, 1020);
    }

    #[test]
    fn renew_extends_and_a_stolen_lease_fails_renewal() {
        let (_d, store, layout) = setup();
        let mut a = acquire(&store, &layout, "node-a", 15, 1000).unwrap();
        renew(&store, &layout, &mut a, 15, 1005).unwrap();
        assert_eq!(a.expires_unix, 1020);

        // node-a goes quiet past the TTL and node-b steals.
        acquire(&store, &layout, "node-b", 15, 1020).unwrap();
        let err = renew(&store, &layout, &mut a, 15, 1021).unwrap_err();
        assert!(matches!(err, LeaseError::Lost { ref holder, epoch: 2 } if holder == "node-b"));
    }

    #[test]
    fn release_clears_the_lease_without_touching_the_epoch() {
        let (_d, store, layout) = setup();
        let held = acquire(&store, &layout, "node-a", 15, 1000).unwrap();
        release(&store, &layout, held).unwrap();

        let (data, _) = store.get(&layout.manifest()).unwrap().unwrap();
        let m = Manifest::from_json(&data).unwrap();
        assert!(m.lease.is_none());
        assert_eq!(m.epoch, 1);

        // The next writer gets in immediately, no TTL wait.
        let b = acquire(&store, &layout, "node-b", 15, 1001).unwrap();
        assert_eq!(b.epoch, 2);
    }

    #[test]
    fn state_changes_leave_history_snapshots_and_lease_churn_does_not() {
        use crate::lsn::Lsn;
        use crate::manifest::{CheckpointKind, CheckpointRef};
        let chk = |id: &str| CheckpointRef {
            id: id.into(),
            lsn: Lsn(0x100),
            kind: CheckpointKind::Full,
            owner: None,
        };
        let (_d, store, layout) = setup();
        let mut held = acquire(&store, &layout, "node-a", 15, 1000).unwrap();

        // A swap that changes nothing but the lease is not history.
        update_manifest(&store, &layout, &mut held, 1001, |_| {}).unwrap();
        assert!(store.list(&layout.manifests_dir()).unwrap().is_empty());

        // A checkpoint publish is, with the lease stripped from the copy.
        update_manifest(&store, &layout, &mut held, 1002, |m| {
            m.checkpoints.push(chk("aaa"));
        })
        .unwrap();
        let keys = store.list(&layout.manifests_dir()).unwrap();
        assert_eq!(keys, vec![layout.manifest_history(1, 1002)]);
        let (data, _) = store.get(&keys[0]).unwrap().unwrap();
        let snap = Manifest::from_json(&data).unwrap();
        assert!(snap.lease.is_none(), "history copies carry no lease");
        assert_eq!(snap.published_unix, Some(1002));
        assert_eq!(snap.checkpoints.len(), 1);

        // Publishes within one second collapse into one snapshot, the
        // next second gets its own.
        update_manifest(&store, &layout, &mut held, 1002, |m| {
            m.checkpoints.push(chk("bbb"));
        })
        .unwrap();
        assert_eq!(store.list(&layout.manifests_dir()).unwrap().len(), 1);
        update_manifest(&store, &layout, &mut held, 1003, |m| {
            m.checkpoints.push(chk("ccc"));
        })
        .unwrap();
        assert_eq!(store.list(&layout.manifests_dir()).unwrap().len(), 2);
    }

    #[test]
    fn missing_manifest_is_an_explicit_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let layout = TenantLayout::new("ghost");
        let err = acquire(&store, &layout, "node-a", 15, 1000).unwrap_err();
        assert!(matches!(err, LeaseError::NoManifest { .. }));
    }

    /// The safety property: however many nodes race, exactly one holds the
    /// lease per round, and epochs strictly increase with no duplicates.
    #[test]
    fn contended_acquisition_elects_exactly_one_holder_per_round() {
        const NODES: usize = 8;
        const ROUNDS: u64 = 10;
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<LocalFsStore> = Arc::new(LocalFsStore::new(dir.path()));
        let layout = TenantLayout::new("t1");
        store
            .put_new(&layout.manifest(), &Manifest::new("t1", 18).to_json())
            .unwrap();

        let mut all_epochs = Vec::new();
        for round in 0..ROUNDS {
            // Time jumps past the TTL each round, so the previous holder is
            // always expired and every node has a fair shot at stealing.
            let now = 1000 + round * 100;
            let winners: Vec<HeldLease> = std::thread::scope(|s| {
                let handles: Vec<_> = (0..NODES)
                    .map(|n| {
                        let store = Arc::clone(&store);
                        let layout = layout.clone();
                        s.spawn(move || {
                            loop {
                                match acquire(&*store, &layout, &format!("node-{n}"), 15, now) {
                                    Ok(held) => break Some(held),
                                    Err(LeaseError::Raced) => continue,
                                    Err(LeaseError::Held { .. }) => break None,
                                    Err(e) => panic!("unexpected: {e}"),
                                }
                            }
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .filter_map(|h| h.join().unwrap())
                    .collect()
            });

            assert_eq!(
                winners.len(),
                1,
                "round {round} elected {} holders",
                winners.len()
            );
            all_epochs.push(winners[0].epoch);
        }

        let mut sorted = all_epochs.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            all_epochs.len(),
            "epochs repeated: {all_epochs:?}"
        );
        assert!(
            all_epochs.windows(2).all(|w| w[0] < w[1]),
            "epochs not increasing: {all_epochs:?}"
        );
    }
}
