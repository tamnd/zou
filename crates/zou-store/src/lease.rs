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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::cas::{CasError, CasStore, Version};
use crate::layout::TenantLayout;
use crate::manifest::{Lease, Manifest, ManifestError};

/// Default lease TTL in seconds. Renewal should run at a third of this.
pub const DEFAULT_TTL_SECS: u64 = 15;

/// How far past the arithmetic bound a lease may sit before the
/// disagreement is called skew rather than absorbed. A second of it is
/// clock granularity on both ends, the rest is the round trip between
/// the holder's CAS and this node's read.
pub const SKEW_GRACE_SECS: u64 = 5;

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
    /// publisher skips the extra PUT instead of racing put_if_absent against
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
    /// The lease runs out further ahead than any correct clock could
    /// have put it. Waiting it out is not an option worth taking, so
    /// this says the number instead of sleeping on it.
    #[error(
        "lease held by {holder} until unix {expires_unix}, {ahead_secs}s further out than a {ttl_secs}s lease can reach from this clock, so one of the two clocks is wrong or {holder} runs a longer lease ttl than this node"
    )]
    Skew {
        holder: String,
        expires_unix: u64,
        ttl_secs: u64,
        /// How far past the furthest a correct clock could have placed it.
        ahead_secs: u64,
    },
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
    take(store, layout, holder, None, ttl_secs, now_unix, false)
}

/// The same, publishing an address other nodes can reach this one at.
///
/// A fleet needs it and nothing else does: a node that cannot serve a
/// request itself has to send it to the node that can, and the manifest
/// is the only thing both of them read. An embedded build and a one
/// shot command take the lease without one.
pub fn acquire_at(
    store: &dyn CasStore,
    layout: &TenantLayout,
    holder: &str,
    endpoint: Option<&str>,
    ttl_secs: u64,
    now_unix: u64,
) -> Result<HeldLease, LeaseError> {
    take(store, layout, holder, endpoint, ttl_secs, now_unix, false)
}

/// [`steal`] with an address, for the same reason [`acquire_at`] has
/// one: the node that took over is the node the others must now reach.
pub fn steal_at(
    store: &dyn CasStore,
    layout: &TenantLayout,
    holder: &str,
    endpoint: Option<&str>,
    ttl_secs: u64,
    now_unix: u64,
) -> Result<HeldLease, LeaseError> {
    take(store, layout, holder, endpoint, ttl_secs, now_unix, true)
}

/// Who holds the writer lease for this tenant, as of `now_unix`.
///
/// None means nobody does: either no lease was ever taken or the one
/// that was has expired, and in both cases the next node to ask may
/// take it. The lease is read straight from the manifest rather than
/// from any registry of nodes, so there is no membership service to be
/// wrong and nothing to keep in sync with the truth.
///
/// An expired lease reads as nobody even though its holder may still be
/// alive and about to renew. That is the same window the protocol
/// already has, and it is safe on both sides: the epoch bump fences a
/// writer that lost the race, so the worst case is a request sent to a
/// node that has just stopped being the writer, which answers it with
/// the error the lease protocol already produces.
pub fn holder(
    store: &dyn CasStore,
    layout: &TenantLayout,
    now_unix: u64,
) -> Result<Option<Holder>, LeaseError> {
    let key = layout.manifest();
    let Some((data, _)) = store.get(&key)? else {
        return Err(LeaseError::NoManifest { key });
    };
    let manifest = Manifest::from_json(&data)?;
    let published_unix = manifest.published_unix;
    let epoch = manifest.epoch;
    Ok(manifest.lease.and_then(|lease| {
        (lease.expires_unix > now_unix).then_some(Holder {
            node: lease.holder,
            endpoint: lease.endpoint,
            expires_unix: lease.expires_unix,
            epoch,
            published_unix,
        })
    }))
}

/// Who is writing a tenant right now, for a node that is not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Holder {
    /// The holder's id, which is what a node compares against its own
    /// to learn whether it is the writer.
    pub node: String,
    /// Where to reach it, when it published one.
    pub endpoint: Option<String>,
    pub expires_unix: u64,
    pub epoch: u64,
    /// When the holder last published state, which is the freshest any
    /// reader of this tenant can possibly be.
    pub published_unix: Option<u64>,
}

/// Deliberate failover: become the writer even though the manifest shows
/// a live lease. This is for a caller that knows the holder is dead, a
/// control plane whose heartbeat timed out or an operator staring at a
/// powered off node, when waiting out the TTL would cost availability.
///
/// Safety never rested on the TTL. The epoch bump fences the old holder:
/// its next renewal or manifest publish fails with `Lost`, and its
/// landing uploads sit in an epoch the chain takeover seals off. A wrong
/// call therefore costs a live writer its session, never an acked commit.
pub fn steal(
    store: &dyn CasStore,
    layout: &TenantLayout,
    holder: &str,
    ttl_secs: u64,
    now_unix: u64,
) -> Result<HeldLease, LeaseError> {
    take(store, layout, holder, None, ttl_secs, now_unix, true)
}

/// Wait for a dead holder's lease to run out, then take it.
///
/// This is what a standby does when a node stops answering: not a
/// [`steal`], because a node that cannot be reached from here may be
/// perfectly alive and serving from somewhere else, and a partition is
/// not a death. Waiting out the TTL is the one signal both sides agree
/// on, so failover is a wait rather than a decision, and the recovery
/// time is the remaining TTL plus one CAS plus the attach.
///
/// It gives up after `limit` rather than waiting forever, because a
/// holder that keeps renewing is alive and this node has a caller
/// waiting on an answer. The `Held` error that comes back names the
/// current holder, which is what a fleet node forwards to.
pub fn takeover(
    store: &dyn CasStore,
    layout: &TenantLayout,
    holder: &str,
    endpoint: Option<&str>,
    ttl_secs: u64,
    limit: Duration,
) -> Result<HeldLease, LeaseError> {
    let waiting = Waiting {
        now: &|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is before the unix epoch")
                .as_secs()
        },
        sleep: &std::thread::sleep,
    };
    takeover_with(store, layout, holder, endpoint, ttl_secs, limit, &waiting)
}

/// The clock and the wait, together because they are one decision: a
/// test that moves the clock has to be the thing that sleeps.
pub struct Waiting<'a> {
    pub now: &'a dyn Fn() -> u64,
    pub sleep: &'a dyn Fn(Duration),
}

/// [`takeover`] with time passed in, so a test can prove the waiting
/// without spending it.
pub fn takeover_with(
    store: &dyn CasStore,
    layout: &TenantLayout,
    holder: &str,
    endpoint: Option<&str>,
    ttl_secs: u64,
    limit: Duration,
    waiting: &Waiting<'_>,
) -> Result<HeldLease, LeaseError> {
    let (now, sleep) = (waiting.now, waiting.sleep);
    /// How many lost manifest races a takeover reads through before it
    /// gives up and says so.
    const RACES: u32 = 5;

    let deadline = now() + limit.as_secs();
    let mut races = 0;
    loop {
        match take(store, layout, holder, endpoint, ttl_secs, now(), false) {
            Err(LeaseError::Held {
                holder: whose,
                expires_unix,
            }) => {
                // Past the deadline is the caller's answer: somebody else
                // is the writer and is renewing, so say who rather than
                // keep waiting on a node that is alive.
                if expires_unix >= deadline {
                    return Err(LeaseError::Held {
                        holder: whose,
                        expires_unix,
                    });
                }
                // Until the expiry and not a second longer, because the
                // second it names is already expired: `take` compares
                // with <=, and a clock read truncated to seconds cannot
                // wake this before that second starts. The jitter on top
                // is what keeps a rack of standbys from firing their CAS
                // into the same millisecond, and it is never zero, so a
                // wait that is already over still makes progress.
                let wait = expires_unix.saturating_sub(now());
                sleep(Duration::from_millis(wait * 1000 + spread(holder)));
            }
            // Another node swapped the manifest under us, which during a
            // failover is another standby doing the same thing. Read it
            // again rather than report the race: either that node took
            // the lease, and the next pass comes back with its name,
            // which is what the caller has to forward to, or it published
            // something else and this pass goes through. Bounded, because
            // a tenant with a busy publisher must not spin here.
            Err(e @ LeaseError::Raced) => {
                races += 1;
                if races > RACES {
                    return Err(e);
                }
                sleep(Duration::from_millis(spread(holder)));
            }
            other => return other,
        }
    }
}

/// A tenth of a second or so, depending on who is asking, so two
/// standbys waiting on the same lease do not wake together. No rng,
/// because the only property needed is that different names differ and
/// that nobody waits zero.
fn spread(holder: &str) -> u64 {
    50 + u64::from(holder.bytes().fold(17u8, |acc, b| acc.wrapping_mul(31) ^ b)) % 200
}

fn take(
    store: &dyn CasStore,
    layout: &TenantLayout,
    holder: &str,
    endpoint: Option<&str>,
    ttl_secs: u64,
    now_unix: u64,
    force: bool,
) -> Result<HeldLease, LeaseError> {
    let key = layout.manifest();
    let Some((data, version)) = store.get(&key)? else {
        return Err(LeaseError::NoManifest { key });
    };
    let mut manifest = Manifest::from_json(&data)?;

    if !force && let Some(lease) = &manifest.lease {
        let expired = lease.expires_unix <= now_unix;
        if !expired && lease.holder != holder {
            // The furthest out a correct clock could have put this: the
            // holder wrote `its now + ttl` at the CAS, and that CAS
            // happened before this read, so anything past our own now
            // plus the ttl is somebody's clock being wrong. Only the
            // grace separates a marginal disagreement from a real one.
            let furthest = now_unix + ttl_secs + SKEW_GRACE_SECS;
            if lease.expires_unix > furthest {
                return Err(LeaseError::Skew {
                    holder: lease.holder.clone(),
                    expires_unix: lease.expires_unix,
                    ttl_secs,
                    ahead_secs: lease.expires_unix - furthest,
                });
            }
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
        endpoint: endpoint.map(str::to_string),
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
/// publishes checkpoint and fold cursor updates: every such write doubles
/// as an ownership check, so a stolen lease surfaces as `Lost` here
/// instead of silently corrupting the manifest.
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
        match store.put_if_absent(&history, &snapshot.to_json()) {
            Ok(_) | Err(CasError::AlreadyExists { .. }) => held.last_history_unix = now_unix,
            Err(e) => {
                log::warn!("history snapshot {history} failed, pitr loses this second: {e}")
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
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::cas::LocalFsStore;

    fn setup() -> (tempfile::TempDir, LocalFsStore, TenantLayout) {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let layout = TenantLayout::new("t1");
        store
            .put_if_absent(&layout.manifest(), &Manifest::new("t1", 18).to_json())
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
    fn the_holder_publishes_where_to_reach_it_and_renewal_keeps_it() {
        // The whole of holder discovery: a node that is not the writer
        // reads the manifest it already reads and finds an address.
        let (_d, store, layout) = setup();
        let mut held = acquire_at(
            &store,
            &layout,
            "node-a",
            Some("http://10.0.0.4:8000"),
            15,
            1000,
        )
        .unwrap();
        let found = holder(&store, &layout, 1001).unwrap().expect("a holder");
        assert_eq!(found.node, "node-a");
        assert_eq!(found.endpoint.as_deref(), Some("http://10.0.0.4:8000"));
        assert_eq!(found.expires_unix, 1015);

        renew(&store, &layout, &mut held, 15, 1005).unwrap();
        let found = holder(&store, &layout, 1006).unwrap().expect("a holder");
        assert_eq!(
            found.endpoint.as_deref(),
            Some("http://10.0.0.4:8000"),
            "a renewal is not a move"
        );
    }

    #[test]
    fn an_expired_lease_reads_as_nobody_holding_it() {
        let (_d, store, layout) = setup();
        acquire_at(
            &store,
            &layout,
            "node-a",
            Some("http://10.0.0.4:8000"),
            15,
            1000,
        )
        .unwrap();
        assert!(holder(&store, &layout, 1014).unwrap().is_some());
        assert!(
            holder(&store, &layout, 1015).unwrap().is_none(),
            "expiry is when the next node may take it, so it is nobody's now"
        );
    }

    #[test]
    fn a_lease_without_an_address_says_so_rather_than_guessing() {
        // Single node, embedded and every one shot command. There is
        // nowhere to forward to and a caller has to be told that.
        let (_d, store, layout) = setup();
        acquire(&store, &layout, "node-a", 15, 1000).unwrap();
        let found = holder(&store, &layout, 1001).unwrap().expect("a holder");
        assert_eq!(found.node, "node-a");
        assert!(found.endpoint.is_none());
    }

    #[test]
    fn nobody_holds_a_lease_that_was_never_taken() {
        let (_d, store, layout) = setup();
        assert!(holder(&store, &layout, 1000).unwrap().is_none());
    }

    /// A clock a test moves by however long the code decided to sleep,
    /// so waiting out a TTL costs no wall clock at all.
    fn stopped(at: u64) -> (Arc<Mutex<u64>>, impl Fn() -> u64, impl Fn(Duration)) {
        let now = Arc::new(Mutex::new(at));
        let read = Arc::clone(&now);
        let ticked = Arc::clone(&now);
        (
            now,
            move || *read.lock().unwrap(),
            move |d: Duration| {
                *ticked.lock().unwrap() += d.as_millis().div_ceil(1000) as u64;
            },
        )
    }

    #[test]
    fn a_standby_waits_out_a_dead_holders_lease_and_takes_it() {
        let (_d, store, layout) = setup();
        acquire(&store, &layout, "node-a", 15, 1000).unwrap();
        // node-a stops renewing here and never comes back.
        let (clock, now, sleep) = stopped(1002);
        let held = takeover_with(
            &store,
            &layout,
            "node-b",
            Some("http://10.0.0.5:8000"),
            15,
            Duration::from_secs(60),
            &Waiting {
                now: &now,
                sleep: &sleep,
            },
        )
        .expect("the lease runs out and the standby takes it");
        assert_eq!(held.holder, "node-b");
        assert_eq!(held.epoch, 2, "a takeover is an acquisition like any other");
        assert!(
            *clock.lock().unwrap() >= 1015,
            "it waited for the expiry rather than stealing"
        );
        assert_eq!(
            holder(&store, &layout, 1020).unwrap().unwrap().endpoint,
            Some("http://10.0.0.5:8000".to_string()),
            "and published where it now answers"
        );
    }

    #[test]
    fn a_holder_that_is_still_renewing_is_not_waited_on() {
        // Which is the difference between a failover and an outage: this
        // node has a caller waiting, and the answer is who to ask.
        let (_d, store, layout) = setup();
        acquire(&store, &layout, "node-a", 15, 1000).unwrap();
        let (clock, now, sleep) = stopped(1001);
        let err = takeover_with(
            &store,
            &layout,
            "node-b",
            None,
            15,
            Duration::from_secs(5),
            &Waiting {
                now: &now,
                sleep: &sleep,
            },
        )
        .unwrap_err();
        assert!(matches!(err, LeaseError::Held { ref holder, .. } if holder == "node-a"));
        assert_eq!(
            *clock.lock().unwrap(),
            1001,
            "and it did not wait to say so"
        );
    }

    #[test]
    fn a_takeover_of_a_lease_already_gone_costs_nothing() {
        // The clean shutdown path: detach cleared the lease, so the next
        // node in is the next node to ask.
        let (_d, store, layout) = setup();
        let held = acquire(&store, &layout, "node-a", 15, 1000).unwrap();
        release(&store, &layout, held).unwrap();
        let (clock, now, sleep) = stopped(1001);
        let held = takeover_with(
            &store,
            &layout,
            "node-b",
            None,
            15,
            Duration::from_secs(60),
            &Waiting {
                now: &now,
                sleep: &sleep,
            },
        )
        .expect("nobody holds it");
        assert_eq!(held.holder, "node-b");
        assert_eq!(*clock.lock().unwrap(), 1001, "no wait at all");
    }

    #[test]
    fn a_lease_renewed_while_a_standby_waits_is_left_alone() {
        let (_d, store, layout) = setup();
        let a = Mutex::new(acquire(&store, &layout, "node-a", 15, 1000).unwrap());
        let (clock, now, sleep) = stopped(1002);
        // node-a wakes up on the way and renews, which the standby sees
        // as a new expiry it will not outlast inside its limit.
        let renewed = Arc::clone(&clock);
        let store_ref = &store;
        let layout_ref = &layout;
        let sleep = move |d: Duration| {
            sleep(d);
            let at = *renewed.lock().unwrap();
            let _ = renew(store_ref, layout_ref, &mut a.lock().unwrap(), 15, at);
        };
        let err = takeover_with(
            &store,
            &layout,
            "node-b",
            None,
            15,
            Duration::from_secs(20),
            &Waiting {
                now: &now,
                sleep: &sleep,
            },
        )
        .unwrap_err();
        assert!(matches!(err, LeaseError::Held { ref holder, .. } if holder == "node-a"));
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
    fn steal_takes_a_live_lease_and_fences_the_holder() {
        let (_d, store, layout) = setup();
        let mut a = acquire(&store, &layout, "node-a", 15, 1000).unwrap();

        // node-b knows node-a is dead and does not wait for 1015.
        let b = steal(&store, &layout, "node-b", 15, 1005).unwrap();
        assert_eq!((b.epoch, b.fence, b.expires_unix), (2, 2, 1020));

        // If node-a was alive after all, its next renewal fences it.
        let err = renew(&store, &layout, &mut a, 15, 1006).unwrap_err();
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
            .put_if_absent(&layout.manifest(), &Manifest::new("t1", 18).to_json())
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
