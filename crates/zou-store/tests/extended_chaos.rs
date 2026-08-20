//! The four fault classes that are not crashes.
//!
//! `lease_chaos.rs` kills operations on both sides of the commit point
//! and `s3_chaos.rs` runs the CAS contract through a proxy that injects
//! a fault every Nth request. Between them they cover the case where a
//! node dies and the case where the store hiccups. Neither covers a
//! store that is up and answering wrongly, or a node that is up and
//! wrong about the time, and those are the failures a fleet actually
//! spends its outages on.
//!
//! Four rows, four different questions.
//!
//! - **Clocks that disagree.** A clock that runs behind is safe by
//!   construction: safety never rested on the TTL, the epoch bump
//!   fences a stale holder and its uploads land in an epoch nothing
//!   references. A clock that runs ahead is the expensive direction,
//!   because the expiry it writes is read by everyone.
//! - **Lists that lag.** A list may not reflect a write that landed a
//!   moment ago. Anything that decides from a listing has to be safe
//!   when the listing is short.
//! - **Throttling storms.** Not a fault every Nth request, which a
//!   bounded retry absorbs by construction, but every request for a
//!   window, which outlasts the budget.
//! - **Partial partitions.** The asymmetric shape: a node that can
//!   still read the store but can no longer write to it.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use zou_store::layer::LayerKey;
use zou_store::layermap::LayerDesc;
use zou_store::layout::TenantLayout;
use zou_store::lsn::Lsn;
use zou_store::manifest::{CheckpointKind, CheckpointRef};
use zou_store::shardmanifest::{LayerEntry, PageShardManifest};
use zou_store::{CasError, CasStore, GuardedStore, LocalFsStore, Manifest, Version, branch, lease};

const TTL: u64 = 15;

/// Deterministic per-seed RNG, the same xorshift the rest of the crate
/// uses, so a failing run is a failing seed and not a mood.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// What a node believes the time is. The true clock is shared, each
/// node reads it through its own offset, and nothing in the protocol
/// ever sees the true one.
#[derive(Clone)]
struct Watch {
    truth: Arc<AtomicU64>,
    skew_secs: i64,
}

impl Watch {
    fn now(&self) -> u64 {
        self.truth
            .load(Ordering::Relaxed)
            .saturating_add_signed(self.skew_secs)
    }
}

fn tenant(store: &dyn CasStore, tenant_ref: &str) -> TenantLayout {
    let layout = TenantLayout::new(tenant_ref);
    store
        .put_if_absent(&layout.manifest(), &Manifest::new(tenant_ref, 18).to_json())
        .expect("bootstrap");
    layout
}

fn fs() -> (tempfile::TempDir, GuardedStore<LocalFsStore>) {
    let dir = tempfile::tempdir().unwrap();
    let store = GuardedStore::new(LocalFsStore::new(dir.path()));
    (dir, store)
}

// ---------------------------------------------------------------- clocks

/// Contenders whose clocks disagree by more than the TTL in both
/// directions, contending for the same lease at the same time.
///
/// The invariants are the ones `lease_chaos.rs` checks under crashes,
/// asserted here under time instead: an epoch belongs to at most one
/// holder ever, and every segment a holder acknowledged sits under the
/// epoch directory that holder owned. Neither one may depend on any
/// node agreeing with any other about what time it is.
#[test]
fn contenders_whose_clocks_disagree_never_share_an_epoch() {
    // Two behind and two ahead, each by more than the TTL, which is
    // exactly the configuration the lease module says it does not
    // support and must still be safe under.
    const SKEWS: [i64; 4] = [-40, -3, 3, 40];

    for seed in 1..=8u64 {
        let (_dir, store) = fs();
        let store = Arc::new(store);
        let layout = tenant(&*store, "skew");
        let truth = Arc::new(AtomicU64::new(1_000_000));

        // epoch -> holder. An insert that finds something there is two
        // nodes believing they own the same epoch, which is the one
        // thing the protocol may never allow.
        let epochs: Arc<Mutex<HashMap<u64, String>>> = Arc::new(Mutex::new(HashMap::new()));
        let segments: Arc<Mutex<HashMap<String, u64>>> = Arc::new(Mutex::new(HashMap::new()));
        // Skew has to be observed, not just configured, or a passing
        // run proves only that nothing contended.
        let skew_seen = Arc::new(AtomicBool::new(false));

        let stop = Arc::new(AtomicBool::new(false));
        let ticker = {
            let truth = Arc::clone(&truth);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    truth.fetch_add(1, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_millis(1));
                }
            })
        };

        let threads: Vec<_> = SKEWS
            .iter()
            .enumerate()
            .map(|(node, skew)| {
                let watch = Watch {
                    truth: Arc::clone(&truth),
                    skew_secs: *skew,
                };
                let store = Arc::clone(&store);
                let layout = layout.clone();
                let epochs = Arc::clone(&epochs);
                let segments = Arc::clone(&segments);
                let skew_seen = Arc::clone(&skew_seen);
                let mut rng = Rng::new(seed.wrapping_mul(node as u64 + 7));
                std::thread::spawn(move || {
                    for generation in 0..10u64 {
                        let holder = format!("node-{node}-gen-{generation}");
                        let held = lease::acquire(&*store, &layout, &holder, TTL, watch.now());
                        let mut held = match held {
                            Ok(held) => held,
                            Err(e) => {
                                if matches!(e, lease::LeaseError::Skew { .. }) {
                                    skew_seen.store(true, Ordering::Relaxed);
                                }
                                std::thread::yield_now();
                                continue;
                            }
                        };
                        {
                            let mut epochs = epochs.lock().unwrap();
                            let stale = epochs.insert(held.epoch, holder.clone());
                            assert!(
                                stale.is_none(),
                                "seed {seed}: epoch {} taken by both {stale:?} and {holder}",
                                held.epoch
                            );
                        }
                        for stint in 0..3u64 {
                            let key = format!(
                                "tenants/skew/wal/{:016}/{:016X}.wal",
                                held.epoch,
                                generation * 10 + stint + 1
                            );
                            if store.put_if_absent(&key, holder.as_bytes()).is_ok() {
                                segments.lock().unwrap().insert(key, held.epoch);
                            }
                            // A pause of a length only this node knows,
                            // so renewals land at unrelated moments on
                            // four unrelated clocks.
                            std::thread::sleep(Duration::from_millis(rng.next() % 4));
                            if lease::renew(&*store, &layout, &mut held, TTL, watch.now()).is_err()
                            {
                                break;
                            }
                        }
                        let _ = lease::release(&*store, &layout, held);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        ticker.join().unwrap();

        assert!(
            skew_seen.load(Ordering::Relaxed),
            "seed {seed}: no node ever read a lease from the fast clock, the run proves nothing"
        );

        let (data, _) = store.get(&layout.manifest()).unwrap().expect("manifest");
        let manifest = Manifest::from_json(&data).unwrap();
        let epochs = epochs.lock().unwrap();
        let highest = epochs.keys().max().copied().unwrap_or(0);
        assert!(
            manifest.epoch >= highest,
            "seed {seed}: final epoch {} below the highest acquired {highest}",
            manifest.epoch
        );

        let listed: HashSet<String> = store
            .list(&format!("{}/wal/", layout.prefix()))
            .unwrap()
            .into_iter()
            .collect();
        for (key, epoch) in segments.lock().unwrap().iter() {
            assert!(
                listed.contains(key),
                "seed {seed}: acked segment {key} gone"
            );
            assert!(
                key.starts_with(&format!("tenants/skew/wal/{epoch:016}/")),
                "seed {seed}: segment {key} outside its own epoch"
            );
            let holder = &epochs[epoch];
            let (content, _) = store.get(key).unwrap().unwrap();
            assert_eq!(
                &content,
                holder.as_bytes(),
                "seed {seed}: {key} written by somebody other than {holder}"
            );
        }
    }
}

/// A lease that runs out further ahead than any correct clock could
/// have put it is named as skew, with the number, rather than waited
/// on.
///
/// The cost of getting this wrong is not a wrong answer, it is silence:
/// the lease is honoured, `takeover` comes straight back with `Held`
/// because the expiry is past its deadline, and the tenant simply stops
/// failing over for as long as the skew lasts with nothing in any log
/// pointing at a clock.
#[test]
fn a_lease_from_a_clock_far_ahead_is_named_as_skew_rather_than_waited_out() {
    let (_dir, store) = fs();
    let layout = tenant(&store, "fast");
    let now = 1_000_000;
    // An hour ahead, which is what a host that came up before ntp
    // disciplined it looks like.
    let ahead = 3_600;
    lease::acquire(&store, &layout, "fast-node", TTL, now + ahead).expect("the fast node takes it");

    let err = lease::acquire(&store, &layout, "correct-node", TTL, now).expect_err("held");
    let text = err.to_string();
    match err {
        lease::LeaseError::Skew {
            holder,
            ttl_secs,
            ahead_secs,
            ..
        } => {
            assert_eq!(holder, "fast-node");
            assert_eq!(ttl_secs, TTL);
            // Past the furthest a correct clock could have reached,
            // which is our now plus the ttl plus the grace.
            assert_eq!(ahead_secs, ahead - zou_store::lease::SKEW_GRACE_SECS);
        }
        other => panic!("wanted skew, got {other}"),
    }
    assert!(
        text.contains("clocks is wrong"),
        "the message has to point somewhere: {text}"
    );

    // And a takeover comes back with it without sleeping. The sleep
    // panics, so a run that reached one fails here rather than in an
    // hour.
    let waiting = lease::Waiting {
        now: &|| now,
        sleep: &|d| panic!("slept {d:?} on a lease no correct clock wrote"),
    };
    let err = lease::takeover_with(
        &store,
        &layout,
        "correct-node",
        None,
        TTL,
        Duration::from_secs(30),
        &waiting,
    )
    .expect_err("held");
    assert!(matches!(err, lease::LeaseError::Skew { .. }), "{err}");

    // A lease inside the bound is an ordinary wait, not skew, or every
    // busy tenant would be reporting broken clocks.
    let (_dir, store) = fs();
    let layout = tenant(&store, "ordinary");
    lease::acquire(&store, &layout, "other-node", TTL, now + 2).expect("takes it");
    let err = lease::acquire(&store, &layout, "correct-node", TTL, now).expect_err("held");
    assert!(matches!(err, lease::LeaseError::Held { .. }), "{err}");
}

// ------------------------------------------------------------ list lag

/// A store whose listings have not caught up: every key written after
/// the lag was armed is invisible to `list` and visible to everything
/// else, which is what a listing that has not converged looks like from
/// the outside.
struct LaggingList<S> {
    inner: S,
    hidden: Mutex<BTreeSet<String>>,
    lagging: AtomicBool,
}

impl<S: CasStore> LaggingList<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            hidden: Mutex::new(BTreeSet::new()),
            lagging: AtomicBool::new(false),
        }
    }

    fn start_lagging(&self) {
        self.lagging.store(true, Ordering::SeqCst);
    }

    fn note(&self, key: &str) {
        if self.lagging.load(Ordering::SeqCst) {
            self.hidden.lock().unwrap().insert(key.to_string());
        }
    }
}

impl<S: CasStore> CasStore for LaggingList<S> {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        self.inner.get(key)
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        let v = self.inner.put_if_match(key, data, expected)?;
        self.note(key);
        Ok(v)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        let v = self.inner.put(key, data)?;
        self.note(key);
        Ok(v)
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        self.inner.delete(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        let hidden = self.hidden.lock().unwrap();
        Ok(self
            .inner
            .list(prefix)?
            .into_iter()
            .filter(|k| !hidden.contains(k))
            .collect())
    }
}

fn key(block: u32) -> LayerKey {
    LayerKey::page(1663, 5, 16384, 0, block)
}

fn layers() -> Vec<LayerEntry> {
    vec![LayerEntry {
        name: LayerDesc::image(key(0), key(9), Lsn(0x100)).name(),
        size: 1000,
        owner: None,
        upto: None,
    }]
}

/// A branch reads its parent's shards. The listing is not allowed to be
/// the thing that decides how many there are.
///
/// A short listing here is not a retry or a delay, it is a child tenant
/// published as complete with one of its shards holding no layers at
/// all: reads of every key that hashed to it come back empty, and
/// nothing anywhere reports an error. The tenant manifest carries the
/// count and every other walk in the tree iterates it.
#[test]
fn a_listing_that_has_not_caught_up_cannot_cost_a_branch_a_shard() {
    let dir = tempfile::tempdir().unwrap();
    let store = LaggingList::new(LocalFsStore::new(dir.path()));
    let src = TenantLayout::new("p");

    let mut parent = Manifest::new("p", 18);
    parent.shards = 4;
    parent.format = 3;
    parent.checkpoints = vec![CheckpointRef {
        id: "f1".into(),
        lsn: Lsn(0x100),
        kind: CheckpointKind::Full,
        owner: None,
    }];
    parent.folded_upto = Some(Lsn(0x100));
    store
        .put_if_absent(&src.manifest(), &parent.to_json())
        .unwrap();

    for shard in 0..4u16 {
        let mut m = PageShardManifest::new(shard);
        m.disk_consistent_lsn = Lsn(0x300);
        m.layers = layers();
        store.put(&src.shard_manifest(shard), &m.encode()).unwrap();
    }

    // Everything written from here is invisible to a listing, and the
    // four shards written above go with it: nothing has been listed
    // yet, so a listing that has not converged has not seen any of it.
    store.start_lagging();
    for shard in 0..4u16 {
        let mut m = PageShardManifest::new(shard);
        m.disk_consistent_lsn = Lsn(0x300);
        m.layers = layers();
        store.put(&src.shard_manifest(shard), &m.encode()).unwrap();
    }
    assert!(
        store.list(&src.shards_dir()).unwrap().is_empty(),
        "the listing is the one that is behind, not the store"
    );

    branch(&store, "p", "c", Some(Lsn(0x100)), 5_000).unwrap();

    let dst = TenantLayout::new("c");
    for shard in 0..4u16 {
        let child = PageShardManifest::load(&store, &dst.shard_manifest(shard))
            .unwrap()
            .unwrap_or_else(|| panic!("shard {shard} of the child is missing"))
            .0;
        assert_eq!(
            child.layers.len(),
            1,
            "shard {shard} inherited nothing, the branch is short a shard"
        );
        assert_eq!(child.layers[0].owner.as_deref(), Some("p"));
    }
}

// ------------------------------------------------------- storms and partitions

/// A store that stops answering entirely for a window, or refuses
/// writes while still serving reads. One wrapper for both, because both
/// are the same thing to a caller: an operation that fails at a store
/// that is still there.
struct Broken<S> {
    inner: S,
    /// Every operation fails while this is set.
    storm: AtomicBool,
    /// Only writes fail while this is set.
    read_only: AtomicBool,
    /// Writes that reached the store, so a false error can be told from
    /// a real one.
    writes: AtomicU64,
}

impl<S: CasStore> Broken<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            storm: AtomicBool::new(false),
            read_only: AtomicBool::new(false),
            writes: AtomicU64::new(0),
        }
    }

    fn read(&self) -> Result<(), CasError> {
        if self.storm.load(Ordering::SeqCst) {
            return Err(throttled());
        }
        Ok(())
    }

    fn write(&self) -> Result<(), CasError> {
        self.read()?;
        if self.read_only.load(Ordering::SeqCst) {
            return Err(CasError::Io {
                key: "partition".into(),
                source: std::io::Error::other("no route to the endpoint"),
            });
        }
        self.writes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn throttled() -> CasError {
    CasError::Io {
        key: "storm".into(),
        source: std::io::Error::other("503 SlowDown, retries exhausted"),
    }
}

impl<S: CasStore> CasStore for Broken<S> {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        self.read()?;
        self.inner.get(key)
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        self.write()?;
        self.inner.put_if_match(key, data, expected)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        self.write()?;
        self.inner.put(key, data)
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        self.write()?;
        self.inner.delete(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        self.read()?;
        self.inner.list(prefix)
    }
}

/// A storm that outlasts the retry budget, and outlasts the TTL with
/// it.
///
/// Two things have to hold. Every operation the storm ate reports the
/// failure, because a caller that believes a throttled renewal
/// succeeded is a caller that keeps writing with no lease. And the
/// holder that could not renew through the storm is not the holder
/// afterwards: the node that took over during the outage owns the
/// epoch, and the first thing the old holder does when the storm lifts
/// is find that out.
#[test]
fn a_throttling_storm_that_outlasts_the_retries_takes_the_lease_with_it() {
    let dir = tempfile::tempdir().unwrap();
    let broken = Arc::new(Broken::new(GuardedStore::new(LocalFsStore::new(
        dir.path(),
    ))));
    let clear = LocalFsStore::new(dir.path());
    let layout = tenant(&*broken, "storm");

    let mut held = lease::acquire(&*broken, &layout, "node-a", TTL, 1_000).expect("acquire");
    let before = broken.writes.load(Ordering::SeqCst);

    broken.storm.store(true, Ordering::SeqCst);
    for tick in 0..(TTL * 2) {
        let err = lease::renew(&*broken, &layout, &mut held, TTL, 1_000 + tick)
            .expect_err("a renewal through a storm cannot report success");
        assert!(
            matches!(err, lease::LeaseError::Store(_)),
            "the failure has to say the store, not the lease: {err}"
        );
    }
    assert_eq!(
        broken.writes.load(Ordering::SeqCst),
        before,
        "the storm ate the renewals, none of them reached the store"
    );

    // A standby that is not being throttled waits out the TTL and takes
    // over, which is the whole point of the TTL being shorter than an
    // outage anyone would notice.
    let taken = lease::acquire(&clear, &layout, "node-b", TTL, 1_000 + TTL + 1).expect("takeover");
    assert_eq!(taken.holder, "node-b");
    assert!(taken.epoch > held.epoch);

    // The storm lifts and the old holder learns it in one round trip,
    // by name and by epoch.
    broken.storm.store(false, Ordering::SeqCst);
    let err = lease::renew(&*broken, &layout, &mut held, TTL, 1_000 + TTL + 2)
        .expect_err("the lease is gone");
    match err {
        lease::LeaseError::Lost { holder, epoch } => {
            assert_eq!(holder, "node-b");
            assert_eq!(epoch, taken.epoch);
        }
        other => panic!("wanted lost, got {other}"),
    }
}

/// The asymmetric partition: a node that can still read the store and
/// can no longer write to it.
///
/// This is the one that could produce two writers, because the node is
/// not down, it is not slow, and nothing it reads tells it anything is
/// wrong until somebody else has already taken the lease. What keeps it
/// honest is that every manifest publish re-reads and checks ownership
/// first, so the step down happens on the read side and does not depend
/// on the write ever failing loudly.
#[test]
fn a_node_that_can_read_but_not_write_steps_down() {
    let dir = tempfile::tempdir().unwrap();
    let broken = Arc::new(Broken::new(GuardedStore::new(LocalFsStore::new(
        dir.path(),
    ))));
    let clear = LocalFsStore::new(dir.path());
    let layout = tenant(&*broken, "half");

    let mut held = lease::acquire(&*broken, &layout, "node-a", TTL, 2_000).expect("acquire");
    let cut_off_at = held.epoch;

    broken.read_only.store(true, Ordering::SeqCst);
    // It can still see everything, including that it is the holder, and
    // it still cannot renew.
    assert!(broken.get(&layout.manifest()).unwrap().is_some());
    assert!(lease::renew(&*broken, &layout, &mut held, TTL, 2_001).is_err());

    let taken = lease::acquire(&clear, &layout, "node-b", TTL, 2_000 + TTL + 1).expect("takeover");

    // Writes it made after losing the lease are in its own epoch
    // directory, which the live manifest does not name. That is the
    // property the whole design rests on, so it is asserted rather than
    // assumed.
    broken.read_only.store(false, Ordering::SeqCst);
    let orphan = format!("tenants/half/wal/{cut_off_at:016}/0000000000000001.wal");
    broken
        .put_if_absent(&orphan, b"written after the steal")
        .unwrap();
    assert_ne!(taken.epoch, cut_off_at);

    // And the moment it tries to publish anything it is told, before
    // any write is attempted, that it is not the writer.
    let err = lease::update_manifest(&*broken, &layout, &mut held, 2_100, |m| {
        m.folded_upto = Some(Lsn(0xDEAD));
    })
    .expect_err("no longer ours");
    assert!(
        matches!(err, lease::LeaseError::Lost { ref holder, .. } if holder == "node-b"),
        "{err}"
    );

    let (data, _) = clear.get(&layout.manifest()).unwrap().unwrap();
    let live = Manifest::from_json(&data).unwrap();
    assert_eq!(live.epoch, taken.epoch);
    assert_eq!(live.folded_upto, None, "nothing of the old holder landed");
}
