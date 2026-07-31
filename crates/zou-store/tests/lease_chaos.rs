//! Chaos test for the lease protocol: contenders die at random points,
//! including mid CAS, and the safety invariants must hold anyway.
//!
//! Death is modeled at the store boundary. A killed operation either
//! never reaches the store or lands and then reports failure, which is
//! exactly what a process crash between issuing a request and seeing the
//! response looks like. The caller cannot tell the two apart, so the
//! protocol has to be safe under both.
//!
//! Invariants checked across every seed:
//!
//! - An epoch is acquired by at most one holder, ever. This is the whole
//!   point of CAS on the manifest: two contenders can never both think
//!   they won the same epoch, even when one of them died mid swap.
//! - Every WAL segment a holder successfully wrote sits under an epoch
//!   directory that holder owned.
//! - Segments are never overwritten, acknowledged writes survive to the
//!   end of the run.
//! - The final manifest is parseable and its epoch is at least the
//!   highest epoch any surviving acquire observed. It can be higher,
//!   because an acquire whose CAS landed just before the killed response
//!   still moved the epoch.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use zou_store::layout::TenantLayout;
use zou_store::lease;
use zou_store::{CasError, CasStore, GuardedStore, LocalFsStore, Lsn, Manifest, Version};

/// Deterministic per-seed RNG, same xorshift used elsewhere in the crate.
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

/// Wraps a real store and kills a fraction of operations. A kill either
/// drops the request before it applies or applies it and then reports an
/// I/O error, chosen by coin flip, so callers experience crashes on both
/// sides of the commit point.
struct ChaosStore {
    inner: GuardedStore<LocalFsStore>,
    rng: Mutex<Rng>,
    /// One in this many operations dies. Zero disables chaos.
    kill_one_in: u64,
}

impl ChaosStore {
    /// Returns true when the underlying operation should still run
    /// (crash after apply), or errors out before it (crash before).
    fn roll(&self) -> Result<bool, CasError> {
        let mut rng = self.rng.lock().unwrap();
        if self.kill_one_in == 0 || !rng.next().is_multiple_of(self.kill_one_in) {
            return Ok(true);
        }
        if rng.next().is_multiple_of(2) {
            return Err(killed());
        }
        Ok(false)
    }
}

fn killed() -> CasError {
    CasError::Io {
        key: "chaos".into(),
        source: std::io::Error::other("killed by chaos"),
    }
}

impl CasStore for ChaosStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        // Reads have no commit point, a killed read just never returns.
        self.roll()?;
        self.inner.get(key)
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        let apply_first = self.roll()?;
        let result = self.inner.put_if_match(key, data, expected);
        if apply_first {
            result
        } else {
            // The write landed (or legitimately conflicted), but the
            // caller crashes before learning which. Either way it must
            // treat the operation as failed.
            Err(killed())
        }
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        let apply_first = self.roll()?;
        let result = self.inner.put(key, data);
        if apply_first { result } else { Err(killed()) }
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        let apply_first = self.roll()?;
        let result = self.inner.delete(key);
        if apply_first { result } else { Err(killed()) }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        self.roll()?;
        self.inner.list(prefix)
    }
}

struct Harness {
    store: Arc<ChaosStore>,
    layout: TenantLayout,
    /// Fake unix seconds, advanced fast by a timekeeper thread so lease
    /// expiry takes milliseconds of real time instead of TTL seconds.
    clock: Arc<AtomicU64>,
    /// epoch -> holder that acquired it. Insertion must always be fresh.
    epochs: Mutex<HashMap<u64, String>>,
    /// Segment keys this run acknowledged as written, with their epoch.
    segments: Mutex<HashMap<String, u64>>,
}

const TTL: u64 = 15;

fn contender(h: &Arc<Harness>, node: usize, generations: usize) {
    for generation in 0..generations {
        let holder = format!("node-{node}-gen-{generation}");
        let now = h.clock.load(Ordering::Relaxed);
        let Ok(mut held) = lease::acquire(&*h.store, &h.layout, &holder, TTL, now) else {
            // Held by someone else, or we died somewhere in the CAS.
            // Either way this incarnation is gone, the next generation
            // contends fresh.
            std::thread::yield_now();
            continue;
        };

        {
            let mut epochs = h.epochs.lock().unwrap();
            let stale = epochs.insert(held.epoch, holder.clone());
            assert!(
                stale.is_none(),
                "epoch {} acquired by both {:?} and {holder}",
                held.epoch,
                stale
            );
        }

        // Do a stint of writing: a WAL segment under our epoch dir, with
        // renewals in between, dying whenever the store kills us.
        for stint in 0..3u64 {
            let key = h
                .layout
                .wal_segment(held.epoch, Lsn(generation as u64 * 10 + stint + 1));
            if h.store.put_new(&key, holder.as_bytes()).is_ok() {
                h.segments.lock().unwrap().insert(key, held.epoch);
            }
            let now = h.clock.load(Ordering::Relaxed);
            if lease::renew(&*h.store, &h.layout, &mut held, TTL, now).is_err() {
                break;
            }
        }
        let _ = lease::release(&*h.store, &h.layout, held);
    }
}

fn run_seed(seed: u64) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChaosStore {
        inner: GuardedStore::new(LocalFsStore::new(dir.path())),
        rng: Mutex::new(Rng::new(seed)),
        kill_one_in: 8,
    });
    let layout = TenantLayout::new("chaos");
    store
        .inner
        .put_if_match(
            &layout.manifest(),
            &Manifest::new("chaos", 18).to_json(),
            None,
        )
        .unwrap();

    let harness = Arc::new(Harness {
        store,
        layout,
        clock: Arc::new(AtomicU64::new(1_000)),
        epochs: Mutex::new(HashMap::new()),
        segments: Mutex::new(HashMap::new()),
    });

    // Timekeeper: one fake second per real millisecond, so a 15 second
    // TTL expires in 15 ms and steals genuinely happen mid run.
    let stop = Arc::new(AtomicBool::new(false));
    let ticker = {
        let clock = Arc::clone(&harness.clock);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                clock.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        })
    };

    let threads: Vec<_> = (0..4)
        .map(|node| {
            let h = Arc::clone(&harness);
            std::thread::spawn(move || contender(&h, node, 12))
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    ticker.join().unwrap();

    // Final state, read through the real store so chaos cannot hide it.
    let (data, _) = harness
        .store
        .inner
        .get(&harness.layout.manifest())
        .unwrap()
        .expect("manifest survives");
    let manifest = Manifest::from_json(&data).unwrap();

    let epochs = harness.epochs.lock().unwrap();
    let max_seen = epochs.keys().max().copied().unwrap_or(0);
    assert!(
        manifest.epoch >= max_seen,
        "seed {seed}: final epoch {} below acquired epoch {max_seen}",
        manifest.epoch
    );

    // Every acknowledged segment still exists with its original content,
    // and lives under the epoch dir of the holder that wrote it.
    let listed: HashSet<String> = harness
        .store
        .inner
        .list(&format!("{}/wal/", harness.layout.prefix()))
        .unwrap()
        .into_iter()
        .collect();
    let segments = harness.segments.lock().unwrap();
    for (key, epoch) in segments.iter() {
        assert!(
            listed.contains(key),
            "seed {seed}: acked segment {key} vanished"
        );
        assert!(
            key.starts_with(&harness.layout.wal_epoch_dir(*epoch)),
            "seed {seed}: segment {key} outside its epoch dir"
        );
        let holder = &epochs[epoch];
        let (content, _) = harness.store.inner.get(key).unwrap().unwrap();
        assert_eq!(
            &content,
            holder.as_bytes(),
            "seed {seed}: segment {key} written by someone other than {holder}"
        );
    }
}

#[test]
fn holders_dying_at_random_points_never_break_safety() {
    for seed in 1..=32 {
        run_seed(seed);
    }
}
