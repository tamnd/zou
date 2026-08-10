//! Background lease renewal.
//!
//! A held lease is only useful while someone keeps renewing it, so the
//! heartbeat owns that job on a plain std thread: renew at a third of the
//! TTL with jitter, retry sooner on transient store errors, and flip a
//! `lost` flag the moment the manifest shows another holder or the TTL
//! runs out locally. The jitter keeps a fleet of writers from hammering
//! the store in lockstep after a shared stall.
//!
//! The `HeldLease` lives behind the same `Arc<Mutex<_>>` the fold path
//! uses for checkpoint publishes. Both paths re-read the manifest
//! before swapping, so a renewal never clobbers a checkpoint update and
//! vice versa.
//!
//! Losing the lease here does not by itself stop the writer, the pipeline
//! discovers loss through its own publishes too, but callers should treat
//! `lost()` as a stop sign: epoch fencing makes a zombie's frames
//! unreachable, this flag is what keeps the zombie from wasting effort.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::cas::CasStore;
use crate::layout::TenantLayout;
use crate::lease::{self, HeldLease, LeaseError};

/// Unix seconds source. Injectable so tests control time.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

pub fn system_clock() -> Clock {
    Arc::new(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before the unix epoch")
            .as_secs()
    })
}

struct HbShared {
    stop: Mutex<bool>,
    wake: Condvar,
    lost: AtomicBool,
    error: Mutex<Option<LeaseError>>,
}

/// Handle to the renewal thread. Drop stops the thread without releasing
/// the lease, `detach` is the clean path that also clears the lease so the
/// next writer does not wait out the TTL.
pub struct Heartbeat {
    shared: Arc<HbShared>,
    handle: Option<JoinHandle<()>>,
    store: Arc<dyn CasStore>,
    layout: TenantLayout,
    held: Arc<Mutex<HeldLease>>,
}

impl Heartbeat {
    pub fn spawn(
        store: Arc<dyn CasStore>,
        layout: TenantLayout,
        held: Arc<Mutex<HeldLease>>,
        ttl_secs: u64,
    ) -> Self {
        Self::spawn_with_clock(store, layout, held, ttl_secs, system_clock())
    }

    pub fn spawn_with_clock(
        store: Arc<dyn CasStore>,
        layout: TenantLayout,
        held: Arc<Mutex<HeldLease>>,
        ttl_secs: u64,
        clock: Clock,
    ) -> Self {
        let shared = Arc::new(HbShared {
            stop: Mutex::new(false),
            wake: Condvar::new(),
            lost: AtomicBool::new(false),
            error: Mutex::new(None),
        });
        let seed = {
            let h = held.lock().expect("lease mutex poisoned");
            h.holder
                .bytes()
                .fold(h.fence, |acc, b| acc.rotate_left(8) ^ u64::from(b))
        };
        let handle = {
            let shared = Arc::clone(&shared);
            let store = Arc::clone(&store);
            let layout = layout.clone();
            let held = Arc::clone(&held);
            std::thread::Builder::new()
                .name("zou-heartbeat".into())
                .spawn(move || run(&shared, &*store, &layout, &held, ttl_secs, &clock, seed))
                .expect("spawn heartbeat thread")
        };
        Self {
            shared,
            handle: Some(handle),
            store,
            layout,
            held,
        }
    }

    /// True once the lease is gone: stolen, or expired locally after the
    /// store refused renewals for a full TTL. The caller must stop writing.
    pub fn lost(&self) -> bool {
        self.shared.lost.load(Ordering::Acquire)
    }

    /// Stop renewing and release the lease so the next writer gets in
    /// immediately. If the lease was already lost the release is skipped,
    /// it is not ours to clear, and the loss is returned instead.
    pub fn detach(mut self) -> Result<(), LeaseError> {
        self.stop_and_join();
        if let Some(e) = self
            .shared
            .error
            .lock()
            .expect("error mutex poisoned")
            .take()
        {
            return Err(e);
        }
        let held = self.held.lock().expect("lease mutex poisoned").clone();
        lease::release(&*self.store, &self.layout, held)
    }

    fn stop_and_join(&mut self) {
        *self.shared.stop.lock().expect("stop mutex poisoned") = true;
        self.shared.wake.notify_all();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.stop_and_join();
        }
    }
}

fn run(
    shared: &HbShared,
    store: &dyn CasStore,
    layout: &TenantLayout,
    held: &Mutex<HeldLease>,
    ttl_secs: u64,
    clock: &Clock,
    seed: u64,
) {
    let base_ms = (ttl_secs.saturating_mul(1000) / 3).max(1);
    let spread_ms = (base_ms / 5).max(1);
    let mut rng = Jitter::new(seed);
    let mut wait_ms = jittered(base_ms, spread_ms, &mut rng);
    loop {
        let stop = shared.stop.lock().expect("stop mutex poisoned");
        let (stop, _) = shared
            .wake
            .wait_timeout_while(stop, Duration::from_millis(wait_ms), |stopped| !*stopped)
            .expect("stop mutex poisoned");
        if *stop {
            return;
        }
        drop(stop);

        let mut held = held.lock().expect("lease mutex poisoned");
        match lease::renew(store, layout, &mut held, ttl_secs, clock()) {
            Ok(()) => wait_ms = jittered(base_ms, spread_ms, &mut rng),
            Err(e @ LeaseError::Lost { .. }) => return fail(shared, e),
            Err(e) => {
                // Transient store or race trouble. Keep trying on a tight
                // cadence while our own TTL has time left, give up honestly
                // once it does not: past the expiry other nodes may steal,
                // so we must assume they have.
                if clock() >= held.expires_unix {
                    return fail(shared, e);
                }
                wait_ms = (base_ms / 4).max(10);
            }
        }
    }
}

fn fail(shared: &HbShared, e: LeaseError) {
    *shared.error.lock().expect("error mutex poisoned") = Some(e);
    shared.lost.store(true, Ordering::Release);
}

/// xorshift64, seeded per holder so contenders desynchronize. Not secure,
/// not meant to be, it only spreads renewal times.
struct Jitter {
    state: u64,
}

impl Jitter {
    fn new(seed: u64) -> Self {
        Self { state: seed | 1 }
    }

    fn next(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
}

fn jittered(base: u64, spread: u64, rng: &mut Jitter) -> u64 {
    base.saturating_sub(spread) + rng.next() % (2 * spread + 1)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;

    use super::*;
    use crate::cas::{CasError, LocalFsStore, Version};
    use crate::manifest::{Lease, Manifest};

    fn setup() -> (tempfile::TempDir, Arc<LocalFsStore>, TenantLayout) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFsStore::new(dir.path()));
        let layout = TenantLayout::new("t1");
        store
            .put_if_absent(&layout.manifest(), &Manifest::new("t1", 18).to_json())
            .unwrap();
        (dir, store, layout)
    }

    fn now_unix() -> u64 {
        (system_clock())()
    }

    #[test]
    fn renews_on_schedule_and_detach_clears_the_lease() {
        let (_d, store, layout) = setup();
        let held = lease::acquire(&*store, &layout, "node-a", 2, now_unix()).unwrap();
        let initial_expiry = held.expires_unix;
        let held = Arc::new(Mutex::new(held));

        let hb = Heartbeat::spawn(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            Arc::clone(&held),
            2,
        );
        // Base interval is ~667 ms, so this covers a few renewals and lets
        // wall clock seconds advance past the acquisition second.
        std::thread::sleep(Duration::from_millis(2100));
        assert!(!hb.lost());
        assert!(
            held.lock().unwrap().expires_unix > initial_expiry,
            "no renewal extended the lease"
        );

        hb.detach().unwrap();
        let (data, _) = store.get(&layout.manifest()).unwrap().unwrap();
        assert!(Manifest::from_json(&data).unwrap().lease.is_none());
    }

    #[test]
    fn a_stolen_lease_flips_lost_and_detach_reports_it() {
        let (_d, store, layout) = setup();
        let held = lease::acquire(&*store, &layout, "node-a", 2, now_unix()).unwrap();
        let held = Arc::new(Mutex::new(held));

        // Steal by editing the manifest directly, the way a rival that saw
        // an expired lease would.
        let key = layout.manifest();
        let (data, version) = store.get(&key).unwrap().unwrap();
        let mut m = Manifest::from_json(&data).unwrap();
        m.epoch += 1;
        m.lease = Some(Lease {
            holder: "node-b".into(),
            expires_unix: now_unix() + 60,
            fence: 2,
            endpoint: None,
        });
        store
            .put_if_match(&key, &m.to_json(), Some(&version))
            .unwrap();

        let hb = Heartbeat::spawn(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            held,
            2,
        );
        std::thread::sleep(Duration::from_millis(1500));
        assert!(hb.lost(), "heartbeat did not notice the steal");
        assert!(matches!(
            hb.detach(),
            Err(LeaseError::Lost { ref holder, epoch: 2 }) if holder == "node-b"
        ));

        // The rival's lease was left alone.
        let (data, _) = store.get(&key).unwrap().unwrap();
        assert_eq!(
            Manifest::from_json(&data).unwrap().lease.unwrap().holder,
            "node-b"
        );
    }

    /// A store that fails the next N mutations, then recovers.
    struct FlakyStore {
        inner: Arc<LocalFsStore>,
        failures_left: AtomicU32,
    }

    impl CasStore for FlakyStore {
        fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
            self.inner.get(key)
        }

        fn put_if_match(
            &self,
            key: &str,
            data: &[u8],
            expected: Option<&Version>,
        ) -> Result<Version, CasError> {
            if self
                .failures_left
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(CasError::Io {
                    key: key.to_string(),
                    source: std::io::Error::other("injected outage"),
                });
            }
            self.inner.put_if_match(key, data, expected)
        }

        fn delete(&self, key: &str) -> Result<(), CasError> {
            self.inner.delete(key)
        }

        fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
            self.inner.list(prefix)
        }
    }

    #[test]
    fn transient_store_failures_are_retried_within_the_ttl() {
        let (_d, store, layout) = setup();
        let held = lease::acquire(&*store, &layout, "node-a", 3, now_unix()).unwrap();
        let initial_expiry = held.expires_unix;
        let held = Arc::new(Mutex::new(held));

        let flaky = Arc::new(FlakyStore {
            inner: Arc::clone(&store),
            failures_left: AtomicU32::new(2),
        });
        let hb = Heartbeat::spawn(
            flaky as Arc<dyn CasStore>,
            layout.clone(),
            Arc::clone(&held),
            3,
        );
        std::thread::sleep(Duration::from_millis(2500));
        assert!(!hb.lost(), "gave up on a transient outage");
        assert!(held.lock().unwrap().expires_unix > initial_expiry);
        hb.detach().unwrap();
    }

    #[test]
    fn jitter_stays_within_twenty_percent_of_the_base() {
        let base = 5000;
        let spread = base / 5;
        let mut rng = Jitter::new(42);
        for _ in 0..1000 {
            let w = jittered(base, spread, &mut rng);
            assert!((4000..=6000).contains(&w), "{w} out of range");
        }
    }
}
