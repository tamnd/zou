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
//!
//! A project nobody is writing backs off. The TTL is a promise about how
//! long a dead node's work is unavailable, and there is no work to be
//! unavailable on a database that has not taken a write since it was
//! attached, so after a few quiet renewals the heartbeat writes a longer
//! lease and renews it proportionally less often. Writers say so through
//! [`Heartbeat::working`], which both resets the quiet count and wakes the
//! thread out of a long sleep, so the first write on a backed off project
//! puts the tight TTL back in the manifest rather than waiting out the
//! sleep it was already in. What an active project costs and how fast it
//! fails over are therefore unchanged, and the backoff only has to carry
//! an idle project as far as the attach manager's idle budget, which
//! detaches it and releases the lease outright.

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

/// How many renewals in a row must pass with nothing written before the
/// heartbeat decides the project is idle. Three is one full TTL of quiet,
/// long enough that a write every few seconds never triggers it.
const QUIET_RENEWALS: u32 = 3;

struct HbShared {
    stop: Mutex<bool>,
    wake: Condvar,
    lost: AtomicBool,
    error: Mutex<Option<LeaseError>>,
    /// Set by a writer, cleared by the renewal that sees it. This is on
    /// the WAL append path, so it is a flag rather than a count: the
    /// heartbeat asks whether anything was written since it last looked,
    /// never how much.
    worked: AtomicBool,
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
    /// Renew at `ttl_secs` and never back off. This is the shape a one
    /// shot command and a test want, where the process is writing for as
    /// long as it exists.
    pub fn spawn(
        store: Arc<dyn CasStore>,
        layout: TenantLayout,
        held: Arc<Mutex<HeldLease>>,
        ttl_secs: u64,
    ) -> Self {
        Self::spawn_with_clock(store, layout, held, ttl_secs, ttl_secs, system_clock())
    }

    /// Renew at `ttl_secs` while the project is being written and at
    /// `idle_ttl_secs` while it is not. An `idle_ttl_secs` at or below
    /// `ttl_secs` turns the backoff off.
    pub fn spawn_idling(
        store: Arc<dyn CasStore>,
        layout: TenantLayout,
        held: Arc<Mutex<HeldLease>>,
        ttl_secs: u64,
        idle_ttl_secs: u64,
    ) -> Self {
        Self::spawn_with_clock(store, layout, held, ttl_secs, idle_ttl_secs, system_clock())
    }

    pub fn spawn_with_clock(
        store: Arc<dyn CasStore>,
        layout: TenantLayout,
        held: Arc<Mutex<HeldLease>>,
        ttl_secs: u64,
        idle_ttl_secs: u64,
        clock: Clock,
    ) -> Self {
        let shared = Arc::new(HbShared {
            stop: Mutex::new(false),
            wake: Condvar::new(),
            lost: AtomicBool::new(false),
            error: Mutex::new(None),
            worked: AtomicBool::new(false),
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
                .spawn(move || {
                    run(
                        &shared,
                        &*store,
                        &layout,
                        &held,
                        Ttls {
                            busy: ttl_secs,
                            idle: idle_ttl_secs.max(ttl_secs),
                        },
                        &clock,
                        seed,
                    )
                })
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

    /// Tell the heartbeat this project is being written, so it holds the
    /// tight TTL instead of backing off. Called from the WAL path on every
    /// append, which is why it reads before it writes: the common case is
    /// a project writing steadily, where the flag is already set and the
    /// cheapest thing to do is leave the cache line alone.
    pub fn working(&self) {
        if self.shared.worked.load(Ordering::Relaxed) {
            return;
        }
        // Under the same mutex the sleeper tests the flag under, so a
        // write that lands between its test and its sleep is not a
        // wakeup lost until the end of a long interval.
        let _stop = self.shared.stop.lock().expect("stop mutex poisoned");
        self.shared.worked.store(true, Ordering::Release);
        // Only matters when the thread is asleep on an idle interval, and
        // it is the point of the flag being visible: the first write after
        // a backoff puts the short lease back now rather than at the end
        // of a sleep that was scheduled when nothing was happening.
        self.shared.wake.notify_all();
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

/// The two TTLs the thread chooses between, `idle` never below `busy`.
#[derive(Clone, Copy)]
struct Ttls {
    busy: u64,
    idle: u64,
}

fn run(
    shared: &HbShared,
    store: &dyn CasStore,
    layout: &TenantLayout,
    held: &Mutex<HeldLease>,
    ttls: Ttls,
    clock: &Clock,
    seed: u64,
) {
    let mut rng = Jitter::new(seed);
    let mut ttl_secs = ttls.busy;
    let (mut base_ms, mut spread_ms) = cadence(ttl_secs);
    let mut wait_ms = jittered(base_ms, spread_ms, &mut rng);
    let mut quiet: u32 = 0;
    loop {
        let sleeping_idle = ttl_secs > ttls.busy;
        let stop = shared.stop.lock().expect("stop mutex poisoned");
        let (stop, _) = shared
            .wake
            .wait_timeout_while(stop, Duration::from_millis(wait_ms), |stopped| {
                // A write only cuts the sleep short when the sleep is a
                // long one. On the busy cadence the next renewal is
                // already close, and waking for every append would put a
                // store request on the write path.
                !*stopped && !(sleeping_idle && shared.worked.load(Ordering::Acquire))
            })
            .expect("stop mutex poisoned");
        if *stop {
            return;
        }
        drop(stop);

        quiet = if shared.worked.swap(false, Ordering::AcqRel) {
            0
        } else {
            quiet.saturating_add(1)
        };
        ttl_secs = if quiet >= QUIET_RENEWALS {
            ttls.idle
        } else {
            ttls.busy
        };
        (base_ms, spread_ms) = cadence(ttl_secs);

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

/// Renew at a third of the TTL, spread by a fifth of that.
fn cadence(ttl_secs: u64) -> (u64, u64) {
    let base_ms = (ttl_secs.saturating_mul(1000) / 3).max(1);
    (base_ms, (base_ms / 5).max(1))
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
    use std::time::Instant;

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
        // Wait for the renewal rather than for the clock. The base
        // interval is ~667 ms and an expiry is whole seconds, so the
        // first renewal that lands in a later wall clock second than
        // the acquisition is the one that moves it, which on a fast
        // machine is the first or second try and on a loaded runner is
        // whichever one gets scheduled. A fixed sleep long enough for
        // the slowest of them is one every other run pays for, and one
        // short enough for the rest is a coin flip there.
        let deadline = Instant::now() + Duration::from_secs(20);
        while held.lock().unwrap().expires_unix == initial_expiry {
            assert!(!hb.lost(), "the heartbeat lost the lease");
            assert!(
                Instant::now() < deadline,
                "no renewal extended the lease in 20 s"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(!hb.lost());

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
            ttl_secs: None,
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

    /// Counts the mutations, which is the whole point of the backoff.
    struct CountingStore {
        inner: Arc<LocalFsStore>,
        puts: AtomicU32,
    }

    impl CasStore for CountingStore {
        fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
            self.inner.get(key)
        }

        fn put_if_match(
            &self,
            key: &str,
            data: &[u8],
            expected: Option<&Version>,
        ) -> Result<Version, CasError> {
            self.puts.fetch_add(1, Ordering::Relaxed);
            self.inner.put_if_match(key, data, expected)
        }

        fn delete(&self, key: &str) -> Result<(), CasError> {
            self.inner.delete(key)
        }

        fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
            self.inner.list(prefix)
        }
    }

    /// The ttl the manifest currently advertises, which is how another
    /// node tells a backed off holder from a broken clock.
    fn advertised_ttl(store: &LocalFsStore, layout: &TenantLayout) -> Option<u64> {
        let (data, _) = store.get(&layout.manifest()).unwrap().unwrap();
        Manifest::from_json(&data).unwrap().lease?.ttl_secs
    }

    fn wait_for(deadline: Duration, mut done: impl FnMut() -> bool) -> bool {
        let until = Instant::now() + deadline;
        while Instant::now() < until {
            if done() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        done()
    }

    #[test]
    fn a_project_nobody_writes_backs_off_and_the_first_write_undoes_it() {
        let (_d, store, layout) = setup();
        let held = lease::acquire(&*store, &layout, "node-a", 1, now_unix()).unwrap();
        let held = Arc::new(Mutex::new(held));
        let counting = Arc::new(CountingStore {
            inner: Arc::clone(&store),
            puts: AtomicU32::new(0),
        });

        // A one second lease renews three times a second, so the quiet
        // count is reached in about a second and the test does not have
        // to sit through a production TTL to see it.
        let hb = Heartbeat::spawn_idling(
            Arc::clone(&counting) as Arc<dyn CasStore>,
            layout.clone(),
            Arc::clone(&held),
            1,
            30,
        );
        assert!(
            wait_for(Duration::from_secs(20), || advertised_ttl(&store, &layout)
                == Some(30)),
            "the heartbeat never backed off on a project nothing was writing"
        );

        // And having backed off, it stops costing anything: at the busy
        // cadence this window is four renewals.
        counting.puts.store(0, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(1500));
        let idle_puts = counting.puts.load(Ordering::Relaxed);
        assert!(
            idle_puts <= 1,
            "{idle_puts} renewals in a window an idle project should have spent asleep"
        );

        // A write is the end of that, and it does not wait out the long
        // sleep it interrupted.
        hb.working();
        assert!(
            wait_for(Duration::from_secs(5), || advertised_ttl(&store, &layout)
                == Some(1)),
            "a write did not put the short lease back"
        );
        assert!(!hb.lost());
        hb.detach().unwrap();
    }

    #[test]
    fn a_project_being_written_never_backs_off() {
        let (_d, store, layout) = setup();
        let held = lease::acquire(&*store, &layout, "node-a", 1, now_unix()).unwrap();
        let held = Arc::new(Mutex::new(held));
        let hb = Heartbeat::spawn_idling(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            Arc::clone(&held),
            1,
            30,
        );
        let until = Instant::now() + Duration::from_millis(2500);
        while Instant::now() < until {
            hb.working();
            std::thread::sleep(Duration::from_millis(25));
            assert_eq!(
                advertised_ttl(&store, &layout),
                Some(1),
                "a project taking writes must keep the failover time it is documented to have"
            );
        }
        assert!(!hb.lost());
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
