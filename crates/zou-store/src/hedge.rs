//! Tail hedging for creation PUTs.
//!
//! An object store's latency curve has a long tail: the p50 PUT is
//! pleasant and the p99 is many multiples of it, and with a pipelined
//! landing chain one slow PUT stalls the ack of every window behind
//! it. [`HedgedStore`] cuts the tail at the source: when a
//! `put_if_absent` dawdles past an adaptive delay, a second identical
//! attempt races it and the first success wins. Same key, same bytes,
//! so the loser's [`CasError::AlreadyExists`] is just our own winner,
//! and a real fence still comes through because a fence turns every
//! attempt away.
//!
//! Only `put_if_absent` hedges. A conditional swap cannot self race,
//! its version check would read the winner as a conflict, and reads
//! have their own retry story in the backends. The delay tracks two
//! times the median of recent winners, so the hedge fires on roughly
//! the slowest tenth of PUTs and the extra request rate stays small,
//! and it never drops below what a second attempt costs to launch, so
//! a store fast enough that a hedge cannot arrive in time does not pay
//! for one. Every hedge is counted, so op counters and the cost
//! simulation see the spend honestly.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use crate::cas::{CasError, CasStore, Version};

/// Wins observed before the first hedge may fire. A cold ring knows
/// nothing about the store's latency shape, and hedging blind would
/// double the request rate exactly when nothing is known to be slow.
const WARMUP: usize = 16;

/// Winner latencies kept for the median, a sliding window so the
/// delay follows the store through the day.
const RING: usize = 128;

/// The delay never drops below this.
///
/// It is not a latency budget, it is what a second attempt costs to
/// arrive. Launching one is a thread and a scheduler round trip, and on
/// a box with other work on it that is hundreds of microseconds at the
/// median and milliseconds at the tail, measured on server2 at p50 106
/// us and p90 2443 us over fifty samples. A floor under that hedges
/// PUTs the second attempt cannot possibly beat, which is every PUT to
/// a local store: it was a millisecond, and fifty inserts into a hash
/// map paid up to fifteen hedges on a loaded box (#701).
///
/// So the floor says what the mechanism is for. An object store's p50
/// PUT is tens of milliseconds and its tail is multiples of that, which
/// is the curve a second request cuts. A store answering in under this
/// has no tail worth a request, and duplicating its work would spend
/// real PUTs, and real money on a real bucket, on the scheduler.
const MIN_DELAY: Duration = Duration::from_millis(25);

/// A store whose creation PUTs hedge their tail. Every other call
/// passes straight through to the wrapped store.
pub struct HedgedStore {
    inner: Arc<dyn CasStore>,
    wins: Mutex<VecDeque<u64>>,
    hedges: AtomicU64,
}

impl HedgedStore {
    pub fn new(inner: Arc<dyn CasStore>) -> Self {
        Self {
            inner,
            wins: Mutex::new(VecDeque::with_capacity(RING)),
            hedges: AtomicU64::new(0),
        }
    }

    /// How many creation PUTs paid for a second attempt, for op
    /// counters and anyone debugging a suspicious bill.
    pub fn hedges(&self) -> u64 {
        self.hedges.load(Ordering::Relaxed)
    }

    /// Two times the median winner latency, or None while the ring is
    /// still warming up.
    fn delay(&self) -> Option<Duration> {
        let wins = self.wins.lock().unwrap();
        if wins.len() < WARMUP {
            return None;
        }
        let mut sorted: Vec<u64> = wins.iter().copied().collect();
        sorted.sort_unstable();
        let p50 = sorted[sorted.len() / 2];
        Some(MIN_DELAY.max(Duration::from_micros(2 * p50)))
    }

    fn record(&self, took: Duration) {
        let mut wins = self.wins.lock().unwrap();
        if wins.len() == RING {
            wins.pop_front();
        }
        wins.push_back(took.as_micros() as u64);
    }
}

impl CasStore for HedgedStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        self.inner.get(key)
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
        self.inner.get_range(key, offset, len)
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        self.inner.put_if_match(key, data, expected)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        self.inner.put(key, data)
    }

    /// Signing a url is arithmetic against no backend, so there is
    /// nothing here to hedge.
    fn presigned_get(
        &self,
        key: &str,
        ttl: Duration,
        response: &[(&str, &str)],
    ) -> Result<Option<String>, CasError> {
        self.inner.presigned_get(key, ttl, response)
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        self.inner.delete(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        self.inner.list(prefix)
    }

    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        let Some(delay) = self.delay() else {
            let start = Instant::now();
            let result = self.inner.put_if_absent(key, data);
            if result.is_ok() {
                self.record(start.elapsed());
            }
            return result;
        };

        let bytes: Arc<[u8]> = data.into();
        let key: Arc<str> = key.into();
        let (tx, rx) = mpsc::channel();
        let (begun_tx, begun_rx) = mpsc::channel();
        let launch = || {
            let store = Arc::clone(&self.inner);
            let bytes = Arc::clone(&bytes);
            let key = Arc::clone(&key);
            let tx = tx.clone();
            let begun = begun_tx.clone();
            thread::spawn(move || {
                let start = Instant::now();
                // The one number both sides of this use. The parent's
                // patience is measured from here rather than from the
                // spawn, and the winner it records is measured from
                // here too, so the delay and the median it is derived
                // from are the same clock.
                let _ = begun.send(start);
                let result = store.put_if_absent(&key, &bytes);
                // The receiver may be gone because the other attempt
                // already won; a loser's report has nowhere to go.
                let _ = tx.send((result, start.elapsed()));
            });
        };
        launch();

        // Waiting for the attempt to reach a core before starting the
        // clock is the whole point. The delay floor is a millisecond
        // and a thread on a busy box can sit unscheduled for several,
        // so a deadline stamped in the caller spends its budget on the
        // scheduler and then hedges a store that was never asked for
        // anything. Every one of those is a second real PUT against a
        // real bucket, counted as a hedge, which reads as a slow store.
        let deadline = begun_rx.recv().unwrap_or_else(|_| Instant::now()) + delay;
        let mut outstanding = 1;
        let mut hedged = false;
        let mut fence = None;
        let mut first_err = None;
        while outstanding > 0 {
            let (result, took) = if hedged {
                rx.recv().expect("a launched attempt always reports")
            } else {
                match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                    Ok(report) => report,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        launch();
                        outstanding += 1;
                        hedged = true;
                        self.hedges.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        unreachable!("a launched attempt always reports")
                    }
                }
            };
            outstanding -= 1;
            match result {
                Ok(version) => {
                    self.record(took);
                    return Ok(version);
                }
                // Ours that lost the self race, or a real fence. Only
                // the other attempt knows: a success from it means the
                // object is ours, a fence from it means it never was.
                Err(e @ CasError::AlreadyExists { .. }) => fence = Some(e),
                // An io error hedges immediately, the way a slow PUT
                // would have at the deadline.
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                    if !hedged {
                        launch();
                        outstanding += 1;
                        hedged = true;
                        self.hedges.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        // No attempt succeeded. A fence outranks an io error: it means
        // someone owns the key and retrying would write into their
        // chain.
        match fence {
            Some(e) => Err(e),
            None => Err(first_err.expect("every attempt failed, so one erred")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::mem::MemStore;

    /// Stalls the first `put_if_absent` for a chosen key, so a test
    /// can prove the hedge overtakes it.
    struct StallOnce {
        inner: MemStore,
        stall_key: String,
        stalled: AtomicBool,
        stall_for: Duration,
    }

    impl CasStore for StallOnce {
        fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
            self.inner.get(key)
        }
        fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
            self.inner.get_range(key, offset, len)
        }
        fn put_if_match(
            &self,
            key: &str,
            data: &[u8],
            expected: Option<&Version>,
        ) -> Result<Version, CasError> {
            if key == self.stall_key && !self.stalled.swap(true, Ordering::SeqCst) {
                thread::sleep(self.stall_for);
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

    fn warmed(inner: Arc<dyn CasStore>) -> HedgedStore {
        let hedged = HedgedStore::new(inner);
        for i in 0..WARMUP {
            hedged.put_if_absent(&format!("warm/{i}"), b"x").unwrap();
        }
        hedged
    }

    #[test]
    fn a_fast_store_never_hedges() {
        let hedged = warmed(Arc::new(MemStore::new()));
        for i in 0..50 {
            hedged.put_if_absent(&format!("k/{i}"), b"payload").unwrap();
        }
        assert_eq!(hedged.hedges(), 0, "a sub millisecond PUT paid a hedge");
    }

    #[test]
    fn the_hedge_overtakes_a_stalled_put() {
        let hedged = warmed(Arc::new(StallOnce {
            inner: MemStore::new(),
            stall_key: "slow".into(),
            stalled: AtomicBool::new(false),
            stall_for: Duration::from_secs(2),
        }));
        let start = Instant::now();
        hedged.put_if_absent("slow", b"payload").unwrap();
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "the hedge should win long before the stalled attempt"
        );
        assert_eq!(hedged.hedges(), 1);
    }

    #[test]
    fn a_real_fence_survives_the_hedge() {
        let inner = Arc::new(MemStore::new());
        inner.put_if_absent("taken", b"the rival's bytes").unwrap();
        let hedged = warmed(Arc::clone(&inner) as Arc<dyn CasStore>);
        match hedged.put_if_absent("taken", b"ours") {
            Err(CasError::AlreadyExists { .. }) => {}
            other => panic!("a fence must come through: {other:?}"),
        }
    }
}
