//! Freeze and thaw: a writer is stopped where it stands, with PUTs in
//! flight, and thawed into a world that moved on.
//!
//! This is not a crash. A crashed writer is gone and its in flight
//! requests died with it, which is what [`chain.rs`] already covers. A
//! frozen one keeps every request it had issued and lets them all go at
//! once, minutes later, after a successor has sealed the chain and been
//! writing for a while. Lambda between invocations and a suspended Fly
//! machine are both exactly this, and neither of them tells the process
//! it happened.
//!
//! Freezing is modeled at the store boundary, per node, so one node's
//! world stops while the other's runs on. Both nodes hold the same
//! directory, so the store itself sees the interleaving the freeze
//! produces rather than a simulation of it.
//!
//! What has to hold, at every freeze point and for every seed:
//!
//! - An ack is never a lie. Whatever either node acked is in the chain a
//!   fresh reader walks at the end, with its bytes intact.
//! - The chain still reads. A thawed writer's stragglers land past the
//!   successor's seal, in seqs the successor has not reached yet, and
//!   recovery has to cross them rather than stop dead at the first one.
//! - The successor keeps the shard. A writer that was frozen out does
//!   not get to poison the node that replaced it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use zou_log::{
    AppendError, MediaSink, Sequencer, SequencerConfig, WalMedia, read_chain, take_over,
};
use zou_store::{CasError, CasStore, Frame2, LocalFsStore, Lsn, Version};

/// A store that can be stopped mid request.
///
/// Every operation checks in before it reaches the disk and parks there
/// while the freeze is on. That is the whole of what a suspend does to a
/// process: the request was issued, the syscall never came back, and the
/// far side learns about it whenever the machine is allowed to run
/// again.
struct Freezer {
    inner: LocalFsStore,
    frozen: Mutex<bool>,
    wake: Condvar,
    /// Requests parked in the freeze right now, so a test can wait until
    /// it has actually caught one instead of guessing with a sleep.
    parked: AtomicUsize,
}

impl Freezer {
    fn new(dir: &std::path::Path) -> Self {
        Self {
            inner: LocalFsStore::new(dir),
            frozen: Mutex::new(false),
            wake: Condvar::new(),
            parked: AtomicUsize::new(0),
        }
    }

    fn freeze(&self) {
        *self.frozen.lock().unwrap() = true;
    }

    fn thaw(&self) {
        *self.frozen.lock().unwrap() = false;
        self.wake.notify_all();
    }

    /// Block until at least `n` requests are parked, or give up.
    /// Returns how many were caught.
    fn caught(&self, n: usize, limit: Duration) -> usize {
        let deadline = Instant::now() + limit;
        loop {
            let parked = self.parked.load(Ordering::Acquire);
            if parked >= n || Instant::now() >= deadline {
                return parked;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn check_in(&self) {
        let frozen = self.frozen.lock().unwrap();
        if !*frozen {
            return;
        }
        self.parked.fetch_add(1, Ordering::AcqRel);
        let held = self
            .wake
            .wait_while(frozen, |frozen| *frozen)
            .expect("freeze mutex poisoned");
        self.parked.fetch_sub(1, Ordering::AcqRel);
        drop(held);
    }
}

impl CasStore for Freezer {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        self.check_in();
        self.inner.get(key)
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        self.check_in();
        self.inner.put_if_match(key, data, expected)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        self.check_in();
        self.inner.put(key, data)
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        self.check_in();
        self.inner.delete(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        self.check_in();
        self.inner.list(prefix)
    }
}

fn frame(tenant: u128, lsn: u64, body: &[u8]) -> Frame2 {
    Frame2 {
        tenant,
        writer_epoch: 1,
        start_lsn: Lsn(lsn),
        end_lsn: Lsn(lsn + body.len() as u64),
        contains_commit: true,
        first_of_epoch: false,
        hints: Vec::new(),
        payload: body.to_vec(),
    }
}

fn config(inflight: usize) -> SequencerConfig {
    SequencerConfig {
        window: Duration::from_millis(2),
        inflight,
        ..SequencerConfig::default()
    }
}

fn resume(media: &Arc<WalMedia>, shard: u32, t: zou_log::Takeover, inflight: usize) -> Sequencer {
    let sink = Arc::new(MediaSink::new(Arc::clone(media), shard, t.sealed_seq));
    Sequencer::resume(
        shard,
        sink as _,
        config(inflight),
        t.next_seq,
        t.prev_digest,
    )
}

/// One acked write, by the payload that has to come back out.
type Acked = Vec<u8>;

/// Freeze a writer with `inflight` PUTs pipelined, hand the shard to a
/// successor while it is stopped, then let it go.
fn run(seed: u64, inflight: usize) {
    let dir = tempfile::tempdir().unwrap();
    let shard = 7;

    // Two nodes, one directory. Only the first one can be frozen, which
    // is the point: a suspend is per machine, not per bucket.
    let freezer = Arc::new(Freezer::new(dir.path()));
    let media_a = Arc::new(WalMedia::single(Arc::clone(&freezer) as Arc<dyn CasStore>));
    let media_b = Arc::new(WalMedia::single(
        Arc::new(LocalFsStore::new(dir.path())) as Arc<dyn CasStore>
    ));

    let ta = take_over(&media_a, shard, "node-a").unwrap();
    let seq_a = resume(&media_a, shard, ta, inflight);

    // Warm the chain so the freeze lands on a shard with history rather
    // than on the seal the takeover just wrote.
    let mut acked: Vec<Acked> = Vec::new();
    for i in 0..(seed % 3 + 1) {
        let body = format!("warm-{i}").into_bytes();
        seq_a
            .append(vec![frame(1, 100 + i * 10, &body)])
            .unwrap()
            .wait()
            .unwrap();
        acked.push(body);
    }

    let mut acked_b: Vec<Acked> = Vec::new();
    let seq_b = std::thread::scope(|scope| {
        // Keep node a appending in the background so the freeze catches
        // PUTs that were already issued, which is the case a crash
        // cannot produce.
        let writer = scope.spawn(|| {
            let mut mine: Vec<(Acked, zou_log::AppendTicket)> = Vec::new();
            for i in 0..64u64 {
                let body = format!("a-{i}").into_bytes();
                match seq_a.append(vec![frame(1, 10_000 + i * 10, &body)]) {
                    Ok(ticket) => mine.push((body, ticket)),
                    // Fenced, which is the expected end of this writer.
                    Err(_) => break,
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            mine
        });

        // Stop node a where it stands, with as much of the pipeline in
        // flight as the seed asks for.
        let want = (seed as usize % inflight) + 1;
        freezer.freeze();
        let caught = freezer.caught(want, Duration::from_secs(5));
        assert!(caught >= 1, "seed {seed}: the freeze caught nothing");

        // The world moves on: a successor fences the shard and writes.
        let tb = take_over(&media_b, shard, "node-b").unwrap();
        let seq_b = resume(&media_b, shard, tb, inflight);
        for i in 0..4u64 {
            let body = format!("b-{i}").into_bytes();
            seq_b
                .append(vec![frame(2, 50_000 + i * 10, &body)])
                .unwrap()
                .wait()
                .expect("the successor owns the shard and its writes must land");
            acked_b.push(body);
        }

        // Thaw. Every request node a had issued goes out at once, into a
        // chain that has been sealed and written past since.
        freezer.thaw();
        for (body, ticket) in writer.join().unwrap() {
            if ticket.wait().is_ok() {
                acked.push(body);
            }
        }
        seq_b
    });
    let _ = seq_a.close();

    // The successor is still the owner: a thawed writer must not be able
    // to take the shard away from the node that replaced it.
    for i in 4..8u64 {
        let body = format!("b-{i}").into_bytes();
        seq_b
            .append(vec![frame(2, 60_000 + i * 10, &body)])
            .map_err(|e| format!("seed {seed}: successor refused after the thaw: {e}"))
            .unwrap()
            .wait()
            .map_err(|e| format!("seed {seed}: successor lost a write after the thaw: {e}"))
            .unwrap();
        acked_b.push(body);
    }
    seq_b.close().unwrap();

    // Recovery reads the whole chain, crossing whatever the thaw left
    // behind, and finds every byte either node was ever told was safe.
    let chain = read_chain(&media_b, shard, 0)
        .map_err(|e| format!("seed {seed}: the chain does not read after a thaw: {e}"))
        .unwrap();
    let landed: Vec<&Frame2> = chain.iter().flat_map(|s| &s.frames).collect();
    for body in acked.iter().chain(acked_b.iter()) {
        assert!(
            landed.iter().any(|f| f.payload == *body),
            "seed {seed}: acked write {} is not in the chain",
            String::from_utf8_lossy(body)
        );
    }
}

#[test]
fn a_thawed_writer_never_costs_the_chain_an_acked_write() {
    for seed in 1..=12 {
        run(seed, 4);
    }
}

#[test]
fn a_deep_pipeline_frozen_mid_flight_still_reads_back() {
    for seed in 1..=4 {
        run(seed, 16);
    }
}

/// The narrow case on its own, so a failure names it: node a's
/// stragglers land in seqs past the successor's seal, and the successor
/// has to keep writing through them rather than find its own next
/// position already taken.
#[test]
fn stragglers_landing_past_the_seal_do_not_poison_the_successor() {
    let dir = tempfile::tempdir().unwrap();
    let shard = 3;
    let freezer = Arc::new(Freezer::new(dir.path()));
    let media_a = Arc::new(WalMedia::single(Arc::clone(&freezer) as Arc<dyn CasStore>));
    let media_b = Arc::new(WalMedia::single(
        Arc::new(LocalFsStore::new(dir.path())) as Arc<dyn CasStore>
    ));

    let ta = take_over(&media_a, shard, "node-a").unwrap();
    let seq_a = resume(&media_a, shard, ta, 8);
    seq_a
        .append(vec![frame(1, 10, b"before the freeze")])
        .unwrap()
        .wait()
        .unwrap();

    let (seq_b, survivors) = std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let mut mine = Vec::new();
            for i in 0..8u64 {
                let Ok(ticket) = seq_a.append(vec![frame(1, 1_000 + i * 10, b"in flight")]) else {
                    break;
                };
                mine.push(ticket);
                std::thread::sleep(Duration::from_millis(1));
            }
            mine
        });

        freezer.freeze();
        assert!(freezer.caught(2, Duration::from_secs(5)) >= 1);

        let tb = take_over(&media_b, shard, "node-b").unwrap();
        let seq_b = resume(&media_b, shard, tb, 8);
        freezer.thaw();

        // Whatever node a's stragglers did, none of them was acked
        // without being in the chain, and the successor writes on.
        let tickets = writer.join().unwrap();
        let survivors = tickets
            .into_iter()
            .map(zou_log::AppendTicket::wait)
            .filter(Result::is_ok)
            .count();
        (seq_b, survivors)
    });
    let _ = seq_a.close();

    seq_b
        .append(vec![frame(2, 9_000, b"after the thaw")])
        .expect("the successor is still admitted")
        .wait()
        .expect("the successor's write lands");
    seq_b.close().unwrap();

    let chain = read_chain(&media_b, shard, 0).expect("the chain reads across the stragglers");
    let landed: Vec<&Frame2> = chain.iter().flat_map(|s| &s.frames).collect();
    assert!(landed.iter().any(|f| f.payload == b"before the freeze"));
    assert!(landed.iter().any(|f| f.payload == b"after the thaw"));
    assert_eq!(
        landed.iter().filter(|f| f.payload == b"in flight").count(),
        survivors,
        "an in flight write was acked without reaching the chain, or reached it without an ack"
    );
}

/// A frozen writer with nothing in flight is the ordinary case: it wakes
/// up, finds its next position sealed, and stops instead of writing into
/// a chain that is not its own.
#[test]
fn a_thawed_writer_with_an_empty_pipeline_finds_itself_fenced() {
    let dir = tempfile::tempdir().unwrap();
    let shard = 1;
    let store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path()));
    let media = Arc::new(WalMedia::single(store));

    let ta = take_over(&media, shard, "node-a").unwrap();
    let seq_a = resume(&media, shard, ta, 4);
    seq_a
        .append(vec![frame(1, 10, b"before")])
        .unwrap()
        .wait()
        .unwrap();

    // The freeze is the gap: nothing happens here for longer than the
    // lease, and the successor takes the shard.
    let tb = take_over(&media, shard, "node-b").unwrap();

    let err = seq_a
        .append(vec![frame(1, 20, b"after the thaw")])
        .unwrap()
        .wait()
        .unwrap_err();
    assert!(matches!(err, AppendError::Store { .. }), "got {err}");
    match seq_a.append(vec![frame(1, 30, b"and again")]) {
        Err(AppendError::Poisoned) => {}
        Err(e) => panic!("expected a poisoned writer, got {e}"),
        Ok(_) => panic!("a fenced writer kept taking work"),
    }
    let _ = seq_a.close();

    let seq_b = resume(&media, shard, tb, 4);
    seq_b
        .append(vec![frame(2, 40, b"the successor")])
        .unwrap()
        .wait()
        .unwrap();
    seq_b.close().unwrap();

    let chain = read_chain(&media, shard, 0).unwrap();
    let landed: Vec<&Frame2> = chain.iter().flat_map(|s| &s.frames).collect();
    assert!(landed.iter().any(|f| f.payload == b"before"));
    assert!(landed.iter().any(|f| f.payload == b"the successor"));
    assert!(landed.iter().all(|f| f.payload != b"after the thaw"));
}
