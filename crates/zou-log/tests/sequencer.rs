//! The sequencer's promises, held against test sinks: one PUT per
//! window no matter how many tenants append, acks strictly after the
//! durable PUT returns, stale epochs rejected with the current one,
//! failures poison instead of lying, and idle windows cost nothing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use zou_log::{
    AppendError, Backpressure, IngestLag, LagBounds, MediaSink, SegmentSink, Sequencer,
    SequencerConfig, Throttle, WalMedia, decode_segment, read_footer, tenants_digest,
};
use zou_store::{CasError, CasStore, Frame2, LocalFsStore, Lsn};

fn frame(tenant: u128, epoch: u32, lsn: u64, body: &[u8]) -> Frame2 {
    Frame2 {
        tenant,
        writer_epoch: epoch,
        start_lsn: Lsn(lsn),
        end_lsn: Lsn(lsn + body.len() as u64),
        contains_commit: true,
        first_of_epoch: false,
        hints: Vec::new(),
        payload: body.to_vec(),
    }
}

fn quick() -> SequencerConfig {
    SequencerConfig {
        window: Duration::from_millis(5),
        ..SequencerConfig::default()
    }
}

/// Records every segment it is handed, in order.
#[derive(Default)]
struct RecordingSink {
    puts: Mutex<Vec<(u64, Vec<u8>)>>,
}

impl SegmentSink for RecordingSink {
    fn put_segment(&self, seq: u64, segment: &[u8]) -> Result<(), CasError> {
        self.puts.lock().unwrap().push((seq, segment.to_vec()));
        Ok(())
    }
}

/// Blocks every PUT until released, to prove no ack outruns the store.
#[derive(Default)]
struct GateSink {
    open: Mutex<bool>,
    cv: Condvar,
    entered: Mutex<usize>,
}

impl GateSink {
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.cv.notify_all();
    }
}

impl SegmentSink for GateSink {
    fn put_segment(&self, _seq: u64, _segment: &[u8]) -> Result<(), CasError> {
        *self.entered.lock().unwrap() += 1;
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.cv.wait(open).unwrap();
        }
        Ok(())
    }
}

struct FailSink {
    calls: AtomicUsize,
}

impl SegmentSink for FailSink {
    fn put_segment(&self, _seq: u64, _segment: &[u8]) -> Result<(), CasError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(CasError::AlreadyExists {
            key: "cellwal/0000/0000000000000001".into(),
        })
    }
}

#[test]
fn many_tenants_share_one_put_per_window() {
    let sink = Arc::new(RecordingSink::default());
    // A wide window so a slow CI box cannot split the appends across
    // two batches and fail the one PUT assertion.
    let config = SequencerConfig {
        window: Duration::from_millis(300),
        ..SequencerConfig::default()
    };
    let seq = Sequencer::start(0, Arc::clone(&sink) as _, config);

    let tickets: Vec<_> = (0..12u128)
        .map(|t| {
            seq.append(vec![frame(t, 1, 1000 * t as u64, b"records")])
                .unwrap()
        })
        .collect();
    for (i, t) in tickets.into_iter().enumerate() {
        assert_eq!(t.wait().unwrap(), Lsn(1000 * i as u64 + 7));
    }

    let puts = sink.puts.lock().unwrap();
    assert_eq!(puts.len(), 1, "12 appends inside one window is one PUT");
    let (seq_no, bytes) = &puts[0];
    assert_eq!(*seq_no, 1);
    let (header, frames, footer) = decode_segment(bytes).unwrap();
    assert_eq!(header.shard, 0);
    assert_eq!(frames.len(), 12);
    assert_eq!(footer.tenants.len(), 12);
}

#[test]
fn a_full_batch_closes_before_the_window() {
    let sink = Arc::new(RecordingSink::default());
    let config = SequencerConfig {
        window: Duration::from_secs(3600),
        batch_frames: 3,
        ..SequencerConfig::default()
    };
    let seq = Sequencer::start(0, Arc::clone(&sink) as _, config);
    let tickets: Vec<_> = (0..3)
        .map(|i| seq.append(vec![frame(1, 1, i * 100, b"x")]).unwrap())
        .collect();
    // The window is an hour, so resolving at all proves the frame cap
    // closed the batch.
    for t in tickets {
        t.wait().unwrap();
    }
    assert_eq!(sink.puts.lock().unwrap().len(), 1);
}

#[test]
fn a_byte_heavy_batch_closes_before_the_window() {
    let sink = Arc::new(RecordingSink::default());
    let config = SequencerConfig {
        window: Duration::from_secs(3600),
        batch_bytes: 1024,
        ..SequencerConfig::default()
    };
    let seq = Sequencer::start(0, Arc::clone(&sink) as _, config);
    // Incompressible payload so the encoded frame carries its full size.
    let mut noise = vec![0u8; 4096];
    let mut state = 99u64;
    for b in &mut noise {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *b = (state >> 33) as u8;
    }
    seq.append(vec![frame(1, 1, 0, &noise)])
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(sink.puts.lock().unwrap().len(), 1);
}

#[test]
fn idle_windows_put_nothing() {
    let sink = Arc::new(RecordingSink::default());
    let seq = Sequencer::start(0, Arc::clone(&sink) as _, quick());
    std::thread::sleep(Duration::from_millis(100));
    seq.close().unwrap();
    assert!(sink.puts.lock().unwrap().is_empty());
}

#[test]
fn acks_wait_for_the_durable_put_and_never_lie() {
    let sink = Arc::new(GateSink::default());
    let seq = Sequencer::start(0, Arc::clone(&sink) as _, quick());
    let ticket = seq.append(vec![frame(1, 1, 500, b"commit")]).unwrap();

    // Far past the window the PUT is in flight and blocked, and the
    // ack must still be withheld.
    std::thread::sleep(Duration::from_millis(80));
    assert_eq!(
        *sink.entered.lock().unwrap(),
        1,
        "the batch should be at the sink"
    );
    assert!(
        ticket.try_wait().is_none(),
        "acked before the store confirmed durability"
    );

    sink.release();
    assert_eq!(ticket.wait().unwrap(), Lsn(506));
}

#[test]
fn stale_epochs_are_rejected_with_the_current_one_and_stage_nothing() {
    let sink = Arc::new(RecordingSink::default());
    let seq = Sequencer::start(0, Arc::clone(&sink) as _, quick());

    seq.append(vec![frame(9, 5, 100, b"epoch five")])
        .unwrap()
        .wait()
        .unwrap();

    match seq.append(vec![frame(9, 4, 200, b"zombie")]) {
        Err(AppendError::WrongEpoch { tenant, current }) => {
            assert_eq!(tenant, 9);
            assert_eq!(current, 5);
        }
        Err(e) => panic!("wrong rejection: {e}"),
        Ok(_) => panic!("a stale epoch got through"),
    }

    // A mixed append is atomic: one stale frame rejects the whole call.
    let mixed = seq.append(vec![
        frame(7, 1, 10, b"fine"),
        frame(9, 4, 300, b"zombie rider"),
    ]);
    assert!(matches!(mixed, Err(AppendError::WrongEpoch { .. })));

    // The successor epoch is admitted.
    seq.append(vec![frame(9, 6, 400, b"epoch six")])
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(seq.tenant_epoch(9), Some(6));

    seq.close().unwrap();
    let puts = sink.puts.lock().unwrap();
    let mut landed = Vec::new();
    for (_, bytes) in puts.iter() {
        let (_, frames, _) = decode_segment(bytes).unwrap();
        landed.extend(frames.into_iter().map(|f| f.payload));
    }
    assert_eq!(landed, vec![b"epoch five".to_vec(), b"epoch six".to_vec()]);
}

#[test]
fn a_failed_put_fails_the_batch_and_poisons_the_role() {
    let sink = Arc::new(FailSink {
        calls: AtomicUsize::new(0),
    });
    let seq = Sequencer::start(0, Arc::clone(&sink) as _, quick());

    let ticket = seq.append(vec![frame(1, 1, 100, b"doomed")]).unwrap();
    match ticket.wait() {
        Err(AppendError::Store { source }) => {
            assert!(matches!(*source, CasError::AlreadyExists { .. }));
        }
        other => panic!("a lost fence must fail the append: {other:?}"),
    }

    // The role is done: no retry into someone else's chain, and every
    // later append is turned away.
    match seq.append(vec![frame(1, 1, 200, b"after")]) {
        Err(AppendError::Poisoned) => {}
        Err(e) => panic!("wrong rejection: {e}"),
        Ok(_) => panic!("a poisoned sequencer accepted work"),
    }
    seq.close().unwrap();
    assert_eq!(sink.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn consecutive_windows_chain_their_digests() {
    let sink = Arc::new(RecordingSink::default());
    let seq = Sequencer::start(4, Arc::clone(&sink) as _, quick());

    seq.append(vec![frame(1, 1, 100, b"first window")])
        .unwrap()
        .wait()
        .unwrap();
    seq.append(vec![frame(2, 1, 900, b"second window")])
        .unwrap()
        .wait()
        .unwrap();
    seq.close().unwrap();

    let puts = sink.puts.lock().unwrap();
    assert_eq!(puts.len(), 2);
    let (h1, f1) = read_footer(&puts[0].1).unwrap();
    let (h2, _) = read_footer(&puts[1].1).unwrap();
    assert_eq!((h1.seq, h2.seq), (1, 2), "seqs are strictly consecutive");
    assert_eq!(h1.prev_digest, 0);
    assert_eq!(
        h2.prev_digest,
        tenants_digest(&f1.tenants),
        "each header links the previous window's tenant tails"
    );
}

#[test]
fn close_drains_the_open_batch() {
    let sink = Arc::new(RecordingSink::default());
    let config = SequencerConfig {
        window: Duration::from_secs(3600),
        ..SequencerConfig::default()
    };
    let seq = Sequencer::start(0, Arc::clone(&sink) as _, config);
    let ticket = seq.append(vec![frame(1, 1, 50, b"last words")]).unwrap();
    seq.close().unwrap();
    assert_eq!(ticket.wait().unwrap(), Lsn(60));
    assert_eq!(sink.puts.lock().unwrap().len(), 1);
}

/// Blocks the PUT for one seq until released, lets every other seq
/// through, and records entry order, to prove windows land in
/// parallel while acks still wait for chain order.
struct HoldSink {
    hold: u64,
    open: Mutex<bool>,
    open_cv: Condvar,
    entered: Mutex<Vec<u64>>,
    entered_cv: Condvar,
}

impl HoldSink {
    fn new(hold: u64) -> Self {
        Self {
            hold,
            open: Mutex::new(false),
            open_cv: Condvar::new(),
            entered: Mutex::new(Vec::new()),
            entered_cv: Condvar::new(),
        }
    }
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.open_cv.notify_all();
    }
    fn wait_entered(&self, want: &[u64]) {
        let mut entered = self.entered.lock().unwrap();
        while entered.as_slice() != want {
            let (next, timed_out) = self
                .entered_cv
                .wait_timeout(entered, Duration::from_secs(10))
                .unwrap();
            entered = next;
            assert!(
                !timed_out.timed_out(),
                "sink never saw {want:?}: {entered:?}"
            );
        }
    }
}

impl SegmentSink for HoldSink {
    fn put_segment(&self, seq: u64, _segment: &[u8]) -> Result<(), CasError> {
        {
            let mut entered = self.entered.lock().unwrap();
            entered.push(seq);
            self.entered_cv.notify_all();
        }
        if seq == self.hold {
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.open_cv.wait(open).unwrap();
            }
        }
        Ok(())
    }
}

#[test]
fn windows_land_in_parallel_and_ack_in_chain_order() {
    let sink = Arc::new(HoldSink::new(1));
    let config = SequencerConfig {
        window: Duration::from_millis(5),
        ..SequencerConfig::default()
    };
    let seq = Sequencer::start(0, Arc::clone(&sink) as _, config);

    let first = seq.append(vec![frame(1, 1, 100, b"held window")]).unwrap();
    sink.wait_entered(&[1]);
    let second = seq.append(vec![frame(1, 1, 200, b"lands ahead")]).unwrap();
    // Window two reaches the store while window one is still on the
    // wire: that is the pipeline.
    sink.wait_entered(&[1, 2]);

    // Window two's PUT has returned by now, or will shortly, but its
    // ack must sit parked behind the held predecessor.
    std::thread::sleep(Duration::from_millis(60));
    assert!(first.try_wait().is_none(), "held window acked early");
    assert!(
        second.try_wait().is_none(),
        "a window acked over a hole in the chain"
    );

    sink.release();
    assert_eq!(first.wait().unwrap(), Lsn(111));
    assert_eq!(second.wait().unwrap(), Lsn(211));
    seq.close().unwrap();
}

/// Fails the PUT for seq 1, but only after seq 2 has entered, so the
/// failure lands behind a window the store already accepted.
struct FailBehindSink {
    entered: Mutex<Vec<u64>>,
    cv: Condvar,
}

impl SegmentSink for FailBehindSink {
    fn put_segment(&self, seq: u64, _segment: &[u8]) -> Result<(), CasError> {
        let mut entered = self.entered.lock().unwrap();
        entered.push(seq);
        self.cv.notify_all();
        if seq == 1 {
            while !entered.contains(&2) {
                entered = self.cv.wait(entered).unwrap();
            }
            return Err(CasError::AlreadyExists {
                key: "cellwal/0000/0000000000000001".into(),
            });
        }
        Ok(())
    }
}

#[test]
fn a_failed_window_fails_every_window_behind_it() {
    let sink = Arc::new(FailBehindSink {
        entered: Mutex::new(Vec::new()),
        cv: Condvar::new(),
    });
    let seq = Sequencer::start(0, Arc::clone(&sink) as _, quick());

    let first = seq
        .append(vec![frame(1, 1, 100, b"loses the fence")])
        .unwrap();
    // Wait for window one to reach the store so window two goes to a
    // second batch behind it.
    while !sink.entered.lock().unwrap().contains(&1) {
        std::thread::sleep(Duration::from_millis(1));
    }
    let second = seq
        .append(vec![frame(1, 1, 200, b"landed orphan")])
        .unwrap();

    match first.wait() {
        Err(AppendError::Store { source }) => {
            assert!(matches!(*source, CasError::AlreadyExists { .. }))
        }
        other => panic!("the fenced window must fail: {other:?}"),
    }
    // Window two landed on the store, but behind a hole, and calling
    // it durable would be a lie.
    match second.wait() {
        Err(AppendError::Poisoned) => {}
        other => panic!("the orphan behind the hole must fail: {other:?}"),
    }
    match seq.append(vec![frame(1, 1, 300, b"after")]) {
        Err(AppendError::Poisoned) => {}
        Err(e) => panic!("wrong rejection: {e}"),
        Ok(_) => panic!("a poisoned sequencer accepted work"),
    }
    seq.close().unwrap();
}

#[test]
fn close_drains_every_window_in_flight() {
    /// A slow store: every PUT takes a beat, so close has real work.
    struct SlowSink(RecordingSink);
    impl SegmentSink for SlowSink {
        fn put_segment(&self, seq: u64, segment: &[u8]) -> Result<(), CasError> {
            std::thread::sleep(Duration::from_millis(30));
            self.0.put_segment(seq, segment)
        }
    }
    let sink = Arc::new(SlowSink(RecordingSink::default()));
    let config = SequencerConfig {
        window: Duration::from_millis(1),
        ..SequencerConfig::default()
    };
    let seq = Sequencer::start(0, Arc::clone(&sink) as _, config);
    let mut tickets = Vec::new();
    for i in 0..4u64 {
        tickets.push(seq.append(vec![frame(1, 1, 100 * i, b"drained")]).unwrap());
        std::thread::sleep(Duration::from_millis(3));
    }
    seq.close().unwrap();
    for t in &tickets {
        assert!(
            matches!(t.try_wait(), Some(Ok(_))),
            "close returned before a window in flight resolved"
        );
    }
    // A coarse timer can batch two appends into one window, so count
    // frames on the store, not PUTs: close must leave none behind.
    let puts = sink.0.puts.lock().unwrap();
    let landed: usize = puts
        .iter()
        .map(|(_, bytes)| decode_segment(bytes).unwrap().1.len())
        .sum();
    assert_eq!(landed, 4);
}

#[test]
fn the_media_sink_lands_fenced_objects_on_a_real_store() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path()));
    let media = Arc::new(WalMedia::single(Arc::clone(&store)));
    let sink = Arc::new(MediaSink::new(media, 7));
    let seq = Sequencer::resume(7, sink as _, quick(), 42, 0xabc);

    seq.append(vec![frame(3, 2, 700, b"onto the store")])
        .unwrap()
        .wait()
        .unwrap();
    seq.close().unwrap();

    let keys = store.list("cellwal/").unwrap();
    assert_eq!(keys, vec!["cellwal/0007/000000000000002a"]);
    let (bytes, _) = store.get(&keys[0]).unwrap().unwrap();
    let (header, frames, _) = decode_segment(&bytes).unwrap();
    assert_eq!(header.seq, 42);
    assert_eq!(header.prev_digest, 0xabc);
    assert_eq!(frames[0].payload, b"onto the store");

    // The fence: a second sequencer resuming at the same head loses.
    let sink2 = Arc::new(MediaSink::new(Arc::new(WalMedia::single(store)), 7));
    let seq2 = Sequencer::resume(7, sink2 as _, quick(), 42, 0xabc);
    let outcome = seq2
        .append(vec![frame(3, 2, 800, b"zombie")])
        .unwrap()
        .wait();
    assert!(matches!(outcome, Err(AppendError::Store { .. })));
    seq2.close().unwrap();
}

#[test]
fn the_gate_sheds_the_lagging_tenant_and_only_that_tenant() {
    let gate = Arc::new(Backpressure::new(LagBounds {
        ingest_bytes: 1000,
        ingest_secs: 10,
        consolidation_bytes: 5000,
    }));
    let sink = Arc::new(RecordingSink::default());
    let config = SequencerConfig {
        window: Duration::from_millis(5),
        gate: Some(Arc::clone(&gate)),
        ..SequencerConfig::default()
    };
    let seq = Sequencer::start(0, Arc::clone(&sink) as _, config);

    // Tenant 1's page service reports itself over the ingest bound:
    // its appends are refused, its neighbor's commit right through.
    gate.report_ingest(
        1,
        IngestLag {
            bytes: 2000,
            secs: 0,
        },
    );
    let Err(err) = seq.append(vec![frame(1, 1, 0, b"lagging")]) else {
        panic!("a throttled tenant got through");
    };
    assert!(
        matches!(
            err,
            AppendError::Throttled {
                tenant: 1,
                reason: Throttle::IngestBytes { .. }
            }
        ),
        "{err}"
    );
    seq.append(vec![frame(2, 1, 0, b"healthy")])
        .unwrap()
        .wait()
        .unwrap();

    // A WAL shard past the consolidation bound is the cell alarm and
    // refuses everyone, healthy tenants included.
    gate.report_consolidation(0, 9000);
    let Err(err) = seq.append(vec![frame(2, 1, 100, b"held")]) else {
        panic!("the cell alarm let a healthy tenant through");
    };
    assert!(
        matches!(
            err,
            AppendError::Throttled {
                tenant: 2,
                reason: Throttle::Consolidation { .. }
            }
        ),
        "{err}"
    );

    // Reports replace, so recovery lifts both throttles and the
    // delayed commits land: delayed, never lost.
    gate.report_consolidation(0, 0);
    gate.report_ingest(1, IngestLag::default());
    seq.append(vec![frame(1, 1, 100, b"caught up")])
        .unwrap()
        .wait()
        .unwrap();
    seq.append(vec![frame(2, 1, 200, b"resumed")])
        .unwrap()
        .wait()
        .unwrap();
    seq.close().unwrap();
    assert!(!sink.puts.lock().unwrap().is_empty());
}
