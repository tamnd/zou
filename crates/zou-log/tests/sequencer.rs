//! The sequencer's promises, held against test sinks: one PUT per
//! window no matter how many tenants append, acks strictly after the
//! durable PUT returns, stale epochs rejected with the current one,
//! failures poison instead of lying, and idle windows cost nothing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use zou_log::{
    AppendError, MediaSink, SegmentSink, Sequencer, SequencerConfig, WalMedia, decode_segment,
    read_footer, tenants_digest,
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
