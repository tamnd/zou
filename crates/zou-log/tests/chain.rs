//! The chain protocol end to end on a real store: takeover fences a
//! live sequencer through the seal segment, acked writes are always
//! inside the chain the successor adopts, a crash between seal and
//! manifest CAS wedges nothing, racing successors leave one clean
//! chain, and head probes cost log, not linear.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use zou_log::{
    AppendError, ChainError, MediaSink, SegmentBuilder, SegmentHeader, SegmentKind, Sequencer,
    SequencerConfig, ShardManifest, WalMedia, chain_head, read_chain, segment_key, take_over,
    tenants_digest,
};
use zou_store::{CasError, CasStore, Frame2, LocalFsStore, Lsn, Version};

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

fn quick() -> SequencerConfig {
    SequencerConfig {
        window: Duration::from_millis(5),
        ..SequencerConfig::default()
    }
}

fn store_in(dir: &tempfile::TempDir) -> (Arc<dyn CasStore>, Arc<WalMedia>) {
    let store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path()));
    let media = Arc::new(WalMedia::single(Arc::clone(&store)));
    (store, media)
}

fn resume(media: &Arc<WalMedia>, shard: u32, t: zou_log::Takeover) -> Sequencer {
    let sink = Arc::new(MediaSink::new(Arc::clone(media), shard, t.sealed_seq));
    Sequencer::resume(shard, sink as _, quick(), t.next_seq, t.prev_digest)
}

#[test]
fn bootstrap_takeover_on_an_empty_shard() {
    let dir = tempfile::tempdir().unwrap();
    let (store, media) = store_in(&dir);

    let t = take_over(&media, 5, "node-a").unwrap();
    assert_eq!(t.chain_epoch, 1);
    assert_eq!(t.sealed_seq, 1);
    assert_eq!(t.next_seq, 2);
    assert_eq!(t.prev_digest, tenants_digest(&[]));

    let (m, _) = ShardManifest::load(store.as_ref(), 5).unwrap().unwrap();
    assert_eq!(m.chain_epoch, 1);
    assert_eq!(m.head, 1);
    assert_eq!(m.sealed_by, "node-a");

    let chain = read_chain(&media, 5, 0).unwrap();
    assert_eq!(chain.len(), 1);
    assert_eq!(chain[0].header.kind, SegmentKind::Seal);
    assert!(chain[0].frames.is_empty());
}

#[test]
fn the_seal_fences_a_live_sequencer_and_acked_writes_survive_takeover() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, media) = store_in(&dir);
    let shard = 9;

    // Node a takes the shard and commits three windows.
    let ta = take_over(&media, shard, "node-a").unwrap();
    let seq_a = resume(&media, shard, ta);
    let mut acked = Vec::new();
    for (i, body) in [b"first".as_ref(), b"second", b"third"].iter().enumerate() {
        let lsn = 100 * (i as u64 + 1);
        seq_a
            .append(vec![frame(1, lsn, body)])
            .unwrap()
            .wait()
            .unwrap();
        acked.push((Lsn(lsn), body.to_vec()));
    }

    // Node b takes over while a still believes it holds the role.
    let tb = take_over(&media, shard, "node-b").unwrap();
    assert_eq!(tb.chain_epoch, 2);
    assert_eq!(tb.sealed_seq, ta.next_seq + 3);

    // Node a's next window lands on the seal's seq, loses the creation
    // race, and the append fails instead of acking a lie.
    let err = seq_a
        .append(vec![frame(1, 999, b"zombie write")])
        .unwrap()
        .wait()
        .unwrap_err();
    assert!(matches!(err, AppendError::Store { .. }), "got {err}");
    // The role is poisoned for good.
    match seq_a.append(vec![frame(1, 1000, b"again")]) {
        Err(AppendError::Poisoned) => {}
        Err(e) => panic!("expected poisoned, got {e}"),
        Ok(_) => panic!("expected poisoned, the append was admitted"),
    }
    seq_a.close().unwrap();

    // Node b commits on the chain it adopted.
    let seq_b = resume(&media, shard, tb);
    seq_b
        .append(vec![frame(2, 5000, b"the successor writes")])
        .unwrap()
        .wait()
        .unwrap();
    seq_b.close().unwrap();

    // The whole chain reads back with every digest link intact, every
    // acked write from a is inside it, and the zombie write is not.
    let chain = read_chain(&media, shard, 0).unwrap();
    let landed: Vec<&Frame2> = chain.iter().flat_map(|s| &s.frames).collect();
    for (lsn, body) in &acked {
        assert!(
            landed
                .iter()
                .any(|f| f.start_lsn == *lsn && &f.payload == body),
            "acked write at {lsn:?} missing from the adopted chain"
        );
    }
    assert!(landed.iter().all(|f| f.payload != b"zombie write"));
    let seals: Vec<u64> = chain
        .iter()
        .filter(|s| s.header.kind == SegmentKind::Seal)
        .map(|s| s.seq)
        .collect();
    assert_eq!(seals, vec![ta.sealed_seq, tb.sealed_seq]);
}

#[test]
fn a_crash_between_seal_and_manifest_cas_wedges_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (store, media) = store_in(&dir);
    let shard = 2;

    let ta = take_over(&media, shard, "node-a").unwrap();
    let seq_a = resume(&media, shard, ta);
    seq_a
        .append(vec![frame(1, 10, b"before the crash")])
        .unwrap()
        .wait()
        .unwrap();
    seq_a.close().unwrap();

    // A successor seals the head and dies before touching the manifest.
    let head = chain_head(&media, shard, 0).unwrap();
    let orphan_seal = head + 1;
    let (bytes, _) = store.get(&segment_key(shard, head)).unwrap().unwrap();
    let (_, footer) = zou_log::read_footer(&bytes).unwrap();
    let (seal, _) = SegmentBuilder::new(SegmentHeader {
        kind: SegmentKind::Seal,
        shard,
        seq: orphan_seal,
        prev_digest: tenants_digest(&footer.tenants),
    })
    .finish();
    store
        .put_if_absent(&segment_key(shard, orphan_seal), &seal)
        .unwrap();

    // The next takeover probes past the orphan and carries on.
    let tb = take_over(&media, shard, "node-b").unwrap();
    assert_eq!(tb.sealed_seq, orphan_seal + 1);
    assert_eq!(tb.chain_epoch, 2);

    let seq_b = resume(&media, shard, tb);
    seq_b
        .append(vec![frame(1, 20, b"after recovery")])
        .unwrap()
        .wait()
        .unwrap();
    seq_b.close().unwrap();

    // The chain reads clean across the orphan seal.
    let chain = read_chain(&media, shard, 0).unwrap();
    assert_eq!(chain.last().unwrap().frames[0].payload, b"after recovery");
}

#[test]
fn racing_successors_leave_one_clean_chain() {
    let dir = tempfile::tempdir().unwrap();
    let (store, media) = store_in(&dir);
    let shard = 4;

    let racers: Vec<_> = (0..4)
        .map(|i| {
            let media = Arc::clone(&media);
            std::thread::spawn(move || take_over(&media, shard, &format!("racer-{i}")))
        })
        .collect();
    let mut wins = 0;
    for r in racers {
        match r.join().unwrap() {
            Ok(_) => wins += 1,
            Err(ChainError::Contested { .. }) => {}
            Err(e) => panic!("takeover failed for a reason besides losing: {e}"),
        }
    }
    assert!(wins >= 1);

    // One linear chain of seals with every link intact. A racer that
    // lost the manifest CAS may have left an extra seal behind, which
    // is fine, a seal is just a fence post, but the manifest must
    // never overshoot the store: probing from its head has to land on
    // the true head.
    let chain = read_chain(&media, shard, 0).unwrap();
    assert!(chain.len() >= wins);
    assert!(chain.iter().all(|s| s.header.kind == SegmentKind::Seal));
    let true_head = chain_head(&media, shard, 0).unwrap();
    let (m, _) = ShardManifest::load(store.as_ref(), shard).unwrap().unwrap();
    assert!(m.head <= true_head);
    assert_eq!(chain_head(&media, shard, m.head).unwrap(), true_head);
}

/// Counts existence probes so the head search can prove it gallops.
struct CountingStore {
    inner: Arc<dyn CasStore>,
    range_gets: AtomicUsize,
}

impl CasStore for CountingStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        self.inner.get(key)
    }
    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
        self.range_gets.fetch_add(1, Ordering::Relaxed);
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
    fn delete(&self, key: &str) -> Result<(), CasError> {
        self.inner.delete(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        self.inner.list(prefix)
    }
}

#[test]
fn head_probes_cost_log_in_the_manifest_lag_not_linear() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _media) = store_in(&dir);
    let shard = 6;

    // A long lived sequencer landed 200 windows since the last takeover
    // wrote the manifest.
    let mut prev_digest = 0;
    for seq in 1..=200u64 {
        let (bytes, summaries) = SegmentBuilder::new(SegmentHeader {
            kind: SegmentKind::Landing,
            shard,
            seq,
            prev_digest,
        })
        .finish();
        store
            .put_if_absent(&segment_key(shard, seq), &bytes)
            .unwrap();
        prev_digest = tenants_digest(&summaries);
    }

    let counting = Arc::new(CountingStore {
        inner: Arc::clone(&store),
        range_gets: AtomicUsize::new(0),
    });
    let counted = WalMedia::single(Arc::clone(&counting) as Arc<dyn CasStore>);
    assert_eq!(chain_head(&counted, shard, 0).unwrap(), 200);
    // Log probes for the boundary plus a backscan bounded by the
    // inflight cap to pin the first hole under a pipelined flusher.
    // The point stands: cost grows with the log of the manifest lag,
    // not the lag itself.
    let probes = counting.range_gets.load(Ordering::Relaxed);
    let bound = 20 + zou_log::MAX_INFLIGHT;
    assert!(
        probes <= bound,
        "{probes} probes to find seq 200, the bound is {bound}"
    );
}

/// Plant a well formed landing segment at a seq, digest links be
/// damned, the shape a pipelined flusher's straggler has after a
/// crash landed n+1 without n.
fn plant_landing(store: &Arc<dyn CasStore>, shard: u32, seq: u64, prev_digest: u64) {
    let (bytes, _) = SegmentBuilder::new(SegmentHeader {
        kind: SegmentKind::Landing,
        shard,
        seq,
        prev_digest,
    })
    .finish();
    store
        .put_if_absent(&segment_key(shard, seq), &bytes)
        .unwrap();
}

#[test]
fn takeover_stops_at_a_crash_hole_and_sweeps_the_stragglers() {
    let dir = tempfile::tempdir().unwrap();
    let (store, media) = store_in(&dir);
    let shard = 11;

    // A pipelined flusher crashed mid flight: windows 1 and 2 landed,
    // 3 never made it, 4 and 6 did.
    plant_landing(&store, shard, 1, 0);
    let (bytes, _) = store.get(&segment_key(shard, 1)).unwrap().unwrap();
    let (_, footer) = zou_log::read_footer(&bytes).unwrap();
    plant_landing(&store, shard, 2, tenants_digest(&footer.tenants));
    plant_landing(&store, shard, 4, 0xdead);
    plant_landing(&store, shard, 6, 0xbeef);

    // The head is the seq before the hole, not the highest object.
    let t = take_over(&media, shard, "node-b").unwrap();
    assert_eq!(t.sealed_seq, 3, "the seal goes into the hole");
    assert_eq!(t.next_seq, 4);

    // The stragglers are gone, so the resumed sequencer lands at 4
    // without reading its own dead windows as a rival's fence.
    assert!(store.get(&segment_key(shard, 4)).unwrap().is_none());
    assert!(store.get(&segment_key(shard, 6)).unwrap().is_none());
    let seq = resume(&media, shard, t);
    seq.append(vec![frame(1, 10, b"after the crash")])
        .unwrap()
        .wait()
        .unwrap();
    seq.close().unwrap();

    let chain = read_chain(&media, shard, 0).unwrap();
    assert_eq!(chain.last().unwrap().frames[0].payload, b"after the crash");
}

#[test]
fn the_straggler_sweep_spares_a_rival_seal() {
    let dir = tempfile::tempdir().unwrap();
    let (store, media) = store_in(&dir);
    let shard = 12;

    // A crash hole at 2, and a rival's seal already sitting at 3.
    plant_landing(&store, shard, 1, 0);
    let (seal, _) = SegmentBuilder::new(SegmentHeader {
        kind: SegmentKind::Seal,
        shard,
        seq: 3,
        prev_digest: 0x5ea1,
    })
    .finish();
    store.put_if_absent(&segment_key(shard, 3), &seal).unwrap();

    let t = take_over(&media, shard, "node-c").unwrap();
    assert_eq!(t.sealed_seq, 2);
    // The rival's seal survives the sweep: deleting it would hand the
    // rival's chain a hole under writes it already acked.
    assert!(store.get(&segment_key(shard, 3)).unwrap().is_some());
}

#[test]
fn a_segment_that_does_not_link_ends_the_chain_read_rather_than_breaking_it() {
    let dir = tempfile::tempdir().unwrap();
    let (store, media) = store_in(&dir);
    let shard = 8;

    let t = take_over(&media, shard, "node-a").unwrap();
    let seq = resume(&media, shard, t);
    seq.append(vec![frame(1, 10, b"good")])
        .unwrap()
        .wait()
        .unwrap();
    seq.close().unwrap();

    // Plant a well formed segment at the next seq with a digest that
    // links to nothing, the shape a fenced writer's late landing put
    // has when a freeze let it out after the sweep.
    let head = chain_head(&media, shard, 0).unwrap();
    let (bytes, _) = SegmentBuilder::new(SegmentHeader {
        kind: SegmentKind::Landing,
        shard,
        seq: head + 1,
        prev_digest: 0xdead_beef,
    })
    .finish();
    store
        .put_if_absent(&segment_key(shard, head + 1), &bytes)
        .unwrap();

    // The chain is what links, so it ends at the last segment that
    // does. The plant is not in it, and recovery is not refused over
    // something nobody was ever told was durable.
    let chain = read_chain(&media, shard, 0).expect("the chain reads up to the plant");
    assert_eq!(chain.last().unwrap().seq, head);
    assert_eq!(chain.last().unwrap().frames[0].payload, b"good");

    // A segment at the wrong key is a different thing: the object under
    // that seq is not the segment it says it is, and no writer of ours
    // produces that. It stays an error.
    let (bytes, _) = SegmentBuilder::new(SegmentHeader {
        kind: SegmentKind::Landing,
        shard,
        seq: head + 9,
        prev_digest: tenants_digest(&chain.last().unwrap().footer.tenants),
    })
    .finish();
    store
        .delete(&segment_key(shard, head + 1))
        .expect("clear the plant");
    store
        .put_if_absent(&segment_key(shard, head + 1), &bytes)
        .unwrap();
    match read_chain(&media, shard, 0) {
        Err(ChainError::BrokenLink { seq, .. }) => assert_eq!(seq, head + 1),
        other => panic!("expected a broken link, got {other:?}"),
    }
}
