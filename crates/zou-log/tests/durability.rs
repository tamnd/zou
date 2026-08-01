//! The durability modes end to end: express dual acks on two media
//! and only two media, the hedge covers a slow AZ without lying, a
//! fence in either bucket fails the batch, takeover and recovery work
//! over dual media, and a half landed loser on one medium is invisible
//! everywhere.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use zou_log::{
    DurabilityMode, MediaSink, SegmentBuilder, SegmentHeader, SegmentKind, Sequencer,
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

struct Cell {
    _dirs: Vec<tempfile::TempDir>,
    az1: Arc<dyn CasStore>,
    az2: Arc<dyn CasStore>,
    standard: Arc<dyn CasStore>,
}

fn cell() -> Cell {
    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let az1: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dirs[0].path()));
    let az2: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dirs[1].path()));
    let standard: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dirs[2].path()));
    Cell {
        _dirs: dirs,
        az1,
        az2,
        standard,
    }
}

fn dual(c: &Cell, hedge_after: Duration) -> Arc<WalMedia> {
    Arc::new(WalMedia::express_dual(
        Arc::clone(&c.az1),
        Arc::clone(&c.az2),
        Arc::clone(&c.standard),
        hedge_after,
    ))
}

fn sequencer(media: &Arc<WalMedia>, shard: u32, t: zou_log::Takeover) -> Sequencer {
    let sink = Arc::new(MediaSink::new(Arc::clone(media), shard));
    Sequencer::resume(shard, sink as _, quick(), t.next_seq, t.prev_digest)
}

/// Delegates to an inner store but holds every put until released, the
/// stand in for a slow AZ.
struct GatedStore {
    inner: Arc<dyn CasStore>,
    open: Mutex<bool>,
    cv: Condvar,
    entered: AtomicBool,
}

impl GatedStore {
    fn new(inner: Arc<dyn CasStore>) -> Self {
        Self {
            inner,
            open: Mutex::new(false),
            cv: Condvar::new(),
            entered: AtomicBool::new(false),
        }
    }
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.cv.notify_all();
    }
    fn hold(&self) {
        let guard = self.open.lock().unwrap();
        let _unused = self.cv.wait_while(guard, |open| !*open).unwrap();
    }
}

impl CasStore for GatedStore {
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
        self.entered.store(true, Ordering::SeqCst);
        self.hold();
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
fn the_mode_serializes_as_the_spec_strings() {
    assert_eq!(
        serde_json::to_string(&DurabilityMode::Standard).unwrap(),
        "\"standard\""
    );
    assert_eq!(
        serde_json::to_string(&DurabilityMode::ExpressDual).unwrap(),
        "\"express-dual\""
    );
    let parsed: DurabilityMode = serde_json::from_str("\"express-dual\"").unwrap();
    assert_eq!(parsed, DurabilityMode::ExpressDual);
}

#[test]
fn express_dual_acks_after_both_azs_and_never_touches_standard() {
    let c = cell();
    let media = dual(&c, Duration::from_secs(3600));
    let shard = 1;

    let t = take_over(&media, shard, "node-a").unwrap();
    let seq = sequencer(&media, shard, t);
    seq.append(vec![frame(1, 100, b"dual landed")])
        .unwrap()
        .wait()
        .unwrap();
    seq.close().unwrap();

    // Both AZ buckets hold identical bytes for every chain position.
    for key in c.az1.list("cellwal/").unwrap() {
        let a = c.az1.get(&key).unwrap().unwrap().0;
        let b = c.az2.get(&key).unwrap().unwrap().0;
        assert_eq!(a, b, "media disagree on {key}");
    }
    // No hedge fired, so Standard holds only the shard manifest.
    let standard_keys = c.standard.list("cellwal/").unwrap();
    assert_eq!(standard_keys.len(), 1);
    assert!(standard_keys[0].ends_with("/manifest"));

    let chain = read_chain(&media, shard, 0).unwrap();
    assert_eq!(chain.len(), 2);
    assert_eq!(chain[1].frames[0].payload, b"dual landed");
}

#[test]
fn a_slow_az_hedges_to_standard_and_the_ack_does_not_wait_for_it() {
    let c = cell();
    let gate = Arc::new(GatedStore::new(Arc::clone(&c.az2)));
    let media = Arc::new(WalMedia::express_dual(
        Arc::clone(&c.az1),
        Arc::clone(&gate) as Arc<dyn CasStore>,
        Arc::clone(&c.standard),
        Duration::from_millis(20),
    ));
    let shard = 3;

    // Bootstrap the chain before gating matters: the takeover seal also
    // goes through az2, so release it for the setup, then re-arm.
    gate.release();
    let t = take_over(&media, shard, "node-a").unwrap();
    *gate.open.lock().unwrap() = false;
    gate.entered.store(false, Ordering::SeqCst);

    let seq = sequencer(&media, shard, t);
    let started = Instant::now();
    seq.append(vec![frame(1, 100, b"hedged commit")])
        .unwrap()
        .wait()
        .unwrap();
    let acked_after = started.elapsed();
    assert!(gate.entered.load(Ordering::SeqCst), "az2 never saw the put");

    // The ack came from az1 plus the Standard hedge while az2 was still
    // stuck, and the segment really is on Standard.
    let landed = segment_key(shard, t.next_seq);
    assert!(c.standard.get(&landed).unwrap().is_some());
    assert!(c.az2.get(&landed).unwrap().is_none());
    assert!(
        acked_after < Duration::from_secs(2),
        "ack waited {acked_after:?} for the gated AZ"
    );

    // Recovery sees the hedged segment: az1 plus Standard is a quorum.
    let chain = read_chain(&media, shard, 0).unwrap();
    assert_eq!(chain.last().unwrap().frames[0].payload, b"hedged commit");
    assert_eq!(chain_head(&media, shard, 0).unwrap(), t.next_seq);

    gate.release();
    seq.close().unwrap();
}

#[test]
fn a_fence_in_one_bucket_fails_the_batch_and_never_acks() {
    let c = cell();
    let media = dual(&c, Duration::from_secs(3600));
    let shard = 5;

    let t = take_over(&media, shard, "node-a").unwrap();
    // Someone else's object already sits at the next seq in az2, the
    // shape a successor's seal has mid takeover.
    c.az2
        .put_if_absent(&segment_key(shard, t.next_seq), b"a rival was here")
        .unwrap();

    let seq = sequencer(&media, shard, t);
    let outcome = seq.append(vec![frame(1, 100, b"doomed")]).unwrap().wait();
    assert!(
        matches!(outcome, Err(zou_log::AppendError::Store { .. })),
        "a fenced bucket must fail the batch"
    );
    seq.close().unwrap();

    // The loser holds at most one medium, so the seq has no owner and
    // recovery never surfaces the doomed write.
    let chain = read_chain(&media, shard, 0).unwrap();
    assert_eq!(chain.len(), 1, "only the takeover seal is in the chain");
}

#[test]
fn takeover_fences_a_live_dual_sequencer_and_seals_two_media() {
    let c = cell();
    let media = dual(&c, Duration::from_secs(3600));
    let shard = 7;

    let ta = take_over(&media, shard, "node-a").unwrap();
    let seq_a = sequencer(&media, shard, ta);
    seq_a
        .append(vec![frame(1, 100, b"before failover")])
        .unwrap()
        .wait()
        .unwrap();

    let tb = take_over(&media, shard, "node-b").unwrap();
    assert_eq!(tb.chain_epoch, 2);
    let seal_key = segment_key(shard, tb.sealed_seq);
    assert!(c.az1.get(&seal_key).unwrap().is_some());
    assert!(c.az2.get(&seal_key).unwrap().is_some());

    let dead = seq_a.append(vec![frame(1, 200, b"zombie")]).unwrap().wait();
    assert!(matches!(dead, Err(zou_log::AppendError::Store { .. })));
    seq_a.close().unwrap();

    let seq_b = sequencer(&media, shard, tb);
    seq_b
        .append(vec![frame(1, 300, b"after failover")])
        .unwrap()
        .wait()
        .unwrap();
    seq_b.close().unwrap();

    let chain = read_chain(&media, shard, 0).unwrap();
    let payloads: Vec<&[u8]> = chain
        .iter()
        .flat_map(|s| &s.frames)
        .map(|f| f.payload.as_slice())
        .collect();
    assert_eq!(
        payloads,
        vec![b"before failover".as_ref(), b"after failover".as_ref()]
    );

    let (m, _) = ShardManifest::load(c.standard.as_ref(), shard)
        .unwrap()
        .unwrap();
    assert_eq!(m.sealed_by, "node-b");
    assert_eq!(m.head, tb.sealed_seq);
}

#[test]
fn a_half_landed_loser_on_one_medium_is_invisible_everywhere() {
    let c = cell();
    let media = dual(&c, Duration::from_secs(3600));
    let shard = 9;

    let t = take_over(&media, shard, "node-a").unwrap();
    let seq = sequencer(&media, shard, t);
    seq.append(vec![frame(1, 100, b"real")])
        .unwrap()
        .wait()
        .unwrap();
    seq.close().unwrap();
    let head = chain_head(&media, shard, 0).unwrap();

    // A zombie's dying PUT reached az1 only. Well formed, correctly
    // linked, one medium: it has no quorum and must not count.
    let prev = media_digest(&media, shard, head);
    let (loser, _) = SegmentBuilder::new(SegmentHeader {
        kind: SegmentKind::Landing,
        shard,
        seq: head + 1,
        prev_digest: prev,
    })
    .finish();
    c.az1
        .put_if_absent(&segment_key(shard, head + 1), &loser)
        .unwrap();

    assert_eq!(chain_head(&media, shard, 0).unwrap(), head);
    assert_eq!(read_chain(&media, shard, 0).unwrap().len(), 2);

    // The next takeover seals right over it: the seal takes az2 plus
    // Standard, two of three, and owns the seq.
    let t2 = take_over(&media, shard, "node-b").unwrap();
    assert_eq!(t2.sealed_seq, head + 1);
    let chain = read_chain(&media, shard, 0).unwrap();
    assert_eq!(chain.last().unwrap().header.kind, SegmentKind::Seal);
    assert_eq!(chain.last().unwrap().seq, head + 1);
}

#[test]
fn a_hedged_segment_with_two_media_survives_takeover() {
    let c = cell();
    let media = dual(&c, Duration::from_secs(3600));
    let shard = 11;

    let t = take_over(&media, shard, "node-a").unwrap();
    let seq = sequencer(&media, shard, t);
    seq.append(vec![frame(1, 100, b"acked on az1 plus standard")])
        .unwrap()
        .wait()
        .unwrap();
    seq.close().unwrap();

    // Rewrite history: drop the az2 copy of the last landing, leaving
    // exactly the state a hedged ack leaves behind, az1 plus Standard.
    let landed = segment_key(shard, t.next_seq);
    let bytes = c.az2.get(&landed).unwrap().unwrap().0;
    c.az2.delete(&landed).unwrap();
    c.standard.put_if_absent(&landed, &bytes).unwrap();

    // The hedged segment still counts: probes see it, takeover adopts
    // it, and the acked write is inside the successor's chain.
    assert_eq!(chain_head(&media, shard, 0).unwrap(), t.next_seq);
    let t2 = take_over(&media, shard, "node-b").unwrap();
    assert_eq!(t2.sealed_seq, t.next_seq + 1);
    let chain = read_chain(&media, shard, 0).unwrap();
    assert_eq!(
        chain[1].frames[0].payload,
        b"acked on az1 plus standard".as_slice()
    );
}

fn media_digest(media: &Arc<WalMedia>, shard: u32, seq: u64) -> u64 {
    let bytes = media.fetch(shard, seq).unwrap().unwrap();
    let (_, footer) = zou_log::read_footer(&bytes).unwrap();
    tenants_digest(&footer.tenants)
}
