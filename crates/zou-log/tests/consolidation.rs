//! Consolidation end to end on a real store: a round folds the landing
//! chain into one sorted sealed segment, catch up reads are range GETs
//! planned from the round index alone, watermarks carry forward and
//! enforce continuity, a crash at the commit point is adopted not
//! redone, and GC deletes landing objects without ever breaking the
//! chain link for takeover or recovery.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use zou_log::{
    ConsolidateError, MediaSink, RoundIndex, Sequencer, SequencerConfig, ShardManifest, WalMedia,
    consolidate, decode_sealed, gc_landing, landing_backlog, read_chain_linked, read_round_tenant,
    round_key, take_over,
};
use zou_store::{CasError, CasStore, Frame2, LocalFsStore, Lsn, Version};

fn frame(tenant: u128, start: u64, body: &[u8]) -> Frame2 {
    Frame2 {
        tenant,
        writer_epoch: 1,
        start_lsn: Lsn(start),
        end_lsn: Lsn(start + body.len() as u64),
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
    let sink = Arc::new(MediaSink::new(Arc::clone(media), shard));
    Sequencer::resume(shard, sink as _, quick(), t.next_seq, t.prev_digest)
}

/// Append frames one window at a time so every call lands durably
/// before the next, keeping the landing chain multi segment.
fn commit(seq: &Sequencer, windows: Vec<Vec<Frame2>>) {
    for frames in windows {
        seq.append(frames).unwrap().wait().unwrap();
    }
}

fn manifest(store: &dyn CasStore, shard: u32) -> ShardManifest {
    ShardManifest::load(store, shard).unwrap().unwrap().0
}

#[test]
fn a_round_folds_the_landing_chain_into_one_sorted_sealed_segment() {
    let dir = tempfile::tempdir().unwrap();
    let (store, media) = store_in(&dir);
    let shard = 3;

    let t = take_over(&media, shard, "node-a").unwrap();
    let seq = resume(&media, shard, t);
    // Two tenants interleaved across three windows, out of tenant
    // order on the wire.
    commit(
        &seq,
        vec![
            vec![frame(2, 500, b"tenant two first"), frame(1, 100, b"one a")],
            vec![frame(1, 105, b"one b")],
            vec![frame(2, 516, b"tenant two second"), frame(1, 110, b"one c")],
        ],
    );
    seq.close().unwrap();

    let out = consolidate(&media, shard).unwrap().unwrap();
    assert_eq!(out.round, 1);
    assert_eq!(out.first_seq, 1);
    assert_eq!(out.frames, 5);
    assert!(!out.adopted);

    // The sealed object holds every frame, sorted by (tenant, lsn).
    let (bytes, _) = store.get(&out.sealed).unwrap().unwrap();
    let (header, frames, footer) = decode_sealed(&bytes).unwrap();
    assert_eq!(header.shard, shard);
    assert_eq!(header.first_seq, 1);
    assert_eq!(header.last_seq, out.last_seq);
    let identity: Vec<(u128, u64)> = frames.iter().map(|f| (f.tenant, f.start_lsn.0)).collect();
    assert_eq!(
        identity,
        vec![(1, 100), (1, 105), (1, 110), (2, 500), (2, 516)]
    );
    assert!(footer.bloom.may_contain(1) && footer.bloom.may_contain(2));

    // The manifest moved to the head with the digest GC will need.
    let m = manifest(store.as_ref(), shard);
    assert_eq!(m.consolidated_upto, out.last_seq);
    assert_ne!(m.consolidated_digest, 0);
    let rounds = m.rounds.unwrap();
    assert_eq!((rounds.first, rounds.last), (1, 1));

    // Nothing new means no round.
    assert!(consolidate(&media, shard).unwrap().is_none());
}

#[test]
fn catch_up_is_range_gets_planned_from_the_round_index_alone() {
    let dir = tempfile::tempdir().unwrap();
    let (store, media) = store_in(&dir);
    let shard = 4;

    let t = take_over(&media, shard, "node-a").unwrap();
    let seq = resume(&media, shard, t);
    commit(
        &seq,
        vec![
            vec![frame(7, 1000, b"seven one"), frame(9, 2000, b"nine one")],
            vec![frame(7, 1009, b"seven two")],
        ],
    );
    seq.close().unwrap();
    consolidate(&media, shard).unwrap().unwrap();

    let index = RoundIndex::load(store.as_ref(), shard, 1).unwrap().unwrap();
    let got = read_round_tenant(store.as_ref(), &index, 7).unwrap();
    let identity: Vec<(u64, Vec<u8>)> = got
        .iter()
        .map(|f| (f.start_lsn.0, f.payload.clone()))
        .collect();
    assert_eq!(
        identity,
        vec![(1000, b"seven one".to_vec()), (1009, b"seven two".to_vec())]
    );
    // A tenant the shard never saw reads back empty, no error.
    assert!(
        read_round_tenant(store.as_ref(), &index, 42)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn watermarks_carry_forward_and_idle_tenants_keep_theirs() {
    let dir = tempfile::tempdir().unwrap();
    let (store, media) = store_in(&dir);
    let shard = 5;

    let t = take_over(&media, shard, "node-a").unwrap();
    let seq = resume(&media, shard, t);
    commit(
        &seq,
        vec![vec![frame(1, 100, b"one"), frame(2, 900, b"two first")]],
    );
    consolidate(&media, shard).unwrap().unwrap();

    // Round two: only tenant 2 writes, resuming exactly at its
    // watermark.
    commit(&seq, vec![vec![frame(2, 909, b"two second")]]);
    seq.close().unwrap();
    let out = consolidate(&media, shard).unwrap().unwrap();
    assert_eq!(out.round, 2);

    let index = RoundIndex::load(store.as_ref(), shard, 2).unwrap().unwrap();
    let t1 = index.tenants.iter().find(|t| t.tenant == 1).unwrap();
    assert_eq!(t1.watermark, 103);
    assert_eq!(t1.frames, 0);
    assert!(t1.chunks.is_empty());
    let t2 = index.tenants.iter().find(|t| t.tenant == 2).unwrap();
    assert_eq!(t2.watermark, 919);
    assert_eq!(t2.frames, 1);
    assert!(
        read_round_tenant(store.as_ref(), &index, 1)
            .unwrap()
            .is_empty()
    );
}

/// Delegates everything, but the manifest CAS fails with an io error
/// while armed, which is a crash at the exact commit point of a round.
struct CrashAtPublish {
    inner: Arc<dyn CasStore>,
    armed: AtomicBool,
}

impl CasStore for CrashAtPublish {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        self.inner.get(key)
    }
    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        if key.ends_with("manifest") && self.armed.swap(false, Ordering::SeqCst) {
            return Err(CasError::Io {
                key: key.to_string(),
                source: std::io::Error::other("crashed at the commit point"),
            });
        }
        self.inner.put_if_match(key, data, expected)
    }
    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
        self.inner.get_range(key, offset, len)
    }
    fn delete(&self, key: &str) -> Result<(), CasError> {
        self.inner.delete(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        self.inner.list(prefix)
    }
}

#[test]
fn a_round_that_crashed_at_the_commit_point_is_adopted_not_redone() {
    let dir = tempfile::tempdir().unwrap();
    let inner: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path()));
    let crash = Arc::new(CrashAtPublish {
        inner: Arc::clone(&inner),
        armed: AtomicBool::new(false),
    });
    let store: Arc<dyn CasStore> = crash.clone();
    let media = Arc::new(WalMedia::single(Arc::clone(&store)));
    let shard = 6;

    let t = take_over(&media, shard, "node-a").unwrap();
    let seq = resume(&media, shard, t);
    commit(&seq, vec![vec![frame(1, 100, b"before the crash")]]);
    seq.close().unwrap();

    // The round writes its sealed segment and index, then dies on the
    // manifest CAS.
    crash.armed.store(true, Ordering::SeqCst);
    let err = consolidate(&media, shard).unwrap_err();
    assert!(matches!(err, ConsolidateError::Store(CasError::Io { .. })));
    assert!(store.get(&round_key(shard, 1)).unwrap().is_some());
    assert_eq!(manifest(store.as_ref(), shard).consolidated_upto, 0);

    // The rerun finds the finished round and publishes it instead of
    // consolidating the same range again.
    let out = consolidate(&media, shard).unwrap().unwrap();
    assert!(out.adopted);
    assert_eq!(out.round, 1);
    assert_eq!(out.frames, 1);
    let m = manifest(store.as_ref(), shard);
    assert_eq!(m.consolidated_upto, out.last_seq);
    assert_eq!(m.rounds.unwrap().last, 1);

    // And the shard is fully caught up.
    assert!(consolidate(&media, shard).unwrap().is_none());
}

#[test]
fn an_lsn_gap_refuses_the_round_and_publishes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (store, media) = store_in(&dir);
    let shard = 7;

    let t = take_over(&media, shard, "node-a").unwrap();
    let seq = resume(&media, shard, t);
    commit(&seq, vec![vec![frame(1, 100, b"ten bytes.")]]);
    consolidate(&media, shard).unwrap().unwrap();

    // The tenant resumes past its watermark of 110: a window went
    // missing somewhere and consolidating over the hole would bake it
    // in.
    commit(&seq, vec![vec![frame(1, 200, b"after a hole")]]);
    seq.close().unwrap();
    let err = consolidate(&media, shard).unwrap_err();
    match err {
        ConsolidateError::Discontinuity {
            tenant,
            expect,
            found,
        } => {
            assert_eq!((tenant, expect, found), (1, 110, 200));
        }
        e => panic!("expected a discontinuity, got {e}"),
    }
    assert!(store.get(&round_key(shard, 2)).unwrap().is_none());
    assert_eq!(
        manifest(store.as_ref(), shard).rounds.unwrap().last,
        1,
        "the failed round must publish nothing"
    );
}

#[test]
fn a_writer_retry_below_the_watermark_is_dropped_as_a_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let (store, media) = store_in(&dir);
    let shard = 8;

    let t = take_over(&media, shard, "node-a").unwrap();
    let seq = resume(&media, shard, t);
    commit(&seq, vec![vec![frame(1, 100, b"ten bytes.")]]);
    consolidate(&media, shard).unwrap().unwrap();

    // A failed over compute resends its last durable frame before the
    // new one, the spec's idempotent retry.
    commit(
        &seq,
        vec![vec![frame(1, 100, b"ten bytes."), frame(1, 110, b"fresh")]],
    );
    seq.close().unwrap();
    let out = consolidate(&media, shard).unwrap().unwrap();
    assert_eq!(out.frames, 1, "the duplicate must not be sealed twice");

    let index = RoundIndex::load(store.as_ref(), shard, 2).unwrap().unwrap();
    let got = read_round_tenant(store.as_ref(), &index, 1).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].start_lsn, Lsn(110));
    let t1 = index.tenants.iter().find(|t| t.tenant == 1).unwrap();
    assert_eq!(t1.watermark, 115);
}

#[test]
fn gc_honors_the_grace_window_and_the_chain_survives_the_deletions() {
    // Express dual, so GC must scrub all three media and the post GC
    // takeover and recovery paths must work against quorum reads.
    let az1 = tempfile::tempdir().unwrap();
    let az2 = tempfile::tempdir().unwrap();
    let std_dir = tempfile::tempdir().unwrap();
    let az1_store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(az1.path()));
    let az2_store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(az2.path()));
    let standard: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(std_dir.path()));
    let media = Arc::new(WalMedia::express_dual(
        Arc::clone(&az1_store),
        Arc::clone(&az2_store),
        Arc::clone(&standard),
        Duration::from_secs(3600),
    ));
    let shard = 9;

    let ta = take_over(&media, shard, "node-a").unwrap();
    let seq = resume(&media, shard, ta);
    commit(
        &seq,
        vec![
            vec![frame(1, 100, b"window one")],
            vec![frame(1, 110, b"window two")],
        ],
    );
    let out = consolidate(&media, shard).unwrap().unwrap();

    // Inside the grace window nothing is deletable.
    assert_eq!(
        gc_landing(&media, shard, Duration::from_secs(3600)).unwrap(),
        0
    );
    assert!(media.present(shard, out.last_seq).unwrap());

    // Past the window every consolidated position goes, on every
    // medium.
    let deleted = gc_landing(&media, shard, Duration::ZERO).unwrap();
    assert_eq!(deleted, out.last_seq - out.first_seq + 1);
    for s in out.first_seq..=out.last_seq {
        assert!(!media.present(shard, s).unwrap());
        assert!(media.fetch(shard, s).unwrap().is_none());
    }
    let m = manifest(standard.as_ref(), shard);
    assert_eq!(m.gc_round, 1);

    // Recovery enters the chain at the boundary through the manifest's
    // digest and sees exactly nothing pending.
    assert!(
        read_chain_linked(&media, shard, m.consolidated_upto, m.consolidated_digest)
            .unwrap()
            .is_empty()
    );

    // A takeover lands its seal right on top of the deleted history,
    // fencing the incumbent, and the successor's writes chain cleanly.
    let tb = take_over(&media, shard, "node-b").unwrap();
    assert_eq!(tb.sealed_seq, m.consolidated_upto + 1);
    assert!(
        seq.append(vec![frame(1, 120, b"zombie")])
            .unwrap()
            .wait()
            .is_err()
    );
    seq.close().unwrap();
    let seq_b = resume(&media, shard, tb);
    commit(&seq_b, vec![vec![frame(1, 120, b"after failover")]]);
    seq_b.close().unwrap();

    let chain =
        read_chain_linked(&media, shard, m.consolidated_upto, m.consolidated_digest).unwrap();
    assert_eq!(chain.len(), 2, "the seal and the successor's window");

    // The next round folds the post GC tail and continuity holds
    // across the whole cycle.
    let out2 = consolidate(&media, shard).unwrap().unwrap();
    assert_eq!(out2.round, 2);
    assert_eq!(out2.first_seq, m.consolidated_upto + 1);
    let index = RoundIndex::load(standard.as_ref(), shard, 2)
        .unwrap()
        .unwrap();
    let got = read_round_tenant(standard.as_ref(), &index, 1).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].payload, b"after failover");
}

#[test]
fn the_backlog_gauge_tracks_unconsolidated_bytes_down_to_zero() {
    let dir = tempfile::tempdir().unwrap();
    let (store, media) = store_in(&dir);
    let shard = 5;
    assert_eq!(
        landing_backlog(&media, shard).unwrap(),
        0,
        "a shard that never landed anything owes nothing"
    );

    let t = take_over(&media, shard, "node-a").unwrap();
    let seq = resume(&media, shard, t);
    commit(&seq, vec![vec![frame(1, 100, b"first window")]]);
    let small = landing_backlog(&media, shard).unwrap();
    assert!(small > 0, "landed bytes count until a round folds them");

    commit(&seq, vec![vec![frame(1, 112, b"second window")]]);
    seq.close().unwrap();
    let grown = landing_backlog(&media, shard).unwrap();
    assert!(grown > small, "the gauge grows with the chain");

    consolidate(&media, shard).unwrap().unwrap();
    assert_eq!(
        landing_backlog(&media, shard).unwrap(),
        0,
        "a published round moves the boundary and clears the gauge"
    );
    let _ = store;
}
