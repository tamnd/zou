//! The tee end to end on a real store: frames reach subscribers only
//! after the window is durable, page shard subscriptions split a
//! tenant's stream into disjoint stripes with hintless frames
//! broadcast, a slow subscriber is cut without stalling commits, and a
//! subscriber that fell behind catches up from the sealed rounds and
//! rejoins losing nothing and duplicating nothing.

use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use zou_log::{
    MediaSink, SegmentSink, Sequencer, SequencerConfig, Tee, TeeEvent, TeeFilter, catch_up,
    consolidate, gc_landing, take_over,
};
use zou_store::{BlockRef, CasError, CasStore, Frame2, LocalFsStore, Lsn};

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

fn hinted(tenant: u128, start: u64, body: &[u8], hints: Vec<BlockRef>) -> Frame2 {
    Frame2 {
        hints,
        ..frame(tenant, start, body)
    }
}

fn config(tee: &Arc<Tee>) -> SequencerConfig {
    SequencerConfig {
        window: Duration::from_millis(5),
        tee: Some(Arc::clone(tee)),
        ..SequencerConfig::default()
    }
}

fn store_in(dir: &tempfile::TempDir) -> (Arc<dyn CasStore>, Arc<zou_log::WalMedia>) {
    let store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path()));
    let media = Arc::new(zou_log::WalMedia::single(Arc::clone(&store)));
    (store, media)
}

fn resume(
    media: &Arc<zou_log::WalMedia>,
    shard: u32,
    t: zou_log::Takeover,
    tee: &Arc<Tee>,
) -> Sequencer {
    let sink = Arc::new(MediaSink::new(Arc::clone(media), shard));
    Sequencer::resume(shard, sink as _, config(tee), t.next_seq, t.prev_digest)
}

fn commit(seq: &Sequencer, windows: Vec<Vec<Frame2>>) {
    for frames in windows {
        seq.append(frames).unwrap().wait().unwrap();
    }
}

/// The lsn identities of every frame in a window stream, for equality
/// checks that survive frame cloning.
fn identities(frames: &[Frame2]) -> Vec<(u128, u64)> {
    frames.iter().map(|f| (f.tenant, f.start_lsn.0)).collect()
}

/// A sink that parks inside `put_segment` until the test releases it,
/// so the test can observe the exact moment between "PUT in flight"
/// and "PUT returned".
struct GatedSink {
    inner: MediaSink,
    entered: Sender<()>,
    release: Mutex<Receiver<()>>,
}

impl SegmentSink for GatedSink {
    fn put_segment(&self, seq: u64, segment: &[u8]) -> Result<(), CasError> {
        self.entered.send(()).unwrap();
        self.release.lock().unwrap().recv().unwrap();
        self.inner.put_segment(seq, segment)
    }
}

#[test]
fn frames_reach_subscribers_only_after_the_window_is_durable() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, media) = store_in(&dir);
    let shard = 1;
    let tee = Arc::new(Tee::new());
    let sub = tee.subscribe(TeeFilter::Tenant(7), zou_log::DEFAULT_TEE_BUFFER);

    let t = take_over(&media, shard, "node-a").unwrap();
    let (entered_tx, entered) = channel();
    let (release, release_rx) = channel();
    let sink = Arc::new(GatedSink {
        inner: MediaSink::new(Arc::clone(&media), shard),
        entered: entered_tx,
        release: Mutex::new(release_rx),
    });
    let seq = Sequencer::resume(shard, sink as _, config(&tee), t.next_seq, t.prev_digest);

    let ticket = seq.append(vec![frame(7, 100, b"hello")]).unwrap();
    entered.recv().unwrap();
    // The PUT is in flight and has not returned: nothing may be
    // observable on the tee, an unacked frame must not leak.
    assert!(sub.try_recv().is_none());
    release.send(()).unwrap();
    assert_eq!(ticket.wait().unwrap(), Lsn(105));

    match sub.recv().unwrap() {
        TeeEvent::Window { seq: s, frames } => {
            assert_eq!(s, t.next_seq);
            assert_eq!(identities(&frames), vec![(7, 100)]);
        }
        other => panic!("expected the durable window, got {other:?}"),
    }
    drop(seq);
}

/// A sink that fails once: the poisoned window must never appear on
/// the tee, because its frames were never durable.
struct FailOnceSink {
    inner: MediaSink,
    armed: std::sync::atomic::AtomicBool,
}

impl SegmentSink for FailOnceSink {
    fn put_segment(&self, seq: u64, segment: &[u8]) -> Result<(), CasError> {
        if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
            return Err(CasError::Conflict {
                key: format!("seq {seq}"),
            });
        }
        self.inner.put_segment(seq, segment)
    }
}

#[test]
fn a_failed_put_publishes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, media) = store_in(&dir);
    let shard = 2;
    let tee = Arc::new(Tee::new());
    let sub = tee.subscribe(TeeFilter::Tenant(7), zou_log::DEFAULT_TEE_BUFFER);

    let t = take_over(&media, shard, "node-a").unwrap();
    let sink = Arc::new(FailOnceSink {
        inner: MediaSink::new(Arc::clone(&media), shard),
        armed: std::sync::atomic::AtomicBool::new(true),
    });
    let seq = Sequencer::resume(shard, sink as _, config(&tee), t.next_seq, t.prev_digest);

    assert!(
        seq.append(vec![frame(7, 100, b"never")])
            .unwrap()
            .wait()
            .is_err()
    );
    drop(seq);
    assert!(sub.try_recv().is_none());
}

#[test]
fn page_shard_stripes_are_disjoint_and_hintless_frames_broadcast() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, media) = store_in(&dir);
    let shard = 3;
    let tenant = 9;
    let shard_count = 4;
    let tee = Arc::new(Tee::new());
    let stripes: Vec<_> = (0..shard_count)
        .map(|s| {
            tee.subscribe(
                TeeFilter::PageShard {
                    tenant,
                    shard: s,
                    shard_count,
                },
                zou_log::DEFAULT_TEE_BUFFER,
            )
        })
        .collect();
    let full = tee.subscribe(TeeFilter::Tenant(tenant), zou_log::DEFAULT_TEE_BUFFER);

    let t = take_over(&media, shard, "node-a").unwrap();
    let seq = resume(&media, shard, t, &tee);

    let refs: Vec<BlockRef> = (0..16u32)
        .map(|i| BlockRef {
            relfilenode: 1000 + i,
            fork: 0,
            block: i * 100_000,
        })
        .collect();
    let mut windows = Vec::new();
    for (i, r) in refs.iter().enumerate() {
        windows.push(vec![hinted(
            tenant,
            100 + 10 * i as u64,
            b"hinted",
            vec![*r],
        )]);
    }
    // One frame without hints, the full stream fallback, and one frame
    // of another tenant that must reach nobody here.
    windows.push(vec![frame(tenant, 900, b"no hints")]);
    windows.push(vec![frame(42, 100, b"other tenant")]);
    commit(&seq, windows);
    drop(seq);

    let mut striped: Vec<Vec<(u128, u64)>> = vec![Vec::new(); shard_count as usize];
    for (s, sub) in stripes.iter().enumerate() {
        while let Some(TeeEvent::Window { frames, .. }) = sub.try_recv() {
            striped[s].extend(identities(&frames));
        }
    }
    // Every hinted frame landed on exactly the stripe its block ref
    // hashes to and no other, so the stripes are disjoint.
    for (i, r) in refs.iter().enumerate() {
        let want = r.page_shard(shard_count) as usize;
        let lsn = 100 + 10 * i as u64;
        for (s, got) in striped.iter().enumerate() {
            assert_eq!(
                got.contains(&(tenant, lsn)),
                s == want,
                "ref {i} expected only on stripe {want}"
            );
        }
    }
    // The hintless frame reached every stripe, the foreign tenant none.
    for got in &striped {
        assert!(got.contains(&(tenant, 900)));
        assert!(!got.iter().any(|(t, _)| *t == 42));
    }
    // The tenant subscription saw the whole stream in order.
    let mut all = Vec::new();
    while let Some(TeeEvent::Window { frames, .. }) = full.try_recv() {
        all.extend(identities(&frames));
    }
    let mut want: Vec<(u128, u64)> = (0..16).map(|i| (tenant, 100 + 10 * i)).collect();
    want.push((tenant, 900));
    assert_eq!(all, want);
}

#[test]
fn a_slow_subscriber_is_cut_with_lagged_while_acks_keep_flowing() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, media) = store_in(&dir);
    let shard = 4;
    let tee = Arc::new(Tee::new());
    // Room for one small window and not two. The subscriber never
    // reads, so the second matching window must cut it.
    let sub = tee.subscribe(TeeFilter::Tenant(7), 100);

    let t = take_over(&media, shard, "node-a").unwrap();
    let seq = resume(&media, shard, t, &tee);
    commit(
        &seq,
        vec![
            vec![frame(7, 100, b"first")],
            vec![frame(7, 105, b"second")],
            vec![frame(7, 110, b"third")],
        ],
    );
    // Every commit acked durably even though the subscriber stalled:
    // the tee never blocks the flush path.
    drop(seq);
    assert_eq!(tee.subscriber_count(), 0);

    match sub.recv().unwrap() {
        TeeEvent::Window { frames, .. } => assert_eq!(identities(&frames), vec![(7, 100)]),
        other => panic!("expected the first window, got {other:?}"),
    }
    match sub.recv().unwrap() {
        TeeEvent::Lagged { next_seq } => assert_eq!(next_seq, 3),
        other => panic!("expected the cut marker, got {other:?}"),
    }
    assert!(sub.recv().is_none());
}

#[test]
fn catch_up_then_rejoin_loses_nothing_and_duplicates_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, media) = store_in(&dir);
    let shard = 5;
    let tenant = 7;
    let tee = Arc::new(Tee::new());

    let t = take_over(&media, shard, "node-a").unwrap();
    let seq = resume(&media, shard, t, &tee);

    // History the subscriber saw before it dropped: applied through
    // lsn 110. Then more history it missed, which consolidation folds
    // and GC deletes from landing.
    commit(
        &seq,
        vec![
            vec![frame(tenant, 100, b"seen0")],
            vec![frame(tenant, 105, b"seen1")],
            vec![frame(tenant, 110, b"missed0")],
        ],
    );
    let applied = Lsn(110);
    consolidate(&media, shard).unwrap().unwrap();
    gc_landing(&media, shard, Duration::ZERO).unwrap();
    // A landing tail past the consolidated boundary.
    commit(&seq, vec![vec![frame(tenant, 117, b"missed1")]]);

    // Rejoin protocol: subscribe first, then catch up, then dedup the
    // overlap. The window committed between the two is the overlap: it
    // arrives both live and in the catch up read.
    let sub = tee.subscribe(TeeFilter::Tenant(tenant), zou_log::DEFAULT_TEE_BUFFER);
    commit(&seq, vec![vec![frame(tenant, 124, b"overlap")]]);
    let caught = catch_up(&media, shard, &TeeFilter::Tenant(tenant), applied).unwrap();
    assert_eq!(
        identities(&caught),
        vec![(tenant, 110), (tenant, 117), (tenant, 124)]
    );
    let watermark = caught.iter().map(|f| f.end_lsn).max().unwrap();

    // Live traffic after the catch up read.
    commit(&seq, vec![vec![frame(tenant, 131, b"live")]]);
    drop(seq);

    let mut replayed = identities(&caught);
    while let Some(TeeEvent::Window { frames, .. }) = sub.try_recv() {
        for f in &frames {
            if f.end_lsn > watermark {
                replayed.push((f.tenant, f.start_lsn.0));
            }
        }
    }
    // Nothing lost, nothing doubled: the missed history, the overlap
    // window once, and the live tail.
    assert_eq!(
        replayed,
        vec![(tenant, 110), (tenant, 117), (tenant, 124), (tenant, 131)]
    );
}

#[test]
fn the_tee_and_catch_up_work_over_express_dual() {
    let dir = tempfile::tempdir().unwrap();
    let az1: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path().join("az1")));
    let az2: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path().join("az2")));
    let standard: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path().join("std")));
    let media = Arc::new(zou_log::WalMedia::express_dual(
        az1,
        az2,
        standard,
        Duration::from_secs(3600),
    ));
    let shard = 6;
    let tenant = 11;
    let tee = Arc::new(Tee::new());
    let sub = tee.subscribe(TeeFilter::Tenant(tenant), zou_log::DEFAULT_TEE_BUFFER);

    let t = take_over(&media, shard, "node-a").unwrap();
    let seq = resume(&media, shard, t, &tee);
    commit(
        &seq,
        vec![
            vec![frame(tenant, 100, b"one")],
            vec![frame(tenant, 103, b"two")],
        ],
    );
    drop(seq);

    let mut live = Vec::new();
    while let Some(TeeEvent::Window { frames, .. }) = sub.try_recv() {
        live.extend(identities(&frames));
    }
    assert_eq!(live, vec![(tenant, 100), (tenant, 103)]);

    consolidate(&media, shard).unwrap().unwrap();
    let caught = catch_up(&media, shard, &TeeFilter::Tenant(tenant), Lsn(0)).unwrap();
    assert_eq!(identities(&caught), live);
}
