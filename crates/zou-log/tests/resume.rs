//! Catching up on a live chain without reading it from the start every
//! time: the cursor skips what a walk already read, a sink that stops
//! early comes back where it left off, and a fold that moves the
//! consolidated boundary sends the walk back to it rather than off the
//! end of a chain that no longer exists.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use zou_log::{
    ChainCursor, ConsolidateError, MediaSink, Sequencer, SequencerConfig, TeeFilter, WalMedia,
    catch_up_resuming, consolidate, take_over,
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

/// Delegates everything and counts the object reads, which is the whole
/// point of the cursor: the same tail read twice is the cost.
struct CountingStore {
    inner: Arc<dyn CasStore>,
    gets: AtomicUsize,
}

impl CountingStore {
    fn gets(&self) -> usize {
        self.gets.load(Ordering::SeqCst)
    }
}

impl CasStore for CountingStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key)
    }
    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
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

fn counting(dir: &tempfile::TempDir) -> (Arc<CountingStore>, Arc<WalMedia>) {
    let inner: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path()));
    let counting = Arc::new(CountingStore {
        inner,
        gets: AtomicUsize::new(0),
    });
    let media = Arc::new(WalMedia::single(Arc::clone(&counting) as Arc<dyn CasStore>));
    (counting, media)
}

fn writer(media: &Arc<WalMedia>, shard: u32) -> Sequencer {
    let t = take_over(media, shard, "node-a").unwrap();
    let sink = Arc::new(MediaSink::new(Arc::clone(media), shard, t.sealed_seq));
    Sequencer::resume(
        shard,
        sink as _,
        SequencerConfig {
            window: Duration::from_millis(5),
            ..SequencerConfig::default()
        },
        t.next_seq,
        t.prev_digest,
    )
}

/// One frame per call so every append lands as its own segment, which
/// is what a live pusher does and what makes the tail long.
fn push(seq: &Sequencer, tenant: u128, start: u64, body: &[u8]) {
    seq.append(vec![frame(tenant, start, body)])
        .unwrap()
        .wait()
        .unwrap();
}

fn drain(
    media: &WalMedia,
    shard: u32,
    filter: &TeeFilter,
    applied: Lsn,
    cursor: &mut Option<ChainCursor>,
) -> Vec<u64> {
    let mut out = Vec::new();
    catch_up_resuming::<ConsolidateError, _>(media, shard, filter, applied, cursor, |f| {
        out.push(f.start_lsn.0);
        Ok(true)
    })
    .unwrap();
    out
}

#[test]
fn the_cursor_reads_the_new_segments_and_not_the_tail_behind_them() {
    let dir = tempfile::tempdir().unwrap();
    let (store, media) = counting(&dir);
    let shard = 11;
    let filter = TeeFilter::Tenant(1);

    let seq = writer(&media, shard);
    for i in 0..8u64 {
        push(&seq, 1, 100 + i * 10, b"landing!!!");
    }

    let mut cursor = None;
    let first = drain(&media, shard, &filter, Lsn(0), &mut cursor);
    assert_eq!(first.len(), 8);
    let after_first = store.gets();
    assert!(cursor.is_some(), "the walk hands back where it stopped");

    // Nothing new: the cursor sits one past the head, so this is the
    // manifest read plus the one miss that says the head has not moved.
    let idle = drain(&media, shard, &filter, Lsn(180), &mut cursor);
    assert!(idle.is_empty());
    let idle_cost = store.gets() - after_first;
    assert!(
        idle_cost <= 3,
        "an idle poll cost {idle_cost} gets, it should not walk the tail again"
    );

    // Two more segments, and only those two are read.
    push(&seq, 1, 180, b"new one!!!");
    push(&seq, 1, 190, b"new two!!!");
    let before_new = store.gets();
    let fresh = drain(&media, shard, &filter, Lsn(180), &mut cursor);
    assert_eq!(fresh, vec![180, 190]);
    let new_cost = store.gets() - before_new;
    assert!(
        new_cost <= 5,
        "reading two new segments cost {new_cost} gets, the tail is being reread"
    );
    seq.close().unwrap();
}

#[test]
fn a_sink_that_stops_early_comes_back_to_the_next_segment() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, media) = counting(&dir);
    let shard = 12;
    let filter = TeeFilter::Tenant(1);

    let seq = writer(&media, shard);
    for i in 0..5u64 {
        push(&seq, 1, 100 + i * 10, b"stopstop!!");
    }
    seq.close().unwrap();

    let mut cursor = None;
    let mut seen: Vec<u64> = Vec::new();
    // Stop on the first frame of every call, which is one segment here.
    for _ in 0..5 {
        let mut taken = 0;
        let caught_up = catch_up_resuming::<ConsolidateError, _>(
            &media,
            shard,
            &filter,
            Lsn(0),
            &mut cursor,
            |f| {
                seen.push(f.start_lsn.0);
                taken += 1;
                Ok(false)
            },
        )
        .unwrap();
        assert_eq!(taken, 1, "a stop should end the walk after its segment");
        assert!(!caught_up, "stopping early is not caught up");
    }
    assert_eq!(seen, vec![100, 110, 120, 130, 140]);

    // The sixth call has nothing left and says so.
    let caught_up = catch_up_resuming::<ConsolidateError, _>(
        &media,
        shard,
        &filter,
        Lsn(0),
        &mut cursor,
        |_| Ok(false),
    )
    .unwrap();
    assert!(caught_up);
}

#[test]
fn a_fold_under_the_cursor_sends_the_walk_back_to_the_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, media) = counting(&dir);
    let shard = 13;
    let filter = TeeFilter::Tenant(1);

    let seq = writer(&media, shard);
    for i in 0..4u64 {
        push(&seq, 1, 100 + i * 10, b"foldable!!");
    }
    let mut cursor = None;
    assert_eq!(drain(&media, shard, &filter, Lsn(0), &mut cursor).len(), 4);
    let stale = cursor;
    seq.close().unwrap();

    // The fold moves the consolidated boundary past everything the
    // cursor walked, so the cursor now points into a chain that starts
    // above it and a walk from there would read nothing.
    consolidate(&media, shard).unwrap().unwrap();

    // A reader that has applied nothing still gets every frame, out of
    // the round the fold wrote.
    let mut cursor = stale;
    let again = drain(&media, shard, &filter, Lsn(0), &mut cursor);
    assert_eq!(again, vec![100, 110, 120, 130]);
}
