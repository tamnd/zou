//! Group commit: the write path between "a record is appended" and "the
//! record is durable on the object store".
//!
//! Producers call [`GroupCommit::append`] and get a ticket. A background
//! flusher drains the buffer into WAL frames and uploads each frame as one
//! immutable object, keyed by its start LSN. Tickets resolve only after the
//! upload succeeds, which is the whole contract: an ack means the bytes are
//! on the object store, never sooner. If the store stalls, the buffer fills
//! and appends block, so backpressure reaches producers instead of memory.
//!
//! This module is deliberately synchronous, plain threads and condvars.
//! zou-store is the embedded core and stays runtime free the way SQLite
//! does; async wrappers belong to the server layer above.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::cas::{CasError, CasStore};
use crate::layout::TenantLayout;
use crate::lsn::Lsn;
use crate::wal::{Frame, MAX_BODY_LEN};

/// Per-record overhead inside a frame payload: a u32 length prefix.
const RECORD_PREFIX: usize = 4;

#[derive(Debug, Clone)]
pub struct GroupCommitConfig {
    /// Flush pending records once the oldest has waited this long.
    pub flush_interval: Duration,
    /// Flush immediately once this many pending bytes accumulate.
    pub flush_bytes: usize,
    /// Appends block once pending bytes reach this, which is how a stalled
    /// object store pushes back on producers.
    pub buffer_capacity: usize,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_millis(2),
            flush_bytes: 512 * 1024,
            buffer_capacity: 8 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum CommitError {
    #[error("wal pipeline is closed")]
    Closed,
    #[error("record of {len} bytes exceeds the frame body cap")]
    RecordTooLarge { len: usize },
    /// The flusher exhausted its retries against the store. Every ticket
    /// past the durable watermark fails with this, none of them were acked.
    #[error("wal flush failed: {0}")]
    Flush(Arc<CasError>),
}

struct State {
    pending: VecDeque<Vec<u8>>,
    pending_bytes: usize,
    first_pending_at: Option<Instant>,
    /// LSN after everything appended so far. LSNs are byte offsets into the
    /// framed record stream, so they advance by prefix plus payload.
    next_lsn: u64,
    /// LSN up to which records have been handed to the flusher.
    taken_lsn: u64,
    /// LSN up to which records are durable on the store.
    durable_lsn: u64,
    failure: Option<CommitError>,
    shutdown: bool,
}

struct Shared {
    state: Mutex<State>,
    /// Wakes the flusher when records arrive or shutdown starts.
    work: Condvar,
    /// Wakes ticket waiters and producers blocked on backpressure.
    progress: Condvar,
    config: GroupCommitConfig,
}

/// Proof of an append. `wait` blocks until the record is durable.
#[must_use = "an append is not durable until the ticket resolves"]
pub struct CommitTicket {
    shared: Arc<Shared>,
    end_lsn: u64,
}

impl CommitTicket {
    /// Block until this record's frame is on the object store, returning
    /// the LSN the record ends at. Errors mean it never became durable.
    pub fn wait(self) -> Result<Lsn, CommitError> {
        let mut state = self.shared.state.lock().unwrap();
        loop {
            if state.durable_lsn >= self.end_lsn {
                return Ok(Lsn(self.end_lsn));
            }
            if let Some(failure) = &state.failure {
                return Err(failure.clone());
            }
            state = self.shared.progress.wait(state).unwrap();
        }
    }
}

pub struct GroupCommit {
    shared: Arc<Shared>,
    flusher: Option<JoinHandle<()>>,
}

impl GroupCommit {
    /// Start the pipeline for one writer session. Epoch and fence come from
    /// the held lease and are stamped into every frame, `start_lsn` is where
    /// this session's WAL stream begins.
    pub fn new(
        store: Arc<dyn CasStore>,
        layout: TenantLayout,
        epoch: u64,
        fence: u64,
        start_lsn: Lsn,
        config: GroupCommitConfig,
    ) -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                pending: VecDeque::new(),
                pending_bytes: 0,
                first_pending_at: None,
                next_lsn: start_lsn.0,
                taken_lsn: start_lsn.0,
                durable_lsn: start_lsn.0,
                failure: None,
                shutdown: false,
            }),
            work: Condvar::new(),
            progress: Condvar::new(),
            config,
        });
        let flusher = {
            let shared = Arc::clone(&shared);
            std::thread::spawn(move || flusher_loop(&shared, &*store, &layout, epoch, fence))
        };
        Self {
            shared,
            flusher: Some(flusher),
        }
    }

    /// Queue one record. Blocks while the buffer is full, errors if the
    /// pipeline has failed or closed. Durability comes from the ticket.
    pub fn append(&self, record: &[u8]) -> Result<CommitTicket, CommitError> {
        let framed_len = RECORD_PREFIX + record.len();
        if framed_len > MAX_BODY_LEN as usize {
            return Err(CommitError::RecordTooLarge { len: record.len() });
        }

        let mut state = self.shared.state.lock().unwrap();
        loop {
            if let Some(failure) = &state.failure {
                return Err(failure.clone());
            }
            if state.shutdown {
                return Err(CommitError::Closed);
            }
            // An oversized record is admitted alone into an empty buffer,
            // otherwise it could never fit and would block forever.
            if state.pending.is_empty()
                || state.pending_bytes + framed_len <= self.shared.config.buffer_capacity
            {
                break;
            }
            state = self.shared.progress.wait(state).unwrap();
        }

        let was_empty = state.pending.is_empty();
        if was_empty {
            state.first_pending_at = Some(Instant::now());
        }
        let mut framed = Vec::with_capacity(framed_len);
        framed.extend_from_slice(&(record.len() as u32).to_le_bytes());
        framed.extend_from_slice(record);
        state.pending.push_back(framed);
        state.pending_bytes += framed_len;
        state.next_lsn += framed_len as u64;
        let end_lsn = state.next_lsn;

        // The first record into an empty buffer must wake the flusher so it
        // arms the interval timer; an untimed wait would otherwise sleep
        // through small appends forever. Later records only wake it when the
        // byte trigger fires.
        if was_empty || state.pending_bytes >= self.shared.config.flush_bytes {
            self.shared.work.notify_one();
        }
        Ok(CommitTicket {
            shared: Arc::clone(&self.shared),
            end_lsn,
        })
    }

    /// Flush everything pending and stop the flusher. Returns the pipeline
    /// failure if one happened, in which case the unflushed tail is lost
    /// and its tickets already reported that.
    pub fn close(mut self) -> Result<(), CommitError> {
        self.begin_shutdown();
        if let Some(handle) = self.flusher.take() {
            let _ = handle.join();
        }
        let state = self.shared.state.lock().unwrap();
        match &state.failure {
            Some(failure) => Err(failure.clone()),
            None => Ok(()),
        }
    }

    fn begin_shutdown(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;
        drop(state);
        self.shared.work.notify_all();
        self.shared.progress.notify_all();
    }
}

impl Drop for GroupCommit {
    fn drop(&mut self) {
        if let Some(handle) = self.flusher.take() {
            self.begin_shutdown();
            let _ = handle.join();
        }
    }
}

fn flusher_loop(
    shared: &Shared,
    store: &dyn CasStore,
    layout: &TenantLayout,
    epoch: u64,
    fence: u64,
) {
    let mut state = shared.state.lock().unwrap();
    loop {
        if state.pending.is_empty() {
            if state.shutdown {
                return;
            }
            state = shared.work.wait(state).unwrap();
            continue;
        }

        let age = state
            .first_pending_at
            .map_or(Duration::ZERO, |t| t.elapsed());
        let due = state.shutdown
            || state.pending_bytes >= shared.config.flush_bytes
            || age >= shared.config.flush_interval;
        if !due {
            let timeout = shared.config.flush_interval - age;
            (state, _) = shared.work.wait_timeout(state, timeout).unwrap();
            continue;
        }

        // Take a prefix of whole records up to the frame body cap.
        let mut payload = Vec::new();
        while let Some(front) = state.pending.front() {
            if !payload.is_empty() && payload.len() + front.len() > MAX_BODY_LEN as usize {
                break;
            }
            let framed = state.pending.pop_front().unwrap();
            state.pending_bytes -= framed.len();
            payload.extend_from_slice(&framed);
        }
        let start_lsn = state.taken_lsn;
        let end_lsn = start_lsn + payload.len() as u64;
        state.taken_lsn = end_lsn;
        state.first_pending_at = if state.pending.is_empty() {
            None
        } else {
            Some(Instant::now())
        };
        drop(state);
        // Buffer space just freed, unblock producers before the upload.
        shared.progress.notify_all();

        let frame = Frame {
            epoch,
            fence,
            start_lsn: Lsn(start_lsn),
            end_lsn: Lsn(end_lsn),
            payload,
        };
        let key = layout.wal_segment(epoch, Lsn(start_lsn));
        let result = upload_with_retry(store, &key, &frame.encode());

        state = shared.state.lock().unwrap();
        match result {
            Ok(()) => {
                state.durable_lsn = end_lsn;
                shared.progress.notify_all();
            }
            Err(e) => {
                state.failure = Some(CommitError::Flush(Arc::new(e)));
                shared.progress.notify_all();
                return;
            }
        }
    }
}

/// Upload one immutable frame object. Retries transient errors with a short
/// backoff, and treats an AlreadyExists holding our exact bytes as success,
/// which makes a retry after an ack-lost upload idempotent.
fn upload_with_retry(store: &dyn CasStore, key: &str, data: &[u8]) -> Result<(), CasError> {
    const ATTEMPTS: u32 = 5;
    let mut last = None;
    for attempt in 0..ATTEMPTS {
        match store.put_new(key, data) {
            Ok(_) => return Ok(()),
            Err(CasError::AlreadyExists { .. }) => {
                return match store.get(key)? {
                    Some((existing, _)) if existing == data => Ok(()),
                    _ => Err(CasError::AlreadyExists {
                        key: key.to_string(),
                    }),
                };
            }
            Err(e) => last = Some(e),
        }
        std::thread::sleep(Duration::from_millis(10 << attempt));
    }
    Err(last.expect("loop ran at least once"))
}

/// Split a frame payload back into the records that went in. Returns None
/// on malformed framing, which recovery treats the same as a bad crc.
pub fn split_records(payload: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut records = Vec::new();
    let mut rest = payload;
    while !rest.is_empty() {
        let len_bytes: [u8; RECORD_PREFIX] = rest.get(..RECORD_PREFIX)?.try_into().ok()?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        rest = &rest[RECORD_PREFIX..];
        records.push(rest.get(..len)?.to_vec());
        rest = &rest[len..];
    }
    Some(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::LocalFsStore;
    use crate::wal::SegmentReader;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn setup() -> (tempfile::TempDir, Arc<LocalFsStore>, TenantLayout) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFsStore::new(dir.path()));
        (dir, store, TenantLayout::new("t1"))
    }

    fn stored_records(store: &dyn CasStore, layout: &TenantLayout, epoch: u64) -> Vec<Vec<u8>> {
        let prefix = layout.wal_segment(epoch, Lsn(0));
        let prefix = &prefix[..prefix.rfind('/').unwrap() + 1];
        let mut out = Vec::new();
        for key in store.list(prefix).unwrap() {
            let (data, _) = store.get(&key).unwrap().unwrap();
            for frame in SegmentReader::new(&data, epoch) {
                out.extend(split_records(&frame.unwrap().payload).unwrap());
            }
        }
        out
    }

    #[test]
    fn records_round_trip_and_lsns_increase() {
        let (_d, store, layout) = setup();
        let gc = GroupCommit::new(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            3,
            7,
            Lsn(0),
            GroupCommitConfig::default(),
        );

        let records: Vec<Vec<u8>> = (0u8..5).map(|i| vec![i; 10 + i as usize]).collect();
        let mut last = Lsn(0);
        for r in &records {
            let lsn = gc.append(r).unwrap().wait().unwrap();
            assert!(lsn > last, "LSNs must strictly increase");
            last = lsn;
        }
        gc.close().unwrap();

        assert_eq!(stored_records(&*store, &layout, 3), records);
    }

    #[test]
    fn concurrent_small_appends_coalesce_into_few_frames() {
        let (_d, store, layout) = setup();
        let gc = Arc::new(GroupCommit::new(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            1,
            1,
            Lsn(0),
            GroupCommitConfig {
                flush_interval: Duration::from_millis(50),
                ..GroupCommitConfig::default()
            },
        ));

        std::thread::scope(|s| {
            for t in 0..4u8 {
                let gc = Arc::clone(&gc);
                s.spawn(move || {
                    for i in 0..10u8 {
                        gc.append(&[t, i]).unwrap().wait().unwrap();
                    }
                });
            }
        });

        let objects = store.list("tenants/t1/wal/").unwrap();
        assert!(
            objects.len() < 40,
            "40 rapid appends produced {} objects, no coalescing happened",
            objects.len()
        );
        assert_eq!(stored_records(&*store, &layout, 1).len(), 40);
    }

    #[test]
    fn the_size_trigger_flushes_without_waiting_for_the_interval() {
        let (_d, store, layout) = setup();
        let gc = GroupCommit::new(
            store,
            layout,
            1,
            1,
            Lsn(0),
            GroupCommitConfig {
                // The interval alone would hang the test past its timeout,
                // so a returning wait() proves the byte trigger fired.
                flush_interval: Duration::from_secs(3600),
                flush_bytes: 1024,
                ..GroupCommitConfig::default()
            },
        );
        gc.append(&[9u8; 2048]).unwrap().wait().unwrap();
        gc.close().unwrap();
    }

    #[test]
    fn close_flushes_the_pending_tail() {
        let (_d, store, layout) = setup();
        let gc = GroupCommit::new(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            1,
            1,
            Lsn(0),
            GroupCommitConfig {
                flush_interval: Duration::from_secs(3600),
                ..GroupCommitConfig::default()
            },
        );
        let tickets: Vec<CommitTicket> = (0u8..3).map(|i| gc.append(&[i]).unwrap()).collect();
        gc.close().unwrap();
        for t in tickets {
            t.wait().unwrap();
        }
        assert_eq!(stored_records(&*store, &layout, 1).len(), 3);
    }

    /// Blocks every put until released, simulating a stalled object store.
    struct StallStore {
        inner: LocalFsStore,
        stalled: Mutex<bool>,
        released: Condvar,
    }

    impl StallStore {
        fn release(&self) {
            *self.stalled.lock().unwrap() = false;
            self.released.notify_all();
        }
    }

    impl CasStore for StallStore {
        fn get(&self, key: &str) -> Result<Option<(Vec<u8>, crate::cas::Version)>, CasError> {
            self.inner.get(key)
        }
        fn put_if_match(
            &self,
            key: &str,
            data: &[u8],
            expected: Option<&crate::cas::Version>,
        ) -> Result<crate::cas::Version, CasError> {
            let mut stalled = self.stalled.lock().unwrap();
            while *stalled {
                stalled = self.released.wait(stalled).unwrap();
            }
            drop(stalled);
            self.inner.put_if_match(key, data, expected)
        }
        fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
            self.inner.list(prefix)
        }
    }

    #[test]
    fn a_stalled_store_backpressures_producers_and_acks_after_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(StallStore {
            inner: LocalFsStore::new(dir.path()),
            stalled: Mutex::new(true),
            released: Condvar::new(),
        });
        let layout = TenantLayout::new("t1");
        let gc = Arc::new(GroupCommit::new(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            1,
            1,
            Lsn(0),
            GroupCommitConfig {
                flush_interval: Duration::from_millis(1),
                flush_bytes: 64,
                buffer_capacity: 256,
            },
        ));

        // The flusher takes one batch and stalls inside put. The producer
        // then fills the 256 byte buffer with 36 byte framed records and
        // must block partway through its 20 appends: at most one taken
        // batch plus seven buffered records can exist before backpressure.
        let appended = Arc::new(AtomicUsize::new(0));
        let all_acked = Arc::new(AtomicBool::new(false));
        let producer = {
            let gc = Arc::clone(&gc);
            let appended = Arc::clone(&appended);
            let all_acked = Arc::clone(&all_acked);
            std::thread::spawn(move || {
                let tickets: Vec<CommitTicket> = (0u8..20)
                    .map(|i| {
                        let t = gc.append(&[i; 32]).unwrap();
                        appended.fetch_add(1, Ordering::SeqCst);
                        t
                    })
                    .collect();
                for t in tickets {
                    t.wait().unwrap();
                }
                all_acked.store(true, Ordering::SeqCst);
            })
        };

        std::thread::sleep(Duration::from_millis(100));
        let during_stall = appended.load(Ordering::SeqCst);
        assert!(
            during_stall < 20,
            "all 20 appends went through while the store was stalled"
        );
        assert!(!all_acked.load(Ordering::SeqCst), "acked during a stall");
        assert!(
            store.inner.list("tenants/").unwrap().is_empty(),
            "bytes reached the store while it was stalled"
        );

        store.release();
        producer.join().unwrap();
        assert!(all_acked.load(Ordering::SeqCst));
        assert_eq!(stored_records(&store.inner, &layout, 1).len(), 20);
    }

    /// Fails every put, simulating a dead object store.
    struct DeadStore;

    impl CasStore for DeadStore {
        fn get(&self, _key: &str) -> Result<Option<(Vec<u8>, crate::cas::Version)>, CasError> {
            Ok(None)
        }
        fn put_if_match(
            &self,
            key: &str,
            _data: &[u8],
            _expected: Option<&crate::cas::Version>,
        ) -> Result<crate::cas::Version, CasError> {
            Err(CasError::Io {
                key: key.to_string(),
                source: std::io::Error::other("store is down"),
            })
        }
        fn list(&self, _prefix: &str) -> Result<Vec<String>, CasError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn a_dead_store_fails_tickets_and_poisons_the_pipeline() {
        let gc = GroupCommit::new(
            Arc::new(DeadStore),
            TenantLayout::new("t1"),
            1,
            1,
            Lsn(0),
            GroupCommitConfig {
                flush_interval: Duration::from_millis(1),
                ..GroupCommitConfig::default()
            },
        );
        let err = gc.append(b"doomed").unwrap().wait().unwrap_err();
        assert!(matches!(err, CommitError::Flush(_)));

        // The pipeline is poisoned, later appends refuse instead of queueing
        // into a buffer nobody will ever drain.
        loop {
            match gc.append(b"after") {
                Err(CommitError::Flush(_)) => break,
                Ok(_ticket) => std::thread::sleep(Duration::from_millis(5)),
                Err(e) => panic!("unexpected: {e}"),
            }
        }
        assert!(gc.close().is_err());
    }

    #[test]
    fn oversized_records_are_rejected_up_front() {
        let (_d, store, layout) = setup();
        let gc = GroupCommit::new(store, layout, 1, 1, Lsn(0), GroupCommitConfig::default());
        let too_big = vec![0u8; MAX_BODY_LEN as usize + 1];
        assert!(matches!(
            gc.append(&too_big),
            Err(CommitError::RecordTooLarge { .. })
        ));
        gc.close().unwrap();
    }

    #[test]
    fn split_records_rejects_malformed_framing() {
        assert_eq!(split_records(&[]), Some(vec![]));
        assert_eq!(split_records(&[5, 0, 0, 0, 1, 2]), None);
        assert_eq!(split_records(&[1, 2]), None);
        let mut good = 3u32.to_le_bytes().to_vec();
        good.extend_from_slice(b"abc");
        assert_eq!(split_records(&good), Some(vec![b"abc".to_vec()]));
    }
}
