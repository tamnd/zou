//! Latency tiers for WAL durability.
//!
//! Commit latency is dominated by the WAL upload, so the tier decides what
//! "durable" means for an ack and how much it costs:
//!
//! - PureS3: every frame lands on the main store before the ack. Regional
//!   durability, ~tens of ms on real S3, zero extra moving parts.
//! - Express: frames land on a low latency store first (S3 Express One
//!   Zone class), single digit ms, single AZ durability until compaction
//!   migrates them to the main store.
//! - Buffered: frames ack from a replicated local buffer ahead of the
//!   upload, sub ms, durability bounded by the buffer's replication.
//!
//! M1 ships the interface and the PureS3 behavior. Express takes its fast
//! store but does not migrate yet, and Buffered is a stub that acks like
//! PureS3, so neither weakens durability before their machinery exists.

use std::sync::Arc;
use std::time::Duration;

use crate::cas::{CasError, CasStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyTier {
    PureS3,
    Express,
    Buffered,
}

/// Where WAL frames go and what an ack from there means. The group commit
/// flusher writes every frame through one of these.
pub trait WalTarget: Send + Sync {
    fn tier(&self) -> LatencyTier;

    /// Upload one encoded frame. Returning Ok means the frame is durable
    /// under this tier's contract, and the caller may ack commits.
    fn put_frame(&self, key: &str, data: &[u8]) -> Result<(), CasError>;
}

/// Upload one immutable frame object. Retries transient errors with a short
/// backoff, and treats an AlreadyExists holding our exact bytes as success,
/// which makes a retry after an ack-lost upload idempotent.
pub(crate) fn upload_with_retry(
    store: &dyn CasStore,
    key: &str,
    data: &[u8],
) -> Result<(), CasError> {
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

/// Frames go straight to the main store. The default tier.
pub struct PureS3Target {
    store: Arc<dyn CasStore>,
}

impl PureS3Target {
    pub fn new(store: Arc<dyn CasStore>) -> Self {
        Self { store }
    }
}

impl WalTarget for PureS3Target {
    fn tier(&self) -> LatencyTier {
        LatencyTier::PureS3
    }

    fn put_frame(&self, key: &str, data: &[u8]) -> Result<(), CasError> {
        upload_with_retry(&*self.store, key, data)
    }
}

/// Frames ack from the fast store, a bucket in a low latency storage class
/// under the same key layout. Migration of sealed segments from fast to
/// main happens with compaction, which does not exist yet, so today the
/// fast store must be treated as the WAL's home.
pub struct ExpressTarget {
    fast: Arc<dyn CasStore>,
}

impl ExpressTarget {
    pub fn new(fast: Arc<dyn CasStore>) -> Self {
        Self { fast }
    }
}

impl WalTarget for ExpressTarget {
    fn tier(&self) -> LatencyTier {
        LatencyTier::Express
    }

    fn put_frame(&self, key: &str, data: &[u8]) -> Result<(), CasError> {
        upload_with_retry(&*self.fast, key, data)
    }
}

/// Stub for the replicated buffer tier. Acks like PureS3 until the buffer
/// machinery lands, so choosing it never weakens durability by accident.
pub struct BufferedTarget {
    store: Arc<dyn CasStore>,
}

impl BufferedTarget {
    pub fn new(store: Arc<dyn CasStore>) -> Self {
        Self { store }
    }
}

impl WalTarget for BufferedTarget {
    fn tier(&self) -> LatencyTier {
        LatencyTier::Buffered
    }

    fn put_frame(&self, key: &str, data: &[u8]) -> Result<(), CasError> {
        upload_with_retry(&*self.store, key, data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::LocalFsStore;

    #[test]
    fn express_frames_land_on_the_fast_store_only() {
        let fast_dir = tempfile::tempdir().unwrap();
        let main_dir = tempfile::tempdir().unwrap();
        let fast = Arc::new(LocalFsStore::new(fast_dir.path()));
        let main = Arc::new(LocalFsStore::new(main_dir.path()));

        let target = ExpressTarget::new(Arc::clone(&fast) as Arc<dyn CasStore>);
        target.put_frame("tenants/t1/wal/x.wal", b"frame").unwrap();

        assert!(fast.get("tenants/t1/wal/x.wal").unwrap().is_some());
        assert!(main.get("tenants/t1/wal/x.wal").unwrap().is_none());
        assert_eq!(target.tier(), LatencyTier::Express);
    }

    #[test]
    fn put_frame_is_idempotent_across_retries() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFsStore::new(dir.path())) as Arc<dyn CasStore>;
        let target = PureS3Target::new(store);
        target.put_frame("k", b"same bytes").unwrap();
        target.put_frame("k", b"same bytes").unwrap();
        assert!(target.put_frame("k", b"different").is_err());
    }
}
