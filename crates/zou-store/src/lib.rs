//! Object storage engine for zou.
//!
//! A tenant is a self-contained prefix on an object store: a manifest that
//! acts as the root of truth, immutable WAL segments, and immutable page
//! checkpoints. The manifest is the only mutable object and is swapped with
//! compare-and-swap, which also carries the writer lease.

pub mod cas;
pub mod commit;
pub mod guard;
pub mod heartbeat;
pub mod layout;
pub mod lease;
pub mod lsn;
pub mod manifest;
pub mod open;
#[cfg(feature = "s3")]
pub mod s3;
pub mod tier;
pub mod wal;

pub use cas::{CasError, CasStore, LocalFsStore, Version};
pub use commit::{CommitError, CommitTicket, GroupCommit, GroupCommitConfig, TailConfig};
pub use guard::GuardedStore;
pub use heartbeat::Heartbeat;
pub use lease::{DEFAULT_TTL_SECS, HeldLease, LeaseError};
pub use lsn::Lsn;
pub use manifest::{MANIFEST_FORMAT, Manifest};
pub use open::{PrefixStore, open_store};
#[cfg(feature = "s3")]
pub use s3::{Dialect, S3Config, S3Store};
pub use tier::{BufferedTarget, ExpressTarget, LatencyTier, PureS3Target, WalTarget};
pub use wal::{Frame, SegmentReader, WAL_VERSION, WalDecodeError};
