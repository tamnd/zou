//! Object storage engine for zou.
//!
//! A tenant is a self-contained prefix on an object store: a manifest that
//! acts as the root of truth, immutable WAL segments, and immutable page
//! checkpoints. The manifest is the only mutable object and is swapped with
//! compare-and-swap, which also carries the writer lease.

pub mod cas;
pub mod commit;
pub mod layout;
pub mod lease;
pub mod lsn;
pub mod manifest;
pub mod wal;

pub use cas::{CasError, CasStore, LocalFsStore, Version};
pub use commit::{CommitError, CommitTicket, GroupCommit, GroupCommitConfig, TailConfig};
pub use lease::{DEFAULT_TTL_SECS, HeldLease, LeaseError};
pub use lsn::Lsn;
pub use manifest::{MANIFEST_FORMAT, Manifest};
pub use wal::{Frame, SegmentReader, WAL_VERSION, WalDecodeError};
