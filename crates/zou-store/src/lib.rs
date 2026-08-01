//! Object storage engine for zou.
//!
//! A tenant is a self-contained prefix on an object store: a manifest that
//! acts as the root of truth and immutable page checkpoints. The manifest
//! is the only mutable object and is swapped with compare-and-swap, which
//! also carries the writer lease. WAL lives outside the tenant prefixes in
//! the shared log the zou-log crate owns.

pub mod branch;
pub mod cas;
pub mod delay;
pub mod frame;
pub mod guard;
pub mod heartbeat;
pub mod layout;
pub mod lease;
pub mod lsn;
pub mod manifest;
pub mod mem;
pub mod open;
#[cfg(feature = "s3")]
pub mod s3;
pub mod sim;
#[cfg(feature = "sqlite")]
pub mod sqlite;
pub mod stats;
pub mod zoufile;

pub use branch::{BranchError, branch, materialize_at, snapshot_at};
pub use cas::{CasError, CasStore, LocalFsStore, Version};
pub use delay::{DelayConfig, DelayStore};
pub use frame::{BlockRef, Frame2, Frame2DecodeError, Frame2Stream, MAX_HINTS, MAX_PAYLOAD_LEN};
pub use guard::GuardedStore;
pub use heartbeat::Heartbeat;
pub use layout::tenant_id;
pub use lease::{DEFAULT_TTL_SECS, HeldLease, LeaseError};
pub use lsn::Lsn;
pub use manifest::{MANIFEST_FORMAT, Manifest};
pub use mem::MemStore;
pub use open::{PrefixStore, open_store};
#[cfg(feature = "s3")]
pub use s3::{Dialect, S3Config, S3Store};
pub use sim::{BUILTIN_PROFILES, OpDist, SimConfig, SimProfile, SimStore};
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteStore;
pub use stats::{Snapshot, StatsStore};
pub use zoufile::ZouFileStore;
