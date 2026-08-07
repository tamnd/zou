//! Object storage engine for zou.
//!
//! A tenant is a self-contained prefix on an object store: a manifest that
//! acts as the root of truth and immutable page checkpoints. The manifest
//! is the only mutable object and is swapped with compare-and-swap, which
//! also carries the writer lease. WAL lives outside the tenant prefixes in
//! the shared log the zou-log crate owns.

pub mod bloom;
pub mod branch;
pub mod cas;
pub mod delay;
pub mod filecache;
pub mod frame;
pub mod guard;
pub mod heartbeat;
pub mod hedge;
pub mod layer;
pub mod layermap;
pub mod layout;
pub mod lease;
pub mod lsn;
pub mod manifest;
pub mod mem;
pub mod memtable;
pub mod open;
pub mod pageread;
pub mod placement;
pub mod registry;
#[cfg(feature = "s3")]
pub mod s3;
pub mod shardmanifest;
pub mod shards;
pub mod sim;
#[cfg(feature = "sqlite")]
pub mod sqlite;
pub mod stats;
pub mod zoufile;

pub use bloom::Bloom;
pub use branch::{BranchError, branch, lineage, materialize_at, snapshot_at};
pub use cas::{CasError, CasStore, LocalFsStore, Version};
pub use delay::{DelayConfig, DelayStore};
pub use filecache::{CacheStats, FileCache, cacheable};
pub use frame::{BlockRef, Frame2, Frame2DecodeError, Frame2Stream, MAX_HINTS, MAX_PAYLOAD_LEN};
pub use guard::GuardedStore;
pub use heartbeat::Heartbeat;
pub use hedge::HedgedStore;
pub use layer::{
    DeltaEntry, ImageEntry, LayerBlock, LayerBuildError, LayerDecodeError, LayerFooter, LayerKey,
    LayerKind, build_delta, build_image, decode_delta, decode_delta_block, decode_image,
    decode_image_block, read_layer_footer,
};
pub use layout::tenant_id;
pub use lease::{DEFAULT_TTL_SECS, HeldLease, LeaseError};
pub use lsn::Lsn;
pub use manifest::{MANIFEST_FORMAT, Manifest, ShardChange};
pub use mem::MemStore;
pub use open::{PrefixStore, open_store};
pub use placement::{MAP_FORMAT, MapClient, MapServer, Node, Pin, PlacementError, ShardMap};
#[cfg(feature = "s3")]
pub use s3::{Dialect, S3Config, S3Store};
pub use shardmanifest::{LayerEntry, PAGE_SHARD_FORMAT, PageShardManifest, publish_layer};
pub use shards::{
    MAX_PAGE_SHARDS, ShardError, load_serving_descs, merge, resume_floor, serving_shards, shard_of,
    split,
};
pub use sim::{BUILTIN_PROFILES, OpDist, SimConfig, SimProfile, SimStore};
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteStore;
pub use stats::{Snapshot, StatsStore};
pub use zoufile::ZouFileStore;
