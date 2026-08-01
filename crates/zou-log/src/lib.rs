//! Shared multi tenant WAL service, the v2 commit path (spec 03).
//!
//! A cell has a fixed set of WAL shards. Each shard is a role held by at
//! most one node, the sequencer, which group commits frames from every
//! tenant pinned to the shard into one landing segment per batch window.
//! A commit costs one durable PUT per window no matter how many tenants
//! are writing, which is what gives the cell a fixed request ceiling.
//!
//! This crate holds the pieces of that role: the landing segment codec
//! in [`segment`], the batching, admission and ack machinery in
//! [`sequencer`], the fenced chain protocol with takeover in [`chain`],
//! and the background fold of landing segments into sorted sealed
//! segments in [`mod@consolidate`] and [`sealed`]. Durability itself sits
//! behind [`SegmentSink`], so the same sequencer runs over the sealed
//! chain, a plain CAS store, or a test double.

pub mod chain;
pub mod consolidate;
pub mod media;
pub mod sealed;
pub mod segment;
pub mod sequencer;

pub use chain::{
    ChainError, ChainSegment, RoundRange, SHARD_MANIFEST_FORMAT, ShardManifest, Takeover,
    chain_head, manifest_key, read_chain, read_chain_linked, segment_key, take_over,
};
pub use consolidate::{
    ConsolidateError, ConsolidateOutcome, ROUND_INDEX_FORMAT, RoundChunk, RoundIndex, RoundTenant,
    consolidate, gc_landing, read_round_tenant, round_key,
};
pub use media::{DEFAULT_HEDGE_AFTER, DurabilityMode, MediaSink, SealOutcome, WalMedia};
pub use sealed::{
    SEALED_CHUNK_TARGET, SEALED_VERSION, SealedBuildError, SealedChunk, SealedDecodeError,
    SealedFooter, SealedHeader, SealedTenant, TenantBloom, build_sealed, decode_sealed,
    read_sealed_footer, sealed_key,
};
pub use segment::{
    Footer, SEGMENT_VERSION, SegmentBuilder, SegmentDecodeError, SegmentHeader, SegmentKind,
    TenantRun, TenantSummary, decode_segment, read_footer, tenants_digest,
};
pub use sequencer::{AppendError, AppendTicket, SegmentSink, Sequencer, SequencerConfig};
