//! Shared multi tenant WAL service, the v2 commit path (spec 03).
//!
//! A cell has a fixed set of WAL shards. Each shard is a role held by at
//! most one node, the sequencer, which group commits frames from every
//! tenant pinned to the shard into one landing segment per batch window.
//! A commit costs one durable PUT per window no matter how many tenants
//! are writing, which is what gives the cell a fixed request ceiling.
//!
//! This crate holds the pieces of that role: the landing segment codec
//! in [`segment`] and the batching, admission and ack machinery in
//! [`sequencer`]. Durability itself sits behind [`SegmentSink`], so the
//! same sequencer runs over the sealed chain, a plain CAS store, or a
//! test double.

pub mod segment;
pub mod sequencer;

pub use segment::{
    Footer, SEGMENT_VERSION, SegmentBuilder, SegmentDecodeError, SegmentHeader, TenantRun,
    TenantSummary, decode_segment, read_footer, tenants_digest,
};
pub use sequencer::{AppendError, AppendTicket, CasSink, SegmentSink, Sequencer, SequencerConfig};
