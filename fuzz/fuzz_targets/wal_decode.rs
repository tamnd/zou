//! Run with: cargo +nightly fuzz run wal_decode
//!
//! The frame decoder consumes bytes straight off the network, so the
//! property under fuzz is simple: no input may panic it, and walking a
//! segment always terminates.

#![no_main]

use libfuzzer_sys::fuzz_target;
use zou_store::wal::{Frame, SegmentReader};

fuzz_target!(|data: &[u8]| {
    let _ = Frame::decode(data);
    for _ in SegmentReader::new(data, 42) {}
});
