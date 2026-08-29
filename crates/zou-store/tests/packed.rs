//! The compression counters, checked through the real encoders.
//!
//! The unit tests next to the counter file check that the slots decode.
//! They cannot check that anything writes to them, because the counters
//! are opened once per process out of `ZOU_STORE_STATS` and a unit test
//! that set the variable would be racing every other test in the binary
//! for which one got there first. An integration test is its own
//! process, so this file sets the variable before anything has looked
//! at it and then runs the encoders that are supposed to report.
//!
//! Which is the half worth checking. The slot arithmetic fails loudly.
//! A counter nobody calls reads zero, and zero is a plausible number.

use std::path::PathBuf;

use zou_store::frame::Frame2;
use zou_store::layer::{DeltaEntry, LayerKey, build_delta};
use zou_store::lsn::Lsn;
use zou_store::stats::Snapshot;
use zou_store::zoufile::ZouFileStore;

use zou_store::cas::CasStore;

/// Compressible on purpose. A random payload would leave the encoder
/// storing raw and the test could not tell that from a counter nobody
/// bumped.
fn compressible(len: usize) -> Vec<u8> {
    b"the same words over and over again, "
        .iter()
        .copied()
        .cycle()
        .take(len)
        .collect()
}

#[test]
fn the_encoders_report_what_they_handed_the_compressor() {
    let dir = tempfile::tempdir().unwrap();
    let counters: PathBuf = dir.path().join("stats");
    // Before the first store call in this process, so the counter file
    // this reads is the one everything below writes.
    unsafe { std::env::set_var("ZOU_STORE_STATS", &counters) };

    let entries: Vec<DeltaEntry> = (0..64)
        .map(|i| DeltaEntry {
            key: LayerKey::page(1663, 5, 16384, 0, i),
            lsn: Lsn(1000 + u64::from(i)),
            record: compressible(4096),
        })
        .collect();
    build_delta(&entries, 32 * 1024).unwrap();

    Frame2 {
        tenant: 1,
        writer_epoch: 1,
        start_lsn: Lsn(1000),
        end_lsn: Lsn(2000),
        contains_commit: true,
        first_of_epoch: false,
        hints: Vec::new(),
        payload: compressible(64 * 1024),
    }
    .encode();

    let store = ZouFileStore::open(dir.path().join("files")).unwrap();
    store
        .put("tenants/a/files/big.txt", &compressible(32 * 1024))
        .unwrap();

    let snap = Snapshot::read(&counters).unwrap();
    let packed = |kind: &str| snap.packed.iter().find(|p| p.kind == kind).unwrap();
    for kind in ["layer", "wal", "file"] {
        assert!(
            packed(kind).raw > 0,
            "{kind} handed the compressor nothing, so nothing is counting it"
        );
        assert!(
            packed(kind).stored > 0 && packed(kind).stored < packed(kind).raw,
            "{kind} counted {} raw into {} stored, which is not a compression",
            packed(kind).raw,
            packed(kind).stored
        );
    }
}
