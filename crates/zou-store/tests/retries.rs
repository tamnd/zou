//! The retry counters, checked through the store that produces retries.
//!
//! The unit tests next to the counter file check that the slots decode.
//! They cannot check that anything writes to them, because the counters
//! are opened once per process out of `ZOU_STORE_STATS` and a unit test
//! that set the variable would be racing every other test in the binary
//! for which one got there first. An integration test is its own
//! process, so this file sets the variable before anything has looked
//! at it and then makes a store fail the way a throttled bucket does.
//!
//! Which is the half worth checking. The slot arithmetic fails loudly.
//! A counter nobody calls reads zero, and zero is a plausible number.

use std::path::PathBuf;

use zou_store::cas::{CasStore, LocalFsStore};
use zou_store::sim::{SimConfig, SimStore};
use zou_store::stats::Snapshot;

#[test]
fn a_throttled_store_says_how_many_times_it_was_told_to_slow_down() {
    let dir = tempfile::tempdir().unwrap();
    let counters: PathBuf = dir.path().join("stats");
    // Before the first store call in this process, so the counter file
    // this reads is the one the store below writes.
    unsafe { std::env::set_var("ZOU_STORE_STATS", &counters) };

    // Every request throttled, and the service times taken out, so what
    // the test waits on is the backoff schedule and nothing else.
    let config =
        SimConfig::parse("s3-standard,slowdown=1.0,seed=7,put_p50=0,put_p95=0,put_p99=0,put_max=0")
            .unwrap();
    let store = SimStore::new(
        Box::new(LocalFsStore::new(dir.path().join("store"))),
        config,
    );
    let err = store.put("tenants/a/pg/1/2/3/0/00000000", b"page");
    assert!(
        err.is_err(),
        "every attempt was throttled, so the put failed"
    );

    let snap = Snapshot::read(&counters).unwrap();
    let retry = |kind: &str| {
        snap.retries
            .iter()
            .find(|r| r.kind == kind)
            .unwrap_or_else(|| panic!("no {kind} counter"))
            .count
    };
    // Four attempts: three of them went out again, the fourth ran out
    // of budget, which is the one number a latency histogram cannot
    // show and the reason these counters exist.
    assert_eq!(retry("throttle"), 3);
    assert_eq!(retry("exhausted"), 1);
    assert_eq!(retry("server"), 0);
    assert_eq!(retry("transport"), 0);
}
