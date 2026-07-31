//! Delete objects no retained manifest references.
//!
//! Usage: `zou-gc <store-root> [window-secs]`
//!
//! Deletion is two phase: a run stamps unreferenced keys as candidates
//! and a later run deletes what stayed unreferenced past the safety
//! window, so it takes at least two runs for anything to go. The
//! window defaults to a day and must exceed the longest checkpoint
//! fold upload and the longest gap between reading a manifest and
//! publishing a branch from it. Run one job at a time.

use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use zou_pg::gc;
use zou_store::open_store;

const DEFAULT_WINDOW_SECS: u64 = 24 * 60 * 60;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let usage = || eprintln!("usage: {} <store-root> [window-secs]", args[0]);
    let (store_root, window) = match args.as_slice() {
        [_, root] => (root, DEFAULT_WINDOW_SECS),
        [_, root, w] => match w.parse() {
            Ok(w) => (root, w),
            Err(_) => {
                usage();
                return ExitCode::FAILURE;
            }
        },
        _ => {
            usage();
            return ExitCode::FAILURE;
        }
    };
    let store = match open_store(store_root) {
        Ok(store) => store,
        Err(e) => {
            eprintln!("zou-gc: {e}");
            return ExitCode::FAILURE;
        }
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before 1970")
        .as_secs();
    match gc::run(&*store, now, window) {
        Ok(stats) => {
            println!(
                "gc: {} tenants, {} candidates waiting, {} objects deleted",
                stats.tenants, stats.candidates, stats.deleted
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("zou-gc: {e}");
            ExitCode::FAILURE
        }
    }
}
