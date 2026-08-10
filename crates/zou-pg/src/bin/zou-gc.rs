//! Delete objects no retained manifest references.
//!
//! Usage: `zou-gc <store-root> [window-secs] [retention-secs]`
//!
//! `zou gc` in the main CLI is the same sweep with named flags, human
//! durations and a dry run. This is the plumbing underneath it, kept
//! because the postgres build harness calls it directly.
//!
//! Deletion is two phase: a run stamps unreferenced keys as candidates
//! and a later run deletes what stayed unreferenced past the safety
//! window, so it takes at least two runs for anything to go. The
//! window defaults to a day and must exceed the longest checkpoint
//! fold upload and the longest gap between reading a manifest and
//! publishing a branch from it. One job runs at a time, which a lock
//! object in the store enforces: a second one refuses rather than
//! running on top of the first.
//!
//! The retention window, a week by default, is how far back PITR
//! reaches: manifest history snapshots younger than it keep the
//! objects they reference alive, older ones are collected.

use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use zou_pg::gc::{self, DEFAULT_RETENTION_SECS, DEFAULT_WINDOW_SECS, Policy, Sweep};
use zou_store::open_store;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let usage = || {
        eprintln!(
            "usage: {} <store-root> [window-secs] [retention-secs]",
            args[0]
        )
    };
    let parse = |s: &String| s.parse::<u64>().ok();
    let (store_root, window, retention) = match args.as_slice() {
        [_, root] => (root, DEFAULT_WINDOW_SECS, DEFAULT_RETENTION_SECS),
        [_, root, w] => match parse(w) {
            Some(w) => (root, w, DEFAULT_RETENTION_SECS),
            None => {
                usage();
                return ExitCode::FAILURE;
            }
        },
        [_, root, w, r] => match (parse(w), parse(r)) {
            (Some(w), Some(r)) => (root, w, r),
            _ => {
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
    let policy = Policy {
        window_secs: window,
        retention_secs: retention,
        ..Policy::default()
    };
    let holder = format!("zou-gc-pid-{}", std::process::id());
    match gc::sweep(&*store, &holder, now, policy) {
        Ok(Sweep::Ran(stats)) => {
            println!(
                "gc: {} tenants, {} candidates waiting, {} objects deleted",
                stats.tenants, stats.candidates, stats.deleted
            );
            ExitCode::SUCCESS
        }
        Ok(Sweep::Busy { holder, until_unix }) => {
            eprintln!("zou-gc: another sweep is running, held by {holder} until unix {until_unix}");
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("zou-gc: {e}");
            ExitCode::FAILURE
        }
    }
}
