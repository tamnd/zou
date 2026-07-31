//! Create a branch of a tenant inside a zou store.
//!
//! Usage: `zou-branch <store-root> <src> <dst> [--at <lsn>|--ts <unix>]`
//!
//! With no flag the branch is taken at the source's last published
//! state. `--at` pins it to an LSN, which must be a checkpoint redo or
//! sit in the still unfolded tail, and `--ts` materializes the newest
//! manifest history snapshot at or before that unix second. The child
//! references the parent's objects, nothing is copied, so the call
//! finishes in the time of two manifest round trips.

use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use zou_store::{Lsn, branch, materialize_at, open_store};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let usage = || {
        eprintln!(
            "usage: {} <store-root> <src> <dst> [--at <lsn>|--ts <unix>]",
            args[0]
        )
    };
    enum Mode {
        Head,
        At(u64),
        Ts(u64),
    }
    let (store_root, src, dst, mode) = match args.as_slice() {
        [_, root, src, dst] => (root, src, dst, Mode::Head),
        [_, root, src, dst, flag, v] => {
            let parsed = match flag.as_str() {
                "--at" => match v.strip_prefix("0x") {
                    Some(hex) => u64::from_str_radix(hex, 16).ok().map(Mode::At),
                    None => v.parse().ok().map(Mode::At),
                },
                "--ts" => v.parse().ok().map(Mode::Ts),
                _ => None,
            };
            match parsed {
                Some(mode) => (root, src, dst, mode),
                None => {
                    usage();
                    return ExitCode::FAILURE;
                }
            }
        }
        _ => {
            usage();
            return ExitCode::FAILURE;
        }
    };
    let store = match open_store(store_root) {
        Ok(store) => store,
        Err(e) => {
            eprintln!("zou-branch: {e}");
            return ExitCode::FAILURE;
        }
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before 1970")
        .as_secs();
    let result = match mode {
        Mode::Head => branch(&*store, src, dst, None, now),
        Mode::At(lsn) => branch(&*store, src, dst, Some(Lsn(lsn)), now),
        Mode::Ts(ts) => materialize_at(&*store, src, dst, ts, now),
    };
    match result {
        Ok(m) => {
            let at = m.branch_of.as_ref().expect("a child names its parent");
            println!(
                "branched {src} into {dst} at {:#X}, {} checkpoints and {} parent tail entries inherited",
                at.at_lsn.0,
                m.checkpoints.len(),
                m.parent_tail.len()
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("zou-branch: {e}");
            ExitCode::FAILURE
        }
    }
}
