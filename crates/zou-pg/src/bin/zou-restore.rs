//! Rebuild a Postgres data directory from a zou store.
//!
//! Usage: `zou-restore <store-root> <pgdata>`
//!
//! The target directory must not exist. After the restore a plain server
//! start runs crash recovery from the genesis checkpoint through the
//! mirrored WAL, so the node attaches with no other local state.

use std::path::Path;
use std::process::ExitCode;

use zou_pg::restore::restore;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let [_, store_root, pgdata] = args.as_slice() else {
        eprintln!("usage: {} <store-root> <pgdata>", args[0]);
        return ExitCode::FAILURE;
    };
    match restore(store_root, Path::new(pgdata)) {
        Ok(stats) => {
            println!(
                "restored {} files and {} empty dirs, overlaid {} WAL chunks ({} bytes) ending at {:#X}",
                stats.files, stats.dirs, stats.wal_records, stats.wal_bytes, stats.wal_end
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("zou-restore: {e}");
            ExitCode::FAILURE
        }
    }
}
