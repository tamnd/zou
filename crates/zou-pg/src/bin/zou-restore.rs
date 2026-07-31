//! Rebuild a Postgres data directory from a zou store.
//!
//! Usage: `zou-restore <store-root> <pgdata> [tenant]`
//!
//! The target directory must not exist and the tenant defaults to local.
//! After the restore a plain server start runs crash recovery from the
//! genesis checkpoint through the mirrored WAL, so the node attaches
//! with no other local state. A branched tenant restores its inherited
//! parent objects and tail the same way.

use std::path::Path;
use std::process::ExitCode;

use zou_pg::restore::restore;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let (store_root, pgdata, tenant) = match args.as_slice() {
        [_, store_root, pgdata] => (store_root, pgdata, "local"),
        [_, store_root, pgdata, tenant] => (store_root, pgdata, tenant.as_str()),
        _ => {
            eprintln!("usage: {} <store-root> <pgdata> [tenant]", args[0]);
            return ExitCode::FAILURE;
        }
    };
    match restore(store_root, tenant, Path::new(pgdata)) {
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
