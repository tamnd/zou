//! Rebuild a Postgres data directory from a zou store.
//!
//! Usage: `zou-restore <store-root> <pgdata> [tenant] [--at <unix>]`
//!
//! The target directory must not exist and the tenant defaults to local.
//! After the restore a plain server start runs crash recovery from the
//! genesis checkpoint through the mirrored WAL, so the node attaches
//! with no other local state. A branched tenant restores its inherited
//! parent objects and tail the same way.
//!
//! With `--at` the restore reads the newest history snapshot published
//! at or before the unix timestamp instead of the live head. That is a
//! time travel attach: the store is never written, and starting the
//! result gives the database exactly as it stood at that moment.

use std::path::Path;
use std::process::ExitCode;

use zou_pg::restore::{restore, restore_at};

fn main() -> ExitCode {
    let mut args: Vec<String> = std::env::args().collect();
    let mut at: Option<u64> = None;
    if let Some(i) = args.iter().position(|a| a == "--at") {
        if i + 1 >= args.len() {
            eprintln!("zou-restore: --at needs a unix timestamp");
            return ExitCode::FAILURE;
        }
        match args[i + 1].parse() {
            Ok(ts) => at = Some(ts),
            Err(_) => {
                eprintln!(
                    "zou-restore: --at wants a unix timestamp, got {:?}",
                    args[i + 1]
                );
                return ExitCode::FAILURE;
            }
        }
        args.drain(i..i + 2);
    }
    let (store_root, pgdata, tenant) = match args.as_slice() {
        [_, store_root, pgdata] => (store_root, pgdata, "local"),
        [_, store_root, pgdata, tenant] => (store_root, pgdata, tenant.as_str()),
        _ => {
            eprintln!(
                "usage: {} <store-root> <pgdata> [tenant] [--at <unix>]",
                args[0]
            );
            return ExitCode::FAILURE;
        }
    };
    let result = match at {
        Some(ts) => restore_at(store_root, tenant, ts, Path::new(pgdata)),
        None => restore(store_root, tenant, Path::new(pgdata)),
    };
    match result {
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
