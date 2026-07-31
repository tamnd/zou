//! Capture a pristine PGDATA as the genesis checkpoint of a zou store.
//!
//! Run it once, after `ZOU_TARGET=<store> initdb` and before the first
//! server start. The heavy lifting lives in [`zou_pg::bootstrap`], which
//! `zou dev` shares.
//!
//! Usage: `zou-bootstrap <store-root> <pgdata> --redo <X/Y>`
//!
//! The redo location comes from pg_controldata, the caller passes it in
//! so this tool does not have to parse the binary control file.

use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use zou_pg::bootstrap;
use zou_store::layout::TenantLayout;
use zou_store::{CasStore, open_store};

/// Parse a Postgres LSN like 0/1F2F510 into a u64.
fn parse_lsn(text: &str) -> Option<u64> {
    let (hi, lo) = text.split_once('/')?;
    let hi = u64::from_str_radix(hi, 16).ok()?;
    let lo = u64::from_str_radix(lo, 16).ok()?;
    Some((hi << 32) | lo)
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().collect();
    let (target, pgdata, redo) = match args.as_slice() {
        [_, target, pgdata, flag, redo] if flag == "--redo" => (target, pgdata, redo),
        _ => {
            return Err(format!(
                "usage: {} <store-root> <pgdata> --redo <X/Y>",
                args[0]
            ));
        }
    };
    let redo = parse_lsn(redo).ok_or_else(|| format!("bad redo lsn {redo:?}"))?;

    let store: Arc<dyn CasStore> = Arc::from(open_store(target)?);
    let layout = TenantLayout::new("local");
    let stats = bootstrap::capture_genesis(&*store, &layout, Path::new(pgdata), redo)?;

    println!(
        "captured {} files, {} empty dirs, {} bytes as checkpoint {} at redo {redo:#X}",
        stats.files,
        stats.dirs,
        stats.bytes,
        bootstrap::GENESIS_ID
    );
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("zou-bootstrap: {e}");
            ExitCode::FAILURE
        }
    }
}
