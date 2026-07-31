//! Capture a pristine PGDATA as the genesis checkpoint of a zou store.
//!
//! Run it once, after `ZOU_TARGET=<store> initdb` and before the first
//! server start. Relation pages are already in the store through the
//! storage manager, this uploads the rest: pg_control, the SLRUs, the
//! initial WAL segment, config files, the whole filesystem skeleton.
//! With that in place a node can attach to the store with no local state,
//! which is what the recovery path builds on.
//!
//! Usage: `zou-bootstrap <store-root> <pgdata> --redo <X/Y>`
//!
//! The redo location comes from pg_controldata, the caller passes it in
//! so this tool does not have to parse the binary control file.

use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::SystemTime;

use zou_pg::capture::{self, Capture};
use zou_store::layout::TenantLayout;
use zou_store::lease;
use zou_store::manifest::{CheckpointKind, CheckpointRef};
use zou_store::{CasStore, Lsn, Manifest, open_store};

const GENESIS_ID: &str = "genesis";
const LEASE_TTL_SECS: u64 = 15;

/// Files that are per instance noise, not database state.
fn skip(relpath: &str) -> bool {
    relpath == "postmaster.pid" || relpath == "postmaster.opts" || relpath.starts_with("log/")
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

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
    let pgdata = Path::new(pgdata);
    if !pgdata.join("PG_VERSION").is_file() {
        return Err(format!("{} is not a data directory", pgdata.display()));
    }

    let store: Arc<dyn CasStore> = Arc::from(open_store(target)?);
    let layout = TenantLayout::new("local");

    let manifest_key = layout.manifest();
    match store.get(&manifest_key) {
        Ok(Some(_)) => {}
        Ok(None) => {
            let genesis = Manifest::new("local", 18);
            let _ = store.put_if_match(&manifest_key, &genesis.to_json(), None);
        }
        Err(e) => return Err(format!("store: {e}")),
    }

    let mut held = lease::acquire(
        &*store,
        &layout,
        "zou-bootstrap",
        LEASE_TTL_SECS,
        now_unix(),
    )
    .map_err(|e| format!("lease: {e}"))?;
    if held
        .manifest()
        .checkpoints
        .iter()
        .any(|c| c.id == GENESIS_ID)
    {
        let _ = lease::release(&*store, &layout, held);
        return Err("store already has a genesis checkpoint".into());
    }

    let mut paths = Capture::default();
    capture::walk(pgdata, "", &skip, &mut paths)
        .map_err(|e| format!("walk {}: {e}", pgdata.display()))?;
    paths.dirs.sort();
    let files = capture::read_files(&paths)?;
    let bytes = capture::upload(&*store, &layout, GENESIS_ID, &files, &paths.dirs, false)?;

    lease::update_manifest(&*store, &layout, &mut held, |m| {
        m.checkpoints.push(CheckpointRef {
            id: GENESIS_ID.to_string(),
            lsn: Lsn(redo),
            kind: CheckpointKind::Full,
        });
    })
    .map_err(|e| format!("manifest: {e}"))?;
    lease::release(&*store, &layout, held).map_err(|e| format!("release: {e}"))?;

    println!(
        "captured {} files, {} empty dirs, {} bytes as checkpoint {GENESIS_ID} at redo {redo:#X}",
        files.len(),
        paths.dirs.len(),
        bytes
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
