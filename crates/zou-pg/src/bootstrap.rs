//! Genesis capture: turn a pristine PGDATA into the first checkpoint of
//! a zou store.
//!
//! Relation pages are already in the store because initdb ran through
//! the patched storage manager, this uploads the rest: pg_control, the
//! SLRUs, the initial WAL segment, config files, the whole filesystem
//! skeleton. With that in place a node can attach to the store with no
//! local state, which is what the recovery path builds on. Both the
//! `zou-bootstrap` tool and `zou dev` call in here.

use std::path::Path;
use std::time::SystemTime;

use zou_store::CasStore;
use zou_store::layout::TenantLayout;
use zou_store::lease;
use zou_store::manifest::{CheckpointKind, CheckpointRef};
use zou_store::{Lsn, Manifest};

use crate::capture::{self, Capture};

pub const GENESIS_ID: &str = "genesis";
const LEASE_TTL_SECS: u64 = 15;

#[derive(Debug)]
pub struct BootstrapStats {
    pub files: usize,
    pub dirs: usize,
    pub bytes: u64,
}

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

/// Capture `pgdata` as the genesis checkpoint of the tenant, creating
/// the manifest if the store is empty. `redo` is the checkpoint redo
/// location from pg_control, [`crate::restore::control_redo`] reads it
/// straight out of the binary control file. Fails if the store already
/// has a genesis checkpoint.
pub fn capture_genesis(
    store: &dyn CasStore,
    layout: &TenantLayout,
    pgdata: &Path,
    redo: u64,
) -> Result<BootstrapStats, String> {
    if !pgdata.join("PG_VERSION").is_file() {
        return Err(format!("{} is not a data directory", pgdata.display()));
    }

    let manifest_key = layout.manifest();
    match store.get(&manifest_key) {
        Ok(Some(_)) => {}
        Ok(None) => {
            let genesis = Manifest::new(layout.tenant_ref(), 18);
            let _ = store.put_if_match(&manifest_key, &genesis.to_json(), None);
        }
        Err(e) => return Err(format!("store: {e}")),
    }

    let mut held = lease::acquire(store, layout, "zou-bootstrap", LEASE_TTL_SECS, now_unix())
        .map_err(|e| format!("lease: {e}"))?;
    if held
        .manifest()
        .checkpoints
        .iter()
        .any(|c| c.id == GENESIS_ID)
    {
        let _ = lease::release(store, layout, held);
        return Err("store already has a genesis checkpoint".into());
    }

    let mut paths = Capture::default();
    capture::walk(pgdata, "", &skip, &mut paths)
        .map_err(|e| format!("walk {}: {e}", pgdata.display()))?;
    paths.dirs.sort();
    let files = capture::read_files(&paths)?;
    let bytes = capture::upload(store, layout, GENESIS_ID, &files, &paths.dirs, false)?;

    lease::update_manifest(store, layout, &mut held, now_unix(), |m| {
        m.checkpoints.push(CheckpointRef {
            id: GENESIS_ID.to_string(),
            lsn: Lsn(redo),
            kind: CheckpointKind::Full,
            owner: None,
        });
    })
    .map_err(|e| format!("manifest: {e}"))?;
    lease::release(store, layout, held).map_err(|e| format!("release: {e}"))?;

    Ok(BootstrapStats {
        files: files.len(),
        dirs: paths.dirs.len(),
        bytes,
    })
}
