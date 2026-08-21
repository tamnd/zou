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
use zou_store::cas::CasError;
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

/// What an interrupted first attach left on the store, cleared so the
/// initdb about to run writes onto an empty prefix.
///
/// A first attach is an initdb through the patched storage manager and
/// then a genesis capture, and the pages initdb writes land in the
/// store as it goes. Kill it partway, over a slow link or a wait that
/// ran out, and the prefix holds pages of a cluster that was never
/// finished: enough that the next initdb finds relations already there
/// and dies with "relation pg_attrdef already exists", not enough that
/// anything can be restored from it.
///
/// A project is a project once the manifest names its first checkpoint,
/// the same rule every later state change already follows, so anything
/// found under a manifest that names none is scratch from an attempt
/// that did not finish and goes. Refuses when the manifest does name a
/// checkpoint, since then the pages are somebody's database, and when a
/// live lease says another node is in the middle of this same work.
///
/// The project's own uploads are left alone. Storage objects and
/// deployed functions are not made by initdb and not read by a restore,
/// so nothing here has an opinion about them.
pub fn clear_unfinished(store: &dyn CasStore, layout: &TenantLayout) -> Result<usize, String> {
    let tenant_ref = layout.tenant_ref();
    if let Some((data, _)) = store
        .get(&layout.manifest())
        .map_err(|e| format!("store: {e}"))?
    {
        let manifest = Manifest::from_json(&data).map_err(|e| format!("manifest: {e}"))?;
        if !manifest.checkpoints.is_empty() {
            return Err(format!(
                "{tenant_ref} already has a database, {} checkpoints, nothing here may touch it",
                manifest.checkpoints.len()
            ));
        }
        if let Some(lease) = &manifest.lease
            && lease.expires_unix > now_unix()
        {
            return Err(format!(
                "{tenant_ref} is being bootstrapped by {} until unix {}, waiting for that to finish or expire beats racing it",
                lease.holder, lease.expires_unix
            ));
        }
    }
    let prefix = format!("{}/", layout.prefix());
    let keys = store.list(&prefix).map_err(|e| format!("store: {e}"))?;
    let mut cleared = 0;
    for key in &keys {
        let rest = key.strip_prefix(&prefix).unwrap_or(key);
        if rest.starts_with("files/") || rest.starts_with("functions/") {
            continue;
        }
        store.delete(key).map_err(|e| format!("store: {e}"))?;
        cleared += 1;
    }
    Ok(cleared)
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
            match store.put_if_match(&manifest_key, &genesis.to_json(), None) {
                Ok(_) => {}
                // A concurrent bootstrap won the create, the acquire
                // below re-reads and the genesis checkpoint guard
                // handles the rest.
                Err(CasError::Conflict { .. }) => {}
                // Swallowing this used to turn a backend that cannot
                // do conditional creates into a baffling "no manifest"
                // from the lease acquire. MinIO RELEASE.2025-09-06
                // answers If-None-Match: * on a missing key with 404
                // NoSuchKey, which is how we learned.
                Err(e) => return Err(format!("create manifest: {e}")),
            }
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
    let bytes = capture::upload(
        store,
        layout,
        GENESIS_ID,
        &files,
        &paths.dirs,
        false,
        Some(redo),
    )?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use zou_store::cas::Version;

    /// A backend whose conditional create always fails, the shape of
    /// MinIO RELEASE.2025-09-06 answering If-None-Match: * on a
    /// missing key with 404 NoSuchKey.
    struct BrokenCreate;

    impl CasStore for BrokenCreate {
        fn get(&self, _key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
            Ok(None)
        }

        fn put_if_match(
            &self,
            key: &str,
            _data: &[u8],
            _expected: Option<&Version>,
        ) -> Result<Version, CasError> {
            Err(CasError::Io {
                key: key.to_string(),
                source: std::io::Error::other("PUT returned 404: NoSuchKey"),
            })
        }

        fn delete(&self, _key: &str) -> Result<(), CasError> {
            unreachable!("capture_genesis must fail before deleting anything")
        }

        fn list(&self, _prefix: &str) -> Result<Vec<String>, CasError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn a_failed_manifest_create_is_reported_not_swallowed() {
        let dir = std::env::temp_dir().join(format!("zou-bootstrap-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("PG_VERSION"), "18\n").unwrap();

        let layout = TenantLayout::new("local");
        let err = capture_genesis(&BrokenCreate, &layout, &dir, 0).unwrap_err();
        std::fs::remove_dir_all(&dir).unwrap();

        assert!(
            err.starts_with("create manifest:"),
            "the create failure must surface directly, got: {err}"
        );
        assert!(
            err.contains("NoSuchKey"),
            "the backend error must survive: {err}"
        );
    }

    /// A store holding what an attach killed during initdb leaves: pages
    /// of a half made cluster, a manifest naming no checkpoint, and the
    /// project's own uploads beside them.
    fn interrupted(store: &dyn CasStore, layout: &TenantLayout) {
        store
            .put(&layout.manifest(), &Manifest::new("local", 18).to_json())
            .unwrap();
        for key in [
            layout.pg_block(1663, 5, 1249, 0, 0),
            layout.pg_size(1663, 5, 1249, 0),
            layout.chk_file(GENESIS_ID, "global/pg_control"),
            format!("{}/log/00000000.frames", layout.prefix()),
        ] {
            store.put_if_absent(&key, b"scratch").unwrap();
        }
        store
            .put_if_absent(
                &format!("{}/files/avatars/one.png", layout.prefix()),
                b"png",
            )
            .unwrap();
        store
            .put_if_absent(&format!("{}/functions/DEPLOYED", layout.prefix()), b"{}")
            .unwrap();
    }

    #[test]
    fn what_an_unfinished_attach_left_goes_and_the_uploads_stay() {
        let dir = tempfile::tempdir().unwrap();
        let store = zou_store::LocalFsStore::new(dir.path());
        let layout = TenantLayout::new("local");
        interrupted(&store, &layout);

        // The manifest and the four scratch objects.
        assert_eq!(clear_unfinished(&store, &layout).unwrap(), 5);
        assert!(store.get(&layout.manifest()).unwrap().is_none());
        assert!(
            store
                .get(&layout.pg_block(1663, 5, 1249, 0, 0))
                .unwrap()
                .is_none(),
            "a page of the cluster that never finished is what the next initdb trips over"
        );
        assert!(
            store
                .get(&format!("{}/files/avatars/one.png", layout.prefix()))
                .unwrap()
                .is_some(),
            "an upload is not made by initdb and not read by a restore"
        );
        assert!(
            store
                .get(&format!("{}/functions/DEPLOYED", layout.prefix()))
                .unwrap()
                .is_some()
        );
        // And on a store with nothing there at all it is a no op.
        assert_eq!(
            clear_unfinished(&store, &TenantLayout::new("nobody")).unwrap(),
            0
        );
    }

    #[test]
    fn a_database_and_a_live_bootstrap_are_both_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let store = zou_store::LocalFsStore::new(dir.path());
        let layout = TenantLayout::new("local");
        interrupted(&store, &layout);

        let mut manifest = Manifest::new("local", 18);
        manifest.checkpoints.push(CheckpointRef {
            id: GENESIS_ID.to_string(),
            lsn: Lsn(0x100),
            kind: CheckpointKind::Full,
            owner: None,
        });
        store.put(&layout.manifest(), &manifest.to_json()).unwrap();
        let err = clear_unfinished(&store, &layout).unwrap_err();
        assert!(err.contains("already has a database"), "{err}");

        let mut manifest = Manifest::new("local", 18);
        manifest.lease = Some(zou_store::manifest::Lease {
            holder: "another-node".to_string(),
            expires_unix: now_unix() + 60,
            fence: 1,
            endpoint: None,
            ttl_secs: None,
        });
        store.put(&layout.manifest(), &manifest.to_json()).unwrap();
        let err = clear_unfinished(&store, &layout).unwrap_err();
        assert!(err.contains("another-node"), "{err}");

        // Nothing went in either case.
        assert!(
            store
                .get(&layout.pg_block(1663, 5, 1249, 0, 0))
                .unwrap()
                .is_some()
        );
    }
}
