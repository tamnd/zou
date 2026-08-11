//! Filesystem capture into checkpoint objects, shared by zou-bootstrap
//! and the checkpoint fold.
//!
//! A capture is a list of files and empty directories under a data
//! directory. Uploading one writes each file as `chk/<id>/fs/<relpath>`
//! plus an INDEX describing the tree, the exact format the restore path
//! reads back. Files are read into memory before anything is uploaded, so
//! one capture is a single point in time snapshot even when the server
//! keeps writing underneath it.

use std::path::{Path, PathBuf};

use zou_store::layout::TenantLayout;
use zou_store::{CasError, CasStore};

/// Files and empty directories found by [`walk`], paths relative to the
/// data directory.
#[derive(Default)]
pub struct Capture {
    pub files: Vec<(String, PathBuf)>,
    pub dirs: Vec<String>,
}

/// Recursively collect files and empty directories under `root/rel`.
/// `skip` filters relative paths out, per instance noise like
/// postmaster.pid has no business in a checkpoint.
pub fn walk(
    root: &Path,
    rel: &str,
    skip: &dyn Fn(&str) -> bool,
    out: &mut Capture,
) -> std::io::Result<()> {
    let dir = if rel.is_empty() {
        root.to_path_buf()
    } else {
        root.join(rel)
    };
    let mut empty = true;
    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let child = if rel.is_empty() {
            name.to_string()
        } else {
            format!("{rel}/{name}")
        };
        if skip(&child) {
            continue;
        }
        empty = false;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            walk(root, &child, skip, out)?;
        } else if kind.is_file() {
            out.files.push((child, entry.path()));
        }
    }
    if empty && !rel.is_empty() {
        out.dirs.push(rel.to_string());
    }
    Ok(())
}

/// Read every captured file into memory, sorted by path.
pub fn read_files(capture: &Capture) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut files = Vec::with_capacity(capture.files.len());
    for (relpath, path) in &capture.files {
        let data = std::fs::read(path).map_err(|e| format!("read {relpath}: {e}"))?;
        files.push((relpath.clone(), data));
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(files)
}

/// Trailing zeros a capture does not have to carry. A restore knows the
/// file's length from the INDEX and creates the file at that length, and
/// a file created at a length is zeros past what was written, so storing
/// the zeros is paying to move bytes the other side would have made
/// anyway. This is not a compression scheme and it is not guessing, it
/// is the one shape a data directory reliably has: files sized in whole
/// blocks with the last block part used, and the wal segment holding the
/// redo location, which is sixteen megabytes however little of it has
/// been written into.
fn without_trailing_zeros(data: &[u8]) -> &[u8] {
    let end = data.iter().rposition(|b| *b != 0).map_or(0, |i| i + 1);
    &data[..end]
}

/// Upload a capture as checkpoint `id` and write its INDEX. Returns the
/// bytes uploaded, which is what the store holds and so what a restore
/// pays to read, not what the files measure on disk. Objects under chk/
/// are immutable, so an object left by an earlier attempt of the same
/// checkpoint is kept when `keep_existing` is set, which keeps a retried
/// fold consistent with what the store already holds. Bootstrap passes
/// false and fails instead, mixing two initdb runs would corrupt.
pub fn upload(
    store: &dyn CasStore,
    layout: &TenantLayout,
    id: &str,
    files: &[(String, Vec<u8>)],
    dirs: &[String],
    keep_existing: bool,
) -> Result<u64, String> {
    let mut index = String::new();
    let mut bytes = 0u64;
    for (relpath, data) in files {
        let key = layout.chk_file(id, relpath);
        let body = without_trailing_zeros(data);
        let stored = match store.put_if_absent(&key, body) {
            Ok(_) => body.len() as u64,
            Err(CasError::AlreadyExists { .. }) if keep_existing => {
                let (existing, _) = store
                    .get(&key)
                    .map_err(|e| format!("store: {e}"))?
                    .ok_or_else(|| format!("chk object for {relpath} vanished"))?;
                existing.len() as u64
            }
            Err(CasError::AlreadyExists { .. }) => {
                return Err(format!("chk object for {relpath} already exists"));
            }
            Err(e) => return Err(format!("put {relpath}: {e}")),
        };
        bytes += stored;
        // An `f` line still means the object is the file. A trimmed one
        // needs both numbers, the file's length and the object's, and
        // says them before the path so a path with a space in it still
        // parses. A kept object from an earlier attempt is described the
        // same way, by what it holds now and the length the file has
        // here, which is the honest description of what a restore of
        // this checkpoint would produce.
        if stored == data.len() as u64 {
            index.push_str(&format!("f {relpath} {stored}\n"));
        } else {
            index.push_str(&format!("t {} {stored} {relpath}\n", data.len()));
        }
    }
    for dir in dirs {
        index.push_str(&format!("d {dir}\n"));
    }
    match store.put_if_absent(&layout.chk_index(id), index.as_bytes()) {
        Ok(_) => Ok(bytes),
        Err(CasError::AlreadyExists { .. }) if keep_existing => Ok(bytes),
        Err(e) => Err(format!("put index: {e}")),
    }
}

/// Skip list for a full capture taken while the server runs. Wal
/// segments live in the mirrored tail and restore rebuilds them from
/// it, the rest is per instance noise the server recreates on start.
/// [`full_capture`] carves out one exception for the segment holding
/// the redo location.
pub fn runtime_skip(relpath: &str) -> bool {
    if relpath == "postmaster.pid" || relpath == "postmaster.opts" || relpath == "current_logfiles"
    {
        return true;
    }
    if relpath.starts_with("log/") || relpath.starts_with("pg_stat_tmp/") {
        return true;
    }
    if let Some(name) = relpath.strip_prefix("pg_wal/") {
        return name.len() == 24 && name.bytes().all(|b| b.is_ascii_hexdigit());
    }
    relpath.rsplit('/').next() == Some("pgsql_tmp")
}

/// Whether a wal segment file name covers `lsn`. Names are 24 hex
/// digits, timeline then the two halves of the segment number, and the
/// timeline is ignored because a capture only ever sees its own.
fn wal_segment_holds(name: &str, lsn: u64) -> bool {
    if name.len() != 24 {
        return false;
    }
    let per_xlogid = 0x1_0000_0000 / crate::restore::WAL_SEGMENT_SIZE;
    match (
        u64::from_str_radix(&name[8..16], 16),
        u64::from_str_radix(&name[16..24], 16),
    ) {
        (Ok(hi), Ok(lo)) => hi * per_xlogid + lo == lsn / crate::restore::WAL_SEGMENT_SIZE,
        _ => false,
    }
}

/// Everything a full checkpoint captures from a running data directory.
/// Relation pages are absent locally under the zou storage manager, so a
/// full walk collects exactly the filesystem skeleton plus the non
/// relation state, which is what a node needs to attach. The wal segment
/// file holding `redo` is captured whole even though wal normally stays
/// out: the mirrored stream can begin mid segment, restore only rebuilds
/// segment files from the first mirrored byte onward, and recovery
/// refuses a segment whose first page header is zeroed. The bytes below
/// the mirrored coverage never change once written, and the restore
/// overlay overwrites everything from coverage on, so the captured copy
/// is always consistent with the stream.
pub fn full_capture(pgdata: &Path, redo: u64) -> Result<Capture, String> {
    let skip = |relpath: &str| {
        if let Some(name) = relpath.strip_prefix("pg_wal/")
            && wal_segment_holds(name, redo)
        {
            return false;
        }
        runtime_skip(relpath)
    };
    let mut out = Capture::default();
    walk(pgdata, "", &skip, &mut out).map_err(|e| format!("walk {}: {e}", pgdata.display()))?;
    out.files.sort();
    out.dirs.sort();
    Ok(out)
}

/// The non relation state a delta checkpoint captures. Relation pages
/// flow through the storage manager and WAL through the pusher, this is
/// the rest of what recovery reads: the control file, the transaction
/// status SLRUs, two phase state, the relation maps, and the config
/// files an operator can edit.
pub fn delta_capture(pgdata: &Path) -> Result<Capture, String> {
    let mut out = Capture::default();
    for dir in ["pg_xact", "pg_multixact", "pg_commit_ts", "pg_twophase"] {
        if pgdata.join(dir).is_dir() {
            walk(pgdata, dir, &|_| false, &mut out).map_err(|e| format!("walk {dir}: {e}"))?;
        }
    }
    for file in [
        "global/pg_control",
        "global/pg_filenode.map",
        "postgresql.auto.conf",
        "postgresql.conf",
        "pg_hba.conf",
        "pg_ident.conf",
    ] {
        let path = pgdata.join(file);
        if path.is_file() {
            out.files.push((file.to_string(), path));
        }
    }
    let base = pgdata.join("base");
    if base.is_dir() {
        for entry in std::fs::read_dir(&base).map_err(|e| format!("read base: {e}"))? {
            let entry = entry.map_err(|e| format!("read base: {e}"))?;
            let map = entry.path().join("pg_filenode.map");
            if map.is_file() {
                let rel = format!(
                    "base/{}/pg_filenode.map",
                    entry.file_name().to_string_lossy()
                );
                out.files.push((rel, map));
            }
        }
    }
    out.files.sort();
    out.dirs.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zou_store::LocalFsStore;

    #[test]
    fn a_delta_capture_collects_the_non_relation_state() {
        let dir = tempfile::tempdir().unwrap();
        let pgdata = dir.path();
        for d in [
            "pg_xact",
            "pg_multixact/offsets",
            "pg_multixact/members",
            "pg_commit_ts",
            "pg_twophase",
            "global",
            "base/1",
            "base/5",
        ] {
            std::fs::create_dir_all(pgdata.join(d)).unwrap();
        }
        std::fs::write(pgdata.join("pg_xact/0000"), b"clog").unwrap();
        std::fs::write(pgdata.join("pg_multixact/offsets/0000"), b"mx").unwrap();
        std::fs::write(pgdata.join("global/pg_control"), b"ctl").unwrap();
        std::fs::write(pgdata.join("global/pg_filenode.map"), b"map").unwrap();
        std::fs::write(pgdata.join("base/5/pg_filenode.map"), b"map5").unwrap();
        std::fs::write(pgdata.join("postgresql.auto.conf"), b"").unwrap();
        // Relation files must stay out, they live in the page store.
        std::fs::write(pgdata.join("base/5/16384"), b"page").unwrap();

        let capture = delta_capture(pgdata).unwrap();
        let paths: Vec<&str> = capture.files.iter().map(|(p, _)| p.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "base/5/pg_filenode.map",
                "global/pg_control",
                "global/pg_filenode.map",
                "pg_multixact/offsets/0000",
                "pg_xact/0000",
                "postgresql.auto.conf",
            ]
        );
        assert!(capture.dirs.contains(&"pg_twophase".to_string()));
        assert!(capture.dirs.contains(&"pg_commit_ts".to_string()));
        assert!(!paths.contains(&"base/5/16384"));
    }

    #[test]
    fn upload_keeps_objects_from_an_earlier_attempt_when_asked() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let layout = TenantLayout::new("local");
        store
            .put_if_absent(&layout.chk_file("d1", "pg_xact/0000"), b"older bytes")
            .unwrap();

        let files = vec![("pg_xact/0000".to_string(), b"newer".to_vec())];
        let err = upload(&store, &layout, "d1", &files, &[], false).unwrap_err();
        assert!(err.contains("already exists"));

        // With keep_existing the INDEX records the stored length, so the
        // description always matches the immutable object, and the
        // file's own length beside it.
        upload(&store, &layout, "d1", &files, &[], true).unwrap();
        let (index, _) = store.get(&layout.chk_index("d1")).unwrap().unwrap();
        assert_eq!(
            String::from_utf8(index).unwrap(),
            format!(
                "t {} {} pg_xact/0000\n",
                b"newer".len(),
                b"older bytes".len()
            )
        );
    }

    #[test]
    fn a_files_trailing_zeros_stay_out_of_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let layout = TenantLayout::new("local");
        let mut segment = b"a wal record".to_vec();
        segment.resize(16 * 1024 * 1024, 0);
        let files = vec![
            ("pg_wal/000000010000000000000001".to_string(), segment),
            ("global/pg_control".to_string(), b"control".to_vec()),
            // All zeros is a legitimate file and trims to nothing.
            ("pg_xact/0000".to_string(), vec![0u8; 8192]),
        ];

        let bytes = upload(&store, &layout, "genesis", &files, &[], false).unwrap();
        assert_eq!(bytes, (b"a wal record".len() + b"control".len()) as u64);
        let (index, _) = store.get(&layout.chk_index("genesis")).unwrap().unwrap();
        assert_eq!(
            String::from_utf8(index).unwrap(),
            "t 16777216 12 pg_wal/000000010000000000000001\n\
             f global/pg_control 7\n\
             t 8192 0 pg_xact/0000\n"
        );
    }
}
