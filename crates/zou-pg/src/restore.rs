//! Attach a node from the store alone: rebuild a data directory from the
//! checkpoint captures and overlay the mirrored WAL, so a plain server
//! start runs crash recovery to the end of the durable stream.
//!
//! Three steps.
//! The newest full capture under `chk/<id>/fs/` is written back exactly
//! as its INDEX describes it, then every delta after it in order, later
//! files overwriting earlier ones.
//! pg_control from a genesis capture says the cluster was shut down
//! cleanly, and a clean shutdown makes Postgres skip WAL replay and start
//! writing right after the old checkpoint, which would overwrite the
//! restored WAL. So the state field is flipped to in production, turning
//! the start into ordinary crash recovery. A delta pg_control comes from
//! a running server and already says so.
//! Then every record in the WAL stream, a Postgres LSN header plus raw
//! bytes, is written into the pg_wal segment file it came from, and
//! recovery replays to the last durable record.
//!
//! A branched tenant restores the same way with two twists: inherited
//! checkpoints read their files from the owner's prefix, and the frozen
//! parent tail entries replay before the tenant's own stream, oldest
//! ancestor first, because the child's WAL begins where the parent's
//! tail ended.
//!
//! Time travel is the same machinery pointed at a history snapshot:
//! [`restore_at`] picks the newest published manifest at or before a
//! timestamp and replays that manifest's own frozen tail, so the result
//! is exactly what an attach at that moment would have seen. The store
//! is never written.

use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use zou_store::commit::{reconcile_tail, segment_epoch, split_records};
use zou_store::layout::TenantLayout;
use zou_store::manifest::CheckpointKind;
use zou_store::{CasStore, Manifest, SegmentReader, open_store};

/// initdb's default, pinned for v0. The capture records no segment size,
/// and a cluster built with --wal-segsize would need it here.
pub const WAL_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;

/// pg_control layout facts this tool relies on, checked against the
/// vendored src/include/catalog/pg_control.h. The state field follows
/// system_identifier (8 bytes) and two version fields (4 bytes each).
const CONTROL_STATE_OFFSET: usize = 16;
/// checkPointCopy.redo: after state comes 4 bytes of alignment padding,
/// then pg_time_t time (8), XLogRecPtr checkPoint (8), and the CheckPoint
/// struct whose first field is the redo location.
const CONTROL_REDO_OFFSET: usize = 40;
const DB_SHUTDOWNED: u32 = 1;
const DB_IN_PRODUCTION: u32 = 6;

#[derive(Debug)]
pub struct RestoreStats {
    pub files: usize,
    pub dirs: usize,
    pub wal_records: usize,
    pub wal_bytes: u64,
    /// Postgres LSN right after the last restored WAL byte.
    pub wal_end: u64,
}

/// The WAL file name for a byte position, mirroring XLogFileName.
pub fn wal_file_name(tli: u32, lsn: u64) -> String {
    let segno = lsn / WAL_SEGMENT_SIZE;
    let per_xlogid = 0x1_0000_0000 / WAL_SEGMENT_SIZE;
    format!(
        "{:08X}{:08X}{:08X}",
        tli,
        segno / per_xlogid,
        segno % per_xlogid
    )
}

/// Locate the pg_control crc field. Its offset inside the file depends on
/// the struct layout, so it is discovered instead of assumed: it is the
/// unique position whose stored word is the crc32c of everything before
/// it. A torn or corrupt file has no such position and errors out, which
/// doubles as an integrity check for callers reading pg_control from
/// under a running server.
pub fn control_crc_offset(control: &[u8]) -> Result<usize, String> {
    if control.len() < 512 {
        return Err(format!("pg_control is {} bytes, too small", control.len()));
    }
    let mut crc_offset = None;
    for k in (CONTROL_STATE_OFFSET + 4)..control.len().min(1024) - 4 {
        let stored = u32::from_le_bytes(control[k..k + 4].try_into().expect("in bounds"));
        if crc32c::crc32c(&control[..k]) == stored {
            if crc_offset.is_some() {
                return Err("pg_control crc position is ambiguous".into());
            }
            crc_offset = Some(k);
        }
    }
    crc_offset.ok_or_else(|| "pg_control crc not found, file layout not understood".into())
}

/// The redo location of the last completed checkpoint recorded in
/// pg_control. Validates the crc first, so a torn concurrent read fails
/// instead of returning garbage.
pub fn control_redo(control: &[u8]) -> Result<u64, String> {
    control_crc_offset(control)?;
    let bytes: [u8; 8] = control[CONTROL_REDO_OFFSET..CONTROL_REDO_OFFSET + 8]
        .try_into()
        .expect("length checked by control_crc_offset");
    Ok(u64::from_le_bytes(bytes))
}

/// Make pg_control say in production, recomputing the crc. A genesis
/// capture says cleanly shut down, and a clean shutdown makes Postgres
/// skip WAL replay and start writing right after the old checkpoint,
/// which would overwrite the restored WAL, so it is flipped into ordinary
/// crash recovery. A delta capture comes from a running server and
/// already says in production, nothing to do. Anything else fails loudly,
/// this touches the one file Postgres trusts blindly.
pub fn patch_control_state(control: &mut [u8]) -> Result<(), String> {
    let k = control_crc_offset(control)?;
    let state_bytes: [u8; 4] = control[CONTROL_STATE_OFFSET..CONTROL_STATE_OFFSET + 4]
        .try_into()
        .expect("checked length");
    let state = u32::from_le_bytes(state_bytes);
    if state == DB_IN_PRODUCTION {
        return Ok(());
    }
    if state != DB_SHUTDOWNED {
        return Err(format!(
            "pg_control state is {state}, expected shut down or in production"
        ));
    }
    control[CONTROL_STATE_OFFSET..CONTROL_STATE_OFFSET + 4]
        .copy_from_slice(&DB_IN_PRODUCTION.to_le_bytes());
    let crc = crc32c::crc32c(&control[..k]);
    control[k..k + 4].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .map_err(|e| format!("chmod {}: {e}", path.display()))
}

/// Postgres does not enforce data directory modes on Windows.
#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) -> Result<(), String> {
    Ok(())
}

/// Rebuild the captured tree under `pgdata` from the checkpoint INDEX.
fn restore_fs(
    store: &dyn CasStore,
    layout: &TenantLayout,
    chk_id: &str,
    pgdata: &Path,
) -> Result<(usize, usize), String> {
    let (index, _) = store
        .get(&layout.chk_index(chk_id))
        .map_err(|e| format!("store: {e}"))?
        .ok_or_else(|| format!("checkpoint {chk_id} has no INDEX object"))?;
    let index = String::from_utf8(index).map_err(|_| "INDEX is not utf8".to_string())?;

    let mut files = 0usize;
    let mut dirs = 0usize;
    for line in index.lines() {
        if let Some(rest) = line.strip_prefix("f ") {
            let (relpath, size) = rest
                .rsplit_once(' ')
                .ok_or_else(|| format!("bad INDEX line {line:?}"))?;
            let size: u64 = size
                .parse()
                .map_err(|_| format!("bad INDEX line {line:?}"))?;
            let (data, _) = store
                .get(&layout.chk_file(chk_id, relpath))
                .map_err(|e| format!("store: {e}"))?
                .ok_or_else(|| format!("checkpoint object for {relpath} is missing"))?;
            if data.len() as u64 != size {
                return Err(format!(
                    "{relpath} is {} bytes in the store, INDEX says {size}",
                    data.len()
                ));
            }
            let path = pgdata.join(relpath);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
            }
            std::fs::write(&path, &data).map_err(|e| format!("write {relpath}: {e}"))?;
            set_mode(&path, 0o600)?;
            files += 1;
        } else if let Some(relpath) = line.strip_prefix("d ") {
            std::fs::create_dir_all(pgdata.join(relpath))
                .map_err(|e| format!("mkdir {relpath}: {e}"))?;
            dirs += 1;
        } else if !line.is_empty() {
            return Err(format!("bad INDEX line {line:?}"));
        }
    }
    Ok((files, dirs))
}

/// Write one chunk of WAL at its Postgres LSN, creating zero filled
/// segment files as needed. Chunks from the pusher never cross a segment
/// boundary, but crossing is handled anyway.
fn overlay_wal_chunk(
    pg_wal: &Path,
    tli: u32,
    mut lsn: u64,
    mut bytes: &[u8],
) -> Result<(), String> {
    while !bytes.is_empty() {
        let off = lsn % WAL_SEGMENT_SIZE;
        let n = ((WAL_SEGMENT_SIZE - off) as usize).min(bytes.len());
        let path = pg_wal.join(wal_file_name(tli, lsn));
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| format!("open {}: {e}", path.display()))?;
        let len = file
            .metadata()
            .map_err(|e| format!("stat {}: {e}", path.display()))?
            .len();
        if len < WAL_SEGMENT_SIZE {
            file.set_len(WAL_SEGMENT_SIZE)
                .map_err(|e| format!("grow {}: {e}", path.display()))?;
        }
        file.seek(SeekFrom::Start(off))
            .map_err(|e| format!("seek {}: {e}", path.display()))?;
        file.write_all(&bytes[..n])
            .map_err(|e| format!("write {}: {e}", path.display()))?;
        lsn += n as u64;
        bytes = &bytes[n..];
    }
    Ok(())
}

/// Replay one list of mirrored segments into pg_wal. Returns records
/// written, chunk bytes, and the LSN right after the last byte.
fn overlay_segments(
    store: &dyn CasStore,
    layout: &TenantLayout,
    segments: &[String],
    tli: u32,
    pg_wal: &Path,
) -> Result<(usize, u64, u64), String> {
    let mut records = 0usize;
    let mut total = 0u64;
    let mut end = 0u64;
    for name in segments {
        let epoch = segment_epoch(name).ok_or_else(|| format!("bad segment name {name:?}"))?;
        let (bytes, _) = store
            .get(&layout.wal_segment_path(name))
            .map_err(|e| format!("store: {e}"))?
            .ok_or_else(|| format!("segment {name} is missing"))?;
        for frame in SegmentReader::new(&bytes, epoch) {
            // Segments upload whole, so a decode failure is real
            // corruption, never a torn tail.
            let frame = frame.map_err(|e| format!("segment {name}: {e}"))?;
            for record in
                split_records(&frame.payload).ok_or_else(|| format!("bad batch in {name}"))?
            {
                if record.len() < 8 {
                    return Err(format!("short record in {name}"));
                }
                let lsn = u64::from_le_bytes(record[..8].try_into().expect("checked"));
                let chunk = &record[8..];
                overlay_wal_chunk(pg_wal, tli, lsn, chunk)?;
                records += 1;
                total += chunk.len() as u64;
                end = end.max(lsn + chunk.len() as u64);
            }
        }
    }
    Ok((records, total, end))
}

/// Restore a tenant's data directory from the store into `pgdata`, which
/// must not already exist. Returns what was rebuilt; a plain server start
/// on the result completes the attach.
pub fn restore(store_root: &str, tenant: &str, pgdata: &Path) -> Result<RestoreStats, String> {
    let store: Arc<dyn CasStore> = Arc::from(open_store(store_root)?);
    let layout = TenantLayout::new(tenant);

    let (data, _) = store
        .get(&layout.manifest())
        .map_err(|e| format!("store: {e}"))?
        .ok_or_else(|| format!("{store_root} has no manifest, nothing to restore"))?;
    let manifest = Manifest::from_json(&data).map_err(|e| format!("manifest: {e}"))?;
    // The live head may hold segments published after the manifest's own
    // tail was folded, so the tail is reconciled against the wal/ listing.
    let tail = reconcile_tail(&*store, &layout, &manifest).map_err(|e| format!("store: {e}"))?;
    restore_manifest(&*store, &layout, &manifest, tail, pgdata)
}

/// Restore the newest history snapshot of `tenant` at or before
/// `unix_ts` into `pgdata`. This is time travel as a read only attach:
/// nothing in the store changes, and the snapshot's own frozen wal_tail
/// replays verbatim. Listing live epoch dirs here would pull in WAL
/// written after the snapshot, so reconcile_tail is deliberately not
/// used.
pub fn restore_at(
    store_root: &str,
    tenant: &str,
    unix_ts: u64,
    pgdata: &Path,
) -> Result<RestoreStats, String> {
    let store: Arc<dyn CasStore> = Arc::from(open_store(store_root)?);
    let layout = TenantLayout::new(tenant);
    let snapshot = zou_store::snapshot_at(&*store, tenant, unix_ts).map_err(|e| e.to_string())?;
    let tail = snapshot.wal_tail.clone();
    restore_manifest(&*store, &layout, &snapshot, tail, pgdata)
}

/// Materialize one manifest, live head or history snapshot, into a fresh
/// `pgdata` and overlay `tail` on top of any inherited parent tails.
fn restore_manifest(
    store: &dyn CasStore,
    layout: &TenantLayout,
    manifest: &Manifest,
    tail: Option<zou_store::manifest::WalTail>,
    pgdata: &Path,
) -> Result<RestoreStats, String> {
    if pgdata.exists() {
        return Err(format!(
            "{} already exists, refusing to restore over it",
            pgdata.display()
        ));
    }
    let full = manifest
        .checkpoints
        .iter()
        .rposition(|c| c.kind == CheckpointKind::Full)
        .ok_or_else(|| "manifest has no full checkpoint, run zou-bootstrap first".to_string())?;
    // The newest full capture plus every delta after it, in order. Deltas
    // only carry what changed, so later files overwrite earlier ones and
    // the last pg_control wins.
    let chain = &manifest.checkpoints[full..];

    std::fs::create_dir_all(pgdata).map_err(|e| format!("mkdir {}: {e}", pgdata.display()))?;
    set_mode(pgdata, 0o700)?;
    let mut files = 0usize;
    let mut dirs = 0usize;
    for checkpoint in chain {
        let lay = crate::fold::chk_layout(layout, checkpoint);
        let (f, d) = restore_fs(store, &lay, &checkpoint.id, pgdata)?;
        files += f;
        dirs += d;
    }

    let control_path = pgdata.join("global/pg_control");
    let mut control = std::fs::read(&control_path).map_err(|e| format!("read pg_control: {e}"))?;
    patch_control_state(&mut control)?;
    std::fs::write(&control_path, &control).map_err(|e| format!("write pg_control: {e}"))?;

    let mut wal_records = 0usize;
    let mut wal_bytes = 0u64;
    let mut wal_end = chain.last().expect("chain is nonempty").lsn.0;
    let tli = manifest.pg.timeline;
    let pg_wal = pgdata.join("pg_wal");
    // Inherited parent tails first, oldest ancestor to newest, then the
    // tenant's own stream on top of them.
    for pt in &manifest.parent_tail {
        let lay = TenantLayout::new(&pt.tenant_ref);
        let (r, b, e) = overlay_segments(store, &lay, &pt.segments, tli, &pg_wal)?;
        wal_records += r;
        wal_bytes += b;
        wal_end = wal_end.max(e);
    }
    if let Some(tail) = tail {
        let (r, b, e) = overlay_segments(store, layout, &tail.segments, tli, &pg_wal)?;
        wal_records += r;
        wal_bytes += b;
        wal_end = wal_end.max(e);
    }

    Ok(RestoreStats {
        files,
        dirs,
        wal_records,
        wal_bytes,
        wal_end,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use zou_store::manifest::{CheckpointRef, WalTail};
    use zou_store::{GroupCommit, GroupCommitConfig, LocalFsStore, Lsn};

    #[test]
    fn wal_file_names_match_postgres() {
        assert_eq!(wal_file_name(1, 0), "000000010000000000000000");
        assert_eq!(
            wal_file_name(1, WAL_SEGMENT_SIZE),
            "000000010000000000000001"
        );
        assert_eq!(wal_file_name(1, 0xFFFFFF), "000000010000000000000000");
        assert_eq!(wal_file_name(1, 0x1_0000_0000), "000000010000000100000000");
        assert_eq!(wal_file_name(3, 0x01F2_F498), "000000030000000000000001");
    }

    fn synthetic_control(state: u32) -> Vec<u8> {
        // Shaped like ControlFileData: some header words, the state at
        // offset 16, the checkpoint redo at 40, junk, then the crc over
        // everything before it at an arbitrary struct dependent position,
        // then zero padding.
        let mut control = vec![0u8; 8192];
        control[0..8].copy_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());
        control[8..12].copy_from_slice(&1800u32.to_le_bytes());
        control[12..16].copy_from_slice(&20_250_601_u32.to_le_bytes());
        control[16..20].copy_from_slice(&state.to_le_bytes());
        for (i, b) in control[20..300].iter_mut().enumerate() {
            *b = (i * 7 + 13) as u8;
        }
        control[40..48].copy_from_slice(&0x0100_0028u64.to_le_bytes());
        let crc = crc32c::crc32c(&control[..300]);
        control[300..304].copy_from_slice(&crc.to_le_bytes());
        control
    }

    #[test]
    fn the_control_state_patch_finds_the_crc_and_flips_the_state() {
        let mut control = synthetic_control(DB_SHUTDOWNED);
        patch_control_state(&mut control).unwrap();
        let state = u32::from_le_bytes(control[16..20].try_into().unwrap());
        assert_eq!(state, DB_IN_PRODUCTION);
        let crc = u32::from_le_bytes(control[300..304].try_into().unwrap());
        assert_eq!(crc, crc32c::crc32c(&control[..300]), "crc recomputed");
    }

    #[test]
    fn the_control_state_patch_leaves_a_running_capture_alone() {
        let mut control = synthetic_control(DB_IN_PRODUCTION);
        let before = control.clone();
        patch_control_state(&mut control).unwrap();
        assert_eq!(control, before, "in production is already right");
    }

    #[test]
    fn the_control_state_patch_refuses_unknown_states() {
        // 2 is DB_SHUTDOWNING, a capture mid shutdown is not restorable.
        let mut control = synthetic_control(2);
        assert!(patch_control_state(&mut control).is_err());
    }

    #[test]
    fn control_redo_reads_the_checkpoint_redo_and_rejects_torn_files() {
        let control = synthetic_control(DB_IN_PRODUCTION);
        assert_eq!(control_redo(&control).unwrap(), 0x0100_0028);
        let mut torn = control.clone();
        torn[100] ^= 0xFF;
        assert!(control_redo(&torn).is_err(), "crc catches the tear");
    }

    #[test]
    fn a_store_restores_to_a_working_tree_with_wal_overlaid() {
        let store_dir = tempfile::tempdir().unwrap();
        let out_dir = tempfile::tempdir().unwrap();
        let store_root = store_dir.path().to_str().unwrap();
        let store = LocalFsStore::new(store_dir.path());
        let layout = TenantLayout::new("local");

        // A miniature genesis capture: pg_control, one config file, an
        // initial WAL segment full of marker bytes, one empty dir.
        let control = synthetic_control(DB_SHUTDOWNED);
        let initial_segment = vec![0x11u8; WAL_SEGMENT_SIZE as usize];
        store
            .put_if_absent(&layout.chk_file("genesis", "global/pg_control"), &control)
            .unwrap();
        store
            .put_if_absent(&layout.chk_file("genesis", "PG_VERSION"), b"18\n")
            .unwrap();
        store
            .put_if_absent(
                &layout.chk_file("genesis", "pg_wal/000000010000000000000001"),
                &initial_segment,
            )
            .unwrap();
        let index = format!(
            "f PG_VERSION 3\nf global/pg_control {}\nf pg_wal/000000010000000000000001 {}\nd pg_wal/archive_status\n",
            control.len(),
            initial_segment.len()
        );
        store
            .put_if_absent(&layout.chk_index("genesis"), index.as_bytes())
            .unwrap();
        // A delta checkpoint after genesis: a newer pg_control already in
        // production and a clog segment, later files overwrite earlier.
        let delta_control = synthetic_control(DB_IN_PRODUCTION);
        store
            .put_if_absent(&layout.chk_file("d1", "global/pg_control"), &delta_control)
            .unwrap();
        store
            .put_if_absent(&layout.chk_file("d1", "pg_xact/0000"), b"delta clog")
            .unwrap();
        let delta_index = format!(
            "f global/pg_control {}\nf pg_xact/0000 10\n",
            delta_control.len()
        );
        store
            .put_if_absent(&layout.chk_index("d1"), delta_index.as_bytes())
            .unwrap();

        let mut manifest = Manifest::new("local", 18);
        manifest.checkpoints.push(CheckpointRef {
            id: "genesis".into(),
            lsn: Lsn(0x0100_0028),
            kind: CheckpointKind::Full,
            owner: None,
        });
        manifest.checkpoints.push(CheckpointRef {
            id: "d1".into(),
            lsn: Lsn(0x0100_1000),
            kind: CheckpointKind::Delta,
            owner: None,
        });
        store
            .put_if_absent(&layout.manifest(), &manifest.to_json())
            .unwrap();

        // A pusher session: two chunks, the second crossing into the next
        // 16MB segment to exercise the split.
        let gc = GroupCommit::new(
            Arc::new(LocalFsStore::new(store_dir.path())) as Arc<dyn CasStore>,
            layout.clone(),
            1,
            1,
            Lsn(0x0100_0028),
            GroupCommitConfig::default(),
        );
        let push = |pg_lsn: u64, fill: u8, len: usize| {
            let mut record = pg_lsn.to_le_bytes().to_vec();
            record.extend(std::iter::repeat_n(fill, len));
            gc.append(&record).unwrap().wait().unwrap();
        };
        push(0x0100_0028, 0x22, 4096);
        push(2 * WAL_SEGMENT_SIZE - 100, 0x33, 300);
        gc.close().unwrap();

        let pgdata = out_dir.path().join("restored");
        let stats = restore(store_root, "local", &pgdata).unwrap();
        assert_eq!(stats.files, 5, "three genesis files plus two delta");
        assert_eq!(stats.dirs, 1);
        assert_eq!(stats.wal_records, 2);
        assert_eq!(stats.wal_bytes, 4096 + 300);
        assert_eq!(stats.wal_end, 2 * WAL_SEGMENT_SIZE + 200);

        // The tree came back, including the empty dir and the delta's
        // additions on top of genesis.
        assert_eq!(std::fs::read(pgdata.join("PG_VERSION")).unwrap(), b"18\n");
        assert!(pgdata.join("pg_wal/archive_status").is_dir());
        assert_eq!(
            std::fs::read(pgdata.join("pg_xact/0000")).unwrap(),
            b"delta clog"
        );

        // The delta pg_control won and stayed in production untouched.
        let restored_control = std::fs::read(pgdata.join("global/pg_control")).unwrap();
        assert_eq!(restored_control, delta_control);
        let state = u32::from_le_bytes(restored_control[16..20].try_into().unwrap());
        assert_eq!(state, DB_IN_PRODUCTION);

        // The first chunk landed inside the genesis segment without
        // disturbing the surrounding capture bytes.
        let seg1 = std::fs::read(pgdata.join("pg_wal/000000010000000000000001")).unwrap();
        let off = 0x0100_0028 % WAL_SEGMENT_SIZE;
        assert_eq!(seg1[off as usize - 1], 0x11);
        assert!(
            seg1[off as usize..off as usize + 4096]
                .iter()
                .all(|b| *b == 0x22)
        );
        assert_eq!(seg1[off as usize + 4096], 0x11);

        // The boundary crossing chunk split across two files, the second
        // created zero filled.
        assert!(
            seg1[(WAL_SEGMENT_SIZE - 100) as usize..]
                .iter()
                .all(|b| *b == 0x33)
        );
        let seg2 = std::fs::read(pgdata.join("pg_wal/000000010000000000000002")).unwrap();
        assert_eq!(seg2.len() as u64, WAL_SEGMENT_SIZE);
        assert!(seg2[..200].iter().all(|b| *b == 0x33));
        assert!(seg2[200..].iter().all(|b| *b == 0));

        // A second restore refuses to clobber the first.
        assert!(restore(store_root, "local", &pgdata).is_err());
    }

    #[test]
    fn restore_at_replays_the_snapshot_tail_not_the_live_head() {
        let store_dir = tempfile::tempdir().unwrap();
        let out_dir = tempfile::tempdir().unwrap();
        let store_root = store_dir.path().to_str().unwrap();
        let store = LocalFsStore::new(store_dir.path());
        let layout = TenantLayout::new("local");

        let control = synthetic_control(DB_SHUTDOWNED);
        let initial_segment = vec![0x11u8; WAL_SEGMENT_SIZE as usize];
        store
            .put_if_absent(&layout.chk_file("genesis", "global/pg_control"), &control)
            .unwrap();
        store
            .put_if_absent(&layout.chk_file("genesis", "PG_VERSION"), b"18\n")
            .unwrap();
        store
            .put_if_absent(
                &layout.chk_file("genesis", "pg_wal/000000010000000000000001"),
                &initial_segment,
            )
            .unwrap();
        let index = format!(
            "f PG_VERSION 3\nf global/pg_control {}\nf pg_wal/000000010000000000000001 {}\n",
            control.len(),
            initial_segment.len()
        );
        store
            .put_if_absent(&layout.chk_index("genesis"), index.as_bytes())
            .unwrap();
        let mut manifest = Manifest::new("local", 18);
        manifest.checkpoints.push(CheckpointRef {
            id: "genesis".into(),
            lsn: Lsn(0x0100_0028),
            kind: CheckpointKind::Full,
            owner: None,
        });
        store
            .put_if_absent(&layout.manifest(), &manifest.to_json())
            .unwrap();

        // Two pusher sessions, one sealed segment each, so the store
        // ends up holding WAL from after the snapshot below.
        let push = |epoch: u64, pg_lsn: u64, fill: u8, len: usize| {
            let gc = GroupCommit::new(
                Arc::new(LocalFsStore::new(store_dir.path())) as Arc<dyn CasStore>,
                layout.clone(),
                epoch,
                epoch,
                Lsn(epoch * 0x1000),
                GroupCommitConfig::default(),
            );
            let mut record = pg_lsn.to_le_bytes().to_vec();
            record.extend(std::iter::repeat_n(fill, len));
            gc.append(&record).unwrap().wait().unwrap();
            gc.close().unwrap();
        };
        push(1, 0x0100_0028, 0x22, 4096);
        push(2, 0x0100_5000, 0x33, 100);
        let live = reconcile_tail(&store, &layout, &manifest)
            .unwrap()
            .expect("two sessions sealed segments");
        assert_eq!(live.segments.len(), 2);

        // A history snapshot published between the two sessions: it
        // froze the tail at the first segment, the second is future to
        // it even though the live listing holds both.
        let mut snapshot = manifest.clone();
        snapshot.wal_tail = Some(WalTail {
            epoch_dir: 1,
            from_lsn: Lsn(0x0100_0028),
            segments: vec![live.segments[0].clone()],
        });
        store
            .put_if_absent(&layout.manifest_history(1, 1000), &snapshot.to_json())
            .unwrap();

        // Time travel to the snapshot: only the first session's record
        // replays, the store's newer WAL stays out of the tree.
        let at_dir = out_dir.path().join("at");
        let stats = restore_at(store_root, "local", 1500, &at_dir).unwrap();
        assert_eq!(stats.wal_records, 1);
        assert_eq!(stats.wal_bytes, 4096);
        let seg1 = std::fs::read(at_dir.join("pg_wal/000000010000000000000001")).unwrap();
        let off = (0x0100_0028 % WAL_SEGMENT_SIZE) as usize;
        assert!(seg1[off..off + 4096].iter().all(|b| *b == 0x22));
        let later = (0x0100_5000 % WAL_SEGMENT_SIZE) as usize;
        assert_eq!(seg1[later], 0x11, "the newer record never landed");

        // The live restore of the same store replays both records.
        let live_dir = out_dir.path().join("live");
        let stats = restore(store_root, "local", &live_dir).unwrap();
        assert_eq!(stats.wal_records, 2);
        assert_eq!(stats.wal_bytes, 4096 + 100);

        // Before the earliest snapshot there is nothing to travel to.
        let err = restore_at(store_root, "local", 500, &out_dir.path().join("gone")).unwrap_err();
        assert!(err.contains("no history"), "{err}");
    }

    #[test]
    fn a_branch_restores_parent_files_and_replays_the_parent_tail() {
        use std::sync::Mutex;
        use zou_store::{TailConfig, lease};

        let store_dir = tempfile::tempdir().unwrap();
        let out_dir = tempfile::tempdir().unwrap();
        let store_root = store_dir.path().to_str().unwrap();
        let store = Arc::new(LocalFsStore::new(store_dir.path()));
        let layout = TenantLayout::new("local");

        let control = synthetic_control(DB_SHUTDOWNED);
        let initial_segment = vec![0x11u8; WAL_SEGMENT_SIZE as usize];
        store
            .put_if_absent(&layout.chk_file("genesis", "global/pg_control"), &control)
            .unwrap();
        store
            .put_if_absent(&layout.chk_file("genesis", "PG_VERSION"), b"18\n")
            .unwrap();
        store
            .put_if_absent(
                &layout.chk_file("genesis", "pg_wal/000000010000000000000001"),
                &initial_segment,
            )
            .unwrap();
        let index = format!(
            "f PG_VERSION 3\nf global/pg_control {}\nf pg_wal/000000010000000000000001 {}\n",
            control.len(),
            initial_segment.len()
        );
        store
            .put_if_absent(&layout.chk_index("genesis"), index.as_bytes())
            .unwrap();
        let mut manifest = Manifest::new("local", 18);
        manifest.checkpoints.push(CheckpointRef {
            id: "genesis".into(),
            lsn: Lsn(0x0100_0028),
            kind: CheckpointKind::Full,
            owner: None,
        });
        store
            .put_if_absent(&layout.manifest(), &manifest.to_json())
            .unwrap();

        // A leased session pushes and publishes the tail, which is the
        // state a branch inherits.
        let held = lease::acquire(&*store, &layout, "test", 15, 1000).unwrap();
        let gc = GroupCommit::with_lease(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            Arc::new(Mutex::new(held)),
            Lsn(0x0100_0028),
            GroupCommitConfig::default(),
            TailConfig::default(),
        );
        let mut record = 0x0100_0028u64.to_le_bytes().to_vec();
        record.extend(std::iter::repeat_n(0x22u8, 4096));
        gc.append(&record).unwrap().wait().unwrap();
        gc.close().unwrap();

        zou_store::branch(&*store, "local", "b1", None, 5000).unwrap();

        let pgdata = out_dir.path().join("child");
        let stats = restore(store_root, "b1", &pgdata).unwrap();
        assert_eq!(stats.files, 3, "the tree comes from the parent capture");
        assert_eq!(stats.wal_records, 1, "the parent tail replays");
        assert_eq!(stats.wal_bytes, 4096);

        assert_eq!(std::fs::read(pgdata.join("PG_VERSION")).unwrap(), b"18\n");
        let seg1 = std::fs::read(pgdata.join("pg_wal/000000010000000000000001")).unwrap();
        let off = (0x0100_0028 % WAL_SEGMENT_SIZE) as usize;
        assert!(seg1[off..off + 4096].iter().all(|b| *b == 0x22));
        assert_eq!(seg1[off - 1], 0x11, "capture bytes around it survive");
    }
}
