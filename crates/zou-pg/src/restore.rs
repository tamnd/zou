//! Attach a node from the store alone: rebuild a data directory from the
//! checkpoint captures and overlay the tenant's WAL out of the shared
//! log, so a plain server start runs crash recovery to the end of the
//! durable stream.
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
//! Then every frame of the tenant's stream past the newest checkpoint,
//! a raw chunk with its Postgres LSN range, is written into the pg_wal
//! segment file it came from, and recovery replays to the last durable
//! frame.
//!
//! A branched tenant restores the same way with one twist: inherited
//! checkpoints read their files from the owner's prefix. Branches cut
//! at a checkpoint lsn, so a child inherits no unfolded WAL.
//!
//! Time travel is the same machinery pointed at a history snapshot:
//! [`restore_at`] picks the newest published manifest at or before a
//! timestamp and stops at that manifest's newest checkpoint. WAL past
//! it replays nothing, the shared log moved on after the snapshot, so
//! point in time granularity is the fold cadence in this release. The
//! store is never written.

use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use zou_log::{TeeFilter, WalMedia, catch_up_with};
use zou_store::layout::TenantLayout;
use zou_store::manifest::CheckpointKind;
use zou_store::{CasStore, Lsn, Manifest, open_store, tenant_id};

use crate::WAL_SHARD;

/// initdb's default, pinned for v0. The capture records no segment size,
/// and a cluster built with --wal-segsize would need it here.
pub const WAL_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;

/// XLOG_BLCKSZ, initdb's default WAL page size.
pub(crate) const WAL_PAGE_SIZE: u64 = 8192;

/// pg_control layout facts this tool relies on, checked against the
/// vendored src/include/catalog/pg_control.h. The state field follows
/// system_identifier (8 bytes) and two version fields (4 bytes each).
const CONTROL_STATE_OFFSET: usize = 16;
/// checkPoint, the lsn of the last checkpoint record itself: after state
/// come 4 bytes of alignment padding and pg_time_t time (8). Recovery
/// starts by reading this record, so an attach with no live stream must
/// still hold the bytes it names.
const CONTROL_CHECKPOINT_OFFSET: usize = 32;
/// checkPointCopy.redo: right after checkPoint comes the CheckPoint
/// struct whose first field is the redo location.
const CONTROL_REDO_OFFSET: usize = 40;
/// Bytes from data_checksum_version to the crc: the uint32 itself is
/// followed by default_char_signedness (1), mock_authentication_nonce
/// (32) and three bytes of padding that align the crc, so the crc
/// starts 40 bytes after the field begins. Checked against a real
/// pg_control in the tests below.
const CONTROL_CHECKSUM_BACK: usize = 40;
/// MOCK_AUTH_NONCE_LEN from the vendored pg_control.h, which the
/// distance above is made of.
#[cfg(test)]
const MOCK_AUTH_NONCE_LEN: usize = 32;
const DB_SHUTDOWNED: u32 = 1;
const DB_IN_PRODUCTION: u32 = 6;

#[derive(Debug, Default)]
pub struct RestoreStats {
    pub files: usize,
    pub dirs: usize,
    pub wal_records: usize,
    pub wal_bytes: u64,
    /// Postgres LSN right after the last restored WAL byte.
    pub wal_end: u64,
    /// The timeline the restored WAL is on, which is what names its
    /// segment files. A warm up reads them back, see [`crate::warm`].
    pub timeline: u32,
    /// How long the skeleton took: the manifest, the checkpoint files,
    /// pg_control and the WAL tail.
    pub skeleton_took: Duration,
    /// How long the WAL catch up took, which is the shared log read from
    /// the newest checkpoint's redo page to the end of the stream.
    pub wal_took: Duration,
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

/// The lsn of the last checkpoint record itself, the position recovery
/// reads first. Sits at or after the redo location: they are equal for
/// a shutdown checkpoint and the record trails the redo for an online
/// one.
pub fn control_checkpoint(control: &[u8]) -> Result<u64, String> {
    control_crc_offset(control)?;
    let bytes: [u8; 8] = control[CONTROL_CHECKPOINT_OFFSET..CONTROL_CHECKPOINT_OFFSET + 8]
        .try_into()
        .expect("length checked by control_crc_offset");
    Ok(u64::from_le_bytes(bytes))
}

/// Whether the cluster protects its data pages with checksums.
///
/// Anything that materializes a page for this cluster has to answer
/// this the same way the cluster does. A page built without the
/// checksum a checksummed cluster expects reads back as "page
/// verification failed, calculated N but expected 0", and the read
/// that finds it is usually recovery, so the cost of guessing is a
/// server that will not start.
///
/// The field is the last uint32 before the trailing bool, the 32 byte
/// nonce and the alignment padding the crc sits after, so it is found
/// from the crc rather than from the front of the struct: everything
/// ahead of it moves between releases and nothing behind it does.
/// Validating the crc first means a torn read errors rather than
/// answering no.
pub fn control_data_checksums(control: &[u8]) -> Result<bool, String> {
    let crc_offset = control_crc_offset(control)?;
    let at = crc_offset
        .checked_sub(CONTROL_CHECKSUM_BACK)
        .ok_or_else(|| "pg_control crc sits too early to hold a checksum version".to_string())?;
    let version = u32::from_le_bytes(control[at..at + 4].try_into().expect("in bounds"));
    Ok(version != 0)
}

/// Whether the tenant's cluster uses data checksums, read out of the
/// store rather than assumed.
///
/// Anything that builds pages outside the postmaster has to know this,
/// and outside the postmaster there is no `DataChecksumsEnabled()` to
/// ask. The answer is in the pg_control of the newest capture that
/// carries one: a delta checkpoint only holds the files that changed,
/// so the walk goes newest first and stops at the first capture that
/// has the file. The setting is fixed at initdb, so any capture's
/// answer is the cluster's answer and the newest is simply the one
/// most likely to still be there.
pub fn store_data_checksums(store: &dyn CasStore, tenant_ref: &str) -> Result<bool, String> {
    let layout = TenantLayout::new(tenant_ref);
    let (data, _) = store
        .get(&layout.manifest())
        .map_err(|e| format!("store: {e}"))?
        .ok_or_else(|| format!("no manifest at {}", layout.manifest()))?;
    let manifest = Manifest::from_json(&data).map_err(|e| format!("manifest: {e}"))?;
    for checkpoint in manifest.checkpoints.iter().rev() {
        let lay = crate::fold::chk_layout(&layout, checkpoint);
        let key = lay.chk_file(&checkpoint.id, "global/pg_control");
        if let Some((control, _)) = store.get(&key).map_err(|e| format!("store: {e}"))? {
            return control_data_checksums(&control);
        }
    }
    Err("no capture in the manifest carries a pg_control".into())
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

    // Path, the file's length, and the object's, which differ when the
    // capture dropped a file's trailing zeros.
    let mut files: Vec<(&str, u64, u64)> = Vec::new();
    let mut dirs = 0usize;
    for line in index.lines() {
        if let Some(rest) = line.strip_prefix("f ") {
            let (relpath, size) = rest
                .rsplit_once(' ')
                .ok_or_else(|| format!("bad INDEX line {line:?}"))?;
            let size: u64 = size
                .parse()
                .map_err(|_| format!("bad INDEX line {line:?}"))?;
            files.push((relpath, size, size));
        } else if let Some(rest) = line.strip_prefix("t ") {
            let (size, rest) = rest
                .split_once(' ')
                .ok_or_else(|| format!("bad INDEX line {line:?}"))?;
            let (stored, relpath) = rest
                .split_once(' ')
                .ok_or_else(|| format!("bad INDEX line {line:?}"))?;
            let (size, stored) = (size.parse::<u64>(), stored.parse::<u64>());
            let (Ok(size), Ok(stored)) = (size, stored) else {
                return Err(format!("bad INDEX line {line:?}"));
            };
            files.push((relpath, size, stored));
        } else if let Some(relpath) = line.strip_prefix("d ") {
            std::fs::create_dir_all(pgdata.join(relpath))
                .map_err(|e| format!("mkdir {relpath}: {e}"))?;
            dirs += 1;
        } else if !line.is_empty() {
            return Err(format!("bad INDEX line {line:?}"));
        }
    }
    // The parents first and on one thread. They are local calls, most
    // of them are the same handful of directories, and a restore that
    // made them inside the fetch would have every thread making the
    // same one.
    for (relpath, _, _) in &files {
        if let Some(parent) = pgdata.join(relpath).parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
        }
    }
    // Then the objects, all at once. A skeleton is a few tens of files
    // and each one is a round trip, so a serial restore against a store
    // thirty milliseconds away spends a second on a database that is
    // otherwise empty. Nothing here depends on anything else here:
    // distinct keys, distinct paths, and the INDEX is already read.
    let failed = std::sync::Mutex::new(None::<String>);
    let note = |e: String| {
        let mut failed = failed.lock().expect("the first error");
        failed.get_or_insert(e);
        false
    };
    crate::pending::for_each_parallel(&files, |(relpath, size, stored)| {
        let data = match store.get(&layout.chk_file(chk_id, relpath)) {
            Ok(Some((data, _))) => data,
            Ok(None) => return note(format!("checkpoint object for {relpath} is missing")),
            Err(e) => return note(format!("store: {e}")),
        };
        if data.len() as u64 != *stored {
            return note(format!(
                "{relpath} is {} bytes in the store, INDEX says {stored}",
                data.len()
            ));
        }
        let path = pgdata.join(relpath);
        // A file whose object is shorter had its trailing zeros dropped
        // by the capture, so the file is made at its real length and the
        // tail comes back as the hole a fresh file already is. See
        // capture::without_trailing_zeros.
        if let Err(e) = write_at_length(&path, &data, *size) {
            return note(format!("write {relpath}: {e}"));
        }
        match set_mode(&path, 0o600) {
            Ok(()) => true,
            Err(e) => note(e),
        }
    });
    if let Some(e) = failed.into_inner().expect("the first error") {
        return Err(e);
    }
    Ok((files.len(), dirs))
}

/// Write `data` into a fresh file that ends up `size` bytes long. Equal
/// lengths is an ordinary write; a shorter `data` leaves the rest of the
/// file as a hole, which reads back as zeros and on a filesystem that
/// keeps holes costs no disk either.
fn write_at_length(path: &Path, data: &[u8], size: u64) -> Result<(), String> {
    let mut file = std::fs::File::create(path).map_err(|e| e.to_string())?;
    file.write_all(data).map_err(|e| e.to_string())?;
    if data.len() as u64 != size {
        file.set_len(size).map_err(|e| e.to_string())?;
    }
    Ok(())
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

/// XLP_LONG_HEADER: this page holds the long form header that opens
/// every WAL segment.
const XLP_LONG_HEADER: u16 = 0x0002;

/// Write the long page header xlog puts at a segment's start, unless
/// bytes already sit there. Recovery validates a segment's first page
/// before reading any record in it, even one far past it, so a segment
/// file created fresh for bytes laid down mid segment needs the header
/// synthesized: the zeros it holds at offset 0 fail the magic check and
/// recovery gives up. Every field is known here, the system identifier
/// comes from the restored pg_control and the rest is layout.
fn ensure_segment_header(pg_wal: &Path, tli: u32, sysid: u64, seg_lsn: u64) -> Result<(), String> {
    let path = pg_wal.join(wal_file_name(tli, seg_lsn));
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut magic = [0u8; 2];
    std::io::Read::read_exact(&mut file, &mut magic)
        .map_err(|e| format!("read {}: {e}", path.display()))?;
    if magic != [0, 0] {
        return Ok(());
    }
    let mut header = Vec::with_capacity(40);
    header.extend_from_slice(&crate::walscan::XLOG_PAGE_MAGIC.to_le_bytes());
    header.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes());
    header.extend_from_slice(&tli.to_le_bytes());
    header.extend_from_slice(&seg_lsn.to_le_bytes());
    // xlp_rem_len and the alignment padding before xlp_sysid.
    header.extend_from_slice(&0u64.to_le_bytes());
    header.extend_from_slice(&sysid.to_le_bytes());
    header.extend_from_slice(&(WAL_SEGMENT_SIZE as u32).to_le_bytes());
    header.extend_from_slice(&(WAL_PAGE_SIZE as u32).to_le_bytes());
    file.seek(SeekFrom::Start(0))
        .map_err(|e| format!("seek {}: {e}", path.display()))?;
    file.write_all(&header)
        .map_err(|e| format!("write {}: {e}", path.display()))?;
    Ok(())
}

/// Restore a tenant's data directory from the store into `pgdata`, which
/// must not already exist. Returns what was rebuilt; a plain server start
/// on the result completes the attach.
pub fn restore(store_root: &str, tenant: &str, pgdata: &Path) -> Result<RestoreStats, String> {
    // The two halves are timed apart because they fail differently: the
    // skeleton is a fixed number of objects a project's size does not
    // change, and the catch up grows with how much stream sits past the
    // newest checkpoint. A caller printing an attach breakdown wants to
    // know which of the two it is looking at.
    let started = Instant::now();
    let store: Arc<dyn CasStore> = Arc::from(open_store(store_root)?);
    let layout = TenantLayout::new(tenant);

    let (data, _) = store
        .get(&layout.manifest())
        .map_err(|e| format!("store: {e}"))?
        .ok_or_else(|| format!("{store_root} has no manifest, nothing to restore"))?;
    let manifest = Manifest::from_json(&data).map_err(|e| format!("manifest: {e}"))?;
    let mut stats = restore_manifest(&*store, &layout, &manifest, pgdata)?;
    stats.skeleton_took = started.elapsed();
    let caught_up = Instant::now();

    // WAL past the newest checkpoint comes straight from the shared log,
    // the same read the barrier and the folder do. The floor is the
    // checkpoint's redo location and usually sits mid WAL page, but
    // recovery reads whole pages and the head of that page, its page
    // header included, lives in frames that end at or before the floor.
    // Catching up from the page boundary pulls those frames back in;
    // anything a checkpoint capture already laid down is simply
    // overwritten with the same bytes. Streamed frame by frame into
    // pg_wal after the manifest restore laid the WAL tail, so the live
    // bytes land on top and a long history never sits whole in memory.
    let floor = manifest
        .checkpoints
        .last()
        .map_or(0, |checkpoint| checkpoint.lsn.0);
    let media = WalMedia::single(crate::log_store(Arc::clone(&store), &layout));
    let tli = manifest.pg.timeline;
    let pg_wal = pgdata.join("pg_wal");
    catch_up_with::<String, _>(
        &media,
        WAL_SHARD,
        &TeeFilter::Tenant(tenant_id(tenant)),
        Lsn(floor & !(WAL_PAGE_SIZE - 1)),
        |frame| {
            let lsn = frame.start_lsn.0;
            overlay_wal_chunk(&pg_wal, tli, lsn, &frame.payload)?;
            stats.wal_records += 1;
            stats.wal_bytes += frame.payload.len() as u64;
            stats.wal_end = stats.wal_end.max(lsn + frame.payload.len() as u64);
            Ok(())
        },
    )
    .map_err(|e| format!("wal catch up: {e}"))?;
    stats.wal_took = caught_up.elapsed();
    Ok(stats)
}

/// Restore the newest history snapshot of `tenant` at or before
/// `unix_ts` into `pgdata`. This is time travel as a read only attach:
/// nothing in the store changes, and the result stops at the snapshot's
/// newest checkpoint. The shared log kept moving after the snapshot was
/// published, so replaying it here would pull the restore past the
/// chosen moment; point in time granularity is the fold cadence.
pub fn restore_at(
    store_root: &str,
    tenant: &str,
    unix_ts: u64,
    pgdata: &Path,
) -> Result<RestoreStats, String> {
    let store: Arc<dyn CasStore> = Arc::from(open_store(store_root)?);
    let layout = TenantLayout::new(tenant);
    let snapshot = zou_store::snapshot_at(&*store, tenant, unix_ts).map_err(|e| e.to_string())?;
    restore_manifest(&*store, &layout, &snapshot, pgdata)
}

/// Materialize one manifest, live head or history snapshot, into a fresh
/// `pgdata`, WAL tail included. Frames past the newest checkpoint are
/// the caller's to overlay on top, [`restore`] streams them from the
/// shared log.
fn restore_manifest(
    store: &dyn CasStore,
    layout: &TenantLayout,
    manifest: &Manifest,
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

    let mut wal_end = chain.last().expect("chain is nonempty").lsn.0;
    let tli = manifest.pg.timeline;
    let pg_wal = pgdata.join("pg_wal");

    // The newest checkpoint's WAL tail: the bytes from its redo page
    // boundary through the end of the checkpoint record pg_control
    // names. A restore with a live stream gets the same bytes and more
    // from the frames below, but a branch child and a time travel
    // restore have no stream, and without the tail the server holds a
    // pg_control naming a record that exists nowhere. Laid down before
    // the frames so live bytes land on top. Absent on genesis, whose
    // capture carried the whole pg_wal, and on stores from before the
    // tail existed.
    let newest = chain.last().expect("chain is nonempty");
    let tail_layout = crate::fold::chk_layout(layout, newest);
    if let Some((tail, _)) = store
        .get(&tail_layout.chk_waltail(&newest.id))
        .map_err(|e| format!("store: {e}"))?
    {
        if tail.len() < 8 {
            return Err(format!("WALTAIL of {} is malformed", newest.id));
        }
        let start = u64::from_le_bytes(tail[..8].try_into().expect("checked length"));
        overlay_wal_chunk(&pg_wal, tli, start, &tail[8..])?;
        let end = start + (tail.len() - 8) as u64;
        // Any segment the tail touched without covering its first page
        // came into being zero filled here and needs the long header
        // recovery validates before it reads the checkpoint record.
        let sysid = u64::from_le_bytes(control[..8].try_into().expect("control is 8192 bytes"));
        let mut seg = start / WAL_SEGMENT_SIZE;
        while seg * WAL_SEGMENT_SIZE < end {
            ensure_segment_header(&pg_wal, tli, sysid, seg * WAL_SEGMENT_SIZE)?;
            seg += 1;
        }
        wal_end = wal_end.max(end);
    }

    Ok(RestoreStats {
        files,
        dirs,
        wal_records: 0,
        wal_bytes: 0,
        wal_end,
        timeline: tli,
        ..RestoreStats::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testv2::V2Wal;
    use zou_store::LocalFsStore;
    use zou_store::manifest::CheckpointRef;

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

    /// A pg_control whose tail is laid out the way a real one is:
    /// data_checksum_version, the char signedness bool, the 32 byte
    /// nonce, the padding that aligns the crc, then the crc. Checked
    /// against a pg18 cluster on server3, whose crc sits at 292 with
    /// the checksum version at 252 and pg_controldata agreeing it is
    /// on.
    fn control_with_checksums(version: u32) -> Vec<u8> {
        let mut control = vec![0u8; 8192];
        control[0..8].copy_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());
        control[16..20].copy_from_slice(&DB_IN_PRODUCTION.to_le_bytes());
        let at = 252;
        control[at..at + 4].copy_from_slice(&version.to_le_bytes());
        control[at + 4] = 1;
        for (i, b) in control[at + 5..at + 37].iter_mut().enumerate() {
            *b = (i * 7 + 13) as u8;
        }
        let crc_at = at + CONTROL_CHECKSUM_BACK;
        let crc = crc32c::crc32c(&control[..crc_at]);
        control[crc_at..crc_at + 4].copy_from_slice(&crc.to_le_bytes());
        control
    }

    #[test]
    fn the_checksum_setting_is_read_back_from_where_the_crc_puts_it() {
        assert!(control_data_checksums(&control_with_checksums(1)).unwrap());
        assert!(!control_data_checksums(&control_with_checksums(0)).unwrap());
        // The distance is the fields between: the uint32 itself, the
        // char signedness bool, the nonce, and the padding that aligns
        // the crc back to four.
        assert_eq!(CONTROL_CHECKSUM_BACK, 4 + 1 + MOCK_AUTH_NONCE_LEN + 3);
    }

    /// A torn or truncated pg_control has no crc position, and the
    /// reader has to say so rather than answer no. Answering no is the
    /// one wrong answer that silently produces pages a checksummed
    /// cluster refuses to start on.
    #[test]
    fn a_control_nobody_can_verify_is_an_error_not_a_no() {
        let mut control = control_with_checksums(1);
        control[100] ^= 0xFF;
        assert!(control_data_checksums(&control).is_err());
        assert!(control_data_checksums(&[0u8; 8192]).is_err());
        assert!(control_data_checksums(&[]).is_err());
    }

    /// The setting is fixed at initdb, so the answer is the same in
    /// every capture that has a pg_control. A delta only holds what
    /// changed, so the walk has to keep going back until it finds one.
    #[test]
    fn the_store_answers_the_checksum_question_off_the_newest_capture_that_can() {
        let store_dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(store_dir.path());
        let layout = TenantLayout::new("local");
        store
            .put_if_absent(
                &layout.chk_file("genesis", "global/pg_control"),
                &control_with_checksums(1),
            )
            .unwrap();
        let mut manifest = Manifest::new("local", 18);
        manifest.checkpoints.push(CheckpointRef {
            id: "genesis".into(),
            lsn: Lsn(0x0100_0028),
            kind: CheckpointKind::Full,
            owner: None,
        });
        // A delta that carried no pg_control, which is the ordinary
        // case and the one that would answer wrong if the walk stopped
        // at the newest capture instead of the newest usable one.
        manifest.checkpoints.push(CheckpointRef {
            id: "d1".into(),
            lsn: Lsn(0x0100_1000),
            kind: CheckpointKind::Delta,
            owner: None,
        });
        store
            .put_if_absent(&layout.manifest(), &manifest.to_json())
            .unwrap();
        assert!(store_data_checksums(&store, "local").unwrap());
    }

    /// A store with nothing to read the setting out of is a refusal.
    /// Answering no would be a fold that quietly writes pages the
    /// cluster will not start on.
    #[test]
    fn a_store_that_cannot_say_is_a_refusal() {
        let store_dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(store_dir.path());
        let layout = TenantLayout::new("local");
        assert!(store_data_checksums(&store, "local").is_err());
        store
            .put_if_absent(&layout.manifest(), &Manifest::new("local", 18).to_json())
            .unwrap();
        assert!(store_data_checksums(&store, "local").is_err());
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

        // A pusher session appends two frames to the shared log, the
        // second crossing into the next 16MB segment to exercise the
        // split on overlay.
        let mut wal2 = V2Wal::open(
            Arc::new(LocalFsStore::new(store_dir.path())) as Arc<dyn CasStore>,
            "local",
            1,
        );
        wal2.push(0x0100_0028, &vec![0x22u8; 4096]);
        wal2.push(2 * WAL_SEGMENT_SIZE - 100, &vec![0x33u8; 300]);
        wal2.close();

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
    fn a_capture_that_dropped_trailing_zeros_restores_the_whole_file() {
        // What a capture writes and what the file measures are two
        // numbers, and the restore owes the file the second one. The
        // sixteen megabyte wal segment a fresh cluster has barely
        // written into is the case that pays for this.
        let store_dir = tempfile::tempdir().unwrap();
        let out_dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(store_dir.path());
        let layout = TenantLayout::new("local");

        let mut segment = vec![0x11u8; 300];
        store
            .put_if_absent(
                &layout.chk_file("genesis", "pg_wal/000000010000000000000001"),
                &segment,
            )
            .unwrap();
        store
            .put_if_absent(&layout.chk_file("genesis", "empty file"), b"")
            .unwrap();
        let index = format!(
            "t {WAL_SEGMENT_SIZE} 300 pg_wal/000000010000000000000001\nt 4096 0 empty file\n"
        );
        store
            .put_if_absent(&layout.chk_index("genesis"), index.as_bytes())
            .unwrap();

        let pgdata = out_dir.path().join("restored");
        std::fs::create_dir_all(&pgdata).unwrap();
        let (files, _) = restore_fs(&store, &layout, "genesis", &pgdata).unwrap();
        assert_eq!(files, 2);

        segment.resize(WAL_SEGMENT_SIZE as usize, 0);
        let restored = std::fs::read(pgdata.join("pg_wal/000000010000000000000001")).unwrap();
        assert_eq!(restored, segment, "the zeros came back");
        // A path with a space in it, which is why the lengths lead.
        assert_eq!(std::fs::read(pgdata.join("empty file")).unwrap(), [0; 4096]);
    }

    #[test]
    fn a_capture_shorter_than_its_own_index_is_refused() {
        let store_dir = tempfile::tempdir().unwrap();
        let out_dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(store_dir.path());
        let layout = TenantLayout::new("local");
        store
            .put_if_absent(&layout.chk_file("genesis", "PG_VERSION"), b"18\n")
            .unwrap();
        store
            .put_if_absent(&layout.chk_index("genesis"), b"t 9 8 PG_VERSION\n")
            .unwrap();

        let pgdata = out_dir.path().join("restored");
        std::fs::create_dir_all(&pgdata).unwrap();
        let err = restore_fs(&store, &layout, "genesis", &pgdata).unwrap_err();
        assert!(err.contains("INDEX says 8"), "{err}");
    }

    #[test]
    fn a_restore_with_no_stream_lays_the_checkpoints_wal_tail_down() {
        // The branch child shape: every checkpoint inherited from the
        // parent, no frames of its own to overlay. Without the WALTAIL
        // the restored pg_control names a checkpoint record no segment
        // holds and recovery panics; with it the record's bytes land in
        // a segment the restore creates.
        let store_dir = tempfile::tempdir().unwrap();
        let out_dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(store_dir.path());
        let parent = TenantLayout::new("parent");
        let child = TenantLayout::new("child");

        let control = synthetic_control(DB_IN_PRODUCTION);
        store
            .put_if_absent(&parent.chk_file("genesis", "global/pg_control"), &control)
            .unwrap();
        store
            .put_if_absent(&parent.chk_file("genesis", "PG_VERSION"), b"18\n")
            .unwrap();
        let index = format!(
            "f PG_VERSION 3\nf global/pg_control {}\nd pg_wal/archive_status\n",
            control.len()
        );
        store
            .put_if_absent(&parent.chk_index("genesis"), index.as_bytes())
            .unwrap();

        // The delta the branch point sits on, its tail starting mid
        // segment at the redo page floor with 0x200 marker bytes through
        // the record. Mid segment is the shape that matters: the segment
        // file comes into being here and its first page needs the long
        // header recovery validates before reading the record.
        store.put_if_absent(&parent.chk_index("d1"), b"").unwrap();
        let tail_start = WAL_SEGMENT_SIZE + 0x2000;
        let mut tail = Vec::new();
        tail.extend_from_slice(&tail_start.to_le_bytes());
        tail.extend_from_slice(&[0x77u8; 0x200]);
        store
            .put_if_absent(&parent.chk_waltail("d1"), &tail)
            .unwrap();

        let mut manifest = Manifest::new("child", 18);
        manifest.checkpoints.push(CheckpointRef {
            id: "genesis".into(),
            lsn: Lsn(0x0100_0028),
            kind: CheckpointKind::Full,
            owner: Some("parent".into()),
        });
        manifest.checkpoints.push(CheckpointRef {
            id: "d1".into(),
            lsn: Lsn(tail_start + 0x80),
            kind: CheckpointKind::Delta,
            owner: Some("parent".into()),
        });

        let pgdata = out_dir.path().join("restored");
        let stats = restore_manifest(&store, &child, &manifest, &pgdata).unwrap();
        assert_eq!(stats.wal_records, 0, "no live frames overlaid");
        assert_eq!(stats.wal_end, tail_start + 0x200);

        // The tail landed at its lsn in a segment created for it, the
        // rest zero filled except the synthesized segment header.
        let seg = std::fs::read(pgdata.join("pg_wal/000000010000000000000001")).unwrap();
        assert_eq!(seg.len() as u64, WAL_SEGMENT_SIZE);
        assert!(seg[0x2000..0x2200].iter().all(|b| *b == 0x77));
        assert!(seg[0x2200..].iter().all(|b| *b == 0));
        assert!(seg[40..0x2000].iter().all(|b| *b == 0));

        // The long header at the segment's start, field by field: magic,
        // the long header flag, the timeline, the page address, rem_len
        // zero, then the system identifier out of pg_control and the
        // segment and page sizes.
        assert_eq!(&seg[0..2], &crate::walscan::XLOG_PAGE_MAGIC.to_le_bytes());
        assert_eq!(&seg[2..4], &XLP_LONG_HEADER.to_le_bytes());
        assert_eq!(&seg[4..8], &1u32.to_le_bytes());
        assert_eq!(&seg[8..16], &WAL_SEGMENT_SIZE.to_le_bytes());
        assert_eq!(&seg[16..24], &0u64.to_le_bytes());
        assert_eq!(&seg[24..32], &control[..8]);
        assert_eq!(&seg[32..36], &(WAL_SEGMENT_SIZE as u32).to_le_bytes());
        assert_eq!(&seg[36..40], &(WAL_PAGE_SIZE as u32).to_le_bytes());
    }

    #[test]
    fn the_overlay_covers_the_wal_page_under_the_newest_checkpoint() {
        // The newest checkpoint's redo usually sits mid WAL page, and
        // recovery reads that page whole: its header and the bytes up
        // to the redo point came from frames that end at or before the
        // floor. Cutting the catch up exactly at the floor drops them
        // and leaves the page head zeroed, which is an unreadable
        // checkpoint record and a cluster that will not start.
        let store_dir = tempfile::tempdir().unwrap();
        let out_dir = tempfile::tempdir().unwrap();
        let store_root = store_dir.path().to_str().unwrap();
        let store = LocalFsStore::new(store_dir.path());
        let layout = TenantLayout::new("local");

        let control = synthetic_control(DB_IN_PRODUCTION);
        store
            .put_if_absent(&layout.chk_file("genesis", "global/pg_control"), &control)
            .unwrap();
        let index = format!("f global/pg_control {}\nd pg_wal\n", control.len());
        store
            .put_if_absent(&layout.chk_index("genesis"), index.as_bytes())
            .unwrap();
        let floor = 0x0100_24F0u64;
        let mut manifest = Manifest::new("local", 18);
        manifest.checkpoints.push(CheckpointRef {
            id: "genesis".into(),
            lsn: Lsn(floor),
            kind: CheckpointKind::Full,
            owner: None,
        });
        store
            .put_if_absent(&layout.manifest(), &manifest.to_json())
            .unwrap();

        // One frame fills the page head and ends exactly at the floor,
        // the next starts there. Both must land in the overlay.
        let mut wal2 = V2Wal::open(
            Arc::new(LocalFsStore::new(store_dir.path())) as Arc<dyn CasStore>,
            "local",
            1,
        );
        wal2.push(0x0100_1F00, &vec![0xAAu8; 0x5F0]);
        wal2.push(floor, &vec![0xBBu8; 0x100]);
        wal2.close();

        let pgdata = out_dir.path().join("restored");
        let stats = restore(store_root, "local", &pgdata).unwrap();
        assert_eq!(stats.wal_records, 2, "the page head frame is kept");
        assert_eq!(stats.wal_bytes, 0x5F0 + 0x100);

        let seg = std::fs::read(pgdata.join("pg_wal/000000010000000000000001")).unwrap();
        let page = 0x0100_2000usize % WAL_SEGMENT_SIZE as usize;
        assert!(
            seg[page..page + 0x4F0].iter().all(|b| *b == 0xAA),
            "the page head under the floor is real WAL, not zeros"
        );
        assert!(
            seg[page + 0x4F0..page + 0x5F0].iter().all(|b| *b == 0xBB),
            "the stream past the floor follows"
        );
    }

    #[test]
    fn restore_at_stops_at_the_snapshot_checkpoint_not_the_live_head() {
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

        // The snapshot is published first, then WAL keeps landing in
        // the shared log, so the live head has moved past it.
        let snapshot = manifest.clone();
        store
            .put_if_absent(&layout.manifest_history(1, 1000), &snapshot.to_json())
            .unwrap();
        let mut wal2 = V2Wal::open(
            Arc::new(LocalFsStore::new(store_dir.path())) as Arc<dyn CasStore>,
            "local",
            1,
        );
        wal2.push(0x0100_0028, &vec![0x22u8; 4096]);
        wal2.push(0x0100_5000, &[0x33u8; 100]);
        wal2.close();

        // Time travel to the snapshot: the tree stops at the snapshot's
        // newest checkpoint, nothing from the stream lands. That is the
        // granularity this release offers, branch points and time
        // travel both sit on the checkpoint grid.
        let at_dir = out_dir.path().join("at");
        let stats = restore_at(store_root, "local", 1500, &at_dir).unwrap();
        assert_eq!(stats.wal_records, 0);
        assert_eq!(stats.wal_bytes, 0);
        let seg1 = std::fs::read(at_dir.join("pg_wal/000000010000000000000001")).unwrap();
        let off = (0x0100_0028 % WAL_SEGMENT_SIZE) as usize;
        assert_eq!(seg1[off], 0x11, "the stream never lands in time travel");

        // The live restore of the same store replays both frames.
        let live_dir = out_dir.path().join("live");
        let stats = restore(store_root, "local", &live_dir).unwrap();
        assert_eq!(stats.wal_records, 2);
        assert_eq!(stats.wal_bytes, 4096 + 100);
        let seg1 = std::fs::read(live_dir.join("pg_wal/000000010000000000000001")).unwrap();
        assert!(seg1[off..off + 4096].iter().all(|b| *b == 0x22));
        let later = (0x0100_5000 % WAL_SEGMENT_SIZE) as usize;
        assert_eq!(seg1[later], 0x33);

        // Before the earliest snapshot there is nothing to travel to.
        let err = restore_at(store_root, "local", 500, &out_dir.path().join("gone")).unwrap_err();
        assert!(err.contains("no history"), "{err}");
    }

    #[test]
    fn a_branch_restores_parent_files_and_inherits_no_tail() {
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

        // The parent appends WAL after the checkpoint, then the branch
        // is taken. The cut is the checkpoint lsn, so that stream stays
        // the parent's and the child inherits files only.
        let mut wal2 = V2Wal::open(Arc::clone(&store) as Arc<dyn CasStore>, "local", 1);
        wal2.push(0x0100_0028, &vec![0x22u8; 4096]);
        wal2.close();

        zou_store::branch(&*store, "local", "b1", None, 5000).unwrap();

        let pgdata = out_dir.path().join("child");
        let stats = restore(store_root, "b1", &pgdata).unwrap();
        assert_eq!(stats.files, 3, "the tree comes from the parent capture");
        assert_eq!(stats.wal_records, 0, "no tail crosses a branch point");
        assert_eq!(stats.wal_bytes, 0);

        assert_eq!(std::fs::read(pgdata.join("PG_VERSION")).unwrap(), b"18\n");
        let seg1 = std::fs::read(pgdata.join("pg_wal/000000010000000000000001")).unwrap();
        let off = (0x0100_0028 % WAL_SEGMENT_SIZE) as usize;
        assert_eq!(
            seg1[off], 0x11,
            "the parent stream never lands in the child"
        );
    }
}
