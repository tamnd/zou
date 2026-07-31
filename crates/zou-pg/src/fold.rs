//! The checkpoint fold: turn the WAL a completed Postgres checkpoint made
//! redundant into a delta checkpoint object and truncate the mirrored
//! tail.
//!
//! After a checkpoint completes, every page change before its redo
//! location is on the page store, so WAL before redo is only needed to
//! carry the non relation state forward: transaction status, the control
//! file, relation maps. The fold captures exactly that state as a delta
//! checkpoint at the redo LSN, then drops the sealed stream segments that
//! lie entirely before it. Restore applies the newest full capture, the
//! deltas after it, and replays the remaining tail.
//!
//! The truncation cut is the 16MB pg_wal segment boundary below redo, not
//! redo itself: the xlog reader validates the first page header of any
//! segment file it opens, so the overlay must rebuild retained segment
//! files from their start.
//!
//! The caller, the wal pusher, only folds while fully caught up, pushed
//! equal to the local flush pointer, so the checkpoint record named by
//! the captured pg_control is already durable in the store. Transaction
//! status captured here can run slightly ahead of the record stream for
//! commits that happened in the capture window; those commits were never
//! acked, and the docs carry the caveat.
//!
//! The fold down policy keeps the chain short: once the deltas since
//! the newest full outweigh it by a factor, the next fold captures a
//! full instead, restore starts there and the superseded chain becomes
//! garbage for the gc job.

use std::path::Path;

use zou_store::layout::TenantLayout;
use zou_store::manifest::{CheckpointKind, CheckpointRef};
use zou_store::{CasStore, GroupCommit, Lsn, Manifest, SegmentReader};

use crate::capture;
use crate::restore::{WAL_SEGMENT_SIZE, control_redo};

/// A new full checkpoint replaces the delta chain once the deltas
/// outweigh the newest full by this factor. Restore cost stays bounded
/// by the full size, and the superseded chain becomes garbage for the
/// gc job. `ZOU_FOLD_DOWN_FACTOR` overrides it, which tests use to
/// force a fold down without writing five fulls worth of deltas.
const FOLD_DOWN_FACTOR: u64 = 5;

fn fold_down_factor() -> u64 {
    std::env::var("ZOU_FOLD_DOWN_FACTOR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(FOLD_DOWN_FACTOR)
}

#[derive(Debug)]
pub struct FoldStats {
    pub id: String,
    pub kind: CheckpointKind,
    pub files: usize,
    pub bytes: u64,
    pub dropped: usize,
}

/// Total file bytes a checkpoint describes, summed from its INDEX.
fn index_bytes(store: &dyn CasStore, layout: &TenantLayout, id: &str) -> Result<u64, String> {
    let (data, _) = store
        .get(&layout.chk_index(id))
        .map_err(|e| format!("store: {e}"))?
        .ok_or_else(|| format!("INDEX for checkpoint {id} is missing"))?;
    let text = String::from_utf8(data).map_err(|_| format!("INDEX for {id} is not utf8"))?;
    let mut total = 0u64;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("f ") {
            let len = rest
                .rsplit(' ')
                .next()
                .and_then(|v| v.parse::<u64>().ok())
                .ok_or_else(|| format!("bad INDEX line {line:?} in {id}"))?;
            total += len;
        }
    }
    Ok(total)
}

/// The fold down policy: capture a full instead of a delta when the
/// deltas since the newest full have grown past the factor times its
/// size. A manifest with no full at all also gets one, nothing to
/// chain a delta onto.
fn chain_wants_full(
    store: &dyn CasStore,
    layout: &TenantLayout,
    manifest: &Manifest,
) -> Result<bool, String> {
    let Some(full) = manifest
        .checkpoints
        .iter()
        .rposition(|c| c.kind == CheckpointKind::Full)
    else {
        return Ok(true);
    };
    let full_bytes = index_bytes(store, layout, &manifest.checkpoints[full].id)?;
    let mut delta_bytes = 0u64;
    for c in &manifest.checkpoints[full + 1..] {
        delta_bytes += index_bytes(store, layout, &c.id)?;
    }
    Ok(delta_bytes > fold_down_factor().saturating_mul(full_bytes))
}

/// First Postgres LSN covered by a stored segment, from the 8 byte
/// header of its first record. Stream order is push order is pg order,
/// so this bounds everything in earlier segments from above.
fn segment_first_pg_lsn(
    store: &dyn CasStore,
    layout: &TenantLayout,
    name: &str,
) -> Result<u64, String> {
    let epoch = zou_store::commit::segment_epoch(name)
        .ok_or_else(|| format!("bad segment name {name:?}"))?;
    let (bytes, _) = store
        .get(&layout.wal_segment_path(name))
        .map_err(|e| format!("store: {e}"))?
        .ok_or_else(|| format!("segment {name} is missing"))?;
    let frame = SegmentReader::new(&bytes, epoch)
        .next()
        .ok_or_else(|| format!("segment {name} is empty"))?
        .map_err(|e| format!("segment {name}: {e}"))?;
    let records = zou_store::commit::split_records(&frame.payload)
        .ok_or_else(|| format!("bad batch in {name}"))?;
    let first = records
        .first()
        .ok_or_else(|| format!("empty batch in {name}"))?;
    if first.len() < 8 {
        return Err(format!("short record in {name}"));
    }
    Ok(u64::from_le_bytes(
        first[..8].try_into().expect("checked length"),
    ))
}

/// Capture the checkpoint at `redo`, a delta normally or a full when
/// the fold down policy says the chain has outgrown its base, and
/// publish it together with the tail truncation. Idempotent per redo:
/// the checkpoint id is derived from it and every step tolerates a
/// retried run. Errors leave the manifest unchanged, the caller retries.
pub fn fold(
    store: &dyn CasStore,
    layout: &TenantLayout,
    commit: &GroupCommit,
    pgdata: &Path,
    redo: u64,
) -> Result<FoldStats, String> {
    let (data, _) = store
        .get(&layout.manifest())
        .map_err(|e| format!("store: {e}"))?
        .ok_or_else(|| "manifest vanished".to_string())?;
    let manifest = Manifest::from_json(&data).map_err(|e| format!("manifest: {e}"))?;
    let kind = if chain_wants_full(store, layout, &manifest)? {
        CheckpointKind::Full
    } else {
        CheckpointKind::Delta
    };
    let paths = match kind {
        CheckpointKind::Full => capture::full_capture(pgdata)?,
        CheckpointKind::Delta => capture::delta_capture(pgdata)?,
    };
    let files = capture::read_files(&paths)?;
    let control = files
        .iter()
        .find(|(rel, _)| rel == "global/pg_control")
        .map(|(_, data)| data)
        .ok_or_else(|| "capture found no pg_control".to_string())?;
    // A torn concurrent read fails the crc and errors here. A mismatched
    // redo means another checkpoint completed since the caller looked;
    // fold that one instead on the next round, this capture would name a
    // checkpoint record the store may not hold yet.
    let captured_redo = control_redo(control)?;
    if captured_redo != redo {
        return Err(format!(
            "pg_control redo {captured_redo:#X} is past the fold at {redo:#X}, retrying later"
        ));
    }

    let id = format!("{redo:016x}");
    let mut bytes = 0u64;
    if store
        .get(&layout.chk_index(&id))
        .map_err(|e| format!("store: {e}"))?
        .is_none()
    {
        bytes = capture::upload(store, layout, &id, &files, &paths.dirs, true)?;
    }

    // Droppable prefix of the published tail. A sealed segment goes once
    // its successor starts at or below the cut, everything it covers is
    // then before the retained window. Unpublished segments are newer
    // than the checkpoint and never candidates.
    let cut = redo & !(WAL_SEGMENT_SIZE - 1);
    let mut drop = Vec::new();
    if let Some(tail) = &manifest.wal_tail {
        for pair in tail.segments.windows(2) {
            if segment_first_pg_lsn(store, layout, &pair[1])? <= cut {
                drop.push(pair[0].clone());
            } else {
                break;
            }
        }
    }
    let dropped = drop.len();
    commit
        .fold_tail(
            CheckpointRef {
                id: id.clone(),
                lsn: Lsn(redo),
                kind,
            },
            drop,
        )
        .map_err(|e| format!("fold publish: {e}"))?;
    Ok(FoldStats {
        id,
        kind,
        files: files.len(),
        bytes,
        dropped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use zou_store::{GroupCommitConfig, LocalFsStore, TailConfig, lease};

    fn synthetic_control(redo: u64) -> Vec<u8> {
        // Same shape the restore tests use: state in production at 16,
        // the redo at 40, junk, the crc over everything before it at 300.
        let mut control = vec![0u8; 8192];
        control[0..8].copy_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());
        control[16..20].copy_from_slice(&6u32.to_le_bytes());
        for (i, b) in control[20..300].iter_mut().enumerate() {
            *b = (i * 7 + 13) as u8;
        }
        control[40..48].copy_from_slice(&redo.to_le_bytes());
        let crc = crc32c::crc32c(&control[..300]);
        control[300..304].copy_from_slice(&crc.to_le_bytes());
        control
    }

    fn manifest_of(store: &dyn CasStore, layout: &TenantLayout) -> Manifest {
        let (data, _) = store.get(&layout.manifest()).unwrap().unwrap();
        Manifest::from_json(&data).unwrap()
    }

    #[test]
    fn a_fold_captures_a_delta_and_truncates_at_the_segment_boundary() {
        let store_dir = tempfile::tempdir().unwrap();
        let pgdata_dir = tempfile::tempdir().unwrap();
        let pgdata = pgdata_dir.path();
        let store = Arc::new(LocalFsStore::new(store_dir.path()));
        let layout = TenantLayout::new("local");

        let redo = 2 * WAL_SEGMENT_SIZE + 0x50;
        std::fs::create_dir_all(pgdata.join("global")).unwrap();
        std::fs::create_dir_all(pgdata.join("pg_xact")).unwrap();
        let control = synthetic_control(redo);
        std::fs::write(pgdata.join("global/pg_control"), &control).unwrap();
        std::fs::write(pgdata.join("pg_xact/0000"), b"clog").unwrap();

        // A genesis full big enough that the deltas never trigger the
        // fold down, this test is about the delta path.
        let mut genesis = Manifest::new("local", 18);
        genesis.checkpoints.push(CheckpointRef {
            id: "genesis".into(),
            lsn: Lsn(0x100),
            kind: CheckpointKind::Full,
        });
        store
            .put_new(&layout.chk_index("genesis"), b"f base/big 1000000\n")
            .unwrap();
        store
            .put_new(&layout.manifest(), &genesis.to_json())
            .unwrap();
        let held = lease::acquire(&*store, &layout, "test", 15, 1000).unwrap();
        let gc = GroupCommit::with_lease(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            Arc::new(Mutex::new(held)),
            Lsn(0),
            GroupCommitConfig::default(),
            TailConfig {
                seal_bytes: 1,
                ..TailConfig::default()
            },
        );
        // Three sealed segments: the first two lie entirely below the cut
        // at the 16MB boundary under redo, the third starts above it.
        let push = |pg_lsn: u64, len: usize| {
            let mut record = pg_lsn.to_le_bytes().to_vec();
            record.extend(std::iter::repeat_n(0x5A, len));
            gc.append(&record).unwrap().wait().unwrap();
        };
        push(0x100, 100);
        push(0x200, 100);
        push(2 * WAL_SEGMENT_SIZE + 10, 100);
        let before = manifest_of(&*store, &layout).wal_tail.unwrap();
        assert_eq!(before.segments.len(), 3);

        let stats = fold(&*store, &layout, &gc, pgdata, redo).unwrap();
        assert_eq!(
            stats.dropped, 1,
            "only the first segment's successor starts below the cut"
        );
        assert_eq!(stats.kind, CheckpointKind::Delta);
        assert_eq!(stats.files, 2);

        let m = manifest_of(&*store, &layout);
        let tail = m.wal_tail.unwrap();
        assert_eq!(tail.segments, before.segments[1..].to_vec());
        assert_eq!(m.checkpoints.len(), 2);
        let chk = &m.checkpoints[1];
        assert_eq!(chk.lsn, Lsn(redo));
        assert_eq!(chk.kind, CheckpointKind::Delta);
        assert_eq!(chk.id, format!("{redo:016x}"));

        // The capture round trips byte for byte.
        let (stored, _) = store
            .get(&layout.chk_file(&chk.id, "global/pg_control"))
            .unwrap()
            .unwrap();
        assert_eq!(stored, control);
        let (index, _) = store.get(&layout.chk_index(&chk.id)).unwrap().unwrap();
        let index = String::from_utf8(index).unwrap();
        assert!(index.contains("f pg_xact/0000 4"));

        // Folding the same redo again is a no op on the manifest.
        let again = fold(&*store, &layout, &gc, pgdata, redo).unwrap();
        assert_eq!(again.dropped, 0);
        assert_eq!(manifest_of(&*store, &layout).checkpoints.len(), 2);

        // A capture whose pg_control moved past the fold is refused.
        let newer = synthetic_control(redo + 0x1000);
        std::fs::write(pgdata.join("global/pg_control"), &newer).unwrap();
        let err = fold(&*store, &layout, &gc, pgdata, redo).unwrap_err();
        assert!(err.contains("past the fold"));

        gc.close().unwrap();
    }

    #[test]
    fn the_fold_down_policy_promotes_a_full_once_the_deltas_outgrow_it() {
        let store_dir = tempfile::tempdir().unwrap();
        let pgdata_dir = tempfile::tempdir().unwrap();
        let pgdata = pgdata_dir.path();
        let store = Arc::new(LocalFsStore::new(store_dir.path()));
        let layout = TenantLayout::new("local");

        let redo = WAL_SEGMENT_SIZE + 0x80;
        for d in ["global", "pg_xact", "pg_wal", "pg_twophase"] {
            std::fs::create_dir_all(pgdata.join(d)).unwrap();
        }
        std::fs::write(pgdata.join("global/pg_control"), synthetic_control(redo)).unwrap();
        std::fs::write(pgdata.join("PG_VERSION"), b"18\n").unwrap();
        std::fs::write(pgdata.join("pg_xact/0000"), b"clog").unwrap();
        std::fs::write(pgdata.join("pg_wal/000000010000000000000001"), b"wal").unwrap();
        std::fs::write(pgdata.join("postmaster.pid"), b"123").unwrap();

        // The delta chain weighs 600 bytes against a 100 byte full, past
        // the factor of five, so the next fold must capture a full.
        let mut m = Manifest::new("local", 18);
        m.checkpoints.push(CheckpointRef {
            id: "genesis".into(),
            lsn: Lsn(0x100),
            kind: CheckpointKind::Full,
        });
        m.checkpoints.push(CheckpointRef {
            id: "d1".into(),
            lsn: Lsn(0x200),
            kind: CheckpointKind::Delta,
        });
        store
            .put_new(&layout.chk_index("genesis"), b"f base/small 100\n")
            .unwrap();
        store
            .put_new(&layout.chk_index("d1"), b"f pg_xact/0000 600\n")
            .unwrap();
        store.put_new(&layout.manifest(), &m.to_json()).unwrap();

        let held = lease::acquire(&*store, &layout, "test", 15, 1000).unwrap();
        let gc = GroupCommit::with_lease(
            Arc::clone(&store) as Arc<dyn CasStore>,
            layout.clone(),
            Arc::new(Mutex::new(held)),
            Lsn(0),
            GroupCommitConfig::default(),
            TailConfig::default(),
        );
        let mut record = 0x100u64.to_le_bytes().to_vec();
        record.extend_from_slice(b"payload");
        gc.append(&record).unwrap().wait().unwrap();

        let stats = fold(&*store, &layout, &gc, pgdata, redo).unwrap();
        assert_eq!(stats.kind, CheckpointKind::Full);

        let m2 = manifest_of(&*store, &layout);
        let last = m2.checkpoints.last().unwrap();
        assert_eq!(last.kind, CheckpointKind::Full);
        assert_eq!(last.id, format!("{redo:016x}"));

        // The full walk keeps the skeleton, drops wal segments and the
        // per instance noise, and resets the policy for the next fold.
        let (index, _) = store.get(&layout.chk_index(&stats.id)).unwrap().unwrap();
        let index = String::from_utf8(index).unwrap();
        assert!(index.contains("f PG_VERSION"));
        assert!(index.contains("f pg_xact/0000"));
        assert!(!index.contains("000000010000000000000001"));
        assert!(!index.contains("postmaster.pid"));
        assert!(index.contains("d pg_wal"));
        assert!(!chain_wants_full(&*store, &layout, &m2).unwrap());

        gc.close().unwrap();
    }
}
