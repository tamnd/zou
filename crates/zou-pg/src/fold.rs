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
//! Each checkpoint also carries sorted page runs: the blocks its WAL
//! window dirtied, packed in (relation, block) order into immutable run
//! objects with a PAGES index, which is what the read path range reads
//! instead of one object per block. The pages are read from the live
//! pg/ prefix at fold time, so a run can hold a block image slightly
//! newer than redo; that is the same replay-idempotence argument
//! Postgres recovery itself rests on, a checkpoint is a consistent
//! starting point, not a point in time snapshot.
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
use zou_store::{CasError, CasStore, GroupCommit, Lsn, Manifest, SegmentReader};

use crate::ZOU_PAGE_SIZE;
use crate::capture;
use crate::restore::{WAL_SEGMENT_SIZE, control_redo};
use crate::walscan::{self, BlockRef};

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
    pub pages: usize,
    pub runs: usize,
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

/// Pages per run object, 8MB runs in v0. The spec targets bigger runs
/// once the read path range reads them, the index records the value so
/// a reader never has to guess.
const RUN_PAGES: usize = 1024;

/// The stream segments worth scanning: the published tail plus this
/// session's own segments uploaded after the last publish, which the
/// manifest has not learned about yet. Unpublished objects from any
/// other epoch stay untrusted, a zombie writer may still be uploading.
fn scan_segments(
    store: &dyn CasStore,
    layout: &TenantLayout,
    manifest: &Manifest,
    epoch: u64,
) -> Result<Vec<String>, String> {
    let mut segments: Vec<String> = manifest
        .wal_tail
        .as_ref()
        .map(|t| t.segments.clone())
        .unwrap_or_default();
    for key in store
        .list(&layout.wal_epoch_dir(epoch))
        .map_err(|e| format!("store: {e}"))?
    {
        let name = key.rsplit('/').next().unwrap_or_default();
        let qualified = format!("{epoch:016}/{name}");
        if !segments.contains(&qualified) {
            segments.push(qualified);
        }
    }
    Ok(segments)
}

/// The blocks dirtied since the previous checkpoint, scanned out of the
/// WAL the stream holds over that window, plus the relations its smgr
/// create and truncate records name. Completeness rests on the write
/// gate: no page object mutates before its WAL is durable in the
/// stream, so the stream names every page the fold must carry. The
/// relation events ride into the PAGES index as r lines because a
/// truncate or a file recreation makes older checkpoint copies of the
/// relation stale without any block reference saying so, and the read
/// path needs that barrier long after this WAL is dropped.
fn delta_scan(
    store: &dyn CasStore,
    layout: &TenantLayout,
    manifest: &Manifest,
    epoch: u64,
    redo: u64,
) -> Result<walscan::ScanOut, String> {
    let prev = manifest
        .checkpoints
        .last()
        .map(|c| c.lsn.0)
        .ok_or_else(|| "delta fold with no prior checkpoint".to_string())?;
    let segments = scan_segments(store, layout, manifest, epoch)?;
    let window = walscan::assemble_window(store, layout, &segments, prev, redo)?;
    // Coverage can start after the previous checkpoint when that one
    // predates the stream, the genesis capture does. Records in the gap
    // are older than the stream's first push, so their page effects are
    // already in the base capture.
    let start = prev.max(window.covered_from);
    walscan::scan_range(&window, start, redo)
}

/// The init fork number, INIT_FORKNUM. A relation with one is unlogged.
const INIT_FORK: u32 = 3;

/// Every page and fork size the store holds, from a listing of the pg/
/// prefix. Sizes ride along in the PAGES index so a reader of a full
/// checkpoint has fork lengths without another source. Relations with
/// an init fork are unlogged, their main fork writes never reach the
/// WAL, so the read path's freshness barrier cannot see them go stale;
/// their pages stay out of the runs and always serve from pg/.
#[allow(clippy::type_complexity)]
fn all_pages(
    store: &dyn CasStore,
    layout: &TenantLayout,
) -> Result<(Vec<BlockRef>, Vec<(u32, u32, u32, u32, u32)>), String> {
    let prefix = layout.pg_dir();
    let mut refs = Vec::new();
    let mut sizes = Vec::new();
    let mut unlogged = std::collections::BTreeSet::new();
    for key in store.list(&prefix).map_err(|e| format!("store: {e}"))? {
        let rest = key.strip_prefix(&prefix).unwrap_or(&key);
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() != 5 {
            continue;
        }
        let (Ok(spc), Ok(db), Ok(rel), Ok(fork)) = (
            parts[0].parse(),
            parts[1].parse(),
            parts[2].parse(),
            parts[3].parse(),
        ) else {
            continue;
        };
        if fork == INIT_FORK {
            unlogged.insert((spc, db, rel));
        }
        if parts[4] == "SIZE" {
            let Some((data, _)) = store.get(&key).map_err(|e| format!("store: {e}"))? else {
                continue;
            };
            let n = <[u8; 4]>::try_from(data.as_slice())
                .map(u32::from_le_bytes)
                .map_err(|_| format!("bad SIZE object at {key}"))?;
            sizes.push((spc, db, rel, fork, n));
        } else if let Ok(blk) = u32::from_str_radix(parts[4], 16) {
            refs.push(BlockRef {
                spc,
                db,
                rel,
                fork,
                blk,
            });
        }
    }
    refs.retain(|r| !unlogged.contains(&(r.spc, r.db, r.rel)));
    refs.sort();
    Ok((refs, sizes))
}

/// Pack the pages into sorted run objects plus the PAGES index. Blocks
/// the WAL names but the store no longer holds belonged to dropped
/// relations and are skipped, the WAL that drops them is retained.
/// Relation events land as r lines after the p lines, the read path
/// stops its chain walk for a relation at the first index naming it.
/// Idempotent: an existing PAGES index means an earlier attempt of this
/// checkpoint finished, and run objects it left behind are kept.
fn pack_page_runs(
    store: &dyn CasStore,
    layout: &TenantLayout,
    id: &str,
    refs: &[BlockRef],
    rels: &[walscan::RelTag],
    sizes: &[(u32, u32, u32, u32, u32)],
) -> Result<(usize, usize), String> {
    if store
        .get(&layout.checkpoint_page_index(id))
        .map_err(|e| format!("store: {e}"))?
        .is_some()
    {
        return Ok((0, 0));
    }
    let mut index = format!("runs {RUN_PAGES}\n");
    let mut run: Vec<u8> = Vec::new();
    let mut runs = 0u32;
    let mut pages = 0usize;
    let flush = |run: &mut Vec<u8>, runs: &mut u32| -> Result<(), String> {
        if run.is_empty() {
            return Ok(());
        }
        match store.put_new(&layout.checkpoint_pages(id, *runs), run) {
            Ok(_) | Err(CasError::AlreadyExists { .. }) => {}
            Err(e) => return Err(format!("put run {runs}: {e}")),
        }
        run.clear();
        *runs += 1;
        Ok(())
    };
    for r in refs {
        let Some((data, _)) = store
            .get(&layout.pg_block(r.spc, r.db, r.rel, r.fork, r.blk))
            .map_err(|e| format!("store: {e}"))?
        else {
            continue;
        };
        if data.len() != ZOU_PAGE_SIZE {
            return Err(format!("page object {r:?} holds {} bytes", data.len()));
        }
        run.extend_from_slice(&data);
        pages += 1;
        index.push_str(&format!(
            "p {} {} {} {} {}\n",
            r.spc, r.db, r.rel, r.fork, r.blk
        ));
        if run.len() >= RUN_PAGES * ZOU_PAGE_SIZE {
            flush(&mut run, &mut runs)?;
        }
    }
    flush(&mut run, &mut runs)?;
    for r in rels {
        index.push_str(&format!("r {} {} {}\n", r.spc, r.db, r.rel));
    }
    for (spc, db, rel, fork, n) in sizes {
        index.push_str(&format!("s {spc} {db} {rel} {fork} {n}\n"));
    }
    match store.put_new(&layout.checkpoint_page_index(id), index.as_bytes()) {
        Ok(_) | Err(CasError::AlreadyExists { .. }) => Ok((pages, runs as usize)),
        Err(e) => Err(format!("put PAGES: {e}")),
    }
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
        CheckpointKind::Full => capture::full_capture(pgdata, redo)?,
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

    // The sorted page runs: a delta packs the blocks the WAL dirtied
    // since the previous checkpoint, a full packs every page the store
    // holds. A failure here leaves fs and run objects behind for gc,
    // the manifest still names nothing.
    let (pages, runs) = match kind {
        CheckpointKind::Delta => {
            let out = delta_scan(store, layout, &manifest, commit.epoch(), redo)?;
            pack_page_runs(store, layout, &id, &out.refs, &out.rels, &[])?
        }
        CheckpointKind::Full => {
            let (refs, sizes) = all_pages(store, layout)?;
            pack_page_runs(store, layout, &id, &refs, &[], &sizes)?
        }
    };

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
        pages,
        runs,
        dropped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::walscan::testwal::Builder;
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

        // Synthetic WAL in segment 2, starting mid page like a real
        // resume point: two records before the fold dirtying blocks of
        // relation 16384, a SAME_REL reference to a block the store no
        // longer holds, and one record past the fold that must stay out
        // of the runs.
        let stream_base = 2 * WAL_SEGMENT_SIZE + 8;
        let block = |rel: u32, blk: u32| BlockRef {
            spc: 1663,
            db: 5,
            rel,
            fork: 0,
            blk,
        };
        let mut wal = Builder::new(stream_base);
        wal.record(&[(block(16384, 1), false)], b"first");
        wal.record(
            &[(block(16384, 0), false), (block(16384, 5), true)],
            b"second",
        );
        // An smgr truncate of relation 30000, which must surface as an
        // r line so the read path stops trusting older copies of it.
        let mut trunc = Vec::new();
        trunc.extend_from_slice(&3u32.to_le_bytes());
        trunc.extend_from_slice(&1663u32.to_le_bytes());
        trunc.extend_from_slice(&5u32.to_le_bytes());
        trunc.extend_from_slice(&30000u32.to_le_bytes());
        trunc.extend_from_slice(&7u32.to_le_bytes());
        wal.record_with(&[], &trunc, 0x20, 2);
        let redo = wal.pos();
        wal.record(&[(block(99999, 9), false)], b"after the fold");

        std::fs::create_dir_all(pgdata.join("global")).unwrap();
        std::fs::create_dir_all(pgdata.join("pg_xact")).unwrap();
        let control = synthetic_control(redo);
        std::fs::write(pgdata.join("global/pg_control"), &control).unwrap();
        std::fs::write(pgdata.join("pg_xact/0000"), b"clog").unwrap();

        // The two dirtied blocks exist in the page store, block 5 does
        // not, a dropped relation the pack must skip.
        store
            .put(
                &layout.pg_block(1663, 5, 16384, 0, 0),
                &[0xAA; ZOU_PAGE_SIZE],
            )
            .unwrap();
        store
            .put(
                &layout.pg_block(1663, 5, 16384, 0, 1),
                &[0xBB; ZOU_PAGE_SIZE],
            )
            .unwrap();

        // A genesis full big enough that the deltas never trigger the
        // fold down, this test is about the delta path. Its lsn is the
        // stream base, so the scan window is exactly the pushed WAL.
        let mut genesis = Manifest::new("local", 18);
        genesis.checkpoints.push(CheckpointRef {
            id: "genesis".into(),
            lsn: Lsn(stream_base),
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
        // Three sealed segments: two garbage chunks below the scan
        // window that exist to exercise the drop logic, then the real
        // WAL. Only the first lies entirely below the cut.
        let push = |pg_lsn: u64, bytes: &[u8]| {
            let mut record = pg_lsn.to_le_bytes().to_vec();
            record.extend_from_slice(bytes);
            gc.append(&record).unwrap().wait().unwrap();
        };
        push(0x100, &[0x5A; 100]);
        push(0x200, &[0x5A; 100]);
        let (stream_lsn, stream_bytes) = wal.stream();
        push(stream_lsn, stream_bytes);
        let before = manifest_of(&*store, &layout).wal_tail.unwrap();
        assert_eq!(before.segments.len(), 3);

        let stats = fold(&*store, &layout, &gc, pgdata, redo).unwrap();
        assert_eq!(
            stats.dropped, 1,
            "only the first segment's successor starts below the cut"
        );
        assert_eq!(stats.kind, CheckpointKind::Delta);
        assert_eq!(stats.files, 2);
        assert_eq!(
            stats.pages, 2,
            "block 5 is gone and the post fold record is out"
        );
        assert_eq!(stats.runs, 1);

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

        // The page runs: both present blocks packed in block order, the
        // PAGES index describing exactly them.
        let (pages_index, _) = store
            .get(&layout.checkpoint_page_index(&chk.id))
            .unwrap()
            .unwrap();
        assert_eq!(
            String::from_utf8(pages_index).unwrap(),
            "runs 1024\np 1663 5 16384 0 0\np 1663 5 16384 0 1\nr 1663 5 30000\n"
        );
        let (run, _) = store
            .get(&layout.checkpoint_pages(&chk.id, 0))
            .unwrap()
            .unwrap();
        assert_eq!(run.len(), 2 * ZOU_PAGE_SIZE);
        assert_eq!(run[0], 0xAA);
        assert_eq!(run[ZOU_PAGE_SIZE], 0xBB);
        assert!(
            store
                .get(&layout.checkpoint_pages(&chk.id, 1))
                .unwrap()
                .is_none()
        );

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
        std::fs::write(pgdata.join("pg_wal/000000010000000000000002"), b"wal2").unwrap();
        std::fs::write(pgdata.join("postmaster.pid"), b"123").unwrap();

        // A full packs every page the store holds plus the fork sizes,
        // no WAL scan involved.
        store
            .put(
                &layout.pg_block(1663, 5, 16384, 0, 0),
                &[0xCC; ZOU_PAGE_SIZE],
            )
            .unwrap();
        store
            .put(
                &layout.pg_block(1663, 5, 16384, 0, 1),
                &[0xDD; ZOU_PAGE_SIZE],
            )
            .unwrap();
        store
            .put(&layout.pg_size(1663, 5, 16384, 0), &2u32.to_le_bytes())
            .unwrap();
        // Relation 28000 has an init fork, so it is unlogged and its
        // pages must stay out of the runs, WAL never names its writes.
        store
            .put(
                &layout.pg_block(1663, 5, 28000, 0, 0),
                &[0xEE; ZOU_PAGE_SIZE],
            )
            .unwrap();
        store
            .put(
                &layout.pg_block(1663, 5, 28000, 3, 0),
                &[0xEF; ZOU_PAGE_SIZE],
            )
            .unwrap();

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
        assert_eq!(stats.pages, 2);
        assert_eq!(stats.runs, 1);

        let m2 = manifest_of(&*store, &layout);
        let last = m2.checkpoints.last().unwrap();
        assert_eq!(last.kind, CheckpointKind::Full);
        assert_eq!(last.id, format!("{redo:016x}"));

        // The full walk keeps the skeleton and the wal segment holding
        // redo, drops later wal segments and the per instance noise, and
        // resets the policy for the next fold. The redo segment stays
        // because the mirrored stream can begin mid segment and recovery
        // needs the segment file readable from its first page header.
        let (index, _) = store.get(&layout.chk_index(&stats.id)).unwrap().unwrap();
        let index = String::from_utf8(index).unwrap();
        assert!(index.contains("f PG_VERSION"));
        assert!(index.contains("f pg_xact/0000"));
        assert!(index.contains("f pg_wal/000000010000000000000001"));
        assert!(!index.contains("000000010000000000000002"));
        assert!(!index.contains("postmaster.pid"));
        assert!(!chain_wants_full(&*store, &layout, &m2).unwrap());

        let (pages_index, _) = store
            .get(&layout.checkpoint_page_index(&stats.id))
            .unwrap()
            .unwrap();
        assert_eq!(
            String::from_utf8(pages_index).unwrap(),
            "runs 1024\np 1663 5 16384 0 0\np 1663 5 16384 0 1\ns 1663 5 16384 0 2\n"
        );
        let (run, _) = store
            .get(&layout.checkpoint_pages(&stats.id, 0))
            .unwrap()
            .unwrap();
        assert_eq!(run[0], 0xCC);
        assert_eq!(run[ZOU_PAGE_SIZE], 0xDD);

        gc.close().unwrap();
    }
}
