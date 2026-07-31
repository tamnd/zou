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

/// The read amplification bound: a page read walks at most one full,
/// this many deltas, and the WAL tail. A fold that would chain a fifth
/// delta captures a full instead, whatever the byte ratio says, so the
/// chain walk and a restore never touch more than five indexes.
const MAX_DELTA_CHAIN: usize = 4;

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

/// The layout holding a checkpoint's objects: an inherited ref names
/// its owner, our own live under the tenant's own prefix.
pub(crate) fn chk_layout(own: &TenantLayout, c: &CheckpointRef) -> TenantLayout {
    match &c.owner {
        Some(owner) => TenantLayout::new(owner),
        None => own.clone(),
    }
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
/// chain since the newest full is already `MAX_DELTA_CHAIN` deltas
/// long, or when those deltas have grown past the factor times its
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
    if manifest.checkpoints.len() - full > MAX_DELTA_CHAIN {
        return Ok(true);
    }
    let base = &manifest.checkpoints[full];
    let full_bytes = index_bytes(store, &chk_layout(layout, base), &base.id)?;
    let mut delta_bytes = 0u64;
    for c in &manifest.checkpoints[full + 1..] {
        delta_bytes += index_bytes(store, &chk_layout(layout, c), &c.id)?;
    }
    Ok(delta_bytes > fold_down_factor().saturating_mul(full_bytes))
}

/// First Postgres LSN covered by a stored segment, from the 8 byte
/// header of its first record. Stream order is push order is pg order,
/// so this bounds everything in earlier segments from above.
pub(crate) fn segment_first_pg_lsn(
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
    // A branched tenant's window begins in the frozen parent tails: its
    // first fold covers WAL from the inherited checkpoint through the
    // branch point into its own stream, and the blocks those parent
    // records dirtied must land in this delta, the fold raises the read
    // floor past them for good. Parents go first so the tenant's own
    // bytes win where the streams overlap.
    let mut sources: Vec<(TenantLayout, Vec<String>)> = manifest
        .parent_tail
        .iter()
        .map(|pt| (TenantLayout::new(&pt.tenant_ref), pt.segments.clone()))
        .collect();
    sources.push((
        layout.clone(),
        scan_segments(store, layout, manifest, epoch)?,
    ));
    let window = walscan::assemble_window_from(store, &sources, prev, redo)?;
    // Coverage can start after the previous checkpoint when that one
    // predates the stream, the genesis capture does. Records in the gap
    // are older than the stream's first push, so their page effects are
    // already in the base capture.
    let start = prev.max(window.covered_from);
    walscan::scan_range(&window, start, redo)
}

/// The init fork number, INIT_FORKNUM. A relation with one is unlogged.
const INIT_FORK: u32 = 3;

/// A fork length destined for an s line: spc, db, rel, fork, nblocks.
type ForkSize = (u32, u32, u32, u32, u32);

/// A page image source for the pack: the live pg/ prefix on an ordinary
/// fold, the merged chain view on a branched full.
type FetchPage<'a> = dyn FnMut(&BlockRef) -> Result<Option<Vec<u8>>, String> + 'a;

/// Every page and fork size the store holds, from a listing of the pg/
/// prefix. Sizes ride along in the PAGES index so a reader of a full
/// checkpoint has fork lengths without another source. Relations with
/// an init fork are unlogged, their main fork writes never reach the
/// WAL, so the read path's freshness barrier cannot see them go stale;
/// their pages stay out of the runs and always serve from pg/.
fn all_pages(
    store: &dyn CasStore,
    layout: &TenantLayout,
) -> Result<(Vec<BlockRef>, Vec<ForkSize>), String> {
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

/// The fork sizes a delta records as s lines: every fork the window's
/// refs touch plus the forks of relations its truncate and create
/// events name, read from the tenant's own SIZE objects. A fork with
/// no own SIZE is skipped, on a branch that means the size never
/// changed under this prefix and the older chain answer stays current,
/// on the owner it means the relation was dropped after the window and
/// the retained WAL carries the drop. Without these lines a branch
/// child would answer nblocks from the last full and never see rows in
/// blocks a later delta packed.
fn touched_sizes(
    store: &dyn CasStore,
    layout: &TenantLayout,
    out: &walscan::ScanOut,
) -> Result<Vec<ForkSize>, String> {
    let mut forks: std::collections::BTreeSet<(u32, u32, u32, u32)> = out
        .refs
        .iter()
        .map(|r| (r.spc, r.db, r.rel, r.fork))
        .collect();
    for t in out.rels.iter().chain(out.truncs.iter().map(|(t, _)| t)) {
        for fork in 0..4 {
            forks.insert((t.spc, t.db, t.rel, fork));
        }
    }
    let mut sizes = Vec::new();
    for (spc, db, rel, fork) in forks {
        let Some((data, _)) = store
            .get(&layout.pg_size(spc, db, rel, fork))
            .map_err(|e| format!("store: {e}"))?
        else {
            continue;
        };
        let n = <[u8; 4]>::try_from(data.as_slice())
            .map(u32::from_le_bytes)
            .map_err(|_| format!("bad SIZE object for {spc}/{db}/{rel}/{fork}"))?;
        sizes.push((spc, db, rel, fork, n));
    }
    Ok(sizes)
}

/// Pack the pages into sorted run objects plus the PAGES index, each
/// image produced by `fetch`, which reads the live pg/ prefix for an
/// ordinary fold and the merged chain view for a branched full. Blocks
/// the WAL names but `fetch` no longer finds belonged to dropped
/// relations and are skipped, the WAL that drops them is retained.
/// Relation events land after the p lines, file recreations as r lines
/// that stop the read path's chain walk outright and truncates as t
/// lines carrying the main fork cutoff below which older copies still
/// serve, then the fork sizes as s lines.
/// Idempotent: an existing PAGES index means an earlier attempt of this
/// checkpoint finished, and run objects it left behind are kept.
#[allow(clippy::too_many_arguments)]
fn pack_page_runs(
    store: &dyn CasStore,
    layout: &TenantLayout,
    id: &str,
    refs: &[BlockRef],
    rels: &[walscan::RelTag],
    truncs: &[(walscan::RelTag, u32)],
    sizes: &[ForkSize],
    fetch: &mut FetchPage,
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
        let Some(data) = fetch(r)? else {
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
    for (t, cut) in truncs {
        index.push_str(&format!("t {} {} {} {}\n", t.spc, t.db, t.rel, cut));
    }
    for (spc, db, rel, fork, n) in sizes {
        index.push_str(&format!("s {spc} {db} {rel} {fork} {n}\n"));
    }
    match store.put_new(&layout.checkpoint_page_index(id), index.as_bytes()) {
        Ok(_) | Err(CasError::AlreadyExists { .. }) => Ok((pages, runs as usize)),
        Err(e) => Err(format!("put PAGES: {e}")),
    }
}

/// Whether the manifest still leans on state it does not own: a branch
/// origin, a frozen parent tail, or checkpoints tagged with another
/// owner. Once a merged full publishes and prunes those, the tenant
/// packs like any other.
fn branched(m: &Manifest) -> bool {
    m.branch_of.is_some()
        || !m.parent_tail.is_empty()
        || m.checkpoints.iter().any(|c| c.owner.is_some())
}

/// The full capture for a branched manifest. Packing from pg/ alone
/// would be a disaster here: the prefix holds only the tenant's own
/// divergent writes, the inherited blocks live in run objects under
/// their owners, and a full that omits them prunes the chain that
/// serves them. So the pack takes the union. The own pg/ block wins
/// where both exist, the write gate flushed every changed block there,
/// and the chain image serves the rest, with recreation and truncate
/// events masking dead refs exactly the way the read path masks them.
/// Effective fork sizes follow the same rule, own SIZE first and the
/// chain walk otherwise, and chain refs at or past the effective size
/// are truncation garbage the pack drops. A relation dropped since the
/// last fold still rides along under its old size, its WAL drop is in
/// the retained tail and nothing reads a relation the catalog no
/// longer names. After this full publishes, the chain prunes to it,
/// the branch is self sufficient, and gc can unpin the parent.
fn pack_merged_full(
    store: &dyn CasStore,
    layout: &TenantLayout,
    id: &str,
    manifest: &Manifest,
) -> Result<(usize, usize), String> {
    use crate::reader::ChkIndex;
    use crate::walscan::RelTag;
    use std::collections::{BTreeMap, BTreeSet};

    let (own_refs, own_sizes) = all_pages(store, layout)?;
    let full = manifest
        .checkpoints
        .iter()
        .rposition(|c| c.kind == CheckpointKind::Full)
        .ok_or_else(|| "a branched manifest has no full to merge".to_string())?;
    let mut chain: Vec<ChkIndex> = Vec::new();
    for c in manifest.checkpoints[full..].iter().rev() {
        let lay = chk_layout(layout, c);
        let (data, _) = store
            .get(&lay.checkpoint_page_index(&c.id))
            .map_err(|e| format!("store: {e}"))?
            .ok_or_else(|| format!("checkpoint {} has no PAGES to merge from", c.id))?;
        let text =
            String::from_utf8(data).map_err(|_| format!("PAGES for {} is not utf8", c.id))?;
        chain.push(ChkIndex::parse(&c.id, lay, &text)?);
    }

    // Walk newest first with the read path's masking: an r line kills
    // older refs of the relation, a t line kills main fork refs at or
    // past the smallest cutoff seen and every other fork, and an
    // index's own entries are never masked by its own events, a
    // recreation packs its new blocks beside the r line that kills the
    // old incarnation. Sizes answer newest first too, an s line before
    // the same index's events, and a t line with a real cutoff is
    // itself the main fork answer, the record named the surviving
    // count.
    let mut sizes: BTreeMap<(u32, u32, u32, u32), u32> = own_sizes
        .iter()
        .map(|&(a, b, c, d, n)| ((a, b, c, d), n))
        .collect();
    let mut seen: BTreeSet<BlockRef> = own_refs.iter().copied().collect();
    let mut masked: BTreeSet<RelTag> = BTreeSet::new();
    let mut cuts: BTreeMap<RelTag, u32> = BTreeMap::new();
    let mut picked: Vec<(BlockRef, usize)> = Vec::new();
    for (i, chk) in chain.iter().enumerate() {
        for r in &chk.entries {
            let tag = RelTag {
                spc: r.spc,
                db: r.db,
                rel: r.rel,
            };
            if masked.contains(&tag)
                || cuts
                    .get(&tag)
                    .is_some_and(|cut| r.fork != 0 || r.blk >= *cut)
                || !seen.insert(*r)
            {
                continue;
            }
            picked.push((*r, i));
        }
        for (key, n) in &chk.sizes {
            let tag = RelTag {
                spc: key.0,
                db: key.1,
                rel: key.2,
            };
            if masked.contains(&tag) || (key.3 != 0 && cuts.contains_key(&tag)) {
                continue;
            }
            sizes.entry(*key).or_insert(*n);
        }
        for (tag, cut) in &chk.truncs {
            if *cut != u32::MAX && !masked.contains(tag) {
                sizes.entry((tag.spc, tag.db, tag.rel, 0)).or_insert(*cut);
            }
            let e = cuts.entry(*tag).or_insert(*cut);
            *e = (*e).min(*cut);
        }
        masked.extend(chk.rels.iter().copied());
    }
    picked.retain(|(r, _)| {
        sizes
            .get(&(r.spc, r.db, r.rel, r.fork))
            .is_none_or(|n| r.blk < *n)
    });

    let mut refs = own_refs;
    refs.extend(picked.iter().map(|(r, _)| *r));
    refs.sort();
    let src: BTreeMap<BlockRef, usize> = picked.into_iter().collect();
    // Whole run objects cache in a small window: the refs are sorted
    // in (relation, block) order, so pages of one region cluster into
    // the same run and a handful of slots absorbs the interleaving
    // between chain indexes.
    let mut cache: BTreeMap<(usize, u32), Vec<u8>> = BTreeMap::new();
    let mut fetch = |r: &BlockRef| -> Result<Option<Vec<u8>>, String> {
        let Some(&i) = src.get(r) else {
            return store
                .get(&layout.pg_block(r.spc, r.db, r.rel, r.fork, r.blk))
                .map(|o| o.map(|(d, _)| d))
                .map_err(|e| format!("store: {e}"));
        };
        let chk = &chain[i];
        let (run, off) = chk
            .lookup(r)
            .ok_or_else(|| format!("merged ref {r:?} vanished from {}", chk.id))?;
        if !cache.contains_key(&(i, run)) {
            if cache.len() >= 4 {
                let oldest = *cache.keys().next().expect("cache is nonempty");
                cache.remove(&oldest);
            }
            let key = chk.layout.checkpoint_pages(&chk.id, run);
            let (bytes, _) = store
                .get(&key)
                .map_err(|e| format!("store: {e}"))?
                .ok_or_else(|| format!("run object {key} is missing"))?;
            cache.insert((i, run), bytes);
        }
        let bytes = &cache[&(i, run)];
        let a = off as usize;
        if a + ZOU_PAGE_SIZE > bytes.len() {
            return Err(format!(
                "run object for {} is shorter than its index",
                chk.id
            ));
        }
        Ok(Some(bytes[a..a + ZOU_PAGE_SIZE].to_vec()))
    };
    let sizes: Vec<(u32, u32, u32, u32, u32)> = sizes
        .into_iter()
        .map(|((a, b, c, d), n)| (a, b, c, d, n))
        .collect();
    pack_page_runs(store, layout, id, &refs, &[], &[], &sizes, &mut fetch)
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
    // holds, and a full on a branched manifest merges the chain in so
    // the inherited refs it prunes stay served. A failure here leaves
    // fs and run objects behind for gc, the manifest still names
    // nothing.
    let mut own_fetch = |r: &BlockRef| -> Result<Option<Vec<u8>>, String> {
        store
            .get(&layout.pg_block(r.spc, r.db, r.rel, r.fork, r.blk))
            .map(|o| o.map(|(d, _)| d))
            .map_err(|e| format!("store: {e}"))
    };
    let (pages, runs) = match kind {
        CheckpointKind::Delta => {
            let out = delta_scan(store, layout, &manifest, commit.epoch(), redo)?;
            let sizes = touched_sizes(store, layout, &out)?;
            pack_page_runs(
                store,
                layout,
                &id,
                &out.refs,
                &out.rels,
                &out.truncs,
                &sizes,
                &mut own_fetch,
            )?
        }
        CheckpointKind::Full if branched(&manifest) => {
            pack_merged_full(store, layout, &id, &manifest)?
        }
        CheckpointKind::Full => {
            let (refs, sizes) = all_pages(store, layout)?;
            pack_page_runs(store, layout, &id, &refs, &[], &[], &sizes, &mut own_fetch)?
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
                owner: None,
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
        // An smgr truncate of relation 30000 down to three blocks,
        // which must surface as a t line so the read path stops
        // trusting older copies past the cutoff.
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
        // not, a dropped relation the pack must skip. The SIZE objects
        // of the touched relation and the truncated one must ride into
        // the index as s lines, a branch child answers nblocks from
        // them long after this WAL is gone.
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
        store
            .put(&layout.pg_size(1663, 5, 16384, 0), &6u32.to_le_bytes())
            .unwrap();
        store
            .put(&layout.pg_size(1663, 5, 30000, 0), &3u32.to_le_bytes())
            .unwrap();

        // A genesis full big enough that the deltas never trigger the
        // fold down, this test is about the delta path. Its lsn is the
        // stream base, so the scan window is exactly the pushed WAL.
        let mut genesis = Manifest::new("local", 18);
        genesis.checkpoints.push(CheckpointRef {
            id: "genesis".into(),
            lsn: Lsn(stream_base),
            kind: CheckpointKind::Full,
            owner: None,
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
            "runs 1024\np 1663 5 16384 0 0\np 1663 5 16384 0 1\nt 1663 5 30000 3\ns 1663 5 16384 0 6\ns 1663 5 30000 0 3\n"
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
            owner: None,
        });
        m.checkpoints.push(CheckpointRef {
            id: "d1".into(),
            lsn: Lsn(0x200),
            kind: CheckpointKind::Delta,
            owner: None,
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
        assert_eq!(
            m2.checkpoints.len(),
            1,
            "the full pruned the superseded chain from the manifest"
        );
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

    #[test]
    fn a_branched_full_merges_the_inherited_chain_and_lets_the_parent_go() {
        let store_dir = tempfile::tempdir().unwrap();
        let pgdata_dir = tempfile::tempdir().unwrap();
        let pgdata = pgdata_dir.path();
        let store = Arc::new(LocalFsStore::new(store_dir.path()));
        let parent = TenantLayout::new("parent");
        let child = TenantLayout::new("child");

        // The parent chain a child inherits: a full and four deltas,
        // five refs, so the child's first fold trips the delta cap and
        // captures a full. The images and events cover every masking
        // rule the merge honors: a newer delta image shadowing the
        // full's, a truncate cutoff killing the block past it, and a
        // recreation killing the older incarnation outright.
        let page = |b: u8| vec![b; ZOU_PAGE_SIZE];
        let put_chk = |id: &str, index: &str, run: &[Vec<u8>]| {
            store
                .put_new(&parent.checkpoint_page_index(id), index.as_bytes())
                .unwrap();
            if !run.is_empty() {
                let bytes: Vec<u8> = run.iter().flatten().copied().collect();
                store
                    .put_new(&parent.checkpoint_pages(id, 0), &bytes)
                    .unwrap();
            }
        };
        put_chk(
            "f1",
            "runs 1024\np 1663 5 16384 0 0\np 1663 5 16384 0 1\np 1663 5 16384 0 2\np 1663 5 20000 0 0\ns 1663 5 16384 0 3\ns 1663 5 20000 0 1\n",
            &[page(0x11), page(0x22), page(0x44), page(0x55)],
        );
        put_chk(
            "d1",
            "runs 1024\np 1663 5 16384 0 1\ns 1663 5 16384 0 3\n",
            &[page(0x33)],
        );
        put_chk("d2", "runs 1024\nt 1663 5 16384 2\n", &[]);
        put_chk(
            "d3",
            "runs 1024\np 1663 5 20000 0 0\nr 1663 5 20000\ns 1663 5 20000 0 1\n",
            &[page(0x66)],
        );
        put_chk("d4", "runs 1024\n", &[]);

        let mut pm = Manifest::new("parent", 18);
        let kinds = [
            ("f1", CheckpointKind::Full),
            ("d1", CheckpointKind::Delta),
            ("d2", CheckpointKind::Delta),
            ("d3", CheckpointKind::Delta),
            ("d4", CheckpointKind::Delta),
        ];
        for (i, (id, kind)) in kinds.into_iter().enumerate() {
            pm.checkpoints.push(CheckpointRef {
                id: id.into(),
                lsn: Lsn(0x100 * (i as u64 + 1)),
                kind,
                owner: None,
            });
        }
        pm.wal_tail = Some(zou_store::manifest::WalTail {
            epoch_dir: 1,
            from_lsn: Lsn(0x500),
            segments: vec!["0000000000000001/0000000000000000.wal".into()],
        });
        store.put_new(&parent.manifest(), &pm.to_json()).unwrap();

        zou_store::branch(&*store, "parent", "child", None, 5000).unwrap();

        // The child diverged on one block, its own image of block 0
        // must win over the inherited copy, and its own SIZE beats the
        // chain's answers.
        store
            .put(&child.pg_block(1663, 5, 16384, 0, 0), &page(0x77))
            .unwrap();
        store
            .put(&child.pg_size(1663, 5, 16384, 0), &2u32.to_le_bytes())
            .unwrap();

        let redo = WAL_SEGMENT_SIZE + 0x80;
        for d in ["global", "pg_xact", "pg_wal", "pg_twophase"] {
            std::fs::create_dir_all(pgdata.join(d)).unwrap();
        }
        std::fs::write(pgdata.join("global/pg_control"), synthetic_control(redo)).unwrap();
        std::fs::write(pgdata.join("PG_VERSION"), b"18\n").unwrap();
        std::fs::write(pgdata.join("pg_wal/000000010000000000000001"), b"wal").unwrap();

        let held = lease::acquire(&*store, &child, "test", 15, 1000).unwrap();
        let gc = GroupCommit::with_lease(
            Arc::clone(&store) as Arc<dyn CasStore>,
            child.clone(),
            Arc::new(Mutex::new(held)),
            Lsn(0),
            GroupCommitConfig::default(),
            TailConfig::default(),
        );
        let mut record = 0x100u64.to_le_bytes().to_vec();
        record.extend_from_slice(b"payload");
        gc.append(&record).unwrap().wait().unwrap();

        let stats = fold(&*store, &child, &gc, pgdata, redo).unwrap();
        assert_eq!(stats.kind, CheckpointKind::Full);
        assert_eq!(stats.pages, 3);
        assert_eq!(stats.runs, 1);

        // The union: the child's own block 0, the newest inherited
        // block 1, the recreated 20000. Block 2 died to the truncate
        // cutoff, the old 20000 incarnation to the r line, and the
        // sizes answer own first, chain otherwise.
        let (pages_index, _) = store
            .get(&child.checkpoint_page_index(&stats.id))
            .unwrap()
            .unwrap();
        assert_eq!(
            String::from_utf8(pages_index).unwrap(),
            "runs 1024\np 1663 5 16384 0 0\np 1663 5 16384 0 1\np 1663 5 20000 0 0\ns 1663 5 16384 0 2\ns 1663 5 20000 0 1\n"
        );
        let (run, _) = store
            .get(&child.checkpoint_pages(&stats.id, 0))
            .unwrap()
            .unwrap();
        assert_eq!(run.len(), 3 * ZOU_PAGE_SIZE);
        assert_eq!(run[0], 0x77, "the child's own image wins");
        assert_eq!(run[ZOU_PAGE_SIZE], 0x33, "the newest chain image serves");
        assert_eq!(run[2 * ZOU_PAGE_SIZE], 0x66, "the recreated incarnation");

        // Published, the chain prunes to the new full, the frozen
        // parent tail lets go, and nothing in the manifest names the
        // parent's objects anymore: the branch is self sufficient and
        // gc may unpin the parent.
        let m = manifest_of(&*store, &child);
        assert_eq!(m.checkpoints.len(), 1);
        assert_eq!(m.checkpoints[0].kind, CheckpointKind::Full);
        assert_eq!(m.checkpoints[0].owner, None);
        assert!(m.parent_tail.is_empty());

        gc.close().unwrap();
    }

    #[test]
    fn a_fifth_delta_folds_down_to_a_full_whatever_the_bytes_say() {
        let store_dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(store_dir.path());
        let layout = TenantLayout::new("local");

        // A huge full and tiny deltas, so the byte ratio alone would
        // let the chain grow forever. The count cap must not.
        let mut m = Manifest::new("local", 18);
        m.checkpoints.push(CheckpointRef {
            id: "genesis".into(),
            lsn: Lsn(0x100),
            kind: CheckpointKind::Full,
            owner: None,
        });
        store
            .put_new(&layout.chk_index("genesis"), b"f base/huge 1000000\n")
            .unwrap();
        for i in 1..=4u64 {
            let id = format!("d{i}");
            store
                .put_new(&layout.chk_index(&id), b"f pg_xact/0000 1\n")
                .unwrap();
            m.checkpoints.push(CheckpointRef {
                id,
                lsn: Lsn(0x100 + i * 0x100),
                kind: CheckpointKind::Delta,
                owner: None,
            });
            let want_full = chain_wants_full(&store, &layout, &m).unwrap();
            assert_eq!(
                want_full,
                i == 4,
                "the chain holds {i} deltas, only the fourth fills it"
            );
        }
    }
}
