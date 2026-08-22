//! The checkpoint fold: turn the WAL a completed Postgres checkpoint made
//! redundant into a delta checkpoint object and raise the tenant's
//! replay floor in the shared log.
//!
//! After a checkpoint completes, every page change before its redo
//! location is on the page store, so WAL before redo is only needed to
//! carry the non relation state forward: transaction status, the control
//! file, relation maps. The fold captures exactly that state as a delta
//! checkpoint at the redo LSN, then publishes `folded_upto` so readers
//! know frames at or below it never need replay. Restore applies the
//! newest full capture, the deltas after it, and replays the remaining
//! stream past the last checkpoint.
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
//! The caller, the wal pusher, only starts a fold once pushed covers
//! redo, so the checkpoint record named by the captured pg_control is
//! already durable in the store, and then keeps pushing while the fold
//! runs on its own thread. Concurrent pushes only widen the window in
//! which live pages run ahead of redo, which replay idempotence already
//! tolerates. Transaction status captured here can run slightly ahead
//! of the record stream for commits that happened in the capture
//! window; those commits were never acked, and the docs carry the
//! caveat.
//!
//! The fold down policy keeps the chain short: once the deltas since
//! the newest full outweigh it by a factor, the next fold captures a
//! full instead, restore starts there and the superseded chain becomes
//! garbage for the gc job.

use std::path::Path;

use zou_log::{TeeFilter, WalMedia, catch_up};
use zou_store::layout::TenantLayout;
use zou_store::lease::{self, HeldLease, LeaseError};
use zou_store::manifest::{CheckpointKind, CheckpointRef};
use zou_store::{CasError, CasStore, Lsn, Manifest};

use crate::capture;
use crate::restore::{WAL_PAGE_SIZE, control_checkpoint, control_redo};
use crate::walscan::{self, BlockRef};
use crate::{WAL_SHARD, ZOU_PAGE_SIZE};

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
    zou_store::setting::number_or(
        "ZOU_FOLD_DOWN_FACTOR",
        "a whole number of deltas",
        FOLD_DOWN_FACTOR,
    )
}

#[derive(Debug)]
pub struct FoldStats {
    pub id: String,
    pub kind: CheckpointKind,
    pub files: usize,
    pub bytes: u64,
    pub pages: usize,
    pub runs: usize,
}

/// The layout holding a checkpoint's objects: an inherited ref names
/// its owner, our own live under the tenant's own prefix.
pub(crate) fn chk_layout(own: &TenantLayout, c: &CheckpointRef) -> TenantLayout {
    match &c.owner {
        Some(owner) => TenantLayout::new(owner),
        None => own.clone(),
    }
}

/// Total stored bytes a checkpoint describes, summed from its INDEX.
/// A trimmed line carries the file's length and the object's; the
/// object's is what the store holds and what a fold down would move, so
/// that is the one the policy weighs. A holed line keeps the object's
/// length in the same field, which is why both read alike here.
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
        } else if let Some(rest) = line.strip_prefix("t ").or_else(|| line.strip_prefix("h ")) {
            let len = rest
                .split(' ')
                .nth(1)
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

/// Pages per run object, 8MB runs in v0. The spec targets bigger runs
/// once the read path range reads them, the index records the value so
/// a reader never has to guess.
const RUN_PAGES: usize = 1024;

/// The blocks dirtied since the previous checkpoint, scanned out of the
/// WAL the shared log holds over that window, plus the relations its
/// smgr create and truncate records name. Completeness rests on the
/// write gate: no page object mutates before its WAL is durable in the
/// stream, so the stream names every page the fold must carry. The
/// relation events ride into the PAGES index as r lines because a
/// truncate or a file recreation makes older checkpoint copies of the
/// relation stale without any block reference saying so, and the read
/// path needs that barrier long after this WAL is gone from the log.
fn delta_scan(
    media: &WalMedia,
    tenant: u128,
    manifest: &Manifest,
    redo: u64,
) -> Result<walscan::ScanOut, String> {
    let prev = manifest
        .checkpoints
        .last()
        .map(|c| c.lsn.0)
        .ok_or_else(|| "delta fold with no prior checkpoint".to_string())?;
    let frames = catch_up(media, WAL_SHARD, &TeeFilter::Tenant(tenant), Lsn(prev))
        .map_err(|e| format!("catch up: {e}"))?;
    let window = walscan::assemble_window_frames(&frames, prev, redo);
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
            let n = zou_store::forksize::ForkSize::decode(&data)
                .map(|fs| fs.nblocks)
                .ok_or_else(|| format!("bad SIZE object at {key}"))?;
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
        let n = zou_store::forksize::ForkSize::decode(&data)
            .map(|fs| fs.nblocks)
            .ok_or_else(|| format!("bad SIZE object for {spc}/{db}/{rel}/{fork}"))?;
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
        match store.put_if_absent(&layout.checkpoint_pages(id, *runs), run) {
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
    match store.put_if_absent(&layout.checkpoint_page_index(id), index.as_bytes()) {
        Ok(_) | Err(CasError::AlreadyExists { .. }) => Ok((pages, runs as usize)),
        Err(e) => Err(format!("put PAGES: {e}")),
    }
}

/// Whether the manifest still leans on state it does not own: a branch
/// origin or checkpoints tagged with another owner. Once a merged full
/// publishes and prunes those, the tenant packs like any other.
fn branched(m: &Manifest) -> bool {
    m.branch_of.is_some() || m.checkpoints.iter().any(|c| c.owner.is_some())
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

/// The stream bytes from the redo page boundary through the end of the
/// checkpoint record at `chk_record`, prefixed with their 8 byte little
/// endian start lsn. The record is written before the fold starts, but
/// the pusher may not have shipped it yet, so this polls the shared log
/// for a while; the fold runs on its own thread and can afford to wait.
fn capture_wal_tail(
    media: &WalMedia,
    tenant: u128,
    redo: u64,
    chk_record: u64,
) -> Result<Vec<u8>, String> {
    if chk_record < redo {
        return Err(format!(
            "pg_control names checkpoint record {chk_record:#X} before its redo {redo:#X}"
        ));
    }
    let floor = redo & !(WAL_PAGE_SIZE - 1);
    for attempt in 0..120u32 {
        if attempt > 0 {
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        let frames = catch_up(media, WAL_SHARD, &TeeFilter::Tenant(tenant), Lsn(floor))
            .map_err(|e| format!("wal catch up: {e}"))?;
        // Two pages of slack past the record's start cover its length
        // and a page header it may straddle.
        let window =
            walscan::assemble_window_frames(&frames, floor, chk_record + 2 * WAL_PAGE_SIZE);
        let Ok(out) = walscan::read_records(&window, chk_record, None) else {
            continue;
        };
        let Some(record) = out.records.first() else {
            continue;
        };
        let start = floor.max(window.covered_from);
        let (lo, hi) = ((start - floor) as usize, (record.end_lsn - floor) as usize);
        let mut tail = Vec::with_capacity(8 + hi - lo);
        tail.extend_from_slice(&start.to_le_bytes());
        tail.extend_from_slice(&window.buf[lo..hi]);
        return Ok(tail);
    }
    Err(format!(
        "the checkpoint record at {chk_record:#X} never appeared in the shared log"
    ))
}

/// Everything [`prepare`] produced and [`publish`] applies: the ref
/// the manifest gains and the stats. The split exists so the capture
/// and pack can run on a thread that never touches the lease, the
/// publish is a manifest edit the caller applies when it collects the
/// result.
pub struct FoldOutcome {
    pub checkpoint: CheckpointRef,
    pub stats: FoldStats,
}

/// Capture the checkpoint at `redo`, a delta normally or a full when
/// the fold down policy says the chain has outgrown its base, without
/// publishing anything. Idempotent per redo: the checkpoint id is
/// derived from it and every step tolerates a retried run. Errors
/// leave the manifest unchanged and any objects written are gc food,
/// the caller retries.
///
/// `pack_runs` false skips the page runs and publishes an indexless
/// checkpoint: filesystem capture, pg_control, and the WAL tail only.
/// That is the shape the page service wants, where pg/ holds stale
/// flag day images kept as reconstruction bases and packing them
/// would publish garbage, but the checkpoint still has to exist so a
/// restore anchors at the newest redo instead of replaying the whole
/// history.
pub fn prepare(
    store: &dyn CasStore,
    layout: &TenantLayout,
    media: &WalMedia,
    tenant: u128,
    pgdata: &Path,
    redo: u64,
    pack_runs: bool,
) -> Result<FoldOutcome, String> {
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
        bytes = capture::upload(store, layout, &id, &files, &paths.dirs, true, Some(redo))?;
    }

    // The WAL slice recovery anchors on: from the redo page boundary
    // through the end of the checkpoint record pg_control names. A
    // restore with a live stream overlays these bytes anyway, but a
    // branch child and a time travel restore have no stream, and
    // without the tail they hold a pg_control naming a record that
    // exists nowhere. Deterministic per redo, so a retried fold that
    // finds it skips the capture.
    if store
        .get(&layout.chk_waltail(&id))
        .map_err(|e| format!("store: {e}"))?
        .is_none()
    {
        let tail = capture_wal_tail(media, tenant, redo, control_checkpoint(control)?)?;
        store
            .put(&layout.chk_waltail(&id), &tail)
            .map_err(|e| format!("store: {e}"))?;
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
        _ if !pack_runs => (0, 0),
        CheckpointKind::Delta => {
            let out = delta_scan(media, tenant, &manifest, redo)?;
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

    Ok(FoldOutcome {
        checkpoint: CheckpointRef {
            id: id.clone(),
            lsn: Lsn(redo),
            kind,
            owner: None,
        },
        stats: FoldStats {
            id,
            kind,
            files: files.len(),
            bytes,
            pages,
            runs,
        },
    })
}

/// Publish a prepared checkpoint into the manifest under the lease:
/// push the ref if a retry has not already, prune everything a full
/// supersedes, and raise `folded_upto` to the fold's redo. Retries the
/// CAS race a bounded number of times, every other error propagates,
/// and [`LeaseError::Lost`] means stop writing.
pub fn publish(
    store: &dyn CasStore,
    layout: &TenantLayout,
    held: &mut HeldLease,
    checkpoint: &CheckpointRef,
    redo: u64,
    now_unix: u64,
) -> Result<(), LeaseError> {
    for _ in 0..5 {
        let result = lease::update_manifest(store, layout, held, now_unix, |m| {
            if !m.checkpoints.iter().any(|c| c.id == checkpoint.id) {
                m.checkpoints.push(checkpoint.clone());
            }
            if checkpoint.kind == CheckpointKind::Full
                && let Some(pos) = m.checkpoints.iter().rposition(|c| c.id == checkpoint.id)
            {
                m.checkpoints.drain(..pos);
            }
            m.folded_upto = Some(Lsn(redo));
        });
        match result {
            Err(LeaseError::Raced) => continue,
            other => return other,
        }
    }
    Err(LeaseError::Raced)
}

/// [`prepare`] plus [`publish`]: the synchronous shape, used by tests
/// and by anyone who holds the lease and does not mind blocking on the
/// capture.
#[allow(clippy::too_many_arguments)]
pub fn fold(
    store: &dyn CasStore,
    layout: &TenantLayout,
    media: &WalMedia,
    tenant: u128,
    held: &mut HeldLease,
    pgdata: &Path,
    redo: u64,
    now_unix: u64,
) -> Result<FoldStats, String> {
    let out = prepare(store, layout, media, tenant, pgdata, redo, true)?;
    publish(store, layout, held, &out.checkpoint, redo, now_unix)
        .map_err(|e| format!("fold publish: {e}"))?;
    Ok(out.stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::restore::WAL_SEGMENT_SIZE;
    use crate::testv2::V2Wal;
    use crate::walscan::testwal::Builder;
    use std::sync::Arc;
    use zou_store::LocalFsStore;

    fn synthetic_control(redo: u64) -> Vec<u8> {
        // Same shape the restore tests use: state in production at 16,
        // the checkpoint record lsn at 32, the redo at 40, junk, the crc
        // over everything before it at 300. The tests model a shutdown
        // style checkpoint whose record sits at the redo itself, so a
        // record pushed at redo is the one the tail capture must find.
        let mut control = vec![0u8; 8192];
        control[0..8].copy_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());
        control[16..20].copy_from_slice(&6u32.to_le_bytes());
        for (i, b) in control[20..300].iter_mut().enumerate() {
            *b = (i * 7 + 13) as u8;
        }
        control[32..40].copy_from_slice(&redo.to_le_bytes());
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
            .put_if_absent(&layout.chk_index("genesis"), b"f base/big 1000000\n")
            .unwrap();
        store
            .put_if_absent(&layout.manifest(), &genesis.to_json())
            .unwrap();
        let mut held = lease::acquire(&*store, &layout, "test", 15, 1000).unwrap();
        let epoch = u32::try_from(held.epoch).unwrap();
        let mut wal2 = V2Wal::open(Arc::clone(&store) as Arc<dyn CasStore>, "local", epoch);
        // Two chunks wholly below the scan window, retries the scan
        // must clip away, then the real WAL.
        wal2.push(0x100, &[0x5A; 100]);
        wal2.push(0x200, &[0x5A; 100]);
        let (stream_lsn, stream_bytes) = wal.stream();
        wal2.push(stream_lsn, stream_bytes);

        let stats = fold(
            &*store,
            &layout,
            &wal2.media,
            wal2.tenant,
            &mut held,
            pgdata,
            redo,
            2000,
        )
        .unwrap();
        assert_eq!(stats.kind, CheckpointKind::Delta);
        assert_eq!(stats.files, 2);
        assert_eq!(
            stats.pages, 2,
            "block 5 is gone and the post fold record is out"
        );
        assert_eq!(stats.runs, 1);

        let m = manifest_of(&*store, &layout);
        assert_eq!(m.folded_upto, Some(Lsn(redo)));
        assert_eq!(m.checkpoints.len(), 2);
        let chk = &m.checkpoints[1];
        assert_eq!(chk.lsn, Lsn(redo));
        assert_eq!(chk.kind, CheckpointKind::Delta);
        assert_eq!(chk.id, format!("{redo:016x}"));

        // The capture round trips byte for byte, minus the trailing
        // zeros of a control file that is mostly padding, which the
        // INDEX describes so a restore puts them back.
        let (stored, _) = store
            .get(&layout.chk_file(&chk.id, "global/pg_control"))
            .unwrap()
            .unwrap();
        let live = control.iter().rposition(|b| *b != 0).unwrap() + 1;
        assert_eq!(stored, control[..live]);
        let (index, _) = store.get(&layout.chk_index(&chk.id)).unwrap().unwrap();
        let index = String::from_utf8(index).unwrap();
        assert!(index.contains("f pg_xact/0000 4"));
        assert!(index.contains(&format!("t {} {live} global/pg_control\n", control.len())));

        // The WAL tail: everything from where coverage starts, the
        // stream base here since nothing older was ever pushed, through
        // the end of the record at the checkpoint lsn, which is the
        // whole synthetic stream because that record is its last. A
        // restore with no live frames lays exactly these bytes down so
        // recovery finds the record pg_control names.
        let (tail, _) = store.get(&layout.chk_waltail(&chk.id)).unwrap().unwrap();
        let start = u64::from_le_bytes(tail[..8].try_into().unwrap());
        assert_eq!(start, stream_lsn);
        assert_eq!(&tail[8..], stream_bytes);

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
        let again = fold(
            &*store,
            &layout,
            &wal2.media,
            wal2.tenant,
            &mut held,
            pgdata,
            redo,
            2001,
        )
        .unwrap();
        assert_eq!(again.kind, CheckpointKind::Delta);
        assert_eq!(manifest_of(&*store, &layout).checkpoints.len(), 2);

        // A capture whose pg_control moved past the fold is refused.
        let newer = synthetic_control(redo + 0x1000);
        std::fs::write(pgdata.join("global/pg_control"), &newer).unwrap();
        let err = fold(
            &*store,
            &layout,
            &wal2.media,
            wal2.tenant,
            &mut held,
            pgdata,
            redo,
            2002,
        )
        .unwrap_err();
        assert!(err.contains("past the fold"));

        wal2.close();
    }

    #[test]
    fn a_fold_without_page_runs_publishes_an_anchor_checkpoint() {
        // The page service shape: pg/ holds stale flag day images, so
        // the fold skips the runs but still captures the filesystem and
        // the WAL tail and publishes the ref. A restore then anchors at
        // this redo and replays only the shared log past it, instead of
        // the whole history from genesis.
        let store_dir = tempfile::tempdir().unwrap();
        let pgdata_dir = tempfile::tempdir().unwrap();
        let pgdata = pgdata_dir.path();
        let store = Arc::new(LocalFsStore::new(store_dir.path()));
        let layout = TenantLayout::new("local");

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
        let redo = wal.pos();
        wal.record(&[], b"the checkpoint record");

        std::fs::create_dir_all(pgdata.join("global")).unwrap();
        std::fs::create_dir_all(pgdata.join("pg_xact")).unwrap();
        let control = synthetic_control(redo);
        std::fs::write(pgdata.join("global/pg_control"), &control).unwrap();
        std::fs::write(pgdata.join("pg_xact/0000"), b"clog").unwrap();

        let mut genesis = Manifest::new("local", 18);
        genesis.checkpoints.push(CheckpointRef {
            id: "genesis".into(),
            lsn: Lsn(stream_base),
            kind: CheckpointKind::Full,
            owner: None,
        });
        store
            .put_if_absent(&layout.chk_index("genesis"), b"f base/big 1000000\n")
            .unwrap();
        store
            .put_if_absent(&layout.manifest(), &genesis.to_json())
            .unwrap();
        let mut held = lease::acquire(&*store, &layout, "test", 15, 1000).unwrap();
        let epoch = u32::try_from(held.epoch).unwrap();
        let mut wal2 = V2Wal::open(Arc::clone(&store) as Arc<dyn CasStore>, "local", epoch);
        let (stream_lsn, stream_bytes) = wal.stream();
        wal2.push(stream_lsn, stream_bytes);

        let out = prepare(
            &*store,
            &layout,
            &wal2.media,
            wal2.tenant,
            pgdata,
            redo,
            false,
        )
        .unwrap();
        assert_eq!(out.stats.kind, CheckpointKind::Delta);
        assert_eq!(out.stats.files, 2);
        assert_eq!(out.stats.pages, 0, "no runs packed");
        assert_eq!(out.stats.runs, 0);
        publish(&*store, &layout, &mut held, &out.checkpoint, redo, 2000).unwrap();

        // The ref landed and the anchor moved to the fold's redo.
        let m = manifest_of(&*store, &layout);
        assert_eq!(m.folded_upto, Some(Lsn(redo)));
        assert_eq!(m.checkpoints.len(), 2);
        let chk = &m.checkpoints[1];
        assert_eq!(chk.lsn, Lsn(redo));
        assert_eq!(chk.id, format!("{redo:016x}"));

        // The capture and the WAL tail are whole, the PAGES index never
        // came into being: an indexless checkpoint.
        let (index, _) = store.get(&layout.chk_index(&chk.id)).unwrap().unwrap();
        assert!(
            String::from_utf8(index)
                .unwrap()
                .contains("f pg_xact/0000 4")
        );
        let (tail, _) = store.get(&layout.chk_waltail(&chk.id)).unwrap().unwrap();
        let start = u64::from_le_bytes(tail[..8].try_into().unwrap());
        assert_eq!(start, stream_lsn);
        assert_eq!(&tail[8..], stream_bytes);
        assert!(
            store
                .get(&layout.checkpoint_page_index(&chk.id))
                .unwrap()
                .is_none(),
            "no page index published"
        );
        assert!(
            store
                .get(&layout.checkpoint_pages(&chk.id, 0))
                .unwrap()
                .is_none(),
            "no run objects published"
        );

        wal2.close();
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
            .put_if_absent(&layout.chk_index("genesis"), b"f base/small 100\n")
            .unwrap();
        store
            .put_if_absent(&layout.chk_index("d1"), b"f pg_xact/0000 600\n")
            .unwrap();
        store
            .put_if_absent(&layout.manifest(), &m.to_json())
            .unwrap();

        let mut held = lease::acquire(&*store, &layout, "test", 15, 1000).unwrap();
        let epoch = u32::try_from(held.epoch).unwrap();
        let mut wal2 = V2Wal::open(Arc::clone(&store) as Arc<dyn CasStore>, "local", epoch);
        wal2.push(0x100, b"payload");
        // The checkpoint record at redo, which the tail capture waits
        // for before the fold can complete.
        let mut wal = Builder::new(redo);
        wal.record(&[], b"checkpoint");
        let (rec_lsn, rec_bytes) = wal.stream();
        wal2.push(rec_lsn, rec_bytes);

        let stats = fold(
            &*store,
            &layout,
            &wal2.media,
            wal2.tenant,
            &mut held,
            pgdata,
            redo,
            2000,
        )
        .unwrap();
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

        wal2.close();
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
                .put_if_absent(&parent.checkpoint_page_index(id), index.as_bytes())
                .unwrap();
            if !run.is_empty() {
                let bytes: Vec<u8> = run.iter().flatten().copied().collect();
                store
                    .put_if_absent(&parent.checkpoint_pages(id, 0), &bytes)
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
        store
            .put_if_absent(&parent.manifest(), &pm.to_json())
            .unwrap();

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

        let mut held = lease::acquire(&*store, &child, "test", 15, 1000).unwrap();
        let epoch = u32::try_from(held.epoch).unwrap();
        let mut wal2 = V2Wal::open(Arc::clone(&store) as Arc<dyn CasStore>, "child", epoch);
        wal2.push(0x100, b"payload");
        // The checkpoint record at redo, which the tail capture waits
        // for before the fold can complete.
        let mut wal = Builder::new(redo);
        wal.record(&[], b"checkpoint");
        let (rec_lsn, rec_bytes) = wal.stream();
        wal2.push(rec_lsn, rec_bytes);

        let stats = fold(
            &*store,
            &child,
            &wal2.media,
            wal2.tenant,
            &mut held,
            pgdata,
            redo,
            6000,
        )
        .unwrap();
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

        // Published, the chain prunes to the new full and nothing in
        // the manifest names the parent's objects anymore: the branch
        // is self sufficient and gc may unpin the parent.
        let m = manifest_of(&*store, &child);
        assert_eq!(m.checkpoints.len(), 1);
        assert_eq!(m.checkpoints[0].kind, CheckpointKind::Full);
        assert_eq!(m.checkpoints[0].owner, None);
        assert_eq!(m.folded_upto, Some(Lsn(redo)));

        wal2.close();
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
            .put_if_absent(&layout.chk_index("genesis"), b"f base/huge 1000000\n")
            .unwrap();
        for i in 1..=4u64 {
            let id = format!("d{i}");
            store
                .put_if_absent(&layout.chk_index(&id), b"f pg_xact/0000 1\n")
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
