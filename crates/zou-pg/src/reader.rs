//! The chain reader: serve relation pages from checkpoint page runs
//! instead of one object per block.
//!
//! The checkpoint chain, the newest full capture and the deltas after
//! it, holds a sorted run copy of every page the WAL dirtied in each
//! window, so a read can binary search the newest index that names the
//! block and range read the page out of an 8MB run object. The pg/
//! prefix stays the fallback for everything the chain does not cover
//! and for every doubt, a pg/ read is always correct.
//!
//! Freshness is the hard part: a run image was packed at fold time and
//! the block may have changed since. The argument that serving is safe
//! has three legs. First, the write gate: a page object never mutates
//! before the WAL covering it is durable in the shared log, so every
//! change newer than the chain has a landing segment on the fenced
//! chain. Second, Postgres serializes eviction against reads through
//! the buffer mapping table: an evicting backend finishes its write
//! before another backend can start reading the same block, so by the
//! time this reader runs, the change's WAL append has completed.
//! Third, the barrier below probes the chain forward from its cursor
//! before every run served read, scans whatever is new, and any block
//! or relation the new records touch goes into a dirty set that forces
//! pg/ for good. A block that is in the chain, not dirtied by any
//! record at or above the newest checkpoint's redo, and not written by
//! this process, is exactly its run image.
//!
//! Relation level staleness needs its own barrier: smgr truncate and
//! create records name a relation with no block references, and after
//! one of those the older checkpoint copies of the relation are stale
//! even for blocks no record names again. The fold persists a file
//! recreation as an r line in the PAGES index and the chain walk stops
//! for a relation at the first index naming it, every block of the old
//! incarnation is dead. A truncate is softer and gets a t line with
//! the record's surviving block count: main fork blocks below it kept
//! their bytes and older copies of them still serve, which matters on
//! a branch where the pg/ fallback holds nothing, only blocks at or
//! past the cutoff and the vm and fsm forks go stale. Unlogged
//! relations never enter the runs at all, the fold skips them, because
//! their writes bypass the WAL and no barrier can see them.
//!
//! A branched tenant complicates the walk in one bounded way: the
//! inherited checkpoints carry an owner tag, so their PAGES indexes
//! and run objects are fetched under the owner's prefix. Branches cut
//! at a checkpoint lsn, so a child inherits no unfolded WAL and the
//! barrier only ever watches the child's own stream.
//!
//! Any parse error, store error, or coverage gap poisons the reader,
//! which then declines every read and the smgr falls back to pg/.
//!
//! The chain probes would be the whole read cost if they ran every
//! time, so the barrier is gated on the durable LSN the wal pusher
//! publishes into shared memory. The write gate reads that same value
//! before letting a page object mutate, so a change to any block
//! always advances the published LSN before the page can change, and a
//! read that sees an unchanged value since its last scan can skip the
//! probes outright. A zero means no pusher has published, initdb,
//! recovery, or a store opened readonly, and the reader probes every
//! time. Run slabs go through [`crate::cache::SlabCache`], RAM in
//! front of an optional shared disk tier, keyed by checkpoint id, run,
//! and offset, which is content addressed because run objects are
//! immutable.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use zou_log::{ShardManifest, TeeFilter, WalMedia, catch_up, decode_segment};
use zou_store::layout::TenantLayout;
use zou_store::manifest::{CheckpointKind, Manifest};
use zou_store::{CasStore, Lsn, tenant_id};

use crate::cache::{CacheConfig, SlabCache};
use crate::walscan::{self, BlockRef, RelTag, WalWindow};
use crate::{WAL_SHARD, ZOU_PAGE_SIZE};

/// Readahead unit for run objects. Neighboring pages of a sequential
/// scan land in the same slab, one range request instead of 128.
const SLAB_BYTES: u64 = 1 << 20;

/// The parsed PAGES index of one checkpoint, remembering which tenant
/// prefix holds its objects: an inherited checkpoint reads from its
/// owner, everything else from the attaching tenant. The fold uses it
/// too when a branched full merges the chain into its own runs.
pub(crate) struct ChkIndex {
    pub(crate) id: String,
    pub(crate) layout: TenantLayout,
    pub(crate) run_pages: usize,
    pub(crate) entries: Vec<BlockRef>,
    pub(crate) rels: BTreeSet<RelTag>,
    /// Truncate events the fold recorded as t lines, the smallest main
    /// fork cutoff per relation in the window. Blocks below it survive
    /// the walk into older indexes, everything else the event names is
    /// dead, see the module docs.
    pub(crate) truncs: BTreeMap<RelTag, u32>,
    /// Fork sizes the fold recorded as s lines: every fork for a full
    /// capture, the touched relations for a delta. The chain walk in
    /// [`ChainReader::fork_size`] answers from the newest index naming
    /// a fork, which is how a branched tenant with an empty pg/ prefix
    /// learns the lengths of inherited relations.
    pub(crate) sizes: BTreeMap<(u32, u32, u32, u32), u32>,
}

impl ChkIndex {
    pub(crate) fn parse(id: &str, layout: TenantLayout, text: &str) -> Result<Self, String> {
        let mut lines = text.lines();
        let run_pages = lines
            .next()
            .and_then(|l| l.strip_prefix("runs "))
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .ok_or_else(|| format!("bad PAGES header in {id}"))?;
        let mut entries = Vec::new();
        let mut rels = BTreeSet::new();
        let mut truncs = BTreeMap::new();
        let mut sizes = BTreeMap::new();
        for line in lines {
            let num = |rest: &str| -> Result<Vec<u32>, String> {
                rest.split(' ')
                    .map(|v| v.parse().map_err(|_| format!("bad PAGES line in {id}")))
                    .collect()
            };
            if let Some(rest) = line.strip_prefix("p ") {
                let v = num(rest)?;
                if v.len() != 5 {
                    return Err(format!("bad PAGES line in {id}"));
                }
                entries.push(BlockRef {
                    spc: v[0],
                    db: v[1],
                    rel: v[2],
                    fork: v[3],
                    blk: v[4],
                });
            } else if let Some(rest) = line.strip_prefix("r ") {
                let v = num(rest)?;
                if v.len() != 3 {
                    return Err(format!("bad PAGES line in {id}"));
                }
                rels.insert(RelTag {
                    spc: v[0],
                    db: v[1],
                    rel: v[2],
                });
            } else if let Some(rest) = line.strip_prefix("t ") {
                let v = num(rest)?;
                if v.len() != 4 {
                    return Err(format!("bad PAGES line in {id}"));
                }
                truncs.insert(
                    RelTag {
                        spc: v[0],
                        db: v[1],
                        rel: v[2],
                    },
                    v[3],
                );
            } else if let Some(rest) = line.strip_prefix("s ") {
                let v = num(rest)?;
                if v.len() != 5 {
                    return Err(format!("bad PAGES line in {id}"));
                }
                sizes.insert((v[0], v[1], v[2], v[3]), v[4]);
            } else {
                return Err(format!("unknown PAGES line in {id}"));
            }
        }
        if !entries.is_sorted() {
            return Err(format!("PAGES entries out of order in {id}"));
        }
        Ok(Self {
            id: id.to_string(),
            layout,
            run_pages,
            entries,
            rels,
            truncs,
            sizes,
        })
    }

    /// Which run object holds the block and at what byte offset.
    pub(crate) fn lookup(&self, r: &BlockRef) -> Option<(u32, u64)> {
        let i = self.entries.binary_search(r).ok()?;
        Some((
            (i / self.run_pages) as u32,
            ((i % self.run_pages) * ZOU_PAGE_SIZE) as u64,
        ))
    }
}

/// The tenant's stream past the newest checkpoint, reassembled
/// incrementally from shared log frames. The window only ever holds
/// bytes past the last complete record scanned, the consumed prefix is
/// dropped to keep memory bounded.
struct TailScan {
    window: WalWindow,
    started: bool,
}

impl TailScan {
    fn new() -> Self {
        Self {
            window: WalWindow {
                base: 0,
                buf: Vec::new(),
                covered_from: 0,
            },
            started: false,
        }
    }

    /// Append one pushed chunk. Bytes below the floor predate the
    /// newest checkpoint and are clipped, an overlap with bytes already
    /// held is clipped too, and a gap is an error, the tenant's stream
    /// is contiguous.
    fn append(&mut self, lsn: u64, bytes: &[u8], floor: u64) -> Result<(), String> {
        let end = lsn + bytes.len() as u64;
        if !self.started {
            if end <= floor {
                return Ok(());
            }
            self.window.base = lsn.max(floor);
            self.window.covered_from = self.window.base;
            self.started = true;
        }
        let have = self.window.base + self.window.buf.len() as u64;
        if end <= have {
            return Ok(());
        }
        if lsn > have {
            return Err(format!("wal gap in the stream at {have:#X}"));
        }
        self.window
            .buf
            .extend_from_slice(&bytes[(have - lsn) as usize..]);
        Ok(())
    }

    /// The Postgres LSN the scan holds bytes through, the resume point
    /// for a catch up read. `None` before the first byte arrived.
    fn end(&self) -> Option<u64> {
        self.started
            .then(|| self.window.base + self.window.buf.len() as u64)
    }

    /// Scan the complete records the window now covers into the dirty
    /// sets and drop the consumed bytes. A record the window ends
    /// inside waits for the next append, the write gate keeps its pages
    /// out of the store until then.
    fn scan(
        &mut self,
        dirty: &mut BTreeSet<BlockRef>,
        rels: &mut BTreeSet<RelTag>,
        truncs: &mut BTreeMap<RelTag, u32>,
    ) -> Result<(), String> {
        if !self.started || self.window.buf.is_empty() {
            return Ok(());
        }
        let out = walscan::scan_available(&self.window, self.window.base)?;
        dirty.extend(out.refs);
        rels.extend(out.rels);
        for (tag, cut) in out.truncs {
            let e = truncs.entry(tag).or_insert(cut);
            *e = (*e).min(cut);
        }
        let consumed = (out.resume - self.window.base) as usize;
        self.window.buf.drain(..consumed);
        self.window.base = out.resume;
        self.window.covered_from = out.resume;
        Ok(())
    }
}

/// Why an attach failed. A fatal failure means the manifest is a
/// branch: the pg/ prefix of a branch holds only its own divergent
/// writes, so falling back to it would read zeros where inherited
/// pages should be, and the caller must refuse to serve instead.
#[derive(Debug)]
pub struct AttachError {
    pub fatal: bool,
    pub why: String,
}

/// What a branch that inherits no readable pages fails with, in
/// [`ChainReader::attach`] when it is being served and in
/// [`why_unservable`] before it is taken.
const NO_CHAIN: &str = "there is no run bearing full capture to serve inherited pages from, \
                        fold one in the source first";

/// The rule [`ChainReader::attach`] holds a branch to, asked of a
/// manifest instead of a running reader: from the newest full capture
/// up, every checkpoint has to carry a page run index. A branch has no
/// pg/ fallback, so one that does not is a window of blocks the child
/// could never read, and the answer is the reason it could not, or
/// `None` when the manifest would serve.
///
/// This only reads the page indexes, no WAL and no runs, so it is
/// cheap enough to ask before branching. `layout` is the manifest's
/// own, the checkpoint owners point at whatever prefix holds them.
pub fn why_unservable(
    store: &dyn CasStore,
    layout: &TenantLayout,
    manifest: &Manifest,
) -> Result<Option<String>, String> {
    let Some(full) = manifest
        .checkpoints
        .iter()
        .rposition(|c| c.kind == CheckpointKind::Full)
    else {
        return Ok(Some(NO_CHAIN.to_string()));
    };
    let mut bearing = 0;
    let mut gap = false;
    for c in manifest.checkpoints[full..].iter().rev() {
        let lay = crate::fold::chk_layout(layout, c);
        match store
            .get(&lay.checkpoint_page_index(&c.id))
            .map_err(|e| format!("store: {e}"))?
        {
            Some(_) if gap => {
                return Ok(Some(format!(
                    "checkpoint {} has PAGES but a newer one does not",
                    c.id
                )));
            }
            Some(_) => bearing += 1,
            None => gap = true,
        }
    }
    if bearing == 0 || gap {
        return Ok(Some(NO_CHAIN.to_string()));
    }
    Ok(None)
}

pub struct ChainReader {
    /// Newest first, starting no earlier than the newest full capture.
    chain: Vec<ChkIndex>,
    /// The newest checkpoint's redo. Everything below it is inside the
    /// chain, the barrier only scans WAL at or above it.
    floor: u64,
    dirty: BTreeSet<BlockRef>,
    dirty_rels: BTreeSet<RelTag>,
    /// Truncates the tail WAL or this process performed, the smallest
    /// cutoff per relation. Blocks below it still serve from the
    /// chain, the truncate never touched them.
    dirty_truncs: BTreeMap<RelTag, u32>,
    /// The shared log the barrier watches, over the same store.
    media: Arc<WalMedia>,
    /// This tenant's frame id in the shared log.
    tenant: u128,
    tail: TailScan,
    /// The next chain position the barrier probes. Everything below it
    /// has been fetched and fed into the tail scan.
    next_seq: u64,
    /// The published durable LSN the last barrier ran under. A read
    /// whose durable LSN has not moved past this skips the probes.
    scanned_durable: u64,
    cache: SlabCache,
    poisoned: bool,
    /// The manifest is a branch, pg/ fallback is not sound for blocks
    /// this reader declines, see [`AttachError`].
    branched: bool,
}

impl ChainReader {
    /// Build a reader from the current manifest. `Ok(None)` means there
    /// is nothing to serve, no manifest, no full capture, or no
    /// checkpoint with a page run index yet, and the caller stays on
    /// pg/. An index pattern that cannot be served soundly is an error:
    /// every checkpoint newer than the oldest index bearing one must
    /// have an index too, otherwise a window of dirtied blocks would be
    /// invisible to the chain walk. A branched manifest tightens both
    /// rules into fatal errors, and demands a run bearing full at the
    /// bottom of the chain, because a branch has no pg/ fallback for
    /// inherited state: blocks below an indexless capture exist only
    /// under the parent's mutable prefix, which moved on after the
    /// branch and must never serve.
    pub fn attach(
        store: &Arc<dyn CasStore>,
        layout: &TenantLayout,
    ) -> Result<Option<Self>, AttachError> {
        let soft = |why: String| AttachError { fatal: false, why };
        let Some((data, _)) = store
            .get(&layout.manifest())
            .map_err(|e| soft(format!("store: {e}")))?
        else {
            return Ok(None);
        };
        let manifest = Manifest::from_json(&data).map_err(|e| soft(format!("manifest: {e}")))?;
        let branched =
            manifest.branch_of.is_some() || manifest.checkpoints.iter().any(|c| c.owner.is_some());
        let fail = |why: String| AttachError {
            fatal: branched,
            why,
        };
        let no_chain = || AttachError {
            fatal: true,
            why: NO_CHAIN.into(),
        };
        let Some(full) = manifest
            .checkpoints
            .iter()
            .rposition(|c| c.kind == CheckpointKind::Full)
        else {
            if branched {
                return Err(no_chain());
            }
            return Ok(None);
        };
        // The index bearing checkpoints must form a contiguous newest
        // suffix. Every fold drops WAL below its redo, so a window
        // whose checkpoint lacks an index has dirtied blocks the
        // barrier can no longer see; a bearing entry older than such a
        // window can therefore never be served. Older indexless
        // entries, like a genesis capture from before the fold packed
        // runs, are fine, blocks only they cover fall back to pg/.
        let mut chain = Vec::new();
        let mut gap = false;
        for c in manifest.checkpoints[full..].iter().rev() {
            let lay = crate::fold::chk_layout(layout, c);
            match store
                .get(&lay.checkpoint_page_index(&c.id))
                .map_err(|e| fail(format!("store: {e}")))?
            {
                Some((data, _)) => {
                    if gap {
                        return Err(fail(format!(
                            "checkpoint {} has PAGES but a newer one does not",
                            c.id
                        )));
                    }
                    let text = String::from_utf8(data)
                        .map_err(|_| fail(format!("PAGES for {} is not utf8", c.id)))?;
                    chain.push(ChkIndex::parse(&c.id, lay, &text).map_err(fail)?);
                }
                None => gap = true,
            }
        }
        if branched && (chain.is_empty() || gap) {
            return Err(no_chain());
        }
        if chain.is_empty() {
            return Ok(None);
        }
        // The dirty floor: the newest checkpoint's redo, which by the
        // suffix rule is exactly where the newest index's window ends.
        // The stream holds everything from here on, folds only raise
        // the replay floor below it.
        let floor = manifest.checkpoints.last().expect("full exists").lsn.0;
        // The history is in the shared log: one catch up read pulls
        // every frame of this tenant past the floor out of the sealed
        // rounds and the landing tail, and the live barrier takes over
        // from the consolidated boundary. Frames the catch up already
        // delivered clip as overlaps when the barrier re-fetches their
        // landing segments.
        let logs = crate::log_store(Arc::clone(store), layout);
        let media = Arc::new(WalMedia::single(Arc::clone(&logs)));
        let tenant = tenant_id(layout.tenant_ref());
        let next_seq = match ShardManifest::load(&*logs, WAL_SHARD) {
            Ok(Some((m, _))) => m.consolidated_upto + 1,
            Ok(None) => 1,
            Err(e) => return Err(fail(format!("shard manifest: {e}"))),
        };
        let mut tail = TailScan::new();
        let frames = catch_up(&media, WAL_SHARD, &TeeFilter::Tenant(tenant), Lsn(floor))
            .map_err(|e| fail(format!("catch up: {e}")))?;
        for frame in &frames {
            tail.append(frame.start_lsn.0, &frame.payload, floor)
                .map_err(fail)?;
        }
        let mut dirty = BTreeSet::new();
        let mut dirty_rels = BTreeSet::new();
        let mut dirty_truncs = BTreeMap::new();
        tail.scan(&mut dirty, &mut dirty_rels, &mut dirty_truncs)
            .map_err(fail)?;
        Ok(Some(Self {
            chain,
            floor,
            dirty,
            dirty_rels,
            dirty_truncs,
            media,
            tenant,
            tail,
            next_seq,
            scanned_durable: 0,
            cache: SlabCache::new(CacheConfig::from_env()),
            poisoned: false,
            branched,
        }))
    }

    /// Emit the cache hit rate summary, see
    /// [`SlabCache::log_summary`].
    pub fn log_cache_summary(&self) {
        self.cache.log_summary();
    }

    pub fn poisoned(&self) -> bool {
        self.poisoned
    }

    fn poison(&mut self, why: &str) {
        if !self.poisoned {
            log::error!("zou chain reader poisoned, all reads fall back to pg/: {why}");
        }
        self.poisoned = true;
    }

    /// A page this process wrote is newer than any run image of it.
    pub fn note_write(&mut self, r: BlockRef) {
        self.dirty.insert(r);
    }

    /// An unlink or create in this process invalidates every run image
    /// of the relation.
    pub fn note_rel(&mut self, t: RelTag) {
        self.dirty_rels.insert(t);
    }

    /// A truncate in this process: main fork blocks below `nblocks`
    /// survived and keep serving, everything else the relation held is
    /// dead. The smallest cutoff wins across repeated truncates.
    pub fn note_truncate(&mut self, t: RelTag, nblocks: u32) {
        let e = self.dirty_truncs.entry(t).or_insert(nblocks);
        *e = (*e).min(nblocks);
    }

    /// The manifest this reader attached to is a branch, so a block or
    /// size the chain declines has no sound pg/ fallback for inherited
    /// state, only the tenant's own divergent writes live there.
    pub fn branched(&self) -> bool {
        self.branched
    }

    /// The fork's block count as the chain knows it: the newest index
    /// with an s line for the fork answers, an r line in a newer index
    /// masks older answers the way it masks pages, and a t line
    /// answers for the main fork directly, the truncate record named
    /// the exact surviving count. `None` means the chain never heard
    /// of the fork or an event killed every older answer.
    ///
    /// The caller consults its own SIZE object first, an own write of
    /// extend, truncate, or create always lands there eagerly and is
    /// newer than any fold. So this only answers for forks untouched
    /// since the index that names them, where the folded size is still
    /// current, which is exactly the inherited state a branch cannot
    /// find under its own prefix. No freshness barrier runs here: a
    /// size can only move through an smgr call that writes the own
    /// SIZE first.
    pub fn fork_size(&self, spc: u32, db: u32, rel: u32, fork: u32) -> Option<u32> {
        if self.poisoned {
            return None;
        }
        let tag = RelTag { spc, db, rel };
        for chk in &self.chain {
            if let Some(n) = chk.sizes.get(&(spc, db, rel, fork)) {
                return Some(*n);
            }
            if let Some(cut) = chk.truncs.get(&tag) {
                match (fork, *cut) {
                    // A vm or fsm only truncate, the main fork length
                    // is whatever an older index says.
                    (0, u32::MAX) => {}
                    (0, cut) => return Some(cut),
                    _ => return None,
                }
            }
            if chk.rels.contains(&tag) {
                return None;
            }
        }
        None
    }

    /// Serve one page from the chain, or `None` when pg/ must answer:
    /// the block is not in the chain, something dirtied it, or the
    /// reader is poisoned. `durable` is the durable LSN published by
    /// the wal pusher at the moment the read began, zero when no
    /// pusher has published one; an unchanged nonzero value since the
    /// last barrier means the stream cannot hold anything new and the
    /// LIST is skipped.
    pub fn read(&mut self, store: &dyn CasStore, r: BlockRef, durable: u64) -> Option<Vec<u8>> {
        if self.poisoned {
            return None;
        }
        let tag = RelTag {
            spc: r.spc,
            db: r.db,
            rel: r.rel,
        };
        let mut hit = None;
        for chk in &self.chain {
            if let Some((run, off)) = chk.lookup(&r) {
                hit = Some((chk.layout.clone(), chk.id.clone(), run, off));
                break;
            }
            if let Some(cut) = chk.truncs.get(&tag) {
                // A truncate in this window: main fork blocks below
                // the cutoff survived with their bytes and older
                // copies still serve, everything else is dead.
                if r.fork != 0 || r.blk >= *cut {
                    break;
                }
            }
            if chk.rels.contains(&tag) {
                // The relation's file was recreated in this window,
                // older copies of it are stale.
                break;
            }
        }
        let (lay, id, run, off) = hit?;
        if durable == 0 || durable > self.scanned_durable {
            if let Err(e) = self.barrier() {
                self.poison(&e);
                return None;
            }
            self.scanned_durable = self.scanned_durable.max(durable);
        }
        if self.dirty.contains(&r) || self.dirty_rels.contains(&tag) {
            return None;
        }
        if let Some(cut) = self.dirty_truncs.get(&tag)
            && (r.fork != 0 || r.blk >= *cut)
        {
            return None;
        }
        match self.fetch(store, &lay, &id, run, off) {
            Ok(page) => Some(page),
            Err(e) => {
                self.poison(&e);
                None
            }
        }
    }

    /// The freshness barrier: probe the fenced chain forward from the
    /// cursor and feed this tenant's frames into the tail scan. Every
    /// durable append lands on the chain before its pages can mutate,
    /// so the probe sees any WAL covering a change to the block being
    /// served, see the module docs. Frames of other tenants and zombie
    /// writers are skipped by tenant id; a rejected zombie frame never
    /// lands at all, admission happens before the PUT.
    fn barrier(&mut self) -> Result<(), String> {
        loop {
            match self.media.fetch(WAL_SHARD, self.next_seq) {
                Ok(Some(bytes)) => {
                    let (_, frames, _) = decode_segment(&bytes)
                        .map_err(|e| format!("landing segment {}: {e}", self.next_seq))?;
                    for frame in frames {
                        if frame.tenant == self.tenant {
                            self.tail
                                .append(frame.start_lsn.0, &frame.payload, self.floor)?;
                        }
                    }
                    self.next_seq += 1;
                }
                Ok(None) => {
                    // Either the head, or the segment was consolidated
                    // and collected while this reader slept. The shard
                    // manifest tells which: a consolidated boundary at
                    // or past the cursor means the missing frames are
                    // in the sealed rounds, catch up from there and
                    // jump the cursor over the collected prefix.
                    let store = self.media.manifest_store();
                    let boundary = match ShardManifest::load(store.as_ref(), WAL_SHARD) {
                        Ok(Some((m, _))) => m.consolidated_upto,
                        Ok(None) => break,
                        Err(e) => return Err(format!("shard manifest: {e}")),
                    };
                    if boundary < self.next_seq {
                        break;
                    }
                    let applied = self.tail.end().unwrap_or(self.floor);
                    let frames = catch_up(
                        &self.media,
                        WAL_SHARD,
                        &TeeFilter::Tenant(self.tenant),
                        Lsn(applied),
                    )
                    .map_err(|e| format!("catch up: {e}"))?;
                    for frame in &frames {
                        self.tail
                            .append(frame.start_lsn.0, &frame.payload, self.floor)?;
                    }
                    self.next_seq = boundary + 1;
                }
                Err(e) => return Err(format!("store: {e}")),
            }
        }
        self.tail.scan(
            &mut self.dirty,
            &mut self.dirty_rels,
            &mut self.dirty_truncs,
        )
    }

    /// Range read the page out of its run object through the slab
    /// cache, one aligned readahead slab per range request.
    fn fetch(
        &mut self,
        store: &dyn CasStore,
        layout: &TenantLayout,
        id: &str,
        run: u32,
        off: u64,
    ) -> Result<Vec<u8>, String> {
        let offset = off / SLAB_BYTES * SLAB_BYTES;
        // The tenant prefix is part of the key: two branches can each
        // hold a checkpoint named like the other's while the objects
        // differ, only (owner, id) is content addressed. Flattened,
        // the disk tier uses the key as a file name.
        let owner = layout.prefix().replace('/', "_");
        let cache_key = format!("{owner}-{id}-{run:08}-{offset:016X}");
        let slab = self.cache.get_or_load(&cache_key, || {
            let key = layout.checkpoint_pages(id, run);
            store
                .get_range(&key, offset, SLAB_BYTES)
                .map_err(|e| format!("store: {e}"))?
                .ok_or_else(|| format!("run object {key} is missing"))
        })?;
        let a = (off - offset) as usize;
        if a + ZOU_PAGE_SIZE > slab.len() {
            return Err(format!(
                "run object for {id} run {run} is shorter than its index"
            ));
        }
        Ok(slab[a..a + ZOU_PAGE_SIZE].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::restore::WAL_SEGMENT_SIZE;
    use crate::testv2::V2Wal;
    use crate::walscan::testwal::Builder;
    use std::sync::Arc;
    use zou_store::manifest::CheckpointRef;
    use zou_store::{LocalFsStore, Lsn};

    fn blk(rel: u32, blk: u32) -> BlockRef {
        BlockRef {
            spc: 1663,
            db: 5,
            rel,
            fork: 0,
            blk,
        }
    }

    fn tag(rel: u32) -> RelTag {
        RelTag {
            spc: 1663,
            db: 5,
            rel,
        }
    }

    /// Write one checkpoint's PAGES index and a single run object with
    /// each page filled by its marker byte.
    fn put_chk(
        store: &dyn CasStore,
        layout: &TenantLayout,
        id: &str,
        pages: &[(BlockRef, u8)],
        rels: &[RelTag],
    ) {
        let mut index = String::from("runs 1024\n");
        let mut run = Vec::new();
        for (r, fill) in pages {
            index.push_str(&format!(
                "p {} {} {} {} {}\n",
                r.spc, r.db, r.rel, r.fork, r.blk
            ));
            run.extend_from_slice(&[*fill; ZOU_PAGE_SIZE]);
        }
        for t in rels {
            index.push_str(&format!("r {} {} {}\n", t.spc, t.db, t.rel));
        }
        if !run.is_empty() {
            store
                .put_if_absent(&layout.checkpoint_pages(id, 0), &run)
                .unwrap();
        }
        store
            .put_if_absent(&layout.checkpoint_page_index(id), index.as_bytes())
            .unwrap();
    }

    fn put_manifest(
        store: &dyn CasStore,
        layout: &TenantLayout,
        chks: &[(&str, u64, CheckpointKind)],
    ) {
        let mut m = Manifest::new("local", 18);
        for (id, lsn, kind) in chks {
            m.checkpoints.push(CheckpointRef {
                id: (*id).to_string(),
                lsn: Lsn(*lsn),
                kind: *kind,
                owner: None,
            });
        }
        store
            .put_if_absent(&layout.manifest(), &m.to_json())
            .unwrap();
    }

    fn setup() -> (tempfile::TempDir, Arc<LocalFsStore>, TenantLayout) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFsStore::new(dir.path()));
        (dir, store, TenantLayout::new("local"))
    }

    /// The store as the trait object attach wants.
    fn dyn_store(store: &Arc<LocalFsStore>) -> Arc<dyn CasStore> {
        Arc::clone(store) as Arc<dyn CasStore>
    }

    #[test]
    fn chain_hits_serve_run_images_and_misses_fall_back() {
        let (_d, store, layout) = setup();
        put_chk(
            &*store,
            &layout,
            "f1",
            &[(blk(16384, 0), 0xAA), (blk(16384, 1), 0xBB)],
            &[],
        );
        put_manifest(&*store, &layout, &[("f1", 0x100, CheckpointKind::Full)]);

        let mut rd = ChainReader::attach(&dyn_store(&store), &layout)
            .unwrap()
            .unwrap();
        let page = rd.read(&*store, blk(16384, 1), 0).unwrap();
        assert_eq!(page.len(), ZOU_PAGE_SIZE);
        assert!(page.iter().all(|b| *b == 0xBB));
        assert!(rd.read(&*store, blk(16384, 7), 0).is_none());
        assert!(rd.read(&*store, blk(999, 0), 0).is_none());
        assert!(!rd.poisoned());
    }

    #[test]
    fn the_newest_index_wins_the_chain_walk() {
        let (_d, store, layout) = setup();
        put_chk(
            &*store,
            &layout,
            "f1",
            &[(blk(16384, 0), 0xAA), (blk(16384, 1), 0xBB)],
            &[],
        );
        put_chk(&*store, &layout, "d2", &[(blk(16384, 0), 0xCC)], &[]);
        put_manifest(
            &*store,
            &layout,
            &[
                ("f1", 0x100, CheckpointKind::Full),
                ("d2", 0x200, CheckpointKind::Delta),
            ],
        );

        let mut rd = ChainReader::attach(&dyn_store(&store), &layout)
            .unwrap()
            .unwrap();
        let newest = rd.read(&*store, blk(16384, 0), 0).unwrap();
        assert!(newest.iter().all(|b| *b == 0xCC));
        let older = rd.read(&*store, blk(16384, 1), 0).unwrap();
        assert!(older.iter().all(|b| *b == 0xBB));
    }

    #[test]
    fn a_rel_event_in_a_newer_delta_masks_older_copies() {
        let (_d, store, layout) = setup();
        put_chk(
            &*store,
            &layout,
            "f1",
            &[(blk(16384, 0), 0xAA), (blk(20000, 0), 0xA0)],
            &[],
        );
        put_chk(
            &*store,
            &layout,
            "d2",
            &[(blk(20000, 1), 0xC1)],
            &[tag(20000)],
        );
        put_manifest(
            &*store,
            &layout,
            &[
                ("f1", 0x100, CheckpointKind::Full),
                ("d2", 0x200, CheckpointKind::Delta),
            ],
        );

        let mut rd = ChainReader::attach(&dyn_store(&store), &layout)
            .unwrap()
            .unwrap();
        // The truncated relation's own delta pages still serve, images
        // packed after the event are current.
        let own = rd.read(&*store, blk(20000, 1), 0).unwrap();
        assert!(own.iter().all(|b| *b == 0xC1));
        // Older copies of it are dead, the untouched relation is fine.
        assert!(rd.read(&*store, blk(20000, 0), 0).is_none());
        assert!(rd.read(&*store, blk(16384, 0), 0).is_some());
    }

    #[test]
    fn a_truncate_event_in_a_newer_delta_spares_blocks_below_its_cutoff() {
        let (_d, store, layout) = setup();
        put_chk(
            &*store,
            &layout,
            "f1",
            &[
                (blk(20000, 0), 0xA0),
                (blk(20000, 1), 0xA1),
                (blk(20000, 5), 0xA5),
                (
                    BlockRef {
                        spc: 1663,
                        db: 5,
                        rel: 20000,
                        fork: 1,
                        blk: 0,
                    },
                    0xF5,
                ),
            ],
            &[],
        );
        let delta = "runs 1024\nt 1663 5 20000 2\n";
        store
            .put_if_absent(&layout.checkpoint_page_index("d2"), delta.as_bytes())
            .unwrap();
        put_manifest(
            &*store,
            &layout,
            &[
                ("f1", 0x100, CheckpointKind::Full),
                ("d2", 0x200, CheckpointKind::Delta),
            ],
        );

        let mut rd = ChainReader::attach(&dyn_store(&store), &layout)
            .unwrap()
            .unwrap();
        // Blocks below the cutoff survived the truncate byte for byte.
        let page = rd.read(&*store, blk(20000, 1), 0).unwrap();
        assert!(page.iter().all(|b| *b == 0xA1));
        // The truncated block and the fsm fork are dead.
        assert!(rd.read(&*store, blk(20000, 5), 0).is_none());
        let fsm = BlockRef {
            spc: 1663,
            db: 5,
            rel: 20000,
            fork: 1,
            blk: 0,
        };
        assert!(rd.read(&*store, fsm, 0).is_none());
        assert!(!rd.poisoned());
    }

    #[test]
    fn tail_wal_dirties_blocks_and_relations() {
        let (_d, store, layout) = setup();
        let floor = WAL_SEGMENT_SIZE;
        put_chk(
            &*store,
            &layout,
            "f1",
            &[
                (blk(16384, 0), 0xAA),
                (blk(16384, 1), 0xBB),
                (blk(30000, 0), 0xDD),
                (blk(30000, 1), 0xDE),
            ],
            &[],
        );
        put_manifest(&*store, &layout, &[("f1", floor, CheckpointKind::Full)]);

        let mut wal2 = V2Wal::open(dyn_store(&store), "local", 1);
        let mut wal = Builder::new(floor);
        wal.record(&[(blk(16384, 0), false)], b"dirty block zero");
        // A truncate of relation 30000 down to one block: the survivor
        // keeps serving its chain image, the truncated block must not.
        let mut trunc = Vec::new();
        trunc.extend_from_slice(&1u32.to_le_bytes());
        trunc.extend_from_slice(&1663u32.to_le_bytes());
        trunc.extend_from_slice(&5u32.to_le_bytes());
        trunc.extend_from_slice(&30000u32.to_le_bytes());
        trunc.extend_from_slice(&7u32.to_le_bytes());
        wal.record_with(&[], &trunc, 0x20, 2);
        let (lsn, bytes) = wal.stream();
        wal2.push(lsn, bytes);

        let mut rd = ChainReader::attach(&dyn_store(&store), &layout)
            .unwrap()
            .unwrap();
        assert!(rd.read(&*store, blk(16384, 0), 0).is_none());
        assert!(rd.read(&*store, blk(30000, 1), 0).is_none());
        let survivor = rd.read(&*store, blk(30000, 0), 0).unwrap();
        assert!(survivor.iter().all(|b| *b == 0xDD));
        let clean = rd.read(&*store, blk(16384, 1), 0).unwrap();
        assert!(clean.iter().all(|b| *b == 0xBB));
        assert!(!rd.poisoned());
        wal2.close();
    }

    #[test]
    fn a_partial_trailing_record_serves_until_it_completes() {
        let (_d, store, layout) = setup();
        let floor = WAL_SEGMENT_SIZE;
        put_chk(&*store, &layout, "f1", &[(blk(16384, 0), 0xAA)], &[]);
        put_manifest(&*store, &layout, &[("f1", floor, CheckpointKind::Full)]);

        let mut wal2 = V2Wal::open(dyn_store(&store), "local", 1);
        let mut wal = Builder::new(floor);
        wal.record(&[(blk(16384, 0), false)], b"not yet whole");
        let (lsn, bytes) = wal.stream();
        let cut = bytes.len() - 10;
        wal2.push(lsn, &bytes[..cut]);

        // The record is only partially in the log: the write gate says
        // its pages cannot be in the store yet, so serving the chain
        // image is still correct and nothing poisons.
        let mut rd = ChainReader::attach(&dyn_store(&store), &layout)
            .unwrap()
            .unwrap();
        let page = rd.read(&*store, blk(16384, 0), 0).unwrap();
        assert!(page.iter().all(|b| *b == 0xAA));
        assert!(!rd.poisoned());

        // Once the rest arrives the record completes and dirties the
        // block, the resumed scan picks up exactly where it stopped.
        wal2.push(lsn + cut as u64, &bytes[cut..]);
        assert!(rd.read(&*store, blk(16384, 0), 0).is_none());
        assert!(!rd.poisoned());
        wal2.close();
    }

    #[test]
    fn local_notes_force_pg_fallback() {
        let (_d, store, layout) = setup();
        put_chk(
            &*store,
            &layout,
            "f1",
            &[(blk(16384, 0), 0xAA), (blk(20000, 0), 0xDD)],
            &[],
        );
        put_manifest(&*store, &layout, &[("f1", 0x100, CheckpointKind::Full)]);

        let mut rd = ChainReader::attach(&dyn_store(&store), &layout)
            .unwrap()
            .unwrap();
        rd.note_write(blk(16384, 0));
        rd.note_rel(tag(20000));
        assert!(rd.read(&*store, blk(16384, 0), 0).is_none());
        assert!(rd.read(&*store, blk(20000, 0), 0).is_none());
    }

    #[test]
    fn an_indexless_checkpoint_newer_than_a_bearing_one_refuses_attach() {
        let (_d, store, layout) = setup();
        put_chk(&*store, &layout, "f1", &[(blk(16384, 0), 0xAA)], &[]);
        put_manifest(
            &*store,
            &layout,
            &[
                ("f1", 0x100, CheckpointKind::Full),
                ("d2", 0x200, CheckpointKind::Delta),
            ],
        );
        let err = match ChainReader::attach(&dyn_store(&store), &layout) {
            Err(e) => e,
            Ok(_) => panic!("attach must refuse the gap"),
        };
        assert!(err.why.contains("newer one does not"));
        assert!(!err.fatal, "pg/ still serves an unbranched tenant");
    }

    #[test]
    fn an_indexless_genesis_below_the_bearing_suffix_is_fine() {
        let (_d, store, layout) = setup();
        put_chk(&*store, &layout, "d1", &[(blk(16384, 0), 0xAB)], &[]);
        put_manifest(
            &*store,
            &layout,
            &[
                ("genesis", 0x100, CheckpointKind::Full),
                ("d1", 0x200, CheckpointKind::Delta),
            ],
        );
        let mut rd = ChainReader::attach(&dyn_store(&store), &layout)
            .unwrap()
            .unwrap();
        let page = rd.read(&*store, blk(16384, 0), 0).unwrap();
        assert!(page.iter().all(|b| *b == 0xAB));
    }

    #[test]
    fn fork_sizes_walk_the_chain_and_rel_events_mask_older_ones() {
        let (_d, store, layout) = setup();
        let full = "runs 1024\ns 1663 5 16384 0 4\ns 1663 5 20000 0 7\n";
        store
            .put_if_absent(&layout.checkpoint_page_index("f1"), full.as_bytes())
            .unwrap();
        let delta = "runs 1024\nr 1663 5 20000\ns 1663 5 16384 0 6\n";
        store
            .put_if_absent(&layout.checkpoint_page_index("d2"), delta.as_bytes())
            .unwrap();
        put_manifest(
            &*store,
            &layout,
            &[
                ("f1", 0x100, CheckpointKind::Full),
                ("d2", 0x200, CheckpointKind::Delta),
            ],
        );

        let rd = ChainReader::attach(&dyn_store(&store), &layout)
            .unwrap()
            .unwrap();
        assert!(!rd.branched());
        assert_eq!(rd.fork_size(1663, 5, 16384, 0), Some(6), "newest s wins");
        assert_eq!(
            rd.fork_size(1663, 5, 20000, 0),
            None,
            "the rel event in d2 masks the size the full recorded"
        );
        assert_eq!(rd.fork_size(1663, 5, 99, 0), None);
    }

    #[test]
    fn a_truncate_event_answers_the_main_fork_size_and_masks_the_rest() {
        let (_d, store, layout) = setup();
        let full = "runs 1024\ns 1663 5 16384 0 9\ns 1663 5 16384 1 3\ns 1663 5 20000 0 4\n";
        store
            .put_if_absent(&layout.checkpoint_page_index("f1"), full.as_bytes())
            .unwrap();
        let delta = format!("runs 1024\nt 1663 5 16384 2\nt 1663 5 20000 {}\n", u32::MAX);
        store
            .put_if_absent(&layout.checkpoint_page_index("d2"), delta.as_bytes())
            .unwrap();
        put_manifest(
            &*store,
            &layout,
            &[
                ("f1", 0x100, CheckpointKind::Full),
                ("d2", 0x200, CheckpointKind::Delta),
            ],
        );

        let rd = ChainReader::attach(&dyn_store(&store), &layout)
            .unwrap()
            .unwrap();
        assert_eq!(
            rd.fork_size(1663, 5, 16384, 0),
            Some(2),
            "the truncate record names the exact surviving count"
        );
        assert_eq!(
            rd.fork_size(1663, 5, 16384, 1),
            None,
            "the fsm fork length from before the truncate is dead"
        );
        assert_eq!(
            rd.fork_size(1663, 5, 20000, 0),
            Some(4),
            "a vm only truncate leaves the main fork answer alone"
        );
    }

    #[test]
    fn a_branch_without_a_run_bearing_chain_refuses_fatally() {
        let (_d, store, layout) = setup();
        put_manifest(
            &*store,
            &layout,
            &[("genesis", 0x100, CheckpointKind::Full)],
        );
        zou_store::branch(&*store, "local", "child", None, 5000).unwrap();
        let err = match ChainReader::attach(&dyn_store(&store), &TenantLayout::new("child")) {
            Err(e) => e,
            Ok(_) => panic!("a branch with nothing to serve inherited pages from must refuse"),
        };
        assert!(
            err.fatal,
            "pg/ fallback would read zeros for inherited pages"
        );
        assert!(err.why.contains("run bearing"));
    }

    /// [`why_unservable`] is asked before a branch exists and attach is
    /// asked when one is being served, so the two have to agree about
    /// every shape a chain can take, or a branch passes the check and
    /// then fails every read. Each case builds the source, branches it,
    /// and holds the two answers against each other.
    #[test]
    fn the_pre_branch_check_agrees_with_what_attach_would_do() {
        let bearing = |store: &LocalFsStore, layout: &TenantLayout, id: &str| {
            put_chk(store, layout, id, &[(blk(16384, 0), 0xAA)], &[]);
        };
        /// A name, the checkpoints the source publishes, and which of
        /// them bear a page run index.
        type Case = (&'static str, &'static [Chk], &'static [&'static str]);
        type Chk = (&'static str, u64, CheckpointKind);
        let cases: [Case; 4] = [
            // A project nobody has written enough into yet: the
            // bootstrap capture is full but carries files, not runs.
            (
                "genesis alone",
                &[("genesis", 0x100, CheckpointKind::Full)],
                &[],
            ),
            // The deltas above it bear runs, but blocks only genesis
            // covers live under the parent's mutable prefix.
            (
                "genesis under a bearing delta",
                &[
                    ("genesis", 0x100, CheckpointKind::Full),
                    ("d1", 0x200, CheckpointKind::Delta),
                ],
                &["d1"],
            ),
            // A folded full with a delta on top, which is what a
            // database that has been running for a while looks like.
            (
                "a folded full and a delta",
                &[
                    ("f1", 0x100, CheckpointKind::Full),
                    ("d2", 0x200, CheckpointKind::Delta),
                ],
                &["f1", "d2"],
            ),
            // A hole at the top: the newest window's dirtied blocks are
            // in no index and the WAL under it is gone.
            (
                "a newest checkpoint with no index",
                &[
                    ("f1", 0x100, CheckpointKind::Full),
                    ("d2", 0x200, CheckpointKind::Delta),
                ],
                &["f1"],
            ),
        ];
        for (name, chks, indexed) in cases {
            let (_d, store, layout) = setup();
            for id in indexed {
                bearing(&store, &layout, id);
            }
            put_manifest(&*store, &layout, chks);
            zou_store::branch(&*store, "local", "child", None, 5000).unwrap();
            let child = TenantLayout::new("child");
            let (data, _) = store.get(&child.manifest()).unwrap().unwrap();
            let manifest = Manifest::from_json(&data).unwrap();

            let ahead = why_unservable(&*store, &child, &manifest).unwrap();
            let served = ChainReader::attach(&dyn_store(&store), &child);
            match (&ahead, &served) {
                (Some(why), Err(e)) => assert_eq!(why, &e.why, "{name}"),
                (None, Ok(Some(_))) => {}
                _ => panic!(
                    "{name}: checked {ahead:?} but attach said {:?}",
                    served.err()
                ),
            }
        }
    }

    #[test]
    fn nothing_to_serve_attaches_as_none() {
        let (_d, store, layout) = setup();
        assert!(
            ChainReader::attach(&dyn_store(&store), &layout)
                .unwrap()
                .is_none()
        );
        put_manifest(
            &*store,
            &layout,
            &[("genesis", 0x100, CheckpointKind::Full)],
        );
        assert!(
            ChainReader::attach(&dyn_store(&store), &layout)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn run_and_offset_math_crosses_run_boundaries() {
        let (_d, store, layout) = setup();
        let mut index = String::from("runs 2\n");
        let mut run0 = Vec::new();
        for (i, fill) in [(0u32, 0x11u8), (1, 0x22)] {
            index.push_str(&format!("p 1663 5 16384 0 {i}\n"));
            run0.extend_from_slice(&[fill; ZOU_PAGE_SIZE]);
        }
        index.push_str("p 1663 5 16384 0 2\n");
        store
            .put_if_absent(&layout.checkpoint_pages("f1", 0), &run0)
            .unwrap();
        store
            .put_if_absent(&layout.checkpoint_pages("f1", 1), &[0x33; ZOU_PAGE_SIZE])
            .unwrap();
        store
            .put_if_absent(&layout.checkpoint_page_index("f1"), index.as_bytes())
            .unwrap();
        put_manifest(&*store, &layout, &[("f1", 0x100, CheckpointKind::Full)]);

        let mut rd = ChainReader::attach(&dyn_store(&store), &layout)
            .unwrap()
            .unwrap();
        for (i, fill) in [(0u32, 0x11u8), (1, 0x22), (2, 0x33)] {
            let page = rd.read(&*store, blk(16384, i), 0).unwrap();
            assert!(page.iter().all(|b| *b == fill), "block {i}");
        }
    }

    #[test]
    fn a_branch_child_serves_inherited_runs_and_ignores_the_parent_stream() {
        let (_d, store, layout) = setup();
        let floor = WAL_SEGMENT_SIZE;
        put_chk(
            &*store,
            &layout,
            "f1",
            &[(blk(16384, 0), 0xAA), (blk(16384, 1), 0xBB)],
            &[],
        );
        put_manifest(&*store, &layout, &[("f1", floor, CheckpointKind::Full)]);

        // The parent appends WAL dirtying block 0 after the checkpoint,
        // then the branch is taken. The cut is the checkpoint lsn, so
        // the parent's unfolded stream is after the branch point and
        // must not leak into the child.
        let mut wal2 = V2Wal::open(dyn_store(&store), "local", 1);
        let mut wal = Builder::new(floor);
        wal.record(&[(blk(16384, 0), false)], b"parent change after the cut");
        let (lsn, bytes) = wal.stream();
        wal2.push(lsn, bytes);
        wal2.close();
        zou_store::branch(&*store, "local", "child", None, 5000).unwrap();

        let child = TenantLayout::new("child");
        let mut rd = ChainReader::attach(&dyn_store(&store), &child)
            .unwrap()
            .unwrap();
        assert!(rd.branched());
        // Both blocks serve out of the parent's run objects, the child
        // prefix holds no chk/ at all. Block 0 serves its checkpoint
        // image too: the parent's later WAL belongs to the parent's
        // stream, the child's own stream is empty.
        let page = rd.read(&*store, blk(16384, 1), 0).unwrap();
        assert!(page.iter().all(|b| *b == 0xBB));
        let page = rd.read(&*store, blk(16384, 0), 0).unwrap();
        assert!(page.iter().all(|b| *b == 0xAA));
        assert!(!rd.poisoned());
    }

    /// Delegating store that counts GET, LIST, and ranged GET calls,
    /// so the tests can see the barrier skip, the slab cache, and the
    /// read amplification bound at work.
    struct CountingStore {
        inner: Arc<LocalFsStore>,
        gets: std::sync::atomic::AtomicU64,
        lists: std::sync::atomic::AtomicU64,
        ranges: std::sync::atomic::AtomicU64,
    }

    impl CountingStore {
        fn new(inner: Arc<LocalFsStore>) -> Self {
            Self {
                inner,
                gets: 0.into(),
                lists: 0.into(),
                ranges: 0.into(),
            }
        }

        fn gets(&self) -> u64 {
            self.gets.load(std::sync::atomic::Ordering::Relaxed)
        }

        fn lists(&self) -> u64 {
            self.lists.load(std::sync::atomic::Ordering::Relaxed)
        }

        fn ranges(&self) -> u64 {
            self.ranges.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl CasStore for CountingStore {
        fn get(
            &self,
            key: &str,
        ) -> Result<Option<(Vec<u8>, zou_store::Version)>, zou_store::CasError> {
            self.gets.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.get(key)
        }

        fn put_if_match(
            &self,
            key: &str,
            data: &[u8],
            expected: Option<&zou_store::Version>,
        ) -> Result<zou_store::Version, zou_store::CasError> {
            self.inner.put_if_match(key, data, expected)
        }

        fn put(&self, key: &str, data: &[u8]) -> Result<zou_store::Version, zou_store::CasError> {
            self.inner.put(key, data)
        }

        fn get_range(
            &self,
            key: &str,
            offset: u64,
            len: u64,
        ) -> Result<Option<Vec<u8>>, zou_store::CasError> {
            self.ranges
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.get_range(key, offset, len)
        }

        fn delete(&self, key: &str) -> Result<(), zou_store::CasError> {
            self.inner.delete(key)
        }

        fn list(&self, prefix: &str) -> Result<Vec<String>, zou_store::CasError> {
            self.lists
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.list(prefix)
        }
    }

    #[test]
    fn a_stable_durable_lsn_skips_the_chain_probes() {
        let (_d, store, layout) = setup();
        put_chk(
            &*store,
            &layout,
            "f1",
            &[(blk(16384, 0), 0xAA), (blk(16384, 1), 0xBB)],
            &[],
        );
        put_manifest(&*store, &layout, &[("f1", 0x100, CheckpointKind::Full)]);
        let counting = Arc::new(CountingStore::new(Arc::clone(&store)));
        let sd = Arc::clone(&counting) as Arc<dyn CasStore>;

        let mut rd = ChainReader::attach(&sd, &layout).unwrap().unwrap();
        let attached = counting.gets();
        // The first read always probes: one landing fetch finds the
        // head, one shard manifest read confirms nothing was collected
        // behind the cursor.
        rd.read(&*counting, blk(16384, 0), 0x500).unwrap();
        assert_eq!(counting.gets(), attached + 2);
        // The published LSN has not moved, no reason to probe again.
        rd.read(&*counting, blk(16384, 1), 0x500).unwrap();
        rd.read(&*counting, blk(16384, 0), 0x500).unwrap();
        assert_eq!(counting.gets(), attached + 2);
        // Zero means nobody publishes, every read must probe.
        rd.read(&*counting, blk(16384, 0), 0).unwrap();
        assert_eq!(counting.gets(), attached + 4);
        // An advance means the stream may hold new WAL, probe once more.
        rd.read(&*counting, blk(16384, 0), 0x600).unwrap();
        assert_eq!(counting.gets(), attached + 6);
        rd.read(&*counting, blk(16384, 0), 0x600).unwrap();
        assert_eq!(counting.gets(), attached + 6);
        assert_eq!(counting.lists(), 0, "no LIST anywhere on the read path");
    }

    #[test]
    fn an_advanced_durable_lsn_still_sees_new_dirtying_wal() {
        let (_d, store, layout) = setup();
        let floor = WAL_SEGMENT_SIZE;
        put_chk(
            &*store,
            &layout,
            "f1",
            &[(blk(16384, 0), 0xAA), (blk(16384, 1), 0xBB)],
            &[],
        );
        put_manifest(&*store, &layout, &[("f1", floor, CheckpointKind::Full)]);
        let counting = Arc::new(CountingStore::new(Arc::clone(&store)));
        let sd = Arc::clone(&counting) as Arc<dyn CasStore>;

        let mut rd = ChainReader::attach(&sd, &layout).unwrap().unwrap();
        rd.read(&*counting, blk(16384, 0), floor).unwrap();

        // WAL arrives dirtying block 0 and the pusher publishes past
        // it, exactly what a concurrent writer looks like.
        let mut wal2 = V2Wal::open(dyn_store(&store), "local", 1);
        let mut wal = Builder::new(floor);
        wal.record(&[(blk(16384, 0), false)], b"concurrent change");
        let (lsn, bytes) = wal.stream();
        wal2.push(lsn, bytes);
        let advanced = floor + bytes.len() as u64;

        assert!(rd.read(&*counting, blk(16384, 0), advanced).is_none());
        let clean = rd.read(&*counting, blk(16384, 1), advanced).unwrap();
        assert!(clean.iter().all(|b| *b == 0xBB));
        assert!(!rd.poisoned());
        wal2.close();
    }

    #[test]
    fn neighboring_pages_share_one_ranged_read() {
        let (_d, store, layout) = setup();
        put_chk(
            &*store,
            &layout,
            "f1",
            &[(blk(16384, 0), 0xAA), (blk(16384, 1), 0xBB)],
            &[],
        );
        put_manifest(&*store, &layout, &[("f1", 0x100, CheckpointKind::Full)]);
        let counting = Arc::new(CountingStore::new(Arc::clone(&store)));
        let sd = Arc::clone(&counting) as Arc<dyn CasStore>;

        let mut rd = ChainReader::attach(&sd, &layout).unwrap().unwrap();
        rd.read(&*counting, blk(16384, 0), 0x500).unwrap();
        rd.read(&*counting, blk(16384, 1), 0x500).unwrap();
        rd.read(&*counting, blk(16384, 0), 0x500).unwrap();
        assert_eq!(counting.ranges(), 1, "one slab covers both pages");
    }

    #[test]
    fn a_page_read_touches_at_most_one_full_four_deltas_and_the_tail() {
        let (_d, store, layout) = setup();
        let floor = WAL_SEGMENT_SIZE;
        // The longest chain the fold down policy allows: one full and
        // four deltas. Block 0 lives only in the full, so serving it is
        // the worst case walk through every index. Each delta packs one
        // other block of the same relation.
        put_chk(
            &*store,
            &layout,
            "f1",
            &[(blk(16384, 0), 0xF1), (blk(16384, 1), 0xF2)],
            &[],
        );
        for (i, id) in ["d2", "d3", "d4", "d5"].iter().enumerate() {
            put_chk(
                &*store,
                &layout,
                id,
                &[(blk(16384, 10 + i as u32), 0xD2 + i as u8)],
                &[],
            );
        }
        put_manifest(
            &*store,
            &layout,
            &[
                ("f1", 0x100, CheckpointKind::Full),
                ("d2", 0x200, CheckpointKind::Delta),
                ("d3", 0x300, CheckpointKind::Delta),
                ("d4", 0x400, CheckpointKind::Delta),
                ("d5", floor, CheckpointKind::Delta),
            ],
        );
        let mut wal2 = V2Wal::open(dyn_store(&store), "local", 1);
        let mut wal = Builder::new(floor);
        wal.record(&[(blk(50000, 0), false)], b"tail noise");
        let (lsn, bytes) = wal.stream();
        wal2.push(lsn, bytes);
        wal2.close();
        let durable = floor + bytes.len() as u64;

        let counting = Arc::new(CountingStore::new(Arc::clone(&store)));
        let sd = Arc::clone(&counting) as Arc<dyn CasStore>;
        let mut rd = ChainReader::attach(&sd, &layout).unwrap().unwrap();
        assert_eq!(
            rd.chain.len(),
            5,
            "the whole walk is the full and four deltas"
        );
        let attached = counting.gets();

        // The worst case read misses all four deltas and lands in the
        // full: the probes walk the landing chain, here the takeover
        // seal and the frame window, then one head miss and one shard
        // manifest read, and the page itself is one ranged GET for the
        // run slab. Nothing else.
        let page = rd.read(&*counting, blk(16384, 0), durable).unwrap();
        assert!(page.iter().all(|b| *b == 0xF1));
        assert_eq!(
            counting.gets(),
            attached + 4,
            "the tail is two landing segments plus the probe overhead"
        );
        assert_eq!(counting.ranges(), 1);

        // Steady state: the durable LSN has not moved and the slab is
        // warm, so the neighboring block costs zero store operations.
        let page = rd.read(&*counting, blk(16384, 1), durable).unwrap();
        assert!(page.iter().all(|b| *b == 0xF2));
        assert_eq!(counting.gets(), attached + 4);
        assert_eq!(counting.ranges(), 1);

        // A delta hit stops the walk at its index and costs one ranged
        // GET for that run object.
        let page = rd.read(&*counting, blk(16384, 13), durable).unwrap();
        assert!(page.iter().all(|b| *b == 0xD5));
        assert_eq!(counting.gets(), attached + 4);
        assert_eq!(counting.ranges(), 2);
        assert_eq!(counting.lists(), 0, "no LIST anywhere on the read path");
        assert!(!rd.poisoned());
    }
}
