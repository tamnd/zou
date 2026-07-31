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
//! before the WAL covering it is durable in the mirrored stream, so
//! every change newer than the chain has a WAL object visible to a
//! LIST. Second, Postgres serializes eviction against reads through
//! the buffer mapping table: an evicting backend finishes its write
//! before another backend can start reading the same block, so by the
//! time this reader runs, the change's WAL upload has completed.
//! Third, the barrier below LISTs the stream before every run served
//! read, scans whatever it has not seen, and any block or relation the
//! new records touch goes into a dirty set that forces pg/ for good.
//! A block that is in the chain, not dirtied by any record at or above
//! the newest checkpoint's redo, and not written by this process, is
//! exactly its run image.
//!
//! Relation level staleness needs its own barrier: smgr truncate and
//! create records name a relation with no block references, and after
//! one of those the older checkpoint copies of the relation are stale
//! even for blocks no record names again. The fold persists those
//! events as r lines in the PAGES index, and the chain walk stops for
//! a relation at the first index naming it. Unlogged relations never
//! enter the runs at all, the fold skips them, because their writes
//! bypass the WAL and no barrier can see them.
//!
//! Any parse error, store error, or coverage gap poisons the reader,
//! which then declines every read and the smgr falls back to pg/.
//! The per read LIST is the v0 freshness barrier, the shared memory
//! durable LSN replaces it together with the cache tiers.

use std::collections::{BTreeMap, BTreeSet};

use zou_store::layout::TenantLayout;
use zou_store::manifest::{CheckpointKind, Manifest};
use zou_store::{CasStore, SegmentReader};

use crate::ZOU_PAGE_SIZE;
use crate::walscan::{self, BlockRef, RelTag, WalWindow};

/// Readahead unit for run objects. Neighboring pages of a sequential
/// scan land in the same slab, one range request instead of 128.
const SLAB_BYTES: u64 = 1 << 20;

/// The parsed PAGES index of one checkpoint.
struct ChkIndex {
    id: String,
    run_pages: usize,
    entries: Vec<BlockRef>,
    rels: BTreeSet<RelTag>,
}

impl ChkIndex {
    fn parse(id: &str, text: &str) -> Result<Self, String> {
        let mut lines = text.lines();
        let run_pages = lines
            .next()
            .and_then(|l| l.strip_prefix("runs "))
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .ok_or_else(|| format!("bad PAGES header in {id}"))?;
        let mut entries = Vec::new();
        let mut rels = BTreeSet::new();
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
            } else if !line.starts_with("s ") {
                return Err(format!("unknown PAGES line in {id}"));
            }
        }
        if !entries.is_sorted() {
            return Err(format!("PAGES entries out of order in {id}"));
        }
        Ok(Self {
            id: id.to_string(),
            run_pages,
            entries,
            rels,
        })
    }

    /// Which run object holds the block and at what byte offset.
    fn lookup(&self, r: &BlockRef) -> Option<(u32, u64)> {
        let i = self.entries.binary_search(r).ok()?;
        Some((
            (i / self.run_pages) as u32,
            ((i % self.run_pages) * ZOU_PAGE_SIZE) as u64,
        ))
    }
}

/// One epoch's mirrored stream, reassembled incrementally. The window
/// only ever holds bytes past the last complete record scanned, the
/// consumed prefix is dropped to keep memory bounded.
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
    /// held is clipped too, and a gap is an error, the stream within an
    /// epoch is contiguous.
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
            return Err(format!("wal gap in epoch stream at {have:#X}"));
        }
        self.window
            .buf
            .extend_from_slice(&bytes[(have - lsn) as usize..]);
        Ok(())
    }

    /// Scan the complete records the window now covers into the dirty
    /// sets and drop the consumed bytes. A record the window ends
    /// inside waits for the next append, the write gate keeps its pages
    /// out of the store until then.
    fn scan(
        &mut self,
        dirty: &mut BTreeSet<BlockRef>,
        rels: &mut BTreeSet<RelTag>,
    ) -> Result<(), String> {
        if !self.started || self.window.buf.is_empty() {
            return Ok(());
        }
        let out = walscan::scan_available(&self.window, self.window.base)?;
        dirty.extend(out.refs);
        rels.extend(out.rels);
        let consumed = (out.resume - self.window.base) as usize;
        self.window.buf.drain(..consumed);
        self.window.base = out.resume;
        self.window.covered_from = out.resume;
        Ok(())
    }
}

struct Slab {
    key: String,
    offset: u64,
    data: Vec<u8>,
}

pub struct ChainReader {
    /// Newest first, starting no earlier than the newest full capture.
    chain: Vec<ChkIndex>,
    /// The newest checkpoint's redo. Everything below it is inside the
    /// chain, the barrier only scans WAL at or above it.
    floor: u64,
    dirty: BTreeSet<BlockRef>,
    dirty_rels: BTreeSet<RelTag>,
    tails: BTreeMap<u64, TailScan>,
    seen: BTreeSet<String>,
    slab: Option<Slab>,
    poisoned: bool,
}

impl ChainReader {
    /// Build a reader from the current manifest. `Ok(None)` means there
    /// is nothing to serve, no manifest, no full capture, or no
    /// checkpoint with a page run index yet, and the caller stays on
    /// pg/. An index pattern that cannot be served soundly is an error:
    /// every checkpoint newer than the oldest index bearing one must
    /// have an index too, otherwise a window of dirtied blocks would be
    /// invisible to the chain walk.
    pub fn attach(store: &dyn CasStore, layout: &TenantLayout) -> Result<Option<Self>, String> {
        let Some((data, _)) = store
            .get(&layout.manifest())
            .map_err(|e| format!("store: {e}"))?
        else {
            return Ok(None);
        };
        let manifest = Manifest::from_json(&data).map_err(|e| format!("manifest: {e}"))?;
        let Some(full) = manifest
            .checkpoints
            .iter()
            .rposition(|c| c.kind == CheckpointKind::Full)
        else {
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
            match store
                .get(&layout.checkpoint_page_index(&c.id))
                .map_err(|e| format!("store: {e}"))?
            {
                Some((data, _)) => {
                    if gap {
                        return Err(format!(
                            "checkpoint {} has PAGES but a newer one does not",
                            c.id
                        ));
                    }
                    let text = String::from_utf8(data)
                        .map_err(|_| format!("PAGES for {} is not utf8", c.id))?;
                    chain.push(ChkIndex::parse(&c.id, &text)?);
                }
                None => gap = true,
            }
        }
        if chain.is_empty() {
            return Ok(None);
        }
        // The dirty floor: the newest checkpoint's redo, which by the
        // suffix rule is exactly where the newest index's window ends.
        // The stream holds everything from here on, folds only drop
        // WAL below it.
        let floor = manifest.checkpoints.last().expect("full exists").lsn.0;
        Ok(Some(Self {
            chain,
            floor,
            dirty: BTreeSet::new(),
            dirty_rels: BTreeSet::new(),
            tails: BTreeMap::new(),
            seen: BTreeSet::new(),
            slab: None,
            poisoned: false,
        }))
    }

    pub fn poisoned(&self) -> bool {
        self.poisoned
    }

    fn poison(&mut self, why: &str) {
        if !self.poisoned {
            eprintln!("zou chain reader poisoned, all reads fall back to pg/: {why}");
        }
        self.poisoned = true;
    }

    /// A page this process wrote is newer than any run image of it.
    pub fn note_write(&mut self, r: BlockRef) {
        self.dirty.insert(r);
    }

    /// A truncate, unlink, or create in this process invalidates every
    /// run image of the relation.
    pub fn note_rel(&mut self, t: RelTag) {
        self.dirty_rels.insert(t);
    }

    /// Serve one page from the chain, or `None` when pg/ must answer:
    /// the block is not in the chain, something dirtied it, or the
    /// reader is poisoned.
    pub fn read(
        &mut self,
        store: &dyn CasStore,
        layout: &TenantLayout,
        r: BlockRef,
    ) -> Option<Vec<u8>> {
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
                hit = Some((chk.id.clone(), run, off));
                break;
            }
            if chk.rels.contains(&tag) {
                // The relation was truncated or its file recreated in
                // this window, older copies of it are stale.
                break;
            }
        }
        let (id, run, off) = hit?;
        if let Err(e) = self.barrier(store, layout) {
            self.poison(&e);
            return None;
        }
        if self.dirty.contains(&r) || self.dirty_rels.contains(&tag) {
            return None;
        }
        match self.fetch(store, layout, &id, run, off) {
            Ok(page) => Some(page),
            Err(e) => {
                self.poison(&e);
                None
            }
        }
    }

    /// The freshness barrier: list the stream across every epoch, fetch
    /// and scan whatever is new. Any WAL object covering a change to
    /// the block being served must be visible to this LIST, see the
    /// module docs. Zombie epochs are scanned too, their dirtying is a
    /// false positive at worst and pg/ absorbs it.
    fn barrier(&mut self, store: &dyn CasStore, layout: &TenantLayout) -> Result<(), String> {
        let dir = layout.wal_dir();
        for key in store.list(&dir).map_err(|e| format!("store: {e}"))? {
            if self.seen.contains(&key) {
                continue;
            }
            let name = key
                .strip_prefix(&dir)
                .ok_or_else(|| format!("unexpected key {key} under the wal prefix"))?;
            let epoch = zou_store::commit::segment_epoch(name)
                .ok_or_else(|| format!("bad segment name {name:?}"))?;
            let (bytes, _) = store
                .get(&key)
                .map_err(|e| format!("store: {e}"))?
                .ok_or_else(|| format!("listed segment {key} vanished"))?;
            let tail = self.tails.entry(epoch).or_insert_with(TailScan::new);
            for frame in SegmentReader::new(&bytes, epoch) {
                let frame = frame.map_err(|e| format!("segment {name}: {e}"))?;
                let records = zou_store::commit::split_records(&frame.payload)
                    .ok_or_else(|| format!("bad batch in {name}"))?;
                for record in records {
                    if record.len() < 8 {
                        return Err(format!("short record in {name}"));
                    }
                    let lsn = u64::from_le_bytes(record[..8].try_into().expect("checked length"));
                    tail.append(lsn, &record[8..], self.floor)?;
                }
            }
            self.seen.insert(key);
        }
        for tail in self.tails.values_mut() {
            tail.scan(&mut self.dirty, &mut self.dirty_rels)?;
        }
        Ok(())
    }

    /// Range read the page out of its run object through a one slot
    /// readahead slab.
    fn fetch(
        &mut self,
        store: &dyn CasStore,
        layout: &TenantLayout,
        id: &str,
        run: u32,
        off: u64,
    ) -> Result<Vec<u8>, String> {
        let key = layout.checkpoint_pages(id, run);
        let page = ZOU_PAGE_SIZE as u64;
        if let Some(s) = &self.slab
            && s.key == key
            && off >= s.offset
            && off + page <= s.offset + s.data.len() as u64
        {
            let a = (off - s.offset) as usize;
            return Ok(s.data[a..a + ZOU_PAGE_SIZE].to_vec());
        }
        let offset = off / SLAB_BYTES * SLAB_BYTES;
        let data = store
            .get_range(&key, offset, SLAB_BYTES)
            .map_err(|e| format!("store: {e}"))?
            .ok_or_else(|| format!("run object {key} is missing"))?;
        if (off - offset) as usize + ZOU_PAGE_SIZE > data.len() {
            return Err(format!("run object {key} is shorter than its index"));
        }
        let a = (off - offset) as usize;
        let out = data[a..a + ZOU_PAGE_SIZE].to_vec();
        self.slab = Some(Slab { key, offset, data });
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::restore::WAL_SEGMENT_SIZE;
    use crate::walscan::testwal::Builder;
    use std::sync::{Arc, Mutex};
    use zou_store::manifest::CheckpointRef;
    use zou_store::{GroupCommit, GroupCommitConfig, LocalFsStore, Lsn, TailConfig, lease};

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
                .put_new(&layout.checkpoint_pages(id, 0), &run)
                .unwrap();
        }
        store
            .put_new(&layout.checkpoint_page_index(id), index.as_bytes())
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
            });
        }
        store.put_new(&layout.manifest(), &m.to_json()).unwrap();
    }

    fn setup() -> (tempfile::TempDir, Arc<LocalFsStore>, TenantLayout) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFsStore::new(dir.path()));
        (dir, store, TenantLayout::new("local"))
    }

    /// A group commit for pushing tail WAL the way the pusher does.
    fn commit(store: &Arc<LocalFsStore>, layout: &TenantLayout) -> GroupCommit {
        let held = lease::acquire(&**store, layout, "test", 15, 1000).unwrap();
        GroupCommit::with_lease(
            Arc::clone(store) as Arc<dyn CasStore>,
            layout.clone(),
            Arc::new(Mutex::new(held)),
            Lsn(0),
            GroupCommitConfig::default(),
            TailConfig::default(),
        )
    }

    fn push(gc: &GroupCommit, pg_lsn: u64, bytes: &[u8]) {
        let mut record = pg_lsn.to_le_bytes().to_vec();
        record.extend_from_slice(bytes);
        gc.append(&record).unwrap().wait().unwrap();
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

        let mut rd = ChainReader::attach(&*store, &layout).unwrap().unwrap();
        let page = rd.read(&*store, &layout, blk(16384, 1)).unwrap();
        assert_eq!(page.len(), ZOU_PAGE_SIZE);
        assert!(page.iter().all(|b| *b == 0xBB));
        assert!(rd.read(&*store, &layout, blk(16384, 7)).is_none());
        assert!(rd.read(&*store, &layout, blk(999, 0)).is_none());
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

        let mut rd = ChainReader::attach(&*store, &layout).unwrap().unwrap();
        let newest = rd.read(&*store, &layout, blk(16384, 0)).unwrap();
        assert!(newest.iter().all(|b| *b == 0xCC));
        let older = rd.read(&*store, &layout, blk(16384, 1)).unwrap();
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

        let mut rd = ChainReader::attach(&*store, &layout).unwrap().unwrap();
        // The truncated relation's own delta pages still serve, images
        // packed after the event are current.
        let own = rd.read(&*store, &layout, blk(20000, 1)).unwrap();
        assert!(own.iter().all(|b| *b == 0xC1));
        // Older copies of it are dead, the untouched relation is fine.
        assert!(rd.read(&*store, &layout, blk(20000, 0)).is_none());
        assert!(rd.read(&*store, &layout, blk(16384, 0)).is_some());
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
            ],
            &[],
        );
        put_manifest(&*store, &layout, &[("f1", floor, CheckpointKind::Full)]);

        let gc = commit(&store, &layout);
        let mut wal = Builder::new(floor);
        wal.record(&[(blk(16384, 0), false)], b"dirty block zero");
        let mut trunc = Vec::new();
        trunc.extend_from_slice(&1u32.to_le_bytes());
        trunc.extend_from_slice(&1663u32.to_le_bytes());
        trunc.extend_from_slice(&5u32.to_le_bytes());
        trunc.extend_from_slice(&30000u32.to_le_bytes());
        trunc.extend_from_slice(&7u32.to_le_bytes());
        wal.record_with(&[], &trunc, 0x20, 2);
        let (lsn, bytes) = wal.stream();
        push(&gc, lsn, bytes);

        let mut rd = ChainReader::attach(&*store, &layout).unwrap().unwrap();
        assert!(rd.read(&*store, &layout, blk(16384, 0)).is_none());
        assert!(rd.read(&*store, &layout, blk(30000, 0)).is_none());
        let clean = rd.read(&*store, &layout, blk(16384, 1)).unwrap();
        assert!(clean.iter().all(|b| *b == 0xBB));
        assert!(!rd.poisoned());
        gc.close().unwrap();
    }

    #[test]
    fn a_partial_trailing_record_serves_until_it_completes() {
        let (_d, store, layout) = setup();
        let floor = WAL_SEGMENT_SIZE;
        put_chk(&*store, &layout, "f1", &[(blk(16384, 0), 0xAA)], &[]);
        put_manifest(&*store, &layout, &[("f1", floor, CheckpointKind::Full)]);

        let gc = commit(&store, &layout);
        let mut wal = Builder::new(floor);
        wal.record(&[(blk(16384, 0), false)], b"not yet whole");
        let (lsn, bytes) = wal.stream();
        let cut = bytes.len() - 10;
        push(&gc, lsn, &bytes[..cut]);

        // The record is only partially mirrored: the write gate says
        // its pages cannot be in the store yet, so serving the chain
        // image is still correct and nothing poisons.
        let mut rd = ChainReader::attach(&*store, &layout).unwrap().unwrap();
        let page = rd.read(&*store, &layout, blk(16384, 0)).unwrap();
        assert!(page.iter().all(|b| *b == 0xAA));
        assert!(!rd.poisoned());

        // Once the rest arrives the record completes and dirties the
        // block, the resumed scan picks up exactly where it stopped.
        push(&gc, lsn + cut as u64, &bytes[cut..]);
        assert!(rd.read(&*store, &layout, blk(16384, 0)).is_none());
        assert!(!rd.poisoned());
        gc.close().unwrap();
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

        let mut rd = ChainReader::attach(&*store, &layout).unwrap().unwrap();
        rd.note_write(blk(16384, 0));
        rd.note_rel(tag(20000));
        assert!(rd.read(&*store, &layout, blk(16384, 0)).is_none());
        assert!(rd.read(&*store, &layout, blk(20000, 0)).is_none());
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
        let err = match ChainReader::attach(&*store, &layout) {
            Err(e) => e,
            Ok(_) => panic!("attach must refuse the gap"),
        };
        assert!(err.contains("newer one does not"));
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
        let mut rd = ChainReader::attach(&*store, &layout).unwrap().unwrap();
        let page = rd.read(&*store, &layout, blk(16384, 0)).unwrap();
        assert!(page.iter().all(|b| *b == 0xAB));
    }

    #[test]
    fn nothing_to_serve_attaches_as_none() {
        let (_d, store, layout) = setup();
        assert!(ChainReader::attach(&*store, &layout).unwrap().is_none());
        put_manifest(
            &*store,
            &layout,
            &[("genesis", 0x100, CheckpointKind::Full)],
        );
        assert!(ChainReader::attach(&*store, &layout).unwrap().is_none());
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
            .put_new(&layout.checkpoint_pages("f1", 0), &run0)
            .unwrap();
        store
            .put_new(&layout.checkpoint_pages("f1", 1), &[0x33; ZOU_PAGE_SIZE])
            .unwrap();
        store
            .put_new(&layout.checkpoint_page_index("f1"), index.as_bytes())
            .unwrap();
        put_manifest(&*store, &layout, &[("f1", 0x100, CheckpointKind::Full)]);

        let mut rd = ChainReader::attach(&*store, &layout).unwrap().unwrap();
        for (i, fill) in [(0u32, 0x11u8), (1, 0x22), (2, 0x33)] {
            let page = rd.read(&*store, &layout, blk(16384, i)).unwrap();
            assert!(page.iter().all(|b| *b == fill), "block {i}");
        }
    }
}
