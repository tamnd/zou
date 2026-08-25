//! GetPage: serve relation pages out of the layer store (spec 04
//! sec 4).
//!
//! A read asks for blocks at an lsn. For each block the layer reader
//! reconstructs the inputs, the newest image at or below the lsn plus
//! every record above it, and the whole batch becomes one redo pool
//! request: base images pushed, the union of the record chains applied
//! in lsn order, the requested pages read back. Merging chains across
//! blocks is safe because redo keeps the standard lsn interlock, a
//! record only applies to a page whose lsn is older, so a record below
//! one block's base image is a no op on that block while it moves
//! another block forward. A block whose reconstruction has no records
//! is its image already; when no block in the batch needs redo the
//! pool is never touched.
//!
//! Batches carry at most [`MAX_GETPAGE_BATCH`] pages. That is the unit
//! the smgr's vectored read path hands over: one prefetch burst of a
//! sequential scan becomes one round trip to the store and one redo
//! batch instead of 128 of each.
//!
//! Reads of blocks nothing ever wrote return zeroed pages, the file
//! hole semantics of the stock md smgr.
//!
//! With data checksums on, redo output and stored images both carry
//! stale checksums, because postgres computes them at eviction time,
//! not in redo. Every served page is restamped here, which is the
//! caveat recorded back in the redo pool box.
//!
//! The last written lsn cache is the read your writes half. Serving a
//! read at the newest WAL lsn would be correct but would make every
//! read wait for ingest to catch up to writes that cannot possibly
//! affect the block. [`LastWrittenLsn`] remembers, per block, the end
//! lsn of the last record that touched it, so a read asks for exactly
//! the lsn it needs: its own last write, or the cache's floor for a
//! block with no tracked writes. The cache is bounded by generation
//! rotation, and the floor absorbs the newest lsn of every dropped
//! generation, so the answer never falls below a write it forgot.

use std::collections::BTreeMap;
use std::collections::HashMap;

use zou_store::cas::CasStore;
use zou_store::layer::LayerKey;
use zou_store::layermap::LayerMap;
use zou_store::lsn::Lsn;
use zou_store::memtable::Memtable;
use zou_store::pageread::{LayerReader, ReadError, Reconstruction};

use crate::redo::{RedoPool, RedoRequest, page_checksum};
use crate::relsize;
use crate::walscan::{self, BlockRef};

/// Most pages one GetPage batch serves, matching the smgr's vectored
/// read fan out.
pub const MAX_GETPAGE_BATCH: usize = 128;

const BLCKSZ: usize = 8192;

/// Bytes of postgres page header, up to the line pointer array.
const PAGE_HEADER: usize = 24;

/// A hook resolving base images for keys no image layer covers, see
/// [`PageService::with_base_fallback`].
type BaseFallback<'a> = Box<dyn Fn(&BlockRef) -> Option<Vec<u8>> + Send + Sync + 'a>;

#[derive(Debug, thiserror::Error)]
pub enum GetPageError {
    #[error("get page batch of {got} pages exceeds the limit of {MAX_GETPAGE_BATCH}")]
    BatchTooLarge { got: usize },
    #[error("reconstruction of {blk:?} needs redo but the service has no redo pool")]
    NoRedoPool { blk: BlockRef },
    #[error(
        "{blk:?} has records from {first_lsn:#x} on but no base image, and the first record cannot build the page alone"
    )]
    BareChain { blk: BlockRef, first_lsn: u64 },
    #[error("redo: {0}")]
    Redo(String),
    #[error("relation size: {0}")]
    Size(String),
    #[error(transparent)]
    Read(#[from] ReadError),
}

/// The read side of one page shard: a layer reader over the shard
/// prefix, an optional redo pool, and the checksum knob of the tenant.
/// The layer map and memtable arrive per call because ingest owns them
/// and swaps them under flush.
pub struct PageService<'a> {
    reader: LayerReader<'a>,
    pool: Option<&'a RedoPool>,
    data_checksums: bool,
    base_fallback: Option<BaseFallback<'a>>,
}

impl<'a> PageService<'a> {
    pub fn new(
        store: &'a dyn CasStore,
        prefix: impl Into<String>,
        pool: Option<&'a RedoPool>,
        data_checksums: bool,
    ) -> Self {
        PageService {
            reader: LayerReader::new(store, prefix),
            pool,
            data_checksums,
            base_fallback: None,
        }
    }

    /// Resolve a base image for a key no image layer covers yet,
    /// typically the pg/ objects frozen at the put elision flag day.
    /// A frozen image can be older than parts of the record chain and
    /// that is sound: redo stamps every page with its record's lsn, so
    /// a record the image already contains sees page lsn at or past
    /// its own and does not apply twice.
    pub fn with_base_fallback(
        mut self,
        f: impl Fn(&BlockRef) -> Option<Vec<u8>> + Send + Sync + 'a,
    ) -> Self {
        self.base_fallback = Some(Box::new(f));
        self
    }

    /// A service bound to one tenant's shard, able to follow the owner
    /// tags on layers a branch inherited. Branched tenants must attach
    /// this way; [`Self::new`] refuses their inherited layers loudly.
    pub fn for_shard(
        store: &'a dyn CasStore,
        tenant_ref: &str,
        shard: u16,
        pool: Option<&'a RedoPool>,
        data_checksums: bool,
    ) -> Self {
        PageService {
            reader: LayerReader::for_shard(store, tenant_ref, shard),
            pool,
            data_checksums,
            base_fallback: None,
        }
    }

    /// The same service with a different budget for the reader's block
    /// cache, see [`LayerReader::with_block_budget`]. A caller reading
    /// keys in order wants a small one; the default is sized for the
    /// serving path, which does not know what it will be asked next.
    pub fn with_block_budget(mut self, bytes: usize) -> Self {
        self.reader = self.reader.with_block_budget(bytes);
        self
    }

    /// Let go of the footers of layers `map` no longer names, and
    /// answer how many the reader still holds. A service kept across
    /// reads has to be told when the map moved on.
    pub fn forget_unnamed(&self, map: &LayerMap) -> usize {
        self.reader.forget_unnamed(map)
    }

    /// Serve one block, [`Self::get_pages`] without the batching.
    pub fn get_page(
        &self,
        map: &LayerMap,
        mem: &Memtable,
        blk: BlockRef,
        at: u64,
    ) -> Result<Vec<u8>, GetPageError> {
        Ok(self
            .get_pages(map, mem, &[blk], at)?
            .pop()
            .expect("one block in, one page out"))
    }

    /// How many blocks one relation fork holds as of `at`, the answer
    /// smgr nblocks wants. Read out of the layers like a page, folded
    /// like a size: no redo, no base image needed, and nothing read
    /// from the parent's `pg/` prefix, so a branch gets the same
    /// answer as the tenant it was cut from.
    ///
    /// `None` is a fork the layers say nothing at all about, which is
    /// not the same as a fork of no blocks: one is silence and the
    /// other is an answer. A caller with somewhere else to look wants
    /// to be able to tell them apart.
    pub fn rel_size(
        &self,
        map: &LayerMap,
        mem: &Memtable,
        fork: relsize::ForkRef,
        at: u64,
    ) -> Result<Option<u32>, GetPageError> {
        let r = self.reader.reconstruct(map, mem, &fork.key(), Lsn(at))?;
        if r.base.is_none() && r.records.is_empty() {
            return Ok(None);
        }
        relsize::fold(r.base.as_deref(), &r.records)
            .map(Some)
            .map_err(GetPageError::Size)
    }

    /// Materialize `blocks` as of `at`, in order, as one redo batch.
    pub fn get_pages(
        &self,
        map: &LayerMap,
        mem: &Memtable,
        blocks: &[BlockRef],
        at: u64,
    ) -> Result<Vec<Vec<u8>>, GetPageError> {
        Ok(self
            .build(map, mem, blocks, at, true)?
            .into_iter()
            .map(|page| page.expect("a strict build answers every block"))
            .collect())
    }

    /// [`Self::get_pages`] for a caller that would rather hear about a
    /// block the store cannot build than be refused the batch: `None`
    /// where [`Self::get_pages`] would return [`GetPageError::BareChain`]
    /// or fabricate a hole out of zeros.
    ///
    /// Compaction reads this way. Whether a key can be imaged at all is
    /// the same question the reader answers while it builds the page,
    /// asked one layer deeper than the caller can see: the base may be
    /// in any image of the plan, in the first record, or in the
    /// fallback. A key that comes back `None` simply keeps its records
    /// in the delta run and gets asked again next pass.
    pub fn get_pages_where_possible(
        &self,
        map: &LayerMap,
        mem: &Memtable,
        blocks: &[BlockRef],
        at: u64,
    ) -> Result<Vec<Option<Vec<u8>>>, GetPageError> {
        self.build(map, mem, blocks, at, false)
    }

    /// The read path both entry points share. `strict` is what to do
    /// with a block no base can be found for: refuse the batch, or drop
    /// that block from it.
    fn build(
        &self,
        map: &LayerMap,
        mem: &Memtable,
        blocks: &[BlockRef],
        at: u64,
        strict: bool,
    ) -> Result<Vec<Option<Vec<u8>>>, GetPageError> {
        if blocks.len() > MAX_GETPAGE_BATCH {
            return Err(GetPageError::BatchTooLarge { got: blocks.len() });
        }
        let mut recons = Vec::with_capacity(blocks.len());
        for blk in blocks {
            let key = LayerKey::page(blk.spc, blk.db, blk.rel, blk.fork as u8, blk.blk);
            recons.push(self.reader.reconstruct(map, mem, &key, Lsn(at))?);
        }

        // A key with no image layer starts from the fallback image
        // when one exists. This covers both a bare chain, whose first
        // record would otherwise have to initialize the page, and a
        // block with no records at all, which without the fallback
        // would read as a hole.
        if let Some(fallback) = &self.base_fallback {
            for (blk, r) in blocks.iter().zip(&mut recons) {
                if r.base.is_none() {
                    r.base = fallback(blk);
                }
            }
        }

        // A main fork block with record history but no base anywhere is
        // a hole in the store, not a fresh block: a fresh block's first
        // record builds the page by itself, an init or a full image.
        // Redo would grow whatever it can out of zeros and the rest of
        // the page would be fabricated. A block with no base and no
        // records is the other hole, the one that reads as zeros.
        // Strict callers hear about the first kind and get zeros for
        // the second; the rest drop both from the batch.
        let mut bare: Option<(BlockRef, u64)> = None;
        let mut usable = vec![true; blocks.len()];
        for (i, (blk, r)) in blocks.iter().zip(&recons).enumerate() {
            if r.base.is_some() {
                continue;
            }
            let Some((first_lsn, bytes)) = r.records.first() else {
                usable[i] = strict;
                continue;
            };
            if blk.fork != 0 {
                continue;
            }
            let self_built = walscan::record_init_refs(bytes)
                .unwrap_or_default()
                .iter()
                .any(|(rref, init)| *init && rref == blk);
            if !self_built {
                // Only a strict caller is owed the complaint. Recording
                // it either way would refuse the batch below on behalf
                // of a caller that asked for exactly the opposite.
                if strict {
                    bare = bare.or(Some((*blk, first_lsn.0)));
                }
                usable[i] = strict;
            }
        }
        let live = |i: usize, r: &Reconstruction| usable[i] && (!r.records.is_empty());

        // The union of every block's record chain, deduplicated by
        // lsn: one record is one position in the tenant's single WAL
        // stream, so two chains listing the same lsn carry the same
        // bytes and redo must see the record once.
        let mut chain: BTreeMap<u64, &[u8]> = BTreeMap::new();
        for (i, r) in recons.iter().enumerate() {
            if !usable[i] {
                continue;
            }
            for (lsn, bytes) in &r.records {
                chain.insert(lsn.0, bytes.as_slice());
            }
        }

        let mut pages: Vec<Option<Vec<u8>>> = Vec::with_capacity(blocks.len());
        if chain.is_empty() {
            // Every block is exactly its image, or a hole.
            for (i, r) in recons.iter().enumerate() {
                pages.push(usable[i].then(|| r.base.clone().unwrap_or_else(|| vec![0; BLCKSZ])));
            }
        } else {
            let Some(pool) = self.pool else {
                let needy = blocks
                    .iter()
                    .zip(&recons)
                    .enumerate()
                    .find(|(i, (_, r))| live(*i, r))
                    .expect("a nonempty chain came from some block");
                return Err(GetPageError::NoRedoPool { blk: *needy.1.0 });
            };
            if let Some((blk, first_lsn)) = bare {
                return Err(GetPageError::BareChain { blk, first_lsn });
            }
            let bases: Vec<(BlockRef, &[u8])> = blocks
                .iter()
                .zip(&recons)
                .enumerate()
                .filter(|(i, _)| usable[*i])
                .filter_map(|(_, (blk, r))| r.base.as_deref().map(|page| (*blk, page)))
                .collect();
            let records: Vec<(u64, u64, &[u8])> = chain
                .iter()
                .map(|(&lsn, &bytes)| (lsn, walscan::record_end(lsn, bytes.len() as u64), bytes))
                .collect();
            // Blocks nothing ever touched stay out of the gets: the
            // redo worker only knows pages it was given something for,
            // holes are answered here with zeros.
            let wanted: Vec<bool> = recons
                .iter()
                .enumerate()
                .map(|(i, r)| usable[i] && (r.base.is_some() || !r.records.is_empty()))
                .collect();
            let gets: Vec<BlockRef> = blocks
                .iter()
                .zip(&wanted)
                .filter(|(_, want)| **want)
                .map(|(blk, _)| *blk)
                .collect();
            let mut applied = match pool.apply(&RedoRequest {
                pages: &bases,
                records: &records,
                gets: &gets,
            }) {
                Ok(pages) => pages.into_iter(),
                Err(e) => {
                    let blamed = match pin_culprit(pool, blocks, &recons, &usable) {
                        Some(i) => blame(at, &blocks[i..i + 1], &recons[i..i + 1]),
                        None => format!("no block failed alone in {}", blame(at, blocks, &recons)),
                    };
                    return Err(GetPageError::Redo(format!("{e}, {blamed}")));
                }
            };
            for (i, want) in wanted.iter().enumerate() {
                pages.push(match (want, usable[i]) {
                    (true, _) => Some(applied.next().expect("one page per get")),
                    (false, true) => Some(vec![0; BLCKSZ]),
                    (false, false) => None,
                });
            }
        }

        if self.data_checksums {
            for (blk, page) in blocks.iter().zip(&mut pages) {
                let Some(page) = page else { continue };
                if page.iter().any(|b| *b != 0) {
                    let sum = page_checksum(page, blk.blk);
                    page[8..10].copy_from_slice(&sum.to_le_bytes());
                }
            }
        }
        Ok(pages)
    }
}

/// Blocks one failed batch names before the message gives up.
const BLAME_BLOCKS: usize = 8;

/// Which block of a failed batch fails on its own. A batch is up to
/// [`MAX_GETPAGE_BATCH`] blocks wide and the worker's PANIC names none
/// of them, so a read that already failed pays one more pass, a block
/// and its own records at a time, to find the one that breaks. The
/// pool replaces the worker each time, so a run of these is slow but
/// safe, and it only ever happens on a read that is lost anyway. A
/// block that survives alone is not the culprit: its chain rebuilds.
/// Nothing failing alone is itself worth knowing, it means the batch
/// broke on the record union rather than on one page's history.
///
/// Only blocks the failed request actually carried are asked about.
/// A block the batch dropped has records and no base anywhere, and
/// redoing those on the zero page the worker starts from is a heap
/// insert landing past the end of a page nothing ever built: it dies
/// every time, for a reason that has nothing to do with why the batch
/// died, and the first one in key order gets the blame for a batch it
/// was never in.
fn pin_culprit(
    pool: &RedoPool,
    blocks: &[BlockRef],
    recons: &[Reconstruction],
    usable: &[bool],
) -> Option<usize> {
    for (i, (blk, r)) in blocks.iter().zip(recons).enumerate() {
        if !usable[i] || (r.base.is_none() && r.records.is_empty()) {
            continue;
        }
        let bases: Vec<(BlockRef, &[u8])> = r
            .base
            .as_deref()
            .map(|page| (*blk, page))
            .into_iter()
            .collect();
        let records: Vec<(u64, u64, &[u8])> = r
            .records
            .iter()
            .map(|(lsn, bytes)| {
                (
                    lsn.0,
                    walscan::record_end(lsn.0, bytes.len() as u64),
                    bytes.as_slice(),
                )
            })
            .collect();
        let one = [*blk];
        if pool
            .apply(&RedoRequest {
                pages: &bases,
                records: &records,
                gets: &one,
            })
            .is_err()
        {
            return Some(i);
        }
    }
    None
}

/// What the batch that killed the worker was made of. The worker dies
/// inside postgres, and its PANIC names neither the block nor the
/// record, so the only place the shape of the failure can be written
/// down is here, where the batch was built. Every one of these
/// failures so far has had the same tell: a base whose page lsn sits
/// below the records it is being asked to take, which means the chain
/// lost the records in between and redo is applying an update to a
/// page that never got the one before it.
fn blame(at: u64, blocks: &[BlockRef], recons: &[Reconstruction]) -> String {
    let mut said = Vec::new();
    for (blk, r) in blocks.iter().zip(recons).take(BLAME_BLOCKS) {
        let base = match &r.base {
            Some(page) if page.len() >= PAGE_HEADER => {
                let from = match r.base_lsn {
                    Some(lsn) => format!("the image at {:#x}", lsn.0),
                    None => "the fallback".into(),
                };
                let lower = u16::from_le_bytes([page[12], page[13]]);
                let upper = u16::from_le_bytes([page[14], page[15]]);
                let hi = u32::from_le_bytes([page[0], page[1], page[2], page[3]]);
                let lo = u32::from_le_bytes([page[4], page[5], page[6], page[7]]);
                let page_lsn = ((hi as u64) << 32) | lo as u64;
                let maxoff = lower.saturating_sub(PAGE_HEADER as u16) / 4;
                format!(
                    "base from {from}, page lsn {page_lsn:#x}, lower {lower} upper {upper} max off {maxoff}"
                )
            }
            Some(page) => format!("base of {} bytes", page.len()),
            None => "no base".into(),
        };
        let recs = match (r.records.first(), r.records.last()) {
            (Some((first, _)), Some((last, _))) => {
                format!("{} records {:#x}..{:#x}", r.records.len(), first.0, last.0)
            }
            _ => "no records".into(),
        };
        said.push(format!(
            "{}/{}/{}.{} blk {}: {base}, {recs}, {} layers touched",
            blk.spc, blk.db, blk.rel, blk.fork, blk.blk, r.layers_touched
        ));
    }
    if blocks.len() > BLAME_BLOCKS {
        said.push(format!("and {} more blocks", blocks.len() - BLAME_BLOCKS));
    }
    format!("the batch read at {at:#x} of {}", said.join("; "))
}

/// The last written lsn per block, bounded. `note` on every ingested
/// record, `read_lsn` to pick the lsn a GetPage should ask for.
///
/// Two generation rotation keeps it bounded without an lsn going
/// missing: when the current generation fills, the previous one is
/// dropped and its newest lsn folds into the floor, so a forgotten
/// block reads at the floor, which is at or above its last write.
/// Conservative in exactly one direction: a read may wait for more
/// ingest than it strictly needed, never serve a page missing its own
/// writes.
pub struct LastWrittenLsn {
    cap: usize,
    floor: u64,
    current: HashMap<BlockRef, u64>,
    old: HashMap<BlockRef, u64>,
}

impl LastWrittenLsn {
    /// `floor` is where tracking starts, the attach point: nothing is
    /// known about older history, so untracked blocks read there.
    pub fn new(cap: usize, floor: u64) -> Self {
        LastWrittenLsn {
            cap: cap.max(1),
            floor,
            current: HashMap::new(),
            old: HashMap::new(),
        }
    }

    /// Record that a WAL record ending at `end_lsn` touched `blk`.
    pub fn note(&mut self, blk: BlockRef, end_lsn: u64) {
        let slot = self.current.entry(blk).or_insert(0);
        *slot = (*slot).max(end_lsn);
        if self.current.len() >= self.cap {
            if let Some(&max) = self.old.values().max() {
                self.floor = self.floor.max(max);
            }
            self.old = std::mem::take(&mut self.current);
        }
    }

    /// Every block a record touched, one call per ingested record.
    pub fn note_record(&mut self, refs: &[BlockRef], end_lsn: u64) {
        for &blk in refs {
            self.note(blk, end_lsn);
        }
    }

    /// The lsn a read of `blk` should ask for: its last tracked write,
    /// or the floor when the cache never saw one.
    pub fn read_lsn(&self, blk: &BlockRef) -> u64 {
        self.current
            .get(blk)
            .or_else(|| self.old.get(blk))
            .copied()
            .unwrap_or(self.floor)
    }

    pub fn floor(&self) -> u64 {
        self.floor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zou_store::layer::{DeltaEntry, ImageEntry, build_delta, build_image};
    use zou_store::layermap::LayerDesc;
    use zou_store::layout::TenantLayout;
    use zou_store::mem::MemStore;
    use zou_store::shardmanifest::{LayerEntry, PageShardManifest, publish_layer};

    fn blk(rel: u32, b: u32) -> BlockRef {
        BlockRef {
            spc: 1663,
            db: 5,
            rel,
            fork: 0,
            blk: b,
        }
    }

    fn page_with(byte: u8) -> Vec<u8> {
        let mut page = vec![0u8; BLCKSZ];
        page[..24].copy_from_slice(&[byte; 24]);
        page[100] = byte;
        page
    }

    fn publish(
        store: &MemStore,
        layout: &TenantLayout,
        bytes: &[u8],
        footer: &zou_store::layer::LayerFooter,
        lsn: u64,
    ) -> LayerEntry {
        let desc = LayerDesc::from_footer(footer, bytes.len() as u64);
        store
            .put_if_absent(&format!("{}{}", layout.shard_prefix(0), desc.name()), bytes)
            .unwrap();
        let entry = LayerEntry {
            name: desc.name(),
            size: bytes.len() as u64,
            owner: None,
            upto: None,
        };
        publish_layer(store, &layout.shard_manifest(0), 0, &entry, Lsn(lsn)).unwrap();
        entry
    }

    #[test]
    fn image_only_batches_skip_redo_restamp_checksums_and_zero_holes() {
        let store = MemStore::default();
        let layout = TenantLayout::new("t");
        let images = vec![
            ImageEntry {
                key: LayerKey::page(1663, 5, 90, 0, 1),
                page: page_with(0xA1),
            },
            ImageEntry {
                key: LayerKey::page(1663, 5, 90, 0, 2),
                page: page_with(0xB2),
            },
        ];
        let (bytes, footer) = build_image(&images, Lsn(100), 4096).unwrap();
        publish(&store, &layout, &bytes, &footer, 100);
        let (manifest, _) = PageShardManifest::load(&store, &layout.shard_manifest(0))
            .unwrap()
            .unwrap();
        let map = manifest.layer_map().unwrap();

        let svc = PageService::new(&store, layout.shard_prefix(0), None, true);
        let got = svc
            .get_pages(
                &map,
                &Memtable::new(),
                &[blk(90, 1), blk(90, 7), blk(90, 2)],
                200,
            )
            .unwrap();

        let mut want1 = page_with(0xA1);
        let sum = page_checksum(&want1, 1);
        want1[8..10].copy_from_slice(&sum.to_le_bytes());
        assert_eq!(got[0], want1, "image served with a fresh checksum");
        assert_eq!(got[1], vec![0u8; BLCKSZ], "a hole reads as zeros");
        assert_eq!(&got[2][100], &0xB2, "order follows the request");
    }

    #[test]
    fn a_split_child_serves_pages_flushed_before_the_split() {
        let store = MemStore::default();
        let layout = TenantLayout::new("t");
        store
            .put_if_absent(
                &layout.manifest(),
                &zou_store::Manifest::new("t", 18).to_json(),
            )
            .unwrap();

        // Two blocks that land on different children once the tenant
        // splits, both flushed by shard 0 while the count was one.
        let (b0, b1) = {
            let mut on0 = None;
            let mut on1 = None;
            for b in 0..200u32 {
                let block = b * 20_000;
                match zou_store::shard_of(&LayerKey::page(1663, 5, 90, 0, block), 2) {
                    0 if on0.is_none() => on0 = Some(block),
                    1 if on1.is_none() => on1 = Some(block),
                    _ => {}
                }
            }
            (on0.unwrap(), on1.unwrap())
        };
        let mut images = vec![
            ImageEntry {
                key: LayerKey::page(1663, 5, 90, 0, b0),
                page: page_with(0xC0),
            },
            ImageEntry {
                key: LayerKey::page(1663, 5, 90, 0, b1),
                page: page_with(0xC1),
            },
        ];
        images.sort_by_key(|e| e.key);
        let (bytes, footer) = build_image(&images, Lsn(100), 4096).unwrap();
        publish(&store, &layout, &bytes, &footer, 100);

        let manifest = zou_store::split(&store, "t").unwrap();

        // Each child sees exactly its own block, through the parent's
        // prefix, with no manifest of its own yet.
        for (shard, block, byte) in [(0u16, b0, 0xC0u8), (1, b1, 0xC1)] {
            let (descs, floor) =
                zou_store::load_serving_descs(&store, "t", &manifest, shard).unwrap();
            assert_eq!(floor, Lsn(100));
            let map = LayerMap::new(descs).unwrap();
            let svc = PageService::for_shard(&store, "t", shard, None, false);
            let got = svc
                .get_page(&map, &Memtable::new(), blk(90, block), 200)
                .unwrap();
            assert_eq!(got[100], byte, "shard {shard} serves its half");
        }
    }

    #[test]
    fn a_getpage_follows_the_map_through_a_wrong_shard_redirect() {
        use zou_store::placement::{self, MapClient, MapServer, Pin, PlacementError};

        let store = MemStore::default();
        let layout = TenantLayout::new("t");
        let images = vec![ImageEntry {
            key: LayerKey::page(1663, 5, 90, 0, 7),
            page: page_with(0xD7),
        }];
        let (bytes, footer) = build_image(&images, Lsn(100), 4096).unwrap();
        publish(&store, &layout, &bytes, &footer, 100);
        let (manifest, _) = PageShardManifest::load(&store, &layout.shard_manifest(0))
            .unwrap()
            .unwrap();
        let map = manifest.layer_map().unwrap();

        // Two page nodes, each with its own admission gate in front of
        // the same shard's service, the way a fleet would stand them
        // up. The rpc is get_page, the envelope is the map version.
        placement::publish(&store, |m| {
            m.nodes = ["a", "b"]
                .map(|id| zou_store::Node {
                    id: id.into(),
                    addr: format!("{id}.cell:6400"),
                })
                .to_vec()
        })
        .unwrap();
        let mut client = MapClient::new(&store).unwrap();
        let owner = client.route("t", 0).unwrap().id.clone();
        let other = if owner == "a" { "b" } else { "a" }.to_string();
        let mut gates = [
            (
                owner.clone(),
                MapServer::new(&store, owner.clone()).unwrap(),
            ),
            (
                other.clone(),
                MapServer::new(&store, other.clone()).unwrap(),
            ),
        ];
        let svc = PageService::for_shard(&store, "t", 0, None, false);
        let serve = |gates: &mut [(String, MapServer)], node: &str, version: u64| {
            let gate = &mut gates.iter_mut().find(|(id, _)| id == node).unwrap().1;
            gate.admit("t", 0, version).map(|v| {
                let page = svc
                    .get_page(&map, &Memtable::new(), blk(90, 7), 200)
                    .unwrap();
                (v, page)
            })
        };

        // Steady state serves through the owner.
        let (v, page) = serve(&mut gates, &owner, client.version()).unwrap();
        assert_eq!((v, page[100]), (1, 0xD7));

        // Heat balancing moves the shard. The client is stale, routes
        // to the old owner, and the redirect alone gets it home: no
        // coordinator, one extra round trip, same page.
        placement::publish(&store, |m| {
            m.pins = vec![Pin {
                tenant: "t".into(),
                shard: 0,
                node: other.clone(),
            }]
        })
        .unwrap();
        assert_eq!(client.route("t", 0).unwrap().id, owner);
        // Until the old owner hears about the publish, both sides are
        // stale and it keeps serving, which is safe: layers are
        // immutable, the map only decides reroute speed.
        let (v, page) = serve(&mut gates, &owner, client.version()).unwrap();
        assert_eq!((v, page[100]), (1, 0xD7));
        let stale = &mut gates.iter_mut().find(|(id, _)| id == &owner).unwrap().1;
        stale.refresh().unwrap();
        let err = serve(&mut gates, &owner, client.version()).unwrap_err();
        let PlacementError::WrongShard { map_version } = err else {
            panic!("expected a redirect, got {err}");
        };
        client.absorb(map_version).unwrap();
        assert_eq!(client.route("t", 0).unwrap().id, other);
        let (v, page) = serve(&mut gates, &other, client.version()).unwrap();
        assert_eq!((v, page[100]), (2, 0xD7));
    }

    #[test]
    fn a_chain_without_a_pool_refuses_and_names_the_block() {
        let store = MemStore::default();
        let layout = TenantLayout::new("t");
        let entries = vec![DeltaEntry {
            key: LayerKey::page(1663, 5, 90, 0, 4),
            lsn: Lsn(150),
            record: vec![9; 40],
        }];
        let (bytes, footer) = build_delta(&entries, 4096).unwrap();
        publish(&store, &layout, &bytes, &footer, 150);
        let (manifest, _) = PageShardManifest::load(&store, &layout.shard_manifest(0))
            .unwrap()
            .unwrap();
        let map = manifest.layer_map().unwrap();

        let svc = PageService::new(&store, layout.shard_prefix(0), None, false);
        let err = svc
            .get_page(&map, &Memtable::new(), blk(90, 4), 200)
            .unwrap_err();
        assert!(
            matches!(err, GetPageError::NoRedoPool { blk: b } if b == blk(90, 4)),
            "{err}"
        );
    }

    #[test]
    fn a_bare_mid_history_chain_refuses_loudly() {
        // The same missing base fixture, but with a pool present, so
        // the refusal is about the hole in the store, not the missing
        // pool. The pool spawns workers lazily and the refusal comes
        // first, so the bogus binary path is never touched.
        let store = MemStore::default();
        let layout = TenantLayout::new("t");
        let entries = vec![DeltaEntry {
            key: LayerKey::page(1663, 5, 90, 0, 4),
            lsn: Lsn(150),
            record: vec![9; 40],
        }];
        let (bytes, footer) = build_delta(&entries, 4096).unwrap();
        publish(&store, &layout, &bytes, &footer, 150);
        let (manifest, _) = PageShardManifest::load(&store, &layout.shard_manifest(0))
            .unwrap()
            .unwrap();
        let map = manifest.layer_map().unwrap();

        let pool = RedoPool::new(crate::redo::RedoPoolConfig {
            postgres: "/nonexistent".into(),
            scratch_root: "/nonexistent".into(),
            workers: 1,
            batch_timeout: std::time::Duration::from_secs(1),
            batches_per_worker: 1,
            data_checksums: false,
        });
        let svc = PageService::new(&store, layout.shard_prefix(0), Some(&pool), false);
        let err = svc
            .get_page(&map, &Memtable::new(), blk(90, 4), 200)
            .unwrap_err();
        assert!(
            matches!(
                err,
                GetPageError::BareChain { blk: b, first_lsn: 150 } if b == blk(90, 4)
            ),
            "{err}"
        );
    }

    #[test]
    fn a_bare_mid_history_chain_is_a_none_to_the_caller_that_asked_for_one() {
        // The same fixture again, read the way compaction reads it. A
        // fold that hears BareChain here dies on the whole shard, and
        // the answer it wanted was only ever "leave that page in the
        // delta run". The pool is bogus for the same reason as above,
        // nothing gets as far as redo.
        let store = MemStore::default();
        let layout = TenantLayout::new("t");
        let entries = vec![DeltaEntry {
            key: LayerKey::page(1663, 5, 90, 0, 4),
            lsn: Lsn(150),
            record: vec![9; 40],
        }];
        let (bytes, footer) = build_delta(&entries, 4096).unwrap();
        publish(&store, &layout, &bytes, &footer, 150);
        let (manifest, _) = PageShardManifest::load(&store, &layout.shard_manifest(0))
            .unwrap()
            .unwrap();
        let map = manifest.layer_map().unwrap();

        let pool = RedoPool::new(crate::redo::RedoPoolConfig {
            postgres: "/nonexistent".into(),
            scratch_root: "/nonexistent".into(),
            workers: 1,
            batch_timeout: std::time::Duration::from_secs(1),
            batches_per_worker: 1,
            data_checksums: false,
        });
        let svc = PageService::new(&store, layout.shard_prefix(0), Some(&pool), false);
        let pages = svc
            .get_pages_where_possible(&map, &Memtable::new(), &[blk(90, 4)], 200)
            .expect("the batch is not refused over one unbuildable page");
        assert_eq!(pages, vec![None]);
    }

    #[test]
    fn the_blame_names_a_block_the_failed_batch_actually_carried() {
        // A block the batch dropped and a block it kept. Redo dies on
        // the batch, and the hunt for the one that dies alone has to
        // leave the dropped one out of it: its records have no base
        // anywhere, so redoing them on their own dies every time, for
        // a reason that has nothing to do with why the batch died.
        let store = MemStore::default();
        let layout = TenantLayout::new("t");
        let images = vec![ImageEntry {
            key: LayerKey::page(1663, 5, 90, 0, 5),
            page: page_with(0xC3),
        }];
        let (bytes, footer) = build_image(&images, Lsn(100), 4096).unwrap();
        publish(&store, &layout, &bytes, &footer, 100);
        let entries = vec![
            DeltaEntry {
                key: LayerKey::page(1663, 5, 90, 0, 4),
                lsn: Lsn(150),
                record: vec![9; 40],
            },
            DeltaEntry {
                key: LayerKey::page(1663, 5, 90, 0, 5),
                lsn: Lsn(160),
                record: vec![9; 40],
            },
        ];
        let (bytes, footer) = build_delta(&entries, 4096).unwrap();
        publish(&store, &layout, &bytes, &footer, 160);
        let (manifest, _) = PageShardManifest::load(&store, &layout.shard_manifest(0))
            .unwrap()
            .unwrap();
        let map = manifest.layer_map().unwrap();

        let pool = RedoPool::new(crate::redo::RedoPoolConfig {
            postgres: "/nonexistent".into(),
            scratch_root: "/nonexistent".into(),
            workers: 1,
            batch_timeout: std::time::Duration::from_secs(1),
            batches_per_worker: 1,
            data_checksums: false,
        });
        let svc = PageService::new(&store, layout.shard_prefix(0), Some(&pool), false);
        let err = svc
            .get_pages_where_possible(&map, &Memtable::new(), &[blk(90, 4), blk(90, 5)], 200)
            .unwrap_err()
            .to_string();
        assert!(err.contains("blk 5"), "{err}");
        assert!(
            !err.contains("blk 4"),
            "block 4 was never in the batch that failed: {err}"
        );
    }

    #[test]
    fn batches_past_the_cap_are_refused() {
        let store = MemStore::default();
        let layout = TenantLayout::new("t");
        let svc = PageService::new(&store, layout.shard_prefix(0), None, false);
        let blocks: Vec<BlockRef> = (0..=MAX_GETPAGE_BATCH as u32).map(|b| blk(90, b)).collect();
        let map = LayerMap::new(Vec::new()).unwrap();
        let err = svc
            .get_pages(&map, &Memtable::new(), &blocks, 10)
            .unwrap_err();
        assert!(
            matches!(err, GetPageError::BatchTooLarge { got } if got == 129),
            "{err}"
        );
    }

    #[test]
    fn the_lw_cache_answers_read_your_writes_and_survives_rotation() {
        let mut lw = LastWrittenLsn::new(4, 1000);
        assert_eq!(lw.read_lsn(&blk(1, 1)), 1000, "untracked reads the floor");

        lw.note(blk(1, 1), 1100);
        lw.note(blk(1, 1), 1050);
        assert_eq!(lw.read_lsn(&blk(1, 1)), 1100, "notes never move backwards");

        // Push enough distinct blocks through to rotate twice.
        for b in 2..12u32 {
            lw.note(blk(1, b), 1000 + 100 * b as u64);
        }
        for b in 1..12u32 {
            let want = if b == 1 { 1100 } else { 1000 + 100 * b as u64 };
            assert!(
                lw.read_lsn(&blk(1, b)) >= want,
                "block {b} answered {} below its last write {want}",
                lw.read_lsn(&blk(1, b))
            );
        }
        assert!(lw.floor() >= 1100, "dropped generations raised the floor");
    }

    #[test]
    fn a_failed_batch_says_what_it_fed_the_worker() {
        // A base sitting well below the records it is asked to take is
        // the shape every one of these failures has had, so the
        // message has to carry both numbers to be worth reading.
        let mut base = vec![0u8; BLCKSZ];
        base[0..4].copy_from_slice(&0u32.to_le_bytes());
        base[4..8].copy_from_slice(&0x0cf9_39b0u32.to_le_bytes());
        base[12..14].copy_from_slice(&96u16.to_le_bytes());
        base[14..16].copy_from_slice(&1024u16.to_le_bytes());
        let recon = Reconstruction {
            base: Some(base),
            base_lsn: Some(Lsn(0x0cf9_39b0)),
            records: vec![
                (Lsn(0x10db_0100), vec![0u8; 32]),
                (Lsn(0x10db_0288), vec![0u8; 32]),
            ],
            layers_touched: 3,
        };
        let said = blame(0x10db_0288, &[blk(16404, 42)], std::slice::from_ref(&recon));
        assert!(said.contains("1663/5/16404.0 blk 42"), "{said}");
        assert!(said.contains("image at 0xcf939b0"), "{said}");
        assert!(said.contains("page lsn 0xcf939b0"), "{said}");
        assert!(said.contains("max off 18"), "{said}");
        assert!(said.contains("2 records 0x10db0100..0x10db0288"), "{said}");

        let bare = Reconstruction {
            base: None,
            base_lsn: None,
            records: Vec::new(),
            layers_touched: 0,
        };
        let said = blame(7, &[blk(16404, 1)], std::slice::from_ref(&bare));
        assert!(said.contains("no base, no records"), "{said}");
    }
}
