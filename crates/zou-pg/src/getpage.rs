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
use zou_store::pageread::{LayerReader, ReadError};

use crate::redo::{RedoPool, RedoRequest, page_checksum};
use crate::walscan::{self, BlockRef};

/// Most pages one GetPage batch serves, matching the smgr's vectored
/// read fan out.
pub const MAX_GETPAGE_BATCH: usize = 128;

const BLCKSZ: usize = 8192;

#[derive(Debug, thiserror::Error)]
pub enum GetPageError {
    #[error("get page batch of {got} pages exceeds the limit of {MAX_GETPAGE_BATCH}")]
    BatchTooLarge { got: usize },
    #[error("reconstruction of {blk:?} needs redo but the service has no redo pool")]
    NoRedoPool { blk: BlockRef },
    #[error("redo: {0}")]
    Redo(String),
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
        }
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
        }
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

    /// Materialize `blocks` as of `at`, in order, as one redo batch.
    pub fn get_pages(
        &self,
        map: &LayerMap,
        mem: &Memtable,
        blocks: &[BlockRef],
        at: u64,
    ) -> Result<Vec<Vec<u8>>, GetPageError> {
        if blocks.len() > MAX_GETPAGE_BATCH {
            return Err(GetPageError::BatchTooLarge { got: blocks.len() });
        }
        let mut recons = Vec::with_capacity(blocks.len());
        for blk in blocks {
            let key = LayerKey::page(blk.spc, blk.db, blk.rel, blk.fork as u8, blk.blk);
            recons.push(self.reader.reconstruct(map, mem, &key, Lsn(at))?);
        }

        // The union of every block's record chain, deduplicated by
        // lsn: one record is one position in the tenant's single WAL
        // stream, so two chains listing the same lsn carry the same
        // bytes and redo must see the record once.
        let mut chain: BTreeMap<u64, &[u8]> = BTreeMap::new();
        for r in &recons {
            for (lsn, bytes) in &r.records {
                chain.insert(lsn.0, bytes.as_slice());
            }
        }

        let mut pages: Vec<Vec<u8>> = Vec::with_capacity(blocks.len());
        if chain.is_empty() {
            // Every block is exactly its image, or a hole.
            for r in &recons {
                pages.push(r.base.clone().unwrap_or_else(|| vec![0; BLCKSZ]));
            }
        } else {
            let Some(pool) = self.pool else {
                let needy = blocks
                    .iter()
                    .zip(&recons)
                    .find(|(_, r)| !r.records.is_empty())
                    .expect("a nonempty chain came from some block");
                return Err(GetPageError::NoRedoPool { blk: *needy.0 });
            };
            let bases: Vec<(BlockRef, &[u8])> = blocks
                .iter()
                .zip(&recons)
                .filter_map(|(blk, r)| r.base.as_deref().map(|page| (*blk, page)))
                .collect();
            let records: Vec<(u64, u64, &[u8])> = chain
                .iter()
                .map(|(&lsn, &bytes)| (lsn, walscan::record_end(lsn, bytes.len() as u64), bytes))
                .collect();
            // Blocks nothing ever touched stay out of the gets: the
            // redo worker only knows pages it was given something for,
            // holes are answered here with zeros.
            let gets: Vec<BlockRef> = blocks
                .iter()
                .zip(&recons)
                .filter(|(_, r)| r.base.is_some() || !r.records.is_empty())
                .map(|(blk, _)| *blk)
                .collect();
            let mut applied = pool
                .apply(&RedoRequest {
                    pages: &bases,
                    records: &records,
                    gets: &gets,
                })
                .map_err(GetPageError::Redo)?
                .into_iter();
            for r in &recons {
                if r.base.is_some() || !r.records.is_empty() {
                    pages.push(applied.next().expect("one page per get"));
                } else {
                    pages.push(vec![0; BLCKSZ]);
                }
            }
        }

        if self.data_checksums {
            for (blk, page) in blocks.iter().zip(&mut pages) {
                if page.iter().any(|b| *b != 0) {
                    let sum = page_checksum(page, blk.blk);
                    page[8..10].copy_from_slice(&sum.to_le_bytes());
                }
            }
        }
        Ok(pages)
    }
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
}
