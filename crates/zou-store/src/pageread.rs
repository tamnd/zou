//! The read path: reconstruct one key at one lsn (spec 04 sec 2, 5).
//!
//! A lookup plans against the [`LayerMap`], fetches only what the
//! plan names, and returns the base image plus the ordered record
//! chain the redo pool needs. Everything is range GETs: a footer is
//! one suffix range the first time and cached after, a block is one
//! range verified by the crc in its own index row. The whole layer
//! object is never fetched.
//!
//! The compaction invariant bounds a lookup at one image layer, four
//! delta layers and the memtable. The reconstruction reports how many
//! layers it touched so callers can watch the bound hold instead of
//! trusting it.
//!
//! Footers cache by object name. Layers are immutable, so a cached
//! footer is never stale; a map swap after flush or compaction just
//! stops naming the layers that went away, and dropping their cache
//! entries is a bounded memory question, not a correctness one, which
//! is what [`LayerReader::forget_unnamed`] is for. The cache is not an
//! optimization to take or leave: a footer runs to megabytes on a
//! layer of any size, so a reader that throws it away pays those
//! megabytes again on the next read of the same layer.
//!
//! Blocks cache the same way and for the same reason, see #465. A
//! block holds many keys and a lookup wants one of them, so a
//! sequential scan without a block cache fetches the same block once
//! per page in it: measured at 1.7 range GETs and 111 KB of store
//! traffic for every 8 KB page served, which is a table read at a
//! megabyte a second. Immutability makes the block cache free of stale
//! reads exactly as it does the footer cache; unlike footers, blocks
//! have no natural bound, so this one is byte budgeted and evicts the
//! least recently used.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::cas::{CasError, CasStore};
use crate::layer::{
    LayerDecodeError, LayerFooter, LayerKey, decode_delta_block, decode_image_block,
    read_layer_footer_suffix,
};
use crate::layermap::{LayerDesc, LayerMap};
use crate::lsn::Lsn;
use crate::memtable::Memtable;

/// First guess for the footer suffix fetch, before this reader has
/// met a layer of its own. A footer bigger than the guess costs one
/// exact refetch, not an error.
const FOOTER_GUESS: u64 = 64 * 1024;

/// Ceiling on the learned guess, so one layer with an outsized footer
/// cannot make every later cold read drag megabytes it does not need.
const FOOTER_GUESS_CAP: u64 = 1024 * 1024;

/// Bytes of layer blocks one reader holds before it starts evicting,
/// overridable with `ZOU_BLOCK_CACHE_MB`.
///
/// This is memory inside the page service worker, which lives in the
/// postmaster and has been killed for its footprint before, so the
/// default is deliberately a fraction of what a machine would give it.
/// A block is cut at [`crate::layer::LAYER_BLOCK_TARGET`], 256 KB of
/// raw entries, so an image block is around thirty pages and sixty
/// four megabytes is several hundred blocks. A scan in key order
/// therefore reads a block once and answers thirty pages out of it
/// with a budget it barely touches. Raising it buys the case the
/// default does not cover, which is a scattered read pattern over a
/// working set that nearly fits.
const BLOCK_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// One cached block: the bytes as the store returned them, and when
/// the reader last wanted them.
struct Cached {
    bytes: Arc<Vec<u8>>,
    used: u64,
}

/// Block bytes by layer name and offset, under a byte budget.
///
/// The recency stamp is a counter rather than a clock: it only ever
/// has to order two entries, and reading a clock per block read is a
/// syscall in the middle of the read path on some platforms.
struct BlockCache {
    blocks: HashMap<(String, u64), Cached>,
    bytes: usize,
    budget: usize,
    clock: u64,
}

impl BlockCache {
    fn new(budget: usize) -> Self {
        Self {
            blocks: HashMap::new(),
            bytes: 0,
            budget,
            clock: 0,
        }
    }

    fn get(&mut self, name: &str, offset: u64) -> Option<Arc<Vec<u8>>> {
        self.clock += 1;
        let clock = self.clock;
        // The key allocates, which on a hit is the whole cost of the
        // lookup, and is still several thousand times cheaper than the
        // range GET it stands in for.
        let entry = self.blocks.get_mut(&(name.to_string(), offset))?;
        entry.used = clock;
        Some(entry.bytes.clone())
    }

    fn put(&mut self, name: &str, offset: u64, bytes: Arc<Vec<u8>>) {
        // A block bigger than the whole budget would evict everything
        // to hold itself and be evicted by the next one.
        if bytes.len() > self.budget {
            return;
        }
        self.clock += 1;
        let cached = Cached {
            used: self.clock,
            bytes,
        };
        self.bytes += cached.bytes.len();
        if let Some(old) = self.blocks.insert((name.to_string(), offset), cached) {
            self.bytes -= old.bytes.len();
        }
        if self.bytes > self.budget {
            self.evict();
        }
    }

    /// Drop the least recently used blocks, down to three quarters of
    /// the budget rather than exactly to it, so that a reader sitting
    /// at the ceiling sorts once every quarter budget of new blocks
    /// instead of once per block.
    fn evict(&mut self) {
        let target = self.budget - self.budget / 4;
        let mut by_age: Vec<(u64, (String, u64))> = self
            .blocks
            .iter()
            .map(|(key, entry)| (entry.used, key.clone()))
            .collect();
        by_age.sort_unstable_by_key(|(used, _)| *used);
        for (_, key) in by_age {
            if self.bytes <= target {
                break;
            }
            if let Some(old) = self.blocks.remove(&key) {
                self.bytes -= old.bytes.len();
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// Forget the blocks of every layer not in `named`, answering how
    /// many blocks are still held.
    fn retain_named(&mut self, named: &std::collections::HashSet<String>) -> usize {
        let mut freed = 0;
        self.blocks.retain(|(name, _), entry| {
            let keep = named.contains(name);
            if !keep {
                freed += entry.bytes.len();
            }
            keep
        });
        self.bytes -= freed;
        self.blocks.len()
    }
}

/// The block budget this process runs with, the default unless
/// `ZOU_BLOCK_CACHE_MB` says otherwise. A value that does not parse is
/// the default too, said out loud rather than in silence: a reader is
/// not the place to refuse to start over a tuning knob, but a knob that
/// does nothing is worth a line.
fn block_budget() -> usize {
    crate::setting::number::<usize>("ZOU_BLOCK_CACHE_MB", "a whole number of megabytes")
        .map(|mb| mb * 1024 * 1024)
        .unwrap_or(BLOCK_CACHE_BYTES)
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("store: {0}")]
    Store(#[from] CasError),
    #[error("layer {name} is gone but the map still names it")]
    Missing { name: String },
    #[error("layer {name}: {source}")]
    Layer {
        name: String,
        source: LayerDecodeError,
    },
    #[error(
        "layer {name} disagrees with its name, refusing to serve from it, the object under that key is not the layer the manifest says it is, so either the store handed back the wrong bytes or something wrote over a key that was meant to be write once, `zou inspect <target> {name}` prints what the object actually holds"
    )]
    Mismatched { name: String },
    #[error("layer {name} came back short at {offset}+{len}")]
    ShortRange { name: String, offset: u64, len: u64 },
    #[error(
        "layer {name} belongs to {owner} but this reader has no shard context to resolve it, open it with `for_shard` so it knows which shard's layers are its own"
    )]
    ForeignLayer { name: String, owner: String },
}

/// One reconstructed key: what the redo pool needs to materialize it.
#[derive(Debug, PartialEq, Eq)]
pub struct Reconstruction {
    /// The page as of `base_lsn`, absent when no image layer covers
    /// the key yet; then the first record must initialize the page.
    pub base: Option<Vec<u8>>,
    pub base_lsn: Option<Lsn>,
    /// Records in `(base_lsn, lsn]`, ascending, ready to apply.
    pub records: Vec<(Lsn, Vec<u8>)>,
    /// Layers this lookup touched, the observed read amplification.
    pub layers_touched: usize,
}

/// Reads keys out of one shard's layer store. Cheap to keep around:
/// it owns nothing but the footer cache.
pub struct LayerReader<'a> {
    store: &'a dyn CasStore,
    /// Object key prefix layer names append to.
    prefix: String,
    /// The tenant and shard the reader is bound to, needed to resolve
    /// an inherited layer to its owner's prefix and a split ancestor's
    /// layer to its home shard. A reader without them serves only maps
    /// with no owner or home tags.
    tenant: Option<String>,
    shard: Option<u16>,
    footers: Mutex<HashMap<String, Arc<LayerFooter>>>,
    /// Block bodies the lookups have already paid for, see the module
    /// comment. Separate lock from the footers so that a reader
    /// evicting blocks is not holding up a footer lookup.
    blocks: Mutex<BlockCache>,
    /// How many tail bytes the next cold footer fetch asks for. The
    /// layers one shard cuts are all about the same shape, so the
    /// first layer whose footer overflowed the guess is a good guess
    /// for the rest, and the reader outlives them all.
    guess: AtomicU64,
}

impl<'a> LayerReader<'a> {
    pub fn new(store: &'a dyn CasStore, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
            tenant: None,
            shard: None,
            footers: Mutex::new(HashMap::new()),
            blocks: Mutex::new(BlockCache::new(block_budget())),
            guess: AtomicU64::new(FOOTER_GUESS),
        }
    }

    /// A reader bound to one tenant's shard, able to follow owner tags
    /// into ancestor prefixes and home tags across a split. Branched
    /// and split tenants must attach this way, [`LayerReader::new`]
    /// refuses their tagged layers loudly.
    pub fn for_shard(store: &'a dyn CasStore, tenant_ref: &str, shard: u16) -> Self {
        Self {
            store,
            prefix: crate::layout::TenantLayout::new(tenant_ref).shard_prefix(shard),
            tenant: Some(tenant_ref.to_string()),
            shard: Some(shard),
            footers: Mutex::new(HashMap::new()),
            blocks: Mutex::new(BlockCache::new(block_budget())),
            guess: AtomicU64::new(FOOTER_GUESS),
        }
    }

    /// The same reader with a different block budget, for a caller who
    /// knows better than the default: compaction reads every block of
    /// its inputs exactly once, so it wants a small one, and a test
    /// that wants to watch eviction wants a tiny one.
    pub fn with_block_budget(self, bytes: usize) -> Self {
        *self.blocks.lock().unwrap() = BlockCache::new(bytes);
        self
    }

    /// The object key for a layer: its name under this shard's prefix,
    /// under the owner's when the layer is inherited from a branch
    /// parent, under the home shard's when it is served across a split,
    /// and under both when the ancestor era was itself branched. Owner
    /// swaps the tenant, home swaps the shard, and each defaults to
    /// this reader's own.
    fn object_key(&self, desc: &LayerDesc, name: &str) -> Result<String, ReadError> {
        if desc.owner.is_none() && desc.home.is_none() {
            return Ok(format!("{}{}", self.prefix, name));
        }
        let foreign = |owner: &Option<String>| ReadError::ForeignLayer {
            name: name.to_string(),
            owner: owner.clone().unwrap_or_else(|| "a split ancestor".into()),
        };
        let tenant = match (&desc.owner, &self.tenant) {
            (Some(owner), _) => owner,
            (None, Some(own)) => own,
            (None, None) => return Err(foreign(&desc.owner)),
        };
        let shard = match (desc.home, self.shard) {
            (Some(home), _) => home,
            (None, Some(own)) => own,
            (None, None) => return Err(foreign(&desc.owner)),
        };
        Ok(format!(
            "{}{}",
            crate::layout::TenantLayout::new(tenant).shard_prefix(shard),
            name
        ))
    }

    /// The whole layer object, resolved through the owner and home
    /// tags like any read. Compaction uses this to pull its inputs
    /// wherever they live; the serving path never does, it reads
    /// blocks by range.
    pub fn fetch(&self, desc: &LayerDesc) -> Result<Vec<u8>, ReadError> {
        let name = desc.name();
        let object = self.object_key(desc, &name)?;
        Ok(self
            .store
            .get(&object)?
            .ok_or(ReadError::Missing { name })?
            .0)
    }

    fn range(
        &self,
        desc: &LayerDesc,
        name: &str,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, ReadError> {
        let object = self.object_key(desc, name)?;
        let bytes =
            self.store
                .get_range(&object, offset, len)?
                .ok_or_else(|| ReadError::Missing {
                    name: name.to_string(),
                })?;
        if (bytes.len() as u64) < len {
            return Err(ReadError::ShortRange {
                name: name.to_string(),
                offset,
                len,
            });
        }
        Ok(bytes)
    }

    /// One block of one layer, from cache or one range GET. Layers
    /// are immutable, so a name and an offset name the same bytes
    /// forever and a hit needs no validation.
    fn block(
        &self,
        desc: &LayerDesc,
        name: &str,
        offset: u64,
        len: u64,
    ) -> Result<Arc<Vec<u8>>, ReadError> {
        if let Some(bytes) = self.blocks.lock().unwrap().get(name, offset) {
            return Ok(bytes);
        }
        let bytes = Arc::new(self.range(desc, name, offset, len)?);
        self.blocks.lock().unwrap().put(name, offset, bytes.clone());
        Ok(bytes)
    }

    /// The footer for one layer, from cache or two range GETs. The
    /// footer's own ranges must agree with the name the manifest
    /// listed; a mismatch means the object is not what the map thinks
    /// it is and nothing in it can be trusted.
    fn footer(&self, desc: &LayerDesc) -> Result<Arc<LayerFooter>, ReadError> {
        let name = desc.name();
        if let Some(footer) = self.footers.lock().unwrap().get(&name) {
            return Ok(footer.clone());
        }
        let layer_err = |source| ReadError::Layer {
            name: name.clone(),
            source,
        };
        let guess = self.guess.load(Ordering::Relaxed).min(desc.size);
        let suffix = self.range(desc, &name, desc.size - guess, guess)?;
        let footer = match read_layer_footer_suffix(desc.kind, &suffix, desc.size) {
            Ok(footer) => footer,
            Err(LayerDecodeError::Truncated { need, .. }) if (need as u64) > guess => {
                let exact = self.range(desc, &name, desc.size - need as u64, need as u64)?;
                // Next time, ask for this much up front.
                self.guess
                    .fetch_max((need as u64).min(FOOTER_GUESS_CAP), Ordering::Relaxed);
                read_layer_footer_suffix(desc.kind, &exact, desc.size).map_err(layer_err)?
            }
            Err(source) => return Err(layer_err(source)),
        };
        if footer.kind != desc.kind
            || footer.min_key != desc.min_key
            || footer.max_key != desc.max_key
            || footer.min_lsn != desc.min_lsn
            || footer.max_lsn != desc.max_lsn
        {
            return Err(ReadError::Mismatched { name });
        }
        let footer = Arc::new(footer);
        self.footers.lock().unwrap().insert(name, footer.clone());
        Ok(footer)
    }

    /// Drop cached footers for layers `map` no longer names, and
    /// answer how many are still held. Compaction retires layers
    /// constantly, and a footer is not the small thing the fetch code
    /// makes it look like: a 350 MB delta layer carries a 2 MB bloom
    /// plus an index row per block. A reader that lives as long as the
    /// process has to let go of them or it keeps the whole retired
    /// history in memory.
    pub fn forget_unnamed(&self, map: &LayerMap) -> usize {
        let mut footers = self.footers.lock().unwrap();
        let mut blocks = self.blocks.lock().unwrap();
        if footers.is_empty() && blocks.is_empty() {
            return 0;
        }
        let named: std::collections::HashSet<String> =
            map.layers().iter().map(|d| d.name()).collect();
        footers.retain(|name, _| named.contains(name));
        blocks.retain_named(&named);
        footers.len()
    }

    /// The read algorithm for `(key, lsn)`: plan on the map, take the
    /// base from the newest image layer that holds the key, collect
    /// records from the delta layers above that image and the
    /// memtable, merge ascending. Overlapping unconsolidated layers
    /// can hold the same record; the merge keys on lsn, so a record
    /// applies once no matter how many layers carry it.
    ///
    /// The floor follows the base, not the plan. An image whose key
    /// range covers the key but which does not carry it leaves the
    /// floor where it was: compaction cuts images with interior gaps,
    /// and reading a gap as if it were a hit would drop the records
    /// below the image lsn and rebuild the page from half a chain.
    pub fn reconstruct(
        &self,
        map: &LayerMap,
        mem: &Memtable,
        key: &LayerKey,
        lsn: Lsn,
    ) -> Result<Reconstruction, ReadError> {
        let plan = map.plan(key, lsn);

        let mut base = None;
        let mut floor = Lsn(0);
        for desc in &plan.images {
            let footer = self.footer(desc)?;
            if !footer.may_contain(key) {
                continue;
            }
            let name = desc.name();
            for meta in footer.locate(key) {
                let bytes = self.block(desc, &name, meta.offset, meta.len as u64)?;
                let entries =
                    decode_image_block(&bytes, meta).map_err(|source| ReadError::Layer {
                        name: name.clone(),
                        source,
                    })?;
                if let Ok(i) = entries.binary_search_by(|e| e.key.cmp(key)) {
                    base = Some(entries.into_iter().nth(i).expect("index from search").page);
                    break;
                }
            }
            if base.is_some() {
                floor = desc.min_lsn;
                break;
            }
        }

        let mut touched = usize::from(base.is_some());
        let mut merged: BTreeMap<Lsn, Vec<u8>> = BTreeMap::new();
        for desc in plan.above(floor) {
            touched += 1;
            let footer = self.footer(desc)?;
            if !footer.may_contain(key) {
                continue;
            }
            // An inherited layer can carry records past the branch cut,
            // the owner's future; the clamp keeps them out of this
            // tenant's history.
            let ceil = desc.clamp(lsn);
            let name = desc.name();
            for meta in footer.locate(key) {
                let bytes = self.block(desc, &name, meta.offset, meta.len as u64)?;
                let entries =
                    decode_delta_block(&bytes, meta).map_err(|source| ReadError::Layer {
                        name: name.clone(),
                        source,
                    })?;
                for e in entries {
                    if e.key == *key && floor < e.lsn && e.lsn <= ceil {
                        merged.insert(e.lsn, e.record);
                    }
                }
            }
        }
        for (at, record) in mem.records_for(key, floor, lsn) {
            merged.insert(at, record.to_vec());
        }

        Ok(Reconstruction {
            base_lsn: base.as_ref().map(|_| floor),
            base,
            records: merged.into_iter().collect(),
            layers_touched: touched,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::{DeltaEntry, ImageEntry, PAGE_IMAGE_LEN, build_delta, build_image};
    use crate::mem::MemStore;
    use std::sync::atomic::AtomicUsize;

    fn k(block: u32) -> LayerKey {
        LayerKey::page(1663, 5, 16384, 0, block)
    }

    fn page(fill: u8) -> Vec<u8> {
        vec![fill; PAGE_IMAGE_LEN]
    }

    /// Build a small layered history in a MemStore: an image at lsn
    /// 100 for blocks 0..10, then two delta generations touching some
    /// of them. Returns the store, the map, and the memtable.
    fn history() -> (MemStore, LayerMap, Memtable) {
        let store = MemStore::default();
        let mut layers = Vec::new();

        let images: Vec<ImageEntry> = (0..10u32)
            .map(|b| ImageEntry {
                key: k(b),
                page: page(b as u8),
            })
            .collect();
        let (buf, footer) = build_image(&images, Lsn(100), 3 * PAGE_IMAGE_LEN).unwrap();
        let desc = LayerDesc::from_footer(&footer, buf.len() as u64);
        store.put(&format!("layers/{}", desc.name()), &buf).unwrap();
        layers.push(desc);

        for (min, max, blocks) in [(101u64, 200u64, [1u32, 3]), (201, 300, [3, 7])] {
            let mut entries: Vec<DeltaEntry> = Vec::new();
            for b in blocks {
                for at in [min, (min + max) / 2, max] {
                    entries.push(DeltaEntry {
                        key: k(b),
                        lsn: Lsn(at),
                        record: format!("r{b}@{at}").into_bytes(),
                    });
                }
            }
            entries.sort_by_key(|e| (e.key, e.lsn));
            let (buf, footer) = build_delta(&entries, 512).unwrap();
            let desc = LayerDesc::from_footer(&footer, buf.len() as u64);
            store.put(&format!("layers/{}", desc.name()), &buf).unwrap();
            layers.push(desc);
        }

        let mut mem = Memtable::new();
        mem.insert(k(3), Lsn(305), b"r3@305".to_vec());
        mem.insert(k(3), Lsn(310), b"r3@310".to_vec());
        (store, LayerMap::new(layers).unwrap(), mem)
    }

    #[test]
    fn a_lookup_returns_the_base_and_the_ordered_chain() {
        let (store, map, mem) = history();
        let reader = LayerReader::new(&store, "layers/");
        let got = reader.reconstruct(&map, &mem, &k(3), Lsn(310)).unwrap();
        assert_eq!(got.base, Some(page(3)));
        assert_eq!(got.base_lsn, Some(Lsn(100)));
        let lsns: Vec<u64> = got.records.iter().map(|(l, _)| l.0).collect();
        assert_eq!(
            lsns,
            vec![101, 150, 200, 201, 250, 300, 305, 310],
            "both delta generations then the memtable, ascending"
        );
        assert_eq!(got.records[0].1, b"r3@101".to_vec());
        assert_eq!(got.layers_touched, 3);
    }

    #[test]
    fn the_read_lsn_cuts_the_chain_and_the_plan() {
        let (store, map, mem) = history();
        let reader = LayerReader::new(&store, "layers/");
        // At lsn 150 the second delta generation and the memtable are
        // in the future and never fetched.
        let got = reader.reconstruct(&map, &mem, &k(3), Lsn(150)).unwrap();
        let lsns: Vec<u64> = got.records.iter().map(|(l, _)| l.0).collect();
        assert_eq!(lsns, vec![101, 150]);
        assert_eq!(got.layers_touched, 2);
        // An untouched block is just its image.
        let got = reader.reconstruct(&map, &mem, &k(5), Lsn(310)).unwrap();
        assert_eq!(got.base, Some(page(5)));
        assert!(got.records.is_empty());
        // A block outside every layer is nothing at all.
        let got = reader.reconstruct(&map, &mem, &k(99), Lsn(310)).unwrap();
        assert_eq!(got.base, None);
        assert_eq!(got.base_lsn, None);
        assert!(got.records.is_empty());
        assert_eq!(got.layers_touched, 0);
    }

    #[test]
    fn an_image_with_a_gap_does_not_floor_the_key_it_skipped() {
        // What compaction actually cuts: an image over the pages redo
        // could rebuild, with a hole where a page's base was not in
        // the store. The hole sits inside the layer's key range, so
        // the map offers the layer for the missing key too. Taking
        // that offer would drop every record below lsn 400 and hand
        // redo half a chain, which is a corrupt page, not a slow one.
        let (store, mut map, mem) = history();
        let mut layers = map.layers().to_vec();
        let sparse: Vec<ImageEntry> = (0..10u32)
            .filter(|b| *b != 3)
            .map(|b| ImageEntry {
                key: k(b),
                page: page(0xAA),
            })
            .collect();
        let (buf, footer) = build_image(&sparse, Lsn(400), 3 * PAGE_IMAGE_LEN).unwrap();
        let desc = LayerDesc::from_footer(&footer, buf.len() as u64);
        assert!(desc.covers(&k(3)), "the gap is inside the key range");
        store.put(&format!("layers/{}", desc.name()), &buf).unwrap();
        layers.push(desc);
        map = LayerMap::new(layers).unwrap();

        let reader = LayerReader::new(&store, "layers/");
        let got = reader.reconstruct(&map, &mem, &k(3), Lsn(500)).unwrap();
        assert_eq!(got.base, Some(page(3)), "the base is the older image");
        assert_eq!(got.base_lsn, Some(Lsn(100)));
        let lsns: Vec<u64> = got.records.iter().map(|(l, _)| l.0).collect();
        assert_eq!(lsns, vec![101, 150, 200, 201, 250, 300, 305, 310]);

        // A key the newer image does hold reads from it, with the
        // records below it folded away.
        let got = reader.reconstruct(&map, &mem, &k(1), Lsn(500)).unwrap();
        assert_eq!(got.base, Some(page(0xAA)));
        assert_eq!(got.base_lsn, Some(Lsn(400)));
        assert!(got.records.is_empty());
        assert_eq!(got.layers_touched, 1);
    }

    #[test]
    fn duplicate_records_across_overlapping_layers_apply_once() {
        // Two unconsolidated deltas carry the same records, the L0
        // shape before compaction sorts them out.
        let store = MemStore::default();
        let entries = vec![DeltaEntry {
            key: k(0),
            lsn: Lsn(150),
            record: b"same".to_vec(),
        }];
        let mut layers = Vec::new();
        for _ in 0..2 {
            let (buf, _) = build_delta(&entries, 512).unwrap();
            let mut desc = LayerDesc::delta(k(0), k(0), Lsn(150), Lsn(150));
            desc.size = buf.len() as u64;
            // Same ranges, same name, same bytes: idempotent publish.
            store.put(&format!("layers/{}", desc.name()), &buf).unwrap();
            layers.push(desc);
        }
        let map = LayerMap::new(layers).unwrap();
        let reader = LayerReader::new(&store, "layers/");
        let got = reader
            .reconstruct(&map, &Memtable::new(), &k(0), Lsn(200))
            .unwrap();
        assert_eq!(got.records.len(), 1);
    }

    #[test]
    fn footers_are_fetched_once_and_a_missing_layer_is_loud() {
        let (store, map, mem) = history();
        let reader = LayerReader::new(&store, "layers/");
        reader.reconstruct(&map, &mem, &k(3), Lsn(310)).unwrap();
        assert_eq!(reader.footers.lock().unwrap().len(), 3);
        reader.reconstruct(&map, &mem, &k(7), Lsn(310)).unwrap();
        assert_eq!(reader.footers.lock().unwrap().len(), 3);

        let gone = LayerDesc::parse(&map.layers()[0].name(), map.layers()[0].size).unwrap();
        store.delete(&format!("layers/{}", gone.name())).unwrap();
        let fresh = LayerReader::new(&store, "layers/");
        assert!(matches!(
            fresh.reconstruct(&map, &mem, &k(3), Lsn(310)),
            Err(ReadError::Missing { .. })
        ));
    }

    #[test]
    fn the_footers_of_retired_layers_are_let_go() {
        let (store, map, mem) = history();
        let reader = LayerReader::new(&store, "layers/");
        reader.reconstruct(&map, &mem, &k(3), Lsn(310)).unwrap();
        assert_eq!(reader.footers.lock().unwrap().len(), 3);

        // What compaction leaves behind: a map naming one of the three.
        let kept = LayerMap::new(vec![map.layers()[0].clone()]).unwrap();
        assert_eq!(reader.forget_unnamed(&kept), 1);
        assert_eq!(
            reader.footers.lock().unwrap().keys().next(),
            Some(&map.layers()[0].name()),
            "the surviving footer is the one the map still names"
        );
        assert_eq!(reader.forget_unnamed(&kept), 1, "a second pass is a no op");
    }

    #[test]
    fn an_object_that_disagrees_with_its_name_is_refused() {
        let (store, map, _) = history();
        let bytes = |desc: &LayerDesc| {
            store
                .get(&format!("layers/{}", desc.name()))
                .unwrap()
                .unwrap()
                .0
        };
        let serve_as = |mut liar: LayerDesc, bytes: Vec<u8>| {
            liar.size = bytes.len() as u64;
            store
                .put(&format!("layers/{}", liar.name()), &bytes)
                .unwrap();
            let map = LayerMap::new(vec![liar]).unwrap();
            let reader = LayerReader::new(&store, "layers/");
            reader.reconstruct(&map, &Memtable::new(), &k(1), Lsn(5))
        };

        // The image layer's bytes under a delta layer's name. The
        // footer crc covers the header the reader named, so claiming
        // the wrong kind fails the crc before anything is parsed.
        let image = map.layers()[0].clone();
        assert!(matches!(
            serve_as(
                LayerDesc::delta(image.min_key, image.max_key, Lsn(1), Lsn(9)),
                bytes(&image)
            ),
            Err(ReadError::Layer {
                source: LayerDecodeError::Corrupt,
                ..
            })
        ));

        // The same kind under a name claiming a different lsn parses
        // fine and is refused on the claim itself.
        assert!(matches!(
            serve_as(
                LayerDesc::image(image.min_key, image.max_key, Lsn(4)),
                bytes(&image)
            ),
            Err(ReadError::Mismatched { .. })
        ));
    }

    #[test]
    fn a_footer_bigger_than_the_guess_costs_one_refetch_and_works() {
        // Thousands of tiny blocks blow the footer past the 64 KB
        // guess; the reader must refetch exactly and still serve.
        let store = MemStore::default();
        let entries: Vec<DeltaEntry> = (0..10_000u32)
            .map(|i| DeltaEntry {
                key: k(i),
                lsn: Lsn(100 + i as u64),
                record: vec![i as u8; 4],
            })
            .collect();
        let (buf, footer) = build_delta(&entries, 16).unwrap();
        assert!(footer.blocks.len() > 2000);
        let mut desc = LayerDesc::delta(k(0), k(9_999), Lsn(100), Lsn(100 + 9_999));
        desc.size = buf.len() as u64;
        store.put(&format!("layers/{}", desc.name()), &buf).unwrap();
        let map = LayerMap::new(vec![desc.clone()]).unwrap();
        let store = Counting {
            inner: store,
            ranges: AtomicUsize::new(0),
        };
        let reader = LayerReader::new(&store, "layers/");
        let got = reader
            .reconstruct(&map, &Memtable::new(), &k(5_000), Lsn(u64::MAX))
            .unwrap();
        assert_eq!(got.records, vec![(Lsn(100 + 5_000), vec![136u8; 4])]);
        assert_eq!(
            store.ranges.load(Ordering::Relaxed),
            3,
            "the guess, the exact refetch, and the block"
        );

        // And the reader learned: the same layer cold in a second
        // reader would pay three again, this one pays two, because a
        // footer that big is now what it asks for up front.
        assert!(reader.guess.load(Ordering::Relaxed) > FOOTER_GUESS);
        let twin = {
            let mut twin = LayerDesc::delta(k(0), k(9_999), Lsn(50), Lsn(99));
            twin.size = buf.len() as u64;
            store.put(&format!("layers/{}", twin.name()), &buf).unwrap();
            twin
        };
        // The twin is the same bytes under another name, so it fails on
        // the claim, but only after the footer parsed, which is the
        // fetch this counts.
        store.ranges.store(0, Ordering::Relaxed);
        let map = LayerMap::new(vec![twin]).unwrap();
        assert!(matches!(
            reader.reconstruct(&map, &Memtable::new(), &k(5_000), Lsn(u64::MAX)),
            Err(ReadError::Mismatched { .. })
        ));
        assert_eq!(
            store.ranges.load(Ordering::Relaxed),
            1,
            "one fetch, no refetch"
        );
    }

    #[test]
    fn a_block_is_fetched_once_however_many_keys_come_out_of_it() {
        // The image layer here cuts a block every couple of pages, so
        // 8 and 9 are one block. Reading them one after another is the
        // shape a sequential scan has, and the shape that cost 1.7
        // range GETs per page served in #465.
        let (inner, map, mem) = history();
        let store = Counting {
            inner,
            ranges: AtomicUsize::new(0),
        };
        let reader = LayerReader::new(&store, "layers/");
        let got = reader.reconstruct(&map, &mem, &k(8), Lsn(310)).unwrap();
        assert_eq!(got.base, Some(page(8)));
        store.ranges.store(0, Ordering::Relaxed);
        let got = reader.reconstruct(&map, &mem, &k(9), Lsn(310)).unwrap();
        assert_eq!(got.base, Some(page(9)));
        assert_eq!(
            store.ranges.load(Ordering::Relaxed),
            0,
            "the neighbour was already in the block the first read paid for"
        );

        // With no budget to hold it, the second read fetches the same
        // bytes again, which is what every read used to do.
        let reader = LayerReader::new(&store, "layers/").with_block_budget(0);
        reader.reconstruct(&map, &mem, &k(8), Lsn(310)).unwrap();
        store.ranges.store(0, Ordering::Relaxed);
        reader.reconstruct(&map, &mem, &k(9), Lsn(310)).unwrap();
        assert_eq!(store.ranges.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn the_block_cache_evicts_the_ones_nobody_asked_for_lately() {
        let mut cache = BlockCache::new(1000);
        for at in 0..3u64 {
            cache.put("layer", at, Arc::new(vec![0u8; 300]));
        }
        assert_eq!(cache.bytes, 900);
        // Reading the oldest makes it the newest.
        assert!(cache.get("layer", 0).is_some());
        cache.put("layer", 3, Arc::new(vec![0u8; 300]));
        assert!(cache.bytes <= 1000);
        assert!(cache.get("layer", 0).is_some(), "touched, so kept");
        assert!(cache.get("layer", 3).is_some(), "newest, so kept");
        assert!(cache.get("layer", 1).is_none());
        assert!(cache.get("layer", 2).is_none());

        // A block that cannot fit is not worth emptying the cache for.
        cache.put("layer", 4, Arc::new(vec![0u8; 2000]));
        assert!(cache.get("layer", 4).is_none());
        assert!(cache.get("layer", 0).is_some());
    }

    #[test]
    fn the_blocks_of_retired_layers_go_with_their_footers() {
        let (store, map, mem) = history();
        let reader = LayerReader::new(&store, "layers/");
        reader.reconstruct(&map, &mem, &k(3), Lsn(310)).unwrap();
        assert!(!reader.blocks.lock().unwrap().is_empty());

        let kept = LayerMap::new(vec![map.layers()[0].clone()]).unwrap();
        reader.forget_unnamed(&kept);
        let blocks = reader.blocks.lock().unwrap();
        let name = map.layers()[0].name();
        assert!(blocks.blocks.keys().all(|(held, _)| *held == name));
        assert_eq!(
            blocks.bytes,
            blocks.blocks.values().map(|c| c.bytes.len()).sum::<usize>(),
            "the byte count followed the eviction"
        );
    }

    /// A store that counts range reads, so a test can say how many
    /// round trips a read cost.
    struct Counting {
        inner: MemStore,
        ranges: AtomicUsize,
    }

    impl CasStore for Counting {
        fn get(&self, key: &str) -> Result<Option<(Vec<u8>, crate::cas::Version)>, CasError> {
            self.inner.get(key)
        }
        fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
            self.ranges.fetch_add(1, Ordering::Relaxed);
            self.inner.get_range(key, offset, len)
        }
        fn put_if_match(
            &self,
            key: &str,
            data: &[u8],
            expected: Option<&crate::cas::Version>,
        ) -> Result<crate::cas::Version, CasError> {
            self.inner.put_if_match(key, data, expected)
        }
        fn delete(&self, key: &str) -> Result<(), CasError> {
            self.inner.delete(key)
        }
        fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
            self.inner.list(prefix)
        }
    }
}
