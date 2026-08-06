//! The read path: reconstruct one key at one lsn (spec 04 sec 2, 5).
//!
//! A lookup plans against the [`LayerMap`], fetches only what the
//! plan names, and returns the base image plus the ordered record
//! chain the redo pool needs. Everything is range GETs: a footer is
//! two small ranges the first time and cached after, a block is one
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
//! entries is a bounded memory question, not a correctness one.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use crate::cas::{CasError, CasStore};
use crate::layer::{
    LAYER_HEADER_LEN, LayerDecodeError, LayerFooter, LayerKey, decode_delta_block,
    decode_image_block, read_layer_footer_ranges,
};
use crate::layermap::{LayerDesc, LayerMap};
use crate::lsn::Lsn;
use crate::memtable::Memtable;

/// First guess for the footer suffix fetch. Wide enough that one
/// range covers the footer of any healthy layer; a footer bigger than
/// this costs one exact refetch, not an error.
const FOOTER_GUESS: u64 = 64 * 1024;

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
    #[error("layer {name} disagrees with its name, refusing to serve from it")]
    Mismatched { name: String },
    #[error("layer {name} came back short at {offset}+{len}")]
    ShortRange { name: String, offset: u64, len: u64 },
    #[error(
        "layer {name} belongs to {owner} but this reader has no shard context to resolve it, attach with for_shard"
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
    /// The shard number, needed to resolve an inherited layer to its
    /// owner's shard prefix. A reader without it serves only maps with
    /// no owner tags.
    shard: Option<u16>,
    footers: Mutex<HashMap<String, Arc<LayerFooter>>>,
}

impl<'a> LayerReader<'a> {
    pub fn new(store: &'a dyn CasStore, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
            shard: None,
            footers: Mutex::new(HashMap::new()),
        }
    }

    /// A reader bound to one tenant's shard, able to follow owner tags
    /// into ancestor prefixes. Branched tenants must attach this way,
    /// [`LayerReader::new`] refuses their inherited layers loudly.
    pub fn for_shard(store: &'a dyn CasStore, tenant_ref: &str, shard: u16) -> Self {
        Self {
            store,
            prefix: crate::layout::TenantLayout::new(tenant_ref).shard_prefix(shard),
            shard: Some(shard),
            footers: Mutex::new(HashMap::new()),
        }
    }

    /// The object key for a layer: its name under this shard's prefix,
    /// or under the owner's shard prefix when the layer is inherited.
    /// The shard number is the same on both sides because a branch
    /// copies shard manifests one to one.
    fn object_key(&self, desc: &LayerDesc, name: &str) -> Result<String, ReadError> {
        match &desc.owner {
            None => Ok(format!("{}{}", self.prefix, name)),
            Some(owner) => match self.shard {
                Some(shard) => Ok(format!(
                    "{}{}",
                    crate::layout::TenantLayout::new(owner).shard_prefix(shard),
                    name
                )),
                None => Err(ReadError::ForeignLayer {
                    name: name.to_string(),
                    owner: owner.clone(),
                }),
            },
        }
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
        let header = self.range(desc, &name, 0, LAYER_HEADER_LEN as u64)?;
        let guess = FOOTER_GUESS.min(desc.size);
        let suffix = self.range(desc, &name, desc.size - guess, guess)?;
        let footer = match read_layer_footer_ranges(&header, &suffix, desc.size) {
            Ok(footer) => footer,
            Err(LayerDecodeError::Truncated { need, .. }) if (need as u64) > guess => {
                let exact = self.range(desc, &name, desc.size - need as u64, need as u64)?;
                read_layer_footer_ranges(&header, &exact, desc.size).map_err(layer_err)?
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

    /// The read algorithm for `(key, lsn)`: plan on the map, fetch the
    /// base image from the plan's image layer, collect records from
    /// the plan's delta layers and the memtable, merge ascending.
    /// Overlapping unconsolidated layers can hold the same record; the
    /// merge keys on lsn, so a record applies once no matter how many
    /// layers carry it.
    pub fn reconstruct(
        &self,
        map: &LayerMap,
        mem: &Memtable,
        key: &LayerKey,
        lsn: Lsn,
    ) -> Result<Reconstruction, ReadError> {
        let plan = map.plan(key, lsn);
        let floor = plan.floor();

        let mut base = None;
        if let Some(desc) = plan.image {
            let footer = self.footer(desc)?;
            if footer.may_contain(key) {
                for meta in footer.locate(key) {
                    let bytes = self.range(desc, &desc.name(), meta.offset, meta.len as u64)?;
                    let entries =
                        decode_image_block(&bytes, meta).map_err(|source| ReadError::Layer {
                            name: desc.name(),
                            source,
                        })?;
                    if let Ok(i) = entries.binary_search_by(|e| e.key.cmp(key)) {
                        base = Some(entries.into_iter().nth(i).expect("index from search").page);
                        break;
                    }
                }
            }
        }

        let mut merged: BTreeMap<Lsn, Vec<u8>> = BTreeMap::new();
        for desc in &plan.deltas {
            let footer = self.footer(desc)?;
            if !footer.may_contain(key) {
                continue;
            }
            // An inherited layer can carry records past the branch cut,
            // the owner's future; the clamp keeps them out of this
            // tenant's history.
            let ceil = desc.clamp(lsn);
            for meta in footer.locate(key) {
                let bytes = self.range(desc, &desc.name(), meta.offset, meta.len as u64)?;
                let entries =
                    decode_delta_block(&bytes, meta).map_err(|source| ReadError::Layer {
                        name: desc.name(),
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
            base_lsn: plan.image.map(|_| floor),
            base,
            records: merged.into_iter().collect(),
            layers_touched: plan.read_amp(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::{DeltaEntry, ImageEntry, PAGE_IMAGE_LEN, build_delta, build_image};
    use crate::mem::MemStore;

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
    fn an_object_that_disagrees_with_its_name_is_refused() {
        let (store, map, _) = history();
        // Serve the image layer's bytes under a delta layer's name.
        let image = &map.layers()[0];
        let bytes = store
            .get(&format!("layers/{}", image.name()))
            .unwrap()
            .unwrap()
            .0;
        let mut liar = LayerDesc::delta(image.min_key, image.max_key, Lsn(1), Lsn(9));
        liar.size = bytes.len() as u64;
        store
            .put(&format!("layers/{}", liar.name()), &bytes)
            .unwrap();
        let map = LayerMap::new(vec![liar]).unwrap();
        let reader = LayerReader::new(&store, "layers/");
        assert!(matches!(
            reader.reconstruct(&map, &Memtable::new(), &k(1), Lsn(5)),
            Err(ReadError::Mismatched { .. })
        ));
    }

    #[test]
    fn a_footer_bigger_than_the_guess_costs_one_refetch_and_works() {
        // Thousands of tiny blocks blow the footer past the 64 KB
        // guess; the reader must refetch exactly and still serve.
        let store = MemStore::default();
        let entries: Vec<DeltaEntry> = (0..30_000u32)
            .map(|i| DeltaEntry {
                key: k(i),
                lsn: Lsn(100 + i as u64),
                record: vec![i as u8; 4],
            })
            .collect();
        let (buf, footer) = build_delta(&entries, 16).unwrap();
        assert!(footer.blocks.len() > 2000);
        let mut desc = LayerDesc::delta(k(0), k(29_999), Lsn(100), Lsn(100 + 29_999));
        desc.size = buf.len() as u64;
        store.put(&format!("layers/{}", desc.name()), &buf).unwrap();
        let map = LayerMap::new(vec![desc]).unwrap();
        let reader = LayerReader::new(&store, "layers/");
        let got = reader
            .reconstruct(&map, &Memtable::new(), &k(12_345), Lsn(u64::MAX))
            .unwrap();
        assert_eq!(got.records, vec![(Lsn(100 + 12_345), vec![57u8; 4])]);
    }
}
