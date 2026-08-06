//! The page shard manifest: which layers a shard serves and how far
//! ingest has flushed (spec 04 sec 3 and 7).
//!
//! Layer objects are immutable and their names carry their coverage,
//! so the manifest is the only mutable object a shard owns. It lists
//! the live layers and carries `disk_consistent_lsn`, the point up to
//! which every record of the shard's stripe sits in a published layer.
//! Publishing a flushed layer is one CAS: a reader either sees the
//! layer listed or does not, there is no window where a half flushed
//! object serves reads. An object in the shard prefix without a
//! manifest entry is an orphan from a crashed flush and compaction gc
//! sweeps it.
//!
//! Small human readable JSON on purpose, same reasoning as the tenant
//! manifest: debugging a shard should start with curl and eyes.

use serde::{Deserialize, Serialize};

use crate::cas::{CasError, CasStore, Version};
use crate::layermap::{LayerDesc, LayerMap, LayerMapError, LayerNameError};
use crate::lsn::Lsn;

/// Current shard manifest format. Readers refuse anything newer with a
/// clear error instead of misreading it.
pub const PAGE_SHARD_FORMAT: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum PageShardError {
    #[error(
        "page shard manifest format {found} is newer than this binary supports ({PAGE_SHARD_FORMAT}), upgrade zou"
    )]
    FormatTooNew { found: u32 },
    #[error("invalid page shard manifest json: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Store(#[from] CasError),
    #[error("layer name in manifest: {0}")]
    Name(#[from] LayerNameError),
    #[error("layer set in manifest: {0}")]
    Map(#[from] LayerMapError),
}

/// One published layer: the name is the coverage, the size lets a
/// reader plan its footer range reads without a stat call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayerEntry {
    pub name: String,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageShardManifest {
    pub format: u32,
    pub shard: u16,
    /// Every record at or below this lsn belonging to the shard's
    /// stripe is in a published layer. A restarted ingest replays the
    /// sealed WAL from here, nothing below it is ever needed again.
    pub disk_consistent_lsn: Lsn,
    pub layers: Vec<LayerEntry>,
}

impl PageShardManifest {
    pub fn new(shard: u16) -> Self {
        PageShardManifest {
            format: PAGE_SHARD_FORMAT,
            shard,
            disk_consistent_lsn: Lsn(0),
            layers: Vec::new(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec_pretty(self).expect("shard manifest serializes")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, PageShardError> {
        let m: PageShardManifest = serde_json::from_slice(bytes)?;
        if m.format > PAGE_SHARD_FORMAT {
            return Err(PageShardError::FormatTooNew { found: m.format });
        }
        Ok(m)
    }

    pub fn load(
        store: &dyn CasStore,
        key: &str,
    ) -> Result<Option<(Self, Version)>, PageShardError> {
        match store.get(key)? {
            None => Ok(None),
            Some((bytes, version)) => Ok(Some((Self::decode(&bytes)?, version))),
        }
    }

    /// The layer map over the listed layers, names parsed back into
    /// coverage. The parse cannot drift from the objects because flush
    /// derives names from footers and the reader verifies footers
    /// against names.
    pub fn layer_map(&self) -> Result<LayerMap, PageShardError> {
        let descs = self
            .layers
            .iter()
            .map(|l| LayerDesc::parse(&l.name, l.size))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(LayerMap::new(descs)?)
    }
}

/// Publish one flushed layer: CAS the manifest to list it and advance
/// `disk_consistent_lsn`. A version conflict re reads and retries,
/// which is safe because the merge is idempotent: a crash retry that
/// finds its layer already listed only advances the lsn, and the lsn
/// only moves forward. Returns the manifest as it was published.
pub fn publish_layer(
    store: &dyn CasStore,
    key: &str,
    shard: u16,
    entry: &LayerEntry,
    disk_consistent_lsn: Lsn,
) -> Result<PageShardManifest, PageShardError> {
    loop {
        let (mut m, version) = match PageShardManifest::load(store, key)? {
            Some((m, v)) => (m, Some(v)),
            None => (PageShardManifest::new(shard), None),
        };
        if !m.layers.iter().any(|l| l.name == entry.name) {
            m.layers.push(entry.clone());
        }
        m.disk_consistent_lsn = m.disk_consistent_lsn.max(disk_consistent_lsn);
        match store.put_if_match(key, &m.encode(), version.as_ref()) {
            Ok(_) => return Ok(m),
            Err(CasError::Conflict { .. }) | Err(CasError::AlreadyExists { .. }) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::{DeltaEntry, LayerKey, build_delta};
    use crate::mem::MemStore;

    fn entry_from_built(blocks: &[u32], lsns: &[u64]) -> LayerEntry {
        let mut entries = Vec::new();
        for &b in blocks {
            for &l in lsns {
                entries.push(DeltaEntry {
                    key: LayerKey::page(1663, 5, 16384, 0, b),
                    lsn: Lsn(l),
                    record: vec![b as u8; 32],
                });
            }
        }
        let (bytes, footer) = build_delta(&entries, 4096).unwrap();
        let desc = LayerDesc::from_footer(&footer, bytes.len() as u64);
        LayerEntry {
            name: desc.name(),
            size: bytes.len() as u64,
        }
    }

    #[test]
    fn the_manifest_round_trips_and_refuses_newer_formats() {
        let mut m = PageShardManifest::new(3);
        m.disk_consistent_lsn = Lsn(0x1234_5678);
        m.layers.push(entry_from_built(&[1, 2], &[10, 20]));
        let back = PageShardManifest::decode(&m.encode()).unwrap();
        assert_eq!(back, m);

        let mut newer = m.clone();
        newer.format = PAGE_SHARD_FORMAT + 1;
        let err = PageShardManifest::decode(&newer.encode()).unwrap_err();
        assert!(matches!(err, PageShardError::FormatTooNew { .. }));
    }

    #[test]
    fn publishing_is_idempotent_and_survives_races() {
        let store = MemStore::default();
        let key = "tenants/t/shards/0003/SHARD";
        let a = entry_from_built(&[1], &[10, 20]);
        let b = entry_from_built(&[5], &[30, 40]);

        publish_layer(&store, key, 3, &a, Lsn(20)).unwrap();
        // A crash retry republishes the same layer: no duplicate entry,
        // the lsn never moves backwards.
        publish_layer(&store, key, 3, &a, Lsn(15)).unwrap();
        let m = publish_layer(&store, key, 3, &b, Lsn(40)).unwrap();
        assert_eq!(m.layers, vec![a, b]);
        assert_eq!(m.disk_consistent_lsn, Lsn(40));

        let (loaded, _) = PageShardManifest::load(&store, key).unwrap().unwrap();
        assert_eq!(loaded, m);
        let map = loaded.layer_map().unwrap();
        assert_eq!(map.layers().len(), 2);
    }
}
