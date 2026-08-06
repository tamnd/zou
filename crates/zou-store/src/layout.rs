//! Typed keys for the tenant prefix layout.
//!
//! ```text
//! tenants/<ref>/
//!   MANIFEST                      current manifest, swapped with CAS
//!   manifests/<epoch>-<unix>.json manifest history
//!   chk/<chk-id>/INDEX            fs capture index for one checkpoint
//!   chk/<chk-id>/fs/<path>        captured files
//!   chk/<chk-id>/PAGES            page run index for one checkpoint
//!   chk/<chk-id>/<n>.pages        sorted page images
//!   shards/<shard>/d-….dl         delta layers, WAL records by key
//!   shards/<shard>/i-….il         image layers, pages at one lsn
//!   files/<bucket>/<key>          Storage API user files
//! ```
//!
//! Everything except `MANIFEST` is immutable once written.

use crate::layer::LayerKey;
use crate::lsn::Lsn;
use sha2::{Digest, Sha256};

/// Deterministic 128-bit tenant id for the shared WAL: the first 16 bytes
/// of sha256 over the tenant ref, big endian. Frames in the store carry
/// this id forever, so the mapping must never change.
pub fn tenant_id(tenant_ref: &str) -> u128 {
    let digest = Sha256::digest(tenant_ref.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    u128::from_be_bytes(bytes)
}

/// Key builder for one tenant's prefix.
///
/// Keys are relative to the store root, so the same layout works on S3,
/// GCS, R2, MinIO, and a local directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantLayout {
    prefix: String,
}

impl TenantLayout {
    pub fn new(tenant_ref: &str) -> Self {
        Self {
            prefix: format!("tenants/{tenant_ref}"),
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The tenant ref the layout was built from.
    pub fn tenant_ref(&self) -> &str {
        self.prefix.strip_prefix("tenants/").unwrap_or(&self.prefix)
    }

    /// The single mutable object, the root of truth.
    pub fn manifest(&self) -> String {
        format!("{}/MANIFEST", self.prefix)
    }

    /// Historical manifest snapshot, one per state changing publish,
    /// at most one per second. The epoch leads so the listing sorts in
    /// write order, the unix stamp disambiguates the many publishes one
    /// epoch makes and is what PITR picks a snapshot by.
    pub fn manifest_history(&self, epoch: u64, unix: u64) -> String {
        format!("{}/manifests/{epoch:016}-{unix:016}.json", self.prefix)
    }

    /// The whole history prefix, listed by PITR and swept by gc past
    /// the retention window.
    pub fn manifests_dir(&self) -> String {
        format!("{}/manifests/", self.prefix)
    }

    /// One file inside a checkpoint's filesystem capture. Checkpoints are
    /// immutable, these go through put_if_absent.
    pub fn chk_file(&self, id: &str, relpath: &str) -> String {
        format!("{}/chk/{id}/fs/{relpath}", self.prefix)
    }

    /// The index object of a checkpoint capture: which files and empty
    /// directories make up the tree, so a restore can rebuild it exactly.
    pub fn chk_index(&self, id: &str) -> String {
        format!("{}/chk/{id}/INDEX", self.prefix)
    }

    /// The WAL slice a fold captures alongside a checkpoint, from the
    /// redo page boundary through the end of the checkpoint record, so
    /// an attach with no stream to overlay, a branch child or a time
    /// travel restore, still finds the record recovery anchors on.
    pub fn chk_waltail(&self, id: &str) -> String {
        format!("{}/chk/{id}/WALTAIL", self.prefix)
    }

    /// One relation page. Mutable derived data, the durable truth for
    /// these bytes is WAL plus checkpoints, so pg/ sits outside the
    /// immutable prefixes on purpose.
    pub fn pg_block(&self, spc: u32, db: u32, rel: u32, fork: u32, blk: u32) -> String {
        format!("{}/{blk:08X}", self.pg_fork_prefix(spc, db, rel, fork))
    }

    /// Fork size marker. Presence means the fork exists, content is the
    /// block count. Blocks at or past the size are logically absent even
    /// if an object lingers, which mirrors file length semantics.
    pub fn pg_size(&self, spc: u32, db: u32, rel: u32, fork: u32) -> String {
        format!("{}/SIZE", self.pg_fork_prefix(spc, db, rel, fork))
    }

    pub fn pg_fork_prefix(&self, spc: u32, db: u32, rel: u32, fork: u32) -> String {
        format!("{}/pg/{spc}/{db}/{rel}/{fork}", self.prefix)
    }

    /// The whole relation page prefix. A full checkpoint lists this to
    /// pack every page into its runs.
    pub fn pg_dir(&self) -> String {
        format!("{}/pg/", self.prefix)
    }

    /// The page run index of a checkpoint: which blocks its `.pages`
    /// objects hold and in what order. Distinct from the fs capture
    /// INDEX, which describes captured files.
    pub fn checkpoint_page_index(&self, chk_id: &str) -> String {
        format!("{}/chk/{chk_id}/PAGES", self.prefix)
    }

    pub fn checkpoint_pages(&self, chk_id: &str, n: u32) -> String {
        format!("{}/chk/{chk_id}/{n:08}.pages", self.prefix)
    }

    /// A Storage API user file.
    pub fn file(&self, bucket: &str, key: &str) -> String {
        format!("{}/files/{bucket}/{key}", self.prefix)
    }

    /// One shard of the layer store. Listed on attach to load the
    /// shard's layer map, swept by compaction gc.
    pub fn shard_prefix(&self, shard: u16) -> String {
        format!("{}/shards/{shard:04x}/", self.prefix)
    }

    /// All shard prefixes at once, for the operations that walk every
    /// shard the tenant has: branching and gc.
    pub fn shards_dir(&self) -> String {
        format!("{}/shards/", self.prefix)
    }

    /// The shard's manifest: the live layer list and the flush
    /// watermark, the one mutable object in the shard prefix. CAS
    /// swapped on every flush and compaction publish.
    pub fn shard_manifest(&self, shard: u16) -> String {
        format!("{}SHARD", self.shard_prefix(shard))
    }

    /// A delta layer object. The name is the coverage: the key range
    /// and lsn range the layer holds, so a listing alone rebuilds the
    /// layer map without opening anything.
    pub fn delta_layer(
        &self,
        shard: u16,
        key_min: &LayerKey,
        key_max: &LayerKey,
        lsn_min: Lsn,
        lsn_max: Lsn,
    ) -> String {
        format!(
            "{}d-{}-{}-{:016x}-{:016x}.dl",
            self.shard_prefix(shard),
            key_min.hex(),
            key_max.hex(),
            lsn_min.0,
            lsn_max.0
        )
    }

    /// An image layer object: every page in the key range materialized
    /// at one lsn.
    pub fn image_layer(
        &self,
        shard: u16,
        key_min: &LayerKey,
        key_max: &LayerKey,
        lsn: Lsn,
    ) -> String {
        format!(
            "{}i-{}-{}-{:016x}.il",
            self.shard_prefix(shard),
            key_min.hex(),
            key_max.hex(),
            lsn.0
        )
    }

    /// Whether a key must never be overwritten. The tenant manifest
    /// and the per shard manifests are the only mutable objects in the
    /// prefix.
    pub fn is_immutable(&self, key: &str) -> bool {
        key.starts_with(&self.prefix)
            && key != self.manifest()
            && !(key.ends_with("/SHARD") && key.starts_with(&format!("{}/shards/", self.prefix)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_match_the_documented_layout() {
        let t = TenantLayout::new("acme-prod");
        assert_eq!(t.manifest(), "tenants/acme-prod/MANIFEST");
        assert_eq!(
            t.manifest_history(42, 1_767_100_000),
            "tenants/acme-prod/manifests/0000000000000042-0000001767100000.json"
        );
        assert_eq!(t.manifests_dir(), "tenants/acme-prod/manifests/");
        assert_eq!(
            t.checkpoint_page_index("chk-000121"),
            "tenants/acme-prod/chk/chk-000121/PAGES"
        );
        assert_eq!(
            t.checkpoint_pages("chk-000121", 3),
            "tenants/acme-prod/chk/chk-000121/00000003.pages"
        );
        assert_eq!(
            t.file("avatars", "u1/pic.png"),
            "tenants/acme-prod/files/avatars/u1/pic.png"
        );
        assert_eq!(t.shard_prefix(3), "tenants/acme-prod/shards/0003/");
        assert_eq!(t.shard_manifest(3), "tenants/acme-prod/shards/0003/SHARD");
        let lo = LayerKey::page(1663, 5, 16384, 0, 0);
        let hi = LayerKey::page(1663, 5, 16384, 0, 8191);
        assert_eq!(
            t.delta_layer(0, &lo, &hi, Lsn(0x16d69a8), Lsn(0x1a03b20)),
            "tenants/acme-prod/shards/0000/d-000000067f00000005000040000000000000-00000006\
             7f00000005000040000000001fff-00000000016d69a8-0000000001a03b20.dl"
        );
        assert_eq!(
            t.image_layer(0, &lo, &hi, Lsn(0x16d69a8)),
            "tenants/acme-prod/shards/0000/i-000000067f00000005000040000000000000-00000006\
             7f00000005000040000000001fff-00000000016d69a8.il"
        );
    }

    #[test]
    fn tenant_ids_are_pinned_forever() {
        // Frames in stores carry these ids, so the mapping must never
        // drift. If this test fails the change orphans every stream.
        assert_eq!(tenant_id("local"), 0x25bf8e1a2393f1108d37029b3df55932);
        assert_eq!(tenant_id("acme-prod"), 0xd2836b7de9447c4aa93c2d1dc4328c15);
        assert_ne!(tenant_id("a"), tenant_id("b"));
    }

    #[test]
    fn only_the_manifests_are_mutable() {
        let t = TenantLayout::new("acme");
        assert!(!t.is_immutable(&t.manifest()));
        assert!(!t.is_immutable(&t.shard_manifest(7)));
        assert!(t.is_immutable(&t.checkpoint_page_index("chk-1")));
        assert!(t.is_immutable(&t.manifest_history(1, 1000)));
        let k = LayerKey::page(1, 1, 1, 0, 0);
        assert!(t.is_immutable(&t.delta_layer(0, &k, &k, Lsn(1), Lsn(2))));
        assert!(t.is_immutable(&t.image_layer(0, &k, &k, Lsn(1))));
        assert!(!t.is_immutable("tenants/other/MANIFEST"));
    }
}
