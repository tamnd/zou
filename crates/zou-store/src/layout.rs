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
//!   files/…                       Storage API object bytes
//!   functions/DEPLOYED            what is deployed under /functions/v1
//!   functions/blobs/<sha256>      the files those functions are made of
//!   functions/SECRETS             those functions' environment, sealed
//! ```
//!
//! Everything except `MANIFEST`, `functions/DEPLOYED` and
//! `functions/SECRETS` is immutable once written.

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

    /// Where this tenant's WAL chain lives.
    ///
    /// A chain has one writer and fences everyone else off it, and the
    /// writer today is the postmaster that has the tenant attached. A
    /// node serving a fleet runs one of those per project, so a chain
    /// shared between projects is a chain they take from each other
    /// forever. Scoping it to the tenant is what makes the one writer
    /// rule hold on a node with more than one project attached; a cell
    /// wide log with a sequencer of its own is the later shape, and it
    /// is a different writer, not a different tenant.
    pub fn log_prefix(&self) -> String {
        format!("{}/log", self.prefix)
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

    /// Where the Storage API's object bytes go.
    ///
    /// A prefix rather than a key builder, because what goes under it
    /// is not this crate's business: the storage api keys bytes by the
    /// row id and version it wrote next to them, and none of that means
    /// anything here. What the layout owns is that they are under the
    /// tenant, so one tenant's bytes are not reachable from another's
    /// even by a bug, and so removing a tenant is removing a prefix.
    ///
    /// Note that gc does not walk this. It collects unpinned
    /// checkpoints and expired history and nothing else, so a file is
    /// only ever removed by the request that removed its row.
    pub fn files_prefix(&self) -> String {
        format!("{}/files/", self.prefix)
    }

    /// What is deployed under `/functions/v1` for this tenant: the
    /// names, what each one starts at, and the sha256 of every file it
    /// is made of.
    ///
    /// The second mutable object in the prefix, swapped with CAS by a
    /// deploy and read by a node bringing the tenant up. It is not part
    /// of the database manifest on purpose: a deploy happens from a
    /// laptop while a postmaster holds the writer lease, and the two
    /// must not be able to overwrite each other's work.
    pub fn functions_manifest(&self) -> String {
        format!("{}/functions/DEPLOYED", self.prefix)
    }

    /// One file of a deployment, keyed by the sha256 of its bytes, so a
    /// redeploy that changed one file writes one object and a rollback
    /// is a manifest that names the older shas.
    pub fn functions_blob(&self, sha: &str) -> String {
        format!("{}/functions/blobs/{sha}", self.prefix)
    }

    /// Everything a deployment is, for removing a tenant's functions
    /// without touching its database.
    pub fn functions_prefix(&self) -> String {
        format!("{}/functions/", self.prefix)
    }

    /// The tenant's function secrets, sealed. One object holding every
    /// name and value, encrypted with a key this store does not have a
    /// copy of, so a bucket somebody walked off with is names and
    /// values they cannot read.
    ///
    /// The third mutable object in the prefix, swapped with CAS the
    /// same way the other two are, because `zou secrets set` is a read
    /// and a write of the whole map.
    pub fn functions_secrets(&self) -> String {
        format!("{}/functions/SECRETS", self.prefix)
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

    /// Whether a key must never be overwritten. The tenant manifest,
    /// the per shard manifests, the deployed functions and their
    /// secrets are the only mutable objects in the prefix.
    pub fn is_immutable(&self, key: &str) -> bool {
        key.starts_with(&self.prefix)
            && key != self.manifest()
            && key != self.functions_manifest()
            && key != self.functions_secrets()
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
        assert_eq!(t.files_prefix(), "tenants/acme-prod/files/");
        assert_eq!(
            t.functions_manifest(),
            "tenants/acme-prod/functions/DEPLOYED"
        );
        assert_eq!(
            t.functions_blob("6f2a"),
            "tenants/acme-prod/functions/blobs/6f2a"
        );
        assert_eq!(t.functions_secrets(), "tenants/acme-prod/functions/SECRETS");
        assert_eq!(t.functions_prefix(), "tenants/acme-prod/functions/");
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
        assert!(!t.is_immutable(&t.functions_manifest()));
        assert!(!t.is_immutable(&t.functions_secrets()));
        assert!(t.is_immutable(&t.functions_blob("6f2a")));
        assert!(t.is_immutable(&t.checkpoint_page_index("chk-1")));
        assert!(t.is_immutable(&t.manifest_history(1, 1000)));
        let k = LayerKey::page(1, 1, 1, 0, 0);
        assert!(t.is_immutable(&t.delta_layer(0, &k, &k, Lsn(1), Lsn(2))));
        assert!(t.is_immutable(&t.image_layer(0, &k, &k, Lsn(1))));
        assert!(!t.is_immutable("tenants/other/MANIFEST"));
    }
}
