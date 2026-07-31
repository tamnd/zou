//! Typed keys for the tenant prefix layout.
//!
//! ```text
//! tenants/<ref>/
//!   MANIFEST                      current manifest, swapped with CAS
//!   manifests/<epoch>-<unix>.json manifest history
//!   wal/<epoch>/<start-lsn>.wal   sealed WAL segments, immutable
//!   chk/<chk-id>/INDEX            fs capture index for one checkpoint
//!   chk/<chk-id>/fs/<path>        captured files
//!   chk/<chk-id>/PAGES            page run index for one checkpoint
//!   chk/<chk-id>/<n>.pages        sorted page images
//!   files/<bucket>/<key>          Storage API user files
//! ```
//!
//! Everything except `MANIFEST` is immutable once written.

use crate::lsn::Lsn;

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

    /// A sealed WAL segment. Segments live under the epoch that wrote them,
    /// which is what makes zombie writers harmless: a stale epoch directory
    /// is simply never referenced by the live manifest.
    pub fn wal_segment(&self, epoch: u64, start: Lsn) -> String {
        format!("{}/wal/{epoch:016}/{:016X}.wal", self.prefix, start.0)
    }

    pub fn wal_epoch_dir(&self, epoch: u64) -> String {
        format!("{}/wal/{epoch:016}/", self.prefix)
    }

    /// The whole WAL prefix across every epoch. Recovery scans this,
    /// because acked frames can postdate the last manifest publish.
    pub fn wal_dir(&self) -> String {
        format!("{}/wal/", self.prefix)
    }

    /// Resolve an epoch qualified segment name from manifest.wal_tail,
    /// like `0000000000000007/000000000B000000.wal`, back to its key.
    pub fn wal_segment_path(&self, qualified: &str) -> String {
        format!("{}/wal/{qualified}", self.prefix)
    }

    /// One file inside a checkpoint's filesystem capture. Checkpoints are
    /// immutable, these go through put_new.
    pub fn chk_file(&self, id: &str, relpath: &str) -> String {
        format!("{}/chk/{id}/fs/{relpath}", self.prefix)
    }

    /// The index object of a checkpoint capture: which files and empty
    /// directories make up the tree, so a restore can rebuild it exactly.
    pub fn chk_index(&self, id: &str) -> String {
        format!("{}/chk/{id}/INDEX", self.prefix)
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

    /// Whether a key must never be overwritten. The manifest is the only
    /// mutable object in the prefix.
    pub fn is_immutable(&self, key: &str) -> bool {
        key.starts_with(&self.prefix) && key != self.manifest()
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
            t.wal_segment(42, Lsn(0x8B00_0000)),
            "tenants/acme-prod/wal/0000000000000042/000000008B000000.wal"
        );
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
    }

    #[test]
    fn wal_keys_sort_by_lsn_within_an_epoch() {
        let t = TenantLayout::new("a");
        let a = t.wal_segment(7, Lsn(0x0FFF));
        let b = t.wal_segment(7, Lsn(0x1000));
        let c = t.wal_segment(7, Lsn(0x0001_0000_0000));
        assert!(a < b && b < c);
    }

    #[test]
    fn only_the_manifest_is_mutable() {
        let t = TenantLayout::new("acme");
        assert!(!t.is_immutable(&t.manifest()));
        assert!(t.is_immutable(&t.wal_segment(1, Lsn(0))));
        assert!(t.is_immutable(&t.checkpoint_page_index("chk-1")));
        assert!(t.is_immutable(&t.manifest_history(1, 1000)));
        assert!(!t.is_immutable("tenants/other/MANIFEST"));
    }
}
