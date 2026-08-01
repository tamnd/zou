//! Immutability guard over any CAS store.
//!
//! Everything in a tenant prefix except MANIFEST is written exactly once.
//! The backends already refuse blind creates of existing keys, but a
//! versioned put_if_match could still legally overwrite a WAL segment or a
//! checkpoint, and one such write corrupts history for every branch that
//! references the object. The guard closes that hole at the trait
//! boundary: overwrites of wal/, chk/, and manifests/ keys are refused
//! before they reach the backend, no matter what version the caller holds.
//!
//! Wrap the store once at construction, everything above the trait is
//! unaffected: creates still work, manifest swaps still work, reads and
//! lists pass straight through.

use crate::cas::{CasError, CasStore, Version};

pub struct GuardedStore<S> {
    inner: S,
}

impl<S: CasStore> GuardedStore<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> S {
        self.inner
    }
}

/// True for keys that live in the write-once parts of a tenant prefix.
/// The check anchors on the layout, `tenants/<ref>/<area>/`, so a user
/// file that happens to contain "wal" in its name is not caught.
fn overwrite_forbidden(key: &str) -> bool {
    let Some(rest) = key.strip_prefix("tenants/") else {
        return false;
    };
    let Some((_tenant, rel)) = rest.split_once('/') else {
        return false;
    };
    rel.starts_with("wal/") || rel.starts_with("chk/") || rel.starts_with("manifests/")
}

impl<S: CasStore> CasStore for GuardedStore<S> {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        self.inner.get(key)
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
        self.inner.get_range(key, offset, len)
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        if expected.is_some() && overwrite_forbidden(key) {
            return Err(CasError::ImmutableOverwrite {
                key: key.to_string(),
            });
        }
        self.inner.put_if_match(key, data, expected)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        // An unconditional write can overwrite, so immutable prefixes are
        // off limits entirely. WAL and checkpoint objects go through
        // put_if_absent, which proves absence.
        if overwrite_forbidden(key) {
            return Err(CasError::ImmutableOverwrite {
                key: key.to_string(),
            });
        }
        self.inner.put(key, data)
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        // Deletes pass through: GC legitimately removes old WAL and
        // checkpoints once the safety window says so. The guard exists to
        // stop overwrites, which are the corruption hazard.
        self.inner.delete(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        self.inner.list(prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::LocalFsStore;
    use crate::layout::TenantLayout;
    use crate::lease;
    use crate::manifest::Manifest;

    fn guarded() -> (tempfile::TempDir, GuardedStore<LocalFsStore>) {
        let dir = tempfile::tempdir().unwrap();
        let store = GuardedStore::new(LocalFsStore::new(dir.path()));
        (dir, store)
    }

    #[test]
    fn a_correct_version_still_cannot_overwrite_wal_or_chk() {
        let (_d, store) = guarded();
        let t = TenantLayout::new("t1");
        for key in [
            t.chk_file("chk-1", "base/one"),
            t.checkpoint_page_index("chk-1"),
            t.checkpoint_pages("chk-1", 0),
            t.manifest_history(1, 1000),
        ] {
            let v = store.put_if_absent(&key, b"original").unwrap();
            let err = store.put_if_match(&key, b"rewrite", Some(&v)).unwrap_err();
            assert!(
                matches!(err, CasError::ImmutableOverwrite { .. }),
                "{key} was overwritable: {err}"
            );
            let (data, _) = store.get(&key).unwrap().unwrap();
            assert_eq!(data, b"original");
        }
    }

    #[test]
    fn creates_pass_through_and_double_create_still_fails() {
        let (_d, store) = guarded();
        let t = TenantLayout::new("t1");
        let key = t.chk_file("chk-1", "base/two");
        store.put_if_absent(&key, b"frame").unwrap();
        assert!(matches!(
            store.put_if_absent(&key, b"other").unwrap_err(),
            CasError::AlreadyExists { .. }
        ));
    }

    #[test]
    fn the_full_lease_protocol_works_through_the_guard() {
        let (_d, store) = guarded();
        let layout = TenantLayout::new("t1");
        store
            .put_if_absent(&layout.manifest(), &Manifest::new("t1", 18).to_json())
            .unwrap();
        let mut held = lease::acquire(&store, &layout, "node-a", 15, 1000).unwrap();
        lease::renew(&store, &layout, &mut held, 15, 1005).unwrap();
        lease::update_manifest(&store, &layout, &mut held, 1010, |_| {}).unwrap();
        lease::release(&store, &layout, held).unwrap();
    }

    #[test]
    fn keys_outside_the_write_once_areas_stay_mutable() {
        let (_d, store) = guarded();
        for key in [
            "tenants/t1/files/photos/wal/x.png",
            "scratch/anything",
            "tenants/t1/MANIFEST-backup",
        ] {
            let v = store.put_if_absent(key, b"v1").unwrap();
            store.put_if_match(key, b"v2", Some(&v)).unwrap();
        }
    }
}
