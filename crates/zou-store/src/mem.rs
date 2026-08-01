//! In memory CAS backend.
//!
//! A real [`CasStore`] over a map: linearizable CAS under one mutex,
//! versions from a monotonic counter, delete idempotent, list sorted.
//! It exists for deterministic protocol simulation and fast tests,
//! where the filesystem backend's syscalls and tempdirs are pure
//! overhead. Nothing survives the process, so nothing durable may ever
//! be built on it.

use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::cas::{CasError, CasStore, Version};

#[derive(Default)]
pub struct MemStore {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    objects: BTreeMap<String, (Vec<u8>, Version)>,
    counter: u64,
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// How many objects exist right now, for tests asserting GC really
    /// deleted things.
    pub fn object_count(&self) -> usize {
        self.inner.lock().unwrap().objects.len()
    }
}

impl CasStore for MemStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        Ok(self.inner.lock().unwrap().objects.get(key).cloned())
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        let mut inner = self.inner.lock().unwrap();
        let current = inner.objects.get(key).map(|(_, v)| v.clone());
        match (expected, current) {
            (None, Some(_)) => {
                return Err(CasError::Conflict {
                    key: key.to_string(),
                });
            }
            (Some(_), None) => {
                return Err(CasError::Conflict {
                    key: key.to_string(),
                });
            }
            (Some(want), Some(have)) if *want != have => {
                return Err(CasError::Conflict {
                    key: key.to_string(),
                });
            }
            _ => {}
        }
        inner.counter += 1;
        let version = Version::from_backend(inner.counter.to_string());
        inner
            .objects
            .insert(key.to_string(), (data.to_vec(), version.clone()));
        Ok(version)
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        self.inner.lock().unwrap().objects.remove(key);
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .objects
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cas_semantics_match_the_contract() {
        let store = MemStore::new();
        let v1 = store.put_if_absent("a", b"one").unwrap();
        assert!(matches!(
            store.put_if_absent("a", b"two"),
            Err(CasError::AlreadyExists { .. })
        ));
        assert!(matches!(
            store.put_if_match("a", b"two", None),
            Err(CasError::Conflict { .. })
        ));
        let v2 = store.put_if_match("a", b"two", Some(&v1)).unwrap();
        assert_ne!(v1, v2);
        assert!(matches!(
            store.put_if_match("a", b"three", Some(&v1)),
            Err(CasError::Conflict { .. })
        ));
        assert!(matches!(
            store.put_if_match("missing", b"x", Some(&v1)),
            Err(CasError::Conflict { .. })
        ));
        let (data, v) = store.get("a").unwrap().unwrap();
        assert_eq!((data.as_slice(), &v), (b"two".as_slice(), &v2));
    }

    #[test]
    fn delete_is_idempotent_and_list_is_sorted_by_prefix() {
        let store = MemStore::new();
        store.put_if_absent("wal/2", b"x").unwrap();
        store.put_if_absent("wal/1", b"x").unwrap();
        store.put_if_absent("chk/1", b"x").unwrap();
        assert_eq!(store.list("wal/").unwrap(), vec!["wal/1", "wal/2"]);
        store.delete("wal/1").unwrap();
        store.delete("wal/1").unwrap();
        assert_eq!(store.list("wal/").unwrap(), vec!["wal/2"]);
        assert_eq!(store.object_count(), 2);
    }

    #[test]
    fn range_reads_clamp_like_the_trait_promises() {
        let store = MemStore::new();
        store.put_if_absent("a", b"0123456789").unwrap();
        assert_eq!(store.get_range("a", 2, 3).unwrap().unwrap(), b"234");
        assert_eq!(store.get_range("a", 8, 100).unwrap().unwrap(), b"89");
        assert!(store.get_range("a", 100, 1).unwrap().unwrap().is_empty());
        assert!(store.get_range("missing", 0, 1).unwrap().is_none());
    }
}
