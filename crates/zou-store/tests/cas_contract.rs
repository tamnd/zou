//! Contract tests every CAS backend must pass. When the S3, GCS, and R2
//! backends land they get wired into the same `run_contract` entry point.

use std::sync::Arc;

use zou_store::{CasError, CasStore, LocalFsStore};

fn run_contract(store: Arc<dyn CasStore>) {
    missing_key_reads_as_none(&*store);
    create_requires_absence(&*store);
    swap_requires_the_current_version(&*store);
    immutable_objects_cannot_be_overwritten(&*store);
    unconditional_put_overwrites_and_delete_removes(&*store);
    list_returns_sorted_keys_under_a_prefix(&*store);
    ranged_reads_clamp_to_the_object(&*store);
    concurrent_swappers_never_lose_an_update(store);
}

fn ranged_reads_clamp_to_the_object(store: &dyn CasStore) {
    let key = "contract/range/obj";
    store.put(key, b"0123456789").unwrap();
    assert_eq!(store.get_range(key, 0, 4).unwrap().unwrap(), b"0123");
    assert_eq!(store.get_range(key, 4, 4).unwrap().unwrap(), b"4567");
    // Ranges past the end come back short or empty, never an error.
    assert_eq!(store.get_range(key, 8, 100).unwrap().unwrap(), b"89");
    assert_eq!(store.get_range(key, 100, 4).unwrap().unwrap(), b"");
    assert!(
        store
            .get_range("contract/range/missing", 0, 4)
            .unwrap()
            .is_none()
    );
}

fn unconditional_put_overwrites_and_delete_removes(store: &dyn CasStore) {
    let key = "contract/mutable/page";
    store.put(key, b"v1").unwrap();
    store.put(key, b"v2").unwrap();
    assert_eq!(store.get(key).unwrap().unwrap().0, b"v2");
    store.delete(key).unwrap();
    assert!(store.get(key).unwrap().is_none());
    // Deleting a missing key succeeds, retries are harmless.
    store.delete(key).unwrap();
}

fn missing_key_reads_as_none(store: &dyn CasStore) {
    assert!(store.get("contract/none/missing").unwrap().is_none());
}

fn create_requires_absence(store: &dyn CasStore) {
    let key = "contract/create/obj";
    let v1 = store.put_if_match(key, b"one", None).unwrap();
    let (data, v) = store.get(key).unwrap().unwrap();
    assert_eq!(data, b"one");
    assert_eq!(v, v1);
    // A second unconditional create must conflict, not overwrite.
    assert!(matches!(
        store.put_if_match(key, b"two", None),
        Err(CasError::Conflict { .. })
    ));
    assert_eq!(store.get(key).unwrap().unwrap().0, b"one");
}

fn swap_requires_the_current_version(store: &dyn CasStore) {
    let key = "contract/swap/obj";
    let v1 = store.put_if_match(key, b"a", None).unwrap();
    let v2 = store.put_if_match(key, b"b", Some(&v1)).unwrap();
    assert_ne!(v1, v2);
    // Swapping against the stale version fails and changes nothing.
    assert!(matches!(
        store.put_if_match(key, b"c", Some(&v1)),
        Err(CasError::Conflict { .. })
    ));
    assert_eq!(store.get(key).unwrap().unwrap().0, b"b");
}

fn immutable_objects_cannot_be_overwritten(store: &dyn CasStore) {
    let key = "contract/immutable/wal-seg";
    store.put_new(key, b"frames").unwrap();
    assert!(matches!(
        store.put_new(key, b"other"),
        Err(CasError::AlreadyExists { .. })
    ));
    assert_eq!(store.get(key).unwrap().unwrap().0, b"frames");
}

fn list_returns_sorted_keys_under_a_prefix(store: &dyn CasStore) {
    store.put_new("contract/list/b", b"2").unwrap();
    store.put_new("contract/list/a", b"1").unwrap();
    store.put_new("contract/list/sub/c", b"3").unwrap();
    store.put_new("contract/other/x", b"4").unwrap();
    assert_eq!(
        store.list("contract/list/").unwrap(),
        vec!["contract/list/a", "contract/list/b", "contract/list/sub/c"]
    );
}

/// The classic CAS counter: N threads each add 1 through a read-modify-swap
/// loop. If the backend ever lets a stale swap through, the final count
/// comes up short.
fn concurrent_swappers_never_lose_an_update(store: Arc<dyn CasStore>) {
    const THREADS: usize = 8;
    const INCREMENTS: usize = 25;
    let key = "contract/counter/obj";
    store.put_if_match(key, b"0", None).unwrap();

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                for _ in 0..INCREMENTS {
                    loop {
                        let (data, version) = store.get(key).unwrap().unwrap();
                        let n: u64 = String::from_utf8(data).unwrap().parse().unwrap();
                        let next = (n + 1).to_string();
                        match store.put_if_match(key, next.as_bytes(), Some(&version)) {
                            Ok(_) => break,
                            Err(CasError::Conflict { .. }) => continue,
                            Err(e) => panic!("unexpected error: {e}"),
                        }
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let (data, _) = store.get(key).unwrap().unwrap();
    let n: u64 = String::from_utf8(data).unwrap().parse().unwrap();
    assert_eq!(n as usize, THREADS * INCREMENTS);
}

#[test]
fn local_fs_backend_passes_the_contract() {
    let dir = tempfile::tempdir().unwrap();
    run_contract(Arc::new(LocalFsStore::new(dir.path())));
}

/// Prefixes every key with a per-run nonce so the contract's fixed key
/// names never collide across runs against a long lived bucket.
#[cfg(feature = "s3")]
struct PrefixedStore {
    inner: zou_store::S3Store,
    prefix: String,
}

#[cfg(feature = "s3")]
impl CasStore for PrefixedStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, zou_store::Version)>, CasError> {
        self.inner.get(&format!("{}{key}", self.prefix))
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&zou_store::Version>,
    ) -> Result<zou_store::Version, CasError> {
        self.inner
            .put_if_match(&format!("{}{key}", self.prefix), data, expected)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<zou_store::Version, CasError> {
        self.inner.put(&format!("{}{key}", self.prefix), data)
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
        self.inner
            .get_range(&format!("{}{key}", self.prefix), offset, len)
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        self.inner.delete(&format!("{}{key}", self.prefix))
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        let keys = self.inner.list(&format!("{}{prefix}", self.prefix))?;
        Ok(keys
            .into_iter()
            .filter_map(|k| k.strip_prefix(&self.prefix).map(str::to_string))
            .collect())
    }
}

/// Runs against any S3 compatible endpoint, MinIO in CI. Skips unless
/// ZOU_S3_TEST_ENDPOINT is set so plain `cargo test` stays offline.
#[cfg(feature = "s3")]
#[test]
fn s3_backend_passes_the_contract() {
    let Ok(endpoint) = std::env::var("ZOU_S3_TEST_ENDPOINT") else {
        eprintln!("skipping: ZOU_S3_TEST_ENDPOINT not set");
        return;
    };
    let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set"));
    let dialect = match std::env::var("ZOU_S3_TEST_DIALECT").as_deref() {
        Ok("gcs") => zou_store::Dialect::Gcs,
        _ => zou_store::Dialect::S3,
    };
    let store = zou_store::S3Store::new(zou_store::S3Config {
        endpoint,
        region: std::env::var("ZOU_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into()),
        bucket: var("ZOU_S3_TEST_BUCKET"),
        access_key: var("ZOU_S3_TEST_ACCESS_KEY"),
        secret_key: var("ZOU_S3_TEST_SECRET_KEY"),
        dialect,
    });
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    run_contract(Arc::new(PrefixedStore {
        inner: store,
        prefix: format!("contract-runs/{nonce:x}/"),
    }));
}
