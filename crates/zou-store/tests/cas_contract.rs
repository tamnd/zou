//! Contract tests every CAS backend must pass. When the S3, GCS, and R2
//! backends land they get wired into the same `run_contract` entry point.

use std::sync::Arc;

use zou_store::{CasError, CasStore, LocalFsStore};

fn run_contract(store: Arc<dyn CasStore>) {
    missing_key_reads_as_none(&*store);
    create_requires_absence(&*store);
    swap_requires_the_current_version(&*store);
    immutable_objects_cannot_be_overwritten(&*store);
    list_returns_sorted_keys_under_a_prefix(&*store);
    concurrent_swappers_never_lose_an_update(store);
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
