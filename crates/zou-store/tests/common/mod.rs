//! Contract properties every CAS backend must pass, shared by the
//! backend matrix in cas_contract.rs and the fault injection run in
//! s3_chaos.rs.

use std::sync::Arc;

use zou_store::{CasError, CasStore};

/// A plain backend must never surface a transient error, so any Io is a
/// contract violation here.
#[allow(dead_code)]
pub fn run_contract(store: Arc<dyn CasStore>) {
    run_contract_inner(store, false);
}

/// Through a fault injecting proxy the store's bounded retries can run
/// dry when injected errors align across concurrent swappers, and the
/// store then surfaces the error honestly. Callers own that retry, the
/// manifest CAS loop rereads and goes again, so the swappers here do
/// the same. The exact final count still catches every lie in both
/// directions: a lost update comes up short and a false error ack, a
/// swap that reported failure but applied, overshoots.
#[allow(dead_code)]
pub fn run_contract_expecting_transient_errors(store: Arc<dyn CasStore>) {
    run_contract_inner(store, true);
}

fn run_contract_inner(store: Arc<dyn CasStore>, transient_io: bool) {
    missing_key_reads_as_none(&*store);
    create_requires_absence(&*store);
    swap_requires_the_current_version(&*store);
    immutable_objects_cannot_be_overwritten(&*store);
    unconditional_put_overwrites_and_delete_removes(&*store);
    list_returns_sorted_keys_under_a_prefix(&*store);
    ranged_reads_clamp_to_the_object(&*store);
    concurrent_swappers_never_lose_an_update(store, transient_io);
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
    // A prefix ending mid name filters by bytes, the way S3 does.
    assert_eq!(
        store.list("contract/list/su").unwrap(),
        vec!["contract/list/sub/c"]
    );
}

/// The classic CAS counter: N threads each add 1 through a read-modify-swap
/// loop. If the backend ever lets a stale swap through, the final count
/// comes up short.
fn concurrent_swappers_never_lose_an_update(store: Arc<dyn CasStore>, transient_io: bool) {
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
                        let (data, version) = match store.get(key) {
                            Ok(found) => found.expect("the counter object vanished"),
                            Err(CasError::Io { .. }) if transient_io => continue,
                            Err(e) => panic!("unexpected error: {e}"),
                        };
                        let n: u64 = String::from_utf8(data).unwrap().parse().unwrap();
                        let next = (n + 1).to_string();
                        match store.put_if_match(key, next.as_bytes(), Some(&version)) {
                            Ok(_) => break,
                            Err(CasError::Conflict { .. }) => continue,
                            Err(CasError::Io { .. }) if transient_io => continue,
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
