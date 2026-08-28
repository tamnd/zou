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
    absence_returns_after_delete(&*store);
    unconditional_put_overwrites_and_delete_removes(&*store);
    list_returns_sorted_keys_under_a_prefix(&*store);
    ranged_reads_clamp_to_the_object(&*store);
    concurrent_creators_elect_exactly_one_winner(Arc::clone(&store), transient_io);
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
    store.put_if_absent(key, b"frames").unwrap();
    assert!(matches!(
        store.put_if_absent(key, b"other"),
        Err(CasError::AlreadyExists { .. })
    ));
    assert_eq!(store.get(key).unwrap().unwrap().0, b"frames");
}

fn list_returns_sorted_keys_under_a_prefix(store: &dyn CasStore) {
    store.put_if_absent("contract/list/b", b"2").unwrap();
    store.put_if_absent("contract/list/a", b"1").unwrap();
    store.put_if_absent("contract/list/sub/c", b"3").unwrap();
    store.put_if_absent("contract/other/x", b"4").unwrap();
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

fn absence_returns_after_delete(store: &dyn CasStore) {
    let key = "contract/recreate/obj";
    store.put_if_absent(key, b"first").unwrap();
    store.delete(key).unwrap();
    // Delete restores absence, so a second create must win cleanly. The
    // landing chain leans on this after GC reclaims a sealed segment.
    let v = store.put_if_absent(key, b"second").unwrap();
    let (data, current) = store.get(key).unwrap().unwrap();
    assert_eq!(data, b"second");
    assert_eq!(current, v);
}

/// The fencing race the v2 landing chain is built on: N threads race
/// put_if_absent on one key, exactly one wins, every loser sees
/// AlreadyExists, and what is stored is the winner's payload, never a
/// blend. A backend that lets two creators through breaks the sealed
/// chain protocol, so this property gets a bigger thread count than the
/// swap counter.
fn concurrent_creators_elect_exactly_one_winner(store: Arc<dyn CasStore>, transient_io: bool) {
    const RACERS: usize = 16;
    const ROUNDS: usize = 8;
    for round in 0..ROUNDS {
        let key = format!("contract/race/create-{round}");
        let barrier = Arc::new(std::sync::Barrier::new(RACERS));
        let handles: Vec<_> = (0..RACERS)
            .map(|i| {
                let store = Arc::clone(&store);
                let key = key.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let payload = format!("creator-{i}");
                    barrier.wait();
                    loop {
                        return match store.put_if_absent(&key, payload.as_bytes()) {
                            Ok(_) => Some(payload),
                            Err(CasError::AlreadyExists { .. }) => None,
                            Err(CasError::Io { .. }) if transient_io => {
                                // A retried create that lost its ack must
                                // still resolve: reread, and if our bytes
                                // landed we are the winner.
                                match store.get(&key) {
                                    Ok(Some((data, _))) if data == payload.as_bytes() => {
                                        Some(payload)
                                    }
                                    Ok(Some(_)) => None,
                                    _ => continue,
                                }
                            }
                            Err(e) => panic!("unexpected error: {e}"),
                        };
                    }
                })
            })
            .collect();
        let winners: Vec<String> = handles
            .into_iter()
            .filter_map(|h| h.join().unwrap())
            .collect();
        assert_eq!(
            winners.len(),
            1,
            "round {round}: exactly one creator must win, got {winners:?}"
        );
        let (data, _) = store.get(&key).unwrap().unwrap();
        assert_eq!(
            data,
            winners[0].as_bytes(),
            "round {round}: the stored bytes must be the winner's"
        );
    }
}

/// The classic CAS counter: N threads each add 1 through a read-modify-swap
/// loop. If the backend ever lets a stale swap through, the final count
/// comes up short.
fn concurrent_swappers_never_lose_an_update(store: Arc<dyn CasStore>, transient_io: bool) {
    const THREADS: usize = 8;
    const INCREMENTS: usize = 25;
    let key = "contract/counter/obj";
    store.put_if_match(key, b"0", None).unwrap();

    let started = std::time::Instant::now();
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
    let took = started.elapsed();

    let (data, _) = store.get(key).unwrap().unwrap();
    let n: u64 = String::from_utf8(data).unwrap().parse().unwrap();
    assert_eq!(n as usize, THREADS * INCREMENTS);

    // A backend that starves one of its waiters still gets the count
    // right, it just gets there a minute later, so the count on its own
    // calls that a pass. Two hundred swaps is under a second of work on
    // any machine this runs on, and the bound is loose enough for a
    // shared runner under load and tight enough that a waiter sitting
    // out the localfs stale age reads as the failure it is.
    assert!(
        took < std::time::Duration::from_secs(30),
        "the swappers took {took:?}, which is a lock protocol that starved somebody rather than slow io"
    );
}
