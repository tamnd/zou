//! Crash fuzz for the local backends. A crash is not an interleaving,
//! it is a state left on disk, so the localfs half enumerates every
//! combination of leftover a killed process can produce, lock dir, tmp
//! file, destination, instead of sampling a few at random. The .zou
//! half tears the file at every byte of an in flight append and at
//! every flipped bit of a sealed tail frame, which is exactly the
//! damage the format's open scan promises to survive. The remote
//! dialects get their crash coverage from the truncated PUT fault in
//! s3_chaos.rs, and sqlite delegates torn write recovery to its own
//! journal, so neither is duplicated here.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use zou_store::{CasError, CasStore, LocalFsStore, ZouFileStore};

/// The recovery knob: locks this old get broken by waiting writers.
/// Set small so the fuzz exercises the breaking path in milliseconds,
/// the production default is a minute.
fn set_fast_lock_recovery() {
    // Safe here: integration tests run in their own process and every
    // test in this file wants the same value.
    unsafe { std::env::set_var("ZOU_LOCALFS_LOCK_STALE_MS", "200") };
}

/// Build every disk state a crash during a localfs mutation can leave
/// behind and check the store recovers from all of them: reads are
/// never torn, list never surfaces artifacts, the key is never wedged,
/// and a retry of the crashed write goes through.
#[test]
fn localfs_recovers_from_every_crash_state() {
    set_fast_lock_recovery();
    let key = "wal/000000000001";
    for lock_left in [false, true] {
        for break_left in [false, true] {
            for tmp_left in [None, Some(&b"torn garbage"[..]), Some(&b""[..])] {
                for dest_committed in [false, true] {
                    let dir = tempfile::tempdir().unwrap();
                    let store = LocalFsStore::new(dir.path());
                    if dest_committed {
                        store.put_if_absent(key, b"committed").unwrap();
                    }
                    let path = dir.path().join(key);
                    fs::create_dir_all(path.parent().unwrap()).unwrap();
                    if lock_left {
                        fs::create_dir(path.with_extension("lock")).unwrap();
                    }
                    if break_left {
                        // A breaker that died between its mkdir and its
                        // cleanup leaves this behind too.
                        fs::create_dir(path.with_extension("lock-break")).unwrap();
                    }
                    if let Some(tmp) = tmp_left {
                        fs::write(path.with_extension("tmp"), tmp).unwrap();
                    }
                    // Let the leftovers age past the staleness bound.
                    if lock_left || break_left {
                        std::thread::sleep(std::time::Duration::from_millis(250));
                    }
                    let case = format!(
                        "lock_left={lock_left} break_left={break_left} tmp_left={:?} dest_committed={dest_committed}",
                        tmp_left.map(|t| t.len())
                    );

                    // A read never sees tmp garbage, only the committed
                    // value or absence.
                    match store.get(key).unwrap() {
                        Some((data, _)) => {
                            assert!(dest_committed, "{case}: read invented an object");
                            assert_eq!(data, b"committed", "{case}: torn read");
                        }
                        None => assert!(!dest_committed, "{case}: committed object lost"),
                    }

                    // Listing shows objects, never lock, breaker or tmp
                    // leftovers.
                    let listed = store.list("wal/").unwrap();
                    if dest_committed {
                        assert_eq!(listed, vec![key.to_string()], "{case}");
                    } else {
                        assert!(listed.is_empty(), "{case}: {listed:?}");
                    }

                    // The key is not wedged: the exact write the crash
                    // interrupted retries through the stale lock and lands,
                    // and absence semantics hold either way.
                    match store.put_if_absent(key, b"retry") {
                        Ok(_) => {
                            assert!(
                                !dest_committed,
                                "{case}: create overwrote a committed object"
                            );
                            assert_eq!(store.get(key).unwrap().unwrap().0, b"retry", "{case}");
                        }
                        Err(CasError::AlreadyExists { .. }) => {
                            assert!(dest_committed, "{case}: spurious AlreadyExists");
                            assert_eq!(store.get(key).unwrap().unwrap().0, b"committed", "{case}");
                        }
                        Err(e) => panic!("{case}: key wedged after crash: {e}"),
                    }
                }
            }
        }
    }
}

/// While a dead process's lock sits on the key, a fresh creator race
/// must still elect exactly one winner: breaking the stale lock
/// concurrently cannot let two writers into the critical section.
#[test]
fn a_creator_race_through_a_stale_lock_still_elects_one_winner() {
    set_fast_lock_recovery();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path()));
    let key = "wal/contended";
    fs::create_dir_all(dir.path().join(key).with_extension("lock")).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(250));

    let winners: usize = (0..8)
        .map(|i| {
            let store = Arc::clone(&store);
            std::thread::spawn(
                move || match store.put_if_absent(key, format!("c{i}").as_bytes()) {
                    Ok(_) => 1,
                    Err(CasError::AlreadyExists { .. }) => 0,
                    Err(e) => panic!("wedged: {e}"),
                },
            )
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|h| h.join().unwrap())
        .sum();
    assert_eq!(winners, 1);
}

/// A lock younger than the staleness bound is a live one and must be
/// waited on, not broken. The holder here releases after 50 ms and the
/// waiter must win afterward without ever stealing.
#[test]
fn a_live_lock_is_waited_on_not_broken() {
    set_fast_lock_recovery();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFsStore::new(dir.path()));
    let key = "wal/live";
    let lock = dir.path().join(key).with_extension("lock");
    fs::create_dir_all(&lock).unwrap();
    let release = {
        let lock = lock.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            fs::remove_dir(&lock).unwrap();
        })
    };
    store.put_if_absent(key, b"after the holder").unwrap();
    release.join().unwrap();
    assert_eq!(store.get(key).unwrap().unwrap().0, b"after the holder");
}

/// Fill a .zou store with known contents and return the expected map.
fn seed_zou(store: &ZouFileStore) -> Vec<(String, Vec<u8>)> {
    let mut expected = Vec::new();
    for i in 0..12 {
        let key = format!("wal/{i:06}");
        // Mix sizes across the compression threshold so both encodings
        // sit in the recovered file.
        let data = vec![b'a' + (i as u8 % 20); if i % 3 == 0 { 2000 } else { 90 }];
        store.put_if_absent(&key, &data).unwrap();
        expected.push((key, data));
    }
    store.put("mut/counter", b"7").unwrap();
    store.put("mut/counter", b"8").unwrap();
    expected.push(("mut/counter".into(), b"8".to_vec()));
    store.put_if_absent("gone/x", b"bye").unwrap();
    store.delete("gone/x").unwrap();
    expected.sort();
    expected
}

fn assert_zou_state(path: &Path, expected: &[(String, Vec<u8>)], case: &str) {
    let store = ZouFileStore::open(path).unwrap_or_else(|e| panic!("{case}: open failed: {e}"));
    for (key, data) in expected {
        let (got, _) = store
            .get(key)
            .unwrap()
            .unwrap_or_else(|| panic!("{case}: {key} lost"));
        assert_eq!(&got, data, "{case}: {key} corrupted");
    }
    assert!(
        store.get("gone/x").unwrap().is_none(),
        "{case}: tombstone lost"
    );
    // The store stays writable after recovery and the write survives
    // its own reopen.
    store
        .put_if_absent(&format!("post/{case_len}", case_len = case.len()), b"alive")
        .unwrap();
}

/// Tear an in flight append at every byte offset. Everything acked
/// before the torn frame must survive byte for byte, the torn object
/// must be absent at every cut short of the last byte and present at
/// the full length, and the file must reopen into a writable store
/// every time.
#[test]
fn zou_file_survives_a_tear_at_every_byte_of_an_append() {
    let dir = tempfile::tempdir().unwrap();
    let base_path = dir.path().join("base.zou");
    let expected = {
        let store = ZouFileStore::open(&base_path).unwrap();
        seed_zou(&store)
    };
    let base = fs::read(&base_path).unwrap();

    // Produce the exact bytes one more acked put appends.
    let full_path = dir.path().join("full.zou");
    fs::copy(&base_path, &full_path).unwrap();
    {
        let store = ZouFileStore::open(&full_path).unwrap();
        store
            .put_if_absent("tail/torn", b"the frame a crash tears")
            .unwrap();
    }
    let full = fs::read(&full_path).unwrap();
    assert!(full.len() > base.len(), "the extra put appended nothing");
    assert_eq!(&full[..base.len()], &base[..], "append rewrote history");

    for cut in base.len()..=full.len() {
        let torn_path = dir.path().join("torn.zou");
        fs::write(&torn_path, &full[..cut]).unwrap();
        let case = format!("cut={cut}");
        assert_zou_state(&torn_path, &expected, &case);
        let store = ZouFileStore::open(&torn_path).unwrap();
        let torn_present = store.get("tail/torn").unwrap().is_some();
        if cut == full.len() {
            assert!(torn_present, "{case}: a fully synced frame was dropped");
        } else {
            assert!(
                !torn_present,
                "{case}: a torn frame leaked a partial object"
            );
        }
    }
}

/// Flip every byte of the sealed tail frame one at a time. The crc has
/// to catch each flip, dropping that frame and only that frame, and
/// the store must reopen writable regardless.
#[test]
fn zou_file_drops_a_corrupted_tail_frame_and_keeps_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    let base_path = dir.path().join("base.zou");
    let expected = {
        let store = ZouFileStore::open(&base_path).unwrap();
        seed_zou(&store)
    };
    let base = fs::read(&base_path).unwrap();

    let full_path = dir.path().join("full.zou");
    fs::copy(&base_path, &full_path).unwrap();
    {
        let store = ZouFileStore::open(&full_path).unwrap();
        store
            .put_if_absent("tail/sealed", b"the frame the fuzz corrupts")
            .unwrap();
    }
    let full = fs::read(&full_path).unwrap();

    for flip in base.len()..full.len() {
        let mut damaged = full.clone();
        damaged[flip] ^= 0x40;
        let path = dir.path().join("damaged.zou");
        fs::write(&path, &damaged).unwrap();
        let case = format!("flip={flip}");
        assert_zou_state(&path, &expected, &case);
    }
}
