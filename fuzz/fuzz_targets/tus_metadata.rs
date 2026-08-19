//! The TUS metadata header is a client's own text, `key value` pairs
//! separated by commas with base64 values, and it is read before
//! anything about the upload has been decided. It never refuses, so
//! there is no error to check: what a target can ask is that it does
//! not panic on any of it, that what it hands back is pairs a name and
//! a value, and that writing those back out gives a header it reads the
//! same way.
//!
//! The round trip is not the identity. `written_metadata` puts a
//! `cacheControl` on the end when the client did not send one, which is
//! what a head hands back, so the second reading is the first with at
//! most that one pair added.

#![no_main]

use libfuzzer_sys::fuzz_target;
use zou_server::tus::{metadata_in, written_metadata};

fuzz_target!(|data: &[u8]| {
    let Ok(header) = std::str::from_utf8(data) else {
        return;
    };
    let read = metadata_in(header);
    for (name, _) in &read {
        // A name is what was before the first space, so it can be
        // anything except empty and it cannot carry one.
        assert!(!name.is_empty(), "an empty name came out of {header:?}");
        assert!(!name.contains(' '), "a name with a space in it: {name:?}");
        assert!(!name.contains(','), "a name with a comma in it: {name:?}");
    }
    let again = metadata_in(&written_metadata(&read));
    let added = again.len() - read.len();
    assert!(added <= 1, "writing {read:?} back added {added} pairs");
    for (before, after) in read.iter().zip(&again) {
        assert_eq!(before, after, "a pair changed on the way out and back");
    }
    if added == 1 {
        assert_eq!(
            again.last().map(|(name, _)| name.as_str()),
            Some("cacheControl"),
            "the only pair a write adds is the cache control a head hands back"
        );
    }
});
