//! The manifest is the first thing a store reads and the last word on
//! what is in it: which layers exist, who holds the lease, what epoch
//! the holder is at. It is json in a bucket, so anything that can write
//! to the bucket decides what this parser sees, and a parser that
//! panics on it takes the process down before the store is even open.
//!
//! It has an encoder that is its inverse, so a manifest that parsed has
//! to survive being written back out and read again. The interesting
//! half is the peek `from_json` does before the real parse: a format
//! newer than this build is refused by number, and a v1 tail is refused
//! by name, and neither refusal may be reached by a manifest this
//! build wrote.

#![no_main]

use libfuzzer_sys::fuzz_target;
use zou_store::{MANIFEST_FORMAT, Manifest};

fuzz_target!(|data: &[u8]| {
    let Ok(manifest) = Manifest::from_json(data) else {
        return;
    };
    // The peek runs before the parse, so anything that got this far is
    // a format this build reads.
    assert!(
        manifest.format <= MANIFEST_FORMAT,
        "a manifest from the future parsed: format {}",
        manifest.format
    );
    let written = manifest.to_json();
    let again = Manifest::from_json(&written).expect("a manifest this wrote must read back");
    assert_eq!(again, manifest, "a manifest changed on the way out and back");
    // And what it says about itself is read off the same fields twice.
    assert_eq!(again.captured_upto(), manifest.captured_upto());
    assert_eq!(again.pages_left_behind(), manifest.pages_left_behind());
});
