//! Run with: cargo +nightly fuzz run rest_select
//!
//! The select grammar reads url decoded values straight off the wire,
//! so no input may panic it, and anything it accepts must render to a
//! canonical string that reparses to the same tree.

#![no_main]

use libfuzzer_sys::fuzz_target;
use zou_rest::select;

fuzz_target!(|data: &str| {
    if let Ok(items) = select::parse(data) {
        let rendered = select::render(&items);
        let again = select::parse(&rendered).expect("the canonical form must reparse");
        assert_eq!(items, again, "reparsing the canonical form must agree");
    }
});
