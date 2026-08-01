//! Run with: cargo +nightly fuzz run rest_filter
//!
//! The filter grammar reads url decoded query pairs straight off the
//! wire, so no input may panic it, and anything it accepts must
//! render to a canonical pair that reparses to the same tree.

#![no_main]

use libfuzzer_sys::fuzz_target;
use zou_rest::filter;

fuzz_target!(|data: &str| {
    let Some((key, value)) = data.split_once('=') else {
        return;
    };
    if let Ok(parsed) = filter::parse_pair(key, value) {
        let (rk, rv) = parsed.render();
        let again = filter::parse_pair(&rk, &rv).expect("the canonical form must reparse");
        assert_eq!(parsed, again, "reparsing the canonical form must agree");
    }
});
