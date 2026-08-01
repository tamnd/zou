//! Run with: cargo +nightly fuzz run rest_order
//!
//! The order grammar reads url decoded values straight off the wire,
//! so no input may panic it, and anything it accepts must render to a
//! canonical string that reparses to the same list. The Range header
//! parser rides along, split on the first newline: it must never
//! panic and an accepted range must have a consistent offset and
//! limit.

#![no_main]

use libfuzzer_sys::fuzz_target;
use zou_rest::{order, page};

fuzz_target!(|data: &str| {
    let (order_part, page_part) = match data.split_once('\n') {
        Some((a, b)) => (a, b),
        None => (data, ""),
    };

    if let Ok(terms) = order::parse(order_part) {
        let rendered = order::render(&terms);
        let again = order::parse(&rendered).expect("the canonical form must reparse");
        assert_eq!(terms, again, "reparsing the canonical form must agree");
    }

    let _ = page::parse_limit(page_part);
    let _ = page::parse_offset(page_part);
    if let Ok(r) = page::parse_range(page_part) {
        assert_eq!(r.offset(), r.first);
        if let Some(limit) = r.limit() {
            assert!(limit >= 1, "an accepted range spans at least one row");
        }
    }
});
