//! Run with: cargo +nightly fuzz run rest_sql
//!
//! Any pair the filter grammar accepts must compile to SQL without
//! panicking, or be refused with a clean error, and the fragment must
//! reference exactly the parameters it collected, $1 through $n with
//! no gaps.

#![no_main]

use libfuzzer_sys::fuzz_target;
use zou_rest::filter::{Node, Parsed, parse_pair};
use zou_rest::sql::where_clause;

fuzz_target!(|data: &str| {
    let Some((key, value)) = data.split_once('=') else {
        return;
    };
    let Ok(parsed) = parse_pair(key, value) else {
        return;
    };
    let node = match parsed {
        Parsed::Filter(c) => Node::Cond(c),
        Parsed::Logic {
            op, negated, kids, ..
        } => Node::Group { op, negated, kids },
    };
    if let Ok(sql) = where_clause(&[node]) {
        // Only the density direction holds as a text search: a field
        // or json key may itself contain a $n lookalike inside its
        // quoted literal, so absence of extras cannot be checked this
        // way.
        for i in 1..=sql.params.len() {
            assert!(
                sql.text.contains(&format!("${i}")),
                "missing ${i} in {}",
                sql.text
            );
        }
    }
});
