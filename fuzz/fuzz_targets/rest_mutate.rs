//! Run with: cargo +nightly fuzz run rest_mutate
//!
//! The body's json keys become spliced identifiers in every mutation
//! builder, so any byte soup must come out quoted: the builders
//! never panic, and a built statement references every parameter it
//! collected, $1 through $n with no gaps, the same density direction
//! the rest_sql and rest_plan targets check.

#![no_main]

use libfuzzer_sys::fuzz_target;
use zou_rest::filter::{Node, Parsed, parse_pair};
use zou_rest::mutate::{Conflict, Missing, Returning, delete, insert, update};
use zou_rest::sql::Sql;

fn dense(sql: &Sql) {
    for i in 1..=sql.params.len() {
        assert!(
            sql.text.contains(&format!("${i}")),
            "missing ${i} in {}",
            sql.text
        );
    }
}

fuzz_target!(|data: &str| {
    let mut lines = data.split('\n');
    let table = lines.next().unwrap_or("t");
    let columns: Vec<String> = lines
        .next()
        .unwrap_or("")
        .split(',')
        .filter(|c| !c.is_empty())
        .map(str::to_string)
        .collect();
    let filter = lines.next().unwrap_or("");
    let payload = lines.next().unwrap_or("[]").to_string();

    let mut filters = Vec::new();
    if let Some((key, value)) = filter.split_once('=')
        && let Ok(parsed) = parse_pair(key, value)
    {
        match parsed {
            Parsed::Filter(c) => filters.push(Node::Cond(c)),
            Parsed::Logic {
                embed,
                op,
                negated,
                kids,
            } if embed.is_empty() => filters.push(Node::Group { op, negated, kids }),
            Parsed::Logic { .. } => {}
        }
    }

    let returnings = [
        Returning::None,
        Returning::Star,
        Returning::Cols(columns.clone()),
    ];
    let conflicts = [
        None,
        Some(Conflict::Ignore {
            target: columns.clone(),
        }),
        Some(Conflict::Merge {
            target: columns.clone(),
            set: columns.clone(),
        }),
    ];
    // No relation: the catalog is not fuzzed here, so a builder that
    // needs one falls back to what it does for a table it was told
    // nothing about, which is the path an unknown table takes anyway.
    for r in &returnings {
        for missing in [Missing::Null, Missing::Default] {
            for c in &conflicts {
                if let Ok(s) = insert(table, None, &columns, payload.clone(), missing, c.as_ref(), r)
                {
                    dense(&s);
                }
            }
            if let Ok(s) = update(table, None, &columns, payload.clone(), missing, &filters, r) {
                dense(&s);
            }
        }
        if let Ok(s) = delete(table, None, &filters, r) {
            dense(&s);
        }
    }
});
