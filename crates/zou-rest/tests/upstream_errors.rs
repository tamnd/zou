//! The parse errors upstream writes down for itself.
//!
//! PostgREST's QueryParams.hs carries doctests, and the failing ones
//! print the exact message parsec produces: a position, the token it
//! could not take, and the set of things it would have taken instead.
//! Those are worth having as tests here, because they cover a lot of
//! grammar the conformance corpus never sends and they are upstream's
//! own words rather than our reading of them.
//!
//! Each case below names the upstream parser it came from. The
//! position is one based and counts characters, the way parsec counts
//! columns, and it is asserted separately because the message the
//! server sends carries the two halves in different fields.
//!
//! Four doctests are left out on purpose, all of them inputs zou reads
//! where upstream refuses:
//!
//!   - `id,clients(name[])` and `*!hint`, where a bare name may hold
//!     bytes upstream's identifier rule has no room for
//!   - `rel->jsonpath(*)` and `...rel->jsonpath(*)`, where a json path
//!     hangs off the name of an embedded resource
//!
//! and two more where zou refuses the same input somewhere else, so
//! the position and the words are its own: `pOrder "clients(name,id)"`
//! and `pSpreadRelationSelect "alias:...rel(*)"`.

use zou_rest::{filter, order, select};

#[track_caller]
fn select_at(value: &str, column: usize, message: &str) {
    let e = select::parse(value).expect_err("expected a parse error");
    assert_eq!((e.at + 1, e.to_string()), (column, message.to_string()));
}

#[track_caller]
fn order_at(value: &str, column: usize, message: &str) {
    let e = order::parse(value).expect_err("expected a parse error");
    assert_eq!((e.at + 1, e.to_string()), (column, message.to_string()));
}

/// The column counts from the front of what upstream parsed, which for
/// a logic tree is the operator out of the key followed by the value.
#[track_caller]
fn filter_at(key: &str, value: &str, column: usize, message: &str) {
    let f = filter::parse_pair(key, value).expect_err("expected a parse error");
    assert_eq!(
        (f.error.at + 1 + f.skew, f.error.to_string()),
        (column, message.to_string())
    );
}

#[test]
fn field_names_and_json_paths() {
    // pFieldName
    select_at(
        ":",
        1,
        "unexpected \":\" expecting field name (* or [a..z0..9_$])",
    );
    // pFieldSelect
    select_at(
        "name::",
        7,
        "unexpected end of input expecting letter or digit",
    );
    // pFieldForest, and pJsonPath at four columns less
    select_at(
        "data->>-78xy",
        11,
        "unexpected \"x\" expecting digit, \"->\", \"::\", \".\", \",\" or end of input",
    );
    // pJsonPath
    select_at("data->>--34", 9, "unexpected \"-\" expecting digit");
    select_at("data->>-xy-4", 9, "unexpected \"x\" expecting digit");
}

#[test]
fn order_modifiers() {
    // pOrder, every one of them
    let modifiers = "\"asc\", \"desc\", \"nullsfirst\" or \"nullslast\"";
    order_at(
        "id.ac",
        4,
        &format!("unexpected \"c\" expecting {modifiers}"),
    );
    order_at(
        "id.nulsfist",
        4,
        &format!("unexpected \"n\" expecting {modifiers}"),
    );
    order_at(
        "id.smth34",
        4,
        &format!("unexpected \"s\" expecting {modifiers}"),
    );
    order_at(
        "id.descc",
        8,
        "unexpected 'c' expecting delimiter (.), \",\" or end of input",
    );
    order_at(
        "id.nullslasttt",
        13,
        "unexpected 't' expecting \",\" or end of input",
    );
    order_at(
        "id.asc.nlsfst",
        8,
        "unexpected \"l\" expecting \"nullsfirst\" or \"nullslast\"",
    );
    order_at(
        "id.asc.smth34",
        8,
        "unexpected \"s\" expecting \"nullsfirst\" or \"nullslast\"",
    );
    order_at(
        "id.asc.nullslasttt",
        17,
        "unexpected 't' expecting \",\" or end of input",
    );
}

#[test]
fn the_operator_slot() {
    // pRequestFilter and qsFilters
    filter_at(
        "a.b",
        "noop.0",
        1,
        "unexpected \"o\" expecting \"not\" or operator (eq, gt, ...)",
    );
    filter_at(
        "id",
        "val",
        1,
        "unexpected \"v\" expecting \"not\" or operator (eq, gt, ...)",
    );
    // pOpExpr. Every alternative in the slot is tried and thrown away,
    // so what is expected is always the slot itself and the only thing
    // that moves is the position.
    filter_at(
        "id",
        "fts().value",
        5,
        "unexpected \")\" expecting operator (eq, gt, ...)",
    );
    filter_at(
        "id",
        "eq().value",
        4,
        "unexpected \")\" expecting operator (eq, gt, ...)",
    );
    filter_at(
        "id",
        "is().value",
        3,
        "unexpected \"(\" expecting operator (eq, gt, ...)",
    );
    filter_at(
        "id",
        "in().value",
        3,
        "unexpected \"(\" expecting operator (eq, gt, ...)",
    );
}

#[test]
fn logic_trees() {
    // pLogicTree
    filter_at(
        "or",
        "()",
        4,
        "unexpected \")\" expecting field name (* or [a..z0..9_$]), negation operator (not) or logic operator (and, or)",
    );
    filter_at(
        "or",
        "(id.in.1,2,id.eq.3)",
        10,
        "unexpected \"1\" expecting \"(\"",
    );
    filter_at("or", ")(", 3, "unexpected \")\" expecting \"(\"");
    filter_at(
        "and",
        "(ord(id.eq.1,id.eq.1),id.eq.2)",
        7,
        "unexpected \"d\" expecting \"(\"",
    );
    filter_at(
        "or",
        "(id.eq.1,not.xor(id.eq.2,id.eq.3))",
        16,
        "unexpected \"x\" expecting logic operator (and, or)",
    );
}
