//! What a changed row costs when a lot of people are listening.
//!
//! The claim the matcher makes is not that it is fast, it is that a
//! change costs what its own table costs rather than what the server
//! holds. That is the difference between a node that can hold ten
//! thousand subscriptions and one that can hold ten thousand
//! subscriptions as long as they are all about the same thing.
//!
//! Two measurements, both in process and neither needing a database.
//! The first is the claim: the same table, measured alone and measured
//! again with ten thousand subscriptions about other tables next to it.
//! The second is the pathological shape, where all ten thousand are
//! about the one table that changed, which is the case the index
//! cannot help with and the one worth knowing the number for.
//!
//!     cargo test --release -p zou-server --test binding_scale -- --nocapture

use std::sync::Arc;
use std::time::{Duration, Instant};

use zou_server::binding::{Binding, Subscriptions};
use zou_server::pgoutput::{Cell, Change, Column, Op, Relation};

/// Ten thousand, which is the number on the milestone.
const MANY: u64 = 10_000;

/// How many changes each measurement puts through, enough that a
/// quantile means something and few enough that a debug build finishes.
const ROWS: usize = 200;

fn relation(table: &str) -> Arc<Relation> {
    Arc::new(Relation {
        oid: 16_384,
        schema: "public".into(),
        table: table.into(),
        replica: zou_server::pgoutput::Replica::Default,
        columns: vec![
            Column {
                name: "id".into(),
                type_oid: 23,
                key: true,
            },
            Column {
                name: "title".into(),
                type_oid: 25,
                key: false,
            },
        ],
    })
}

fn change(table: &str, id: u64) -> Change {
    Change {
        relation: relation(table),
        op: Op::Insert,
        record: vec![
            Cell::Text(id.to_string()),
            Cell::Text("wash up".to_string()),
        ],
        old: None,
        old_key: false,
        commit_ts: 0,
        lsn: id,
    }
}

fn binding(json: String) -> Binding {
    Binding::of(&serde_json::from_str(&json).expect("json")).expect("a binding")
}

/// The median of the per change times, which is the number that says
/// what a change costs rather than what the slowest one did.
fn each(subs: &Subscriptions, table: &str) -> Duration {
    let mut took = Vec::with_capacity(ROWS);
    for id in 0..ROWS {
        let change = change(table, id as u64);
        let start = Instant::now();
        let ids = subs.matching(&change);
        took.push(start.elapsed());
        std::hint::black_box(ids);
    }
    took.sort_unstable();
    took[took.len() / 2]
}

fn spread(subs: &Subscriptions, table: &str) -> String {
    let mut took = Vec::with_capacity(ROWS);
    for id in 0..ROWS {
        let change = change(table, id as u64);
        let start = Instant::now();
        let ids = subs.matching(&change);
        took.push(start.elapsed());
        std::hint::black_box(ids);
    }
    took.sort_unstable();
    let at = |q: f64| took[((took.len() as f64 * q) as usize).min(took.len() - 1)];
    format!(
        "n={} p50={:?} p90={:?} p99={:?}",
        took.len(),
        at(0.50),
        at(0.90),
        at(0.99)
    )
}

/// The claim. Ten subscriptions on one table, measured, and then ten
/// thousand more about other tables, measured again. If the second
/// number is the first number, the index is doing what it says.
#[test]
fn a_change_costs_what_its_own_table_costs_and_not_what_the_server_holds() {
    let mut subs = Subscriptions::new();
    for id in 0..10 {
        subs.add(id, binding(r#"{"event":"*","table":"todos"}"#.to_string()));
    }
    let alone = each(&subs, "todos");

    for id in 10..MANY + 10 {
        subs.add(id, binding(format!(r#"{{"event":"*","table":"t{id}"}}"#)));
    }
    assert_eq!(subs.len() as u64, MANY + 10);
    let crowded = each(&subs, "todos");

    println!("ten on the table alone {alone:?}, and with {MANY} elsewhere {crowded:?}");
    // Generous, because these are nanoseconds and a shared runner is a
    // noisy place to measure nanoseconds. What it refuses is a matcher
    // that walks everything, which at a thousand times the
    // subscriptions would not be a factor of five out, it would be a
    // factor of a thousand.
    assert!(
        crowded < alone * 5 + Duration::from_micros(10),
        "{crowded:?} against {alone:?}: the other subscriptions are being walked"
    );
}

/// The shape no index helps with: everybody listening to the one table
/// that changed. Nothing to assert about it beyond that it is not
/// absurd, so the number is printed and the bound is loose enough to
/// mean something on the slowest runner rather than tight enough to
/// fail on a busy one.
#[test]
fn ten_thousand_subscriptions_on_one_table_still_answer_in_time() {
    let mut subs = Subscriptions::new();
    for id in 0..MANY {
        subs.add(id, binding(r#"{"event":"*","table":"todos"}"#.to_string()));
    }
    println!("{MANY} on one table, no filter: {}", spread(&subs, "todos"));
    assert_eq!(subs.matching(&change("todos", 1)).len(), MANY as usize);

    let mut filtered = Subscriptions::new();
    for id in 0..MANY {
        filtered.add(
            id,
            binding(format!(
                r#"{{"event":"*","table":"todos","filter":"id=eq.{id}"}}"#
            )),
        );
    }
    println!(
        "{MANY} on one table, filtered: {}",
        spread(&filtered, "todos")
    );
    assert_eq!(
        filtered.matching(&change("todos", 7)),
        vec![7],
        "one of the ten thousand asked for that row"
    );
    let took = each(&filtered, "todos");
    assert!(
        took < Duration::from_millis(50),
        "{took:?} to compare ten thousand filters"
    );
}

/// A table nobody asked about costs a hash lookup and nothing else,
/// which is what makes a busy table in a database nobody is subscribed
/// to free rather than merely cheap.
#[test]
fn a_table_nobody_asked_about_costs_nothing() {
    let mut subs = Subscriptions::new();
    for id in 0..MANY {
        subs.add(id, binding(format!(r#"{{"event":"*","table":"t{id}"}}"#)));
    }
    let took = each(&subs, "nobody_cares");
    println!("a change nobody asked for, against {MANY} subscriptions: {took:?}");
    assert!(subs.matching(&change("nobody_cares", 1)).is_empty());
    assert!(took < Duration::from_micros(50), "{took:?}");
}
