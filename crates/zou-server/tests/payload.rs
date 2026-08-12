//! What a subscriber is told a change was, against a live postgres.
//!
//! The unit tests in the module say what the conversion does. This says
//! that what it does is what postgres would have done, which is the
//! only claim worth making: upstream builds a payload by calling
//! `to_jsonb(value::type)` in the database, so the specification for
//! every value in a payload is that function, and the way to check a
//! reimplementation of a function is to run it.
//!
//! So these insert a row of every type worth arguing about, read the
//! change back through the tap, build the payload, and ask the same
//! database for `to_jsonb(t.*)` of the row that is still sitting there.
//! The two are compared whole. A difference in any column is a payload
//! a Supabase client would read differently from upstream's.
//!
//! Gated on ZOU_PG_TEST_DSN and skipped when that database is not
//! logical, the same as the tap's own suite.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test payload

use serde_json::Value;
use tokio_postgres::{Client, NoTls};
use zou_server::cdc::{Closed, PUBLICATION, Tap};
use zou_server::payload::{Types, data};
use zou_server::pgoutput::Change;

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

/// A connection with the bootstrap contract applied and its own time
/// zone pinned.
///
/// UTC because this connection is the one that says what the answer
/// should have been: `to_jsonb` of a timestamptz prints the offset of
/// whatever session asks, and the tap pins its own session the same
/// way, so a comparison between the two is a comparison of the
/// conversion rather than of two time zones.
async fn connect(dsn: &str) -> Client {
    let (client, connection) = dsn
        .parse::<tokio_postgres::Config>()
        .expect("a dsn")
        .connect(NoTls)
        .await
        .expect("a connection");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    zou_server::sql::bootstrap(&client)
        .await
        .expect("the bootstrap contract");
    client
        .batch_execute("set datestyle = 'ISO, MDY'; set timezone = 'UTC'")
        .await
        .expect("a session that prints the way the tap's does");
    client
}

async fn published(client: &Client, table: &str, columns: &str) {
    client
        .batch_execute(&format!(
            "drop table if exists {table};
             create table {table} ({columns})"
        ))
        .await
        .expect("a table");
    client
        .batch_execute(&format!(
            "alter publication {PUBLICATION} add table {table}"
        ))
        .await
        .expect("a table in the publication");
}

async fn tapping(dsn: &str) -> Option<Tap> {
    match Tap::open(dsn, PUBLICATION).await {
        Ok(tap) => Some(tap),
        Err(Closed::NotLogical(level)) => {
            eprintln!("skipping: wal_level is {level}");
            None
        }
        Err(other) => panic!("no tap: {other}"),
    }
}

/// Poll for this test's own table until `many` changes have arrived.
async fn until(tap: &mut Tap, table: &str, many: usize) -> Vec<Change> {
    let mut changes = Vec::new();
    for _ in 0..100 {
        let read = tap.changes(0).await.expect("a read");
        changes.extend(
            read.into_iter()
                .filter(|change| change.relation.table == table),
        );
        if changes.len() >= many {
            return changes;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    changes
}

/// The payload for a change, with the types it needs looked up out of
/// the database the change came from, which is what the loop that owns
/// a tap will do once per table.
async fn payload(tap: &Tap, change: &Change) -> Value {
    let mut types = Types::new();
    let want = types.missing(&change.relation);
    types
        .learn(tap.client(), &want)
        .await
        .expect("the types of the columns");
    data(change, &types)
}

/// What upstream would have sent for the row still sitting in the
/// table, which is postgres's own conversion of it.
async fn expected(client: &Client, table: &str) -> Value {
    let text: String = client
        .query_one(&format!("select to_jsonb(t.*)::text from {table} t"), &[])
        .await
        .expect("the row as json")
        .get(0);
    serde_json::from_str(&text).expect("json")
}

/// The claim, over every type a project is likely to have a column of.
///
/// A failure here names the column, because the two objects are
/// compared key by key before they are compared whole.
#[tokio::test]
async fn every_value_is_the_json_postgres_would_have_made_of_it() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(
        &client,
        "pay_types",
        "id int primary key,
         small smallint, big bigint, num numeric, single real, double_ double precision,
         flag boolean, name text, tag varchar(10), fixed char(3), ident uuid,
         doc jsonb, plain json,
         at timestamptz, naive timestamp, day date, clock time,
         ints int[], texts text[], grid int[][],
         span interval, rng int4range, addr inet",
    )
    .await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    client
        .batch_execute(
            "insert into pay_types values (
                 1, 32767, 9007199254740993, 1.20, 1.5, 0.1,
                 true, 'wash up', 'short', 'ab', '00000000-0000-0000-0000-000000000001',
                 '{\"a\": [1, 2], \"b\": null}', '{\"a\": 1}',
                 '2021-11-05 17:20:51.524+00', '2021-11-05 17:20:51.524', '2021-11-05', '17:20:51',
                 '{1,2,3}', '{a,\"b,c\",NULL}', '{{1,2},{3,4}}',
                 '1 day 02:03:04', '[1,5)', '10.0.0.1'
             )",
        )
        .await
        .expect("a row of everything");

    let changes = until(&mut tap, "pay_types", 1).await;
    assert_eq!(changes.len(), 1);
    let payload = payload(&tap, &changes[0]).await;
    let record = payload["record"].as_object().expect("a record");
    let expected = expected(&client, "pay_types").await;
    let expected = expected.as_object().expect("an object");

    for (name, want) in expected {
        assert_eq!(
            record.get(name),
            Some(want),
            "the {name} column is not what postgres would have made of it"
        );
    }
    assert_eq!(
        record.len(),
        expected.len(),
        "every column of the row and no others"
    );
}

/// A null is a null in every one of those types, which is a separate
/// row rather than a separate assertion because a value being absent
/// and a value being null are different payloads and only one of them
/// is right.
#[tokio::test]
async fn a_null_column_is_null_and_not_missing() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(
        &client,
        "pay_nulls",
        "id int primary key, num numeric, name text, doc jsonb, at timestamptz, ints int[]",
    )
    .await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    client
        .execute("insert into pay_nulls (id) values (1)", &[])
        .await
        .expect("a row of nulls");

    let changes = until(&mut tap, "pay_nulls", 1).await;
    assert_eq!(changes.len(), 1);
    let payload = payload(&tap, &changes[0]).await;
    assert_eq!(payload["record"], expected(&client, "pay_nulls").await);
    assert!(payload["record"]["doc"].is_null());
}

/// The one place where matching upstream means not matching postgres:
/// wal2json writes a bytea as hex with no prefix, so upstream's payload
/// has none and `to_jsonb` of the same value has one.
#[tokio::test]
async fn a_bytea_is_the_hex_upstream_sends() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(&client, "pay_bytes", "id int primary key, raw bytea").await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    client
        .execute(
            "insert into pay_bytes values (1, '\\x0102030405'::bytea)",
            &[],
        )
        .await
        .expect("a row with bytes in it");

    let changes = until(&mut tap, "pay_bytes", 1).await;
    assert_eq!(changes.len(), 1);
    let payload = payload(&tap, &changes[0]).await;
    assert_eq!(
        payload["record"]["raw"], "0102030405",
        "upstream sends what wal2json wrote, which is the hex without the prefix postgres prints"
    );
    assert_eq!(
        expected(&client, "pay_bytes").await["raw"],
        "\\x0102030405",
        "and this is the difference, which is postgres's prefix and not upstream's"
    );
}

/// A domain is a name over a type, and a payload carries the name while
/// the value is converted as the type, which is what upstream's cast to
/// the column's own type does.
#[tokio::test]
async fn a_domain_carries_its_own_name_and_the_json_of_what_it_is_made_of() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    client
        .batch_execute(
            "drop table if exists pay_domain;
             drop domain if exists pay_count;
             create domain pay_count as int check (value >= 0)",
        )
        .await
        .expect("a domain");
    published(&client, "pay_domain", "id int primary key, seen pay_count").await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    client
        .execute("insert into pay_domain values (1, 7)", &[])
        .await
        .expect("a row");

    let changes = until(&mut tap, "pay_domain", 1).await;
    assert_eq!(changes.len(), 1);
    let payload = payload(&tap, &changes[0]).await;
    assert_eq!(
        payload["record"]["seen"], 7,
        "a domain over an int holds an int, and a client reading a string here would be reading a \
         different column from the one upstream sends"
    );
    assert_eq!(
        payload["columns"][1],
        serde_json::json!({"name": "seen", "type": "pay_count"}),
        "the column's type is the domain, which is what it was declared as"
    );
    assert_eq!(payload["record"], expected(&client, "pay_domain").await);
}

/// What a delete says about the row that is gone, under the identity
/// every table has until somebody changes it.
///
/// Postgres pads the tuple it publishes with nulls for the columns that
/// are not part of the key. A payload that carried them would tell a
/// subscriber that a deleted row had a null title, which is not what
/// upstream sends and not true.
#[tokio::test]
async fn an_old_record_is_what_the_table_publishes_and_not_the_padding() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(
        &client,
        "pay_gone",
        "id int primary key, title text, done boolean",
    )
    .await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    client
        .batch_execute(
            "insert into pay_gone values (1, 'wash up', false);
             delete from pay_gone where id = 1",
        )
        .await
        .expect("a row and its deletion");

    let changes = until(&mut tap, "pay_gone", 2).await;
    assert_eq!(changes.len(), 2);
    let payload = payload(&tap, &changes[1]).await;
    assert_eq!(payload["type"], "DELETE");
    assert_eq!(payload.get("record"), None, "a deleted row has no record");
    assert_eq!(
        payload["old_record"],
        serde_json::json!({"id": 1}),
        "the default identity publishes the key and postgres pads the rest with nulls nobody wrote"
    );
    assert_eq!(
        payload["columns"],
        serde_json::json!([
            {"name": "id", "type": "int4"},
            {"name": "title", "type": "text"},
            {"name": "done", "type": "bool"},
        ]),
        "the columns are the table's, whatever the change published of them"
    );
}

/// With the identity full, the old record is the whole row, converted
/// the same way the new one is.
#[tokio::test]
async fn a_table_that_publishes_its_old_rows_converts_them_too() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(
        &client,
        "pay_full",
        "id int primary key, count int, at timestamptz",
    )
    .await;
    client
        .batch_execute("alter table pay_full replica identity full")
        .await
        .expect("full identity");
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    client
        .batch_execute(
            "insert into pay_full values (1, 1, '2021-11-05 17:20:51.524+00');
             update pay_full set count = 2 where id = 1",
        )
        .await
        .expect("two statements");

    let changes = until(&mut tap, "pay_full", 2).await;
    assert_eq!(changes.len(), 2);
    let payload = payload(&tap, &changes[1]).await;
    assert_eq!(
        payload["old_record"],
        serde_json::json!({
            "id": 1,
            "count": 1,
            "at": "2021-11-05T17:20:51.524+00:00",
        })
    );
    assert_eq!(payload["record"]["count"], 2);
    assert_eq!(payload["record"], expected(&client, "pay_full").await);
}

/// A commit timestamp is the time the transaction committed, in the
/// format upstream's `to_char` writes, which is milliseconds and a Z.
#[tokio::test]
async fn a_commit_timestamp_is_the_format_upstream_sends() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(&client, "pay_when", "id int primary key").await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    client
        .execute("insert into pay_when values (1)", &[])
        .await
        .expect("a row");

    let changes = until(&mut tap, "pay_when", 1).await;
    assert_eq!(changes.len(), 1);
    let payload = payload(&tap, &changes[0]).await;
    let stamp = payload["commit_timestamp"].as_str().expect("a timestamp");
    // What postgres itself would have written for that instant in the
    // format the payload uses, which is upstream's format string.
    let want: String = client
        .query_one(
            "select to_char(timestamp 'epoch' + $1::bigint * interval '1 microsecond', \
             'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')",
            &[&changes[0].commit_ts],
        )
        .await
        .expect("the format")
        .get(0);
    assert_eq!(stamp, want);
    assert!(stamp.ends_with('Z'), "{stamp}");
}
