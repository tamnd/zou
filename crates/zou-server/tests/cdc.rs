//! The tap, against a live postgres.
//!
//! What is pinned here is the half the decoder's own tests cannot say:
//! that the bytes postgres actually writes are the bytes the decoder
//! was written against. Those tests build their fixtures by hand from
//! the protocol documentation, which proves the reading and proves
//! nothing about whether postgres agrees. This runs the statements and
//! reads what came out.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, and skips again
//! when that database is not running with `wal_level = logical`, since
//! then postgres wrote no logical decoding information and there is
//! nothing here to prove.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test cdc
//!
//! Every table these make is named for the test that made it and
//! dropped at the start of it, because the suites run against one
//! database at once and a table left behind from a failed run would
//! otherwise be in the publication for the next one.

use tokio_postgres::{Client, NoTls};
use zou_server::cdc::{Closed, PUBLICATION, Tap};
use zou_server::pgoutput::{Cell, Change, Op, Replica};

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

/// A connection with the bootstrap contract applied, which is where
/// the publication comes from. Idempotent, so every test asking is a
/// few catalog reads after the first one.
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
}

/// A publication and a table in it, the way a project sets one up.
///
/// The publication is the bootstrap contract's, so this asks for it the
/// way the server does rather than creating it, which also means a
/// database where the contract could not create one fails here loudly.
async fn published(client: &Client, table: &str) {
    client
        .batch_execute(&format!(
            "drop table if exists {table};
             create table {table} (id int primary key, title text, note text)"
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

/// A tap, or nothing when this database cannot have one.
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

/// What a change says about itself, flattened to the columns the tests
/// compare, which keeps an assertion readable.
fn row(change: &Change) -> Vec<Option<String>> {
    change
        .record
        .iter()
        .map(|cell| match cell {
            Cell::Text(text) => Some(text.clone()),
            _ => None,
        })
        .collect()
}

/// Poll for this test's own tables until `many` changes have arrived or
/// the tries run out.
///
/// Two things make this a loop over a filter rather than one read.
/// Logical decoding hands over committed transactions, and a commit
/// returning to the writer and the decoder seeing it are not the same
/// instant. And a tap hears every table in the publication, which is
/// the point of it, so a suite whose tests run at the same time has
/// each tap hearing the others' rows.
async fn until(tap: &mut Tap, tables: &[&str], many: usize) -> Vec<Change> {
    let mut changes = Vec::new();
    for _ in 0..100 {
        let read = tap.changes(0).await.expect("a read");
        changes.extend(
            read.into_iter()
                .filter(|change| tables.contains(&change.relation.table.as_str())),
        );
        if changes.len() >= many {
            return changes;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    changes
}

/// The whole thing end to end: three statements in, three changes out,
/// named and typed and stamped the way a payload will need them.
#[tokio::test]
async fn what_postgres_wrote_is_what_the_decoder_reads() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(&client, "cdc_wrote").await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    client
        .batch_execute(
            "insert into cdc_wrote values (1, 'wash up', null);
             update cdc_wrote set title = 'washed up' where id = 1;
             delete from cdc_wrote where id = 1",
        )
        .await
        .expect("three statements");

    let changes = until(&mut tap, &["cdc_wrote"], 3).await;
    assert_eq!(changes.len(), 3, "an insert, an update and a delete");

    let insert = &changes[0];
    assert_eq!(insert.op, Op::Insert);
    assert_eq!(insert.relation.schema, "public");
    assert_eq!(insert.relation.table, "cdc_wrote");
    assert_eq!(insert.relation.replica, Replica::Default);
    assert_eq!(
        insert
            .relation
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.key))
            .collect::<Vec<_>>(),
        vec![("id", true), ("title", false), ("note", false)],
        "the primary key is the only identifying column"
    );
    assert_eq!(
        row(insert),
        vec![Some("1".into()), Some("wash up".into()), None]
    );
    assert_eq!(insert.old, None);

    let update = &changes[1];
    assert_eq!(update.op, Op::Update);
    assert_eq!(
        row(update),
        vec![Some("1".into()), Some("washed up".into()), None]
    );
    assert_eq!(
        update.old, None,
        "the key did not change, so postgres published no old row"
    );

    let delete = &changes[2];
    assert_eq!(delete.op, Op::Delete);
    assert!(delete.record.is_empty());
    assert!(delete.old_key, "the default identity publishes the key");
    assert_eq!(
        delete.old.as_ref().map(|old| old[0].clone()),
        Some(Cell::Text("1".into())),
        "which id is gone"
    );
}

/// Every change carries the commit time of the transaction that wrote
/// it, which is what a payload's commit_timestamp is made from, and the
/// arithmetic that turns the postgres epoch into the unix one is the
/// kind of thing that is wrong by thirty years or not at all.
#[tokio::test]
async fn a_change_is_stamped_with_a_time_that_is_actually_now() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(&client, "cdc_stamped").await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    let before: i64 = client
        .query_one("select (extract(epoch from now()) * 1000000)::bigint", &[])
        .await
        .expect("the time")
        .get(0);
    client
        .execute("insert into cdc_stamped values (1, 'now', null)", &[])
        .await
        .expect("an insert");

    let changes = until(&mut tap, &["cdc_stamped"], 1).await;
    assert_eq!(changes.len(), 1);
    let stamped = changes[0].commit_ts;
    assert!(
        stamped >= before,
        "{stamped} is before the statement that wrote it, {before}"
    );
    // A minute is more slack than a test needs and less than the thirty
    // years an epoch this got wrong would be out by.
    assert!(
        stamped < before + 60_000_000,
        "{stamped} is a minute past the statement that wrote it, {before}"
    );
}

/// With the identity set to full an update carries the row it replaced,
/// which is the only way old_record is worth reading and the reason the
/// decoder tells the two shapes apart.
#[tokio::test]
async fn a_table_that_publishes_its_old_rows_sends_them() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(&client, "cdc_full").await;
    client
        .batch_execute("alter table cdc_full replica identity full")
        .await
        .expect("full identity");
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    client
        .batch_execute(
            "insert into cdc_full values (1, 'wash up', null);
             update cdc_full set title = 'washed up' where id = 1",
        )
        .await
        .expect("two statements");

    let changes = until(&mut tap, &["cdc_full"], 2).await;
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[1].relation.replica, Replica::Full);
    assert!(!changes[1].old_key, "the whole row, not the key");
    assert_eq!(
        changes[1].old,
        Some(vec![
            Cell::Text("1".into()),
            Cell::Text("wash up".into()),
            Cell::Null,
        ])
    );
}

/// A value stored out of line that the statement did not write is not
/// sent, which is the one part of the format a hand written fixture
/// cannot honestly prove: it takes a value big enough for postgres to
/// push out of the row, and postgres decides when that is.
#[tokio::test]
async fn a_toasted_column_nobody_wrote_arrives_as_unchanged() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(&client, "cdc_toast").await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    // Thirty two kilobytes of hex rather than a repeated character,
    // because postgres compresses before it stores out of line and a
    // hundred kilobytes of one letter compresses to nothing and stays
    // in the row.
    client
        .batch_execute(
            "insert into cdc_toast
                 select 1, 'wash up', string_agg(md5(random()::text), '')
                 from generate_series(1, 1000);
             update cdc_toast set title = 'washed up' where id = 1",
        )
        .await
        .expect("two statements");

    let changes = until(&mut tap, &["cdc_toast"], 2).await;
    assert_eq!(changes.len(), 2);
    assert_eq!(
        changes[1].record[2],
        Cell::Unchanged,
        "the note is stored out of line and the update did not touch it"
    );
    assert_eq!(changes[1].record[1], Cell::Text("washed up".into()));
}

/// A table nobody added to the publication is not this server's
/// business, which is what makes postgres changes opt in rather than
/// every write in the database going out to sockets.
#[tokio::test]
async fn a_table_outside_the_publication_says_nothing() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(&client, "cdc_in").await;
    client
        .batch_execute(
            "drop table if exists cdc_out;
             create table cdc_out (id int primary key, title text)",
        )
        .await
        .expect("a table nobody published");
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    client
        .batch_execute(
            "insert into cdc_out values (1, 'quiet');
             insert into cdc_in values (1, 'heard', null)",
        )
        .await
        .expect("two inserts");

    let changes = until(&mut tap, &["cdc_in", "cdc_out"], 1).await;
    assert_eq!(changes.len(), 1, "only the published table");
    assert_eq!(changes[0].relation.table, "cdc_in");
}

/// The slot goes when the tap does, which is the whole reason for a
/// temporary one: a server with nobody subscribed retains no write
/// ahead log and leaves nothing behind to clean up.
#[tokio::test]
async fn the_slot_lives_exactly_as_long_as_the_tap() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    let Some(tap) = tapping(&dsn).await else {
        return;
    };
    let slot = tap.slot().to_string();
    let held: bool = client
        .query_one(
            "select exists (select 1 from pg_replication_slots where slot_name = $1)",
            &[&slot],
        )
        .await
        .expect("the slots")
        .get(0);
    assert!(held, "{slot} is not there while the tap is");
    assert!(slot.starts_with("zou_cdc_"), "{slot}");

    drop(tap);
    // The slot goes with the session, and the backend noticing its
    // client hung up is not instant.
    for _ in 0..50 {
        let still: bool = client
            .query_one(
                "select exists (select 1 from pg_replication_slots where slot_name = $1)",
                &[&slot],
            )
            .await
            .expect("the slots")
            .get(0);
        if !still {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("{slot} outlived the tap that held it");
}
