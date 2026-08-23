//! What the bootstrap takes out on a table other people are using.
//!
//! Every boot applies the ddl that has to be applied on every boot,
//! and one piece of it used to drop and recreate the trigger on
//! realtime.messages. Both statements take an AccessExclusiveLock on
//! the table every realtime.send writes into, so a node joining a busy
//! project queued behind the traffic and two of them arriving together
//! deadlocked. See #583.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test boot_locks

use tokio_postgres::{Client, NoTls};
use zou_server::sql::{bootstrap, with_database};

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

/// A connection of its own, outside any pool, because this suite is
/// about what one session does while another one holds a lock.
async fn raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect to {dsn}: {e}"));
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// A database nobody has opened yet, dropped first so a run that died
/// halfway does not decide the next one.
async fn scratch(dsn: &str, name: &str) -> String {
    let admin = raw(dsn).await;
    admin
        .batch_execute(&format!("drop database if exists {name} with (force)"))
        .await
        .expect("drop any leftover");
    admin
        .batch_execute(&format!("create database {name}"))
        .await
        .expect("create scratch");
    with_database(dsn, name)
}

/// The one trigger this suite is about, named the way every query
/// here has to name it: by table as well as by name, since a trigger
/// name is unique per table and not per database.
const TRIGGER: &str = "t.tgrelid = 'realtime.messages'::regclass \
     and t.tgname = 'zou_realtime_sent' and not t.tgisinternal";

/// Its oid, which is a different one after a rewrite and the same one
/// after a boot that left it alone.
async fn oid_of(client: &Client) -> i64 {
    client
        .query_one(
            &format!("select t.oid::int8 from pg_trigger t where {TRIGGER}"),
            &[],
        )
        .await
        .expect("an oid")
        .get(0)
}

/// A boot into a project somebody is already sending to.
///
/// The sender is a transaction holding what an insert holds, a
/// RowExclusiveLock, and keeping it. That does not conflict with
/// anything a boot has business doing, and it conflicts with the
/// AccessExclusiveLock a `drop trigger` takes, so the second boot
/// either walks past it or hangs until the statement timeout fires.
/// Five seconds because the point is to fail quickly when it is
/// wrong, not to measure anything.
#[tokio::test]
async fn a_boot_does_not_wait_for_a_session_that_is_sending() {
    let Some(dsn) = dsn() else { return };
    let fresh = scratch(&dsn, "zou_boot_locks_busy").await;

    // The first boot is the one allowed to write the trigger, and it
    // runs with nobody in the way.
    let first = raw(&fresh).await;
    bootstrap(&first).await.expect("the first boot");

    let sender = raw(&fresh).await;
    sender
        .batch_execute("begin; lock table realtime.messages in row exclusive mode")
        .await
        .expect("hold what an insert holds");

    let second = raw(&fresh).await;
    second
        .batch_execute("set statement_timeout = '5s'")
        .await
        .expect("a bound to fail against");
    let booted = bootstrap(&second).await;

    sender.batch_execute("rollback").await.expect("let go");
    booted.expect("a boot into a busy project");
}

/// And the trigger is still there and still the one this release
/// ships, which is the thing the skip is allowed to assume.
///
/// Asked of pg_get_triggerdef rather than of pg_trigger's columns
/// because that is what the guard in the ddl compares, so a release
/// that changes the trigger and forgets to change the string it is
/// compared against fails here.
#[tokio::test]
async fn the_trigger_a_boot_skipped_writing_is_the_one_it_would_have_written() {
    let Some(dsn) = dsn() else { return };
    let fresh = scratch(&dsn, "zou_boot_locks_trigger").await;

    let client = raw(&fresh).await;
    bootstrap(&client).await.expect("the first boot");
    let written: String = client
        .query_one(
            &format!("select pg_get_triggerdef(t.oid) from pg_trigger t where {TRIGGER}"),
            &[],
        )
        .await
        .expect("the trigger the first boot wrote")
        .get(0);

    // A second boot, which is the one that skips, and then the same
    // question. Same definition, and the same oid, since a rewrite
    // would be a new row.
    let before = oid_of(&client).await;
    bootstrap(&client).await.expect("the second boot");
    let after = oid_of(&client).await;

    assert_eq!(
        written,
        "CREATE TRIGGER zou_realtime_sent AFTER INSERT ON realtime.messages \
         FOR EACH ROW WHEN ((new.extension = 'broadcast'::text)) \
         EXECUTE FUNCTION zou.realtime_sent()"
    );
    assert_eq!(before, after, "the second boot rewrote the trigger");
}

/// A trigger that is wrong is still fixed, which is what makes the
/// skip safe to have.
#[tokio::test]
async fn a_boot_writes_the_trigger_when_what_is_there_is_not_it() {
    let Some(dsn) = dsn() else { return };
    let fresh = scratch(&dsn, "zou_boot_locks_wrong").await;

    let client = raw(&fresh).await;
    bootstrap(&client).await.expect("the first boot");

    // Somebody turned it off, which is the cheapest way to be wrong
    // without being absent.
    client
        .batch_execute("alter table realtime.messages disable trigger zou_realtime_sent")
        .await
        .expect("turn it off");
    bootstrap(&client).await.expect("the second boot");

    let enabled: String = client
        .query_one(
            &format!("select t.tgenabled::text from pg_trigger t where {TRIGGER}"),
            &[],
        )
        .await
        .expect("the trigger")
        .get(0);
    assert_eq!(enabled, "O", "a disabled trigger was left disabled");

    // And gone entirely, which is the first boot's case arriving late.
    client
        .batch_execute("drop trigger zou_realtime_sent on realtime.messages")
        .await
        .expect("take it away");
    bootstrap(&client).await.expect("the third boot");
    let back: i64 = client
        .query_one(
            &format!("select count(*) from pg_trigger t where {TRIGGER}"),
            &[],
        )
        .await
        .expect("count")
        .get(0);
    assert_eq!(back, 1, "the trigger was not put back");
}
