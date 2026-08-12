//! The whole path, against a live postgres.
//!
//! Every other suite in this set checks one piece: the decoder against
//! bytes, the matcher against bindings, the payload against `to_jsonb`,
//! the visibility check against real policies. This is the one that
//! writes a row and waits for it to come out of a queue, which is the
//! only test that can fail because two correct pieces were wired
//! together wrong.
//!
//! Two properties beyond delivery are worth a test of their own here,
//! because both are about a database rather than about a client. A
//! logical slot pins the write ahead log, so a server with nobody
//! subscribed must be holding no slot at all. And a policy is checked
//! on the way out, so a row a subscriber may not see must never reach
//! their queue rather than being filtered by whatever reads it.
//!
//! Gated on ZOU_PG_TEST_DSN and skipped when that database is not
//! logical, the same as the tap's own suite.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test reader

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};
use zou_server::binding::Binding;
use zou_server::cdc::PUBLICATION;
use zou_server::reader::{Changes, Heard, Listening, Reader};
use zou_server::sql::Pool;
use zou_server::visible::Asker;

const ANA: &str = "11111111-1111-1111-1111-111111111111";
const BEN: &str = "22222222-2222-2222-2222-222222222222";

/// One reader at a time.
///
/// A slot only carries what was written after it was taken, so a test
/// that wrote its row while another test's reader was starting would be
/// a test that sometimes waits for a change nobody recorded. Serialising
/// them makes the wait below exact rather than hopeful.
fn alone() -> &'static Mutex<()> {
    static ALONE: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();
    ALONE.get_or_init(|| Mutex::new(()))
}

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

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

/// Whether this database can do logical decoding at all, which is a
/// postmaster setting and so a skip rather than a failure.
async fn logical(client: &Client) -> bool {
    let level: String = client
        .query_one("show wal_level", &[])
        .await
        .expect("the wal level")
        .get(0);
    if level != "logical" {
        eprintln!("skipping: wal_level is {level}");
    }
    level == "logical"
}

async fn published(client: &Client, table: &str, ddl: &str) {
    client
        .batch_execute(&format!(
            "drop table if exists {table};
             {ddl}
             alter publication {PUBLICATION} add table {table}"
        ))
        .await
        .expect("a table in the publication");
}

/// How many slots this server is holding, which is the number that has
/// to be zero when nobody is subscribed.
async fn slots(client: &Client) -> i64 {
    client
        .query_one(
            "select count(*) from pg_replication_slots where slot_name like 'zou\\_cdc\\_%'",
            &[],
        )
        .await
        .expect("the slots")
        .get(0)
}

/// Wait until the reader has a slot, so that what is written next is
/// written into it rather than before it.
async fn tapped(client: &Client) {
    for _ in 0..200 {
        if slots(client).await > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the reader never took a slot");
}

/// Wait until nothing is holding a slot, which is what dropping the
/// last subscriber is supposed to do.
async fn untapped(client: &Client) -> bool {
    for _ in 0..200 {
        if slots(client).await == 0 {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}

fn user(sub: &str) -> Asker {
    Asker {
        role: "authenticated".into(),
        claims: json!({"sub": sub, "role": "authenticated"}),
    }
}

fn binding(table: &str) -> Binding {
    Binding::of(&json!({"event": "*", "schema": "public", "table": table})).expect("a binding")
}

/// The next thing this subscriber hears, or nothing within the wait.
async fn next(who: &mut Listening, within: Duration) -> Option<Heard> {
    tokio::time::timeout(within, who.heard.recv())
        .await
        .ok()
        .flatten()
}

fn data(heard: Heard) -> Value {
    match heard {
        Heard::Change { data, .. } => data,
        Heard::Gap => panic!("a gap rather than a change"),
    }
}

#[tokio::test]
async fn a_changed_row_comes_out_of_the_queue_of_whoever_asked_for_it() {
    let Some(dsn) = dsn() else { return };
    let _one = alone().lock().await;
    let client = connect(&dsn).await;
    if !logical(&client).await {
        return;
    }
    published(
        &client,
        "read_todos",
        "create table read_todos (id int primary key, details text);
         grant select on read_todos to anon, authenticated;",
    )
    .await;
    published(
        &client,
        "read_other",
        "create table read_other (id int primary key);
         grant select on read_other to anon, authenticated;",
    )
    .await;

    let changes = Arc::new(Changes::new());
    let reader = Reader::new(
        &dsn,
        Pool::new(&dsn, 4).expect("a pool"),
        Arc::clone(&changes),
    )
    .every(Duration::from_millis(10));
    let running = tokio::spawn(reader.run());

    let mut ana = changes.listen(user(ANA));
    let bound = changes
        .bind(ana.id, binding("read_todos"))
        .expect("a binding");
    tapped(&client).await;

    client
        .batch_execute(
            "insert into read_other values (1);
             insert into read_todos values (1, 'wash up');
             update read_todos set details = 'washed up' where id = 1;
             delete from read_todos where id = 1",
        )
        .await
        .expect("a row's whole life");

    let within = Duration::from_secs(10);
    let first = next(&mut ana, within).await.expect("the insert");
    match &first {
        Heard::Change { ids, .. } => assert_eq!(
            ids,
            &vec![bound],
            "the id is the client's own, which is how it knows which subscription this answers"
        ),
        Heard::Gap => panic!("a gap rather than the insert"),
    }
    let insert = data(first);
    assert_eq!(insert["type"], "INSERT");
    assert_eq!(insert["table"], "read_todos");
    assert_eq!(insert["record"], json!({"id": 1, "details": "wash up"}));

    let update = data(next(&mut ana, within).await.expect("the update"));
    assert_eq!(update["type"], "UPDATE");
    assert_eq!(update["record"]["details"], "washed up");

    let delete = data(next(&mut ana, within).await.expect("the delete"));
    assert_eq!(delete["type"], "DELETE");
    assert_eq!(
        delete["old_record"],
        json!({"id": 1}),
        "the default replica identity publishes the key, and the payload says so rather than \
         padding it out with nulls"
    );

    assert_eq!(
        next(&mut ana, Duration::from_millis(200)).await,
        None,
        "the other table was changed too and nobody asked about it"
    );
    running.abort();
}

#[tokio::test]
async fn a_row_a_policy_hides_never_reaches_the_subscriber_it_hides_it_from() {
    let Some(dsn) = dsn() else { return };
    let _one = alone().lock().await;
    let client = connect(&dsn).await;
    if !logical(&client).await {
        return;
    }
    published(
        &client,
        "read_own",
        "create table read_own (id int primary key, owner uuid, details text);
         grant select on read_own to anon, authenticated;
         alter table read_own enable row level security;
         create policy mine on read_own for select using (owner = auth.uid());",
    )
    .await;

    let changes = Arc::new(Changes::new());
    let reader = Reader::new(
        &dsn,
        Pool::new(&dsn, 4).expect("a pool"),
        Arc::clone(&changes),
    )
    .every(Duration::from_millis(10));
    let running = tokio::spawn(reader.run());

    let mut ana = changes.listen(user(ANA));
    let mut ben = changes.listen(user(BEN));
    changes
        .bind(ana.id, binding("read_own"))
        .expect("a binding");
    changes
        .bind(ben.id, binding("read_own"))
        .expect("a binding");
    tapped(&client).await;

    client
        .batch_execute(&format!(
            "insert into read_own values (1, '{ANA}', 'hers'), (2, '{BEN}', 'his')"
        ))
        .await
        .expect("two rows");

    let within = Duration::from_secs(10);
    let hers = data(next(&mut ana, within).await.expect("her own row"));
    assert_eq!(hers["record"]["details"], "hers");
    assert_eq!(
        next(&mut ana, Duration::from_millis(300)).await,
        None,
        "his row was checked against her claims and did not reach her queue, which is the \
         difference between a change feed and a way around the policy"
    );

    let his = data(next(&mut ben, within).await.expect("his own row"));
    assert_eq!(
        his["record"]["details"], "his",
        "and the same change reached the person it belongs to, so the row was withheld rather \
         than lost"
    );
    running.abort();
}

#[tokio::test]
async fn nothing_holds_a_slot_open_when_nobody_is_listening() {
    let Some(dsn) = dsn() else { return };
    let _one = alone().lock().await;
    let client = connect(&dsn).await;
    if !logical(&client).await {
        return;
    }
    published(
        &client,
        "read_slots",
        "create table read_slots (id int primary key);
         grant select on read_slots to anon, authenticated;",
    )
    .await;

    let changes = Arc::new(Changes::new());
    let reader = Reader::new(
        &dsn,
        Pool::new(&dsn, 4).expect("a pool"),
        Arc::clone(&changes),
    )
    .every(Duration::from_millis(10));
    let running = tokio::spawn(reader.run());

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        slots(&client).await,
        0,
        "a running reader with no subscribers holds nothing, because a slot nobody reads is write \
         ahead log a database cannot free"
    );

    let ana = changes.listen(user(ANA));
    changes
        .bind(ana.id, binding("read_slots"))
        .expect("a binding");
    tapped(&client).await;

    changes.hung_up(ana.id);
    drop(ana);
    assert!(
        untapped(&client).await,
        "and the slot goes with the last subscriber rather than lingering until the server does"
    );
    running.abort();
}
