//! Postgres changes over a real socket, against a real database.
//!
//! Every piece of this has a suite of its own already: the decoder
//! against bytes, the matcher against bindings, the payload against
//! `to_jsonb`, the visibility check against real policies, the reader
//! against a queue. This is the one that opens a websocket, subscribes
//! the way realtime-js subscribes, writes a row with sql, and waits for
//! the row to come back down the socket.
//!
//! What it is really asking is whether a supabase-js client would work,
//! which comes down to two things that no unit test can see. The join
//! reply has to hand back the client's own subscription entries with an
//! id added, because realtime-js compares them field by field and
//! unsubscribes the channel when any of them differ. And the change has
//! to arrive on the `postgres_changes` event with the ids of the
//! subscriptions it answers, because that is how the client picks which
//! callback to run.
//!
//! Gated on ZOU_PG_TEST_DSN and skipped when that database is not
//! logical, like the other suites that need a replication slot.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test changes

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use zou_server::sql::Pool;
use zou_server::{Config, jwt, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

/// The two people these tests sign in as, uuids because `auth.uid()`
/// casts the subject to one.
const ANA: &str = "11111111-1111-1111-1111-111111111111";
const BEN: &str = "22222222-2222-2222-2222-222222222222";

/// One suite at a time on this database.
///
/// A slot only carries what was written after it was taken, and this
/// server takes one on the first subscription, so two tests subscribing
/// and writing at once would be two tests waiting on each other's rows.
fn alone() -> &'static tokio::sync::Mutex<()> {
    static ALONE: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    ALONE.get_or_init(|| tokio::sync::Mutex::new(()))
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

/// Whether this database can decode logically at all, which is a
/// postmaster setting and so a skip rather than a failure.
async fn logical(dsn: &str) -> bool {
    let level: String = one(dsn, "show wal_level").await;
    if level != "logical" {
        eprintln!("skipping: wal_level is {level}");
    }
    level == "logical"
}

async fn one<T: for<'a> tokio_postgres::types::FromSql<'a>>(dsn: &str, sql: &str) -> T {
    let pool = Pool::new(dsn, 2).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    let rows = sess.query(sql, &[]).await.expect(sql);
    let value = rows[0].get(0);
    sess.commit().await.expect("done");
    value
}

/// Run some sql as the owner, which is who the writer of a row is.
async fn run(dsn: &str, sql: &str) {
    let pool = Pool::new(dsn, 2).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(sql, &[]).await.expect(sql);
    sess.commit().await.expect("it commits");
}

/// A table in the publication, which is the one thing a project has to
/// do by hand before any of this carries anything.
///
/// On a connection of its own rather than through the pool, because
/// this is several statements and the pool prepares what it is given.
async fn published(dsn: &str, table: &str, ddl: &str) {
    let (client, connection) = dsn
        .parse::<tokio_postgres::Config>()
        .expect("a dsn")
        .connect(tokio_postgres::NoTls)
        .await
        .expect("a connection");
    let held = tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "drop table if exists {table};
             {ddl}
             alter publication supabase_realtime add table {table}"
        ))
        .await
        .expect("a table in the publication");
    held.abort();
}

/// A table that exists and that nobody added to the publication,
/// which is the state a project is in between writing a table and
/// remembering the `alter publication`.
async fn unpublished(dsn: &str, table: &str, ddl: &str) {
    let (client, connection) = dsn
        .parse::<tokio_postgres::Config>()
        .expect("a dsn")
        .connect(tokio_postgres::NoTls)
        .await
        .expect("a connection");
    let held = tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("drop table if exists {table}; {ddl}"))
        .await
        .expect("a table outside the publication");
    held.abort();
}

async fn serving(dsn: &str) -> SocketAddr {
    let app = router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        ..Config::default()
    })
    .expect("router builds");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
    let at = listener.local_addr().expect("the port");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    at
}

/// How many replication slots this server is holding.
async fn slots(dsn: &str) -> i64 {
    one(
        dsn,
        "select count(*) from pg_replication_slots where slot_name like 'zou\\_cdc\\_%'",
    )
    .await
}

/// The join has been answered, so there is a slot.
///
/// This used to be a wait, because the reply came back before the
/// reader had taken one and a row written straight afterwards was
/// written before the slot existed and lost. Now the reply waits for
/// the tap, so this is the assertion that it still does: what a client
/// writes the moment it is told it is subscribed is a row it hears
/// about.
async fn tapping(dsn: &str) {
    assert!(
        slots(dsn).await > 0,
        "the join was answered before anything was reading the write ahead log, so the next \
         write would go into a database nobody had a slot on"
    );
}

/// Wait until no slot of ours is open anywhere, which is where a test
/// that counts them has to start.
///
/// The count is server wide and cannot be anything else, because a slot
/// is named after the backend that took it and nothing knows that name
/// in advance. So a slot another suite in the same run is still letting
/// go of reads here as this one's own, and waiting it out is cheaper
/// than a test that fails on a machine with a slow database.
async fn settled(dsn: &str) {
    for _ in 0..400 {
        if slots(dsn).await == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("something else is holding a replication slot of ours");
}

fn anon_key() -> String {
    jwt::mint(&jwt::key_claims("anon"), SECRET)
}

fn token(sub: &str) -> String {
    jwt::mint(
        &json!({"role": "authenticated", "sub": sub, "exp": 4102444800u64}),
        SECRET,
    )
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(at: SocketAddr) -> Socket {
    let url = format!(
        "ws://{at}/realtime/v1/websocket?apikey={}&vsn=2.0.0",
        anon_key()
    );
    let (socket, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("the socket upgrades");
    socket
}

async fn send(socket: &mut Socket, text: &str) {
    socket
        .send(Message::Text(text.into()))
        .await
        .expect("the socket takes it");
}

/// The next frame this socket hears, waited for as long as a change
/// coming out of a database that was just written to can take.
async fn next(socket: &mut Socket) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(10), socket.next())
        .await
        .expect("something arrives within ten seconds")
        .expect("the socket is still open")
        .expect("a message rather than an error");
    match message {
        Message::Text(text) => serde_json::from_str(&text).expect("json"),
        other => panic!("{other:?} is not text"),
    }
}

/// Nothing arrives on this socket for a moment.
async fn quiet(socket: &mut Socket) {
    let heard = tokio::time::timeout(Duration::from_millis(400), socket.next()).await;
    assert!(heard.is_err(), "something arrived that should not have");
}

/// Subscribe the way realtime-js does: a join whose config carries the
/// list of changes wanted, and a token saying who is asking.
async fn subscribe(socket: &mut Socket, sub: &str, wants: Value) -> Value {
    let token = token(sub);
    send(
        socket,
        &json!(["1", "1", "realtime:db", "phx_join", {
            "config": {"postgres_changes": wants},
            "access_token": token,
        }])
        .to_string(),
    )
    .await;
    let reply = next(socket).await;
    assert_eq!(reply[4]["status"], "ok", "{reply}");
    // And the line upstream says after it, which is on the socket before
    // any change is, so it is read here rather than by whoever is
    // waiting for a row.
    let system = next(socket).await;
    assert_eq!(system[3], "system", "{system}");
    assert_eq!(
        system[4],
        json!({
            "message": "Subscribed to PostgreSQL",
            "status": "ok",
            "extension": "postgres_changes",
            "channel": "db",
        }),
        "the frame Supabase Realtime sends once a join's subscriptions exist"
    );
    reply[4]["response"].clone()
}

/// The payload of the next postgres change on this socket.
async fn changed(socket: &mut Socket) -> Value {
    let frame = next(socket).await;
    assert_eq!(frame[3], "postgres_changes", "{frame}");
    frame[4].clone()
}

/// A whole row's life, seen by the client that asked for it.
#[tokio::test]
async fn a_row_written_with_sql_arrives_on_the_socket_that_subscribed_to_it() {
    let Some(dsn) = dsn() else { return };
    let _one = alone().lock().await;
    if !logical(&dsn).await {
        return;
    }
    settled(&dsn).await;
    let at = serving(&dsn).await;
    published(
        &dsn,
        "chg_todos",
        "create table chg_todos (id int primary key, details text);
         grant select on chg_todos to anon, authenticated;",
    )
    .await;

    let mut socket = connect(at).await;
    let response = subscribe(
        &mut socket,
        ANA,
        json!([
            {"event": "*", "schema": "public", "table": "chg_todos"},
            {"event": "INSERT", "schema": "public", "table": "chg_todos", "filter": "id=eq.2"},
        ]),
    )
    .await;
    let echoed = response["postgres_changes"]
        .as_array()
        .expect("the reply carries the subscriptions")
        .clone();
    assert_eq!(echoed.len(), 2, "{response}");
    assert_eq!(
        echoed[0]["event"], "*",
        "the entries come back as they were sent, in the order they were sent, because \
         realtime-js walks its own bindings and this list together and unsubscribes the channel \
         when a field differs"
    );
    assert_eq!(echoed[1]["filter"], "id=eq.2");
    assert!(echoed[0].get("filter").is_none(), "{response}");
    assert!(echoed[0]["id"].is_number(), "{response}");
    let both = vec![echoed[0]["id"].clone(), echoed[1]["id"].clone()];
    tapping(&dsn).await;

    run(&dsn, "insert into chg_todos values (1, 'wash up')").await;
    let insert = changed(&mut socket).await;
    assert_eq!(
        insert["ids"],
        json!([both[0]]),
        "one of the two subscriptions wanted this row, so one id, {insert}"
    );
    assert_eq!(insert["data"]["type"], "INSERT");
    assert_eq!(insert["data"]["table"], "chg_todos");
    assert_eq!(
        insert["data"]["record"],
        json!({"id": 1, "details": "wash up"})
    );

    run(&dsn, "insert into chg_todos values (2, 'and again')").await;
    let filtered = changed(&mut socket).await;
    assert_eq!(
        filtered["ids"],
        json!(both),
        "and a row both subscriptions wanted arrives once carrying both ids rather than twice, \
         {filtered}"
    );

    run(
        &dsn,
        "update chg_todos set details = 'washed up' where id = 1",
    )
    .await;
    let update = changed(&mut socket).await;
    assert_eq!(update["data"]["type"], "UPDATE");
    assert_eq!(update["data"]["record"]["details"], "washed up");

    run(&dsn, "delete from chg_todos where id = 1").await;
    let delete = changed(&mut socket).await;
    assert_eq!(delete["data"]["type"], "DELETE");
    assert_eq!(
        delete["data"]["old_record"],
        json!({"id": 1}),
        "the default replica identity publishes the key and the payload says so"
    );

    // The delivery latency is counted here rather than in a test of
    // its own, because what could be wrong about it is the wiring: a
    // number observed in a unit test proves the arithmetic and nothing
    // about whether a change that reached a socket was ever measured.
    //
    // Waited for rather than read once, since the count goes up after
    // the frame has gone out and the client is what read it.
    let mut scrape = String::new();
    let mut counted = 0;
    for _ in 0..200 {
        scrape = zou_ops::metrics::registry().render();
        counted = scrape
            .lines()
            .find_map(|line| line.strip_prefix("zou_realtime_changes_total "))
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or_default();
        if counted >= 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        counted >= 4,
        "four frames went out on this socket and each of them is a delivery, {counted}"
    );
    assert!(
        scrape.contains("zou_realtime_commit_to_socket_seconds_bucket"),
        "{scrape}"
    );
}

/// The policies decide what a socket is told, not just what it can
/// select, which is the whole difference between a change feed and a
/// way around row level security.
#[tokio::test]
async fn a_row_a_policy_hides_never_reaches_the_socket_it_hides_it_from() {
    let Some(dsn) = dsn() else { return };
    let _one = alone().lock().await;
    if !logical(&dsn).await {
        return;
    }
    settled(&dsn).await;
    let at = serving(&dsn).await;
    published(
        &dsn,
        "chg_own",
        "create table chg_own (id int primary key, owner uuid, details text);
         grant select on chg_own to anon, authenticated;
         alter table chg_own enable row level security;
         create policy mine on chg_own for select using (owner = auth.uid());",
    )
    .await;

    let wants = json!([{"event": "*", "schema": "public", "table": "chg_own"}]);
    let mut hers = connect(at).await;
    let mut his = connect(at).await;
    subscribe(&mut hers, ANA, wants.clone()).await;
    subscribe(&mut his, BEN, wants).await;
    tapping(&dsn).await;

    run(
        &dsn,
        &format!("insert into chg_own values (1, '{ANA}', 'hers'), (2, '{BEN}', 'his')"),
    )
    .await;

    let ana = changed(&mut hers).await;
    assert_eq!(ana["data"]["record"]["details"], "hers");
    quiet(&mut hers).await;

    let ben = changed(&mut his).await;
    assert_eq!(
        ben["data"]["record"]["details"], "his",
        "the same insert reached the person it belongs to, so the other row was withheld rather \
         than lost"
    );
}

/// A channel that goes takes its subscriptions with it, and the last
/// A subscription to a table nobody published, which is the most
/// common way a subscription that reads correctly hears nothing.
///
/// The channel joins, the reply carries an id per entry because the
/// client compares that list with its own bindings before it reads
/// anything else, and the system frame carries the reason instead of
/// `Subscribed to PostgreSQL`. Upstream draws it here rather than at
/// the reply, and one entry it cannot subscribe takes the whole list
/// with it, so a channel that asked for a published table and an
/// unpublished one hears about neither.
#[tokio::test]
async fn a_table_nobody_published_is_refused_on_the_system_frame() {
    let Some(dsn) = dsn() else { return };
    let _one = alone().lock().await;
    if !logical(&dsn).await {
        return;
    }
    settled(&dsn).await;
    let at = serving(&dsn).await;
    published(
        &dsn,
        "chg_open",
        "create table chg_open (id int primary key);
         grant select on chg_open to anon, authenticated;",
    )
    .await;
    unpublished(
        &dsn,
        "chg_shut",
        "create table chg_shut (id int primary key);
         grant select on chg_shut to anon, authenticated;",
    )
    .await;

    let mut socket = connect(at).await;
    send(
        &mut socket,
        &json!(["1", "1", "realtime:db", "phx_join", {
            "config": {"postgres_changes": [
                {"event": "*", "schema": "public", "table": "chg_open"},
                {"event": "INSERT", "schema": "public", "table": "chg_shut", "filter": "id=eq.7"},
            ]},
            "access_token": token(ANA),
        }])
        .to_string(),
    )
    .await;
    let reply = next(&mut socket).await;
    assert_eq!(reply[4]["status"], "ok", "{reply}");
    let echoed = reply[4]["response"]["postgres_changes"]
        .as_array()
        .expect("the reply carries the subscriptions")
        .clone();
    assert_eq!(echoed.len(), 2, "{reply}");
    assert!(echoed[0]["id"].is_number(), "{reply}");

    let system = next(&mut socket).await;
    assert_eq!(system[3], "system", "{system}");
    assert_eq!(
        system[4],
        json!({
            "message": "Unable to subscribe to changes with given parameters. Please check \
                        Realtime is enabled for the given connect parameters: [event: INSERT, \
                        schema: public, table: chg_shut, filters: [{\"id\", \"eq\", \"7\", \
                        false}], select: nil]",
            "status": "error",
            "extension": "postgres_changes",
            "channel": "db",
        }),
        "upstream's wording, parameters and all, because it is what somebody reads when their \
         subscription is silent"
    );

    // The published table in the same list, written to and not heard
    // about, which is the half of this that is a decision rather than a
    // message.
    run(&dsn, "insert into chg_open (id) values (1)").await;
    quiet(&mut socket).await;
}

/// socket to go takes the replication slot with it, which is the
/// property that keeps a quiet server from filling a disk.
#[tokio::test]
async fn nothing_is_read_once_the_last_subscriber_has_gone() {
    let Some(dsn) = dsn() else { return };
    let _one = alone().lock().await;
    if !logical(&dsn).await {
        return;
    }
    settled(&dsn).await;
    let at = serving(&dsn).await;
    published(
        &dsn,
        "chg_gone",
        "create table chg_gone (id int primary key);
         grant select on chg_gone to anon, authenticated;",
    )
    .await;

    let mut socket = connect(at).await;
    subscribe(
        &mut socket,
        ANA,
        json!([{"event": "*", "schema": "public", "table": "chg_gone"}]),
    )
    .await;
    tapping(&dsn).await;

    // Closing the socket rather than leaving the channel, because that
    // is what a browser tab does and the harder of the two to get
    // right: nothing sends a leave on the way out.
    socket.close(None).await.expect("the socket closes");
    drop(socket);
    for _ in 0..200 {
        if slots(&dsn).await == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the slot outlived the socket that needed it");
}
