//! The realtime attack suite: a subscriber trying to hear rows they
//! have no claim to.
//!
//! The storage and auth surfaces each have one of these, and this is
//! the realtime version. Everything here opens a real socket, subscribes
//! the way a client subscribes, and then asks what an attacker gets.
//! The other realtime suites are written from the point of view of
//! somebody the policies allow; this one assumes the person on the
//! socket is not who the data belongs to.
//!
//! A change feed is a different shape of hazard from a request. A
//! request that is refused is over, and a subscription that is allowed
//! goes on being answered for as long as the socket is open, every time
//! anybody writes a row. So the questions here are about what the
//! server keeps deciding rather than what it decided once: whether the
//! wording of a subscription can get around a policy, whether a
//! subscription outlives the token that made it, and what a change
//! about a row the subscriber may not read still tells them.
//!
//! Gated on ZOU_PG_TEST_DSN and skipped when that database is not
//! logical, like the other suites that need a replication slot.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test attack_realtime

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use zou_server::sql::Pool;
use zou_server::{Config, jwt, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

/// A secret this server was never told about, which is what an attacker
/// who has read the docs and not the environment signs with.
const FORGED: &[u8] = b"another-secret-that-is-also-at-least-32-characters-long";

/// Whose rows these are, and who is trying to read them.
const OWNER: &str = "11111111-1111-1111-1111-111111111111";
const ATTACKER: &str = "22222222-2222-2222-2222-222222222222";

/// One suite at a time on this database, for the reason the socket
/// suite has it: a slot only carries what was written after it was
/// taken, so two tests subscribing and writing at once would be two
/// tests waiting on each other's rows.
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

/// A table in the publication, on a connection of its own because this
/// is several statements and the pool prepares what it is given.
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
/// that reasons about them has to start, since the count is server wide
/// and another suite in the same run may still be letting one go.
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
///
/// Every test here that proves a row was withheld leans on this, so it
/// waits long enough to be worth something: a change that was going to
/// arrive has been through the poll, the payload and the visibility
/// check by now, which is what the socket next to it is there to show.
async fn quiet(socket: &mut Socket) {
    let heard = tokio::time::timeout(Duration::from_millis(1500), socket.next()).await;
    assert!(
        heard.is_err(),
        "something arrived that should not have: {heard:?}"
    );
}

/// A join carrying subscriptions, answered with the ids they were given.
async fn subscribe(socket: &mut Socket, topic: &str, token: &str, wants: Value) -> Value {
    send(
        socket,
        &json!(["1", "1", topic, "phx_join", {
            "config": {"postgres_changes": wants},
            "access_token": token,
        }])
        .to_string(),
    )
    .await;
    next(socket).await
}

/// The same, asserting it was allowed, which is most of them.
async fn subscribed(socket: &mut Socket, topic: &str, sub: &str, wants: Value) -> Value {
    let reply = subscribe(socket, topic, &token(sub), wants).await;
    assert_eq!(reply[4]["status"], "ok", "{reply}");
    // The line that follows the reply on a join that asked for postgres
    // changes, taken off the socket here so that a test waiting for a
    // change does not read it as one.
    let system = next(socket).await;
    assert_eq!(system[3], "system", "{system}");
    reply[4]["response"].clone()
}

/// The payload of the next postgres change on this socket.
async fn changed(socket: &mut Socket) -> Value {
    let frame = next(socket).await;
    assert_eq!(frame[3], "postgres_changes", "{frame}");
    frame[4].clone()
}

/// Three ways of writing the same subscription, none of which is a way
/// around the policy on the table.
///
/// The interesting one is the schema wide subscription, because it is
/// the one that reads like a hole: no table named, so nothing about the
/// table to check. It is checked per changed row rather than per
/// subscription, which is the only place it could be checked and still
/// mean anything.
#[tokio::test]
async fn no_wording_of_a_subscription_gets_around_the_policy_on_the_table() {
    let Some(dsn) = dsn() else { return };
    let _one = alone().lock().await;
    if !logical(&dsn).await {
        return;
    }
    settled(&dsn).await;
    let at = serving(&dsn).await;
    published(
        &dsn,
        "atk_notes",
        "create table atk_notes (id int primary key, owner uuid, body text);
         alter table atk_notes enable row level security;
         grant select on atk_notes to anon, authenticated;
         create policy mine on atk_notes for select to authenticated
             using (owner = auth.uid());",
    )
    .await;

    let mut theirs = connect(at).await;
    subscribed(
        &mut theirs,
        "realtime:owner",
        OWNER,
        json!([{"event": "*", "schema": "public", "table": "atk_notes"}]),
    )
    .await;

    // The attacker asks three ways at once: the table itself, the whole
    // schema with no table named, and a filter that names the row they
    // are after by its key.
    let mut mine = connect(at).await;
    subscribed(
        &mut mine,
        "realtime:attacker",
        ATTACKER,
        json!([
            {"event": "*", "schema": "public", "table": "atk_notes"},
            {"event": "*", "schema": "public"},
            {"event": "*", "schema": "public", "table": "atk_notes", "filter": "id=eq.1"},
        ]),
    )
    .await;
    tapping(&dsn).await;

    run(
        &dsn,
        &format!("insert into atk_notes values (1, '{OWNER}', 'the plan')"),
    )
    .await;

    let heard = changed(&mut theirs).await;
    assert_eq!(
        heard["data"]["record"]["body"], "the plan",
        "the row reached the person it belongs to, so the change was flowing, {heard}"
    );
    quiet(&mut mine).await;
}

/// A table with no grant on it at all, which is the other half of the
/// question: row level security decides which rows, and the grant
/// decides whether there is anything to decide about.
///
/// What the attacker gets is not silence. Upstream sends the change
/// with `Error 401: Unauthorized` in `errors` and nothing else in it,
/// and this does the same, so what leaks is that something changed in a
/// table with that name. That is worth pinning rather than leaving
/// implied: it is the reference's behaviour, and a project that cannot
/// afford it should not put the table in the publication.
#[tokio::test]
async fn a_table_the_attacker_may_not_select_gives_them_the_refusal_and_no_values() {
    let Some(dsn) = dsn() else { return };
    let _one = alone().lock().await;
    if !logical(&dsn).await {
        return;
    }
    settled(&dsn).await;
    let at = serving(&dsn).await;
    published(
        &dsn,
        "atk_ledger",
        "create table atk_ledger (id int primary key, amount int);
         revoke all on atk_ledger from anon, authenticated;",
    )
    .await;

    let mut mine = connect(at).await;
    subscribed(
        &mut mine,
        "realtime:attacker",
        ATTACKER,
        json!([{"event": "*", "schema": "public", "table": "atk_ledger"}]),
    )
    .await;
    tapping(&dsn).await;

    run(&dsn, "insert into atk_ledger values (1, 900000)").await;

    let heard = changed(&mut mine).await;
    assert_eq!(
        heard["data"]["errors"],
        json!(["Error 401: Unauthorized"]),
        "{heard}"
    );
    assert_eq!(
        heard["data"]["record"],
        json!({}),
        "and not one value out of the row, {heard}"
    );
    assert_eq!(heard["data"]["columns"], json!([]), "{heard}");
}

/// A column the attacker may not select is not in the payload, on a row
/// they are otherwise allowed to see.
///
/// This is the case a per row check on its own would miss: the policy
/// says yes, so the row is theirs to hear about, and the column grant
/// still has to be asked separately or the change carries a value a
/// select would never have returned.
#[tokio::test]
async fn a_column_the_attacker_may_not_select_is_not_in_the_change() {
    let Some(dsn) = dsn() else { return };
    let _one = alone().lock().await;
    if !logical(&dsn).await {
        return;
    }
    settled(&dsn).await;
    let at = serving(&dsn).await;
    published(
        &dsn,
        "atk_people",
        "create table atk_people (id int primary key, name text, salary int);
         revoke all on atk_people from anon, authenticated;
         grant select (id, name) on atk_people to authenticated;",
    )
    .await;

    let mut mine = connect(at).await;
    subscribed(
        &mut mine,
        "realtime:attacker",
        ATTACKER,
        json!([{"event": "*", "schema": "public", "table": "atk_people"}]),
    )
    .await;
    tapping(&dsn).await;

    run(&dsn, "insert into atk_people values (1, 'ana', 120000)").await;

    let heard = changed(&mut mine).await;
    assert_eq!(heard["data"]["record"]["name"], "ana", "{heard}");
    assert!(
        heard["data"]["record"].get("salary").is_none(),
        "the column they may not select is not in the change either, {heard}"
    );
    let named: Vec<&str> = heard["data"]["columns"]
        .as_array()
        .expect("the column list")
        .iter()
        .map(|c| c["name"].as_str().expect("a name"))
        .collect();
    assert_eq!(
        named,
        vec!["id", "name"],
        "and the column list is what they may read rather than what the table has, {heard}"
    );
}

/// A filter is compared in Rust against a decoded row, so there is
/// nowhere in one to put sql.
///
/// The value goes nowhere near a statement, and neither does the column
/// name: the name is looked up in the relation the decoder built, and a
/// value that will not parse as the column's type is a no rather than
/// an error. Which means the attack does not fail loudly, it fails by
/// matching nothing, and the assertion is that the table is still there
/// and the socket heard nothing.
#[tokio::test]
async fn a_filter_is_not_a_place_to_put_sql() {
    let Some(dsn) = dsn() else { return };
    let _one = alone().lock().await;
    if !logical(&dsn).await {
        return;
    }
    settled(&dsn).await;
    let at = serving(&dsn).await;
    published(
        &dsn,
        "atk_open",
        "create table atk_open (id int primary key, body text);
         grant select on atk_open to anon, authenticated;",
    )
    .await;

    let mut mine = connect(at).await;
    subscribed(
        &mut mine,
        "realtime:attacker",
        ATTACKER,
        json!([
            {
                "event": "*", "schema": "public", "table": "atk_open",
                "filter": "id=eq.1); drop table atk_open; --",
            },
            {
                "event": "*", "schema": "public", "table": "atk_open",
                "filter": "id\"; drop table atk_open; --=eq.1",
            },
        ]),
    )
    .await;
    tapping(&dsn).await;

    run(&dsn, "insert into atk_open values (1, 'still here')").await;
    quiet(&mut mine).await;

    let rows: i64 = one(&dsn, "select count(*) from atk_open").await;
    assert_eq!(rows, 1, "the table is still there with its row in it");
}

/// A delete says a row went and not what was in it.
///
/// No policy can be asked about a row that is gone, so a delete is
/// published to everybody, which is upstream's behaviour and this
/// server's. What keeps it from being a way to read a table is that
/// what a delete carries is cut to the columns that identify the row.
/// A subscriber watching a table they may not read learns that a row
/// with that key was deleted and nothing else about it.
#[tokio::test]
async fn a_delete_tells_the_attacker_a_row_went_and_not_what_was_in_it() {
    let Some(dsn) = dsn() else { return };
    let _one = alone().lock().await;
    if !logical(&dsn).await {
        return;
    }
    settled(&dsn).await;
    let at = serving(&dsn).await;
    published(
        &dsn,
        "atk_secrets",
        "create table atk_secrets (id int primary key, owner uuid, body text);
         alter table atk_secrets enable row level security;
         grant select on atk_secrets to anon, authenticated;
         create policy mine on atk_secrets for select to authenticated
             using (owner = auth.uid());
         alter table atk_secrets replica identity full;",
    )
    .await;
    run(
        &dsn,
        &format!("insert into atk_secrets values (1, '{OWNER}', 'the combination')"),
    )
    .await;

    let mut mine = connect(at).await;
    subscribed(
        &mut mine,
        "realtime:attacker",
        ATTACKER,
        json!([{"event": "DELETE", "schema": "public", "table": "atk_secrets"}]),
    )
    .await;
    tapping(&dsn).await;

    // Replica identity full, which publishes every column of the old
    // row, so what the attacker is not told is a decision here rather
    // than an accident of what postgres sent.
    run(&dsn, "delete from atk_secrets where id = 1").await;

    let heard = changed(&mut mine).await;
    assert_eq!(heard["data"]["type"], "DELETE", "{heard}");
    assert_eq!(
        heard["data"]["old_record"],
        json!({"id": 1}),
        "the key, and not the body of a row they were never allowed to read, {heard}"
    );
}

/// A subscription does not outlive the token that made it.
///
/// The socket stays open across a token refresh, and the subscription
/// on it was made by somebody. So a refresh that does not verify has to
/// take the channel down rather than leave the old claims running,
/// because the alternative is a subscription that goes on being served
/// against a person who has stopped existing.
#[tokio::test]
async fn a_refresh_that_does_not_verify_takes_the_subscription_with_it() {
    let Some(dsn) = dsn() else { return };
    let _one = alone().lock().await;
    if !logical(&dsn).await {
        return;
    }
    settled(&dsn).await;
    let at = serving(&dsn).await;
    published(
        &dsn,
        "atk_feed",
        "create table atk_feed (id int primary key, body text);
         grant select on atk_feed to anon, authenticated;",
    )
    .await;

    let mut mine = connect(at).await;
    subscribed(
        &mut mine,
        "realtime:attacker",
        ATTACKER,
        json!([{"event": "*", "schema": "public", "table": "atk_feed"}]),
    )
    .await;
    tapping(&dsn).await;

    run(&dsn, "insert into atk_feed values (1, 'before')").await;
    let heard = changed(&mut mine).await;
    assert_eq!(heard["data"]["record"]["body"], "before", "{heard}");

    // A token for the same person, signed with a secret this server was
    // never told about.
    let forged = jwt::mint(
        &json!({"role": "authenticated", "sub": ATTACKER, "exp": 4102444800u64}),
        FORGED,
    );
    send(
        &mut mine,
        &json!(["1", "2", "realtime:attacker", "access_token", {"access_token": forged}])
            .to_string(),
    )
    .await;
    // The push itself is answered, since a client that sent a token is
    // owed a reply to it, and the channels come down after.
    let refused = next(&mut mine).await;
    assert_eq!(refused[3], "phx_reply", "{refused}");
    assert_eq!(refused[4]["status"], "error", "{refused}");
    let down = next(&mut mine).await;
    assert_eq!(down[3], "phx_error", "{down}");

    run(&dsn, "insert into atk_feed values (2, 'after')").await;
    quiet(&mut mine).await;

    // And the slot goes with it, because that channel was the only
    // subscriber on the server.
    for _ in 0..200 {
        if slots(&dsn).await == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the tap outlived the subscription that needed it");
}

/// A key signed with the wrong secret is not a key, whatever it claims
/// to be.
///
/// The token here says `service_role`, which is the one role every
/// policy in this suite would let through, and it is signed with a
/// secret the server does not have. The socket never opens.
#[tokio::test]
async fn a_forged_service_key_does_not_open_a_socket() {
    let Some(dsn) = dsn() else { return };
    let _one = alone().lock().await;
    let at = serving(&dsn).await;

    let forged = jwt::mint(
        &json!({"role": "service_role", "exp": 4102444800u64}),
        FORGED,
    );
    let url = format!("ws://{at}/realtime/v1/websocket?apikey={forged}&vsn=2.0.0");
    let answer = tokio_tungstenite::connect_async(url).await;
    assert!(
        answer.is_err(),
        "a socket opened on a key this server never issued"
    );
}
