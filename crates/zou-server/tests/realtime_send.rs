//! Sending to a room from sql, against a live postgres.
//!
//! `realtime.send()` and `realtime.broadcast_changes()` are how a
//! project broadcasts from inside the database: a trigger on its own
//! table calls one of them and whoever is on the channel hears it,
//! with no http client and no socket anywhere in the transaction. Both
//! of them do one thing, which is insert a row into realtime.messages,
//! and the interesting half is what happens after that: upstream reads
//! the row out of a replication slot, this server hears it through
//! pg_notify. Either way the client cannot tell, and that is what this
//! suite is asking.
//!
//! Everything here goes through the real functions on a real database,
//! because the shape of the row they write is the whole interface and
//! a test that wrote the row itself would be testing nothing.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test realtime_send

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use zou_server::sql::Pool;
use zou_server::{Config, jwt, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

/// The person these tests sign in as, a uuid because `auth.uid()`
/// casts the subject to one.
const U1: &str = "3d9b2c11-7e44-4c0f-9a51-2b6c8d4e1f30";

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

/// A policy letting the signed in person read the rooms this suite
/// uses, written the way a project would write it.
///
/// Only reads, because nothing here writes as a person: a send from
/// sql is a trigger or a job, and it runs as whoever owns the code
/// rather than as whoever is listening.
async fn policies(dsn: &str) {
    static ONCE: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    ONCE.get_or_init(|| async {
        let pool = Pool::new(dsn, 4).expect("dsn parses");
        let sess = pool.unscoped().await.expect("connect");
        for sql in [
            "drop policy if exists zou_send_read on realtime.messages",
            "create policy zou_send_read on realtime.messages for select to authenticated
                 using (realtime.topic() like 'sent%')",
        ] {
            sess.execute(sql, &[]).await.expect(sql);
        }
        sess.commit().await.expect("the policy lands");
    })
    .await;
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
    // The router's own pool installs the realtime schema and the send
    // functions on its first connection, and the policy below is
    // written on the table it makes, so one connection goes through
    // before it.
    let pool = Pool::new(dsn, 2).expect("dsn parses");
    pool.unscoped()
        .await
        .expect("connect")
        .commit()
        .await
        .expect("bootstrap runs");
    policies(dsn).await;
    at
}

/// Wait until the server is actually listening for database sends.
///
/// It starts that on the first socket, so a send fired the instant
/// after a join could land in the gap before the listen took. Waiting
/// for the connection to show up is the difference between a suite
/// that passes and a suite that usually passes.
async fn listening(dsn: &str) {
    let pool = Pool::new(dsn, 2).expect("dsn parses");
    for _ in 0..100 {
        let sess = pool.unscoped().await.expect("connect");
        let rows = sess
            .query(
                "select count(*) from pg_stat_activity where query = 'listen zou_realtime'",
                &[],
            )
            .await
            .expect("the view reads");
        let waiting: i64 = rows[0].get(0);
        sess.commit().await.expect("done");
        if waiting > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the server never started listening for database sends");
}

/// Run some sql as the owner, which is who a trigger runs as.
async fn run(dsn: &str, sql: &str) {
    let pool = Pool::new(dsn, 2).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(sql, &[]).await.expect(sql);
    sess.commit().await.expect("it commits");
}

fn anon_key() -> String {
    jwt::mint(&jwt::key_claims("anon"), SECRET)
}

fn user_token() -> String {
    jwt::mint(
        &serde_json::json!({"role": "authenticated", "sub": U1, "exp": 4102444800u64}),
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

async fn next(socket: &mut Socket) -> Message {
    tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("something arrives within five seconds")
        .expect("the socket is still open")
        .expect("a message rather than an error")
}

async fn send(socket: &mut Socket, text: &str) {
    socket
        .send(Message::Text(text.into()))
        .await
        .expect("the socket takes it");
}

/// Join privately as the signed in person, which is the only way to be
/// on a room a private send goes to.
async fn join_private(socket: &mut Socket, topic: &str) {
    let token = user_token();
    send(
        socket,
        &format!(
            r#"["1","1","{topic}","phx_join",{{"config":{{"private":true}},"access_token":"{token}"}}]"#
        ),
    )
    .await;
    let reply = match next(socket).await {
        Message::Text(text) => serde_json::from_str::<serde_json::Value>(&text).expect("json"),
        other => panic!("{other:?} is not the reply"),
    };
    assert_eq!(reply[4]["status"], "ok", "{reply}");
}

/// Join the ordinary way, which asks nobody anything.
async fn join_public(socket: &mut Socket, topic: &str) {
    send(
        socket,
        &format!(r#"["1","1","{topic}","phx_join",{{"config":{{}}}}]"#),
    )
    .await;
    let reply = match next(socket).await {
        Message::Text(text) => serde_json::from_str::<serde_json::Value>(&text).expect("json"),
        other => panic!("{other:?} is not the reply"),
    };
    assert_eq!(reply[4]["status"], "ok", "{reply}");
}

/// The next broadcast this socket hears, as the text of its frame.
async fn broadcast(socket: &mut Socket) -> String {
    match next(socket).await {
        Message::Binary(bytes) => String::from_utf8_lossy(&bytes).to_string(),
        other => panic!("{other:?} is not the binary broadcast a 2.0.0 socket takes"),
    }
}

/// Nothing arrives on this socket for a quarter of a second.
async fn quiet(socket: &mut Socket) {
    let heard = tokio::time::timeout(Duration::from_millis(250), socket.next()).await;
    assert!(heard.is_err(), "something arrived that should not have");
}

#[tokio::test]
async fn a_send_from_sql_reaches_whoever_is_on_the_room() {
    let Some(dsn) = dsn() else { return };
    let at = serving(&dsn).await;
    let mut socket = connect(at).await;
    join_private(&mut socket, "realtime:sent-one").await;
    listening(&dsn).await;

    run(
        &dsn,
        "select realtime.send('{\"hello\": \"room\"}'::jsonb, 'greeting', 'sent-one')",
    )
    .await;

    let frame = broadcast(&mut socket).await;
    assert!(frame.contains("greeting"), "{frame}");
    assert!(frame.contains("hello"), "{frame}");
    // The id the function generated rides inside the payload, which is
    // how a sender and a receiver end up talking about one message.
    assert!(frame.contains("\"id\""), "{frame}");
}

#[tokio::test]
async fn a_send_is_private_unless_it_says_otherwise() {
    let Some(dsn) = dsn() else { return };
    let at = serving(&dsn).await;
    let mut private = connect(at).await;
    let mut public = connect(at).await;
    join_private(&mut private, "realtime:sent-two").await;
    join_public(&mut public, "realtime:sent-two").await;
    listening(&dsn).await;

    // Three arguments, so private defaults to true, which is the
    // signature every trigger in the wild is written against.
    run(
        &dsn,
        "select realtime.send('{\"n\": 1}'::jsonb, 'secret', 'sent-two')",
    )
    .await;
    let frame = broadcast(&mut private).await;
    assert!(frame.contains("secret"), "{frame}");
    quiet(&mut public).await;

    // And the other way round: a send that said it was not private is
    // for the public room of that name and nothing else.
    run(
        &dsn,
        "select realtime.send('{\"n\": 2}'::jsonb, 'notice', 'sent-two', false)",
    )
    .await;
    let frame = broadcast(&mut public).await;
    assert!(frame.contains("notice"), "{frame}");
    quiet(&mut private).await;
}

#[tokio::test]
async fn a_payload_too_big_for_a_notification_arrives_anyway() {
    let Some(dsn) = dsn() else { return };
    let at = serving(&dsn).await;
    let mut socket = connect(at).await;
    join_private(&mut socket, "realtime:sent-three").await;
    listening(&dsn).await;

    // Well past the eight thousand bytes a notification is capped at,
    // so this one is announced by id and read back out of the table.
    run(
        &dsn,
        "select realtime.send(
             jsonb_build_object('blob', repeat('x', 20000)), 'large', 'sent-three')",
    )
    .await;

    let frame = broadcast(&mut socket).await;
    assert!(frame.contains("large"), "the event survived the read back");
    assert!(
        frame.matches('x').count() >= 20000,
        "the whole payload arrived, not a truncated one"
    );
}

#[tokio::test]
async fn bytes_sent_from_sql_arrive_as_bytes() {
    let Some(dsn) = dsn() else { return };
    let at = serving(&dsn).await;
    let mut socket = connect(at).await;
    join_private(&mut socket, "realtime:sent-four").await;
    listening(&dsn).await;

    run(
        &dsn,
        "select realtime.send_binary('\\x7a6f75'::bytea, 'raw', 'sent-four')",
    )
    .await;

    let frame = broadcast(&mut socket).await;
    assert!(frame.contains("raw"), "{frame}");
    // Three bytes that spell zou, carried through untouched rather
    // than wrapped in json on the way.
    assert!(frame.ends_with("zou"), "{frame}");
}

#[tokio::test]
async fn a_trigger_on_a_project_table_broadcasts_its_rows() {
    let Some(dsn) = dsn() else { return };
    let at = serving(&dsn).await;
    let mut socket = connect(at).await;
    join_private(&mut socket, "realtime:sent-five").await;
    listening(&dsn).await;

    // What the Supabase docs tell a project to write, copied down to
    // the argument order, because the argument order is the interface.
    for sql in [
        "drop table if exists public.zou_send_orders",
        "create table public.zou_send_orders (id int primary key, state text)",
        "create or replace function public.zou_send_orders_changed() returns trigger
             language plpgsql as $$
             begin
                 perform realtime.broadcast_changes(
                     'sent-five', tg_op, tg_op, tg_table_name, tg_table_schema, new, old);
                 return null;
             end;
             $$",
        "create trigger zou_send_orders_changed after insert or update on public.zou_send_orders
             for each row execute function public.zou_send_orders_changed()",
    ] {
        run(&dsn, sql).await;
    }

    run(
        &dsn,
        "insert into public.zou_send_orders values (1, 'placed')",
    )
    .await;
    let frame = broadcast(&mut socket).await;
    assert!(frame.contains("INSERT"), "{frame}");
    assert!(frame.contains("zou_send_orders"), "{frame}");
    assert!(frame.contains("placed"), "{frame}");

    run(
        &dsn,
        "update public.zou_send_orders set state = 'shipped' where id = 1",
    )
    .await;
    let frame = broadcast(&mut socket).await;
    assert!(frame.contains("UPDATE"), "{frame}");
    assert!(frame.contains("shipped"), "{frame}");
    // The row as it was is in there too, which is what an update is
    // for.
    assert!(frame.contains("placed"), "{frame}");

    run(&dsn, "drop table public.zou_send_orders").await;
}

#[tokio::test]
async fn a_policy_probe_is_not_a_message() {
    let Some(dsn) = dsn() else { return };
    let at = serving(&dsn).await;
    let mut socket = connect(at).await;
    join_private(&mut socket, "realtime:sent-six").await;
    listening(&dsn).await;

    // Deciding whether this socket may write to the room means
    // inserting a row and rolling it back. The trigger runs on that
    // insert, and a notification from a transaction that rolled back
    // is never delivered, so nothing is heard here. If that were ever
    // untrue, every private join in the server would broadcast itself.
    let mut other = connect(at).await;
    join_private(&mut other, "realtime:sent-six").await;
    quiet(&mut socket).await;
}
