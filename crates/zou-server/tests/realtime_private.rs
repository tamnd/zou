//! Private channels against a live postgres.
//!
//! A private channel has no rules of its own. What it may do is
//! whatever ordinary row level security policies on `realtime.messages`
//! say it may, written in sql by the project, with the room name in
//! `realtime.topic()` and the person in `auth.uid()`. So this suite
//! writes a policy the way a project would and then asks the server
//! questions it can only answer by going to the database and reading
//! it.
//!
//! The policy here is small on purpose: signed in people may read
//! `lobby` and `listen` and the room named after themselves, and may
//! write only to `lobby` and their own room. Everything below follows
//! from those two sentences, which is the point: nothing in the server
//! knows the word lobby.
//!
//! Sockets authenticate the way a browser does, with the project's anon
//! key on the connect url and the person's own token in the join
//! payload, because a websocket cannot carry a header.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test realtime_private

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use zou_server::sql::Pool;
use zou_server::{Config, jwt, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

/// The person these tests sign in as. A uuid because `auth.uid()`
/// casts the subject to one, and a policy that mentions it would fail
/// rather than refuse if the subject were not.
const U1: &str = "6f8a1d20-2f0a-4a2e-9a1d-0a8f1c2b3d4e";

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

/// The project's policies, written once for the whole binary.
///
/// A project writes these in the sql editor and forgets about them.
/// Dropping first makes a rerun mean the same thing as a first run.
async fn policies(dsn: &str) {
    static ONCE: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    ONCE.get_or_init(|| async {
        let pool = Pool::new(dsn, 4).expect("dsn parses");
        let sess = pool.unscoped().await.expect("connect");
        for sql in [
            "drop policy if exists zou_test_read on realtime.messages",
            "drop policy if exists zou_test_write on realtime.messages",
            "create policy zou_test_read on realtime.messages for select to authenticated
                 using (realtime.topic() in ('lobby', 'listen')
                        or realtime.topic() = (auth.uid())::text)",
            "create policy zou_test_write on realtime.messages for insert to authenticated
                 with check (realtime.topic() = 'lobby'
                             or realtime.topic() = (auth.uid())::text)",
        ] {
            sess.execute(sql, &[]).await.expect(sql);
        }
        sess.commit().await.expect("the policies land");
    })
    .await;
}

/// The server on a port of the kernel's choosing, with the policies
/// already in place behind it.
async fn serving(dsn: &str) -> SocketAddr {
    let app = router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        ..Config::default()
    })
    .expect("router builds");
    // The router's own pool installs the realtime schema on its first
    // connection, and the policies are written on the table it makes,
    // so one request goes through before they do.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
    let at = listener.local_addr().expect("the port");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
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

fn anon_key() -> String {
    jwt::mint(&jwt::key_claims("anon"), SECRET)
}

fn service_key() -> String {
    jwt::mint(&jwt::key_claims("service_role"), SECRET)
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

async fn next_json(socket: &mut Socket) -> serde_json::Value {
    match next(socket).await {
        Message::Text(text) => serde_json::from_str(&text).expect("json"),
        other => panic!("{other:?} is not text"),
    }
}

async fn send(socket: &mut Socket, text: &str) {
    socket
        .send(Message::Text(text.into()))
        .await
        .expect("the socket takes it");
}

/// Join a private channel as whoever `token` is, or as the anon key
/// the socket connected with when there is no token.
async fn join_private(socket: &mut Socket, topic: &str, token: Option<&str>) -> serde_json::Value {
    let payload = match token {
        Some(token) => format!(r#"{{"config":{{"private":true}},"access_token":"{token}"}}"#),
        None => r#"{"config":{"private":true}}"#.to_string(),
    };
    send(
        socket,
        &format!(r#"["1","1","{topic}","phx_join",{payload}]"#),
    )
    .await;
    next_json(socket).await
}

/// Join the ordinary way, which asks nobody anything.
async fn join_public(socket: &mut Socket, topic: &str) -> serde_json::Value {
    send(
        socket,
        &format!(r#"["1","1","{topic}","phx_join",{{"config":{{}}}}]"#),
    )
    .await;
    next_json(socket).await
}

fn reason(reply: &serde_json::Value) -> String {
    assert_eq!(reply[4]["status"], "error", "{reply}");
    reply[4]["response"]["reason"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

async fn push(socket: &mut Socket, topic: &str, event: &str) {
    send(
        socket,
        &format!(
            r#"["1","2","{topic}","broadcast",{{"type":"broadcast","event":"{event}","payload":{{"x":1}}}}]"#
        ),
    )
    .await;
}

/// Nothing arrives on this socket for a quarter of a second, which is
/// how a refusal that drops rather than delivers is read.
async fn quiet(socket: &mut Socket) {
    let heard = tokio::time::timeout(Duration::from_millis(250), socket.next()).await;
    assert!(heard.is_err(), "something arrived that should not have");
}

async fn heard(socket: &mut Socket, event: &str) {
    match next(socket).await {
        Message::Binary(bytes) => {
            let text = String::from_utf8_lossy(&bytes).to_string();
            assert!(text.contains(event), "{text}");
        }
        other => panic!("{other:?} is not the binary broadcast a 2.0.0 socket takes"),
    }
}

/// A post to the realtime surface as `key`, with an optional bearer on
/// top of it, since http is where a person's token does travel in a
/// header.
async fn post(
    at: SocketAddr,
    path: &str,
    key: String,
    bearer: Option<String>,
    body: String,
) -> (u16, String) {
    let url = format!("http://{at}{path}");
    tokio::task::spawn_blocking(move || {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        let mut request = agent
            .post(&url)
            .header("apikey", &key)
            .header("content-type", "application/json");
        if let Some(bearer) = bearer {
            request = request.header("authorization", &format!("Bearer {bearer}"));
        }
        let mut answer = request.send(body.as_bytes()).expect("the request goes");
        let status = answer.status().as_u16();
        let text = answer.body_mut().read_to_string().expect("a body");
        (status, text)
    })
    .await
    .expect("the request runs")
}

#[tokio::test]
async fn a_join_the_read_policy_allows_gets_in_and_one_it_does_not_is_told_what_it_may_not_read() {
    let Some(dsn) = dsn() else { return };
    let at = serving(&dsn).await;
    let token = user_token();

    let mut socket = connect(at).await;
    let reply = join_private(&mut socket, "realtime:lobby", Some(&token)).await;
    assert_eq!(reply[4]["status"], "ok", "{reply}");

    let mut refused = connect(at).await;
    let reply = join_private(&mut refused, "realtime:vault", Some(&token)).await;
    assert_eq!(
        reason(&reply),
        "You do not have permissions to read from this Channel topic: vault"
    );
}

#[tokio::test]
async fn a_room_named_after_the_person_is_read_by_that_person_and_nobody_else() {
    let Some(dsn) = dsn() else { return };
    let at = serving(&dsn).await;

    // The policy says `realtime.topic() = auth.uid()::text`, so this
    // only passes if the claims out of the join payload reached the
    // policy.
    let mut mine = connect(at).await;
    let reply = join_private(&mut mine, &format!("realtime:{U1}"), Some(&user_token())).await;
    assert_eq!(reply[4]["status"], "ok", "{reply}");

    let somebody_else = jwt::mint(
        &serde_json::json!({
            "role": "authenticated",
            "sub": "11111111-2222-3333-4444-555555555555",
            "exp": 4102444800u64,
        }),
        SECRET,
    );
    let mut theirs = connect(at).await;
    let reply = join_private(&mut theirs, &format!("realtime:{U1}"), Some(&somebody_else)).await;
    assert_eq!(
        reason(&reply),
        format!("You do not have permissions to read from this Channel topic: {U1}")
    );
}

#[tokio::test]
async fn a_socket_that_is_only_a_project_key_is_refused_a_room_written_for_signed_in_people() {
    let Some(dsn) = dsn() else { return };
    let at = serving(&dsn).await;
    let mut socket = connect(at).await;
    let reply = join_private(&mut socket, "realtime:lobby", None).await;
    assert_eq!(
        reason(&reply),
        "You do not have permissions to read from this Channel topic: lobby"
    );
}

#[tokio::test]
async fn a_push_the_write_policy_allows_is_delivered_and_one_it_refuses_is_answered() {
    let Some(dsn) = dsn() else { return };
    let at = serving(&dsn).await;
    let token = user_token();

    let mut sender = connect(at).await;
    let mut listener = connect(at).await;
    join_private(&mut sender, "realtime:lobby", Some(&token)).await;
    join_private(&mut listener, "realtime:lobby", Some(&token)).await;
    push(&mut sender, "realtime:lobby", "cursor").await;
    heard(&mut listener, "cursor").await;

    // The same two sockets on a room they may read and not write. The
    // push is answered rather than dropped, which is where this parts
    // company with upstream on purpose.
    join_private(&mut sender, "realtime:listen", Some(&token)).await;
    join_private(&mut listener, "realtime:listen", Some(&token)).await;
    push(&mut sender, "realtime:listen", "cursor").await;
    let refused = next_json(&mut sender).await;
    assert_eq!(
        reason(&refused),
        "You do not have permissions to write to this Channel topic: listen"
    );
    quiet(&mut listener).await;
}

#[tokio::test]
async fn a_post_to_a_room_the_policies_allow_is_delivered_and_one_they_refuse_is_unauthorized() {
    let Some(dsn) = dsn() else { return };
    let at = serving(&dsn).await;
    let token = user_token();
    let mut listener = connect(at).await;
    join_private(&mut listener, "realtime:lobby", Some(&token)).await;

    let (status, _) = post(
        at,
        "/realtime/v1/api/broadcast/lobby/events/cursor?private=true",
        anon_key(),
        Some(token.clone()),
        r#"{"x":1}"#.to_string(),
    )
    .await;
    assert_eq!(status, 202);
    heard(&mut listener, "cursor").await;

    let (status, body) = post(
        at,
        "/realtime/v1/api/broadcast/listen/events/cursor?private=true",
        anon_key(),
        Some(token),
        r#"{"x":1}"#.to_string(),
    )
    .await;
    assert_eq!(status, 403);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).expect("json"),
        serde_json::json!({"message": "Unauthorized"})
    );
}

#[tokio::test]
async fn a_batch_sends_the_rooms_the_policies_allow_and_drops_the_rest_without_saying_so() {
    let Some(dsn) = dsn() else { return };
    let at = serving(&dsn).await;
    let token = user_token();
    let mut allowed = connect(at).await;
    let mut refused = connect(at).await;
    join_private(&mut allowed, "realtime:lobby", Some(&token)).await;
    join_private(&mut refused, "realtime:listen", Some(&token)).await;

    let (status, _) = post(
        at,
        "/realtime/v1/api/broadcast",
        anon_key(),
        Some(token),
        serde_json::json!({"messages": [
            {"topic": "lobby", "event": "cursor", "payload": {"x": 1}, "private": true},
            {"topic": "listen", "event": "cursor", "payload": {"x": 1}, "private": true},
        ]})
        .to_string(),
    )
    .await;
    // Upstream answers a batch with a mix in it 202 and says nothing
    // about what it dropped, and a client that reads the status alone
    // would never know. That is the shape, so this is the shape.
    assert_eq!(status, 202);
    heard(&mut allowed, "cursor").await;
    quiet(&mut refused).await;
}

#[tokio::test]
async fn a_public_channel_of_the_same_name_is_a_different_room() {
    let Some(dsn) = dsn() else { return };
    let at = serving(&dsn).await;
    let token = user_token();

    // Nobody had to ask the policies anything to be here: a public
    // channel is joined by name and that is all. So if this socket
    // heard what the private lobby is saying, the policies would be
    // decoration.
    let mut public = connect(at).await;
    join_public(&mut public, "realtime:lobby").await;
    let mut private = connect(at).await;
    join_private(&mut private, "realtime:lobby", Some(&token)).await;

    let (status, _) = post(
        at,
        "/realtime/v1/api/broadcast/lobby/events/private-cursor?private=true",
        anon_key(),
        Some(token.clone()),
        r#"{"x":1}"#.to_string(),
    )
    .await;
    assert_eq!(status, 202);
    heard(&mut private, "private-cursor").await;
    quiet(&mut public).await;

    // And the other way round, which is the same rule read backwards:
    // a public send is not smuggled into a private room either.
    let (status, _) = post(
        at,
        "/realtime/v1/api/broadcast/lobby/events/public-cursor",
        anon_key(),
        Some(token),
        r#"{"x":1}"#.to_string(),
    )
    .await;
    assert_eq!(status, 202);
    heard(&mut public, "public-cursor").await;
    quiet(&mut private).await;
}

#[tokio::test]
async fn the_service_key_is_not_stopped_by_policies_it_bypasses() {
    let Some(dsn) = dsn() else { return };
    let at = serving(&dsn).await;
    let mut listener = connect(at).await;
    join_private(&mut listener, "realtime:lobby", Some(&user_token())).await;

    // No policy names service_role anywhere. It gets through because
    // the role has bypassrls, which is how a project's own backend
    // reaches every room without being written into every policy.
    let (status, _) = post(
        at,
        "/realtime/v1/api/broadcast/lobby/events/from-the-server?private=true",
        service_key(),
        None,
        r#"{"x":1}"#.to_string(),
    )
    .await;
    assert_eq!(status, 202);
    heard(&mut listener, "from-the-server").await;

    let url = format!(
        "ws://{at}/realtime/v1/websocket?apikey={}&vsn=2.0.0",
        service_key()
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("the socket upgrades");
    let reply = join_private(&mut socket, "realtime:vault", None).await;
    assert_eq!(reply[4]["status"], "ok", "{reply}");
}
