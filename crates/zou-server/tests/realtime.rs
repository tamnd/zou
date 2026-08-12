//! The realtime surface over a real socket.
//!
//! Everything about the protocol is decided in zou-realtime and tested
//! there without a port. What is left to prove here is that the wiring
//! is real: the route upgrades, the apikey gate is in front of it, two
//! sockets on one topic hear each other, and one on another topic does
//! not.
//!
//! No database. A broadcast channel touches nothing but memory, which
//! is half of why the demos Supabase leads with are broadcast and
//! presence rather than postgres changes.

use std::net::SocketAddr;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use zou_server::{Config, jwt, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

/// The server on a port of the kernel's choosing, left running for the
/// rest of the test.
async fn serving() -> SocketAddr {
    let app = router(Config {
        jwt_secret: SECRET.to_vec(),
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

fn anon_key() -> String {
    jwt::mint(&jwt::key_claims("anon"), SECRET)
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(at: SocketAddr, vsn: &str) -> Socket {
    let url = format!(
        "ws://{at}/realtime/v1/websocket?apikey={}&vsn={vsn}",
        anon_key()
    );
    open(&url).await
}

async fn open(url: &str) -> Socket {
    let (socket, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("the socket upgrades");
    socket
}

/// The next text frame, decoded. Anything binary is handed back as the
/// bytes it is.
async fn next(socket: &mut Socket) -> Message {
    tokio::time::timeout(std::time::Duration::from_secs(5), socket.next())
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

/// Join `topic` and wait for the reply, which is what
/// `channel.subscribe()` does.
async fn join(socket: &mut Socket, topic: &str) -> serde_json::Value {
    send(
        socket,
        &format!(r#"["1","1","{topic}","phx_join",{{"config":{{}}}}]"#),
    )
    .await;
    next_json(socket).await
}

#[tokio::test]
async fn a_socket_without_a_key_does_not_get_in() {
    let at = serving().await;
    let url = format!("ws://{at}/realtime/v1/websocket?vsn=2.0.0");
    let refused = tokio_tungstenite::connect_async(url).await;
    assert!(refused.is_err(), "the gate let a keyless socket through");
}

#[tokio::test]
async fn a_get_of_the_websocket_url_says_to_upgrade() {
    let at = serving().await;
    let url = format!("http://{at}/realtime/v1/websocket?apikey={}", anon_key());
    let answer = tokio::task::spawn_blocking(move || {
        ureq::get(&url)
            .call()
            .map(|r| r.status().as_u16())
            .unwrap_or_else(|e| match e {
                ureq::Error::StatusCode(code) => code,
                other => panic!("{other}"),
            })
    })
    .await
    .expect("the request runs");
    assert_eq!(answer, 426);
}

#[tokio::test]
async fn a_join_is_answered_and_a_heartbeat_keeps_it_open() {
    let at = serving().await;
    let mut socket = connect(at, "2.0.0").await;
    let reply = join(&mut socket, "realtime:room").await;
    assert_eq!(reply[3], "phx_reply");
    assert_eq!(reply[4]["status"], "ok");
    assert_eq!(reply[4]["response"], serde_json::json!({}));

    send(&mut socket, r#"[null,"2","phoenix","heartbeat",{}]"#).await;
    let beat = next_json(&mut socket).await;
    assert_eq!(beat[2], "phoenix");
    assert_eq!(beat[4]["status"], "ok");
}

#[tokio::test]
async fn what_one_socket_broadcasts_the_others_on_the_topic_hear() {
    let at = serving().await;
    let mut sender = connect(at, "2.0.0").await;
    let mut listener = connect(at, "2.0.0").await;
    let mut elsewhere = connect(at, "2.0.0").await;
    join(&mut sender, "realtime:room").await;
    join(&mut listener, "realtime:room").await;
    join(&mut elsewhere, "realtime:other").await;

    send(
        &mut sender,
        r#"["1","2","realtime:room","broadcast",{"type":"broadcast","event":"cursor","payload":{"x":1}}]"#,
    )
    .await;

    // The current client encoding, which is binary for a broadcast.
    let heard = next(&mut listener).await;
    let Message::Binary(bytes) = heard else {
        panic!("{heard:?} is not the binary broadcast a 2.0.0 socket takes")
    };
    assert_eq!(bytes[0], 4, "the kind byte says user broadcast");
    let text = String::from_utf8_lossy(&bytes);
    assert!(text.contains("realtime:room"), "{text}");
    assert!(text.contains("cursor"), "{text}");
    assert!(text.ends_with(r#"{"x":1}"#), "{text}");

    // And the sender hears nothing, because it did not ask to.
    let echoed = tokio::time::timeout(std::time::Duration::from_millis(250), sender.next()).await;
    assert!(echoed.is_err(), "the sender heard its own broadcast");

    // Nor does the socket on another topic.
    let crossed =
        tokio::time::timeout(std::time::Duration::from_millis(250), elsewhere.next()).await;
    assert!(crossed.is_err(), "a broadcast crossed topics");
}

#[tokio::test]
async fn a_socket_on_the_old_version_is_sent_the_shape_it_reads() {
    let at = serving().await;
    let mut sender = connect(at, "2.0.0").await;
    let mut old = connect(at, "1.0.0").await;
    join(&mut sender, "realtime:room").await;
    send(
        &mut old,
        r#"{"join_ref":"1","ref":"1","topic":"realtime:room","event":"phx_join","payload":{"config":{}}}"#,
    )
    .await;
    let reply = next_json(&mut old).await;
    assert_eq!(reply["event"], "phx_reply");
    assert_eq!(reply["payload"]["status"], "ok");

    send(
        &mut sender,
        r#"["1","2","realtime:room","broadcast",{"type":"broadcast","event":"cursor","payload":{"x":1}}]"#,
    )
    .await;
    let heard = next_json(&mut old).await;
    assert_eq!(heard["event"], "broadcast");
    assert_eq!(heard["payload"]["event"], "cursor");
    assert_eq!(heard["payload"]["payload"], serde_json::json!({"x": 1}));
}

/// Join with presence on under `key`, then read the reply and the
/// state that follows it, which is what `channel.subscribe()` does for
/// a channel with a presence binding on it.
async fn join_present(socket: &mut Socket, topic: &str, key: &str) -> serde_json::Value {
    send(
        socket,
        &format!(
            r#"["1","1","{topic}","phx_join",{{"config":{{"presence":{{"enabled":true,"key":"{key}"}}}}}}]"#
        ),
    )
    .await;
    let reply = next_json(socket).await;
    assert_eq!(reply[4]["status"], "ok", "{reply}");
    let state = next_json(socket).await;
    assert_eq!(state[3], "presence_state", "{state}");
    state[4].clone()
}

async fn track(socket: &mut Socket, topic: &str, payload: &str) {
    send(
        socket,
        &format!(
            r#"["1","5","{topic}","presence",{{"type":"presence","event":"track","payload":{payload}}}]"#
        ),
    )
    .await;
}

#[tokio::test]
async fn presence_says_who_is_here_now_and_who_arrives_after() {
    let at = serving().await;
    let mut first = connect(at, "2.0.0").await;
    assert_eq!(
        join_present(&mut first, "realtime:room", "u1").await,
        serde_json::json!({}),
        "the first socket on a topic is alone on it"
    );

    track(&mut first, "realtime:room", r#"{"typing":false}"#).await;
    // Its own track comes back twice: the reply it awaited, and then
    // the diff everyone on the topic gets, this socket included.
    assert_eq!(next_json(&mut first).await[4]["status"], "ok");
    let mine = next_json(&mut first).await;
    assert_eq!(mine[3], "presence_diff");
    assert_eq!(mine[4]["joins"]["u1"]["metas"][0]["typing"], false);
    assert!(
        mine[4]["joins"]["u1"]["metas"][0]["phx_ref"].is_string(),
        "a meta the client can tell from another"
    );

    // A socket that arrives later is told who is already here rather
    // than left to wait for the next thing to change.
    let mut second = connect(at, "2.0.0").await;
    let state = join_present(&mut second, "realtime:room", "u2").await;
    assert_eq!(state["u1"]["metas"][0]["typing"], false);

    track(&mut second, "realtime:room", r#"{"typing":true}"#).await;
    let seen = next_json(&mut first).await;
    assert_eq!(seen[3], "presence_diff");
    assert_eq!(seen[4]["joins"]["u2"]["metas"][0]["typing"], true);
    assert_eq!(seen[4]["leaves"], serde_json::json!({}));

    // And a socket that goes takes its presence with it, which is the
    // case a client cannot announce for itself.
    drop(second);
    let left = next_json(&mut first).await;
    assert_eq!(left[3], "presence_diff");
    assert_eq!(left[4]["leaves"]["u2"]["metas"][0]["typing"], true);
    assert_eq!(left[4]["joins"], serde_json::json!({}));
}

#[tokio::test]
async fn a_socket_that_asked_for_no_presence_is_still_seen_by_the_ones_that_did() {
    let at = serving().await;
    let mut watching = connect(at, "2.0.0").await;
    join_present(&mut watching, "realtime:room", "u1").await;

    let mut quiet = connect(at, "2.0.0").await;
    join(&mut quiet, "realtime:room").await;
    track(&mut quiet, "realtime:room", r#"{"at":"the back"}"#).await;
    // The reply, and nothing else: this socket has no presence
    // bindings, so upstream sends it no presence and neither does this.
    assert_eq!(next_json(&mut quiet).await[4]["status"], "ok");

    let seen = next_json(&mut watching).await;
    assert_eq!(seen[3], "presence_diff");
    let joins = seen[4]["joins"].as_object().expect("an object of keys");
    let (key, entry) = joins.iter().next().expect("somebody joined");
    assert_eq!(entry["metas"][0]["at"], "the back");
    assert!(
        key.parse::<u64>().is_ok(),
        "a socket that named no key is known by its own name, got {key}"
    );

    // A broadcast still reaches it, which is the point of the flag
    // being about presence and not about the channel.
    send(
        &mut watching,
        r#"["1","6","realtime:room","broadcast",{"type":"broadcast","event":"cursor","payload":{"x":1}}]"#,
    )
    .await;
    assert!(matches!(next(&mut quiet).await, Message::Binary(_)));
}

#[tokio::test]
async fn an_untrack_and_a_leave_both_take_a_socket_off_the_topic() {
    let at = serving().await;
    let mut watching = connect(at, "2.0.0").await;
    join_present(&mut watching, "realtime:room", "u1").await;
    let mut coming_and_going = connect(at, "2.0.0").await;
    join_present(&mut coming_and_going, "realtime:room", "u2").await;

    track(&mut coming_and_going, "realtime:room", r#"{"round":1}"#).await;
    assert_eq!(next_json(&mut watching).await[3], "presence_diff");

    send(
        &mut coming_and_going,
        r#"["1","7","realtime:room","presence",{"type":"presence","event":"untrack"}]"#,
    )
    .await;
    let gone = next_json(&mut watching).await;
    assert_eq!(gone[4]["leaves"]["u2"]["metas"][0]["round"], 1);

    // Tracking again and then leaving the channel outright, which is a
    // different path off the topic and has to say the same thing.
    track(&mut coming_and_going, "realtime:room", r#"{"round":2}"#).await;
    assert_eq!(
        next_json(&mut watching).await[4]["joins"]["u2"]["metas"][0]["round"],
        2
    );
    send(
        &mut coming_and_going,
        r#"["1","8","realtime:room","phx_leave",{}]"#,
    )
    .await;
    let left = next_json(&mut watching).await;
    assert_eq!(left[3], "presence_diff");
    assert_eq!(left[4]["leaves"]["u2"]["metas"][0]["round"], 2);
}

#[tokio::test]
async fn a_channel_that_asks_for_postgres_changes_is_told_they_are_not_built() {
    let at = serving().await;
    let mut socket = connect(at, "2.0.0").await;
    send(
        &mut socket,
        r#"["1","1","realtime:room","phx_join",{"config":{"postgres_changes":[{"event":"*","schema":"public","table":"todos"}]}}]"#,
    )
    .await;
    let reply = next_json(&mut socket).await;
    assert_eq!(reply[4]["status"], "error");
    let reason = reply[4]["response"]["reason"].as_str().unwrap();
    assert!(reason.contains("postgres changes"), "{reason}");
}

#[tokio::test]
async fn a_token_that_verifies_is_taken_and_one_that_does_not_takes_the_channel_down() {
    let at = serving().await;
    let mut socket = connect(at, "2.0.0").await;
    join(&mut socket, "realtime:room").await;

    let user = jwt::mint(
        &serde_json::json!({
            "role": "authenticated",
            "sub": "11111111-1111-1111-1111-111111111111",
            "exp": 4102444800u64,
        }),
        SECRET,
    );
    send(
        &mut socket,
        &format!(r#"["1","2","realtime:room","access_token",{{"access_token":"{user}"}}]"#),
    )
    .await;
    assert_eq!(next_json(&mut socket).await[4]["status"], "ok");

    send(
        &mut socket,
        r#"["1","3","realtime:room","access_token",{"access_token":"not.a.token"}]"#,
    )
    .await;
    assert_eq!(next_json(&mut socket).await[4]["status"], "error");
    assert_eq!(next_json(&mut socket).await[3], "phx_error");
}

#[tokio::test]
async fn a_left_channel_stops_arriving() {
    let at = serving().await;
    let mut sender = connect(at, "2.0.0").await;
    let mut listener = connect(at, "2.0.0").await;
    join(&mut sender, "realtime:room").await;
    join(&mut listener, "realtime:room").await;
    send(&mut listener, r#"["1","2","realtime:room","phx_leave",{}]"#).await;
    assert_eq!(next_json(&mut listener).await[4]["status"], "ok");

    send(
        &mut sender,
        r#"["1","3","realtime:room","broadcast",{"type":"broadcast","event":"cursor","payload":{}}]"#,
    )
    .await;
    let after = tokio::time::timeout(std::time::Duration::from_millis(250), listener.next()).await;
    assert!(after.is_err(), "a channel that was left still arrives");
}

/// The same socket on a node that serves many projects.
///
/// A fleet does not hand the client the router the tests above use. The
/// request arrives at the gateway, which finds the project by name and
/// passes the request to that project's own router, and two things are
/// worth proving about that and are not visible from a json answer.
///
/// The upgrade survives the hand off, since a 101 is not a response the
/// gateway can rewrite and hand back the way it does every other one.
/// And a topic belongs to a project: `realtime:room` on acme-prod and
/// `realtime:room` on beta-co are two rooms, because each project is a
/// router of its own with a hub of its own behind it, which is the same
/// reason each carries its own jwt secret.
#[tokio::test]
async fn two_projects_on_one_node_do_not_share_a_room() {
    use std::sync::Arc;
    use zou_server::attach::{Attached, Backend};
    use zou_server::gateway::gateway;
    use zou_server::tenant::{Registry, Routing};
    use zou_store::registry::{self, Tenant};
    use zou_store::{CasStore, open_store};

    fn secret(tenant_ref: &str) -> String {
        format!("{tenant_ref}-secret-of-at-least-32-characters-long")
    }

    /// Every project gets a router with its own secret and no pool,
    /// which is all a broadcast needs and all this can have without a
    /// postgres per project.
    struct Fake;

    impl Backend for Fake {
        fn up(&self, entry: &Tenant) -> Result<Config, String> {
            Ok(Config {
                jwt_secret: entry.jwt_secret.as_bytes().to_vec(),
                ..Config::default()
            })
        }
        fn down(&self, _tenant_ref: &str) {}
    }

    let dir = tempfile::tempdir().expect("a directory");
    let store: Arc<dyn CasStore> =
        Arc::from(open_store(&dir.path().to_string_lossy()).expect("a store opens"));
    for tenant_ref in ["acme-prod", "beta-co"] {
        registry::create(
            store.as_ref(),
            &Tenant::new(tenant_ref, &secret(tenant_ref), 1),
        )
        .expect("it registers");
    }
    let front = gateway(
        // A laptop has no wildcard dns, so the project is the first
        // path segment here. It is the same resolution either way.
        Routing {
            domains: Vec::new(),
            path_prefix: true,
        },
        Arc::new(Registry::new(store)),
        Arc::new(Attached::new(Arc::new(Fake))),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
    let at = listener.local_addr().expect("the port");
    tokio::spawn(async move {
        let _ = axum::serve(listener, front).await;
    });

    let url = |tenant_ref: &str| {
        format!(
            "ws://{at}/{tenant_ref}/realtime/v1/websocket?apikey={}&vsn=2.0.0",
            jwt::mint(&jwt::key_claims("anon"), secret(tenant_ref).as_bytes())
        )
    };
    let mut sender = open(&url("acme-prod")).await;
    let mut mate = open(&url("acme-prod")).await;
    let mut stranger = open(&url("beta-co")).await;
    for socket in [&mut sender, &mut mate, &mut stranger] {
        assert_eq!(join(socket, "realtime:room").await[4]["status"], "ok");
    }

    send(
        &mut sender,
        r#"["1","2","realtime:room","broadcast",{"type":"broadcast","event":"cursor","payload":{"x":1}}]"#,
    )
    .await;
    let heard = next(&mut mate).await;
    assert!(
        matches!(&heard, Message::Binary(bytes) if bytes[0] == 4),
        "{heard:?} is not the broadcast the other socket on the project sent"
    );
    let crossed =
        tokio::time::timeout(std::time::Duration::from_millis(250), stranger.next()).await;
    assert!(crossed.is_err(), "a broadcast crossed projects");
}
