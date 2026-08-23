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
    // Realtime's own numbers, which no test here is anywhere near.
    limited(zou_realtime::Limits::default()).await
}

/// The same server on a budget of the test's choosing, which is how
/// the limits are proved without opening two hundred sockets or
/// sending three megabytes.
async fn limited(realtime: zou_realtime::Limits) -> SocketAddr {
    let app = router(Config {
        jwt_secret: SECRET.to_vec(),
        realtime,
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
    // A join is answered with the postgres changes it asked for, and a
    // join that asked for none is answered with an empty list rather
    // than with nothing at all, which is what upstream sends.
    assert_eq!(
        reply[4]["response"],
        serde_json::json!({"postgres_changes": []})
    );

    // The heartbeat is the socket's own and belongs to no channel, so
    // its reply is the empty one.
    send(&mut socket, r#"[null,"2","phoenix","heartbeat",{}]"#).await;
    let beat = next_json(&mut socket).await;
    assert_eq!(beat[2], "phoenix");
    assert_eq!(beat[4]["status"], "ok");
    assert_eq!(beat[4]["response"], serde_json::json!({}));
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

/// A post to the realtime surface, with the key on it and the status
/// and body handed back rather than raised, since a refusal is an
/// answer these tests want to read.
async fn post(at: SocketAddr, path: &str, content_type: &str, body: Vec<u8>) -> (u16, String) {
    let url = format!("http://{at}{path}");
    let key = anon_key();
    let content_type = content_type.to_string();
    tokio::task::spawn_blocking(move || {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        let mut answer = agent
            .post(&url)
            .header("apikey", &key)
            .header("content-type", &content_type)
            .send(&body[..])
            .expect("the request goes");
        let status = answer.status().as_u16();
        let text = answer.body_mut().read_to_string().expect("a body");
        (status, text)
    })
    .await
    .expect("the request runs")
}

#[tokio::test]
async fn a_broadcast_posted_over_http_reaches_the_sockets_on_the_topic() {
    let at = serving().await;
    let mut listener = connect(at, "2.0.0").await;
    let mut elsewhere = connect(at, "2.0.0").await;
    join(&mut listener, "realtime:room").await;
    join(&mut elsewhere, "realtime:other").await;

    // The batch shape, which is what `channel.send()` falls back to
    // when the socket is not up. The topic in it is the channel's own
    // name, without the prefix the socket topic carries.
    let (status, body) = post(
        at,
        "/realtime/v1/api/broadcast",
        "application/json",
        br#"{"messages":[{"topic":"room","event":"cursor","payload":{"x":1},"private":false}]}"#
            .to_vec(),
    )
    .await;
    assert_eq!(status, 202, "{body}");
    assert_eq!(body, "", "an accepted broadcast says nothing else");

    let heard = next(&mut listener).await;
    let Message::Binary(bytes) = heard else {
        panic!("{heard:?} is not the binary broadcast a 2.0.0 socket takes")
    };
    let text = String::from_utf8_lossy(&bytes);
    assert!(text.contains("realtime:room"), "{text}");
    assert!(text.contains("cursor"), "{text}");
    assert!(text.ends_with(r#"{"x":1}"#), "{text}");

    let crossed =
        tokio::time::timeout(std::time::Duration::from_millis(250), elsewhere.next()).await;
    assert!(crossed.is_err(), "a posted broadcast crossed topics");
}

#[tokio::test]
async fn the_single_url_carries_the_names_in_the_path_and_bytes_as_bytes() {
    let at = serving().await;
    let mut listener = connect(at, "2.0.0").await;
    join(&mut listener, "realtime:room").await;

    // The shape `httpSend` posts, with the payload as the whole body.
    let (status, body) = post(
        at,
        "/realtime/v1/api/broadcast/room/events/cursor",
        "application/json",
        br#"{"x":2}"#.to_vec(),
    )
    .await;
    assert_eq!(status, 202, "{body}");
    let heard = next(&mut listener).await;
    let Message::Binary(bytes) = heard else {
        panic!("{heard:?} is not a broadcast")
    };
    // The encoding byte says json, so the client parses the payload
    // rather than handing it over as an ArrayBuffer.
    assert_eq!(bytes[4], 1, "the payload encoding says json");
    assert!(String::from_utf8_lossy(&bytes).ends_with(r#"{"x":2}"#));

    let (status, body) = post(
        at,
        "/realtime/v1/api/broadcast/room/events/frame",
        "application/octet-stream",
        vec![0, 1, 2, 250],
    )
    .await;
    assert_eq!(status, 202, "{body}");
    let heard = next(&mut listener).await;
    let Message::Binary(bytes) = heard else {
        panic!("{heard:?} is not a broadcast")
    };
    assert_eq!(bytes[4], 0, "the payload encoding says bytes");
    assert!(
        bytes.ends_with(&[0, 1, 2, 250]),
        "the bytes posted are not the bytes delivered"
    );

    let (status, body) = post(
        at,
        "/realtime/v1/api/broadcast/room/events/cursor",
        "text/plain",
        b"whatever".to_vec(),
    )
    .await;
    assert_eq!(status, 415, "{body}");
}

#[tokio::test]
async fn a_batch_with_a_bad_message_in_it_is_refused_whole() {
    let at = serving().await;
    let mut listener = connect(at, "2.0.0").await;
    join(&mut listener, "realtime:room").await;

    // The first message is fine and the second has no event on it, so
    // neither is sent: half a batch delivered and then complained
    // about is the answer nobody can do anything with.
    let (status, body) = post(
        at,
        "/realtime/v1/api/broadcast",
        "application/json",
        br#"{"messages":[{"topic":"room","event":"cursor","payload":{}},{"topic":"room","payload":{}}]}"#
            .to_vec(),
    )
    .await;
    assert_eq!(status, 422, "{body}");
    let said: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(said["errors"]["messages"][0], serde_json::json!({}));
    assert_eq!(said["errors"]["messages"][1]["event"][0], "can't be blank");

    let nothing =
        tokio::time::timeout(std::time::Duration::from_millis(250), listener.next()).await;
    assert!(nothing.is_err(), "a refused batch was delivered anyway");

    let (status, body) = post(
        at,
        "/realtime/v1/api/broadcast",
        "application/json",
        br#"{}"#.to_vec(),
    )
    .await;
    assert_eq!(status, 422, "{body}");
    let said: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(said["errors"]["messages"][0], "can't be blank");
}

#[tokio::test]
async fn a_private_join_with_no_database_is_refused_by_name() {
    let at = serving().await;
    let mut socket = connect(at, "2.0.0").await;
    send(
        &mut socket,
        r#"["1","1","realtime:room","phx_join",{"config":{"private":true}}]"#,
    )
    .await;
    let reply = next_json(&mut socket).await;
    assert_eq!(reply[4]["status"], "error");
    let reason = reply[4]["response"]["reason"].as_str().unwrap_or_default();
    assert!(reason.contains("no database"), "{reason}");
}

#[tokio::test]
async fn a_private_broadcast_with_no_database_behind_it_says_so() {
    let at = serving().await;
    // This server has no database, so there are no policies to check a
    // private broadcast against. Saying no would read as the caller's
    // own policies refusing it, which is a thing to go and look at,
    // and there is nothing there to look at.
    let mut listener = connect(at, "2.0.0").await;
    join(&mut listener, "realtime:room").await;

    let (status, body) = post(
        at,
        "/realtime/v1/api/broadcast/room/events/cursor?private=true",
        "application/json",
        br#"{}"#.to_vec(),
    )
    .await;
    assert_eq!(status, 422, "{body}");
    assert!(body.contains("no database"), "{body}");

    // The batch shape drops what it cannot check and still answers
    // 202, which is upstream's own answer: a batch is not told which
    // of its messages the policies refused.
    let (status, body) = post(
        at,
        "/realtime/v1/api/broadcast",
        "application/json",
        br#"{"messages":[{"topic":"room","event":"cursor","payload":{},"private":true}]}"#.to_vec(),
    )
    .await;
    assert_eq!(status, 202, "{body}");

    let nothing =
        tokio::time::timeout(std::time::Duration::from_millis(250), listener.next()).await;
    assert!(
        nothing.is_err(),
        "a private broadcast nobody could check was delivered anyway"
    );
}

#[tokio::test]
async fn a_posted_broadcast_needs_a_key_like_everything_else_does() {
    let at = serving().await;
    let url = format!("http://{at}/realtime/v1/api/broadcast");
    let status = tokio::task::spawn_blocking(move || {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        agent
            .post(&url)
            .header("content-type", "application/json")
            .send(r#"{"messages":[]}"#)
            .expect("the request goes")
            .status()
            .as_u16()
    })
    .await
    .expect("the request runs");
    assert_eq!(status, 401);
}

/// A server with no database cannot read changes out of one, and says
/// so in the join reply rather than subscribing the channel to a feed
/// that will never carry anything.
///
/// The same route against a real database is `tests/changes.rs`, which
/// writes a row and waits for it to arrive over a socket.
#[tokio::test]
async fn a_channel_asking_for_postgres_changes_on_a_server_with_no_database_is_told_so() {
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
    assert!(reason.contains("no database"), "{reason}");
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

/// The same post with the rate headers read off it, in the order
/// upstream's plug writes them: what the project is moving, what it is
/// allowed, and what is left of that.
async fn post_rated(at: SocketAddr, path: &str, body: Vec<u8>) -> (u16, Vec<String>, String) {
    let url = format!("http://{at}{path}");
    let key = anon_key();
    tokio::task::spawn_blocking(move || {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        let mut answer = agent
            .post(&url)
            .header("apikey", &key)
            .header("content-type", "application/json")
            .send(&body[..])
            .expect("the request goes");
        let status = answer.status().as_u16();
        let rate = ["x-rate-rolling", "x-rate-limit", "x-rate-limit-remaining"]
            .iter()
            .map(|name| {
                answer
                    .headers()
                    .get(*name)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_string()
            })
            .collect();
        let text = answer.body_mut().read_to_string().expect("a body");
        (status, rate, text)
    })
    .await
    .expect("the request runs")
}

/// One socket too many is refused at the handshake, before there is a
/// socket to say anything down, which is why this is an http answer
/// and not a channel error.
#[tokio::test]
async fn a_socket_over_the_ceiling_is_refused_before_it_opens() {
    let at = limited(zou_realtime::Limits {
        concurrent_users: 1,
        ..zou_realtime::Limits::none()
    })
    .await;
    let _first = connect(at, "2.0.0").await;
    let url = format!(
        "ws://{at}/realtime/v1/websocket?apikey={}&vsn=2.0.0",
        anon_key()
    );
    match tokio_tungstenite::connect_async(url).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(answer)) => {
            assert_eq!(answer.status(), 429);
            let body = answer.body().clone().unwrap_or_default();
            let body = String::from_utf8_lossy(&body).to_string();
            assert!(body.contains("Too many connected users"), "{body}");
        }
        other => panic!("{other:?} is not the refusal a full project makes"),
    }

    // The one that was refused did not take a place in the count, so
    // the project is full and not overfull: the socket that hung up
    // makes room for the next one.
    drop(_first);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let _again = connect(at, "2.0.0").await;
}

#[tokio::test]
async fn a_socket_holding_every_channel_it_may_is_refused_the_next_one() {
    let at = limited(zou_realtime::Limits {
        channels_per_client: 1,
        ..zou_realtime::Limits::none()
    })
    .await;
    let mut socket = connect(at, "2.0.0").await;
    assert_eq!(join(&mut socket, "realtime:room").await[4]["status"], "ok");
    let refused = join(&mut socket, "realtime:other").await;
    assert_eq!(refused[4]["status"], "error");
    assert_eq!(
        refused[4]["response"]["reason"],
        "ChannelRateLimitReached: Too many channels"
    );
}

/// A project joining faster than it may is told on the join it asked
/// for and then disconnected, which is upstream's answer and the one
/// refusal that takes the socket with it.
#[tokio::test]
async fn a_project_joining_too_fast_is_told_and_hung_up_on() {
    let at = limited(zou_realtime::Limits {
        joins_per_second: 1,
        ..zou_realtime::Limits::none()
    })
    .await;
    let mut socket = connect(at, "2.0.0").await;
    // Five joins in the first five second bucket is one a second, the
    // limit itself, and upstream trips at the limit rather than past
    // it.
    for room in ["one", "two", "three", "four", "five"] {
        let joined = join(&mut socket, &format!("realtime:{room}")).await;
        assert_eq!(joined[4]["status"], "ok", "{room}");
    }
    let refused = join(&mut socket, "realtime:six").await;
    assert_eq!(refused[4]["status"], "error");
    assert_eq!(
        refused[4]["response"]["reason"],
        "ClientJoinRateLimitReached: Too many joins per second"
    );
    let gone = tokio::time::timeout(std::time::Duration::from_secs(5), socket.next())
        .await
        .expect("the socket closes within five seconds");
    assert!(
        matches!(gone, None | Some(Ok(Message::Close(_)))),
        "{gone:?} is not the disconnect that follows the refusal"
    );
}

/// Every answer from the broadcast endpoints says what the project is
/// spending, and one that is already over it is refused rather than
/// sent.
#[tokio::test]
async fn a_project_over_its_events_budget_is_refused_over_http_and_told_by_how_much() {
    let at = limited(zou_realtime::Limits {
        events_per_second: 1,
        ..zou_realtime::Limits::none()
    })
    .await;
    let body = br#"{"messages":[{"topic":"room","event":"cursor","payload":{"x":1}}]}"#.to_vec();
    // Nobody is listening, so each of these costs the project the one
    // send and no deliveries. Five of them in a bucket is one a
    // second, which is the limit.
    for round in 0..5 {
        let (status, rate, body) = post_rated(at, "/realtime/v1/api/broadcast", body.clone()).await;
        assert_eq!(status, 202, "{body}");
        assert_eq!(rate[1], "1", "round {round}");
    }
    let (status, rate, body) = post_rated(at, "/realtime/v1/api/broadcast", body).await;
    assert_eq!(status, 429);
    assert_eq!(rate, vec!["1", "1", "0"], "{body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).expect("json"),
        serde_json::json!({"message": "Too many requests"})
    );

    // The single url is behind the same budget, since upstream runs
    // the one plug in front of both.
    let (status, _, body) = post_rated(
        at,
        "/realtime/v1/api/broadcast/room/events/cursor",
        b"{}".to_vec(),
    )
    .await;
    assert_eq!(status, 429, "{body}");
}

/// A project with nothing counted says nothing about it, since a
/// header reading zero of zero left would read as refused.
#[tokio::test]
async fn a_project_with_no_budget_reports_none() {
    let at = limited(zou_realtime::Limits::none()).await;
    let (status, rate, body) = post_rated(
        at,
        "/realtime/v1/api/broadcast",
        br#"{"messages":[{"topic":"room","event":"cursor","payload":{}}]}"#.to_vec(),
    )
    .await;
    assert_eq!(status, 202, "{body}");
    assert_eq!(rate, vec!["", "", ""]);
}

/// A socket keeps its project attached for as long as it is connected.
///
/// A request holds its tenant until the answer is written, which for an
/// upgrade is the moment the 101 goes out and the connection starts
/// being useful. From there the socket used to hold nothing, so a node
/// at its ceiling was free to stop the postmaster its subscriptions
/// read from. The ceiling here is one project and the second project is
/// asked for while the first one has a socket on it, so the sweep has
/// every reason to take the first and must not. See #574.
#[tokio::test]
async fn a_socket_keeps_its_project_attached_for_as_long_as_it_lives() {
    use std::sync::{Arc, Mutex};
    use zou_server::attach::{Attached, Backend};
    use zou_server::gateway::gateway;
    use zou_server::tenant::{Registry, Routing};
    use zou_store::registry::{self, Tenant};
    use zou_store::{CasStore, open_store};

    fn secret(tenant_ref: &str) -> String {
        format!("{tenant_ref}-secret-of-at-least-32-characters-long")
    }

    /// A backend that writes down who it was told to stop, which is the
    /// whole question here.
    #[derive(Default)]
    struct Fake {
        downs: Mutex<Vec<String>>,
    }

    impl Backend for Fake {
        fn up(&self, entry: &Tenant) -> Result<Config, String> {
            Ok(Config {
                jwt_secret: entry.jwt_secret.as_bytes().to_vec(),
                ..Config::default()
            })
        }
        fn down(&self, tenant_ref: &str) {
            self.downs.lock().unwrap().push(tenant_ref.to_string());
        }
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
    let backend = Arc::new(Fake::default());
    let attached = Arc::new(
        Attached::new(backend.clone()).with_budget(1, std::time::Duration::from_millis(1)),
    );
    let front = gateway(
        Routing {
            domains: Vec::new(),
            path_prefix: true,
        },
        Arc::new(Registry::new(store)),
        attached.clone(),
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
    let mut socket = open(&url("acme-prod")).await;
    assert_eq!(join(&mut socket, "realtime:room").await[4]["status"], "ok");

    // The other project, which puts the node one over a ceiling of one,
    // and then the idle sweep on top, which is the other budget and has
    // the same reason to pass over a tenant somebody is inside.
    let _other = open(&url("beta-co")).await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    attached.sweep().await;
    assert!(
        !backend
            .downs
            .lock()
            .unwrap()
            .contains(&"acme-prod".to_string()),
        "the project with a socket on it was stopped: {:?}",
        backend.downs.lock().unwrap()
    );
    // And it is still a working socket rather than one that survived on
    // paper, so it answers when spoken to.
    send(&mut socket, r#"["1","2","phoenix","heartbeat",{}]"#).await;
    let beat = next_json(&mut socket).await;
    assert_eq!(beat[4]["status"], "ok", "{beat}");

    // Gone once the socket is, since a hold that never ends is a leak
    // rather than a fix.
    drop(socket);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    attached.sweep().await;
    assert!(
        backend
            .downs
            .lock()
            .unwrap()
            .contains(&"acme-prod".to_string()),
        "the hold outlived the socket: {:?}",
        backend.downs.lock().unwrap()
    );
}
