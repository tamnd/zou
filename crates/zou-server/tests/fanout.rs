//! Sockets on a node that does not hold the tenant.
//!
//! Two servers on two ports, one holding the project and one holding
//! only the sockets, linked by the websocket the second opens to the
//! first. What has to be true is that a client cannot tell which one it
//! reached: two sockets on one topic hear each other whichever node
//! they are on, presence is one room across both, and a socket that
//! asks for something its node cannot do is told so in words.
//!
//! No database, which is what makes this a test rather than a fixture:
//! broadcast and presence are the two things that need no postgres, and
//! they are the two things this first piece of the fan out tier carries.

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use zou_server::{Config, jwt, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

/// A server on a port of the kernel's choosing. `holder` is where the
/// project is, and None is a node that has it.
async fn serving(holder: Option<SocketAddr>) -> SocketAddr {
    let app = router(Config {
        jwt_secret: SECRET.to_vec(),
        holder: holder.map(|at| format!("http://{at}")),
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

/// The holder and one node with only the sockets on it.
async fn two() -> (SocketAddr, SocketAddr) {
    let holder = serving(None).await;
    (holder, serving(Some(holder)).await)
}

fn anon_key() -> String {
    jwt::mint(&jwt::key_claims("anon"), SECRET)
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

async fn join(socket: &mut Socket, topic: &str, config: &str) -> serde_json::Value {
    send(
        socket,
        &format!(r#"["1","1","{topic}","phx_join",{{"config":{config}}}]"#),
    )
    .await;
    next_json(socket).await
}

async fn broadcast(socket: &mut Socket, topic: &str, event: &str, payload: &str) {
    send(
        socket,
        &format!(
            r#"["1","2","{topic}","broadcast",{{"type":"broadcast","event":"{event}","payload":{payload}}}]"#
        ),
    )
    .await;
}

/// The binary broadcast a 2.0.0 socket is sent, as the text in it.
async fn heard(socket: &mut Socket) -> String {
    match next(socket).await {
        Message::Binary(bytes) => {
            assert_eq!(bytes[0], 4, "the kind byte says user broadcast");
            String::from_utf8_lossy(&bytes).to_string()
        }
        other => panic!("{other:?} is not the binary broadcast a 2.0.0 socket takes"),
    }
}

async fn nothing(socket: &mut Socket, why: &str) {
    let arrived = tokio::time::timeout(Duration::from_millis(250), socket.next()).await;
    assert!(arrived.is_err(), "{why}");
}

#[tokio::test]
async fn two_sockets_on_one_topic_hear_each_other_from_either_node() {
    let (holder, node) = two().await;
    let mut here = connect(holder).await;
    let mut away = connect(node).await;
    join(&mut here, "realtime:room", "{}").await;
    join(&mut away, "realtime:room", "{}").await;

    // Up the link, fanned by the holder, and down to the socket on it.
    // This is also what proves the link is up and the topic is carried,
    // which is why it goes first.
    broadcast(&mut away, "realtime:room", "cursor", r#"{"x":1}"#).await;
    let text = heard(&mut here).await;
    assert!(text.contains("realtime:room"), "{text}");
    assert!(text.contains("cursor"), "{text}");
    assert!(text.ends_with(r#"{"x":1}"#), "{text}");
    // And the socket that sent it hears nothing, though its own node
    // was handed a copy: who sent it crosses the link both ways, so a
    // sender is still recognised after a round trip.
    nothing(&mut away, "the sender heard its own broadcast back").await;

    // And the other way, which is the ordinary hub fan on the holder
    // reaching a node instead of a socket.
    broadcast(&mut here, "realtime:room", "cursor", r#"{"x":2}"#).await;
    let text = heard(&mut away).await;
    assert!(text.ends_with(r#"{"x":2}"#), "{text}");
    nothing(&mut here, "the sender heard its own broadcast back").await;
}

#[tokio::test]
async fn a_topic_is_still_a_room_rather_than_a_node() {
    let (holder, node) = two().await;
    let mut away = connect(node).await;
    let mut elsewhere = connect(node).await;
    let mut here = connect(holder).await;
    join(&mut away, "realtime:room", "{}").await;
    join(&mut elsewhere, "realtime:other", "{}").await;
    join(&mut here, "realtime:other", "{}").await;

    // A join is answered on the spot, before the holder has been told
    // about the topic, because a client waiting on another node for a
    // reply that needs nothing from it would be a slow join for the
    // sake of it. So the round trip is what proves the holder has both
    // topics: they were asked for in this order down one ordered link,
    // so the second arriving means the first already has.
    broadcast(&mut elsewhere, "realtime:other", "cursor", r#"{"x":3}"#).await;
    let text = heard(&mut here).await;
    assert!(text.ends_with(r#"{"x":3}"#), "{text}");

    // Two sockets on one node on two topics, and the one that is not on
    // the topic hears nothing: a node carries the topics its sockets
    // are on and fans each of them to the sockets on that one.
    broadcast(&mut here, "realtime:other", "cursor", r#"{"x":3}"#).await;
    let text = heard(&mut elsewhere).await;
    assert!(text.ends_with(r#"{"x":3}"#), "{text}");
    nothing(
        &mut away,
        "a broadcast crossed topics on the way down a link",
    )
    .await;
}

#[tokio::test]
async fn presence_is_one_room_across_both_nodes() {
    let (holder, node) = two().await;
    let mut away = connect(node).await;
    let presence = r#"{"presence":{"enabled":true,"key":"u1"}}"#;
    join(&mut away, "realtime:room", presence).await;
    // The state at the join, read off the holder rather than out of
    // this node's own map, which is empty and always will be.
    let state = next_json(&mut away).await;
    assert_eq!(state[3], "presence_state");
    assert_eq!(state[4], serde_json::json!({}));

    send(
        &mut away,
        r#"["1","5","realtime:room","presence",{"type":"presence","event":"track","payload":{"at":"now"}}]"#,
    )
    .await;
    // The track is acked on the spot, because whether the payload is
    // one a client may send is a question the socket answers by itself.
    let reply = next_json(&mut away).await;
    assert_eq!(reply[3], "phx_reply");
    assert_eq!(reply[4]["status"], "ok", "{reply}");
    // The track went up and the diff came back, which is the socket
    // applying its own presence through the same path everybody else's
    // copy is kept level by.
    let diff = next_json(&mut away).await;
    assert_eq!(diff[3], "presence_diff");
    assert_eq!(diff[4]["joins"]["u1"]["metas"][0]["at"], "now");

    // And a socket on the holder that joins afterwards is told about
    // somebody who is on another node entirely.
    let mut here = connect(holder).await;
    join(&mut here, "realtime:room", presence).await;
    let state = next_json(&mut here).await;
    assert_eq!(state[3], "presence_state");
    assert_eq!(state[4]["u1"]["metas"][0]["at"], "now");

    // A socket that goes takes its presence with it, wherever it was.
    drop(away);
    let diff = next_json(&mut here).await;
    assert_eq!(diff[3], "presence_diff");
    assert!(diff[4]["leaves"]["u1"].is_object(), "{diff}");
}

#[tokio::test]
async fn a_node_says_what_it_cannot_do_rather_than_saying_nothing() {
    // Database changes over a link are the next piece of this and not
    // this one. A client that asks for them joins the channel and is
    // told why there are none on the system frame, which is what
    // upstream does for a table nobody published: the alternative is a
    // subscription that looks fine and never says anything.
    let (_holder, node) = two().await;
    let mut away = connect(node).await;
    let reply = join(
        &mut away,
        "realtime:db",
        r#"{"postgres_changes":[{"event":"*","schema":"public","table":"notes"}]}"#,
    )
    .await;
    assert_eq!(reply[3], "phx_reply");
    assert_eq!(reply[4]["status"], "error", "{reply}");
    let message = reply[4]["response"]["reason"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(
        message.contains("does not serve database changes"),
        "{reply}"
    );
}

#[tokio::test]
async fn a_socket_whose_node_cannot_reach_the_holder_is_told_rather_than_left_quiet() {
    // Port 1 answers nothing on any machine this runs on, so the link
    // never opens. What a socket must not get out of that is a channel
    // that joins and stays silent: it is told there is a gap the same
    // way a subscriber the change reader dropped is, and a client that
    // is closed on reconnects and rejoins.
    let node = serving(Some("127.0.0.1:1".parse().expect("an address"))).await;
    let mut away = connect(node).await;
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match away.next().await {
                Some(Ok(_)) => continue,
                _ => return,
            }
        }
    })
    .await;
    assert!(
        closed.is_ok(),
        "the socket was left waiting on a link that will never open"
    );
}

#[tokio::test]
async fn a_link_is_the_projects_own_infrastructure_and_nobody_elses() {
    let (holder, _node) = two().await;
    // The anon key gets through the gate, because every key does, and
    // then gets no further: what a link may do is everything, so what
    // opens one is the service role and nothing less.
    let url = format!("ws://{holder}/realtime/v1/link?apikey={}", anon_key());
    let refused = tokio_tungstenite::connect_async(url).await;
    assert!(refused.is_err(), "the anon key opened a link");

    // And a node that holds nothing cannot be linked to either, since a
    // link to one would be a link to a link.
    let node = serving(Some(holder)).await;
    let key = jwt::mint(&jwt::key_claims("service_role"), SECRET);
    let url = format!("ws://{node}/realtime/v1/link?apikey={key}");
    let refused = tokio_tungstenite::connect_async(url).await;
    assert!(refused.is_err(), "a node with no tenant took a link");
}
