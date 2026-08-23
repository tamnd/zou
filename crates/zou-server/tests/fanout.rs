//! Sockets on a node that does not hold the tenant.
//!
//! Two servers on two ports, one holding the project and one holding
//! only the sockets, linked by the websocket the second opens to the
//! first. What has to be true is that a client cannot tell which one it
//! reached: two sockets on one topic hear each other whichever node
//! they are on, presence is one room across both, and a question only
//! the database can answer is answered by the node that has it.
//!
//! No database, which is what makes this a test rather than a fixture:
//! broadcast and presence are the two things that need no postgres, so
//! they are asked here, and the rows a subscriber hears are asked in
//! tests/changes.rs against a real one.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use zou_server::fanout::Wire;
use zou_server::{Config, jwt, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

/// A server on a port of the kernel's choosing. `holder` is where the
/// project is, and None is a node that has it.
async fn serving(holder: Option<SocketAddr>) -> SocketAddr {
    serving_allowed(holder, zou_realtime::Limits::default()).await
}

/// The same, with the tier's numbers turned down to something a test
/// can reach.
async fn serving_allowed(holder: Option<SocketAddr>, realtime: zou_realtime::Limits) -> SocketAddr {
    let app = router(Config {
        jwt_secret: SECRET.to_vec(),
        holder: holder.map(|at| format!("http://{at}")),
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

/// The holder and one node with only the sockets on it.
async fn two() -> (SocketAddr, SocketAddr) {
    let holder = serving(None).await;
    (holder, serving(Some(holder)).await)
}

fn anon_key() -> String {
    jwt::mint(&jwt::key_claims("anon"), SECRET)
}

/// An address that takes a connection and then drops it, which is a
/// link that will never open and is the same answer on every machine.
///
/// Naming a port nothing listens on is the obvious way to ask for this
/// and it is not the same question everywhere. A connect to a closed
/// port is refused at once on linux and on macos, and on WSL2 it sits
/// there long enough that a five second wait says nothing about the
/// server under test. Failing the handshake instead of the connect
/// takes the host's opinion out of it. See #575 and #588.
async fn deaf() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
    let at = listener.local_addr().expect("the port");
    tokio::spawn(async move {
        while let Ok((connection, _)) = listener.accept().await {
            drop(connection);
        }
    });
    at
}

/// A tcp proxy that can be cut and mended, so that the link between two
/// servers can drop without either of them going.
///
/// Cutting drops every connection through it and refuses the next ones,
/// which is what a node with a working holder and a broken network
/// between them sees. Mending lets the redial through.
struct Cuttable {
    at: SocketAddr,
    open: Arc<AtomicBool>,
    cut: tokio::sync::watch::Sender<u64>,
}

impl Cuttable {
    async fn to(onward: SocketAddr) -> Cuttable {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
        let at = listener.local_addr().expect("the port");
        let open = Arc::new(AtomicBool::new(true));
        let (cut, _) = tokio::sync::watch::channel(0u64);
        let accepting = Arc::clone(&open);
        let cutting = cut.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut from, _)) = listener.accept().await else {
                    return;
                };
                if !accepting.load(Ordering::Relaxed) {
                    continue;
                }
                let mut watching = cutting.subscribe();
                tokio::spawn(async move {
                    let Ok(mut to) = tokio::net::TcpStream::connect(onward).await else {
                        return;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut from, &mut to) => {}
                        _ = watching.changed() => {}
                    }
                });
            }
        });
        Cuttable { at, open, cut }
    }

    fn cut(&self) {
        self.open.store(false, Ordering::Relaxed);
        self.cut.send_modify(|cuts| *cuts += 1);
    }

    fn mend(&self) {
        self.open.store(true, Ordering::Relaxed);
    }
}

/// A link opened by hand, as a node would, with the service key a node
/// presents.
async fn linked(at: SocketAddr) -> Socket {
    let key = jwt::mint(&jwt::key_claims("service_role"), SECRET);
    let (socket, _) =
        tokio_tungstenite::connect_async(format!("ws://{at}/realtime/v1/link?apikey={key}"))
            .await
            .expect("a link opens");
    socket
}

async fn crossed(socket: &mut Socket, frame: Wire) {
    socket
        .send(Message::Binary(frame.encode().into()))
        .await
        .expect("the link takes it");
}

async fn frame(socket: &mut Socket) -> Wire {
    match next(socket).await {
        Message::Binary(bytes) => Wire::decode(&bytes).expect("a frame"),
        other => panic!("{other:?} is not a frame"),
    }
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
async fn a_subscription_is_answered_by_the_node_with_the_database() {
    // A subscription is not something the node with the sockets can
    // decide: what a subscriber may see of a row is a select of that row
    // as them, so the list goes up the link and the holder answers it.
    //
    // Neither of these two has a database, which is what makes the
    // answer readable here: what comes back is the holder's own words
    // about its own missing database, not this node's about a link it
    // will not carry. That is the whole point of the test, since the
    // rows themselves need a database and are asked about in
    // tests/changes.rs against a real one.
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
        message.contains("no database to read changes out of"),
        "{reply}"
    );
}

#[tokio::test]
async fn a_socket_whose_node_cannot_reach_the_holder_is_told_rather_than_left_quiet() {
    // A holder that answers a connect and then says nothing, so the
    // link never opens. What a socket must not get out of that is a
    // channel that joins and stays silent: it is told there is a gap the
    // same way a subscriber the change reader dropped is, and a client
    // that is closed on reconnects and rejoins.
    let node = serving(Some(deaf().await)).await;
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
async fn a_link_that_drops_and_comes_back_delivers_what_it_missed() {
    // The whole of the bargain: a link is two servers losing a tcp
    // connection, and the sockets behind it must not find out. What was
    // sent while it was down arrives when it is back, in the order it
    // was sent, and nothing is closed.
    let holder = serving(None).await;
    let between = Cuttable::to(holder).await;
    let node = serving(Some(between.at)).await;
    let mut away = connect(node).await;
    let mut here = connect(holder).await;
    join(&mut away, "realtime:room", "{}").await;
    join(&mut here, "realtime:room", "{}").await;

    // The link is up and the topic is carried, which nothing below is
    // worth anything without. It has to be proved by a broadcast from
    // the away socket rather than to it: a join is answered on the spot,
    // so the holder learning about the topic is a frame still on its way
    // up, and the up frame is behind it on the same ordered link.
    broadcast(&mut away, "realtime:room", "cursor", r#"{"x":0}"#).await;
    assert!(heard(&mut here).await.ends_with(r#"{"x":0}"#));
    // And then the direction this test is about.
    broadcast(&mut here, "realtime:room", "cursor", r#"{"x":1}"#).await;
    assert!(heard(&mut away).await.ends_with(r#"{"x":1}"#));

    // Cut, and then say three things into the hole.
    between.cut();
    for x in 2..=4 {
        broadcast(
            &mut here,
            "realtime:room",
            "cursor",
            &format!(r#"{{"x":{x}}}"#),
        )
        .await;
    }
    // Long enough that the node has tried to redial and failed, so this
    // is a resume rather than a race with a link that never dropped.
    tokio::time::sleep(Duration::from_millis(600)).await;
    between.mend();

    // All three, in order, on the socket that never noticed.
    for x in 2..=4 {
        let text = heard(&mut away).await;
        assert!(text.ends_with(&format!(r#"{{"x":{x}}}"#)), "{text}");
    }
    // And it is still the same socket, on the same channel, still in
    // the room: a broadcast of its own goes up the new link and comes
    // back to the other node.
    broadcast(&mut away, "realtime:room", "cursor", r#"{"x":5}"#).await;
    let text = heard(&mut here).await;
    assert!(text.ends_with(r#"{"x":5}"#), "{text}");
}

#[tokio::test]
async fn a_link_that_comes_back_is_given_the_frames_after_the_one_it_got_to() {
    // The same thing from the link's own end, where the numbers are
    // readable. A link says which one it is and how far it got, and
    // what it is handed is everything after that, under the numbers it
    // would have had.
    let holder = serving(None).await;
    let mut link = linked(holder).await;
    crossed(
        &mut link,
        Wire::of(serde_json::json!({"up": "hello", "version": 2, "link": 7, "seen": 0})),
    )
    .await;
    let hello = frame(&mut link).await;
    assert_eq!(hello.head["down"], "hello");
    assert_eq!(hello.head["resumed"], false, "there was nothing to resume");
    // The handshake is outside the numbering, because it is the frame
    // that says where the numbering starts.
    assert!(hello.head.get("seq").is_none(), "{}", hello.head);

    // Two numbered frames, which is two questions answered.
    for id in 1..=2u64 {
        crossed(
            &mut link,
            Wire::of(serde_json::json!({"up": "state", "id": id, "topic": "realtime:room"})),
        )
        .await;
        let reply = frame(&mut link).await;
        assert_eq!(reply.head["for"], id);
        assert_eq!(reply.head["seq"], id);
    }

    // The link goes, and comes back saying it got as far as the first.
    drop(link);
    // Long enough for the holder to notice, since what it is holding is
    // put away when the socket ends rather than when it is dropped here.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut link = linked(holder).await;
    crossed(
        &mut link,
        Wire::of(serde_json::json!({"up": "hello", "version": 2, "link": 7, "seen": 1})),
    )
    .await;
    let hello = frame(&mut link).await;
    assert_eq!(hello.head["resumed"], true, "{}", hello.head);
    // And the second frame again, under the number it had, so the far
    // end's own check that each frame is the one after the last holds
    // straight through the reconnect.
    let again = frame(&mut link).await;
    assert_eq!(again.head["for"], 2);
    assert_eq!(again.head["seq"], 2);
    nothing(&mut link, "a frame it had already seen was sent again").await;

    // A link nobody was holding is told so rather than quietly handed a
    // stream that starts in the middle.
    let mut other = linked(holder).await;
    crossed(
        &mut other,
        Wire::of(serde_json::json!({"up": "hello", "version": 2, "link": 99, "seen": 40})),
    )
    .await;
    assert_eq!(frame(&mut other).await.head["resumed"], false);
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

/// A project's ceiling is the project's, so two nodes with one socket
/// each are two sockets towards it and not one each.
///
/// Both directions at once, which is the point: the node has to hear
/// what the holder has as much as the holder has to hear what the node
/// has, or a fleet is a licence to have the tier's number of sockets
/// once per server.
#[tokio::test]
async fn sockets_on_one_node_count_against_the_ceiling_on_the_other() {
    let limits = zou_realtime::Limits {
        concurrent_users: 2,
        ..zou_realtime::Limits::none()
    };
    let holder = serving_allowed(None, limits).await;
    let node = serving_allowed(Some(holder), limits).await;
    // One each, which is the project's two, and the link is dialled by
    // the first socket to need it rather than at boot.
    let _here = connect(holder).await;
    let mut away = connect(node).await;
    join(&mut away, "realtime:room", "{}").await;
    // Long enough for a tally to go up and the answer to come back.
    tokio::time::sleep(Duration::from_millis(2500)).await;

    for (at, whose) in [(holder, "the holder"), (node, "the node")] {
        let url = format!(
            "ws://{at}/realtime/v1/websocket?apikey={}&vsn=2.0.0",
            anon_key()
        );
        let refused = tokio_tungstenite::connect_async(url).await;
        assert!(
            refused.is_err(),
            "{whose} took a third socket for a project allowed two"
        );
    }
}

/// What the http broadcast endpoint reports is the project's rate, so
/// a caller polling a node reads the same number it would have read
/// off the holder rather than that node's share of it.
#[tokio::test]
async fn the_broadcast_headers_report_the_projects_rate_on_every_node() {
    let limits = zou_realtime::Limits {
        events_per_second: 1000,
        ..zou_realtime::Limits::none()
    };
    let holder = serving_allowed(None, limits).await;
    let node = serving_allowed(Some(holder), limits).await;
    // A socket on the node, so that the link is up and there is
    // something for the node to report.
    let mut away = connect(node).await;
    join(&mut away, "realtime:room", "{}").await;
    // Spend some of the budget on the holder and none of it on the
    // node, which is the case a per process number gets wrong.
    for _ in 0..200 {
        let (status, _, _) = post_rated(
            holder,
            "/realtime/v1/api/broadcast",
            br#"{"messages":[{"topic":"realtime:room","event":"x","payload":{}}]}"#.to_vec(),
        )
        .await;
        assert_eq!(status, 202, "the broadcast was taken");
    }
    tokio::time::sleep(Duration::from_millis(2500)).await;

    let (_, rate, _) = post_rated(
        node,
        "/realtime/v1/api/broadcast",
        br#"{"messages":[{"topic":"realtime:other","event":"x","payload":{}}]}"#.to_vec(),
    )
    .await;
    let rolling: u64 = rate[0].parse().expect("a rolling rate");
    assert!(
        rolling > 1,
        "the node reported {rolling} messages a second, which is its own share and not the project's"
    );
    assert_eq!(rate[1], "1000", "the limit is the project's either way");
}

/// The same post with the rate headers read off it, in the order
/// upstream's plug writes them: what the project is moving, what it is
/// allowed, and what is left of that.
async fn post_rated(at: SocketAddr, path: &str, body: Vec<u8>) -> (u16, Vec<String>, String) {
    let url = format!("http://{at}{path}");
    let key = jwt::mint(&jwt::key_claims("service_role"), SECRET);
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
