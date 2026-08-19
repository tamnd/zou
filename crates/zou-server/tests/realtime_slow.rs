//! What a socket that stopped reading costs everybody else.
//!
//! A realtime tier is a room where one participant can be arbitrarily
//! slow, and slow is not a failure the client reports: a laptop that
//! went to sleep, a phone on a train, a tab the browser throttled, and
//! a debugger stopped on a breakpoint all look the same from here.
//! They are a live TCP connection whose window is closed. Nothing
//! times out, nothing errors, and the server's write to it simply does
//! not finish.
//!
//! The failure that costs a project its evening is the one where that
//! socket takes the room with it. A server that buffers what a stuck
//! socket has not read grows with how long the socket stays stuck. A
//! server that fans out on one task delivers to everybody at the speed
//! of its slowest reader. A server that drops what it cannot deliver
//! and says nothing leaves a client holding a state it believes is
//! current and is not.
//!
//! So three claims, and this file is here to hold them:
//!
//! - A stuck socket does not slow the sockets beside it, and does not
//!   slow the sender either.
//! - What is held for it is bounded by the hub's backlog and not by
//!   how much has been sent past it.
//! - When it reads again it is closed rather than carried on with, so
//!   what a client sees is a prefix and then a close. Never a gap.
//!
//! Stuck here is the real thing rather than a test that declines to
//! call `recv`. The client's socket is opened with a receive buffer of
//! its own choosing, small, so that the kernel closes the window after
//! a few frames and the server's send stops finishing. Everything past
//! that is the server behaving as it would against a real slow client.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpSocket};
use tokio_tungstenite::tungstenite::Message;
use zou_server::{Config, jwt, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

/// The room everybody in this file is in.
const TOPIC: &str = "realtime:chaos";

/// How big one broadcast's payload is. Large enough that a few of them
/// fill a small receive window, small enough that the whole run is a
/// few megabytes.
const PAYLOAD: usize = 4096;

/// How many go out while a socket is stuck. Comfortably past the hub's
/// backlog of 256, so a stuck socket has provably lost some.
const SENT: usize = 600;

async fn serving() -> SocketAddr {
    let app = router(Config {
        jwt_secret: SECRET.to_vec(),
        // The default budget is a hundred messages a second over a
        // minute's window, and a burst of six hundred deliveries is
        // exactly the shape it exists to refuse. Nothing here is about
        // the budget, so it is off.
        realtime: zou_realtime::Limits {
            events_per_second: 0,
            ..zou_realtime::Limits::default()
        },
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

type Socket = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;

/// A client socket, with a receive buffer it asked for.
///
/// `None` is whatever the machine hands out, which is the ordinary
/// client. A small number is the stuck one: the kernel advertises a
/// window that a handful of frames fills, and once it is full the
/// server's write to this socket stops finishing, which is what being
/// stuck is.
///
/// The buffer is set before the connect, because it is the SYN that
/// carries the window scale and a size set afterwards is a size the
/// other end was never told about.
async fn connect(at: SocketAddr, recv_buffer: Option<u32>) -> Socket {
    let socket = TcpSocket::new_v4().expect("a tcp socket");
    if let Some(bytes) = recv_buffer {
        socket
            .set_recv_buffer_size(bytes)
            .expect("a receive buffer of our own choosing");
    }
    let stream = socket.connect(at).await.expect("the server is listening");
    let url = format!(
        "ws://{at}/realtime/v1/websocket?apikey={}&vsn=2.0.0",
        anon_key()
    );
    let (socket, _) = tokio_tungstenite::client_async(&url, stream)
        .await
        .expect("the socket upgrades");
    socket
}

async fn send(socket: &mut Socket, text: String) {
    socket
        .send(Message::Text(text.into()))
        .await
        .expect("the socket takes it");
}

/// Join the room and wait for the reply, which is what
/// `channel.subscribe()` does.
async fn join(socket: &mut Socket) {
    send(
        socket,
        format!(r#"["1","1","{TOPIC}","phx_join",{{"config":{{}}}}]"#),
    )
    .await;
    match heard(socket).await {
        Heard::Other(text) => assert!(text.contains(r#""status":"ok""#), "{text}"),
        other => panic!("a join was not answered, {other:?}"),
    }
}

/// What came off the wire.
#[derive(Debug)]
enum Heard {
    /// A delivered broadcast, and the number it carries.
    Tick(usize),
    /// Anything else: a join reply, a heartbeat, a presence diff.
    Other(String),
    /// The server closed the socket, or nothing came at all.
    Ended,
}

async fn heard(socket: &mut Socket) -> Heard {
    let frame = tokio::time::timeout(Duration::from_secs(10), socket.next()).await;
    let text = match frame {
        Err(_) | Ok(None) | Ok(Some(Err(_))) | Ok(Some(Ok(Message::Close(_)))) => {
            return Heard::Ended;
        }
        Ok(Some(Ok(Message::Text(text)))) => text.to_string(),
        // A broadcast reaches a 2.0.0 socket as bytes: a kind byte, the
        // lengths of the three names, the names, and then the payload
        // as json. The number is read out of the tail either way, which
        // is why nothing here decodes the frame.
        Ok(Some(Ok(Message::Binary(bytes)))) => String::from_utf8_lossy(&bytes).into_owned(),
        // Ping and pong are the transport's, and tungstenite answers a
        // ping itself. Neither is a message on the topic.
        Ok(Some(Ok(_))) => String::new(),
    };
    match number_in(&text) {
        Some(n) => Heard::Tick(n),
        None => Heard::Other(text),
    }
}

/// One broadcast on the topic, numbered, and padded so that a few of
/// them fill a small window.
fn broadcast(n: usize) -> String {
    let filler = "x".repeat(PAYLOAD);
    format!(
        r#"["1","{n}","{TOPIC}","broadcast",{{"type":"broadcast","event":"tick","payload":{{"n":{n},"filler":"{filler}"}}}}]"#
    )
}

/// The number a tick carries, found by name rather than by decoding,
/// since one encoding is json and the other is bytes with json in the
/// tail and the number is written the same way in both.
fn number_in(text: &str) -> Option<usize> {
    let rest = &text[text.find(r#""n":"#)? + 4..];
    let end = rest.find(|c: char| !c.is_ascii_digit())?;
    rest[..end].parse().ok()
}

/// Read a socket on a task of its own, until it has `want` ticks in
/// order or something goes wrong.
///
/// On a task rather than after the sending, because a socket read only
/// once everything has been sent is a socket as far behind as the one
/// these tests are about, and would be closed for the same reason. A
/// client that is keeping up is one that is reading while the room is
/// talking.
fn reading(mut socket: Socket, want: usize) -> tokio::task::JoinHandle<Result<Duration, String>> {
    tokio::spawn(async move {
        let start = Instant::now();
        let mut expected = 0;
        while expected < want {
            match heard(&mut socket).await {
                Heard::Tick(n) if n == expected => expected += 1,
                Heard::Tick(n) => {
                    return Err(format!(
                        "out of order, {n} arrived where {expected} was owed"
                    ));
                }
                Heard::Other(_) => {}
                Heard::Ended => return Err(format!("stopped after {expected} of {want}")),
            }
        }
        Ok(start.elapsed())
    })
}

/// The claim the room depends on: one socket that is not reading does
/// not stop the socket beside it from being current.
///
/// The stuck socket is stuck for real. Its receive window fills within
/// the first few frames, the server's write to it stops finishing, and
/// it is left that way for the whole run. If deliveries went out on one
/// task, or if the sender waited for its slowest reader, the fast
/// socket would be as stuck as it is.
#[tokio::test]
async fn a_socket_that_stopped_reading_does_not_hold_up_the_one_beside_it() {
    let at = serving().await;

    let mut stuck = connect(at, Some(2048)).await;
    join(&mut stuck).await;
    let mut fast = connect(at, None).await;
    join(&mut fast).await;
    let mut sender = connect(at, None).await;
    join(&mut sender).await;

    // Nothing reads `stuck` from here to the end of the test.
    let reader = reading(fast, SENT);
    let start = Instant::now();
    for n in 0..SENT {
        send(&mut sender, broadcast(n)).await;
    }
    let sending = start.elapsed();
    let receiving = reader
        .await
        .expect("the reader finishes")
        .unwrap_or_else(|why| panic!("the fast socket {why}"));

    // Both halves are generous by a wide margin. What they are here to
    // catch is not a slow machine, it is a fast one that has started
    // waiting on the stuck socket, which does not finish at all.
    assert!(
        sending < Duration::from_secs(20),
        "the sender waited on the stuck socket, {sending:?} to send {SENT}"
    );
    assert!(
        receiving < Duration::from_secs(20),
        "the fast socket waited on the stuck one, {receiving:?} to read {SENT}"
    );

    // Held to here so the claim is about a socket that is stuck rather
    // than one that was dropped and closed.
    drop(stuck);
}

/// What the stuck socket itself is owed, which is the truth about what
/// it missed rather than a stream with a hole in it.
///
/// It falls further behind than the hub holds, so some of what it was
/// owed is gone. When it reads again, what it gets is everything up to
/// the point it fell off, and then a close. A client that reconnects
/// resubscribes and reads the table, which is the only honest answer
/// once frames have been dropped, and the close is how it is told to.
#[tokio::test]
async fn a_socket_that_fell_behind_reads_a_prefix_and_then_a_close() {
    let at = serving().await;

    let mut stuck = connect(at, Some(2048)).await;
    join(&mut stuck).await;
    let mut sender = connect(at, None).await;
    join(&mut sender).await;

    for n in 0..SENT {
        send(&mut sender, broadcast(n)).await;
    }

    // And now it reads. Whatever the kernel held for it comes out
    // first, contiguous from the start, and then the socket ends.
    let mut seen = 0;
    let mut ended = false;
    for _ in 0..SENT + 16 {
        match heard(&mut stuck).await {
            Heard::Ended => {
                ended = true;
                break;
            }
            Heard::Tick(n) => {
                assert_eq!(
                    n, seen,
                    "the stuck socket was handed a gap rather than closed"
                );
                seen += 1;
            }
            Heard::Other(_) => {}
        }
    }

    assert!(
        ended,
        "the stuck socket read all {SENT} and was never closed, so it never fell behind and this test is not testing anything"
    );
    assert!(
        seen < SENT,
        "the stuck socket got all {SENT}, so nothing was dropped and the close was something else"
    );
    // And a prefix rather than nothing, which is the other way this
    // could pass while proving less than it says: a socket that was
    // closed before it read anything would satisfy both lines above.
    assert!(
        seen > 0,
        "the stuck socket was closed without reading any of what was already on its way"
    );
    println!("the stuck socket read {seen} of {SENT} and was then closed");
}

/// A room does not stop admitting people because somebody in it is
/// asleep.
///
/// The join is the part worth asking about: a socket joining a topic
/// takes the hub's lock to find or make the topic's sender, and a
/// design that held that lock while delivering would make a stuck
/// socket a stuck room.
#[tokio::test]
async fn a_socket_can_join_a_room_that_has_a_stuck_socket_in_it() {
    let at = serving().await;

    let mut stuck = connect(at, Some(2048)).await;
    join(&mut stuck).await;
    let mut sender = connect(at, None).await;
    join(&mut sender).await;

    // Enough to fill the stuck socket's window and stop the server's
    // write to it finishing.
    for n in 0..64 {
        send(&mut sender, broadcast(n)).await;
    }

    let start = Instant::now();
    let mut late = connect(at, None).await;
    join(&mut late).await;
    let joining = start.elapsed();
    assert!(
        joining < Duration::from_secs(10),
        "joining took {joining:?} with a stuck socket in the room"
    );

    // And it is a member rather than a connection: the next broadcast
    // reaches it.
    send(&mut sender, broadcast(999)).await;
    loop {
        match heard(&mut late).await {
            Heard::Tick(999) => break,
            Heard::Tick(n) => {
                panic!("the socket that joined late was handed {n}, which is older than it is")
            }
            Heard::Other(_) => {}
            Heard::Ended => panic!("the socket that joined late never heard the room"),
        }
    }
}

/// Many at once, which is the shape a real bad minute has: a cell tower
/// or a sleeping fleet is not one slow socket, it is most of them.
///
/// The claim is the same one and it is the one that matters at scale:
/// what the server holds is the hub's backlog for the topic, one copy
/// of it, and not a queue per socket that has not read. Sixteen stuck
/// sockets cost what one does, and the socket that is reading is still
/// current.
#[tokio::test]
async fn a_room_where_most_of_it_is_stuck_still_serves_the_one_that_is_not() {
    let at = serving().await;

    let mut asleep = Vec::new();
    for _ in 0..16 {
        let mut socket = connect(at, Some(2048)).await;
        join(&mut socket).await;
        asleep.push(socket);
    }
    let mut fast = connect(at, None).await;
    join(&mut fast).await;
    let mut sender = connect(at, None).await;
    join(&mut sender).await;

    let reader = reading(fast, SENT);
    for n in 0..SENT {
        send(&mut sender, broadcast(n)).await;
    }
    let took = reader
        .await
        .expect("the reader finishes")
        .unwrap_or_else(|why| panic!("the reading socket {why} with sixteen asleep"));
    assert!(
        took < Duration::from_secs(30),
        "sixteen stuck sockets cost the reading one {took:?}"
    );

    // Held to here so that nothing above is racing a socket that was
    // dropped and closed rather than stuck.
    drop(asleep);
}
