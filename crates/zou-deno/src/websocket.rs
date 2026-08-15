//! `WebSocket`, the half of the protocol this repository did not have.
//!
//! `zou-server` speaks websockets already, and speaks the server's half
//! of them: an axum upgrade, a socket handed to a task, frames written
//! to whoever connected. A function wants the other half, which is a
//! handshake to somebody else's server, masked frames on the way out
//! and a close that is agreed rather than announced.
//!
//! So this is `tokio-tungstenite`, which is the crate the server side
//! is already built on, with the client features turned on. Writing a
//! frame codec by hand here would be a second implementation of
//! something already in the build, and the interesting part of a
//! websocket client is not the codec.
//!
//! The connection is split in two. A function that sends while it is
//! waiting to receive is the normal case rather than the exotic one,
//! and a single lock over the whole socket would make the send wait for
//! a message that may never come.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use deno_core::{OpState, ToJsBuffer, op2};
use deno_error::JsErrorBox;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// How long the handshake may take. The same reasoning as `fetch`'s
/// timeout: without one, a host that accepts a connection and then says
/// nothing is an isolate that never comes back.
const HANDSHAKE: Duration = Duration::from_secs(30);

/// The largest message a function may be sent, which is the ceiling
/// `fetch` puts on a body for the same reason.
const MESSAGE_LIMIT: usize = crate::fetch::BODY_LIMIT as usize;

type Connected = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// One open connection, in two halves that are locked separately.
struct Socket {
    writer: Rc<Mutex<SplitSink<Connected, Message>>>,
    reader: Rc<Mutex<SplitStream<Connected>>>,
}

/// Every socket this isolate has open, by the id javascript holds.
#[derive(Default)]
pub struct Sockets {
    last: u32,
    open: HashMap<u32, Socket>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Opened {
    id: u32,
    protocol: String,
    extensions: String,
}

/// What arrived, in the one shape javascript reads: `kind` says which
/// of the fields under it means anything.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Arrived {
    kind: &'static str,
    text: String,
    bytes: ToJsBuffer,
    code: u16,
    reason: String,
}

impl Arrived {
    fn text(text: String) -> Arrived {
        Arrived {
            kind: "text",
            text,
            ..Arrived::nothing()
        }
    }

    fn bytes(bytes: Vec<u8>) -> Arrived {
        Arrived {
            kind: "binary",
            bytes: bytes.into(),
            ..Arrived::nothing()
        }
    }

    fn closed(code: u16, reason: String) -> Arrived {
        Arrived {
            kind: "close",
            code,
            reason,
            ..Arrived::nothing()
        }
    }

    fn nothing() -> Arrived {
        Arrived {
            kind: "close",
            text: String::new(),
            bytes: Vec::new().into(),
            code: 0,
            reason: String::new(),
        }
    }
}

/// The handshake, and an id for what it opened.
///
/// `lazy` for the same reason `fetch` is: there is nothing an eager
/// poll could finish, since the first thing this does is a name lookup.
#[op2(async(lazy))]
#[serde]
pub async fn op_zou_ws_connect(
    state: Rc<RefCell<OpState>>,
    #[string] url: String,
    #[serde] protocols: Vec<String>,
) -> Result<Opened, JsErrorBox> {
    let request = asked(&url, &protocols)?;
    let connecting = tokio_tungstenite::connect_async_with_config(request, Some(limits()), false);
    let answered = tokio::time::timeout(HANDSHAKE, connecting)
        .await
        .map_err(|_| {
            refused(
                &url,
                &format!("no answer within {} seconds", HANDSHAKE.as_secs()),
            )
        })?;
    let (socket, answer) = answered.map_err(|e| refused(&url, &why(&e)))?;
    let protocol = said(answer.headers(), "sec-websocket-protocol");
    let extensions = said(answer.headers(), "sec-websocket-extensions");
    let (writer, reader) = socket.split();
    let mut state = state.borrow_mut();
    let sockets = state.borrow_mut::<Sockets>();
    sockets.last += 1;
    let id = sockets.last;
    sockets.open.insert(
        id,
        Socket {
            writer: Rc::new(Mutex::new(writer)),
            reader: Rc::new(Mutex::new(reader)),
        },
    );
    Ok(Opened {
        id,
        protocol,
        extensions,
    })
}

/// The next thing the other end said, or the close that ends it.
#[op2(async(lazy), fast)]
#[serde]
pub async fn op_zou_ws_next(
    state: Rc<RefCell<OpState>>,
    #[smi] id: u32,
) -> Result<Arrived, JsErrorBox> {
    let Some(reader) = reader(&state, id) else {
        // A read after the socket was dropped is not an error worth
        // throwing: the close already happened and this is the loop
        // finding out.
        return Ok(Arrived::closed(1006, String::new()));
    };
    let mut reader = reader.lock().await;
    loop {
        return match reader.next().await {
            Some(Ok(Message::Text(text))) => Ok(Arrived::text(text.to_string())),
            Some(Ok(Message::Binary(bytes))) => Ok(Arrived::bytes(bytes.to_vec())),
            // A ping is answered by the library and is not something a
            // handler is told about, so this waits for the next thing
            // that is.
            Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
            Some(Ok(Message::Close(frame))) => Ok(ended(frame)),
            // The socket ended without a close frame, which is what
            // 1006 is for and is never a code a peer may send.
            None => Ok(Arrived::closed(1006, String::new())),
            Some(Err(e)) => Err(JsErrorBox::type_error(why(&e))),
        };
    }
}

#[op2(async(lazy), fast)]
pub async fn op_zou_ws_send_text(
    state: Rc<RefCell<OpState>>,
    #[smi] id: u32,
    #[string] text: String,
) -> Result<(), JsErrorBox> {
    sent(&state, id, Message::Text(text.into())).await
}

#[op2(async(lazy), fast)]
pub async fn op_zou_ws_send_bytes(
    state: Rc<RefCell<OpState>>,
    #[smi] id: u32,
    #[buffer(copy)] bytes: Vec<u8>,
) -> Result<(), JsErrorBox> {
    sent(&state, id, Message::Binary(bytes.into())).await
}

/// The close this end asked for, which is a frame and not a hang up.
#[op2(async(lazy), fast)]
pub async fn op_zou_ws_close(
    state: Rc<RefCell<OpState>>,
    #[smi] id: u32,
    #[smi] code: u32,
    #[string] reason: String,
) -> Result<(), JsErrorBox> {
    // A socket that is already gone is a close that already happened,
    // which is what calling `close()` twice means and is not an error.
    if writer(&state, id).is_none() {
        return Ok(());
    }
    sent(&state, id, Message::Close(closing(code, reason))).await
}

/// Let go of a socket, which is what javascript does once it has seen
/// the close event and has nothing left to read.
#[op2(fast)]
pub fn op_zou_ws_drop(state: &mut OpState, #[smi] id: u32) {
    state.borrow_mut::<Sockets>().open.remove(&id);
}

async fn sent(state: &Rc<RefCell<OpState>>, id: u32, message: Message) -> Result<(), JsErrorBox> {
    let writer = writer(state, id)
        .ok_or_else(|| JsErrorBox::type_error("the socket is already closed".to_string()))?;
    let mut writer = writer.lock().await;
    writer.send(message).await.map_err(|e| {
        // Sending on a socket the other end has closed is the ordinary
        // race rather than a failure worth a stack trace.
        JsErrorBox::type_error(why(&e))
    })
}

fn reader(state: &Rc<RefCell<OpState>>, id: u32) -> Option<Rc<Mutex<SplitStream<Connected>>>> {
    let mut state = state.borrow_mut();
    let sockets = state.borrow_mut::<Sockets>();
    sockets
        .open
        .get(&id)
        .map(|socket| Rc::clone(&socket.reader))
}

fn writer(
    state: &Rc<RefCell<OpState>>,
    id: u32,
) -> Option<Rc<Mutex<SplitSink<Connected, Message>>>> {
    let mut state = state.borrow_mut();
    let sockets = state.borrow_mut::<Sockets>();
    sockets
        .open
        .get(&id)
        .map(|socket| Rc::clone(&socket.writer))
}

/// The request the handshake is, which is a plain HTTP request with the
/// upgrade headers on it and, if the function named any, the
/// subprotocols it is willing to speak.
fn asked(
    url: &str,
    protocols: &[String],
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, JsErrorBox> {
    let asked = websocket_url(url)?;
    let mut request = asked
        .as_str()
        .into_client_request()
        .map_err(|e| refused(url, &why(&e)))?;
    if !protocols.is_empty() {
        let named = protocols.join(", ");
        let value = named
            .parse()
            .map_err(|_| refused(url, "the subprotocols are not a header value"))?;
        request
            .headers_mut()
            .insert("sec-websocket-protocol", value);
    }
    Ok(request)
}

/// The url a websocket is opened on, which is `ws:` or `wss:` and the
/// two schemes the spec says to rewrite into them.
///
/// Everything else is refused here rather than being turned into a
/// connection to something that is not a websocket server. `http:` and
/// `https:` are not a courtesy: the spec says to rewrite them, and a
/// project that keeps one url in an environment variable and adds a
/// path to it should not have to care.
fn websocket_url(url: &str) -> Result<String, JsErrorBox> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| refused(url, "that is not a url"))?;
    match scheme.to_ascii_lowercase().as_str() {
        "ws" | "wss" => Ok(url.to_string()),
        "http" => Ok(format!("ws://{rest}")),
        "https" => Ok(format!("wss://{rest}")),
        other => Err(refused(
            url,
            &format!("{other} is not a scheme a websocket is opened on"),
        )),
    }
}

fn limits() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(MESSAGE_LIMIT))
        .max_frame_size(Some(MESSAGE_LIMIT))
}

/// One header of the server's answer, or an empty string, which is what
/// `socket.protocol` and `socket.extensions` are when nothing was
/// agreed.
fn said(headers: &tokio_tungstenite::tungstenite::http::HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

/// What the other end's close frame means, which is 1005 when there was
/// no frame at all: not a code anybody sent, and the one the spec says
/// to report when nothing was said.
fn ended(frame: Option<CloseFrame>) -> Arrived {
    match frame {
        Some(frame) => Arrived::closed(u16::from(frame.code), frame.reason.to_string()),
        None => Arrived::closed(1005, String::new()),
    }
}

/// The frame this end sends, out of the code and reason javascript
/// asked for. 1005 is the absence of a code rather than a code, so it
/// is sent as no frame at all.
fn closing(code: u32, reason: String) -> Option<CloseFrame> {
    if code == 1005 || code == 0 {
        return None;
    }
    Some(CloseFrame {
        code: CloseCode::from(code as u16),
        reason: reason.into(),
    })
}

/// The shape Deno's own message has for a connection that could not be
/// made, so a project moving between the two reads the same sentence.
fn refused(url: &str, why: &str) -> JsErrorBox {
    JsErrorBox::type_error(format!("failed to connect to WebSocket ({url}): {why}"))
}

/// The words for what went wrong, short enough to end a sentence.
fn why(e: &Error) -> String {
    match e {
        Error::ConnectionClosed => "the connection is closed".to_string(),
        Error::AlreadyClosed => "the connection was already closed".to_string(),
        Error::Io(e) => e.to_string(),
        Error::Url(e) => e.to_string(),
        Error::Http(answer) => format!("the server answered {}", answer.status()),
        Error::Capacity(e) => e.to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_close_frame_is_the_code_and_the_reason_that_came_with_it() {
        let arrived = ended(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "that is all".into(),
        }));
        assert_eq!(arrived.kind, "close");
        assert_eq!(arrived.code, 1000);
        assert_eq!(arrived.reason, "that is all");
    }

    /// No frame is not code zero and not a clean 1000. It is the code
    /// that means nobody said one, which is a distinction a handler
    /// branching on `event.code` can see.
    #[test]
    fn a_close_with_no_frame_is_the_code_that_means_nothing_was_said() {
        let arrived = ended(None);
        assert_eq!(arrived.code, 1005);
        assert_eq!(arrived.reason, "");
    }

    #[test]
    fn closing_with_a_code_sends_it_and_closing_without_one_sends_no_frame() {
        let frame = closing(1000, "done".to_string()).expect("a frame");
        assert_eq!(u16::from(frame.code), 1000);
        assert_eq!(frame.reason.as_str(), "done");
        assert_eq!(
            u16::from(closing(4000, String::new()).expect("a frame").code),
            4000
        );
        // The two ways of saying nothing.
        assert!(closing(1005, String::new()).is_none());
        assert!(closing(0, String::new()).is_none());
    }

    #[test]
    fn the_subprotocols_a_function_named_go_out_as_one_header() {
        let request = asked(
            "ws://localhost:9000/socket",
            &["phoenix".to_string(), "graphql-ws".to_string()],
        )
        .expect("a request");
        assert_eq!(
            request
                .headers()
                .get("sec-websocket-protocol")
                .expect("the header")
                .to_str()
                .expect("text"),
            "phoenix, graphql-ws"
        );
    }

    /// The two schemes the spec says to rewrite are rewritten, and a
    /// scheme that is not a websocket's is refused before anything is
    /// opened, by a message that says which url it was.
    #[test]
    fn what_is_not_a_websocket_url_never_reaches_the_network() {
        assert_eq!(
            websocket_url("WS://localhost:9000/socket").expect("a url"),
            "WS://localhost:9000/socket"
        );
        assert_eq!(
            websocket_url("http://localhost:9000/socket").expect("a url"),
            "ws://localhost:9000/socket"
        );
        assert_eq!(
            websocket_url("https://example.com/socket?apikey=one").expect("a url"),
            "wss://example.com/socket?apikey=one"
        );
        let refused = websocket_url("file:///etc/passwd").expect_err("not a websocket url");
        assert!(
            refused.to_string().contains("file:///etc/passwd"),
            "{refused}"
        );
        assert!(websocket_url("not a url at all").is_err());
        assert!(asked("ws://localhost:9000/socket", &[]).is_ok());
    }
}
