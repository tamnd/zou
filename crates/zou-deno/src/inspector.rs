//! A debugger's way in, which is upstream's `inspector_port`.
//!
//! `[edge_runtime] inspector_port = 8083` in `config.toml` is a port a
//! debugger connects to, and what answers on it is the Chrome DevTools
//! Protocol: a little HTTP for finding out what is running, and a
//! websocket per session carrying the protocol itself. Chrome's
//! `chrome://inspect`, VS Code's attach configuration and every other
//! debugger that speaks to node or to Deno speak exactly this.
//!
//! # Two halves that do not share a thread
//!
//! An isolate is thread bound and a listening socket is not, so this is
//! a thread with an accept loop on it and a table of isolates beside it.
//! What an isolate puts in the table is one channel: `get_session_sender`
//! from `deno_core`, down which a whole session's pair of channels can be
//! handed over. Nothing else about the isolate is shared, and everything
//! the debugger says is dispatched on the isolate's own thread.
//!
//! A target lives as long as its isolate does. Under `per_worker` that
//! is between calls as well as during them, which is the policy a
//! project debugging its functions is in anyway, and it is why the
//! worker services its sessions while it waits for the next call.
//!
//! # What is deliberately not here
//!
//! There is no `--inspect-brk`. Upstream has one, as a flag on
//! `supabase functions serve` rather than as a setting in the config
//! file, and stopping a server's first request until somebody attaches
//! is a decision for the command line rather than for a file that a
//! deployment also reads.
//!
//! The port is bound on the loopback address only. A debugger session
//! is a shell on the process: it evaluates arbitrary javascript in an
//! isolate holding the project's secrets, so a port that answers the
//! network is not something a config file should be able to ask for by
//! accident.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use deno_core::futures::channel::mpsc;
use deno_core::{InspectorSessionChannels, InspectorSessionKind, InspectorSessionProxy};
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;

/// One isolate a debugger can attach to.
struct Target {
    /// The module it was built from, which is what a debugger shows and
    /// what it matches a file on disk against.
    url: String,
    /// The function's own name, for the line in the target list.
    name: String,
    /// The way in. Every session is a pair of channels handed down this
    /// one, and the isolate picks them up the next time it is polled.
    sessions: mpsc::UnboundedSender<InspectorSessionProxy>,
}

/// The port, and everything currently listening on the other end of it.
pub(crate) struct Inspector {
    at: SocketAddr,
    open: Shared,
    next: AtomicU64,
}

type Shared = Arc<Mutex<HashMap<String, Target>>>;

/// A target's place in the table, which it leaves when the isolate goes.
///
/// A debugger holding a session on an isolate that has been dropped is
/// what this prevents: the sender goes with the isolate, the entry goes
/// with this, and a socket that was attached to it is closed by the
/// channel it was pumping ending.
pub(crate) struct Attached {
    id: String,
    open: Shared,
}

impl Drop for Attached {
    fn drop(&mut self) {
        if let Ok(mut open) = self.open.lock() {
            open.remove(&self.id);
        }
    }
}

impl Inspector {
    /// Bind the port and start answering on it.
    ///
    /// Bound here rather than on the thread, so a port already in use is
    /// something the server says at boot instead of something an
    /// operator finds out by attaching and getting nothing.
    pub(crate) fn start(port: u16) -> Result<Arc<Inspector>, String> {
        let listening = std::net::TcpListener::bind(("127.0.0.1", port))
            .map_err(|e| format!("the inspector could not have port {port}: {e}"))?;
        listening
            .set_nonblocking(true)
            .map_err(|e| format!("the inspector's port could not be made async: {e}"))?;
        let at = listening
            .local_addr()
            .map_err(|e| format!("the inspector's port could not be read back: {e}"))?;
        let open: Shared = Arc::new(Mutex::new(HashMap::new()));
        let serving = Arc::clone(&open);
        std::thread::Builder::new()
            .name("zou-inspector".to_string())
            .spawn(move || serve(listening, at, &serving))
            .map_err(|e| format!("the inspector could not have a thread: {e}"))?;
        log::info!("functions can be debugged at ws://{at}, which is the loopback address only");
        Ok(Arc::new(Inspector {
            at,
            open,
            next: AtomicU64::new(0),
        }))
    }

    /// Where it is listening, which is the port asked for unless that
    /// was zero and the operating system chose.
    pub(crate) fn at(&self) -> SocketAddr {
        self.at
    }

    /// Put an isolate in the target list, until the guard is dropped.
    pub(crate) fn attach(
        &self,
        url: &deno_core::ModuleSpecifier,
        sessions: mpsc::UnboundedSender<InspectorSessionProxy>,
    ) -> Attached {
        let id = format!("zou-{}", self.next.fetch_add(1, Ordering::Relaxed));
        let name = url
            .path_segments()
            .and_then(|mut parts| {
                let last = parts.next_back()?;
                // `functions/hello/index.ts` is called hello by
                // everybody, and a list of eight `index.ts` targets is
                // a list nobody can pick from.
                match last.starts_with("index.") {
                    true => parts.next_back().map(str::to_string),
                    false => Some(last.to_string()),
                }
            })
            .unwrap_or_else(|| url.to_string());
        let target = Target {
            url: url.to_string(),
            name,
            sessions,
        };
        if let Ok(mut open) = self.open.lock() {
            open.insert(id.clone(), target);
        }
        Attached {
            id,
            open: Arc::clone(&self.open),
        }
    }
}

/// The accept loop, on a runtime of its own so that a debugger
/// connecting is never something an isolate's thread has to notice.
fn serve(listening: std::net::TcpListener, at: SocketAddr, open: &Shared) {
    let tokio = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(tokio) => tokio,
        Err(e) => {
            log::error!("the inspector could not have a runtime: {e}");
            return;
        }
    };
    tokio.block_on(async move {
        // Inside the runtime rather than beside it, because a socket is
        // registered with the reactor of whichever runtime is running
        // when it is made.
        let listening = match TcpListener::from_std(listening) {
            Ok(listening) => listening,
            Err(e) => {
                log::error!("the inspector could not listen: {e}");
                return;
            }
        };
        loop {
            let Ok((stream, _)) = listening.accept().await else {
                continue;
            };
            let open = Arc::clone(open);
            tokio::spawn(async move {
                if let Err(why) = asked(stream, at, &open).await {
                    log::debug!("inspector: {why}");
                }
            });
        }
    });
}

/// One connection, which is either a question about what is running or
/// a session with one of the things that is.
async fn asked(mut stream: TcpStream, at: SocketAddr, open: &Shared) -> Result<(), String> {
    let (head, until) = peek(&stream).await?;
    let path = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/")
        .to_string();
    // The websocket path is left in the socket rather than read out of
    // it, because the handshake that follows is tungstenite's to do and
    // it wants the request it is answering.
    if let Some(id) = path.strip_prefix("/ws/") {
        return session(stream, id, open).await;
    }
    // Read out what was peeked at, because a socket closed with bytes
    // still unread in it is closed with a reset, and a reset throws away
    // the answer that was just written into it.
    taken(&mut stream, until).await?;
    let said = match path.as_str() {
        "/json/version" => version(),
        "/json" | "/json/list" => list(at, open),
        _ => return answer(stream, "404 Not Found", "text/plain", "nothing is there").await,
    };
    answer(stream, "200 OK", "application/json", &said).await
}

/// The bytes the head took up, out of the socket and thrown away.
async fn taken(stream: &mut TcpStream, until: usize) -> Result<(), String> {
    use tokio::io::AsyncReadExt;
    let mut head = vec![0u8; until];
    stream
        .read_exact(&mut head)
        .await
        .map(|_| ())
        .map_err(|e| format!("reading a request: {e}"))
}

/// The request head, without taking it out of the socket.
///
/// Peeked because the two things this port answers want the bytes
/// differently: the json endpoints want the path and nothing else, and
/// a websocket handshake wants the whole request still in front of it.
async fn peek(stream: &TcpStream) -> Result<(String, usize), String> {
    let mut buffer = vec![0u8; 4096];
    let mut before = 0;
    // Half a second's worth of looking, which is a request that started
    // arriving and stopped. A whole request is one packet and gets here
    // on the first turn.
    for _ in 0..500 {
        stream
            .readable()
            .await
            .map_err(|e| format!("waiting for a request: {e}"))?;
        let seen = match stream.peek(&mut buffer).await {
            Ok(0) => return Err("the connection said nothing".to_string()),
            Ok(seen) => seen,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(format!("reading a request: {e}")),
        };
        // Bytes that are still in the socket keep it readable, so a
        // request that arrived in halves would otherwise be a spin
        // rather than a wait: what is waited for here is more of it.
        if seen == before {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            continue;
        }
        before = seen;
        let head = String::from_utf8_lossy(&buffer[..seen]).to_string();
        if let Some(end) = buffer[..seen]
            .windows(4)
            .position(|four| four == b"\r\n\r\n")
        {
            return Ok((head, end + 4));
        }
        if seen == buffer.len() {
            return Ok((head, seen));
        }
    }
    Err("a request that never ended".to_string())
}

/// What DevTools asks for first, to find out what it is talking to.
fn version() -> String {
    serde_json::json!({
        "Browser": "zou",
        "Protocol-Version": "1.3",
        "V8-Version": deno_core::v8::VERSION_STRING,
    })
    .to_string()
}

/// The isolates a debugger may attach to, in the shape node and Deno
/// both answer this in.
fn list(at: SocketAddr, open: &Shared) -> String {
    let Ok(open) = open.lock() else {
        return "[]".to_string();
    };
    let mut targets: Vec<serde_json::Value> = open
        .iter()
        .map(|(id, target)| {
            let socket = format!("ws://{at}/ws/{id}");
            serde_json::json!({
                "id": id,
                "type": "node",
                "title": target.name,
                "description": "a zou edge function",
                "url": target.url,
                "webSocketDebuggerUrl": socket,
                "devtoolsFrontendUrl": format!(
                    "devtools://devtools/bundled/js_app.html?ws={}&experiments=true&v8only=true",
                    socket.trim_start_matches("ws://")
                ),
            })
        })
        .collect();
    // The map's order is nobody's, and a list that reorders itself
    // between two refreshes is a list somebody clicks the wrong row in.
    targets.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    serde_json::Value::Array(targets).to_string()
}

async fn answer(mut stream: TcpStream, status: &str, kind: &str, body: &str) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;
    let head = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {kind}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .await
        .map_err(|e| format!("answering: {e}"))?;
    stream
        .write_all(body.as_bytes())
        .await
        .map_err(|e| format!("answering: {e}"))?;
    stream
        .flush()
        .await
        .map_err(|e| format!("answering: {e}"))?;
    // Said politely rather than by dropping the socket, so that what
    // was written arrives even though nothing else will follow it.
    stream
        .shutdown()
        .await
        .map_err(|e| format!("hanging up: {e}"))?;
    Ok(())
}

/// One debugger session: a websocket at one end and an isolate at the
/// other, with a pair of channels in between.
///
/// Nothing here understands a word of the protocol. What arrives goes
/// down the channel the isolate reads, what the isolate says goes out
/// as a text frame, and either end closing ends both.
async fn session(stream: TcpStream, id: &str, open: &Shared) -> Result<(), String> {
    let sessions = {
        let table = open.lock().map_err(|_| "the target list".to_string())?;
        let target = table
            .get(id)
            .ok_or_else(|| format!("{id} is not running any more"))?;
        target.sessions.clone()
    };
    let socket = tokio_tungstenite::accept_async(stream)
        .await
        .map_err(|e| format!("a debugger's handshake: {e}"))?;
    // Two channels and four ends: the isolate is given the sending end
    // of what it says and the receiving end of what it is told, and
    // this keeps the other two.
    let (said, mut saying) = mpsc::unbounded::<deno_core::InspectorMsg>();
    let (mut telling, told) = mpsc::unbounded::<String>();
    sessions
        .unbounded_send(InspectorSessionProxy {
            channels: InspectorSessionChannels::Regular { tx: said, rx: told },
            kind: InspectorSessionKind::NonBlocking {
                wait_for_disconnect: false,
            },
        })
        .map_err(|_| format!("{id} stopped before the session started"))?;
    log::info!("a debugger attached to {id}");
    let (mut out, mut incoming) = socket.split();
    // Two directions and one socket, so they are two tasks rather than
    // one loop: a debugger that sends nothing for a minute while the
    // isolate sends it events is the ordinary case, and a single loop
    // waiting on the socket would have nowhere to write them from.
    let asking = tokio::spawn(async move {
        while let Some(arrived) = incoming.next().await {
            match arrived {
                Ok(Message::Text(text)) => {
                    if telling.send(text.to_string()).await.is_err() {
                        break;
                    }
                }
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
    });
    let answering = tokio::spawn(async move {
        while let Some(said) = saying.next().await {
            if out.send(Message::text(said.content)).await.is_err() {
                break;
            }
        }
    });
    // Whichever end goes first takes the other with it, which is what
    // closes the socket: both halves of it are in these two tasks.
    match futures_util::future::select(asking, answering).await {
        futures_util::future::Either::Left((_, other))
        | futures_util::future::Either::Right((_, other)) => other.abort(),
    }
    log::info!("a debugger left {id}");
    Ok(())
}
