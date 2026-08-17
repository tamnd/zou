//! Sockets through the front door of a node that holds no lease.
//!
//! Two whole front doors this time rather than two tenant routers: one
//! store with one project in it, a lease that says which node writes it,
//! and a client that connects to either node and cannot tell which one
//! it reached. This is the piece that was a 503 with the sentence "this
//! project's sockets are served by the node holding it" in it, so what
//! has to be true is that the sockets work and that nothing was started
//! here to make them work.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use zou_server::attach::{Attached, Backend};
use zou_server::forward::{self, Forwarding, Holders, Peers};
use zou_server::gateway::{fleet, gateway};
use zou_server::tenant::{Registry, Routing};
use zou_server::{Config, jwt};
use zou_store::lease::Holder;
use zou_store::registry::{self, Tenant};
use zou_store::{CasStore, open_store};

const SECRET: &str = "super-secret-jwt-token-with-at-least-32-characters-long";
const REF: &str = "acme-prod";

/// A project brought up with no database under it, which is everything
/// broadcast and presence need, and a note of every time it happened so
/// a test can say that it did not.
#[derive(Default)]
struct Ups(Mutex<Vec<String>>);

impl Ups {
    fn attached(&self) -> Vec<String> {
        self.0.lock().expect("the list").clone()
    }
}

impl Backend for Ups {
    fn up(&self, entry: &Tenant) -> Result<Config, String> {
        self.0
            .lock()
            .expect("the list")
            .push(entry.tenant_ref.clone());
        Ok(Config {
            jwt_secret: entry.jwt_secret.as_bytes().to_vec(),
            ..Config::default()
        })
    }

    fn down(&self, _tenant_ref: &str) {}
}

/// Who writes the project, as the lease would say it. None is a project
/// nobody has taken, which any node may then take itself.
struct Lease(Option<String>);

impl Peers for Lease {
    fn holder(&self, _tenant_ref: &str) -> Result<Option<Holder>, String> {
        Ok(self.0.as_ref().map(|endpoint| Holder {
            node: "node-holder".to_string(),
            endpoint: Some(endpoint.clone()),
            expires_unix: u64::MAX,
            epoch: 1,
            published_unix: None,
        }))
    }
}

/// Paths rather than hostnames, because a test client dials an address
/// and a Host header a load balancer would set is not the part being
/// tested here. Resolving a link's url on a host routed node is a unit
/// test in the gateway.
fn routing() -> Routing {
    Routing {
        domains: Vec::new(),
        path_prefix: true,
    }
}

struct Fleet {
    _dir: tempfile::TempDir,
    holder: SocketAddr,
    away: SocketAddr,
    /// What the node with the sockets on it brought up, which is the
    /// number this is all for.
    ups: Arc<Ups>,
}

async fn serve(router: axum::Router) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
    let at = listener.local_addr().expect("the port");
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    at
}

/// Two nodes over one store. `held_elsewhere` is whether the lease says
/// the first of them writes the project, which is the difference between
/// a node that may serve sockets itself and one that may not.
async fn two(held_elsewhere: bool) -> Fleet {
    let dir = tempfile::tempdir().expect("a directory to write into");
    let store: Arc<dyn CasStore> =
        Arc::from(open_store(&dir.path().to_string_lossy()).expect("a store opens"));
    registry::create(store.as_ref(), &Tenant::new(REF, SECRET, 1)).expect("it registers");

    // The node that writes the project. No forwarding on it at all,
    // which is one node's entire view of the world and is what makes
    // every request it is asked its own.
    let held = Arc::new(Ups::default());
    let holder = serve(gateway(
        routing(),
        Arc::new(Registry::new(store.clone())),
        Arc::new(Attached::new(held)),
    ))
    .await;

    // And the node with only the sockets on it: the lease names an
    // address, and the name on it is not this node's.
    let ups = Arc::new(Ups::default());
    let forwarding = Forwarding::new(
        "node-away",
        Holders::new(Arc::new(Lease(
            held_elsewhere.then(|| format!("http://{holder}")),
        ))),
        Arc::new(forward::Http::default()),
    );
    let away = serve(fleet(
        routing(),
        Arc::new(Registry::new(store)),
        Arc::new(Attached::new(ups.clone())),
        Some(Arc::new(forwarding)),
    ))
    .await;
    Fleet {
        _dir: dir,
        holder,
        away,
        ups,
    }
}

fn key(role: &str) -> String {
    jwt::mint(&jwt::key_claims(role), SECRET.as_bytes())
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(at: SocketAddr) -> Socket {
    let url = format!(
        "ws://{at}/{REF}/realtime/v1/websocket?apikey={}&vsn=2.0.0",
        key("anon")
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

async fn join(socket: &mut Socket, topic: &str) -> serde_json::Value {
    send(
        socket,
        &format!(r#"["1","1","{topic}","phx_join",{{"config":{{}}}}]"#),
    )
    .await;
    next_json(socket).await
}

async fn broadcast(socket: &mut Socket, topic: &str, payload: &str) {
    send(
        socket,
        &format!(
            r#"["1","2","{topic}","broadcast",{{"type":"broadcast","event":"cursor","payload":{payload}}}]"#
        ),
    )
    .await;
}

/// The binary broadcast a 2.0.0 socket is sent, as the text in it.
async fn heard(socket: &mut Socket) -> String {
    match next(socket).await {
        Message::Binary(bytes) => String::from_utf8_lossy(&bytes).to_string(),
        other => panic!("{other:?} is not the binary broadcast a 2.0.0 socket takes"),
    }
}

#[tokio::test]
async fn a_socket_at_a_node_that_writes_nothing_is_still_in_the_projects_room() {
    let at = two(true).await;
    let mut here = connect(at.holder).await;
    let mut away = connect(at.away).await;
    join(&mut here, "realtime:room").await;
    join(&mut away, "realtime:room").await;

    // From the node with no lease, up its link, fanned by the writer.
    // First, because it is also what proves the link came up at all.
    broadcast(&mut away, "realtime:room", r#"{"x":1}"#).await;
    let text = heard(&mut here).await;
    assert!(text.contains("realtime:room"), "{text}");
    assert!(text.ends_with(r#"{"x":1}"#), "{text}");

    // And the other way.
    broadcast(&mut here, "realtime:room", r#"{"x":2}"#).await;
    let text = heard(&mut away).await;
    assert!(text.ends_with(r#"{"x":2}"#), "{text}");

    assert!(
        at.ups.attached().is_empty(),
        "a node holding sockets for a project it does not write started a database for them"
    );
}

#[tokio::test]
async fn a_request_that_is_not_a_socket_still_goes_to_the_writer() {
    // The socket tier is one arm of the front door and not a new way to
    // serve a project locally: everything that is not an upgrade is
    // forwarded exactly as it was before there was a tier at all.
    let at = two(true).await;
    let url = format!("http://{}/{REF}/auth/v1/health", at.away);
    let anon = key("anon");
    let status = tokio::task::spawn_blocking(move || {
        ureq::get(&url)
            .header("apikey", &anon)
            .call()
            .expect("the writer answered")
            .status()
            .as_u16()
    })
    .await
    .expect("the call ran");
    assert_eq!(status, 200);
    assert!(
        at.ups.attached().is_empty(),
        "the node that was asked answered it out of its own database"
    );
}

#[tokio::test]
async fn a_socket_for_a_project_nobody_writes_is_this_nodes_own_work() {
    // Nobody holds the lease, so this node may take it, and a node that
    // may write the project has no reason to hold its sockets at arm's
    // length. What decides is the lease and not the request.
    let at = two(false).await;
    let mut socket = connect(at.away).await;
    let reply = join(&mut socket, "realtime:room").await;
    assert_eq!(reply[3], "phx_reply");
    assert_eq!(reply[4]["status"], "ok", "{reply}");
    assert_eq!(at.ups.attached(), vec![REF]);
}
