//! A socket tier nobody is on does not last forever.
//!
//! Its own file rather than another test beside the fleet ones, and for
//! one reason: what says a tier went is a process gauge, and a gauge is
//! a process global. A test binary with other tests building tiers in
//! parallel would have this one reading their arithmetic as well as its
//! own. One test in one binary reads only what it did.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tower::ServiceExt as _;
use zou_server::attach::{Attached, Backend};
use zou_server::forward::{self, Forwarding, Holders, Peers};
use zou_server::gateway::{fleet_keeping, gateway};
use zou_server::tenant::{Registry, Routing};
use zou_server::{Config, jwt};
use zou_store::lease::Holder;
use zou_store::registry::{self, Tenant};
use zou_store::{CasStore, open_store};

const SECRET: &str = "super-secret-jwt-token-with-at-least-32-characters-long";
const REF: &str = "acme-prod";

/// A project brought up with no database under it, which is everything
/// a socket needs of one.
#[derive(Default)]
struct Ups(Mutex<Vec<String>>);

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

fn routing() -> Routing {
    Routing {
        domains: Vec::new(),
        path_prefix: true,
    }
}

async fn serve(router: axum::Router) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
    let at = listener.local_addr().expect("the port");
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    at
}

/// How many socket tiers this process is holding, off the ops port,
/// which is the number an operator watches and the only one from out
/// here that says whether a tier is still there.
async fn tiers() -> u64 {
    let answer = zou_server::ops::ops("0.0.0-test")
        .oneshot(
            axum::http::Request::builder()
                .uri("/metrics")
                .body(axum::body::Body::empty())
                .expect("a request"),
        )
        .await
        .expect("the ops port answers");
    let body = axum::body::to_bytes(answer.into_body(), 1 << 20)
        .await
        .expect("the body");
    let body = String::from_utf8_lossy(&body);
    body.lines()
        .find_map(|line| line.strip_prefix("zou_realtime_socket_tiers "))
        .map(|n| n.trim().parse::<u64>().expect("a gauge"))
        // Absent until the first one is built, which is a real zero
        // rather than a missing reading.
        .unwrap_or(0)
}

#[tokio::test]
async fn a_socket_tier_nobody_is_on_is_let_go() {
    let dir = tempfile::tempdir().expect("a directory to write into");
    let store: Arc<dyn CasStore> =
        Arc::from(open_store(&dir.path().to_string_lossy()).expect("a store opens"));
    registry::create(store.as_ref(), &Tenant::new(REF, SECRET, 1)).expect("it registers");

    let holder = serve(gateway(
        routing(),
        Arc::new(Registry::new(store.clone())),
        Arc::new(Attached::new(Arc::new(Ups::default()))),
    ))
    .await;
    let forwarding = Forwarding::new(
        "node-away",
        Holders::new(Arc::new(Lease(Some(format!("http://{holder}"))))),
        Arc::new(forward::Http::default()),
    );
    // A second rather than a quarter of an hour, which is the whole
    // reason the window is a parameter.
    let away = serve(fleet_keeping(
        routing(),
        Arc::new(Registry::new(store)),
        Arc::new(Attached::new(Arc::new(Ups::default()))),
        Some(Arc::new(forwarding)),
        Duration::from_secs(1),
    ))
    .await;

    assert_eq!(tiers().await, 0, "nothing is held before anybody connects");
    let url = format!(
        "ws://{away}/{REF}/realtime/v1/websocket?apikey={}&vsn=2.0.0",
        jwt::mint(&jwt::key_claims("anon"), SECRET.as_bytes())
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("the socket upgrades");
    // Joining is what puts the socket in the count as well as in the
    // room, and the count is half of what the sweep asks.
    socket
        .send(r#"["1","1","realtime:room","phx_join",{"config":{}}]"#.into())
        .await
        .expect("the socket takes it");
    socket.next().await.expect("a reply").expect("a message");
    assert_eq!(tiers().await, 1, "the tier the socket is on");

    // Held while somebody is on it, whatever the window says. Three
    // seconds is three sweeps at a one second window.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(tiers().await, 1, "a tier with a socket on it is not idle");

    drop(socket);
    let gone = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if tiers().await == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    assert!(
        gone.is_ok(),
        "the tier is still held a long time after the last socket left it"
    );
}
