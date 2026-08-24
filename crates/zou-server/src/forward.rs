//! Where a request goes when this node is not the writer.
//!
//! One node writes a tenant at a time, and which one is a fact that
//! lives in the manifest, because the lease lives there. Any node can
//! read it, so holder discovery is a GET of an object every node
//! already reads and there is no membership service, no gossip and
//! nothing to be out of date with the truth.
//!
//! What a node that is not the holder does with a request is the whole
//! of this module. The default is to send it to the node that is: the
//! request goes on the wire once more, with its method, headers and
//! body intact, and comes back as the answer. That is slower than
//! serving it here and it is the only correct thing a non holder can do
//! with a write, since a write here would need the lease and the lease
//! is somewhere else.
//!
//! Reads are the exception a fleet is built for, and they are off by
//! default. In stale read mode a safe method is answered from this
//! node's own copy, which lags the writer by however long ago the
//! writer last published, and the answer says so in a header rather
//! than pretending. A caller that cannot live with that sends
//! `x-zou-max-staleness` and gets forwarded instead, which makes the
//! choice the caller's per request rather than the operator's per
//! fleet.
//!
//! Everything here is a trait at the edges: who holds the lease, and
//! what puts a request on the wire. The same reason the attach manager
//! takes a [`crate::attach::Backend`], which is that the policy is the
//! part worth being sure about and it should not need two nodes and a
//! postmaster to be sure about it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, header};
use axum::response::Response;
use tokio::sync::RwLock;
use zou_store::CasStore;
use zou_store::layout::TenantLayout;
use zou_store::lease::{self, Holder};

/// Set by the node that forwarded a request, read by the node that
/// receives it. One hop is a fleet doing its job and two is a loop, so
/// a request that already carries this is never forwarded again.
pub const FORWARDED_BY: &str = "x-zou-forwarded-by";

/// How far behind the writer an answer served locally may be. On every
/// answer a stale read produced, and on no other answer, so its absence
/// is the statement that this came from the writer.
pub const STALE_SECONDS: &str = "x-zou-stale-seconds";

/// What a caller will put up with, in seconds. Zero means the writer
/// itself, which is what a client does between writing a row and
/// reading it back.
pub const MAX_STALENESS: &str = "x-zou-max-staleness";

/// The most this node will buffer to forward. A proxy that buffers must
/// bound the buffer, and a body bigger than this belongs on a direct
/// connection to the writer rather than in the memory of a node that is
/// only passing it along.
pub const MAX_BODY: usize = 64 << 20;

/// How long a peer has to answer a forwarded request. Long enough for a
/// slow query on the writer, short enough that a node that has gone
/// away does not hold this node's request open behind it.
pub const TIMEOUT: Duration = Duration::from_secs(30);

/// Who holds the lease, as a trait so the policy can be tested without
/// a second node.
pub trait Peers: Send + Sync {
    /// The writer for this tenant right now, or None when nobody holds
    /// the lease and the next node to ask may take it.
    fn holder(&self, tenant_ref: &str) -> Result<Option<Holder>, String>;
}

/// The manifest, which is where the answer already is.
pub struct StorePeers {
    store: Arc<dyn CasStore>,
}

impl StorePeers {
    pub fn new(store: Arc<dyn CasStore>) -> StorePeers {
        StorePeers { store }
    }
}

impl Peers for StorePeers {
    fn holder(&self, tenant_ref: &str) -> Result<Option<Holder>, String> {
        let layout = TenantLayout::new(tenant_ref);
        lease::holder(self.store.as_ref(), &layout, now_unix()).map_err(|e| e.to_string())
    }
}

/// How long the answer to "who holds it" is believed.
///
/// A lease is renewed every few seconds and a manifest GET per request
/// would put the object store in front of every write, so this is
/// cached. It is short because the thing it caches changes: a second is
/// under the renewal cadence, so a handover is noticed within a lease
/// TTL, and the cost of being a second late is a request forwarded to a
/// node that has just stopped being the writer, which answers it with
/// the lease error it already has for that.
pub const HOLDER_TTL: Duration = Duration::from_secs(1);

/// The lease lookup with a memory, on the same shape as the tenant
/// registry cache and for the same reason.
pub struct Holders {
    peers: Arc<dyn Peers>,
    ttl: Duration,
    seen: RwLock<HashMap<String, (Option<Holder>, Instant)>>,
}

impl Holders {
    pub fn new(peers: Arc<dyn Peers>) -> Holders {
        Holders {
            peers,
            ttl: HOLDER_TTL,
            seen: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Holders {
        self.ttl = ttl;
        self
    }

    pub async fn get(&self, tenant_ref: &str) -> Result<Option<Holder>, String> {
        let now = Instant::now();
        if let Some((holder, until)) = self.seen.read().await.get(tenant_ref)
            && *until > now
        {
            return Ok(holder.clone());
        }
        let peers = self.peers.clone();
        let wanted = tenant_ref.to_string();
        let holder = tokio::task::spawn_blocking(move || peers.holder(&wanted))
            .await
            .map_err(|e| format!("holder lookup: {e}"))??;
        self.seen.write().await.insert(
            tenant_ref.to_string(),
            (holder.clone(), Instant::now() + self.ttl),
        );
        Ok(holder)
    }

    /// Forget one tenant, for a node that has just taken or lost the
    /// lease itself and should not wait out its own ttl.
    pub async fn forget(&self, tenant_ref: &str) {
        self.seen.write().await.remove(tenant_ref);
    }
}

/// One request on its way to a peer.
pub struct Call {
    pub url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// And what came back.
pub struct Answer {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// What puts a forwarded request on the wire. A trait, so a test can
/// prove what is sent without a peer listening.
pub trait Proxy: Send + Sync {
    fn send(&self, call: Call) -> Result<Answer, String>;
}

/// The wire.
pub struct Http {
    agent: ureq::Agent,
}

impl Default for Http {
    fn default() -> Http {
        Http::new(TIMEOUT)
    }
}

impl Http {
    pub fn new(timeout: Duration) -> Http {
        Http {
            agent: ureq::Agent::config_builder()
                // A peer's 404 is the caller's 404 and not a transport
                // failure, so every status comes back as an answer.
                .http_status_as_error(false)
                .timeout_global(Some(timeout))
                .build()
                .into(),
        }
    }
}

impl Proxy for Http {
    fn send(&self, call: Call) -> Result<Answer, String> {
        // Built by hand rather than through the per method helpers,
        // because the method is whatever the caller used and this is
        // not the place to have opinions about which ones exist.
        let mut req = Request::builder()
            .method(call.method.as_str())
            .uri(&call.url);
        for (name, value) in &call.headers {
            req = req.header(name, value);
        }
        let req = req
            .body(&call.body[..])
            .map_err(|e| format!("forwarding to {}: {e}", call.url))?;
        let answer = self
            .agent
            .run(req)
            .map_err(|e| format!("forwarding to {}: {e}", call.url))?;
        let status = answer.status().as_u16();
        let headers = answer
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    String::from_utf8_lossy(value.as_bytes()).to_string(),
                )
            })
            .collect();
        let body = answer
            .into_body()
            .with_config()
            .limit(MAX_BODY as u64)
            .read_to_vec()
            .map_err(|e| format!("reading the answer from {}: {e}", call.url))?;
        Ok(Answer {
            status,
            headers,
            body,
        })
    }
}

/// What this node does with a request for a tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Where {
    /// Serve it here, which is what a holder does and what any node
    /// does for a tenant nobody holds.
    Here,
    /// Serve it here knowing the data is `behind` seconds old, and say
    /// so on the answer.
    Stale { behind: u64 },
    /// Send it to the node that holds the lease.
    There { endpoint: String },
    /// Refuse, because there is a writer and this node can neither be
    /// it nor reach it.
    Refuse { why: String, status: StatusCode },
}

/// The forwarding policy of one node.
pub struct Forwarding {
    /// This node's id, the same string it takes leases under, which is
    /// how it recognises its own lease.
    node: String,
    holders: Holders,
    proxy: Arc<dyn Proxy>,
    stale_reads: bool,
}

impl Forwarding {
    pub fn new(node: impl Into<String>, holders: Holders, proxy: Arc<dyn Proxy>) -> Forwarding {
        Forwarding {
            node: node.into(),
            holders,
            proxy,
            stale_reads: false,
        }
    }

    /// Answer safe methods from this node's own copy rather than
    /// forwarding them.
    ///
    /// Off by default, and it should stay off until a node can read a
    /// tenant it does not hold the lease for, which is the lazy hydrate
    /// work. On, it trades freshness for a round trip, which is the
    /// right trade for a dashboard and the wrong one for a client
    /// reading back what it just wrote.
    pub fn with_stale_reads(mut self, on: bool) -> Forwarding {
        self.stale_reads = on;
        self
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    pub fn holders(&self) -> &Holders {
        &self.holders
    }

    /// Where a request goes.
    pub async fn decide(&self, tenant_ref: &str, method: &Method, headers: &HeaderMap) -> Where {
        let holder = match self.holders.get(tenant_ref).await {
            Ok(holder) => holder,
            Err(e) => {
                log::warn!("lease lookup for {tenant_ref}: {e}");
                return Where::Refuse {
                    // The lease could not be read, so this node does not
                    // know whether it may write. Answering the request
                    // here would be a guess with a database at the end
                    // of it.
                    why: "the writer for this project could not be found".to_string(),
                    status: StatusCode::SERVICE_UNAVAILABLE,
                };
            }
        };
        // Nobody holds it, so this node may: the attach that follows
        // takes the lease, and if another node wins that race the loser
        // fails its attach rather than writing anything.
        let Some(holder) = holder else {
            return Where::Here;
        };
        if holder.node == self.node {
            return Where::Here;
        }
        if headers.contains_key(FORWARDED_BY) {
            // Two nodes each think the other is the writer, which is a
            // lease that changed hands mid flight. One hop is a fleet
            // working, two is a loop, and a loop is worth an error the
            // moment it starts rather than a chain of them.
            return Where::Refuse {
                why: "this request has already been forwarded once".to_string(),
                status: StatusCode::LOOP_DETECTED,
            };
        }
        // A socket is not a read, whatever its method says. A handshake
        // is a GET and what follows it is a connection that lives as
        // long as the client keeps it, so answering one out of this
        // node's own copy would not be a slightly old answer, it would
        // be a room of this node's own for the rest of the day. Where a
        // socket goes is decided by the caller of this, which serves it
        // here on a link to the holder rather than out of the data here.
        if self.stale_reads && safe(method) && !upgrading(headers) {
            let behind = behind(&holder, now_unix());
            if allowed(headers).is_none_or(|max| behind <= max) {
                return Where::Stale { behind };
            }
        }
        match holder.endpoint {
            Some(endpoint) => Where::There { endpoint },
            // A holder that published no address is a single node
            // deployment that has grown a second node, or a node behind
            // a network this one cannot cross. Either way the honest
            // answer is that this request cannot be served, not a write
            // that goes nowhere.
            None => Where::Refuse {
                why: "the writer for this project is on another node with no address".to_string(),
                status: StatusCode::SERVICE_UNAVAILABLE,
            },
        }
    }

    /// Send a request to `endpoint` and bring the answer back.
    ///
    /// The url is the peer's address with this request's own path and
    /// query on it, and the headers go across as they came in, `Host`
    /// included, so the peer resolves the same tenant this node did and
    /// nothing has to be told which one it is.
    pub async fn relay(&self, endpoint: &str, req: Request<Body>) -> Response {
        let (parts, body) = req.into_parts();
        let path = parts
            .uri
            .path_and_query()
            .map_or_else(|| parts.uri.path().to_string(), ToString::to_string);
        let url = format!("{}{}", endpoint.trim_end_matches('/'), path);
        let body = match axum::body::to_bytes(body, MAX_BODY).await {
            Ok(body) => body.to_vec(),
            Err(_) => {
                return crate::json_body(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    serde_json::json!({
                        "message": "this body is too large to forward, send it to the writer directly",
                    }),
                );
            }
        };
        let mut span = crate::ops::span("forward");
        let mut headers: Vec<(String, String)> = parts
            .headers
            .iter()
            .filter(|(name, _)| !hop_by_hop(name.as_str()) && *name != header::CONTENT_LENGTH)
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    String::from_utf8_lossy(value.as_bytes()).to_string(),
                )
            })
            .collect();
        headers.push((FORWARDED_BY.to_string(), self.node.clone()));
        if let Some(span) = span.as_ref() {
            // The peer's work belongs under this span, so the trace is
            // one trace across both nodes and the hop is visible in it.
            headers.retain(|(name, _)| name != zou_ops::trace::HEADER);
            headers.push((zou_ops::trace::HEADER.to_string(), span.ids.header()));
        }
        if let Some(span) = span.as_mut() {
            span.text("server.address", endpoint.to_string())
                .text("url.path", parts.uri.path());
        }
        let call = Call {
            url: url.clone(),
            method: parts.method.as_str().to_string(),
            headers,
            body,
        };
        let proxy = self.proxy.clone();
        let sent = tokio::task::spawn_blocking(move || proxy.send(call))
            .await
            .unwrap_or_else(|e| Err(format!("forwarding: {e}")));
        let answer = match sent {
            Ok(answer) => answer,
            Err(e) => {
                log::warn!("{e}");
                crate::ops::forwarded("failed");
                if let Some(span) = span.as_mut() {
                    span.failed(e);
                }
                if let Some(span) = span {
                    crate::ops::record(span);
                }
                return crate::json_body(
                    StatusCode::BAD_GATEWAY,
                    serde_json::json!({"message": "the writer for this project could not be reached"}),
                );
            }
        };
        crate::ops::forwarded("sent");
        if let Some(mut span) = span {
            span.int("http.response.status_code", i64::from(answer.status));
            crate::ops::record(span);
        }
        rebuild(answer)
    }
}

/// The answer a peer gave, as this node's answer.
fn rebuild(answer: Answer) -> Response {
    let mut out = Response::builder()
        .status(StatusCode::from_u16(answer.status).unwrap_or(StatusCode::BAD_GATEWAY));
    for (name, value) in answer.headers.iter().filter(|(name, _)| !hop_by_hop(name)) {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            out = out.header(name, value);
        }
    }
    out.body(Body::from(answer.body)).unwrap_or_else(|e| {
        crate::json_body(
            StatusCode::BAD_GATEWAY,
            serde_json::json!({"message": e.to_string()}),
        )
    })
}

/// Say on the answer how old the data behind it was. Only ever added to
/// an answer a stale read produced, so a caller can tell the two apart
/// by the header being there at all.
pub fn mark_stale(answer: &mut Response, behind: u64) {
    if let Ok(value) = HeaderValue::from_str(&behind.to_string()) {
        answer
            .headers_mut()
            .insert(HeaderName::from_static(STALE_SECONDS), value);
    }
}

/// A method that cannot change anything, which is the only kind a node
/// that is not the writer may answer itself.
///
/// POST is not on this list even though `POST /rest/v1/rpc/<fn>` is
/// often a read, because whether it is depends on what the function
/// does and this server cannot know that from the outside. A read
/// forwarded is slower; a write served locally is data that never
/// happened.
fn safe(method: &Method) -> bool {
    matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
}

/// How far behind the writer a local answer would be, in seconds.
///
/// It is measured from the writer's last publish, because that is the
/// freshest state any other node could possibly have: nothing a reader
/// does can make it newer than what the writer has written down.
fn behind(holder: &Holder, now_unix: u64) -> u64 {
    holder
        .published_unix
        .map_or(0, |published| now_unix.saturating_sub(published))
}

/// Whether this request is asking to stop being a request, which is
/// what a websocket handshake is. The header is a list, and a client
/// that sends `Upgrade: websocket` sends nothing else in it.
pub fn upgrading(headers: &HeaderMap) -> bool {
    headers
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
}

/// What the caller said it will put up with, if it said anything.
fn allowed(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(MAX_STALENESS)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// Headers that belong to one connection and not to the message on it.
/// Copying these onto the next hop is how a proxy breaks keep alive,
/// double encodes a body, or forwards an upgrade nobody is going to
/// honour.
fn hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use std::sync::Mutex;

    /// A lease nobody has to write to a store to be able to test.
    #[derive(Default)]
    struct Fixed {
        holder: Mutex<Option<Holder>>,
        fail: Mutex<bool>,
    }

    impl Peers for Fixed {
        fn holder(&self, _tenant_ref: &str) -> Result<Option<Holder>, String> {
            if *self.fail.lock().unwrap() {
                return Err("the store said no".to_string());
            }
            Ok(self.holder.lock().unwrap().clone())
        }
    }

    /// What arrived at the peer, which is the part worth asserting.
    struct Sent {
        method: String,
        url: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    /// A peer that answers without a socket, and remembers what it was
    /// sent.
    #[derive(Default)]
    struct Peer {
        seen: Mutex<Vec<Sent>>,
        answer: Mutex<Option<Answer>>,
        broken: Mutex<bool>,
    }

    impl Proxy for Peer {
        fn send(&self, call: Call) -> Result<Answer, String> {
            self.seen.lock().unwrap().push(Sent {
                method: call.method,
                url: call.url,
                headers: call.headers,
                body: call.body,
            });
            if *self.broken.lock().unwrap() {
                return Err("connection refused".to_string());
            }
            Ok(self.answer.lock().unwrap().take().unwrap_or(Answer {
                status: 200,
                headers: vec![("content-type".to_string(), "application/json".to_string())],
                body: b"[]".to_vec(),
            }))
        }
    }

    fn elsewhere(endpoint: Option<&str>, published_unix: Option<u64>) -> Holder {
        Holder {
            node: "node-b".to_string(),
            endpoint: endpoint.map(str::to_string),
            expires_unix: now_unix() + 15,
            epoch: 4,
            published_unix,
        }
    }

    fn front(holder: Option<Holder>) -> (Arc<Fixed>, Arc<Peer>, Forwarding) {
        let peers = Arc::new(Fixed::default());
        *peers.holder.lock().unwrap() = holder;
        let peer = Arc::new(Peer::default());
        let holders = Holders::new(peers.clone()).with_ttl(Duration::from_millis(0));
        (
            peers.clone(),
            peer.clone(),
            Forwarding::new("node-a", holders, peer),
        )
    }

    fn req(method: &str, url: &str, headers: &[(&str, &str)]) -> Request<Body> {
        let mut req = Request::builder().method(method).uri(url);
        for (name, value) in headers {
            req = req.header(*name, *value);
        }
        req.body(Body::empty()).expect("a request")
    }

    async fn decide(
        forwarding: &Forwarding,
        method: &str,
        headers: &[(&str, &str)],
    ) -> (Where, Request<Body>) {
        let req = req(method, "/rest/v1/todos?select=*", headers);
        let at = forwarding
            .decide("acme-prod", req.method(), req.headers())
            .await;
        (at, req)
    }

    #[tokio::test]
    async fn a_tenant_nobody_holds_is_served_here() {
        // Which is the whole of a cold start: the first node to be
        // asked takes the lease as it attaches.
        let (_peers, _peer, forwarding) = front(None);
        assert_eq!(decide(&forwarding, "POST", &[]).await.0, Where::Here);
    }

    #[tokio::test]
    async fn a_tenant_this_node_holds_is_served_here() {
        let mut mine = elsewhere(Some("http://10.0.0.4:8000"), None);
        mine.node = "node-a".to_string();
        let (_peers, _peer, forwarding) = front(Some(mine));
        assert_eq!(decide(&forwarding, "POST", &[]).await.0, Where::Here);
    }

    #[tokio::test]
    async fn a_write_goes_to_the_writer() {
        let (_peers, _peer, forwarding) =
            front(Some(elsewhere(Some("http://10.0.0.4:8000"), None)));
        for method in ["POST", "PATCH", "PUT", "DELETE"] {
            assert_eq!(
                decide(&forwarding, method, &[]).await.0,
                Where::There {
                    endpoint: "http://10.0.0.4:8000".to_string()
                },
                "{method}"
            );
        }
    }

    #[tokio::test]
    async fn a_read_goes_to_the_writer_too_until_stale_reads_are_on() {
        // The default, and it is the default because a node that does
        // not hold the lease has nothing to read a tenant out of yet.
        let (_peers, _peer, forwarding) =
            front(Some(elsewhere(Some("http://10.0.0.4:8000"), None)));
        assert!(matches!(
            decide(&forwarding, "GET", &[]).await.0,
            Where::There { .. }
        ));
    }

    #[tokio::test]
    async fn a_stale_read_is_answered_here_and_says_how_old_it_is() {
        let holder = elsewhere(Some("http://10.0.0.4:8000"), Some(now_unix() - 4));
        let (_peers, _peer, forwarding) = front(Some(holder));
        let forwarding = forwarding.with_stale_reads(true);
        // The clock can tick between the timestamp above and the
        // answer, so the lag is asked for as a range. What is under
        // test is that a read this old is served here and carries how
        // old it is, not which side of a tick the run landed on.
        let Where::Stale { behind } = decide(&forwarding, "GET", &[]).await.0 else {
            panic!("a read four seconds behind is served here");
        };
        assert!(
            (4..=6).contains(&behind),
            "four seconds behind, said as {behind}"
        );
        // And a write is still a write.
        assert!(matches!(
            decide(&forwarding, "POST", &[]).await.0,
            Where::There { .. }
        ));
    }

    #[tokio::test]
    async fn a_handshake_is_never_a_stale_read() {
        let holder = elsewhere(Some("http://10.0.0.4:8000"), Some(now_unix() - 1));
        let (_peers, _peer, forwarding) = front(Some(holder));
        let forwarding = forwarding.with_stale_reads(true);
        // One second behind is inside anything a caller would ask for,
        // and the method is the same GET a stale read is answered from
        // here. What makes this different is that the answer is a
        // connection: the holder is named so its sockets can be served
        // on a link to it rather than out of this node's own copy.
        assert_eq!(
            decide(
                &forwarding,
                "GET",
                &[(header::UPGRADE.as_str(), "websocket")]
            )
            .await
            .0,
            Where::There {
                endpoint: "http://10.0.0.4:8000".to_string()
            }
        );
    }

    #[tokio::test]
    async fn a_caller_that_cannot_afford_the_lag_is_forwarded_instead() {
        let holder = elsewhere(Some("http://10.0.0.4:8000"), Some(now_unix() - 9));
        let (_peers, _peer, forwarding) = front(Some(holder));
        let forwarding = forwarding.with_stale_reads(true);
        assert!(
            matches!(
                decide(&forwarding, "GET", &[(MAX_STALENESS, "2")]).await.0,
                Where::There { .. }
            ),
            "nine seconds behind is more than two"
        );
        let Where::Stale { behind } = decide(&forwarding, "GET", &[(MAX_STALENESS, "30")]).await.0
        else {
            panic!("thirty seconds is more than nine");
        };
        assert!(
            (9..=11).contains(&behind),
            "nine seconds behind, said as {behind}"
        );
        assert!(
            matches!(
                decide(&forwarding, "GET", &[(MAX_STALENESS, "0")]).await.0,
                Where::There { .. }
            ),
            "zero is what read your own write means"
        );
    }

    #[tokio::test]
    async fn a_writer_with_no_address_is_said_out_loud() {
        let (_peers, _peer, forwarding) = front(Some(elsewhere(None, None)));
        let Where::Refuse { status, why } = decide(&forwarding, "POST", &[]).await.0 else {
            panic!("a write cannot be served here");
        };
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(why.contains("no address"), "{why}");
    }

    #[tokio::test]
    async fn a_request_is_forwarded_once_and_never_twice() {
        let (_peers, _peer, forwarding) =
            front(Some(elsewhere(Some("http://10.0.0.4:8000"), None)));
        let Where::Refuse { status, .. } = decide(&forwarding, "POST", &[(FORWARDED_BY, "node-c")])
            .await
            .0
        else {
            panic!("a second hop is a loop");
        };
        assert_eq!(status, StatusCode::LOOP_DETECTED);
    }

    #[tokio::test]
    async fn a_lease_that_cannot_be_read_is_not_guessed_at() {
        let (peers, _peer, forwarding) = front(Some(elsewhere(Some("http://10.0.0.4:8000"), None)));
        *peers.fail.lock().unwrap() = true;
        let Where::Refuse { status, .. } = decide(&forwarding, "GET", &[]).await.0 else {
            panic!("not knowing who the writer is is not the same as being it");
        };
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn what_goes_across_is_the_request_that_came_in() {
        let (_peers, peer, forwarding) =
            front(Some(elsewhere(Some("http://10.0.0.4:8000/"), None)));
        let req = Request::builder()
            .method("POST")
            .uri("/rest/v1/todos?select=*")
            .header(header::HOST, "acme-prod.zou.example")
            .header("apikey", "the-anon-key")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONNECTION, "keep-alive")
            .body(Body::from(r#"{"title":"write me"}"#))
            .expect("a request");
        let answer = forwarding.relay("http://10.0.0.4:8000/", req).await;
        assert_eq!(answer.status(), StatusCode::OK);

        let seen = peer.seen.lock().unwrap();
        let sent = seen.first().expect("one call");
        assert_eq!(sent.method, "POST");
        assert_eq!(
            sent.url, "http://10.0.0.4:8000/rest/v1/todos?select=*",
            "path and query, and one slash between"
        );
        assert_eq!(sent.body, br#"{"title":"write me"}"#);
        let header = |wanted: &str| {
            sent.headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(wanted))
                .map(|(_, value)| value.clone())
        };
        assert_eq!(
            header("host").as_deref(),
            Some("acme-prod.zou.example"),
            "the peer resolves the tenant the same way this node did"
        );
        assert_eq!(header("apikey").as_deref(), Some("the-anon-key"));
        assert_eq!(header(FORWARDED_BY).as_deref(), Some("node-a"));
        assert!(
            header("connection").is_none(),
            "hop by hop headers belong to the hop"
        );
    }

    #[tokio::test]
    async fn the_peers_answer_is_the_answer() {
        let (_peers, peer, forwarding) = front(Some(elsewhere(Some("http://10.0.0.4:8000"), None)));
        *peer.answer.lock().unwrap() = Some(Answer {
            status: 201,
            headers: vec![
                ("content-type".to_string(), "application/json".to_string()),
                ("location".to_string(), "/rest/v1/todos?id=eq.7".to_string()),
                ("transfer-encoding".to_string(), "chunked".to_string()),
            ],
            body: br#"[{"id":7}]"#.to_vec(),
        });
        let answer = forwarding
            .relay("http://10.0.0.4:8000", req("POST", "/rest/v1/todos", &[]))
            .await;
        assert_eq!(answer.status(), StatusCode::CREATED);
        assert_eq!(
            answer
                .headers()
                .get("location")
                .and_then(|v| v.to_str().ok()),
            Some("/rest/v1/todos?id=eq.7")
        );
        assert!(
            answer.headers().get("transfer-encoding").is_none(),
            "the peer's framing is not this connection's framing"
        );
        let body = to_bytes(answer.into_body(), 1 << 20).await.expect("a body");
        assert_eq!(&body[..], br#"[{"id":7}]"#);
    }

    #[tokio::test]
    async fn a_peer_that_cannot_be_reached_is_a_bad_gateway() {
        let (_peers, peer, forwarding) = front(Some(elsewhere(Some("http://10.0.0.4:8000"), None)));
        *peer.broken.lock().unwrap() = true;
        let answer = forwarding
            .relay("http://10.0.0.4:8000", req("POST", "/rest/v1/todos", &[]))
            .await;
        assert_eq!(answer.status(), StatusCode::BAD_GATEWAY);
        let body = to_bytes(answer.into_body(), 1 << 20).await.expect("a body");
        let body = String::from_utf8_lossy(&body);
        assert!(
            !body.contains("10.0.0.4"),
            "an address a caller cannot reach is not a caller's business: {body}"
        );
    }

    #[tokio::test]
    async fn a_stale_answer_carries_its_age() {
        let mut answer = crate::json_body(StatusCode::OK, serde_json::json!([]));
        mark_stale(&mut answer, 12);
        assert_eq!(
            answer
                .headers()
                .get(STALE_SECONDS)
                .and_then(|v| v.to_str().ok()),
            Some("12")
        );
    }
}
