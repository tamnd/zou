//! The port an operator points a scraper at.
//!
//! Two routes, `/healthz` and `/metrics`, on a listener of their own.
//! Their own listener and not the front door's, for two reasons that
//! both come from what the front door is. Under path routing the first
//! segment of every url is a tenant ref, with nothing reserved, so a
//! `/metrics` route there would quietly take a name a project could
//! have had. And a scrape is the operational state of the node, which
//! is not something to hand to whoever holds an anon key for one of the
//! projects on it. A second listener is the cheap answer to both, and
//! it is the one every deployment already knows how to firewall.
//!
//! What is instrumented lives where it happens: the attach manager
//! counts attaches, the tenant registry counts lookups, the request
//! path counts requests. This module owns the names, so that the names
//! are in one place and a call site is one line.
//!
//! Traces are the other half and they do not live on this listener at
//! all, because a trace is not scraped: it is made on the request path
//! and posted to a collector. What is here is the naming, again, so
//! that a span is opened in one line at the place the work happens.
//!
//! The store's numbers are not counted here at all. They already exist,
//! in the shared counter file `ZOU_STORE_STATS` names, which is how
//! ops made by a postgres backend in another process get counted at
//! all, and a scrape folds that file in as it reads it. Counting them
//! twice would mean counting the ones this process can see and missing
//! the rest.

use std::time::Instant;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use zou_ops::metrics::{Registry, SECONDS};
use zou_ops::registry;
use zou_ops::trace;

/// What Prometheus expects a scrape to be, version and all. A scraper
/// that gets `application/json` here does not guess.
const EXPOSITION: &str = "text/plain; version=0.0.4; charset=utf-8";

/// The ops router. `version` is what the build reports about itself,
/// which is the one thing a scrape carries that the process cannot
/// count.
pub fn ops(version: &'static str) -> Router {
    // A gauge that is always one, which is the standard way to get a
    // build's version into a query: join on it and every series can be
    // grouped by the version that produced it.
    registry()
        .gauge(
            "zou_build_info",
            "always 1, labelled with the build",
            &[("version", version)],
        )
        .set(1);
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .fallback(missing)
}

/// Alive, which is all this answers.
///
/// It does not touch a store, a lease or a database on purpose. A
/// liveness check that reads a dependency turns one slow dependency
/// into a rolling restart of everything that depends on it, and there
/// is no readiness check here either, because readiness for this
/// process would have to name a tenant: nothing is attached until a
/// request asks for one, so a node with nothing attached is not
/// unready, it is idle.
async fn healthz() -> Response {
    (StatusCode::OK, "ok\n").into_response()
}

async fn metrics() -> Response {
    let mut out = registry().render();
    if let Some(store) = store_metrics() {
        out.push_str(&store);
    }
    ([(header::CONTENT_TYPE, EXPOSITION)], out).into_response()
}

async fn missing() -> Response {
    (StatusCode::NOT_FOUND, "not found\n").into_response()
}

/// The store counter file as metric families, or None when nothing set
/// `ZOU_STORE_STATS` and there is no file to read.
///
/// It is rendered through a registry built for this scrape rather than
/// added into the process one, because the file is the truth and the
/// numbers in it only go up: adding deltas into counters of our own
/// would mean keeping a copy of the last scrape and being wrong the
/// first time the file was reset.
fn store_metrics() -> Option<String> {
    let path = std::env::var("ZOU_STORE_STATS")
        .ok()
        .filter(|p| !p.is_empty())?;
    store_families(std::path::Path::new(&path))
}

/// The counter file at `path` as metric families, or None when it
/// cannot be read, which is a warning and not a failed scrape: the rest
/// of the numbers are still worth having.
fn store_families(path: &std::path::Path) -> Option<String> {
    let snapshot = match zou_store::Snapshot::read(path) {
        Ok(snapshot) => snapshot,
        Err(e) => {
            log::warn!("store counters: {e}");
            return None;
        }
    };
    let reg = Registry::new();
    reg.counter(
        "zou_store_conflicts_total",
        "conditional writes that lost a race",
        &[],
    )
    .add(snapshot.conflicts);
    for op in &snapshot.ops {
        for class in &op.by_class {
            reg.counter(
                "zou_store_ops_total",
                "store operations",
                &[("op", op.op), ("class", class.class)],
            )
            .add(class.count);
            reg.counter(
                "zou_store_bytes_total",
                "bytes through the store",
                &[("op", op.op), ("class", class.class)],
            )
            .add(class.bytes);
        }
        reg.counter(
            "zou_store_errors_total",
            "store operations that failed",
            &[("op", op.op)],
        )
        .add(op.errors);
        let seconds = reg.histogram(
            "zou_store_op_seconds",
            "how long store operations took",
            STORE_SECONDS,
            &[("op", op.op)],
        );
        for (at, count) in op.buckets.iter().enumerate() {
            // Slot b counted what finished under 2^(b+1) microseconds,
            // and each is folded in at that bound.
            seconds.observe_count((1u64 << (at + 1)) as f64 / 1e6, *count);
        }
    }
    for tier in &snapshot.reads {
        reg.counter(
            "zou_store_read_calls_total",
            "smgr reads by tier",
            &[("tier", tier.tier)],
        )
        .add(tier.calls);
        reg.counter(
            "zou_store_read_pages_total",
            "pages read by tier",
            &[("tier", tier.tier)],
        )
        .add(tier.pages);
    }
    Some(reg.render())
}

/// The store's own bucket edges, seconds, which are the powers of two
/// microseconds its counter file keeps. Only the useful stretch of them
/// is exposed: under a microsecond is noise on any op that touched a
/// network, and everything above eight seconds lands in the last
/// bucket, which is where a scrape wants it anyway.
const STORE_SECONDS: &[f64] = &[
    0.000_002, 0.000_008, 0.000_032, 0.000_128, 0.000_512, 0.002_048, 0.008_192, 0.032_768,
    0.131_072, 0.524_288, 2.097_152, 8.388_608,
];

/// Count a request and how long it took, and trace it. Layered on the
/// whole router, so a 404 and a 429 are counted too, which are the two
/// a graph is most often drawn to explain.
///
/// The trace context comes from the caller when the caller sent one, so
/// this server's span is a child of whatever made the call, and a new
/// trace starts here when it did not. The inner service runs under that
/// context, which is what puts the trace ids on the log lines a request
/// writes as well as on the span.
pub async fn measure(req: Request<Body>, next: Next) -> Response {
    let surface = surface(req.uri().path());
    let start = Instant::now();
    let mut span = opening(&req, surface);
    let answer = match span.as_ref().map(|(_, ids)| *ids) {
        Some(ids) => trace::with(ids, next.run(req)).await,
        None => next.run(req).await,
    };
    if let Some((mut span, _)) = span.take() {
        span.int(
            "http.response.status_code",
            i64::from(answer.status().as_u16()),
        );
        if answer.status().is_server_error() {
            span.failed(answer.status().to_string());
        }
        record(span);
    }
    let status = answer.status().as_u16().to_string();
    registry()
        .counter(
            "zou_http_requests_total",
            "requests answered",
            &[("surface", surface), ("status", &status)],
        )
        .inc();
    registry()
        .histogram(
            "zou_http_request_seconds",
            "how long requests took",
            SECONDS,
            &[("surface", surface)],
        )
        .since(start);
    answer
}

/// The span a request opens, and the context to run it under, or None
/// when nothing is collecting traces or the caller asked for this one
/// not to be recorded.
///
/// The name is the method and the surface rather than the path, because
/// a span name is grouped on the same way a metric label is and a path
/// carries a tenant ref, an object key and a row filter in it. The path
/// itself is an attribute, where high cardinality is what a trace is
/// for. The query string is not, at any cardinality: `?apikey=<jwt>` is
/// a spelling this server accepts, so exporting the query would mail
/// credentials to a collector.
fn opening(req: &Request<Body>, surface: &'static str) -> Option<(trace::Span, trace::Ids)> {
    trace::tracer()?;
    let parent = req
        .headers()
        .get(trace::HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(trace::Ids::parse);
    let ids = parent.map_or_else(|| trace::Ids::root(true), |parent| parent.child());
    if !ids.sampled {
        return None;
    }
    let mut span = trace::Span::start(
        format!("{} /{surface}", req.method()),
        trace::Kind::Server,
        ids,
        parent.map(|parent| parent.span),
    );
    span.text("http.request.method", req.method().as_str())
        .text("url.path", req.uri().path())
        .text("zou.surface", surface);
    Some((span, ids))
}

/// An internal span under whatever request is being served, or None
/// when nothing is collecting or this is not happening under a request.
///
/// Callers hold an `Option` and do nothing with a `None`, which is the
/// whole cost of tracing being off: no span is built rather than one
/// built and thrown away.
pub fn span(name: &'static str) -> Option<zou_ops::trace::Span> {
    trace::tracer()?;
    let parent = trace::current()?;
    Some(trace::Span::start(
        name,
        trace::Kind::Internal,
        parent.child(),
        Some(parent.span),
    ))
}

/// Stamp a span and queue it. A span that is never passed here is never
/// exported, which is what should happen on a path that returned early.
pub fn record(span: zou_ops::trace::Span) {
    if let Some(tracer) = trace::tracer() {
        tracer.record(span.end());
    }
}

/// Which surface a path belongs to, as a fixed set of names.
///
/// A label per tenant is what the shape of this server invites and it
/// is the one label that must not be here: a thousand tenants times the
/// statuses times the surfaces is a series count no scrape wants to
/// carry, and per tenant numbers are a billing question, not a
/// monitoring one. Everything unrecognised is `other`, which keeps a
/// stranger's url from inventing a series.
fn surface(path: &str) -> &'static str {
    match path.split('/').nth(1) {
        Some("rest") => "rest",
        Some("auth") => "auth",
        Some("storage") => "storage",
        Some("realtime") => "realtime",
        Some("functions") => "functions",
        _ => "other",
    }
}

/// How many tenants are attached right now.
pub fn attached(n: usize) {
    registry()
        .gauge("zou_tenants_attached", "tenants attached right now", &[])
        .set(n as u64);
}

/// One attach, how it went and how long it took. Failures are counted
/// as attempts too, since an attach that is failing fast is the shape
/// of an outage and a graph of successes alone hides it.
pub fn attach(ok: bool, start: Instant) {
    registry()
        .counter(
            "zou_tenant_attaches_total",
            "tenants started",
            &[("outcome", if ok { "ok" } else { "error" })],
        )
        .inc();
    if ok {
        registry()
            .histogram(
                "zou_tenant_attach_seconds",
                "how long a cold attach took",
                SECONDS,
                &[],
            )
            .since(start);
    }
}

/// One request this node did not answer itself. `outcome` is `sent`
/// when the writer answered and `failed` when it could not be reached,
/// which is the difference between a fleet that is working and one that
/// is partitioned.
pub fn forwarded(outcome: &'static str) {
    registry()
        .counter(
            "zou_forwarded_requests_total",
            "requests passed to the tenant's writer",
            &[("outcome", outcome)],
        )
        .inc();
}

/// One read answered from this node's own copy, and how far behind the
/// writer it was. Both, because the count says how much of the traffic
/// is taking the fast path and the seconds say what it cost.
pub fn stale_read(behind: u64) {
    registry()
        .counter(
            "zou_stale_reads_total",
            "reads answered without the writer",
            &[],
        )
        .inc();
    registry()
        .histogram(
            "zou_stale_read_seconds",
            "how far behind the writer a local read was",
            SECONDS,
            &[],
        )
        .observe(behind as f64);
}

/// One postgres wire session opening or closing. A level rather than a
/// rate, because what a pooler is sized against is how many sessions
/// are open at once.
pub fn pg_session(open: bool) {
    let sessions = registry().gauge("zou_pg_sessions", "postgres sessions open right now", &[]);
    match open {
        true => sessions.inc(),
        false => sessions.dec(),
    }
}

/// One login on the pg port. `refused` is a client that was told why in
/// its own protocol, a wrong key or an unknown project, and `error` is
/// everything that did not get that far, so the two apart are the
/// difference between somebody typing a password wrong and a database
/// that will not answer.
pub fn pg_login(outcome: &'static str) {
    registry()
        .counter(
            "zou_pg_logins_total",
            "postgres connections that reached a decision",
            &[("outcome", outcome)],
        )
        .inc();
}

/// One pooled backend opening or closing. Against `zou_pg_sessions`
/// this is the whole claim a transaction pooler makes: many sessions,
/// few backends.
pub fn pg_backend(open: bool) {
    let backends = registry().gauge(
        "zou_pg_backends",
        "pooled connections to tenant databases open right now",
        &[],
    );
    match open {
        true => backends.inc(),
        false => backends.dec(),
    }
}

/// One transaction finished on the pooler, and how long its client
/// waited for a backend to run it on. The wait is the number that says
/// whether a pool is too small, and it is not the query time, so the
/// two are not mixed.
pub fn pg_transaction() {
    registry()
        .counter(
            "zou_pg_transactions_total",
            "transactions run through the pooler",
            &[],
        )
        .inc();
}

pub fn pg_checkout(start: Instant) {
    registry()
        .histogram(
            "zou_pg_checkout_seconds",
            "how long a transaction waited for a backend",
            SECONDS,
            &[],
        )
        .since(start);
}

/// What a finished session moved, in each direction, counted from the
/// client's point of view.
pub fn pg_bytes(sent: u64, received: u64) {
    let bytes = |direction: &'static str| {
        registry().counter(
            "zou_pg_bytes_total",
            "bytes proxied on the postgres port",
            &[("direction", direction)],
        )
    };
    bytes("sent").add(sent);
    bytes("received").add(received);
}

/// One `postgres_changes` frame handed to a socket, and how long the
/// change took to reach it.
///
/// Two numbers, because they answer different questions and one of them
/// is not entirely this server's. The commit to socket seconds is what
/// an application feels, counted from the commit timestamp postgres
/// wrote by its own clock, so on a database that is not on this machine
/// it carries whatever the two clocks disagree about. It is still the
/// number somebody asking how live a subscription is means, and it is
/// the one the milestone puts a target on.
///
/// The other is this server's own share of the same interval, from the
/// tap reading the change out of the slot to the frame going out, on
/// one clock. That is the one to look at when the first one moves,
/// since it says whether what moved was here.
///
/// A clock behind the database's would make the first one negative,
/// which is not a duration and would poison the sum. It is counted as
/// zero, and the two apart are what says that is what happened.
pub fn change_delivered(commit_ts: i64, read: Instant) {
    registry()
        .counter(
            "zou_realtime_changes_total",
            "database changes delivered to a socket",
            &[],
        )
        .inc();
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_micros() as i64)
        .unwrap_or(commit_ts);
    registry()
        .histogram(
            "zou_realtime_commit_to_socket_seconds",
            "how long a change took to reach a socket, from the transaction's commit",
            SECONDS,
            &[],
        )
        .observe(micros.saturating_sub(commit_ts).max(0) as f64 / 1_000_000.0);
    registry()
        .histogram(
            "zou_realtime_change_seconds",
            "how long a change took inside this server, from the tap reading it",
            SECONDS,
            &[],
        )
        .since(read);
}

/// How many realtime sockets this node is holding right now, and how
/// many of them asked for database changes.
///
/// Two gauges rather than one, because they cost different things. A
/// socket is a connection, a task and whatever the client left in its
/// send queue, and a subscriber is that plus a place in the change
/// reader's tables and a policy check per row that matches it. A node
/// holding a hundred thousand of the first and none of the second is a
/// different machine from one holding a hundred thousand of both, and a
/// number that added them up would not say which.
///
/// Counted per node rather than per project on purpose: this is the
/// reading an operator sizes a box on, and it is the same reading
/// whether the sockets belong to one project or to a thousand. What one
/// project is allowed is the quota's business and it refuses on its own.
///
/// A socket on a fan out node is a socket there and a subscriber on the
/// holder, so a fleet's two gauges do not add up to one machine's, and
/// that is the honest shape: the connection is on one box and the place
/// in the change reader is on the other.
pub fn socket_joined() {
    sockets().inc();
}

pub fn socket_left() {
    sockets().dec();
}

fn sockets() -> zou_ops::Gauge {
    registry().gauge(
        "zou_realtime_sockets",
        "realtime sockets connected to this node right now",
        &[],
    )
}

/// The subscriber half of the pair above, moved by the change reader,
/// since a subscriber's life is the reader's rather than the socket's:
/// one arrives when a subscription is registered and goes when the
/// socket does, or when a link carrying it does.
pub fn subscribers(n: usize) {
    registry()
        .gauge(
            "zou_realtime_subscribers",
            "sockets subscribed to database changes on this node right now",
            &[],
        )
        .set(n as u64);
}

/// One registry lookup, hit or miss, which is the reading that says
/// whether the cache ttls are doing anything.
pub fn lookup(hit: bool) {
    registry()
        .counter(
            "zou_registry_lookups_total",
            "tenant registry lookups",
            &[("result", if hit { "hit" } else { "miss" })],
        )
        .inc();
}

/// Serve the ops router on `listener` forever, on a runtime of its own.
///
/// One worker thread, because this answers a scrape every fifteen
/// seconds and nothing else, and because the reason it is a separate
/// listener is so that it stays answerable when the front door is
/// busy.
pub fn serve_blocking(
    listener: std::net::TcpListener,
    version: &'static str,
) -> Result<(), String> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;
    rt.block_on(async move {
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("nonblocking: {e}"))?;
        let listener =
            tokio::net::TcpListener::from_std(listener).map_err(|e| format!("listener: {e}"))?;
        axum::serve(listener, ops(version).into_make_service())
            .await
            .map_err(|e| format!("serve: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use tower::ServiceExt;

    async fn call(router: &Router, url: &str) -> (StatusCode, String, String) {
        let answer = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(url)
                    .body(Body::empty())
                    .expect("a request"),
            )
            .await
            .expect("an answer");
        let status = answer.status();
        let kind = answer
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body = to_bytes(answer.into_body(), 1 << 20).await.expect("a body");
        (status, kind, String::from_utf8_lossy(&body).to_string())
    }

    #[tokio::test]
    async fn healthz_answers_without_touching_anything() {
        let (status, _, body) = call(&ops("0.0.0-test"), "/healthz").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "ok\n");
    }

    #[tokio::test]
    async fn metrics_are_the_exposition_format() {
        // The process registry is shared with every other test in this
        // binary, so what is asserted here is which families a scrape
        // carries, not what they are counting at the moment.
        attached(3);
        lookup(true);
        let (status, kind, body) = call(&ops("0.0.0-test"), "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(kind, EXPOSITION);
        assert!(
            body.contains("zou_build_info{version=\"0.0.0-test\"} 1\n"),
            "{body}"
        );
        assert!(
            body.contains("# TYPE zou_tenants_attached gauge\n"),
            "{body}"
        );
        assert!(
            body.contains("zou_registry_lookups_total{result=\"hit\"}"),
            "{body}"
        );
    }

    /// A delivery is two intervals and a count, and the one measured
    /// across two clocks cannot be allowed to report a negative.
    #[tokio::test]
    async fn a_change_that_reached_a_socket_is_two_intervals_and_a_count() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock")
            .as_micros() as i64;
        change_delivered(now - 250_000, Instant::now());
        // A commit timestamp from a database whose clock is ahead of
        // this one, which is an hour that must not land in the sum.
        change_delivered(now + 3_600_000_000, Instant::now());
        let (_, _, body) = call(&ops("0.0.0-test"), "/metrics").await;
        assert!(body.contains("zou_realtime_changes_total 2\n"), "{body}");
        assert!(
            body.contains("# TYPE zou_realtime_commit_to_socket_seconds histogram\n"),
            "{body}"
        );
        assert!(
            body.contains("# TYPE zou_realtime_change_seconds histogram\n"),
            "{body}"
        );
        let sum = body
            .lines()
            .find_map(|line| line.strip_prefix("zou_realtime_commit_to_socket_seconds_sum "))
            .and_then(|n| n.parse::<f64>().ok())
            .expect("a sum");
        assert!(
            (0.2..1.0).contains(&sum),
            "the quarter second is in it and the clock that is an hour ahead is a zero, {sum}"
        );
    }

    /// The two numbers a socket tier is sized on. A gauge that only
    /// ever went up would say a node was holding sockets that had gone
    /// hours ago, which is the reading somebody would buy a box on.
    #[tokio::test]
    async fn the_sockets_a_node_holds_go_down_again() {
        socket_joined();
        socket_joined();
        let (_, _, body) = call(&ops("0.0.0-test"), "/metrics").await;
        let held = |body: &str| {
            body.lines()
                .find_map(|line| line.strip_prefix("zou_realtime_sockets "))
                .and_then(|n| n.parse::<u64>().ok())
                .expect("a gauge")
        };
        let two = held(&body);
        assert!(two >= 2, "both of them are in it, {two}");
        socket_left();
        socket_left();
        let (_, _, body) = call(&ops("0.0.0-test"), "/metrics").await;
        assert_eq!(held(&body), two - 2, "and both of them are out of it again");
        subscribers(7);
        let (_, _, body) = call(&ops("0.0.0-test"), "/metrics").await;
        assert!(body.contains("zou_realtime_subscribers 7\n"), "{body}");
    }

    #[tokio::test]
    async fn the_ops_port_has_nothing_else_on_it() {
        // Whatever else is asked for here is not a tenant's url. This
        // listener serves an operator and nobody else.
        let (status, _, _) = call(&ops("0.0.0-test"), "/rest/v1/todos").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn a_path_is_named_by_its_surface_and_never_by_its_tenant() {
        assert_eq!(surface("/rest/v1/todos"), "rest");
        assert_eq!(surface("/auth/v1/token"), "auth");
        assert_eq!(surface("/storage/v1/object/pics/a.png"), "storage");
        assert_eq!(surface("/realtime/v1/websocket"), "realtime");
        assert_eq!(surface("/"), "other");
        assert_eq!(surface("/acme-prod/rest/v1/todos"), "other");
        assert_eq!(
            surface("/../../etc/passwd"),
            "other",
            "a stranger does not get to name a series"
        );
    }

    #[test]
    fn the_store_counter_file_is_folded_in_when_there_is_one() {
        use zou_store::CasStore;
        let dir = tempfile::tempdir().expect("a directory");
        let counters = dir.path().join("stats");
        let store = zou_store::StatsStore::new(
            Box::new(zou_store::cas::LocalFsStore::new(dir.path().join("store"))),
            &counters,
        )
        .expect("counters open");
        store
            .put("tenants/local/MANIFEST", b"{}")
            .expect("a put lands");
        let out = store_families(&counters).expect("a snapshot renders");
        assert!(
            out.contains("zou_store_ops_total{op=\"put\",class=\"manifest\"} 1\n"),
            "{out}"
        );
        assert!(
            out.contains("zou_store_op_seconds_count{op=\"put\"} 1\n"),
            "{out}"
        );
        assert!(
            out.contains("zou_store_bytes_total{op=\"put\",class=\"manifest\"} 2\n"),
            "{out}"
        );
    }

    #[test]
    fn a_counter_file_that_is_not_there_is_a_warning_and_not_a_failed_scrape() {
        let dir = tempfile::tempdir().expect("a directory");
        assert!(store_families(&dir.path().join("nothing")).is_none());
    }

    #[derive(Default)]
    struct Collected {
        spans: std::sync::Mutex<Vec<trace::Span>>,
    }

    impl trace::Sink for Collected {
        fn send(&self, batch: &[trace::Span]) {
            self.spans
                .lock()
                .expect("the lock")
                .extend_from_slice(batch);
        }
    }

    /// The process tracer, installed once for this binary, since that is
    /// what a tracer is. Every test here looks for its own span rather
    /// than assuming it is the only one in the collection.
    fn sink() -> &'static std::sync::Arc<Collected> {
        static SINK: std::sync::OnceLock<std::sync::Arc<Collected>> = std::sync::OnceLock::new();
        SINK.get_or_init(|| {
            let sink = std::sync::Arc::new(Collected::default());
            trace::install_with(sink.clone(), std::time::Duration::from_millis(5));
            sink
        })
    }

    fn traced() -> Router {
        Router::new()
            .fallback(|| async { (StatusCode::OK, "ok").into_response() })
            .layer(axum::middleware::from_fn(measure))
    }

    async fn ask(url: &str, traceparent: Option<&str>) -> StatusCode {
        let mut req = Request::builder().uri(url);
        if let Some(header) = traceparent {
            req = req.header(trace::HEADER, header);
        }
        traced()
            .oneshot(req.body(Body::empty()).expect("a request"))
            .await
            .expect("an answer")
            .status()
    }

    /// The span for `path`, waited for, since the exporter thread sends
    /// on its own window.
    fn exported(path: &str) -> trace::Span {
        for _ in 0..200 {
            let spans = sink().spans.lock().expect("the lock");
            let mine = spans.iter().find(|span| {
                span.attrs
                    .iter()
                    .any(|(key, at)| *key == "url.path" && *at == trace::At::Text(path.to_string()))
            });
            if let Some(span) = mine {
                return span.clone();
            }
            drop(spans);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("no span for {path}");
    }

    #[tokio::test]
    async fn a_request_joins_the_trace_it_arrived_in() {
        sink();
        let status = ask(
            "/rest/v1/todos",
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let span = exported("/rest/v1/todos");
        assert_eq!(span.ids.trace_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(
            span.parent.map(|parent| parent.to_vec()),
            Some(vec![0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7]),
            "the caller's span is this one's parent"
        );
        // The name is grouped on, so it says the surface and not the
        // path, which carries a tenant ref and a row filter in it.
        assert_eq!(span.name, "GET /rest");
        assert_eq!(span.kind, trace::Kind::Server);
        assert!(
            span.attrs
                .contains(&("http.response.status_code", trace::At::Int(200)))
        );
    }

    #[tokio::test]
    async fn a_query_string_never_leaves_this_process() {
        // `?apikey=<jwt>` is a spelling this server accepts, so a span
        // that carried the query would mail credentials to a collector.
        sink();
        let status = ask("/auth/v1/user?apikey=header.payload.signature", None).await;
        assert_eq!(status, StatusCode::OK);
        let span = exported("/auth/v1/user");
        for (key, at) in &span.attrs {
            if let trace::At::Text(text) = at {
                assert!(!text.contains("apikey"), "{key} carried the query");
                assert!(!text.contains("signature"), "{key} carried the query");
            }
        }
        assert!(
            span.parent.is_none(),
            "nothing sent a header, so this is a root"
        );
    }

    #[tokio::test]
    async fn a_broken_traceparent_costs_the_caller_nothing() {
        sink();
        let status = ask("/storage/v1/object/pics/a.png", Some("not-a-traceparent")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "a header nobody can read is not worth refusing a request over"
        );
        let span = exported("/storage/v1/object/pics/a.png");
        assert!(span.parent.is_none(), "and the trace starts here instead");
    }

    #[tokio::test]
    async fn a_caller_that_said_not_to_record_is_believed() {
        sink();
        let status = ask(
            "/functions/v1/hello",
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        // Long enough for the exporter window to have passed twice.
        std::thread::sleep(std::time::Duration::from_millis(60));
        let spans = sink().spans.lock().expect("the lock");
        assert!(
            !spans.iter().any(|span| span.name == "GET /functions"),
            "the sampled flag was zero"
        );
    }
}
