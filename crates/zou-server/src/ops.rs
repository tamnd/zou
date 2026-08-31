//! The port an operator points a scraper at.
//!
//! `/healthz` and `/metrics` for a scraper, and `/_zou` for a person,
//! on a listener of their own. Their own listener and not the front
//! door's, for two reasons that both come from what the front door is.
//! Under path routing the first segment of every url is a tenant ref,
//! with nothing reserved, so a `/metrics` route there would quietly take
//! a name a project could have had. And a scrape is the operational
//! state of the node, which is not something to hand to whoever holds an
//! anon key for one of the projects on it. A second listener is the
//! cheap answer to both, and it is the one every deployment already
//! knows how to firewall.
//!
//! The page is here for the second of those reasons rather than the
//! first. Which projects a node has up is a fact about the node, and the
//! only credential the front door could check it against is one
//! project's service key, which has no business answering questions
//! about the other projects on the box. So the list of tenants is on the
//! operator's port, where the rest of what an operator asks already is.
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

use std::sync::Arc;
use std::time::{Duration, Instant};

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
///
/// `attached` is the node's attached set, when the process has one. It
/// is what `/_zou` lists, and the reason the list is on this port rather
/// than on the front door is the same reason `/metrics` is: which
/// projects a node is serving is a fact about the node, and the only key
/// the front door could check it against is one project's service key,
/// which has no business answering questions about the others. None is a
/// process with one project and no set to list, which is what `zou dev`
/// is.
pub fn ops(version: &'static str, attached: Option<Arc<crate::attach::Attached>>) -> Router {
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
        .route("/_zou", get(tenants_page))
        .route("/_zou/", get(tenants_page))
        .route("/_zou/api/tenants", get(tenants))
        .fallback(missing)
        .with_state(attached)
}

/// The tenants page, which is the whole web admin this port serves.
///
/// Compiled in, no data in it, the same bytes on every node, for the
/// reasons the project console is the same: an operator who can reach
/// the port can read the page, on a box with no internet and nothing
/// installed beside the binary.
const TENANTS: &str = include_str!("tenants.html");

async fn tenants_page() -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        TENANTS,
    )
        .into_response()
}

/// GET /_zou/api/tenants, which projects this node has up.
///
/// Up, and not every project on the store. A node attaches a project
/// when something asks for it and lets it go when nothing has for a
/// while, so this is the working set and not an inventory. The
/// inventory lives in the store and reading it is a listing, which is
/// not a thing to do behind a page that refreshes.
///
/// Both budgets are in the answer beside the list, because a list of
/// nine hundred tenants means one thing under a ceiling of a thousand
/// and another under a ceiling of nine hundred.
async fn tenants(
    axum::extract::State(attached): axum::extract::State<Option<Arc<crate::attach::Attached>>>,
) -> Response {
    let Some(attached) = attached else {
        // Not an error and not an empty list either. A process with one
        // project has no set to list and saying so is the difference
        // between a node with nothing up and a node that does not work
        // this way at all.
        return json(
            StatusCode::OK,
            serde_json::json!({ "tenants": [], "fleet": false }),
        );
    };
    let listed: Vec<serde_json::Value> = attached
        .listing()
        .await
        .into_iter()
        .map(|one| {
            serde_json::json!({
                "ref": one.tenant_ref,
                "up_for": one.up_for.as_secs(),
                "idle_for": one.idle_for.as_secs(),
                "in_use": one.in_use,
                "ready": one.ready,
            })
        })
        .collect();
    json(
        StatusCode::OK,
        serde_json::json!({
            "tenants": listed,
            "fleet": true,
            "ceiling": attached.ceiling(),
            "idle_after": attached.idle_after().as_secs(),
        }),
    )
}

fn json(status: StatusCode, body: serde_json::Value) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
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
    for step in &snapshot.commit {
        // The one number a person watching a write heavy node wants is
        // durable, and the other six are there so that a durable that
        // went up says which of them took it there.
        let seconds = reg.histogram(
            "zou_commit_step_seconds",
            "how long each step of the commit path took",
            STORE_SECONDS,
            &[("step", step.step)],
        );
        for (at, count) in step.buckets.iter().enumerate() {
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

/// The realtime family's own bucket edges, seconds, from a tenth of a
/// millisecond to five minutes.
///
/// The shared latency edges are a request path's spread, and a request
/// path that took five minutes has already been given up on. A change
/// on a node holding a hundred thousand sockets has not: a measured run
/// put the p99 of what a client waited at forty six seconds and the
/// worst at three hundred, which the shared edges cannot tell apart
/// because they stop at ten and everything past it is one bucket.
///
/// The other half of it is resolution where the answers are. The shared
/// edges step from a quarter of a second to a half to a whole, so a p99
/// anywhere in that stretch reads as exactly 250 or 500 milliseconds,
/// which is a bucket edge being reported as a measurement. These are
/// five edges to the decade between a millisecond and ten seconds, so
/// the worst a quantile is wrong by is the ratio between neighbours,
/// which is at most a two thirds overstatement rather than a doubling.
///
/// Twenty seven edges is more than this tree spends anywhere else, and
/// on the stage histogram it is four times that in series, one set to a
/// stage. That is the price of a fan out p99 that can be published, and
/// it is paid by the three families that need one rather than by every
/// histogram in the process.
const FANOUT_SECONDS: &[f64] = &[
    0.0001, 0.00025, 0.0005, 0.001, 0.0015, 0.0025, 0.004, 0.006, 0.01, 0.015, 0.025, 0.04, 0.06,
    0.1, 0.15, 0.25, 0.4, 0.6, 1.0, 1.5, 2.5, 4.0, 6.0, 10.0, 30.0, 100.0, 300.0,
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
            FANOUT_SECONDS,
            &[],
        )
        .observe(micros.saturating_sub(commit_ts).max(0) as f64 / 1_000_000.0);
    registry()
        .histogram(
            "zou_realtime_change_seconds",
            "how long a change took inside this server, from the tap reading it",
            FANOUT_SECONDS,
            &[],
        )
        .since(read);
}

/// Where a change spent its time, in the five parts it passes through.
///
/// `zou_realtime_change_seconds` says how long a change took inside
/// this server, which is the number to look at first and the one that
/// says nothing at all about what to do next. A change that took forty
/// milliseconds took it in one of five quite different places, and they
/// are fixed by five quite different things.
///
/// The tap is the round trip to postgres for the next batch, so it is
/// the database and the network between here and it. The decode is
/// turning those messages into changes, which is this process's own
/// work on whatever postgres sent. The selection is asking who wanted
/// each change and what each of them may see of it, which is a matcher,
/// a catalog and a policy check, and it is the part that grows with the
/// subscribers on a table. The sending is the reader handing each
/// finished payload to a queue, which grows with the sockets owed a
/// row. The socket is what happens after that, which is one task
/// waiting its turn and writing a frame, and it grows with how many
/// tasks this node is running rather than with anything about the
/// change.
///
/// The first four are one observation apiece per batch. Per change they
/// would not be, because the tap is one poll for up to a thousand
/// messages, and the count would be the changes rather than the polls.
/// The socket stage is per delivery, because it is the only one that
/// happens once per socket rather than once for all of them, and that
/// difference in what is being counted is the point of it.
///
/// The four batch stages add up to a cycle of the reader, and the
/// largest share is the useful reading: the tap says the database is
/// the limit, the selection says the policies are, the sending says the
/// queues are. None of the four waits on any other, so the tap can be
/// run alongside the selection and the sending and the cycle stops
/// being a sum. That was built and measured, and it shortened the cycle
/// by a third while the median a client waited grew by half, so the
/// reader still runs them one after the other and the stages still add
/// up.
///
/// The tap is only counted on a poll that came back with something. An
/// idle reader asks every hundred milliseconds and is told there is
/// nothing, and averaging those in would say the tap is fast when what
/// it is is unused.
pub fn change_stage(stage: &'static str, took: Duration) {
    registry()
        .histogram(
            "zou_realtime_stage_seconds",
            "where one batch of database changes spent its time",
            FANOUT_SECONDS,
            &[("stage", stage)],
        )
        .observe(took.as_secs_f64());
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

/// A socket this node closed because carrying on with it would have
/// meant a client missing messages and not knowing it.
///
/// Three reasons and they are worth telling apart. `lagged` is a socket
/// that fell further behind than the topic's backlog holds, which says
/// the client stopped reading or the node is fanning out faster than it
/// can write. `gap` is the database change feed having lost its place,
/// which is the reader's health rather than the socket's. `reader` is
/// the change reader letting a subscriber go, which it does to one that
/// stopped consuming.
///
/// This is the count an operator watches after a fan out change: the
/// sockets gauge going down says people left, and this says the node
/// sent them away.
pub fn socket_dropped(reason: &'static str) {
    registry()
        .counter(
            "zou_realtime_sockets_dropped_total",
            "realtime sockets this node closed rather than carry on with a client that would be missing messages",
            &[("reason", reason)],
        )
        .inc();
}

fn sockets() -> zou_ops::Gauge {
    registry().gauge(
        "zou_realtime_sockets",
        "realtime sockets connected to this node right now",
        &[],
    )
}

/// The socket tiers this node is holding for projects it does not
/// write, one per project rather than per socket: a tier is one hub and
/// one link, and the link is a second one of these worth of work on the
/// holder, which is the node with the write path on it.
///
/// It goes down as well as up. A gauge that only ever went up would say
/// a node was holding a link for every project it had ever seen a
/// socket for, which is what it did until an empty tier was given a
/// life (#443), and it is the reading that says whether the sweep is
/// keeping up on a node with a lot of projects moving through it.
pub fn tier_built() {
    tiers().inc();
}

pub fn tier_dropped() {
    tiers().dec();
}

fn tiers() -> zou_ops::Gauge {
    registry().gauge(
        "zou_realtime_socket_tiers",
        "projects this node serves sockets for and does not write",
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
///
/// No attached set, because this is the door a one project process opens
/// and a one project process has none: the tenants page says so rather
/// than showing an empty node.
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
        axum::serve(listener, ops(version, None).into_make_service())
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
        let (status, _, body) = call(&ops("0.0.0-test", None), "/healthz").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "ok\n");
    }

    #[tokio::test]
    async fn the_tenants_page_is_markup_and_holds_no_node_in_it() {
        let (status, kind, body) = call(&ops("0.0.0-test", None), "/_zou").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(kind, "text/html; charset=utf-8");
        assert!(body.starts_with("<!doctype html>"), "{body}");
        // The same bytes on every node. Everything it shows arrives
        // through the endpoint below and nothing is baked into it.
        assert!(body.contains("/_zou/api/tenants"), "{body}");
    }

    /// A process with one project is not a node with nothing up, and the
    /// answer has to say which of the two it is.
    #[tokio::test]
    async fn a_process_with_no_attached_set_says_so_rather_than_listing_none() {
        let (status, kind, body) = call(&ops("0.0.0-test", None), "/_zou/api/tenants").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(kind, "application/json");
        let answer: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(answer["fleet"], false);
        assert_eq!(answer["tenants"].as_array().expect("a list").len(), 0);
        assert!(answer["ceiling"].is_null(), "no set, no budget: {body}");
    }

    /// A node with projects on it, listed the way an operator reads it.
    #[tokio::test]
    async fn a_node_lists_the_projects_it_has_up_with_the_budgets_they_are_under() {
        /// Attaches nothing. The list is about the map, not about what
        /// is behind it, so a backend that starts no postmaster is the
        /// right one to ask the question through.
        struct Nothing;
        impl crate::attach::Backend for Nothing {
            fn up(&self, entry: &zou_store::registry::Tenant) -> Result<crate::Config, String> {
                Ok(crate::Config {
                    jwt_secret: entry.jwt_secret.as_bytes().to_vec(),
                    ..crate::Config::default()
                })
            }
            fn down(&self, _tenant_ref: &str) {}
        }
        let attached = Arc::new(crate::attach::Attached::new(Arc::new(Nothing)));
        for name in ["beta", "alpha"] {
            let entry = zou_store::registry::Tenant::new(
                name,
                "super-secret-jwt-token-with-at-least-32-characters-long",
                1,
            );
            let _ = attached.router(&entry).await.expect("attach");
        }
        let router = ops("0.0.0-test", Some(Arc::clone(&attached)));
        let (status, _, body) = call(&router, "/_zou/api/tenants").await;
        assert_eq!(status, StatusCode::OK);
        let answer: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(answer["fleet"], true);
        assert_eq!(answer["ceiling"], crate::attach::MAX_ATTACHED);
        assert_eq!(answer["idle_after"], crate::attach::IDLE.as_secs());
        let listed = answer["tenants"].as_array().expect("a list");
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0]["ref"], "alpha", "by name: {body}");
        assert_eq!(listed[1]["ref"], "beta");
        // Nothing is holding either of them: the request that attached
        // them let go on its way out.
        assert_eq!(listed[0]["in_use"], 0);
        assert_eq!(listed[0]["ready"], true);
    }

    #[tokio::test]
    async fn metrics_are_the_exposition_format() {
        // The process registry is shared with every other test in this
        // binary, so what is asserted here is which families a scrape
        // carries, not what they are counting at the moment.
        attached(3);
        lookup(true);
        let (status, kind, body) = call(&ops("0.0.0-test", None), "/metrics").await;
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
        let (_, _, body) = call(&ops("0.0.0-test", None), "/metrics").await;
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
        // And on the fan out edges rather than the shared ones. What
        // that buys is on both sides of the quarter second: an edge at
        // four tenths, where the shared set steps straight to a half
        // and reports every p99 in between as exactly one or the other,
        // and an edge at five minutes, which is where a node holding a
        // hundred thousand sockets has actually been measured.
        let bucket = |le: &str| {
            body.lines()
                .find_map(|line| {
                    line.strip_prefix(&format!(
                        "zou_realtime_commit_to_socket_seconds_bucket{{le=\"{le}\"}} "
                    ))
                })
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or_else(|| panic!("a bucket at {le}, {body}"))
        };
        assert_eq!(
            bucket("0.15"),
            1,
            "the clock an hour ahead is the zero, {body}"
        );
        assert_eq!(
            bucket("0.4"),
            2,
            "and the quarter second is under it, {body}"
        );
        assert_eq!(bucket("300"), 2, "{body}");
    }

    /// The five stages are one family with a label rather than five
    /// names, because the question they answer is which part of a
    /// change went where, and that is one question about one thing.
    #[tokio::test]
    async fn a_change_says_which_of_the_five_stages_it_spent_its_time_in() {
        change_stage("tap", Duration::from_millis(4));
        change_stage("decode", Duration::from_millis(2));
        change_stage("select", Duration::from_millis(30));
        change_stage("send", Duration::from_millis(6));
        change_stage("socket", Duration::from_millis(11));
        let (_, _, body) = call(&ops("0.0.0-test", None), "/metrics").await;
        assert!(
            body.contains("# TYPE zou_realtime_stage_seconds histogram\n"),
            "{body}"
        );
        let sum = |stage: &str| {
            body.lines()
                .find_map(|line| {
                    line.strip_prefix(&format!(
                        "zou_realtime_stage_seconds_sum{{stage=\"{stage}\"}} "
                    ))
                })
                .and_then(|n| n.parse::<f64>().ok())
                .expect("a sum")
        };
        assert!((0.004..0.02).contains(&sum("tap")), "{body}");
        assert!((0.002..0.02).contains(&sum("decode")), "{body}");
        assert!((0.030..0.05).contains(&sum("select")), "{body}");
        assert!((0.006..0.02).contains(&sum("send")), "{body}");
        assert!((0.011..0.03).contains(&sum("socket")), "{body}");
    }

    /// The two numbers a socket tier is sized on. A gauge that only
    /// ever went up would say a node was holding sockets that had gone
    /// hours ago, which is the reading somebody would buy a box on.
    #[tokio::test]
    async fn the_sockets_a_node_holds_go_down_again() {
        socket_joined();
        socket_joined();
        let (_, _, body) = call(&ops("0.0.0-test", None), "/metrics").await;
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
        let (_, _, body) = call(&ops("0.0.0-test", None), "/metrics").await;
        assert_eq!(held(&body), two - 2, "and both of them are out of it again");
        subscribers(7);
        let (_, _, body) = call(&ops("0.0.0-test", None), "/metrics").await;
        // The number is not asked for. This gauge is a process global
        // and the reader's own tests set it whenever one of them
        // registers a subscription, so a run that happens to interleave
        // reads back their number rather than this one. What is asked is
        // that it is exported, under the name a dashboard reads.
        assert!(
            body.lines().any(|line| line
                .strip_prefix("zou_realtime_subscribers ")
                .is_some_and(|n| n.trim().parse::<u64>().is_ok())),
            "{body}"
        );
    }

    #[tokio::test]
    async fn the_ops_port_has_nothing_else_on_it() {
        // Whatever else is asked for here is not a tenant's url. This
        // listener serves an operator and nobody else.
        let (status, _, _) = call(&ops("0.0.0-test", None), "/rest/v1/todos").await;
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
