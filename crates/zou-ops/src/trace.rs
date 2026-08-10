//! Spans, the trace context they carry, and an OTLP exporter.
//!
//! A counter says a request was slow. A trace says which part of it
//! was, and which caller it belonged to, which is the question a
//! counter cannot answer and the reason this exists at all.
//!
//! The context is W3C trace context, one `traceparent` header, because
//! it is what every client library, proxy and collector already speaks
//! and a trace that starts at this server is a trace that lost its
//! first half. An incoming header makes this server's span a child of
//! whatever sent the request; no header, and a new trace starts here.
//!
//! Export is OTLP over http with a json body, which is a documented
//! encoding of the same protocol the grpc exporter speaks, and which
//! costs a post and a serialisation rather than a protobuf toolchain
//! and a second async runtime. Spans leave on a thread of their own
//! through a bounded queue: a request never waits for a collector, and
//! a collector that has stopped reading loses spans rather than
//! slowing the server down, which is counted so that it is visible.
//!
//! Off unless `ZOU_OTLP_ENDPOINT` names a collector. With nothing set
//! there is no thread, no queue and no span, and the request path pays
//! one load of a pointer that is null.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::metrics::registry;

/// The header the whole thing hangs on.
pub const HEADER: &str = "traceparent";

/// How many spans may be waiting to be sent. A collector that has
/// stopped reading is a collector this server is not going to wait for,
/// so the queue is small enough that the memory is bounded and the
/// overflow is counted rather than swallowed.
const QUEUE: usize = 1024;

/// How long a batch waits for company before it is sent. A trace that
/// arrives five seconds late is still a trace, and one post per span
/// would be one connection per request.
const WINDOW: Duration = Duration::from_secs(5);

/// The most spans in one post, so a burst does not turn into a body a
/// collector refuses.
const BATCH: usize = 512;

/// A trace id and a span id, plus whether anyone asked for this one to
/// be recorded.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Ids {
    pub trace: [u8; 16],
    pub span: [u8; 8],
    pub sampled: bool,
}

impl Ids {
    /// A brand new trace, for a request that arrived without one.
    pub fn root(sampled: bool) -> Ids {
        let mut trace = [0u8; 16];
        let mut span = [0u8; 8];
        getrandom::fill(&mut trace).expect("the os rng never fails");
        getrandom::fill(&mut span).expect("the os rng never fails");
        Ids {
            trace,
            span,
            sampled,
        }
    }

    /// The same trace, one level down. What a server does with the
    /// header it was given: the incoming span becomes the parent and
    /// this server's work gets an id of its own.
    pub fn child(&self) -> Ids {
        let mut span = [0u8; 8];
        getrandom::fill(&mut span).expect("the os rng never fails");
        Ids {
            trace: self.trace,
            span,
            sampled: self.sampled,
        }
    }

    /// Read a `traceparent`. None for anything malformed, which is
    /// treated as no header at all rather than as a reason to refuse
    /// the request: a broken tracing header is not the caller's fault
    /// and is certainly not worth a 400.
    ///
    /// All zero ids are refused because the spec says they are invalid,
    /// and a client that sends them is a client whose instrumentation
    /// is not initialised.
    pub fn parse(header: &str) -> Option<Ids> {
        let mut parts = header.trim().split('-');
        let version = parts.next()?;
        let trace = parts.next()?;
        let span = parts.next()?;
        let flags = parts.next()?;
        // Version ff is invalid, and a version this does not know is
        // still readable, since the spec fixes the first four fields.
        if version.len() != 2 || version == "ff" || u8::from_str_radix(version, 16).is_err() {
            return None;
        }
        let trace: [u8; 16] = unhex(trace)?.try_into().ok()?;
        let span: [u8; 8] = unhex(span)?.try_into().ok()?;
        if trace == [0u8; 16] || span == [0u8; 8] {
            return None;
        }
        let flags = u8::from_str_radix(flags.get(..2)?, 16).ok()?;
        Some(Ids {
            trace,
            span,
            sampled: flags & 1 == 1,
        })
    }

    /// The header to send onward, which is what a downstream service
    /// needs to join this trace.
    pub fn header(&self) -> String {
        format!(
            "00-{}-{}-{:02x}",
            hex(&self.trace),
            hex(&self.span),
            u8::from(self.sampled)
        )
    }

    pub fn trace_hex(&self) -> String {
        hex(&self.trace)
    }

    pub fn span_hex(&self) -> String {
        hex(&self.span)
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) || text.is_empty() {
        return None;
    }
    (0..text.len() / 2)
        .map(|at| u8::from_str_radix(text.get(at * 2..at * 2 + 2)?, 16).ok())
        .collect()
}

/// What the span was doing, with the numbers OTLP gives them.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Internal,
    Server,
    Client,
}

impl Kind {
    fn code(self) -> u8 {
        match self {
            Kind::Internal => 1,
            Kind::Server => 2,
            Kind::Client => 3,
        }
    }
}

/// An attribute value, in the two shapes anything here needs.
#[derive(Clone, Debug, PartialEq)]
pub enum At {
    Text(String),
    Int(i64),
}

/// One unit of work, from [`Span::start`] to [`Span::end`].
#[derive(Clone, Debug)]
pub struct Span {
    pub name: String,
    pub kind: Kind,
    pub ids: Ids,
    pub parent: Option<[u8; 8]>,
    pub start: u64,
    pub end: u64,
    pub attrs: Vec<(&'static str, At)>,
    /// Set and the span is an error, which is what a collector colours
    /// red and what a trace is usually opened to find.
    pub failed: Option<String>,
}

impl Span {
    pub fn start(name: impl Into<String>, kind: Kind, ids: Ids, parent: Option<[u8; 8]>) -> Span {
        Span {
            name: name.into(),
            kind,
            ids,
            parent,
            start: now_nanos(),
            end: 0,
            attrs: Vec::new(),
            failed: None,
        }
    }

    pub fn text(&mut self, key: &'static str, value: impl Into<String>) -> &mut Span {
        self.attrs.push((key, At::Text(value.into())));
        self
    }

    pub fn int(&mut self, key: &'static str, value: i64) -> &mut Span {
        self.attrs.push((key, At::Int(value)));
        self
    }

    pub fn failed(&mut self, why: impl Into<String>) -> &mut Span {
        self.failed = Some(why.into());
        self
    }

    /// Stamp the end and hand the span over. A span that is never ended
    /// is never recorded, which is the behaviour that matters when a
    /// path returns early.
    pub fn end(mut self) -> Span {
        self.end = now_nanos();
        self
    }
}

fn now_nanos() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    )
    .unwrap_or(u64::MAX)
}

/// Where finished spans go. A trait, so the batching and the shape of
/// the payload can be proved without a collector listening, the same
/// way the attach manager's policy is proved without a postmaster.
pub trait Sink: Send + Sync {
    fn send(&self, batch: &[Span]);
}

/// The exporter, and the thread behind it.
pub struct Tracer {
    spans: SyncSender<Span>,
    dropped: AtomicU64,
}

impl Tracer {
    pub fn new(sink: Arc<dyn Sink>) -> Tracer {
        Tracer::with_window(sink, WINDOW)
    }

    /// The same with a batch window of your own, which is what a test
    /// wants and what a collector on the same host might.
    pub fn with_window(sink: Arc<dyn Sink>, window: Duration) -> Tracer {
        let (spans, waiting) = sync_channel(QUEUE);
        std::thread::Builder::new()
            .name("zou-traces".to_string())
            .spawn(move || {
                let mut batch: Vec<Span> = Vec::new();
                loop {
                    match waiting.recv_timeout(window) {
                        Ok(span) => {
                            batch.push(span);
                            if batch.len() >= BATCH {
                                sink.send(&batch);
                                batch.clear();
                            }
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            if !batch.is_empty() {
                                sink.send(&batch);
                                batch.clear();
                            }
                        }
                        // Every sender is gone, so the process is on its
                        // way out. Send what is in hand and stop.
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                            if !batch.is_empty() {
                                sink.send(&batch);
                            }
                            return;
                        }
                    }
                }
            })
            .expect("a thread for traces");
        Tracer {
            spans,
            dropped: AtomicU64::new(0),
        }
    }

    /// Queue a finished span, or drop it if the queue is full.
    ///
    /// Dropping is the right failure here: a request is not going to
    /// wait on a collector, and losing telemetry is cheaper than losing
    /// the thing the telemetry is about. It is counted, so a graph
    /// shows the gap rather than hiding it.
    pub fn record(&self, span: Span) {
        match self.spans.try_send(span) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                registry()
                    .counter(
                        "zou_trace_spans_dropped_total",
                        "spans the exporter could not keep up with",
                        &[],
                    )
                    .inc();
            }
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// The process tracer, or None when nothing is collecting.
///
/// A `None` here is the whole off switch: no thread, no queue, and
/// every call site skips building a span at all rather than building
/// one and throwing it away.
pub fn tracer() -> Option<&'static Tracer> {
    PROCESS.get().and_then(Option::as_ref)
}

static PROCESS: OnceLock<Option<Tracer>> = OnceLock::new();

/// Turn tracing on with a sink of your own. The first call wins, and
/// later ones are ignored rather than swapping the exporter under
/// spans that are already in flight.
pub fn install(sink: Arc<dyn Sink>) -> Option<&'static Tracer> {
    install_with(sink, WINDOW)
}

/// The same with a batch window of your own, which is what a collector
/// on the same host can afford and what a test cannot do without.
pub fn install_with(sink: Arc<dyn Sink>, window: Duration) -> Option<&'static Tracer> {
    let _ = PROCESS.set(Some(Tracer::with_window(sink, window)));
    tracer()
}

/// Turn tracing on if `ZOU_OTLP_ENDPOINT` names a collector, using the
/// OTLP http exporter. `service` is what this process calls itself in
/// every span it sends.
///
/// The endpoint is a base url, `http://localhost:4318`, and the traces
/// path is appended, which is what every collector documents and what
/// the OTLP environment variables mean elsewhere.
pub fn from_env(service: &str) -> Option<&'static Tracer> {
    let endpoint = std::env::var("ZOU_OTLP_ENDPOINT")
        .ok()
        .filter(|e| !e.is_empty())?;
    log::info!("traces go to {endpoint}");
    install(Arc::new(Otlp::new(&endpoint, service)))
}

/// The trace context of the request being served on this task, if
/// there is one.
pub fn current() -> Option<Ids> {
    CURRENT.try_with(|ids| *ids).ok()
}

/// Run `work` with `ids` as the current trace context, so that anything
/// under it can join the trace without being handed a parameter.
pub async fn with<F: Future>(ids: Ids, work: F) -> F::Output {
    CURRENT.scope(ids, work).await
}

tokio::task_local! {
    static CURRENT: Ids;
}

/// The OTLP http exporter.
pub struct Otlp {
    url: String,
    service: String,
    agent: ureq::Agent,
}

impl Otlp {
    pub fn new(endpoint: &str, service: &str) -> Otlp {
        Otlp {
            url: format!("{}/v1/traces", endpoint.trim_end_matches('/')),
            service: service.to_string(),
            // Short, because a collector that is not answering is not
            // worth a thread waiting on it, and this thread is the one
            // that would be draining the queue.
            agent: ureq::Agent::config_builder()
                .http_status_as_error(false)
                .timeout_global(Some(Duration::from_secs(5)))
                .build()
                .into(),
        }
    }
}

impl Sink for Otlp {
    fn send(&self, batch: &[Span]) {
        let body = payload(&self.service, batch).to_string();
        match self
            .agent
            .post(&self.url)
            .header("content-type", "application/json")
            .send(&body)
        {
            Ok(answer) if answer.status().as_u16() < 300 => {}
            Ok(answer) => log::warn!("{} answered {}", self.url, answer.status()),
            Err(e) => log::warn!("sending spans to {}: {e}", self.url),
        }
    }
}

/// A batch as an OTLP json body.
///
/// Ids are hex strings and timestamps are nanosecond strings, which is
/// what the json mapping says: json numbers cannot hold a 64 bit
/// integer without losing the bottom of it, and a timestamp that has
/// lost its bottom bits is a span of the wrong length.
pub fn payload(service: &str, batch: &[Span]) -> serde_json::Value {
    let spans: Vec<serde_json::Value> = batch
        .iter()
        .map(|span| {
            let mut out = serde_json::json!({
                "traceId": span.ids.trace_hex(),
                "spanId": span.ids.span_hex(),
                "name": span.name,
                "kind": span.kind.code(),
                "startTimeUnixNano": span.start.to_string(),
                "endTimeUnixNano": span.end.to_string(),
                "attributes": span.attrs.iter().map(attribute).collect::<Vec<_>>(),
            });
            if let Some(parent) = span.parent {
                out["parentSpanId"] = hex(&parent).into();
            }
            out["status"] = match &span.failed {
                // 2 is error and 1 is ok, and unset is what a span that
                // simply finished should carry, so only a failure says
                // anything at all.
                Some(why) => serde_json::json!({"code": 2, "message": why}),
                None => serde_json::json!({"code": 0}),
            };
            out
        })
        .collect();
    serde_json::json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [attribute(&("service.name", At::Text(service.to_string())))],
            },
            "scopeSpans": [{
                "scope": {"name": "zou"},
                "spans": spans,
            }],
        }],
    })
}

fn attribute(pair: &(&'static str, At)) -> serde_json::Value {
    let (key, value) = pair;
    let value = match value {
        At::Text(text) => serde_json::json!({"stringValue": text}),
        At::Int(n) => serde_json::json!({"intValue": n.to_string()}),
    };
    serde_json::json!({"key": key, "value": value})
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Collected {
        spans: Mutex<Vec<Span>>,
    }

    impl Sink for Collected {
        fn send(&self, batch: &[Span]) {
            self.spans
                .lock()
                .expect("the lock")
                .extend_from_slice(batch);
        }
    }

    fn ids() -> Ids {
        Ids {
            trace: [1u8; 16],
            span: [2u8; 8],
            sampled: true,
        }
    }

    #[test]
    fn a_traceparent_round_trips() {
        let header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let parsed = Ids::parse(header).expect("it parses");
        assert!(parsed.sampled);
        assert_eq!(parsed.header(), header);
    }

    #[test]
    fn an_unsampled_flag_survives() {
        let parsed = Ids::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
            .expect("it parses");
        assert!(!parsed.sampled, "the caller said not to record this one");
    }

    #[test]
    fn a_header_this_does_not_understand_is_no_header_at_all() {
        // None of these is worth refusing a request over, and a broken
        // tracing header is never the caller's fault in a way that
        // should cost them their answer.
        for header in [
            "",
            "00",
            "garbage",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "00-4bf92f3577b34da6-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e473g-00f067aa0ba902b7-01",
        ] {
            assert!(Ids::parse(header).is_none(), "{header:?}");
        }
    }

    #[test]
    fn a_newer_version_is_still_read() {
        // The spec fixes the first four fields forever, so a version
        // this build has never heard of is still a trace to join.
        let parsed = Ids::parse("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra")
            .expect("it parses");
        assert_eq!(parsed.trace_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
    }

    #[test]
    fn a_child_keeps_the_trace_and_takes_a_new_span() {
        let parent = ids();
        let child = parent.child();
        assert_eq!(child.trace, parent.trace);
        assert_ne!(child.span, parent.span);
        assert!(child.sampled);
    }

    #[test]
    fn two_roots_are_two_traces() {
        assert_ne!(Ids::root(true).trace, Ids::root(true).trace);
    }

    #[test]
    fn a_span_is_otlp_json() {
        let mut span = Span::start("GET /rest/v1", Kind::Server, ids(), Some([9u8; 8]));
        span.text("http.request.method", "GET")
            .int("http.response.status_code", 200);
        let span = span.end();
        let body = payload("zou", &[span]);
        let one = &body["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(one["traceId"], "01010101010101010101010101010101");
        assert_eq!(one["spanId"], "0202020202020202");
        assert_eq!(one["parentSpanId"], "0909090909090909");
        assert_eq!(one["kind"], 2);
        assert_eq!(one["name"], "GET /rest/v1");
        // Nanosecond timestamps as strings, because a json number
        // cannot hold one without losing the bottom of it.
        assert!(one["startTimeUnixNano"].is_string());
        assert!(
            one["endTimeUnixNano"].as_str().unwrap() >= one["startTimeUnixNano"].as_str().unwrap()
        );
        assert_eq!(one["attributes"][0]["key"], "http.request.method");
        assert_eq!(one["attributes"][0]["value"]["stringValue"], "GET");
        assert_eq!(one["attributes"][1]["value"]["intValue"], "200");
        assert_eq!(
            body["resourceSpans"][0]["resource"]["attributes"][0]["value"]["stringValue"],
            "zou"
        );
    }

    #[test]
    fn a_failure_is_a_status_a_collector_colours_red() {
        let mut span = Span::start("attach", Kind::Internal, ids(), None);
        span.failed("the lease was held elsewhere");
        let body = payload("zou", &[span.end()]);
        let one = &body["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(one["status"]["code"], 2);
        assert_eq!(one["status"]["message"], "the lease was held elsewhere");
        assert!(one.get("parentSpanId").is_none(), "a root has no parent");
    }

    #[test]
    fn a_recorded_span_reaches_the_sink() {
        let sink = Arc::new(Collected::default());
        let tracer = Tracer::with_window(sink.clone(), Duration::from_millis(5));
        tracer.record(Span::start("one", Kind::Server, ids(), None).end());
        for _ in 0..200 {
            if sink.spans.lock().expect("the lock").len() == 1 {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("the span never left");
    }

    #[tokio::test]
    async fn the_current_context_is_the_one_this_task_was_given() {
        assert!(current().is_none(), "nothing outside a request");
        let mine = ids();
        with(mine, async {
            assert_eq!(current(), Some(mine));
            // A task spawned under it does not inherit, which is the
            // one sharp edge worth pinning: anything that wants the
            // context across a spawn has to carry it.
            let handle = tokio::spawn(async { current() });
            assert_eq!(handle.await.expect("it ran"), None);
        })
        .await;
        assert!(current().is_none(), "and nothing after");
    }
}
