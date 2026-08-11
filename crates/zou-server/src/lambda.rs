//! The same front door, behind AWS Lambda's runtime API.
//!
//! A function has no listener. What it has is a loop: ask the runtime
//! api for the next invocation, do the work, post the answer back, ask
//! again. So this is the http door with the socket taken out and that
//! loop put in its place, and everything under it, the routing, the
//! attach, the surfaces, is the same code a node runs.
//!
//! Two things about a function make the mode single tenant rather than
//! configurably so. One is that a function url is one url and there is
//! no hostname left to name a project with. The other is the freeze: an
//! environment that is done answering is suspended with whatever it was
//! holding, so a second attached project is a second postmaster frozen
//! mid transaction, and the honest number of databases to have up in
//! here is one.
//!
//! The project comes up during initialisation rather than on the first
//! invocation. Lambda gives a function a window before its first event
//! and does not bill the same way for it, and an attach in that window
//! is an attach nobody waited for.

use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, Response};
use base64ct::{Base64, Encoding};
use serde_json::{Value, json};
use tower::ServiceExt as _;

use crate::attach::Attached;
use crate::tenant::Registry;

/// The largest body this will take off an event or put back on an
/// answer. API Gateway stops at 10 MB of payload and Lambda at 6 MB of
/// response, so anything near this is already refused upstream and the
/// limit is here so that a malformed event cannot ask for a gigabyte.
const MAX_BODY: usize = 16 << 20;

/// One invocation: what to answer it with, and what to answer it on.
pub struct Invocation {
    /// `Lambda-Runtime-Aws-Request-Id`, which is what an answer is
    /// posted against.
    pub id: String,
    pub event: Value,
}

/// The runtime api, as the two calls a loop makes of it.
///
/// A trait because the loop is worth testing and a test cannot have a
/// Lambda around it. `failed` is separate from `answered` because they
/// are different urls to the runtime: an answer is what the caller
/// asked for, an error is what makes the invocation a failed one in the
/// function's own metrics.
pub trait Api: Send + Sync {
    /// Blocks until there is one, which on an idle function is until
    /// the environment is frozen and thawed again.
    fn next(&self) -> Result<Invocation, String>;
    fn answered(&self, id: &str, answer: &Value) -> Result<(), String>;
    fn failed(&self, id: &str, why: &str) -> Result<(), String>;
    /// Said once, when the function could not be initialised at all.
    /// The environment is torn down after it rather than asked for
    /// work.
    fn broken(&self, why: &str) -> Result<(), String>;
}

/// How long the runtime api gets to answer anything except the next
/// invocation, which has no timeout at all: waiting for an event is the
/// idle state of a function and can be hours.
const TIMEOUT: Duration = Duration::from_secs(30);

/// The real one, over the loopback address Lambda puts in the
/// environment.
pub struct Lambda {
    agent: ureq::Agent,
    waiting: ureq::Agent,
    api: String,
}

impl Lambda {
    /// The endpoint Lambda names in `AWS_LAMBDA_RUNTIME_API`, which is
    /// how a process finds out it is a function at all.
    pub fn from_env() -> Result<Lambda, String> {
        let api = std::env::var("AWS_LAMBDA_RUNTIME_API")
            .map_err(|_| "AWS_LAMBDA_RUNTIME_API is not set, this is not a lambda".to_string())?;
        Ok(Lambda::new(&api))
    }

    pub fn new(api: &str) -> Lambda {
        let build = |timeout: Option<Duration>| {
            ureq::Agent::config_builder()
                .http_status_as_error(false)
                .timeout_global(timeout)
                .build()
                .into()
        };
        Lambda {
            agent: build(Some(TIMEOUT)),
            waiting: build(None),
            api: api.to_string(),
        }
    }

    fn url(&self, rest: &str) -> String {
        format!("http://{}/2018-06-01/runtime/{rest}", self.api)
    }

    fn post(&self, url: String, body: &Value) -> Result<(), String> {
        self.agent
            .post(&url)
            .content_type("application/json")
            .send(body.to_string())
            .map(|_| ())
            .map_err(|e| format!("posting to {url}: {e}"))
    }
}

impl Api for Lambda {
    fn next(&self) -> Result<Invocation, String> {
        let url = self.url("invocation/next");
        let answer = self
            .waiting
            .get(&url)
            .call()
            .map_err(|e| format!("waiting for an invocation: {e}"))?;
        let id = answer
            .headers()
            .get("lambda-runtime-aws-request-id")
            .and_then(|v| v.to_str().ok())
            .ok_or("an invocation with no request id")?
            .to_string();
        let body = answer
            .into_body()
            .with_config()
            .limit(MAX_BODY as u64)
            .read_to_vec()
            .map_err(|e| format!("reading invocation {id}: {e}"))?;
        let event = serde_json::from_slice(&body)
            .map_err(|e| format!("invocation {id} is not json: {e}"))?;
        Ok(Invocation { id, event })
    }

    fn answered(&self, id: &str, answer: &Value) -> Result<(), String> {
        self.post(self.url(&format!("invocation/{id}/response")), answer)
    }

    fn failed(&self, id: &str, why: &str) -> Result<(), String> {
        self.post(self.url(&format!("invocation/{id}/error")), &fault(why))
    }

    fn broken(&self, why: &str) -> Result<(), String> {
        self.post(self.url("init/error"), &fault(why))
    }
}

/// What the runtime api is told about a failure. The shape is theirs,
/// and the type is what shows up in the function's logs and metrics.
fn fault(why: &str) -> Value {
    json!({"errorMessage": why, "errorType": "ZouError"})
}

/// Serve one project as a function, until the environment goes away.
///
/// The attach happens first and a failure to attach is an
/// initialisation failure rather than a first invocation that answers
/// 503, because Lambda tears an environment down over the first and
/// keeps it over the second: a function whose store is misconfigured
/// should not stay up to say so ten thousand times.
pub fn serve_blocking(
    api: &dyn Api,
    tenant_ref: &str,
    registry: Arc<Registry>,
    attached: Arc<Attached>,
) -> Result<(), String> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;
    if let Err(e) = rt.block_on(crate::gateway::preattach(tenant_ref, &registry, &attached)) {
        let why = format!("attaching {tenant_ref}: {e}");
        let _ = api.broken(&why);
        return Err(why);
    }
    let router = crate::gateway::only(tenant_ref.to_string(), registry, attached);
    loop {
        let invocation = api.next()?;
        let id = invocation.id;
        match request(&invocation.event) {
            Ok(req) => {
                let answered = rt.block_on(async {
                    let served = router
                        .clone()
                        .into_service::<Body>()
                        .oneshot(req)
                        .await
                        .unwrap_or_else(|never| match never {});
                    answer(served).await
                });
                match answered {
                    Ok(answered) => api.answered(&id, &answered)?,
                    Err(e) => api.failed(&id, &e)?,
                }
            }
            // An event this cannot read is the function's failure and
            // not the caller's, since whatever sent it was configured
            // by whoever deployed this.
            Err(e) => api.failed(&id, &e)?,
        }
    }
}

/// An http request out of an api gateway event.
///
/// Two payload formats, because both are in front of functions people
/// already run: 2.0, which is what a function url and an http api send,
/// and 1.0, which is what a rest api sends. They disagree about where
/// the method lives, how the query string is spelled and whether a
/// repeated header is a list, and about nothing else that matters here.
pub fn request(event: &Value) -> Result<Request<Body>, String> {
    let two = event.get("version").and_then(Value::as_str) == Some("2.0");
    let (method, path, query) = if two {
        let method = event
            .pointer("/requestContext/http/method")
            .and_then(Value::as_str)
            .ok_or("a 2.0 event with no method")?;
        let path = event
            .get("rawPath")
            .and_then(Value::as_str)
            .ok_or("a 2.0 event with no rawPath")?;
        let query = event
            .get("rawQueryString")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        (method, path, query)
    } else {
        let method = event
            .get("httpMethod")
            .and_then(Value::as_str)
            .ok_or("an event with neither version 2.0 nor httpMethod")?;
        let path = event
            .get("path")
            .and_then(Value::as_str)
            .ok_or("a 1.0 event with no path")?;
        (method, path, query_1_0(event))
    };
    let uri = if query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{query}")
    };
    let mut req = Request::builder().method(method).uri(&uri);
    if let Some(headers) = event.get("headers").and_then(Value::as_object) {
        for (name, value) in headers {
            // 1.0 nulls a header it has nothing for, and 2.0 has
            // already joined a repeated one with a comma.
            if let Some(value) = value.as_str() {
                req = req.header(name, value);
            }
        }
    }
    // 1.0 keeps the repeats here, and they are the truth when both are
    // present: set-cookie and accept both mean something different as
    // two headers than as one.
    if let Some(headers) = event.get("multiValueHeaders").and_then(Value::as_object) {
        for (name, values) in headers {
            let Some(values) = values.as_array() else {
                continue;
            };
            let mut first = true;
            for value in values.iter().filter_map(Value::as_str) {
                if first {
                    // Whatever the single valued map said about this
                    // header, the list is the whole of it.
                    if let Some(headers) = req.headers_mut() {
                        headers.remove(name);
                    }
                    first = false;
                }
                req = req.header(name, value);
            }
        }
    }
    // 2.0 takes the cookies out of the headers and hands them over as a
    // list, so putting them back is the only way the session that came
    // with the request survives the trip.
    if let Some(cookies) = event.get("cookies").and_then(Value::as_array) {
        let cookies: Vec<&str> = cookies.iter().filter_map(Value::as_str).collect();
        if !cookies.is_empty() {
            req = req.header("cookie", cookies.join("; "));
        }
    }
    req.body(Body::from(body(event)?))
        .map_err(|e| format!("building the request for {method} {uri}: {e}"))
}

/// The query string of a 1.0 event, which arrives already decoded and
/// has to be put back together. The multi valued map wins where both
/// are there, since it is the one that can say `?tag=a&tag=b`.
fn query_1_0(event: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(map) = event
        .get("multiValueQueryStringParameters")
        .and_then(Value::as_object)
    {
        for (name, values) in map {
            for value in values.as_array().into_iter().flatten() {
                if let Some(value) = value.as_str() {
                    parts.push(format!("{}={}", encode(name), encode(value)));
                }
            }
        }
        return parts.join("&");
    }
    if let Some(map) = event
        .get("queryStringParameters")
        .and_then(Value::as_object)
    {
        for (name, value) in map {
            if let Some(value) = value.as_str() {
                parts.push(format!("{}={}", encode(name), encode(value)));
            }
        }
    }
    parts.join("&")
}

/// Percent encode everything a query string does not define, which is
/// most of it. PostgREST filters are full of commas, dots, quotes and
/// parentheses, and a value that arrives decoded and goes back in raw
/// changes what the filter means.
fn encode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// The bytes of an event's body. Base64 when the sender says so, which
/// is how anything that is not text arrives.
fn body(event: &Value) -> Result<Vec<u8>, String> {
    let Some(body) = event.get("body").and_then(Value::as_str) else {
        return Ok(Vec::new());
    };
    if event.get("isBase64Encoded").and_then(Value::as_bool) == Some(true) {
        return Base64::decode_vec(body).map_err(|e| format!("the event body is not base64: {e}"));
    }
    Ok(body.as_bytes().to_vec())
}

/// The json an api gateway wants back.
///
/// One shape for both payload formats. 2.0 accepts `multiValueHeaders`
/// and 1.0 accepts `cookies` being absent, so a response carrying both
/// spellings of a repeated header is understood by either, and the
/// alternative is a translation that has to be told which gateway sent
/// the request it is answering.
pub async fn answer(res: Response<Body>) -> Result<Value, String> {
    let status = res.status().as_u16();
    let (parts, body) = res.into_parts();
    let body = to_bytes(body, MAX_BODY)
        .await
        .map_err(|e| format!("reading the answer: {e}"))?;
    let mut headers = serde_json::Map::new();
    let mut multi: serde_json::Map<String, Value> = serde_json::Map::new();
    let mut cookies = Vec::new();
    for (name, value) in parts.headers.iter() {
        let value = String::from_utf8_lossy(value.as_bytes()).to_string();
        if name.as_str() == "set-cookie" {
            cookies.push(Value::String(value.clone()));
        }
        multi
            .entry(name.as_str().to_string())
            .or_insert_with(|| Value::Array(Vec::new()))
            .as_array_mut()
            .expect("an array")
            .push(Value::String(value.clone()));
        headers.insert(name.as_str().to_string(), Value::String(value));
    }
    // Text as text, so a caller reading a log or a gateway applying a
    // transform sees what was sent. Base64 for the rest, which is
    // images, tus offsets in a HEAD, and anything the storage door
    // hands back whole.
    let (body, base64) = match String::from_utf8(body.to_vec()) {
        Ok(text) => (text, false),
        Err(_) => (Base64::encode_string(&body), true),
    };
    Ok(json!({
        "statusCode": status,
        "headers": Value::Object(headers),
        "multiValueHeaders": Value::Object(multi),
        "cookies": Value::Array(cookies),
        "body": body,
        "isBase64Encoded": base64,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use std::sync::Mutex;

    fn v2(method: &str, path: &str, query: &str) -> Value {
        json!({
            "version": "2.0",
            "rawPath": path,
            "rawQueryString": query,
            "headers": {"host": "abc.lambda-url.eu-west-1.on.aws", "apikey": "an-anon-key"},
            "requestContext": {"http": {"method": method, "path": path}},
        })
    }

    async fn read(req: Request<Body>) -> (String, String, Vec<(String, String)>, String) {
        let (parts, body) = req.into_parts();
        let body = to_bytes(body, MAX_BODY).await.expect("a body");
        (
            parts.method.to_string(),
            parts.uri.to_string(),
            parts
                .headers
                .iter()
                .map(|(n, v)| {
                    (
                        n.as_str().to_string(),
                        String::from_utf8_lossy(v.as_bytes()).to_string(),
                    )
                })
                .collect(),
            String::from_utf8_lossy(&body).to_string(),
        )
    }

    #[tokio::test]
    async fn a_function_url_event_is_the_request_it_describes() {
        let event = v2("GET", "/rest/v1/todos", "select=id,title&done=eq.true");
        let (method, uri, headers, body) = read(request(&event).expect("a request")).await;
        assert_eq!(method, "GET");
        assert_eq!(uri, "/rest/v1/todos?select=id,title&done=eq.true");
        assert!(headers.contains(&("apikey".to_string(), "an-anon-key".to_string())));
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn a_2_0_events_cookies_come_back_as_a_header() {
        let mut event = v2("GET", "/auth/v1/user", "");
        event["cookies"] = json!(["sb-access-token=abc", "other=1"]);
        let (_, _, headers, _) = read(request(&event).expect("a request")).await;
        assert!(
            headers.contains(&(
                "cookie".to_string(),
                "sb-access-token=abc; other=1".to_string()
            )),
            "{headers:?}"
        );
    }

    #[tokio::test]
    async fn a_base64_body_arrives_as_the_bytes_it_was() {
        let mut event = v2("POST", "/storage/v1/object/pics/a.png", "");
        event["body"] = json!(Base64::encode_string(&[0u8, 159, 146, 150]));
        event["isBase64Encoded"] = json!(true);
        let (parts, body) = request(&event).expect("a request").into_parts();
        assert_eq!(parts.method, "POST");
        let body = to_bytes(body, MAX_BODY).await.expect("a body");
        assert_eq!(&body[..], &[0u8, 159, 146, 150]);
    }

    #[tokio::test]
    async fn a_rest_api_event_is_the_same_request_spelled_differently() {
        let event = json!({
            "httpMethod": "GET",
            "path": "/rest/v1/todos",
            "headers": {"apikey": "an-anon-key", "accept": "application/json"},
            "queryStringParameters": {"title": "eq.hello, world"},
        });
        let (method, uri, headers, _) = read(request(&event).expect("a request")).await;
        assert_eq!(method, "GET");
        assert_eq!(
            uri, "/rest/v1/todos?title=eq.hello%2C%20world",
            "a 1.0 event hands over a decoded value and a filter is full of punctuation"
        );
        assert!(headers.contains(&("apikey".to_string(), "an-anon-key".to_string())));
    }

    #[tokio::test]
    async fn a_repeated_header_stays_repeated() {
        let event = json!({
            "httpMethod": "GET",
            "path": "/rest/v1/todos",
            "headers": {"accept": "application/json"},
            "multiValueHeaders": {"accept": ["application/json", "text/csv"]},
        });
        let (_, _, headers, _) = read(request(&event).expect("a request")).await;
        let accepts: Vec<&String> = headers
            .iter()
            .filter(|(n, _)| n == "accept")
            .map(|(_, v)| v)
            .collect();
        assert_eq!(
            accepts,
            vec!["application/json", "text/csv"],
            "the multi valued map is the whole truth about a header it names"
        );
    }

    #[tokio::test]
    async fn a_repeated_query_parameter_survives() {
        let event = json!({
            "httpMethod": "GET",
            "path": "/rest/v1/todos",
            "multiValueQueryStringParameters": {"id": ["eq.1", "eq.2"]},
            "queryStringParameters": {"id": "eq.2"},
        });
        let (_, uri, _, _) = read(request(&event).expect("a request")).await;
        assert_eq!(uri, "/rest/v1/todos?id=eq.1&id=eq.2");
    }

    #[test]
    fn an_event_from_nowhere_is_refused_rather_than_guessed_at() {
        let why = request(&json!({"detail-type": "Scheduled Event"})).expect_err("not a request");
        assert!(why.contains("httpMethod"), "{why}");
    }

    #[tokio::test]
    async fn an_answer_says_its_status_headers_and_body() {
        let res = crate::json_body(StatusCode::CREATED, json!({"id": 1}));
        let answer = answer(res).await.expect("an answer");
        assert_eq!(answer["statusCode"], 201);
        assert_eq!(answer["headers"]["content-type"], "application/json");
        assert_eq!(answer["body"], "{\"id\":1}");
        assert_eq!(answer["isBase64Encoded"], false);
    }

    #[tokio::test]
    async fn bytes_that_are_not_text_come_back_base64() {
        let res = Response::builder()
            .status(200)
            .header("content-type", "image/png")
            .body(Body::from(vec![0u8, 159, 146, 150]))
            .expect("a response");
        let answer = answer(res).await.expect("an answer");
        assert_eq!(answer["isBase64Encoded"], true);
        assert_eq!(
            Base64::decode_vec(answer["body"].as_str().expect("a body")).expect("base64"),
            vec![0u8, 159, 146, 150]
        );
    }

    #[tokio::test]
    async fn two_cookies_are_two_cookies_in_both_spellings() {
        let res = Response::builder()
            .status(200)
            .header("set-cookie", "a=1")
            .header("set-cookie", "b=2")
            .body(Body::empty())
            .expect("a response");
        let answer = answer(res).await.expect("an answer");
        assert_eq!(answer["cookies"], json!(["a=1", "b=2"]));
        assert_eq!(
            answer["multiValueHeaders"]["set-cookie"],
            json!(["a=1", "b=2"])
        );
    }

    /// A runtime api that hands over the invocations it was made with
    /// and remembers what it was told about them.
    struct Fake {
        events: Mutex<Vec<Value>>,
        answers: Mutex<Vec<(String, Value)>>,
        errors: Mutex<Vec<(String, String)>>,
    }

    impl Fake {
        fn with(events: Vec<Value>) -> Fake {
            Fake {
                events: Mutex::new(events),
                answers: Mutex::new(Vec::new()),
                errors: Mutex::new(Vec::new()),
            }
        }
    }

    impl Api for Fake {
        fn next(&self) -> Result<Invocation, String> {
            let mut events = self.events.lock().expect("the events");
            if events.is_empty() {
                // A real one blocks here. This is how a test says the
                // environment went away.
                return Err("no more invocations".to_string());
            }
            Ok(Invocation {
                id: format!("id-{}", events.len()),
                event: events.remove(0),
            })
        }
        fn answered(&self, id: &str, answer: &Value) -> Result<(), String> {
            self.answers
                .lock()
                .expect("the answers")
                .push((id.to_string(), answer.clone()));
            Ok(())
        }
        fn failed(&self, id: &str, why: &str) -> Result<(), String> {
            self.errors
                .lock()
                .expect("the errors")
                .push((id.to_string(), why.to_string()));
            Ok(())
        }
        fn broken(&self, why: &str) -> Result<(), String> {
            self.errors
                .lock()
                .expect("the errors")
                .push(("init".to_string(), why.to_string()));
            Ok(())
        }
    }

    fn store() -> (tempfile::TempDir, Arc<Registry>, Arc<Attached>) {
        use crate::attach::Backend;
        use zou_store::registry::{self, Tenant};
        use zou_store::{CasStore, open_store};

        struct One;
        impl Backend for One {
            fn up(&self, entry: &zou_store::registry::Tenant) -> Result<crate::Config, String> {
                Ok(crate::Config {
                    jwt_secret: entry.jwt_secret.as_bytes().to_vec(),
                    ..crate::Config::default()
                })
            }
            fn down(&self, _tenant_ref: &str) {}
        }

        let dir = tempfile::tempdir().expect("a directory");
        let store: Arc<dyn CasStore> =
            Arc::from(open_store(&dir.path().to_string_lossy()).expect("a store"));
        registry::create(
            store.as_ref(),
            &Tenant::new("acme-prod", "a-secret-of-at-least-32-characters-long", 1),
        )
        .expect("it registers");
        let attached = Arc::new(Attached::new(Arc::new(One)));
        (dir, Arc::new(Registry::new(store)), attached)
    }

    #[test]
    fn the_loop_answers_an_invocation_through_the_front_door() {
        let (_dir, registry, attached) = store();
        let key = crate::jwt::mint(
            &crate::jwt::key_claims("anon"),
            b"a-secret-of-at-least-32-characters-long",
        );
        let mut event = v2("GET", "/auth/v1/health", "");
        event["headers"]["apikey"] = json!(key);
        let api = Fake::with(vec![event]);
        let why = serve_blocking(&api, "acme-prod", registry, attached)
            .expect_err("the loop only ends when the api does");
        assert_eq!(why, "no more invocations");
        let answers = api.answers.lock().expect("the answers");
        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0].0, "id-1");
        assert_eq!(answers[0].1["statusCode"], 200);
        assert!(api.errors.lock().expect("the errors").is_empty());
    }

    #[test]
    fn an_event_that_is_not_a_request_fails_that_invocation_and_not_the_function() {
        let (_dir, registry, attached) = store();
        let api = Fake::with(vec![json!({"detail-type": "Scheduled Event"})]);
        serve_blocking(&api, "acme-prod", registry, attached).expect_err("the api ran out");
        let errors = api.errors.lock().expect("the errors");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, "id-1");
        assert!(api.answers.lock().expect("the answers").is_empty());
    }

    #[test]
    fn a_project_that_is_not_there_is_an_init_failure() {
        let (_dir, registry, attached) = store();
        let api = Fake::with(vec![v2("GET", "/auth/v1/health", "")]);
        let why = serve_blocking(&api, "nobody", registry, attached).expect_err("nothing to serve");
        assert!(why.contains("nobody"), "{why}");
        let errors = api.errors.lock().expect("the errors");
        assert_eq!(errors.len(), 1);
        assert_eq!(
            errors[0].0, "init",
            "the environment is torn down, not asked for work"
        );
    }
}
