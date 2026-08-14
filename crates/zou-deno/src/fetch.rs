//! `fetch`, which is the thing a function is written to do.
//!
//! Deno's own is `deno_fetch`, which arrives with hyper, a second TLS
//! stack and a connection pool with its own opinions. This crate
//! already has an HTTP client, the one `zou-server` calls a database
//! webhook with, so `fetch` is that client behind an op rather than a
//! second one linked in beside it.
//!
//! The client is blocking and the isolate is not, so the call is made
//! on a blocking thread and awaited. That is the honest shape while a
//! response is collected before it is handed back: nothing here streams
//! yet, in either direction, and a function that wants a chunk at a
//! time is waiting on the same change a streamed answer is.
//!
//! What a function may reach is not restricted. It can call a metadata
//! endpoint on the machine it is running on, the same as upstream's
//! runtime can and the same as `pg_net` can from inside the database.
//! A function is the project's own code and this is written down in
//! `docs/functions.md` rather than left to be discovered.

use std::sync::OnceLock;
use std::time::Duration;

use deno_core::{JsBuffer, ToJsBuffer, op2};
use deno_error::JsErrorBox;

/// The same ceiling the server puts on a call's body, applied to what
/// a function may read back, so one function cannot answer one request
/// by holding a gigabyte of somebody else's json.
const BODY_LIMIT: u64 = 20 * 1024 * 1024;

/// How long a call may take. Deno itself has no default and waits
/// forever, which is a fine answer for a program somebody is watching
/// and a bad one for a request holding an isolate: without this, one
/// unreachable host is one isolate that never comes back.
const TIMEOUT: Duration = Duration::from_secs(30);

/// What a receiver sees when the function did not name one. Upstream's
/// runtime sends Deno's, and this is neither a lie about being Deno nor
/// a blank, both of which get a request refused by something eventually.
const AGENT: &str = "zou-edge-runtime";

#[derive(serde::Deserialize)]
pub struct Sent {
    method: String,
    url: String,
    headers: Vec<(String, String)>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Received {
    status: u16,
    status_text: String,
    url: String,
    redirected: bool,
    headers: Vec<(String, String)>,
    body: ToJsBuffer,
}

/// One call out, awaited by the handler that asked for it.
///
/// `lazy` because the work happens on a blocking thread and there is
/// nothing an eager poll of this future could finish: it would be a
/// poll that always returns pending, once per call.
#[op2(async(lazy))]
#[serde]
pub async fn op_zou_fetch(
    #[serde] sent: Sent,
    #[buffer] body: JsBuffer,
) -> Result<Received, JsErrorBox> {
    let body = body.to_vec();
    tokio::task::spawn_blocking(move || call(sent, body))
        .await
        .map_err(|e| JsErrorBox::type_error(format!("the call could not be started: {e}")))?
}

/// One agent for the process, so a function that calls the same host
/// twice reuses the connection, and so the TLS setup happens once.
fn agent() -> &'static ureq::Agent {
    static AGENTS: OnceLock<ureq::Agent> = OnceLock::new();
    AGENTS.get_or_init(|| {
        ureq::Agent::config_builder()
            // A 404 is an answer a handler is entitled to read, not a
            // transport failure, which is what `fetch` means by only
            // rejecting when the request could not be made at all.
            .http_status_as_error(false)
            .timeout_global(Some(TIMEOUT))
            .user_agent(AGENT)
            .build()
            .into()
    })
}

fn call(sent: Sent, body: Vec<u8>) -> Result<Received, JsErrorBox> {
    let asked = sent.url.clone();
    let mut built = ureq::http::Request::builder()
        .method(sent.method.as_str())
        .uri(&sent.url);
    for (name, value) in &sent.headers {
        built = built.header(name, value);
    }
    let built = built
        .body(&body[..])
        .map_err(|e| failed(&asked, &e.to_string()))?;
    let answer = agent()
        .run(built)
        .map_err(|e| failed(&asked, &reason(&e)))?;
    let status = answer.status().as_u16();
    let status_text = answer
        .status()
        .canonical_reason()
        .unwrap_or_default()
        .to_string();
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
    // Where the answer came from rather than where it was asked for,
    // which is the two things `response.url` and `response.redirected`
    // are for, and the only way a handler can tell it was moved.
    let landed = {
        use ureq::ResponseExt;
        answer.get_uri().to_string()
    };
    let body = answer
        .into_body()
        .with_config()
        .limit(BODY_LIMIT)
        .read_to_vec()
        .map_err(|e| failed(&asked, &reason(&e)))?;
    Ok(Received {
        status,
        status_text,
        redirected: landed != asked,
        url: landed,
        headers,
        body: body.into(),
    })
}

/// The shape Deno's own message has, because a function that logs what
/// went wrong is logging this and a project moving between the two
/// should read the same sentence.
fn failed(url: &str, why: &str) -> JsErrorBox {
    JsErrorBox::type_error(format!("error sending request for url ({url}): {why}"))
}

/// The words for what went wrong, short enough to end a sentence.
fn reason(e: &ureq::Error) -> String {
    match e {
        ureq::Error::Timeout(_) => format!("no answer within {} seconds", TIMEOUT.as_secs()),
        ureq::Error::HostNotFound => "could not resolve the host name".to_string(),
        ureq::Error::ConnectionFailed => "the connection failed".to_string(),
        ureq::Error::BodyExceedsLimit(limit) => {
            format!("the answer is longer than the {limit} byte ceiling")
        }
        other => other.to_string(),
    }
}
