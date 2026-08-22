//! `fetch`, which is the thing a function is written to do.
//!
//! Deno's own is `deno_fetch`, which arrives with hyper, a second TLS
//! stack and a connection pool with its own opinions. This crate
//! already has an HTTP client, the one `zou-server` calls a database
//! webhook with, so `fetch` is that client behind an op rather than a
//! second one linked in beside it.
//!
//! The client is blocking and the isolate is not, so the call is made
//! on a blocking thread and awaited. A response is collected before it
//! is handed back, which is the gap left here: an answer this runtime
//! sends reaches its caller in chunks, and an answer this runtime reads
//! does not, so a function that wants somebody else's body a chunk at a
//! time is waiting on a client that can be read as it arrives.
//!
//! A signal ends both halves of a call. It stops the op awaiting the
//! answer, and it shuts the socket down under the thread reading it, so
//! that the server hears about it and the thread comes back. The socket
//! is reachable at all because of `hangup`, which is where the client's
//! own TCP transport would otherwise be.
//!
//! What a function may reach is not restricted. It can call a metadata
//! endpoint on the machine it is running on, the same as upstream's
//! runtime can and the same as `pg_net` can from inside the database.
//! A function is the project's own code and this is written down in
//! `docs/functions.md` rather than left to be discovered.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::OnceLock;
use std::time::Duration;

use deno_core::{JsBuffer, OpState, ToJsBuffer, op2};
use deno_error::JsErrorBox;
use tokio::sync::oneshot;

/// The same ceiling the server puts on a call's body, applied to what
/// a function may read back, so one function cannot answer one request
/// by holding a gigabyte of somebody else's json.
pub(crate) const BODY_LIMIT: u64 = 20 * 1024 * 1024;

/// How long a call may take. Deno itself has no default and waits
/// forever, which is a fine answer for a program somebody is watching
/// and a bad one for a request holding an isolate: without this, one
/// unreachable host is one isolate that never comes back.
const TIMEOUT: Duration = Duration::from_secs(30);

/// What a receiver sees when the function did not name one.
///
/// The same string `navigator.userAgent` says, because that is what
/// upstream was measured sending: a function on a real `supabase start`
/// fetched an echo and the header read back
/// `Deno/2.1.4 (variant; SupabaseEdgeRuntime/1.74.2)`, which is its own
/// navigator string. This one has zou in the brackets and says the same
/// thing about the surface.
pub(crate) fn user_agent() -> &'static str {
    static AGENT: OnceLock<String> = OnceLock::new();
    AGENT.get_or_init(crate::isolate::user_agent)
}

/// What the module loader is, which is not what a function's `fetch`
/// is, and the difference is measured rather than tidy.
///
/// esm.sh reads this header and answers a different build depending on
/// it. A Deno agent is served the `denonext` build, which is the one
/// upstream runs and which imports `node:process` and `node:buffer`.
/// Anything else is served a build for a browser, with the platform
/// bits stubbed out.
///
/// Asking as Deno would be the better build in principle, since it is
/// the one a package author tested on a Deno runtime. It is not the
/// better build here yet, and the number is the reason: the same
/// corpus on the same machine on the same afternoon ran thirty two of
/// forty functions asking as this and twenty five asking as Deno. The
/// seven it costs are packages whose Deno build imports
/// `node:child_process`, `node:diagnostics_channel` or `node:module`,
/// and the browser build has those stubbed out by the registry.
/// Four of the seven want to start a process, which is not something
/// this will ever have, so the flip waits on a built in that refuses
/// the way the registry's stub does rather than on more of them
/// existing. `docs/functions.md` has the rest of the measurement.
fn loader() -> &'static str {
    "zou-edge-runtime"
}

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

/// The calls this isolate has in flight, so that a signal can end one.
///
/// A call is in `waiting` from its first poll until it answers. It is
/// in `ended` only in the window where a signal fired before the op
/// was first polled, which is narrow because the op is called before
/// the listener that ends it is attached, and is handled anyway
/// because a narrow window is still a window.
#[derive(Default)]
pub(crate) struct Calls {
    waiting: HashMap<u32, Waiting>,
    ended: HashSet<u32>,
}

/// A call in flight: the way to stop the op awaiting it, and the way to
/// stop the connection under it, which are two different threads' jobs.
struct Waiting {
    tell: oneshot::Sender<()>,
    ticket: u64,
}

/// One call out, awaited by the handler that asked for it.
///
/// `lazy` because the work happens on a blocking thread and there is
/// nothing an eager poll of this future could finish: it would be a
/// poll that always returns pending, once per call.
#[op2(async(lazy))]
#[serde]
pub async fn op_zou_fetch(
    state: Rc<RefCell<OpState>>,
    #[serde] sent: Sent,
    #[buffer] body: JsBuffer,
    #[smi] id: u32,
) -> Result<Received, JsErrorBox> {
    let body = body.to_vec();
    let (tell, told) = oneshot::channel();
    let ticket = crate::hangup::Ticket::new();
    {
        let mut state = state.borrow_mut();
        let calls = state.borrow_mut::<Calls>();
        if calls.ended.remove(&id) {
            return Err(JsErrorBox::type_error("the call was ended before it began"));
        }
        calls.waiting.insert(
            id,
            Waiting {
                tell,
                ticket: ticket.id(),
            },
        );
    }
    // Whichever comes first. An abort has already shut the socket down
    // by the time this side hears about it, so the blocking thread is
    // on its way out with an error nobody wants rather than reading an
    // answer nobody wants.
    let answer = tokio::select! {
        done = tokio::task::spawn_blocking(move || ticket.during(|| call(sent, body))) => done
            .map_err(|e| JsErrorBox::type_error(format!("the call could not be started: {e}")))?,
        _ = told => Err(JsErrorBox::type_error("the call was ended")),
    };
    state.borrow_mut().borrow_mut::<Calls>().waiting.remove(&id);
    answer
}

/// A call nobody is waiting for any more.
///
/// Both halves of it end here: the op stops awaiting, and the socket
/// the call is on is shut down under the thread reading it, so the
/// server is told that nobody wants what it is building.
///
/// Nothing is reported back. A signal that fired after the answer
/// arrived has nothing to end, and telling the caller so would be
/// telling it about a race it already won.
#[op2(fast)]
pub fn op_zou_fetch_abort(state: &mut OpState, #[smi] id: u32) {
    let calls = state.borrow_mut::<Calls>();
    match calls.waiting.remove(&id) {
        Some(waiting) => {
            crate::hangup::hangup(waiting.ticket);
            let _ = waiting.tell.send(());
        }
        None => {
            calls.ended.insert(id);
        }
    }
}

/// One agent for the process, so a function that calls the same host
/// twice reuses the connection, and so the TLS setup happens once.
/// The module loader fetches with it too, which is the same claim: a
/// package's graph is a dozen requests to one host.
pub(crate) fn agent() -> &'static ureq::Agent {
    static AGENTS: OnceLock<ureq::Agent> = OnceLock::new();
    AGENTS.get_or_init(|| {
        let config = ureq::Agent::config_builder()
            // A 404 is an answer a handler is entitled to read, not a
            // transport failure, which is what `fetch` means by only
            // rejecting when the request could not be made at all.
            .http_status_as_error(false)
            .timeout_global(Some(TIMEOUT))
            // The loader's, because the loader is the one that does not
            // name a header of its own. A call from a function has
            // `user_agent()` put on it below.
            .user_agent(loader())
            .build();
        // Built by hand rather than by the client's own default,
        // because the socket a call is on has to be reachable by the
        // signal that ends the call. `hangup` is the whole of why.
        crate::hangup::agent(config)
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
    // Unless the function named one itself, in which case it is the
    // function's call and the function's name on it.
    if !sent
        .headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("user-agent"))
    {
        built = built.header(ureq::http::header::USER_AGENT, user_agent());
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
