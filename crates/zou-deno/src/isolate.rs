//! What a call runs in, under either of upstream's two policies.
//!
//! A call is two halves, and they are separate here because the policy
//! is the question of how long the first half lives. `Ready` is an
//! isolate with the function's module loaded, transpiled and evaluated
//! in it, and `Ready::once` is one invocation in that isolate.
//!
//! Under `oneshot` a `Ready` is built for the call and dropped after
//! it, which is `once_off` below and is what the hosted service does.
//! Under `per_worker` it is kept and called again, which is `pool` and
//! is what the CLI does, hot reload and all.
//!
//! Either way an isolate is thread bound state, which is why
//! `Runtime::invoke` is sync: `oneshot` builds and drives it on the
//! blocking thread the server already handed us, on a current thread
//! tokio runtime of its own, and `per_worker` hands the call to the
//! thread its isolate already lives on. Nothing about an isolate is
//! ever moved anywhere else.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use deno_core::{JsRuntime, OpState, PollEventLoopOptions, RuntimeOptions, op2, v8};
use deno_error::JsErrorBox;
use zou_functions::{Answer, Call, Failed, Function, Runtime, Sink, Writer};

use zou_functions::Policy;

use crate::inspector::Inspector;
use crate::limits::{Limits, Watch};
use crate::{crypto, fetch, inspector, limits, module, pool, timer, url, websocket};

/// What the isolate has whether or not a call is in it: the function's
/// environment and the files it may read.
///
/// Apart from `Held` because it is the isolate's rather than the
/// call's, and because it has to be in the op state before the module
/// is evaluated. A function's top level runs once, before any handler
/// is registered, and reading a key out of the environment there is
/// the most ordinary line in the corpus: `const client = new
/// Thing(Deno.env.get("KEY"))`. An op that borrowed a call for that
/// would not find one.
pub(crate) struct Owned {
    env: Vec<(String, String)>,
    /// The files this function may read, which is its `static_files`
    /// and nothing else on the disk.
    statics: zou_functions::Statics,
}

/// What the isolate is told about the call it is running, and what it
/// left behind afterwards. Both live in the runtime's op state, which
/// is the only place ops can reach.
pub(crate) struct Held {
    call: Call,
    peer: String,
    answered: Option<Answer>,
    /// Where the answer goes. In here rather than held by `run`
    /// because a streamed answer leaves while the handler is still
    /// making it, and the only thing running then is an op.
    sink: Option<Sink>,
    /// The end of a streamed body, once one has been started.
    writing: Option<Writer>,
}

#[derive(serde::Serialize)]
struct Given {
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    peer: String,
    body: deno_core::ToJsBuffer,
}

/// The call these ops are for, or an error saying there is not one.
///
/// `Deno.core.ops` is reachable from a function, so every one of these
/// is a function call away from being made at the top of a module,
/// where no call has been put into the op state yet. A missing type in
/// the op state is a panic, and a panic in an op cannot unwind, so it
/// takes the whole server with it. This is what stands between a
/// mistyped line in somebody's javascript and a process that is gone.
fn held(state: &mut OpState) -> Result<&mut Held, JsErrorBox> {
    state
        .try_borrow_mut::<Held>()
        .ok_or_else(|| JsErrorBox::type_error("no call is being answered"))
}

/// Everything the call is: read once, at the top of `run`.
#[op2]
#[serde]
fn op_zou_call(state: &mut OpState) -> Result<Given, JsErrorBox> {
    let held = held(state)?;
    Ok(Given {
        method: held.call.method.clone(),
        url: held.call.url.clone(),
        headers: held.call.headers.clone(),
        peer: held.peer.clone(),
        body: held.call.body.clone().into(),
    })
}

/// What the handler answered, all of it.
#[op2]
fn op_zou_answer(
    state: &mut OpState,
    #[smi] status: u32,
    #[serde] headers: Vec<(String, String)>,
    #[buffer] body: &[u8],
) -> Result<(), JsErrorBox> {
    held(state)?.answered = Some(Answer {
        status: status as u16,
        headers,
        body: zou_functions::Body::Bytes(body.to_vec()),
    });
    Ok(())
}

/// The head of an answer whose body is still being made, which goes to
/// the caller now and not when the handler is finished.
#[op2]
fn op_zou_answer_start(
    state: &mut OpState,
    #[smi] status: u32,
    #[serde] headers: Vec<(String, String)>,
) -> Result<(), JsErrorBox> {
    let held = held(state)?;
    let Some(sink) = held.sink.take() else {
        return Err(JsErrorBox::type_error("the answer has already been sent"));
    };
    let (answer, writer) = Answer::streaming(status as u16, headers);
    held.writing = Some(writer);
    sink(answer);
    Ok(())
}

/// One chunk of it. Awaited, so a function that generates faster than
/// the caller reads is made to wait rather than allowed to hold the
/// whole body in memory on the way past.
#[op2(async(lazy), fast)]
async fn op_zou_chunk(
    state: Rc<RefCell<OpState>>,
    #[buffer(copy)] chunk: Vec<u8>,
) -> Result<(), JsErrorBox> {
    let writer = held(&mut state.borrow_mut())?.writing.clone();
    let Some(writer) = writer else {
        return Err(JsErrorBox::type_error("no answer is being streamed"));
    };
    if !writer.write(chunk).await {
        return Err(JsErrorBox::type_error(
            "the caller stopped reading the answer",
        ));
    }
    Ok(())
}

/// The end of it, which is the writer being dropped.
#[op2(fast)]
fn op_zou_chunk_end(state: &mut OpState) -> Result<(), JsErrorBox> {
    held(state)?.writing = None;
    Ok(())
}

/// The end of it, badly. The caller is already reading a 200, so all
/// this can do is stop the body where it is, and the reason is the
/// operator's the way a handler that threw is.
#[op2(fast)]
fn op_zou_chunk_fail(state: &mut OpState, #[string] why: String) -> Result<(), JsErrorBox> {
    if let Some(writer) = held(state)?.writing.take() {
        writer.fail(why);
    }
    Ok(())
}

/// One variable out of the function's environment, or null.
///
/// The last of a name rather than the first, because that is the rule
/// the environment was stacked with: a project's secrets go in and then
/// what the server owns goes in over them. `toObject` below collects
/// into a map, which keeps the last of a name too, and the two answers
/// have to be the same answer.
#[op2]
#[string]
fn op_zou_env_get(state: &mut OpState, #[string] name: String) -> Option<String> {
    let owned = state.borrow::<Owned>();
    owned
        .env
        .iter()
        .rev()
        .find(|(key, _)| *key == name)
        .map(|(_, value)| value.clone())
}

/// All of it, for `Deno.env.toObject()`.
#[op2]
#[serde]
fn op_zou_env(state: &mut OpState) -> std::collections::BTreeMap<String, String> {
    let owned = state.borrow::<Owned>();
    owned.env.iter().cloned().collect()
}

/// What `Deno.version` says, which is three strings a function is
/// allowed to read and nothing is allowed to depend on.
///
/// The shape is upstream's, measured on `supabase/edge-runtime` 1.74.2
/// rather than guessed: `deno` is the runtime naming itself and then
/// the Deno release its surface is written against, in brackets, which
/// there reads `supabase-edge-runtime-1.74.2 (compatible with Deno
/// v2.1.4)`. So nothing there is a bare version number either, and a
/// function comparing this string against one is already wrong on
/// upstream before it is wrong here.
#[op2]
#[serde]
fn op_zou_version() -> Version {
    Version {
        deno: format!(
            "zou-{} (compatible with Deno v{DENO})",
            env!("CARGO_PKG_VERSION")
        ),
        v8: v8::VERSION_STRING.to_string(),
        typescript: TYPESCRIPT.to_string(),
    }
}

/// What `navigator.userAgent` says, which a library reads to work out
/// what it is running on and which is therefore the one string here
/// that other people's code branches on.
///
/// Upstream's, measured through a function on a real `supabase start`,
/// is `Deno/2.1.4 (variant; SupabaseEdgeRuntime/1.74.2)`. That is Deno's
/// own format, where the brackets hold what embedded it, so the honest
/// answer for this runtime is the same sentence with this runtime's name
/// in the brackets. A library matching `/Deno\//` gets what it came for
/// and one reading the brackets is told the truth.
#[op2]
#[string]
fn op_zou_agent() -> String {
    user_agent()
}

/// The same sentence, for the client this runtime calls out with, which
/// is the other half of the same claim: upstream sends its navigator
/// string as the `User-Agent` of a `fetch` a function makes, measured
/// against an echo on a real `supabase start`.
pub(crate) fn user_agent() -> String {
    format!("Deno/{DENO} (variant; zou/{})", env!("CARGO_PKG_VERSION"))
}

/// The Deno release this runtime's surface is written against, which is
/// the one upstream's runtime names, because the gaps in it are written
/// down in `docs/functions.md` and measured against that.
const DENO: &str = "2.1.4";

/// The highest typescript release whose syntax this transpiler is
/// tested against, in `tests/typescript.rs`: `satisfies` and `accessor`
/// from 4.9, `const` type parameters and decorators from 5.0, `using`
/// from 5.2 and import attributes from 5.3. It moves when a test for a
/// later release's syntax is written, and not before.
const TYPESCRIPT: &str = "5.3.3";

#[derive(serde::Serialize)]
struct Version {
    deno: String,
    v8: String,
    typescript: String,
}

/// What one of the four read calls got, which is one of four things and
/// not a value or a throw.
///
/// A throw would have been a message, and javascript would then have had
/// to read the message to know whether this was a file that is not there
/// or a file the function may not have. Those are two different errors
/// in Deno, `NotFound` and `PermissionDenied`, and a function catching
/// the first and not the second is ordinary code.
#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Read {
    Bytes { bytes: deno_core::ToJsBuffer },
    Missing { why: String },
    Refused { why: String },
    Failed { why: String },
}

impl Read {
    /// One file, once the name has already been allowed.
    fn of(at: &std::path::Path, read: std::io::Result<Vec<u8>>) -> Read {
        match read {
            Ok(bytes) => Read::Bytes {
                bytes: bytes.into(),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Read::Missing {
                why: format!("{}: {e}", at.display()),
            },
            Err(e) => Read::Failed {
                why: format!("{}: {e}", at.display()),
            },
        }
    }
}

/// The wait a module parked on an accept is in, held out here rather
/// than in javascript.
///
/// A module that ends in `await app.listen({ port: 8000 })` is the
/// ordinary shape of an oak server and it never finishes evaluating,
/// because what it is waiting for is a request and the request is
/// waiting for the module. Both halves of that knot are cut here:
/// `said` is what `boot` waits on to stop waiting for a module that has
/// already said everything it is going to say, and `calls` is the
/// arrival that ends the park.
///
/// The wait is an op rather than a flag because in real Deno an accept
/// is an op on a real socket, and being one is what makes the runtime
/// underneath treat an unfinished top level as work in progress rather
/// than as a deadlock. Both halves are channels rather than flags for
/// the same reason: an op runs on the runtime's own turn of the
/// scheduler and not inside the poll that dispatched it, so a flag set
/// there is a flag nobody is woken to read, and what it costs to read
/// it late is the whole wall clock.
#[derive(Clone)]
pub(crate) struct Parked {
    said: Arc<tokio::sync::watch::Sender<bool>>,
    calls: Arc<tokio::sync::watch::Sender<u64>>,
}

impl Parked {
    fn new() -> Parked {
        Parked {
            said: Arc::new(tokio::sync::watch::channel(false).0),
            calls: Arc::new(tokio::sync::watch::channel(0).0),
        }
    }

    /// Wait until the module has parked on an accept.
    ///
    /// A module that parked before this is asked does not have to park
    /// again to be heard: the answer is the current value and not the
    /// next change to it.
    async fn parked(&self) {
        let mut said = self.said.subscribe();
        let _ = said.wait_for(|said| *said).await;
    }

    /// A call has been handed to the isolate, which is the thing every
    /// park is waiting for.
    fn arrived(&self) {
        self.calls
            .send_modify(|calls| *calls = calls.wrapping_add(1));
    }
}

/// The module has parked on an accept, and stays parked until a call
/// arrives.
///
/// One of these is dispatched per park rather than one per isolate, so
/// that a pooled isolate serving its tenth call is waiting on the same
/// terms as one serving its first.
#[op2(async(lazy), fast)]
async fn op_zou_parked(state: Rc<RefCell<OpState>>) {
    let parked = state.borrow().borrow::<Parked>().clone();
    // Subscribing before saying anything, because the two ends of this
    // are a call away from each other: what is said here is what lets
    // the call be made, and a receiver made after that call would have
    // been made too late to hear it.
    let mut calls = parked.calls.subscribe();
    parked.said.send_replace(true);
    let _ = calls.changed().await;
}

/// `Deno.readFile` and `Deno.readTextFile`, which are the same op and
/// differ in javascript by what is done with the bytes.
#[op2(async(lazy), fast)]
#[serde]
async fn op_zou_read_file(state: Rc<RefCell<OpState>>, #[string] name: String) -> Read {
    let asked = state.borrow().borrow::<Owned>().statics.at(&name);
    match asked {
        Err(why) => Read::Refused { why },
        Ok(at) => Read::of(&at, tokio::fs::read(&at).await),
    }
}

/// The same, for the two sync spellings, which upstream turns on for a
/// worker with `useReadSyncFileAPI` and which a function serving a page
/// out of its own directory usually reaches for.
#[op2]
#[serde]
fn op_zou_read_file_sync(state: &mut OpState, #[string] name: String) -> Read {
    match state.borrow::<Owned>().statics.at(&name) {
        Err(why) => Read::Refused { why },
        Ok(at) => Read::of(&at, std::fs::read(&at)),
    }
}

deno_core::extension!(
    zou,
    ops = [
        op_zou_call,
        op_zou_answer,
        op_zou_answer_start,
        op_zou_chunk,
        op_zou_chunk_end,
        op_zou_chunk_fail,
        op_zou_env_get,
        op_zou_env,
        op_zou_parked,
        op_zou_version,
        op_zou_agent,
        op_zou_read_file,
        op_zou_read_file_sync,
        crypto::op_zou_random,
        crypto::op_zou_digest,
        crypto::op_zou_sign,
        crypto::op_zou_verify,
        crypto::op_zou_encrypt,
        crypto::op_zou_decrypt,
        fetch::op_zou_fetch,
        fetch::op_zou_fetch_abort,
        timer::op_zou_sleep,
        timer::op_zou_clear,
        timer::op_zou_now,
        url::op_zou_url_parse,
        url::op_zou_url_set,
        websocket::op_zou_ws_connect,
        websocket::op_zou_ws_next,
        websocket::op_zou_ws_send_text,
        websocket::op_zou_ws_send_bytes,
        websocket::op_zou_ws_close,
        websocket::op_zou_ws_drop
    ]
);

/// The engine, which is a v8 isolate per call or a v8 isolate per
/// function depending on the policy it was built with.
///
/// The environment is the same for every function this runtime serves,
/// which is what a project's secrets are: `Deno.env` inside a function
/// is this map and never the host process's own environment, so a
/// server holding a database password does not hand it to somebody
/// else's javascript by having been started with it set.
pub struct Isolate {
    env: Vec<(String, String)>,
    limits: Limits,
    policy: Policy,
    /// The isolates kept between calls, which is nothing at all under
    /// `oneshot` and is where every call goes under `per_worker`.
    pool: pool::Pool,
    /// The port a debugger attaches to, if the project asked for one.
    inspector: Option<Arc<Inspector>>,
}

/// Upstream's one per invocation variable, which the call carries and
/// the environment does not.
const EXECUTION_ID: &str = "SB_EXECUTION_ID";

impl Isolate {
    pub fn new() -> Isolate {
        Isolate::with_env(Vec::new())
    }

    /// The environment every function this runtime serves will see.
    pub fn with_env(env: Vec<(String, String)>) -> Isolate {
        Isolate {
            env,
            limits: Limits::default(),
            // A fresh isolate per call is the shape that needs nothing
            // explained, so it is what this is until somebody asks for
            // the other one. The server asks, out of `config.toml`.
            policy: Policy::OneShot,
            pool: pool::Pool::default(),
            inspector: None,
        }
    }

    /// Other limits than upstream's, which is what a test needs and
    /// what a deployment that knows its own functions may want.
    pub fn with_limits(mut self, limits: Limits) -> Isolate {
        self.limits = limits;
        self
    }

    /// Upstream's `[edge_runtime] policy`: a fresh isolate per call, or
    /// one kept between calls per function.
    pub fn with_policy(mut self, policy: Policy) -> Isolate {
        self.policy = policy;
        self
    }

    /// Upstream's `[edge_runtime] inspector_port`: a port a debugger
    /// attaches to, and every isolate this runtime makes listed on it.
    ///
    /// Zero means the operating system picks, which is what a test
    /// wants and what [`Isolate::debugging_at`] is for.
    pub fn with_inspector(self, port: u16) -> Result<Isolate, String> {
        Ok(self.debugged_by(Inspector::start(port)?))
    }

    /// The same, for a port somebody else has already bound.
    pub(crate) fn debugged_by(mut self, inspector: Arc<Inspector>) -> Isolate {
        self.inspector = Some(inspector);
        self
    }

    /// Where a debugger would attach, once one has been asked for.
    pub fn debugging_at(&self) -> Option<std::net::SocketAddr> {
        self.inspector.as_ref().map(|inspector| inspector.at())
    }

    /// What a call runs under, which is not what the project's config
    /// file said when a debugger can stop it.
    ///
    /// A breakpoint is a function not making progress on purpose, and a
    /// two second cpu limit is this server promising to stop a function
    /// that is not making progress. They cannot both be honoured, and
    /// the one the operator asked for most recently is the debugger.
    /// What stays is the memory limit, which a debugger has no opinion
    /// about and which is the one whose absence takes the machine down
    /// with it.
    fn limits(&self) -> Limits {
        match self.inspector {
            Some(_) => self.limits.patient(),
            None => self.limits,
        }
    }
}

impl Default for Isolate {
    fn default() -> Isolate {
        Isolate::new()
    }
}

impl Runtime for Isolate {
    fn invoke(&self, function: &Function, call: Call) -> Result<Answer, Failed> {
        // The blocking shape, for a caller that wants the answer and
        // the background work in the same wait: the sink writes into a
        // slot rather than anywhere the call could have gone on ahead.
        //
        // A streamed body cannot wait in a slot, though. Nothing would
        // be reading it, the isolate would fill the channel and stop,
        // and the call would never come back to be asked for its
        // answer. So a body that is still arriving is collected on a
        // thread of its own, started the moment the answer is handed
        // over, and what this returns is the bytes it collected.
        let held = std::sync::Arc::new(std::sync::Mutex::new(None));
        let collecting = std::sync::Arc::new(std::sync::Mutex::new(None));
        let slot = std::sync::Arc::clone(&held);
        let started = std::sync::Arc::clone(&collecting);
        self.invoke_answering(
            function,
            call,
            Box::new(move |mut answer| {
                if let zou_functions::Body::Chunks(chunks) = answer.body {
                    answer.body = zou_functions::Body::Bytes(Vec::new());
                    *started.lock().expect("nothing else holds this") =
                        Some(std::thread::spawn(move || chunks.collect_blocking()));
                }
                *slot.lock().expect("nothing else holds this") = Some(answer);
            }),
        )?;
        let answer = held.lock().expect("the isolate is done with it").take();
        let mut answer = answer
            .ok_or_else(|| Failed::Threw("the handler returned without an answer".to_string()))?;
        if let Some(collector) = collecting
            .lock()
            .expect("the isolate is done with it")
            .take()
        {
            let collected = collector
                .join()
                .map_err(|_| "the answer's body could not be collected".to_string())??;
            answer.body = zou_functions::Body::Bytes(collected);
        }
        Ok(answer)
    }

    fn invoke_answering(
        &self,
        function: &Function,
        call: Call,
        answer: Sink,
    ) -> Result<(), Failed> {
        // V8 needs one process wide platform and does not care who set
        // it up, so long as it happened before the first isolate.
        static PLATFORM: std::sync::Once = std::sync::Once::new();
        PLATFORM.call_once(|| {
            JsRuntime::init_platform(None);
        });
        let peer = call.header("x-real-ip").unwrap_or("127.0.0.1").to_string();
        let entrypoint = std::path::absolute(&function.entrypoint)
            .map_err(|e| format!("{}: {e}", function.entrypoint.display()))?;
        let specifier = deno_core::ModuleSpecifier::from_file_path(&entrypoint)
            .map_err(|()| format!("{} is not a path v8 can be given", entrypoint.display()))?;
        let source = Source {
            specifier,
            import_map: function.import_map.clone(),
        };
        // Four of upstream's five variables are the project's and are
        // the same every call. The fifth is this call's own, which is
        // what ties a log line from inside a function to the request
        // that caused it, so it is added here rather than being asked
        // of the environment the runtime was built with.
        let mut env = self.env.clone();
        env.retain(|(name, _)| name != EXECUTION_ID);
        env.push((EXECUTION_ID.to_string(), call.execution_id.clone()));
        let owned = Owned {
            env,
            statics: zou_functions::Statics::of(function),
        };
        let held = Held {
            call,
            peer,
            answered: None,
            sink: Some(answer),
            writing: None,
        };
        let limits = self.limits();
        let debugger = self.inspector.clone();
        if self.policy == Policy::PerWorker {
            // Somebody else's thread, which already has an isolate with
            // this function in it, or is about to.
            return self.pool.run(&source, owned, held, limits, debugger);
        }
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("the isolate could not have a runtime: {e}"))?;
        tokio.block_on(once_off(source, owned, held, limits, debugger))
    }

    fn describe(&self) -> String {
        match self.policy {
            Policy::OneShot => "a v8 isolate per call".to_string(),
            Policy::PerWorker => "a v8 isolate per function, kept between calls".to_string(),
        }
    }
}

/// What an isolate is built out of: the module to start at, and the
/// file that says what the bare names in it mean.
///
/// The two travel together because they are one decision. An isolate
/// built from the same entrypoint and a different import map is a
/// different function, and hot reload treats the map as one of the
/// files the isolate came from for the same reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Source {
    pub specifier: deno_core::ModuleSpecifier,
    pub import_map: Option<std::path::PathBuf>,
}

impl Source {
    /// What the pool files a kept isolate under, which is both halves
    /// and not just the entrypoint: two functions pointed at one file
    /// through different maps are two isolates.
    pub(crate) fn key(&self) -> String {
        match &self.import_map {
            Some(at) => format!("{} through {}", self.specifier, at.display()),
            None => self.specifier.to_string(),
        }
    }
}

/// A call in an isolate of its own, which is thrown away afterwards.
///
/// The wall clock is watched twice, from the two sides a call can
/// overrun on. This timer is the one that catches a function that is
/// asleep, because terminating execution does not wake a sleeper, and
/// the watchdog inside is the one that catches a function that never
/// gives its thread back.
pub(crate) async fn once_off(
    source: Source,
    owned: Owned,
    held: Held,
    limits: Limits,
    debugger: Option<Arc<Inspector>>,
) -> Result<(), Failed> {
    let named = source.specifier.to_string();
    let call = async move {
        let mut ready = Ready::new(source, limits, debugger, owned).await?;
        ready.once(held).await
    };
    match tokio::time::timeout(limits.wall, call).await {
        Ok(ran) => ran,
        Err(_) => Err(Failed::Limit(format!(
            "{named}: it was still running after the {:?} it is allowed",
            limits.wall
        ))),
    }
}

/// An isolate with a function's module evaluated in it, waiting to be
/// called.
///
/// Under `oneshot` one of these is built and dropped per call, and
/// under `per_worker` one is built and called until something makes it
/// unfit: a limit reached, which leaves the isolate terminated, or a
/// file it was built out of changing on disk, which is hot reload.
pub(crate) struct Ready {
    js: JsRuntime,
    entry: v8::Global<v8::Function>,
    /// What the module exported as `default`, or undefined. Read once
    /// here rather than per call, because a module is evaluated once and
    /// its exports do not change after it.
    exported: v8::Global<v8::Value>,
    drain: v8::Global<v8::Function>,
    watch: Arc<Watch>,
    handle: v8::IsolateHandle,
    specifier: deno_core::ModuleSpecifier,
    limits: Limits,
    /// The files that went into it, for the question hot reload asks.
    read: module::Reads,
    /// Its line in the debugger's target list, which it leaves by being
    /// dropped along with the rest of this.
    attached: Option<inspector::Attached>,
}

impl Ready {
    pub(crate) async fn new(
        source: Source,
        limits: Limits,
        debugger: Option<Arc<Inspector>>,
        owned: Owned,
    ) -> Result<Ready, Failed> {
        build(source, limits, debugger, owned).await
    }

    /// The environment this isolate answers with from now on.
    ///
    /// A kept isolate is called again with a new `SB_EXECUTION_ID` in
    /// it, and the rest of the environment is the same vector it was
    /// built with, so this is a replacement rather than a merge.
    pub(crate) fn owns(&mut self, owned: Owned) {
        self.js.op_state().borrow_mut().put(owned);
    }

    /// Whether a file this was built out of has moved since it was.
    pub(crate) fn stale(&self) -> bool {
        module::changed(&self.read)
    }

    /// Whether this isolate has reached a limit, which is the end of it
    /// whatever the call it was reached in went on to do: what v8 does
    /// to a terminated isolate is not somewhere the next call should
    /// start from.
    pub(crate) fn spent(&self) -> bool {
        self.watch.reached().is_some()
    }

    /// Whether a debugger could be attached to this isolate.
    pub(crate) fn debuggable(&self) -> bool {
        self.attached.is_some()
    }

    /// Give whatever a debugger has said a turn, without running any of
    /// the function's own event loop.
    ///
    /// This is what a worker does while it waits for its next call. A
    /// debugger setting a breakpoint, reading a source or evaluating an
    /// expression between two calls is asking the isolate and not the
    /// function, and none of it should start a timer the function left
    /// behind or resolve a promise nobody is waiting for.
    pub(crate) fn served(&mut self) {
        let inspector = self.js.inspector();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        inspector.poll_sessions_from_event_loop(&mut cx);
    }
}

async fn build(
    source: Source,
    limits: Limits,
    debugger: Option<Arc<Inspector>>,
    owned: Owned,
) -> Result<Ready, Failed> {
    let Source {
        specifier,
        import_map,
    } = source;
    // The map is read before the isolate exists, because a function
    // whose `deno.json` is broken has not got a module to load: the
    // names in it are how the module would have said what it imports.
    let imports = match &import_map {
        Some(at) => Some(crate::imports::Imports::read(at)?),
        None => None,
    };
    // Everything that stops this call is set up before any of the
    // function's own code has run, including the module it is in, and
    // two of the three have to be in place before the isolate exists at
    // all rather than after it.
    let watch = Watch::new(limits);
    let (loader, read) = module::loader(imports);
    let mut js = JsRuntime::new(RuntimeOptions {
        module_loader: Some(loader),
        extensions: vec![zou::init()],
        // V8 only carries the machinery a debugger needs when it was
        // built with it, so this is the project's `inspector_port`
        // reaching all the way down: no port, no inspector, and an
        // isolate that costs what it did before this existed.
        inspector: debugger.is_some(),
        is_main: debugger.is_some(),
        // The memory limits are v8's heap and, because a buffer is not
        // on that heap, an allocator that counts what the function asks
        // for outside it.
        create_params: Some(
            v8::CreateParams::default()
                .heap_limits(0, limits.memory)
                .array_buffer_allocator(limits::buffers(Arc::clone(&watch), limits)),
        ),
        ..Default::default()
    });
    let handle = js.v8_isolate().thread_safe_handle();
    // Before the module is loaded, so that a debugger which attached
    // while the isolate was being built sees the scripts go past rather
    // than having to ask for them afterwards.
    let attached = debugger.map(|inspector| {
        let sessions = js.inspector().get_session_sender();
        inspector.attach(&specifier, sessions)
    });
    let watchdog = limits::watch(handle.clone(), Arc::clone(&watch), limits);
    js.add_near_heap_limit_callback(limits::near_heap_limit(handle.clone(), Arc::clone(&watch)));
    js.op_state().borrow_mut().put(timer::Pending::default());
    js.op_state().borrow_mut().put(timer::Started::default());
    // Before the module is evaluated rather than with the call, because
    // the top of a module runs here and reading the environment there
    // is the most ordinary line there is.
    js.op_state().borrow_mut().put(owned);
    let parked = Parked::new();
    js.op_state().borrow_mut().put(parked.clone());
    js.op_state()
        .borrow_mut()
        .put(websocket::Sockets::default());
    js.op_state().borrow_mut().put(fetch::Calls::default());

    // The prelude is the value of its own last expression, so the two
    // entry points are held here and never on an object the function
    // can reach.
    let entries = {
        let _running = watch.running();
        js.execute_script("zou:prelude.js", include_str!("prelude.js"))
    }
    .map_err(|e| format!("the prelude did not run: {e}"))?;

    let id = watch
        .timing(js.load_main_es_module(&specifier))
        .await
        .map_err(|e| why(&specifier, &watch, e))?;
    // The module is evaluated by waiting on its own promise and not by
    // running the loop until it is idle. A module that leaves a timer
    // behind it is ordinary rather than exotic, and `createClient` is
    // one: it starts a refresh interval on the way through, and an idle
    // loop is a condition an interval means never happens.
    let evaluated = {
        // Everything a module does at the top of itself happens here
        // rather than in the future this returns, so the counting has
        // to start before the call and not around the await.
        let _running = watch.running();
        js.mod_evaluate(id)
    };
    let evaluated = std::pin::pin!(evaluated);
    // The other way out of that wait: a module that has parked on an
    // accept has said everything it is going to say, and what it is
    // waiting for is the call this is on the way to making. oak is
    // written that way, `await app.listen({ port: 8000 })` as the last
    // line of the module, and so is everything built on oak, so the
    // evaluation finishing cannot be the only door out of here or none
    // of that runs at all.
    let listening = {
        let mut waiting =
            std::pin::pin!(js.with_event_loop_promise(evaluated, PollEventLoopOptions::default()));
        let mut parked = std::pin::pin!(parked.parked());
        let until = std::future::poll_fn(|cx| {
            use std::task::Poll;
            match std::future::Future::poll(waiting.as_mut(), cx) {
                Poll::Pending => std::future::Future::poll(parked.as_mut(), cx).map(|()| Ok(true)),
                other => other.map(|done| done.map(|()| false)),
            }
        });
        watch
            .timing(until)
            .await
            .map_err(|e| why(&specifier, &watch, e))?
    };

    // `export default { fetch }` is one of the three ways upstream lets
    // a module say what to run, and the only one the module says by
    // exporting rather than by calling something the prelude handed it.
    // So it is read out of the namespace here and passed in at call
    // time, and the prelude decides between it and the other two.
    //
    // Except for a module that is still on its own top level, which is
    // not asked. It took the socket, and taking the socket already
    // beats a default export in the order the prelude picks in, so the
    // answer would be thrown away. Reading it would mean reading a
    // binding the module has not reached yet.
    let namespace = match listening {
        true => None,
        false => Some(
            js.get_module_namespace(id)
                .map_err(|e| why(&specifier, &watch, e))?,
        ),
    };

    let (entry, exported, drain) = {
        let context = js.main_context();
        let isolate = &mut *js.v8_isolate();
        v8::scope_with_context!(let scope, isolate, context);
        let value = v8::Local::new(scope, entries);
        let pair: v8::Local<v8::Array> = value
            .try_into()
            .map_err(|_| "the prelude did not end in its entry points".to_string())?;
        let mut held = Vec::new();
        for at in [0, 1] {
            let value = pair
                .get_index(scope, at)
                .ok_or_else(|| format!("the prelude's entry point {at} is missing"))?;
            let function: v8::Local<v8::Function> = value
                .try_into()
                .map_err(|_| format!("the prelude's entry point {at} is not a function"))?;
            held.push(v8::Global::new(scope, function));
        }
        let drain = held.pop().expect("two of them");
        // A module namespace answers for a name it does not export with
        // undefined rather than by failing, which is exactly what the
        // prelude wants to be told about a module that has no default.
        let exported = match &namespace {
            None => v8::undefined(scope).into(),
            Some(namespace) => {
                let namespace = v8::Local::new(scope, namespace);
                let default = v8::String::new(scope, "default").expect("a four character name");
                namespace
                    .get(scope, default.into())
                    .unwrap_or_else(|| v8::undefined(scope).into())
            }
        };
        (
            held.pop().expect("two of them"),
            v8::Global::new(scope, exported),
            drain,
        )
    };
    drop(watchdog);
    Ok(Ready {
        js,
        entry,
        exported,
        drain,
        watch,
        handle,
        specifier,
        limits,
        read,
        attached,
    })
}

impl Ready {
    /// One call in this isolate.
    pub(crate) async fn once(&mut self, held: Held) -> Result<(), Failed> {
        let Ready {
            js,
            entry,
            exported,
            drain,
            watch,
            handle,
            specifier,
            limits,
            ..
        } = self;
        let limits = *limits;
        // The clocks are the call's and not the isolate's, so an
        // isolate on its tenth call has the same two seconds as one on
        // its first. What is not reset is the memory, which is the
        // isolate's for as long as it lives and is the reason a pooled
        // isolate can reach a limit a fresh one would not.
        watch.restart();
        let watchdog = limits::watch(handle.clone(), Arc::clone(watch), limits);
        js.op_state().borrow_mut().put(held);
        // A module parked on an accept is waiting on an op out here,
        // the way a real accept waits on a real socket, and this is
        // the call arriving that ends that wait.
        js.op_state().borrow().borrow::<Parked>().arrived();

        // The same again, and this is the one that matters: a handler
        // that is not async runs to its end inside `call`, so a
        // function that never returns never returns from here.
        let called = {
            let _running = watch.running();
            js.call_with_args(entry, std::slice::from_ref(exported))
        };
        let ran = watch
            .timing(js.with_event_loop_promise(called, PollEventLoopOptions::default()))
            .await
            .map_err(|e| why(specifier, watch, e));
        if let Err(failed) = ran {
            // The sink goes with the failure, and it has to go from
            // here rather than with the isolate. A caller is waiting on
            // the other end of it, and under `per_worker` the isolate
            // holding it is kept: a handler that threw would have left
            // the caller waiting for the minute it takes that isolate
            // to go idle, which is how this was found.
            let state = js.op_state();
            let mut state = state.borrow_mut();
            let held = state.borrow_mut::<Held>();
            held.answered.take();
            held.sink.take();
            return Err(failed);
        }

        // The answer goes now, not when this function returns, because
        // what is left to do after it is the function's own business
        // and the caller is not a party to it.
        //
        // A streamed answer has already gone, from inside the op that
        // started it, and the body finished arriving before the
        // handler's promise resolved. So the sink being gone is the
        // whole test for whether the caller has been answered, and
        // having neither a sink nor an answer is the only way to have
        // answered nobody.
        let (answered, sink) = {
            let state = js.op_state();
            let mut state = state.borrow_mut();
            let held = state.borrow_mut::<Held>();
            (held.answered.take(), held.sink.take())
        };
        match (answered, sink) {
            (Some(answer), Some(sink)) => sink(answer),
            (_, None) => {}
            (None, Some(_)) => {
                return Err(Failed::Threw(
                    "the handler returned without an answer".to_string(),
                ));
            }
        }

        // Whatever `EdgeRuntime.waitUntil` was given, until it settles
        // or until it has had long enough. A promise nobody resolves is
        // not an error a function is told about, and it is not a thread
        // this process keeps forever either.
        //
        // This budget is the shorter of the two the background work is
        // under: the call's wall clock is still running and still
        // counts whatever the function did before it answered.
        let drained = {
            let _running = watch.running();
            js.call(drain)
        };
        let waited =
            watch.timing(js.with_event_loop_promise(drained, PollEventLoopOptions::default()));
        let ended = match tokio::time::timeout(limits.background, waited).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(why(
                specifier,
                watch,
                format_args!("work left after the answer failed: {e}"),
            )),
            Err(_) => Err(Failed::Threw(format!(
                "{specifier}: work left after the answer was still running after {:?} and was dropped",
                limits.background
            ))),
        };
        drop(watchdog);
        ended
    }
}

/// Why a step of a call ended badly.
///
/// The limit is asked about first because a terminated isolate does not
/// explain itself: what v8 says about a function that was stopped
/// halfway through is an uncaught null or a bare `execution terminated`,
/// which tells an operator nothing about the number that was reached.
fn why(
    specifier: &deno_core::ModuleSpecifier,
    watch: &Watch,
    said: impl std::fmt::Display,
) -> Failed {
    match watch.reached() {
        Some(what) => Failed::Limit(format!("{specifier}: {}", watch.sentence(what))),
        None => Failed::Threw(format!("{specifier}: {said}")),
    }
}
