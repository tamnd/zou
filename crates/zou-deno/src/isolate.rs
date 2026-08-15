//! One call, one isolate.
//!
//! This is upstream's `oneshot` policy and nothing else yet: a v8
//! isolate is created for the call, the function's module is loaded and
//! evaluated in it, the handler is called once, and the isolate is
//! dropped. That is the honest starting point, because a pool is a
//! decision about what a function may keep between calls and that
//! decision deserves its own change rather than being smuggled in with
//! the engine.
//!
//! It is also the reason `Runtime::invoke` is sync. An isolate is
//! thread bound state, so it is built and driven on the blocking thread
//! the server already handed us, on a current thread tokio runtime of
//! its own, and nothing about it is ever moved anywhere else.

use std::cell::RefCell;
use std::rc::Rc;

use deno_core::{JsRuntime, OpState, PollEventLoopOptions, RuntimeOptions, op2, v8};
use deno_error::JsErrorBox;
use zou_functions::{Answer, Call, Function, Runtime, Sink, Writer};

use crate::{crypto, fetch, module, timer, url, websocket};

/// What the isolate is told about the call it is running, and what it
/// left behind afterwards. Both live in the runtime's op state, which
/// is the only place ops can reach.
struct Held {
    call: Call,
    peer: String,
    env: Vec<(String, String)>,
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

/// Everything the call is: read once, at the top of `run`.
#[op2]
#[serde]
fn op_zou_call(state: &mut OpState) -> Given {
    let held = state.borrow::<Held>();
    Given {
        method: held.call.method.clone(),
        url: held.call.url.clone(),
        headers: held.call.headers.clone(),
        peer: held.peer.clone(),
        body: held.call.body.clone().into(),
    }
}

/// What the handler answered, all of it.
#[op2]
fn op_zou_answer(
    state: &mut OpState,
    #[smi] status: u32,
    #[serde] headers: Vec<(String, String)>,
    #[buffer] body: &[u8],
) {
    let held = state.borrow_mut::<Held>();
    held.answered = Some(Answer {
        status: status as u16,
        headers,
        body: zou_functions::Body::Bytes(body.to_vec()),
    });
}

/// The head of an answer whose body is still being made, which goes to
/// the caller now and not when the handler is finished.
#[op2]
fn op_zou_answer_start(
    state: &mut OpState,
    #[smi] status: u32,
    #[serde] headers: Vec<(String, String)>,
) -> Result<(), JsErrorBox> {
    let held = state.borrow_mut::<Held>();
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
    let writer = state.borrow().borrow::<Held>().writing.clone();
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
fn op_zou_chunk_end(state: &mut OpState) {
    state.borrow_mut::<Held>().writing = None;
}

/// The end of it, badly. The caller is already reading a 200, so all
/// this can do is stop the body where it is, and the reason is the
/// operator's the way a handler that threw is.
#[op2(fast)]
fn op_zou_chunk_fail(state: &mut OpState, #[string] why: String) {
    if let Some(writer) = state.borrow_mut::<Held>().writing.take() {
        writer.fail(why);
    }
}

/// One variable out of the function's environment, or null.
#[op2]
#[string]
fn op_zou_env_get(state: &mut OpState, #[string] name: String) -> Option<String> {
    let held = state.borrow::<Held>();
    held.env
        .iter()
        .find(|(key, _)| *key == name)
        .map(|(_, value)| value.clone())
}

/// All of it, for `Deno.env.toObject()`.
#[op2]
#[serde]
fn op_zou_env(state: &mut OpState) -> std::collections::BTreeMap<String, String> {
    let held = state.borrow::<Held>();
    held.env.iter().cloned().collect()
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
        crypto::op_zou_random,
        crypto::op_zou_digest,
        crypto::op_zou_sign,
        crypto::op_zou_verify,
        fetch::op_zou_fetch,
        timer::op_zou_sleep,
        timer::op_zou_clear,
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

/// A v8 isolate per call.
///
/// The environment is the same for every function this runtime serves,
/// which is what a project's secrets are: `Deno.env` inside a function
/// is this map and never the host process's own environment, so a
/// server holding a database password does not hand it to somebody
/// else's javascript by having been started with it set.
pub struct Isolate {
    env: Vec<(String, String)>,
}

/// Upstream's one per invocation variable, which the call carries and
/// the environment does not.
const EXECUTION_ID: &str = "SB_EXECUTION_ID";

impl Isolate {
    pub fn new() -> Isolate {
        Isolate { env: Vec::new() }
    }

    /// The environment every function this runtime serves will see.
    pub fn with_env(env: Vec<(String, String)>) -> Isolate {
        Isolate { env }
    }
}

impl Default for Isolate {
    fn default() -> Isolate {
        Isolate::new()
    }
}

/// How long work registered with `EdgeRuntime.waitUntil` may go on
/// after the caller has been answered.
///
/// There is a limit because the thread this runs on is a real one and
/// the isolate holding it is real memory, and because a promise that
/// never settles is a thing a function can write by accident. Thirty
/// seconds is long enough for the reason `waitUntil` exists, which is
/// a log line or a webhook that should not have been on the caller's
/// critical path, and short enough that a leak is a blip. Per isolate
/// limits are their own box on #369 and this number moves there when
/// they arrive.
const BACKGROUND: std::time::Duration = std::time::Duration::from_secs(30);

impl Runtime for Isolate {
    fn invoke(&self, function: &Function, call: Call) -> Result<Answer, String> {
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
        let mut answer =
            answer.ok_or_else(|| "the handler returned without an answer".to_string())?;
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
    ) -> Result<(), String> {
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
        // Four of upstream's five variables are the project's and are
        // the same every call. The fifth is this call's own, which is
        // what ties a log line from inside a function to the request
        // that caused it, so it is added here rather than being asked
        // of the environment the runtime was built with.
        let mut env = self.env.clone();
        env.retain(|(name, _)| name != EXECUTION_ID);
        env.push((EXECUTION_ID.to_string(), call.execution_id.clone()));
        let held = Held {
            call,
            peer,
            env,
            answered: None,
            sink: Some(answer),
            writing: None,
        };
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("the isolate could not have a runtime: {e}"))?;
        tokio.block_on(run(specifier, held))
    }

    fn describe(&self) -> String {
        "a v8 isolate per call".to_string()
    }
}

async fn run(specifier: deno_core::ModuleSpecifier, held: Held) -> Result<(), String> {
    let mut js = JsRuntime::new(RuntimeOptions {
        module_loader: Some(module::loader()),
        extensions: vec![zou::init()],
        ..Default::default()
    });
    js.op_state().borrow_mut().put(held);
    js.op_state().borrow_mut().put(timer::Pending::default());
    js.op_state()
        .borrow_mut()
        .put(websocket::Sockets::default());

    // The prelude is the value of its own last expression, so the two
    // entry points are held here and never on an object the function
    // can reach.
    let entries = js
        .execute_script("zou:prelude.js", include_str!("prelude.js"))
        .map_err(|e| format!("the prelude did not run: {e}"))?;

    let id = js
        .load_main_es_module(&specifier)
        .await
        .map_err(|e| format!("{specifier}: {e}"))?;
    // The module is evaluated by waiting on its own promise and not by
    // running the loop until it is idle. A module that leaves a timer
    // behind it is ordinary rather than exotic, and `createClient` is
    // one: it starts a refresh interval on the way through, and an idle
    // loop is a condition an interval means never happens.
    let evaluated = js.mod_evaluate(id);
    let evaluated = std::pin::pin!(evaluated);
    js.with_event_loop_promise(evaluated, PollEventLoopOptions::default())
        .await
        .map_err(|e| format!("{specifier}: {e}"))?;

    let (entry, drain) = {
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
        (held.pop().expect("two of them"), drain)
    };
    let called = js.call(&entry);
    js.with_event_loop_promise(called, PollEventLoopOptions::default())
        .await
        .map_err(|e| e.to_string())?;

    // The answer goes now, not when this function returns, because
    // what is left to do after it is the function's own business and
    // the caller is not a party to it.
    //
    // A streamed answer has already gone, from inside the op that
    // started it, and the body finished arriving before the handler's
    // promise resolved. So the sink being gone is the whole test for
    // whether the caller has been answered, and having neither a sink
    // nor an answer is the only way to have answered nobody.
    let (answered, sink) = {
        let state = js.op_state();
        let mut state = state.borrow_mut();
        let held = state.borrow_mut::<Held>();
        (held.answered.take(), held.sink.take())
    };
    match (answered, sink) {
        (Some(answer), Some(sink)) => sink(answer),
        (_, None) => {}
        (None, Some(_)) => return Err("the handler returned without an answer".to_string()),
    }

    // Whatever `EdgeRuntime.waitUntil` was given, until it settles or
    // until it has had long enough. A promise nobody resolves is not
    // an error a function is told about, and it is not a thread this
    // process keeps forever either.
    let drained = js.call(&drain);
    let waited = js.with_event_loop_promise(drained, PollEventLoopOptions::default());
    match tokio::time::timeout(BACKGROUND, waited).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(format!(
            "{specifier}: work left after the answer failed: {e}"
        )),
        Err(_) => Err(format!(
            "{specifier}: work left after the answer was still running after {BACKGROUND:?} and was dropped"
        )),
    }
}
