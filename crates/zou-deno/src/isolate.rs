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

use crate::limits::{Limits, Watch};
use crate::{crypto, fetch, limits, module, pool, timer, url, websocket};

/// What the isolate is told about the call it is running, and what it
/// left behind afterwards. Both live in the runtime's op state, which
/// is the only place ops can reach.
pub(crate) struct Held {
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
        if self.policy == Policy::PerWorker {
            // Somebody else's thread, which already has an isolate with
            // this function in it, or is about to.
            return self.pool.run(&specifier, held, self.limits);
        }
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("the isolate could not have a runtime: {e}"))?;
        let limits = self.limits;
        tokio.block_on(once_off(specifier, held, limits))
    }

    fn describe(&self) -> String {
        match self.policy {
            Policy::OneShot => "a v8 isolate per call".to_string(),
            Policy::PerWorker => "a v8 isolate per function, kept between calls".to_string(),
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
    specifier: deno_core::ModuleSpecifier,
    held: Held,
    limits: Limits,
) -> Result<(), Failed> {
    let named = specifier.to_string();
    let call = async move {
        let mut ready = Ready::new(specifier, limits).await?;
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
    drain: v8::Global<v8::Function>,
    watch: Arc<Watch>,
    handle: v8::IsolateHandle,
    specifier: deno_core::ModuleSpecifier,
    limits: Limits,
    /// The files that went into it, for the question hot reload asks.
    read: module::Reads,
}

impl Ready {
    pub(crate) async fn new(
        specifier: deno_core::ModuleSpecifier,
        limits: Limits,
    ) -> Result<Ready, Failed> {
        build(specifier, limits).await
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
}

async fn build(specifier: deno_core::ModuleSpecifier, limits: Limits) -> Result<Ready, Failed> {
    // Everything that stops this call is set up before any of the
    // function's own code has run, including the module it is in, and
    // two of the three have to be in place before the isolate exists at
    // all rather than after it.
    let watch = Watch::new(limits);
    let (loader, read) = module::loader();
    let mut js = JsRuntime::new(RuntimeOptions {
        module_loader: Some(loader),
        extensions: vec![zou::init()],
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
    let watchdog = limits::watch(handle.clone(), Arc::clone(&watch), limits);
    js.add_near_heap_limit_callback(limits::near_heap_limit(handle.clone(), Arc::clone(&watch)));
    js.op_state().borrow_mut().put(timer::Pending::default());
    js.op_state()
        .borrow_mut()
        .put(websocket::Sockets::default());

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
    watch
        .timing(js.with_event_loop_promise(evaluated, PollEventLoopOptions::default()))
        .await
        .map_err(|e| why(&specifier, &watch, e))?;

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
    drop(watchdog);
    Ok(Ready {
        js,
        entry,
        drain,
        watch,
        handle,
        specifier,
        limits,
        read,
    })
}

impl Ready {
    /// One call in this isolate.
    pub(crate) async fn once(&mut self, held: Held) -> Result<(), Failed> {
        let Ready {
            js,
            entry,
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

        // The same again, and this is the one that matters: a handler
        // that is not async runs to its end inside `call`, so a
        // function that never returns never returns from here.
        let called = {
            let _running = watch.running();
            js.call(entry)
        };
        watch
            .timing(js.with_event_loop_promise(called, PollEventLoopOptions::default()))
            .await
            .map_err(|e| why(specifier, watch, e))?;

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
