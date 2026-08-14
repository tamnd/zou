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

use deno_core::{JsRuntime, OpState, PollEventLoopOptions, RuntimeOptions, op2, v8};
use zou_functions::{Answer, Call, Function, Runtime};

use crate::{crypto, fetch, module, url};

/// What the isolate is told about the call it is running, and what it
/// left behind afterwards. Both live in the runtime's op state, which
/// is the only place ops can reach.
struct Held {
    call: Call,
    peer: String,
    env: Vec<(String, String)>,
    answered: Option<Answer>,
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

/// What the handler answered.
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
        body: body.to_vec(),
    });
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
        op_zou_env_get,
        op_zou_env,
        crypto::op_zou_random,
        crypto::op_zou_digest,
        crypto::op_zou_sign,
        crypto::op_zou_verify,
        fetch::op_zou_fetch,
        url::op_zou_url_parse,
        url::op_zou_url_set
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

impl Runtime for Isolate {
    fn invoke(&self, function: &Function, call: Call) -> Result<Answer, String> {
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

async fn run(specifier: deno_core::ModuleSpecifier, held: Held) -> Result<Answer, String> {
    let mut js = JsRuntime::new(RuntimeOptions {
        module_loader: Some(module::loader()),
        extensions: vec![zou::init()],
        ..Default::default()
    });
    js.op_state().borrow_mut().put(held);

    // The prelude is the value of its own last expression, so the entry
    // point is held here and never on an object the function can reach.
    let entry = js
        .execute_script("zou:prelude.js", include_str!("prelude.js"))
        .map_err(|e| format!("the prelude did not run: {e}"))?;

    let id = js
        .load_main_es_module(&specifier)
        .await
        .map_err(|e| format!("{specifier}: {e}"))?;
    let evaluated = js.mod_evaluate(id);
    js.run_event_loop(PollEventLoopOptions::default())
        .await
        .map_err(|e| format!("{specifier}: {e}"))?;
    evaluated.await.map_err(|e| format!("{specifier}: {e}"))?;

    let entry = {
        let context = js.main_context();
        let isolate = &mut *js.v8_isolate();
        v8::scope_with_context!(let scope, isolate, context);
        let value = v8::Local::new(scope, entry);
        let function: v8::Local<v8::Function> = value
            .try_into()
            .map_err(|_| "the prelude did not end in a function".to_string())?;
        v8::Global::new(scope, function)
    };
    let called = js.call(&entry);
    js.with_event_loop_promise(called, PollEventLoopOptions::default())
        .await
        .map_err(|e| e.to_string())?;

    let state = js.op_state();
    let mut state = state.borrow_mut();
    let held = state.borrow_mut::<Held>();
    held.answered
        .take()
        .ok_or_else(|| "the handler returned without an answer".to_string())
}
