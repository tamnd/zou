//! The javascript half of edge functions: a v8 isolate that runs the
//! `index.ts` a project deployed, behind the `isolate` feature.
//!
//! The feature is the point of this crate being its own crate. V8 is
//! fifty one megabytes of static library, measured against the same
//! binary built both ways, and swc is a minute of build time, and a zou
//! that serves a database and a bucket should pay for neither. With the
//! feature off this compiles to nothing at all.
//!
//! What is here is `Isolate`, which implements
//! [`zou_functions::Runtime`], and which is spelled without a link
//! because with the feature off there is no such item to link to and
//! these docs are built the way a default build is.
//! Everything a call meets before and after it is in `zou-server` and
//! knows nothing about v8, so the two halves can be worked on
//! separately and the server's tests do not need an engine to run.
//!
//! # What a function gets, and what it does not
//!
//! Deno's own `Headers`, `Request` and `Response` live in `deno_fetch`,
//! which arrives with an HTTP client and a second TLS stack behind it.
//! Rather than take that by accident, this runtime carries its own
//! small implementation of the shapes a handler is written against, and
//! the gaps are written down here rather than found out in production:
//!
//! - `crypto.subtle` is a digest and HMAC and no more: no encryption,
//!   no key derivation, no asymmetric algorithms, each refused by name.
//! - Streams are readable only. There is no `WritableStream` and so no
//!   `pipeTo`, and no byte stream and no BYOB reader. A response body
//!   reaches the caller in chunks, and a body coming back from `fetch`
//!   is still collected before the handler sees it.
//! - A timer only fires while there is a call for it to fire in,
//!   which is until the answer plus whatever `EdgeRuntime.waitUntil`
//!   is still waiting for, and thirty seconds is the ceiling on that.
//! - There are no node built ins, so `node:` is refused by name and a
//!   package that reaches for one will not run.
//!
//! What is there is `Headers`, `Request`, `Response`, `URL`,
//! `URLSearchParams`, `Blob`, `File`, `FormData`, `crypto`, the timers,
//! `EdgeRuntime.waitUntil`, `WebSocket` and the events it dispatches,
//! `ReadableStream`,
//! `TextEncoder`, `TextDecoder`, `atob`, `btoa`, `console`,
//! `Deno.serve`, `Deno.env` and `fetch`, which is enough to run a
//! handler that reads a request, calls something else and answers, and
//! enough for `@supabase/supabase-js` to build a client.
//!
//! `URL` is the `url` crate behind two ops, which is the same parser
//! Deno's own is built on, rather than a few hundred lines of
//! javascript that is wrong in the corners.
//!
//! `Blob`, `File` and `FormData` are javascript, because a blob is
//! bytes with a media type on it and a form is a list of pairs and two
//! wire formats, none of which is work for the host.
//!
//! `crypto` is the other way around: randomness is the operating
//! system's and a hash is `sha2` and `hmac`, which is the same code
//! this server signs its own tokens with.
//!
//! The timers are a sleeping op and the bookkeeping around it, so a
//! handler waiting on the clock is the event loop's business and not a
//! thread's, and clearing one cancels the sleep rather than leaving it
//! pending with nobody wanting the answer.
//!
//! `EdgeRuntime.waitUntil` is why `Runtime::invoke_answering` exists:
//! the answer is handed over the moment the handler has one and the
//! call goes on afterwards, so a caller is never made to wait for work
//! that was moved off its critical path on purpose.
//!
//! Streams are javascript for the same reason bodies are: a readable
//! stream is a queue, the readers waiting on it and a source's three
//! functions, and none of that is work for the host. Every body is one,
//! whichever way it was made, which is what `req.body` and `res.body`
//! being real means.
//!
//! A response built out of a stream is the one place that reaches the
//! host: the head is handed over when the handler returns and every
//! chunk goes down a channel as it is enqueued, so the caller reads the
//! first one while the function is still making the last. The op that
//! hands a chunk over is awaited, which is where backpressure comes
//! from: a function generating faster than the caller reads waits.
//!
//! `WebSocket` is the client half of a protocol this repository already
//! speaks the server half of, so it is `tokio-tungstenite` with its
//! client features on rather than a second frame codec. What is
//! javascript is the object: four states, four events and a read loop.
//!
//! `fetch` is the client `zou-server` calls a database webhook with,
//! behind an op, rather than a second HTTP stack linked in beside it.
//!
//! `npm:` and `jsr:` specifiers are fetched from a registry that serves
//! packages as modules, esm.sh by default, and kept on disk after the
//! first time. There is no node module resolution here and no CJS: what
//! runs is the registry's build of the package.
//!
//! Typescript is real: `deno_ast` is the same swc transpiler Deno uses,
//! so what runs is what would run there.

#[cfg(not(feature = "isolate"))]
mod absent;
#[cfg(feature = "isolate")]
mod crypto;
#[cfg(feature = "isolate")]
mod fetch;
#[cfg(feature = "isolate")]
mod isolate;
#[cfg(feature = "isolate")]
mod limits;
#[cfg(feature = "isolate")]
mod module;
#[cfg(feature = "isolate")]
mod pool;
#[cfg(feature = "isolate")]
mod timer;
#[cfg(feature = "isolate")]
mod url;
#[cfg(feature = "isolate")]
mod websocket;

#[cfg(feature = "isolate")]
pub use isolate::Isolate;
#[cfg(feature = "isolate")]
pub use limits::Limits;

/// What this build runs a function with, given the environment every
/// function of the project will see and the policy the project's
/// `config.toml` asked for.
///
/// The switch lives here rather than in whatever links this crate,
/// because the question "is there an engine" is this crate's own and
/// answering it anywhere else means a second place that can be wrong.
/// It is also why the dependency on this crate is not optional: a
/// `--all-features` build of the workspace should not pull V8 in by
/// accident, so the feature that does it is spelled `zou-deno/isolate`
/// and nothing enables it on anyone's behalf.
pub fn engine(
    env: Vec<(String, String)>,
    policy: zou_functions::Policy,
) -> std::sync::Arc<dyn zou_functions::Runtime> {
    #[cfg(feature = "isolate")]
    {
        std::sync::Arc::new(isolate::Isolate::with_env(env).with_policy(policy))
    }
    #[cfg(not(feature = "isolate"))]
    {
        let _ = (env, policy);
        std::sync::Arc::new(absent::Absent)
    }
}

/// Whether this build has an engine in it.
///
/// A server can say so at boot rather than answering a call with a
/// puzzle, which is the whole reason a feature flag needs a way to be
/// read at runtime.
pub const fn available() -> bool {
    cfg!(feature = "isolate")
}
