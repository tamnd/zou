//! The javascript half of edge functions: a v8 isolate that runs the
//! `index.ts` a project deployed, behind the `isolate` feature.
//!
//! The feature is the point of this crate being its own crate. V8 is
//! forty megabytes of static library and swc is a minute of build time,
//! and a zou that serves a database and a bucket should pay for neither.
//! With the feature off this compiles to nothing at all.
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
//! - `fetch` is not there. A function that calls out is the next change.
//! - `crypto` is not there, and `crypto.subtle` least of all.
//! - Streams are not there. A `ReadableStream` body throws by name.
//! - `URL` and `URLSearchParams` are not there.
//! - Timers are not there, so a handler that sleeps will not.
//! - `npm:`, `jsr:` and `https:` specifiers are refused by name.
//!
//! What is there is `Headers`, `Request`, `Response`, `TextEncoder`,
//! `TextDecoder`, `atob`, `btoa`, `console`, `Deno.serve` and
//! `Deno.env`, which is enough to run a handler that reads a request and
//! answers it, and enough to prove the seam works end to end.
//!
//! Typescript is real: `deno_ast` is the same swc transpiler Deno uses,
//! so what runs is what would run there.

#[cfg(feature = "isolate")]
mod isolate;
#[cfg(feature = "isolate")]
mod module;

#[cfg(feature = "isolate")]
pub use isolate::Isolate;

/// Whether this build has an engine in it.
///
/// A server can say so at boot rather than answering a call with a
/// puzzle, which is the whole reason a feature flag needs a way to be
/// read at runtime.
pub const fn available() -> bool {
    cfg!(feature = "isolate")
}
