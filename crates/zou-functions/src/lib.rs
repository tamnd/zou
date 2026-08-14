//! The functions half of a Supabase project: what is deployed under a
//! name, and the seam whatever runs it sits behind.
//!
//! An edge function upstream is a directory with an entrypoint in it,
//! `supabase/functions/<name>/index.ts`, calling `Deno.serve(handler)`.
//! The url is `/functions/v1/<name>` and everything after the name is
//! the function's own path. That is the whole of the contract a caller
//! sees, and this crate is the two halves of it that are not a
//! javascript engine: which names are served, and what a call to one
//! looks like going in and coming out.
//!
//! The engine is deliberately not here. A [`Runtime`] is a trait with
//! one method, so the isolate that runs typescript and a handler
//! written in Rust in the host application are the same kind of thing
//! to the server in front of them. That is not a hypothetical: zou is
//! embeddable, and something linking this in to serve its own project
//! usually already has the code it wants to run and no wish for a
//! second language to run it in. The isolate arrives behind a feature
//! flag on top of this seam rather than under it.
//!
//! What is served and what merely exists are two different sets, and
//! the difference is upstream's. A function whose `config.toml` block
//! says `enabled = false` is not answered with a refusal, it is not
//! there at all: the CLI logs `Skipped serving Function: hello` and the
//! url answers the same 404 a name nobody wrote answers. So a
//! [`Registry`] holds what is served, and everything that decides
//! whether a directory makes it that far happens while it is being
//! built.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

mod project;

pub use project::{Layout, Settings, read};

/// One function, as far as anything in front of the runtime cares.
///
/// `verify_jwt` is on this rather than left to the server to look up
/// per request, because it is the one setting that changes what a
/// caller is answered and not what the function does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Function {
    /// The name in the url, which is the directory's name on disk.
    /// Case sensitive and exact: upstream answers `/functions/v1/HELLO`
    /// with the same 404 as a name nobody deployed.
    pub name: String,
    /// The file the runtime starts at. Empty for a function that is
    /// Rust in the host application and has no file anywhere.
    pub entrypoint: PathBuf,
    /// Whether a caller must carry a token this project can verify.
    /// On unless the project's own config says otherwise, which is
    /// upstream's default and the one worth keeping: a function that
    /// forgot to configure anything is a function nobody can call
    /// without a key, rather than one anybody can.
    pub verify_jwt: bool,
    /// The import map the runtime resolves bare specifiers through,
    /// when the project named one.
    pub import_map: Option<PathBuf>,
    /// Files served beside the function, upstream's `static_files`.
    /// Kept whole here because it is the runtime that decides what to
    /// do with them.
    pub static_files: Vec<PathBuf>,
}

impl Function {
    /// A function with upstream's defaults and nothing configured,
    /// which is what a bare directory with an `index.ts` in it is.
    pub fn new(name: &str, entrypoint: PathBuf) -> Function {
        Function {
            name: name.to_string(),
            entrypoint,
            verify_jwt: true,
            import_map: None,
            static_files: Vec::new(),
        }
    }
}

/// How isolates are spent, upstream's `[edge_runtime] policy`.
///
/// It is here rather than in the runtime because the dev loop reads it
/// out of the same file everything else comes from, and a runtime that
/// cannot honour it should be able to say so rather than have to guess
/// what it was asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Policy {
    /// One isolate per function, kept between calls, which is what
    /// makes hot reload worth having. Upstream's default and this one.
    #[default]
    PerWorker,
    /// A fresh isolate per call, thrown away after, which is what the
    /// hosted service does.
    OneShot,
}

impl Policy {
    /// The spelling in `config.toml`, and None for a word that is
    /// neither, so the caller can complain about the file rather than
    /// quietly take a default.
    pub fn named(word: &str) -> Option<Policy> {
        match word {
            "per_worker" => Some(Policy::PerWorker),
            "oneshot" => Some(Policy::OneShot),
            _ => None,
        }
    }
}

/// A request on its way into a function.
///
/// This is deliberately not an http type. The server in front is axum
/// and the runtime behind may be an isolate whose idea of a request is
/// a javascript object, and neither of them should have to agree on a
/// crate to talk to each other through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    pub method: String,
    /// What the handler reads as `req.url`, absolute, with the query
    /// string on it. Upstream hands the isolate its own internal url
    /// rather than the one the caller typed, and what goes here is the
    /// server's decision rather than this crate's.
    pub url: String,
    /// Every header the function is told about, in the order they will
    /// be handed over. Names are lowercase, because that is what a
    /// `Headers` object gives javascript back and a function comparing
    /// them will have been written against that.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Upstream's `SB_EXECUTION_ID`, one per call, which is what a log
    /// line from inside a function is tied to.
    pub execution_id: String,
}

impl Call {
    /// The first value under `name`, which must already be lowercase.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// What a function answered with.
///
/// A body is bytes rather than a stream in this shape, and that is a
/// gap with a name: upstream sends a `ReadableStream` body as it is
/// enqueued, with `Transfer-Encoding: chunked`, and a function written
/// to stream tokens out of a model is written against exactly that.
/// The runtime that can produce one does not exist yet, and inventing
/// the channel before there is anything to put in it would be
/// inventing it twice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Answer {
    /// A 200 carrying bytes and one content type, which is most of
    /// what a function ever answers.
    pub fn new(content_type: &str, body: Vec<u8>) -> Answer {
        Answer {
            status: 200,
            headers: vec![("content-type".to_string(), content_type.to_string())],
            body,
        }
    }
}

/// Whatever runs a function.
///
/// Sync on purpose. A javascript isolate is a thread's worth of state
/// that cannot be moved between threads, and a handler in the host
/// application is ordinary Rust, so both of them are happier being
/// called on a blocking thread than being asked to be a future. The
/// server hands this to `spawn_blocking` the way it does the mail
/// sender.
///
/// An `Err` is what upstream calls a handler that threw: the caller is
/// answered 500 `Internal Server Error` as text and the message goes to
/// the log, which means whatever is in the string is for the operator
/// and never for the caller.
pub trait Runtime: Send + Sync {
    fn invoke(&self, function: &Function, call: Call) -> Result<Answer, String>;

    /// What to call this in a log line and in `zou status`.
    fn describe(&self) -> String;
}

/// A handler that is Rust in the host application.
pub type Handler = Box<dyn Fn(&Call) -> Result<Answer, String> + Send + Sync>;

/// The runtime for an application that embeds zou and wants its own
/// code on the end of `/functions/v1/<name>`.
///
/// There is no file, no isolate and no bundle: a name is registered
/// with a closure and that is the whole function. It exists because the
/// embedded story is a real one rather than a fallback, and because it
/// makes every question in front of the runtime answerable in a test
/// without a javascript engine in the build.
#[derive(Default)]
pub struct Hosted {
    handlers: BTreeMap<String, Handler>,
}

impl Hosted {
    pub fn new() -> Hosted {
        Hosted::default()
    }

    /// Register `name`, verified the way upstream verifies by default.
    pub fn at<F>(mut self, name: &str, handler: F) -> Hosted
    where
        F: Fn(&Call) -> Result<Answer, String> + Send + Sync + 'static,
    {
        self.handlers.insert(name.to_string(), Box::new(handler));
        self
    }

    /// The names registered, in order, which is what the registry
    /// built from this serves.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.handlers.keys().map(String::as_str)
    }
}

impl Runtime for Hosted {
    fn invoke(&self, function: &Function, call: Call) -> Result<Answer, String> {
        match self.handlers.get(&function.name) {
            Some(handler) => handler(&call),
            // Only reachable if a registry was built by hand with a
            // name this runtime has never heard of, which is a wiring
            // mistake rather than a caller's, so it reads like one.
            None => Err(format!("no host handler registered for {}", function.name)),
        }
    }

    fn describe(&self) -> String {
        format!("{} host registered functions", self.handlers.len())
    }
}

/// What a server serves under `/functions/v1`, and the runtime it runs
/// them through.
///
/// Only what is actually served is in here. A directory that is
/// switched off, or has no entrypoint, or is one of the shared
/// directories a project keeps beside its functions, never becomes an
/// entry, so the lookup a request does is the whole of the question
/// `is there a function here`.
pub struct Registry {
    served: BTreeMap<String, Function>,
    runtime: Arc<dyn Runtime>,
}

impl Registry {
    /// A registry over an explicit list, which is what the disk reader
    /// and the tests both end up calling.
    pub fn new(functions: Vec<Function>, runtime: Arc<dyn Runtime>) -> Registry {
        Registry {
            served: functions.into_iter().map(|f| (f.name.clone(), f)).collect(),
            runtime,
        }
    }

    /// Everything a host application registered in Rust, with
    /// upstream's defaults on each: verified, no file, no import map.
    pub fn hosted(hosted: Hosted) -> Registry {
        let functions = hosted
            .names()
            .map(|name| Function::new(name, PathBuf::new()))
            .collect();
        Registry::new(functions, Arc::new(hosted))
    }

    /// The function under `name`, or None, which the caller answers
    /// `Function not found` to.
    pub fn lookup(&self, name: &str) -> Option<&Function> {
        self.served.get(name)
    }

    /// The names served, in order, which is what the dev loop prints
    /// at boot the way `supabase functions serve` does.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.served.keys().map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.served.is_empty()
    }

    /// Run one. Blocking, so the caller owes it a thread that is
    /// allowed to block.
    pub fn invoke(&self, function: &Function, call: Call) -> Result<Answer, String> {
        self.runtime.invoke(function, call)
    }

    pub fn describe(&self) -> String {
        self.runtime.describe()
    }
}

impl std::fmt::Debug for Registry {
    /// By hand because a runtime is a trait object and cannot derive
    /// one, and because what is worth printing about a registry is the
    /// names in it rather than the closures behind them.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("served", &self.served.keys().collect::<Vec<_>>())
            .field("runtime", &self.runtime.describe())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call() -> Call {
        Call {
            method: "POST".to_string(),
            url: "http://127.0.0.1:8081/hello".to_string(),
            headers: vec![("content-type".to_string(), "application/json".to_string())],
            body: b"{}".to_vec(),
            execution_id: "e1".to_string(),
        }
    }

    #[test]
    fn a_host_registered_handler_is_a_function() {
        let hosted = Hosted::new().at("hello", |call| {
            Ok(Answer::new(
                "application/json",
                format!("{{\"method\":\"{}\"}}", call.method).into_bytes(),
            ))
        });
        let registry = Registry::hosted(hosted);
        let found = registry.lookup("hello").expect("registered");
        assert!(found.verify_jwt, "upstream verifies unless told not to");
        let answer = registry.invoke(found, call()).expect("handler answered");
        assert_eq!(answer.status, 200);
        assert_eq!(answer.body, br#"{"method":"POST"}"#);
    }

    #[test]
    fn a_name_nobody_registered_is_not_in_the_registry() {
        let registry = Registry::hosted(
            Hosted::new().at("hello", |_| Ok(Answer::new("text/plain", b"hi".to_vec()))),
        );
        assert!(registry.lookup("nosuch").is_none());
        // Upstream is case sensitive about this, and the 404 it
        // answers /functions/v1/HELLO is the same one it answers a
        // name nobody deployed.
        assert!(registry.lookup("HELLO").is_none());
    }

    #[test]
    fn a_handler_that_fails_says_so_to_the_operator_and_not_to_the_caller() {
        let registry =
            Registry::hosted(Hosted::new().at("boom", |_| Err("connection refused".to_string())));
        let found = registry.lookup("boom").expect("registered");
        let why = registry.invoke(found, call()).expect_err("it fails");
        assert_eq!(why, "connection refused");
    }

    #[test]
    fn a_call_reads_its_own_headers_lowercase() {
        assert_eq!(call().header("content-type"), Some("application/json"));
        assert_eq!(call().header("Content-Type"), None);
    }

    #[test]
    fn the_policy_words_are_the_files_own() {
        assert_eq!(Policy::named("per_worker"), Some(Policy::PerWorker));
        assert_eq!(Policy::named("oneshot"), Some(Policy::OneShot));
        assert_eq!(Policy::named("one_shot"), None);
        assert_eq!(Policy::default(), Policy::PerWorker);
    }
}
