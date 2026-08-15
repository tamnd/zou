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
mod secrets;

pub use project::{Layout, Settings, read};
pub use secrets::{env_file, read as secrets};

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
#[derive(Debug)]
pub struct Answer {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Body,
}

/// A body, which is either all of it or the reading end of a pipe it
/// is still being written to.
///
/// The second is what a `ReadableStream` body is upstream: the headers
/// go out when the handler returns, the chunks go out as they are
/// enqueued with `Transfer-Encoding: chunked`, and a caller reads the
/// first token out of a model long before the last one exists. It is a
/// channel rather than a trait because both ends of it are already
/// decided by the shape of everything else here: whatever makes the
/// chunks is on a blocking thread of its own, and whoever writes them
/// to a socket is on the server's executor.
#[derive(Debug)]
pub enum Body {
    /// All of it, which is what a handler that returned a string, a
    /// buffer, or nothing at all answered with.
    Bytes(Vec<u8>),
    /// Not all of it yet.
    Chunks(Chunks),
}

/// How many chunks may be waiting before whatever is writing them is
/// made to wait too.
///
/// There is a number here because a function generating faster than
/// the caller reads is otherwise a function that may hold as much
/// memory as it likes. Eight is enough that a writer is not stopped
/// and started for every chunk, and small enough that a slow reader is
/// felt quickly, which is the whole of what backpressure has to be.
const AHEAD: usize = 8;

/// The reading end of a body that is still arriving.
///
/// An `Err` in it is a body that went wrong after its headers were
/// sent, which cannot be turned back into a status code: the caller is
/// already reading a 200. All that can be done with it is to stop, and
/// a truncated response is exactly what an http client is shown when a
/// chunked body ends early, which is the right thing for it to see.
#[derive(Debug)]
pub struct Chunks(tokio::sync::mpsc::Receiver<Result<Vec<u8>, String>>);

/// The writing end of one.
#[derive(Debug, Clone)]
pub struct Writer(tokio::sync::mpsc::Sender<Result<Vec<u8>, String>>);

impl Chunks {
    /// The next chunk, and none when there are no more.
    pub async fn next(&mut self) -> Option<Result<Vec<u8>, String>> {
        self.0.recv().await
    }

    /// All of it, on a thread that is allowed to block and that is not
    /// the one making the chunks. That is what wanting the whole
    /// answer as one value costs, and it is how the blocking shape of
    /// a runtime call gets bytes back however they were made.
    pub fn collect_blocking(mut self) -> Result<Vec<u8>, String> {
        let mut all = Vec::new();
        while let Some(chunk) = self.0.blocking_recv() {
            all.extend_from_slice(&chunk?);
        }
        Ok(all)
    }
}

impl Writer {
    /// Hand over one chunk, waiting while the reader is behind.
    ///
    /// False means nobody is listening any more, which is a caller that
    /// went away rather than a failure: whatever is generating should
    /// stop, and there is nobody left to tell about it.
    pub async fn write(&self, chunk: Vec<u8>) -> bool {
        self.0.send(Ok(chunk)).await.is_ok()
    }

    /// End it badly. Dropped rather than waited for when the reader is
    /// behind, because this is the last thing said on a body that is
    /// ending either way.
    pub fn fail(self, why: String) {
        let _ = self.0.try_send(Err(why));
    }
}

impl Answer {
    /// A 200 carrying bytes and one content type, which is most of
    /// what a function ever answers.
    pub fn new(content_type: &str, body: Vec<u8>) -> Answer {
        Answer {
            status: 200,
            headers: vec![("content-type".to_string(), content_type.to_string())],
            body: Body::Bytes(body),
        }
    }

    /// The head of an answer whose body is still being made, and the
    /// end that body is written to. The answer can be handed on the
    /// moment this returns, which is the entire point of it.
    pub fn streaming(status: u16, headers: Vec<(String, String)>) -> (Answer, Writer) {
        let (sender, receiver) = tokio::sync::mpsc::channel(AHEAD);
        (
            Answer {
                status,
                headers,
                body: Body::Chunks(Chunks(receiver)),
            },
            Writer(sender),
        )
    }

    /// The bytes, for an answer that has them, and empty for one still
    /// arriving. Only something that asked for the answer the moment
    /// there was one can be holding the second.
    pub fn bytes(&self) -> &[u8] {
        match &self.body {
            Body::Bytes(bytes) => bytes,
            Body::Chunks(_) => &[],
        }
    }
}

/// Why a call did not end in an answer.
///
/// Two of them because upstream answers two different things. A
/// function that threw is a 500 in plain text. A function that ran past
/// what it was allowed is a 546 with `WORKER_LIMIT` in it, which is a
/// status code nothing else in this project uses and upstream's own
/// invention. Both carry a sentence, and both sentences are the
/// operator's: they go to the log and never to the caller.
#[derive(Debug, PartialEq, Eq)]
pub enum Failed {
    /// It threw, or it never answered, or its module would not load.
    Threw(String),
    /// It ran past its memory, its wall clock or its cpu time.
    Limit(String),
}

impl Failed {
    /// The sentence, whichever kind this is, because the log line is
    /// the same either way.
    pub fn why(&self) -> &str {
        match self {
            Failed::Threw(why) | Failed::Limit(why) => why,
        }
    }
}

impl std::fmt::Display for Failed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.why())
    }
}

/// Anything that went wrong and did not say it was a limit is a
/// function that threw, which is what lets a runtime keep saying
/// `map_err(|e| format!(...))?` about the ordinary failures.
impl From<String> for Failed {
    fn from(why: String) -> Failed {
        Failed::Threw(why)
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
/// An `Err` is what upstream calls a handler that threw or a handler
/// that was stopped: the caller is answered 500 or 546 and the message
/// goes to the log, which means whatever is in the string is for the
/// operator and never for the caller.
pub trait Runtime: Send + Sync {
    fn invoke(&self, function: &Function, call: Call) -> Result<Answer, Failed>;

    /// The same call, with the answer handed over the moment there is
    /// one rather than when the call is finished.
    ///
    /// The two are the same moment for a runtime with nothing after
    /// the answer, which is what the default here says. They are not
    /// the same moment for the isolate: `EdgeRuntime.waitUntil` is
    /// work that outlives the response, and the caller is not made to
    /// wait for it. Nor is an answer whose body is [`Body::Chunks`]
    /// finished when it is handed over, which is why a runtime that
    /// streams must be called this way and not through `invoke`.
    ///
    /// An `Err` after the answer has been handed over is the
    /// background work's and not the caller's, so it is logged and
    /// nobody is told about it.
    fn invoke_answering(
        &self,
        function: &Function,
        call: Call,
        answer: Sink,
    ) -> Result<(), Failed> {
        answer(self.invoke(function, call)?);
        Ok(())
    }

    /// What to call this in a log line and in `zou status`.
    fn describe(&self) -> String;
}

/// Where an answer goes as soon as the handler has one.
pub type Sink = Box<dyn FnOnce(Answer) + Send>;

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
    /// A host handler is Rust that either answers or does not, and
    /// there is no isolate here to run out of anything, so everything
    /// that goes wrong on this side is a function that threw.
    fn invoke(&self, function: &Function, call: Call) -> Result<Answer, Failed> {
        match self.handlers.get(&function.name) {
            Some(handler) => handler(&call).map_err(Failed::Threw),
            // Only reachable if a registry was built by hand with a
            // name this runtime has never heard of, which is a wiring
            // mistake rather than a caller's, so it reads like one.
            None => Err(Failed::Threw(format!(
                "no host handler registered for {}",
                function.name
            ))),
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
    pub fn invoke(&self, function: &Function, call: Call) -> Result<Answer, Failed> {
        self.runtime.invoke(function, call)
    }

    /// Run one, and hand the answer over as soon as the handler has
    /// one. This returns when the runtime is finished, which is later
    /// than the answer whenever the function left work behind it.
    pub fn invoke_answering(
        &self,
        function: &Function,
        call: Call,
        answer: Sink,
    ) -> Result<(), Failed> {
        self.runtime.invoke_answering(function, call, answer)
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
        assert_eq!(answer.bytes(), br#"{"method":"POST"}"#);
    }

    #[tokio::test]
    async fn a_streamed_body_arrives_a_chunk_at_a_time_and_ends() {
        let (answer, writer) = Answer::streaming(200, Vec::new());
        let written = tokio::spawn(async move {
            assert!(writer.write(b"one ".to_vec()).await);
            assert!(writer.write(b"two".to_vec()).await);
        });
        let Body::Chunks(mut chunks) = answer.body else {
            panic!("a streamed answer");
        };
        assert_eq!(chunks.next().await, Some(Ok(b"one ".to_vec())));
        assert_eq!(chunks.next().await, Some(Ok(b"two".to_vec())));
        written.await.expect("the writer");
        // The writer is dropped, which is how a body ends.
        assert_eq!(chunks.next().await, None);
    }

    #[tokio::test]
    async fn a_body_that_went_wrong_says_so_where_a_status_code_cannot() {
        let (answer, writer) = Answer::streaming(200, Vec::new());
        assert!(writer.write(b"half of it".to_vec()).await);
        writer.fail("the model hung up".to_string());
        let Body::Chunks(mut chunks) = answer.body else {
            panic!("a streamed answer");
        };
        assert_eq!(chunks.next().await, Some(Ok(b"half of it".to_vec())));
        assert_eq!(
            chunks.next().await,
            Some(Err("the model hung up".to_string()))
        );
        assert_eq!(chunks.next().await, None);
    }

    #[tokio::test]
    async fn a_caller_that_went_away_is_told_to_whatever_is_writing() {
        let (answer, writer) = Answer::streaming(200, Vec::new());
        drop(answer);
        assert!(!writer.write(b"nobody is reading".to_vec()).await);
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
        assert_eq!(why, Failed::Threw("connection refused".to_string()));
        assert_eq!(why.why(), "connection refused");
    }

    #[test]
    fn a_limit_is_not_a_function_that_threw() {
        // The two are different answers to the caller, 546 and 500, so
        // whatever runs a function has to be able to say which one it
        // is and a string cannot.
        let limit = Failed::Limit("it used more than 256 MiB of memory".to_string());
        assert!(matches!(limit, Failed::Limit(_)));
        assert_eq!(limit.to_string(), "it used more than 256 MiB of memory");
        assert_eq!(
            Failed::from("it threw".to_string()),
            Failed::Threw("it threw".to_string()),
            "anything that did not say it was a limit is not one"
        );
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
