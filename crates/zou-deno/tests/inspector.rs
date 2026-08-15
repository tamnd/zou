//! A debugger attaching to a function, which is upstream's
//! `inspector_port`.
//!
//! Every test here talks to a real port with the real protocol: the
//! json endpoints a debugger finds its target through, and a websocket
//! carrying Chrome DevTools Protocol messages into a real isolate. A
//! test that asserted the port was open would not have told anybody
//! whether a debugger could do anything once it got there.

#![cfg(feature = "isolate")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use zou_deno::Isolate;
use zou_functions::{Call, Function, Policy, Runtime};

/// A project with one function that answers with a constant, and holds
/// a value at the top of its module for a debugger to ask about.
fn project() -> (tempfile::TempDir, Function) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let at = dir.path().join("functions/hello");
    std::fs::create_dir_all(&at).expect("the function's directory");
    std::fs::write(
        at.join("index.ts"),
        r#"
const kept = "a value the module made";
globalThis.kept = kept;
function answer() {
  return new Response(kept);
}
Deno.serve(answer);
"#,
    )
    .expect("the function");
    let entrypoint: PathBuf = at.join("index.ts");
    (dir, Function::new("hello", entrypoint))
}

fn call() -> Call {
    Call {
        method: "GET".to_string(),
        url: "http://localhost:9000/functions/v1/hello".to_string(),
        headers: Vec::new(),
        body: Vec::new(),
        execution_id: "one".to_string(),
    }
}

/// One GET against the inspector's HTTP side, which is the half of the
/// protocol that is not a websocket.
fn get(at: SocketAddr, path: &str) -> String {
    let mut socket = TcpStream::connect(at).expect("the inspector's port");
    socket
        .write_all(format!("GET {path} HTTP/1.1\r\nhost: localhost\r\n\r\n").as_bytes())
        .expect("a request");
    let mut said = String::new();
    socket.read_to_string(&mut said).expect("an answer");
    let (head, body) = said.split_once("\r\n\r\n").expect("a whole answer");
    assert!(
        head.starts_with("HTTP/1.1 200 OK") || head.starts_with("HTTP/1.1 404"),
        "{head}"
    );
    body.to_string()
}

/// The targets, once there is one. The isolate is built by the call
/// before this, so nothing here waits for longer than a machine under
/// load needs to finish putting it in the table.
fn targets(at: SocketAddr) -> Vec<serde_json::Value> {
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let said = get(at, "/json/list");
        let listed: Vec<serde_json::Value> = serde_json::from_str(&said).expect("a json array");
        if !listed.is_empty() || Instant::now() > until {
            return listed;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A session on a target, in the shape a debugger holds one.
struct Debugger {
    socket: tungstenite::WebSocket<tungstenite::stream::MaybeTlsStream<TcpStream>>,
    last: i64,
    /// Notifications that arrived while an answer was being waited for.
    ///
    /// They are kept rather than dropped because the interesting ones
    /// come first: enabling a domain is answered after everything that
    /// enabling it announced.
    seen: Vec<serde_json::Value>,
}

impl Debugger {
    fn attach(url: &str) -> Debugger {
        let (socket, _) = tungstenite::connect(url).expect("a session");
        // A debugger that is waiting for something which is never going
        // to arrive should say so rather than hold a CI job until it is
        // killed for taking an hour.
        if let tungstenite::stream::MaybeTlsStream::Plain(stream) = socket.get_ref() {
            stream
                .set_read_timeout(Some(Duration::from_secs(60)))
                .expect("a socket with a clock on it");
        }
        Debugger {
            socket,
            last: 0,
            seen: Vec::new(),
        }
    }

    /// One command, and the answer to that command.
    ///
    /// Notifications arrive down the same socket and in between, which
    /// is what the id is for: the answer to a command is the message
    /// carrying the id the command went out with.
    fn ask(&mut self, method: &str, params: serde_json::Value) -> serde_json::Value {
        self.last += 1;
        let id = self.last;
        let sent = serde_json::json!({ "id": id, "method": method, "params": params });
        self.socket
            .send(tungstenite::Message::text(sent.to_string()))
            .expect("a command");
        loop {
            let said = self.said();
            if said["id"].as_i64() == Some(id) {
                return said;
            }
            self.seen.push(said);
        }
    }

    /// The next message of any kind.
    fn said(&mut self) -> serde_json::Value {
        loop {
            match self.socket.read().expect("an answer") {
                tungstenite::Message::Text(text) => {
                    return serde_json::from_str(&text).expect("json");
                }
                tungstenite::Message::Close(_) => panic!("the session ended"),
                _ => {}
            }
        }
    }

    /// Messages until one of them is `named`, which is how a
    /// notification is waited for.
    fn until(&mut self, named: &str) -> serde_json::Value {
        if let Some(at) = self
            .seen
            .iter()
            .position(|said| said["method"].as_str() == Some(named))
        {
            return self.seen.remove(at);
        }
        for _ in 0..200 {
            let said = self.said();
            if said["method"].as_str() == Some(named) {
                return said;
            }
        }
        panic!("{named} never arrived");
    }
}

/// A runtime with a debugger port on it, and the function it serves.
///
/// Port zero, so that the tests here can run beside each other and
/// beside whatever else is on the machine.
fn debugging() -> (tempfile::TempDir, Function, Isolate, SocketAddr) {
    let (dir, function) = project();
    let isolate = Isolate::new()
        .with_policy(Policy::PerWorker)
        .with_inspector(0)
        .expect("a port");
    let at = isolate.debugging_at().expect("a port it bound");
    (dir, function, isolate, at)
}

#[test]
fn the_port_says_what_it_is_before_anything_is_running() {
    let (_dir, _function, _isolate, at) = debugging();
    let said: serde_json::Value = serde_json::from_str(&get(at, "/json/version")).expect("json");
    assert_eq!(said["Protocol-Version"], "1.3");
    assert!(
        said["V8-Version"].as_str().is_some_and(|v| v.contains('.')),
        "a debugger is told which engine it is talking to: {said}"
    );
    let listed: Vec<serde_json::Value> =
        serde_json::from_str(&get(at, "/json/list")).expect("json");
    assert!(
        listed.is_empty(),
        "nothing has been called, so there is nothing to debug: {listed:?}"
    );
}

#[test]
fn a_function_that_has_been_called_is_a_target() {
    let (_dir, function, isolate, at) = debugging();
    let answer = isolate.invoke(&function, call()).expect("an answer");
    assert_eq!(answer.bytes(), b"a value the module made");
    let listed = targets(at);
    assert_eq!(listed.len(), 1, "{listed:?}");
    let target = &listed[0];
    assert_eq!(target["title"], "hello", "the function's own name");
    assert!(
        target["url"]
            .as_str()
            .is_some_and(|url| url.ends_with("functions/hello/index.ts")),
        "the file a debugger opens: {target}"
    );
    assert!(
        target["webSocketDebuggerUrl"]
            .as_str()
            .is_some_and(|url| url.starts_with("ws://127.0.0.1:")),
        "{target}"
    );
}

/// The whole point of the port: a debugger evaluating an expression in
/// the isolate that ran the function, between two calls rather than
/// during one, which is what `per_worker` keeping the isolate is for.
#[test]
fn a_debugger_evaluates_in_the_isolate_that_ran_the_function() {
    let (_dir, function, isolate, at) = debugging();
    isolate.invoke(&function, call()).expect("an answer");
    let listed = targets(at);
    let url = listed[0]["webSocketDebuggerUrl"].as_str().expect("a url");
    let mut debugger = Debugger::attach(url);
    debugger.ask("Runtime.enable", serde_json::json!({}));
    let said = debugger.ask(
        "Runtime.evaluate",
        serde_json::json!({ "expression": "globalThis.kept", "returnByValue": true }),
    );
    assert_eq!(
        said["result"]["result"]["value"], "a value the module made",
        "the module's own state is still there to be asked about: {said}"
    );
    // And the function still answers afterwards, because a session is
    // something beside the isolate rather than something in the way of
    // it.
    let again = isolate.invoke(&function, call()).expect("an answer");
    assert_eq!(again.bytes(), b"a value the module made");
}

/// A debugger's real first move, which is asking for the source. The
/// answer proves the file on disk and the script in the isolate are the
/// same thing to a debugger, which is what a breakpoint needs.
#[test]
fn the_functions_source_is_what_a_debugger_is_shown() {
    let (_dir, function, isolate, at) = debugging();
    isolate.invoke(&function, call()).expect("an answer");
    let listed = targets(at);
    let url = listed[0]["webSocketDebuggerUrl"].as_str().expect("a url");
    let mut debugger = Debugger::attach(url);
    debugger.ask("Debugger.enable", serde_json::json!({}));
    // Every script the isolate has is announced when a debugger enables
    // the domain, so the function's own is among them.
    let mut script = None;
    for _ in 0..200 {
        let said = debugger.until("Debugger.scriptParsed");
        if said["params"]["url"]
            .as_str()
            .is_some_and(|url| url.ends_with("functions/hello/index.ts"))
        {
            script = said["params"]["scriptId"].as_str().map(str::to_string);
            break;
        }
    }
    let script = script.expect("the function's own script");
    let said = debugger.ask(
        "Debugger.getScriptSource",
        serde_json::json!({ "scriptId": script }),
    );
    let source = said["result"]["scriptSource"].as_str().expect("source");
    assert!(
        source.contains("a value the module made"),
        "the source a debugger reads is the function's: {source}"
    );
}

/// A breakpoint, which is the reason any of this exists. The call is
/// made from a thread of its own because a function stopped at a
/// breakpoint has not answered yet, and the debugger is what lets it
/// go.
#[test]
fn a_breakpoint_stops_the_function_and_the_debugger_lets_it_go() {
    let (_dir, function, isolate, at) = debugging();
    isolate.invoke(&function, call()).expect("an answer");
    let listed = targets(at);
    let url = listed[0]["webSocketDebuggerUrl"].as_str().expect("a url");
    let mut debugger = Debugger::attach(url);
    debugger.ask("Debugger.enable", serde_json::json!({}));
    let file = listed[0]["url"].as_str().expect("a file").to_string();
    let set = debugger.ask(
        "Debugger.setBreakpointByUrl",
        // The line inside the handler, which is the one a call runs
        // and the module's own evaluation did not.
        serde_json::json!({ "lineNumber": 4, "url": file, "columnNumber": 0 }),
    );
    assert!(
        set["result"]["breakpointId"].as_str().is_some(),
        "the breakpoint was accepted: {set}"
    );
    let isolate = std::sync::Arc::new(isolate);
    let calling = std::sync::Arc::clone(&isolate);
    let called = std::thread::spawn(move || calling.invoke(&function, call()));
    let paused = debugger.until("Debugger.paused");
    assert_eq!(
        paused["params"]["reason"], "other",
        "a breakpoint, rather than an exception: {paused}"
    );
    debugger.ask("Debugger.resume", serde_json::json!({}));
    let answer = called
        .join()
        .expect("the call came back")
        .expect("answered");
    assert_eq!(
        answer.bytes(),
        b"a value the module made",
        "a function that was stopped and let go answers the way it would have"
    );
}

/// An isolate that has gone leaves the list, so a debugger is never
/// offered a target that cannot answer it.
#[test]
fn a_target_goes_when_its_isolate_does() {
    let (_dir, function, isolate, at) = debugging();
    isolate.invoke(&function, call()).expect("an answer");
    assert_eq!(targets(at).len(), 1);
    drop(isolate);
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let listed: Vec<serde_json::Value> =
            serde_json::from_str(&get(at, "/json/list")).expect("json");
        if listed.is_empty() {
            return;
        }
        assert!(Instant::now() < until, "the target stayed: {listed:?}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A build that was not asked for a debugger does not have one, which
/// is the case every deployment is in.
#[test]
fn no_port_asked_for_is_no_port_open() {
    let isolate = Isolate::new();
    assert!(isolate.debugging_at().is_none());
}

/// A port somebody else is already on is a sentence rather than a
/// panic, because the thing that reads this out of a config file is a
/// server that has a database to serve as well.
#[test]
fn a_port_already_in_use_says_so() {
    let held = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
    let port = held.local_addr().expect("a port").port();
    let why = Isolate::new()
        .with_inspector(port)
        .err()
        .expect("the port is taken");
    assert!(why.contains(&port.to_string()), "{why}");
}

/// Reading the head without taking it out of the socket is what lets
/// one port answer both a GET and a websocket handshake, so a request
/// that arrives in pieces has to be waited for rather than guessed at.
#[test]
fn a_request_that_arrives_in_pieces_is_still_a_request() {
    let (_dir, _function, _isolate, at) = debugging();
    let mut socket = TcpStream::connect(at).expect("the inspector's port");
    socket.write_all(b"GET /json/ver").expect("half a request");
    socket.flush().expect("half a request");
    std::thread::sleep(Duration::from_millis(50));
    socket
        .write_all(b"sion HTTP/1.1\r\nhost: localhost\r\n\r\n")
        .expect("the rest");
    let mut reader = BufReader::new(socket);
    let mut status = String::new();
    reader.read_line(&mut status).expect("an answer");
    assert!(status.starts_with("HTTP/1.1 200 OK"), "{status}");
}
