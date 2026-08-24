//! What the host tells a function about the function's own life.
//!
//! Four events, and none of them is something the function's own code
//! can cause: `beforeunload` when a limit is close or when the isolate
//! is going away, `unload` when it is going, `error` for a throw that
//! escaped everything, and `unhandledrejection` for a promise nobody
//! caught. They are one surface because a function author reaches for
//! them together, and they are worth testing because a count of
//! functions that answer cannot see any of them.

#![cfg(feature = "isolate")]

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::time::Duration;

use zou_deno::{Isolate, Limits};
use zou_functions::{Answer, Call, Function, Policy, Runtime};

fn deployed(source: &str) -> (tempfile::TempDir, Function) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let entrypoint = dir.path().join("index.ts");
    std::fs::write(&entrypoint, source).expect("the function's file");
    let function = Function::new("hello", entrypoint);
    (dir, function)
}

fn get() -> Call {
    Call {
        method: "GET".to_string(),
        url: "http://localhost:9000/functions/v1/hello".to_string(),
        headers: Vec::new(),
        body: Vec::new(),
        execution_id: "one".to_string(),
    }
}

fn answered(source: &str) -> Answer {
    let (_dir, function) = deployed(source);
    Isolate::new().invoke(&function, get()).expect("an answer")
}

fn body(answer: &Answer) -> String {
    String::from_utf8(answer.bytes().to_vec()).expect("utf-8")
}

/// Upstream's limits with the cpu one where a test can reach it. The
/// warning goes at ninety percent of it, so the number here is both
/// what the test waits for and what it has left to answer in.
fn tight(cpu: Duration) -> Limits {
    Limits {
        memory: 64 * 1024 * 1024,
        wall: Duration::from_secs(30),
        cpu,
        boot: Duration::from_secs(30),
        background: Duration::from_secs(30),
    }
}

#[test]
fn a_throw_nobody_could_catch_is_an_error_event_on_the_global() {
    // A timer callback is the plainest case: whatever set the timer
    // returned long before it fired, so there is no catch anywhere
    // above it and the runtime is the only thing left to tell.
    let answer = answered(
        r#"
        let seen = null;
        addEventListener("error", (ev) => {
          ev.preventDefault();
          seen = `${ev.message} ${ev.error instanceof Error} ${ev.type}`;
        });
        Deno.serve(async () => {
          setTimeout(() => { throw new Error("out of a timer"); }, 0);
          await new Promise((resolve) => setTimeout(resolve, 20));
          return new Response(String(seen));
        });
        "#,
    );
    assert_eq!(body(&answer), "out of a timer true error");
}

#[test]
fn a_listener_for_the_error_event_that_throws_does_not_report_itself_forever() {
    let answer = answered(
        r#"
        let told = 0;
        addEventListener("error", () => {
          told += 1;
          throw new Error("and this one too");
        });
        Deno.serve(async () => {
          setTimeout(() => { throw new Error("the first one"); }, 0);
          await new Promise((resolve) => setTimeout(resolve, 20));
          return new Response(String(told));
        });
        "#,
    );
    assert_eq!(body(&answer), "1");
}

#[test]
fn a_promise_nobody_caught_is_an_unhandledrejection_event() {
    let answer = answered(
        r#"
        let caught = null;
        addEventListener("unhandledrejection", (ev) => {
          ev.preventDefault();
          caught = `${ev.reason.message} ${ev.promise instanceof Promise}`;
        });
        Deno.serve(async () => {
          Promise.reject(new Error("nobody is waiting"));
          await new Promise((resolve) => setTimeout(resolve, 20));
          return new Response(String(caught));
        });
        "#,
    );
    assert_eq!(body(&answer), "nobody is waiting true");
}

/// A rejection nobody prevented the default of ends the call in Deno.
/// It does not here: the answer is often already written by the time
/// one of these is noticed, and losing it would be worse than the
/// rejection. So the function keeps going and the rejection is logged.
#[test]
fn a_rejection_nobody_listens_for_does_not_end_the_call() {
    let answer = answered(
        r#"
        Deno.serve(async () => {
          Promise.reject(new Error("into the log"));
          await new Promise((resolve) => setTimeout(resolve, 20));
          return new Response("answered anyway");
        });
        "#,
    );
    assert_eq!(body(&answer), "answered anyway");
}

/// The event upstream dispatches at ninety percent of the cpu limit,
/// with the reason on `detail` where a listener reads it.
///
/// The loop gives the event loop a turn between slices of work, which
/// is what a function doing anything real does anyway and is what the
/// warning needs: it arrives as a task the loop runs, so a function
/// that never lets the loop run is never told.
#[test]
fn a_function_near_the_cpu_limit_is_told_before_it_is_stopped() {
    let (_dir, function) = deployed(
        r#"
        let said = null;
        addEventListener("beforeunload", (ev) => { said = ev.detail.reason; });
        Deno.serve(async () => {
          const until = Date.now() + 20000;
          while (said === null && Date.now() < until) {
            const slice = Date.now() + 5;
            while (Date.now() < slice) {}
            await new Promise((resolve) => setTimeout(resolve, 0));
          }
          return new Response(String(said));
        });
        "#,
    );
    let answer = Isolate::new()
        .with_limits(tight(Duration::from_secs(2)))
        .invoke(&function, get())
        .expect("the warning leaves a tenth of the limit to answer in");
    assert_eq!(body(&answer), "cpu");
}

/// The pair at the end: `beforeunload` with `termination` on it, and
/// `unload`.
///
/// The answer has already gone by then, so the function says what it
/// was told down a socket instead. `OneShot` because that is the policy
/// under which the end of the call is the end of the isolate; a kept
/// isolate is told the same thing a minute later, which is not a wait a
/// test should have.
///
/// Both of them, rather than one and then the other. The events are
/// dispatched in upstream's order, but neither `told` is awaited, so
/// what arrives first is whichever of two connections opened
/// microseconds apart the accept loop happened to take, which is not
/// something this end decides. See #626.
#[test]
fn a_function_whose_isolate_is_going_away_is_told_twice() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("a port");
    let at = listener.local_addr().expect("an address");
    let (heard, said) = mpsc::channel();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().expect("the same socket"));
            let mut line = String::new();
            if reader.read_line(&mut line).is_err() {
                continue;
            }
            let path = line.split_whitespace().nth(1).unwrap_or("/").to_string();
            let _ = stream.write_all(
                b"HTTP/1.1 204 No Content\r\nconnection: close\r\ncontent-length: 0\r\n\r\n",
            );
            let _ = stream.flush();
            if heard.send(path).is_err() {
                return;
            }
        }
    });

    let (_dir, function) = deployed(&format!(
        r#"
        const told = (what) => fetch("http://{at}/" + what);
        addEventListener("beforeunload", (ev) => told("beforeunload-" + ev.detail.reason));
        addEventListener("unload", () => told("unload"));
        Deno.serve(() => new Response("answered"));
        "#
    ));
    let answer = Isolate::new()
        .with_policy(Policy::OneShot)
        .invoke(&function, get())
        .expect("an answer");
    assert_eq!(body(&answer), "answered");

    let mut told = vec![
        said.recv_timeout(Duration::from_secs(10))
            .expect("the function was told it was going"),
        said.recv_timeout(Duration::from_secs(10))
            .expect("and that it had gone"),
    ];
    told.sort();
    assert_eq!(told, ["/beforeunload-termination", "/unload"]);
}

/// A function that has none of these listeners is the ordinary case and
/// costs nothing: the events are dispatched into a global with no
/// listeners on it, which is a map lookup that misses.
#[test]
fn a_function_that_listens_for_none_of_it_is_unaffected() {
    let (_dir, function) = deployed(r#"Deno.serve(() => new Response("quiet"));"#);
    let answer = Isolate::new()
        .with_policy(Policy::OneShot)
        .invoke(&function, get())
        .expect("an answer");
    assert_eq!(body(&answer), "quiet");
}
