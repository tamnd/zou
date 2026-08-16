//! Isolates kept between calls, which is upstream's `per_worker`.
//!
//! What is being tested is the difference between the two policies,
//! because that difference is the whole feature: the same function
//! called twice either remembers what it built the first time or does
//! not, and every test here is a version of that question. The one
//! thing tested by absence is the idle timer, which is a minute long
//! and so is left to the code that reads it.

#![cfg(feature = "isolate")]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use zou_deno::{Isolate, Limits};
use zou_functions::{Answer, Call, Failed, Function, Policy, Runtime};

/// A function on disk, kept as a directory rather than a file, because
/// hot reload is about a file changing under an isolate that has
/// already read it.
struct Deployed {
    dir: tempfile::TempDir,
    function: Function,
}

impl Deployed {
    fn at(&self) -> &Path {
        self.dir.path()
    }

    /// Write one of its files again, which is what an editor does.
    fn edit(&self, name: &str, source: &str) {
        // A second write inside the same clock tick would be a file
        // that changed and does not look like it, which is a flake
        // rather than a bug in what is being tested.
        std::thread::sleep(Duration::from_millis(20));
        write(self.at(), name, source);
    }
}

fn deployed(source: &str) -> Deployed {
    written(&[("index.ts", source)])
}

fn written(files: &[(&str, &str)]) -> Deployed {
    let dir = tempfile::tempdir().expect("a temporary directory");
    for (name, source) in files {
        write(dir.path(), name, source);
    }
    let entrypoint: PathBuf = dir.path().join(files[0].0);
    Deployed {
        dir,
        function: Function::new("hello", entrypoint),
    }
}

fn write(dir: &Path, name: &str, source: &str) {
    let path = dir.join(name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("the function's directory");
    }
    std::fs::write(&path, source).expect("the function's file");
}

fn call(execution_id: &str) -> Call {
    Call {
        method: "GET".to_string(),
        url: "http://localhost:9000/functions/v1/hello".to_string(),
        headers: Vec::new(),
        body: Vec::new(),
        execution_id: execution_id.to_string(),
    }
}

fn kept() -> Isolate {
    Isolate::new().with_policy(Policy::PerWorker)
}

fn body(answer: &Answer) -> String {
    String::from_utf8(answer.bytes().to_vec()).expect("utf-8")
}

fn said(runtime: &Isolate, deployed: &Deployed, execution_id: &str) -> String {
    let answer = runtime
        .invoke(&deployed.function, call(execution_id))
        .expect("an answer");
    body(&answer)
}

/// A function that counts the calls its module has seen, which is the
/// simplest thing that tells the two policies apart.
const COUNTING: &str = r#"
    let calls = 0;
    Deno.serve(() => new Response("calls " + (++calls)));
    "#;

#[test]
fn a_kept_isolate_remembers_what_its_module_built() {
    let deployed = deployed(COUNTING);
    let runtime = kept();
    assert_eq!(said(&runtime, &deployed, "one"), "calls 1");
    assert_eq!(said(&runtime, &deployed, "two"), "calls 2");
    assert_eq!(said(&runtime, &deployed, "three"), "calls 3");
}

#[test]
fn a_fresh_isolate_per_call_remembers_nothing() {
    let deployed = deployed(COUNTING);
    let runtime = Isolate::new().with_policy(Policy::OneShot);
    assert_eq!(said(&runtime, &deployed, "one"), "calls 1");
    assert_eq!(said(&runtime, &deployed, "two"), "calls 1");
}

/// The point of keeping one, in the only unit a caller cares about.
/// The module here waits a fifth of a second at the top of itself, so
/// the cold start is a number rather than a hope, and the second call
/// does not pay it.
#[test]
fn the_cold_start_is_paid_once() {
    let deployed = deployed(
        r#"
        await new Promise((resolve) => setTimeout(resolve, 200));
        Deno.serve(() => new Response("warm"));
        "#,
    );
    let runtime = kept();
    let first = Instant::now();
    assert_eq!(said(&runtime, &deployed, "one"), "warm");
    let cold = first.elapsed();
    let second = Instant::now();
    assert_eq!(said(&runtime, &deployed, "two"), "warm");
    let warm = second.elapsed();
    assert!(
        cold >= Duration::from_millis(200),
        "the module's own two hundred milliseconds were not paid: {cold:?}"
    );
    assert!(
        warm < Duration::from_millis(100),
        "the second call built the module again: {warm:?}"
    );
}

/// A kept isolate is not a kept call. The environment is the project's
/// and is the same every time, except for the one variable that is the
/// invocation's own, and reading a stale execution id out of a warm
/// isolate is exactly the bug this policy invites.
#[test]
fn each_call_in_a_kept_isolate_is_its_own_call() {
    let deployed = deployed(
        r#"
        Deno.serve(() => new Response(Deno.env.get("SB_EXECUTION_ID")));
        "#,
    );
    let runtime = kept();
    assert_eq!(said(&runtime, &deployed, "the-first"), "the-first");
    assert_eq!(said(&runtime, &deployed, "the-second"), "the-second");
}

/// Hot reload, which the pinned CLI's own `config.toml` says is what
/// `per_worker` is for.
#[test]
fn a_function_edited_on_disk_is_the_one_the_next_call_gets() {
    let deployed = deployed(r#"Deno.serve(() => new Response("before"));"#);
    let runtime = kept();
    assert_eq!(said(&runtime, &deployed, "one"), "before");
    deployed.edit("index.ts", r#"Deno.serve(() => new Response("after"));"#);
    assert_eq!(said(&runtime, &deployed, "two"), "after");
}

/// And a file the function imports counts as much as the file it
/// starts at, because a project keeps its shared code in `_shared` and
/// editing it is editing the function.
#[test]
fn a_shared_file_edited_on_disk_reloads_the_function_too() {
    let deployed = written(&[
        (
            "index.ts",
            r#"
            import { greeting } from "./_shared/greet.ts";
            Deno.serve(() => new Response(greeting));
            "#,
        ),
        ("_shared/greet.ts", r#"export const greeting = "before";"#),
    ]);
    let runtime = kept();
    assert_eq!(said(&runtime, &deployed, "one"), "before");
    deployed.edit("_shared/greet.ts", r#"export const greeting = "after";"#);
    assert_eq!(said(&runtime, &deployed, "two"), "after");
}

/// An unedited function is not reloaded, which is the other half of the
/// same statement: if every call looked stale then `per_worker` would
/// be `oneshot` with more machinery.
#[test]
fn a_function_nobody_edited_keeps_its_isolate() {
    let deployed = deployed(COUNTING);
    let runtime = kept();
    assert_eq!(said(&runtime, &deployed, "one"), "calls 1");
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(said(&runtime, &deployed, "two"), "calls 2");
}

/// A terminated isolate is not somewhere the next call should start
/// from. The function decides what to do out of the invocation's own
/// variable, so one entrypoint can be both the call that overruns and
/// the call after it, and the count is what says which isolate
/// answered: a kept one would be on its second call.
#[test]
fn an_isolate_that_reached_a_limit_is_not_kept() {
    let deployed = deployed(
        r#"
        let calls = 0;
        Deno.serve(() => {
          calls++;
          if (Deno.env.get("SB_EXECUTION_ID") === "spin") { for (;;) {} }
          return new Response("calls " + calls);
        });
        "#,
    );
    let runtime = kept().with_limits(Limits {
        cpu: Duration::from_millis(200),
        ..Limits::default()
    });
    let why = runtime
        .invoke(&deployed.function, call("spin"))
        .expect_err("a call that ran past its cpu");
    let Failed::Limit(complaint) = why else {
        panic!("a limit and not a function that threw: {why:?}");
    };
    assert!(complaint.contains("cpu time"), "{complaint}");
    assert_eq!(said(&runtime, &deployed, "after"), "calls 1");
}

/// A handler that threw is an ordinary answer and the isolate is
/// intact, so throwing it away would make a broken function a slow one
/// as well as a broken one.
#[test]
fn an_isolate_whose_handler_threw_is_kept() {
    let deployed = deployed(
        r#"
        let calls = 0;
        Deno.serve(() => {
          calls++;
          if (calls === 1) { throw new Error("not this time"); }
          return new Response("calls " + calls);
        });
        "#,
    );
    let runtime = kept();
    runtime
        .invoke(&deployed.function, call("one"))
        .expect_err("the handler threw");
    assert_eq!(said(&runtime, &deployed, "two"), "calls 2");
}

/// A call that arrives while the isolate is busy gets one of its own
/// rather than waiting behind the call in front of it, because a
/// function that answers slowly should not make the next caller answer
/// slowly too.
#[test]
fn a_busy_isolate_does_not_make_the_next_caller_wait_for_it() {
    let deployed = deployed(
        r#"
        Deno.serve(async () => {
          await new Promise((resolve) => setTimeout(resolve, 500));
          return new Response("slept");
        });
        "#,
    );
    let runtime = Arc::new(kept());
    let started = Instant::now();
    let both: Vec<_> = ["one", "two"]
        .into_iter()
        .map(|execution_id| {
            let runtime = Arc::clone(&runtime);
            let function = deployed.function.clone();
            std::thread::spawn(move || {
                let answer = runtime
                    .invoke(&function, call(execution_id))
                    .expect("an answer");
                body(&answer)
            })
        })
        .collect();
    for calling in both {
        assert_eq!(calling.join().expect("the caller's thread"), "slept");
    }
    let took = started.elapsed();
    assert!(
        took < Duration::from_millis(900),
        "the second call queued behind the first: {took:?}"
    );
}

#[test]
fn the_runtime_says_which_policy_it_is_on() {
    assert_eq!(
        kept().describe(),
        "a v8 isolate per function, kept between calls"
    );
    assert_eq!(
        Isolate::new().with_policy(Policy::OneShot).describe(),
        "a v8 isolate per call"
    );
}

/// A kept isolate serves its second call through the loop the first one
/// left running.
///
/// That is the part of the listener shim a single call cannot show. The
/// loop is still parked on `nextRequest` when the answer to the first
/// call has already gone, and the second call has to arrive there
/// rather than at a connection nobody is accepting any more.
#[test]
fn a_kept_isolate_serves_its_second_call_through_the_loop_it_left_running() {
    let deployed = deployed(
        r#"
        let calls = 0;
        const listener = Deno.listen({ port: 8000 });
        (async () => {
          for await (const conn of listener) {
            const http = Deno.serveHttp(conn);
            for await (const event of http) {
              await event.respondWith(new Response("calls " + (++calls)));
            }
          }
        })();
        "#,
    );
    let runtime = kept();
    assert_eq!(said(&runtime, &deployed, "one"), "calls 1");
    assert_eq!(said(&runtime, &deployed, "two"), "calls 2");
    assert_eq!(said(&runtime, &deployed, "three"), "calls 3");
}

/// The same for a default export, which has nothing to leave running
/// and so is read out of the module namespace once and called again.
#[test]
fn a_kept_isolate_calls_the_same_default_export_again() {
    let deployed = deployed(
        r#"
        let calls = 0;
        export default { fetch: () => new Response("calls " + (++calls)) };
        "#,
    );
    let runtime = kept();
    assert_eq!(said(&runtime, &deployed, "one"), "calls 1");
    assert_eq!(said(&runtime, &deployed, "two"), "calls 2");
}
