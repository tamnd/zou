//! A real `index.ts` through a real v8 isolate.
//!
//! Every test here writes a function to a temporary directory the way a
//! project would have it on disk and calls it through the same
//! [`Runtime`] trait the server uses, so what is being tested is the
//! whole path: typescript in, transpile, module graph, prelude, handler,
//! answer out. There is no unit sized way to test an engine.
//!
//! They are plain `#[test]` rather than `#[tokio::test]` on purpose.
//! `invoke` builds a current thread runtime of its own, which is the
//! shape the server's `spawn_blocking` hands it, and starting a runtime
//! inside a runtime panics.

#![cfg(feature = "isolate")]

use std::path::PathBuf;

use zou_deno::{Isolate, Limits};
use zou_functions::{Answer, Call, Failed, Function, Runtime};

/// A function on disk, one file, named the way a project names it.
struct Deployed {
    _dir: tempfile::TempDir,
    function: Function,
}

fn deployed(source: &str) -> Deployed {
    written(&[("index.ts", source)])
}

/// A function of several files, the first of which is the entrypoint.
fn written(files: &[(&str, &str)]) -> Deployed {
    let dir = tempfile::tempdir().expect("a temporary directory");
    for (name, source) in files {
        let path = dir.path().join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("the function's directory");
        }
        std::fs::write(&path, source).expect("the function's file");
    }
    let entrypoint: PathBuf = dir.path().join(files[0].0);
    Deployed {
        _dir: dir,
        function: Function::new("hello", entrypoint),
    }
}

fn get(url: &str) -> Call {
    Call {
        method: "GET".to_string(),
        url: url.to_string(),
        headers: Vec::new(),
        body: Vec::new(),
        execution_id: "one".to_string(),
    }
}

fn called(source: &str, call: Call) -> Result<Answer, Failed> {
    let deployed = deployed(source);
    Isolate::new().invoke(&deployed.function, call)
}

fn answered(source: &str) -> Answer {
    called(source, get("http://localhost:9000/functions/v1/hello")).expect("an answer")
}

fn body(answer: &Answer) -> String {
    String::from_utf8(answer.bytes().to_vec()).expect("utf-8")
}

#[test]
fn a_handler_answers_and_the_answer_arrives_whole() {
    let answer = answered(
        r#"
        Deno.serve(() => new Response("hello from a function", { status: 201, headers: { "x-made-by": "zou" } }));
        "#,
    );
    assert_eq!(answer.status, 201);
    assert_eq!(body(&answer), "hello from a function");
    assert_eq!(
        answer.headers,
        vec![
            (
                "content-type".to_string(),
                "text/plain;charset=UTF-8".to_string()
            ),
            ("x-made-by".to_string(), "zou".to_string()),
        ]
    );
}

#[test]
fn the_typescript_is_real_typescript_and_not_javascript_with_hope() {
    // Every one of these is a syntax error to v8: an interface, a type
    // annotation, a generic, an enum and a non null assertion. If the
    // transpiler were not there this would not parse.
    let answer = answered(
        r#"
        interface Greeting { readonly said: string }
        enum Loudness { Quiet, Loud }
        function say<T extends Greeting>(it: T, how: Loudness): string {
            return how === Loudness.Loud ? it.said.toUpperCase() : it.said;
        }
        const greeting: Greeting | null = { said: "typed" };
        Deno.serve((_req: Request): Response => new Response(say(greeting!, Loudness.Loud)));
        "#,
    );
    assert_eq!(body(&answer), "TYPED");
}

#[test]
fn a_request_is_the_call_the_server_was_given() {
    let call = Call {
        method: "POST".to_string(),
        url: "http://localhost:9000/functions/v1/hello/deep?who=world".to_string(),
        headers: vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("x-twice".to_string(), "one".to_string()),
            ("x-twice".to_string(), "two".to_string()),
        ],
        body: br#"{"name":"world"}"#.to_vec(),
        execution_id: "one".to_string(),
    };
    let answer = called(
        r#"
        Deno.serve(async (req) => Response.json({
            method: req.method,
            url: req.url,
            type: req.headers.get("content-type"),
            twice: req.headers.get("x-twice"),
            body: await req.json(),
        }));
        "#,
        call,
    )
    .expect("an answer");
    assert_eq!(answer.status, 200);
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["method"], "POST");
    assert_eq!(
        said["url"],
        "http://localhost:9000/functions/v1/hello/deep?who=world"
    );
    assert_eq!(said["type"], "application/json");
    // Two headers of one name are one value joined, which is what the
    // spec says a `Headers` gives back.
    assert_eq!(said["twice"], "one, two");
    assert_eq!(said["body"]["name"], "world");
}

#[test]
fn a_get_has_no_body_to_read_and_a_post_does() {
    let answer = answered(r#"Deno.serve(async (req) => new Response(`[${await req.text()}]`));"#);
    assert_eq!(body(&answer), "[]");

    let call = Call {
        method: "PUT".to_string(),
        url: "http://localhost:9000/functions/v1/hello".to_string(),
        headers: Vec::new(),
        body: b"the bytes that were sent".to_vec(),
        execution_id: "one".to_string(),
    };
    let answer = called(
        r#"Deno.serve(async (req) => new Response(`[${await req.text()}]`));"#,
        call,
    )
    .expect("an answer");
    assert_eq!(body(&answer), "[the bytes that were sent]");
}

#[test]
fn bytes_survive_the_trip_in_both_directions() {
    let call = Call {
        method: "POST".to_string(),
        url: "http://localhost:9000/functions/v1/hello".to_string(),
        headers: Vec::new(),
        body: vec![0, 1, 2, 253, 254, 255],
        execution_id: "one".to_string(),
    };
    let answer = called(
        r#"
        Deno.serve(async (req) => {
            const sent = new Uint8Array(await req.arrayBuffer());
            const back = new Uint8Array(sent.length + 1);
            back.set(sent);
            back[sent.length] = 42;
            return new Response(back, { headers: { "content-type": "application/octet-stream" } });
        });
        "#,
        call,
    )
    .expect("an answer");
    assert_eq!(answer.bytes(), [0, 1, 2, 253, 254, 255, 42]);
}

#[test]
fn the_environment_is_the_runtimes_and_never_the_processes() {
    // Set on the test's own process, which is exactly what a function
    // must not be able to read: a server started with a database
    // password in its environment does not hand it to somebody else's
    // javascript.
    unsafe { std::env::set_var("ZOU_A_HOST_SECRET", "not for the function") };
    let deployed = deployed(
        r#"
        Deno.serve(() => Response.json({
            named: Deno.env.get("SUPABASE_URL"),
            missing: Deno.env.get("NOT_SET") ?? null,
            has: Deno.env.has("SUPABASE_URL"),
            host: Deno.env.get("ZOU_A_HOST_SECRET") ?? null,
            all: Deno.env.toObject(),
        }));
        "#,
    );
    let isolate = Isolate::with_env(vec![(
        "SUPABASE_URL".to_string(),
        "http://localhost:54321".to_string(),
    )]);
    let answer = isolate
        .invoke(
            &deployed.function,
            get("http://localhost:9000/functions/v1/hello"),
        )
        .expect("an answer");
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["named"], "http://localhost:54321");
    assert_eq!(said["missing"], serde_json::Value::Null);
    assert_eq!(said["has"], true);
    assert_eq!(said["host"], serde_json::Value::Null);
    // The project's one variable and the call's own, and nothing else:
    // `SB_EXECUTION_ID` is upstream's per invocation name and is the
    // only thing the runtime adds to what it was built with.
    assert_eq!(
        said["all"],
        serde_json::json!({
            "SUPABASE_URL": "http://localhost:54321",
            "SB_EXECUTION_ID": "one",
        })
    );
}

/// The line the Supabase examples open with, near enough word for word:
/// `const client = new Thing(Deno.env.get("KEY"))` at the top of the
/// module, before anything is served. Twenty five of the thirty nine
/// functions in that repository read the environment there, so a
/// runtime that only has one once a call is in it serves none of them.
#[test]
fn the_environment_is_readable_before_anything_is_served() {
    let deployed = deployed(
        r#"
        const key = Deno.env.get("RESEND_API_KEY") ?? "nothing at the top";
        Deno.serve(() => new Response(key));
        "#,
    );
    let isolate = Isolate::with_env(vec![(
        "RESEND_API_KEY".to_string(),
        "re_the_projects_own".to_string(),
    )]);
    let answer = isolate
        .invoke(
            &deployed.function,
            get("http://localhost:9000/functions/v1/hello"),
        )
        .expect("an answer");
    assert_eq!(body(&answer), "re_the_projects_own");
}

/// One per invocation, which is what ties a log line inside a function
/// to the request that caused it, so a project that sets the name in its
/// own environment does not get to decide what the logs are keyed on.
#[test]
fn the_execution_id_is_the_calls_and_not_the_projects() {
    let deployed =
        deployed(r#"Deno.serve(() => new Response(Deno.env.get("SB_EXECUTION_ID") ?? "none"));"#);
    let isolate = Isolate::with_env(vec![(
        "SB_EXECUTION_ID".to_string(),
        "the project tried".to_string(),
    )]);
    let mut call = get("http://localhost:9000/functions/v1/hello");
    call.execution_id = "the call's own".to_string();
    let answer = isolate.invoke(&deployed.function, call).expect("an answer");
    assert_eq!(body(&answer), "the call's own");
}

/// A function may write to its own environment, and what it writes is
/// its own: it reads back, it covers what was underneath, and it is
/// gone with the isolate rather than reaching the node.
#[test]
fn the_environment_is_the_functions_to_write_to() {
    let deployed = deployed(
        r#"
        const seen = [];
        Deno.env.set("MINE", "yes");
        seen.push(Deno.env.get("MINE"));
        seen.push(String(Deno.env.has("MINE")));

        // Over what the node handed in, and back again when it goes.
        seen.push(Deno.env.get("SUPABASE_URL"));
        Deno.env.set("SUPABASE_URL", "mine now");
        seen.push(Deno.env.get("SUPABASE_URL"));
        seen.push(String(Deno.env.toObject().SUPABASE_URL));

        // A delete hides what is underneath rather than leaving it
        // showing.
        Deno.env.delete("SUPABASE_URL");
        seen.push(String(Deno.env.get("SUPABASE_URL")));
        seen.push(String("SUPABASE_URL" in Deno.env.toObject()));

        // The same environment through the other name for it, which is
        // how a package written for node reaches it.
        process.env.THROUGH_PROCESS = "set";
        seen.push(Deno.env.get("THROUGH_PROCESS"));
        seen.push(process.env.MINE);

        // A name that could not be read back out of a list of them.
        try {
            Deno.env.set("A=B", "no");
            seen.push("it let me");
        } catch (e) {
            seen.push(e.constructor.name);
        }

        Deno.serve(() => new Response(seen.join(" ")));
        "#,
    );
    let isolate = Isolate::with_env(vec![(
        "SUPABASE_URL".to_string(),
        "http://localhost:54321".to_string(),
    )]);
    let answer = isolate
        .invoke(
            &deployed.function,
            get("http://localhost:9000/functions/v1/hello"),
        )
        .expect("an answer");
    assert_eq!(
        body(&answer),
        "yes true http://localhost:54321 mine now mine now undefined false set yes TypeError"
    );
    // And the process that ran it never heard about any of it.
    assert!(std::env::var("MINE").is_err());
    assert!(std::env::var("THROUGH_PROCESS").is_err());
}

#[test]
fn a_function_may_import_the_files_beside_it() {
    let deployed = written(&[
        (
            "index.ts",
            r#"
            import { greet } from "./_shared/greet.ts";
            import settings from "./settings.json" with { type: "json" };
            Deno.serve(() => new Response(greet(settings.who)));
            "#,
        ),
        (
            "_shared/greet.ts",
            "export function greet(who: string): string { return `hello ${who}`; }",
        ),
        ("settings.json", r#"{ "who": "the world" }"#),
    ]);
    let answer = Isolate::new()
        .invoke(
            &deployed.function,
            get("http://localhost:9000/functions/v1/hello"),
        )
        .expect("an answer");
    assert_eq!(body(&answer), "hello the world");
}

/// The three that are refused before a request is made, which is why
/// this test needs no network: `npm:` and `jsr:` are fetched and are
/// tested in `registry.rs`. The node built in named here is one nobody
/// has written, since the ones that are written are served.
#[test]
fn a_specifier_this_runtime_does_not_serve_says_so_by_name() {
    for (specifier, said) in [
        ("http://esm.sh/zod", "over https"),
        ("node:dgram", "no node built in dgram"),
        ("data:text/javascript,1", "the data: specifier"),
    ] {
        let source = format!(r#"import "{specifier}"; Deno.serve(() => new Response("no"));"#);
        let complaint = called(&source, get("http://localhost:9000/functions/v1/hello"))
            .expect_err("a refusal")
            .why()
            .to_string();
        assert!(
            complaint.contains(said),
            "{specifier} was refused with {complaint}"
        );
    }
}

#[test]
fn a_function_that_forgot_to_serve_is_an_error_and_not_an_empty_answer() {
    let complaint = called(
        "const nothing = 1;",
        get("http://localhost:9000/functions/v1/hello"),
    )
    .expect_err("a complaint")
    .why()
    .to_string();
    assert!(complaint.contains("did not say what to run"), "{complaint}");
}

/// The other two ways a module says what to run.
///
/// Upstream takes all three, measured on `supabase/edge-runtime:v1.74.2`
/// asked directly: `Deno.serve(handler)`, a default export with a
/// `fetch`, and the older `serve()` out of `std/http/server.ts`, which
/// says so by asking for a socket. The last one is still what most of
/// the examples in the wild are written against, so a runtime that took
/// only the first would refuse most of the functions people already
/// have.
#[test]
fn a_default_export_with_a_fetch_is_the_handler() {
    let answer = answered(
        r#"
        export default {
          fetch(req: Request) {
            return new Response(`fetched ${new URL(req.url).pathname}`, { status: 202 });
          },
        };
        "#,
    );
    assert_eq!(answer.status, 202);
    assert_eq!(body(&answer), "fetched /functions/v1/hello");
}

#[test]
fn a_default_export_whose_fetch_is_async_is_the_handler_too() {
    let answer = answered(
        r#"
        export default {
          async fetch() {
            await new Promise((done) => setTimeout(done, 1));
            return new Response("awaited", { status: 203 });
          },
        };
        "#,
    );
    assert_eq!(answer.status, 203);
    assert_eq!(body(&answer), "awaited");
}

/// Measured rather than chosen, a pair at a time, on modules that said
/// it two ways at once. `Deno.serve` beats both of the others, and a
/// listener beats a default export.
///
/// Which is one rule and not three: upstream is a socket, so the module
/// that took the socket is the module that is served and the default
/// export is what is left when nobody took it.
#[test]
fn the_module_that_took_the_socket_is_the_one_that_is_served() {
    let answer = answered(
        r#"
        Deno.serve(() => new Response("served"));
        export default { fetch: () => new Response("exported") };
        "#,
    );
    assert_eq!(body(&answer), "served");

    let answer = answered(
        r#"
        const listener = Deno.listen({ port: 8000 });
        (async () => {
          const http = Deno.serveHttp(await listener.accept());
          const event = await http.nextRequest();
          await event.respondWith(new Response("listened"));
        })();
        Deno.serve(() => new Response("served"));
        "#,
    );
    assert_eq!(body(&answer), "served");

    let answer = answered(
        r#"
        const listener = Deno.listen({ port: 8000 });
        (async () => {
          const http = Deno.serveHttp(await listener.accept());
          const event = await http.nextRequest();
          await event.respondWith(new Response("listened"));
        })();
        export default { fetch: () => new Response("exported") };
        "#,
    );
    assert_eq!(body(&answer), "listened");
}

/// The listener shim, driven by hand the way `std/http/server.ts`
/// drives it: listen, accept, upgrade, pull one request off, answer it.
#[test]
fn a_module_that_asks_for_a_socket_is_served_through_it() {
    let answer = answered(
        r#"
        const listener = Deno.listen({ port: 8000 });
        (async () => {
          for await (const conn of listener) {
            const http = Deno.serveHttp(conn);
            for await (const event of http) {
              await event.respondWith(new Response(`through ${event.request.method}`, { status: 200 }));
            }
          }
        })();
        "#,
    );
    assert_eq!(answer.status, 200);
    assert_eq!(body(&answer), "through GET");
}

/// The request that goes into the loop is the whole request, body and
/// all, and the response that comes out of `respondWith` is the answer.
#[test]
fn the_body_a_module_sends_through_a_socket_arrives_whole() {
    let answer = called(
        r#"
        const listener = Deno.listen({ port: 8000 });
        (async () => {
          const conn = await listener.accept();
          const http = Deno.serveHttp(conn);
          const event = await http.nextRequest();
          const sent = await event.request.text();
          await event.respondWith(new Response(`heard ${sent}`, { status: 200 }));
        })();
        "#,
        Call {
            method: "POST".to_string(),
            url: "http://localhost:9000/functions/v1/hello".to_string(),
            headers: Vec::new(),
            body: b"a shout".to_vec(),
            execution_id: "one".to_string(),
        },
    )
    .expect("an answer");
    assert_eq!(body(&answer), "heard a shout");
}

/// The real `std/http/server.ts` over the network is in `registry.rs`,
/// because it is a claim about deno.land answering. This is the same
/// loop written out locally: the pieces of it the shim has to satisfy,
/// in the order that file puts them in.
#[test]
fn the_loop_the_older_examples_run_is_served_end_to_end() {
    let answer = answered(
        r#"
        const closing = new AbortController();
        closing.signal.addEventListener("abort", () => listener.close(), { once: true });
        const listener = Deno.listen({ port: 8000, hostname: "0.0.0.0", transport: "tcp" });
        (async () => {
          while (true) {
            let conn: Deno.Conn;
            try {
              conn = await listener.accept();
            } catch (error) {
              if (
                error instanceof Deno.errors.BadResource ||
                error instanceof Deno.errors.InvalidData ||
                error instanceof Deno.errors.UnexpectedEof ||
                error instanceof Deno.errors.ConnectionReset ||
                error instanceof Deno.errors.NotConnected
              ) {
                continue;
              }
              throw error;
            }
            const http = Deno.serveHttp(conn);
            const info = { localAddr: conn.localAddr, remoteAddr: conn.remoteAddr };
            (async () => {
              while (true) {
                const event = await http.nextRequest();
                if (event === null) {
                  break;
                }
                const response = new Response(`std ${info.localAddr.transport}`, { status: 207 });
                await event.respondWith(response);
              }
            })();
          }
        })();
        "#,
    );
    assert_eq!(answer.status, 207);
    assert_eq!(body(&answer), "std tcp");
}

/// The same loop with the module waiting on it, which is oak's shape
/// and therefore the shape of everything written on oak:
///
/// ```ts
/// const app = new Application()
/// app.use(router.routes())
/// await app.listen({ port: 8000 })
/// ```
///
/// A module written like that never finishes evaluating, because what
/// it is waiting for is a request and the request is waiting for the
/// module. So the module having parked on an accept is the other way
/// out of that wait: it has said everything it is going to say, and
/// the call goes into the loop it parked in.
#[test]
fn a_module_that_waits_on_its_own_listener_is_served_through_it() {
    let answer = answered(
        r#"
        const listener = Deno.listen({ port: 8000 });
        for await (const conn of listener) {
          const http = Deno.serveHttp(conn);
          for await (const event of http) {
            await event.respondWith(new Response("awaited", { status: 201 }));
          }
        }
        "#,
    );
    assert_eq!(answer.status, 201);
    assert_eq!(body(&answer), "awaited");
}

/// And everything above the listen has run by then. The wait ends
/// where the module parks and not before, so what the module set up on
/// the way down to the listen is set up.
#[test]
fn what_a_module_did_before_it_parked_is_done_before_it_is_called() {
    let answer = answered(
        r#"
        const slept = await new Promise((resolve) => setTimeout(() => resolve("late"), 20));
        const listener = Deno.listen({ port: 8000 });
        for await (const conn of listener) {
          const http = Deno.serveHttp(conn);
          for await (const event of http) {
            await event.respondWith(new Response(`ready ${slept}`));
          }
        }
        "#,
    );
    assert_eq!(body(&answer), "ready late");
}

#[test]
fn a_handler_that_throws_is_the_operators_message_and_not_the_callers() {
    let complaint = called(
        r#"Deno.serve(() => { throw new Error("the database was not there"); });"#,
        get("http://localhost:9000/functions/v1/hello"),
    )
    .expect_err("a complaint")
    .why()
    .to_string();
    assert!(
        complaint.contains("the database was not there"),
        "{complaint}"
    );
}

#[test]
fn a_handler_that_answers_with_something_that_is_not_a_response_is_an_error() {
    let complaint = called(
        r#"Deno.serve(() => "a string is not a Response");"#,
        get("http://localhost:9000/functions/v1/hello"),
    )
    .expect_err("a complaint")
    .why()
    .to_string();
    assert!(complaint.contains("must return a Response"), "{complaint}");
}

/// `Deno.core.ops` is reachable from a function, and the ops behind it
/// are written for a call that is being answered. Reaching one where
/// there is no call is a mistake somebody's javascript is allowed to
/// make, and the whole server is not allowed to go for it: a panic in
/// an op cannot unwind and takes the process with it.
#[test]
fn an_op_reached_where_there_is_no_call_is_an_error_and_not_a_dead_server() {
    let complaint = called(
        r#"
        Deno.core.ops.op_zou_call();
        Deno.serve(() => new Response("never reached"));
        "#,
        get("http://localhost:9000/functions/v1/hello"),
    )
    .expect_err("a complaint")
    .why()
    .to_string();
    assert!(
        complaint.contains("no call is being answered"),
        "{complaint}"
    );
}

#[test]
fn a_module_that_will_not_parse_names_the_file_it_would_not_parse() {
    let complaint = called(
        "Deno.serve(() => { ",
        get("http://localhost:9000/functions/v1/hello"),
    )
    .expect_err("a complaint")
    .why()
    .to_string();
    assert!(complaint.contains("index.ts"), "{complaint}");
}

#[test]
fn the_handler_is_told_who_called() {
    let call = Call {
        method: "GET".to_string(),
        url: "http://localhost:9000/functions/v1/hello".to_string(),
        headers: vec![("x-real-ip".to_string(), "203.0.113.7".to_string())],
        body: Vec::new(),
        execution_id: "one".to_string(),
    };
    let answer = called(
        r#"Deno.serve((_req, info) => new Response(info.remoteAddr.hostname));"#,
        call,
    )
    .expect("an answer");
    assert_eq!(body(&answer), "203.0.113.7");
}

/// The other encoding a `TextDecoder` here can be asked for, which is
/// what a wasm module compiled by emscripten reads its own heap with.
#[test]
fn text_decodes_out_of_utf_16_in_both_orders() {
    let answer = answered(
        r#"
        Deno.serve(() => {
            const le = new Uint8Array([0x68, 0x00, 0xe9, 0x00, 0x3d, 0xd8, 0x00, 0xde]);
            const be = new Uint8Array([0x00, 0x68, 0x00, 0xe9, 0xd8, 0x3d, 0xde, 0x00]);
            const bom = new Uint8Array([0xff, 0xfe, 0x68, 0x00]);
            const refused = (label) => {
                try { new TextDecoder(label); return "let me"; } catch (e) { return e.message; }
            };
            return Response.json({
                little: new TextDecoder("utf-16le").decode(le),
                big: new TextDecoder("utf-16be").decode(be),
                plain: new TextDecoder("UTF-16").decode(le),
                named: new TextDecoder("ucs-2").encoding,
                bom: new TextDecoder("utf-16le").decode(bom),
                kept: new TextDecoder("utf-16le", { ignoreBOM: true }).decode(bom).length,
                half: new TextDecoder("utf-16le").decode(new Uint8Array([0x68, 0x00, 0x21])),
                utf8: new TextDecoder("utf8").encoding,
                refused: refused("latin1"),
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["little"], "h\u{e9}\u{1f600}");
    assert_eq!(said["big"], "h\u{e9}\u{1f600}");
    assert_eq!(said["plain"], "h\u{e9}\u{1f600}");
    assert_eq!(said["named"], "utf-16le");
    assert_eq!(said["bom"], "h");
    assert_eq!(said["kept"], 2);
    assert_eq!(said["half"], "h\u{fffd}");
    assert_eq!(said["utf8"], "utf-8");
    assert_eq!(
        said["refused"],
        "the encoding label provided ('latin1') is not supported"
    );
}

/// A decoder told more is coming holds the character that straddles
/// two chunks, which is what makes one usable on a body arriving in
/// pieces. Without it every multi byte character landing on a chunk
/// boundary quietly becomes replacement characters.
#[test]
fn a_decoder_holds_the_character_that_straddles_two_chunks() {
    let answer = answered(
        r#"
        Deno.serve(() => {
            const bytes = new TextEncoder().encode("héllo");
            const split = new TextDecoder();
            const first = split.decode(bytes.subarray(0, 2), { stream: true });
            const rest = split.decode(bytes.subarray(2), { stream: true });
            const wide = new TextEncoder().encode("a😀b");
            const four = new TextDecoder();
            const cut = four.decode(wide.subarray(0, 3), { stream: true });
            // And a decoder that was not told is the one that was here
            // before: it decodes what it was given, boundary and all.
            const blind = new TextDecoder().decode(bytes.subarray(0, 2));
            const wideEnd = new TextDecoder("utf-16le");
            const odd = wideEnd.decode(new Uint8Array([0x68, 0x00, 0xe9]), { stream: true });
            return Response.json({
                first,
                rest,
                cut,
                rejoined: cut + four.decode(wide.subarray(3), { stream: true }),
                blind,
                flushed: split.decode(),
                odd,
                oddRest: wideEnd.decode(new Uint8Array([0x00])),
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["first"], "h");
    assert_eq!(said["rest"], "\u{e9}llo");
    assert_eq!(said["cut"], "a");
    assert_eq!(said["rejoined"], "a\u{1f600}b");
    assert_eq!(said["blind"], "h\u{fffd}");
    assert_eq!(said["flushed"], "");
    assert_eq!(said["odd"], "h");
    assert_eq!(said["oddRest"], "\u{e9}");
}

#[test]
fn the_web_shapes_a_handler_is_written_against_are_there() {
    let answer = answered(
        r#"
        Deno.serve(() => {
            const encoded = new TextEncoder().encode("héllo");
            const decoded = new TextDecoder().decode(encoded);
            const headers = new Headers({ "X-One": "1" });
            headers.append("x-two", "2");
            headers.set("X-One", "one");
            return Response.json({
                round: decoded,
                bytes: Array.from(encoded),
                base64: btoa("hello"),
                back: atob("aGVsbG8="),
                headers: Array.from(headers.entries()),
                had: headers.has("X-ONE"),
                json: new Response("x").headers.get("content-type"),
                ok: new Response(null, { status: 404 }).ok,
                empty: (() => { try { new Response("x", { status: 204 }); return "let me"; } catch (e) { return e.message; } })(),
                where: Response.redirect("https://example.com", 301).headers.get("location"),
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["round"], "héllo");
    assert_eq!(
        said["bytes"],
        serde_json::json!([104, 195, 169, 108, 108, 111])
    );
    assert_eq!(said["base64"], "aGVsbG8=");
    assert_eq!(said["back"], "hello");
    assert_eq!(
        said["headers"],
        serde_json::json!([["x-one", "one"], ["x-two", "2"]])
    );
    assert_eq!(said["had"], true);
    assert_eq!(said["json"], "text/plain;charset=UTF-8");
    assert_eq!(said["ok"], false);
    assert_eq!(
        said["empty"],
        "Response with null body status cannot have body"
    );
    assert_eq!(said["where"], "https://example.com");
    assert_eq!(
        answer.headers.iter().find(|(k, _)| k == "content-type"),
        Some(&("content-type".to_string(), "application/json".to_string()))
    );
}

#[test]
fn the_gaps_are_gaps_by_name_and_not_by_undefined() {
    let answer = answered(
        r#"
        Deno.serve(() => Response.json({
            fetch: typeof fetch,
            url: typeof URL,
            params: typeof URLSearchParams,
            blob: typeof Blob,
            file: typeof File,
            form: typeof FormData,
            crypto: typeof crypto,
            subtle: typeof crypto?.subtle?.digest,
            edge: typeof EdgeRuntime?.waitUntil,
            timer: typeof setTimeout,
            interval: typeof setInterval,
            microtask: typeof queueMicrotask,
            socket: typeof WebSocket,
            events: [typeof Event, typeof MessageEvent, typeof CloseEvent, typeof ErrorEvent].join(" "),
            stream: typeof ReadableStream,
            reader: typeof ReadableStreamDefaultReader,
            writable: typeof WritableStream,
            writer: typeof WritableStreamDefaultWriter,
            transform: typeof TransformStream,
            through: typeof ReadableStream.prototype.pipeThrough,
            target: typeof EventTarget,
            custom: typeof CustomEvent,
            clock: typeof performance?.now,
            agent: typeof navigator?.userAgent,
            bytes: typeof ReadableStreamBYOBReader,
            text: typeof TextDecoderStream,
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    // What is here, asserted beside the gaps rather than somewhere else,
    // because a list of what is missing is only true if the same list
    // says what is not.
    assert_eq!(said["fetch"], "function");
    assert_eq!(said["url"], "function");
    assert_eq!(said["params"], "function");
    assert_eq!(said["blob"], "function");
    assert_eq!(said["file"], "function");
    assert_eq!(said["form"], "function");
    assert_eq!(said["crypto"], "object");
    assert_eq!(said["subtle"], "function");
    assert_eq!(said["edge"], "function");
    assert_eq!(said["timer"], "function");
    assert_eq!(said["interval"], "function");
    assert_eq!(said["microtask"], "function");
    assert_eq!(said["socket"], "function");
    assert_eq!(said["events"], "function function function function");
    assert_eq!(said["stream"], "function");
    assert_eq!(said["reader"], "function");
    assert_eq!(said["writable"], "function");
    assert_eq!(said["writer"], "function");
    assert_eq!(said["transform"], "function");
    assert_eq!(said["through"], "function");
    assert_eq!(said["target"], "function");
    assert_eq!(said["custom"], "function");
    assert_eq!(said["clock"], "function");
    assert_eq!(said["agent"], "string");
    // The gaps that are left, and they are gaps by being absent rather
    // than by throwing, because there is nothing to construct.
    assert_eq!(said["bytes"], "undefined");
    assert_eq!(said["text"], "undefined");
}

#[test]
fn a_url_comes_apart_into_the_pieces_a_handler_reads() {
    let answer = answered(
        r#"
        const url = new URL("https://ana:secret@example.com:8443/one/two?a=1&b=2#top");
        Deno.serve(() => Response.json({
            href: url.href,
            origin: url.origin,
            protocol: url.protocol,
            username: url.username,
            password: url.password,
            host: url.host,
            hostname: url.hostname,
            port: url.port,
            pathname: url.pathname,
            search: url.search,
            hash: url.hash,
            asString: `${url}`,
            asJson: JSON.stringify({ url }),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["href"],
        "https://ana:secret@example.com:8443/one/two?a=1&b=2#top"
    );
    assert_eq!(said["origin"], "https://example.com:8443");
    assert_eq!(said["protocol"], "https:");
    assert_eq!(said["username"], "ana");
    assert_eq!(said["password"], "secret");
    assert_eq!(said["host"], "example.com:8443");
    assert_eq!(said["hostname"], "example.com");
    assert_eq!(said["port"], "8443");
    assert_eq!(said["pathname"], "/one/two");
    assert_eq!(said["search"], "?a=1&b=2");
    assert_eq!(said["hash"], "#top");
    assert_eq!(said["asString"], said["href"]);
    assert_eq!(
        said["asJson"],
        r#"{"url":"https://ana:secret@example.com:8443/one/two?a=1&b=2#top"}"#
    );
}

#[test]
fn a_url_can_be_built_on_another_one_and_changed_afterwards() {
    let answer = answered(
        r#"
        const joined = new URL("../three?x=1", "https://example.com/one/two/four");
        const changed = new URL("https://example.com/one");
        changed.pathname = "/two";
        changed.search = "?a=1";
        changed.hash = "top";
        changed.port = "8443";
        Deno.serve(() => Response.json({
            joined: joined.href,
            changed: changed.href,
            canParse: URL.canParse("https://example.com"),
            cannot: URL.canParse("not a url"),
            parsed: URL.parse("not a url"),
            refused: (() => { try { new URL("/relative"); return "made one"; } catch (e) { return e.message; } })(),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["joined"], "https://example.com/one/three?x=1");
    assert_eq!(said["changed"], "https://example.com:8443/two?a=1#top");
    assert_eq!(said["canParse"], true);
    assert_eq!(said["cannot"], false);
    assert_eq!(said["parsed"], serde_json::Value::Null);
    assert_eq!(said["refused"], "Invalid URL: '/relative'");
}

#[test]
fn a_query_string_is_read_and_written_a_pair_at_a_time() {
    let answer = answered(
        r#"
        const params = new URLSearchParams("a=1&b=two&a=3&empty");
        params.append("c", "a value & a half");
        params.set("b", "TWO");
        params.delete("a", "1");
        Deno.serve(() => Response.json({
            get: params.get("b"),
            all: params.getAll("a"),
            has: params.has("empty"),
            missing: params.get("nothing"),
            size: params.size,
            string: params.toString(),
            entries: Array.from(params.entries()),
            keys: Array.from(params.keys()),
            fromPairs: new URLSearchParams([["one", "1"], ["two", "2"]]).toString(),
            fromObject: new URLSearchParams({ one: 1, two: "2 3" }).toString(),
            decoded: new URLSearchParams("who=ana+bo%C3%9F&where=%2Fone%2Ftwo").get("who"),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["get"], "TWO");
    assert_eq!(said["all"], serde_json::json!(["3"]));
    assert_eq!(said["has"], true);
    assert_eq!(said["missing"], serde_json::Value::Null);
    assert_eq!(said["size"], 4);
    assert_eq!(said["string"], "b=TWO&a=3&empty=&c=a+value+%26+a+half");
    assert_eq!(said["keys"], serde_json::json!(["b", "a", "empty", "c"]));
    assert_eq!(said["fromPairs"], "one=1&two=2");
    assert_eq!(said["fromObject"], "one=1&two=2+3");
    assert_eq!(said["decoded"], "ana boß");
    assert_eq!(
        said["entries"],
        serde_json::json!([
            ["b", "TWO"],
            ["a", "3"],
            ["empty", ""],
            ["c", "a value & a half"]
        ])
    );
}

/// A url's `searchParams` is the url's, which is the part of this that
/// is easy to get wrong: two objects that agree once and then drift.
#[test]
fn a_urls_query_and_the_url_are_the_same_thing() {
    let answer = answered(
        r#"
        const url = new URL("https://example.com/one?a=1");
        const params = url.searchParams;
        params.set("b", "2");
        const afterAppend = url.href;
        url.search = "?c=3";
        const afterSearch = Array.from(params.entries());
        Deno.serve(() => Response.json({
            same: url.searchParams === params,
            afterAppend,
            afterSearch,
            sorted: (() => {
                const other = new URL("https://example.com/?b=2&a=1&b=1");
                other.searchParams.sort();
                return other.href;
            })(),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["same"], true);
    assert_eq!(said["afterAppend"], "https://example.com/one?a=1&b=2");
    assert_eq!(said["afterSearch"], serde_json::json!([["c", "3"]]));
    assert_eq!(said["sorted"], "https://example.com/?a=1&b=2&b=1");
}

/// `req.url` is a url, so a handler can take it apart, which is how
/// every function that routes on a path or reads a query is written.
#[test]
fn a_handler_can_read_its_own_url_with_the_parser() {
    let answer = called(
        r#"
        Deno.serve((req) => {
            const url = new URL(req.url);
            return Response.json({
                path: url.pathname,
                who: url.searchParams.get("who"),
                normalised: new Request("https://example.com").url,
            });
        });
        "#,
        get("http://localhost:9000/functions/v1/hello/one/two?who=ana"),
    )
    .expect("an answer");
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["path"], "/functions/v1/hello/one/two");
    assert_eq!(said["who"], "ana");
    // A Request's url is parsed rather than kept as it was written, the
    // same as Deno, so the empty path is the root.
    assert_eq!(said["normalised"], "https://example.com/");
}

#[test]
fn a_blob_is_bytes_with_a_type_on_it() {
    let answer = answered(
        r#"
        const blob = new Blob(["one ", new TextEncoder().encode("two "), new Blob(["three"])], {
            type: "TEXT/Plain",
        });
        Deno.serve(async () => Response.json({
            size: blob.size,
            type: blob.type,
            text: await blob.text(),
            sliced: await blob.slice(4, 8).text(),
            slicedType: blob.slice(0, 1, "application/json").type,
            fromTheEnd: await blob.slice(-5).text(),
            bytes: Array.from(await new Blob(["ß"]).bytes()),
            buffer: (await blob.arrayBuffer()).byteLength,
            empty: new Blob().size,
            refused: (() => { try { new Blob("one"); return "made one"; } catch (e) { return e.message; } })(),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["size"], 13);
    // Lowercased, because a media type is compared case insensitively
    // and this is where that is settled rather than at every reader.
    assert_eq!(said["type"], "text/plain");
    assert_eq!(said["text"], "one two three");
    assert_eq!(said["sliced"], "two ");
    assert_eq!(said["slicedType"], "application/json");
    assert_eq!(said["fromTheEnd"], "three");
    assert_eq!(said["bytes"], serde_json::json!([195, 159]));
    assert_eq!(said["buffer"], 13);
    assert_eq!(said["empty"], 0);
    assert_eq!(said["refused"], "Blob parts must be an iterable of parts");
}

#[test]
fn a_file_is_a_blob_that_knows_what_it_is_called() {
    let answer = answered(
        r#"
        const file = new File(["a,b\n1,2\n"], "rows.csv", { type: "text/csv", lastModified: 1700000000000 });
        Deno.serve(async () => Response.json({
            name: file.name,
            type: file.type,
            size: file.size,
            when: file.lastModified,
            text: await file.text(),
            isBlob: file instanceof Blob,
            needsAName: (() => { try { new File(["x"]); return "made one"; } catch (e) { return e.message; } })(),
            keys: Object.keys(file),
            json: JSON.stringify(file),
            onProto: typeof Object.getOwnPropertyDescriptor(File.prototype, "name").get,
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["name"], "rows.csv");
    assert_eq!(said["type"], "text/csv");
    assert_eq!(said["size"], 8);
    assert_eq!(said["when"], 1_700_000_000_000i64);
    assert_eq!(said["text"], "a,b\n1,2\n");
    assert_eq!(said["isBlob"], true);
    assert_eq!(said["needsAName"], "File requires a name");
    // What it is called is on the prototype rather than on the file, so
    // a file has nothing of its own that a `JSON.stringify` or a
    // `structuredClone` can see, which is where upstream keeps it.
    assert_eq!(said["keys"], serde_json::json!([]));
    assert_eq!(said["json"], "{}");
    assert_eq!(said["onProto"], "function");
}

/// A form written out as multipart and read straight back in, which is
/// the only test that says the writer and the reader agree. The bytes
/// in it are not utf-8 on purpose: a part is bytes, and a round trip
/// through a string would lose them.
#[test]
fn a_form_goes_out_as_multipart_and_comes_back_the_same_form() {
    let answer = answered(
        r#"
        const form = new FormData();
        form.append("who", "ana");
        form.append("who", "ben");
        form.append("file", new File([new Uint8Array([0, 159, 146, 255])], "raw.bin", {
            type: "application/octet-stream",
        }));
        const request = new Request("https://example.com/", { method: "POST", body: form });
        const type = request.headers.get("content-type");
        const read = await request.formData();
        const file = read.get("file");
        Deno.serve(async () => Response.json({
            type: type.split(";")[0],
            boundary: type.includes("boundary=") && !type.endsWith("boundary="),
            who: read.getAll("who"),
            keys: Array.from(read.keys()),
            name: file.name,
            fileType: file.type,
            bytes: Array.from(await file.bytes()),
            missing: read.get("nobody"),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["type"], "multipart/form-data");
    assert_eq!(said["boundary"], true);
    assert_eq!(said["who"], serde_json::json!(["ana", "ben"]));
    assert_eq!(said["keys"], serde_json::json!(["who", "who", "file"]));
    assert_eq!(said["name"], "raw.bin");
    assert_eq!(said["fileType"], "application/octet-stream");
    assert_eq!(said["bytes"], serde_json::json!([0, 159, 146, 255]));
    assert_eq!(said["missing"], serde_json::Value::Null);
}

/// The other form encoding, which is the one an html form posts and the
/// one a handler is likelier to be sent.
#[test]
fn a_posted_form_is_read_as_a_form() {
    let answer = called(
        r#"
        Deno.serve(async (req) => {
            const form = await req.formData();
            return Response.json({
                who: form.get("who"),
                said: form.get("said"),
                has: form.has("who"),
                afterSet: (() => { form.set("who", "ben"); return form.getAll("who"); })(),
                afterDelete: (() => { form.delete("said"); return Array.from(form.keys()); })(),
            });
        });
        "#,
        Call {
            method: "POST".to_string(),
            url: "http://localhost:9000/functions/v1/hello".to_string(),
            headers: vec![(
                "content-type".to_string(),
                "application/x-www-form-urlencoded".to_string(),
            )],
            body: b"who=ana&said=a+value+%26+a+half".to_vec(),
            execution_id: "one".to_string(),
        },
    )
    .expect("an answer");
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["who"], "ana");
    assert_eq!(said["said"], "a value & a half");
    assert_eq!(said["has"], true);
    assert_eq!(said["afterSet"], serde_json::json!(["ben"]));
    assert_eq!(said["afterDelete"], serde_json::json!(["who"]));
}

/// A blob as a body, and a body as a blob, which is the pair of them a
/// storage client uses in both directions.
#[test]
fn a_blob_can_be_sent_and_a_body_can_be_read_as_one() {
    let answer = answered(
        r#"
        const sent = new Response(new Blob(["{}"], { type: "application/json" }));
        const read = await new Response("some bytes", { headers: { "content-type": "text/csv" } }).blob();
        Deno.serve(async () => Response.json({
            type: sent.headers.get("content-type"),
            body: await sent.text(),
            blobType: read.type,
            blobText: await read.text(),
            params: await new Response(new URLSearchParams({ a: "1" })).text(),
            paramsType: new Response(new URLSearchParams({ a: "1" })).headers.get("content-type"),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["type"], "application/json");
    assert_eq!(said["body"], "{}");
    assert_eq!(said["blobType"], "text/csv");
    assert_eq!(said["blobText"], "some bytes");
    assert_eq!(said["params"], "a=1");
    assert_eq!(
        said["paramsType"],
        "application/x-www-form-urlencoded;charset=UTF-8"
    );
}

#[test]
fn random_bytes_are_random_and_land_where_they_were_asked_for() {
    let answer = answered(
        r#"
        const one = crypto.getRandomValues(new Uint8Array(16));
        const two = crypto.getRandomValues(new Uint8Array(16));
        const wide = crypto.getRandomValues(new Uint32Array(4));
        const inside = new Uint8Array(8);
        const view = new Uint8Array(inside.buffer, 4, 4);
        crypto.getRandomValues(view);
        Deno.serve(() => Response.json({
            length: one.length,
            differ: one.some((byte, at) => byte !== two[at]),
            filled: one.some((byte) => byte !== 0),
            same: crypto.getRandomValues(one) === one,
            wide: wide.some((word) => word > 65535),
            untouched: Array.from(inside.slice(0, 4)),
            uuid: crypto.randomUUID(),
            unique: crypto.randomUUID() !== crypto.randomUUID(),
            floats: (() => {
                try { crypto.getRandomValues(new Float64Array(4)); return "filled one"; }
                catch (e) { return e.message; }
            })(),
            tooMuch: (() => {
                try { crypto.getRandomValues(new Uint8Array(65537)); return "filled one"; }
                catch (e) { return e.message.split("(")[0].trim(); }
            })(),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["length"], 16);
    assert_eq!(said["differ"], true);
    assert_eq!(said["filled"], true);
    assert_eq!(said["same"], true);
    // A wider array is filled with bytes and not with small numbers,
    // which is what a fill that only wrote the first byte of each word
    // would look like.
    assert_eq!(said["wide"], true);
    // The bytes beside the view are not the view's.
    assert_eq!(said["untouched"], serde_json::json!([0, 0, 0, 0]));
    let uuid = said["uuid"].as_str().expect("a uuid");
    assert_eq!(uuid.len(), 36);
    assert_eq!(uuid.as_bytes()[14], b'4', "{uuid}");
    assert!(
        ['8', '9', 'a', 'b'].contains(&uuid.chars().nth(19).expect("a variant")),
        "{uuid}"
    );
    assert_eq!(said["unique"], true);
    assert_eq!(
        said["floats"],
        "The provided ArrayBufferView is not an integer array type"
    );
    assert_eq!(said["tooMuch"], "The ArrayBufferView's byte length");
}

/// The digests, checked against the values the command line tools give
/// for the same input, which is what says the bytes crossed intact.
#[test]
fn a_digest_is_the_digest_everything_else_computes() {
    let answer = answered(
        r#"
        const hex = (buffer) =>
            Array.from(new Uint8Array(buffer), (byte) => byte.toString(16).padStart(2, "0")).join("");
        const zou = new TextEncoder().encode("zou");
        Deno.serve(async () => Response.json({
            one: hex(await crypto.subtle.digest("SHA-1", zou)),
            "256": hex(await crypto.subtle.digest("SHA-256", zou)),
            "384": hex(await crypto.subtle.digest("SHA-384", zou)),
            "512": hex(await crypto.subtle.digest("SHA-512", zou)),
            named: hex(await crypto.subtle.digest({ name: "sha-256" }, zou)),
            fromBuffer: hex(await crypto.subtle.digest("SHA-256", zou.buffer)),
            missing: await crypto.subtle.digest("MD5", zou).catch((e) => e.message),
            notBytes: await crypto.subtle.digest("SHA-256", "zou").catch((e) => e.message),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["one"], "138c4434ce6b0de777e96966217455e122753986");
    assert_eq!(
        said["256"],
        "b20a7d254bdab4ee822c1973b2dca94197261860c5ad468b401c430a9d2c6ca4"
    );
    assert_eq!(
        said["384"],
        "40c98c7cedcf7a474f65d0d3648bbd85128b898dd50d82354b0c7f6ee11c6c61c57a708216355db4015e9ff1a33284fd"
    );
    assert_eq!(
        said["512"],
        "cbc2065695af4c488931ed168e8d5da7f043ce9a2502b250b48b34ab2c39d930322652bcd6940be01d12595daa624072688654ebd20f2fd96b28f708da946fb7"
    );
    // A name is matched the way the spec matches it, so the lowercase
    // one is the same hash and not a second one.
    assert_eq!(said["named"], said["256"]);
    assert_eq!(said["fromBuffer"], said["256"]);
    assert_eq!(said["missing"], "Unrecognized algorithm name: MD5");
    assert_eq!(said["notBytes"], "data must be a BufferSource");
}

/// Signing and verifying, which is what a function checking a webhook
/// signature does and is the reason `subtle` is here at all.
#[test]
fn an_hmac_signs_what_it_verifies() {
    let answer = answered(
        r#"
        const hex = (buffer) =>
            Array.from(new Uint8Array(buffer), (byte) => byte.toString(16).padStart(2, "0")).join("");
        const bytes = (text) => new TextEncoder().encode(text);
        const key = await crypto.subtle.importKey(
            "raw", bytes("Jefe"), { name: "HMAC", hash: "SHA-256" }, false, ["sign", "verify"],
        );
        const message = bytes("what do ya want for nothing?");
        const signature = await crypto.subtle.sign("HMAC", key, message);
        const other = await crypto.subtle.importKey(
            "raw", bytes("not Jefe"), { name: "HMAC", hash: { name: "SHA-256" } }, false, ["verify"],
        );
        Deno.serve(async () => Response.json({
            signature: hex(signature),
            hash: key.algorithm.hash.name,
            length: key.algorithm.length,
            type: key.type,
            usages: key.usages,
            verified: await crypto.subtle.verify("HMAC", key, signature, message),
            wrongKey: await crypto.subtle.verify("HMAC", other, signature, message),
            wrongMessage: await crypto.subtle.verify("HMAC", key, signature, bytes("something else")),
            shortSignature: await crypto.subtle.verify("HMAC", key, bytes("short"), message),
            notHmac: await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, message).catch((e) => e.message),
            fromJwk: hex(await crypto.subtle.sign("HMAC", await crypto.subtle.importKey(
                "jwk", { kty: "oct", k: "SmVmZQ" }, { name: "HMAC", hash: "SHA-256" }, false, ["sign"],
            ), message)),
            notDer: await crypto.subtle
                .importKey("pkcs8", bytes("x"), { name: "HMAC", hash: "SHA-256" }, false, ["sign"])
                .catch((e) => e.message),
            noHmacGeneration: await crypto.subtle
                .generateKey({ name: "HMAC", hash: "SHA-256" }, false, ["sign"])
                .catch((e) => e.message),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    // RFC 4231's second test case, the same one the unit tests use, so
    // the whole path from javascript to the mac and back is the value
    // everybody else computes.
    assert_eq!(
        said["signature"],
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
    assert_eq!(said["hash"], "SHA-256");
    assert_eq!(said["length"], 32);
    assert_eq!(said["type"], "secret");
    assert_eq!(said["usages"], serde_json::json!(["sign", "verify"]));
    assert_eq!(said["verified"], true);
    assert_eq!(said["wrongKey"], false);
    assert_eq!(said["wrongMessage"], false);
    assert_eq!(said["shortSignature"], false);
    assert_eq!(
        said["notHmac"],
        "RSASSA-PKCS1-v1_5 is not supported yet, only HMAC is"
    );
    // The same key again, spelled the way a key set spells it, which
    // is the format a key arrives in when it came off a jwks rather
    // than out of a variable.
    assert_eq!(said["fromJwk"], said["signature"]);
    assert_eq!(
        said["notDer"],
        "the pkcs8 key format is not supported yet, only raw and jwk are"
    );
    // A key out of randomness is a key for a cipher here and not for a
    // mac, because the one thing that asks is an AES key nobody handed
    // in.
    assert_eq!(
        said["noHmacGeneration"],
        "HMAC keys cannot be generated yet, only AES can"
    );
}

/// ECDSA over P-256, which is what an access token is signed with and
/// what a function verifying its caller checks against the key set the
/// project publishes.
///
/// The signature is one another implementation made, node's webcrypto
/// over the same key and the same bytes, so a pass here is interop and
/// not this runtime agreeing with itself. The rest is what a key set
/// asks for: the public half out of a jwk verifies, the private half
/// signs what the public half then accepts, a tampered message and a
/// signature of the wrong length are refusals rather than errors, and a
/// public key handed to `sign` is an error rather than a signature.
#[test]
fn an_ecdsa_key_verifies_what_another_runtime_signed() {
    let answer = answered(
        r#"
        const b64 = (text) => {
            const padded = text.replace(/-/g, "+").replace(/_/g, "/");
            const whole = padded + "=".repeat((4 - (padded.length % 4)) % 4);
            return Uint8Array.from(atob(whole), (c) => c.charCodeAt(0));
        };
        const bytes = (text) => new TextEncoder().encode(text);
        const x = "wWGjH6WjqKYdGCGGan8c5-DZmqu0S8bRdJ2t7w0au3s";
        const y = "_tuMqtnqXBkmQJYKr8fsjXIyWr9NU4twsAL3JTDlumc";
        const ecdsa = { name: "ECDSA", namedCurve: "P-256" };
        const sha256 = { name: "ECDSA", hash: "SHA-256" };
        const publicKey = await crypto.subtle.importKey(
            "jwk", { kty: "EC", crv: "P-256", x, y }, ecdsa, false, ["verify"],
        );
        const privateKey = await crypto.subtle.importKey(
            "jwk",
            { kty: "EC", crv: "P-256", d: "ycxb4LMOL6mImeF_ApNvtp4RCpE_5r1VvtNoAmbmDU4", x, y },
            ecdsa, false, ["sign"],
        );
        // The private half spelled with the scalar alone, which is what
        // a key set that never published the point looks like.
        const scalarOnly = await crypto.subtle.importKey(
            "jwk", { kty: "EC", crv: "P-256", d: "ycxb4LMOL6mImeF_ApNvtp4RCpE_5r1VvtNoAmbmDU4" },
            ecdsa, false, ["sign"],
        );
        const elsewhere = b64(
            "JnCTL_JmDRroI-tDnO4nOwGOfp5d69J1XZWRSqCMu0wx_-ttPL5sjQ6pTU08IYOL4zvAWPHVr8SaYqUYqHKMtg",
        );
        const message = bytes("what do ya want for nothing?");
        const signature = await crypto.subtle.sign(sha256, privateKey, message);
        Deno.serve(async () => Response.json({
            type: publicKey.type,
            privateType: privateKey.type,
            curve: publicKey.algorithm.namedCurve,
            length: signature.byteLength,
            fromNode: await crypto.subtle.verify(sha256, publicKey, elsewhere, bytes("zou")),
            fromNodeTampered: await crypto.subtle.verify(sha256, publicKey, elsewhere, bytes("zoU")),
            roundTrip: await crypto.subtle.verify(sha256, publicKey, signature, message),
            byItself: await crypto.subtle.verify(sha256, privateKey, signature, message),
            fromScalarOnly: await crypto.subtle.verify(
                sha256, publicKey, await crypto.subtle.sign(sha256, scalarOnly, message), message,
            ),
            wrongMessage: await crypto.subtle.verify(sha256, publicKey, signature, bytes("else")),
            shortSignature: await crypto.subtle.verify(sha256, publicKey, bytes("short"), message),
            publicSigning: await crypto.subtle
                .sign(sha256, publicKey, message).catch((e) => e.message),
            wrongCall: await crypto.subtle
                .sign("HMAC", privateKey, message).catch((e) => e.message),
            wrongHash: await crypto.subtle
                .sign({ name: "ECDSA", hash: "SHA-512" }, privateKey, message).catch((e) => e.message),
            wrongCurve: await crypto.subtle
                .importKey("jwk", { kty: "EC", crv: "P-384", x, y }, ecdsa, false, ["verify"])
                .catch((e) => e.message),
            notOnTheCurve: await crypto.subtle.verify(
                sha256,
                await crypto.subtle.importKey(
                    "jwk", { kty: "EC", crv: "P-256", x, y: x }, ecdsa, false, ["verify"],
                ),
                signature,
                message,
            ).catch((e) => e.message),
            wrongAlgorithm: await crypto.subtle
                .importKey("jwk", { kty: "EC", crv: "P-256", x, y },
                    { name: "HMAC", hash: "SHA-256" }, false, ["verify"])
                .catch((e) => e.message),
            wrongKind: await crypto.subtle
                .importKey("jwk", { kty: "RSA", n: "0", e: "AQAB" }, ecdsa, false, ["verify"])
                .catch((e) => e.message),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["type"], "public");
    assert_eq!(said["privateType"], "private");
    assert_eq!(said["curve"], "P-256");
    // Two coordinates, which is the shape a JWS carries and not der.
    assert_eq!(said["length"], 64);
    assert_eq!(said["fromNode"], true);
    assert_eq!(said["fromNodeTampered"], false);
    assert_eq!(said["roundTrip"], true);
    assert_eq!(said["byItself"], true);
    assert_eq!(said["fromScalarOnly"], true);
    assert_eq!(said["wrongMessage"], false);
    assert_eq!(said["shortSignature"], false);
    assert_eq!(said["publicSigning"], "a public key signs nothing");
    assert_eq!(
        said["wrongCall"],
        "this key is for ECDSA and the call is for HMAC"
    );
    assert_eq!(
        said["wrongHash"],
        "ECDSA here is P-256 with SHA-256, and this one asked for SHA-512"
    );
    assert_eq!(
        said["wrongCurve"],
        "the only curve here is P-256 and this jwk is on P-384"
    );
    assert_eq!(said["notOnTheCurve"], "the key is not a point on P-256");
    assert_eq!(
        said["wrongAlgorithm"],
        "an EC jwk is an ECDSA key and the call asked for HMAC"
    );
    assert_eq!(
        said["wrongKind"],
        "RSA keys are not supported yet, only oct and EC are"
    );
}

/// AES, which is what a session cookie is written with and is the one
/// thing in the examples corpus that asked `subtle` for a cipher.
///
/// The vector is the first CBC case of NIST SP 800-38A, so the whole
/// path from javascript through the op and back is the ciphertext
/// everybody else computes, and the rest is what a round trip has to
/// hold: a wrong key is one refusal, GCM authenticates what is beside
/// the ciphertext as well as the ciphertext, and a key made here can be
/// read back out only if it said it could be.
#[test]
fn aes_encrypts_what_it_decrypts_in_both_modes() {
    let answer = answered(
        r#"
        const hex = (buffer) =>
            Array.from(new Uint8Array(buffer), (byte) => byte.toString(16).padStart(2, "0")).join("");
        const unhex = (said) => new Uint8Array(said.match(/../g).map((two) => parseInt(two, 16)));
        const text = (buffer) => new TextDecoder().decode(buffer);
        const bytes = (said) => new TextEncoder().encode(said);

        const known = await crypto.subtle.importKey(
            "raw", unhex("2b7e151628aed2a6abf7158809cf4f3c"), "AES-CBC", false, ["encrypt", "decrypt"],
        );
        const iv = unhex("000102030405060708090a0b0c0d0e0f");
        const plain = unhex("6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e51");
        const known256 = hex(await crypto.subtle.encrypt({ name: "AES-CBC", iv }, known, plain));

        const cbc = await crypto.subtle.generateKey({ name: "AES-CBC", length: 256 }, true, ["encrypt", "decrypt"]);
        const said = await crypto.subtle.encrypt({ name: "AES-CBC", iv }, cbc, bytes("a session"));
        const other = await crypto.subtle.generateKey({ name: "AES-CBC", length: 256 }, false, ["decrypt"]);

        const gcmIv = crypto.getRandomValues(new Uint8Array(12));
        const gcm = await crypto.subtle.generateKey({ name: "AES-GCM", length: 128 }, false, ["encrypt", "decrypt"]);
        const sealed = await crypto.subtle.encrypt(
            { name: "AES-GCM", iv: gcmIv, additionalData: bytes("one") }, gcm, bytes("a payload"),
        );

        Deno.serve(async () => Response.json({
            known: known256,
            name: cbc.algorithm.name,
            length: cbc.algorithm.length,
            usages: cbc.usages,
            roundTrip: text(await crypto.subtle.decrypt({ name: "AES-CBC", iv }, cbc, said)),
            padded: said.byteLength,
            wrongKey: await crypto.subtle
                .decrypt({ name: "AES-CBC", iv }, other, said)
                .then(() => "read it", (e) => `${e.name}: ${e.message}`),
            sealed: text(await crypto.subtle.decrypt(
                { name: "AES-GCM", iv: gcmIv, additionalData: bytes("one") }, gcm, sealed,
            )),
            wrongExtra: await crypto.subtle
                .decrypt({ name: "AES-GCM", iv: gcmIv, additionalData: bytes("two") }, gcm, sealed)
                .then(() => "read it", (e) => `${e.name}: ${e.message}`),
            exported: hex(await crypto.subtle.exportKey("raw", cbc)).length,
            notExtractable: await crypto.subtle
                .exportKey("raw", gcm)
                .then(() => "read it", (e) => `${e.name}: ${e.message}`),
            wrongCipher: await crypto.subtle
                .encrypt({ name: "AES-GCM", iv: gcmIv }, cbc, bytes("a session"))
                .catch((e) => e.message),
            shortKey: await crypto.subtle
                .importKey("raw", new Uint8Array(7), "AES-CBC", false, ["encrypt"])
                .then(() => "made one", (e) => `${e.name}: ${e.message}`),
            shortIv: await crypto.subtle
                .encrypt({ name: "AES-CBC", iv: gcmIv }, cbc, bytes("a session"))
                .catch((e) => e.message),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["known"].as_str().expect("a ciphertext")[..64].to_string(),
        "7649abac8119b246cee98e9b12e9197d5086cb9b507219ee95db113a917678b2"
    );
    assert_eq!(said["name"], "AES-CBC");
    assert_eq!(said["length"], 256);
    assert_eq!(said["usages"], serde_json::json!(["encrypt", "decrypt"]));
    assert_eq!(said["roundTrip"], "a session");
    // Nine bytes padded up to the block, which is what PKCS#7 does and
    // is why a ciphertext is longer than what went into it.
    assert_eq!(said["padded"], 16);
    assert_eq!(said["sealed"], "a payload");
    // The one sentence, for the two ways it can fail, and it is a
    // `DOMException` because that is what the specification calls it.
    assert_eq!(said["wrongKey"], "OperationError: Decryption failed");
    assert_eq!(said["wrongExtra"], "OperationError: Decryption failed");
    assert_eq!(said["exported"], 64);
    assert_eq!(
        said["notExtractable"],
        "InvalidAccessError: key is not extractable"
    );
    assert_eq!(
        said["wrongCipher"],
        "this key is for AES-CBC and the encrypt is for AES-GCM"
    );
    assert_eq!(
        said["shortKey"],
        "DataError: an AES key is 128, 192 or 256 bits and this one is 56"
    );
    assert_eq!(
        said["shortIv"],
        "an AES-CBC iv is 16 bytes and this one is 12"
    );
}

/// A handler that sleeps, which is what every retry with a backoff in
/// it is written as.
#[test]
fn a_handler_can_wait_on_the_clock_and_the_answer_waits_with_it() {
    let started = std::time::Instant::now();
    let answer = answered(
        r#"
        const sleep = (millis) => new Promise((wake) => setTimeout(wake, millis));
        Deno.serve(async () => {
            const said = [];
            said.push("before");
            await sleep(60);
            said.push("after");
            return Response.json(said);
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said, serde_json::json!(["before", "after"]));
    // The wait was a real wait and not a promise that resolved at once,
    // which is what a `setTimeout` that ignores its delay would look
    // like from the outside.
    assert!(started.elapsed() >= std::time::Duration::from_millis(60));
}

/// Which timer fires first is the delay's business and not the order
/// they were set in, and a zero delay is still a turn of the loop.
#[test]
fn timers_fire_in_the_order_their_delays_say() {
    let answer = answered(
        r#"
        Deno.serve(() => new Promise((answer) => {
            const said = [];
            setTimeout(() => said.push("thirty"), 30);
            setTimeout(() => said.push("ten"), 10);
            setTimeout(() => said.push("twenty"), 20);
            setTimeout(() => said.push("zero"), 0);
            queueMicrotask(() => said.push("microtask"));
            said.push("now");
            setTimeout(() => answer(Response.json(said)), 60);
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said,
        serde_json::json!(["now", "microtask", "zero", "ten", "twenty", "thirty"])
    );
}

#[test]
fn a_timer_that_was_cleared_does_not_fire_and_one_that_repeats_does() {
    let answer = answered(
        r#"
        Deno.serve(() => new Promise((answer) => {
            const said = [];
            const cleared = setTimeout(() => said.push("this should not be here"), 10);
            clearTimeout(cleared);
            let ticks = 0;
            const every = setInterval(() => {
                ticks += 1;
                said.push(`tick ${ticks}`);
                if (ticks === 3) {
                    clearInterval(every);
                    setTimeout(() => answer(Response.json({ said, id: typeof cleared })), 30);
                }
            }, 10);
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["said"],
        serde_json::json!(["tick 1", "tick 2", "tick 3"])
    );
    // A timer is a number, the same as it is in Deno and in a browser,
    // rather than an object a handler has to hold onto.
    assert_eq!(said["id"], "number");
}

/// A callback throws after whatever set it has returned, so there is
/// nobody left to catch it. Deno ends the process. Here the answer is
/// often already written, so ending the process would lose it.
#[test]
fn a_timer_that_throws_does_not_take_the_call_with_it() {
    let answer = answered(
        r#"
        Deno.serve(() => new Promise((answer) => {
            setTimeout(() => { throw new Error("thrown from a timer"); }, 5);
            setTimeout(() => answer(new Response("answered anyway")), 30);
        }));
        "#,
    );
    assert_eq!(body(&answer), "answered anyway");
}

/// A module is done when its own evaluation says so and not when the
/// event loop runs out of things to do, which is a distinction a module
/// with an interval in it makes: `createClient` starts a refresh ticker
/// while it is being imported, and waiting for an idle loop after that
/// is waiting forever.
#[test]
fn a_module_that_leaves_a_timer_running_still_answers() {
    let answer = answered(
        r#"
        const ticking = setInterval(() => {}, 5);
        Deno.serve(() => new Response(`answered with ${typeof ticking}`));
        "#,
    );
    assert_eq!(body(&answer), "answered with number");
}

/// The other half of that: a module that never finishes is an error
/// saying so rather than a call that hangs until something kills it.
#[test]
fn a_module_that_never_finishes_evaluating_says_so() {
    let refused = called(
        r#"
        await new Promise(() => {});
        Deno.serve(() => new Response("never reached"));
        "#,
        get("http://localhost:9000/functions/v1/hello"),
    )
    .expect_err("a module that does not finish")
    .why()
    .to_string();
    assert!(
        refused.contains("Top-level await promise never resolved"),
        "{refused}"
    );
}

#[test]
fn a_string_of_code_is_not_a_timer_callback_here() {
    let answer = answered(
        r#"
        Deno.serve(() => Response.json({
            refused: (() => {
                try { setTimeout("globalThis.snuck = true", 0); return "took it"; }
                catch (e) { return e.message; }
            })(),
            snuck: globalThis.snuck ?? null,
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["refused"],
        "a timer needs a function, and a string of code is not one here"
    );
    assert_eq!(said["snuck"], serde_json::Value::Null);
}

#[test]
fn a_handler_may_await_and_the_answer_waits_for_it() {
    let answer = answered(
        r#"
        const later = () => Promise.resolve("awaited");
        Deno.serve(async () => new Response(await later()));
        "#,
    );
    assert_eq!(body(&answer), "awaited");
}

#[test]
fn the_entry_point_is_not_something_a_function_can_reach() {
    let answer = answered(
        r#"
        Deno.serve(() => Response.json({
            run: typeof globalThis.run,
            drain: typeof globalThis.drain,
            handler: typeof globalThis.handler,
            names: Object.keys(globalThis).filter((n) => n.toLowerCase().includes("run")),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["run"], "undefined");
    assert_eq!(said["drain"], "undefined");
    assert_eq!(said["handler"], "undefined");
    // `EdgeRuntime` is a global a function is meant to have and it has
    // the word in it, which is the whole of why it is named here.
    assert_eq!(said["names"], serde_json::json!(["EdgeRuntime"]));
}

#[test]
fn serving_twice_is_the_functions_own_mistake() {
    let complaint = called(
        r#"
        Deno.serve(() => new Response("one"));
        Deno.serve(() => new Response("two"));
        "#,
        get("http://localhost:9000/functions/v1/hello"),
    )
    .expect_err("a complaint")
    .why()
    .to_string();
    assert!(complaint.contains("called twice"), "{complaint}");
}

#[test]
fn the_runtime_says_what_it_is() {
    assert_eq!(Isolate::new().describe(), "a v8 isolate per call");
    assert!(zou_deno::available());
}

// -------------------------------------------------------------------
// EdgeRuntime.waitUntil

#[test]
fn work_left_after_the_answer_does_not_keep_the_caller_waiting_for_it() {
    let deployed = deployed(
        r#"
        Deno.serve(() => {
            EdgeRuntime.waitUntil(new Promise((resolve) => setTimeout(resolve, 200)));
            return new Response("answered");
        });
        "#,
    );
    let started = std::time::Instant::now();
    let (sent, arrived) = std::sync::mpsc::channel();
    Isolate::new()
        .invoke_answering(
            &deployed.function,
            get("http://localhost:9000/functions/v1/hello"),
            Box::new(move |answer| {
                let _ = sent.send((answer, started.elapsed()));
            }),
        )
        .expect("it ran");
    let finished = started.elapsed();
    let (answer, answered) = arrived.recv().expect("an answer");
    assert_eq!(body(&answer), "answered");
    // The answer is handed over while the work is still going, and the
    // call is not finished until the work is.
    assert!(
        answered < std::time::Duration::from_millis(150),
        "the caller waited for the background work: {answered:?}"
    );
    assert!(
        finished >= std::time::Duration::from_millis(200),
        "the background work did not get to run: {finished:?}"
    );
}

#[test]
fn work_left_after_the_answer_is_really_run_and_may_leave_more_behind_it() {
    let server = wire::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(() => {{
            EdgeRuntime.waitUntil(
                fetch("{first}", {{ method: "POST" }}).then(() => {{
                    // Work registered from inside work is still work.
                    EdgeRuntime.waitUntil(fetch("{second}", {{ method: "POST" }}));
                }}),
            );
            return new Response("answered");
        }});
        "#,
        first = server.url("/webhook"),
        second = server.url("/and-another"),
    ));
    assert_eq!(body(&answer), "answered");
    assert!(server.saw("/webhook"), "the background work never ran");
    assert!(
        server.saw("/and-another"),
        "work registered from inside work never ran"
    );
}

#[test]
fn work_that_fails_after_the_answer_is_not_the_callers_problem() {
    let server = wire::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(() => {{
            EdgeRuntime.waitUntil(Promise.reject(new Error("nobody is listening")));
            EdgeRuntime.waitUntil(fetch("{after}"));
            return new Response("answered", {{ status: 202 }});
        }});
        "#,
        after = server.url("/after-the-failure"),
    ));
    // The rejection is logged and the rest of the work still happens.
    assert_eq!(answer.status, 202);
    assert_eq!(body(&answer), "answered");
    assert!(server.saw("/after-the-failure"));
}

// -------------------------------------------------------------------
// fetch

/// A server on a port nobody chose, for the tests that call out.
///
/// Written here rather than pulled in, because a test of `fetch` that
/// needs an HTTP client to be trustworthy is testing two things.
mod wire {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};

    pub struct Server {
        pub port: u16,
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl Server {
        pub fn url(&self, path: &str) -> String {
            format!("http://127.0.0.1:{}{path}", self.port)
        }

        /// Whether a request for `path` ever arrived, which is the only
        /// way a test can see work that answered nobody.
        pub fn saw(&self, path: &str) -> bool {
            self.seen
                .lock()
                .expect("the server thread is not holding it")
                .iter()
                .any(|asked| asked == path)
        }
    }

    /// A port that had a listener on it and does not any more, which is
    /// the closest a test can get to a host that will not answer
    /// without waiting for a timeout.
    pub fn closed() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
        let port = listener.local_addr().expect("an address").port();
        drop(listener);
        port
    }

    pub fn start() -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
        let port = listener.local_addr().expect("an address").port();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let kept = Arc::clone(&seen);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let kept = Arc::clone(&kept);
                std::thread::spawn(move || answer(stream, kept));
            }
        });
        Server { port, seen }
    }

    fn answer(mut stream: TcpStream, seen: Arc<Mutex<Vec<String>>>) {
        let mut reader = BufReader::new(stream.try_clone().expect("a second handle"));
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() || line.is_empty() {
            return;
        }
        let mut parts = line.split_whitespace();
        let method = parts.next().unwrap_or("").to_string();
        let path = parts.next().unwrap_or("/").to_string();
        let mut headers: Vec<(String, String)> = Vec::new();
        let mut length = 0usize;
        loop {
            let mut header = String::new();
            if reader.read_line(&mut header).is_err() {
                return;
            }
            let header = header.trim_end().to_string();
            if header.is_empty() {
                break;
            }
            let Some((name, value)) = header.split_once(':') else {
                continue;
            };
            let name = name.trim().to_lowercase();
            let value = value.trim().to_string();
            if name == "content-length" {
                length = value.parse().unwrap_or(0);
            }
            headers.push((name, value));
        }
        let mut body = vec![0u8; length];
        if length > 0 && reader.read_exact(&mut body).is_err() {
            return;
        }
        seen.lock()
            .expect("nobody else is holding it")
            .push(path.clone());
        // Long enough for a signal to fire while the call is still out,
        // which is the only way a test can end one in flight rather
        // than end one that had not started.
        if path == "/slow" {
            std::thread::sleep(std::time::Duration::from_millis(750));
        }
        let body = String::from_utf8_lossy(&body).to_string();
        let (status, reason, kind, said) = match path.as_str() {
            "/moved" => (302, "Found", "text/plain", String::new()),
            "/landed" => (200, "OK", "text/plain", "landed".to_string()),
            "/teapot" => (418, "I'm a Teapot", "text/plain", "no".to_string()),
            // Everything else is a mirror: what the function sent, back
            // as json, which is the only way a test can see what left.
            _ => {
                let shown: Vec<serde_json::Value> = headers
                    .iter()
                    .map(|(name, value)| serde_json::json!({ "name": name, "value": value }))
                    .collect();
                (
                    200,
                    "OK",
                    "application/json",
                    serde_json::json!({
                        "method": method,
                        "path": path,
                        "body": body,
                        "headers": shown,
                    })
                    .to_string(),
                )
            }
        };
        let mut head = format!(
            "HTTP/1.1 {status} {reason}\r\ncontent-type: {kind}\r\ncontent-length: {}\r\nx-said-by: the test server\r\nconnection: close\r\n",
            said.len()
        );
        if path == "/moved" {
            head.push_str("location: /landed\r\n");
        }
        head.push_str("\r\n");
        let _ = stream.write_all(head.as_bytes());
        let _ = stream.write_all(said.as_bytes());
        let _ = stream.flush();
    }
}

#[test]
fn a_function_may_call_out_and_read_what_came_back() {
    let server = wire::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const res = await fetch("{}");
            const said = await res.json();
            return Response.json({{ ok: res.ok, status: res.status, url: res.url, redirected: res.redirected, method: said.method, path: said.path }});
        }});
        "#,
        server.url("/echo")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["ok"], true);
    assert_eq!(said["status"], 200);
    assert_eq!(said["method"], "GET");
    assert_eq!(said["path"], "/echo");
    assert_eq!(said["url"], server.url("/echo"));
    assert_eq!(said["redirected"], false);
}

#[test]
fn what_a_function_posts_is_what_arrives() {
    let server = wire::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const res = await fetch("{}", {{
                method: "POST",
                headers: {{ "x-asked-by": "the function" }},
                body: JSON.stringify({{ name: "world" }}),
            }});
            const said = await res.json();
            const header = (name) => (said.headers.find((h) => h.name === name) ?? {{ value: null }}).value;
            return Response.json({{
                method: said.method,
                body: said.body,
                asked: header("x-asked-by"),
                type: header("content-type"),
                agent: header("user-agent"),
                navigator: navigator.userAgent,
                length: header("content-length"),
            }});
        }});
        "#,
        server.url("/echo")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["method"], "POST");
    assert_eq!(said["body"], "{\"name\":\"world\"}");
    assert_eq!(said["asked"], "the function");
    // Set by the Request constructor because the body is a string, the
    // same as it would be in Deno.
    assert_eq!(said["type"], "text/plain;charset=UTF-8");
    // The same string `navigator.userAgent` says, which is what
    // upstream sends: measured on a real `supabase start` by having a
    // function fetch an echo and read the header back, where both were
    // `Deno/2.1.4 (variant; SupabaseEdgeRuntime/1.74.2)`.
    assert_eq!(said["agent"], said["navigator"]);
    assert!(
        said["agent"]
            .as_str()
            .expect("a string")
            .starts_with("Deno/2.1.4 (variant; zou/"),
        "{}",
        said["agent"]
    );
    assert_eq!(said["length"], "16");
}

#[test]
fn an_answer_that_is_not_ok_is_still_an_answer() {
    let server = wire::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const res = await fetch("{}");
            return Response.json({{ ok: res.ok, status: res.status, statusText: res.statusText, said: await res.text(), by: res.headers.get("x-said-by") }});
        }});
        "#,
        server.url("/teapot")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["ok"], false);
    assert_eq!(said["status"], 418);
    // The canonical phrase for the code and not the one the server
    // wrote on the status line, which the client does not keep. A
    // server answering 418 with the word "no" as its reason is a server
    // whose reason a handler here cannot read.
    assert_eq!(said["statusText"], "I'm a teapot");
    assert_eq!(said["said"], "no");
    assert_eq!(said["by"], "the test server");
}

#[test]
fn a_redirect_is_followed_and_the_answer_says_where_it_landed() {
    let server = wire::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const res = await fetch("{}");
            return Response.json({{ url: res.url, redirected: res.redirected, said: await res.text() }});
        }});
        "#,
        server.url("/moved")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["said"], "landed");
    assert_eq!(said["url"], server.url("/landed"));
    assert_eq!(said["redirected"], true);
}

#[test]
fn a_host_that_will_not_answer_is_a_type_error_naming_the_url() {
    let url = format!("http://127.0.0.1:{}/nothing", wire::closed());
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            try {{
                await fetch("{url}");
                return new Response("it worked, which it should not have");
            }} catch (e) {{
                return new Response(`${{e.constructor.name}}: ${{e.message}}`);
            }}
        }});
        "#
    ));
    let said = body(&answer);
    assert!(
        said.starts_with("TypeError: error sending request for url"),
        "{said}"
    );
    assert!(said.contains(&url), "{said}");
}

#[test]
fn a_scheme_fetch_does_not_serve_says_which_one() {
    let answer = answered(
        r#"
        Deno.serve(async () => {
            const tried = [];
            for (const url of ["file:///etc/passwd", "data:text/plain,hi", "nonsense"]) {
                try {
                    await fetch(url);
                    tried.push("it worked");
                } catch (e) {
                    tried.push(e.message);
                }
            }
            return Response.json(tried);
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said[0], "fetch does not serve the file scheme yet");
    assert_eq!(said[1], "fetch does not serve the data scheme yet");
    assert_eq!(said[2], "Invalid URL: 'nonsense'");
}

#[test]
fn a_function_may_call_out_more_than_once() {
    let server = wire::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const first = await (await fetch("{first}")).json();
            const second = await (await fetch("{second}")).json();
            return Response.json([first.path, second.path]);
        }});
        "#,
        first = server.url("/one"),
        second = server.url("/two")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said, serde_json::json!(["/one", "/two"]));
}

#[test]
fn a_request_may_be_fetched_and_its_body_goes_with_it() {
    let server = wire::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const request = new Request("{}", {{ method: "PUT", body: "the bytes" }});
            const said = await (await fetch(request)).json();
            return Response.json({{ method: said.method, body: said.body }});
        }});
        "#,
        server.url("/echo")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["method"], "PUT");
    assert_eq!(said["body"], "the bytes");
}

/// The signal a library bounds a call with, which is how `jose` fetches
/// a jwks and how most of the sdks reach anything at all.
///
/// What is asserted is the rejection and its name: a caller that gave up
/// gets an `AbortError` and a clock that ran out gets a `TimeoutError`,
/// and a library branches on which.
#[test]
fn a_call_ends_when_the_signal_it_was_given_says_so() {
    let server = wire::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const name = async (signal) => {{
                try {{
                    await fetch("{slow}", {{ signal }});
                    return "answered";
                }} catch (e) {{
                    return `${{e.name}}: ${{e.message}}`;
                }}
            }};
            const giving_up = new AbortController();
            setTimeout(() => giving_up.abort(), 50);
            const gave_up = await name(giving_up.signal);
            const ran_out = await name(AbortSignal.timeout(50));
            const already = await name(AbortSignal.abort());
            const reason = await name(AbortSignal.abort(new Error("no time for that")));
            // The same fetch, with nothing to end it, still answers.
            const fine = await name(undefined);
            return Response.json({{ gave_up, ran_out, already, reason, fine }});
        }});
        "#,
        slow = server.url("/slow")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["gave_up"], "AbortError: The signal has been aborted");
    assert_eq!(said["ran_out"], "TimeoutError: Signal timed out.");
    assert_eq!(said["already"], "AbortError: The signal has been aborted");
    assert_eq!(said["reason"], "Error: no time for that");
    assert_eq!(said["fine"], "answered");
}

/// A call that was given up on had already left, and no signal takes a
/// request back: what an abort ends is the answer, and the server has
/// seen the path either way. What happens to the connection after that
/// is `tests/hangup.rs`, which watches from the other end.
///
/// Four of the five calls above are aborted and only four requests can
/// have been in flight, so what this asserts is that the aborted ones
/// still arrived rather than how many did.
#[test]
fn a_call_that_was_given_up_on_had_already_gone_out() {
    let server = wire::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            try {{
                await fetch("{slow}", {{ signal: AbortSignal.timeout(50) }});
                return Response.json({{ threw: null }});
            }} catch (e) {{
                return Response.json({{ threw: e.name }});
            }}
        }});
        "#,
        slow = server.url("/slow")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["threw"], "TimeoutError");
    assert!(server.saw("/slow"), "the request went out anyway");
}

/// A request always has a signal, even when nobody gave it one, because
/// a caller passing `request.signal` on to the next call is the ordinary
/// way a handler forwards a cancellation it might one day get.
///
/// A request handed one gets its own that follows it, which is what a
/// real `supabase start` was measured doing: the identity is not the
/// signal that was passed in, and aborting the controller aborts the
/// request's anyway, through a clone and through a copy.
#[test]
fn a_request_always_has_a_signal_to_pass_on() {
    let server = wire::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const named = (signal) => signal.aborted ? signal.reason.message : null;
            const bare = new Request("{echo}");
            const giving_up = new AbortController();
            const built = new Request("{echo}", {{ signal: giving_up.signal }});
            const copied = built.clone();
            const onward = new Request(built);
            const before = [named(built.signal), named(copied.signal), named(onward.signal)];
            giving_up.abort(new Error("the caller"));
            return Response.json({{
                bare: bare.signal instanceof AbortSignal,
                asleep: bare.signal.aborted,
                same: built.signal === giving_up.signal,
                before,
                after: [named(built.signal), named(copied.signal), named(onward.signal)],
            }});
        }});
        "#,
        echo = server.url("/echo")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["bare"], true);
    assert_eq!(said["asleep"], false);
    assert_eq!(said["same"], false);
    assert_eq!(said["before"], serde_json::json!([null, null, null]));
    assert_eq!(
        said["after"],
        serde_json::json!(["the caller", "the caller", "the caller"])
    );
}

/// A request built with a signal and then fetched as a request rather
/// than as a url, which is how a handler forwards a call it was given
/// and is the path where a signal is easiest to drop on the floor.
#[test]
fn a_signal_on_a_request_bounds_the_call_the_request_becomes() {
    let server = wire::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const bounded = new Request("{slow}", {{ signal: AbortSignal.timeout(50) }});
            try {{
                await fetch(bounded);
                return Response.json({{ threw: "answered" }});
            }} catch (e) {{
                return Response.json({{ threw: `${{e.name}}: ${{e.message}}` }});
            }}
        }});
        "#,
        slow = server.url("/slow")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["threw"], "TimeoutError: Signal timed out.");
}

#[test]
fn a_stream_a_function_wrote_is_read_back_a_chunk_at_a_time() {
    let answer = answered(
        r#"
        Deno.serve(async () => {
            const asked = [];
            let at = 0;
            const stream = new ReadableStream({
                start(controller) { asked.push("start"); },
                pull(controller) {
                    at += 1;
                    asked.push(`pull ${at}`);
                    if (at > 3) { controller.close(); return; }
                    controller.enqueue(new TextEncoder().encode(`chunk ${at} `));
                },
            });
            const seen = [];
            for await (const chunk of stream) {
                seen.push(new TextDecoder().decode(chunk));
            }
            return Response.json({ seen, asked });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["seen"],
        serde_json::json!(["chunk 1 ", "chunk 2 ", "chunk 3 "])
    );
    // The source is asked for more as the reader takes what is there,
    // rather than everything up front.
    assert_eq!(
        said["asked"],
        serde_json::json!(["start", "pull 1", "pull 2", "pull 3", "pull 4"])
    );
}

/// A body that is a stream, which is the shape a handler that builds
/// its answer as it goes writes. Read through the blocking shape of a
/// call here, so what is asserted is that all of it arrives and in
/// order. That it arrives a chunk at a time is asserted further down,
/// where the answer is taken the way the server takes it.
#[test]
fn a_response_may_be_given_a_stream_and_the_caller_gets_all_of_it() {
    let answer = answered(
        r#"
        Deno.serve(() => {
            const encoder = new TextEncoder();
            let at = 0;
            const stream = new ReadableStream({
                pull(controller) {
                    at += 1;
                    if (at > 3) { controller.close(); return; }
                    controller.enqueue(encoder.encode(`part ${at}\n`));
                },
            });
            return new Response(stream, { headers: { "content-type": "text/plain" } });
        });
        "#,
    );
    assert_eq!(body(&answer), "part 1\npart 2\npart 3\n");
}

#[test]
fn a_reader_is_a_lock_on_the_stream_until_it_is_released() {
    let answer = answered(
        r#"
        Deno.serve(async () => {
            const stream = ReadableStream.from(["one", "two"]);
            const reader = stream.getReader();
            const said = (f) => { try { f(); return "it worked"; } catch (e) { return e.message; } };
            const twice = said(() => stream.getReader());
            const first = await reader.read();
            const second = await reader.read();
            const third = await reader.read();
            reader.releaseLock();
            const released = await reader.read().then(() => "it read", (e) => e.message);
            return Response.json({
                twice,
                locked: stream.locked,
                first,
                second,
                third,
                released,
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["twice"], "the stream is locked to a reader");
    assert_eq!(said["locked"], false);
    assert_eq!(
        said["first"],
        serde_json::json!({ "value": "one", "done": false })
    );
    assert_eq!(
        said["second"],
        serde_json::json!({ "value": "two", "done": false })
    );
    assert_eq!(said["third"], serde_json::json!({ "done": true }));
    assert_eq!(said["released"], "the reader has been released");
}

#[test]
fn a_body_is_a_stream_whether_or_not_it_started_as_one() {
    let server = wire::start();
    let answer = answered(&format!(
        r#"
        async function all(stream) {{
            const seen = [];
            for await (const chunk of stream) {{
                seen.push(new TextDecoder().decode(chunk));
            }}
            return seen.join("");
        }}
        Deno.serve(async () => {{
            const res = await fetch("{url}");
            const fetched = await all(res.body);
            const made = await all(new Response("what a handler built").body);
            const blob = await all(new Blob(["out of a blob"]).stream());
            return Response.json({{
                fetched: JSON.parse(fetched).path,
                made,
                blob,
                used: new Response("x").bodyUsed,
                empty: new Response(null).body,
                nothing: new Request("http://example.com/one").body,
                still: (await all(new Response("read as a stream").body)).length,
            }});
        }});
        "#,
        url = server.url("/streamed")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["fetched"], "/streamed");
    assert_eq!(said["made"], "what a handler built");
    assert_eq!(said["blob"], "out of a blob");
    assert_eq!(said["used"], false);
    // A body that is not there is null rather than an empty stream,
    // which is a difference a handler branching on `res.body` sees.
    assert_eq!(said["empty"], serde_json::Value::Null);
    assert_eq!(said["nothing"], serde_json::Value::Null);
    assert_eq!(said["still"], 16);
}

#[test]
fn the_request_a_handler_is_given_can_be_read_as_a_stream() {
    let call = Call {
        method: "POST".to_string(),
        url: "http://localhost:9000/functions/v1/hello".to_string(),
        headers: Vec::new(),
        body: b"the bytes that were posted".to_vec(),
        execution_id: "one".to_string(),
    };
    let answer = called(
        r#"
        Deno.serve(async (req) => {
            const seen = [];
            for await (const chunk of req.body) {
                seen.push(new TextDecoder().decode(chunk));
            }
            const after = req.bodyUsed;
            const again = await req.text().then(() => "read twice", (e) => e.message);
            return Response.json({ seen, after, again });
        });
        "#,
        call,
    )
    .expect("an answer");
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["seen"],
        serde_json::json!(["the bytes that were posted"])
    );
    // Reading the stream is reading the body, so what is left for
    // `text()` is nothing, and it says so rather than answering with an
    // empty string.
    assert_eq!(said["after"], true);
    assert_eq!(said["again"], "Body already consumed.");
}

#[test]
fn a_stream_can_be_split_in_two_and_both_halves_see_everything() {
    let answer = answered(
        r#"
        async function all(stream) {
            const seen = [];
            for await (const chunk of stream) { seen.push(chunk); }
            return seen;
        }
        Deno.serve(async () => {
            const [one, two] = ReadableStream.from(["a", "b", "c"]).tee();
            const both = await Promise.all([all(one), all(two)]);
            const res = new Response("a body worth reading twice");
            const copy = res.clone();
            return Response.json({ both, first: await res.text(), second: await copy.text() });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["both"],
        serde_json::json!([["a", "b", "c"], ["a", "b", "c"]])
    );
    assert_eq!(said["first"], "a body worth reading twice");
    assert_eq!(said["second"], "a body worth reading twice");
}

#[test]
fn a_stream_that_is_given_up_on_tells_its_source_so() {
    let answer = answered(
        r#"
        Deno.serve(async () => {
            const seen = [];
            const stream = new ReadableStream({
                pull(controller) { controller.enqueue("more"); },
                cancel(why) { seen.push(`cancelled because ${why}`); },
            });
            const reader = stream.getReader();
            await reader.read();
            await reader.cancel("nobody wants it");
            const after = await reader.read();
            return Response.json({ seen, after });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["seen"],
        serde_json::json!(["cancelled because nobody wants it"])
    );
    assert_eq!(said["after"], serde_json::json!({ "done": true }));
}

#[test]
fn a_source_that_throws_is_the_readers_error_and_not_a_hang() {
    let answer = answered(
        r#"
        Deno.serve(async () => {
            const stream = new ReadableStream({
                pull() { throw new Error("the source gave up"); },
            });
            const read = await stream.getReader().read().then(() => "it read", (e) => e.message);
            const answered = new ReadableStream({
                start(controller) { controller.error(new Error("errored on purpose")); },
            });
            const body = await new Response(answered).text().then(() => "it read", (e) => e.message);
            return Response.json({ read, body });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["read"], "the source gave up");
    assert_eq!(said["body"], "errored on purpose");
}

/// What is not there is refused by name, the same as the rest of the
/// gaps, because a byte stream and a BYOB reader are a different thing
/// from a stream of chunks and silently treating one as the other is
/// how a function ends up wrong rather than broken.
#[test]
fn a_byte_stream_is_a_gap_and_says_so() {
    let answer = answered(
        r#"
        Deno.serve(() => {
            const said = (f) => { try { f(); return "it worked"; } catch (e) { return e.message; } };
            return Response.json({
                bytes: said(() => new ReadableStream({ type: "bytes" })),
                byob: said(() => new ReadableStream().getReader({ mode: "byob" })),
                chunks: said(() => new Response(ReadableStream.from(["not bytes"]))),
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["bytes"], "a bytes stream is not supported yet");
    assert_eq!(said["byob"], "a byob reader is not supported yet");
    // A body of strings is not an error until somebody asks for the
    // bytes of it, which is where the message is.
    assert_eq!(said["chunks"], "it worked");
}

/// A sink written to by hand: the chunks arrive in the order they were
/// written, one at a time, and closing it is the last thing the sink
/// hears.
#[test]
fn a_writable_stream_takes_chunks_in_the_order_they_were_written() {
    let answer = answered(
        r#"
        Deno.serve(async () => {
            const got: string[] = [];
            const written = new WritableStream({
                start() { got.push("start"); },
                async write(chunk) {
                    // A slow sink, so a writer that did not wait for
                    // it would be out of order here.
                    await new Promise((done) => setTimeout(done, 5 - got.length));
                    got.push(chunk);
                },
                close() { got.push("close"); },
            });
            const writer = written.getWriter();
            const sizes = [writer.desiredSize];
            writer.write("one");
            writer.write("two");
            await writer.write("three");
            sizes.push(writer.desiredSize);
            await writer.close();
            await writer.closed;
            return Response.json({ got, sizes, locked: written.locked });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["got"],
        serde_json::json!(["start", "one", "two", "three", "close"])
    );
    // One before anything is in flight, and nothing left in flight
    // once the last write has been awaited.
    assert_eq!(said["sizes"], serde_json::json!([1, 1]));
    assert_eq!(said["locked"], true);
}

/// A sink that throws is the writer's error, and the stream is errored
/// from then on rather than quietly taking more.
#[test]
fn a_sink_that_throws_is_the_writers_error() {
    let answer = answered(
        r#"
        Deno.serve(async () => {
            const written = new WritableStream({
                write(chunk) {
                    if (chunk === "bad") {
                        throw new Error("the disk was full");
                    }
                },
            });
            const writer = written.getWriter();
            await writer.write("fine");
            let first = "";
            try { await writer.write("bad"); } catch (why) { first = why.message; }
            let after = "";
            try { await writer.write("more"); } catch (why) { after = why.message; }
            let closed = "";
            try { await writer.closed; } catch (why) { closed = why.message; }
            return Response.json({ first, after, closed });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["first"], "the disk was full");
    assert_eq!(said["after"], "the disk was full");
    assert_eq!(said["closed"], "the disk was full");
}

/// The pair, used the way a transform is always used, and the answer
/// is a body the caller reads.
#[test]
fn a_stream_piped_through_a_transform_is_a_body() {
    let answer = answered(
        r#"
        Deno.serve(() => {
            const shouting = new TransformStream({
                transform(chunk, controller) {
                    controller.enqueue(new TextEncoder().encode(chunk.toUpperCase()));
                },
                flush(controller) {
                    controller.enqueue(new TextEncoder().encode("!"));
                },
            });
            const said = ReadableStream.from(["one ", "two ", "three"]);
            return new Response(said.pipeThrough(shouting));
        });
        "#,
    );
    assert_eq!(body(&answer), "ONE TWO THREE!");
}

/// A transform with nothing in it passes chunks through, which is what
/// `new TransformStream()` on its own means, and it is the line the
/// examples that would not load were written with.
#[test]
fn a_transform_that_transforms_nothing_passes_everything_through() {
    let answer = answered(
        r#"
        Deno.serve(async () => {
            const pair = new TransformStream();
            const writer = pair.writable.getWriter();
            (async () => {
                await writer.write("one");
                await writer.write("two");
                await writer.close();
            })();
            const got: string[] = [];
            for await (const chunk of pair.readable) {
                got.push(chunk);
            }
            return Response.json({ got });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["got"], serde_json::json!(["one", "two"]));
}

/// `pipeTo` is the whole stream and then the close, and a source that
/// fails part way is an abort at the other end rather than a close.
#[test]
fn a_pipe_ends_the_way_the_source_ended() {
    let answer = answered(
        r#"
        Deno.serve(async () => {
            const ends: string[] = [];
            const sink = (name: string) => new WritableStream({
                write(chunk) { ends.push(`${name} ${chunk}`); },
                close() { ends.push(`${name} closed`); },
                abort(why) { ends.push(`${name} aborted: ${why.message}`); },
            });
            await ReadableStream.from(["a", "b"]).pipeTo(sink("whole"));
            let failed = "";
            try {
                let given = 0;
                await new ReadableStream({
                    pull(controller) {
                        // A chunk and then a failure, given out one
                        // pull at a time, because a stream that errors
                        // before anything has been read throws its
                        // queue away and nothing would be piped at all.
                        if (given++ === 0) {
                            controller.enqueue("a");
                        } else {
                            controller.error(new Error("the source gave up"));
                        }
                    },
                }).pipeTo(sink("half"));
            } catch (why) {
                failed = why.message;
            }
            return Response.json({ ends, failed });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["ends"],
        serde_json::json!([
            "whole a",
            "whole b",
            "whole closed",
            "half a",
            "half aborted: the source gave up"
        ])
    );
    assert_eq!(said["failed"], "the source gave up");
}

/// The answer to a call, taken the way the server takes it: the moment
/// there is one, rather than when the call is over.
///
/// The isolate is driven on a thread of its own here because that is
/// what `spawn_blocking` does to it, and because the whole claim being
/// tested is that something else can read the body while that thread
/// is still inside the handler.
fn streamed(
    source: &str,
) -> (
    std::sync::mpsc::Receiver<Answer>,
    std::thread::JoinHandle<()>,
) {
    let deployed = deployed(source);
    let (sent, arrives) = std::sync::mpsc::channel();
    let ran = std::thread::spawn(move || {
        let answering = Isolate::new().invoke_answering(
            &deployed.function,
            get("http://localhost:9000/functions/v1/hello"),
            Box::new(move |answer| {
                sent.send(answer).expect("the test is listening");
            }),
        );
        // Held to here so the function's own directory outlives it.
        drop(deployed);
        answering.expect("the call");
    });
    (arrives, ran)
}

/// Every chunk of a streamed body, in order, read on a thread that is
/// allowed to block, which is what the collecting is.
fn chunks(answer: Answer) -> Vec<Result<Vec<u8>, String>> {
    let zou_functions::Body::Chunks(mut chunks) = answer.body else {
        panic!("a body that is still arriving");
    };
    let mut all = Vec::new();
    let held = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime to read on");
    held.block_on(async {
        while let Some(chunk) = chunks.next().await {
            all.push(chunk);
        }
    });
    all
}

/// The point of all of it: the caller reads the first chunk long
/// before the function has made the last one.
///
/// The assertion is about time because the difference is time and
/// nothing else. The chunk is enqueued before the function's second
/// of waiting starts, so if the caller had it in hand well before the
/// call ended, it was not waiting for the end.
#[test]
fn a_streamed_answer_reaches_the_caller_while_it_is_still_being_made() {
    let started = std::time::Instant::now();
    let (arrives, ran) = streamed(
        r#"
        Deno.serve(() => new Response(new ReadableStream({
            async start(controller) {
                const encoder = new TextEncoder();
                controller.enqueue(encoder.encode("first "));
                await new Promise((resolve) => setTimeout(resolve, 1000));
                controller.enqueue(encoder.encode("last"));
                controller.close();
            },
        }), { headers: { "content-type": "text/plain" } }));
        "#,
    );
    let answer = arrives.recv().expect("an answer");
    assert_eq!(answer.status, 200);
    assert_eq!(
        answer.headers,
        vec![("content-type".to_string(), "text/plain".to_string())]
    );
    let zou_functions::Body::Chunks(mut chunks) = answer.body else {
        panic!("a body that is still arriving");
    };
    let held = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime to read on");
    let first = held.block_on(async { chunks.next().await });
    let early = started.elapsed();
    assert_eq!(first, Some(Ok(b"first ".to_vec())));
    let rest = held.block_on(async {
        let mut rest = Vec::new();
        while let Some(chunk) = chunks.next().await {
            rest.push(chunk);
        }
        rest
    });
    ran.join().expect("the call finished");
    assert_eq!(rest, vec![Ok(b"last".to_vec())]);
    assert!(
        started.elapsed() - early >= std::time::Duration::from_millis(500),
        "the first chunk arrived {early:?} in and the call took {:?}",
        started.elapsed()
    );
}

/// A body that goes wrong after its headers have gone out.
///
/// There is no status code left to change, so what the caller gets is
/// what was sent and then an end, which is what a chunked body that
/// stops early is.
#[test]
fn a_streamed_body_that_throws_ends_where_it_got_to() {
    let (arrives, ran) = streamed(
        r#"
        Deno.serve(() => new Response(new ReadableStream({
            start(controller) {
                controller.enqueue(new TextEncoder().encode("as far as here"));
            },
            pull() { throw new Error("the model hung up"); },
        })));
        "#,
    );
    let answer = arrives.recv().expect("an answer");
    let all = chunks(answer);
    ran.join().expect("the call finished");
    assert_eq!(all[0], Ok(b"as far as here".to_vec()));
    assert_eq!(all.len(), 2, "what was sent, and then the reason: {all:?}");
    assert_eq!(all[1], Err("the model hung up".to_string()));
}

/// A stream of strings is a body that cannot be sent, and the refusal
/// is the same one collecting a body of strings gives.
#[test]
fn a_streamed_body_of_anything_but_bytes_is_refused() {
    let (arrives, ran) = streamed(
        r#"
        Deno.serve(() => new Response(ReadableStream.from(["not bytes"])));
        "#,
    );
    let answer = arrives.recv().expect("an answer");
    let all = chunks(answer);
    ran.join().expect("the call finished");
    assert_eq!(all.len(), 1);
    assert_eq!(
        all[0],
        Err("a response body stream may only enqueue buffers".to_string())
    );
}

/// A body that was never a stream is still sent whole, which is worth
/// asserting beside the rest: the streamed path is for a response that
/// was built out of a stream and for nothing else.
#[test]
fn a_body_that_is_bytes_is_not_streamed() {
    let (arrives, ran) = streamed(
        r#"
        Deno.serve(() => new Response("all of it at once"));
        "#,
    );
    let answer = arrives.recv().expect("an answer");
    ran.join().expect("the call finished");
    assert!(
        matches!(answer.body, zou_functions::Body::Bytes(_)),
        "{:?}",
        answer.body
    );
    assert_eq!(body(&answer), "all of it at once");
}

/// The other end of a websocket.
///
/// The same shape as `wire` above and for the same reason: a websocket
/// client is only true if something speaks the protocol back at it, and
/// what a handshake and a frame codec do is not something a test can
/// assert by reading bytes off a socket by hand. This is the server half
/// of the crate the client half is built on, one thread per connection,
/// answering by path.
mod socket {
    // The refusal an upgrade is turned down with is a whole http
    // response by value, because that is the shape tungstenite's
    // handshake callback is given and returns.
    #![allow(clippy::result_large_err)]

    use std::net::TcpListener;

    use tungstenite::Message;
    use tungstenite::handshake::server::{ErrorResponse, Request, Response};
    use tungstenite::protocol::CloseFrame;
    use tungstenite::protocol::frame::coding::CloseCode;

    pub struct Server {
        pub port: u16,
    }

    impl Server {
        pub fn url(&self, path: &str) -> String {
            format!("ws://127.0.0.1:{}{path}", self.port)
        }
    }

    pub fn start() -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
        let port = listener.local_addr().expect("an address").port();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                std::thread::spawn(move || talk(stream));
            }
        });
        Server { port }
    }

    fn talk(stream: std::net::TcpStream) {
        let mut path = String::new();
        let shook = tungstenite::accept_hdr(stream, |request: &Request, response: Response| {
            path = request.uri().path().to_string();
            answer(&path, request, response)
        });
        let Ok(mut socket) = shook else { return };
        if path == "/greeting" {
            let _ = socket.send(Message::Text("hello".into()));
        }
        if path == "/goodbye" {
            let _ = socket.close(Some(CloseFrame {
                code: CloseCode::from(4000u16),
                reason: "that is enough of that".into(),
            }));
            let _ = socket.flush();
        }
        // Everything else is a mirror, which is what makes what a
        // function sent visible to the test that sent it.
        loop {
            match socket.read() {
                Ok(Message::Text(text)) => {
                    let _ = socket.send(Message::Text(text));
                }
                Ok(Message::Binary(bytes)) => {
                    let _ = socket.send(Message::Binary(bytes));
                }
                Ok(Message::Close(_)) => {
                    // The reply frame is the library's, and it goes out
                    // on the flush rather than on its own.
                    let _ = socket.flush();
                    return;
                }
                Ok(_) => {}
                Err(_) => return,
            }
        }
    }

    fn answer(
        path: &str,
        request: &Request,
        mut response: Response,
    ) -> Result<Response, ErrorResponse> {
        if path == "/refused" {
            let mut refused = ErrorResponse::new(Some("not you".to_string()));
            *refused.status_mut() = tungstenite::http::StatusCode::FORBIDDEN;
            return Err(refused);
        }
        if path == "/named" {
            // The first subprotocol offered, which is how a server picks
            // one and is what `socket.protocol` is afterwards.
            let asked = request
                .headers()
                .get("sec-websocket-protocol")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split(',').next())
                .map(str::trim)
                .unwrap_or_default()
                .to_string();
            if !asked.is_empty() {
                response.headers_mut().insert(
                    "sec-websocket-protocol",
                    asked.parse().expect("a header value"),
                );
            }
        }
        Ok(response)
    }
}

#[test]
fn a_function_can_open_a_socket_and_hear_back_what_it_said() {
    let server = socket::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const seen = [];
            const ws = new WebSocket("{url}");
            await new Promise((done) => {{
                ws.onopen = () => {{
                    seen.push(`open ${{ws.readyState}}`);
                    ws.send("what a function said");
                }};
                ws.onmessage = (event) => {{
                    seen.push(`message ${{event.data}} from ${{event.origin}}`);
                    ws.close(1000, "that is all");
                }};
                ws.onclose = (event) => {{
                    seen.push(`close ${{event.code}} ${{event.reason}} ${{event.wasClean}} ${{ws.readyState}}`);
                    done();
                }};
            }});
            return Response.json({{ seen, url: ws.url, protocol: ws.protocol }});
        }});
        "#,
        url = server.url("/echo")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["seen"],
        serde_json::json!([
            "open 1",
            format!("message what a function said from {}", server.url("/echo")),
            "close 1000 that is all true 3",
        ])
    );
    assert_eq!(said["url"], server.url("/echo"));
    // Nothing was offered and so nothing was agreed.
    assert_eq!(said["protocol"], "");
}

/// The two things a binary message can arrive as, which is a property of
/// the socket and not of the message, and both of them round trip the
/// bytes a function sent.
#[test]
fn bytes_on_a_socket_arrive_as_whatever_the_binary_type_says() {
    let server = socket::start();
    let answer = answered(&format!(
        r#"
        function once(url, kind) {{
            return new Promise((done) => {{
                const ws = new WebSocket(url);
                ws.binaryType = kind;
                ws.onopen = () => ws.send(new Uint8Array([104, 105, 0, 255]));
                ws.onmessage = async (event) => {{
                    const bytes = event.data instanceof Blob
                        ? new Uint8Array(await event.data.arrayBuffer())
                        : new Uint8Array(event.data);
                    ws.close();
                    done({{ kind, shape: event.data.constructor.name, bytes: Array.from(bytes) }});
                }};
            }});
        }}
        Deno.serve(async () => {{
            const blob = await once("{url}", "blob");
            const buffer = await once("{url}", "arraybuffer");
            const said = (() => {{ try {{ new WebSocket("{url}").binaryType = "bytes"; return "it worked"; }} catch (e) {{ return e.message; }} }})();
            return Response.json({{ blob, buffer, said }});
        }});
        "#,
        url = server.url("/echo")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["blob"]["shape"], "Blob");
    assert_eq!(said["blob"]["bytes"], serde_json::json!([104, 105, 0, 255]));
    assert_eq!(said["buffer"]["shape"], "ArrayBuffer");
    assert_eq!(
        said["buffer"]["bytes"],
        serde_json::json!([104, 105, 0, 255])
    );
    assert_eq!(said["said"], "bytes is not a binaryType");
}

#[test]
fn a_socket_the_other_end_closes_is_the_code_and_the_reason_it_gave() {
    let server = socket::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const ws = new WebSocket("{url}");
            const closed = await new Promise((done) => {{
                ws.onclose = (event) => done({{ code: event.code, reason: event.reason, wasClean: event.wasClean, type: event.type, state: ws.readyState }});
            }});
            return Response.json(closed);
        }});
        "#,
        url = server.url("/goodbye")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["code"], 4000);
    assert_eq!(said["reason"], "that is enough of that");
    assert_eq!(said["wasClean"], true);
    assert_eq!(said["type"], "close");
    assert_eq!(said["state"], 3);
}

/// A message the server sent first, heard by a listener rather than by
/// the `on` property, because both of them are how a library written
/// against this reaches for one.
#[test]
fn a_listener_hears_the_same_events_the_properties_do() {
    let server = socket::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const ws = new WebSocket("{url}");
            const seen = [];
            const ignored = () => seen.push("this one was removed");
            ws.addEventListener("message", (event) => seen.push(`listener ${{event.data}}`));
            ws.addEventListener("message", ignored);
            ws.removeEventListener("message", ignored);
            ws.onmessage = (event) => seen.push(`property ${{event.data}}`);
            await new Promise((done) => {{
                ws.addEventListener("message", () => ws.close());
                ws.addEventListener("close", done);
            }});
            return Response.json(seen);
        }});
        "#,
        url = server.url("/greeting")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    // The property first, then the listeners in the order they were
    // added, which is what the event spec says and what a library that
    // registers both will see.
    assert_eq!(
        said,
        serde_json::json!(["property hello", "listener hello"])
    );
}

#[test]
fn the_subprotocol_the_server_picked_is_the_one_the_socket_says_it_speaks() {
    let server = socket::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const ws = new WebSocket("{url}", ["phoenix", "graphql-ws"]);
            await new Promise((done) => {{ ws.onopen = done; }});
            const protocol = ws.protocol;
            ws.close();
            return Response.json({{ protocol, extensions: ws.extensions }});
        }});
        "#,
        url = server.url("/named")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["protocol"], "phoenix");
    assert_eq!(said["extensions"], "");
}

/// A handshake that never happened, twice: nothing listening at all, and
/// a server that answered the upgrade with a refusal. Both are an error
/// event and then a close nobody agreed, in that order.
#[test]
fn a_socket_that_will_not_open_is_an_error_and_then_a_close() {
    let server = socket::start();
    let nowhere = format!("ws://127.0.0.1:{}/nothing", wire::closed());
    let answer = answered(&format!(
        r#"
        function opened(url) {{
            return new Promise((done) => {{
                const seen = [];
                const ws = new WebSocket(url);
                ws.onerror = (event) => seen.push(`error ${{event.message}}`);
                ws.onclose = (event) => {{
                    seen.push(`close ${{event.code}} ${{event.wasClean}} ${{ws.readyState}}`);
                    done(seen);
                }};
            }});
        }}
        Deno.serve(async () => Response.json({{
            nowhere: await opened("{nowhere}"),
            refused: await opened("{refused}"),
        }}));
        "#,
        refused = server.url("/refused")
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    let nothing = said["nowhere"][0].as_str().expect("an error");
    assert!(
        nothing.starts_with("error failed to connect to WebSocket ("),
        "{nothing}"
    );
    assert!(nothing.contains(&nowhere), "{nothing}");
    assert_eq!(said["nowhere"][1], "close 1006 false 3");
    let refused = said["refused"][0].as_str().expect("an error");
    assert!(
        refused.ends_with("the server answered 403 Forbidden"),
        "{refused}"
    );
    assert_eq!(said["refused"][1], "close 1006 false 3");
}

/// What the constructor and the two methods refuse, all of it before
/// anything is opened, so a mistake in a function is a message and not a
/// connection to something that is not a websocket server.
#[test]
fn what_a_socket_will_not_do_it_says_rather_than_tries() {
    let port = wire::closed();
    let answer = answered(&format!(
        r#"
        Deno.serve(() => {{
            const said = (f) => {{ try {{ f(); return "it worked"; }} catch (e) {{ return `${{e.constructor.name}}: ${{e.message}}`; }} }};
            const opening = new WebSocket("ws://127.0.0.1:{port}/nothing");
            return Response.json({{
                scheme: said(() => new WebSocket("ftp://example.com/socket")),
                fragment: said(() => new WebSocket("ws://example.com/socket#part")),
                nonsense: said(() => new WebSocket("nonsense")),
                early: said(() => opening.send("too soon")),
                code: said(() => opening.close(2000)),
                allowed: said(() => opening.close(4001, "mine")),
                rewritten: new WebSocket("http://127.0.0.1:{port}/rewritten").url,
                states: [WebSocket.CONNECTING, WebSocket.OPEN, WebSocket.CLOSING, WebSocket.CLOSED],
                own: [WebSocket.prototype.CLOSED, opening.CLOSED],
            }});
        }});
        "#
    ));
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["scheme"],
        "TypeError: ftp: is not a scheme a websocket is opened on"
    );
    assert_eq!(
        said["fragment"],
        "TypeError: a websocket url may not have a fragment on it"
    );
    assert_eq!(said["nonsense"], "TypeError: Invalid URL: 'nonsense'");
    assert_eq!(said["early"], "TypeError: the socket is still connecting");
    assert_eq!(
        said["code"],
        "TypeError: 2000 is not a code a websocket may be closed with"
    );
    assert_eq!(said["allowed"], "it worked");
    // The scheme the spec rewrites, so one url in one environment
    // variable is enough for a project that has both.
    assert_eq!(
        said["rewritten"],
        format!("ws://127.0.0.1:{port}/rewritten")
    );
    assert_eq!(said["states"], serde_json::json!([0, 1, 2, 3]));
    assert_eq!(said["own"], serde_json::json!([3, 3]));
}

/// A socket a function opened and left open is not a call that never
/// ends: the answer goes when the handler is done and the isolate goes
/// with it.
#[test]
fn a_socket_left_open_does_not_hold_the_answer() {
    let server = socket::start();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const ws = new WebSocket("{url}");
            await new Promise((done) => {{ ws.onopen = done; }});
            ws.send("nobody is waiting for the answer to this");
            return new Response("answered with it open");
        }});
        "#,
        url = server.url("/echo")
    ));
    assert_eq!(body(&answer), "answered with it open");
}

/// TLS, which is a different code path and needs a real host, so it is
/// ignored by default and run by hand: `cargo test -p zou-deno
/// --features isolate -- --ignored`.
#[test]
#[ignore = "reaches the network"]
fn a_function_may_call_out_over_tls() {
    let answer = answered(
        r#"
        Deno.serve(async () => {
            const res = await fetch("https://example.com/");
            const said = await res.text();
            return Response.json({ status: res.status, opened: said.slice(0, 15) });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["status"], 200);
    assert_eq!(said["opened"], "<!doctype html>");
}

// -------------------------------------------------------------------
// Per isolate limits

/// Upstream's numbers are 256 MiB, four hundred seconds and two seconds
/// of cpu, and a test that waited for any of them would be a test
/// nobody runs. What is being tested is that each limit is reached and
/// which one is named, so each test sets the one it is about small and
/// leaves the other two where a passing call cannot reach them.
fn small() -> Limits {
    Limits {
        memory: 64 * 1024 * 1024,
        wall: std::time::Duration::from_secs(30),
        cpu: std::time::Duration::from_secs(20),
        boot: std::time::Duration::from_secs(20),
        background: std::time::Duration::from_secs(30),
    }
}

fn stopped(limits: Limits, source: &str) -> Failed {
    let deployed = deployed(source);
    Isolate::new()
        .with_limits(limits)
        .invoke(
            &deployed.function,
            get("http://localhost:9000/functions/v1/hello"),
        )
        .expect_err("a call that ran past a limit")
}

#[test]
fn a_function_that_wants_more_memory_than_it_is_allowed_is_stopped() {
    let started = std::time::Instant::now();
    let why = stopped(
        small(),
        r#"
        Deno.serve(() => {
          const held = [];
          for (let i = 0; i < 100000; i++) { held.push(new Array(100000).fill(i)); }
          return new Response("never " + held.length);
        });
        "#,
    );
    let Failed::Limit(said) = why else {
        panic!("a limit and not a function that threw: {why:?}");
    };
    assert!(said.contains("more memory than the 64 MiB"), "{said}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(20),
        "it was stopped by the heap limit and not by the clock"
    );
}

/// The same function written with buffers, which is a different limit
/// and the reason there is a counting allocator: this one ran to a
/// hundred gigabytes under the heap limit alone, because the heap is
/// not where a backing store lives.
#[test]
fn a_function_that_wants_more_buffers_than_it_is_allowed_is_stopped() {
    let started = std::time::Instant::now();
    let why = stopped(
        small(),
        r#"
        Deno.serve(() => {
          const held = [];
          for (let i = 0; i < 100000; i++) { held.push(new Uint8Array(1024 * 1024)); }
          return new Response("never " + held.length);
        });
        "#,
    );
    let Failed::Limit(said) = why else {
        panic!("a limit and not a function that threw: {why:?}");
    };
    assert!(said.contains("more memory than the 64 MiB"), "{said}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(20),
        "it was stopped by the allocator and not by the clock"
    );
}

/// And a function that keeps a large buffer for as long as it needs it
/// and then does not is a function that works, which is what the count
/// coming back down is for.
#[test]
fn a_function_that_uses_buffers_and_gives_them_back_answers() {
    let deployed = deployed(
        r#"
        Deno.serve(() => {
          let sum = 0;
          for (let i = 0; i < 200; i++) {
            const buffer = new Uint8Array(1024 * 1024);
            buffer[0] = 1;
            sum += buffer[0];
          }
          return new Response("used " + sum);
        });
        "#,
    );
    let answer = Isolate::new()
        .with_limits(small())
        .invoke(
            &deployed.function,
            get("http://localhost:9000/functions/v1/hello"),
        )
        .expect("two hundred mebibytes one at a time is not two hundred mebibytes at once");
    assert_eq!(body(&answer), "used 200");
}

#[test]
fn a_function_that_stays_under_the_memory_it_is_allowed_answers() {
    let deployed = deployed(
        r#"
        Deno.serve(() => {
          const held = [];
          for (let i = 0; i < 8; i++) { held.push("x".repeat(1024 * 1024)); }
          return new Response("held " + held.length);
        });
        "#,
    );
    let answer = Isolate::new()
        .with_limits(small())
        .invoke(
            &deployed.function,
            get("http://localhost:9000/functions/v1/hello"),
        )
        .expect("an answer");
    assert_eq!(body(&answer), "held 8");
}

/// The reason a watchdog thread exists at all. Nothing on the isolate's
/// own thread can stop this: the loop yields to no timer, no op and no
/// executor, so only `terminate_execution` from outside ends it.
#[test]
fn a_function_that_never_stops_running_is_stopped() {
    let started = std::time::Instant::now();
    let why = stopped(
        Limits {
            cpu: std::time::Duration::from_millis(200),
            ..small()
        },
        r#"Deno.serve(() => { for (;;) {} });"#,
    );
    let Failed::Limit(said) = why else {
        panic!("a limit and not a function that threw: {why:?}");
    };
    assert!(said.contains("cpu time"), "{said}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(20),
        "the clock did not have to be the one to stop it"
    );
}

/// The other shape of a call that overruns, and the reason a timer is
/// kept as well as the watchdog: terminating execution does nothing to
/// a function that is not executing.
#[test]
fn a_function_that_is_asleep_is_stopped_by_the_clock() {
    let started = std::time::Instant::now();
    let why = stopped(
        Limits {
            wall: std::time::Duration::from_millis(300),
            ..small()
        },
        r#"
        Deno.serve(async () => {
          await new Promise((resolve) => setTimeout(resolve, 60000));
          return new Response("never");
        });
        "#,
    );
    let Failed::Limit(said) = why else {
        panic!("a limit and not a function that threw: {why:?}");
    };
    assert!(said.contains("still running after"), "{said}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(20),
        "it was the clock that stopped it"
    );
}

/// What makes the cpu limit worth having rather than a second wall
/// clock: a function that spends its time waiting for something else is
/// not spending cpu, and upstream's two seconds would be useless if it
/// were.
#[test]
fn a_function_that_waits_is_not_charged_for_waiting() {
    let deployed = deployed(
        r#"
        Deno.serve(async () => {
          await new Promise((resolve) => setTimeout(resolve, 1500));
          return new Response("awake");
        });
        "#,
    );
    let answer = Isolate::new()
        .with_limits(Limits {
            cpu: std::time::Duration::from_millis(500),
            ..small()
        })
        .invoke(
            &deployed.function,
            get("http://localhost:9000/functions/v1/hello"),
        )
        .expect("a second and a half of waiting is not half a second of cpu");
    assert_eq!(body(&answer), "awake");
}

/// Getting the modules onto the isolate is spent against its own
/// budget, because it happens once and every call after the first one
/// gets it free. A module that burns more than a call is allowed and
/// then answers is a function that works.
#[test]
fn what_loading_the_modules_costs_is_not_the_first_calls_to_pay() {
    let deployed = deployed(
        r#"
        const until = Date.now() + 300;
        while (Date.now() < until) {}
        Deno.serve(() => new Response("loaded"));
        "#,
    );
    let answer = Isolate::new()
        .with_limits(Limits {
            cpu: std::time::Duration::from_millis(100),
            boot: std::time::Duration::from_secs(20),
            ..small()
        })
        .invoke(
            &deployed.function,
            get("http://localhost:9000/functions/v1/hello"),
        )
        .expect("a module that took longer than a call may take");
    assert_eq!(body(&answer), "loaded");
}

/// It is a budget and not an absence of one, and the sentence names
/// the budget that was actually being spent rather than the call's.
#[test]
fn a_module_that_never_finishes_loading_is_stopped_by_its_own_budget() {
    let why = stopped(
        Limits {
            boot: std::time::Duration::from_millis(300),
            ..small()
        },
        r#"
        for (;;) {}
        Deno.serve(() => new Response("never"));
        "#,
    );
    let Failed::Limit(said) = why else {
        panic!("a limit, and got {why:?}");
    };
    assert!(said.contains("300ms of cpu time"), "{said}");
}

/// A limit reached after the head of the answer has gone out cannot
/// become a status code, which is what upstream does too: the caller
/// keeps its 200 and the body stops where it got to.
#[test]
fn a_limit_reached_after_the_answer_truncates_the_body() {
    let deployed = deployed(
        r#"
        Deno.serve(() => new Response(new ReadableStream({
          start(controller) { controller.enqueue(new TextEncoder().encode("first ")); },
          async pull() {
            // A pull is called while the first chunk is being taken, so
            // one that spun straight away would stop the chunk it was
            // asked to follow from ever leaving.
            await new Promise((resolve) => setTimeout(resolve, 10));
            for (;;) {}
          },
        })));
        "#,
    );
    let (sent, arrives) = std::sync::mpsc::channel();
    let ran = std::thread::spawn(move || {
        let answering = Isolate::new()
            .with_limits(Limits {
                cpu: std::time::Duration::from_millis(500),
                ..small()
            })
            .invoke_answering(
                &deployed.function,
                get("http://localhost:9000/functions/v1/hello"),
                Box::new(move |answer| {
                    sent.send(answer).expect("the test is listening");
                }),
            );
        drop(deployed);
        answering
    });
    let answer = arrives.recv().expect("a head while the body is still made");
    assert_eq!(answer.status, 200);
    assert_eq!(chunks(answer), vec![Ok(b"first ".to_vec())]);
    let why = ran.join().expect("the isolate's thread");
    let Failed::Limit(said) = why.expect_err("the call ran past its cpu") else {
        panic!("a limit");
    };
    assert!(said.contains("cpu time"), "{said}");
}

/// `AbortController` exists because `std/http/server.ts` builds one in
/// a class field before it has done anything else, so a runtime without
/// one cannot even load that file.
///
/// It is the signal and not the wiring: nothing in this runtime takes an
/// `AbortSignal` yet, so aborting one tells whoever is listening and
/// stops nothing.
#[test]
fn an_abort_controller_tells_whoever_is_listening() {
    let answer = answered(
        r#"
        Deno.serve(() => {
            const said: string[] = [];
            const controller = new AbortController();
            controller.signal.addEventListener("abort", () => said.push("once"), { once: true });
            controller.signal.addEventListener("abort", () => said.push("again"));
            const before = controller.signal.aborted;
            controller.abort();
            controller.abort();
            let threw = "";
            try {
                controller.signal.throwIfAborted();
            } catch (e) {
                threw = `${e.name}: ${e.message}`;
            }
            return Response.json({
                before,
                after: controller.signal.aborted,
                said,
                threw,
                reason: controller.signal.reason instanceof DOMException,
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["before"], false);
    assert_eq!(said["after"], true);
    assert_eq!(said["said"], serde_json::json!(["once", "again"]));
    assert_eq!(said["threw"], "AbortError: The signal has been aborted");
    assert_eq!(said["reason"], true);
}

/// The three ways a signal is made without a controller, which is how
/// every library that bounds a call makes one now.
///
/// `any` is the one a caller needs when it has a signal of its own and
/// wants a timeout as well, and it is the reason the other two are not
/// enough on their own.
#[test]
fn a_signal_can_be_made_without_a_controller() {
    let answer = answered(
        r#"
        Deno.serve(async () => {
            const named = (signal) => signal.aborted ? `${signal.reason.name}: ${signal.reason.message}` : null;
            const wait = (ms) => new Promise((done) => setTimeout(done, ms));

            const ran_out = AbortSignal.timeout(20);
            const before = named(ran_out);
            const heard: string[] = [];
            ran_out.addEventListener("abort", () => heard.push("timeout"));

            const giving_up = new AbortController();
            const either = AbortSignal.any([giving_up.signal, AbortSignal.timeout(10_000)]);
            either.addEventListener("abort", () => heard.push("any"));
            giving_up.abort(new Error("the caller"));

            await wait(120);

            let refused = "";
            try {
                AbortSignal.timeout("soon");
            } catch (e) {
                refused = e.name;
            }
            let uninstantiable = "";
            try {
                new AbortSignal();
            } catch (e) {
                uninstantiable = e.name;
            }
            return Response.json({
                before,
                after: named(ran_out),
                heard,
                either: named(either),
                already: named(AbortSignal.any([AbortSignal.abort(new Error("first")), giving_up.signal])),
                refused,
                uninstantiable,
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["before"], serde_json::Value::Null);
    assert_eq!(said["after"], "TimeoutError: Signal timed out.");
    // The caller's abort is heard first because it happened first, and
    // the timeout is heard at all because a signal nobody is waiting on
    // still fires.
    assert_eq!(said["heard"], serde_json::json!(["any", "timeout"]));
    assert_eq!(said["either"], "Error: the caller");
    assert_eq!(said["already"], "Error: first");
    assert_eq!(said["refused"], "TypeError");
    assert_eq!(said["uninstantiable"], "TypeError");
}

/// `EventTarget` is what a library extends when it wants to emit
/// something of its own, and three of the Supabase examples never got
/// past the top of a module without it: `@upstash/redis` and
/// `stripe` both build an emitter while they are being imported.
#[test]
fn an_event_target_is_a_thing_a_library_can_extend() {
    let answer = answered(
        r#"
        class Client extends EventTarget {}
        Deno.serve(() => {
            const said: string[] = [];
            const client = new Client();
            const heard = (event: CustomEvent) => said.push(`heard ${event.detail}`);
            client.addEventListener("retry", heard);
            client.addEventListener("retry", () => said.push("once"), { once: true });
            client.dispatchEvent(new CustomEvent("retry", { detail: "one" }));
            client.dispatchEvent(new CustomEvent("retry", { detail: "two" }));
            client.removeEventListener("retry", heard);
            client.dispatchEvent(new CustomEvent("retry", { detail: "three" }));
            return Response.json({ said });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["said"],
        serde_json::json!(["heard one", "once", "heard two"])
    );
}

/// What `dispatchEvent` answers, and the one listener that stops the
/// ones after it.
#[test]
fn an_event_that_was_prevented_is_a_false_and_the_rest_are_true() {
    let answer = answered(
        r#"
        Deno.serve(() => {
            const target = new EventTarget();
            const said: string[] = [];
            target.addEventListener("go", (event: Event) => {
                said.push("first");
                event.preventDefault();
            });
            target.addEventListener("go", (event: Event) => {
                said.push("second");
                event.stopImmediatePropagation();
            });
            target.addEventListener("go", () => said.push("third"));
            const cancelled = target.dispatchEvent(new Event("go", { cancelable: true }));
            const plain = target.dispatchEvent(new Event("nobody is listening"));
            return Response.json({ said, cancelled, plain });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["said"], serde_json::json!(["first", "second"]));
    assert_eq!(said["cancelled"], false);
    assert_eq!(said["plain"], true);
}

/// `performance.now` is a duration and not a date, which is why it is
/// worth having when `Date.now` is already here: `@sentry/deno` reads
/// it while the sdk is being initialised, so a function importing that
/// does not load at all without one.
#[test]
fn performance_counts_from_when_the_isolate_started() {
    let answer = answered(
        r#"
        Deno.serve(async () => {
            const first = performance.now();
            await new Promise((done) => setTimeout(done, 20));
            const second = performance.now();
            return Response.json({
                first,
                second,
                origin: performance.timeOrigin,
                now: Date.now(),
                fraction: performance.now() % 1 !== 0,
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    let first = said["first"].as_f64().expect("a number");
    let second = said["second"].as_f64().expect("a number");
    assert!((0.0..60_000.0).contains(&first), "{first}");
    assert!(second >= first + 15.0, "{first} then {second}");
    // The origin is a wall clock reading, and the clock the isolate
    // was made by is the same clock the function reads.
    let origin = said["origin"].as_f64().expect("a number");
    let now = said["now"].as_f64().expect("a number");
    assert!(now - origin >= 0.0 && now - origin < 60_000.0, "{origin}");
    // Milliseconds with a fraction, which is what tells this apart
    // from `Date.now()` for anything measuring short work.
    assert_eq!(said["fraction"], true);
}

/// A mark and a measure are recorded and read back. The buffer used to
/// be absent, so a library that timed itself found no `mark` at all,
/// and the whole of what it costs to have is a list in the isolate.
#[test]
fn what_a_function_marks_is_what_it_reads_back() {
    let answer = answered(
        r#"
        Deno.serve(async () => {
            performance.mark("start", { detail: { why: "the query" } });
            await new Promise((done) => setTimeout(done, 20));
            performance.mark("end");
            const span = performance.measure("the query", "start", "end");
            const byName = performance.getEntriesByName("the query");
            const marks = performance.getEntriesByType("mark").map((e) => e.name);
            const started = performance.getEntriesByName("start", "mark")[0];
            performance.clearMarks("start");
            return Response.json({
                type: span.entryType,
                name: span.name,
                duration: span.duration,
                startsAtTheMark: span.startTime === started.startTime,
                detail: started.detail.why,
                measures: byName.length,
                marks,
                left: performance.getEntriesByType("mark").map((e) => e.name),
                all: performance.getEntries().length,
                isEntry: span instanceof PerformanceEntry && span instanceof PerformanceMeasure,
                json: JSON.stringify(performance.getEntriesByName("end")[0].toJSON()).includes("\"entryType\":\"mark\""),
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["type"], "measure");
    assert_eq!(said["name"], "the query");
    let duration = said["duration"].as_f64().expect("a number");
    assert!(duration >= 15.0, "{duration}");
    assert_eq!(said["measures"], 1);
    assert_eq!(said["startsAtTheMark"], true);
    // A mark carries whatever the library put on it, unchanged.
    assert_eq!(said["detail"], "the query");
    assert_eq!(said["marks"], serde_json::json!(["start", "end"]));
    // A cleared mark is gone and the measure it was used for is not.
    assert_eq!(said["left"], serde_json::json!(["end"]));
    assert_eq!(said["all"], 2);
    assert_eq!(said["isEntry"], true);
    assert_eq!(said["json"], true);
}

/// The entry a nobody asked for. A measure between two names that were
/// never marked is a mistake in the library rather than a zero, and the
/// web says so with a `SyntaxError`.
#[test]
fn a_measure_between_marks_that_were_never_made_says_so() {
    let answer = answered(
        r#"
        Deno.serve(() => {
            const said = [];
            try {
                performance.measure("nothing", "never marked");
            } catch (e) {
                said.push(e.name);
            }
            // A duration with both ends given is the other refusal.
            try {
                performance.measure("both", { start: 1, end: 2, duration: 3 });
            } catch (e) {
                said.push(e.constructor.name);
            }
            // Numbers are times, and a measure of them needs no marks.
            const plain = performance.measure("plain", { start: 5, end: 12 });
            // An entry is the runtime's to make.
            try {
                new PerformanceEntry("mine", "mark", 0, 0);
            } catch (e) {
                said.push(e.constructor.name);
            }
            return Response.json({
                said,
                start: plain.startTime,
                duration: plain.duration,
                types: PerformanceObserver.supportedEntryTypes,
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["said"],
        serde_json::json!(["SyntaxError", "TypeError", "TypeError"])
    );
    assert_eq!(said["start"], 5.0);
    assert_eq!(said["duration"], 7.0);
    assert_eq!(said["types"], serde_json::json!(["mark", "measure"]));
}

/// An observer is told about what was recorded after it started
/// watching, in a microtask rather than inside the call that recorded
/// it, and `buffered` is how a library that set itself up late still
/// sees the setup.
#[test]
fn an_observer_hears_about_what_was_recorded() {
    let answer = answered(
        r#"
        Deno.serve(async () => {
            const heard = [];
            let insideTheCall = false;
            const watcher = new PerformanceObserver((list) => {
                heard.push(...list.getEntries().map((e) => `${e.entryType}:${e.name}`));
            });
            watcher.observe({ entryTypes: ["mark", "measure"] });
            performance.mark("one");
            insideTheCall = heard.length > 0;
            performance.measure("two", "one");
            await new Promise((done) => setTimeout(done, 5));

            // A second observer that asks for what it missed.
            const late = [];
            const catching = new PerformanceObserver((list) => {
                late.push(...list.getEntriesByType("mark").map((e) => e.name));
            });
            catching.observe({ type: "mark", buffered: true });
            await new Promise((done) => setTimeout(done, 5));

            watcher.disconnect();
            catching.disconnect();
            performance.mark("after the disconnect");
            await new Promise((done) => setTimeout(done, 5));

            return Response.json({ heard, late, insideTheCall });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["heard"],
        serde_json::json!(["mark:one", "measure:two"])
    );
    assert_eq!(said["late"], serde_json::json!(["one"]));
    // Not in the call that recorded it, which is what keeps a library
    // that marks inside its own callback out of a loop.
    assert_eq!(said["insideTheCall"], false);
}

/// `navigator` is the four properties upstream has and not a fifth,
/// because a library feature detecting on `navigator.gpu` should find
/// nothing rather than find something that is not there.
///
/// The user agent is the one string in this runtime that other people's
/// code branches on, so it is asserted by shape: Deno's own format, the
/// release the surface is written against, and this runtime named in
/// the brackets where upstream names itself.
#[test]
fn a_navigator_says_what_the_function_is_running_on() {
    let answer = answered(
        r#"
        Deno.serve(() => Response.json({
            agent: navigator.userAgent,
            cores: navigator.hardwareConcurrency,
            language: navigator.language,
            languages: navigator.languages,
            gpu: typeof navigator.gpu,
            keys: Object.keys(navigator).sort().join(","),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    let agent = said["agent"].as_str().expect("a string");
    assert!(agent.starts_with("Deno/2.1.4 (variant; zou/"), "{agent}");
    assert!(agent.ends_with(')'), "{agent}");
    // One, whatever the host has, because a function gets one thread
    // and sizing a pool by the machine's cores would be wrong here.
    assert_eq!(said["cores"], 1);
    assert_eq!(said["language"], "en");
    assert_eq!(said["languages"], serde_json::json!(["en"]));
    assert_eq!(said["gpu"], "undefined");
    assert_eq!(
        said["keys"],
        "hardwareConcurrency,language,languages,userAgent"
    );
}

/// The global is an event target, and a library that calls the bare
/// `addEventListener` at the top of a module gets a listener rather
/// than a ReferenceError.
///
/// Every value here was measured through a function on a real
/// `supabase start`: the global is an `EventTarget`, its
/// `addEventListener` is the one on the prototype rather than a copy,
/// and `self` and `window` are both the global.
#[test]
fn the_global_is_an_event_target_the_way_upstream_is() {
    let answer = answered(
        r#"
        let heard = 0;
        addEventListener("zou", (event) => { heard = event.detail; });
        Deno.serve(() => {
            dispatchEvent(new CustomEvent("zou", { detail: 7 }));
            const once = heard;
            removeEventListener("zou", () => {});
            return Response.json({
                target: globalThis instanceof EventTarget,
                same: globalThis.addEventListener === EventTarget.prototype.addEventListener,
                self: globalThis.self === globalThis,
                window: globalThis.window === globalThis,
                heard: once,
                bare: [typeof addEventListener, typeof removeEventListener, typeof dispatchEvent].join(","),
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["target"], true);
    assert_eq!(said["same"], true);
    assert_eq!(said["self"], true);
    assert_eq!(said["window"], true);
    // A listener added at the top of the module, before there was a
    // call, still hears what the handler dispatches.
    assert_eq!(said["heard"], 7);
    assert_eq!(said["bare"], "function,function,function");
}

/// What a function may do, asked the way a library asks it before it
/// reaches for something it can do without.
///
/// Upstream answers `granted` to all eight and a worker there can no
/// more start a process than one here can, so the three that are not
/// here say so instead, which is the difference a library can act on.
#[test]
fn what_a_function_may_do_is_answered_and_not_a_missing_object() {
    let answer = answered(
        r#"
        const names = ["env", "net", "read", "hrtime", "write", "run", "ffi", "sys"];
        Deno.serve(async () => {
            const sync = {};
            for (const name of names) sync[name] = Deno.permissions.querySync({ name }).state;
            const asked = await Deno.permissions.query({ name: "net" });
            let refused = "it did not throw";
            try {
                Deno.permissions.querySync({ name: "telepathy" });
            } catch (e) {
                refused = `${e.constructor.name}: ${e.message}`;
            }
            return Response.json({
                sync,
                asked: asked.state,
                listens: typeof asked.addEventListener,
                partial: asked.partial,
                change: asked.onchange,
                requested: (await Deno.permissions.request({ name: "env" })).state,
                revoked: Deno.permissions.revokeSync({ name: "run" }).state,
                refused,
                promised: (await Deno.permissions.query({ name: "nothing" }).catch((e) => e)).name,
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["sync"]["env"], "granted");
    assert_eq!(said["sync"]["net"], "granted");
    assert_eq!(said["sync"]["read"], "granted");
    assert_eq!(said["sync"]["hrtime"], "granted");
    assert_eq!(said["sync"]["write"], "denied");
    assert_eq!(said["sync"]["run"], "denied");
    assert_eq!(said["sync"]["ffi"], "denied");
    assert_eq!(said["sync"]["sys"], "denied");
    assert_eq!(said["asked"], "granted");
    // A status is an event target, because that is where `onchange`
    // would arrive if anything here could change.
    assert_eq!(said["listens"], "function");
    assert_eq!(said["partial"], false);
    assert_eq!(said["change"], serde_json::Value::Null);
    assert_eq!(said["requested"], "granted");
    // Nothing here is revocable, so revoking says what asking says
    // rather than pretending to take away what it is not enforcing.
    assert_eq!(said["revoked"], "denied");
    assert_eq!(
        said["refused"],
        "TypeError: telepathy is not a permission a function can ask about"
    );
    // The async half rejects where the sync half throws.
    assert_eq!(said["promised"], "TypeError");
}

/// A reason given is the reason reported, which is the other half of
/// what a caller catching an abort branches on.
#[test]
fn an_abort_with_a_reason_carries_it() {
    let answer = answered(
        r#"
        Deno.serve(() => {
            const controller = new AbortController();
            controller.abort("enough");
            return new Response(String(controller.signal.reason));
        });
        "#,
    );
    assert_eq!(body(&answer), "enough");
}

/// A deep copy that carries what a spread and a trip through JSON both
/// lose, which is what a library reaches for when it does not want its
/// caller's object to change under it.
///
/// The shapes are the algorithm's own and the cycle is the reason none
/// of the cheap substitutes is one.
#[test]
fn a_value_can_be_copied_whole() {
    let answer = answered(
        r#"
        Deno.serve(() => {
            const buf = new Uint8Array([1, 2, 3]).buffer;
            const value = {
                n: 1,
                s: "x",
                u: undefined,
                z: null,
                nested: { deep: [1, 2, { d: 3 }] },
                map: new Map([["k", { in: 1 }], [2, "two"]]),
                set: new Set([1, "a"]),
                date: new Date(1700000000123),
                re: /ab+c/gi,
                buf,
                u8: new Uint8Array([4, 5, 6]),
                dv: new DataView(new Uint8Array([9, 9]).buffer),
                big: 12345678901234567890n,
                nan: NaN,
                negzero: -0,
            };
            const copy = structuredClone(value);
            const shared = { s: 1 };
            const cyclic = { one: shared, two: shared };
            cyclic.self = cyclic;
            const round = structuredClone(cyclic);
            return Response.json({
                same: copy === value,
                nested: copy.nested === value.nested,
                deep: copy.nested.deep[2].d,
                has_undefined: "u" in copy,
                map: [copy.map instanceof Map, copy.map.get("k").in, copy.map.get(2), copy.map.size],
                set: [copy.set instanceof Set, copy.set.size, copy.set.has("a")],
                date: [copy.date instanceof Date, copy.date.getTime()],
                re: [copy.re instanceof RegExp, copy.re.source, copy.re.flags],
                buf: [copy.buf instanceof ArrayBuffer, copy.buf.byteLength, new Uint8Array(copy.buf)[0]],
                u8: [copy.u8 instanceof Uint8Array, Array.from(copy.u8).join(",")],
                dv: [copy.dv instanceof DataView, copy.dv.getUint8(0)],
                big: [typeof copy.big, String(copy.big)],
                odd: [Number.isNaN(copy.nan), Object.is(copy.negzero, -0)],
                cycle: round.self === round,
                twice: round.one === round.two,
                fresh: round.one !== shared,
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["same"], false);
    assert_eq!(said["nested"], false);
    assert_eq!(said["deep"], 3);
    // A key whose value is `undefined` is a key, which JSON drops.
    assert_eq!(said["has_undefined"], true);
    assert_eq!(said["map"], serde_json::json!([true, 1, "two", 2]));
    assert_eq!(said["set"], serde_json::json!([true, 2, true]));
    assert_eq!(
        said["date"],
        serde_json::json!([true, 1_700_000_000_123u64])
    );
    assert_eq!(said["re"], serde_json::json!([true, "ab+c", "gi"]));
    assert_eq!(said["buf"], serde_json::json!([true, 3, 1]));
    assert_eq!(said["u8"], serde_json::json!([true, "4,5,6"]));
    assert_eq!(said["dv"], serde_json::json!([true, 9]));
    assert_eq!(
        said["big"],
        serde_json::json!(["bigint", "12345678901234567890"])
    );
    assert_eq!(said["odd"], serde_json::json!([true, true]));
    // The three JSON cannot do at all.
    assert_eq!(said["cycle"], true);
    assert_eq!(said["twice"], true);
    assert_eq!(said["fresh"], true);
}

/// What a copy refuses, and in the sentence a real `supabase start`
/// refuses it in, because a library catching one branches on the name
/// and prints the message.
#[test]
fn what_cannot_be_copied_says_so_by_name() {
    let answer = answered(
        r#"
        Deno.serve(() => {
            const say = (f) => {
                try {
                    f();
                    return "no";
                } catch (e) {
                    return `${e.name}: ${e.message}`;
                }
            };
            return Response.json({
                fn: say(() => structuredClone(() => 1)),
                sym: say(() => structuredClone(Symbol("s"))),
                weak: say(() => structuredClone(new WeakMap())),
                inside: say(() => structuredClone({ ok: 1, bad: () => 1 })),
                is_dom: (() => {
                    try {
                        structuredClone(() => 1);
                        return false;
                    } catch (e) {
                        return e instanceof DOMException;
                    }
                })(),
                none: say(() => structuredClone()),
                dictionary: say(() => structuredClone({ a: 1 }, 5)),
                sequence: say(() => structuredClone({ a: 1 }, { transfer: 5 })),
                str_sequence: say(() => structuredClone({ a: 1 }, { transfer: "ab" })),
                null_sequence: say(() => structuredClone({ a: 1 }, { transfer: null })),
                not_object: say(() => structuredClone({ a: 1 }, { transfer: [null] })),
                second: say(() => structuredClone({ a: 1 }, { transfer: [new ArrayBuffer(2), 5] })),
                stream: say(() => structuredClone({ a: 1 }, { transfer: [new ReadableStream()] })),
                view: say(() => structuredClone({ a: 1 }, { transfer: [new Uint8Array(4)] })),
                getter: say(() => structuredClone({ get g() { throw new RangeError("from the getter"); } })),
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["fn"], "DataCloneError: ()=>1 could not be cloned.");
    assert_eq!(
        said["sym"],
        "DataCloneError: Symbol(s) could not be cloned."
    );
    assert_eq!(
        said["weak"],
        "DataCloneError: #<WeakMap> could not be cloned."
    );
    assert_eq!(said["inside"], "DataCloneError: ()=>1 could not be cloned.");
    assert_eq!(said["is_dom"], true);
    assert_eq!(
        said["none"],
        "TypeError: Failed to execute 'structuredClone': 1 argument required, but only 0 present"
    );
    assert_eq!(
        said["dictionary"],
        "TypeError: Failed to execute 'structuredClone': Argument 2 can not be converted to a dictionary"
    );
    let sequence = "TypeError: Failed to execute 'structuredClone': 'transfer' of \
                    'StructuredSerializeOptions' (Argument 2) can not be converted to sequence.";
    assert_eq!(said["sequence"], sequence);
    // A string is iterable and is refused all the same, which is the
    // one place this is not plain iteration.
    assert_eq!(said["str_sequence"], sequence);
    assert_eq!(said["null_sequence"], sequence);
    assert_eq!(
        said["not_object"],
        "TypeError: Failed to execute 'structuredClone': 'transfer' of \
         'StructuredSerializeOptions' (Argument 2), index 0 is not an object"
    );
    assert_eq!(
        said["second"],
        "TypeError: Failed to execute 'structuredClone': 'transfer' of \
         'StructuredSerializeOptions' (Argument 2), index 1 is not an object"
    );
    // An `ArrayBuffer` is the only transferable thing on either server.
    assert_eq!(said["stream"], "DataCloneError: Value not transferable");
    assert_eq!(said["view"], "DataCloneError: Value not transferable");
    // A getter that throws throws its own error rather than a copy's.
    assert_eq!(said["getter"], "RangeError: from the getter");
}

/// The two things a copy does that look like losses and are upstream's,
/// so a function written against one of them behaves the same on both.
///
/// A platform object arrives as an empty object rather than as itself,
/// and a buffer named for transfer is copied and left where it was.
#[test]
fn a_copy_loses_what_upstream_loses() {
    let answer = answered(
        r#"
        Deno.serve(() => {
            const blob = structuredClone(new Blob(["hello"], { type: "text/plain" }));
            const headers = structuredClone(new Headers({ a: "b" }));
            const url = structuredClone(new URL("http://x/y"));
            const buf = new Uint8Array([1, 2, 3, 4]).buffer;
            const moved = structuredClone({ buf }, { transfer: [buf] });
            class Thing {
                constructor(v) {
                    this.v = v;
                }
                get twice() {
                    return this.v * 2;
                }
            }
            const thing = structuredClone(new Thing(2));
            const plain = structuredClone({ get g() { return 7; } });
            const error = new TypeError("wrong");
            error.extra = "kept?";
            const copied = structuredClone(error);
            const a = [1, , 3];
            a.extra = "yes";
            const sparse = structuredClone(a);
            return Response.json({
                blob: [blob instanceof Blob, JSON.stringify(blob), typeof blob.size],
                headers: [headers instanceof Headers, Object.keys(headers).length],
                url: [url instanceof URL, Object.keys(url).length],
                transfer: [buf.byteLength, moved.buf.byteLength, new Uint8Array(moved.buf)[0]],
                thing: [thing instanceof Thing, thing.constructor.name, thing.v, thing.twice],
                getter: [
                    plain.g,
                    typeof Object.getOwnPropertyDescriptor(plain, "g").get,
                ],
                error: [copied.name, copied.message, copied instanceof TypeError, copied.extra],
                sparse: [sparse.length, 1 in sparse, sparse.extra],
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    // A platform object holds what it holds under symbols and the
    // serializer copies string keys, so the copy is `{}`.
    assert_eq!(said["blob"], serde_json::json!([false, "{}", "undefined"]));
    assert_eq!(said["headers"], serde_json::json!([false, 0]));
    assert_eq!(said["url"], serde_json::json!([false, 0]));
    // The buffer is copied and is still four bytes long afterwards,
    // where a browser would have left it detached.
    assert_eq!(said["transfer"], serde_json::json!([4, 4, 1]));
    // A class instance is a plain object and a getter becomes a value.
    assert_eq!(
        said["thing"],
        serde_json::json!([false, "Object", 2, serde_json::Value::Null])
    );
    assert_eq!(said["getter"], serde_json::json!([7, "undefined"]));
    // An error keeps its name, message and kind and loses what was
    // hung on it.
    assert_eq!(
        said["error"],
        serde_json::json!(["TypeError", "wrong", true, serde_json::Value::Null])
    );
    // A hole stays a hole and an own property of an array survives.
    assert_eq!(said["sparse"], serde_json::json!([3, false, "yes"]));
}

/// A channel and its two ports, which several libraries make on the way
/// to something else and until now was a `ReferenceError` at import
/// time.
///
/// The shape is measured against the reference runtime, down to which
/// names are on the prototype and what a caller who tries to construct
/// a port for themselves is told.
#[test]
fn a_channel_has_two_ports() {
    let answer = answered(
        r#"
        Deno.serve(() => {
            const channel = new MessageChannel();
            let refused = "no";
            try {
                new MessagePort();
            } catch (e) {
                refused = `${e.name}: ${e.message}`;
            }
            return Response.json({
                kinds: [typeof MessageChannel, typeof MessagePort, typeof MessageEvent],
                ports: [
                    channel.port1 instanceof MessagePort,
                    channel.port2 instanceof MessagePort,
                    channel.port1 instanceof EventTarget,
                    channel.port1 === channel.port2,
                ],
                names: [MessageChannel.name, MessagePort.name, channel.port1.constructor.name],
                methods: ["postMessage", "start", "close", "addEventListener"].map(
                    (name) => typeof channel.port1[name],
                ),
                own: [
                    Object.getOwnPropertyNames(channel).length,
                    Object.getOwnPropertyNames(channel.port1).length,
                ],
                keys: Object.getOwnPropertyNames(MessageChannel.prototype).sort(),
                tags: [
                    Object.prototype.toString.call(channel),
                    Object.prototype.toString.call(channel.port1),
                ],
                handler: ["onmessage" in channel.port1, channel.port1.onmessage],
                refused,
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["kinds"],
        serde_json::json!(["function", "function", "function"])
    );
    assert_eq!(said["ports"], serde_json::json!([true, true, true, false]));
    assert_eq!(
        said["names"],
        serde_json::json!(["MessageChannel", "MessagePort", "MessagePort"])
    );
    assert_eq!(
        said["methods"],
        serde_json::json!(["function", "function", "function", "function"])
    );
    // Both ends of both objects are accessors on the prototype, so a
    // caller enumerating one finds nothing of the machinery.
    assert_eq!(said["own"], serde_json::json!([0, 0]));
    assert_eq!(
        said["keys"],
        serde_json::json!(["constructor", "port1", "port2"])
    );
    assert_eq!(
        said["tags"],
        serde_json::json!(["[object MessageChannel]", "[object MessagePort]"])
    );
    assert_eq!(
        said["handler"],
        serde_json::json!([true, serde_json::Value::Null])
    );
    assert_eq!(said["refused"], "TypeError: Illegal constructor");
}

/// What arrives is a copy of what was posted, taken when it was posted,
/// and it arrives as the same `MessageEvent` a socket delivers.
#[test]
fn a_message_arrives_as_a_copy_of_what_was_posted() {
    let answer = answered(
        r#"
        const wait = (ms = 30) => new Promise((r) => setTimeout(r, ms));
        Deno.serve(async () => {
            const channel = new MessageChannel();
            const seen = {};
            let got = null;
            channel.port1.onmessage = (event) => {
                got = event.data;
                seen.type = event.type;
                seen.origin = event.origin;
                seen.lastEventId = event.lastEventId;
                seen.source = event.source;
                seen.ports = Array.isArray(event.ports) ? event.ports.length : String(event.ports);
                seen.kind = event instanceof MessageEvent;
                seen.target = event.target === channel.port1;
                seen.cancelable = event.cancelable;
                seen.bubbles = event.bubbles;
            };
            const sent = { n: 1, deep: { m: new Map([["k", "v"]]) } };
            channel.port2.postMessage(sent);
            sent.n = 2;
            let refused = "no";
            try {
                channel.port2.postMessage(() => {});
            } catch (e) {
                refused = `${e.name}: ${e.message}`;
            }
            await wait();
            return Response.json({
                seen,
                copy: [got === sent, got.n, got.deep.m instanceof Map, got.deep.m.get("k")],
                refused,
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["seen"]["type"], "message");
    assert_eq!(said["seen"]["origin"], "");
    assert_eq!(said["seen"]["lastEventId"], "");
    assert_eq!(said["seen"]["source"], serde_json::Value::Null);
    assert_eq!(said["seen"]["ports"], 0);
    assert_eq!(said["seen"]["kind"], true);
    assert_eq!(said["seen"]["target"], true);
    assert_eq!(said["seen"]["cancelable"], false);
    assert_eq!(said["seen"]["bubbles"], false);
    // The copy is taken at the post, so the sender changing it
    // afterwards changes nothing that arrives.
    assert_eq!(said["copy"], serde_json::json!([false, 1, true, "v"]));
    assert_eq!(
        said["refused"],
        "DataCloneError: ()=>{} could not be cloned."
    );
}

/// When a message arrives, which is the part a library can tell apart
/// from the outside.
///
/// A message posted from a call arrives before a timer that was set
/// before it, and a handler that posts waits a turn, so two ports
/// answering each other do not starve the timers. Both are the
/// reference runtime's order rather than this file's.
#[test]
fn a_message_arrives_ahead_of_a_timer_and_a_handler_that_answers_waits_a_turn() {
    let answer = answered(
        r#"
        const wait = (ms = 50) => new Promise((r) => setTimeout(r, ms));
        Deno.serve(async () => {
            const order = [];
            const first = new MessageChannel();
            first.port1.onmessage = (event) => order.push(`message:${event.data}`);
            queueMicrotask(() => order.push("micro"));
            setTimeout(() => order.push("timer"), 0);
            first.port2.postMessage("one");
            first.port2.postMessage("two");
            await wait();

            const back = [];
            const second = new MessageChannel();
            let left = 3;
            setTimeout(() => back.push("timer"), 0);
            second.port1.onmessage = () => {
                back.push("ping");
                if (left-- > 0) second.port1.postMessage("back");
            };
            second.port2.onmessage = () => {
                back.push("pong");
                second.port2.postMessage("back");
            };
            second.port2.postMessage("start");
            await wait();

            const nested = [];
            const third = new MessageChannel();
            third.port1.onmessage = (event) => {
                nested.push(`in:${event.data}`);
                if (event.data === "one") {
                    setTimeout(() => nested.push("timer inside"), 0);
                    third.port2.postMessage("two");
                }
            };
            third.port2.postMessage("one");
            await wait();

            return Response.json({ order, back, nested });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(
        said["order"],
        serde_json::json!(["micro", "message:one", "message:two", "timer"])
    );
    let back: Vec<String> = serde_json::from_value(said["back"].clone()).expect("a list");
    // Four pings and three pongs, because the handler stops answering,
    // and the timer somewhere in among them rather than after all of
    // them: the pair does not hold the loop.
    assert_eq!(back.iter().filter(|each| *each == "ping").count(), 4);
    assert_eq!(back.iter().filter(|each| *each == "pong").count(), 3);
    assert_eq!(back.first().map(String::as_str), Some("ping"));
    assert_ne!(back.last().map(String::as_str), Some("timer"));
    assert_eq!(
        said["nested"],
        serde_json::json!(["in:one", "in:two", "timer inside"])
    );
}

/// A port holds what it was sent until somebody reads it, and setting a
/// handler is what starts the reading.
#[test]
fn a_port_holds_what_it_was_sent_until_it_is_started() {
    let answer = answered(
        r#"
        const wait = (ms = 30) => new Promise((r) => setTimeout(r, ms));
        Deno.serve(async () => {
            const listening = new MessageChannel();
            const heard = [];
            listening.port1.addEventListener("message", (event) => heard.push(event.data));
            listening.port2.postMessage("held");
            await wait();
            const before = [...heard];
            listening.port1.start();
            await wait();

            const late = new MessageChannel();
            const lately = [];
            late.port2.postMessage("early");
            await wait();
            late.port1.onmessage = (event) => lately.push(event.data);
            await wait();

            const cleared = new MessageChannel();
            const still = [];
            cleared.port1.onmessage = () => still.push("handler");
            cleared.port1.onmessage = null;
            cleared.port1.addEventListener("message", () => still.push("listener"));
            cleared.port2.postMessage("after");
            await wait();

            const never = new MessageChannel();
            const nothing = [];
            never.port1.onmessage = null;
            never.port1.addEventListener("message", (event) => nothing.push(event.data));
            never.port2.postMessage("nobody started this");
            await wait();

            return Response.json({
                before,
                after: heard,
                lately,
                still,
                nothing,
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    // A listener on its own does not start the port.
    assert_eq!(said["before"], serde_json::json!([]));
    assert_eq!(said["after"], serde_json::json!(["held"]));
    // A handler set long after the message was posted still sees it.
    assert_eq!(said["lately"], serde_json::json!(["early"]));
    // Setting the handler started the port and taking it away again
    // does not stop it.
    assert_eq!(said["still"], serde_json::json!(["listener"]));
    // Setting it to null never started anything.
    assert_eq!(said["nothing"], serde_json::json!([]));
}

/// Closing a port throws away what was waiting and takes the other end
/// with it, and posting into a closed one is quiet rather than an
/// error, which is what the reference runtime does.
#[test]
fn closing_a_port_throws_away_what_was_waiting() {
    let answer = answered(
        r#"
        const wait = (ms = 30) => new Promise((r) => setTimeout(r, ms));
        Deno.serve(async () => {
            const receiver = new MessageChannel();
            const heard = [];
            receiver.port1.onmessage = (event) => heard.push(event.data);
            receiver.port2.postMessage("before");
            receiver.port1.close();
            receiver.port2.postMessage("after");

            const sender = new MessageChannel();
            const other = [];
            sender.port1.onmessage = (event) => other.push(event.data);
            sender.port2.close();
            let threw = null;
            try {
                sender.port2.postMessage("after");
            } catch (e) {
                threw = `${e.name}: ${e.message}`;
            }

            await wait();
            return Response.json({ heard, other, threw });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    assert_eq!(said["heard"], serde_json::json!([]));
    assert_eq!(said["other"], serde_json::json!([]));
    assert_eq!(said["threw"], serde_json::Value::Null);
}

/// A port is the one thing here that is really transferred: named in a
/// transfer list it arrives as a fresh port holding the same end, and
/// not named it is refused in v8's own words rather than arriving as an
/// empty object the way a `Blob` does.
#[test]
fn a_port_is_transferred_or_refused_by_name() {
    let answer = answered(
        r#"
        const wait = (ms = 50) => new Promise((r) => setTimeout(r, ms));
        Deno.serve(async () => {
            const say = (f) => {
                try {
                    f();
                    return "no";
                } catch (e) {
                    return `${e.name}: ${e.message}`;
                }
            };

            const cloned = new MessageChannel();
            const copy = structuredClone(cloned.port1, { transfer: [cloned.port1] });
            const heard = [];
            copy.onmessage = (event) => heard.push(event.data);
            cloned.port2.postMessage("to the copy");
            const stranded = say(() => cloned.port1.postMessage("to nobody"));
            await wait();

            const carrier = new MessageChannel();
            const carried = new MessageChannel();
            carried.port2.postMessage("posted before");
            const arrived = [];
            let ports = null;
            carrier.port1.onmessage = (event) => {
                ports = event.ports.length;
                event.ports[0].onmessage = (inner) => arrived.push(inner.data);
            };
            carrier.port2.postMessage("carrying", [carried.port1]);
            await wait();

            const self = new MessageChannel();
            const alone = new MessageChannel();
            return Response.json({
                clone: [copy instanceof MessagePort, copy === cloned.port1, heard, stranded],
                carry: [ports, arrived],
                bare: say(() => structuredClone(alone.port1)),
                inside: say(() => alone.port1.postMessage({ port: alone.port2 })),
                itself: say(() => self.port2.postMessage("itself", [self.port2])),
            });
        });
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(answer.bytes()).expect("json");
    // The copy is a different port holding the end the original had, so
    // the message reaches it and the original reaches nobody without
    // saying so.
    assert_eq!(
        said["clone"],
        serde_json::json!([true, false, ["to the copy"], "no"])
    );
    // A port sent down another channel arrives in `ports`, entangled
    // with what its own other end was entangled with, and still holding
    // what was posted to it before it was sent.
    assert_eq!(said["carry"], serde_json::json!([1, ["posted before"]]));
    // v8's refusal, which is the reference runtime's refusal too.
    assert_eq!(said["bare"], "DataCloneError: Unsupported object type");
    assert_eq!(said["inside"], "DataCloneError: Unsupported object type");
    assert_eq!(said["itself"], "DataCloneError: Can not transfer self");
}

/// A commonjs script, run the way the module that stands in for one
/// runs it: the built ins imported first, then the script, then what it
/// put on its exports.
///
/// Everything a script is given is in here: `require` of a built in, of
/// a file beside it, and of a json file, plus the two names node hands
/// a script for where it is.
#[test]
fn a_script_runs_with_the_five_names_node_gives_one() {
    let deployed = written(&[
        (
            "index.ts",
            r#"
            import "zou:node";
            const module = globalThis.__zouRequire(new URL("./thing.js", import.meta.url).href);
            Deno.serve(() => Response.json(module.exports));
            "#,
        ),
        (
            "thing.js",
            r#"
            const path = require("path");
            exports.joined = path.join("a", "b");
            exports.borrowed = require("./other.js").value;
            exports.named = require("./data.json").name;
            exports.here = __filename.endsWith("/thing.js");
            exports.beside = __dirname === path.dirname(__filename);
            exports.itself = module.exports === exports;
            "#,
        ),
        ("other.js", "module.exports = { value: 7 };\n"),
        ("data.json", r#"{"name": "zou"}"#),
    ]);
    let answer = Isolate::new()
        .invoke(
            &deployed.function,
            get("http://localhost:9000/functions/v1/hello"),
        )
        .expect("an answer");
    assert_eq!(
        body(&answer),
        r#"{"joined":"a/b","borrowed":7,"named":"zou","here":true,"beside":true,"itself":true}"#
    );
}

/// Two scripts that require each other, which is a shape real packages
/// have and which node answers by handing back what has been set so
/// far. A runtime without that answer runs one of them twice or spins.
#[test]
fn two_scripts_that_require_each_other_both_finish() {
    let deployed = written(&[
        (
            "index.ts",
            r#"
            import "zou:node";
            const module = globalThis.__zouRequire(new URL("./one.js", import.meta.url).href);
            Deno.serve(() => Response.json(module.exports));
            "#,
        ),
        (
            "one.js",
            r#"
            exports.name = "one";
            const two = require("./two.js");
            exports.sawFromTwo = two.sawFromOne;
            "#,
        ),
        (
            "two.js",
            r#"
            const one = require("./one.js");
            exports.sawFromOne = one.name;
            "#,
        ),
    ]);
    let answer = Isolate::new()
        .invoke(
            &deployed.function,
            get("http://localhost:9000/functions/v1/hello"),
        )
        .expect("an answer");
    assert_eq!(body(&answer), r#"{"name":"one","sawFromTwo":"one"}"#);
}

/// A require is not a way to read the disk. The two places it reads are
/// the module cache and the function's own directory, which are the two
/// places an import already reads, and anywhere else is a sentence
/// saying so.
#[test]
fn a_require_of_somewhere_else_on_the_disk_is_refused() {
    let answer = answered(
        r#"
        import "zou:node";
        Deno.serve(() => {
          try {
            globalThis.__zouRequire("file:///etc/hosts");
            return new Response("read it");
          } catch (why) {
            return new Response(String(why.message ?? why));
          }
        });
        "#,
    );
    assert!(
        body(&answer).contains("nor in this function's own directory"),
        "{}",
        body(&answer)
    );
}

/// The built ins arrive as one module, so that a script asking for one
/// in the middle of running has an answer already. A name this runtime
/// does not have says which name it was.
#[test]
fn a_built_in_this_runtime_does_not_have_is_named_in_the_refusal() {
    let deployed = written(&[
        (
            "index.ts",
            r#"
            import "zou:node";
            const url = new URL("./thing.js", import.meta.url).href;
            Deno.serve(() => {
              try {
                globalThis.__zouRequire(url);
                return new Response("ran");
              } catch (why) {
                return new Response(String(why.message ?? why));
              }
            });
            "#,
        ),
        ("thing.js", r#"exports.it = require("node:dgram");"#),
    ]);
    let answer = Isolate::new()
        .invoke(
            &deployed.function,
            get("http://localhost:9000/functions/v1/hello"),
        )
        .expect("an answer");
    assert!(body(&answer).contains("dgram"), "{}", body(&answer));
}
