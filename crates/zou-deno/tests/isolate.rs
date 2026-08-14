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

use zou_deno::Isolate;
use zou_functions::{Answer, Call, Function, Runtime};

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

fn called(source: &str, call: Call) -> Result<Answer, String> {
    let deployed = deployed(source);
    Isolate::new().invoke(&deployed.function, call)
}

fn answered(source: &str) -> Answer {
    called(source, get("http://localhost:9000/functions/v1/hello")).expect("an answer")
}

fn body(answer: &Answer) -> String {
    String::from_utf8(answer.body.clone()).expect("utf-8")
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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
    assert_eq!(answer.body, vec![0, 1, 2, 253, 254, 255, 42]);
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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

#[test]
fn the_environment_cannot_be_written_to() {
    let answer = answered(
        r#"
        Deno.serve(() => {
            try {
                Deno.env.set("SUPABASE_URL", "mine now");
                return new Response("it let me");
            } catch (e) {
                return new Response(e.message);
            }
        });
        "#,
    );
    assert_eq!(body(&answer), "the environment of a function is read only");
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

#[test]
fn a_specifier_this_runtime_does_not_serve_yet_says_so_by_name() {
    for (specifier, said) in [
        ("npm:zod", "the npm: specifier"),
        ("jsr:@std/assert", "the jsr: specifier"),
        ("https://esm.sh/zod", "the https: specifier"),
        ("node:fs", "the node: specifier"),
    ] {
        let source = format!(r#"import "{specifier}"; Deno.serve(() => new Response("no"));"#);
        let complaint = called(&source, get("http://localhost:9000/functions/v1/hello"))
            .expect_err("a refusal");
        assert!(
            complaint.contains(said) && complaint.contains("not supported yet"),
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
    .expect_err("a complaint");
    assert!(complaint.contains("did not call Deno.serve"), "{complaint}");
}

#[test]
fn a_handler_that_throws_is_the_operators_message_and_not_the_callers() {
    let complaint = called(
        r#"Deno.serve(() => { throw new Error("the database was not there"); });"#,
        get("http://localhost:9000/functions/v1/hello"),
    )
    .expect_err("a complaint");
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
    .expect_err("a complaint");
    assert!(complaint.contains("must return a Response"), "{complaint}");
}

#[test]
fn a_module_that_will_not_parse_names_the_file_it_would_not_parse() {
    let complaint = called(
        "Deno.serve(() => { ",
        get("http://localhost:9000/functions/v1/hello"),
    )
    .expect_err("a complaint");
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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
            crypto: typeof crypto,
            url: typeof URL,
            timer: typeof setTimeout,
            stream: (() => { try { new ReadableStream(); return "made one"; } catch (e) { return e.message; } })(),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
    assert_eq!(said["fetch"], "undefined");
    assert_eq!(said["crypto"], "undefined");
    assert_eq!(said["url"], "undefined");
    assert_eq!(said["timer"], "undefined");
    assert_eq!(said["stream"], "ReadableStream is not implemented yet");
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
            handler: typeof globalThis.handler,
            names: Object.keys(globalThis).filter((n) => n.toLowerCase().includes("run")),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
    assert_eq!(said["run"], "undefined");
    assert_eq!(said["handler"], "undefined");
    assert_eq!(said["names"], serde_json::json!([]));
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
    .expect_err("a complaint");
    assert!(complaint.contains("called twice"), "{complaint}");
}

#[test]
fn the_runtime_says_what_it_is() {
    assert_eq!(Isolate::new().describe(), "a v8 isolate per call");
    assert!(zou_deno::available());
}
