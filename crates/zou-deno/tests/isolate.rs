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

/// The three that are refused before a request is made, which is why
/// this test needs no network: `npm:` and `jsr:` are fetched and are
/// tested in `registry.rs`.
#[test]
fn a_specifier_this_runtime_does_not_serve_says_so_by_name() {
    for (specifier, said) in [
        ("http://esm.sh/zod", "over https"),
        ("node:fs", "no node built in fs"),
        ("data:text/javascript,1", "the data: specifier"),
    ] {
        let source = format!(r#"import "{specifier}"; Deno.serve(() => new Response("no"));"#);
        let complaint = called(&source, get("http://localhost:9000/functions/v1/hello"))
            .expect_err("a refusal");
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
            url: typeof URL,
            params: typeof URLSearchParams,
            blob: typeof Blob,
            file: typeof File,
            form: typeof FormData,
            crypto: typeof crypto,
            timer: typeof setTimeout,
            stream: (() => { try { new ReadableStream(); return "made one"; } catch (e) { return e.message; } })(),
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
    // What is here, asserted beside the gaps rather than somewhere else,
    // because a list of what is missing is only true if the same list
    // says what is not.
    assert_eq!(said["fetch"], "function");
    assert_eq!(said["url"], "function");
    assert_eq!(said["params"], "function");
    assert_eq!(said["blob"], "function");
    assert_eq!(said["file"], "function");
    assert_eq!(said["form"], "function");
    assert_eq!(said["crypto"], "undefined");
    assert_eq!(said["timer"], "undefined");
    assert_eq!(said["stream"], "ReadableStream is not implemented yet");
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
    assert_eq!(said["name"], "rows.csv");
    assert_eq!(said["type"], "text/csv");
    assert_eq!(said["size"], 8);
    assert_eq!(said["when"], 1_700_000_000_000i64);
    assert_eq!(said["text"], "a,b\n1,2\n");
    assert_eq!(said["isBlob"], true);
    assert_eq!(said["needsAName"], "File requires a name");
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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

// -------------------------------------------------------------------
// fetch

/// A server on a port nobody chose, for the tests that call out.
///
/// Written here rather than pulled in, because a test of `fetch` that
/// needs an HTTP client to be trustworthy is testing two things.
mod wire {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};

    pub struct Server {
        pub port: u16,
    }

    impl Server {
        pub fn url(&self, path: &str) -> String {
            format!("http://127.0.0.1:{}{path}", self.port)
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
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                std::thread::spawn(|| answer(stream));
            }
        });
        Server { port }
    }

    fn answer(mut stream: TcpStream) {
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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
                length: header("content-length"),
            }});
        }});
        "#,
        server.url("/echo")
    ));
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
    assert_eq!(said["method"], "POST");
    assert_eq!(said["body"], "{\"name\":\"world\"}");
    assert_eq!(said["asked"], "the function");
    // Set by the Request constructor because the body is a string, the
    // same as it would be in Deno.
    assert_eq!(said["type"], "text/plain;charset=UTF-8");
    assert_eq!(said["agent"], "zou-edge-runtime");
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
    assert_eq!(said["method"], "PUT");
    assert_eq!(said["body"], "the bytes");
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
    let said: serde_json::Value = serde_json::from_slice(&answer.body).expect("json");
    assert_eq!(said["status"], 200);
    assert_eq!(said["opened"], "<!doctype html>");
}
