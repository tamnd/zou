//! Wasm handed to the runtime as a response rather than as bytes.
//!
//! `WebAssembly.instantiateStreaming` used to abort the process.
//! v8 asks the embedder for the bytes through a callback, deno_core
//! forwards that to a javascript handler, and a runtime that never set
//! one dies where it stands rather than throwing. Every function on
//! the node after it answered nothing, from three lines of one
//! tenant's code. See #592.
//!
//! So the two claims here are that the call works and that a call that
//! cannot work is an error the function sees. Both are worth having as
//! tests, but the second is the one this issue was about: a test that
//! only checked the happy path would pass on a build that aborts on
//! everything else.

#![cfg(feature = "isolate")]

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;

use zou_deno::Isolate;
use zou_functions::{Call, Function, Runtime};

/// The smallest thing that is a wasm module: the magic number and the
/// version, and no sections at all. It instantiates to an instance
/// with no exports, which is everything this needs from it. A real
/// module would only be more bytes making the same point.
const EMPTY_MODULE: [u8; 8] = [0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];

/// A server that answers `/module.wasm` with a module and anything
/// else with the same bytes under the wrong content type, which is the
/// ordinary way this fails in the world: a url that used to serve wasm
/// now serves an error page or a redirect notice.
fn serving() -> String {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("a port");
    let at = listener.local_addr().expect("an address");
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().expect("the same socket"));
            let mut line = String::new();
            if reader.read_line(&mut line).is_err() {
                continue;
            }
            let path = line.split_whitespace().nth(1).unwrap_or("/").to_string();
            let kind = match path.ends_with("/module.wasm") {
                true => "application/wasm",
                false => "text/html",
            };
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: {kind}\r\nconnection: close\r\ncontent-length: {}\r\n\r\n",
                EMPTY_MODULE.len()
            );
            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(&EMPTY_MODULE);
            let _ = stream.flush();
        }
    });
    format!("http://{at}")
}

fn answered(source: &str) -> Result<zou_functions::Answer, zou_functions::Failed> {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let entrypoint = dir.path().join("index.ts");
    std::fs::write(&entrypoint, source).expect("the function's file");
    let function = Function::new("hello", entrypoint);
    Isolate::new().invoke(
        &function,
        Call {
            method: "GET".to_string(),
            url: "http://localhost:9000/functions/v1/hello".to_string(),
            headers: Vec::new(),
            body: Vec::new(),
            execution_id: "one".to_string(),
        },
    )
}

fn body(answer: &zou_functions::Answer) -> String {
    String::from_utf8(answer.bytes().to_vec()).expect("utf-8")
}

/// The three lines from the issue, and an answer rather than a corpse.
#[test]
fn a_function_can_instantiate_a_module_it_fetched() {
    let at = serving();
    let served = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const {{ module, instance }} = await WebAssembly.instantiateStreaming(
                fetch("{at}/module.wasm"),
            );
            return new Response(
                [
                    module instanceof WebAssembly.Module,
                    instance instanceof WebAssembly.Instance,
                    Object.keys(instance.exports).length,
                ].join(" "),
            );
        }});
        "#
    ))
    .expect("an answer");
    assert_eq!(body(&served), "true true 0");
}

/// And the compile half, which takes the same response and is the same
/// callback underneath.
#[test]
fn a_function_can_compile_a_module_it_fetched() {
    let at = serving();
    let served = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const module = await WebAssembly.compileStreaming(fetch("{at}/module.wasm"));
            return new Response(String(module instanceof WebAssembly.Module));
        }});
        "#
    ))
    .expect("an answer");
    assert_eq!(body(&served), "true");
}

/// A url that answers with something that is not wasm is a TypeError
/// naming what it was, caught inside the function, which is the whole
/// difference from an abort: there is still a function to catch it in.
#[test]
fn a_response_that_is_not_wasm_is_an_error_the_function_can_catch() {
    let at = serving();
    let served = answered(&format!(
        r#"
        Deno.serve(async () => {{
            try {{
                await WebAssembly.instantiateStreaming(fetch("{at}/not-really.wasm"));
                return new Response("no error");
            }} catch (e) {{
                return new Response(`${{e.constructor.name}}: ${{e.message}}`);
            }}
        }});
        "#
    ))
    .expect("an answer");
    assert_eq!(
        body(&served),
        "TypeError: WebAssembly.instantiateStreaming needs a response of type application/wasm, this one is text/html"
    );
}

/// And something that is not a response at all, which is the other way
/// to reach the callback and the one a mistyped call takes.
#[test]
fn something_that_is_not_a_response_is_an_error_naming_the_call() {
    let served = answered(
        r#"
        Deno.serve(async () => {
            try {
                await WebAssembly.instantiateStreaming("a url, probably");
                return new Response("no error");
            } catch (e) {
                return new Response(`${e.constructor.name}: ${e.message}`);
            }
        });
        "#,
    )
    .expect("an answer");
    assert_eq!(
        body(&served),
        "TypeError: WebAssembly.instantiateStreaming takes a Response or a promise of one"
    );
}

/// The isolate that asked is the only thing that ended, which is what
/// the issue is about and what a test of the error alone would miss:
/// an abort takes the process, so a second invocation in the same
/// process is the claim.
#[test]
fn the_isolate_that_asked_is_the_only_thing_that_ends() {
    let served = answered(
        r#"
        Deno.serve(async () => {
            await WebAssembly.instantiateStreaming(Promise.resolve(null));
            return new Response("unreachable");
        });
        "#,
    );
    assert!(served.is_err(), "a refused instantiation answered anyway");

    let after = answered(r#"Deno.serve(() => new Response("still here"));"#).expect("an answer");
    assert_eq!(body(&after), "still here");
}
