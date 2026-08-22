//! A build the registry could not make, asked for again as the one it
//! can.
//!
//! A registry that serves packages as modules builds them, and a build
//! can fail: esm.sh answers 500 for `@vercel/og` because its browser
//! build hands esbuild a `.wasm` and esbuild has no loader for one.
//! Which build was asked for is the user agent, so the answer to a 500
//! is to ask again as the runtime the registry does build for.
//!
//! The registry here is a socket in this test rather than esm.sh, and
//! it answers by what asked: that is the whole claim, and a real
//! registry cannot be made to fail on demand. One test binary, because
//! it sets the environment the loader reads.

#![cfg(feature = "isolate")]

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;

use zou_deno::Isolate;
use zou_functions::{Call, Function, Runtime};

/// A registry that answers a package with the name of whoever asked,
/// and answers `unbuildable` with a 500 unless the one asking is a
/// Deno, which is esm.sh's shape for a package esbuild will not take.
fn registry() -> String {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("a port");
    let at = listener.local_addr().expect("an address");
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().expect("the same socket"));
            let mut line = String::new();
            reader.read_line(&mut line).expect("a request line");
            let path = line.split_whitespace().nth(1).unwrap_or("/").to_string();
            let mut agent = String::new();
            loop {
                let mut header = String::new();
                if reader.read_line(&mut header).unwrap_or(0) == 0 || header.trim().is_empty() {
                    break;
                }
                if let Some(said) = header.to_ascii_lowercase().strip_prefix("user-agent:") {
                    agent = said.trim().to_string();
                }
            }
            let deno = agent.starts_with("deno/");
            let answer = match (path.contains("unbuildable"), deno) {
                (true, false) => "HTTP/1.1 500 Internal Server Error\r\nconnection: close\r\ncontent-length: 0\r\n\r\n".to_string(),
                _ => {
                    let build = match deno {
                        true => "denonext",
                        false => "browser",
                    };
                    let body =
                        format!("export const built = \"{build}\";\nexport const asked = \"{path}\";");
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/javascript\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    )
                }
            };
            let _ = stream.write_all(answer.as_bytes());
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

#[test]
fn a_build_the_registry_will_not_make_is_asked_for_again_as_the_one_it_will() {
    let cache = tempfile::tempdir().expect("a temporary directory");
    // Safety: this is the only test in this binary, so there is no
    // other thread to read the environment while it is being set.
    unsafe {
        std::env::set_var("ZOU_MODULE_CACHE", cache.path());
        std::env::set_var("ZOU_MODULE_REGISTRY", registry());
    }

    // The build for a browser is the one that is asked for, because
    // that is the trade the loader made and the corpus measured.
    let served = answered(
        r#"
        import { built } from "npm:ordinary@1";
        Deno.serve(() => new Response(built));
        "#,
    )
    .expect("an answer");
    assert_eq!(body(&served), "browser");

    // Unless there is no such build, in which case the other one is a
    // package that runs rather than a function that will not load.
    let served = answered(
        r#"
        import { built } from "npm:unbuildable@1";
        Deno.serve(() => new Response(built));
        "#,
    )
    .expect("an answer");
    assert_eq!(body(&served), "denonext");

    // And what the registry was asked for carries the build, since a
    // minified package is a package whose classes are called things
    // like `I` and the unminified one is what the names come from.
    let served = answered(
        r#"
        import { asked } from "npm:ordinary@1";
        Deno.serve(() => new Response(asked));
        "#,
    )
    .expect("an answer");
    assert_eq!(body(&served), "/ordinary@1?dev");

    unsafe {
        std::env::remove_var("ZOU_MODULE_CACHE");
        std::env::remove_var("ZOU_MODULE_REGISTRY");
    }
}
