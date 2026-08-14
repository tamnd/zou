//! A function importing a package, off the registry, over the network.
//!
//! Every test here is `#[ignore]`, and CI runs them with `--ignored` in
//! the edge functions job. Reaching esm.sh should not be the price of
//! `cargo test` on a laptop on a train, and a package resolving is a
//! claim about somebody else's server that a mock cannot make: the
//! point of these is exactly that the real registry answers the way the
//! loader expects, so there is nothing here to stub.
//!
//! What is tested offline lives in `cached.rs`, which never reaches the
//! network, and in `isolate.rs` for the specifiers that are refused
//! before a request is made.

#![cfg(feature = "isolate")]

use zou_deno::Isolate;
use zou_functions::{Answer, Call, Function, Runtime};

fn answered(source: &str) -> Answer {
    let dir = tempfile::tempdir().expect("a temporary directory");
    std::fs::write(dir.path().join("index.ts"), source).expect("the function's file");
    let function = Function::new("hello", dir.path().join("index.ts"));
    let call = Call {
        method: "GET".to_string(),
        url: "http://localhost:9000/functions/v1/hello".to_string(),
        headers: Vec::new(),
        body: Vec::new(),
        execution_id: "one".to_string(),
    };
    Isolate::new().invoke(&function, call).expect("an answer")
}

fn body(answer: &Answer) -> String {
    String::from_utf8(answer.body.clone()).expect("utf-8")
}

#[test]
#[ignore = "reaches the registry"]
fn a_jsr_package_can_be_imported_and_used() {
    let answer = answered(
        r#"
        import { encodeHex } from "jsr:@std/encoding@1/hex";
        Deno.serve(() => new Response(encodeHex(new TextEncoder().encode("zou"))));
        "#,
    );
    assert_eq!(body(&answer), "7a6f75");
}

#[test]
#[ignore = "reaches the registry"]
fn an_npm_package_can_be_imported_and_used() {
    let answer = answered(
        r#"
        import { z } from "npm:zod@3.23.8";
        const schema = z.object({ name: z.string(), count: z.number() });
        Deno.serve(() => Response.json(schema.parse({ name: "zou", count: 1 })));
        "#,
    );
    assert_eq!(body(&answer), r#"{"name":"zou","count":1}"#);
}

/// A package whose whole graph is one file, which is the shape most of
/// npm is, as against zod's dozen.
#[test]
#[ignore = "reaches the registry"]
fn a_default_export_off_npm_is_a_default_export() {
    let answer = answered(
        r#"
        import ms from "npm:ms@2.1.3";
        Deno.serve(() => new Response(ms(90000)));
        "#,
    );
    assert_eq!(body(&answer), "2m");
}

/// The typescript in a package is the loader's problem too: what jsr
/// serves is source, so this is the transpiler running on somebody
/// else's code rather than on the function's own.
#[test]
#[ignore = "reaches the registry"]
fn a_package_and_a_function_can_import_the_same_thing_once() {
    let answer = answered(
        r#"
        import { encodeBase64 } from "jsr:@std/encoding@1/base64";
        import { decodeBase64 } from "jsr:@std/encoding@1/base64";
        Deno.serve(() => {
            const encoded = encodeBase64(new TextEncoder().encode("zou"));
            const back = new TextDecoder().decode(decodeBase64(encoded));
            return Response.json({ encoded, back });
        });
        "#,
    );
    assert_eq!(body(&answer), r#"{"encoded":"em91","back":"zou"}"#);
}

#[test]
#[ignore = "reaches the registry"]
fn a_package_that_is_not_there_says_so_with_its_name() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    std::fs::write(
        dir.path().join("index.ts"),
        r#"import "npm:@zou/nothing-is-published-here@9.9.9";"#,
    )
    .expect("the function's file");
    let function = Function::new("hello", dir.path().join("index.ts"));
    let refused = Isolate::new()
        .invoke(
            &function,
            Call {
                method: "GET".to_string(),
                url: "http://localhost:9000/functions/v1/hello".to_string(),
                headers: Vec::new(),
                body: Vec::new(),
                execution_id: "one".to_string(),
            },
        )
        .expect_err("a package that does not exist");
    assert!(
        refused.contains("@zou/nothing-is-published-here@9.9.9"),
        "{refused}"
    );
}
