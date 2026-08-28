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
//!
//! Every test here pins `ZOU_NPM=registry`, which stopped being the
//! default on 2026-08-28. The file is about what a registry serves and
//! what this loader makes of it, and the tarball the default resolves to
//! now is a different claim about a different server, tested in
//! `tarball.rs`.

#![cfg(feature = "isolate")]

use zou_deno::Isolate;
use zou_functions::{Answer, Call, Function, Runtime};

/// The registry path, set once for this binary.
///
/// Every test in here goes through this before it builds an isolate, so
/// the thread that wins the lock writes the variable while the others
/// are still waiting on it and nobody is reading it yet.
fn registry_path() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // Safety: no other thread in this binary is past this call yet,
        // since every one of them goes through this lock first.
        unsafe { std::env::set_var("ZOU_NPM", "registry") };
    });
}

fn answered(source: &str) -> Answer {
    registry_path();
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
    String::from_utf8(answer.bytes().to_vec()).expect("utf-8")
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

/// The one package this whole runtime is for.
///
/// `createClient` builds a realtime client on the way through, and that
/// constructor reaches for `WebSocket`, so until there was one this line
/// was a `ReferenceError` and every function in every supabase project
/// stopped on it. Nothing here connects anywhere: what is asserted is
/// that the client is built and that the pieces a function reaches for
/// are on it.
#[test]
#[ignore = "reaches the registry"]
fn the_supabase_client_is_built_and_has_its_pieces() {
    let answer = answered(
        r#"
        import { createClient } from "npm:@supabase/supabase-js@2";
        const client = createClient("http://127.0.0.1:9000", "an anon key");
        Deno.serve(() => Response.json({
            from: typeof client.from,
            rpc: typeof client.rpc,
            auth: typeof client.auth.getUser,
            storage: typeof client.storage.from,
            functions: typeof client.functions.invoke,
            channel: typeof client.channel,
            socket: client.realtime.endpointURL().split("?")[0],
        }));
        "#,
    );
    let said: serde_json::Value = serde_json::from_str(&body(&answer)).expect("json");
    assert_eq!(said["from"], "function");
    assert_eq!(said["rpc"], "function");
    assert_eq!(said["auth"], "function");
    assert_eq!(said["storage"], "function");
    assert_eq!(said["functions"], "function");
    assert_eq!(said["channel"], "function");
    assert_eq!(said["socket"], "ws://127.0.0.1:9000/realtime/v1/websocket");
}

#[test]
#[ignore = "reaches the registry"]
fn a_package_that_is_not_there_says_so_with_its_name() {
    registry_path();
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
        refused
            .why()
            .contains("@zou/nothing-is-published-here@9.9.9"),
        "{refused}"
    );
}

/// The entry point most of the examples in the wild still use, from the
/// place they still import it from.
///
/// This is here rather than in `isolate.rs` for the same reason as the
/// rest of this file: what it proves is that the real `std/http/server.ts`
/// runs against this runtime's `Deno.listen` and `Deno.serveHttp`, and a
/// hand written copy of that file proves only that the copy does.
#[test]
#[ignore = "reaches the registry"]
fn the_std_serve_the_older_examples_import_is_served() {
    let answer = answered(
        r#"
        import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
        serve((req: Request) => new Response(`std ${new URL(req.url).pathname}`, { status: 207 }));
        "#,
    );
    assert_eq!(answer.status, 207);
    assert_eq!(body(&answer), "std /functions/v1/hello");
}

/// The same by way of jsr, which is where std moved to.
#[test]
#[ignore = "reaches the registry"]
fn the_jsr_spelling_of_that_import_is_served_too() {
    let answer = answered(
        r#"
        import { serve } from "jsr:@std/http@0.224.5/server";
        serve(() => new Response("jsr std", { status: 208 }));
        "#,
    );
    assert_eq!(answer.status, 208);
    assert_eq!(body(&answer), "jsr std");
}

/// The line two of the Supabase examples open with, which is a project
/// telling its editor what `Deno.serve` is.
///
/// A declaration file has no runtime code in it, so there is nothing to
/// run and nothing to fetch, and the registry has no such file to serve
/// either: asking esm.sh for it is a 404 and the function it was the
/// first line of never loaded.
#[test]
#[ignore = "reaches the registry"]
fn a_declaration_file_is_imported_and_nothing_happens() {
    let answer = answered(
        r#"
        import 'jsr:@supabase/functions-js/edge-runtime.d.ts';
        import { encodeHex } from "jsr:@std/encoding@1/hex";
        Deno.serve(() => new Response(encodeHex(new TextEncoder().encode("types"))));
        "#,
    );
    assert_eq!(body(&answer), "7479706573");
}

/// A subpath of a package, spelled the way a registry's own build of
/// that package spells it: `npm:/name@version/subpath`, with the slash
/// the scheme allows.
#[test]
#[ignore = "reaches the registry"]
fn a_specifier_with_the_slash_the_scheme_allows_resolves() {
    let answer = answered(
        r#"
        import { encodeHex } from "jsr:/@std/encoding@1/hex";
        Deno.serve(() => new Response(encodeHex(new TextEncoder().encode("slash"))));
        "#,
    );
    assert_eq!(body(&answer), "736c617368");
}

/// A package's own file, off the registry, beside where the module
/// landed rather than beside the range that was asked for.
///
/// `image-manipulation` in the Supabase examples is the function that
/// asks: it reads the wasm blob that sits next to the javascript that
/// instantiates it. That is fourteen megabytes off esm.sh, which is
/// why the assertion is on the header and the size rather than on the
/// bytes.
#[test]
#[ignore = "reaches the registry"]
fn a_package_file_is_read_from_beside_where_the_module_landed() {
    let answer = answered(
        r#"
        import { ImageMagick } from "npm:@imagemagick/magick-wasm@^0";
        const wasm = await Deno.readFile(
          new URL("magick.wasm", import.meta.resolve("npm:@imagemagick/magick-wasm@^0")),
        );
        const magic = Array.from(wasm.slice(0, 4)).join(",");
        Deno.serve(() => new Response(`${magic} ${wasm.length > 10_000_000} ${typeof ImageMagick}`));
        "#,
    );
    assert_eq!(body(&answer), "0,97,115,109 true function");
}
