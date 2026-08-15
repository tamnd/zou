//! Which typescript this runtime takes, said in syntax rather than in a
//! version number.
//!
//! `Deno.version.typescript` is a claim, and this file is what backs it:
//! one test per release whose syntax has an effect on what runs, each
//! naming the release it is from. The constant in `isolate.rs` moves
//! when a test here is added, and not before.
//!
//! Two of these are not type stripping and would run wrong without a
//! transform: a decorator is a call that has to happen, and an
//! `accessor` field is a pair of methods and a private field that v8
//! does not make on its own. Both were measured against
//! `supabase/edge-runtime` 1.74.2 before they were written down here,
//! and both work there, which is why they work here.

#![cfg(feature = "isolate")]

use zou_deno::Isolate;
use zou_functions::{Answer, Call, Function, Runtime};

/// A function on disk, entrypoint first, called the way the server
/// calls one.
fn ran(files: &[(&str, &str)]) -> Answer {
    let dir = tempfile::tempdir().expect("a temporary directory");
    for (name, text) in files {
        std::fs::write(dir.path().join(name), text).expect("the function's file");
    }
    let function = Function::new("hello", dir.path().join(files[0].0));
    let call = Call {
        method: "GET".to_string(),
        url: "http://localhost:9000/functions/v1/hello".to_string(),
        headers: Vec::new(),
        body: Vec::new(),
        execution_id: "one".to_string(),
    };
    Isolate::new().invoke(&function, call).expect("an answer")
}

fn answered(source: &str) -> String {
    let answer = ran(&[("index.ts", source)]);
    String::from_utf8(answer.bytes().to_vec()).expect("utf-8")
}

#[test]
fn satisfies_is_taken_and_stripped() {
    // TypeScript 4.9.
    assert_eq!(
        answered(
            r#"
            const config = { port: 9000 } satisfies Record<string, number>;
            Deno.serve(() => new Response(String(config.port)));
            "#,
        ),
        "9000"
    );
}

#[test]
fn an_accessor_field_becomes_the_pair_of_methods_it_stands_for() {
    // TypeScript 4.9, and not type stripping: v8 has no `accessor`, so
    // if this runs at all the transform ran.
    assert_eq!(
        answered(
            r#"
            class Counter {
                accessor n = 1;
            }
            const counter = new Counter();
            counter.n = counter.n + 1;
            const kind = Object.getOwnPropertyDescriptor(Counter.prototype, "n")?.get ? "getter" : "field";
            Deno.serve(() => new Response(`${counter.n} ${kind}`));
            "#,
        ),
        "2 getter"
    );
}

#[test]
fn a_const_type_parameter_is_a_type_parameter() {
    // TypeScript 5.0.
    assert_eq!(
        answered(
            r#"
            function first<const T extends readonly string[]>(items: T): string {
                return items[0];
            }
            Deno.serve(() => new Response(first(["one", "two"])));
            "#,
        ),
        "one"
    );
}

#[test]
fn a_decorator_is_a_call_that_happens() {
    // TypeScript 5.0, which is the TC39 proposal rather than
    // `experimentalDecorators`, so a class decorator is handed the
    // class and a context object and its return value replaces the
    // class if it returns one.
    assert_eq!(
        answered(
            r#"
            const seen: string[] = [];
            function noted(target: unknown, context: { name?: string; kind: string }) {
                seen.push(`${context.kind}:${context.name ?? "?"}`);
                return target as never;
            }
            @noted
            class Thing {
                @noted greet() { return "hello"; }
            }
            Deno.serve(() => new Response(`${new Thing().greet()} ${seen.sort().join(",")}`));
            "#,
        ),
        "hello class:Thing,method:greet"
    );
}

#[test]
fn using_disposes_at_the_end_of_the_block() {
    // TypeScript 5.2.
    assert_eq!(
        answered(
            r#"
            let closed = false;
            {
                using held = { [Symbol.dispose]() { closed = true; } };
                void held;
                if (closed) throw new Error("disposed too early");
            }
            Deno.serve(() => new Response(String(closed)));
            "#,
        ),
        "true"
    );
}

#[test]
fn an_import_attribute_brings_json_in() {
    // TypeScript 5.3, and the only one of these that is about the
    // module graph rather than about one file.
    let answer = ran(&[
        (
            "index.ts",
            r#"
            import settings from "./settings.json" with { type: "json" };
            Deno.serve(() => new Response(String((settings as { rows: number }).rows)));
            "#,
        ),
        ("settings.json", r#"{ "rows": 1000 }"#),
    ]);
    assert_eq!(
        String::from_utf8(answer.bytes().to_vec()).expect("utf-8"),
        "1000"
    );
}

#[test]
fn the_version_is_the_shape_upstream_says_it_in() {
    // `supabase-edge-runtime-1.74.2 (compatible with Deno v2.1.4)` is
    // what upstream answers, measured, so this answers the same shape
    // with its own name in it. The v8 is real and the typescript is the
    // claim the rest of this file backs.
    let said = answered(r#"Deno.serve(() => new Response(JSON.stringify(Deno.version)));"#);
    let version: serde_json::Value = serde_json::from_str(&said).expect("json");
    let deno = version["deno"].as_str().expect("a deno version");
    assert!(deno.starts_with("zou-"), "{deno}");
    assert!(deno.ends_with("(compatible with Deno v2.1.4)"), "{deno}");
    assert_eq!(version["v8"].as_str(), Some(deno_core::v8::VERSION_STRING));
    assert_eq!(version["typescript"].as_str(), Some("5.3.3"));
}
