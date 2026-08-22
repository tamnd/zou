//! A function importing a module the cache already has, with nothing
//! reachable.
//!
//! This is what a deployment wants: the modules fetched once, somewhere
//! that had a network, and a server that starts without one. It is one
//! test rather than several because it sets the environment the loader
//! reads, and a test binary with one test in it is a test binary with
//! one thread in it.

#![cfg(feature = "isolate")]

use std::path::{Path, PathBuf};

use zou_deno::Isolate;
use zou_functions::{Call, Function, Runtime};

/// The same name the loader will look under: the sha256 of the url, in
/// a directory named for the host. Written out by hand here on purpose,
/// because a test that asks the code under test where to put the file
/// is a test that agrees with itself.
fn at(cache: &Path, url: &str) -> PathBuf {
    let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
    sha2::Digest::update(&mut hasher, url.as_bytes());
    let digest = sha2::Digest::finalize(hasher);
    let name: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    cache.join("esm.sh").join(name)
}

fn keep(cache: &Path, url: &str, content_type: &str, body: &str) {
    let path = at(cache, url);
    std::fs::create_dir_all(path.parent().expect("a directory")).expect("the cache directory");
    std::fs::write(&path, body).expect("the module");
    std::fs::write(
        path.with_extension("about"),
        format!("{url}\n{content_type}\n"),
    )
    .expect("what the module is");
}

fn invoke(function: &Function) -> Result<zou_functions::Answer, zou_functions::Failed> {
    Isolate::new().invoke(
        function,
        Call {
            method: "GET".to_string(),
            url: "http://localhost:9000/functions/v1/hello".to_string(),
            headers: Vec::new(),
            body: Vec::new(),
            execution_id: "one".to_string(),
        },
    )
}

#[test]
fn a_warm_cache_is_a_cold_start_that_touches_nothing() {
    let cache = tempfile::tempdir().expect("a temporary directory");
    // Safety: this is the only test in this binary, so there is no
    // other thread to read the environment while it is being set.
    unsafe {
        std::env::set_var("ZOU_MODULE_CACHE", cache.path());
        std::env::set_var("ZOU_MODULE_CACHE_ONLY", "1");
        // Nothing asked of the registry, so the url under which this
        // writes the cache by hand is the url the loader looks under.
        // Which build a package is served is a different question and
        // has its own tests.
        std::env::set_var("ZOU_MODULE_BUILD", "");
    }
    // A package as the registry would have served it, imports and all,
    // so what is exercised is the graph and not one file.
    keep(
        cache.path(),
        "https://esm.sh/greet@1",
        "application/javascript; charset=utf-8",
        r#"import { name } from "/greet@1/who.mjs";
           export const greet = () => `hello ${name}`;"#,
    );
    keep(
        cache.path(),
        "https://esm.sh/greet@1/who.mjs",
        "application/javascript",
        "export const name = \"zou\";",
    );

    let dir = tempfile::tempdir().expect("a temporary directory");
    std::fs::write(
        dir.path().join("index.ts"),
        r#"
        import { greet } from "npm:greet@1";
        Deno.serve(() => new Response(greet()));
        "#,
    )
    .expect("the function's file");
    let function = Function::new("hello", dir.path().join("index.ts"));
    let answer = invoke(&function).expect("an answer");
    assert_eq!(
        String::from_utf8(answer.bytes().to_vec()).expect("utf-8"),
        "hello zou"
    );

    // And the other half of the same claim: what is not in the cache is
    // not fetched, it is refused, by name.
    std::fs::write(dir.path().join("index.ts"), r#"import "npm:unheard-of@1";"#)
        .expect("the function's file");
    let refused = invoke(&function).expect_err("a module nothing has");
    assert!(refused.why().contains("unheard-of@1"), "{refused}");
    assert!(
        refused.why().contains("not in the module cache"),
        "{refused}"
    );

    unsafe {
        std::env::remove_var("ZOU_MODULE_CACHE");
        std::env::remove_var("ZOU_MODULE_CACHE_ONLY");
        std::env::remove_var("ZOU_MODULE_BUILD");
    }
}
