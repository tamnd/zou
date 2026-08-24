//! An `npm:` import that means the tarball rather than a url.
//!
//! With `ZOU_NPM=tarball` a package is what npm publishes: a directory
//! of files, an `exports` map saying which of them a subpath means, a
//! `dependencies` list saying what its own bare names mean, and
//! commonjs wherever the package ships commonjs. That is what Deno runs
//! and it is a different runtime to be than a registry's build of the
//! same package, so it is worth a test that goes through the whole of
//! it: resolution, the walk into a dependency, the script, and the
//! names an `import { thing }` of a script needs.
//!
//! One test binary, because it sets the environment the loader reads.
//! The first test writes the packages by hand and touches no network at
//! all. The second is ignored, because it fetches a real package off the
//! real registry, and an ignored test runs in its own pass, so the two
//! are never in this process at the same time.

#![cfg(feature = "isolate")]

use std::path::{Path, PathBuf};

use zou_deno::Isolate;
use zou_functions::{Call, Function, Runtime};

/// One package on the disk, laid out the way the registry client lays
/// one out: `npm/<name>/<version>`.
fn package(cache: &Path, name: &str, version: &str, files: &[(&str, &str)]) {
    let root = cache.join("npm").join(name).join(version);
    for (named, body) in files {
        let at = root.join(named);
        std::fs::create_dir_all(at.parent().expect("a directory")).expect("the package directory");
        std::fs::write(&at, body).expect("the file");
    }
}

fn deployed(source: &str) -> (tempfile::TempDir, Function) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let entrypoint: PathBuf = dir.path().join("index.ts");
    std::fs::write(&entrypoint, source).expect("the function's file");
    let function = Function::new("hello", entrypoint);
    (dir, function)
}

fn answered(function: &Function) -> String {
    let answer = Isolate::new()
        .invoke(
            function,
            Call {
                method: "GET".to_string(),
                url: "http://localhost:9000/functions/v1/hello".to_string(),
                headers: Vec::new(),
                body: Vec::new(),
                execution_id: "one".to_string(),
            },
        )
        .expect("an answer");
    String::from_utf8(answer.bytes().to_vec()).expect("utf-8")
}

/// The whole of it, off a cache written by hand: a package that ships
/// commonjs, a dependency reached by the range the package declared, a
/// built in asked for in the middle of a script, and both the named
/// export and the default an importer of a script asks for.
#[test]
fn a_package_is_the_files_it_publishes_and_the_names_they_set() {
    let cache = tempfile::tempdir().expect("a temporary directory");
    // Safety: the other test in this binary is ignored, so it runs in
    // its own pass and there is no other thread here to read the
    // environment while it is being set.
    unsafe {
        std::env::set_var("ZOU_MODULE_CACHE", cache.path());
        std::env::set_var("ZOU_NPM", "tarball");
    }
    package(
        cache.path(),
        "greeter",
        "1.4.2",
        &[
            (
                "package.json",
                r#"{
                  "name": "greeter",
                  "version": "1.4.2",
                  "exports": { ".": "./index.js" },
                  "dependencies": { "shouty": "^2.0.0" }
                }"#,
            ),
            (
                "index.js",
                r#"
                const path = require("node:path");
                const shouty = require("shouty");
                exports.greet = (who) => shouty.shout(`hello ${who}`);
                exports.where = path.basename(__filename);
                "#,
            ),
        ],
    );
    package(
        cache.path(),
        "shouty",
        "2.1.0",
        &[
            (
                "package.json",
                r#"{"name": "shouty", "version": "2.1.0", "main": "./shout.js"}"#,
            ),
            (
                "shout.js",
                "module.exports.shout = (said) => said.toUpperCase();\n",
            ),
        ],
    );
    let (_dir, function) = deployed(
        r#"
        import greeter, { greet, where } from "npm:greeter@^1.0.0";
        Deno.serve(() => Response.json({
          said: greet("world"),
          where,
          default: typeof greeter.greet,
        }));
        "#,
    );
    assert_eq!(
        answered(&function),
        r#"{"said":"HELLO WORLD","where":"index.js","default":"function"}"#
    );
}

/// A real package off the real registry, which is the claim this is all
/// for: `npm:` means what npm published, and what npm published for
/// dotenv is commonjs.
#[test]
#[ignore]
fn a_real_package_off_the_registry_runs_out_of_its_own_tarball() {
    let cache = tempfile::tempdir().expect("a temporary directory");
    // Safety: an ignored test runs in a pass of its own, so nothing
    // else in this binary is running while this is set.
    unsafe {
        std::env::set_var("ZOU_MODULE_CACHE", cache.path());
        std::env::set_var("ZOU_NPM", "tarball");
    }
    let (_dir, function) = deployed(
        r#"
        import { parse } from "npm:dotenv@^16.0.0";
        Deno.serve(() => Response.json(parse("ONE=1\nTWO=two\n")));
        "#,
    );
    assert_eq!(answered(&function), r#"{"ONE":"1","TWO":"two"}"#);
}

/// The package every function in the examples corpus imports, off its
/// own tarball rather than off a registry's build of it. Ignored for
/// the same reason: it fetches, this time a graph of them.
#[test]
#[ignore]
fn the_client_every_example_imports_runs_out_of_its_tarball() {
    let cache = tempfile::tempdir().expect("a temporary directory");
    unsafe {
        std::env::set_var("ZOU_MODULE_CACHE", cache.path());
        std::env::set_var("ZOU_NPM", "tarball");
    }
    let (_dir, function) = deployed(
        r#"
        import { createClient } from "npm:@supabase/supabase-js@2";
        Deno.serve(() => new Response(typeof createClient));
        "#,
    );
    assert_eq!(answered(&function), "function");
}
