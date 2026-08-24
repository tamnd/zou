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
//! all. The rest are ignored, because they fetch real packages off the
//! real registry, and an ignored test runs in a pass of its own, so the
//! hand written cache and the fetched one are never in this process at
//! the same time.
//!
//! The fetching ones do run beside each other, though, so they share
//! one cache rather than each setting the variable to a directory of
//! its own: the loader reads that variable on every resolve and on
//! every require, and the last writer of it was deciding what the
//! others were allowed to read. A cache is keyed by package and version
//! and holds any number of packages, which is what makes sharing one
//! the honest fix rather than a way of hiding the race.

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

/// The cache the fetching tests share, made once and pointed at once.
///
/// Every test that fetches calls this before it does anything else, so
/// the one thread that wins the initialisation sets the variables while
/// the others are still waiting on it and nothing is reading them yet.
fn fetching() {
    static CACHE: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| {
        let cache = tempfile::tempdir().expect("a temporary directory");
        // Safety: no other thread in this binary is past this call yet,
        // since every one of them goes through this lock first.
        unsafe {
            std::env::set_var("ZOU_MODULE_CACHE", cache.path());
            std::env::set_var("ZOU_NPM", "tarball");
        }
        cache
    });
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
/// built in asked for in the middle of a script under the bare name
/// node lets a package use for one, and both the named export and the
/// default an importer of a script asks for.
#[test]
fn a_package_is_the_files_it_publishes_and_the_names_they_set() {
    let cache = tempfile::tempdir().expect("a temporary directory");
    // Safety: every other test in this binary is ignored, so they run
    // in a pass of their own and there is no other thread here to read
    // the environment while it is being set.
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
                const path = require("path");
                const shouty = require("shouty");
                exports.greet = (who) => shouty.shout(`hello ${who}`);
                exports.where = path.basename(__filename);
                // A core module node has and this does not, asked for
                // the way a package that ships two implementations of
                // something asks: in a try, to find out.
                try {
                  require("dgram");
                } catch (why) {
                  exports.missing = String(why.message ?? why);
                }
                // node's other name for the global object, which a
                // package built for both node and a browser reads to
                // find out which of the two it is running on.
                exports.global = global === globalThis;
                "#,
            ),
        ],
    );
    package(
        cache.path(),
        "esmy",
        "1.0.0",
        &[
            (
                "package.json",
                r#"{
                  "name": "esmy",
                  "version": "1.0.0",
                  "type": "module",
                  "exports": { ".": "./mod.js" }
                }"#,
            ),
            (
                "mod.js",
                "import path from \"path\";\nexport const base = path.basename(\"/one/two.js\");\n",
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
        import greeter, { greet, where, missing, global as named } from "npm:greeter@^1.0.0";
        import { base } from "npm:esmy@1";
        Deno.serve(() => Response.json({
          said: greet("world"),
          where,
          base,
          missing,
          global: named,
          default: typeof greeter.greet,
        }));
        "#,
    );
    assert_eq!(
        answered(&function),
        r#"{"said":"HELLO WORLD","where":"index.js","base":"two.js","missing":"node:dgram is a node built in this runtime does not have","global":true,"default":"function"}"#
    );
}

/// A real package off the real registry, which is the claim this is all
/// for: `npm:` means what npm published, and what npm published for
/// dotenv is commonjs.
#[test]
#[ignore]
fn a_real_package_off_the_registry_runs_out_of_its_own_tarball() {
    fetching();
    let (_dir, function) = deployed(
        r#"
        import { parse } from "npm:dotenv@^16.0.0";
        Deno.serve(() => Response.json(parse("ONE=1\nTWO=two\n")));
        "#,
    );
    assert_eq!(answered(&function), r#"{"ONE":"1","TWO":"two"}"#);
}

/// A jsr package, which is the other half of the knob: two lookups on
/// jsr.io and then the typescript it publishes, read the way any other
/// module off a url is read. Ignored because it fetches.
#[test]
#[ignore]
fn a_jsr_package_is_the_typescript_it_publishes() {
    fetching();
    let (_dir, function) = deployed(
        r#"
        import { encodeHex } from "jsr:@std/encoding@^1/hex";
        Deno.serve(() => new Response(encodeHex(new Uint8Array([255, 0, 16]))));
        "#,
    );
    assert_eq!(answered(&function), "ff0010");
}

/// The package every function in the examples corpus imports, off its
/// own tarball rather than off a registry's build of it. Ignored for
/// the same reason: it fetches, this time a graph of them.
#[test]
#[ignore]
fn the_client_every_example_imports_runs_out_of_its_tarball() {
    fetching();
    let (_dir, function) = deployed(
        r#"
        import { createClient } from "npm:@supabase/supabase-js@2";
        Deno.serve(() => new Response(typeof createClient));
        "#,
    );
    assert_eq!(answered(&function), "function");
}
