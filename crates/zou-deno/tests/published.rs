//! A package is the files it publishes, not only the javascript ones.
//!
//! `@vercel/og` reads a font out of its own `dist` directory while it is
//! loading. So does every package that ships a wasm blob, a table of
//! country codes, a licence it prints, or a wordlist. The unpack has
//! always written those files, because it writes every regular file in
//! the tarball, but the read was refused: a function's `static_files`
//! are the project's own patterns and a package's directory is not one
//! of them, so serving a package's modules and refusing its files drew
//! the line through the middle of one thing.
//!
//! What is here is both spellings a package uses. An ES module reads
//! beside itself with `import.meta.url`, a commonjs script reads beside
//! itself with `__dirname`, and both of them do it at load rather than
//! in the handler, which is the case that used to fail before the
//! function had answered anything at all.
//!
//! The other half of the claim is the half that has to stay true: this
//! widens nothing about the project. The function here configures no
//! `static_files`, so its own directory is still closed to it, and the
//! test reads a file it put there itself to say so.
//!
//! One test binary with one test in it, because the loader reads
//! `ZOU_MODULE_CACHE` and `ZOU_NPM` on every resolve and there is no
//! honest way to set those from two threads at once.

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

#[test]
fn a_package_reads_its_own_files_and_the_project_is_still_shut() {
    let cache = tempfile::tempdir().expect("a temporary directory");
    // Safety: this is the only test in this binary, so there is no
    // other thread to read the environment while it is being set.
    unsafe {
        std::env::set_var("ZOU_MODULE_CACHE", cache.path());
        std::env::set_var("ZOU_NPM", "tarball");
    }
    // A module that reads a file of its own while it is loading, the
    // way a renderer reads the font it draws with.
    package(
        cache.path(),
        "fonty",
        "1.0.0",
        &[
            (
                "package.json",
                r#"{
                  "name": "fonty",
                  "version": "1.0.0",
                  "type": "module",
                  "exports": { ".": "./dist/mod.js" }
                }"#,
            ),
            (
                "dist/mod.js",
                r#"
                const bytes = Deno.readFileSync(new URL("noto.txt", import.meta.url));
                export const face = new TextDecoder().decode(bytes);
                export const weight = bytes.length;
                "#,
            ),
            ("dist/noto.txt", "not really a font"),
        ],
    );
    // The same thing in commonjs, which is where most of the packages
    // that do this are, and which finds its own directory a different
    // way.
    package(
        cache.path(),
        "tabley",
        "2.0.0",
        &[
            (
                "package.json",
                r#"{"name": "tabley", "version": "2.0.0", "main": "./index.js"}"#,
            ),
            (
                "index.js",
                r#"
                const fs = require("fs");
                const path = require("path");
                const codes = fs.readFileSync(path.join(__dirname, "data", "codes.json"), "utf8");
                exports.codes = JSON.parse(codes);
                "#,
            ),
            ("data/codes.json", r#"{"vn": 84, "sg": 65}"#),
        ],
    );
    let dir = tempfile::tempdir().expect("a temporary directory");
    // A file of the project's, beside the entrypoint, which is the one
    // the function may not have.
    std::fs::write(dir.path().join("secret.txt"), "not for the function").expect("the file");
    let entrypoint: PathBuf = dir.path().join("index.ts");
    std::fs::write(
        &entrypoint,
        r#"
        import { face, weight } from "npm:fonty@1";
        import { codes } from "npm:tabley@^2.0.0";
        let refused = "the project's own file was readable";
        try {
          Deno.readTextFileSync("./secret.txt");
        } catch (why) {
          refused = String(why.message ?? why);
        }
        Deno.serve(() => Response.json({ face, weight, vn: codes.vn, refused }));
        "#,
    )
    .expect("the function's file");
    // No `static_files` at all, so nothing of the project's is open and
    // whatever the packages managed to read they read on their own.
    let function = Function::new("hello", entrypoint);
    let answer = Isolate::new()
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
        .expect("an answer");
    let said = String::from_utf8(answer.bytes().to_vec()).expect("utf-8");
    let said: serde_json::Value = serde_json::from_str(&said).expect("json");
    assert_eq!(said["face"], "not really a font");
    assert_eq!(said["weight"], 17);
    assert_eq!(said["vn"], 84);
    let refused = said["refused"].as_str().expect("a sentence");
    assert!(
        refused.contains("static_files"),
        "the project's own file is still refused: {refused}"
    );
    assert!(
        refused.contains("hello"),
        "and the refusal still names the function: {refused}"
    );
}
