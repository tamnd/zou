//! A function's own names, through the file that says what they mean.
//!
//! Everything here runs a real isolate against a real directory, and
//! every address in every map is a file on that disk, because a test
//! that needs a registry to answer is a test that fails when the
//! registry is having a day. What the addresses would have been in a
//! real project, `npm:` and `jsr:`, is the loader's job and is tested
//! where the loader is.

#![cfg(feature = "isolate")]

use std::path::{Path, PathBuf};
use std::time::Duration;

use zou_deno::Isolate;
use zou_functions::{Call, Function, Policy, Runtime};

/// A project directory with a function in it, and whatever else the
/// test wrote beside it.
struct Deployed {
    dir: tempfile::TempDir,
    function: Function,
}

impl Deployed {
    fn at(&self) -> &Path {
        self.dir.path()
    }

    /// Write one of its files again, which is what an editor does. The
    /// sleep is so that a rewrite inside one clock tick is still a file
    /// that visibly moved.
    fn edit(&self, name: &str, source: &str) {
        std::thread::sleep(Duration::from_millis(20));
        write(self.at(), name, source);
    }
}

/// A function at `functions/hello/index.ts`, with the import map the
/// project would have found beside it if it has one.
fn project(files: &[(&str, &str)]) -> Deployed {
    let dir = tempfile::tempdir().expect("a temporary directory");
    for (name, source) in files {
        write(dir.path(), name, source);
    }
    let entrypoint: PathBuf = dir.path().join("functions/hello/index.ts");
    let mut function = Function::new("hello", entrypoint);
    function.import_map = zou_functions::read(dir.path(), &zou_functions::Layout::default())
        .expect("read")
        .into_iter()
        .find(|f| f.name == "hello")
        .and_then(|f| f.import_map);
    Deployed { dir, function }
}

fn write(dir: &Path, name: &str, source: &str) {
    let path = dir.join(name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("the function's directory");
    }
    std::fs::write(&path, source).expect("the function's file");
}

fn call(execution_id: &str) -> Call {
    Call {
        method: "GET".to_string(),
        url: "http://localhost:9000/functions/v1/hello".to_string(),
        headers: Vec::new(),
        body: Vec::new(),
        execution_id: execution_id.to_string(),
    }
}

fn said(deployed: &Deployed, runtime: &Isolate, execution_id: &str) -> String {
    let answer = runtime
        .invoke(&deployed.function, call(execution_id))
        .expect("an answer");
    String::from_utf8(answer.bytes().to_vec()).expect("utf-8")
}

/// A handler that answers with whatever the name it imported resolved
/// to, so the answer is the resolution.
const IMPORTING: &str = r#"
    import { greeting } from "greeter";
    Deno.serve(() => new Response(greeting));
    "#;

#[test]
fn a_deno_json_beside_the_function_says_what_a_bare_name_means() {
    let deployed = project(&[
        ("functions/hello/index.ts", IMPORTING),
        (
            "functions/hello/deno.json",
            r#"{"imports": {"greeter": "./lib/greet.ts"}}"#,
        ),
        (
            "functions/hello/lib/greet.ts",
            r#"export const greeting = "hello from the map""#,
        ),
    ]);
    assert!(
        deployed
            .function
            .import_map
            .as_ref()
            .is_some_and(|at| at.ends_with("deno.json")),
        "the map was found without the config file naming it"
    );
    assert_eq!(
        said(&deployed, &Isolate::new(), "one"),
        "hello from the map"
    );
}

#[test]
fn a_name_the_map_does_not_have_is_refused_rather_than_guessed() {
    let deployed = project(&[
        ("functions/hello/index.ts", IMPORTING),
        (
            "functions/hello/deno.json",
            r#"{"imports": {"something-else": "./lib/greet.ts"}}"#,
        ),
    ]);
    let why = Isolate::new()
        .invoke(&deployed.function, call("one"))
        .expect_err("nothing defines that name");
    assert!(
        why.why().contains("greeter"),
        "the complaint names the specifier nobody defined: {why}"
    );
}

#[test]
fn the_deprecated_import_map_json_still_works() {
    let deployed = project(&[
        ("functions/hello/index.ts", IMPORTING),
        (
            "functions/hello/import_map.json",
            r#"{"imports": {"greeter": "./lib/greet.ts"}}"#,
        ),
        (
            "functions/hello/lib/greet.ts",
            r#"export const greeting = "the old file""#,
        ),
    ]);
    assert_eq!(said(&deployed, &Isolate::new(), "one"), "the old file");
}

#[test]
fn a_deno_json_beats_the_projects_own_map() {
    let deployed = project(&[
        ("functions/hello/index.ts", IMPORTING),
        (
            "functions/import_map.json",
            r#"{"imports": {"greeter": "./hello/lib/shared.ts"}}"#,
        ),
        (
            "functions/hello/deno.json",
            r#"{"imports": {"greeter": "./lib/greet.ts"}}"#,
        ),
        (
            "functions/hello/lib/greet.ts",
            r#"export const greeting = "the function's own""#,
        ),
        (
            "functions/hello/lib/shared.ts",
            r#"export const greeting = "the project's""#,
        ),
    ]);
    assert_eq!(
        said(&deployed, &Isolate::new(), "one"),
        "the function's own"
    );
}

#[test]
fn the_projects_own_map_covers_a_function_with_none() {
    let deployed = project(&[
        ("functions/hello/index.ts", IMPORTING),
        (
            "functions/import_map.json",
            r#"{"imports": {"greeter": "./hello/lib/shared.ts"}}"#,
        ),
        (
            "functions/hello/lib/shared.ts",
            r#"export const greeting = "the project's""#,
        ),
    ]);
    assert_eq!(said(&deployed, &Isolate::new(), "one"), "the project's");
}

/// The map is one of the files the isolate was built out of, so editing
/// it is editing the function, the same as editing something under
/// `_shared`.
#[test]
fn a_map_edited_on_disk_reloads_the_function() {
    let deployed = project(&[
        ("functions/hello/index.ts", IMPORTING),
        (
            "functions/hello/deno.json",
            r#"{"imports": {"greeter": "./lib/one.ts"}}"#,
        ),
        (
            "functions/hello/lib/one.ts",
            r#"export const greeting = "one""#,
        ),
        (
            "functions/hello/lib/two.ts",
            r#"export const greeting = "two""#,
        ),
    ]);
    let runtime = Isolate::new().with_policy(Policy::PerWorker);
    assert_eq!(said(&deployed, &runtime, "one"), "one");
    deployed.edit(
        "functions/hello/deno.json",
        r#"{"imports": {"greeter": "./lib/two.ts"}}"#,
    );
    assert_eq!(
        said(&deployed, &runtime, "two"),
        "two",
        "the kept isolate was built through a map that has changed since"
    );
}

#[test]
fn a_broken_map_is_a_call_that_says_which_file() {
    let deployed = project(&[
        ("functions/hello/index.ts", IMPORTING),
        ("functions/hello/deno.json", r#"{"imports": "#),
    ]);
    let why = Isolate::new()
        .invoke(&deployed.function, call("one"))
        .expect_err("the map is not json");
    assert!(why.why().contains("deno.json"), "{why}");
}

#[test]
fn a_function_with_no_map_at_all_still_runs() {
    let deployed = project(&[(
        "functions/hello/index.ts",
        r#"Deno.serve(() => new Response("no map here"))"#,
    )]);
    assert!(deployed.function.import_map.is_none());
    assert_eq!(said(&deployed, &Isolate::new(), "one"), "no map here");
}
