//! What a function may read off the disk, which is its `static_files`.
//!
//! Every test here is a real isolate reading a real directory, because
//! the question is what the file system does when javascript asks it
//! something, and a mock of a file system answers a different question.

#![cfg(feature = "isolate")]

use std::path::{Path, PathBuf};

use zou_deno::Isolate;
use zou_functions::{Call, Function, Runtime};

/// A project with one function in it, configured with the patterns the
/// `[functions.hello]` block would have carried.
struct Deployed {
    dir: tempfile::TempDir,
    function: Function,
}

fn project(files: &[(&str, &str)], statics: &[&str]) -> Deployed {
    let dir = tempfile::tempdir().expect("a temporary directory");
    for (name, source) in files {
        let path = dir.path().join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("the function's directory");
        }
        std::fs::write(&path, source).expect("the function's file");
    }
    let entrypoint: PathBuf = dir.path().join("functions/hello/index.ts");
    let mut function = Function::new("hello", entrypoint);
    function.static_files = statics.iter().map(|at| dir.path().join(at)).collect();
    Deployed { dir, function }
}

impl Deployed {
    fn at(&self) -> &Path {
        self.dir.path()
    }
}

fn call() -> Call {
    Call {
        method: "GET".to_string(),
        url: "http://localhost:9000/functions/v1/hello".to_string(),
        headers: Vec::new(),
        body: Vec::new(),
        execution_id: "one".to_string(),
    }
}

/// The answer, as text, for a handler that answers with what it read or
/// with the name of the error it was refused by.
fn said(deployed: &Deployed) -> String {
    let answer = Isolate::new()
        .invoke(&deployed.function, call())
        .expect("an answer");
    String::from_utf8(answer.bytes().to_vec()).expect("utf-8")
}

/// A handler that answers with the file, or with `<name>: <message>` if
/// reading it threw, so one test can look at either.
const READING: &str = r#"
    Deno.serve(async () => {
      try {
        return new Response(await Deno.readTextFile("./page.html"));
      } catch (why) {
        return new Response(`${why.name}: ${why.message}`);
      }
    });
    "#;

#[test]
fn a_file_the_patterns_cover_is_read() {
    let deployed = project(
        &[
            ("functions/hello/index.ts", READING),
            ("functions/hello/page.html", "<h1>hello</h1>"),
        ],
        &["./functions/hello/*.html"],
    );
    assert_eq!(said(&deployed), "<h1>hello</h1>");
}

#[test]
fn a_file_nothing_covers_is_a_permission_denied() {
    let deployed = project(
        &[
            ("functions/hello/index.ts", READING),
            ("functions/hello/page.html", "<h1>hello</h1>"),
        ],
        &["./functions/hello/*.css"],
    );
    let said = said(&deployed);
    assert!(said.starts_with("PermissionDenied: "), "{said}");
    assert!(
        said.contains("static_files") && said.contains("page.html"),
        "the refusal says what would have allowed it: {said}"
    );
}

#[test]
fn a_function_that_configured_nothing_reads_nothing() {
    let deployed = project(
        &[
            ("functions/hello/index.ts", READING),
            ("functions/hello/page.html", "<h1>hello</h1>"),
        ],
        &[],
    );
    assert!(said(&deployed).starts_with("PermissionDenied: "));
}

#[test]
fn a_covered_file_that_is_not_there_is_a_not_found() {
    let deployed = project(
        &[("functions/hello/index.ts", READING)],
        &["./functions/hello/*.html"],
    );
    let said = said(&deployed);
    assert!(said.starts_with("NotFound: "), "{said}");
    assert!(said.contains("page.html"), "{said}");
}

/// The project's `.env` is the file worth naming, because it is the one
/// a function that could walk the disk would go for first.
#[test]
fn a_name_that_climbs_out_of_the_function_is_refused() {
    let deployed = project(
        &[
            (
                "functions/hello/index.ts",
                r#"
                Deno.serve(async () => {
                  try {
                    return new Response(await Deno.readTextFile("../.env"));
                  } catch (why) {
                    return new Response(why.name);
                  }
                });
                "#,
            ),
            ("functions/.env", "STRIPE_KEY=sk_live_nope"),
        ],
        &["./functions/hello/*.html"],
    );
    assert_eq!(said(&deployed), "PermissionDenied");
}

/// Four spellings, one rule. The sync pair is what upstream turns on
/// for a worker with `useReadSyncFileAPI`, and a page served out of a
/// function's own directory is usually read with one of them.
#[test]
fn all_four_of_denos_read_calls_are_here() {
    let deployed = project(
        &[
            (
                "functions/hello/index.ts",
                r#"
                Deno.serve(async () => {
                  const decoder = new TextDecoder();
                  const said = [
                    await Deno.readTextFile("./page.html"),
                    Deno.readTextFileSync("./page.html"),
                    decoder.decode(await Deno.readFile("./page.html")),
                    decoder.decode(Deno.readFileSync("./page.html")),
                  ];
                  return new Response(said.join("|"));
                });
                "#,
            ),
            ("functions/hello/page.html", "ok"),
        ],
        &["./functions/hello/*.html"],
    );
    assert_eq!(said(&deployed), "ok|ok|ok|ok");
}

#[test]
fn a_file_url_is_a_name_too() {
    let deployed = project(
        &[
            (
                "functions/hello/index.ts",
                r#"
                Deno.serve(async () => {
                  const at = new URL("./page.html", import.meta.url);
                  return new Response(await Deno.readTextFile(at));
                });
                "#,
            ),
            ("functions/hello/page.html", "through a url"),
        ],
        &["./functions/hello/*.html"],
    );
    assert_eq!(said(&deployed), "through a url");
}

/// A pattern that crosses directories, which is the other half of the
/// globbing and the one a project with a `dist` directory in it needs.
#[test]
fn a_double_star_reaches_down_and_the_slash_after_it_is_still_a_slash() {
    let deployed = project(
        &[
            (
                "functions/hello/index.ts",
                r#"
                Deno.serve(async () => {
                  const said = [];
                  for (const at of ["./dist/app/main.css", "./dist/one.css"]) {
                    try {
                      said.push(await Deno.readTextFile(at));
                    } catch (why) {
                      said.push(why.name);
                    }
                  }
                  return new Response(said.join("|"));
                });
                "#,
            ),
            ("functions/hello/dist/app/main.css", "deep"),
            ("functions/hello/dist/one.css", "shallow"),
        ],
        &["./functions/hello/dist/**/*.css"],
    );
    // `dist/**/*.css` does not cover `dist/one.css`, because the
    // slash after the `**` is a slash the path has to have. That is
    // upstream's behaviour rather than a shortcut here: the regular
    // expression it builds is `dist/.*/[^/]*\.css`, and a glob
    // library that treats `**/` as "or nothing" would answer
    // differently. A project that wants both writes both patterns.
    assert_eq!(said(&deployed), "deep|PermissionDenied");
}

/// The static files are data and not code, so a page edited on disk is
/// the next call's page even in a kept isolate: nothing about the
/// module graph changed and nothing should have been reloaded.
#[test]
fn an_edited_page_is_read_again_without_the_isolate_going_anywhere() {
    let deployed = project(
        &[
            ("functions/hello/index.ts", READING),
            ("functions/hello/page.html", "before"),
        ],
        &["./functions/hello/*.html"],
    );
    let runtime = Isolate::new().with_policy(zou_functions::Policy::PerWorker);
    let once = runtime
        .invoke(&deployed.function, call())
        .expect("an answer");
    assert_eq!(String::from_utf8_lossy(once.bytes()), "before");
    std::fs::write(deployed.at().join("functions/hello/page.html"), "after")
        .expect("the page again");
    let twice = runtime
        .invoke(&deployed.function, call())
        .expect("an answer");
    assert_eq!(String::from_utf8_lossy(twice.bytes()), "after");
}

/// A page read at the top of the module and answered with from then on,
/// which is the other half of what an isolate owns whether or not a
/// call is in it: the environment and the files. A template loaded once
/// into a constant is ordinary enough that it has to work.
#[test]
fn a_page_is_readable_before_anything_is_served() {
    let deployed = project(
        &[
            (
                "functions/hello/index.ts",
                r#"
                const page = Deno.readTextFileSync("./page.html");
                Deno.serve(() => new Response(page));
                "#,
            ),
            ("functions/hello/page.html", "<h1>read at the top</h1>"),
        ],
        &["./functions/hello/*.html"],
    );
    assert_eq!(said(&deployed), "<h1>read at the top</h1>");
}
