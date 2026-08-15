//! The edge functions a project has on disk, and the thing that runs
//! them.
//!
//! Two questions, and they are separate on purpose. Which functions are
//! served is a directory listing and a config file, and no javascript
//! engine is needed to answer it. What runs them is a build time
//! decision that `zou-deno` owns: V8 is fifty megabytes of static
//! library, so `zou` is built without one unless somebody asked for
//! `zou-deno/isolate`, and a binary that has no engine says so at boot
//! rather than at the first call.
//!
//! The environment is the other half. `Deno.env` inside a function is
//! what this module hands the runtime and never the environment this
//! process was started with, which matters because the process is
//! holding a database password and the function is somebody else's
//! code.

use std::path::Path;
use std::sync::Arc;

use zou_functions::Registry;

/// What every function of this project sees in `Deno.env`, upstream's
/// four project wide variables.
///
/// The fifth, `SB_EXECUTION_ID`, is one per invocation and is added by
/// the runtime out of the call rather than living here.
pub fn env(port: u16, anon: &str, service: &str, db: &str) -> Vec<(String, String)> {
    vec![
        (
            "SUPABASE_URL".to_string(),
            format!("http://127.0.0.1:{port}"),
        ),
        ("SUPABASE_ANON_KEY".to_string(), anon.to_string()),
        ("SUPABASE_SERVICE_ROLE_KEY".to_string(), service.to_string()),
        ("SUPABASE_DB_URL".to_string(), db.to_string()),
    ]
}

/// What this build runs functions with, in the words the log and
/// `zou status` both print.
///
/// Asking a runtime rather than writing the sentence twice, because the
/// two answers must not be able to drift: the whole point of the line
/// is telling an operator which binary they are running.
pub fn engine_describe(policy: zou_functions::Policy) -> String {
    zou_deno::engine(Vec::new(), policy).describe()
}

/// The functions under `dir`, ready to be served, or None when the
/// project has none.
///
/// None and an empty registry are the same thing to a caller, so the
/// simpler of the two goes to the server: a project with no functions
/// directory is not carrying a runtime around.
pub fn registry(
    dir: &Path,
    layout: &zou_functions::Layout,
    env: Vec<(String, String)>,
) -> Result<Option<Arc<Registry>>, String> {
    let found = zou_functions::read(dir, layout)?;
    if found.is_empty() {
        return Ok(None);
    }
    for function in &found {
        // The line `supabase functions serve` prints, which is how a
        // project checks that the name it is about to call is one this
        // server agreed to serve.
        log::info!(
            "function {} at {}",
            function.name,
            function.entrypoint.display()
        );
    }
    let registry = Registry::new(found, zou_deno::engine(env, layout.policy));
    log::info!("functions run on {}", registry.describe());
    if !zou_deno::available() {
        log::warn!(
            "this build has no javascript engine, every function above answers 500 until it is rebuilt with --features zou-deno/isolate"
        );
    }
    Ok(Some(Arc::new(registry)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project(names: &[&str]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        for name in names {
            let at = dir.path().join("functions").join(name);
            std::fs::create_dir_all(&at).expect("mkdir");
            std::fs::write(
                at.join("index.ts"),
                "Deno.serve(() => new Response(\"hi\"))",
            )
            .expect("write");
        }
        dir
    }

    fn call() -> zou_functions::Call {
        zou_functions::Call {
            method: "GET".to_string(),
            url: "http://127.0.0.1:54321/functions/v1/hello".to_string(),
            headers: Vec::new(),
            body: Vec::new(),
            execution_id: "one".to_string(),
        }
    }

    #[test]
    fn a_project_with_no_functions_carries_no_runtime() {
        let dir = tempfile::tempdir().expect("tempdir");
        let none =
            registry(dir.path(), &zou_functions::Layout::default(), Vec::new()).expect("read");
        assert!(none.is_none(), "nothing to serve is nothing to carry");
    }

    #[test]
    fn the_functions_on_disk_are_the_ones_served() {
        let dir = project(&["hello", "open"]);
        let served = registry(dir.path(), &zou_functions::Layout::default(), Vec::new())
            .expect("read")
            .expect("a registry");
        let names: Vec<&str> = served.names().collect();
        assert_eq!(names, ["hello", "open"]);
    }

    #[test]
    fn the_four_variables_are_the_projects_own() {
        let env = env(54321, "an-anon-key", "a-service-key", "postgres://x/y");
        assert_eq!(
            env,
            vec![
                (
                    "SUPABASE_URL".to_string(),
                    "http://127.0.0.1:54321".to_string()
                ),
                ("SUPABASE_ANON_KEY".to_string(), "an-anon-key".to_string()),
                (
                    "SUPABASE_SERVICE_ROLE_KEY".to_string(),
                    "a-service-key".to_string()
                ),
                ("SUPABASE_DB_URL".to_string(), "postgres://x/y".to_string()),
            ]
        );
        assert!(
            !env.iter().any(|(name, _)| name == "SB_EXECUTION_ID"),
            "the per invocation one is the call's and not the project's"
        );
    }

    /// The engine is a build time choice, so one of these two runs and
    /// the other is compiled out, and both are worth asserting because
    /// the wrong one silently is the failure this is guarding against.
    #[test]
    fn what_answers_a_call_is_what_this_build_has() {
        let dir = project(&["hello"]);
        let served = registry(dir.path(), &zou_functions::Layout::default(), Vec::new())
            .expect("read")
            .expect("a registry");
        let hello = served.lookup("hello").expect("served").clone();
        let ran = served.invoke(&hello, call());
        if zou_deno::available() {
            let answer = ran.expect("an isolate ran it");
            assert_eq!(answer.status, 200);
            assert_eq!(answer.bytes(), b"hi");
        } else {
            let complaint = ran.expect_err("no engine to run it");
            assert!(complaint.why().contains("zou-deno/isolate"), "{complaint}");
        }
    }
}
