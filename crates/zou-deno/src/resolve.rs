//! Which file in a package a specifier means.
//!
//! `import { createClient } from "npm:@supabase/supabase-js"` names a
//! package and no file in it, and what file that is is a question only
//! the package can answer. Modern packages answer it with `exports`, a
//! map from subpath to file that can branch on who is asking, and older
//! ones answer with `main` and a pile of conventions about extensions
//! and `index.js`. Both are here, because both are on npm in quantity.
//!
//! `exports` is also a fence: a package that has one exports what it
//! lists and nothing else, so `pkg/internal/secret.js` is not reachable
//! just because the file is there. That is a rule about what a package
//! author meant rather than about security, and it is kept because a
//! package built around it will break in confusing ways without it.
//!
//! The conditions are the ones Deno matches, in Deno's order, since a
//! package that ships a build per runtime is choosing between builds
//! that were tested with that runtime's conditions. Asking as somebody
//! else is how you end up running the browser build of a thing that
//! wanted the node one, which is the whole complaint #596 exists to
//! answer.

// The caller is the loader, which arrives with the rest of #596.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use serde_json::Value;

/// What resolving a specifier found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Found {
    /// A file in this package.
    File(PathBuf),
    /// Somebody else's package, which an `imports` entry is allowed to
    /// name and the caller has to go and fetch.
    Package { name: String, sub: String },
}

/// The conditions this runtime satisfies, most specific first.
///
/// `import` rather than `require` because a specifier being resolved
/// here came from an `import`; the require side belongs with CJS.
pub(crate) const CONDITIONS: &[&str] = &["deno", "node", "import", "default"];

/// Which file in a package a subpath means.
///
/// The subpath is what is left of the specifier after the package name:
/// `.` for the package itself, `./locale` for `pkg/locale`. What comes
/// back is a file that exists, since a package that points at one that
/// does not is a broken package and saying so here is more use than
/// saying it later.
pub(crate) fn file(root: &Path, sub: &str, conditions: &[&str]) -> Result<PathBuf, String> {
    let manifest = manifest(root)?;
    let sub = match sub {
        "" | "." => ".".to_string(),
        sub if sub.starts_with("./") => sub.to_string(),
        sub => format!("./{sub}"),
    };
    if let Some(exports) = manifest.get("exports") {
        let target = exported(exports, &sub, conditions)
            .ok_or_else(|| format!("this package does not export {sub}"))?;
        return under(root, &target).and_then(|at| exists(at, &target));
    }
    match sub.as_str() {
        "." => main(root, &manifest),
        sub => {
            let at = under(root, sub)?;
            around(&at).ok_or_else(|| format!("this package has no {sub}"))
        }
    }
}

/// Which file an `#imports` specifier means, which may be a file in
/// this package or somebody else's package entirely.
pub(crate) fn own(root: &Path, spec: &str, conditions: &[&str]) -> Result<Found, String> {
    let manifest = manifest(root)?;
    let imports = manifest
        .get("imports")
        .ok_or_else(|| format!("{spec} is a package's own import and this package has none"))?;
    let target = exported(imports, spec, conditions)
        .ok_or_else(|| format!("this package does not import {spec}"))?;
    match target.starts_with("./") || target.starts_with("../") {
        true => under(root, &target)
            .and_then(|at| exists(at, &target))
            .map(Found::File),
        false => {
            let (name, sub) = split(&target);
            Ok(Found::Package { name, sub })
        }
    }
}

/// A package's manifest, which is the only file in it this trusts to
/// say anything about the rest.
pub(crate) fn manifest(root: &Path) -> Result<Value, String> {
    let at = root.join("package.json");
    let raw = std::fs::read(&at).map_err(|e| format!("{}: {e}", at.display()))?;
    serde_json::from_slice(&raw).map_err(|e| format!("{}: {e}", at.display()))
}

/// What `exports` or `imports` says a subpath is, as the target it
/// names, or nothing for a subpath the package does not have.
///
/// A target of `null` is a package saying this subpath is deliberately
/// not reachable, which reads the same as absent from here.
fn exported(map: &Value, sub: &str, conditions: &[&str]) -> Option<String> {
    // A bare string or array under `exports` is the package itself, and
    // so is an object whose keys are conditions rather than subpaths.
    // The two are told apart by the first key, which is what node does.
    let subpaths = match map {
        Value::Object(fields) => fields
            .keys()
            .next()
            .is_some_and(|it| it.starts_with('.') || it.starts_with('#')),
        _ => false,
    };
    if !subpaths {
        return match sub == "." {
            true => target(map, conditions, None),
            false => None,
        };
    }
    let fields = map.as_object()?;
    if let Some(exact) = fields.get(sub) {
        return target(exact, conditions, None);
    }
    // A pattern key has one `*` in it, and the one that wins is the one
    // whose part before the `*` is longest, so `./a/b/*` beats `./a/*`
    // for `./a/b/c`. The part the `*` stood for goes into the target
    // wherever its own `*` is.
    let mut best: Option<(&str, &Value, String)> = None;
    for (key, value) in fields {
        let Some((before, after)) = key.split_once('*') else {
            continue;
        };
        if key.matches('*').count() != 1 {
            continue;
        }
        let Some(rest) = sub.strip_prefix(before) else {
            continue;
        };
        let Some(rest) = rest.strip_suffix(after) else {
            continue;
        };
        if !after.is_empty() && rest.is_empty() {
            continue;
        }
        if best
            .as_ref()
            .is_none_or(|(won, _, _)| before.len() > won.split('*').next().unwrap_or("").len())
        {
            best = Some((key, value, rest.to_string()));
        }
    }
    let (_, value, stood_for) = best?;
    target(value, conditions, Some(&stood_for))
}

/// One target, out of a string, a list of them to try in order, or a
/// map of conditions to more of the same.
fn target(value: &Value, conditions: &[&str], stood_for: Option<&str>) -> Option<String> {
    match value {
        Value::String(target) => Some(match stood_for {
            Some(stood_for) => target.replace('*', stood_for),
            None => target.clone(),
        }),
        // A list is a fallback list: the first entry that resolves to
        // anything is the answer, which is how a package ships a wasm
        // build for the runtimes that take one and a js build for the
        // rest.
        Value::Array(list) => list.iter().find_map(|it| target(it, conditions, stood_for)),
        Value::Object(fields) => {
            for (condition, value) in fields {
                let matches = condition == "default" || conditions.contains(&condition.as_str());
                if matches && let Some(found) = target(value, conditions, stood_for) {
                    return Some(found);
                }
            }
            None
        }
        // `null`, which is the package saying no on purpose.
        _ => None,
    }
}

/// What a package with no `exports` means by itself: `main`, and the
/// conventions around it that predate anybody writing any of this down.
fn main(root: &Path, manifest: &Value) -> Result<PathBuf, String> {
    if let Some(named) = manifest.get("main").and_then(|it| it.as_str())
        && !named.trim().is_empty()
        && let Ok(at) = under(root, named)
        && let Some(found) = around(&at)
    {
        return Ok(found);
    }
    // No `main`, or a `main` pointing at nothing, and node falls back to
    // `index.js` either way rather than giving up.
    around(&root.join("index")).ok_or_else(|| "this package has no main and no index".to_string())
}

/// A path as written, or the file it means once the conventions are
/// applied: an extension it did not bother writing, or a directory with
/// an index in it.
fn around(at: &Path) -> Option<PathBuf> {
    if at.is_file() {
        return Some(at.to_path_buf());
    }
    let named = at.as_os_str().to_str()?;
    for extension in ["js", "json", "mjs", "cjs", "node"] {
        let with = PathBuf::from(format!("{named}.{extension}"));
        if with.is_file() {
            return Some(with);
        }
    }
    if at.is_dir() {
        // A directory's own `package.json` gets a say before its index
        // does, which is how half of npm points at `dist/`.
        if let Ok(manifest) = manifest(at)
            && let Some(named) = manifest.get("main").and_then(|it| it.as_str())
            && let Ok(inner) = under(at, named)
            && let Some(found) = around(&inner)
        {
            return Some(found);
        }
        for named in ["index.js", "index.json", "index.mjs", "index.cjs"] {
            let with = at.join(named);
            if with.is_file() {
                return Some(with);
            }
        }
    }
    None
}

/// A target as a path under the package, or a refusal.
///
/// A package may point at its own files and at nothing else. `../` out
/// of the root and an absolute path are both refused, and so is a path
/// through `node_modules`, which is node's own rule and is there
/// because a package reaching into the tree that installed it is
/// reaching at something nobody promised would be the same twice.
fn under(root: &Path, target: &str) -> Result<PathBuf, String> {
    let cleaned = target.trim_start_matches("./");
    if target.starts_with('/') || cleaned.contains(':') {
        return Err(format!("{target} is not a path inside this package"));
    }
    if cleaned.split('/').any(|part| part == "node_modules") {
        return Err(format!("{target} reaches through node_modules"));
    }
    let mut walk = PathBuf::new();
    for part in cleaned.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if !walk.pop() {
                    return Err(format!("{target} climbs out of this package"));
                }
            }
            part => walk.push(part),
        }
    }
    Ok(root.join(walk))
}

/// The file, if the package was telling the truth about it.
fn exists(at: PathBuf, target: &str) -> Result<PathBuf, String> {
    match at.is_file() {
        true => Ok(at),
        // An `exports` entry is exact: no extension guessing and no
        // index, because a package that has `exports` is a package that
        // wrote its own paths out.
        false => Err(format!(
            "this package points at {target}, which is not there"
        )),
    }
}

/// A specifier into the package it names and the subpath after it,
/// scopes included: `@a/b/c` is `@a/b` and `./c`.
fn split(spec: &str) -> (String, String) {
    let parts: Vec<&str> = spec.splitn(4, '/').collect();
    let (name, rest) = match spec.starts_with('@') {
        true => (
            parts[..2.min(parts.len())].join("/"),
            parts.get(2..).map(|it| it.join("/")),
        ),
        false => (parts[0].to_string(), parts.get(1..).map(|it| it.join("/"))),
    };
    let sub = match rest.filter(|it| !it.is_empty()) {
        Some(rest) => format!("./{rest}"),
        None => ".".to_string(),
    };
    (name, sub)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A package on a disk, which is what resolution is against: the
    /// manifest as written and whatever files are named beside it.
    fn package(manifest: &str, files: &[&str]) -> tempfile::TempDir {
        let root = tempfile::tempdir().expect("a temporary package");
        std::fs::write(root.path().join("package.json"), manifest).expect("a manifest");
        for named in files {
            let at = root.path().join(named);
            std::fs::create_dir_all(at.parent().expect("a directory")).expect("directories");
            std::fs::write(&at, "export const it = 1;\n").expect("a file");
        }
        root
    }

    fn found(root: &Path, sub: &str) -> String {
        let at = file(root, sub, CONDITIONS).unwrap_or_else(|e| panic!("{sub}: {e}"));
        at.strip_prefix(root)
            .expect("inside the package")
            .display()
            .to_string()
    }

    #[test]
    fn a_package_with_nothing_but_files_has_an_index() {
        let root = package(r#"{"name": "a"}"#, &["index.js", "other.js"]);
        assert_eq!(found(root.path(), "."), "index.js");
        assert_eq!(found(root.path(), "./other.js"), "other.js");
        // The extension nobody wrote, which is the oldest convention in
        // the language.
        assert_eq!(found(root.path(), "other"), "other.js");
    }

    #[test]
    fn a_main_is_followed_wherever_it_points() {
        let root = package(
            r#"{"name": "a", "main": "dist/entry.js"}"#,
            &["dist/entry.js"],
        );
        assert_eq!(found(root.path(), "."), "dist/entry.js");
        let directory = package(r#"{"name": "a", "main": "./lib"}"#, &["lib/index.js"]);
        assert_eq!(found(directory.path(), "."), "lib/index.js");
        // A main that points at nothing is not the end of it: node
        // looks for an index anyway, and packages depend on that.
        let wrong = package(r#"{"name": "a", "main": "./gone.js"}"#, &["index.js"]);
        assert_eq!(found(wrong.path(), "."), "index.js");
    }

    #[test]
    fn exports_answers_for_the_package_and_for_its_subpaths() {
        let root = package(
            r#"{"name": "a", "exports": {".": "./dist/index.js", "./locale": "./dist/locale.js"}}"#,
            &["dist/index.js", "dist/locale.js", "dist/secret.js"],
        );
        assert_eq!(found(root.path(), "."), "dist/index.js");
        assert_eq!(found(root.path(), "./locale"), "dist/locale.js");
        // The fence: the file is there and the package did not export
        // it, so it is not reachable.
        let said = file(root.path(), "./dist/secret.js", CONDITIONS).expect_err("not exported");
        assert!(said.contains("does not export"), "{said}");
    }

    #[test]
    fn a_bare_exports_string_is_the_package_itself() {
        let root = package(
            r#"{"name": "a", "exports": "./dist/index.js"}"#,
            &["dist/index.js"],
        );
        assert_eq!(found(root.path(), "."), "dist/index.js");
        assert!(
            file(root.path(), "./anything", CONDITIONS).is_err(),
            "and nothing else"
        );
    }

    #[test]
    fn the_condition_that_wins_is_the_first_one_the_package_listed() {
        let manifest = r#"{"name": "a", "exports": {".": {
            "browser": "./browser.js",
            "deno": "./deno.js",
            "import": "./module.js",
            "default": "./default.js"
        }}}"#;
        let root = package(
            manifest,
            &["browser.js", "deno.js", "module.js", "default.js"],
        );
        assert_eq!(
            found(root.path(), "."),
            "deno.js",
            "browser is not a condition here"
        );
        // Asking as somebody else gets the build that somebody else
        // was meant to run, which is the whole of what #596 is about.
        let as_browser = file(root.path(), ".", &["browser", "default"]).expect("a file");
        assert!(
            as_browser.ends_with("browser.js"),
            "{}",
            as_browser.display()
        );
        // A package that lists none of them still has a default.
        let plain = package(
            r#"{"name": "a", "exports": {".": {"require": "./cjs.js", "default": "./esm.js"}}}"#,
            &["cjs.js", "esm.js"],
        );
        assert_eq!(found(plain.path(), "."), "esm.js");
    }

    #[test]
    fn a_pattern_stands_for_whatever_matched_it() {
        let root = package(
            r#"{"name": "a", "exports": {"./lib/*": "./dist/lib/*.js", "./lib/deep/*": "./dist/deep/*.js"}}"#,
            &["dist/lib/one.js", "dist/deep/two.js"],
        );
        assert_eq!(found(root.path(), "./lib/one"), "dist/lib/one.js");
        // The longer pattern wins, which is why a package can carve out
        // a subtree of one it already exported.
        assert_eq!(found(root.path(), "./lib/deep/two"), "dist/deep/two.js");
    }

    #[test]
    fn a_subpath_a_package_blocked_reads_as_one_it_does_not_have() {
        let root = package(
            r#"{"name": "a", "exports": {"./*": "./src/*.js", "./internal/*": null}}"#,
            &["src/fine.js", "src/internal/hidden.js"],
        );
        assert_eq!(found(root.path(), "./fine"), "src/fine.js");
        let said =
            file(root.path(), "./internal/hidden", CONDITIONS).expect_err("blocked on purpose");
        assert!(said.contains("does not export"), "{said}");
    }

    #[test]
    fn a_list_of_targets_is_tried_until_one_of_them_is_there() {
        let root = package(
            r#"{"name": "a", "exports": {".": [{"wasm": "./it.wasm"}, "./it.js"]}}"#,
            &["it.js"],
        );
        assert_eq!(found(root.path(), "."), "it.js");
    }

    #[test]
    fn an_exported_path_is_exact() {
        let root = package(
            r#"{"name": "a", "exports": {".": "./dist/index"}}"#,
            &["dist/index.js"],
        );
        let said = file(root.path(), ".", CONDITIONS).expect_err("exports do not guess");
        assert!(said.contains("not there"), "{said}");
    }

    #[test]
    fn a_target_that_leaves_the_package_is_refused() {
        let root = package(
            r#"{"name": "a", "exports": {".": "../../etc/passwd"}}"#,
            &[],
        );
        let said =
            file(root.path(), ".", CONDITIONS).expect_err("a package may not point out of itself");
        assert!(said.contains("climbs out"), "{said}");
        let through = package(
            r#"{"name": "a", "exports": {".": "./node_modules/x/index.js"}}"#,
            &[],
        );
        let said =
            file(through.path(), ".", CONDITIONS).expect_err("nor through the tree around it");
        assert!(said.contains("node_modules"), "{said}");
    }

    #[test]
    fn a_package_can_import_its_own_names_and_other_peoples_packages() {
        let root = package(
            r##"{"name": "a", "imports": {"#inside": "./src/inside.js", "#outside": {"deno": "@scope/other/deep", "default": "fallback"}}}"##,
            &["src/inside.js"],
        );
        assert_eq!(
            own(root.path(), "#inside", CONDITIONS).expect("a file"),
            Found::File(root.path().join("src/inside.js"))
        );
        assert_eq!(
            own(root.path(), "#outside", CONDITIONS).expect("a package"),
            Found::Package {
                name: "@scope/other".into(),
                sub: "./deep".into()
            }
        );
    }

    /// A real package, fetched and then resolved: the two halves of
    /// #596 that are here so far, against a package that does have
    /// `exports` with conditions in it. Ignored because it wants the
    /// network, and run in the suite that is allowed to have one.
    #[test]
    #[ignore]
    fn a_real_package_answers_for_itself() {
        let cache = tempfile::tempdir().expect("a temporary cache");
        let root = crate::registry::package("@supabase/supabase-js", "^2.39.0", cache.path())
            .expect("a package");
        let at = file(&root, ".", CONDITIONS).expect("the package itself");
        let source = std::fs::read_to_string(&at).expect("a module");
        assert!(source.contains("createClient"), "{}", at.display());
        // Which file that is has moved between versions of this
        // package, so what is asserted is that it is a file the
        // package's own `exports` named and not one guessed at.
        assert!(at.starts_with(&root), "{}", at.display());
        assert!(at.is_file(), "{}", at.display());
        let deep = file(&root, "./dist/module/index.js", CONDITIONS);
        assert!(
            deep.is_err(),
            "and a path it did not export is not reachable"
        );
    }

    #[test]
    fn a_specifier_splits_at_the_package_name_scope_and_all() {
        assert_eq!(split("ms"), ("ms".into(), ".".into()));
        assert_eq!(split("ms/locale"), ("ms".into(), "./locale".into()));
        assert_eq!(
            split("@supabase/supabase-js"),
            ("@supabase/supabase-js".into(), ".".into())
        );
        assert_eq!(
            split("@supabase/supabase-js/dist/module/index.js"),
            (
                "@supabase/supabase-js".into(),
                "./dist/module/index.js".into()
            )
        );
    }
}
