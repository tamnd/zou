//! Reading a commonjs file well enough to import from it.
//!
//! Half of npm is still commonjs, and the half that is not depends on
//! the half that is, so node module resolution without commonjs moves
//! nothing: the file it resolves to is a script with a `require` in it
//! and no `export` anywhere.
//!
//! Running one is the easy part, since a script wrapped in a function
//! that is handed `module`, `exports` and `require` is what node does
//! and it works here too. The hard part is `import { createClient }
//! from "a-cjs-package"`, because the names an es module exports are
//! decided when the module is compiled and a script decides its exports
//! by running. Node solves it by reading the source and guessing well:
//! `exports.foo = ...`, `module.exports = { foo }`, and the handful of
//! shapes a bundler emits. This does the same, with the same reading
//! Deno uses for it, and a name it guessed wrong is a name that is
//! `undefined` rather than a module that will not compile.
//!
//! What is here is both halves of it: the reading, the module text it
//! produces, and the two ops that text's `require` runs on. What is not
//! here is the loader deciding to produce that text at all, which is
//! the last piece and arrives with the change that makes an `npm:`
//! import mean the tarball rather than a url on a registry.

// The caller of the reading is that change.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::path::Path;

use deno_core::{OpState, op2};
use deno_error::JsErrorBox;

use crate::tree::{Reached, Tree};

/// Which of the two languages in a `.js` file this one is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// `import` and `export`, compiled as a module.
    Module,
    /// `require` and `module.exports`, run as a script.
    Script,
}

/// How deep a chain of re-exports is followed before this decides the
/// package is playing a game rather than exporting something.
const DEEP: usize = 8;

/// Which language a file in a package is, by the rules node reads it
/// with: the extension when the extension says, and the package's
/// `type` when it does not.
pub(crate) fn kind(at: &Path, root: &Path) -> Kind {
    match at.extension().and_then(|it| it.to_str()) {
        Some("mjs") | Some("mts") => Kind::Module,
        Some("cjs") | Some("cts") => Kind::Script,
        _ => match crate::resolve::manifest(root)
            .ok()
            .and_then(|it| it.get("type").and_then(|it| it.as_str()).map(String::from))
        {
            Some(said) if said == "module" => Kind::Module,
            // No `type` at all is commonjs, which is the default node
            // has kept for compatibility and which most of npm relies
            // on without saying so.
            _ => Kind::Script,
        },
    }
}

/// What a script assigns to its exports, and what it hands off to
/// somebody else.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Analysis {
    /// The names it sets on `exports` or `module.exports`.
    pub exports: Vec<String>,
    /// The specifiers it re-exports wholesale, `__exportStar(require(
    /// "./other"), exports)` and its several spellings, whose own
    /// names belong to this file too.
    pub reexports: Vec<String>,
}

/// What a file's source says it exports.
///
/// The reading is deno_ast's, which is swc's parse and the same walk
/// over it that cjs-module-lexer does, so a package that node can
/// import names from is a package this can too.
pub(crate) fn analyze(at: &Path, text: &str) -> Result<Analysis, String> {
    let specifier = deno_core::ModuleSpecifier::from_file_path(at)
        .map_err(|()| format!("{} is not a path", at.display()))?;
    let media = deno_ast::MediaType::from_specifier(&specifier);
    let parsed = deno_ast::parse_program(deno_ast::ParseParams {
        specifier,
        text: text.into(),
        media_type: media,
        capture_tokens: false,
        scope_analysis: false,
        maybe_syntax: None,
    })
    .map_err(|e| format!("{}: {e}", at.display()))?;
    let found = match parsed.compute_is_script() {
        true => parsed.analyze_cjs(),
        // A file that is an es module after all, which happens when a
        // package says commonjs in its manifest and ships modules
        // anyway. Its exports are the ones it wrote down.
        false => parsed.analyze_es_runtime_exports(),
    };
    Ok(Analysis {
        exports: found.exports,
        reexports: found.reexports,
    })
}

/// Every name importing this file can ask for, the ones it re-exports
/// from elsewhere included.
///
/// A re-export this cannot follow is passed over rather than failed on:
/// the names behind it become `undefined` instead of the whole import
/// becoming an error, which is the same trade the reading itself makes.
pub(crate) fn names(tree: &Tree, from: &Reached) -> Result<Vec<String>, String> {
    let mut found = BTreeSet::new();
    let mut seen = BTreeSet::new();
    walk(tree, from, &mut found, &mut seen, DEEP)?;
    Ok(found.into_iter().filter(|it| usable(it)).collect())
}

fn walk(
    tree: &Tree,
    from: &Reached,
    found: &mut BTreeSet<String>,
    seen: &mut BTreeSet<std::path::PathBuf>,
    left: usize,
) -> Result<(), String> {
    if !seen.insert(from.at.clone()) {
        return Ok(());
    }
    let text =
        std::fs::read_to_string(&from.at).map_err(|e| format!("{}: {e}", from.at.display()))?;
    let analysis = analyze(&from.at, &text)?;
    found.extend(analysis.exports);
    if left == 0 {
        return Ok(());
    }
    for spec in analysis.reexports {
        // A built in re-exported wholesale is a thing this cannot read
        // the names of, since there is no file to read. What it costs
        // is named imports from such a package, which is rare enough
        // to be worth less than the failure would be.
        if spec.starts_with("node:") {
            continue;
        }
        let Ok(next) = tree.required(&spec, from) else {
            continue;
        };
        walk(tree, &next, found, seen, left - 1)?;
    }
    Ok(())
}

/// Whether a name is one an es module can export under.
///
/// A script can put anything on its exports, including `1`, `class` and
/// the empty string, and an es module can export none of those under
/// that name. What is dropped here is reachable as a property of the
/// default export, which is where code that uses such a name is looking
/// anyway.
fn usable(name: &str) -> bool {
    const RESERVED: &[&str] = &[
        "await",
        "break",
        "case",
        "catch",
        "class",
        "const",
        "continue",
        "debugger",
        "default",
        "delete",
        "do",
        "else",
        "enum",
        "export",
        "extends",
        "false",
        "finally",
        "for",
        "function",
        "if",
        "import",
        "in",
        "instanceof",
        "new",
        "null",
        "return",
        "super",
        "switch",
        "this",
        "throw",
        "true",
        "try",
        "typeof",
        "var",
        "void",
        "while",
        "with",
        "yield",
    ];
    let mut letters = name.chars();
    let first = letters.next();
    first.is_some_and(|it| it.is_ascii_alphabetic() || it == '_' || it == '$')
        && letters.all(|it| it.is_ascii_alphanumeric() || it == '_' || it == '$')
        && !RESERVED.contains(&name)
}

/// The es module that stands in for a script.
///
/// It is a module the isolate compiles like any other, and all it does
/// is run the script and hand back what the script assigned. The
/// default export is `module.exports` itself, which is what a package
/// doing `module.exports = function () {}` means and what
/// `import thing from "pkg"` should get. The named exports are the
/// guesses, taken off that same object, so a name that was guessed
/// wrong is `undefined` and a name that was missed is still there
/// through the default.
pub(crate) fn wrapper(url: &str, names: &[String]) -> String {
    let mut module = String::new();
    // First, because the script it is about to run may call `require`
    // for a built in before it does anything else, and an import that
    // is written first is evaluated first.
    module.push_str("import \"");
    module.push_str(crate::module::BUILTINS);
    module.push_str("\";\n");
    module.push_str("const module = globalThis.__zouRequire(");
    module.push_str(&json(url));
    module.push_str(");\nexport default module.exports;\n");
    if !names.is_empty() {
        module.push_str("export const { ");
        module.push_str(&names.join(", "));
        module.push_str(" } = module.exports;\n");
    }
    module
}

/// A string as javascript source, which is json's rules and json's
/// escaping.
fn json(text: &str) -> String {
    serde_json::Value::String(text.to_string()).to_string()
}

/// One script, as the thing javascript needs to run one.
#[derive(serde::Serialize)]
pub(crate) struct Script {
    /// The source, which the require wraps in a function and calls.
    text: String,
    /// Whether it is data rather than code. A json file is required all
    /// the time, by every package that reads its own version number out
    /// of its manifest, and what a require of one hands back is the
    /// parsed value.
    data: bool,
    /// The file itself, since a script is handed `__filename` and works
    /// out `__dirname` from it.
    path: String,
}

/// The source of the file a require landed on.
///
/// Two places on the disk are readable this way and no others: the
/// module cache, where a package that was fetched was unpacked, and the
/// directory the function was deployed into. Both are places the module
/// loader would already read a file out of, so nothing is reachable
/// through a require that was not reachable through an import, which is
/// the whole rule.
#[op2]
#[serde]
pub(crate) fn op_zou_cjs_read(
    state: &mut OpState,
    #[string] url: String,
) -> Result<Script, JsErrorBox> {
    let at = file(&url)?;
    let root = &state.borrow::<crate::isolate::Owned>().root;
    if !at.starts_with(crate::module::cache()) && !at.starts_with(root) {
        return Err(JsErrorBox::type_error(format!(
            "{} is neither in the module cache nor in this function's own directory, so it is not a script this function can require",
            at.display()
        )));
    }
    let text = std::fs::read_to_string(&at)
        .map_err(|e| JsErrorBox::type_error(format!("{}: {e}", at.display())))?;
    Ok(Script {
        data: at.extension().and_then(|it| it.to_str()) == Some("json"),
        path: at.to_string_lossy().to_string(),
        text,
    })
}

/// What a specifier inside a script means, as the url of the file it
/// landed on, or as `node:` and a name for a built in.
///
/// The resolution is the same one an import goes through, asked under
/// the require conditions, so a package that ships two builds hands the
/// script the build it wrote for a script.
#[op2]
#[string]
pub(crate) fn op_zou_cjs_resolve(
    #[string] spec: String,
    #[string] from: String,
) -> Result<String, JsErrorBox> {
    let name = spec.strip_prefix("node:").unwrap_or(&spec);
    // A core module wins over anything a package declares, which is
    // node's rule and the reason `require("events")` inside a package
    // that depends on the npm package of that name is still the built
    // in. A name node has and this does not is answered as `node:` all
    // the same, so the refusal names the built in that is missing
    // rather than a manifest that was never going to mention it.
    if crate::node::source(name).is_some() || crate::node::core(name) {
        return Ok(format!("node:{name}"));
    }
    if spec.starts_with("node:") {
        return Err(JsErrorBox::type_error(format!(
            "there is no node built in {name} here, so require(\"{spec}\") has no answer"
        )));
    }
    let at = file(&from)?;
    let asking = Reached {
        root: rooted(&at),
        at,
    };
    let tree = Tree::at(&crate::module::cache());
    let reached = tree
        .required(&spec, &asking)
        .map_err(|why| JsErrorBox::type_error(format!("require(\"{spec}\"): {why}")))?;
    deno_core::ModuleSpecifier::from_file_path(&reached.at)
        .map(|it| it.to_string())
        .map_err(|()| JsErrorBox::type_error(format!("{} is not a path", reached.at.display())))
}

/// The package a file belongs to, which is the nearest manifest above
/// it that names a package.
///
/// Nearest and named, because a package ships manifests that are not
/// its own: a `dist/package.json` saying `{"type": "module"}` is a
/// package telling node how to read one directory, and answering a
/// dependency out of it would be answering out of a manifest that
/// declares nothing. A file with no manifest above it at all is its own
/// island, which is what a script beside a function's `index.ts` is.
pub(crate) fn rooted(at: &Path) -> std::path::PathBuf {
    let mut walking = at.parent();
    while let Some(directory) = walking {
        let manifest = directory.join("package.json");
        if manifest.is_file()
            && crate::resolve::manifest(directory)
                .ok()
                .and_then(|it| it.get("name").cloned())
                .is_some()
        {
            return directory.to_path_buf();
        }
        walking = directory.parent();
    }
    at.parent().unwrap_or(at).to_path_buf()
}

/// A file url as the file it names.
fn file(url: &str) -> Result<std::path::PathBuf, JsErrorBox> {
    deno_core::ModuleSpecifier::parse(url)
        .ok()
        .and_then(|it| it.to_file_path().ok())
        .ok_or_else(|| JsErrorBox::type_error(format!("{url} is not a file this can be read from")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn analysed(named: &str, text: &str) -> Analysis {
        let at = PathBuf::from("/tmp/zou-cjs").join(named);
        analyze(&at, text).unwrap_or_else(|e| panic!("{named}: {e}"))
    }

    #[test]
    fn the_names_a_script_assigns_are_the_names_it_exports() {
        let found = analysed(
            "one.js",
            r#"
            exports.one = 1;
            module.exports.two = function () {};
            Object.defineProperty(exports, "three", { get: function () { return 3; } });
            "#,
        );
        // Sorted, because the reading collects them into a set: what
        // an importer needs is the list of names and not the order the
        // file happened to set them in.
        assert_eq!(found.exports, vec!["one", "three", "two"]);
        assert!(found.reexports.is_empty());
    }

    #[test]
    fn a_whole_object_assigned_at_once_is_read_too() {
        let found = analysed(
            "two.js",
            r#"
            function createClient() {}
            module.exports = { createClient, VERSION: "1" };
            "#,
        );
        assert!(
            found.exports.contains(&"createClient".to_string()),
            "{found:?}"
        );
        assert!(found.exports.contains(&"VERSION".to_string()), "{found:?}");
    }

    #[test]
    fn what_a_script_hands_off_to_somebody_else_is_written_down() {
        let found = analysed(
            "three.js",
            r#"
            const other = require("./other.js");
            Object.keys(other).forEach(function (k) { exports[k] = other[k]; });
            exports.own = 1;
            "#,
        );
        assert_eq!(found.exports, vec!["own"]);
        assert_eq!(found.reexports, vec!["./other.js"], "{found:?}");
    }

    #[test]
    fn a_file_that_turned_out_to_be_a_module_is_read_as_one() {
        let found = analysed("four.js", "export const one = 1;\nexport default 2;\n");
        assert!(found.exports.contains(&"one".to_string()), "{found:?}");
    }

    #[test]
    fn which_language_a_file_is_written_in_is_the_extension_then_the_manifest() {
        let root = tempfile::tempdir().expect("a package");
        std::fs::write(root.path().join("package.json"), r#"{"name": "a"}"#).expect("a manifest");
        assert_eq!(
            kind(&root.path().join("index.js"), root.path()),
            Kind::Script
        );
        assert_eq!(
            kind(&root.path().join("index.mjs"), root.path()),
            Kind::Module
        );
        std::fs::write(
            root.path().join("package.json"),
            r#"{"name": "a", "type": "module"}"#,
        )
        .expect("a manifest");
        assert_eq!(
            kind(&root.path().join("index.js"), root.path()),
            Kind::Module
        );
        // The extension still wins over what the manifest says, which
        // is how a package ships one file of each.
        assert_eq!(
            kind(&root.path().join("legacy.cjs"), root.path()),
            Kind::Script
        );
    }

    #[test]
    fn a_name_no_module_can_export_is_left_to_the_default_export() {
        assert!(usable("createClient"));
        assert!(usable("_private"));
        assert!(usable("$"));
        assert!(!usable("class"), "a reserved word");
        assert!(!usable("default"), "which the wrapper exports itself");
        assert!(!usable("2fast"), "not an identifier");
        assert!(!usable(""), "nor is nothing");
    }

    #[test]
    fn the_module_that_stands_in_for_a_script_runs_it_and_hands_back_what_it_set() {
        let text = wrapper("file:///a/index.js", &["one".into(), "two".into()]);
        assert!(text.contains(r#"import "zou:node";"#), "{text}");
        assert!(
            text.contains(r#"__zouRequire("file:///a/index.js")"#),
            "{text}"
        );
        assert!(text.contains("export default module.exports;"), "{text}");
        assert!(
            text.contains("export const { one, two } = module.exports;"),
            "{text}"
        );
        // A script nobody could read a name out of is still importable,
        // for its default and for whatever is on it.
        let bare = wrapper("file:///a/index.js", &[]);
        assert!(!bare.contains("export const"), "{bare}");
    }

    #[test]
    fn a_url_with_something_awkward_in_it_is_still_a_string() {
        let text = wrapper("file:///a/\"; evil(); //.js", &[]);
        assert!(text.contains(r#"\"; evil(); //.js"#), "{text}");
    }

    #[test]
    fn the_names_behind_a_re_export_belong_to_the_file_that_re_exported_them() {
        let cache = tempfile::tempdir().expect("a temporary cache");
        let root = cache.path().join("npm").join("a").join("1.0.0");
        std::fs::create_dir_all(&root).expect("a package");
        std::fs::write(root.join("package.json"), r#"{"name": "a"}"#).expect("a manifest");
        std::fs::write(
            root.join("index.js"),
            r#"
            const other = require("./other.js");
            Object.keys(other).forEach(function (k) { exports[k] = other[k]; });
            exports.own = 1;
            "#,
        )
        .expect("a file");
        std::fs::write(root.join("other.js"), "exports.borrowed = 1;\n").expect("a file");
        let tree = Tree::at(cache.path());
        let from = Reached {
            at: root.join("index.js"),
            root: root.clone(),
        };
        let found = names(&tree, &from).expect("the names");
        assert_eq!(found, vec!["borrowed".to_string(), "own".to_string()]);
    }

    /// A real commonjs package off the registry, read the way an
    /// importer of it would need it read. dotenv because it is
    /// commonjs, it is small, and what it exports is the thing every
    /// example does `import { config } from "dotenv"` for. Ignored
    /// because it wants the network.
    #[test]
    #[ignore]
    fn a_real_commonjs_package_says_what_it_exports() {
        let cache = tempfile::tempdir().expect("a temporary cache");
        let tree = Tree::at(cache.path());
        let entry = tree.entry("dotenv", "^16.0.0", ".").expect("a package");
        assert_eq!(
            kind(&entry.at, &entry.root),
            Kind::Script,
            "{}",
            entry.at.display()
        );
        let found = names(&tree, &entry).expect("the names");
        for wanted in ["config", "parse"] {
            assert!(
                found.contains(&wanted.to_string()),
                "{wanted} is missing from {found:?}"
            );
        }
    }

    #[test]
    fn a_re_export_that_goes_in_a_circle_stops() {
        let cache = tempfile::tempdir().expect("a temporary cache");
        let root = cache.path().join("npm").join("a").join("1.0.0");
        std::fs::create_dir_all(&root).expect("a package");
        std::fs::write(root.join("package.json"), r#"{"name": "a"}"#).expect("a manifest");
        let round = |named: &str, other: &str, own: &str| {
            std::fs::write(
                root.join(named),
                format!(
                    r#"
                    const other = require("./{other}");
                    Object.keys(other).forEach(function (k) {{ exports[k] = other[k]; }});
                    exports.{own} = 1;
                    "#
                ),
            )
            .expect("a file");
        };
        round("index.js", "other.js", "one");
        round("other.js", "index.js", "two");
        let tree = Tree::at(cache.path());
        let from = Reached {
            at: root.join("index.js"),
            root: root.clone(),
        };
        let found = names(&tree, &from).expect("a walk that ends");
        assert_eq!(found, vec!["one".to_string(), "two".to_string()]);
    }
}
