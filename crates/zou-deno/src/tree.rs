//! Following a specifier from one file to the next, across packages.
//!
//! A package's own code imports things: `lodash`, `./util.js`,
//! `#internal`. Those are the same three shapes a function's code has,
//! and the answer to a bare one depends on where it was asked from,
//! because `lodash` inside `a` is whatever version `a` said it depends
//! on and inside `b` is whatever `b` said, and the two need not be the
//! same. That is what a `node_modules` tree encodes by putting a copy
//! of each answer next to whoever asked.
//!
//! There is no `node_modules` here. A package is unpacked once under
//! its own name and version, and who asked is worked out from the file
//! doing the asking: which package it is in, and what that package's
//! `dependencies` say the name means. Two packages depending on
//! different versions of the same thing get different directories, so
//! nothing is deduplicated into being wrong, and two depending on the
//! same version share one, so nothing is downloaded twice.
//!
//! A package that imports something it never declared is refused rather
//! than guessed at. npm installs such an import by accident all the
//! time, when something else in the tree happens to have pulled the
//! package in, and inheriting that accident means a function that works
//! until an unrelated dependency changes.

// The caller is the loader, which arrives with the rest of #596.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use crate::npm::Version;
use crate::resolve::{self, Found};

/// The unpacked packages on a disk, and the walk between them.
pub(crate) struct Tree {
    cache: PathBuf,
}

/// Where a specifier led.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Reached {
    /// The file itself.
    pub at: PathBuf,
    /// The package it is in, which is where the next specifier out of
    /// it will be answered from.
    pub root: PathBuf,
}

impl Tree {
    /// The packages under a cache directory, which is the module cache
    /// with a place of its own inside it.
    pub(crate) fn at(cache: &Path) -> Tree {
        Tree {
            cache: cache.join("npm"),
        }
    }

    /// A package by name and range, unpacked, as its directory.
    ///
    /// A version already on the disk that the range allows is the
    /// answer, without asking the registry anything. That is the same
    /// promise the module cache already makes for a url, and it is what
    /// lets a warm cache start a function with no network at all: what
    /// a range picks changes when a registry publishes, and a
    /// deployment that was handed a cache should run what the cache
    /// has.
    pub(crate) fn package(&self, name: &str, want: &str) -> Result<PathBuf, String> {
        if let Some(already) = self.have(name, want) {
            return Ok(already);
        }
        crate::registry::package(name, want, self.cache.parent().unwrap_or(&self.cache))
    }

    /// The version of a package on the disk that the range allows, the
    /// highest of them if there is more than one.
    fn have(&self, name: &str, want: &str) -> Option<PathBuf> {
        let range = crate::npm::Range::parse(want)?;
        let under = self.cache.join(name);
        let listed: Vec<(Version, PathBuf)> = std::fs::read_dir(&under)
            .ok()?
            .filter_map(|it| it.ok())
            .filter(|it| it.path().join("package.json").is_file())
            .filter_map(|it| {
                let named = it.file_name();
                let version = Version::parse(named.to_str()?)?;
                Some((version, it.path()))
            })
            .collect();
        let best = range.best(listed.iter().map(|(version, _)| version))?;
        listed
            .into_iter()
            .find(|(version, _)| *version == best)
            .map(|(_, at)| at)
    }

    /// What a specifier means, asked from a file.
    ///
    /// The three shapes are node's three: a path, a package's own
    /// `#name`, and somebody else's package. Anything with a scheme in
    /// it, `node:` or `https:` or `npm:`, belongs to the loader above
    /// this and is not answered here.
    pub(crate) fn resolve(&self, spec: &str, from: &Reached) -> Result<Reached, String> {
        if spec.starts_with("./") || spec.starts_with("../") || spec.starts_with('/') {
            let beside = from.at.parent().unwrap_or(&from.root);
            let at = walk(beside, spec);
            let at = around(&at).ok_or_else(|| format!("{spec}: no such file in this package"))?;
            return match at.starts_with(&from.root) {
                true => Ok(Reached {
                    at,
                    root: from.root.clone(),
                }),
                false => Err(format!("{spec} leaves the package that imported it")),
            };
        }
        if spec.starts_with('#') {
            return match resolve::own(&from.root, spec, resolve::CONDITIONS)? {
                Found::File(at) => Ok(Reached {
                    at,
                    root: from.root.clone(),
                }),
                Found::Package { name, sub } => self.other(&from.root, &name, &sub),
            };
        }
        let (name, sub) = split(spec);
        self.other(&from.root, &name, &sub)
    }

    /// Somebody else's package, as the asking package's manifest names
    /// it.
    fn other(&self, from: &Path, name: &str, sub: &str) -> Result<Reached, String> {
        let want = declared(from, name).ok_or_else(|| {
            format!("{name} is imported here and this package does not depend on it")
        })?;
        let root = self.package(name, &want)?;
        let at = resolve::file(&root, sub, resolve::CONDITIONS)
            .map_err(|why| format!("{name}{}: {why}", sub.trim_start_matches('.')))?;
        Ok(Reached { at, root })
    }

    /// The package and file a function's own import of `npm:` names,
    /// which is where a walk into this tree starts.
    pub(crate) fn entry(&self, name: &str, want: &str, sub: &str) -> Result<Reached, String> {
        let root = self.package(name, want)?;
        let at = resolve::file(&root, sub, resolve::CONDITIONS)?;
        Ok(Reached { at, root })
    }
}

/// What a package's manifest says a name is, out of the three places it
/// can say it.
///
/// `dependencies` is the ordinary one. `optionalDependencies` is a
/// dependency whose absence the package handles, and the code importing
/// it is written to survive that, so the range is worth following.
/// `peerDependencies` is a package saying somebody else installs this,
/// and following it is a guess, but the alternative is refusing a great
/// deal of code that works, so the guess is made and the range is the
/// one the package named.
fn declared(root: &Path, name: &str) -> Option<String> {
    let manifest = resolve::manifest(root).ok()?;
    ["dependencies", "optionalDependencies", "peerDependencies"]
        .iter()
        .find_map(|which| {
            manifest
                .get(which)?
                .get(name)?
                .as_str()
                .map(|it| it.to_string())
        })
}

/// A path specifier, relative to the file that wrote it.
fn walk(beside: &Path, spec: &str) -> PathBuf {
    let mut at = beside.to_path_buf();
    for part in spec.trim_start_matches('/').split('/') {
        match part {
            "" | "." => {}
            ".." => {
                at.pop();
            }
            part => at.push(part),
        }
    }
    at
}

/// The file a path means once the conventions are applied.
///
/// The same guessing the old resolution does, and it is here rather
/// than only there because a package's own files import each other with
/// extensions left off constantly, `exports` or no `exports`.
fn around(at: &Path) -> Option<PathBuf> {
    if at.is_file() {
        return Some(at.to_path_buf());
    }
    let named = at.as_os_str().to_str()?;
    for extension in ["js", "mjs", "cjs", "json", "ts", "tsx", "jsx"] {
        let with = PathBuf::from(format!("{named}.{extension}"));
        if with.is_file() {
            return Some(with);
        }
    }
    for named in ["index.js", "index.mjs", "index.cjs", "index.json"] {
        let with = at.join(named);
        if with.is_file() {
            return Some(with);
        }
    }
    None
}

/// A specifier into the package it names and the subpath after it.
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

    /// A cache with packages already unpacked in it, laid out the way
    /// the registry client lays them out: `npm/<name>/<version>`.
    struct Cache {
        at: tempfile::TempDir,
    }

    impl Cache {
        fn new() -> Cache {
            Cache {
                at: tempfile::tempdir().expect("a temporary cache"),
            }
        }

        fn with(
            &self,
            name: &str,
            version: &str,
            manifest: &str,
            files: &[(&str, &str)],
        ) -> PathBuf {
            let root = self.at.path().join("npm").join(name).join(version);
            std::fs::create_dir_all(&root).expect("a package directory");
            std::fs::write(root.join("package.json"), manifest).expect("a manifest");
            for (named, body) in files {
                let at = root.join(named);
                std::fs::create_dir_all(at.parent().expect("a directory")).expect("directories");
                std::fs::write(&at, body).expect("a file");
            }
            root
        }

        fn tree(&self) -> Tree {
            Tree::at(self.at.path())
        }
    }

    fn at(root: &Path, named: &str) -> Reached {
        Reached {
            at: root.join(named),
            root: root.to_path_buf(),
        }
    }

    #[test]
    fn a_relative_import_is_the_file_beside_the_one_that_wrote_it() {
        let cache = Cache::new();
        let root = cache.with(
            "a",
            "1.0.0",
            r#"{"name": "a", "main": "index.js"}"#,
            &[
                ("index.js", ""),
                ("lib/util.js", ""),
                ("lib/deep/one.js", ""),
            ],
        );
        let tree = cache.tree();
        let from = at(&root, "index.js");
        assert_eq!(
            tree.resolve("./lib/util.js", &from).expect("a file").at,
            root.join("lib/util.js")
        );
        // The extension nobody wrote, and the walk back up out of a
        // subdirectory.
        let deep = at(&root, "lib/deep/one.js");
        assert_eq!(
            tree.resolve("../util", &deep).expect("a file").at,
            root.join("lib/util.js")
        );
        assert!(
            tree.resolve("./deep", &at(&root, "lib/util.js")).is_err(),
            "no index there"
        );
    }

    #[test]
    fn a_relative_import_may_not_leave_the_package_that_wrote_it() {
        let cache = Cache::new();
        let root = cache.with("a", "1.0.0", r#"{"name": "a"}"#, &[("index.js", "")]);
        cache.with("b", "1.0.0", r#"{"name": "b"}"#, &[("index.js", "")]);
        let said = cache
            .tree()
            .resolve("../../b/1.0.0/index.js", &at(&root, "index.js"))
            .expect_err("a package's files are its own");
        assert!(said.contains("leaves the package"), "{said}");
    }

    #[test]
    fn a_bare_import_is_whatever_this_package_said_that_name_is() {
        let cache = Cache::new();
        let root = cache.with(
            "a",
            "1.0.0",
            r#"{"name": "a", "dependencies": {"c": "^1.0.0"}}"#,
            &[("index.js", "")],
        );
        let one = cache.with(
            "c",
            "1.2.0",
            r#"{"name": "c", "main": "main.js"}"#,
            &[("main.js", "")],
        );
        // A second version of the same package, which the range does
        // not allow, so it is not the one that answers.
        cache.with(
            "c",
            "2.0.0",
            r#"{"name": "c", "main": "main.js"}"#,
            &[("main.js", "")],
        );
        let reached = cache
            .tree()
            .resolve("c", &at(&root, "index.js"))
            .expect("a package");
        assert_eq!(reached.at, one.join("main.js"));
        assert_eq!(
            reached.root, one,
            "and the next specifier is asked from there"
        );
    }

    #[test]
    fn two_packages_asking_for_the_same_name_can_get_different_versions() {
        let cache = Cache::new();
        let a = cache.with(
            "a",
            "1.0.0",
            r#"{"name": "a", "dependencies": {"c": "^1.0.0"}}"#,
            &[("index.js", "")],
        );
        let b = cache.with(
            "b",
            "1.0.0",
            r#"{"name": "b", "dependencies": {"c": "^2.0.0"}}"#,
            &[("index.js", "")],
        );
        cache.with(
            "c",
            "1.2.0",
            r#"{"name": "c", "main": "main.js"}"#,
            &[("main.js", "")],
        );
        cache.with(
            "c",
            "2.0.0",
            r#"{"name": "c", "main": "main.js"}"#,
            &[("main.js", "")],
        );
        let tree = cache.tree();
        let from_a = tree.resolve("c", &at(&a, "index.js")).expect("a package");
        let from_b = tree.resolve("c", &at(&b, "index.js")).expect("a package");
        assert!(
            from_a.root.ends_with("c/1.2.0"),
            "{}",
            from_a.root.display()
        );
        assert!(
            from_b.root.ends_with("c/2.0.0"),
            "{}",
            from_b.root.display()
        );
    }

    #[test]
    fn a_subpath_of_a_dependency_is_asked_of_that_dependency() {
        let cache = Cache::new();
        let root = cache.with(
            "a",
            "1.0.0",
            r#"{"name": "a", "dependencies": {"@scope/c": "1.0.0"}}"#,
            &[("index.js", "")],
        );
        let c = cache.with(
            "@scope/c",
            "1.0.0",
            r#"{"name": "@scope/c", "exports": {".": "./index.js", "./deep": "./deep.js"}}"#,
            &[("index.js", ""), ("deep.js", "")],
        );
        let tree = cache.tree();
        let reached = tree
            .resolve("@scope/c/deep", &at(&root, "index.js"))
            .expect("a subpath");
        assert_eq!(reached.at, c.join("deep.js"));
        let said = tree
            .resolve("@scope/c/hidden", &at(&root, "index.js"))
            .expect_err("not exported");
        assert!(said.contains("does not export"), "{said}");
    }

    #[test]
    fn an_import_a_package_never_declared_is_refused_rather_than_guessed_at() {
        let cache = Cache::new();
        let root = cache.with("a", "1.0.0", r#"{"name": "a"}"#, &[("index.js", "")]);
        cache.with("c", "1.0.0", r#"{"name": "c"}"#, &[("index.js", "")]);
        let said = cache
            .tree()
            .resolve("c", &at(&root, "index.js"))
            .expect_err("undeclared is not the same as installed");
        assert!(said.contains("does not depend on it"), "{said}");
    }

    #[test]
    fn the_three_places_a_dependency_can_be_declared_all_count() {
        let cache = Cache::new();
        let manifest = r#"{"name": "a",
            "dependencies": {"one": "1.0.0"},
            "optionalDependencies": {"two": "1.0.0"},
            "peerDependencies": {"three": "1.0.0"}}"#;
        let root = cache.with("a", "1.0.0", manifest, &[("index.js", "")]);
        for named in ["one", "two", "three"] {
            cache.with(
                named,
                "1.0.0",
                &format!(r#"{{"name": "{named}"}}"#),
                &[("index.js", "")],
            );
        }
        let tree = cache.tree();
        for named in ["one", "two", "three"] {
            let reached = tree.resolve(named, &at(&root, "index.js")).expect(named);
            assert!(
                reached.root.ends_with(format!("{named}/1.0.0")),
                "{}",
                reached.root.display()
            );
        }
    }

    #[test]
    fn a_packages_own_name_leads_where_its_imports_say() {
        let cache = Cache::new();
        let manifest = r##"{"name": "a", "imports": {"#in": "./src/in.js", "#out": "c"},
            "dependencies": {"c": "1.0.0"}}"##;
        let root = cache.with(
            "a",
            "1.0.0",
            manifest,
            &[("index.js", ""), ("src/in.js", "")],
        );
        let c = cache.with(
            "c",
            "1.0.0",
            r#"{"name": "c", "main": "main.js"}"#,
            &[("main.js", "")],
        );
        let tree = cache.tree();
        assert_eq!(
            tree.resolve("#in", &at(&root, "index.js"))
                .expect("a file")
                .at,
            root.join("src/in.js")
        );
        let out = tree
            .resolve("#out", &at(&root, "index.js"))
            .expect("a package");
        assert_eq!(out.at, c.join("main.js"));
        assert_eq!(out.root, c);
    }

    #[test]
    fn a_warm_cache_answers_a_range_without_asking_anybody() {
        let cache = Cache::new();
        // Two versions on the disk and no network in this test at all:
        // if the tree asked the registry anything this would not be a
        // test that passes, it would be a test that hangs.
        cache.with(
            "c",
            "1.2.0",
            r#"{"name": "c", "main": "main.js"}"#,
            &[("main.js", "")],
        );
        let two = cache.with(
            "c",
            "1.9.0",
            r#"{"name": "c", "main": "main.js"}"#,
            &[("main.js", "")],
        );
        assert_eq!(
            cache.tree().package("c", "^1.0.0").expect("the higher one"),
            two
        );
        assert_eq!(
            cache
                .tree()
                .package("c", "1.2.0")
                .expect("the exact one")
                .file_name()
                .unwrap(),
            "1.2.0"
        );
    }

    /// A real package and its real dependencies: supabase-js imports
    /// four of its own, and each of those is a name that has to be
    /// looked up in its manifest, fetched and resolved. Ignored because
    /// it wants the network.
    #[test]
    #[ignore]
    fn a_real_package_reaches_its_real_dependencies() {
        let cache = tempfile::tempdir().expect("a temporary cache");
        let tree = Tree::at(cache.path());
        let entry = tree
            .entry("@supabase/supabase-js", "^2.39.0", ".")
            .expect("the package itself");
        let source = std::fs::read_to_string(&entry.at).expect("a module");
        // What its entry point imports, which is the whole of the
        // rest of supabase: auth, storage, functions, postgrest.
        let mut reached = 0;
        for spec in [
            "@supabase/auth-js",
            "@supabase/postgrest-js",
            "@supabase/storage-js",
            "@supabase/functions-js",
        ] {
            if !source.contains(spec) {
                continue;
            }
            let next = tree
                .resolve(spec, &entry)
                .unwrap_or_else(|e| panic!("{spec}: {e}"));
            assert!(next.at.is_file(), "{spec}: {}", next.at.display());
            reached += 1;
        }
        assert!(
            reached >= 3,
            "its entry point imports its own packages: {reached}"
        );
    }
}
