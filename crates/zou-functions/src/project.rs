//! Reading the functions a project keeps on disk.
//!
//! Everything below was taken off `supabase functions serve` 1.74.2
//! against a real `supabase start` rather than read out of the CLI's
//! source, because what a project can rely on is what the running
//! thing does. Five directories were put beside four working functions
//! and the runtime listed which of them it would serve:
//!
//! ```text
//! Skipped serving Function: hello        (enabled = false)
//!  - .../functions/v1/boom
//!  - .../functions/v1/open
//!  - .../functions/v1/probe
//!  - .../functions/v1/stream
//! ```
//!
//! `jsfn/index.js` was not served, so the entrypoint is `index.ts` and
//! not whatever Deno would happily run. `_shared` was not served, and
//! neither was `.hidden`, which is the convention every example
//! project already uses for the code its functions import. `nested/deep`
//! was not served, so the listing is one level deep rather than a walk.
//! `noindex`, a directory with a file in it that is not an entrypoint,
//! was not served. And every one of those five answered the same 404
//! `Function not found` a name nobody wrote answers, rather than
//! anything that would tell a caller the difference.

use std::collections::BTreeMap;
use std::path::Path;

use crate::{Function, Policy};

/// One `[functions.<name>]` block.
///
/// This crate does not read TOML. The project's config file is read in
/// one place, by the thing that owns the file, and what arrives here is
/// what that reader made of it, so a server embedded in an application
/// that keeps its settings somewhere else entirely can fill this in
/// without pretending to have a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// Off, and the function is not served at all rather than refused,
    /// which is upstream's behaviour and the reason this crate has a
    /// separate idea of what exists and what is served.
    pub enabled: bool,
    /// Whether a caller must carry a token this project can verify.
    pub verify_jwt: bool,
    /// A path relative to the directory the config file is in, the way
    /// the file writes it.
    pub import_map: Option<String>,
    /// The same, for a function whose entrypoint is not the usual one.
    pub entrypoint: Option<String>,
    /// Upstream's globs, relative to the same directory, kept as they
    /// were written because it is the runtime that expands them.
    pub static_files: Vec<String>,
}

impl Default for Settings {
    /// Upstream's defaults, which are the ones a function that
    /// configures nothing gets: served, and verified.
    fn default() -> Settings {
        Settings {
            enabled: true,
            verify_jwt: true,
            import_map: None,
            entrypoint: None,
            static_files: Vec::new(),
        }
    }
}

/// What the project's config file says about functions: the blocks per
/// function, and the `[edge_runtime]` settings that are about all of
/// them at once.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Layout {
    pub policy: Policy,
    /// Where a debugger attaches, upstream's `inspector_port`. A build
    /// with an engine in it opens the port and answers the Chrome
    /// DevTools Protocol on it; a build without one has nothing to
    /// attach to and ignores it.
    pub inspector_port: Option<u16>,
    /// Upstream's `[edge_runtime.secrets]`, names and values with the
    /// `env(NAME)` already resolved by whoever read the file, and what
    /// a project's own `.env` is merged over.
    pub secrets: BTreeMap<String, String>,
    pub settings: BTreeMap<String, Settings>,
}

impl Layout {
    /// The block for `name`, or upstream's defaults, which is what a
    /// function the file never mentions gets.
    pub fn settings(&self, name: &str) -> Settings {
        self.settings.get(name).cloned().unwrap_or_default()
    }
}

/// The functions served out of `dir`, which is the directory the
/// project's config file lives in and has `functions/` beside it.
///
/// A project with no `functions` directory has no functions, which is
/// not an error: it is most projects. A directory that cannot be read
/// at all is an error, because that is a permission or a disk saying
/// something rather than a project saying nothing.
///
/// The names come from the listing. An `entrypoint` in the config moves
/// which file a function starts at, it does not add a function that has
/// no directory of its own.
pub fn read(dir: &Path, layout: &Layout) -> Result<Vec<Function>, String> {
    let root = dir.join("functions");
    let entries = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("read {}: {e}", root.display())),
    };
    let mut found: Vec<Function> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| format!("read {}: {e}", root.display()))?;
        let name = entry.file_name().to_string_lossy().to_string();
        if !entry.path().is_dir() || name.starts_with('_') || name.starts_with('.') {
            continue;
        }
        let settings = layout.settings(&name);
        if !settings.enabled {
            continue;
        }
        let entrypoint = match &settings.entrypoint {
            Some(path) => dir.join(path),
            None => root.join(&name).join("index.ts"),
        };
        if !entrypoint.is_file() {
            continue;
        }
        let import_map = match &settings.import_map {
            Some(path) => Some(dir.join(path)),
            None => beside(&root, &name),
        };
        found.push(Function {
            name,
            entrypoint,
            verify_jwt: settings.verify_jwt,
            import_map,
            static_files: settings.static_files.iter().map(|p| dir.join(p)).collect(),
        });
    }
    // The listing arrives in whatever order the filesystem hands it
    // over, and what is printed at boot should not depend on that.
    found.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(found)
}

/// The import map a function has without saying so, in the order
/// `GetFunctionConfig` looks for one.
///
/// The config file's own `import_map` beats all of these and is handled
/// by the caller, which leaves four places and a precedence between
/// them: `deno.json` beside the function, then `deno.jsonc`, then
/// `import_map.json`, then one `import_map.json` for the whole project.
/// The last two are deprecated upstream and warn on the way past, and
/// the warning is the useful half of copying the order at all: a
/// project that has both a `deno.json` and an `import_map.json` should
/// hear the same thing here it hears from the CLI rather than quietly
/// get the other file.
///
/// The directory searched is the function's own, `functions/<name>`,
/// even when the entrypoint was moved somewhere else, which is what
/// upstream does: `functionDir` there is built from the name and not
/// from the entrypoint.
fn beside(root: &Path, name: &str) -> Option<std::path::PathBuf> {
    let dir = root.join(name);
    for file in ["deno.json", "deno.jsonc"] {
        let at = dir.join(file);
        if at.is_file() {
            return Some(at);
        }
    }
    let own = dir.join("import_map.json");
    if own.is_file() {
        once(format!(
            "function {name} uses a deprecated import_map.json, which deno.json replaces: {}",
            own.display()
        ));
        return Some(own);
    }
    let shared = root.join("import_map.json");
    if shared.is_file() {
        once(format!(
            "function {name} falls back to the project's import map, which a deno.json beside the function replaces: {}",
            shared.display()
        ));
        return Some(shared);
    }
    None
}

/// A warning about how a project is laid out, said once.
///
/// The functions of a project are read again on every call, because a
/// function edited on disk is served without a restart, so a warning
/// said where they are read is a warning said on every call. A project
/// with an import map and forty functions in it wrote six thousand
/// lines into a log for thirty nine calls, which buries the errors the
/// same log is for. The thing being complained about is the layout, and
/// the layout is the same on the second reading as it was on the first.
fn once(said: String) {
    static ALREADY: std::sync::Mutex<Option<std::collections::HashSet<String>>> =
        std::sync::Mutex::new(None);
    let mut already = ALREADY.lock().expect("the warnings already said");
    if already
        .get_or_insert_with(Default::default)
        .insert(said.clone())
    {
        log::warn!("{said}");
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    /// A project directory shaped like the one the answers above were
    /// taken from.
    fn project() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("functions");
        for name in ["hello", "open", "probe", "_shared", ".hidden", "noindex"] {
            std::fs::create_dir_all(root.join(name)).expect("mkdir");
        }
        std::fs::create_dir_all(root.join("nested/deep")).expect("mkdir");
        for name in ["hello", "open", "probe", ".hidden"] {
            std::fs::write(root.join(name).join("index.ts"), "Deno.serve(() => {})")
                .expect("write");
        }
        std::fs::write(root.join("_shared/util.ts"), "export const x = 1").expect("write");
        std::fs::write(root.join("noindex/other.ts"), "export const y = 2").expect("write");
        std::fs::write(root.join("nested/deep/index.ts"), "Deno.serve(() => {})").expect("write");
        std::fs::write(root.join("jsfn.js"), "Deno.serve(() => {})").expect("write");
        dir
    }

    #[test]
    fn only_the_directories_upstream_serves_are_served() {
        let dir = project();
        let found = read(dir.path(), &Layout::default()).expect("read");
        let names: Vec<&str> = found.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["hello", "open", "probe"]);
        assert_eq!(
            found[0].entrypoint,
            dir.path().join("functions/hello/index.ts")
        );
        assert!(
            found.iter().all(|f| f.verify_jwt),
            "a function that configures nothing is verified"
        );
    }

    #[test]
    fn a_function_switched_off_is_not_there_at_all() {
        let dir = project();
        let mut layout = Layout::default();
        layout.settings.insert(
            "hello".to_string(),
            Settings {
                enabled: false,
                ..Settings::default()
            },
        );
        let found = read(dir.path(), &layout).expect("read");
        let names: Vec<&str> = found.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["open", "probe"], "skipped rather than refused");
    }

    #[test]
    fn verify_jwt_is_per_function() {
        let dir = project();
        let mut layout = Layout::default();
        layout.settings.insert(
            "open".to_string(),
            Settings {
                verify_jwt: false,
                ..Settings::default()
            },
        );
        let found = read(dir.path(), &layout).expect("read");
        let open = found.iter().find(|f| f.name == "open").expect("served");
        assert!(!open.verify_jwt);
        let hello = found.iter().find(|f| f.name == "hello").expect("served");
        assert!(hello.verify_jwt, "one block does not move the others");
    }

    #[test]
    fn an_entrypoint_and_an_import_map_are_relative_to_the_config_file() {
        let dir = project();
        std::fs::write(
            dir.path().join("functions/probe/main.ts"),
            "Deno.serve(() => {})",
        )
        .expect("write");
        let mut layout = Layout::default();
        layout.settings.insert(
            "probe".to_string(),
            Settings {
                entrypoint: Some("./functions/probe/main.ts".to_string()),
                import_map: Some("./functions/import_map.json".to_string()),
                static_files: vec!["./functions/probe/*.html".to_string()],
                ..Settings::default()
            },
        );
        let found = read(dir.path(), &layout).expect("read");
        let probe = found.iter().find(|f| f.name == "probe").expect("served");
        assert_eq!(
            probe.entrypoint,
            dir.path().join("./functions/probe/main.ts")
        );
        assert_eq!(
            probe.import_map,
            Some(dir.path().join("./functions/import_map.json"))
        );
        assert_eq!(
            probe.static_files,
            vec![dir.path().join("./functions/probe/*.html")]
        );
    }

    /// The order `GetFunctionConfig` looks in, one file at a time, most
    /// specific first. Each round writes the next place down and the
    /// answer has to stay where it was.
    #[test]
    fn a_function_finds_the_map_beside_it_without_being_told() {
        let dir = project();
        let root = dir.path().join("functions");
        let map = |dir: &TempDir| {
            read(dir.path(), &Layout::default())
                .expect("read")
                .into_iter()
                .find(|f| f.name == "hello")
                .expect("served")
                .import_map
        };
        assert_eq!(map(&dir), None, "a function with nothing beside it");

        std::fs::write(root.join("import_map.json"), "{}").expect("write");
        assert_eq!(
            map(&dir),
            Some(root.join("import_map.json")),
            "the project's own is the last resort"
        );

        std::fs::write(root.join("hello/import_map.json"), "{}").expect("write");
        assert_eq!(
            map(&dir),
            Some(root.join("hello/import_map.json")),
            "one beside the function beats the project's"
        );

        std::fs::write(root.join("hello/deno.jsonc"), "{}").expect("write");
        assert_eq!(map(&dir), Some(root.join("hello/deno.jsonc")));

        std::fs::write(root.join("hello/deno.json"), "{}").expect("write");
        assert_eq!(
            map(&dir),
            Some(root.join("hello/deno.json")),
            "and deno.json beats everything, which is the one upstream tells projects to write"
        );
    }

    #[test]
    fn the_config_file_beats_anything_found_beside_the_function() {
        let dir = project();
        std::fs::write(dir.path().join("functions/hello/deno.json"), "{}").expect("write");
        let mut layout = Layout::default();
        layout.settings.insert(
            "hello".to_string(),
            Settings {
                import_map: Some("./functions/named.json".to_string()),
                ..Settings::default()
            },
        );
        let found = read(dir.path(), &layout).expect("read");
        let hello = found.iter().find(|f| f.name == "hello").expect("served");
        assert_eq!(
            hello.import_map,
            Some(dir.path().join("./functions/named.json")),
            "a project that named one is not overruled by a file that happens to be there"
        );
    }

    #[test]
    fn an_entrypoint_that_is_not_there_is_not_served() {
        let dir = project();
        let mut layout = Layout::default();
        layout.settings.insert(
            "hello".to_string(),
            Settings {
                entrypoint: Some("./functions/hello/nowhere.ts".to_string()),
                ..Settings::default()
            },
        );
        let found = read(dir.path(), &layout).expect("read");
        let names: Vec<&str> = found.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["open", "probe"]);
    }

    #[test]
    fn a_project_with_no_functions_directory_has_no_functions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let found = read(dir.path(), &Layout::default()).expect("read");
        assert!(found.is_empty(), "not an error, just a project without any");
    }
}
