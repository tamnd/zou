//! The import map a function's bare specifiers go through.
//!
//! `import { createClient } from "@supabase/supabase-js"` is not a
//! module specifier v8 or anything else can resolve on its own. It is a
//! name, and the file that says what the name means is the function's
//! `deno.json`, which is what `supabase functions new` writes beside
//! every new function:
//!
//! ```json
//! {
//!   "imports": {
//!     "@supabase/functions-js": "jsr:@supabase/functions-js@^2",
//!     "@supabase/server": "npm:@supabase/server@^1"
//!   }
//! }
//! ```
//!
//! Which file that is, for a function that named none, is decided one
//! layer up in `zou-functions`, in the order the pinned CLI's
//! `GetFunctionConfig` looks. What arrives here is a path.
//!
//! # The file
//!
//! Two shapes, both of them upstream's. A `deno.json` holds the map
//! inline under `imports` and `scopes`, and a plain `import_map.json`
//! is the same two keys at the top of the file, so one parser reads
//! both. A `deno.json` may instead be a pointer: `{"importMap":
//! "./somewhere.json"}` with no `imports` and no `scopes` of its own is
//! a reference to another file, which is `IsReference` in the CLI and
//! is followed exactly one step, the same as there.
//!
//! Both are read as JSONC, because `deno.jsonc` exists and the CLI
//! parses every one of these through `tidwall/jsonc` whatever it is
//! called. Comments and a trailing comma are therefore not errors.
//!
//! An address is resolved against the file it was written in, so
//! `"./lib/"` in `functions/hello/deno.json` is
//! `functions/hello/lib/`, and a url or a bare specifier like
//! `npm:zod@3` is left alone for the loader behind this to deal with.
//!
//! # The resolution
//!
//! This is the import maps specification and not a prefix substitution,
//! because the difference shows up on the first realistic map. Keys are
//! tried longest first, a key that ends in `/` matches a prefix and
//! whatever followed it is appended to the address, and a `scopes`
//! entry is consulted before the top level map when the file doing the
//! importing is inside that scope. A key ending in `/` whose address
//! does not is dropped when the file is read, which is what the
//! specification says to do with it.
//!
//! What is deliberately not here is a fallback for a bare specifier
//! nothing matched. Deno refuses those and so does this, because a name
//! nobody defined resolving to something is how a function ends up
//! running a package it did not ask for.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use deno_core::ModuleSpecifier;

/// One import map, ready to answer questions.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Imports {
    /// The top level map, keys as written and addresses resolved.
    imports: Map,
    /// `scopes`, by the prefix they apply to, resolved the same way.
    scopes: BTreeMap<String, Map>,
    /// The files this was built out of, for hot reload: editing the map
    /// is editing the function.
    pub sources: Vec<PathBuf>,
}

/// A set of entries, longest key first, which is the order the
/// specification says to try them in.
type Map = Vec<(String, String)>;

impl Imports {
    /// The map at `path`, with one `importMap` reference followed.
    pub fn read(path: &Path) -> Result<Imports, String> {
        let mut out = Imports::default();
        let (map, source) = one(path)?;
        out.sources.push(source);
        let map = match reference(&map) {
            // A `deno.json` that only points at another file is that
            // other file, resolved against the pointer's own directory.
            Some(at) => {
                let next = path.parent().unwrap_or(Path::new(".")).join(at);
                let (map, source) = one(&next)?;
                out.sources.push(source);
                map
            }
            None => map,
        };
        let base = base_of(path)?;
        out.imports = entries(map.get("imports"), &base);
        if let Some(serde_json::Value::Object(scopes)) = map.get("scopes") {
            for (prefix, inner) in scopes {
                // A scope's own key is a url too, so a relative one is
                // a directory of this project rather than a name.
                let Ok(at) = base.join(prefix) else { continue };
                out.scopes
                    .insert(at.to_string(), entries(Some(inner), &base));
            }
        }
        Ok(out)
    }

    /// What `specifier` means to the file at `referrer`, or None for a
    /// specifier this map says nothing about.
    pub fn resolve(&self, specifier: &str, referrer: &str) -> Option<String> {
        // The innermost scope that contains the importer wins, and
        // `sorted by length, descending` is what innermost means to the
        // specification.
        let mut scopes: Vec<&String> = self
            .scopes
            .keys()
            .filter(|prefix| referrer.starts_with(prefix.as_str()))
            .collect();
        scopes.sort_by_key(|prefix| std::cmp::Reverse(prefix.len()));
        for prefix in scopes {
            if let Some(found) = matched(&self.scopes[prefix], specifier) {
                return Some(found);
            }
        }
        matched(&self.imports, specifier)
    }
}

/// The first entry of `map` that covers `specifier`.
fn matched(map: &Map, specifier: &str) -> Option<String> {
    for (key, address) in map {
        if key == specifier {
            return Some(address.clone());
        }
        // A key ending in `/` is a prefix and the rest of the specifier
        // comes along, which is how `"@std/": "jsr:/@std/"` covers
        // every module under it without an entry each.
        if key.ends_with('/')
            && let Some(rest) = specifier.strip_prefix(key.as_str())
        {
            return Some(format!("{address}{rest}"));
        }
    }
    None
}

/// One file, parsed, and the path it was read from.
fn one(path: &Path) -> Result<(serde_json::Map<String, serde_json::Value>, PathBuf), String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("read the import map {}: {e}", path.display()))?;
    let value: serde_json::Value = serde_json::from_str(&plain(&text))
        .map_err(|e| format!("the import map {} is not json: {e}", path.display()))?;
    match value {
        serde_json::Value::Object(map) => Ok((map, path.to_path_buf())),
        _ => Err(format!(
            "the import map {} is not an object",
            path.display()
        )),
    }
}

/// The file this one points at, when pointing at another file is all it
/// does. Upstream's `IsReference`: an `importMap` and neither of the
/// two keys that would make it a map in its own right.
fn reference(map: &serde_json::Map<String, serde_json::Value>) -> Option<&str> {
    let has = |name: &str| {
        map.get(name)
            .and_then(serde_json::Value::as_object)
            .is_some_and(|it| !it.is_empty())
    };
    if has("imports") || has("scopes") {
        return None;
    }
    map.get("importMap")?.as_str().filter(|it| !it.is_empty())
}

/// Where relative addresses in this file point from.
fn base_of(path: &Path) -> Result<ModuleSpecifier, String> {
    let at = std::path::absolute(path).map_err(|e| format!("{}: {e}", path.display()))?;
    ModuleSpecifier::from_file_path(&at)
        .map_err(|()| format!("{} is not a path a url can be made of", at.display()))
}

/// One `imports` or one scope's worth of them, with the addresses
/// resolved and the entries the specification throws away thrown away.
fn entries(from: Option<&serde_json::Value>, base: &ModuleSpecifier) -> Map {
    let Some(serde_json::Value::Object(table)) = from else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (key, value) in table {
        let Some(address) = value.as_str() else {
            continue;
        };
        if key.is_empty() || address.is_empty() {
            continue;
        }
        // A key that is a prefix must map to one, or the specification
        // says to drop the entry rather than build a specifier that
        // runs two names together.
        if key.ends_with('/') != address.ends_with('/') {
            log::warn!(
                "the import map entry {key} to {address} is dropped, because only one of the two ends in a slash"
            );
            continue;
        }
        out.push((key.clone(), resolved(address, base)));
    }
    out.sort_by_key(|(key, _)| std::cmp::Reverse(key.len()));
    out
}

/// An address as the loader behind this will see it.
///
/// A relative one is a file of this project and becomes a url now,
/// while `npm:`, `jsr:` and an ordinary `https:` are left exactly as
/// they were written, because the loader already knows what those mean
/// and turning them into urls here would be doing its job badly.
fn resolved(address: &str, base: &ModuleSpecifier) -> String {
    if address.starts_with("./") || address.starts_with("../") || address.starts_with('/') {
        return match base.join(address) {
            Ok(url) => url.to_string(),
            Err(_) => address.to_string(),
        };
    }
    address.to_string()
}

/// JSONC as json: comments gone and a trailing comma gone.
///
/// The CLI runs every one of these files through `tidwall/jsonc`
/// whatever it is named, so a comment in an `import_map.json` is as
/// legal as one in a `deno.jsonc`, and a parser that refused it would
/// be stricter than the thing it is copying.
fn plain(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let src: Vec<char> = text.chars().collect();
    let mut at = 0;
    let mut inside = false;
    while at < src.len() {
        let c = src[at];
        if inside {
            out.push(c);
            if c == '\\' && at + 1 < src.len() {
                out.push(src[at + 1]);
                at += 2;
                continue;
            }
            if c == '"' {
                inside = false;
            }
            at += 1;
            continue;
        }
        match (c, src.get(at + 1)) {
            ('/', Some('/')) => {
                while at < src.len() && src[at] != '\n' {
                    at += 1;
                }
            }
            ('/', Some('*')) => {
                at += 2;
                while at + 1 < src.len() && !(src[at] == '*' && src[at + 1] == '/') {
                    at += 1;
                }
                at = (at + 2).min(src.len());
            }
            (',', _) => {
                // A comma is trailing when the next thing that is not
                // whitespace or a comment closes what it is in.
                let mut peek = at + 1;
                loop {
                    while peek < src.len() && src[peek].is_whitespace() {
                        peek += 1;
                    }
                    match (src.get(peek), src.get(peek + 1)) {
                        (Some('/'), Some('/')) => {
                            while peek < src.len() && src[peek] != '\n' {
                                peek += 1;
                            }
                        }
                        (Some('/'), Some('*')) => {
                            peek += 2;
                            while peek + 1 < src.len()
                                && !(src[peek] == '*' && src[peek + 1] == '/')
                            {
                                peek += 1;
                            }
                            peek = (peek + 2).min(src.len());
                        }
                        _ => break,
                    }
                }
                if !matches!(src.get(peek), Some('}' | ']')) {
                    out.push(',');
                }
                at += 1;
            }
            ('"', _) => {
                inside = true;
                out.push(c);
                at += 1;
            }
            _ => {
                out.push(c);
                at += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn written(dir: &Path, name: &str, text: &str) -> PathBuf {
        let at = dir.join(name);
        if let Some(parent) = at.parent() {
            std::fs::create_dir_all(parent).expect("mkdir");
        }
        std::fs::write(&at, text).expect("write");
        at
    }

    fn map(text: &str) -> (tempfile::TempDir, Imports) {
        let dir = tempfile::tempdir().expect("tempdir");
        let at = written(dir.path(), "functions/hello/deno.json", text);
        let read = Imports::read(&at).expect("read");
        (dir, read)
    }

    /// The url of a file in the function's own directory, which is what
    /// a referrer is in every one of these.
    fn from(dir: &Path, name: &str) -> String {
        ModuleSpecifier::from_file_path(dir.join("functions/hello").join(name))
            .expect("a url")
            .to_string()
    }

    #[test]
    fn a_name_means_what_the_file_says_it_means() {
        let (dir, imports) =
            map(r#"{"imports": {"@supabase/supabase-js": "npm:@supabase/supabase-js@2"}}"#);
        assert_eq!(
            imports.resolve("@supabase/supabase-js", &from(dir.path(), "index.ts")),
            Some("npm:@supabase/supabase-js@2".to_string())
        );
        assert_eq!(
            imports.resolve("@supabase/something-else", &from(dir.path(), "index.ts")),
            None,
            "a name nobody defined means nothing rather than something"
        );
    }

    #[test]
    fn a_key_that_ends_in_a_slash_is_a_prefix() {
        let (dir, imports) =
            map(r#"{"imports": {"@std/": "jsr:/@std/", "@std/encoding": "jsr:@std/encoding@1"}}"#);
        let at = from(dir.path(), "index.ts");
        assert_eq!(
            imports.resolve("@std/encoding/hex", &at),
            Some("jsr:/@std/encoding/hex".to_string())
        );
        assert_eq!(
            imports.resolve("@std/encoding", &at),
            Some("jsr:@std/encoding@1".to_string()),
            "the longer key wins, which is what makes an exception to a prefix possible"
        );
    }

    #[test]
    fn a_relative_address_is_relative_to_the_file_it_was_written_in() {
        let (dir, imports) = map(r#"{"imports": {"util": "./lib/util.ts", "lib/": "./lib/"}}"#);
        let at = from(dir.path(), "index.ts");
        let expected =
            ModuleSpecifier::from_file_path(dir.path().join("functions/hello/lib/util.ts"))
                .expect("a url")
                .to_string();
        assert_eq!(imports.resolve("util", &at), Some(expected.clone()));
        assert_eq!(imports.resolve("lib/util.ts", &at), Some(expected));
    }

    #[test]
    fn a_url_address_is_left_exactly_as_it_was_written() {
        let (dir, imports) = map(
            r#"{"imports": {"zod": "npm:zod@3.23.8", "std": "jsr:@std/encoding@1", "cdn": "https://esm.sh/preact@10"}}"#,
        );
        let at = from(dir.path(), "index.ts");
        assert_eq!(
            imports.resolve("zod", &at),
            Some("npm:zod@3.23.8".to_string())
        );
        assert_eq!(
            imports.resolve("std", &at),
            Some("jsr:@std/encoding@1".to_string())
        );
        assert_eq!(
            imports.resolve("cdn", &at),
            Some("https://esm.sh/preact@10".to_string())
        );
    }

    #[test]
    fn a_scope_beats_the_map_above_it_for_the_files_inside_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let at = written(
            dir.path(),
            "functions/hello/deno.json",
            r#"{
                "imports": {"zod": "npm:zod@3"},
                "scopes": {"./legacy/": {"zod": "npm:zod@2"}}
            }"#,
        );
        let imports = Imports::read(&at).expect("read");
        assert_eq!(
            imports.resolve("zod", &from(dir.path(), "index.ts")),
            Some("npm:zod@3".to_string())
        );
        assert_eq!(
            imports.resolve("zod", &from(dir.path(), "legacy/old.ts")),
            Some("npm:zod@2".to_string()),
            "the same name, a different answer, because of who is asking"
        );
    }

    #[test]
    fn a_deno_json_that_only_points_at_another_file_is_that_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        written(
            dir.path(),
            "functions/hello/import_map.json",
            r#"{"imports": {"zod": "npm:zod@3"}}"#,
        );
        let at = written(
            dir.path(),
            "functions/hello/deno.json",
            r#"{"importMap": "./import_map.json"}"#,
        );
        let imports = Imports::read(&at).expect("read");
        assert_eq!(
            imports.resolve("zod", &from(dir.path(), "index.ts")),
            Some("npm:zod@3".to_string())
        );
        assert_eq!(
            imports.sources.len(),
            2,
            "both files are what it was built out of, so editing either reloads it"
        );
    }

    #[test]
    fn a_deno_json_with_a_map_of_its_own_does_not_follow_the_pointer() {
        let dir = tempfile::tempdir().expect("tempdir");
        written(
            dir.path(),
            "functions/hello/other.json",
            r#"{"imports": {"zod": "npm:zod@2"}}"#,
        );
        let at = written(
            dir.path(),
            "functions/hello/deno.json",
            r#"{"importMap": "./other.json", "imports": {"zod": "npm:zod@3"}}"#,
        );
        let imports = Imports::read(&at).expect("read");
        assert_eq!(
            imports.resolve("zod", &from(dir.path(), "index.ts")),
            Some("npm:zod@3".to_string())
        );
    }

    #[test]
    fn an_entry_where_only_one_side_ends_in_a_slash_is_dropped() {
        let (dir, imports) = map(r#"{"imports": {"lib/": "./lib", "other": "./other/"}}"#);
        let at = from(dir.path(), "index.ts");
        assert_eq!(imports.resolve("lib/thing.ts", &at), None);
        assert_eq!(imports.resolve("other", &at), None);
    }

    #[test]
    fn comments_and_a_trailing_comma_are_not_errors() {
        let (dir, imports) = map(r#"{
                // the client, pinned
                "imports": {
                    "zod": "npm:zod@3", /* and nothing else */
                },
            }"#);
        assert_eq!(
            imports.resolve("zod", &from(dir.path(), "index.ts")),
            Some("npm:zod@3".to_string())
        );
    }

    #[test]
    fn a_comma_inside_a_string_is_part_of_the_string() {
        let (dir, imports) = map(r#"{"imports": {"a,b": "npm:one@1", "slash": "npm:two@2"}}"#);
        let at = from(dir.path(), "index.ts");
        assert_eq!(imports.resolve("a,b", &at), Some("npm:one@1".to_string()));
        assert_eq!(imports.resolve("slash", &at), Some("npm:two@2".to_string()));
    }

    #[test]
    fn a_file_that_is_not_json_says_which_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let at = written(dir.path(), "functions/hello/deno.json", "{\"imports\":");
        let why = Imports::read(&at).expect_err("not json");
        assert!(why.contains("deno.json"), "{why}");
        let missing = dir.path().join("functions/hello/nowhere.json");
        let why = Imports::read(&missing).expect_err("not there");
        assert!(why.contains("nowhere.json"), "{why}");
    }

    #[test]
    fn an_empty_map_is_a_map_that_answers_nothing() {
        let (dir, imports) = map("{}");
        assert_eq!(imports.resolve("zod", &from(dir.path(), "index.ts")), None);
        assert_eq!(imports.sources.len(), 1);
    }
}
