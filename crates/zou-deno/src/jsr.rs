//! A `jsr:` specifier as the files jsr publishes.
//!
//! jsr is easier than npm in the way that matters here. A package is
//! typescript modules and nothing else: no commonjs, no build output,
//! no `node_modules`. And a package's own dependencies are written into
//! its source as `jsr:` and `npm:` specifiers rather than declared in a
//! manifest somewhere, so there is no tree to walk between packages.
//! What is left is one question, asked twice over http: which version
//! does this range mean, and which file does this subpath mean.
//!
//! The answer to both is a url on jsr, and from there the module is an
//! ordinary `https:` import that the loader already fetches, caches and
//! transpiles. So this is a lookup rather than a second module system.

// The caller is the loader, under the same knob npm's tarballs are.
#![allow(dead_code)]

use serde_json::Value;

use crate::npm::{Range, Version};

/// Where a package is looked up, overridable with `ZOU_JSR_REGISTRY` for
/// the same reason npm's is: a project that will not reach the public
/// one should be able to name its own.
const REGISTRY: &str = "https://jsr.io";

fn registry() -> String {
    std::env::var("ZOU_JSR_REGISTRY")
        .ok()
        .filter(|it| !it.is_empty())
        .unwrap_or_else(|| REGISTRY.to_string())
        .trim_end_matches('/')
        .to_string()
}

/// The url of the file a specifier means, with the `jsr:` already taken
/// off it.
pub(crate) fn entry(rest: &str) -> Result<String, String> {
    let (name, want, sub) = parts(rest);
    if !name.starts_with('@') || !name.contains('/') {
        return Err(format!(
            "{name} is not a jsr package: a jsr package is a scope and a name, as in @std/encoding"
        ));
    }
    let registry = registry();
    let listed = ask(&format!("{registry}/{name}/meta.json"))?;
    let version =
        pick(&listed, &want).ok_or_else(|| format!("{name} has no version that {want} allows"))?;
    let described = ask(&format!("{registry}/{name}/{version}_meta.json"))?;
    let file = exported(&described, &sub).ok_or_else(|| {
        format!("{name}@{version} does not export {sub}, so there is no file to load")
    })?;
    Ok(format!(
        "{registry}/{name}/{version}/{}",
        file.trim_start_matches("./")
    ))
}

/// One document off the registry, as json.
///
/// Uncached, because which version a range means is a thing that
/// changes when somebody publishes, and the module cache keeps what it
/// fetched forever. The file this ends up naming is cached, and that is
/// the one worth keeping: a version's files do not change.
fn ask(url: &str) -> Result<Value, String> {
    let body = crate::module::fetched(url, Some("application/json"))?;
    serde_json::from_slice(&body).map_err(|e| format!("{url}: {e}"))
}

/// The version a range means, or what the registry calls latest when
/// nothing was asked for.
///
/// A yanked version is one the publisher took back, and it is still
/// served so that a lockfile naming it keeps working. A range does not
/// reach for one, which is the same rule npm has for a version that was
/// unpublished.
fn pick(listed: &Value, want: &str) -> Option<String> {
    let latest = listed.get("latest").and_then(|it| it.as_str());
    // Nothing asked for is the registry's own answer rather than the
    // highest number in the list, because latest is a thing a publisher
    // sets and the highest number is not always what they set it to.
    if want.trim().is_empty()
        && let Some(latest) = latest
    {
        return Some(latest.to_string());
    }
    let versions = listed.get("versions")?.as_object()?;
    let range = Range::parse(want)?;
    let allowed: Vec<Version> = versions
        .iter()
        .filter(|(_, about)| about.get("yanked") != Some(&Value::Bool(true)))
        .filter_map(|(said, _)| Version::parse(said))
        .collect();
    range.best(allowed.iter()).map(|it| it.to_string())
}

/// The file a subpath means, out of the package's own `exports`.
///
/// jsr's map is a flat one: every subpath a package publishes is a key
/// with a file next to it, so there is no pattern matching and no
/// conditions in it, which is the whole of the difference from npm's.
fn exported(described: &Value, sub: &str) -> Option<String> {
    let exports = described.get("exports")?;
    if let Some(said) = exports.as_str() {
        return match sub == "." {
            true => Some(said.to_string()),
            false => None,
        };
    }
    exports
        .get(sub)
        .and_then(|it| it.as_str())
        .map(String::from)
}

/// What is written after `jsr:`: the package, the range asked of it,
/// and the subpath into it.
fn parts(rest: &str) -> (String, String, String) {
    let rest = rest.trim_start_matches('/');
    let mut segments = rest.split('/');
    let mut named = segments.next().unwrap_or_default().to_string();
    if let Some(next) = segments.next() {
        named = format!("{named}/{next}");
    }
    let sub: Vec<&str> = segments.collect();
    let (name, want) = match named.rfind('@') {
        Some(cut) if cut > 0 => (named[..cut].to_string(), named[cut + 1..].to_string()),
        _ => (named, String::new()),
    };
    let sub = match sub.is_empty() {
        true => ".".to_string(),
        false => format!("./{}", sub.join("/")),
    };
    (name, want, sub)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn listed() -> Value {
        serde_json::from_str(include_str!("../tests/fixtures/jsr-meta-std-encoding.json"))
            .expect("the registry's own answer")
    }

    fn described() -> Value {
        serde_json::from_str(include_str!(
            "../tests/fixtures/jsr-version-std-encoding.json"
        ))
        .expect("the registry's own answer")
    }

    #[test]
    fn a_specifier_is_a_package_a_range_and_a_subpath() {
        assert_eq!(
            parts("@std/encoding@1/hex"),
            ("@std/encoding".into(), "1".into(), "./hex".into())
        );
        assert_eq!(
            parts("/@std/encoding@^1.0.0"),
            ("@std/encoding".into(), "^1.0.0".into(), ".".into())
        );
        assert_eq!(
            parts("@luca/flag"),
            ("@luca/flag".into(), String::new(), ".".into())
        );
    }

    /// Against the registry's own answer for `@std/encoding`, recorded
    /// rather than written, so what is being tested is the reading of
    /// the document jsr actually serves.
    #[test]
    fn the_version_a_range_means_comes_out_of_the_registrys_own_list() {
        let listed = listed();
        assert_eq!(pick(&listed, "1.0.5").as_deref(), Some("1.0.5"));
        assert_eq!(pick(&listed, "^1.0.0").as_deref(), Some("1.0.11"));
        assert_eq!(pick(&listed, "~1.0.5").as_deref(), Some("1.0.11"));
        assert_eq!(pick(&listed, "0.224.x").as_deref(), Some("0.224.3"));
        // Nothing asked for is what the registry calls latest, which is
        // its own answer and not the highest number this can see.
        assert_eq!(pick(&listed, "").as_deref(), Some("1.0.11"));
        // And a range nothing satisfies is nothing, rather than latest
        // handed over as if it had been asked for.
        assert_eq!(pick(&listed, "^9"), None);
    }

    #[test]
    fn the_file_a_subpath_means_comes_out_of_the_packages_own_exports() {
        let described = described();
        assert_eq!(exported(&described, ".").as_deref(), Some("./mod.ts"));
        assert_eq!(exported(&described, "./hex").as_deref(), Some("./hex.ts"));
        // A subpath a package does not publish is nothing, since jsr's
        // map is the whole list of what a package exports.
        assert_eq!(exported(&described, "./private"), None);
    }

    /// A package that exports one file names it as a string rather than
    /// as a map, which is a shape jsr allows and a small package uses.
    #[test]
    fn a_package_that_exports_one_file_says_so_as_a_string() {
        let described = serde_json::json!({ "exports": "./mod.ts" });
        assert_eq!(exported(&described, ".").as_deref(), Some("./mod.ts"));
        assert_eq!(exported(&described, "./anything"), None);
    }

    #[test]
    fn a_package_that_is_not_a_scope_and_a_name_is_refused_by_name() {
        let refused = entry("encoding@1").expect_err("not a jsr package");
        assert!(refused.contains("scope and a name"), "{refused}");
    }

    /// The registry itself, which is the only way to know that the two
    /// documents are still the shape this reads. Ignored because it
    /// wants the network.
    #[test]
    #[ignore]
    fn a_real_package_is_a_url_on_the_registry() {
        let url = entry("@std/encoding@^1/hex").expect("a file");
        assert!(url.starts_with("https://jsr.io/@std/encoding/1."), "{url}");
        assert!(url.ends_with("/hex.ts"), "{url}");
    }
}
