//! One version number, and every manifest that has to carry it.
//!
//! Nothing in this tree authors a version. The tag is the version, and
//! `scripts/zou-version.sh` stamps it over the placeholder in each job
//! that builds or publishes something. That was not always the shape:
//! the release used to carry a sed of its own in three places and miss
//! two, so tagging v1.0.0 would have published `zou-cli` at 1.0.0 around
//! a binary answering `zou --version` with `zou 0.0.1`, and reporting
//! 0.0.1 on `/auth/v1/health`, which is one of the four differences from
//! GoTrue this project defends on purpose.
//!
//! Nothing caught that, and nothing could have: a manifest nobody stamps
//! builds, installs and runs. It only says the wrong number. So the list
//! of manifests is data rather than five seds, and these are the rules
//! over it.

use std::collections::BTreeSet;
use std::path::Path;

mod common;
use common::{text, tracked};

/// The list the script reads, which is the list this checks.
const LIST: &str = "packaging/versioned-manifests.txt";

/// The one manifest with a version that goes nowhere. `fuzz` is outside
/// the workspace and is published to nothing, so its number means only
/// what cargo needs it to mean. It is named here rather than filtered by
/// a rule, because a rule that skipped it would skip the next one too.
const NOT_PUBLISHED: &[&str] = &["fuzz/Cargo.toml"];

/// Where a version is written in the release, which is one place now.
const WORKFLOW: &str = ".github/workflows/release.yml";
const STAMP: &str = "scripts/zou-version.sh";

/// The version a manifest carries, in the one shape the script rewrites.
/// A manifest that writes it some other way is a manifest the script
/// would silently leave alone, so a manifest that carries its version in
/// some other shape reads as having none, and the first rule below fails
/// on it the moment it is listed.
fn version_in(path: &Path, body: &str) -> Option<String> {
    let prefix = match path.file_name().and_then(|name| name.to_str())? {
        "Cargo.toml" | "pyproject.toml" => "version = \"",
        "package.json" => "  \"version\": \"",
        _ => return None,
    };
    body.lines()
        .find_map(|line| line.strip_prefix(prefix))
        .and_then(|rest| rest.split_once('"'))
        .map(|(version, _)| version.to_string())
}

/// What the script will stamp, read out of the same file it reads.
fn listed() -> BTreeSet<String> {
    let body = text(Path::new(LIST)).unwrap_or_else(|| panic!("{LIST} is in the repository"));
    body.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_string)
        .collect()
}

#[test]
fn every_manifest_with_a_version_is_one_the_release_stamps() {
    let found: BTreeSet<String> = tracked()
        .into_iter()
        .filter(|path| {
            text(path)
                .and_then(|body| version_in(path, &body))
                .is_some()
        })
        .map(|path| path.to_string_lossy().replace('\\', "/"))
        .collect();
    let stamped = listed();
    let known: BTreeSet<String> = stamped
        .iter()
        .cloned()
        .chain(NOT_PUBLISHED.iter().map(|path| path.to_string()))
        .collect();

    for path in &found {
        assert!(
            known.contains(path),
            "{path} carries a version and nothing stamps it, so a release would publish it at the placeholder committed here. Add it to {LIST}, or to NOT_PUBLISHED in this file if it really goes nowhere."
        );
    }
    for path in &stamped {
        assert!(
            found.contains(path),
            "{LIST} names {path}, and there is no version in it for the script to rewrite. Either the file moved, or the line it wrote the version on changed shape and the script has been quietly matching nothing."
        );
    }
}

#[test]
fn the_placeholder_is_one_number_and_not_five() {
    let mut carried: BTreeSet<(String, String)> = BTreeSet::new();
    for path in listed() {
        let body = text(Path::new(&path)).unwrap_or_else(|| panic!("{path} is in the repository"));
        let version = version_in(Path::new(&path), &body)
            .unwrap_or_else(|| panic!("{path} has a version in it"));
        carried.insert((version, path));
    }
    let versions: BTreeSet<&String> = carried.iter().map(|(version, _)| version).collect();
    assert!(
        versions.len() == 1,
        "the manifests are not all on one number: {carried:?}. Nothing here is authored, so two different numbers means somebody bumped one by hand, and a release will publish whichever the tag happened to reach."
    );
}

#[test]
fn the_release_writes_a_version_in_one_place_and_that_place_is_the_script() {
    let body =
        text(Path::new(WORKFLOW)).unwrap_or_else(|| panic!("{WORKFLOW} is in the repository"));
    assert!(
        body.contains(STAMP),
        "{WORKFLOW} does not call {STAMP}, so nothing puts the tag into the manifests"
    );
    for (index, line) in body.lines().enumerate() {
        if line.contains(STAMP) {
            continue;
        }
        let writes_a_version = line.contains("npm version ")
            || (line.contains("sed") && line.contains("version = "))
            || (line.contains("sed") && line.contains("\"version\""));
        assert!(
            !writes_a_version,
            "{WORKFLOW}:{}\n{line}\nputs a version into a manifest itself. That is how the release came to stamp three of the five and miss two: each job had its own line and nobody could see the set. {STAMP} is the one place, and it reads {LIST}.",
            index + 1
        );
    }
}
