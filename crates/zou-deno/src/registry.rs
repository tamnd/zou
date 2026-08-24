//! Asking npm what a package is, and getting the version that was
//! asked for onto a disk.
//!
//! A registry answers three things and this needs all three. What
//! versions exist and what the tags point at is the packument, a
//! document at the package's own name. Where a version's tarball is and
//! what it should hash to is in that same document, under the version.
//! The tarball is the package.
//!
//! The digest is checked before anything is unpacked, and a tarball
//! that does not match it is thrown away rather than written down. What
//! that is worth is not much against a registry that lied, since the
//! digest came from the same registry as the bytes, and is worth a
//! great deal against everything else: a proxy in the middle, a mirror
//! with a corrupt disk, a download that stopped early and a cache that
//! kept the half of it.
//!
//! A package is unpacked under its name and version, and never written
//! over: npm does not let a published version change, so a directory
//! that is already there is already right. What is written is written
//! beside and moved, so two isolates fetching the same package at the
//! same time is the ordinary case rather than a race.
//!
//! Nothing here resolves a specifier to a file. That is node module
//! resolution, and it is #596's next piece; this is the part that
//! turns a name and a range into a directory for it to walk.

// The caller is that next piece.
#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

use base64ct::Encoding;
use sha2::Digest;

use crate::npm::{Range, Version};

/// Where a `npm:` package is fetched from, and the real registry rather
/// than a builder of modules, since what is wanted here is what the
/// author published. Overridable with `ZOU_NPM_REGISTRY` for a mirror
/// or for a private registry.
const NPM: &str = "https://registry.npmjs.org";

/// What a version's `dist` says: where the tarball is, and what it
/// hashes to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Dist {
    pub tarball: String,
    /// The modern one, `sha512-` and base64. Every version published in
    /// years has it.
    pub integrity: Option<String>,
    /// The old one, sha1 in hex, which is what a version published in
    /// 2013 had before npm went back and computed the other for it.
    pub shasum: Option<String>,
}

/// Which version of a package the range meant, and where to get it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Chosen {
    pub version: Version,
    pub dist: Dist,
}

/// The package a name and a range mean, unpacked, as the directory it
/// was unpacked into.
///
/// `want` is a range, a single version, or a dist tag: `^2.1.0`,
/// `2.1.3` and `latest` are all things a specifier says, and only the
/// registry knows what the third one points at today.
pub(crate) fn package(name: &str, want: &str, cache: &Path) -> Result<PathBuf, String> {
    let packument = ask(&at(&registry(), name))?;
    let chosen = pick(&packument, want).map_err(|why| format!("{name}@{want}: {why}"))?;
    let into = cache
        .join("npm")
        .join(name)
        .join(chosen.version.to_string());
    if into.join("package.json").is_file() {
        return Ok(into);
    }
    let tarball = bytes(&chosen.dist.tarball)?;
    check(&tarball, &chosen.dist).map_err(|why| format!("{name}@{}: {why}", chosen.version))?;
    put(&tarball, &into)
        .map_err(|e| format!("{name}@{} could not be unpacked: {e}", chosen.version))?;
    Ok(into)
}

/// The registry this asks, which is npm unless the environment named
/// somebody else.
fn registry() -> String {
    std::env::var("ZOU_NPM_REGISTRY")
        .ok()
        .filter(|it| !it.trim().is_empty())
        .map(|it| it.trim_end_matches('/').to_string())
        .unwrap_or_else(|| NPM.to_string())
}

/// The url a package's packument is at.
///
/// A scoped name has a slash in it and the slash is part of the name
/// rather than part of the path, so it is escaped, which is what npm's
/// own client does and what the registry expects. Lowercase `%2f`,
/// because a couple of registries in the wild are particular about it
/// and npm sends it that way.
fn at(registry: &str, name: &str) -> String {
    format!("{registry}/{}", name.replace('/', "%2f"))
}

/// The packument, as json.
fn ask(url: &str) -> Result<serde_json::Value, String> {
    // The abbreviated document, which is the one npm's own client asks
    // for: same versions and the same `dist`, without the readme and
    // the maintainer list, and for a package with a long history that
    // is megabytes of difference.
    let answer = crate::module::fetched(url, Some("application/vnd.npm.install-v1+json"))?;
    serde_json::from_slice(&answer)
        .map_err(|e| format!("{url} did not answer with a packument: {e}"))
}

/// A tarball off the network.
fn bytes(url: &str) -> Result<Vec<u8>, String> {
    crate::module::fetched(url, None)
}

/// Which version the ask meant, out of what the packument lists.
///
/// A dist tag is looked up rather than matched, since `latest` is not a
/// range and only the registry knows today's answer. Everything else is
/// npm's range language, and the highest version the range allows wins,
/// which is what npm installs when there is no lock file to say
/// otherwise.
fn pick(packument: &serde_json::Value, want: &str) -> Result<Chosen, String> {
    let versions = packument
        .get("versions")
        .and_then(|it| it.as_object())
        .ok_or("the packument lists no versions")?;
    let want = match want.trim().is_empty() {
        true => "latest",
        false => want.trim(),
    };
    let named = match Range::parse(want) {
        Some(range) => {
            let listed: Vec<Version> = versions
                .keys()
                .filter_map(|it| Version::parse(it))
                .collect();
            range
                .best(&listed)
                .ok_or("no version the registry lists is in this range")?
                .to_string()
        }
        // Not a range, so a tag, and a tag the registry does not have is
        // a mistake in the specifier rather than a version that has not
        // been published yet.
        None => packument
            .get("dist-tags")
            .and_then(|it| it.get(want))
            .and_then(|it| it.as_str())
            .ok_or("the registry has no tag by that name")?
            .to_string(),
    };
    let version = Version::parse(&named)
        .ok_or_else(|| format!("the registry points at {named}, which is not a version"))?;
    let dist = versions
        .get(&named)
        .and_then(|it| it.get("dist"))
        .ok_or_else(|| format!("the registry lists {named} without saying where it is"))?;
    Ok(Chosen {
        version,
        dist: Dist {
            tarball: dist
                .get("tarball")
                .and_then(|it| it.as_str())
                .ok_or_else(|| format!("{named} has no tarball"))?
                .to_string(),
            integrity: text(dist.get("integrity")),
            shasum: text(dist.get("shasum")),
        },
    })
}

fn text(value: Option<&serde_json::Value>) -> Option<String> {
    value
        .and_then(|it| it.as_str())
        .filter(|it| !it.is_empty())
        .map(|it| it.to_string())
}

/// Whether the bytes are the ones the registry said they would be.
///
/// `integrity` is a list, in principle, of one digest per algorithm,
/// and the strongest one that is understood is the one that counts. In
/// practice it is one sha512. The `shasum` behind it is sha1 and is
/// weak, and it is still the difference between a truncated download
/// and a package, so it is what a registry that gave nothing else is
/// held to.
fn check(bytes: &[u8], dist: &Dist) -> Result<(), String> {
    if let Some(integrity) = dist.integrity.as_deref() {
        let mut understood = 0;
        for one in integrity.split_whitespace() {
            let Some((algorithm, said)) = one.split_once('-') else {
                continue;
            };
            let Some(computed) = digest(algorithm, bytes) else {
                continue;
            };
            understood += 1;
            let said = base64ct::Base64::decode_vec(said)
                .map_err(|_| format!("the registry's {algorithm} digest is not base64"))?;
            if computed != said {
                return Err(format!(
                    "the tarball is not the one the registry's {algorithm} names"
                ));
            }
        }
        if understood > 0 {
            return Ok(());
        }
    }
    let Some(shasum) = dist.shasum.as_deref() else {
        return Err(
            "the registry gave no digest at all, so nothing here can say what arrived".into(),
        );
    };
    let computed = sha1::Sha1::digest(bytes);
    let said = hex(shasum).ok_or("the registry's shasum is not hex")?;
    match computed.as_slice() == said {
        true => Ok(()),
        false => Err("the tarball is not the one the registry's shasum names".into()),
    }
}

/// One digest, or nothing for an algorithm this does not know, since a
/// registry naming a hash nobody here has is a thing to pass over
/// rather than to fail on.
fn digest(algorithm: &str, bytes: &[u8]) -> Option<Vec<u8>> {
    match algorithm {
        "sha512" => Some(sha2::Sha512::digest(bytes).to_vec()),
        "sha384" => Some(sha2::Sha384::digest(bytes).to_vec()),
        "sha256" => Some(sha2::Sha256::digest(bytes).to_vec()),
        "sha1" => Some(sha1::Sha1::digest(bytes).to_vec()),
        _ => None,
    }
}

fn hex(text: &str) -> Option<Vec<u8>> {
    match text.len() % 2 {
        0 => (0..text.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(text.get(at..at + 2)?, 16).ok())
            .collect(),
        _ => None,
    }
}

/// The tarball onto the disk, under a name nothing reads until it is
/// whole.
///
/// A package that arrives while another isolate is unpacking the same
/// one finds the directory already there and is happy; one that loses
/// the race to move its own copy into place throws that copy away,
/// since the two are the same bytes from the same digest.
///
/// The name nothing reads has to be different for every unpack and not
/// only for every process. A graph the size of a real function has a
/// dozen loads in the air at once and two of them wanting the same
/// package is the ordinary case, so two threads here are two calls with
/// the same `into`: sharing a partial means one of them clears the
/// directory the other is still writing into, and what the caller sees
/// is the package failing to unpack with no such file. The count is what
/// keeps them apart, the same way it does for a fetched module.
fn put(tarball: &[u8], into: &Path) -> std::io::Result<()> {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let parent = into
        .parent()
        .ok_or_else(|| std::io::Error::other("a package with nowhere to go"))?;
    fs::create_dir_all(parent)?;
    let count = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let partial = parent.join(format!(
        ".{}.{count}.{}.partial",
        std::process::id(),
        into.file_name()
            .and_then(|it| it.to_str())
            .unwrap_or("package")
    ));
    if let Err(e) = crate::tarball::unpack(tarball, &partial) {
        let _ = fs::remove_dir_all(&partial);
        return Err(e);
    }
    match fs::rename(&partial, into) {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = fs::remove_dir_all(&partial);
            match into.join("package.json").is_file() {
                true => Ok(()),
                false => Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The registry's own answer for `ms`, recorded rather than written
    /// here: a package old enough that its first versions have a shasum
    /// and no integrity, and current enough to have four dist tags,
    /// three of which point at prereleases. Refetching it would give a
    /// longer list and the same shape.
    fn ms() -> serde_json::Value {
        let at = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/npm-packument-ms.json");
        serde_json::from_slice(&fs::read(&at).expect("the recorded packument")).expect("json")
    }

    #[test]
    fn a_range_takes_the_highest_version_it_allows() {
        let chosen = pick(&ms(), "^2.0.0").expect("a version in that range");
        assert_eq!(chosen.version.to_string(), "2.1.3");
        assert_eq!(
            chosen.dist.tarball,
            "https://registry.npmjs.org/ms/-/ms-2.1.3.tgz"
        );
        assert!(chosen.dist.integrity.is_some(), "a recent version has one");
    }

    #[test]
    fn a_single_version_is_a_range_that_allows_only_it() {
        let chosen = pick(&ms(), "2.0.0").expect("a version that exists");
        assert_eq!(chosen.version.to_string(), "2.0.0");
        assert!(
            chosen.dist.tarball.ends_with("ms-2.0.0.tgz"),
            "{:?}",
            chosen.dist
        );
    }

    #[test]
    fn a_prerelease_is_not_what_a_plain_range_meant() {
        let chosen = pick(&ms(), "*").expect("some version");
        assert_eq!(chosen.version.to_string(), "2.1.3", "and not a nightly");
    }

    #[test]
    fn a_tag_is_looked_up_rather_than_matched() {
        assert_eq!(
            pick(&ms(), "beta").expect("a tag").version.to_string(),
            "3.0.0-beta.2"
        );
        assert_eq!(
            pick(&ms(), "latest").expect("a tag").version.to_string(),
            "2.1.3"
        );
        // Nothing after the `@` is what a specifier says most of the
        // time, and it means the same as `latest`.
        assert_eq!(
            pick(&ms(), "").expect("a default").version.to_string(),
            "2.1.3"
        );
    }

    #[test]
    fn a_version_published_before_integrity_existed_carries_both_now() {
        // The registry went back and computed a sha512 for versions
        // published when there was no such field, so what is recorded
        // here for 2013 is a shasum and an integrity both. Which is
        // why the shasum path below is tested against a made up dist
        // rather than against this one: npm has no version left that
        // would take it.
        let chosen = pick(&ms(), "0.1.0").expect("the first version");
        assert!(chosen.dist.shasum.is_some(), "the digest of the day");
        assert!(chosen.dist.integrity.is_some(), "and the one added since");
    }

    #[test]
    fn an_ask_the_registry_cannot_answer_says_which_half_was_wrong() {
        let said = pick(&ms(), "^9.0.0").expect_err("no such version");
        assert!(said.contains("no version"), "{said}");
        let said = pick(&ms(), "experimental").expect_err("no such tag");
        assert!(said.contains("no tag"), "{said}");
    }

    #[test]
    fn a_tarball_is_checked_against_what_the_registry_said_it_is() {
        let bytes = b"a package, supposedly";
        let sha512 = base64ct::Base64::encode_string(&sha2::Sha512::digest(bytes));
        let right = Dist {
            tarball: "https://registry.npmjs.org/x/-/x-1.0.0.tgz".into(),
            integrity: Some(format!("sha512-{sha512}")),
            shasum: None,
        };
        check(bytes, &right).expect("the digest is the bytes");
        check(b"a different package", &right).expect_err("and nothing else is");
    }

    #[test]
    fn a_shasum_is_what_an_old_version_is_checked_against() {
        let bytes = b"a package from 2013";
        let hex: String = sha1::Sha1::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let old = Dist {
            tarball: "https://registry.npmjs.org/x/-/x-0.1.0.tgz".into(),
            integrity: None,
            shasum: Some(hex),
        };
        check(bytes, &old).expect("the shasum is the bytes");
        check(b"something else", &old).expect_err("and nothing else is");
    }

    #[test]
    fn a_tarball_with_no_digest_at_all_is_refused() {
        let nothing = Dist {
            tarball: "https://registry.npmjs.org/x/-/x-1.0.0.tgz".into(),
            integrity: None,
            shasum: None,
        };
        let said = check(b"anything", &nothing).expect_err("unverifiable is not acceptable");
        assert!(said.contains("no digest"), "{said}");
    }

    #[test]
    fn an_algorithm_nobody_here_has_is_passed_over_rather_than_trusted() {
        let bytes = b"a package";
        let sha512 = base64ct::Base64::encode_string(&sha2::Sha512::digest(bytes));
        let both = Dist {
            tarball: "https://registry.npmjs.org/x/-/x-1.0.0.tgz".into(),
            integrity: Some(format!("sha3-abcdef sha512-{sha512}")),
            shasum: None,
        };
        check(bytes, &both).expect("the one that is understood is the one that counts");
        let only = Dist {
            integrity: Some("sha3-abcdef".into()),
            ..both
        };
        check(bytes, &only)
            .expect_err("and an integrity that says nothing this knows is no integrity");
    }

    #[test]
    fn a_scoped_name_is_escaped_the_way_the_registry_wants_it() {
        assert_eq!(
            at("https://registry.npmjs.org", "ms"),
            "https://registry.npmjs.org/ms"
        );
        assert_eq!(
            at("https://registry.npmjs.org", "@supabase/supabase-js"),
            "https://registry.npmjs.org/@supabase%2fsupabase-js"
        );
    }

    /// Eight threads unpacking the same package onto the same place at
    /// once, which is what a graph of any size does. Every one of them
    /// has to come back with the whole package: a directory that is
    /// half there is the failure this is for, and it showed up in the
    /// corpus run as a package that could not be unpacked with no such
    /// file, which is one thread clearing the directory another was
    /// still writing into.
    #[test]
    fn the_same_package_unpacked_by_everybody_at_once_arrives_whole() {
        let cache = tempfile::tempdir().expect("a temporary cache");
        let into = cache.path().join("npm").join("one").join("1.0.0");
        let at = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tarball-ustar.tgz");
        let tarball = fs::read(&at).expect("the recorded tarball");
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let (tarball, into) = (tarball.as_slice(), into.as_path());
                scope.spawn(move || {
                    put(tarball, into).expect("the package unpacks");
                    assert_eq!(
                        fs::read_to_string(into.join("index.js")).expect("the code"),
                        "export const one = 1;\n"
                    );
                });
            }
        });
        assert!(into.join("package.json").is_file());
        assert!(into.join("lib/deep/deeper.js").is_file());
        // And nothing is left beside it under the name nothing reads.
        let beside: Vec<_> = fs::read_dir(into.parent().expect("a directory"))
            .expect("the directory")
            .filter_map(|it| it.ok())
            .map(|it| it.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(beside, vec!["1.0.0".to_string()], "{beside:?}");
    }

    /// The whole of it, against the real registry: ask for a range, get
    /// a tarball, check the digest, unpack it, and find the manifest of
    /// the version that range meant. Ignored because it wants the
    /// network, and run in the suite that is allowed to have one.
    #[test]
    #[ignore]
    fn a_package_off_the_registry_arrives_and_is_what_it_said_it_was() {
        let cache = tempfile::tempdir().expect("a temporary cache");
        let at = package("ms", "^2.0.0", cache.path()).expect("a package");
        let manifest = fs::read_to_string(at.join("package.json")).expect("a manifest");
        let manifest: serde_json::Value = serde_json::from_str(&manifest).expect("json");
        assert_eq!(manifest["name"], "ms");
        assert_eq!(manifest["version"], "2.1.3");
        assert!(at.join("index.js").is_file(), "and the code beside it");
        // A second ask is the directory that is already there.
        assert_eq!(package("ms", "^2.0.0", cache.path()).expect("again"), at);
    }
}
