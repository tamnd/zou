//! What the tests about the repository itself all need.
//!
//! The rules in `trademarks.rs` and `versions.rs` are about what this tree
//! says and what it publishes rather than about what the server answers,
//! so they all start from the same two questions: which files are there,
//! and what is in them. Those answers live here so that a change to what
//! counts as a tracked file is one edit rather than one per rule.

use std::path::{Path, PathBuf};
use std::process::Command;

pub fn root() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
}

/// Every file git is tracking, which is the right set: what is committed
/// is what somebody reads, and everything else on the disk is a build
/// artefact or a checkout of somebody else's tree.
///
/// It has one trap in it. A rule that reads this is green on a tree where
/// the file it would fail on is still untracked, so a local run can pass
/// on work that CI then refuses. That is not a reason to use a different
/// set, it is a reason to `git add` before believing a green run.
pub fn tracked() -> Vec<PathBuf> {
    let listing = Command::new("git")
        .arg("-C")
        .arg(root())
        .args(["ls-files", "-z"])
        .output()
        .expect("git is on the path and this is a checkout");
    assert!(listing.status.success(), "git ls-files failed");
    String::from_utf8(listing.stdout)
        .expect("git wrote utf8 paths")
        .split('\0')
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .filter(|path| !path.starts_with("vendor"))
        .collect()
}

/// A tracked file's text, or nothing if it is not text. Reading the bytes
/// and giving up on the ones that are not utf8 is how a binary is skipped
/// without keeping a list of extensions that would go stale on its own.
pub fn text(path: &Path) -> Option<String> {
    std::fs::read(root().join(path))
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
}
