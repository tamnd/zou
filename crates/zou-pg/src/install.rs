//! Where the patched postgres is.
//!
//! Four answers in order: what the caller said, what the environment
//! says, the install this binary was shipped in, and the place a
//! checkout builds into. The third is the one that matters to somebody
//! who downloaded a release, where the tree is
//!
//! ```text
//! zou-linux-x64/
//!   bin/zou
//!   pg/bin/postgres
//! ```
//!
//! and nothing has been said about postgres at all, because there is
//! nothing they could usefully say: the postmaster next to the binary
//! is the one that binary was built against.

use std::path::{Path, PathBuf};

/// The developer answer, and the last one tried.
const IN_A_CHECKOUT: &str = "build/pg/bin";

/// Resolve the install, given whatever the caller passed on the command
/// line or in options. An empty path counts as nothing said.
pub fn pg_bin(said: Option<PathBuf>) -> PathBuf {
    if let Some(path) = said
        && !path.as_os_str().is_empty()
    {
        return path;
    }
    if let Some(path) = std::env::var_os("ZOU_PG_BIN") {
        return PathBuf::from(path);
    }
    if let Some(path) = beside_this_binary() {
        return path;
    }
    PathBuf::from(IN_A_CHECKOUT)
}

/// The `pg/bin` of a release bundle, when this executable is the `zou`
/// inside one. Guarded on the postmaster actually being there, so a
/// binary that happens to live in a `bin` directory does not start
/// pointing at a `pg` that is not.
///
/// The link is followed first, because the installer puts versions side
/// by side and links `~/.zou/bin/zou` at the current one, and the
/// postgres that matters is the one in the version, not the one that
/// would be next to the link. `current_exe` resolves that on some
/// platforms and not others, so this does not rely on which.
fn beside_this_binary() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let followed = std::fs::canonicalize(&exe).ok();
    followed.as_deref().and_then(above).or_else(|| above(&exe))
}

/// The `pg/bin` next to the `bin` this executable is in, if a
/// postmaster is there.
fn above(exe: &Path) -> Option<PathBuf> {
    let candidate = exe.parent()?.parent()?.join("pg").join("bin");
    postmaster(&candidate).then_some(candidate)
}

fn postmaster(bin: &Path) -> bool {
    bin.join("postgres").exists() || bin.join("postgres.exe").exists()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_the_caller_said_wins() {
        assert_eq!(
            pg_bin(Some(PathBuf::from("/somewhere/bin"))),
            PathBuf::from("/somewhere/bin")
        );
    }

    #[test]
    fn an_empty_path_is_nothing_said() {
        // Which is how zou-embed's Options carries "not set", so it has
        // to fall through rather than resolve to the empty directory.
        assert_ne!(pg_bin(Some(PathBuf::new())), PathBuf::new());
    }

    #[test]
    fn a_binary_that_is_not_in_a_bundle_finds_nothing_beside_it() {
        // The test binary is under target/debug/deps, and there is no
        // pg/bin above it holding a postmaster.
        assert_eq!(beside_this_binary(), None);
    }

    #[test]
    fn a_bundle_is_recognised_by_the_postmaster_in_it() {
        let bundle = tempfile::tempdir().expect("a directory");
        let bin = bundle.path().join("bin");
        let pg = bundle.path().join("pg").join("bin");
        std::fs::create_dir_all(&bin).expect("bin");
        std::fs::create_dir_all(&pg).expect("pg/bin");
        let zou = bin.join("zou");
        std::fs::write(&zou, b"pretend").expect("write");

        // An empty pg/bin is a directory somebody made, not an install.
        assert_eq!(above(&zou), None);
        std::fs::write(pg.join("postgres"), b"pretend").expect("write");
        assert_eq!(above(&zou), Some(pg));
    }
}
