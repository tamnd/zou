//! A reader that stops reading is the end of a listing, not a crash.
//!
//! `zou info store | head` and `zou info store | grep -q something`
//! both close the pipe while the command is still writing, the second
//! the moment it matches, and a command that panicked there would cost
//! a stack trace and a nonzero status to a pipeline that did exactly
//! what it meant to.

use std::process::{Command, Stdio};

const ZOU: &str = env!("CARGO_BIN_EXE_zou");

/// A store with a few tenants in it, which is a listing worth piping.
fn store() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("a directory");
    for name in ["acme-prod", "acme-staging", "demo"] {
        let made = Command::new(ZOU)
            .args([
                "tenant",
                dir.path().to_str().expect("a path"),
                "create",
                name,
            ])
            .output()
            .expect("zou runs");
        assert!(
            made.status.success(),
            "{}",
            String::from_utf8_lossy(&made.stderr)
        );
    }
    dir
}

/// The pipe closed under the command, which is what `grep -q` does on
/// its first match.
#[test]
fn a_listing_whose_reader_hangs_up_ends_as_a_command_that_finished() {
    let dir = store();
    let mut child = Command::new(ZOU)
        .args(["tenant", dir.path().to_str().expect("a path"), "list"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("zou runs");
    // The read end, gone before the command has written a line.
    drop(child.stdout.take());
    let done = child.wait_with_output().expect("it stops");
    let said = String::from_utf8_lossy(&done.stderr);
    assert!(!said.contains("panicked"), "{said}");
    assert_eq!(
        done.status.code(),
        Some(0),
        "a reader that has seen enough is not a failure: {said}"
    );
}

/// The same through a shell, which is where it actually bites, with
/// pipefail on so the writer's status is the one that is reported.
#[test]
fn grep_q_and_head_leave_the_pipeline_green() {
    let dir = store();
    let at = dir.path().to_str().expect("a path");
    for reader in ["head -1", "grep -q acme"] {
        let script = format!("set -o pipefail; {ZOU} tenant {at} list | {reader}");
        let ran = Command::new("sh")
            .arg("-c")
            .arg(&script)
            .output()
            .expect("sh runs");
        let said = String::from_utf8_lossy(&ran.stderr);
        assert!(!said.contains("panicked"), "{reader}: {said}");
        assert_eq!(ran.status.code(), Some(0), "{reader}: {said}");
    }
}

/// A write that failed for any other reason is still a failure, and
/// this says the fix did not turn every one of them into a quiet zero.
/// `/dev/full` takes nothing and is where linux keeps that case.
#[test]
fn a_write_that_fails_for_another_reason_is_still_a_failure() {
    if !std::path::Path::new("/dev/full").exists() {
        return;
    }
    let dir = store();
    let at = dir.path().to_str().expect("a path");
    let script = format!("{ZOU} tenant {at} list > /dev/full");
    let ran = Command::new("sh")
        .arg("-c")
        .arg(&script)
        .output()
        .expect("sh runs");
    let said = String::from_utf8_lossy(&ran.stderr);
    assert_eq!(ran.status.code(), Some(1), "{said}");
    assert!(said.contains("writing to stdout"), "{said}");
}
