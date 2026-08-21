//! What the commands print, and what happens when nobody is reading.
//!
//! `println!` panics when the pipe it is writing to is gone, and a
//! reader closing early is not a failure: `zou info store | head` and
//! `zou info store | grep -q something` both do it on purpose, the
//! second the moment it matches. A panic there is a stack trace and a
//! nonzero status for a pipeline that did exactly what it meant to, in
//! the one place a listing command belongs.
//!
//! So every line goes through here, and a broken pipe is the reader
//! saying it has seen enough: the command stops where it is and exits
//! as a command that finished. Any other write error is a real one and
//! is said on the error stream, which is still there.
//!
//! This is a check on writes and not a signal disposition, because
//! this binary is also a server: `zou serve` writes to sockets whose
//! clients hang up all day, and a process wide SIGPIPE would take the
//! node down with the first one of them.

use std::io::Write;

/// One line, the way `println!` writes one.
pub fn line(args: std::fmt::Arguments<'_>) {
    let mut out = std::io::stdout().lock();
    check(out.write_fmt(args).and_then(|()| out.write_all(b"\n")));
}

/// The same, without the newline, for the commands whose output is a
/// document rather than a list.
pub fn text(args: std::fmt::Arguments<'_>) {
    let mut out = std::io::stdout().lock();
    check(out.write_fmt(args));
}

fn check(written: std::io::Result<()>) {
    let Err(e) = written else {
        return;
    };
    if e.kind() == std::io::ErrorKind::BrokenPipe {
        std::process::exit(0);
    }
    let _ = writeln!(std::io::stderr(), "zou: writing to stdout: {e}");
    std::process::exit(1);
}
