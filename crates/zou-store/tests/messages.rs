//! The half of an error message that is usually missing.
//!
//! Every error in this tree says what happened. That is the easy half
//! and it was never the problem. The half a person reading it at two in
//! the morning needs is what to do about it, and the place it goes
//! missing is always the same one: an error that reports something is
//! not there. "the tenant does not exist" is true, complete, and leaves
//! the reader with nowhere to go, because the two things it could mean,
//! a ref typed wrong and a tenant never created, have different answers
//! and the message picks neither.
//!
//! So this walks the workspace for the messages of that shape and
//! requires each one to carry a next step. It is a narrow rule on
//! purpose. A blanket "every message must end in advice" would be
//! noise: `crc mismatch, frame is corrupt` has no next step worth
//! writing and inventing one would make it longer without making it
//! more useful. The rule bites exactly where the reader is stuck.
//!
//! Narrow in one more way: this is about the errors zou writes in its
//! own voice. The json bodies the rest, auth and storage endpoints
//! return are Supabase's text and have to stay word for word what the
//! client libraries and their users already expect, so an improvement
//! to one of those is a compatibility break rather than a kindness.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// A message that says something is absent. Not every phrasing of
/// absence, only the ones that name a thing the caller asked for by
/// name and could have asked for differently.
const ABSENCE: &[&str] = &["does not exist", "no tenant ", "has no manifest"];

/// What counts as a next step. Either the message names something to
/// run, in backticks so it reads as a command and not as prose, or it
/// says which of the possible causes it is, which is the answer when
/// there is nothing to run.
fn carries_a_next_step(message: &str) -> bool {
    message.contains('`') || message.contains("was never created") || message.contains("shows what")
}

/// Messages that report absence to something other than a person, so
/// the rule does not apply. Each one is here with the reason, because
/// an exception list nobody has to justify grows until the rule is
/// gone.
/// Matched as a substring, and written to be specific enough that it
/// covers only the message it names. The two sites that build this one
/// spell the interpolation differently, so the shared part is the key.
const NOT_FOR_A_PERSON: &[(&str, &str)] = &[(
    "database \\\"",
    "sqlstate 3D000 on the postgres wire, whose text is fixed by the protocol and which drivers match on, so the place a person gets told what to do is the connection error their client library raises around it",
)];

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the workspace is two levels up from this crate")
        .to_path_buf()
}

/// Where an error message is built. A thiserror variant's attribute,
/// and a `format!`, which is how the CLI and the servers report. The
/// literal is the first thing inside the parens in both, which is what
/// makes them findable without parsing rust.
const BUILDERS: &[&str] = &["#[error(", "format!("];

/// Every string literal that reaches a person as an error, keyed by the
/// file and line it came from.
fn messages() -> BTreeMap<String, String> {
    let mut found = BTreeMap::new();
    let mut stack = vec![root().join("crates")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().is_none_or(|e| e != "rs") {
                continue;
            }
            let shown = path
                .strip_prefix(root())
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            // Tests write deliberately broken messages to prove the
            // checks that read them.
            if shown.contains("/tests/") || shown.contains("/benches/") {
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            for (at, message) in literals(&text) {
                let line = text[..at].lines().count();
                found.insert(format!("{shown}:{line}"), message);
            }
        }
    }
    found
}

/// The literal each builder in a file opens, with the byte it started
/// at. Whitespace between the paren and the quote is skipped, because
/// rustfmt puts the literal on its own line as soon as the call is long
/// enough, and a message long enough to wrap is not one to stop
/// checking. A builder whose first argument is not a literal, which is
/// `#[error(transparent)]` and nothing else here, has no message.
///
/// This finds a builder by its own text rather than by the shape of the
/// line, so a comparison against a message, or a doc comment quoting
/// one, is not mistaken for writing one.
fn literals(text: &str) -> Vec<(usize, String)> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    for builder in BUILDERS {
        let mut from = 0;
        while let Some(hit) = text[from..].find(builder) {
            let mut i = from + hit + builder.len();
            from = i;
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i >= bytes.len() || bytes[i] != b'"' {
                continue;
            }
            let open = i + 1;
            let mut j = open;
            while j < bytes.len() && !(bytes[j] == b'"' && bytes[j - 1] != b'\\') {
                j += 1;
            }
            if j >= bytes.len() {
                break;
            }
            out.push((open, text[open..j].to_string()));
            from = j + 1;
        }
    }
    out
}

#[test]
fn an_error_that_says_a_thing_is_not_there_says_how_to_find_out_what_is() {
    let excused: Vec<&str> = NOT_FOR_A_PERSON.iter().map(|(m, _)| *m).collect();
    let mut stuck = Vec::new();
    for (site, message) in messages() {
        if !ABSENCE.iter().any(|a| message.contains(a)) {
            continue;
        }
        if excused.iter().any(|e| message.contains(e)) {
            continue;
        }
        if !carries_a_next_step(&message) {
            stuck.push(format!("{site}: {message}"));
        }
    }
    assert!(
        stuck.is_empty(),
        "these say what is missing and not what to do about it:\n  {}",
        stuck.join("\n  ")
    );
}

/// The walk has to actually be finding things, or the rule above passes
/// on an empty set forever. The count is a floor rather than an exact
/// number, since messages come and go and a floor is the part that
/// means something.
#[test]
fn the_walk_reaches_the_messages_it_is_meant_to_check() {
    let all = messages();
    assert!(
        all.len() > 200,
        "only {} error messages found in the tree, the walk is broken",
        all.len()
    );
    let absent = all
        .values()
        .filter(|m| ABSENCE.iter().any(|a| m.contains(a)))
        .count();
    assert!(
        absent >= 5,
        "only {absent} messages report something missing, the rule is checking nothing"
    );
}

/// An excuse has to name a reason. The list is the pressure valve on
/// the rule and a valve nobody has to justify is a hole.
#[test]
fn every_message_excused_from_the_rule_says_why() {
    for (message, reason) in NOT_FOR_A_PERSON {
        assert!(
            reason.len() > 30,
            "{message} is excused with {reason:?}, which does not say why"
        );
    }
}
