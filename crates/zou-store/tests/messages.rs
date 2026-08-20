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
//! The other place is an error that refuses. "destination proj already
//! exists" and "lease held by node-a" both leave the same gap, and it
//! is a worse one, because the reader cannot tell from the words
//! whether they are looking at a normal thing that resolves itself, a
//! name they should have picked differently, or a bug.
//!
//! The third is a setting somebody typed that zou would not take, and
//! the gap there is the opposite one. They already know what they
//! wrote, so saying it back is not information. What they do not know
//! is what would have been accepted, and `bad lsn "12345"` picks the
//! half they had.
//!
//! So this walks the workspace for messages of those shapes and
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

/// The message shapes the rule covers. Not every phrasing of absence or
/// refusal, only the ones that name a thing the caller asked for and
/// could have asked for differently, which is what makes a next step
/// something a reader can act on.
const STUCK: &[&str] = &[
    // Something the caller named is not there.
    "does not exist",
    "no tenant ",
    "has no manifest",
    // Something the caller asked for was refused.
    "already exists",
    "already registered",
    "already claimed",
    "belongs to",
    "refusing to",
    "nothing to merge",
    "the ceiling",
    "lease held by",
    "lease lost",
];

/// What counts as a next step. Usually something to run, in backticks so
/// it reads as a command and not as prose, or a setting to change. The
/// rest are for messages whose honest answer is not a command: which of
/// the possible causes it is, that the thing to do is move a file, stop
/// a process or file a bug, or that there is genuinely no action.
///
/// Be clear about what this can and cannot do. It cannot tell whether an
/// ending is good advice, only that the author wrote one, and a
/// determined author gets past it by pasting a backtick. That is fine.
/// The failure it exists to catch is not bad advice, it is the message
/// that stops at the diagnosis because nobody thought about the reader,
/// and for that a check that makes the ending a required field is
/// enough. Keeping the list short is what keeps it from drifting into a
/// prose grader that anything passes.
const NEXT_STEP: &[&str] = &[
    "`",
    "ZOU_",
    "was never created",
    "report it",
    "nothing to do about it",
    "move it aside",
    "stop this node",
    "check both",
];

fn carries_a_next_step(message: &str) -> bool {
    NEXT_STEP.iter().any(|n| message.contains(n))
}

/// Messages that reach something other than a person, so the rule does
/// not apply. Each one is here with the reason, because an exception
/// list nobody has to justify grows until the rule is gone.
///
/// Matched as a substring, and written to be specific enough that it
/// covers only the message it names. The two sites that build this one
/// spell the interpolation differently, so the shared part is the key.
const NOT_FOR_A_PERSON: &[(&str, &str)] = &[
    (
        "database \\\"",
        "sqlstate 3D000 on the postgres wire, whose text is fixed by the protocol and which drivers match on, so the place a person gets told what to do is the connection error their client library raises around it",
    ),
    (
        "A factor with the friendly name",
        "the body the mfa enroll endpoint returns, which is GoTrue's word for word, so the supabase client libraries and everything written against them already expect this exact string",
    ),
];

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
fn an_error_that_reports_absence_or_refuses_says_what_to_do_about_it() {
    let excused: Vec<&str> = NOT_FOR_A_PERSON.iter().map(|(m, _)| *m).collect();
    let mut stuck = Vec::new();
    for (site, message) in messages() {
        if !STUCK.iter().any(|a| message.contains(a)) {
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
        .filter(|m| STUCK.iter().any(|a| m.contains(a)))
        .count();
    assert!(
        absent >= 5,
        "only {absent} messages report something missing, the rule is checking nothing"
    );
}

/// Where text a person typed enters zou: the command line, which is the
/// `zou` crate, and the two settings whose value is a small language of
/// its own. The files that parse zou's own formats are outside this,
/// and deliberately so. `bad INDEX line "x y z"` is not a value
/// somebody chose, there is no accepted shape to offer them, and the
/// honest answer there is that a layer is corrupt.
const TYPED_BY_A_PERSON: &[&str] = &[
    "crates/zou/src/",
    "crates/zou-store/src/sim.rs",
    "crates/zou-store/src/delay.rs",
];

/// A message that refuses a value somebody typed.
const REFUSED_A_VALUE: &[&str] = &["bad ", "unknown "];

/// What counts as naming the accepted shape. `{USAGE}` is on the list
/// because a message that prints the usage text has already answered
/// the question, at more length than the line itself could.
const SHAPE: &[&str] = &[
    "write ",
    "want ",
    "takes ",
    "one of ",
    "the keys are",
    "builtins are",
    "form",
    "USAGE",
    "usage",
];

/// Refusals that hand the question to the error they wrap. This is the
/// pressure valve on the rule above and it is a narrow one on purpose,
/// because "the cause explains it" is the excuse every unhelpful
/// message would give if it were allowed to.
///
/// It is also this tree's answer to the wider question of what to do
/// with the long tail of wrapped errors, the ones shaped `cannot read
/// the schemas: {e}`. The answer is that a wrapper whose cause already
/// carries the shape should say less rather than more, and repeating
/// the value in both halves of one line is worse than not saying it
/// twice. What the wrapper owes the reader is the context the cause
/// cannot know, which is which setting was being read.
const THE_CAUSE_SAYS_IT: &[(&str, &str)] = &[(
    "bad horizon: ",
    "the lsn parse this wraps already says the value and the X/XXXXXXXX form it wanted, so naming the value again here would put it twice in one line and the word horizon is the only thing that parse cannot know",
)];

/// The other half of the same idea as the rule above, for the other
/// reader. Somebody who set a value already knows what they set, so
/// repeating it back to them is not information. What they do not know
/// is what zou would have taken instead, and that is the whole content
/// of a good message here. `invalid lsn "12345", expected the
/// X/XXXXXXXX form` in lsn.rs is the shape, `bad lsn "12345"` was not.
#[test]
fn refusing_a_value_somebody_typed_names_the_shape_that_was_wanted() {
    let mut bare = Vec::new();
    for (site, message) in messages() {
        if !TYPED_BY_A_PERSON.iter().any(|p| site.starts_with(p)) {
            continue;
        }
        if !REFUSED_A_VALUE.iter().any(|r| message.starts_with(r)) {
            continue;
        }
        if THE_CAUSE_SAYS_IT.iter().any(|(m, _)| message.contains(m)) {
            continue;
        }
        if !SHAPE.iter().any(|s| message.contains(s)) {
            bare.push(format!("{site}: {message}"));
        }
    }
    assert!(
        bare.is_empty(),
        "these refuse a value without saying what would have been taken:\n  {}",
        bare.join("\n  ")
    );
}

/// The same floor as the walk test above, for the same reason. A rule
/// that matches nothing passes forever.
#[test]
fn the_walk_reaches_the_refusals_it_is_meant_to_check() {
    let refusals = messages()
        .into_iter()
        .filter(|(site, message)| {
            TYPED_BY_A_PERSON.iter().any(|p| site.starts_with(p))
                && REFUSED_A_VALUE.iter().any(|r| message.starts_with(r))
        })
        .count();
    assert!(
        refusals >= 15,
        "only {refusals} messages refuse a typed value, the rule is checking nothing"
    );
}

/// An excuse has to name a reason. The list is the pressure valve on
/// the rule and a valve nobody has to justify is a hole.
#[test]
fn every_message_excused_from_the_rule_says_why() {
    for (message, reason) in NOT_FOR_A_PERSON.iter().chain(THE_CAUSE_SAYS_IT) {
        assert!(
            reason.len() > 30,
            "{message} is excused with {reason:?}, which does not say why"
        );
    }
}
