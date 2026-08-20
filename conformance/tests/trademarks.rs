//! The trademark position, as a test rather than as a paragraph.
//!
//! `TRADEMARKS.md` says zou is not a Supabase product, that the mark is
//! used here only to say what zou is compatible with, and that nothing
//! published from here is named after somebody else's. All of that was
//! true on the day it was read, and none of it was held by anything.
//!
//! That is the shape of a claim that quietly stops being true. The
//! sentence that breaks it is one line in a README somebody writes in a
//! hurry, or a package renamed to be easier to find, and neither is the
//! kind of thing a reviewer is watching for. So the rules are here, over
//! the tracked files, and they fail at the line that wrote them.
//!
//! This lives in the conformance crate because that is where the tests
//! about what this repository claims already live: the scoreboard's own
//! tests read `docs/compatibility.md` and `docs/conformance.md` for the
//! same reason.

use std::path::Path;

mod common;
use common::{text, tracked};

/// Phrasings that say, or let a reader conclude, that this project is
/// somebody else's or has somebody else's blessing.
const AFFILIATION: &[&str] = &[
    "official supabase",
    "supabase official",
    "endorsed by",
    "affiliated with",
    "sponsored by",
    "in partnership with",
    "powered by supabase",
    "open source supabase",
    "open-source supabase",
    "self hosted supabase",
    "self-hosted supabase",
];

/// A disclaimer is made of the words it denies, so a rule that refused
/// these outright would refuse the one file whose job is to write them
/// down, and exempting that file would leave the words unwatched in the
/// place they matter most. So the rule reads the sentence instead: the
/// phrase is allowed where the sentence around it is a denial.
const DENIALS: &[&str] = &["not", "neither", "nor", "never", "isn't", "aren't"];

/// The sentence the phrase is in, as far back as the nearest full stop,
/// which is close enough to a sentence for this and needs no parser.
fn denied(before: &str) -> bool {
    before
        .rsplit(['.', ';', '\t'])
        .next()
        .unwrap_or(before)
        .split(|c: char| !c.is_ascii_alphabetic() && c != '\'')
        .any(|word| DENIALS.contains(&word))
}

/// The file whose job is to deny them, and the sentence it cannot lose
/// without this failing.
const NOTICE: &str = "TRADEMARKS.md";

/// This file, which is the one place the phrases appear as a list rather
/// than as a sentence, so there is no sentence around them to be a denial.
/// CI found it before this line existed, on a run where the list had been
/// committed and the local run that passed had been against a tree where
/// it was still untracked.
const RULES: &str = "conformance/tests/trademarks.rs";
const DISCLAIMER: &str = "not affiliated with, sponsored by or endorsed by Supabase Inc";

/// Marks that must not turn up in the name of anything published from
/// here. The crates are `zou` and `zou-*`, the command line is `zou-cli`
/// on npm, and the wheel is `zou-postgres`.
const NOT_OURS: &[&str] = &["supabase", "postgrest", "gotrue", "postgres-meta"];

#[test]
fn nothing_here_claims_to_be_somebody_else_s_or_to_have_their_blessing() {
    for path in tracked() {
        if path == Path::new(RULES) {
            continue;
        }
        let Some(body) = text(&path) else { continue };
        for (index, line) in body.lines().enumerate() {
            let lowered = line.to_lowercase();
            for claim in AFFILIATION {
                let Some(at) = lowered.find(claim) else {
                    continue;
                };
                assert!(
                    denied(&lowered[..at]),
                    "{}:{}\n{line}\nreads as a claim of affiliation or endorsement, and zou has neither. The mark is used here to say what zou is compatible with and for nothing else, which is what {NOTICE} says.",
                    path.display(),
                    index + 1
                );
            }
        }
    }
}

#[test]
fn the_notice_still_says_the_thing_the_rest_of_this_leaves_out() {
    let body = text(Path::new(NOTICE)).unwrap_or_else(|| panic!("{NOTICE} is in the repository"));
    assert!(
        body.contains(DISCLAIMER),
        "{NOTICE} no longer says {DISCLAIMER:?}, which is the one sentence it exists for. Every other file in the tree may only write those words to deny them, so if this one stops saying them outright the position is stated nowhere."
    );
    let readme = text(Path::new("README.md")).expect("the README is in the repository");
    assert!(
        readme.contains(NOTICE),
        "the README does not link {NOTICE}, and a notice nobody is sent to is a notice nobody reads"
    );
}

#[test]
fn nothing_published_from_here_is_named_after_a_mark_that_is_not_ours() {
    for path in tracked() {
        let manifest = match path.file_name().and_then(|name| name.to_str()) {
            Some("Cargo.toml") | Some("pyproject.toml") => "name = \"",
            Some("package.json") => "\"name\": \"",
            _ => continue,
        };
        let Some(body) = text(&path) else { continue };
        for (index, line) in body.lines().enumerate() {
            let Some(rest) = line.trim_start().strip_prefix(manifest) else {
                continue;
            };
            let Some((name, _)) = rest.split_once('"') else {
                continue;
            };
            let lowered = name.to_lowercase();
            for mark in NOT_OURS {
                assert!(
                    !lowered.contains(mark),
                    "{}:{}\n{line}\nnames something published from here after {mark}, which is not this project's to name things after. Compatible with is a sentence, not a package name.",
                    path.display(),
                    index + 1
                );
            }
        }
    }
}
