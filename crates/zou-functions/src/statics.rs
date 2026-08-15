//! What a function is allowed to read off the disk, which is upstream's
//! `static_files` and nothing else.
//!
//! A function is somebody else's javascript running in a process that
//! holds a database superuser connection and a JWT secret, so the file
//! system is not something it is handed and then asked to be polite
//! about. Upstream draws the same line: the worker gets a list of
//! `static_patterns` and the runtime it is in will not open anything
//! that is not one of them.
//!
//! # The patterns are the config file's
//!
//! `static_files = ["./functions/hello/*.html"]` under
//! `[functions.hello]`, relative to the project directory, and glob
//! patterns because a project that serves a page usually has more than
//! one file in it.
//!
//! The globbing is upstream's, character for character: `*` matches
//! within one path segment, `**` matches across them, `?` is one
//! character that is not a separator, and `[abc]` is a class that `!`
//! negates. Everything else is a literal.
//!
//! # A name is relative to the function's own directory
//!
//! `Deno.readTextFile("./index.html")` in `functions/hello/index.ts`
//! means `functions/hello/index.html`, because the directory the
//! entrypoint is in is upstream's `servicePath` and is what the worker
//! resolves a relative name against.
//!
//! The name is tidied lexically before it is matched: `.` goes and `..`
//! pops, so a name cannot walk out of what the patterns cover by going
//! up and back down again. What is not followed is a symlink, so a link
//! the project put inside its own static files is read as the project
//! asked for it to be.

use std::path::{Component, Path, PathBuf};

use crate::Function;

/// The files one function may read, and where its relative names start.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Statics {
    /// For the sentence a refusal ends up as in the function's log.
    name: String,
    /// The directory the entrypoint is in, upstream's `servicePath`.
    root: PathBuf,
    /// The `static_files` patterns, absolute and tidied.
    patterns: Vec<String>,
}

impl Statics {
    /// What this function may read, out of what it was configured with.
    pub fn of(function: &Function) -> Statics {
        let root = function.entrypoint.parent().map(tidy).unwrap_or_default();
        Statics {
            name: function.name.clone(),
            root,
            patterns: function
                .static_files
                .iter()
                .map(|pattern| tidy(pattern).to_string_lossy().into_owned())
                .collect(),
        }
    }

    /// The file `name` means, if the patterns cover it.
    ///
    /// The `Err` is the sentence the function is refused with, and it
    /// names the function so that a project with ten of them can tell
    /// which one asked.
    pub fn at(&self, name: &str) -> Result<PathBuf, String> {
        let asked = Path::new(name);
        let whole = if asked.is_absolute() {
            tidy(asked)
        } else {
            tidy(&self.root.join(asked))
        };
        let shown = whole.to_string_lossy();
        if self.patterns.iter().any(|pattern| matches(pattern, &shown)) {
            return Ok(whole);
        }
        Err(format!(
            "{}: {shown} is not one of the function's static_files",
            self.name
        ))
    }
}

/// A path with `.` dropped and `..` popped, absolute, and without the
/// disk having been asked about any of it.
///
/// Lexical rather than `canonicalize` because the file may not be there
/// and a name that is refused should be refused by its pattern rather
/// than by whether somebody had created it yet.
fn tidy(path: &Path) -> PathBuf {
    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let mut out = PathBuf::new();
    for part in absolute.components() {
        match part {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Upstream's glob, which is `globToRegExp` in the CLI's deploy path
/// written out as a matcher rather than as a regular expression.
///
/// Backtracking, and only at a star, because that is the only place a
/// glob has a choice to make. What is remembered is where the star was
/// and how much it had swallowed, so a mismatch after it can hand it one
/// more character and carry on.
fn matches(pattern: &str, path: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let path: Vec<char> = path.chars().collect();
    let (mut p, mut s) = (0, 0);
    // Where to go back to: the pattern just after the star, how far the
    // star had eaten, and whether it is allowed to eat a separator.
    let mut star: Option<(usize, usize, bool)> = None;
    while s < path.len() {
        let taken = p < pattern.len()
            && match pattern[p] {
                '*' => {
                    let crosses = pattern.get(p + 1) == Some(&'*');
                    p += if crosses { 2 } else { 1 };
                    star = Some((p, s, crosses));
                    continue;
                }
                '?' if path[s] != '/' => {
                    p += 1;
                    s += 1;
                    continue;
                }
                '[' => match class(&pattern, p, path[s]) {
                    Some((next, true)) => {
                        p = next;
                        s += 1;
                        continue;
                    }
                    Some((_, false)) => false,
                    // A `[` with nothing closing it is a literal `[`,
                    // which is what the regular expression this is
                    // written from falls back to.
                    None => pattern[p] == path[s],
                },
                other => other == path[s],
            };
        if taken {
            p += 1;
            s += 1;
            continue;
        }
        // Nothing matched here, so the last star has to swallow one
        // more character, if there was one and if it may.
        match star {
            Some((after, at, crosses)) if crosses || path[at] != '/' => {
                p = after;
                s = at + 1;
                star = Some((after, s, crosses));
            }
            _ => return false,
        }
    }
    // The path is used up, so what is left of the pattern must be stars.
    while p < pattern.len() && pattern[p] == '*' {
        p += 1;
    }
    p == pattern.len()
}

/// One `[...]` class: where the pattern carries on, and whether the
/// character is in it. None for a `[` that never closes.
fn class(pattern: &[char], open: usize, against: char) -> Option<(usize, bool)> {
    let close = pattern[open + 1..].iter().position(|c| *c == ']')? + open + 1;
    // `[]` closes on nothing, which is a literal `[` and not a class,
    // and is the one case upstream's regular expression falls through.
    if close < open + 2 {
        return None;
    }
    let mut inside = &pattern[open + 1..close];
    let negated = inside.first() == Some(&'!');
    if negated {
        inside = &inside[1..];
    }
    let held = inside.contains(&against);
    Some((close + 1, held != negated))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn function(statics: &[&str]) -> Function {
        let mut function = Function::new("hello", PathBuf::from("/p/functions/hello/index.ts"));
        function.static_files = statics.iter().map(PathBuf::from).collect();
        function
    }

    #[test]
    fn a_star_stays_inside_one_segment_and_a_double_star_does_not() {
        assert!(matches("/p/*.html", "/p/index.html"));
        assert!(!matches("/p/*.html", "/p/deep/index.html"));
        assert!(matches("/p/**/*.html", "/p/deep/index.html"));
        assert!(matches("/p/**", "/p/deep/down/here.txt"));
        // The slash after a `**` is a slash the path has to have, so
        // `**/` is not "or nothing". Upstream builds `/p/.*/[^/]*` out
        // of this and answers the same way, and a glob library that
        // special cases `**/` would not.
        assert!(matches("/p/**/*.css", "/p/dist/one.css"));
        assert!(!matches("/p/**/*.css", "/p/one.css"));
        assert!(!matches("/p/*", "/p/deep/down"));
    }

    #[test]
    fn the_rest_of_the_pattern_is_upstreams_too() {
        assert!(matches("/p/page?.html", "/p/page1.html"));
        assert!(!matches("/p/page?.html", "/p/page10.html"));
        assert!(!matches("/p/page?.html", "/p/page/.html"));
        assert!(matches("/p/[abc]one.txt", "/p/bone.txt"));
        assert!(!matches("/p/[abc]one.txt", "/p/done.txt"));
        assert!(matches("/p/[!abc]one.txt", "/p/done.txt"));
        // A dot is a dot and not "any character", which is the half of
        // this that a regular expression gets wrong when it is built by
        // pasting the pattern into one.
        assert!(!matches("/p/index.html", "/p/indexXhtml"));
        assert!(matches("/p/a[b.txt", "/p/a[b.txt"));
    }

    #[test]
    fn a_star_that_has_to_give_a_character_back() {
        // The naive match eats "one.two.html" with the star and then
        // has nothing left for ".html".
        assert!(matches("/p/*.html", "/p/one.two.html"));
        assert!(matches("/p/**/x/*.ts", "/p/a/b/x/y.ts"));
        assert!(!matches("/p/**/x/*.ts", "/p/a/b/x/y/z.ts"));
    }

    #[test]
    fn a_relative_name_starts_at_the_function() {
        let statics = Statics::of(&function(&["/p/functions/hello/*.html"]));
        assert_eq!(
            statics.at("./index.html"),
            Ok(PathBuf::from("/p/functions/hello/index.html"))
        );
        assert_eq!(
            statics.at("index.html"),
            Ok(PathBuf::from("/p/functions/hello/index.html"))
        );
    }

    #[test]
    fn an_absolute_name_is_matched_as_it_is() {
        let statics = Statics::of(&function(&["/p/functions/hello/*.html"]));
        assert!(statics.at("/p/functions/hello/other.html").is_ok());
        assert!(statics.at("/etc/passwd").is_err());
    }

    #[test]
    fn a_name_cannot_walk_out_and_back_in() {
        let statics = Statics::of(&function(&["/p/functions/hello/*.html"]));
        // Tidied first, so the pattern sees where it actually points
        // rather than a path with the function's directory in it.
        assert!(statics.at("../secrets/../hello/index.html").is_ok());
        assert!(
            statics.at("../../.env").is_err(),
            "a name that leaves is refused by the patterns like any other"
        );
    }

    #[test]
    fn a_function_that_configured_nothing_reads_nothing() {
        let statics = Statics::of(&function(&[]));
        let why = statics.at("./index.html").expect_err("nothing is allowed");
        assert!(why.contains("hello"), "the refusal names the function");
        assert!(why.contains("static_files"), "and what would allow it");
    }
}
