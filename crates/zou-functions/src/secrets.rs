//! The variables a project's own functions see, and where they come
//! from.
//!
//! Upstream has two places and one rule. The places are
//! `[edge_runtime.secrets]` in `config.toml`, which is names and values
//! with `env(NAME)` for anything that should not be committed, and
//! `supabase/functions/.env`, which is a dotenv file and is what
//! `--env-file` defaults to when nobody passes one. The rule is that
//! the file wins over the block, because `supabase secrets set` copies
//! the block into a map and then copies the file over the top of it.
//!
//! A name starting with `SUPABASE_` is dropped and said out loud, which
//! is upstream's `Env name cannot start with SUPABASE_, skipping:`.
//! The four variables that name a project are the server's to set and a
//! function that could be handed a different `SUPABASE_URL` than the
//! one it is running against is a function nobody can reason about.
//!
//! Nothing of the host's own environment arrives here on its own. The
//! CLI hands its container whatever the shell had and then the main
//! worker throws out `HOME`, `HOSTNAME`, `PATH` and `PWD` on the way to
//! the function, which is a container's four and not a developer's
//! hundred. This server has no container in the middle, so the whole
//! process environment stays where it is and the only way anything of
//! it reaches a function is a project writing `env(NAME)` in its own
//! config file and meaning it.
//!
//! # The dotenv format
//!
//! Upstream parses with `godotenv`, so this does what `godotenv` does
//! rather than what a reasonable person would invent:
//!
//! - `NAME=value`, and `NAME: value` too, because that library takes
//!   the yaml spelling as well.
//! - `export NAME=value`, so a file that can also be sourced by a shell
//!   is read the same way here.
//! - A line whose first character is `#` is a comment. An unquoted
//!   value ends at a `#` that has a space before it, so
//!   `NAME=value # why` is `value`.
//! - `'single quotes'` are literal, all the way to the closing quote,
//!   newlines included.
//! - `"double quotes"` unescape `\n` and `\r`, drop the backslash from
//!   any other escape, and expand variables. They may also span lines.
//! - `$NAME` and `${NAME}` in an unquoted or double quoted value are
//!   replaced by what an earlier line of the same file set, and by
//!   nothing at all when no earlier line set it. Not by the host's
//!   environment: this is the file talking to itself. `\$` is a dollar
//!   sign, and the name has to be upper case, digits and underscores,
//!   which is the library's regular expression rather than a choice.
//! - A quote that never closes is an error, and so is a name with a
//!   character in it that is not a letter, a digit, `_` or `.`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::Layout;

/// Upstream's prefix, which a project may not set for itself.
const RESERVED: &str = "SUPABASE_";

/// Where the CLI looks when nobody passed `--env-file`, relative to the
/// directory the config file is in.
pub fn env_file(dir: &Path) -> PathBuf {
    dir.join("functions").join(".env")
}

/// Everything the project asked its functions to see, the block first
/// and the file over it.
///
/// The names this server sets itself are not in here. They are added
/// afterwards by whatever is starting the runtime, so a project cannot
/// arrange to be told a different `SUPABASE_URL` than the one it is
/// answering on.
pub fn read(dir: &Path, layout: &Layout) -> Result<Vec<(String, String)>, String> {
    from(&env_file(dir), layout)
}

/// The same, with the dotenv file named rather than found beside the
/// functions, which is upstream's `--env-file`.
///
/// A file that was asked for by name and is not there is still not an
/// error here, the same as the one that was not asked for. It is the
/// caller's to complain about, because a dev loop that is watching the
/// disk is watching for that file to be written.
pub fn from(path: &Path, layout: &Layout) -> Result<Vec<(String, String)>, String> {
    let mut out: BTreeMap<String, String> = layout.secrets.clone();
    match std::fs::read_to_string(path) {
        Ok(text) => {
            let parsed = parse(&text).map_err(|e| format!("{}: {e}", path.display()))?;
            out.extend(parsed);
        }
        // A project without one is most projects.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(format!("read {}: {e}", path.display())),
    }
    let mut kept = Vec::new();
    for (name, value) in out {
        if name.starts_with(RESERVED) {
            log::warn!("env name cannot start with {RESERVED}, skipping: {name}");
            continue;
        }
        kept.push((name, value));
    }
    Ok(kept)
}

/// One dotenv file, in the order a later line overwrites an earlier
/// one.
///
/// Public because `zou secrets set --env-file` reads the same format,
/// and a project that has been running against a file locally should be
/// able to hand that exact file to a deployment without finding out
/// that two parsers disagree about it.
pub fn dotenv(text: &str) -> Result<BTreeMap<String, String>, String> {
    parse(text)
}

pub(crate) fn parse(text: &str) -> Result<BTreeMap<String, String>, String> {
    let text = text.replace("\r\n", "\n");
    let src: Vec<char> = text.chars().collect();
    let mut out = BTreeMap::new();
    let mut at = 0;
    loop {
        let Some(start) = statement(&src, at) else {
            return Ok(out);
        };
        at = start;
        let (name, next) = key(&src, at)?;
        let (value, next) = value(&src, next, &out)?;
        out.insert(name, value);
        at = next;
    }
}

/// Where the next name begins, skipping blank lines and comments.
fn statement(src: &[char], mut at: usize) -> Option<usize> {
    loop {
        while at < src.len() && src[at].is_whitespace() {
            at += 1;
        }
        if at >= src.len() {
            return None;
        }
        if src[at] != '#' {
            return Some(at);
        }
        while at < src.len() && src[at] != '\n' {
            at += 1;
        }
    }
}

/// The name, and where its value starts.
fn key(src: &[char], mut at: usize) -> Result<(String, usize), String> {
    while at < src.len() && space(src[at]) {
        at += 1;
    }
    // `export NAME=value` is a file that can be sourced as well as
    // read, and the word is only a prefix when a space follows it.
    if src[at..].starts_with(&['e', 'x', 'p', 'o', 'r', 't']) {
        let after = at + 6;
        if src.get(after).copied().is_some_and(space) {
            at = after;
            while at < src.len() && space(src[at]) {
                at += 1;
            }
        }
    }
    let mut name = String::new();
    let mut cursor = at;
    while cursor < src.len() {
        let c = src[cursor];
        cursor += 1;
        if c == '=' || c == ':' {
            let name = name.trim_end().to_string();
            if name.is_empty() {
                return Err("a value with no name".to_string());
            }
            return Ok((name, cursor));
        }
        if space(c) {
            name.push(c);
            continue;
        }
        if c.is_alphanumeric() || c == '_' || c == '.' {
            name.push(c);
            continue;
        }
        return Err(format!("unexpected character {c:?} in variable name"));
    }
    Err(format!("{} has no value", name.trim_end()))
}

/// The value, and where the line after it starts.
fn value(
    src: &[char],
    mut at: usize,
    seen: &BTreeMap<String, String>,
) -> Result<(String, usize), String> {
    while at < src.len() && space(src[at]) {
        at += 1;
    }
    let quote = src.get(at).copied().filter(|c| *c == '"' || *c == '\'');
    let Some(quote) = quote else {
        let end = src[at..]
            .iter()
            .position(|c| *c == '\n' || *c == '\r')
            .map_or(src.len(), |n| at + n);
        let line: Vec<char> = src[at..end].to_vec();
        // Backwards, because what ends the value is the last thing that
        // looks like the start of a comment and not the first `#` in
        // it: `NAME=a#b # why` is `a#b`.
        let mut stop = line.len();
        for i in (1..line.len()).rev() {
            if line[i] == '#' && space(line[i - 1]) {
                stop = i;
                break;
            }
        }
        let raw: String = line[..stop].iter().collect();
        return Ok((expand(raw.trim_matches(space), seen), end));
    };
    let mut cursor = at + 1;
    while cursor < src.len() {
        if src[cursor] == quote && src[cursor - 1] != '\\' {
            let raw: String = src[at + 1..cursor].iter().collect();
            let value = match quote {
                '"' => expand(&escapes(&raw), seen),
                _ => raw,
            };
            return Ok((value, cursor + 1));
        }
        cursor += 1;
    }
    Err(format!("unterminated quoted value {quote}"))
}

/// What a double quoted value does to its backslashes: `\n` and `\r`
/// are the characters they name, and every other escape is the
/// character it was escaping, except a dollar sign, which keeps its
/// backslash for the expansion below to see.
fn escapes(raw: &str) -> String {
    let mut out = String::new();
    let mut chars = raw.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('$') => out.push_str("\\$"),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    out
}

/// `$NAME` and `${NAME}`, from what the file has already said.
///
/// Upper case, digits and underscore, because that is the library's
/// regular expression and a file that works there should work here. A
/// name nothing set is nothing at all rather than left as it was, which
/// is also the library's answer and is the one that keeps a secret that
/// was never set out of a log line.
fn expand(raw: &str, seen: &BTreeMap<String, String>) -> String {
    let mut out = String::new();
    let src: Vec<char> = raw.chars().collect();
    let mut at = 0;
    while at < src.len() {
        if src[at] == '\\' && src.get(at + 1) == Some(&'$') {
            out.push('$');
            at += 2;
            continue;
        }
        if src[at] != '$' {
            out.push(src[at]);
            at += 1;
            continue;
        }
        let mut cursor = at + 1;
        let braced = src.get(cursor) == Some(&'{');
        if braced {
            cursor += 1;
        }
        let mut name = String::new();
        while let Some(c) = src.get(cursor) {
            if c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_' {
                name.push(*c);
                cursor += 1;
                continue;
            }
            break;
        }
        if braced && src.get(cursor) == Some(&'}') {
            cursor += 1;
        }
        if name.is_empty() {
            out.push('$');
            at += 1;
            continue;
        }
        out.push_str(seen.get(&name).map_or("", String::as_str));
        at = cursor;
    }
    out
}

/// A space that is not the end of a line, which is the distinction the
/// library makes and the reason a value can be trimmed without eating
/// the newline after it.
fn space(c: char) -> bool {
    matches!(
        c,
        '\t' | '\u{b}' | '\u{c}' | '\r' | ' ' | '\u{85}' | '\u{a0}'
    )
}

/// Every expectation below was run through `godotenv` 1.5.1 itself,
/// the version the pinned CLI depends on, and the answers here are the
/// answers it gave rather than the answers its source reads like it
/// would give. The two disagreed about nothing, `"a\tb"` included,
/// which that library turns into `atb` rather than a tab.
#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(text: &str) -> BTreeMap<String, String> {
        parse(text).expect("a dotenv file")
    }

    fn one(text: &str) -> String {
        parsed(text).remove("NAME").expect("NAME")
    }

    #[test]
    fn a_pair_per_line() {
        let env = parsed("NAME=value\nOTHER=second\n");
        assert_eq!(env["NAME"], "value");
        assert_eq!(env["OTHER"], "second");
    }

    #[test]
    fn the_shapes_a_line_can_have() {
        assert_eq!(one("NAME=value"), "value");
        assert_eq!(one("NAME = value"), "value");
        assert_eq!(one("NAME: value"), "value");
        assert_eq!(one("export NAME=value"), "value");
        assert_eq!(one("NAME=value\n"), "value");
        assert_eq!(one("NAME="), "");
        assert_eq!(one("NAME=one two three"), "one two three");
        assert_eq!(one("NAME=trailing   "), "trailing");
        // The word is a prefix only when a space follows it, so this is
        // a variable somebody named badly and not an export.
        assert_eq!(parsed("exportNAME=value")["exportNAME"], "value");
    }

    #[test]
    fn comments_and_blank_lines_are_not_settings() {
        let env = parsed("# a comment\n\nNAME=value\n   # another\nOTHER=second\n");
        assert_eq!(env.len(), 2);
        assert_eq!(env["NAME"], "value");
        assert_eq!(env["OTHER"], "second");
    }

    /// A comment ends an unquoted value only when there is a space in
    /// front of it, so a value with a hash in it survives.
    #[test]
    fn a_comment_after_a_value_is_not_part_of_it() {
        assert_eq!(one("NAME=value # why"), "value");
        assert_eq!(one("NAME=a#b"), "a#b");
        assert_eq!(one("NAME=a#b # why"), "a#b");
        assert_eq!(
            one("NAME=\"value # not a comment\""),
            "value # not a comment"
        );
    }

    #[test]
    fn quotes_are_not_part_of_the_value() {
        assert_eq!(one("NAME=\"value\""), "value");
        assert_eq!(one("NAME='value'"), "value");
        assert_eq!(one("NAME=\"  spaces  \""), "  spaces  ");
    }

    #[test]
    fn double_quotes_unescape_and_single_quotes_do_not() {
        assert_eq!(one("NAME=\"one\\ntwo\""), "one\ntwo");
        assert_eq!(one("NAME='one\\ntwo'"), "one\\ntwo");
        assert_eq!(one("NAME=\"a \\\"quoted\\\" word\""), "a \"quoted\" word");
        // Two escapes and no more, so this is not a tab, it is a `t`
        // that lost its backslash on the way through.
        assert_eq!(one("NAME=\"a\\tb\""), "atb");
    }

    #[test]
    fn a_quoted_value_may_span_lines() {
        let key = "NAME=\"-----BEGIN KEY-----\nabc\n-----END KEY-----\"\nOTHER=second\n";
        let env = parsed(key);
        assert_eq!(env["NAME"], "-----BEGIN KEY-----\nabc\n-----END KEY-----");
        assert_eq!(env["OTHER"], "second");
    }

    #[test]
    fn a_value_can_be_built_out_of_the_ones_above_it() {
        let env = parsed("HOST=example.com\nNAME=https://$HOST/path\nBRACED=\"${HOST}:443\"\n");
        assert_eq!(env["NAME"], "https://example.com/path");
        assert_eq!(env["BRACED"], "example.com:443");
    }

    /// The three ways an expansion is not one, all of them the
    /// library's own answers.
    #[test]
    fn what_is_not_an_expansion() {
        // Nothing set it, so it is nothing.
        assert_eq!(one("NAME=$NOBODY_SET_THIS"), "");
        // An escaped dollar sign is a dollar sign.
        assert_eq!(one("NAME=\"\\$5.00\""), "$5.00");
        // Lower case is not a variable name.
        assert_eq!(one("NAME=$lower"), "$lower");
        // A single quoted value is never expanded.
        assert_eq!(one("HOST=example.com\nNAME='$HOST'"), "$HOST");
    }

    #[test]
    fn a_later_line_wins() {
        assert_eq!(one("NAME=first\nNAME=second\n"), "second");
    }

    #[test]
    fn a_file_that_is_not_one_says_so() {
        let why = parse("NAME=\"unterminated\n").expect_err("a complaint");
        assert!(why.contains("unterminated"), "{why}");
        let why = parse("NA-ME=value\n").expect_err("a complaint");
        assert!(why.contains("variable name"), "{why}");
    }

    #[test]
    fn the_file_wins_over_the_block() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        std::fs::create_dir_all(dir.path().join("functions")).expect("the functions directory");
        std::fs::write(env_file(dir.path()), "FROM_FILE=file\nBOTH=file\n").expect("the file");
        let layout = Layout {
            secrets: BTreeMap::from([
                ("FROM_BLOCK".to_string(), "block".to_string()),
                ("BOTH".to_string(), "block".to_string()),
            ]),
            ..Layout::default()
        };
        let env = read(dir.path(), &layout).expect("the secrets");
        assert_eq!(
            env,
            vec![
                ("BOTH".to_string(), "file".to_string()),
                ("FROM_BLOCK".to_string(), "block".to_string()),
                ("FROM_FILE".to_string(), "file".to_string()),
            ]
        );
    }

    #[test]
    fn a_project_cannot_set_a_name_the_server_owns() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        std::fs::create_dir_all(dir.path().join("functions")).expect("the functions directory");
        std::fs::write(
            env_file(dir.path()),
            "SUPABASE_URL=https://elsewhere.example\nMINE=kept\n",
        )
        .expect("the file");
        let env = read(dir.path(), &Layout::default()).expect("the secrets");
        assert_eq!(env, vec![("MINE".to_string(), "kept".to_string())]);
    }

    #[test]
    fn a_project_with_no_env_file_has_the_block_and_nothing_else() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let layout = Layout {
            secrets: BTreeMap::from([("ONLY".to_string(), "block".to_string())]),
            ..Layout::default()
        };
        let env = read(dir.path(), &layout).expect("the secrets");
        assert_eq!(env, vec![("ONLY".to_string(), "block".to_string())]);
    }

    /// The whole of the host's environment is right here in this
    /// process, and none of it is a project's to see.
    #[test]
    fn nothing_of_the_hosts_own_environment_arrives() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let env = read(dir.path(), &Layout::default()).expect("the secrets");
        assert!(env.is_empty(), "{env:?}");
    }
}
