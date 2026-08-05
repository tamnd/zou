//! The byte cursor and the low level pieces every zou-rest grammar
//! shares: bare and quoted names, json arrow paths, and the escaping
//! used to render them back.
//!
//! The filter and select grammars break bare names on different byte
//! sets, a colon is a delimiter in a select item but a plain byte in
//! a filter key, so the scanners here take the break predicate as an
//! argument instead of hard coding one.

use std::fmt;

/// Where and why a parse failed. `at` is a zero based byte offset
/// into whichever string was being read.
///
/// The shape is upstream's, which is parsec's: a position, the token
/// the grammar could not take there, and the set of things it would
/// have taken instead. A sentence would be easier to write and worse
/// to read, since what a client needs is the list of what was allowed
/// and not our opinion of what they meant, so the scanners carry the
/// list and `Display` renders it the way parsec's
/// `showErrorMessages` does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub found: Found,
    /// What the grammar would have taken, in the order it offers
    /// them, already quoted where parsec quotes them.
    pub expecting: Vec<String>,
    pub at: usize,
}

/// The token half of a parse error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Found {
    /// The input ran out.
    End,
    /// A byte no rule could take. parsec shows it the way it shows a
    /// one character string, in double quotes.
    Token(char),
    /// Input left over where the grammar wanted the end of it. This
    /// is `eof`'s own complaint rather than a rule's, and parsec
    /// shows the character in single quotes, which is the only
    /// visible difference between the two.
    Leftover(char),
}

impl fmt::Display for Found {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Found::End => write!(f, "end of input"),
            Found::Token(c) => write!(f, "\"{c}\""),
            Found::Leftover(c) => write!(f, "'{c}'"),
        }
    }
}

impl Error {
    /// Rename what was expected, when the rule that failed did so
    /// without taking anything.
    ///
    /// This is parsec's `<?>`, which relabels an alternative only
    /// while it is still at its own starting position: once a rule has
    /// taken a byte its own error is the specific one and a label over
    /// the top of it would say less.
    pub(crate) fn label(mut self, start: usize, expecting: &[&str]) -> Error {
        if self.at == start {
            self.expecting = expecting.iter().map(|s| s.to_string()).collect();
        }
        self
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unexpected {}", self.found)?;
        let mut seen: Vec<&str> = Vec::new();
        for want in &self.expecting {
            if !seen.contains(&want.as_str()) {
                seen.push(want);
            }
        }
        for (i, want) in seen.iter().enumerate() {
            let sep = match i {
                0 => " expecting ",
                _ if i + 1 == seen.len() => " or ",
                _ => ", ",
            };
            write!(f, "{sep}{want}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {}

/// A parse failure and the parameter it happened in, which is what
/// the message names. `skew` is how many characters the parser had
/// already read before `input` started, which is only ever nonzero
/// for a logic tree, where upstream parses the key's operator and the
/// value as one string and then quotes the value alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub what: &'static str,
    pub input: String,
    pub skew: usize,
    pub error: Error,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "failed to parse {} ({}): {}",
            self.what, self.input, self.error
        )
    }
}

/// A word as parsec quotes one, which is how every literal the
/// grammars expect is written in an error.
pub(crate) fn word(s: &str) -> String {
    format!("\"{s}\"")
}

/// The label of a name in any of the grammars.
pub(crate) const FIELD_NAME: &str = "field name (* or [a..z0..9_$])";

/// The label of a key inside a json path.
pub(crate) const JSON_KEY: &str = "any non reserved character different from: .,>()";

pub(crate) const DELIMITER: &str = "delimiter (.)";

pub(crate) const END: &str = "end of input";

pub(crate) const DIGIT: &str = "digit";

/// How far into `rest` a set of alternatives gets before none of them
/// can go on.
///
/// parsec keeps the furthest position any alternative reached, the
/// backtracked ones included, so a value that shares a prefix with one
/// of the words is reported at the byte where that word stopped
/// matching rather than at the word's start. `is.nil` is the one worth
/// remembering: it points at the `i`, because `null` got that far.
pub(crate) fn reach(rest: &str, words: &[&str]) -> usize {
    words
        .iter()
        .map(|w| {
            w.bytes()
                .zip(rest.bytes())
                .take_while(|(a, b)| a == b)
                .count()
        })
        .max()
        .unwrap_or(0)
}

/// One json arrow step hung off a column, `->key`, `->>key`, or an
/// array index in either flavor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonStep {
    /// True for `->>`, the text extraction arrow.
    pub text: bool,
    pub key: JsonKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonKey {
    Name(String),
    Index(i64),
}

pub(crate) struct Cur<'a> {
    pub(crate) s: &'a str,
    pub(crate) pos: usize,
}

impl<'a> Cur<'a> {
    pub(crate) fn new(s: &'a str) -> Self {
        Cur { s, pos: 0 }
    }

    pub(crate) fn peek(&self) -> Option<u8> {
        self.s.as_bytes().get(self.pos).copied()
    }

    pub(crate) fn bump(&mut self) {
        self.pos += 1;
    }

    pub(crate) fn done(&self) -> bool {
        self.pos >= self.s.len()
    }

    pub(crate) fn eat(&mut self, b: u8) -> bool {
        if self.peek() == Some(b) {
            self.bump();
            true
        } else {
            false
        }
    }

    pub(crate) fn eat_str(&mut self, w: &str) -> bool {
        if self.s.as_bytes()[self.pos..].starts_with(w.as_bytes()) {
            self.pos += w.len();
            true
        } else {
            false
        }
    }

    pub(crate) fn starts(&self, w: &str) -> bool {
        self.s.as_bytes()[self.pos..].starts_with(w.as_bytes())
    }

    /// Step over spaces and tabs.
    ///
    /// Only a grammar that allows them unconditionally may call this.
    /// A logic tree does, around every bracket and comma in it, which
    /// is what upstream's `lexeme` combinator means.
    pub(crate) fn skip_spaces(&mut self) {
        while matches!(self.peek(), Some(b' ') | Some(b'\t')) {
            self.bump();
        }
    }

    /// Step over spaces and tabs, but only when they lead to `b`.
    ///
    /// The conditional half is the point. Where the grammar allows
    /// spaces it allows them as part of a token that has to be there,
    /// so spaces leading to nothing are still the error they were.
    pub(crate) fn skip_spaces_before(&mut self, b: u8) {
        let mut at = self.pos;
        while matches!(self.s.as_bytes().get(at), Some(b' ') | Some(b'\t')) {
            at += 1;
        }
        if self.s.as_bytes().get(at) == Some(&b) {
            self.pos = at;
        }
    }

    /// The token at an offset, or the end of the input.
    pub(crate) fn found(&self, at: usize) -> Found {
        match self.s[at.min(self.s.len())..].chars().next() {
            None => Found::End,
            Some(c) => Found::Token(c),
        }
    }

    pub(crate) fn err<T>(&self, expecting: &[&str]) -> Result<T, Error> {
        self.err_at(self.pos, expecting)
    }

    pub(crate) fn err_at<T>(&self, at: usize, expecting: &[&str]) -> Result<T, Error> {
        Err(Error {
            found: self.found(at),
            expecting: expecting.iter().map(|s| s.to_string()).collect(),
            at,
        })
    }

    /// The error for a set of alternatives written `try (string w)`,
    /// none of which matched.
    ///
    /// Upstream reports all of these at the position the attempt
    /// started, since a word is taken or not taken as one thing, and
    /// where they all report the same position the character named is
    /// the one that broke the alternative offered first. So `id.ac`
    /// points at the `a` and complains about the `c`, which is where
    /// `asc` stopped, and `id.nulsfist` points at the same place and
    /// complains about the `n`, which is where `asc` stopped as well.
    pub(crate) fn err_word<T>(
        &self,
        at: usize,
        words: &[&str],
        expecting: &[&str],
    ) -> Result<T, Error> {
        let found = self.found(at + reach(&self.s[at.min(self.s.len())..], &words[..1]));
        Err(Error {
            found,
            expecting: expecting.iter().map(|s| s.to_string()).collect(),
            at,
        })
    }

    /// The same for alternatives that are matched a character at a
    /// time and without regard to case, upstream's `ciString`, where
    /// the position does follow the match: `is.nil` points at the `i`
    /// rather than at the `n`, because `null` got that far.
    pub(crate) fn err_ci_word<T>(
        &self,
        at: usize,
        words: &[&str],
        expecting: &[&str],
    ) -> Result<T, Error> {
        let rest = self.s[at.min(self.s.len())..].to_ascii_lowercase();
        self.err_at(at + reach(&rest, words), expecting)
    }

    /// The error for input left where the grammar wanted the end of
    /// it, which quotes the character the other way.
    pub(crate) fn leftover<T>(&self, expecting: &[&str]) -> Result<T, Error> {
        let found = match self.found(self.pos) {
            Found::Token(c) => Found::Leftover(c),
            other => other,
        };
        Err(Error {
            found,
            expecting: expecting.iter().map(|s| s.to_string()).collect(),
            at: self.pos,
        })
    }

    pub(crate) fn expect(&mut self, b: u8) -> Result<(), Error> {
        let quoted = word(&(b as char).to_string());
        self.expect_as(b, &[&quoted])
    }

    /// The same for a byte the grammar has a name for, the way `.`
    /// between an operator and its value is upstream's delimiter
    /// rather than a full stop.
    pub(crate) fn expect_as(&mut self, b: u8, expecting: &[&str]) -> Result<(), Error> {
        if self.eat(b) {
            Ok(())
        } else {
            self.err(expecting)
        }
    }

    pub(crate) fn take_rest(&mut self) -> String {
        let out = self.s[self.pos..].to_string();
        self.pos = self.s.len();
        out
    }
}

/// Scan a bare name up to the next break byte. The scan also stops at
/// `->`, handled separately so a lone dash stays part of the name.
pub(crate) fn scan_name(cur: &mut Cur, brk: fn(u8) -> bool) -> String {
    let start = cur.pos;
    while let Some(b) = cur.peek() {
        if brk(b) || (b == b'-' && cur.starts("->")) {
            break;
        }
        cur.bump();
    }
    cur.s[start..cur.pos].to_string()
}

pub(crate) fn scan_quoted(cur: &mut Cur) -> Result<String, Error> {
    cur.expect(b'"')?;
    let mut out = Vec::new();
    loop {
        match cur.peek() {
            None => return cur.err(&[&word("\"")]),
            Some(b'"') => {
                cur.bump();
                break;
            }
            Some(b'\\') => {
                cur.bump();
                match cur.peek() {
                    None => return cur.err(&[&word("\"")]),
                    Some(b) => {
                        out.push(b);
                        cur.bump();
                    }
                }
            }
            Some(b) => {
                out.push(b);
                cur.bump();
            }
        }
    }
    // Only ascii delimiters split the input, so the bytes between
    // them are still the valid utf8 they arrived as.
    Ok(String::from_utf8(out).expect("splits happen at ascii bytes"))
}

/// The arrow chain after a column name, empty when the next bytes are
/// not `->`.
///
/// A key inside the chain is read by its own rule rather than by the
/// grammar the column name came from. Upstream gives it one, and it
/// is a wider rule than any name's: `!` and `@` and `#` are ordinary
/// characters in a json key and only the six the grammar has other
/// uses for end one.
pub(crate) fn parse_json_path(cur: &mut Cur) -> Result<Vec<JsonStep>, Error> {
    let mut path = Vec::new();
    while cur.eat_str("->") {
        let text = cur.eat(b'>');
        let key = parse_json_key(cur)?;
        path.push(JsonStep { text, key });
    }
    Ok(path)
}

/// Bytes that end a piece of a json path key, upstream's
/// `noneOf "(-:.,>)"`. The dash is handled a step up, since a dash
/// only ends a key when it is the head of the next arrow.
fn json_key_break(b: u8) -> bool {
    matches!(b, b'(' | b')' | b':' | b'.' | b',' | b'>')
}

fn parse_json_key(cur: &mut Cur) -> Result<JsonKey, Error> {
    if let Some(n) = json_index(cur) {
        return Ok(JsonKey::Index(n));
    }
    // A sign is the front of an index and nothing else, since a dash
    // cannot start a key. So once it is there the failure is the
    // index's rather than the key's, and it is reported past the sign
    // where the number should have been.
    if cur.peek() == Some(b'-') {
        let digits = cur.pos + 1;
        let mut at = digits;
        while cur.s.as_bytes().get(at).is_some_and(u8::is_ascii_digit) {
            at += 1;
        }
        if at == digits {
            return cur.err_at(digits, &[DIGIT]);
        }
        return cur.err_at(
            at,
            &[DIGIT, &word("->"), &word("::"), &word("."), &word(","), END],
        );
    }
    if cur.peek() == Some(b'"') {
        return Ok(JsonKey::Name(scan_quoted(cur)?));
    }
    let mut parts = Vec::new();
    loop {
        let start = cur.pos;
        while cur.peek().is_some_and(|b| !json_key_break(b) && b != b'-') {
            cur.bump();
        }
        if cur.pos == start {
            // A key that has already taken a piece and a dash is past
            // the point where naming what a key looks like would say
            // anything, the same way a label stops applying upstream
            // once the rule under it has consumed input.
            if parts.is_empty() {
                return cur.err(&[&word("-"), DIGIT, JSON_KEY]);
            }
            return cur.err(&[]);
        }
        parts.push(cur.s[start..cur.pos].trim_matches([' ', '\t']));
        // A dash is part of the key and joins two pieces of it, which
        // is how `23-xy-45` stays one key, unless it is the head of
        // the arrow that starts the next step.
        if cur.peek() == Some(b'-') && !cur.starts("->") {
            cur.bump();
            continue;
        }
        return Ok(JsonKey::Name(parts.join("-")));
    }
}

/// The index a step reads as a number, `None` when the step is a key.
///
/// Digits alone are not an index. Upstream asks for what follows them
/// as well, `->`, `::`, `.`, `,` or nothing, so `0xy1` is the key
/// `0xy1` rather than the index 0 with a key stuck to it, and only a
/// well formed number counts.
fn json_index(cur: &mut Cur) -> Option<i64> {
    let bytes = cur.s.as_bytes();
    let mut at = cur.pos;
    if bytes.get(at) == Some(&b'-') {
        at += 1;
    }
    let digits = at;
    while bytes.get(at).is_some_and(|b| b.is_ascii_digit()) {
        at += 1;
    }
    if at == digits {
        return None;
    }
    let rest = &cur.s[at..];
    let ends = rest.is_empty()
        || rest.starts_with("->")
        || rest.starts_with("::")
        || rest.starts_with('.')
        || rest.starts_with(',');
    if !ends {
        return None;
    }
    let n = cur.s[cur.pos..at].parse::<i64>().ok()?;
    cur.pos = at;
    Some(n)
}

pub(crate) fn fmt_path(f: &mut fmt::Formatter<'_>, path: &[JsonStep]) -> fmt::Result {
    for step in path {
        write!(f, "->{}", if step.text { ">" } else { "" })?;
        match &step.key {
            JsonKey::Name(n) => write!(f, "{n}")?,
            JsonKey::Index(i) => write!(f, "{i}")?,
        }
    }
    Ok(())
}

pub(crate) fn write_escaped(f: &mut fmt::Formatter<'_>, s: &str) -> fmt::Result {
    write!(f, "\"")?;
    for c in s.chars() {
        if c == '"' || c == '\\' {
            write!(f, "\\")?;
        }
        write!(f, "{c}")?;
    }
    write!(f, "\"")
}
