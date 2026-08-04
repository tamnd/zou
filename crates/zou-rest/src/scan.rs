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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub message: String,
    pub at: usize,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at offset {}", self.message, self.at)
    }
}

impl std::error::Error for Error {}

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

    pub(crate) fn err<T>(&self, message: impl Into<String>) -> Result<T, Error> {
        Err(Error {
            message: message.into(),
            at: self.pos,
        })
    }

    pub(crate) fn expect(&mut self, b: u8, what: &str) -> Result<(), Error> {
        if self.eat(b) { Ok(()) } else { self.err(what) }
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
    cur.expect(b'"', "expected an opening quote")?;
    let mut out = Vec::new();
    loop {
        match cur.peek() {
            None => return cur.err("unterminated quoted string"),
            Some(b'"') => {
                cur.bump();
                break;
            }
            Some(b'\\') => {
                cur.bump();
                match cur.peek() {
                    None => return cur.err("unterminated quoted string"),
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
            return cur.err("expected a json path key");
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
