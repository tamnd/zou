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
pub(crate) fn parse_json_path(cur: &mut Cur, brk: fn(u8) -> bool) -> Result<Vec<JsonStep>, Error> {
    let mut path = Vec::new();
    while cur.eat_str("->") {
        let text = cur.eat(b'>');
        let key = parse_json_key(cur, brk)?;
        path.push(JsonStep { text, key });
    }
    Ok(path)
}

fn parse_json_key(cur: &mut Cur, brk: fn(u8) -> bool) -> Result<JsonKey, Error> {
    let bytes = cur.s.as_bytes();
    let digit_at = |i: usize| bytes.get(i).is_some_and(|b| b.is_ascii_digit());
    let index = digit_at(cur.pos) || (cur.peek() == Some(b'-') && digit_at(cur.pos + 1));
    if index {
        let start = cur.pos;
        if cur.peek() == Some(b'-') {
            cur.bump();
        }
        while cur.peek().is_some_and(|b| b.is_ascii_digit()) {
            cur.bump();
        }
        return match cur.s[start..cur.pos].parse::<i64>() {
            Ok(n) => Ok(JsonKey::Index(n)),
            Err(_) => cur.err("json index out of range"),
        };
    }
    let name = scan_name(cur, brk);
    if name.is_empty() {
        return cur.err("expected a json path key");
    }
    Ok(JsonKey::Name(name))
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
