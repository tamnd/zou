//! What Go says about a body that is not json.
//!
//! GoTrue reads a request body and hands it to `json.Unmarshal`, and
//! when that refuses it puts the error straight into the message the
//! client reads: "Could not parse request body as JSON: " followed by
//! whatever `encoding/json` had to say. So the sentence a client sees
//! is not GoTrue's, it is Go's scanner naming the byte it choked on
//! and the state it was in when it did.
//!
//! serde decides whether a body parses. This module is only asked the
//! second question, once serde has already refused: what would Go have
//! called it. It is Go's scanner rewritten state for state, including
//! the way `quoteChar` writes the offending byte, because half of the
//! message is that quoting.
//!
//! The two parsers do not draw the line in exactly the same place. Go
//! scans bytes and never looks at utf-8; serde rejects a string with
//! bytes that are not utf-8, and refuses to nest deeper than its own
//! limit. When this scanner finds nothing wrong with a body serde has
//! already refused, it says so with None, and the caller falls back to
//! the sentence without a colon.

/// The parse states, one per state function in Go's scanner.go. The
/// names are Go's, minus the `state` prefix.
#[derive(Clone, Copy)]
enum Step {
    BeginValue,
    BeginValueOrEmpty,
    BeginString,
    BeginStringOrEmpty,
    EndValue,
    EndTop,
    InString,
    InStringEsc,
    /// How many of the four hex digits of a \u escape are still owed.
    InStringEscU(u8),
    /// One of true, false or null, and how much of it has been read.
    Lit(&'static [u8], usize),
    Neg,
    Zero,
    Num,
    Dot,
    DotDigits,
    Exp,
    ExpSign,
    ExpDigits,
}

/// What is open around the value being read, which is what decides
/// whether a comma is allowed and what closes it. Go's parseState.
#[derive(Clone, Copy)]
enum Open {
    ObjectKey,
    ObjectValue,
    ArrayValue,
}

/// Go's error for a body it could not scan, or None when Go would have
/// scanned it happily and the refusal came from somewhere serde looks
/// and Go does not.
pub(crate) fn syntax(input: &[u8]) -> Option<String> {
    let mut scan = Scan {
        step: Step::BeginValue,
        open: Vec::new(),
        top: false,
    };
    for &c in input {
        if let Err(msg) = scan.byte(c) {
            return Some(msg);
        }
    }
    // Go's scanner.eof: feed one space, and if that does not finish the
    // top level value the input simply stopped in the middle.
    if scan.top {
        return None;
    }
    if let Err(msg) = scan.byte(b' ') {
        return Some(msg);
    }
    if scan.top {
        return None;
    }
    Some("unexpected end of JSON input".to_string())
}

struct Scan {
    step: Step,
    open: Vec<Open>,
    top: bool,
}

impl Scan {
    fn byte(&mut self, c: u8) -> Result<(), String> {
        loop {
            match self.step {
                Step::BeginValueOrEmpty => {
                    if is_space(c) {
                        return Ok(());
                    }
                    if c == b']' {
                        self.step = Step::EndValue;
                        continue;
                    }
                    self.step = Step::BeginValue;
                    continue;
                }
                Step::BeginValue => return self.begin_value(c),
                Step::BeginStringOrEmpty => {
                    if is_space(c) {
                        return Ok(());
                    }
                    if c == b'}' {
                        // The object is empty, so what is open is no
                        // longer a key but a value that just ended.
                        if let Some(top) = self.open.last_mut() {
                            *top = Open::ObjectValue;
                        }
                        self.step = Step::EndValue;
                        continue;
                    }
                    self.step = Step::BeginString;
                    continue;
                }
                Step::BeginString => {
                    if is_space(c) {
                        return Ok(());
                    }
                    if c == b'"' {
                        self.step = Step::InString;
                        return Ok(());
                    }
                    return Err(bad(c, "looking for beginning of object key string"));
                }
                Step::EndValue => return self.end_value(c),
                Step::EndTop => {
                    if is_space(c) {
                        return Ok(());
                    }
                    return Err(bad(c, "after top-level value"));
                }
                Step::InString => {
                    if c == b'"' {
                        self.step = Step::EndValue;
                        return Ok(());
                    }
                    if c == b'\\' {
                        self.step = Step::InStringEsc;
                        return Ok(());
                    }
                    if c < 0x20 {
                        return Err(bad(c, "in string literal"));
                    }
                    return Ok(());
                }
                Step::InStringEsc => {
                    match c {
                        b'b' | b'f' | b'n' | b'r' | b't' | b'\\' | b'/' | b'"' => {
                            self.step = Step::InString;
                        }
                        b'u' => self.step = Step::InStringEscU(4),
                        _ => return Err(bad(c, "in string escape code")),
                    }
                    return Ok(());
                }
                Step::InStringEscU(owed) => {
                    if !c.is_ascii_hexdigit() {
                        return Err(bad(c, "in \\u hexadecimal character escape"));
                    }
                    self.step = if owed == 1 {
                        Step::InString
                    } else {
                        Step::InStringEscU(owed - 1)
                    };
                    return Ok(());
                }
                Step::Lit(word, read) => {
                    let want = word[read];
                    if c != want {
                        let word = std::str::from_utf8(word).unwrap_or_default();
                        return Err(bad(
                            c,
                            &format!("in literal {word} (expecting '{}')", want as char),
                        ));
                    }
                    self.step = if read + 1 == word.len() {
                        Step::EndValue
                    } else {
                        Step::Lit(word, read + 1)
                    };
                    return Ok(());
                }
                Step::Neg => {
                    if c == b'0' {
                        self.step = Step::Zero;
                        return Ok(());
                    }
                    if c.is_ascii_digit() {
                        self.step = Step::Num;
                        return Ok(());
                    }
                    return Err(bad(c, "in numeric literal"));
                }
                Step::Num => {
                    if c.is_ascii_digit() {
                        return Ok(());
                    }
                    self.step = Step::Zero;
                    continue;
                }
                Step::Zero => {
                    if c == b'.' {
                        self.step = Step::Dot;
                        return Ok(());
                    }
                    if c == b'e' || c == b'E' {
                        self.step = Step::Exp;
                        return Ok(());
                    }
                    self.step = Step::EndValue;
                    continue;
                }
                Step::Dot => {
                    if c.is_ascii_digit() {
                        self.step = Step::DotDigits;
                        return Ok(());
                    }
                    return Err(bad(c, "after decimal point in numeric literal"));
                }
                Step::DotDigits => {
                    if c.is_ascii_digit() {
                        return Ok(());
                    }
                    if c == b'e' || c == b'E' {
                        self.step = Step::Exp;
                        return Ok(());
                    }
                    self.step = Step::EndValue;
                    continue;
                }
                Step::Exp => {
                    if c == b'+' || c == b'-' {
                        self.step = Step::ExpSign;
                        return Ok(());
                    }
                    if c.is_ascii_digit() {
                        self.step = Step::ExpDigits;
                        return Ok(());
                    }
                    return Err(bad(c, "in exponent of numeric literal"));
                }
                Step::ExpSign => {
                    if c.is_ascii_digit() {
                        self.step = Step::ExpDigits;
                        return Ok(());
                    }
                    return Err(bad(c, "in exponent of numeric literal"));
                }
                Step::ExpDigits => {
                    if c.is_ascii_digit() {
                        return Ok(());
                    }
                    self.step = Step::EndValue;
                    continue;
                }
            }
        }
    }

    fn begin_value(&mut self, c: u8) -> Result<(), String> {
        if is_space(c) {
            return Ok(());
        }
        match c {
            b'{' => {
                self.open.push(Open::ObjectKey);
                self.step = Step::BeginStringOrEmpty;
            }
            b'[' => {
                self.open.push(Open::ArrayValue);
                self.step = Step::BeginValueOrEmpty;
            }
            b'"' => self.step = Step::InString,
            b'-' => self.step = Step::Neg,
            b'0' => self.step = Step::Zero,
            b'1'..=b'9' => self.step = Step::Num,
            b't' => self.step = Step::Lit(b"true", 1),
            b'f' => self.step = Step::Lit(b"false", 1),
            b'n' => self.step = Step::Lit(b"null", 1),
            _ => return Err(bad(c, "looking for beginning of value")),
        }
        Ok(())
    }

    fn end_value(&mut self, c: u8) -> Result<(), String> {
        let Some(&open) = self.open.last() else {
            self.step = Step::EndTop;
            self.top = true;
            if is_space(c) {
                return Ok(());
            }
            return Err(bad(c, "after top-level value"));
        };
        if is_space(c) {
            return Ok(());
        }
        match open {
            Open::ObjectKey => {
                if c == b':' {
                    *self.open.last_mut().expect("just read") = Open::ObjectValue;
                    self.step = Step::BeginValue;
                    return Ok(());
                }
                Err(bad(c, "after object key"))
            }
            Open::ObjectValue => {
                if c == b',' {
                    *self.open.last_mut().expect("just read") = Open::ObjectKey;
                    self.step = Step::BeginString;
                    return Ok(());
                }
                if c == b'}' {
                    self.open.pop();
                    self.step = Step::EndValue;
                    return Ok(());
                }
                Err(bad(c, "after object key:value pair"))
            }
            Open::ArrayValue => {
                if c == b',' {
                    self.step = Step::BeginValue;
                    return Ok(());
                }
                if c == b']' {
                    self.open.pop();
                    self.step = Step::EndValue;
                    return Ok(());
                }
                Err(bad(c, "after array element"))
            }
        }
    }
}

/// Go's scanner.error: the byte, quoted the way Go quotes it, and the
/// state it was found in.
fn bad(c: u8, doing: &str) -> String {
    format!("invalid character {} {doing}", quote_char(c))
}

/// Go's quoteChar. A single quoted byte, except that a quote of either
/// kind is written the way it reads rather than the way strconv would
/// escape it.
///
/// Go quotes the byte by first widening it to the code point of the
/// same number, so 0xff is not a stray byte in the message, it is the
/// letter y with a diaeresis. Anything that code point cannot be seen
/// as gets an escape: the short ones Go has names for, \xhh below a
/// space and at delete, \u00hh for the block of controls above ascii
/// and for the two invisible characters that live among the letters.
fn quote_char(c: u8) -> String {
    match c {
        b'\'' => r"'\''".to_string(),
        b'"' => "'\"'".to_string(),
        b'\\' => r"'\\'".to_string(),
        0x07 => r"'\a'".to_string(),
        0x08 => r"'\b'".to_string(),
        0x09 => r"'\t'".to_string(),
        0x0a => r"'\n'".to_string(),
        0x0b => r"'\v'".to_string(),
        0x0c => r"'\f'".to_string(),
        0x0d => r"'\r'".to_string(),
        0x20..=0x7e => format!("'{}'", c as char),
        0x00..=0x1f | 0x7f => format!("'\\x{c:02x}'"),
        // The c1 controls, the no break space and the soft hyphen.
        0x80..=0xa0 | 0xad => format!("'\\u00{c:02x}'"),
        _ => format!("'{}'", c as char),
    }
}

/// The four bytes Go's scanner skips between values.
fn is_space(c: u8) -> bool {
    c == b' ' || c == b'\t' || c == b'\r' || c == b'\n'
}

#[cfg(test)]
mod tests {
    use super::syntax;

    #[test]
    fn a_body_that_parses_has_nothing_to_say() {
        for ok in [
            "{}",
            "[]",
            " {\"a\": 1} ",
            "[1, 2.5, -3e10, true, false, null]",
            "\"\\u00e9\\n\"",
            "{\"a\":{\"b\":[{}]}}",
            "0",
            "-0.5E+3",
        ] {
            assert_eq!(syntax(ok.as_bytes()), None, "{ok}");
        }
    }

    #[test]
    fn a_word_that_is_not_a_word_names_the_letter_it_wanted() {
        assert_eq!(
            syntax(b"notjson").as_deref(),
            Some("invalid character 'o' in literal null (expecting 'u')")
        );
        assert_eq!(
            syntax(b"tru3").as_deref(),
            Some("invalid character '3' in literal true (expecting 'e')")
        );
        // A word or a number cut short is reported as though a space
        // followed it, because that is how Go's scanner asks the state
        // it is in whether the input could have ended there.
        assert_eq!(
            syntax(b"fals").as_deref(),
            Some("invalid character ' ' in literal false (expecting 'e')")
        );
        assert_eq!(
            syntax(b"1e").as_deref(),
            Some("invalid character ' ' in exponent of numeric literal")
        );
    }

    #[test]
    fn a_body_that_stops_in_the_middle_is_not_about_a_byte() {
        for cut in [
            "",
            "  ",
            "{",
            "{\"a\"",
            "{\"a\":",
            "[1,",
            "\"unclosed",
            "[{",
        ] {
            assert_eq!(
                syntax(cut.as_bytes()).as_deref(),
                Some("unexpected end of JSON input"),
                "{cut}"
            );
        }
    }

    #[test]
    fn every_place_a_byte_can_be_wrong_says_where_it_was() {
        let cases = [
            (
                "{,}",
                "invalid character ',' looking for beginning of object key string",
            ),
            ("{\"a\" 1}", "invalid character '1' after object key"),
            (
                "{\"a\":1 2}",
                "invalid character '2' after object key:value pair",
            ),
            ("[1 2]", "invalid character '2' after array element"),
            (
                "[1,]",
                "invalid character ']' looking for beginning of value",
            ),
            (
                "{\"a\":1,}",
                "invalid character '}' looking for beginning of object key string",
            ),
            ("1 2", "invalid character '2' after top-level value"),
            (">", "invalid character '>' looking for beginning of value"),
            ("\"a\\qb\"", "invalid character 'q' in string escape code"),
            (
                "\"\\u12g4\"",
                "invalid character 'g' in \\u hexadecimal character escape",
            ),
            ("-x", "invalid character 'x' in numeric literal"),
            (
                "1.x",
                "invalid character 'x' after decimal point in numeric literal",
            ),
            (
                "1e+x",
                "invalid character 'x' in exponent of numeric literal",
            ),
            (
                "1ex",
                "invalid character 'x' in exponent of numeric literal",
            ),
        ];
        for (body, want) in cases {
            assert_eq!(syntax(body.as_bytes()).as_deref(), Some(want), "{body}");
        }
    }

    #[test]
    fn a_byte_that_does_not_print_is_written_the_way_go_writes_it() {
        assert_eq!(
            syntax(b"\"a\nb\"").as_deref(),
            Some("invalid character '\\n' in string literal")
        );
        assert_eq!(
            syntax(b"\"a\x00b\"").as_deref(),
            Some("invalid character '\\x00' in string literal")
        );
        assert_eq!(
            syntax(b"'a'").as_deref(),
            Some("invalid character '\\'' looking for beginning of value")
        );
        assert_eq!(
            syntax(b"[1 \"x\"]").as_deref(),
            Some("invalid character '\"' after array element")
        );
        // A byte above ascii is widened to the code point of the same
        // number and printed if that code point can be seen.
        assert_eq!(
            syntax(b"\xff{}").as_deref(),
            Some("invalid character '\u{ff}' looking for beginning of value")
        );
        assert_eq!(
            syntax(b"\x80{}").as_deref(),
            Some("invalid character '\\u0080' looking for beginning of value")
        );
    }

    #[test]
    fn the_bodies_the_suite_sends_read_the_way_upstream_answered_them() {
        // Both halves of what the auth suite recorded GoTrue saying
        // after "Could not parse request body as JSON: ".
        assert_eq!(
            syntax(b"not json").as_deref(),
            Some("invalid character 'o' in literal null (expecting 'u')")
        );
        assert_eq!(syntax(b"").as_deref(), Some("unexpected end of JSON input"));
    }

    #[test]
    fn nesting_deeper_than_a_stack_would_like_is_still_only_a_loop() {
        let deep = format!("{}{}", "[".repeat(200_000), "]".repeat(200_000));
        assert_eq!(syntax(deep.as_bytes()), None);
    }
}
