//! Limit, offset, and the Range header, the three ways a client
//! bounds how many rows come back.
//!
//! `limit` and `offset` are integers on the query string, embed
//! scoped variants like `posts.limit` differ only in the key so this
//! module only parses values. A negative one of either is not a parse
//! error: PostgREST reads them as the bounds of a window and lets the
//! window itself say whether it makes sense, which is why this parser
//! signs them and the caller judges them. The `Range` header carries
//! `first-last` or the open ended `first-` in items, the `Range-Unit`
//! header naming the unit is the router's business. A range that ends
//! before it starts is an error here and a 416 at the HTTP layer, per
//! what PostgREST does.

use crate::scan::{Cur, DIGIT, END, Error, word};

/// An items range from the Range header, zero based and inclusive on
/// both ends, `last` is `None` for the open ended form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Range {
    pub first: u64,
    pub last: Option<u64>,
}

impl Range {
    /// The equivalent offset query parameter.
    pub fn offset(&self) -> u64 {
        self.first
    }

    /// The equivalent limit query parameter, `None` when the range is
    /// open ended.
    pub fn limit(&self) -> Option<u64> {
        // parse_range rejected last < first, the saturation only
        // matters for a range ending at u64::MAX.
        self.last.map(|l| (l - self.first).saturating_add(1))
    }
}

/// Parse a limit value, an integer that may be negative.
pub fn parse_limit(value: &str) -> Result<i64, Error> {
    signed(value)
}

/// Parse an offset value, an integer that may be negative.
pub fn parse_offset(value: &str) -> Result<i64, Error> {
    signed(value)
}

/// Parse a Range header value, `first-last` or `first-`.
///
/// A header nobody can read is a header nobody reads: upstream takes
/// the range off a request that has a well formed one and ignores the
/// rest, so there is nothing to say about a bad one and no error to
/// say it with.
pub fn parse_range(header: &str) -> Option<Range> {
    let dash = header.find('-')?;
    let first = number(&header[..dash])?;
    let last = match dash + 1 == header.len() {
        true => None,
        false => Some(number(&header[dash + 1..])?),
    };
    if last.is_some_and(|l| l < first) {
        return None;
    }
    Some(Range { first, last })
}

/// Strict decimal with an optional leading minus, which is the whole
/// difference between a paging parameter and the Range header: the
/// header's grammar is a regex over digits and a parameter's is a
/// Haskell `readMaybe` over an Integer.
fn signed(value: &str) -> Result<i64, Error> {
    let cur = Cur::new(value);
    let signed = value.starts_with('-');
    let at = usize::from(signed);
    let taken = value[at..].bytes().take_while(u8::is_ascii_digit).count();
    if taken == 0 {
        let wants: &[&str] = match signed {
            true => &[DIGIT],
            false => &[&word("-"), DIGIT],
        };
        return cur.err_at(at, wants);
    }
    if at + taken < value.len() {
        return cur.err_at(at + taken, &[DIGIT, END]);
    }
    // An Integer is what upstream reads these into and it has no
    // ceiling. Ours does, and a number past it is still a number, so
    // it is worth saying which of the two complaints this is.
    value.parse::<i64>().map_err(|_| Error {
        found: cur.found(0),
        expecting: vec!["an integer that fits in 64 bits".to_string()],
        at: 0,
    })
}

/// Strict decimal: digits only, no sign, no blanks.
fn number(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_and_offsets() {
        assert_eq!(parse_limit("15").unwrap(), 15);
        assert_eq!(parse_limit("0").unwrap(), 0);
        assert_eq!(parse_offset("30").unwrap(), 30);

        // A sign parses. What a window starting below zero or ending
        // before it starts means is the caller's question.
        assert_eq!(parse_limit("-1").unwrap(), -1);
        assert_eq!(parse_offset("-4").unwrap(), -4);

        for bad in ["", "-", "+1", "1.5", " 1", "1 ", "ten", "0x10", "--1"] {
            assert!(parse_limit(bad).is_err(), "{bad:?} must not parse");
            assert!(parse_offset(bad).is_err(), "{bad:?} must not parse");
        }
        assert!(parse_limit("99999999999999999999").is_err());
    }

    #[test]
    fn ranges() {
        let r = parse_range("0-24").unwrap();
        assert_eq!(
            r,
            Range {
                first: 0,
                last: Some(24)
            }
        );
        assert_eq!(r.offset(), 0);
        assert_eq!(r.limit(), Some(25));

        let r = parse_range("30-").unwrap();
        assert_eq!(
            r,
            Range {
                first: 30,
                last: None
            }
        );
        assert_eq!(r.offset(), 30);
        assert_eq!(r.limit(), None);

        let r = parse_range("5-5").unwrap();
        assert_eq!(r.limit(), Some(1));
    }

    #[test]
    fn bad_ranges() {
        // A range that ends before it starts is one of these rather
        // than a 416: only a well formed header gets that far.
        assert_eq!(parse_range("24-0"), None);
        for bad in ["", "-", "-24", "a-b", "0-24extra", "0 - 24", "1--2"] {
            assert_eq!(parse_range(bad), None, "{bad:?} must not parse");
        }
    }
}
