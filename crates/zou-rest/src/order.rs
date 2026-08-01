//! The order parameter grammar.
//!
//! `order=age.desc,height.asc` picks the sort for a request, each
//! comma separated term is a column with an optional direction and an
//! optional nulls placement, direction first when both appear. Json
//! arrow paths order by a key inside a column, `order=data->>age.desc`.
//! Embed scoped ordering arrives on a different query key entirely,
//! `posts.order=...`, so this module only ever sees the value side.
//!
//! Bare names break on the same bytes as the filter grammar, and the
//! modifier words never collide with column names because a term
//! always starts with the column: `order=asc` orders by a column
//! named asc, only the words after a dot are modifiers.

use std::fmt;

use crate::scan::{Cur, fmt_path, parse_json_path, scan_name, scan_quoted, write_escaped};

pub use crate::scan::{Error, JsonKey, JsonStep};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Asc,
    Desc,
}

impl Direction {
    fn name(self) -> &'static str {
        match self {
            Direction::Asc => "asc",
            Direction::Desc => "desc",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Nulls {
    First,
    Last,
}

impl Nulls {
    fn name(self) -> &'static str {
        match self {
            Nulls::First => "nullsfirst",
            Nulls::Last => "nullslast",
        }
    }
}

/// One sort key. `direction` and `nulls` stay `None` when the client
/// left them out, the SQL layer applies postgres defaults so the
/// canonical form does not have to invent them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Term {
    pub name: String,
    pub path: Vec<JsonStep>,
    pub direction: Option<Direction>,
    pub nulls: Option<Nulls>,
}

/// Parse a whole order value into its terms. The caller has already
/// url decoded it. An empty value is an error, a missing order
/// parameter never reaches here.
pub fn parse(value: &str) -> Result<Vec<Term>, Error> {
    let mut cur = Cur::new(value);
    let mut terms = vec![parse_term(&mut cur)?];
    while cur.eat(b',') {
        terms.push(parse_term(&mut cur)?);
    }
    if !cur.done() {
        return cur.err("unexpected characters after the order list");
    }
    Ok(terms)
}

/// Bytes that end a bare name, the same set the filter grammar uses.
fn name_break(b: u8) -> bool {
    matches!(b, b'.' | b',' | b'(' | b')' | b'"' | b'{' | b'}')
}

fn parse_term(cur: &mut Cur) -> Result<Term, Error> {
    let name = if cur.peek() == Some(b'"') {
        scan_quoted(cur)?
    } else {
        scan_name(cur, name_break)
    };
    if name.is_empty() {
        return cur.err("expected a field name");
    }
    let path = parse_json_path(cur, name_break)?;

    let mut direction = None;
    let mut nulls = None;
    while cur.eat(b'.') {
        let at = cur.pos;
        let word = scan_name(cur, name_break);
        if word.is_empty() {
            return Err(Error {
                message: "expected an order modifier".into(),
                at,
            });
        }
        let dir = match word.as_str() {
            "asc" => Some(Direction::Asc),
            "desc" => Some(Direction::Desc),
            _ => None,
        };
        if let Some(d) = dir {
            if direction.is_some() {
                return Err(Error {
                    message: "a term takes one direction".into(),
                    at,
                });
            }
            if nulls.is_some() {
                return Err(Error {
                    message: "the direction goes before the nulls".into(),
                    at,
                });
            }
            direction = Some(d);
            continue;
        }
        let n = match word.as_str() {
            "nullsfirst" => Some(Nulls::First),
            "nullslast" => Some(Nulls::Last),
            _ => None,
        };
        if let Some(n) = n {
            if nulls.is_some() {
                return Err(Error {
                    message: "a term takes one nulls placement".into(),
                    at,
                });
            }
            nulls = Some(n);
            continue;
        }
        return Err(Error {
            message: format!("unknown order modifier ({word})"),
            at,
        });
    }

    Ok(Term {
        name,
        path,
        direction,
        nulls,
    })
}

/// True when a name cannot be written bare: it contains a delimiter,
/// an arrow, or a backslash.
fn name_needs_quotes(s: &str) -> bool {
    s.is_empty() || s.bytes().any(|b| name_break(b) || b == b'\\') || s.contains("->")
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if name_needs_quotes(&self.name) {
            write_escaped(f, &self.name)?;
        } else {
            write!(f, "{}", self.name)?;
        }
        fmt_path(f, &self.path)?;
        if let Some(d) = self.direction {
            write!(f, ".{}", d.name())?;
        }
        if let Some(n) = self.nulls {
            write!(f, ".{}", n.name())?;
        }
        Ok(())
    }
}

/// The canonical order value for these terms. Reparsing it yields an
/// equal list, the property the tests and the fuzz target both check.
pub fn render(terms: &[Term]) -> String {
    let mut out = String::new();
    for (i, term) in terms.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&term.to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(value: &str) -> Vec<Term> {
        parse(value).unwrap_or_else(|e| panic!("{value} failed: {e}"))
    }

    fn fail(value: &str) -> Error {
        match parse(value) {
            Ok(terms) => panic!("{value} parsed as {terms:?}"),
            Err(e) => e,
        }
    }

    #[test]
    fn bare_columns_and_directions() {
        let terms = ok("age");
        assert_eq!(terms[0].name, "age");
        assert_eq!(terms[0].direction, None);
        assert_eq!(terms[0].nulls, None);

        let terms = ok("age.desc,height.asc");
        assert_eq!(terms.len(), 2);
        assert_eq!(terms[0].direction, Some(Direction::Desc));
        assert_eq!(terms[1].direction, Some(Direction::Asc));
    }

    #[test]
    fn nulls_placement() {
        let terms = ok("age.nullsfirst");
        assert_eq!(terms[0].direction, None);
        assert_eq!(terms[0].nulls, Some(Nulls::First));

        let terms = ok("age.desc.nullslast");
        assert_eq!(terms[0].direction, Some(Direction::Desc));
        assert_eq!(terms[0].nulls, Some(Nulls::Last));

        assert_eq!(
            fail("age.nullsfirst.desc").message,
            "the direction goes before the nulls"
        );
        assert_eq!(fail("age.asc.desc").message, "a term takes one direction");
        assert_eq!(
            fail("age.nullsfirst.nullslast").message,
            "a term takes one nulls placement"
        );
    }

    #[test]
    fn json_paths() {
        let terms = ok("data->>age.desc");
        assert_eq!(terms[0].name, "data");
        assert_eq!(
            terms[0].path,
            vec![JsonStep {
                text: true,
                key: JsonKey::Name("age".into())
            }]
        );
        assert_eq!(terms[0].direction, Some(Direction::Desc));

        let terms = ok("data->items->0->>price");
        assert_eq!(terms[0].path.len(), 3);
        assert_eq!(terms[0].path[1].key, JsonKey::Index(0));
    }

    #[test]
    fn quoted_names_and_the_modifier_words() {
        // A term always starts with the column, so the modifier words
        // are plain names in first position.
        let terms = ok("asc.desc");
        assert_eq!(terms[0].name, "asc");
        assert_eq!(terms[0].direction, Some(Direction::Desc));

        let terms = ok("\"my.col\".asc");
        assert_eq!(terms[0].name, "my.col");
        assert_eq!(terms[0].direction, Some(Direction::Asc));
    }

    #[test]
    fn errors() {
        assert_eq!(fail("").message, "expected a field name");
        assert_eq!(fail("a,").message, "expected a field name");
        assert_eq!(fail(",a").message, "expected a field name");
        assert!(fail("a.upward").message.contains("unknown order modifier"));
        assert_eq!(fail("a.").message, "expected an order modifier");
        assert_eq!(
            fail("a b)c").message,
            "unexpected characters after the order list"
        );
    }

    const CORPUS: &[&str] = &[
        "age",
        "age.asc",
        "age.desc",
        "age.nullsfirst",
        "age.nullslast",
        "age.desc.nullslast",
        "age.asc.nullsfirst",
        "age.desc,height.asc",
        "a,b,c",
        "data->>age.desc",
        "data->items->0->>price.asc.nullslast",
        "\"my.col\".desc",
        "\"weird\\\"name\"",
        "\"\\\\slash\".asc",
        "asc",
        "asc.desc",
        "nullsfirst.nullsfirst",
    ];

    #[test]
    fn canonical_roundtrip() {
        for value in CORPUS {
            let terms = ok(value);
            let rendered = render(&terms);
            let again = parse(&rendered)
                .unwrap_or_else(|e| panic!("{value} rendered as {rendered} which failed: {e}"));
            assert_eq!(terms, again, "{value} did not roundtrip via {rendered}");
            assert_eq!(rendered, render(&again), "{value} did not reach a fixpoint");
        }
    }
}
