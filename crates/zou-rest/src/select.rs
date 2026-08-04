//! The PostgREST select grammar.
//!
//! A select value like `id,name:full_name::text,amount.sum()` or
//! `*,author:authors!fk!inner(id,books(title)),...stats(views)` parses
//! into a tree of items the SQL compiler and the embedding planner
//! walk. An item is `*`, a column pick, an embedded resource, or a
//! spread, and embedded resources nest through their parenthesized
//! item lists.
//!
//! A column pick is `[alias:]name[jsonpath][::cast]` with an optional
//! aggregate `.sum()`, `.avg()`, `.max()`, `.min()`, or `.count()`
//! after it, itself castable, so the full shape is
//! `alias:data->>amount::numeric.sum()::text`. A bare `count()` is the
//! one aggregate that needs no column. An embedded resource is
//! `[alias:]relation` followed by any mix of `!hint`, `!inner`, and
//! `!left` and then the item list; `...` in front spreads the embedded
//! columns into the parent instead of nesting them, and a spread takes
//! no alias.
//!
//! Names quote the same way they do in the filter grammar, and quoting
//! is also how the two collisions are spelled apart: a bare `count()`
//! is the aggregate while `"count"()` is an empty embed of a relation
//! named count, and a bare `!inner` is the join flag while `!"inner"`
//! is a hint named inner.
//!
//! A select list may be written with spaces after its commas, and a
//! name may be written with spaces around it, because upstream builds
//! an unquoted name out of letters, digits, `_`, `$` and spaces and
//! then strips the ends off it. So `id, name` is two columns rather
//! than a column called ` name`, `clients (id)` is an embed, and
//! `first name` is still one column called `first name`. A quoted name
//! keeps whatever is inside the quotes.
//!
//! Every parse renders back to a canonical string via [`render`], and
//! reparsing that string yields an equal tree. The canonical form
//! writes the hint before the join flag; parsing accepts either order.
//! The fuzz target lives on the roundtrip property.

use std::fmt;

use crate::scan::{Cur, fmt_path, parse_json_path, scan_name, scan_quoted, write_escaped};
pub use crate::scan::{Error, JsonKey, JsonStep};

/// An aggregate function, the token before the empty parens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Agg {
    Sum,
    Avg,
    Max,
    Min,
    Count,
}

impl Agg {
    fn parse(s: &str) -> Option<Agg> {
        Some(match s {
            "sum" => Agg::Sum,
            "avg" => Agg::Avg,
            "max" => Agg::Max,
            "min" => Agg::Min,
            "count" => Agg::Count,
            _ => return None,
        })
    }

    /// The token this aggregate spells in a query string.
    pub fn name(self) -> &'static str {
        match self {
            Agg::Sum => "sum",
            Agg::Avg => "avg",
            Agg::Max => "max",
            Agg::Min => "min",
            Agg::Count => "count",
        }
    }
}

/// The join flag of an embedded resource, `!inner` or `!left`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Join {
    Inner,
    Left,
}

/// What a column pick reads before any aggregate: the column, a json
/// path into it, and a cast of the extracted value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldSel {
    pub name: String,
    pub path: Vec<JsonStep>,
    pub cast: Option<String>,
}

/// One column pick, aggregated or not. `field` is `None` only for the
/// bare `count()` aggregate, which reads no column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Col {
    pub alias: Option<String>,
    pub field: Option<FieldSel>,
    pub agg: Option<Agg>,
    /// The cast after the aggregate parens, distinct from the field
    /// cast, which applies before aggregation.
    pub agg_cast: Option<String>,
}

/// One embedded resource, either nested under its own key or spread
/// into the parent. A spread never carries an alias.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Embed {
    pub alias: Option<String>,
    pub relation: String,
    /// The fk or column disambiguation hint after a `!`.
    pub hint: Option<String>,
    pub join: Option<Join>,
    pub items: Vec<Item>,
}

/// One comma separated item of a select list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Item {
    Star,
    Col(Col),
    Embed(Embed),
    Spread(Embed),
}

/// Parse a whole select value into its items. The caller has already
/// url decoded it. An empty value is an error here; defaulting a
/// missing select parameter to `*` is the router's call.
pub fn parse(value: &str) -> Result<Vec<Item>, Error> {
    let mut cur = Cur::new(value);
    let items = parse_items(&mut cur)?;
    if !cur.done() {
        return cur.err("unexpected characters after the select list");
    }
    Ok(items)
}

/// Bytes that end a bare name in a select item. Unlike the filter
/// grammar this breaks on the alias and cast colon and the hint bang.
fn name_break(b: u8) -> bool {
    matches!(
        b,
        b'.' | b',' | b'(' | b')' | b'"' | b'{' | b'}' | b':' | b'!'
    )
}

fn parse_items(cur: &mut Cur) -> Result<Vec<Item>, Error> {
    let mut out = Vec::new();
    loop {
        let item = parse_item(cur)?;
        // A column takes the spaces before the comma into its own name
        // and gives them back when it is trimmed. An embed ends on its
        // closing bracket with nothing left to take them with, so the
        // list steps over them, and only as far as a comma, because
        // upstream reads them as the front of the separator: a list
        // that ends on a bracket and a space is a parse error there
        // too.
        if matches!(item, Item::Embed(_) | Item::Spread(_)) {
            cur.skip_spaces_before(b',');
        }
        out.push(item);
        if cur.eat(b',') {
            continue;
        }
        return Ok(out);
    }
}

/// A name at the cursor, and whether it was quoted, which exempts it
/// from being read as `*`, `count()`, `inner`, or `left`.
///
/// Unquoted, the spaces around it are not part of it. Upstream gets
/// there by a route worth knowing about: a space is one of the
/// characters an identifier is made of, so `id, name` reads the second
/// name as ` name` and then strips it. That is why `select=first name`
/// is a column called `first name` and `select=id, name` is not a
/// column called ` name`, and why the spaces come off both ends rather
/// than only the front.
fn parse_name(cur: &mut Cur) -> Result<(String, bool), Error> {
    let (name, quoted) = parse_raw_name(cur)?;
    let name = match quoted {
        true => name,
        false => name.trim_matches([' ', '\t']).to_string(),
    };
    if name.is_empty() {
        return cur.err("expected a name");
    }
    Ok((name, quoted))
}

/// The same, with the spaces left on.
///
/// One caller wants them: `!inner` is matched as a word rather than
/// read as a name, so upstream takes `!inner (id)` as a hint that
/// failed to be followed by a list rather than as an inner join, while
/// `!hint (id)` is a hint with the space stripped off it.
fn parse_raw_name(cur: &mut Cur) -> Result<(String, bool), Error> {
    let quoted = cur.peek() == Some(b'"');
    let name = if quoted {
        scan_quoted(cur)?
    } else {
        scan_name(cur, name_break)
    };
    if name.is_empty() {
        return cur.err("expected a name");
    }
    Ok((name, quoted))
}

fn parse_item(cur: &mut Cur) -> Result<Item, Error> {
    if cur.eat_str("...") {
        let (relation, _) = parse_name(cur)?;
        let embed = parse_embed_tail(cur, None, relation)?;
        return Ok(Item::Spread(embed));
    }

    let (mut name, mut quoted) = parse_name(cur)?;
    let mut alias = None;
    if cur.peek() == Some(b':') && !cur.starts("::") {
        cur.bump();
        alias = Some(name);
        (name, quoted) = parse_name(cur)?;
    }

    if name == "*" && !quoted {
        if alias.is_some() {
            return cur.err("* takes no alias");
        }
        return Ok(Item::Star);
    }

    if name == "count" && !quoted && cur.eat_str("()") {
        let agg_cast = parse_cast(cur)?;
        return Ok(Item::Col(Col {
            alias,
            field: None,
            agg: Some(Agg::Count),
            agg_cast,
        }));
    }

    if matches!(cur.peek(), Some(b'!') | Some(b'(')) {
        let embed = parse_embed_tail(cur, alias, name)?;
        return Ok(Item::Embed(embed));
    }

    let path = parse_json_path(cur, name_break)?;
    let cast = parse_cast(cur)?;
    let (agg, agg_cast) = parse_agg(cur)?;
    Ok(Item::Col(Col {
        alias,
        field: Some(FieldSel { name, path, cast }),
        agg,
        agg_cast,
    }))
}

fn parse_cast(cur: &mut Cur) -> Result<Option<String>, Error> {
    if !cur.eat_str("::") {
        return Ok(None);
    }
    // A type name is an identifier like any other, so `id::text , x`
    // leaves its trailing space on the type and the type gives it back.
    let name = scan_name(cur, name_break);
    let name = name.trim_matches([' ', '\t']);
    if name.is_empty() {
        return cur.err("expected a type after ::");
    }
    Ok(Some(name.to_string()))
}

fn parse_agg(cur: &mut Cur) -> Result<(Option<Agg>, Option<String>), Error> {
    if !cur.eat(b'.') {
        return Ok((None, None));
    }
    let at = cur.pos;
    let name = scan_name(cur, name_break);
    let Some(agg) = Agg::parse(&name) else {
        return Err(Error {
            message: format!("unknown aggregate ({name})"),
            at,
        });
    };
    if !cur.eat_str("()") {
        return cur.err("an aggregate wants ()");
    }
    let cast = parse_cast(cur)?;
    Ok((Some(agg), cast))
}

/// The flags and item list of an embedded resource, cursor sitting on
/// the first `!` or the opening paren.
fn parse_embed_tail(
    cur: &mut Cur,
    alias: Option<String>,
    relation: String,
) -> Result<Embed, Error> {
    let mut hint = None;
    let mut join = None;
    while cur.eat(b'!') {
        let at = cur.pos;
        let (raw, fq) = parse_raw_name(cur)?;
        let flag_join = match (raw.as_str(), fq) {
            ("inner", false) => Some(Join::Inner),
            ("left", false) => Some(Join::Left),
            _ => None,
        };
        let flag = match fq {
            true => raw,
            false => raw.trim_matches([' ', '\t']).to_string(),
        };
        if flag.is_empty() {
            return cur.err("expected a name");
        }
        if let Some(j) = flag_join {
            if join.is_some() {
                return Err(Error {
                    message: "an embed takes one of inner or left".into(),
                    at,
                });
            }
            join = Some(j);
        } else {
            if hint.is_some() {
                return Err(Error {
                    message: "an embed takes one hint".into(),
                    at,
                });
            }
            hint = Some(flag);
        }
    }
    cur.expect(b'(', "an embedded resource wants a (list)")?;
    let items = if cur.eat(b')') {
        Vec::new()
    } else {
        let items = parse_items(cur)?;
        cur.expect(b')', "unterminated embedded resource")?;
        items
    };
    Ok(Embed {
        alias,
        relation,
        hint,
        join,
        items,
    })
}

/// True when a name cannot be written bare in a select item: it
/// contains a delimiter, an arrow, would be read as the star item, or
/// carries a space on an end, where a bare one would lose it.
fn name_needs_quotes(s: &str) -> bool {
    s.is_empty()
        || s == "*"
        || s.bytes().any(|b| name_break(b) || b == b'\\')
        || s.contains("->")
        || s.starts_with([' ', '\t'])
        || s.ends_with([' ', '\t'])
}

struct Name<'a>(&'a str);

impl fmt::Display for Name<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if name_needs_quotes(self.0) {
            write_escaped(f, self.0)
        } else {
            write!(f, "{}", self.0)
        }
    }
}

impl fmt::Display for Col {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(a) = &self.alias {
            write!(f, "{}:", Name(a))?;
        }
        if let Some(fld) = &self.field {
            write!(f, "{}", Name(&fld.name))?;
            fmt_path(f, &fld.path)?;
            if let Some(c) = &fld.cast {
                write!(f, "::{c}")?;
            }
        }
        if let Some(agg) = self.agg {
            if self.field.is_some() {
                write!(f, ".")?;
            }
            write!(f, "{}()", agg.name())?;
            if let Some(c) = &self.agg_cast {
                write!(f, "::{c}")?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for Embed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(a) = &self.alias {
            write!(f, "{}:", Name(a))?;
        }
        // A flagless empty embed of a relation named count would read
        // back as the bare count() aggregate, quoting keeps it an
        // embed.
        let ambiguous = self.relation == "count"
            && self.hint.is_none()
            && self.join.is_none()
            && self.items.is_empty();
        if ambiguous {
            write_escaped(f, &self.relation)?;
        } else {
            write!(f, "{}", Name(&self.relation))?;
        }
        if let Some(h) = &self.hint {
            write!(f, "!")?;
            if matches!(h.as_str(), "inner" | "left") {
                write_escaped(f, h)?;
            } else {
                write!(f, "{}", Name(h))?;
            }
        }
        match self.join {
            Some(Join::Inner) => write!(f, "!inner")?,
            Some(Join::Left) => write!(f, "!left")?,
            None => {}
        }
        write!(f, "(")?;
        for (i, item) in self.items.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{item}")?;
        }
        write!(f, ")")
    }
}

impl fmt::Display for Item {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Item::Star => write!(f, "*"),
            Item::Col(c) => write!(f, "{c}"),
            Item::Embed(e) => write!(f, "{e}"),
            Item::Spread(e) => write!(f, "...{e}"),
        }
    }
}

/// The canonical select value for these items. Reparsing it yields an
/// equal tree, the property the tests and the fuzz target both check.
pub fn render(items: &[Item]) -> String {
    let mut out = String::new();
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&item.to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(value: &str) -> Vec<Item> {
        parse(value).unwrap_or_else(|e| panic!("{value} failed: {e}"))
    }

    fn fail(value: &str) -> Error {
        match parse(value) {
            Ok(items) => panic!("{value} parsed as {items:?}"),
            Err(e) => e,
        }
    }

    fn col(item: &Item) -> &Col {
        match item {
            Item::Col(c) => c,
            other => panic!("expected a column, got {other:?}"),
        }
    }

    fn embed(item: &Item) -> &Embed {
        match item {
            Item::Embed(e) => e,
            other => panic!("expected an embed, got {other:?}"),
        }
    }

    #[test]
    fn columns_star_and_aliases() {
        let items = ok("id,name");
        assert_eq!(items.len(), 2);
        assert_eq!(col(&items[0]).field.as_ref().unwrap().name, "id");

        let items = ok("*");
        assert_eq!(items, vec![Item::Star]);

        let items = ok("*,id");
        assert_eq!(items[0], Item::Star);

        let items = ok("first:full_name");
        let c = col(&items[0]);
        assert_eq!(c.alias.as_deref(), Some("first"));
        assert_eq!(c.field.as_ref().unwrap().name, "full_name");

        assert_eq!(fail("a:*").message, "* takes no alias");
        assert_eq!(fail("").message, "expected a name");
        assert_eq!(fail("a,").message, "expected a name");
    }

    #[test]
    fn casts_and_json_paths() {
        let items = ok("full_name::text");
        assert_eq!(
            col(&items[0]).field.as_ref().unwrap().cast.as_deref(),
            Some("text")
        );

        let items = ok("json_data->phones->0->>number");
        let f = col(&items[0]).field.as_ref().unwrap();
        assert_eq!(f.name, "json_data");
        assert_eq!(
            f.path,
            vec![
                JsonStep {
                    text: false,
                    key: JsonKey::Name("phones".into())
                },
                JsonStep {
                    text: false,
                    key: JsonKey::Index(0)
                },
                JsonStep {
                    text: true,
                    key: JsonKey::Name("number".into())
                },
            ]
        );

        let items = ok("city:address->>city::text");
        let c = col(&items[0]);
        assert_eq!(c.alias.as_deref(), Some("city"));
        assert_eq!(c.field.as_ref().unwrap().cast.as_deref(), Some("text"));

        assert_eq!(fail("a::").message, "expected a type after ::");
    }

    #[test]
    fn aggregates_and_the_two_casts() {
        let items = ok("amount.sum()");
        let c = col(&items[0]);
        assert_eq!(c.agg, Some(Agg::Sum));
        assert_eq!(c.agg_cast, None);

        let items = ok("amount.avg()::int");
        assert_eq!(col(&items[0]).agg_cast.as_deref(), Some("int"));

        let items = ok("data->>amount::numeric.sum()::text");
        let c = col(&items[0]);
        let f = c.field.as_ref().unwrap();
        assert_eq!(f.cast.as_deref(), Some("numeric"));
        assert_eq!(c.agg, Some(Agg::Sum));
        assert_eq!(c.agg_cast.as_deref(), Some("text"));

        for (v, agg) in [
            ("a.sum()", Agg::Sum),
            ("a.avg()", Agg::Avg),
            ("a.max()", Agg::Max),
            ("a.min()", Agg::Min),
            ("a.count()", Agg::Count),
        ] {
            assert_eq!(col(&ok(v)[0]).agg, Some(agg));
        }

        assert!(fail("a.median()").message.contains("unknown aggregate"));
        assert_eq!(fail("a.sum").message, "an aggregate wants ()");
        assert_eq!(fail("a.sum(x)").message, "an aggregate wants ()");
    }

    #[test]
    fn bare_count_is_the_field_free_aggregate() {
        let items = ok("count()");
        let c = col(&items[0]);
        assert_eq!(c.field, None);
        assert_eq!(c.agg, Some(Agg::Count));

        let items = ok("total:count()::bigint");
        let c = col(&items[0]);
        assert_eq!(c.alias.as_deref(), Some("total"));
        assert_eq!(c.agg_cast.as_deref(), Some("bigint"));

        // With columns inside, count is a relation like any other, and
        // a quoted count is an empty embed of that relation.
        let items = ok("count(id)");
        assert_eq!(embed(&items[0]).relation, "count");
        let items = ok("\"count\"()");
        let e = embed(&items[0]);
        assert_eq!(e.relation, "count");
        assert!(e.items.is_empty());
        assert_eq!(render(&items), "\"count\"()");
    }

    #[test]
    fn embeds_nest_and_take_flags() {
        let items = ok("authors(id,name)");
        let e = embed(&items[0]);
        assert_eq!(e.relation, "authors");
        assert_eq!(e.items.len(), 2);

        let items = ok("author:authors(*)");
        let e = embed(&items[0]);
        assert_eq!(e.alias.as_deref(), Some("author"));
        assert_eq!(e.items, vec![Item::Star]);

        let items = ok("clients!inner(*)");
        assert_eq!(embed(&items[0]).join, Some(Join::Inner));

        let items = ok("users!tasks_owner_fkey(id)");
        assert_eq!(embed(&items[0]).hint.as_deref(), Some("tasks_owner_fkey"));

        let items = ok("rel!fk!left(a)");
        let e = embed(&items[0]);
        assert_eq!(e.hint.as_deref(), Some("fk"));
        assert_eq!(e.join, Some(Join::Left));

        // The flags come in either order, the canonical form writes
        // the hint first.
        let items = ok("rel!inner!fk(a)");
        assert_eq!(render(&items), "rel!fk!inner(a)");

        let items = ok("a(b(c))");
        let inner = embed(&embed(&items[0]).items[0]);
        assert_eq!(inner.relation, "b");

        let items = ok("rel()");
        assert!(embed(&items[0]).items.is_empty());

        assert_eq!(fail("rel!fk!fk2(a)").message, "an embed takes one hint");
        assert_eq!(
            fail("rel!inner!left(a)").message,
            "an embed takes one of inner or left"
        );
        assert_eq!(fail("rel(a").message, "unterminated embedded resource");
        assert_eq!(fail("rel!(a)").message, "expected a name");
    }

    #[test]
    fn a_quoted_inner_is_a_hint() {
        let items = ok("rel!\"inner\"(a)");
        let e = embed(&items[0]);
        assert_eq!(e.hint.as_deref(), Some("inner"));
        assert_eq!(e.join, None);
        assert_eq!(render(&items), "rel!\"inner\"(a)");
    }

    #[test]
    fn spreads_take_no_alias() {
        let items = ok("id,...projects(name)");
        let Item::Spread(e) = &items[1] else {
            panic!("expected a spread, got {:?}", items[1]);
        };
        assert_eq!(e.relation, "projects");
        assert_eq!(e.alias, None);

        let items = ok("...projects!inner(name,client:clients(id))");
        let Item::Spread(e) = &items[0] else { panic!() };
        assert_eq!(e.join, Some(Join::Inner));

        assert_eq!(
            fail("...a:b(c)").message,
            "an embedded resource wants a (list)"
        );
    }

    #[test]
    fn quoted_names_carry_delimiters() {
        let items = ok("\"first name\"");
        assert_eq!(col(&items[0]).field.as_ref().unwrap().name, "first name");

        let items = ok("\"weird,name\":col");
        assert_eq!(col(&items[0]).alias.as_deref(), Some("weird,name"));
        assert_eq!(render(&items), "\"weird,name\":col");

        // A quoted star is a column named *, not the star item.
        let items = ok("\"*\"");
        assert_eq!(col(&items[0]).field.as_ref().unwrap().name, "*");
        assert_eq!(render(&items), "\"*\"");
    }

    #[test]
    fn a_list_may_be_written_with_spaces() {
        let items = ok("id, name");
        assert_eq!(items.len(), 2);
        assert_eq!(col(&items[1]).field.as_ref().unwrap().name, "name");

        let items = ok(" id , name ");
        assert_eq!(col(&items[0]).field.as_ref().unwrap().name, "id");
        assert_eq!(col(&items[1]).field.as_ref().unwrap().name, "name");

        // Inside a name a space is a plain identifier character, only
        // the ends come off.
        let items = ok("first name");
        assert_eq!(col(&items[0]).field.as_ref().unwrap().name, "first name");

        let items = ok("alias : name");
        let c = col(&items[0]);
        assert_eq!(c.alias.as_deref(), Some("alias"));
        assert_eq!(c.field.as_ref().unwrap().name, "name");

        let items = ok("amount :: numeric");
        let f = col(&items[0]).field.as_ref().unwrap();
        assert_eq!(f.name, "amount");
        assert_eq!(f.cast.as_deref(), Some("numeric"));

        let items = ok("id, clients (id, name), x");
        let e = embed(&items[1]);
        assert_eq!(e.relation, "clients");
        assert_eq!(col(&e.items[1]).field.as_ref().unwrap().name, "name");
        assert_eq!(col(&items[2]).field.as_ref().unwrap().name, "x");

        let items = ok("...clients (id), t !hint (id), u ! inner (id)");
        let Item::Spread(e) = &items[0] else { panic!() };
        assert_eq!(e.relation, "clients");
        assert_eq!(embed(&items[1]).hint.as_deref(), Some("hint"));
        // A space in front of inner makes it a hint, since the join
        // flags are matched as words before the name rule sees them.
        assert_eq!(embed(&items[2]).hint.as_deref(), Some("inner"));
        assert_eq!(embed(&items[2]).join, None);
    }

    #[test]
    fn a_quoted_name_keeps_its_spaces() {
        let items = ok("\" name \"");
        assert_eq!(col(&items[0]).field.as_ref().unwrap().name, " name ");
        assert_eq!(render(&items), "\" name \"");

        // Bare, it would read back short, so rendering quotes it.
        let items = vec![Item::Col(Col {
            alias: None,
            field: Some(FieldSel {
                name: "name ".into(),
                path: Vec::new(),
                cast: None,
            }),
            agg: None,
            agg_cast: None,
        })];
        assert_eq!(render(&items), "\"name \"");
        assert_eq!(parse(&render(&items)).unwrap(), items);
    }

    #[test]
    fn spaces_the_grammar_does_not_take() {
        // An embed ends on its bracket and has nothing to carry the
        // spaces after it, so they are only allowed where a comma
        // follows.
        ok("clients(id) , x");
        assert_eq!(
            fail("clients(id) ").message,
            "unexpected characters after the select list"
        );
        assert_eq!(fail("a(b(c) )").message, "unterminated embedded resource");

        // An aggregate ends the same way and does not even get the
        // comma.
        assert_eq!(
            fail("id.sum() , x").message,
            "unexpected characters after the select list"
        );
    }

    #[test]
    fn errors_point_at_the_problem() {
        assert_eq!(fail("a,,b").message, "expected a name");
        assert_eq!(
            fail("a)b").message,
            "unexpected characters after the select list"
        );
        assert_eq!(
            fail("*::text").message,
            "unexpected characters after the select list"
        );
        assert_eq!(fail("a->>").message, "expected a json path key");
        let e = fail("x,a.median()");
        assert!(e.message.contains("unknown aggregate"));
        assert_eq!(e.at, 4);
    }

    const CORPUS: &[&str] = &[
        "*",
        "id,name",
        "full_name::text",
        "first:full_name",
        "city:address->>city",
        "json_data->phones->0->>number",
        "data->-1",
        "a->b::int",
        "amount.sum()",
        "amount.avg()::int",
        "amount::numeric.sum()::text",
        "count()",
        "count()::int",
        "total:count()",
        "id.count()",
        "count",
        "count::int",
        "count(id)",
        "\"count\"()",
        "authors(id,name)",
        "author:authors(*)",
        "clients!inner(*)",
        "users!tasks_owner_fkey(id)",
        "rel!fk!left(a)",
        "rel!inner(a,b(c))",
        "rel!\"inner\"(a)",
        "rel()",
        "or(x)",
        "...projects(name)",
        "...projects!inner(name,client:clients(id))",
        "\"weird,name\":col",
        "\"first name\"",
        "\"*\"",
        "*,rel(*),id",
        "id, name",
        "id, clients (id, name), x",
        "\" name \"",
    ];

    #[test]
    fn the_canonical_render_reparses_to_the_same_tree() {
        for value in CORPUS {
            let first = ok(value);
            let rendered = render(&first);
            let second = parse(&rendered)
                .unwrap_or_else(|e| panic!("render of {value} gave {rendered}: {e}"));
            assert_eq!(first, second, "{value} via {rendered}");
            assert_eq!(rendered, render(&second), "render must be a fixpoint");
        }
    }
}
