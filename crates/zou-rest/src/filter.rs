//! The PostgREST filter grammar.
//!
//! A query pair like `age=not.gte.18` or `or=(a.eq.1,and(b.eq.2,c.eq.3))`
//! parses into a tree the SQL compiler can walk. The grammar follows
//! PostgREST's reading of the same strings: the key names a field,
//! possibly dotted through embedded resources and json arrows, or one
//! of the logic keywords or, and, not.or, not.and, and the value is
//! `[not.]operator[(modifier)].literal` or a parenthesized tree of
//! conditions.
//!
//! Quoting works the way PostgREST documents it. Inside in lists,
//! any/all arrays, and logic trees a double quoted string protects
//! reserved characters, with backslash escaping the next character.
//! A top level scalar takes the rest of the value verbatim, quotes
//! included, because nothing needs delimiting there. Field names may
//! be double quoted anywhere a name is read from the key or a tree,
//! which is also how a column that collides with or, and, or not is
//! addressed.
//!
//! Every parse renders back to a canonical pair via [`Parsed::render`],
//! and reparsing that pair yields an equal tree. The fuzz target lives
//! on that property.

use std::fmt;

use crate::scan::{Cur, fmt_path, parse_json_path, scan_name, scan_quoted, write_escaped};
pub use crate::scan::{Error, JsonKey, JsonStep};

/// A filter operator, the token between the two dots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    Ilike,
    Match,
    Imatch,
    In,
    Is,
    IsDistinct,
    Fts,
    Plfts,
    Phfts,
    Wfts,
    Cs,
    Cd,
    Ov,
    Sl,
    Sr,
    Nxl,
    Nxr,
    Adj,
}

impl Op {
    fn parse(s: &str) -> Option<Op> {
        Some(match s {
            "eq" => Op::Eq,
            "neq" => Op::Neq,
            "gt" => Op::Gt,
            "gte" => Op::Gte,
            "lt" => Op::Lt,
            "lte" => Op::Lte,
            "like" => Op::Like,
            "ilike" => Op::Ilike,
            "match" => Op::Match,
            "imatch" => Op::Imatch,
            "in" => Op::In,
            "is" => Op::Is,
            "isdistinct" => Op::IsDistinct,
            "fts" => Op::Fts,
            "plfts" => Op::Plfts,
            "phfts" => Op::Phfts,
            "wfts" => Op::Wfts,
            "cs" => Op::Cs,
            "cd" => Op::Cd,
            "ov" => Op::Ov,
            "sl" => Op::Sl,
            "sr" => Op::Sr,
            "nxl" => Op::Nxl,
            "nxr" => Op::Nxr,
            "adj" => Op::Adj,
            _ => return None,
        })
    }

    /// The token this operator spells in a query string.
    pub fn name(self) -> &'static str {
        match self {
            Op::Eq => "eq",
            Op::Neq => "neq",
            Op::Gt => "gt",
            Op::Gte => "gte",
            Op::Lt => "lt",
            Op::Lte => "lte",
            Op::Like => "like",
            Op::Ilike => "ilike",
            Op::Match => "match",
            Op::Imatch => "imatch",
            Op::In => "in",
            Op::Is => "is",
            Op::IsDistinct => "isdistinct",
            Op::Fts => "fts",
            Op::Plfts => "plfts",
            Op::Phfts => "phfts",
            Op::Wfts => "wfts",
            Op::Cs => "cs",
            Op::Cd => "cd",
            Op::Ov => "ov",
            Op::Sl => "sl",
            Op::Sr => "sr",
            Op::Nxl => "nxl",
            Op::Nxr => "nxr",
            Op::Adj => "adj",
        }
    }

    /// The operators PostgREST lets an any or all modifier attach to.
    pub fn quantifiable(self) -> bool {
        matches!(
            self,
            Op::Eq
                | Op::Gt
                | Op::Gte
                | Op::Lt
                | Op::Lte
                | Op::Like
                | Op::Ilike
                | Op::Match
                | Op::Imatch
        )
    }

    /// Full text operators, whose parenthesized modifier is a text
    /// search configuration instead of a quantifier.
    pub fn is_fts(self) -> bool {
        matches!(self, Op::Fts | Op::Plfts | Op::Phfts | Op::Wfts)
    }
}

/// The any or all written after a quantifiable operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quant {
    Any,
    All,
}

/// The connective of a logic group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicOp {
    And,
    Or,
}

impl LogicOp {
    fn name(self) -> &'static str {
        match self {
            LogicOp::And => "and",
            LogicOp::Or => "or",
        }
    }
}

/// Where a condition points: zero or more embedded resource names,
/// the column, and an optional json path into it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub embed: Vec<String>,
    pub column: String,
    pub path: Vec<JsonStep>,
}

/// The right hand side of a condition. A top level scalar is kept
/// verbatim, elements of lists and arrays are unquoted and unescaped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Lit(String),
    /// The parenthesized list an `in` takes.
    List(Vec<String>),
    /// The braced array an any or all quantifier takes.
    Array(Vec<String>),
}

/// One comparison, fully resolved except for types, which need the
/// catalog and belong to the compiler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cond {
    pub field: Field,
    /// A `not.` prefix on the operator.
    pub negated: bool,
    pub op: Op,
    pub quant: Option<Quant>,
    /// The text search configuration of an fts operator.
    pub config: Option<String>,
    pub value: Value,
}

/// A node of a logic tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    Group {
        op: LogicOp,
        negated: bool,
        kids: Vec<Node>,
    },
    Cond(Cond),
}

/// What one query pair parses to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Parsed {
    /// The key named a field and the value held one condition.
    Filter(Cond),
    /// The key was or, and, not.or, or not.and, possibly behind an
    /// embedded resource path, and the value held a tree.
    Logic {
        embed: Vec<String>,
        op: LogicOp,
        negated: bool,
        kids: Vec<Node>,
    },
}

/// Parse one query pair. The caller has already url decoded both
/// sides and decided the key is not one of the reserved words like
/// select, order, or limit.
pub fn parse_pair(key: &str, value: &str) -> Result<Parsed, Error> {
    let mut kc = Cur::new(key);
    let mut segs: Vec<Seg> = Vec::new();
    loop {
        let seg = parse_segment(&mut kc)?;
        let had_path = !seg.path.is_empty();
        segs.push(seg);
        if kc.eat(b'.') {
            if had_path {
                return kc.err("a json path ends the field");
            }
            continue;
        }
        if kc.done() {
            break;
        }
        return kc.err("unexpected character in the field");
    }

    let last = segs.last().expect("the loop always pushes one segment");
    let logic = match (last.quoted, last.path.is_empty(), last.name.as_str()) {
        (false, true, "or") => Some(LogicOp::Or),
        (false, true, "and") => Some(LogicOp::And),
        _ => None,
    };

    if let Some(op) = logic {
        segs.pop();
        let negated = matches!(
            segs.last(),
            Some(s) if !s.quoted && s.path.is_empty() && s.name == "not"
        );
        if negated {
            segs.pop();
        }
        let embed = embed_names(&kc, segs)?;
        let mut vc = Cur::new(value);
        let kids = parse_group_body(&mut vc)?;
        if !vc.done() {
            return vc.err("unexpected characters after the group");
        }
        return Ok(Parsed::Logic {
            embed,
            op,
            negated,
            kids,
        });
    }

    let last = segs.pop().expect("checked non empty above");
    let embed = embed_names(&kc, segs)?;
    let field = Field {
        embed,
        column: last.name,
        path: last.path,
    };
    let mut vc = Cur::new(value);
    let cond = parse_cond_rhs(&mut vc, field, false)?;
    Ok(Parsed::Filter(cond))
}

/// A parsed key segment: a name, its json path, and whether the name
/// was quoted, which exempts it from the logic keywords.
struct Seg {
    name: String,
    path: Vec<JsonStep>,
    quoted: bool,
}

fn embed_names(kc: &Cur, segs: Vec<Seg>) -> Result<Vec<String>, Error> {
    let mut out = Vec::with_capacity(segs.len());
    for s in segs {
        if !s.path.is_empty() {
            return kc.err("a json path ends the field");
        }
        out.push(s.name);
    }
    Ok(out)
}

/// Bytes that end a bare name: the field and value delimiters.
fn name_break(b: u8) -> bool {
    matches!(b, b'.' | b',' | b'(' | b')' | b'"' | b'{' | b'}')
}

fn parse_segment(cur: &mut Cur) -> Result<Seg, Error> {
    let quoted = cur.peek() == Some(b'"');
    let name = if quoted {
        scan_quoted(cur)?
    } else {
        scan_name(cur, name_break)
    };
    if name.is_empty() {
        return cur.err("expected a field name");
    }
    let path = parse_json_path(cur, name_break)?;
    Ok(Seg { name, path, quoted })
}

/// Parse `[not.]op[(modifier)].value` at the cursor. In a tree the
/// literal ends at a comma or closing paren, at top level it runs to
/// the end of the string verbatim.
fn parse_cond_rhs(cur: &mut Cur, field: Field, in_tree: bool) -> Result<Cond, Error> {
    let negated = cur.eat_str("not.");
    let at = cur.pos;
    let opname = scan_name(cur, name_break);
    if opname.is_empty() {
        return cur.err("expected an operator");
    }
    let Some(op) = Op::parse(&opname) else {
        return Err(Error {
            message: format!("unknown operator ({opname})"),
            at,
        });
    };
    let mut quant = None;
    let mut config = None;
    if cur.eat(b'(') {
        let m = scan_name(cur, name_break);
        if op.is_fts() {
            config = Some(m);
        } else if m == "any" && op.quantifiable() {
            quant = Some(Quant::Any);
        } else if m == "all" && op.quantifiable() {
            quant = Some(Quant::All);
        } else {
            return cur.err(format!("unexpected modifier ({m}) for {opname}"));
        }
        cur.expect(b')', "unterminated modifier")?;
    }
    cur.expect(b'.', "expected . after the operator")?;

    let mut value = if op == Op::In {
        cur.expect(b'(', "in wants a (list)")?;
        if cur.peek() == Some(b')') {
            return cur.err("in wants at least one value");
        }
        Value::List(parse_elements(cur, b')', "unterminated list")?)
    } else if quant.is_some() {
        cur.expect(b'{', "any and all want an {array}")?;
        if cur.eat(b'}') {
            Value::Array(Vec::new())
        } else {
            Value::Array(parse_elements(cur, b'}', "unterminated array")?)
        }
    } else if in_tree {
        Value::Lit(scan_element(cur, b",)")?)
    } else {
        Value::Lit(cur.take_rest())
    };

    if !in_tree && !cur.done() {
        return cur.err("unexpected characters after the value");
    }

    if op == Op::Is {
        let Value::Lit(s) = &value else {
            unreachable!("is never takes a list");
        };
        let lower = s.to_ascii_lowercase();
        if !matches!(lower.as_str(), "null" | "true" | "false" | "unknown") {
            return cur.err(format!("is wants null, true, false, or unknown, not ({s})"));
        }
        value = Value::Lit(lower);
    }

    Ok(Cond {
        field,
        negated,
        op,
        quant,
        config,
        value,
    })
}

fn parse_elements(cur: &mut Cur, close: u8, unterminated: &str) -> Result<Vec<String>, Error> {
    let mut out = Vec::new();
    loop {
        out.push(scan_element(cur, &[b',', close])?);
        if cur.eat(b',') {
            continue;
        }
        cur.expect(close, unterminated)?;
        return Ok(out);
    }
}

/// One literal in a delimited context: quoted with escapes, or bare
/// up to the next terminator.
fn scan_element(cur: &mut Cur, terms: &[u8]) -> Result<String, Error> {
    if cur.peek() == Some(b'"') {
        let s = scan_quoted(cur)?;
        if let Some(b) = cur.peek()
            && !terms.contains(&b)
        {
            return cur.err("expected a separator after the quoted value");
        }
        return Ok(s);
    }
    let start = cur.pos;
    while let Some(b) = cur.peek() {
        if terms.contains(&b) {
            break;
        }
        cur.bump();
    }
    Ok(cur.s[start..cur.pos].to_string())
}

/// The body of a logic group, cursor sitting on the opening paren.
fn parse_group_body(cur: &mut Cur) -> Result<Vec<Node>, Error> {
    cur.expect(b'(', "a group wants a (list)")?;
    let mut kids = Vec::new();
    loop {
        kids.push(parse_item(cur)?);
        if cur.eat(b',') {
            continue;
        }
        cur.expect(b')', "unterminated group")?;
        return Ok(kids);
    }
}

fn parse_item(cur: &mut Cur) -> Result<Node, Error> {
    for (prefix, op, negated) in [
        ("not.and(", LogicOp::And, true),
        ("not.or(", LogicOp::Or, true),
        ("and(", LogicOp::And, false),
        ("or(", LogicOp::Or, false),
    ] {
        if cur.starts(prefix) {
            cur.pos += prefix.len() - 1;
            let kids = parse_group_body(cur)?;
            return Ok(Node::Group { op, negated, kids });
        }
    }
    let seg = parse_segment(cur)?;
    cur.expect(b'.', "expected . after the field")?;
    let field = Field {
        embed: Vec::new(),
        column: seg.name,
        path: seg.path,
    };
    parse_cond_rhs(cur, field, true).map(Node::Cond)
}

/// True when a name cannot be written bare: it contains a delimiter,
/// an arrow, or collides with a logic keyword.
fn name_needs_quotes(s: &str) -> bool {
    s.is_empty()
        || s.bytes().any(|b| name_break(b) || b == b'\\')
        || s.contains("->")
        || matches!(s, "or" | "and" | "not")
}

/// True when an element cannot be written bare inside a delimited
/// context.
fn element_needs_quotes(s: &str) -> bool {
    s.is_empty()
        || s.bytes()
            .any(|b| matches!(b, b',' | b'(' | b')' | b'"' | b'{' | b'}' | b'\\'))
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

struct Element<'a>(&'a str);

impl fmt::Display for Element<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if element_needs_quotes(self.0) {
            write_escaped(f, self.0)
        } else {
            write!(f, "{}", self.0)
        }
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for e in &self.embed {
            write!(f, "{}.", Name(e))?;
        }
        write!(f, "{}", Name(&self.column))?;
        fmt_path(f, &self.path)
    }
}

impl Cond {
    /// The operator and value half, `not.gte.18`. Top level literals
    /// print verbatim, delimited ones get quoted as needed.
    fn fmt_rhs(&self, f: &mut fmt::Formatter<'_>, top: bool) -> fmt::Result {
        if self.negated {
            write!(f, "not.")?;
        }
        write!(f, "{}", self.op.name())?;
        if let Some(cfg) = &self.config {
            write!(f, "({cfg})")?;
        }
        match self.quant {
            Some(Quant::Any) => write!(f, "(any)")?,
            Some(Quant::All) => write!(f, "(all)")?,
            None => {}
        }
        write!(f, ".")?;
        match &self.value {
            Value::Lit(s) if top => write!(f, "{s}"),
            Value::Lit(s) => write!(f, "{}", Element(s)),
            Value::List(items) => {
                write!(f, "(")?;
                for (i, s) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{}", Element(s))?;
                }
                write!(f, ")")
            }
            Value::Array(items) => {
                write!(f, "{{")?;
                for (i, s) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{}", Element(s))?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl fmt::Display for Node {
    /// A node as it appears inside a tree.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Node::Group { op, negated, kids } => {
                if *negated {
                    write!(f, "not.")?;
                }
                write!(f, "{}(", op.name())?;
                for (i, kid) in kids.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{kid}")?;
                }
                write!(f, ")")
            }
            Node::Cond(c) => {
                write!(f, "{}.", c.field)?;
                c.fmt_rhs(f, false)
            }
        }
    }
}

impl Parsed {
    /// The canonical query pair for this tree. Reparsing it yields an
    /// equal [`Parsed`], the property the tests and the fuzz target
    /// both check.
    pub fn render(&self) -> (String, String) {
        struct Rhs<'a>(&'a Cond);
        impl fmt::Display for Rhs<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt_rhs(f, true)
            }
        }
        struct Kids<'a>(&'a [Node]);
        impl fmt::Display for Kids<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "(")?;
                for (i, kid) in self.0.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{kid}")?;
                }
                write!(f, ")")
            }
        }
        match self {
            Parsed::Filter(c) => (c.field.to_string(), Rhs(c).to_string()),
            Parsed::Logic {
                embed,
                op,
                negated,
                kids,
            } => {
                let mut key = String::new();
                for e in embed {
                    key.push_str(&Name(e).to_string());
                    key.push('.');
                }
                if *negated {
                    key.push_str("not.");
                }
                key.push_str(op.name());
                (key, Kids(kids).to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(key: &str, value: &str) -> Parsed {
        parse_pair(key, value).unwrap_or_else(|e| panic!("{key}={value} failed: {e}"))
    }

    fn fail(key: &str, value: &str) -> Error {
        match parse_pair(key, value) {
            Ok(p) => panic!("{key}={value} parsed as {p:?}"),
            Err(e) => e,
        }
    }

    fn cond(p: &Parsed) -> &Cond {
        match p {
            Parsed::Filter(c) => c,
            other => panic!("expected a plain filter, got {other:?}"),
        }
    }

    #[test]
    fn every_operator_has_a_spelling_and_parses() {
        let all = [
            "eq",
            "neq",
            "gt",
            "gte",
            "lt",
            "lte",
            "like",
            "ilike",
            "match",
            "imatch",
            "in",
            "is",
            "isdistinct",
            "fts",
            "plfts",
            "phfts",
            "wfts",
            "cs",
            "cd",
            "ov",
            "sl",
            "sr",
            "nxl",
            "nxr",
            "adj",
        ];
        for name in all {
            let op = Op::parse(name).unwrap_or_else(|| panic!("{name} did not parse"));
            assert_eq!(op.name(), name);
        }
        assert!(Op::parse("foo").is_none());
    }

    #[test]
    fn simple_filters_and_negation() {
        let p = ok("age", "gte.18");
        let c = cond(&p);
        assert_eq!(c.op, Op::Gte);
        assert!(!c.negated);
        assert_eq!(c.value, Value::Lit("18".into()));
        assert_eq!(c.field.column, "age");
        assert!(c.field.embed.is_empty() && c.field.path.is_empty());

        let p = ok("age", "not.eq.18");
        assert!(cond(&p).negated);
    }

    #[test]
    fn a_top_level_literal_is_verbatim_to_the_end() {
        let p = ok("name", "eq.John.Smith, \"the second\"");
        assert_eq!(
            cond(&p).value,
            Value::Lit("John.Smith, \"the second\"".into())
        );
        let p = ok("name", "eq.");
        assert_eq!(cond(&p).value, Value::Lit("".into()));
        let p = ok("range", "sl.(1,10)");
        assert_eq!(cond(&p).value, Value::Lit("(1,10)".into()));
        let p = ok("tags", "cs.{a,b}");
        assert_eq!(cond(&p).value, Value::Lit("{a,b}".into()));
    }

    #[test]
    fn in_lists_with_quoting_and_escapes() {
        let p = ok("id", "in.(1,2,3)");
        assert_eq!(
            cond(&p).value,
            Value::List(vec!["1".into(), "2".into(), "3".into()])
        );
        let p = ok("name", "in.(\"a,b\",c,\"d\\\"e\",\"\")");
        assert_eq!(
            cond(&p).value,
            Value::List(vec!["a,b".into(), "c".into(), "d\"e".into(), "".into()])
        );
        assert_eq!(fail("id", "in.()").message, "in wants at least one value");
        assert_eq!(fail("id", "in.(1,2").message, "unterminated list");
        assert_eq!(
            fail("id", "in.(1)x").message,
            "unexpected characters after the value"
        );
        assert_eq!(
            fail("id", "in.(\"a\"b)").message,
            "expected a separator after the quoted value"
        );
    }

    #[test]
    fn quantifiers_take_arrays() {
        let p = ok("age", "eq(any).{18,21,65}");
        let c = cond(&p);
        assert_eq!(c.quant, Some(Quant::Any));
        assert_eq!(
            c.value,
            Value::Array(vec!["18".into(), "21".into(), "65".into()])
        );
        let p = ok("name", "like(all).{O*,*i}");
        assert_eq!(cond(&p).quant, Some(Quant::All));
        let p = ok("age", "gt(any).{}");
        assert_eq!(cond(&p).value, Value::Array(vec![]));
        assert!(
            fail("id", "in(any).(1)")
                .message
                .contains("unexpected modifier")
        );
        assert!(
            fail("age", "eq(some).{1}")
                .message
                .contains("unexpected modifier")
        );
        assert_eq!(
            fail("age", "eq(any).1").message,
            "any and all want an {array}"
        );
    }

    #[test]
    fn fts_modifiers_are_configs() {
        let p = ok("doc", "fts(english).fat & rat");
        let c = cond(&p);
        assert_eq!(c.op, Op::Fts);
        assert_eq!(c.config.as_deref(), Some("english"));
        assert_eq!(c.value, Value::Lit("fat & rat".into()));
        let p = ok("doc", "plfts.amusant");
        assert_eq!(cond(&p).config, None);
    }

    #[test]
    fn is_normalizes_and_validates() {
        let p = ok("flag", "is.TRUE");
        assert_eq!(cond(&p).value, Value::Lit("true".into()));
        assert_eq!(p.render(), ("flag".into(), "is.true".into()));
        for v in ["null", "true", "false", "unknown"] {
            ok("flag", &format!("is.{v}"));
        }
        assert!(fail("flag", "is.maybe").message.contains("is wants"));
        ok("flag", "not.is.null");
    }

    #[test]
    fn json_paths_parse_into_steps() {
        let p = ok("data->prefs->>theme", "eq.dark");
        let c = cond(&p);
        assert_eq!(c.field.column, "data");
        assert_eq!(
            c.field.path,
            vec![
                JsonStep {
                    text: false,
                    key: JsonKey::Name("prefs".into())
                },
                JsonStep {
                    text: true,
                    key: JsonKey::Name("theme".into())
                },
            ]
        );
        let p = ok("data->2->>name", "eq.x");
        assert_eq!(cond(&p).field.path[0].key, JsonKey::Index(2));
        let p = ok("data->-1", "eq.9");
        assert_eq!(cond(&p).field.path[0].key, JsonKey::Index(-1));
        assert_eq!(
            fail("a->>b.c", "eq.1").message,
            "a json path ends the field"
        );
    }

    #[test]
    fn embedded_paths_come_off_the_key() {
        let p = ok("author.name", "eq.John");
        let c = cond(&p);
        assert_eq!(c.field.embed, vec!["author".to_string()]);
        assert_eq!(c.field.column, "name");
        let p = ok("a.b.c", "eq.1");
        assert_eq!(cond(&p).field.embed, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn logic_trees_nest_and_negate() {
        let p = ok("or", "(age.gte.18,age.lte.65)");
        let Parsed::Logic {
            embed,
            op,
            negated,
            kids,
        } = &p
        else {
            panic!("expected logic, got {p:?}");
        };
        assert!(embed.is_empty() && !negated);
        assert_eq!(*op, LogicOp::Or);
        assert_eq!(kids.len(), 2);

        let p = ok("or", "(a.eq.1,not.and(b.eq.2,c.eq.3))");
        let Parsed::Logic { kids, .. } = &p else {
            panic!()
        };
        let Node::Group { op, negated, kids } = &kids[1] else {
            panic!("expected a nested group, got {:?}", kids[1]);
        };
        assert_eq!(*op, LogicOp::And);
        assert!(*negated);
        assert_eq!(kids.len(), 2);

        let p = ok("not.or", "(a.eq.1,b.eq.2)");
        let Parsed::Logic { negated, .. } = &p else {
            panic!()
        };
        assert!(*negated);

        let p = ok("author.not.and", "(a.eq.1,b.eq.2)");
        let Parsed::Logic { embed, negated, .. } = &p else {
            panic!()
        };
        assert_eq!(embed, &["author".to_string()]);
        assert!(*negated);
    }

    #[test]
    fn tree_conditions_quote_and_carry_everything_conds_do() {
        let p = ok("or", "(name.eq.\"Smith, John\",id.in.(1,2),b.not.like.x*)");
        let Parsed::Logic { kids, .. } = &p else {
            panic!()
        };
        let Node::Cond(c) = &kids[0] else { panic!() };
        assert_eq!(c.value, Value::Lit("Smith, John".into()));
        let Node::Cond(c) = &kids[1] else { panic!() };
        assert_eq!(c.value, Value::List(vec!["1".into(), "2".into()]));
        let Node::Cond(c) = &kids[2] else { panic!() };
        assert!(c.negated);
        assert_eq!(c.value, Value::Lit("x*".into()));

        let p = ok("or", "(data->>k.eq.1,a.eq(any).{1,2})");
        let Parsed::Logic { kids, .. } = &p else {
            panic!()
        };
        let Node::Cond(c) = &kids[0] else { panic!() };
        assert_eq!(c.field.path.len(), 1);
        let Node::Cond(c) = &kids[1] else { panic!() };
        assert_eq!(c.quant, Some(Quant::Any));
    }

    #[test]
    fn tree_errors_point_at_the_problem() {
        assert_eq!(fail("or", "(a.eq.1").message, "unterminated group");
        assert_eq!(fail("or", "()").message, "expected a field name");
        assert_eq!(
            fail("or", "(a.eq.1)x").message,
            "unexpected characters after the group"
        );
        assert_eq!(fail("or", "a.eq.1").message, "a group wants a (list)");
        assert!(fail("or", "(a.foo.1)").message.contains("unknown operator"));
        assert_eq!(
            fail("or", "(a.eq)").message,
            "expected . after the operator"
        );
    }

    #[test]
    fn key_errors() {
        assert_eq!(fail("", "eq.1").message, "expected a field name");
        assert_eq!(fail("a.", "eq.1").message, "expected a field name");
        assert_eq!(fail("a..b", "eq.1").message, "expected a field name");
        assert_eq!(fail("age", "gte").message, "expected . after the operator");
        assert!(fail("age", "foo.1").message.contains("unknown operator"));
        assert_eq!(fail("a->>", "eq.1").message, "expected a json path key");
    }

    #[test]
    fn quoted_names_dodge_the_keywords() {
        let p = ok("\"weird.name\"", "eq.1");
        assert_eq!(cond(&p).field.column, "weird.name");
        let p = ok("\"or\"", "eq.1");
        assert!(matches!(&p, Parsed::Filter(_)));
        assert_eq!(p.render().0, "\"or\"");
        let p = ok("or", "(\"not\".eq.1)");
        let Parsed::Logic { kids, .. } = &p else {
            panic!()
        };
        let Node::Cond(c) = &kids[0] else { panic!() };
        assert_eq!(c.field.column, "not");
    }

    const CORPUS: &[(&str, &str)] = &[
        ("age", "eq.18"),
        ("age", "not.gte.18"),
        ("name", "like.*john*"),
        ("name", "ilike.%J%"),
        ("name", "match.^j.*n$"),
        ("name", "imatch.^J"),
        ("data->>role", "eq.admin"),
        ("data->prefs->>theme", "eq.dark"),
        ("data->2->>name", "eq.x"),
        ("data->-1", "eq.9"),
        ("author.name", "eq.John Smith"),
        ("a.b.c", "neq.1"),
        ("id", "in.(1,2,3)"),
        ("id", "not.in.(1,2)"),
        ("name", "in.(\"a,b\",c,\"d\\\"e\",\"\")"),
        ("flag", "is.null"),
        ("flag", "is.TRUE"),
        ("flag", "not.is.unknown"),
        ("flag", "isdistinct.5"),
        ("doc", "fts.cat"),
        ("doc", "fts(english).fat & rat"),
        ("doc", "plfts(french).amusant"),
        ("doc", "phfts(english).The Fat Cats"),
        ("doc", "wfts.jumping quickly"),
        ("tags", "cs.{a,b}"),
        ("tags", "cd.{a,b,c}"),
        ("period", "ov.[2000-01-01,2000-12-31]"),
        ("range", "sl.(1,10)"),
        ("range", "sr.(1,10)"),
        ("range", "nxl.(1,10)"),
        ("range", "nxr.(1,10)"),
        ("range", "adj.(1,10)"),
        ("age", "eq(any).{18,21,65}"),
        ("name", "like(all).{O*,*i}"),
        ("age", "gt(any).{}"),
        ("name", "eq."),
        ("\"weird.name\"", "eq.1"),
        ("or", "(age.gte.18,age.lte.65)"),
        ("and", "(a.eq.1,b.eq.2)"),
        ("not.or", "(a.eq.1,b.eq.2)"),
        ("or", "(a.eq.1,and(b.eq.2,c.eq.3))"),
        ("or", "(a.eq.1,not.and(b.eq.2,c.eq.3))"),
        ("author.or", "(age.gte.18,age.lte.65)"),
        ("author.not.and", "(a.eq.1,b.eq.2)"),
        ("or", "(name.eq.\"Smith, John\",age.gte.18)"),
        ("or", "(id.in.(1,2),b.not.like.x*)"),
        ("or", "(data->>k.eq.1,b.is.null)"),
        ("or", "(a.eq(any).{1,2},b.eq.2)"),
        ("or", "(\"not\".eq.1,\"or\".eq.2)"),
    ];

    #[test]
    fn the_canonical_render_reparses_to_the_same_tree() {
        for (key, value) in CORPUS {
            let first = ok(key, value);
            let (rk, rv) = first.render();
            let second = parse_pair(&rk, &rv)
                .unwrap_or_else(|e| panic!("render of {key}={value} gave {rk}={rv}: {e}"));
            assert_eq!(first, second, "{key}={value} via {rk}={rv}");
            let (rk2, rv2) = second.render();
            assert_eq!((rk, rv), (rk2, rv2), "render must be a fixpoint");
        }
    }
}
