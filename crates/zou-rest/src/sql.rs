//! Filter tree to parameterized SQL.
//!
//! Every literal a client sends becomes a $n parameter, never a
//! spliced string, and postgres infers the parameter type from the
//! column it faces, which is the same coercion PostgREST leans on
//! when it inlines unknown typed literals. The type aware pieces live
//! where inference cannot reach: `is` compiles to the IS keywords the
//! grammar already validated, like and ilike translate `*` to `%` in
//! the pattern, quantified arrays re-render as one postgres array
//! literal parameter, and a text search configuration binds behind a
//! ::regconfig cast.
//!
//! A column whose type reads values out of text through a cast, a
//! data representation, is the one place a literal does not face its
//! column bare: the placeholder goes through that function first, on
//! every operator that compares the column with a value of it. Which
//! operators those are was read off a live 14.15 rather than reasoned
//! about, and the surprise is `isdistinct`, which compares and is
//! left uncast anyway.
//!
//! Conditions on embedded resources need the fk graph and the lateral
//! join shape, so anything with an embed path is refused here and
//! waits for the embedding planner.

use std::fmt;

use crate::catalog::Relation;
use crate::filter::{Cond, LogicOp, Node, Op, Quant, Value};
use crate::scan::{JsonKey, JsonStep};

/// Why a tree cannot compile. No offsets here, the input already
/// parsed, the problem is what it asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompileError {
    pub message: String,
}

impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for CompileError {}

fn err<T>(message: impl Into<String>) -> Result<T, CompileError> {
    Err(CompileError {
        message: message.into(),
    })
}

/// A SQL fragment and the parameters it references, $1 through
/// $params.len() with no gaps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sql {
    pub text: String,
    pub params: Vec<String>,
}

/// What an embedded resource answers when a filter names it rather
/// than a column: `posts=not.is.null` asks whether the embed found
/// anything. The two predicates are handed in already rendered
/// because what they are depends on the query. The read query has a
/// lateral join to point at, and the count query, which has no
/// laterals at all, has an EXISTS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbedTest {
    /// The name the embed goes by, its alias or its relation.
    pub key: String,
    /// What `not.is.null` compiles to.
    pub present: String,
    /// What `is.null` compiles to.
    pub absent: String,
}

/// Compile the conjunction of these trees into one WHERE fragment.
/// Every query pair on a request is one node, PostgREST ands them.
pub fn where_clause(nodes: &[Node]) -> Result<Sql, CompileError> {
    where_clause_from(nodes, None, Vec::new(), None)
}

/// The planner's entry: columns qualify with `qualifier` when given,
/// placeholder numbering continues from wherever `params` already
/// stands, so fragments from different subqueries share one dense
/// parameter list, and `rel` is the relation the columns belong to,
/// which is how a literal finds the cast its column reads text
/// through. No relation is every column taken bare.
pub fn where_clause_from(
    nodes: &[Node],
    qualifier: Option<&str>,
    params: Vec<String>,
    rel: Option<&Relation>,
) -> Result<Sql, CompileError> {
    where_clause_over(nodes, qualifier, params, rel, &[])
}

/// The same, for a level that has embedded resources a filter is
/// allowed to name. Only `is.null` and `not.is.null` reach them: every
/// other operator reads the name as a column, which is upstream's own
/// rule and is why a table with a column and an embed of the same name
/// answers both.
pub fn where_clause_over(
    nodes: &[Node],
    qualifier: Option<&str>,
    params: Vec<String>,
    rel: Option<&Relation>,
    embeds: &[EmbedTest],
) -> Result<Sql, CompileError> {
    if nodes.is_empty() {
        return err("nothing to compile");
    }
    let mut c = Compiler {
        params,
        qualifier: qualifier.map(str::to_string),
        rel,
        embeds,
    };
    let mut parts = Vec::with_capacity(nodes.len());
    for node in nodes {
        parts.push(c.node(node)?);
    }
    Ok(Sql {
        text: parts.join(" AND "),
        params: c.params,
    })
}

struct Compiler<'a> {
    params: Vec<String>,
    qualifier: Option<String>,
    rel: Option<&'a Relation>,
    embeds: &'a [EmbedTest],
}

impl Compiler<'_> {
    fn bind(&mut self, value: &str) -> String {
        self.params.push(value.to_string());
        format!("${}", self.params.len())
    }

    /// A literal bound and then read the way its column reads text.
    /// The function wraps whatever the placeholder stands for, which
    /// for a quantifier is a whole array and is why upstream refuses
    /// that pair too rather than casting element by element.
    fn bind_read(&mut self, read: Option<&str>, value: &str) -> String {
        let hole = self.bind(value);
        match read {
            Some(func) => format!("{func}({hole})"),
            None => hole,
        }
    }

    /// The cast this condition's column reads text through, if its
    /// type has one and if the operator is comparing the column with
    /// a value of it.
    ///
    /// Three kinds of condition are not. A pattern is a pattern
    /// rather than a value, and a live 14.15 leaves `like` and
    /// `ilike` bound as they came, quantified or not. A json path
    /// steps inside the column and lands on json rather than on the
    /// column's own type. `is` takes a keyword and the full text
    /// family takes a query, neither of which reaches this at all.
    ///
    /// `isdistinct` is the odd one, since it is a comparison and
    /// upstream still leaves it uncast. Recorded rather than
    /// reasoned: that is what the binary generates.
    fn read_cast(&self, c: &Cond) -> Option<String> {
        if pattern_op(c.op) || matches!(c.op, Op::IsDistinct) || !c.field.path.is_empty() {
            return None;
        }
        self.rel?.column(&c.field.column)?.from_text.clone()
    }

    fn node(&mut self, node: &Node) -> Result<String, CompileError> {
        match node {
            Node::Cond(c) => self.cond(c),
            Node::Group { op, negated, kids } => {
                if kids.is_empty() {
                    return err("an empty logic group");
                }
                let sep = match op {
                    LogicOp::And => " AND ",
                    LogicOp::Or => " OR ",
                };
                let mut parts = Vec::with_capacity(kids.len());
                for kid in kids {
                    parts.push(self.node(kid)?);
                }
                let body = parts.join(sep);
                Ok(if *negated {
                    format!("NOT ({body})")
                } else {
                    format!("({body})")
                })
            }
        }
    }

    /// The predicate a null test on an embedded resource compiles to,
    /// when the name is one and the test is that. The embed wins over
    /// a column of the same name here and loses to it everywhere
    /// else, which is what a live 14.15 does.
    fn embed_test(&self, c: &Cond) -> Option<&str> {
        if c.op != Op::Is || !c.field.path.is_empty() || c.quant.is_some() {
            return None;
        }
        let Value::Lit(word) = &c.value else {
            return None;
        };
        if word != "null" {
            return None;
        }
        let test = self.embeds.iter().find(|e| e.key == c.field.column)?;
        Some(match c.negated {
            true => &test.present,
            false => &test.absent,
        })
    }

    fn cond(&mut self, c: &Cond) -> Result<String, CompileError> {
        if !c.field.embed.is_empty() {
            return err(format!(
                "a filter on the embedded resource ({}) needs the embedding planner",
                c.field.embed.join(".")
            ));
        }
        if let Some(test) = self.embed_test(c) {
            return Ok(test.to_string());
        }
        let lhs = field_expr(self.qualifier.as_deref(), &c.field.column, &c.field.path);
        let read = self.read_cast(c);
        let read = read.as_deref();

        let expr = if let Some(quant) = c.quant {
            let Value::Array(elems) = &c.value else {
                unreachable!("the grammar gives quantifiers an array");
            };
            let sym = plain_symbol(c.op).expect("the grammar quantifies plain operators only");
            let literal = array_literal(elems, pattern_op(c.op));
            let q = match quant {
                Quant::Any => "ANY",
                Quant::All => "ALL",
            };
            format!("{lhs} {sym} {q}({})", self.bind_read(read, &literal))
        } else {
            match c.op {
                Op::In => {
                    let Value::List(elems) = &c.value else {
                        unreachable!("the grammar gives in a list");
                    };
                    // A list of one empty element is the empty list,
                    // which `IN ()` has no syntax for. Upstream writes
                    // every list as an array to have somewhere to put
                    // this one, and the empty array is what it writes.
                    if elems.len() == 1 && elems[0].is_empty() {
                        format!("{lhs} = ANY('{{}}')")
                    } else {
                        let holes: Vec<String> =
                            elems.iter().map(|e| self.bind_read(read, e)).collect();
                        format!("{lhs} IN ({})", holes.join(", "))
                    }
                }
                Op::Is => {
                    let Value::Lit(word) = &c.value else {
                        unreachable!("is never takes a list");
                    };
                    // The grammar lowercased and validated the word.
                    // Only one of them is two words in SQL.
                    match word.as_str() {
                        "not_null" => format!("{lhs} IS NOT NULL"),
                        w => format!("{lhs} IS {}", w.to_ascii_uppercase()),
                    }
                }
                Op::IsDistinct => {
                    let lit = lit_value(&c.value);
                    format!("{lhs} IS DISTINCT FROM {}", self.bind_read(read, lit))
                }
                Op::Fts | Op::Plfts | Op::Phfts | Op::Wfts => {
                    let func = match c.op {
                        Op::Fts => "to_tsquery",
                        Op::Plfts => "plainto_tsquery",
                        Op::Phfts => "phraseto_tsquery",
                        Op::Wfts => "websearch_to_tsquery",
                        _ => unreachable!(),
                    };
                    match &c.config {
                        Some(cfg) => {
                            // Bound, not spliced: the config came off
                            // the wire like everything else.
                            let cfg = self.bind(cfg);
                            let query = self.bind(lit_value(&c.value));
                            format!("{lhs} @@ {func}({cfg}::regconfig, {query})")
                        }
                        None => {
                            let query = self.bind(lit_value(&c.value));
                            format!("{lhs} @@ {func}({query})")
                        }
                    }
                }
                _ => {
                    let sym = plain_symbol(c.op).expect("the special operators are matched above");
                    let mut lit = lit_value(&c.value).to_string();
                    if pattern_op(c.op) {
                        lit = lit.replace('*', "%");
                    }
                    format!("{lhs} {sym} {}", self.bind_read(read, &lit))
                }
            }
        };

        Ok(if c.negated {
            format!("NOT ({expr})")
        } else {
            expr
        })
    }
}

fn lit_value(value: &Value) -> &str {
    match value {
        Value::Lit(s) => s,
        Value::List(_) | Value::Array(_) => {
            unreachable!("the grammar only builds lists for in and arrays for quantifiers")
        }
    }
}

/// The operators that read `*` as a wildcard.
fn pattern_op(op: Op) -> bool {
    matches!(op, Op::Like | Op::Ilike)
}

/// The SQL spelling of an operator that sits between two expressions.
fn plain_symbol(op: Op) -> Option<&'static str> {
    Some(match op {
        Op::Eq => "=",
        Op::Neq => "<>",
        Op::Gt => ">",
        Op::Gte => ">=",
        Op::Lt => "<",
        Op::Lte => "<=",
        Op::Like => "LIKE",
        Op::Ilike => "ILIKE",
        Op::Match => "~",
        Op::Imatch => "~*",
        Op::Cs => "@>",
        Op::Cd => "<@",
        Op::Ov => "&&",
        Op::Sl => "<<",
        Op::Sr => ">>",
        Op::Nxr => "&<",
        Op::Nxl => "&>",
        Op::Adj => "-|-",
        Op::In | Op::Is | Op::IsDistinct | Op::Fts | Op::Plfts | Op::Phfts | Op::Wfts => {
            return None;
        }
    })
}

/// A quantifier array as one postgres array literal. Every element is
/// quoted, which sidesteps the whole special character table and
/// keeps a literal "null" a string instead of a null.
fn array_literal(elems: &[String], patterns: bool) -> String {
    let mut out = String::from("{");
    for (i, e) in elems.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        for ch in e.chars() {
            let ch = if patterns && ch == '*' { '%' } else { ch };
            if ch == '"' || ch == '\\' {
                out.push('\\');
            }
            out.push(ch);
        }
        out.push('"');
    }
    out.push('}');
    out
}

/// A column reference and its json path, name keys as single quoted
/// strings, index keys as bare integers, qualified when the caller
/// works across more than one table in scope. The planner leans on
/// this for select items and order terms too.
pub(crate) fn field_expr(qualifier: Option<&str>, column: &str, path: &[JsonStep]) -> String {
    let mut out = String::new();
    if let Some(q) = qualifier {
        out.push_str(&quote_ident(q));
        out.push('.');
    }
    out.push_str(&quote_ident(column));
    for JsonStep { text, key } in path {
        out.push_str(if *text { "->>" } else { "->" });
        match key {
            JsonKey::Name(n) => {
                out.push('\'');
                for ch in n.chars() {
                    if ch == '\'' {
                        out.push('\'');
                    }
                    out.push(ch);
                }
                out.push('\'');
            }
            JsonKey::Index(i) => out.push_str(&i.to_string()),
        }
    }
    out
}

pub(crate) fn quote_ident(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    out.push('"');
    for ch in name.chars() {
        if ch == '"' {
            out.push('"');
        }
        out.push(ch);
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Column;
    use crate::filter::{Parsed, parse_pair};

    fn node(key: &str, value: &str) -> Node {
        match parse_pair(key, value).unwrap_or_else(|e| panic!("{key}={value} failed: {e}")) {
            Parsed::Filter(c) => Node::Cond(c),
            Parsed::Logic {
                embed,
                op,
                negated,
                kids,
            } => {
                assert!(embed.is_empty(), "tests keep logic at the top level");
                Node::Group { op, negated, kids }
            }
        }
    }

    fn sql(key: &str, value: &str) -> Sql {
        where_clause(&[node(key, value)]).unwrap_or_else(|e| panic!("{key}={value}: {e}"))
    }

    #[test]
    fn plain_comparisons() {
        let s = sql("age", "gte.18");
        assert_eq!(s.text, r#""age" >= $1"#);
        assert_eq!(s.params, vec!["18"]);

        let s = sql("name", "eq.John");
        assert_eq!(s.text, r#""name" = $1"#);
        assert_eq!(s.params, vec!["John"]);

        let s = sql("age", "not.eq.18");
        assert_eq!(s.text, r#"NOT ("age" = $1)"#);
    }

    #[test]
    fn every_plain_operator_spells_right() {
        for (v, sym) in [
            ("eq.1", "="),
            ("neq.1", "<>"),
            ("gt.1", ">"),
            ("gte.1", ">="),
            ("lt.1", "<"),
            ("lte.1", "<="),
            ("match.^a", "~"),
            ("imatch.^a", "~*"),
            ("cs.{1}", "@>"),
            ("cd.{1}", "<@"),
            ("ov.[1,2]", "&&"),
            ("sl.(1,2)", "<<"),
            ("sr.(1,2)", ">>"),
            ("nxr.(1,2)", "&<"),
            ("nxl.(1,2)", "&>"),
            ("adj.(1,2)", "-|-"),
        ] {
            let s = sql("c", v);
            assert_eq!(s.text, format!(r#""c" {sym} $1"#), "for {v}");
        }
    }

    #[test]
    fn like_translates_stars() {
        let s = sql("name", "like.O*Brien*");
        assert_eq!(s.text, r#""name" LIKE $1"#);
        assert_eq!(s.params, vec!["O%Brien%"]);

        let s = sql("name", "ilike.*son");
        assert_eq!(s.params, vec!["%son"]);

        // Regexes keep their stars.
        let s = sql("name", "match.a*b");
        assert_eq!(s.params, vec!["a*b"]);
    }

    #[test]
    fn in_lists_bind_each_element() {
        let s = sql("status", "in.(active,\"on hold\",done)");
        assert_eq!(s.text, r#""status" IN ($1, $2, $3)"#);
        assert_eq!(s.params, vec!["active", "on hold", "done"]);
    }

    #[test]
    fn an_empty_in_list_asks_the_empty_array() {
        // `IN ()` is not syntax and `= ANY('{}')` is, which is why
        // the empty list is written as an array and binds nothing.
        let s = sql("id", "in.()");
        assert_eq!(s.text, r#""id" = ANY('{}')"#);
        assert!(s.params.is_empty());
        let s = sql("id", "not.in.()");
        assert_eq!(s.text, r#"NOT ("id" = ANY('{}'))"#);
    }

    #[test]
    fn is_and_isdistinct() {
        assert_eq!(sql("deleted_at", "is.null").text, r#""deleted_at" IS NULL"#);
        assert_eq!(sql("active", "is.true").text, r#""active" IS TRUE"#);
        assert_eq!(sql("active", "is.false").text, r#""active" IS FALSE"#);
        assert_eq!(sql("active", "is.unknown").text, r#""active" IS UNKNOWN"#);
        assert_eq!(
            sql("deleted_at", "is.not_null").text,
            r#""deleted_at" IS NOT NULL"#
        );
        assert_eq!(
            sql("active", "not.is.null").text,
            r#"NOT ("active" IS NULL)"#
        );

        let s = sql("a", "isdistinct.b");
        assert_eq!(s.text, r#""a" IS DISTINCT FROM $1"#);
        assert_eq!(s.params, vec!["b"]);
    }

    #[test]
    fn quantifiers_render_one_array_parameter() {
        let s = sql("name", "eq(any).{alice,bob}");
        assert_eq!(s.text, r#""name" = ANY($1)"#);
        assert_eq!(s.params, vec![r#"{"alice","bob"}"#]);

        let s = sql("name", "like(all).{O*,P*}");
        assert_eq!(s.text, r#""name" LIKE ALL($1)"#);
        assert_eq!(s.params, vec![r#"{"O%","P%"}"#]);

        // Quoting keeps a literal null a string and protects the
        // characters the array syntax cares about.
        let s = sql("name", "eq(any).{null,\"a,b\",\"c\\\"d\"}");
        assert_eq!(s.params, vec![r#"{"null","a,b","c\"d"}"#]);

        let s = sql("name", "eq(any).{}");
        assert_eq!(s.params, vec!["{}"]);
    }

    #[test]
    fn full_text_search() {
        let s = sql("body", "fts.cat");
        assert_eq!(s.text, r#""body" @@ to_tsquery($1)"#);
        assert_eq!(s.params, vec!["cat"]);

        let s = sql("body", "plfts(french).chat");
        assert_eq!(s.text, r#""body" @@ plainto_tsquery($1::regconfig, $2)"#);
        assert_eq!(s.params, vec!["french", "chat"]);

        assert_eq!(
            sql("body", "phfts.the cat").text,
            r#""body" @@ phraseto_tsquery($1)"#
        );
        assert_eq!(
            sql("body", "wfts.cat or dog").text,
            r#""body" @@ websearch_to_tsquery($1)"#
        );
    }

    #[test]
    fn json_paths_and_identifier_quoting() {
        let s = sql("data->phones->0->>number", "eq.123");
        assert_eq!(s.text, r#""data"->'phones'->0->>'number' = $1"#);

        let s = sql("\"weird\\\"name\"", "eq.1");
        assert_eq!(s.text, r#""weird""name" = $1"#);

        let s = sql("data->>it's", "eq.1");
        assert_eq!(s.text, r#""data"->>'it''s' = $1"#);

        // A key that spells like a placeholder stays a quoted
        // literal, only the bind on the right is a parameter.
        let s = sql("data->$1", "eq.x");
        assert_eq!(s.text, r#""data"->'$1' = $1"#);
        assert_eq!(s.params, vec!["x"]);
    }

    #[test]
    fn logic_trees() {
        let s = sql("or", "(age.gte.18,age.is.null)");
        assert_eq!(s.text, r#"("age" >= $1 OR "age" IS NULL)"#);
        assert_eq!(s.params, vec!["18"]);

        let s = sql("not.and", "(a.eq.1,or(b.eq.2,c.eq.3))");
        assert_eq!(s.text, r#"NOT ("a" = $1 AND ("b" = $2 OR "c" = $3))"#);
        assert_eq!(s.params, vec!["1", "2", "3"]);
    }

    #[test]
    fn pairs_and_together() {
        let s = where_clause(&[node("age", "gte.18"), node("name", "like.J*")]).unwrap();
        assert_eq!(s.text, r#""age" >= $1 AND "name" LIKE $2"#);
        assert_eq!(s.params, vec!["18", "J%"]);
    }

    #[test]
    fn embedded_fields_wait_for_the_planner() {
        let e = where_clause(&[node("posts.author", "eq.1")]).unwrap_err();
        assert!(e.message.contains("embedding planner"), "{e}");

        let e = where_clause(&[]).unwrap_err();
        assert_eq!(e.message, "nothing to compile");
    }

    /// A relation whose `label_color` reads text through a cast and
    /// whose `name` is a plain one.
    fn painted() -> Relation {
        Relation {
            name: "todos".into(),
            columns: vec![
                Column {
                    name: "label_color".into(),
                    to_json: Some("test.json".into()),
                    from_text: Some("test.color".into()),
                    from_json: Some("test.color".into()),
                    type_name: "test.color".into(),
                    default_expr: None,
                },
                Column {
                    name: "name".into(),
                    ..Column::default()
                },
            ],
        }
    }

    fn painted_clause(key: &str, value: &str) -> Sql {
        let rel = painted();
        where_clause_from(&[node(key, value)], None, Vec::new(), Some(&rel))
            .unwrap_or_else(|e| panic!("{key}={value} failed: {e}"))
    }

    #[test]
    fn a_url_value_is_read_through_the_cast_its_column_has() {
        let s = painted_clause("label_color", "eq.red");
        assert_eq!(s.text, r#""label_color" = test.color($1)"#);
        assert_eq!(s.params, vec!["red"]);

        // A column of a plain type, and a column of no relation at
        // all, are bound the way they always were.
        assert_eq!(painted_clause("name", "eq.x").text, r#""name" = $1"#);
        assert_eq!(
            where_clause(&[node("label_color", "eq.red")]).unwrap().text,
            r#""label_color" = $1"#
        );
    }

    #[test]
    fn a_comparison_reads_its_value_whatever_the_operator_is() {
        // Every one of these is generated with the call in it by a
        // live 14.15, including the one where the call cannot work.
        // Upstream wraps the placeholder without asking whether the
        // function will take what it stands for, so a quantifier
        // hands it a whole array and postgres answers 42809.
        assert_eq!(
            painted_clause("label_color", "gte.red").text,
            r#""label_color" >= test.color($1)"#
        );
        assert_eq!(
            painted_clause("label_color", "match.^re").text,
            r#""label_color" ~ test.color($1)"#
        );
        assert_eq!(
            painted_clause("label_color", "cs.red").text,
            r#""label_color" @> test.color($1)"#
        );
        assert_eq!(
            painted_clause("label_color", "not.eq.red").text,
            r#"NOT ("label_color" = test.color($1))"#
        );
        assert_eq!(
            painted_clause("label_color", "in.(red,blue)").text,
            r#""label_color" IN (test.color($1), test.color($2))"#
        );
        assert_eq!(
            painted_clause("label_color", "eq(any).{red,blue}").text,
            r#""label_color" = ANY(test.color($1))"#
        );
    }

    #[test]
    fn what_is_not_a_value_of_the_column_is_not_cast() {
        // A pattern is a pattern rather than a colour, `is` takes a
        // keyword, full text search takes a query, and a json path
        // has left the column's type behind before it is compared.
        // `isdistinct` is a comparison and is uncast anyway, which
        // is upstream's own exception and not a rule anybody would
        // arrive at.
        assert_eq!(
            painted_clause("label_color", "like.re*").text,
            r#""label_color" LIKE $1"#
        );
        assert_eq!(
            painted_clause("label_color", "ilike(all).{re*,b*}").text,
            r#""label_color" ILIKE ALL($1)"#
        );
        assert_eq!(
            painted_clause("label_color", "isdistinct.red").text,
            r#""label_color" IS DISTINCT FROM $1"#
        );
        assert_eq!(
            painted_clause("label_color", "is.null").text,
            r#""label_color" IS NULL"#
        );
        assert_eq!(
            painted_clause("label_color", "fts.cat").text,
            r#""label_color" @@ to_tsquery($1)"#
        );
        assert_eq!(
            painted_clause("label_color->>shade", "eq.dark").text,
            r#""label_color"->>'shade' = $1"#
        );
    }

    #[test]
    fn placeholders_are_dense_and_ordered() {
        let s = where_clause(&[
            node("or", "(a.in.(1,2),b.eq(any).{x,y})"),
            node("body", "fts(english).cat"),
        ])
        .unwrap();
        for i in 1..=s.params.len() {
            assert!(
                s.text.contains(&format!("${i}")),
                "missing ${i} in {}",
                s.text
            );
        }
        assert_eq!(s.params.len(), 5);
    }
}
