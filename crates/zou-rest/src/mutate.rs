//! Mutation statements: insert, upsert, update, and delete.
//!
//! The body a client sends never splices into SQL. It binds whole as
//! one json parameter and json_populate_recordset unpacks it against
//! the table's row type, so postgres types every value the same way
//! it types the read path's filter literals. The only identifiers
//! spliced here are the column names lifted from the body's keys and
//! the conflict targets, and every one goes through quote_ident.
//!
//! Update and delete compile their filters through the same WHERE
//! compiler as reads, qualified with the table name because the
//! update shape has the payload relation in scope too. Placeholders
//! stay dense: the payload is $1 and filter literals follow.
//!
//! Prefer return=representation plans the request's select tree over
//! the mutation's returned rows: the mutation mounts as a CTE named
//! "_zou_mut" and the planner's root level reads from it while
//! relationships still resolve against the real table, so embeds
//! work on writes exactly as on reads.

use crate::catalog::Catalog;
use crate::filter::Node;
use crate::plan::{PlanError, Query, plan_from};
use crate::sql::{CompileError, Sql, quote_ident, where_clause_from};

/// The CTE a representation select reads the returned rows from.
pub const SOURCE: &str = "_zou_mut";

/// The alias the unpacked payload travels under inside a statement.
const SRC: &str = "_zou_src";

fn err<T>(message: impl Into<String>) -> Result<T, CompileError> {
    Err(CompileError {
        message: message.into(),
    })
}

/// What a mutation hands back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Returning {
    /// Nothing, the return=minimal shape.
    None,
    /// Every column, what the representation CTE needs.
    Star,
    /// Named columns, headers-only wants the primary key for the
    /// Location header. An empty list returns nothing.
    Cols(Vec<String>),
}

/// How an insert treats a conflicting row, PostgREST's resolution
/// preferences. The target is the conflict column list, the primary
/// key by default or the on_conflict parameter's columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Conflict {
    /// resolution=ignore-duplicates. An empty target is the bare
    /// `on conflict do nothing`, any unique constraint absorbs.
    Ignore { target: Vec<String> },
    /// resolution=merge-duplicates: the body's columns overwrite
    /// from excluded. An empty set degrades to do nothing, there is
    /// nothing left to merge.
    Merge {
        target: Vec<String>,
        set: Vec<String>,
    },
}

/// Insert the rows of one json array parameter, always an array even
/// for a single object. The columns are the union of the body's
/// keys, the router's job to collect; an empty list still inserts,
/// one all defaults row per element, carried by the empty select
/// list.
pub fn insert(
    table: &str,
    columns: &[String],
    payload: String,
    conflict: Option<&Conflict>,
    returning: &Returning,
) -> Result<Sql, CompileError> {
    let t = quote_ident(table);
    let cols = ident_list(columns);
    let mut text = if columns.is_empty() {
        format!(
            "insert into {t} select from json_populate_recordset(null::{t}, $1) as {}",
            quote_ident(SRC)
        )
    } else {
        format!(
            "insert into {t} ({cols}) select {cols} from json_populate_recordset(null::{t}, $1) as {}",
            quote_ident(SRC)
        )
    };
    match conflict {
        None => {}
        Some(Conflict::Ignore { target }) if target.is_empty() => {
            text.push_str(" on conflict do nothing");
        }
        Some(Conflict::Ignore { target }) => {
            text.push_str(&format!(" on conflict ({}) do nothing", ident_list(target)));
        }
        Some(Conflict::Merge { target, .. }) if target.is_empty() => {
            return err("a merging upsert needs its conflict target columns");
        }
        Some(Conflict::Merge { target, set }) if set.is_empty() => {
            text.push_str(&format!(" on conflict ({}) do nothing", ident_list(target)));
        }
        Some(Conflict::Merge { target, set }) => {
            let sets: Vec<String> = set
                .iter()
                .map(|c| {
                    let c = quote_ident(c);
                    format!("{c} = excluded.{c}")
                })
                .collect();
            text.push_str(&format!(
                " on conflict ({}) do update set {}",
                ident_list(target),
                sets.join(", ")
            ));
        }
    }
    text.push_str(&returning_sql(returning, None));
    Ok(Sql {
        text,
        params: vec![payload],
    })
}

/// Update the filtered rows from one json object parameter. The
/// payload unpacks against the row type like an insert does and each
/// body column assigns from it, so absent columns keep their values.
/// RETURNING qualifies with the table because the payload relation
/// is in scope and would otherwise make every shared column
/// ambiguous. No filters updates the whole table, PostgREST's
/// stance.
pub fn update(
    table: &str,
    columns: &[String],
    payload: String,
    filters: &[Node],
    returning: &Returning,
) -> Result<Sql, CompileError> {
    if columns.is_empty() {
        return err("an update needs at least one column to set");
    }
    let t = quote_ident(table);
    let s = quote_ident(SRC);
    let sets: Vec<String> = columns
        .iter()
        .map(|c| {
            let c = quote_ident(c);
            format!("{c} = {s}.{c}")
        })
        .collect();
    let mut text = format!(
        "update {t} set {} from (select * from json_populate_record(null::{t}, $1)) as {s}",
        sets.join(", ")
    );
    let mut params = vec![payload];
    if !filters.is_empty() {
        let compiled = where_clause_from(filters, Some(table), params)?;
        params = compiled.params;
        text.push_str(" where ");
        text.push_str(&compiled.text);
    }
    text.push_str(&returning_sql(returning, Some(table)));
    Ok(Sql { text, params })
}

/// Delete the filtered rows. No filters deletes the whole table,
/// PostgREST's stance, the router decides whether to allow that.
pub fn delete(table: &str, filters: &[Node], returning: &Returning) -> Result<Sql, CompileError> {
    let mut text = format!("delete from {}", quote_ident(table));
    let mut params = Vec::new();
    if !filters.is_empty() {
        let compiled = where_clause_from(filters, Some(table), params)?;
        params = compiled.params;
        text.push_str(" where ");
        text.push_str(&compiled.text);
    }
    text.push_str(&returning_sql(returning, None));
    Ok(Sql { text, params })
}

/// The two pieces of a representation response: the mutation mounted
/// as a CTE and the select tree reading from it. They stay separate
/// because postgres only accepts a data modifying CTE at the top
/// level of a statement, so the caller that wraps rows into a body
/// assembles `with {cte} {wrap of select}` itself.
#[derive(Debug)]
pub struct Represented {
    /// `"_zou_mut" as (insert ...)`, ready to sit after `with`.
    pub cte: String,
    /// The planned select over the returned rows, params carrying
    /// the payload and the filter literals in one dense list.
    pub select: Sql,
}

/// Plan the request's select tree over a mutation's returned rows.
/// Build the mutation with Returning::Star, the CTE has to carry
/// every column an embed join might need.
pub fn representation(
    catalog: &Catalog,
    mutation: Sql,
    q: &mut Query,
) -> Result<Represented, PlanError> {
    let Sql { text, params } = mutation;
    q.source = Some(SOURCE.to_string());
    let select = plan_from(catalog, q, params)?;
    Ok(Represented {
        cte: format!("{} as ({text})", quote_ident(SOURCE)),
        select,
    })
}

fn returning_sql(r: &Returning, qualifier: Option<&str>) -> String {
    match r {
        Returning::None => String::new(),
        Returning::Star => match qualifier {
            Some(q) => format!(" returning {}.*", quote_ident(q)),
            None => " returning *".into(),
        },
        Returning::Cols(cols) if cols.is_empty() => String::new(),
        Returning::Cols(cols) => {
            let list: Vec<String> = cols
                .iter()
                .map(|c| match qualifier {
                    Some(q) => format!("{}.{}", quote_ident(q), quote_ident(c)),
                    None => quote_ident(c),
                })
                .collect();
            format!(" returning {}", list.join(", "))
        }
    }
}

fn ident_list(cols: &[String]) -> String {
    cols.iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::FkRow;
    use crate::filter::{Parsed, parse_pair};
    use crate::select;

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn node(key: &str, value: &str) -> Node {
        match parse_pair(key, value).unwrap_or_else(|e| panic!("{key}={value}: {e}")) {
            Parsed::Filter(c) => Node::Cond(c),
            Parsed::Logic {
                op, negated, kids, ..
            } => Node::Group { op, negated, kids },
        }
    }

    #[test]
    fn a_plain_insert() {
        let s = insert(
            "books",
            &cols(&["id", "title"]),
            "[]".into(),
            None,
            &Returning::None,
        )
        .unwrap();
        assert_eq!(
            s.text,
            r#"insert into "books" ("id", "title") select "id", "title" from json_populate_recordset(null::"books", $1) as "_zou_src""#
        );
        assert_eq!(s.params, vec!["[]"]);
    }

    #[test]
    fn an_empty_body_inserts_default_rows() {
        let s = insert("books", &[], "[{}]".into(), None, &Returning::Star).unwrap();
        assert_eq!(
            s.text,
            r#"insert into "books" select from json_populate_recordset(null::"books", $1) as "_zou_src" returning *"#
        );
    }

    #[test]
    fn conflict_resolutions() {
        let ignore_all = Conflict::Ignore { target: vec![] };
        let s = insert(
            "t",
            &cols(&["a"]),
            "[]".into(),
            Some(&ignore_all),
            &Returning::None,
        )
        .unwrap();
        assert!(s.text.ends_with(" on conflict do nothing"), "{}", s.text);

        let ignore = Conflict::Ignore {
            target: cols(&["id"]),
        };
        let s = insert(
            "t",
            &cols(&["a"]),
            "[]".into(),
            Some(&ignore),
            &Returning::None,
        )
        .unwrap();
        assert!(
            s.text.ends_with(r#" on conflict ("id") do nothing"#),
            "{}",
            s.text
        );

        let merge = Conflict::Merge {
            target: cols(&["id"]),
            set: cols(&["a", "b"]),
        };
        let s = insert(
            "t",
            &cols(&["a", "b"]),
            "[]".into(),
            Some(&merge),
            &Returning::None,
        )
        .unwrap();
        assert!(
            s.text.ends_with(
                r#" on conflict ("id") do update set "a" = excluded."a", "b" = excluded."b""#
            ),
            "{}",
            s.text
        );

        // Nothing beyond the key to merge degrades to do nothing.
        let hollow = Conflict::Merge {
            target: cols(&["id"]),
            set: vec![],
        };
        let s = insert(
            "t",
            &cols(&["id"]),
            "[]".into(),
            Some(&hollow),
            &Returning::None,
        )
        .unwrap();
        assert!(
            s.text.ends_with(r#" on conflict ("id") do nothing"#),
            "{}",
            s.text
        );

        // A merge with no target has no valid SQL spelling.
        let bad = Conflict::Merge {
            target: vec![],
            set: cols(&["a"]),
        };
        let e = insert(
            "t",
            &cols(&["a"]),
            "[]".into(),
            Some(&bad),
            &Returning::None,
        )
        .unwrap_err();
        assert!(e.message.contains("conflict target"), "{e}");
    }

    #[test]
    fn an_update_binds_the_payload_first() {
        let s = update(
            "books",
            &cols(&["title", "price"]),
            r#"{"title":"x"}"#.into(),
            &[node("id", "eq.7")],
            &Returning::Star,
        )
        .unwrap();
        assert_eq!(
            s.text,
            r#"update "books" set "title" = "_zou_src"."title", "price" = "_zou_src"."price" from (select * from json_populate_record(null::"books", $1)) as "_zou_src" where "books"."id" = $2 returning "books".*"#
        );
        assert_eq!(s.params, vec![r#"{"title":"x"}"#.to_string(), "7".into()]);
    }

    #[test]
    fn an_unfiltered_update_touches_the_whole_table() {
        let s = update(
            "t",
            &cols(&["a"]),
            "{}".into(),
            &[],
            &Returning::Cols(cols(&["id"])),
        )
        .unwrap();
        assert!(!s.text.contains(" where "), "{}", s.text);
        assert!(s.text.ends_with(r#" returning "t"."id""#), "{}", s.text);

        let e = update("t", &[], "{}".into(), &[], &Returning::None).unwrap_err();
        assert!(e.message.contains("at least one column"), "{e}");
    }

    #[test]
    fn a_delete_speaks_plainly() {
        let s = delete(
            "books",
            &[node("id", "eq.4")],
            &Returning::Cols(cols(&["id"])),
        )
        .unwrap();
        assert_eq!(
            s.text,
            r#"delete from "books" where "books"."id" = $1 returning "id""#
        );
        assert_eq!(s.params, vec!["4"]);

        let s = delete("books", &[], &Returning::None).unwrap();
        assert_eq!(s.text, r#"delete from "books""#);
        assert!(s.params.is_empty());
    }

    #[test]
    fn identifiers_from_body_keys_stay_quoted() {
        let sneaky = cols(&[r#"a"; drop table books; --"#]);
        let s = insert(
            "books",
            &sneaky,
            "[]".into(),
            None,
            &Returning::Cols(sneaky.clone()),
        )
        .unwrap();
        assert!(
            s.text.contains(r#""a""; drop table books; --""#),
            "the quote doubled and the key stayed one identifier: {}",
            s.text
        );
    }

    #[test]
    fn the_representation_reads_the_cte_and_numbering_continues() {
        let catalog = Catalog::new(vec![FkRow {
            constraint: "books_author_id_fkey".into(),
            table: "books".into(),
            columns: vec!["author_id".into()],
            ref_table: "authors".into(),
            ref_columns: vec!["id".into()],
            unique: false,
            in_pk: false,
        }]);
        let m = insert(
            "books",
            &cols(&["id", "title"]),
            "[]".into(),
            None,
            &Returning::Star,
        )
        .unwrap();
        let mut q = Query {
            table: "books".into(),
            select: select::parse("id,authors(name)").unwrap(),
            ..Query::default()
        };
        q.filters.push((Vec::new(), node("id", "gte.1")));
        let r = representation(&catalog, m, &mut q).unwrap();
        assert!(
            r.cte.starts_with(r#""_zou_mut" as (insert into "books""#),
            "{}",
            r.cte
        );
        assert!(r.cte.ends_with("returning *)"), "{}", r.cte);
        assert!(
            r.select.text.contains(r#"from "_zou_mut" as "z0""#),
            "the root reads the returned rows: {}",
            r.select.text
        );
        assert!(
            r.select.text.contains(r#"from "authors" as "z1""#),
            "the embed still reads its real table: {}",
            r.select.text
        );
        assert!(
            r.select.text.contains(r#""z0"."id" >= $2"#),
            "the filter binds after the payload: {}",
            r.select.text
        );
        assert_eq!(r.select.params, vec!["[]".to_string(), "1".into()]);
    }
}
