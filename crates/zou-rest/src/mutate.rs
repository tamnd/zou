//! Mutation statements: insert, upsert, update, and delete.
//!
//! The body a client sends never splices into SQL. It binds whole as
//! one json parameter and json_to_recordset unpacks it under a column
//! definition list built from the catalog, so postgres types every
//! value the same way it types the read path's filter literals, and
//! the body reaches it as the bytes that arrived. The only identifiers
//! spliced here are the column names lifted from the body's keys and
//! the conflict targets, and every one goes through quote_ident. The
//! defaults `Prefer: missing=default` merges into the body are the
//! one other thing spliced, and they come off the catalog rather
//! than off the request.
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
//!
//! PUT is the odd one. Its filters are the primary key and they
//! compile against the payload rather than against the table, so a
//! body that disagrees with the url matches nothing and the statement
//! writes nothing, which is the whole of the check.

use crate::catalog::{Catalog, Column, Relation};
use crate::filter::Node;
use crate::plan::{PlanError, Query, plan_from};
use crate::select::Item;
use crate::sql::{CompileError, Sql, quote_ident, where_clause_from};

/// The CTE a representation select reads the returned rows from.
pub const SOURCE: &str = "_zou_mut";

/// The alias the unpacked payload travels under inside a statement.
const SRC: &str = "_zou_src";

/// The alias the column definition list hangs off, one step further
/// in than [`SRC`], where the casts are still uncalled.
const BODY: &str = "_zou_body";

/// One element of a json array body, while the defaults are being
/// merged under it.
const ELEM: &str = "_zou_elem";

/// What a write puts in a column the body said nothing about, the
/// Prefer missing token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Missing {
    /// Null, which is what unpacking a body without the key gives
    /// on its own, so nothing has to be done to get it.
    #[default]
    Null,
    /// Whatever the column defaults to. Nothing about an absent key
    /// says default, so the default has to be put into the body
    /// before the body is unpacked.
    Default,
}

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
    /// Every column, which a representation asks for only when it
    /// cannot say which ones it needs.
    Star,
    /// Named columns, headers-only wants the primary key for the
    /// Location header. An empty list returns nothing.
    Cols(Vec<String>),
}

/// How an insert treats a conflicting row, PostgREST's resolution
/// preferences. The target is the conflict column list, the primary
/// key by default or the on_conflict parameter's columns. An empty
/// target is nothing to conflict on, whichever resolution asked, and
/// what comes out is a plain insert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Conflict {
    /// resolution=ignore-duplicates.
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
    rel: Option<&Relation>,
    columns: &[String],
    payload: String,
    missing: Missing,
    conflict: Option<&Conflict>,
    returning: &Returning,
) -> Result<Sql, CompileError> {
    let t = quote_ident(table);
    let cols = ident_list(columns);
    let src = rows_of(table, rel, columns, missing);
    let mut text = if columns.is_empty() {
        format!("insert into {t} select from {src}")
    } else {
        format!("insert into {t} ({cols}) select {cols} from {src}")
    };
    // A merging upsert counts, because its status turns on whether
    // anything was inserted. Ignoring one does not: a row it swallowed
    // is a row upstream still calls a creation.
    let merging = matches!(conflict, Some(Conflict::Merge { .. }));
    if merging {
        text.push_str(&format!(" where {COUNT_INSERT}"));
    }
    match conflict {
        // No conflict target, no conflict clause. A table with no
        // primary key and no on_conflict has nothing to conflict on,
        // and upstream writes the plain insert rather than refusing
        // the request.
        None => {}
        Some(Conflict::Ignore { target } | Conflict::Merge { target, .. }) if target.is_empty() => {
        }
        Some(Conflict::Ignore { target }) => {
            text.push_str(&format!(" on conflict ({}) do nothing", ident_list(target)));
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
                " on conflict ({}) do update set {} where {COUNT_UPDATE}",
                ident_list(target),
                sets.join(", ")
            ));
        }
    }
    text.push_str(&returning_sql(returning, table));
    Ok(Sql {
        text,
        params: vec![payload],
    })
}

/// The setting an upsert counts itself in, and the read that asks for
/// the count afterwards. It is transaction local, so it is gone by
/// the time the connection goes back to the pool.
///
/// One counter, counting up on the way in and down again for a row
/// the conflict clause turned into an update, so what is left is the
/// number of rows that were actually new. Two counters would say the
/// same thing and cost a column to read it in.
pub const INSERTED_SQL: &str =
    "select coalesce(nullif(current_setting('zou.inserted', true), '')::int, 0)";

/// The condition on the insert's own select, counting every row that
/// goes in, and the one on the conflict clause, taking back the ones
/// that turned out to be updates. Both let every row through:
/// set_config returns the new value, and a number is never the empty
/// string.
const COUNT_INSERT: &str = "set_config('zou.inserted', (coalesce(nullif(current_setting('zou.inserted', true), '')::int, 0) + 1)::text, true) <> ''";
const COUNT_UPDATE: &str = "set_config('zou.inserted', (coalesce(nullif(current_setting('zou.inserted', true), '')::int, 0) - 1)::text, true) <> ''";

/// PUT's upsert, the one row PostgREST calls a single upsert. The
/// conflict target is the primary key, always, and the url's filters
/// are compiled against the unpacked payload rather than against the
/// table: a body whose key is not the url's key survives no filter,
/// no row goes in, and the router reads the count of nothing as the
/// mismatch. It counts in `zou.inserted` like a merging upsert does,
/// and a net of one row inserted is the 201 against the 200.
///
/// It takes no missing preference. A PUT's columns are the body's
/// own keys, so there is never a column it is writing that the body
/// did not name, and upstream neither fills a default here nor
/// echoes the token back.
pub fn upsert_one(
    table: &str,
    rel: Option<&Relation>,
    columns: &[String],
    payload: String,
    pk: &[String],
    filters: &[Node],
    returning: &Returning,
) -> Result<Sql, CompileError> {
    if pk.is_empty() {
        return err("a put needs a primary key to conflict on");
    }
    let t = quote_ident(table);
    let cols = ident_list(columns);
    let src = rows_of(table, rel, columns, Missing::Null);
    let mut text = if columns.is_empty() {
        format!("insert into {t} select from {src}")
    } else {
        format!("insert into {t} ({cols}) select {cols} from {src}")
    };
    let mut params = vec![payload];
    text.push_str(&format!(" where {COUNT_INSERT}"));
    if !filters.is_empty() {
        let compiled = where_clause_from(filters, Some(SRC), params, rel)?;
        params = compiled.params;
        text.push_str(" and ");
        text.push_str(&compiled.text);
    }
    let target = ident_list(pk);
    if columns.is_empty() {
        // Nothing to merge, so the conflict absorbs and the count of
        // updates stays at nothing, same as an insert that found no
        // conflict at all.
        text.push_str(&format!(" on conflict ({target}) do nothing"));
    } else {
        let sets: Vec<String> = columns
            .iter()
            .map(|c| {
                let c = quote_ident(c);
                format!("{c} = excluded.{c}")
            })
            .collect();
        text.push_str(&format!(
            " on conflict ({target}) do update set {} where {COUNT_UPDATE}",
            sets.join(", ")
        ));
    }
    text.push_str(&returning_sql(returning, table));
    Ok(Sql { text, params })
}

/// Update the filtered rows from one json object parameter. The
/// payload unpacks against the row type like an insert does and each
/// body column assigns from it, so absent columns keep their values.
/// RETURNING qualifies with the table because the payload relation
/// is in scope and would otherwise make every shared column
/// ambiguous. No filters updates the whole table, PostgREST's
/// stance.
///
/// A body with no columns in it is not an error and not a no-op
/// either. `update t set` is not a statement postgres will take, so
/// upstream answers with a select of the same shape that returns
/// nothing, and the filters go with the columns: the row count is
/// zero whatever the url said. It has to be a select of the table
/// rather than nothing at all, because a representation reads the
/// column types off it.
pub fn update(
    table: &str,
    rel: Option<&Relation>,
    columns: &[String],
    payload: String,
    missing: Missing,
    filters: &[Node],
    returning: &Returning,
) -> Result<Sql, CompileError> {
    if columns.is_empty() {
        return Ok(Sql {
            text: format!("select * from {} where false", quote_ident(table)),
            params: Vec::new(),
        });
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
        "update {t} set {} from {}",
        sets.join(", "),
        row_of(table, rel, columns, missing)
    );
    let mut params = vec![payload];
    if !filters.is_empty() {
        let compiled = where_clause_from(filters, Some(table), params, rel)?;
        params = compiled.params;
        text.push_str(" where ");
        text.push_str(&compiled.text);
    }
    text.push_str(&returning_sql(returning, table));
    Ok(Sql { text, params })
}

/// Delete the filtered rows. No filters deletes the whole table,
/// PostgREST's stance, the router decides whether to allow that.
pub fn delete(
    table: &str,
    rel: Option<&Relation>,
    filters: &[Node],
    returning: &Returning,
) -> Result<Sql, CompileError> {
    let mut text = format!("delete from {}", quote_ident(table));
    let mut params = Vec::new();
    if !filters.is_empty() {
        let compiled = where_clause_from(filters, Some(table), params, rel)?;
        params = compiled.params;
        text.push_str(" where ");
        text.push_str(&compiled.text);
    }
    text.push_str(&returning_sql(returning, table));
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

/// Which columns a representation has to hand back: the ones the
/// select list names, the parent side of every relationship it
/// embeds, and the primary key.
///
/// A returning list is not only about what the response carries.
/// `returning *` reads every column of the table, so a role with
/// select on two columns of three cannot write a row it is otherwise
/// allowed to write, which is the whole of what these cases are
/// about. Naming the columns is what makes the privileges line up
/// with what was asked for.
///
/// The embed columns are what the join needs rather than what the
/// client asked for: a select of `name,clients(name)` has to bring
/// `client_id` back or the join over the CTE has nothing to match on.
/// The primary key rides along because that is what a Location header
/// is built out of. A computed relationship takes the whole row as
/// its argument, so there is no column list to name and the star
/// stays.
pub fn needed(catalog: &Catalog, table: &str, items: &[Item], pk: &[String]) -> Returning {
    let mut cols: Vec<String> = Vec::new();
    for item in items {
        match item {
            Item::Star => return Returning::Star,
            Item::Col(c) => {
                if let Some(f) = &c.field {
                    push(&mut cols, &f.name);
                }
            }
            Item::Embed(e) | Item::Spread(e) => {
                match catalog.resolve(table, &e.relation, e.hint.as_deref()) {
                    Ok(rel) if rel.call.is_none() => {
                        for (parent, _) in &rel.join {
                            push(&mut cols, parent);
                        }
                    }
                    // A computed relationship, or a relationship that
                    // does not resolve at all: the planner is about to
                    // report the second one, and neither has columns
                    // to name here.
                    _ => return Returning::Star,
                }
            }
        }
    }
    for c in pk {
        push(&mut cols, c);
    }
    // A select of nothing but aggregates over a table with no primary
    // key. Upstream returns the constant 1, which is a row and not a
    // column; the star is the nearest thing zou can return, and the
    // response names nothing out of it either way.
    if cols.is_empty() {
        return Returning::Star;
    }
    Returning::Cols(cols)
}

fn push(cols: &mut Vec<String>, name: &str) {
    if !cols.iter().any(|c| c == name) {
        cols.push(name.to_string());
    }
}

/// Plan the request's select tree over a mutation's returned rows.
/// The mutation returns what [`needed`] worked out, which is every
/// column a select item or an embed join can reach for.
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

/// The returning clause, always qualified with the table. The update
/// shape needs the qualifier because it has the payload relation in
/// scope too, and every shape needs it for a computed column, which
/// is a call on the row written as if it were a column and which
/// postgres resolves only on something that has a type.
fn returning_sql(r: &Returning, table: &str) -> String {
    let t = quote_ident(table);
    match r {
        Returning::None => String::new(),
        Returning::Star => format!(" returning {t}.*"),
        Returning::Cols(cols) if cols.is_empty() => String::new(),
        Returning::Cols(cols) => {
            let list: Vec<String> = cols
                .iter()
                .map(|c| format!("{t}.{}", quote_ident(c)))
                .collect();
            format!(" returning {}", list.join(", "))
        }
    }
}

/// The rows of a json array body, ready to be selected from.
///
/// The shape is `json_to_recordset` under a column definition list,
/// which is what upstream writes and is the only shape a data
/// representation can use: the values of such a column arrive as
/// json and the type reads json through a function postgres records
/// and refuses to apply, so the column is declared json and the
/// function is called by name. A write naming no columns at all, or
/// naming one the catalog has no type for, has no list to write and
/// falls back to `json_populate_recordset` against the table's own
/// row type.
///
/// A write asking for the defaults merges them under every element
/// of the body first, so a key the element carries wins and a key it
/// lacks arrives as the default. That merge is a jsonb operator, so
/// the whole body crosses as jsonb from there on.
fn rows_of(table: &str, rel: Option<&Relation>, columns: &[String], missing: Missing) -> String {
    let (json, body) = match defaults_of(rel, columns, missing) {
        Some(obj) => {
            let e = quote_ident(ELEM);
            (
                "jsonb",
                format!("(select jsonb_agg({obj} || {e}) from jsonb_array_elements($1) as {e})"),
            )
        }
        None => ("json", "$1".to_string()),
    };
    match body_cast(rel, columns) {
        Some((list, defs)) => format!(
            "(select {list} from {json}_to_recordset({body}) as {}({defs})) as {}",
            quote_ident(BODY),
            quote_ident(SRC)
        ),
        None => format!(
            "{json}_populate_recordset(null::{}, {body}) as {}",
            quote_ident(table),
            quote_ident(SRC)
        ),
    }
}

/// The one row of a json object body, [`rows_of`] for an update.
fn row_of(table: &str, rel: Option<&Relation>, columns: &[String], missing: Missing) -> String {
    let (json, body) = match defaults_of(rel, columns, missing) {
        Some(obj) => ("jsonb", format!("({obj} || $1)")),
        None => ("json", "$1".to_string()),
    };
    match body_cast(rel, columns) {
        Some((list, defs)) => format!(
            "(select {list} from {json}_to_record({body}) as {}({defs})) as {}",
            quote_ident(BODY),
            quote_ident(SRC)
        ),
        None => format!(
            "(select * from {json}_populate_record(null::{}, {body})) as {}",
            quote_ident(table),
            quote_ident(SRC)
        ),
    }
}

/// The object of column defaults the body is merged under, or
/// nothing when the write did not ask for them or no column it is
/// writing has one.
///
/// Every column of the write that has a default goes in, whether or
/// not the body names it, because the body is merged over the top
/// and wins wherever it says anything. That is upstream's shape and
/// it saves knowing which keys each element of a bulk body carries.
///
/// The default is postgres's own spelling of the expression, read
/// off the catalog and spliced as it stands. It has to be: a
/// `nextval` call or a `now()` is the default, not the value it
/// would have produced when the cache was filled.
fn defaults_of(rel: Option<&Relation>, columns: &[String], missing: Missing) -> Option<String> {
    if missing == Missing::Null {
        return None;
    }
    let rel = rel?;
    let pairs: Vec<String> = columns
        .iter()
        .filter_map(|c| rel.column(c))
        .filter_map(|c| {
            let d = c.default_expr.as_ref()?;
            Some(format!("{}, {d}", quote_text(&c.name)))
        })
        .collect();
    if pairs.is_empty() {
        return None;
    }
    Some(format!("jsonb_build_object({})", pairs.join(", ")))
}

/// A column name as a string literal, which is what a json key is.
fn quote_text(name: &str) -> String {
    format!("'{}'", name.replace('\'', "''"))
}

/// The select list and the column definition list the body unpacks
/// through, or nothing at all when there is no list to write.
///
/// A column reading json through a cast is declared `json` and the
/// cast is called here, by name, the way upstream calls it, since
/// postgres records such a cast and refuses to apply it. Every other
/// column is declared as the type the catalog has for it, which is
/// `format_type` and so is spelled the way a column definition list
/// wants it.
fn body_cast(rel: Option<&Relation>, columns: &[String]) -> Option<(String, String)> {
    let rel = rel?;
    let cols: Vec<&Column> = columns.iter().filter_map(|c| rel.column(c)).collect();
    // A name the relation does not have leaves nothing to declare a
    // type for, so the plain shape takes it and postgres says what
    // it thinks. The router has usually refused it long before here.
    // A write of no columns has no list either, and takes the shape
    // that needs none.
    if cols.is_empty() || cols.len() != columns.len() {
        return None;
    }
    let mut list = Vec::with_capacity(cols.len());
    let mut defs = Vec::with_capacity(cols.len());
    for c in &cols {
        let name = quote_ident(&c.name);
        match &c.from_json {
            Some(func) => {
                list.push(format!("{func}({name}) as {name}"));
                defs.push(format!("{name} json"));
            }
            None => {
                list.push(name.to_string());
                defs.push(format!("{name} {}", c.type_name));
            }
        }
    }
    Some((list.join(", "), defs.join(", ")))
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
            None,
            &cols(&["id", "title"]),
            "[]".into(),
            Missing::Null,
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
        let s = insert(
            "books",
            None,
            &[],
            "[{}]".into(),
            Missing::Null,
            None,
            &Returning::Star,
        )
        .unwrap();
        assert_eq!(
            s.text,
            r#"insert into "books" select from json_populate_recordset(null::"books", $1) as "_zou_src" returning "books".*"#
        );
    }

    #[test]
    fn conflict_resolutions() {
        let ignore_all = Conflict::Ignore { target: vec![] };
        let s = insert(
            "t",
            None,
            &cols(&["a"]),
            "[]".into(),
            Missing::Null,
            Some(&ignore_all),
            &Returning::None,
        )
        .unwrap();
        // Nothing to conflict on, so there is no conflict clause and
        // no counting either.
        assert!(!s.text.contains("on conflict"), "{}", s.text);
        assert!(!s.text.contains("set_config"), "{}", s.text);

        let ignore = Conflict::Ignore {
            target: cols(&["id"]),
        };
        let s = insert(
            "t",
            None,
            &cols(&["a"]),
            "[]".into(),
            Missing::Null,
            Some(&ignore),
            &Returning::None,
        )
        .unwrap();
        assert!(
            s.text.ends_with(r#" on conflict ("id") do nothing"#),
            "{}",
            s.text
        );
        // An ignored duplicate is still a creation, so nothing counts.
        assert!(!s.text.contains("set_config"), "{}", s.text);

        let merge = Conflict::Merge {
            target: cols(&["id"]),
            set: cols(&["a", "b"]),
        };
        let s = insert(
            "t",
            None,
            &cols(&["a", "b"]),
            "[]".into(),
            Missing::Null,
            Some(&merge),
            &Returning::None,
        )
        .unwrap();
        // One counter up on the way in and down again for whatever
        // the conflict clause caught.
        assert!(
            s.text
                .contains(r#"as "_zou_src" where set_config('zou.inserted'"#),
            "{}",
            s.text
        );
        assert!(
            s.text.contains(
                r#" on conflict ("id") do update set "a" = excluded."a", "b" = excluded."b" where set_config('zou.inserted'"#
            ),
            "{}",
            s.text
        );
        assert!(
            s.text.contains(" + 1)::text") && s.text.contains(" - 1)::text"),
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
            None,
            &cols(&["id"]),
            "[]".into(),
            Missing::Null,
            Some(&hollow),
            &Returning::None,
        )
        .unwrap();
        assert!(
            s.text.ends_with(r#" on conflict ("id") do nothing"#),
            "{}",
            s.text
        );

        // A merge with no target is a plain insert. There is nothing
        // to conflict on, so every row it writes is a new one, and it
        // still counts them to be able to say so.
        let no_target = Conflict::Merge {
            target: vec![],
            set: cols(&["a"]),
        };
        let s = insert(
            "t",
            None,
            &cols(&["a"]),
            "[]".into(),
            Missing::Null,
            Some(&no_target),
            &Returning::None,
        )
        .unwrap();
        assert!(!s.text.contains("on conflict"), "{}", s.text);
        assert!(s.text.contains("set_config('zou.inserted'"), "{}", s.text);
    }

    #[test]
    fn a_put_filters_the_payload_not_the_table() {
        let s = upsert_one(
            "tiobe_pls",
            None,
            &cols(&["name", "rank"]),
            r#"[{"name":"Go","rank":19}]"#.into(),
            &cols(&["name"]),
            &[node("name", "eq.Go")],
            &Returning::Star,
        )
        .unwrap();
        // The counter goes first and the url's own filter after it,
        // upstream's order.
        assert!(
            s.text
                .contains(r#"::text, true) <> '' and "_zou_src"."name" = $2"#),
            "{}",
            s.text
        );
        assert!(
            s.text.contains(
                r#"on conflict ("name") do update set "name" = excluded."name", "rank" = excluded."rank" where set_config("#
            ),
            "{}",
            s.text
        );
        assert!(s.text.ends_with(" returning \"tiobe_pls\".*"), "{}", s.text);
        assert_eq!(s.params, vec![r#"[{"name":"Go","rank":19}]"#, "Go"]);
    }

    #[test]
    fn a_put_with_nothing_to_merge_lets_the_conflict_absorb() {
        let s = upsert_one(
            "only_pk",
            None,
            &[],
            "[{}]".into(),
            &cols(&["id"]),
            &[node("id", "eq.1")],
            &Returning::None,
        )
        .unwrap();
        assert!(
            s.text.ends_with(r#" on conflict ("id") do nothing"#),
            "{}",
            s.text
        );
        // The row still counts on the way in. Nothing takes it back,
        // but nothing was updated either, so a row that did go in is
        // the creation it looks like.
        assert!(!s.text.contains(" - 1)::text"), "{}", s.text);

        let e = upsert_one(
            "no_pk",
            None,
            &cols(&["a"]),
            "[]".into(),
            &[],
            &[],
            &Returning::None,
        )
        .unwrap_err();
        assert!(e.message.contains("primary key"), "{e}");
    }

    #[test]
    fn an_update_binds_the_payload_first() {
        let s = update(
            "books",
            None,
            &cols(&["title", "price"]),
            r#"{"title":"x"}"#.into(),
            Missing::Null,
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
            None,
            &cols(&["a"]),
            "{}".into(),
            Missing::Null,
            &[],
            &Returning::Cols(cols(&["id"])),
        )
        .unwrap();
        assert!(!s.text.contains(" where "), "{}", s.text);
        assert!(s.text.ends_with(r#" returning "t"."id""#), "{}", s.text);

        // Nothing to set is a statement of the same shape that
        // returns nothing, and the url's filters go with it.
        let s = update(
            "t",
            None,
            &[],
            "{}".into(),
            Missing::Null,
            &[node("id", "eq.4")],
            &Returning::None,
        )
        .unwrap();
        assert_eq!(s.text, r#"select * from "t" where false"#);
        assert!(s.params.is_empty());
    }

    /// A relation whose `label_color` reads json through a cast and
    /// whose other two columns are ordinary.
    fn painted() -> Relation {
        Relation {
            name: "todos".into(),
            keys: vec!["id".into()],
            columns: vec![
                Column {
                    name: "id".into(),
                    type_name: "bigint".into(),
                    base_type: "bigint".into(),
                    ..Column::default()
                },
                Column {
                    name: "label_color".into(),
                    type_name: "test.color".into(),
                    base_type: "test.color".into(),
                    to_json: Some("test.json".into()),
                    from_text: Some("test.color".into()),
                    from_json: Some("test.color".into()),
                    default_expr: None,
                },
                Column {
                    name: "name".into(),
                    type_name: "text".into(),
                    base_type: "text".into(),
                    ..Column::default()
                },
            ],
        }
    }

    #[test]
    fn a_written_value_is_read_through_the_cast_its_type_has() {
        let rel = painted();
        let s = insert(
            "todos",
            Some(&rel),
            &cols(&["id", "label_color"]),
            r#"[{"id":1,"label_color":"000100"}]"#.into(),
            Missing::Null,
            None,
            &Returning::None,
        )
        .unwrap();
        assert_eq!(
            s.text,
            r#"insert into "todos" ("id", "label_color") select "id", "label_color" from (select "id", test.color("label_color") as "label_color" from json_to_recordset($1) as "_zou_body"("id" bigint, "label_color" json)) as "_zou_src""#
        );

        // An update takes the object shape of the same thing, and
        // only the columns it is writing are declared.
        let s = update(
            "todos",
            Some(&rel),
            &cols(&["label_color"]),
            r#"{"label_color":"000100"}"#.into(),
            Missing::Null,
            &[node("id", "eq.7")],
            &Returning::None,
        )
        .unwrap();
        assert_eq!(
            s.text,
            r#"update "todos" set "label_color" = "_zou_src"."label_color" from (select test.color("label_color") as "label_color" from json_to_record($1) as "_zou_body"("label_color" json)) as "_zou_src" where "todos"."id" = $2"#
        );
        assert_eq!(s.params, vec![r#"{"label_color":"000100"}"#, "7"]);
    }

    #[test]
    fn a_body_with_nothing_to_cast_still_declares_its_columns() {
        // Every column the catalog has a type for is declared, cast
        // or no cast, because the definition list is also what tells
        // postgres the body has to be objects.
        let rel = painted();
        let s = insert(
            "todos",
            Some(&rel),
            &cols(&["id", "name"]),
            "[]".into(),
            Missing::Null,
            None,
            &Returning::None,
        )
        .unwrap();
        assert!(
            s.text
                .contains(r#"json_to_recordset($1) as "_zou_body"("id" bigint, "name" text)"#),
            "{}",
            s.text
        );
        // A write naming a column the relation does not have has no
        // type to declare and is somebody else's refusal, so it goes
        // the plain way and postgres says what it thinks.
        let s = insert(
            "todos",
            Some(&rel),
            &cols(&["label_color", "nope"]),
            "[]".into(),
            Missing::Null,
            None,
            &Returning::None,
        )
        .unwrap();
        assert!(
            s.text
                .contains(r#"json_populate_recordset(null::"todos", $1)"#),
            "{}",
            s.text
        );
        // And so does a write naming no columns at all, which is a
        // row of defaults per element and has no list to write.
        let s = insert(
            "todos",
            Some(&rel),
            &[],
            "[{}]".into(),
            Missing::Null,
            None,
            &Returning::None,
        )
        .unwrap();
        assert_eq!(
            s.text,
            r#"insert into "todos" select from json_populate_recordset(null::"todos", $1) as "_zou_src""#
        );
    }

    /// The same three columns as [`painted`], two of them with a
    /// default and the middle one without, so the object a write
    /// builds can be watched leaving one out.
    fn defaulted() -> Relation {
        let mut rel = painted();
        rel.column_mut("id").default_expr = Some("nextval('todos_id_seq')".into());
        rel.column_mut("label_color").default_expr = Some("0".into());
        rel
    }

    impl Relation {
        fn column_mut(&mut self, name: &str) -> &mut Column {
            self.columns
                .iter_mut()
                .find(|c| c.name == name)
                .expect("the fixture has that column")
        }
    }

    #[test]
    fn a_column_the_body_left_out_takes_the_default_it_was_given() {
        let rel = defaulted();
        let s = insert(
            "todos",
            Some(&rel),
            &cols(&["id", "name"]),
            r#"[{"name":"a"}]"#.into(),
            Missing::Default,
            None,
            &Returning::None,
        )
        .unwrap();
        // The defaults go under the element rather than over it, so
        // a body that does name the column still wins, and `name`
        // has no default to put anywhere.
        assert_eq!(
            s.text,
            r#"insert into "todos" ("id", "name") select "id", "name" from (select "id", "name" from jsonb_to_recordset((select jsonb_agg(jsonb_build_object('id', nextval('todos_id_seq')) || "_zou_elem") from jsonb_array_elements($1) as "_zou_elem")) as "_zou_body"("id" bigint, "name" text)) as "_zou_src""#
        );

        // An update merges the same object under the one object its
        // body is.
        let s = update(
            "todos",
            Some(&rel),
            &cols(&["id", "name"]),
            r#"{"name":"a"}"#.into(),
            Missing::Default,
            &[],
            &Returning::None,
        )
        .unwrap();
        assert!(
            s.text.contains(
                r#"jsonb_to_record((jsonb_build_object('id', nextval('todos_id_seq')) || $1)) as "_zou_body"("id" bigint, "name" text)"#
            ),
            "{}",
            s.text
        );
    }

    #[test]
    fn a_default_and_a_cast_are_both_on_the_way_in() {
        // The default is a json value like any other by the time the
        // column definition list sees it, so the cast from json
        // fires on a default exactly as it fires on a body's value.
        let rel = defaulted();
        let s = insert(
            "todos",
            Some(&rel),
            &cols(&["label_color", "name"]),
            "[{}]".into(),
            Missing::Default,
            None,
            &Returning::None,
        )
        .unwrap();
        assert_eq!(
            s.text,
            r#"insert into "todos" ("label_color", "name") select "label_color", "name" from (select test.color("label_color") as "label_color", "name" from jsonb_to_recordset((select jsonb_agg(jsonb_build_object('label_color', 0) || "_zou_elem") from jsonb_array_elements($1) as "_zou_elem")) as "_zou_body"("label_color" json, "name" text)) as "_zou_src""#
        );
    }

    #[test]
    fn nothing_to_default_is_the_body_as_it_arrived() {
        // Asking for the defaults where no column being written has
        // one would build an empty object to merge, which is the
        // body again. The plain shape says the same thing and says
        // it in json, so the ask leaves no trace.
        let rel = defaulted();
        let s = insert(
            "todos",
            Some(&rel),
            &cols(&["name"]),
            "[]".into(),
            Missing::Default,
            None,
            &Returning::None,
        )
        .unwrap();
        assert_eq!(
            s.text,
            r#"insert into "todos" ("name") select "name" from (select "name" from json_to_recordset($1) as "_zou_body"("name" text)) as "_zou_src""#
        );
    }

    #[test]
    fn a_delete_speaks_plainly() {
        let s = delete(
            "books",
            None,
            &[node("id", "eq.4")],
            &Returning::Cols(cols(&["id"])),
        )
        .unwrap();
        assert_eq!(
            s.text,
            r#"delete from "books" where "books"."id" = $1 returning "books"."id""#
        );
        assert_eq!(s.params, vec!["4"]);

        let s = delete("books", None, &[], &Returning::None).unwrap();
        assert_eq!(s.text, r#"delete from "books""#);
        assert!(s.params.is_empty());
    }

    #[test]
    fn identifiers_from_body_keys_stay_quoted() {
        let sneaky = cols(&[r#"a"; drop table books; --"#]);
        let s = insert(
            "books",
            None,
            &sneaky,
            "[]".into(),
            Missing::Null,
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
            None,
            &cols(&["id", "title"]),
            "[]".into(),
            Missing::Null,
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
        assert!(r.cte.ends_with("returning \"books\".*)"), "{}", r.cte);
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

    /// A representation returns what it can name and no more, since
    /// returning a column is reading it.
    #[test]
    fn a_representation_returns_what_it_names_and_what_a_join_needs() {
        let catalog = Catalog::new(vec![FkRow {
            constraint: "books_author_id_fkey".into(),
            table: "books".into(),
            columns: vec!["author_id".into()],
            ref_table: "authors".into(),
            ref_columns: vec!["id".into()],
            unique: false,
            in_pk: false,
        }]);
        let pk = cols(&["id"]);
        let sel = |s: &str| select::parse(s).unwrap();

        assert_eq!(
            needed(&catalog, "books", &sel("title"), &pk),
            Returning::Cols(cols(&["title", "id"]))
        );
        // The join column comes back whether or not it was asked for,
        // and comes back once when it was.
        assert_eq!(
            needed(&catalog, "books", &sel("title,authors(name)"), &pk),
            Returning::Cols(cols(&["title", "author_id", "id"]))
        );
        assert_eq!(
            needed(&catalog, "books", &sel("author_id,authors(name)"), &pk),
            Returning::Cols(cols(&["author_id", "id"]))
        );
        // A star anywhere in the list is the whole row anyway.
        assert_eq!(needed(&catalog, "books", &sel("*"), &pk), Returning::Star);
        assert_eq!(
            needed(&catalog, "books", &sel("id,*"), &pk),
            Returning::Star
        );
        // A relationship that does not resolve is the planner's error
        // to report, not this one's.
        assert_eq!(
            needed(&catalog, "books", &sel("id,nowhere(name)"), &pk),
            Returning::Star
        );
    }
}
