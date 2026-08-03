//! The embedding planner: a select tree to one SQL statement.
//!
//! Every embedded resource becomes a lateral subquery against the
//! target table, to many aggregated with jsonb_agg over to_jsonb, to
//! one carried as a single jsonb value, and spreads merged into the
//! parent's column list. The default join is left, so parents keep
//! their rows and an absent embed reads as `[]` or `null`; `!inner`
//! swaps the lateral in as a plain join, which is what makes a filter
//! on the embed remove parent rows.
//!
//! Nested filters, order, limit, and offset route to their embed by
//! path: the query pair `orders.status=eq.x` carries its path in the
//! filter's field, `orders.order=total.desc` and `orders.limit=2`
//! carry it in the parameter name and arrive here as explicit routes.
//! A route that names no embed in the select tree is refused the way
//! PostgREST refuses it, since silently dropping a filter is the one
//! thing a REST layer must never do.
//!
//! Every level reads from exactly one table under a generated alias,
//! z0 for the root and z{n} per embed, so a self referencing embed
//! joins two aliases of the same table instead of colliding, and a
//! many to many walks its junction through an IN subquery rather
//! than a second FROM entry. All literals still bind as parameters
//! through the WHERE compiler, the planner itself splices only
//! identifiers it quotes and numbers it formats.
//!
//! Aggregates group by every non aggregated output expression, the
//! PostgREST rule, and jsonb embed columns take part since jsonb has
//! equality. What the planner refuses, it refuses loudly: mixing an
//! aggregate with `*` or with a spread, spreading a to many, casting
//! to a type that is not a plain identifier.

use std::fmt;

use crate::catalog::{Catalog, Column, EmbedError, Kind, Rel, Relation};
use crate::filter::Node;
use crate::order::{Direction, Nulls, Term};
use crate::select::{Col, Embed, Item, Join};
use crate::sql::{CompileError, Sql, field_expr, quote_ident, where_clause_from};

/// Why a query cannot plan: a relationship problem with its PGRST
/// code, a filter that cannot compile, or a shape the planner
/// refuses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanError {
    Embed(EmbedError),
    Compile(CompileError),
    Other(String),
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanError::Embed(e) => write!(f, "{e}"),
            PlanError::Compile(e) => write!(f, "{e}"),
            PlanError::Other(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for PlanError {}

impl From<EmbedError> for PlanError {
    fn from(e: EmbedError) -> PlanError {
        PlanError::Embed(e)
    }
}

impl From<CompileError> for PlanError {
    fn from(e: CompileError) -> PlanError {
        PlanError::Compile(e)
    }
}

fn refuse<T>(message: impl Into<String>) -> Result<T, PlanError> {
    Err(PlanError::Other(message.into()))
}

/// Everything the router collected for one read request. Routes are
/// embed paths, alias when the embed has one, relation name
/// otherwise; the empty route is the root.
#[derive(Debug, Default)]
pub struct Query {
    pub table: String,
    pub select: Vec<Item>,
    /// Filter trees with the route they arrived on. A condition
    /// whose field carries its own embed path routes by that too.
    pub filters: Vec<(Vec<String>, Node)>,
    pub order: Vec<(Vec<String>, Vec<Term>)>,
    pub limit: Vec<(Vec<String>, u64)>,
    pub offset: Vec<(Vec<String>, u64)>,
    /// When set, the root level reads FROM this relation while
    /// relationships still resolve against `table`: the
    /// representation select over a mutation CTE.
    pub source: Option<String>,
}

/// Plan the whole request into one statement whose rows are the
/// response records, embed columns already folded to jsonb. The
/// caller wraps rows into the response body.
pub fn plan(catalog: &Catalog, q: &Query) -> Result<Sql, PlanError> {
    plan_from(catalog, q, Vec::new())
}

/// The same, with placeholder numbering continuing from wherever
/// `params` already stands, the way where_clause_from does it. A
/// mutation binds its payload first and the representation select's
/// own filters follow, one dense parameter list across the whole
/// statement.
pub fn plan_from(catalog: &Catalog, q: &Query, params: Vec<String>) -> Result<Sql, PlanError> {
    let routes = collect_routes(&q.select);
    let filters = route_filters(q, &routes)?;
    for (route, _) in q.order.iter().filter(|(r, _)| !r.is_empty()) {
        check_route(&routes, route, "order")?;
    }
    for (route, _) in q.limit.iter().filter(|(r, _)| !r.is_empty()) {
        check_route(&routes, route, "limit")?;
    }
    for (route, _) in q.offset.iter().filter(|(r, _)| !r.is_empty()) {
        check_route(&routes, route, "offset")?;
    }

    let mut p = Planner {
        catalog,
        q,
        filters,
        params,
        next: 0,
    };
    let root = p.next_alias();
    let text = p.level(&q.table, &root, &q.select, &[], None)?;
    Ok(Sql {
        text,
        params: p.params,
    })
}

/// The count query a `Prefer: count=` total runs beside a read:
/// `select 1` over the root with its filters, and each `!inner`
/// embed folded into an EXISTS carrying its own filters and its own
/// inner children, since those are the only parts of the select tree
/// that change how many rows the root keeps. Left joined embeds,
/// output columns, order, and paging all drop out, which is exactly
/// PostgREST's readPlanToCountQuery.
pub fn count(catalog: &Catalog, q: &Query) -> Result<Sql, PlanError> {
    let routes = collect_routes(&q.select);
    let filters = route_filters(q, &routes)?;
    let mut p = Planner {
        catalog,
        q,
        filters,
        params: Vec::new(),
        next: 0,
    };
    let root = p.next_alias();
    let text = p.count_level(&q.table, &root, &q.select, &[], None)?;
    Ok(Sql {
        text,
        params: p.params,
    })
}

/// The key an embed answers to in routes and in the response.
fn key_of(e: &Embed) -> &str {
    e.alias.as_deref().unwrap_or(&e.relation)
}

fn collect_routes(items: &[Item]) -> Vec<Vec<String>> {
    let mut out = vec![Vec::new()];
    walk(items, &mut Vec::new(), &mut out);
    fn walk(items: &[Item], path: &mut Vec<String>, out: &mut Vec<Vec<String>>) {
        for item in items {
            if let Item::Embed(e) | Item::Spread(e) = item {
                path.push(key_of(e).to_string());
                out.push(path.clone());
                walk(&e.items, path, out);
                path.pop();
            }
        }
    }
    out
}

fn check_route(routes: &[Vec<String>], route: &[String], what: &str) -> Result<(), PlanError> {
    if routes.iter().any(|r| r == route) {
        return Ok(());
    }
    refuse(format!(
        "'{}' is not an embedded resource in this request, the {what} on it has nowhere to go",
        route.join(".")
    ))
}

/// Normalize every filter onto its full route: a condition carries
/// the rest of its path in the field, groups must already sit where
/// they apply.
fn route_filters(q: &Query, routes: &[Vec<String>]) -> Result<Vec<(Vec<String>, Node)>, PlanError> {
    let mut out = Vec::with_capacity(q.filters.len());
    for (route, node) in &q.filters {
        let (full, node) = match node {
            Node::Cond(c) if !c.field.embed.is_empty() => {
                let mut full = route.clone();
                full.extend(c.field.embed.iter().cloned());
                let mut c = c.clone();
                c.field.embed.clear();
                (full, Node::Cond(c))
            }
            other => {
                no_embeds_inside(other)?;
                (route.clone(), other.clone())
            }
        };
        check_route(routes, &full, "filter")?;
        out.push((full, node));
    }
    Ok(out)
}

fn no_embeds_inside(node: &Node) -> Result<(), PlanError> {
    match node {
        Node::Cond(c) if !c.field.embed.is_empty() => {
            refuse("a filter inside a logic group cannot reach an embedded resource")
        }
        Node::Cond(_) => Ok(()),
        Node::Group { kids, .. } => kids.iter().try_for_each(no_embeds_inside),
    }
}

struct Planner<'a> {
    catalog: &'a Catalog,
    q: &'a Query,
    filters: Vec<(Vec<String>, Node)>,
    params: Vec<String>,
    next: usize,
}

/// One expression of a level's select list.
struct OutCol {
    /// The expression alone, what GROUP BY repeats.
    expr: String,
    /// The full item, expression plus its output alias.
    rendered: String,
    aggregated: bool,
    /// `z.*` and spread `e.*` items, which cannot GROUP BY.
    splat: bool,
}

impl Planner<'_> {
    fn next_alias(&mut self) -> String {
        let a = format!("z{}", self.next);
        self.next += 1;
        a
    }

    /// One SELECT over one table: the root or the inside of an embed
    /// lateral. `link` is the join condition to the parent, already
    /// rendered against both aliases.
    fn level(
        &mut self,
        table: &str,
        alias: &str,
        items: &[Item],
        path: &[String],
        link: Option<String>,
    ) -> Result<String, PlanError> {
        let mut cols: Vec<OutCol> = Vec::new();
        let mut laterals: Vec<String> = Vec::new();

        let rel = self.catalog.relation(table);
        for item in items {
            match item {
                // A relation with a column that is written out
                // through a cast has its star spelled out, because
                // the call has to go somewhere and `t.*` has no room
                // for one. Every other star stays a star.
                Item::Star => match rel.filter(|r| r.represented()) {
                    Some(rel) => cols.extend(rel.columns.iter().map(|c| {
                        let expr = represented(alias, c);
                        OutCol {
                            rendered: format!("{expr} as {}", quote_ident(&c.name)),
                            expr,
                            aggregated: false,
                            splat: false,
                        }
                    })),
                    None => cols.push(OutCol {
                        expr: format!("{}.*", quote_ident(alias)),
                        rendered: format!("{}.*", quote_ident(alias)),
                        aggregated: false,
                        splat: true,
                    }),
                },
                Item::Col(c) => cols.push(self.col(alias, rel, c)?),
                Item::Embed(e) => {
                    self.embed(table, alias, e, path, false, &mut cols, &mut laterals)?
                }
                Item::Spread(e) => {
                    self.embed(table, alias, e, path, true, &mut cols, &mut laterals)?
                }
            }
        }
        if cols.is_empty() {
            // An empty embed selects nothing but still joins, the
            // inner flag is its whole point.
            cols.push(OutCol {
                expr: "1".into(),
                rendered: "1".into(),
                aggregated: false,
                splat: false,
            });
        }

        let grouped = cols.iter().any(|c| c.aggregated);
        if grouped && cols.iter().any(|c| c.splat) {
            return refuse("an aggregate cannot mix with * or a spread in one select list");
        }

        let mut sql = String::from("select ");
        sql.push_str(
            &cols
                .iter()
                .map(|c| c.rendered.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        );
        // The root reads from the source relation when the query is
        // a representation over a mutation CTE, embeds keep reading
        // their real tables.
        let from = match &self.q.source {
            Some(s) if path.is_empty() => s.as_str(),
            _ => table,
        };
        sql.push_str(&format!(
            " from {} as {}",
            quote_ident(from),
            quote_ident(alias)
        ));
        for l in &laterals {
            sql.push(' ');
            sql.push_str(l);
        }

        let mut conjuncts: Vec<String> = Vec::new();
        if let Some(link) = link {
            conjuncts.push(link);
        }
        let mine: Vec<Node> = self
            .filters
            .iter()
            .filter(|(r, _)| r == path)
            .map(|(_, n)| n.clone())
            .collect();
        if !mine.is_empty() {
            let rel = self.catalog.relation(table);
            let compiled =
                where_clause_from(&mine, Some(alias), std::mem::take(&mut self.params), rel)?;
            self.params = compiled.params;
            conjuncts.push(compiled.text);
        }
        if !conjuncts.is_empty() {
            sql.push_str(" where ");
            sql.push_str(&conjuncts.join(" AND "));
        }

        if grouped {
            let keys: Vec<&str> = cols
                .iter()
                .filter(|c| !c.aggregated)
                .map(|c| c.expr.as_str())
                .collect();
            if !keys.is_empty() {
                sql.push_str(" group by ");
                sql.push_str(&keys.join(", "));
            }
        }

        if let Some((_, terms)) = self.q.order.iter().find(|(r, _)| r == path) {
            sql.push_str(" order by ");
            sql.push_str(&order_sql(alias, terms));
        }
        if let Some((_, n)) = self.q.limit.iter().find(|(r, _)| r == path) {
            sql.push_str(&format!(" limit {n}"));
        }
        if let Some((_, n)) = self.q.offset.iter().find(|(r, _)| r == path) {
            sql.push_str(&format!(" offset {n}"));
        }
        Ok(sql)
    }

    /// One level of the count query: `select 1` from this table with
    /// the filters routed here, inner embeds recursing as EXISTS.
    fn count_level(
        &mut self,
        table: &str,
        alias: &str,
        items: &[Item],
        path: &[String],
        link: Option<String>,
    ) -> Result<String, PlanError> {
        let from = match &self.q.source {
            Some(s) if path.is_empty() => s.as_str(),
            _ => table,
        };
        let mut sql = format!(
            "select 1 from {} as {}",
            quote_ident(from),
            quote_ident(alias)
        );

        let mut conjuncts: Vec<String> = Vec::new();
        if let Some(link) = link {
            conjuncts.push(link);
        }
        let mine: Vec<Node> = self
            .filters
            .iter()
            .filter(|(r, _)| r == path)
            .map(|(_, n)| n.clone())
            .collect();
        if !mine.is_empty() {
            let rel = self.catalog.relation(table);
            let compiled =
                where_clause_from(&mine, Some(alias), std::mem::take(&mut self.params), rel)?;
            self.params = compiled.params;
            conjuncts.push(compiled.text);
        }
        for item in items {
            let (Item::Embed(e) | Item::Spread(e)) = item else {
                continue;
            };
            if e.join != Some(Join::Inner) {
                continue;
            }
            let rel = self
                .catalog
                .resolve(table, &e.relation, e.hint.as_deref())?;
            let child = self.next_alias();
            let junction = rel.via.as_ref().map(|_| self.next_alias());
            let mut child_path = path.to_vec();
            child_path.push(key_of(e).to_string());
            let link = link_sql(&rel, alias, &child, junction.as_deref());
            let sub = self.count_level(&e.relation, &child, &e.items, &child_path, Some(link))?;
            conjuncts.push(format!("exists ({sub})"));
        }
        if !conjuncts.is_empty() {
            sql.push_str(" where ");
            sql.push_str(&conjuncts.join(" AND "));
        }
        Ok(sql)
    }

    /// One column pick: the expression, its casts, its aggregate,
    /// and the output key PostgREST would use.
    ///
    /// A data representation goes on first and the request's own
    /// cast on top of it, so `label_color::text` is the text of what
    /// the client would have been shown rather than the text of what
    /// the column holds. An aggregate takes the column bare: nothing
    /// sums a json value, and upstream has no case for it either.
    fn col(&self, alias: &str, rel: Option<&Relation>, c: &Col) -> Result<OutCol, PlanError> {
        let mut expr = match &c.field {
            Some(f) => {
                let mut e = match rel.filter(|_| c.agg.is_none() && f.path.is_empty()) {
                    Some(rel) => match rel.column(&f.name) {
                        Some(col) => represented(alias, col),
                        None => field_expr(Some(alias), &f.name, &f.path),
                    },
                    None => field_expr(Some(alias), &f.name, &f.path),
                };
                if let Some(cast) = &f.cast {
                    e = format!("{e}::{}", checked_cast(cast)?);
                }
                e
            }
            None => String::new(),
        };
        if let Some(agg) = c.agg {
            expr = match &c.field {
                Some(_) => format!("{}({expr})", agg.name()),
                None => "count(*)".into(),
            };
            if let Some(cast) = &c.agg_cast {
                expr = format!("{expr}::{}", checked_cast(cast)?);
            }
        }
        let key = match (&c.alias, c.agg, &c.field) {
            (Some(a), _, _) => a.clone(),
            (None, Some(agg), _) => agg.name().to_string(),
            (None, None, Some(f)) => f
                .path
                .iter()
                .rev()
                .find_map(|s| match &s.key {
                    crate::scan::JsonKey::Name(n) => Some(n.clone()),
                    crate::scan::JsonKey::Index(_) => None,
                })
                .unwrap_or_else(|| f.name.clone()),
            (None, None, None) => unreachable!("a field free column is always an aggregate"),
        };
        Ok(OutCol {
            rendered: format!("{expr} as {}", quote_ident(&key)),
            expr,
            aggregated: c.agg.is_some(),
            splat: false,
        })
    }

    /// One embedded resource: resolve the relationship, build the
    /// child level, wrap it in the lateral shape its kind wants.
    #[allow(clippy::too_many_arguments)]
    fn embed(
        &mut self,
        parent_table: &str,
        parent_alias: &str,
        e: &Embed,
        path: &[String],
        spread: bool,
        cols: &mut Vec<OutCol>,
        laterals: &mut Vec<String>,
    ) -> Result<(), PlanError> {
        let rel = self
            .catalog
            .resolve(parent_table, &e.relation, e.hint.as_deref())?;
        if spread && rel.kind == Kind::ToMany {
            return refuse(format!(
                "a spread embed needs a to one relationship, '{}' is to many here",
                e.relation
            ));
        }

        let child = self.next_alias();
        let junction = rel.via.as_ref().map(|_| self.next_alias());
        let mut child_path = path.to_vec();
        child_path.push(key_of(e).to_string());
        let link = link_sql(&rel, parent_alias, &child, junction.as_deref());
        let body = self.level(&e.relation, &child, &e.items, &child_path, Some(link))?;

        let wrap = quote_ident(&format!("e_{child}"));
        let inner = e.join == Some(Join::Inner);
        let key = quote_ident(key_of(e));
        if spread {
            let joiner = if inner { "join" } else { "left join" };
            laterals.push(format!("{joiner} lateral ({body}) as {wrap} on true"));
            cols.push(OutCol {
                expr: format!("{wrap}.*"),
                rendered: format!("{wrap}.*"),
                aggregated: false,
                splat: true,
            });
            return Ok(());
        }

        let row = quote_ident(&format!("r_{child}"));
        match rel.kind {
            Kind::ToMany => {
                let lateral = format!(
                    "(select jsonb_agg(to_jsonb({row})) as \"j\" from ({body}) as {row}) as {wrap}"
                );
                if inner {
                    laterals.push(format!(
                        "join lateral {lateral} on {wrap}.\"j\" is not null"
                    ));
                } else {
                    laterals.push(format!("left join lateral {lateral} on true"));
                }
                if !e.items.is_empty() {
                    let expr = format!("coalesce({wrap}.\"j\", '[]'::jsonb)");
                    cols.push(OutCol {
                        rendered: format!("{expr} as {key}"),
                        expr,
                        aggregated: false,
                        splat: false,
                    });
                }
            }
            Kind::ToOne => {
                let joiner = if inner { "join" } else { "left join" };
                laterals.push(format!(
                    "{joiner} lateral (select to_jsonb({row}) as \"j\" from ({body}) as {row}) as {wrap} on true"
                ));
                if !e.items.is_empty() {
                    cols.push(OutCol {
                        expr: format!("{wrap}.\"j\""),
                        rendered: format!("{wrap}.\"j\" as {key}"),
                        aggregated: false,
                        splat: false,
                    });
                }
            }
        }
        Ok(())
    }
}

/// One column of a level as the client should see it: the column
/// itself, or the call that writes it as json when its type carries
/// a cast to json. The function name arrived quoted from postgres.
fn represented(alias: &str, col: &Column) -> String {
    let plain = format!("{}.{}", quote_ident(alias), quote_ident(&col.name));
    match &col.to_json {
        Some(func) => format!("{func}({plain})"),
        None => plain,
    }
}

/// The join condition between a child level and its parent, straight
/// column pairs, or an IN subquery through the junction of a many to
/// many so the child level still reads from exactly one table.
fn link_sql(rel: &Rel, parent: &str, child: &str, junction: Option<&str>) -> String {
    match &rel.via {
        None => rel
            .join
            .iter()
            .map(|(p, c)| {
                format!(
                    "{}.{} = {}.{}",
                    quote_ident(child),
                    quote_ident(c),
                    quote_ident(parent),
                    quote_ident(p)
                )
            })
            .collect::<Vec<_>>()
            .join(" AND "),
        Some(j) => {
            let w = quote_ident(junction.expect("a junction alias travels with a via"));
            let child_side: Vec<String> = j
                .join
                .iter()
                .map(|(_, c)| format!("{}.{}", quote_ident(child), quote_ident(c)))
                .collect();
            let junction_side: Vec<String> = j
                .join
                .iter()
                .map(|(jc, _)| format!("{w}.{}", quote_ident(jc)))
                .collect();
            let parent_side: Vec<String> = rel
                .join
                .iter()
                .map(|(p, jc)| {
                    format!(
                        "{w}.{} = {}.{}",
                        quote_ident(jc),
                        quote_ident(parent),
                        quote_ident(p)
                    )
                })
                .collect();
            format!(
                "({}) in (select {} from {} as {w} where {})",
                child_side.join(", "),
                junction_side.join(", "),
                quote_ident(&j.table),
                parent_side.join(" AND ")
            )
        }
    }
}

/// A cast survives only as a plain identifier, which is exactly the
/// set of type names that need no quoting and carry no injection.
fn checked_cast(cast: &str) -> Result<&str, PlanError> {
    let mut bytes = cast.bytes();
    let head_ok = bytes
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_');
    if head_ok && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        Ok(cast)
    } else {
        refuse(format!("unsupported cast ({cast})"))
    }
}

fn order_sql(alias: &str, terms: &[Term]) -> String {
    terms
        .iter()
        .map(|t| {
            let mut s = field_expr(Some(alias), &t.name, &t.path);
            match t.direction {
                Some(Direction::Asc) => s.push_str(" asc"),
                Some(Direction::Desc) => s.push_str(" desc"),
                None => {}
            }
            match t.nulls {
                Some(Nulls::First) => s.push_str(" nulls first"),
                Some(Nulls::Last) => s.push_str(" nulls last"),
                None => {}
            }
            s
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnRow, FkRow};
    use crate::filter::{Parsed, parse_pair};
    use crate::{order, select};

    fn fk(
        constraint: &str,
        table: &str,
        columns: &[&str],
        ref_table: &str,
        ref_columns: &[&str],
    ) -> FkRow {
        FkRow {
            constraint: constraint.into(),
            table: table.into(),
            columns: columns.iter().map(|s| s.to_string()).collect(),
            ref_table: ref_table.into(),
            ref_columns: ref_columns.iter().map(|s| s.to_string()).collect(),
            unique: false,
            in_pk: false,
        }
    }

    fn shop() -> Catalog {
        let mut fks = vec![
            fk(
                "orders_user_id_fkey",
                "orders",
                &["user_id"],
                "users",
                &["id"],
            ),
            fk(
                "orders_billing_address_id_fkey",
                "orders",
                &["billing_address_id"],
                "addresses",
                &["id"],
            ),
            fk(
                "orders_shipping_address_id_fkey",
                "orders",
                &["shipping_address_id"],
                "addresses",
                &["id"],
            ),
            fk(
                "order_items_order_id_fkey",
                "order_items",
                &["order_id"],
                "orders",
                &["id"],
            ),
            fk(
                "order_items_product_id_fkey",
                "order_items",
                &["product_id"],
                "products",
                &["id"],
            ),
        ];
        fks[3].in_pk = true;
        fks[4].in_pk = true;
        Catalog::new(fks)
    }

    fn query(table: &str, sel: &str) -> Query {
        Query {
            table: table.into(),
            select: select::parse(sel).unwrap_or_else(|e| panic!("{sel}: {e}")),
            ..Query::default()
        }
    }

    fn filt(q: &mut Query, key: &str, value: &str) {
        match parse_pair(key, value).unwrap_or_else(|e| panic!("{key}={value}: {e}")) {
            Parsed::Filter(c) => q.filters.push((Vec::new(), Node::Cond(c))),
            Parsed::Logic {
                embed,
                op,
                negated,
                kids,
            } => q.filters.push((embed, Node::Group { op, negated, kids })),
        }
    }

    fn text(q: &Query) -> String {
        plan(&shop(), q).unwrap_or_else(|e| panic!("{e}")).text
    }

    fn fails(q: &Query) -> PlanError {
        match plan(&shop(), q) {
            Ok(sql) => panic!("planned as {}", sql.text),
            Err(e) => e,
        }
    }

    /// Two relations whose colour column is a domain with a cast to
    /// json, and a foreign key between them so the cast can be
    /// watched crossing into an embed.
    fn painted() -> Catalog {
        let colour = |table: &str, name: &str| ColumnRow {
            table: table.into(),
            column: Column {
                name: name.into(),
                to_json: Some("test.json".into()),
                from_text: Some("test.color".into()),
                from_json: Some("test.color".into()),
                type_name: "test.color".into(),
                default_expr: None,
            },
        };
        let plain = |table: &str, name: &str| ColumnRow {
            table: table.into(),
            column: Column {
                name: name.into(),
                ..Column::default()
            },
        };
        Catalog::new(vec![fk(
            "notes_todo_id_fkey",
            "notes",
            &["todo_id"],
            "todos",
            &["id"],
        )])
        .with_relations(
            vec!["notes".into(), "todos".into(), "users".into()],
            vec![
                plain("todos", "id"),
                colour("todos", "label_color"),
                plain("notes", "id"),
                plain("notes", "todo_id"),
                colour("notes", "tint"),
                plain("users", "id"),
            ],
        )
    }

    fn painted_text(table: &str, sel: &str) -> String {
        let q = query(table, sel);
        plan(&painted(), &q).unwrap_or_else(|e| panic!("{e}")).text
    }

    #[test]
    fn a_column_is_written_out_through_the_cast_its_type_has() {
        assert!(
            painted_text("todos", "id,label_color")
                .contains(r#""z0"."id" as "id", test.json("z0"."label_color") as "label_color""#),
            "{}",
            painted_text("todos", "id,label_color")
        );
        // The request's own cast goes on top of the representation
        // rather than instead of it, so this is the text of what the
        // client would have been shown.
        assert!(
            painted_text("todos", "label_color::text")
                .contains(r#"test.json("z0"."label_color")::text as "label_color""#),
            "{}",
            painted_text("todos", "label_color::text")
        );
    }

    #[test]
    fn a_star_is_spelled_out_only_where_a_cast_needs_the_room() {
        assert_eq!(
            painted_text("todos", "*"),
            r#"select "z0"."id" as "id", test.json("z0"."label_color") as "label_color" from "todos" as "z0""#
        );
        // A relation with nothing to represent keeps its star, which
        // is what upstream leaves alone too.
        assert_eq!(
            painted_text("users", "*"),
            r#"select "z0".* from "users" as "z0""#
        );
    }

    #[test]
    fn an_embed_is_cast_the_same_way_the_root_is() {
        assert!(
            painted_text("todos", "label_color,notes(tint)")
                .contains(r#"select test.json("z1"."tint") as "tint" from "notes" as "z1""#),
            "{}",
            painted_text("todos", "label_color,notes(tint)")
        );
    }

    #[test]
    fn what_is_not_the_column_itself_is_left_alone() {
        // An aggregate takes the column bare: nothing sums a json
        // value, and a json path has already left the column's type
        // behind by the time it lands.
        assert!(
            painted_text("todos", "label_color.count()").contains(r#"count("z0"."label_color")"#),
            "{}",
            painted_text("todos", "label_color.count()")
        );
        assert!(
            painted_text("todos", "label_color->shade").contains(r#""z0"."label_color"->'shade'"#),
            "{}",
            painted_text("todos", "label_color->shade")
        );
    }

    #[test]
    fn a_filter_reads_its_value_through_the_relation_it_names() {
        let mut q = query("todos", "id");
        filt(&mut q, "label_color", "eq.red");
        let s = plan(&painted(), &q).unwrap_or_else(|e| panic!("{e}"));
        assert!(
            s.text
                .ends_with(r#" where "z0"."label_color" = test.color($1)"#),
            "{}",
            s.text
        );
        assert_eq!(s.params, vec!["red"]);
    }

    #[test]
    fn a_to_many_embed_folds_with_jsonb_agg() {
        let q = query("users", "id,orders(id)");
        assert_eq!(
            text(&q),
            r#"select "z0"."id" as "id", coalesce("e_z1"."j", '[]'::jsonb) as "orders" from "users" as "z0" left join lateral (select jsonb_agg(to_jsonb("r_z1")) as "j" from (select "z1"."id" as "id" from "orders" as "z1" where "z1"."user_id" = "z0"."id") as "r_z1") as "e_z1" on true"#
        );
    }

    #[test]
    fn a_to_one_embed_carries_one_jsonb_value() {
        let q = query("orders", "id,users(id)");
        assert_eq!(
            text(&q),
            r#"select "z0"."id" as "id", "e_z1"."j" as "users" from "orders" as "z0" left join lateral (select to_jsonb("r_z1") as "j" from (select "z1"."id" as "id" from "users" as "z1" where "z1"."id" = "z0"."user_id") as "r_z1") as "e_z1" on true"#
        );
    }

    #[test]
    fn inner_join_and_the_routed_filter() {
        let mut q = query("users", "id,orders!inner(id)");
        filt(&mut q, "orders.total", "gte.100");
        let sql = plan(&shop(), &q).unwrap();
        assert_eq!(
            sql.text,
            r#"select "z0"."id" as "id", coalesce("e_z1"."j", '[]'::jsonb) as "orders" from "users" as "z0" join lateral (select jsonb_agg(to_jsonb("r_z1")) as "j" from (select "z1"."id" as "id" from "orders" as "z1" where "z1"."user_id" = "z0"."id" AND "z1"."total" >= $1) as "r_z1") as "e_z1" on "e_z1"."j" is not null"#
        );
        assert_eq!(sql.params, vec!["100"]);
    }

    #[test]
    fn many_to_many_walks_the_junction_in_a_subquery() {
        let q = query("orders", "id,products(id)");
        let t = text(&q);
        assert!(
            t.contains(
                r#"where ("z1"."id") in (select "z2"."product_id" from "order_items" as "z2" where "z2"."order_id" = "z0"."id")"#
            ),
            "{t}"
        );
    }

    #[test]
    fn a_spread_merges_the_columns() {
        let q = query("orders", "id,...users(id)");
        assert_eq!(
            text(&q),
            r#"select "z0"."id" as "id", "e_z1".* from "orders" as "z0" left join lateral (select "z1"."id" as "id" from "users" as "z1" where "z1"."id" = "z0"."user_id") as "e_z1" on true"#
        );

        let q = query("users", "id,...orders(id)");
        let e = fails(&q);
        assert!(e.to_string().contains("to one"), "{e}");
    }

    #[test]
    fn nested_order_and_limit_land_in_the_subquery() {
        let mut q = query("users", "id,orders(id)");
        q.order
            .push((vec!["orders".into()], order::parse("total.desc").unwrap()));
        q.limit.push((vec!["orders".into()], 2));
        let t = text(&q);
        assert!(
            t.contains(r#""z0"."id" order by "z1"."total" desc limit 2) as "r_z1""#),
            "{t}"
        );
    }

    #[test]
    fn aggregates_group_by_the_plain_columns() {
        let q = query("orders", "user_id,total.sum()");
        assert_eq!(
            text(&q),
            r#"select "z0"."user_id" as "user_id", sum("z0"."total") as "sum" from "orders" as "z0" group by "z0"."user_id""#
        );

        let q = query("orders", "*,count()");
        let e = fails(&q);
        assert!(e.to_string().contains("cannot mix"), "{e}");
    }

    #[test]
    fn routes_answer_to_the_alias() {
        let mut q = query("users", "id,o:orders(id)");
        filt(&mut q, "o.total", "gte.1");
        assert!(text(&q).contains(r#""z1"."total" >= $1"#));

        let mut q = query("users", "id,o:orders(id)");
        filt(&mut q, "orders.total", "gte.1");
        let e = fails(&q);
        assert!(e.to_string().contains("not an embedded resource"), "{e}");
    }

    #[test]
    fn an_empty_inner_embed_is_pure_existence() {
        let mut q = query("users", "id,orders!inner()");
        filt(&mut q, "orders.total", "gte.100");
        let t = text(&q);
        assert!(t.starts_with(r#"select "z0"."id" as "id" from"#), "{t}");
        assert!(t.contains(r#"select 1 from "orders""#), "{t}");
        assert!(t.contains(r#"is not null"#), "{t}");
    }

    #[test]
    fn the_count_query_keeps_only_what_changes_the_count() {
        // A left joined embed and the output shape drop out entirely.
        let mut q = query("users", "id,orders(id)");
        filt(&mut q, "id", "gte.5");
        let sql = count(&shop(), &q).unwrap();
        assert_eq!(
            sql.text,
            r#"select 1 from "users" as "z0" where "z0"."id" >= $1"#
        );
        assert_eq!(sql.params, vec!["5"]);

        // An inner embed folds to EXISTS and carries its filter.
        let mut q = query("users", "id,orders!inner(id)");
        filt(&mut q, "orders.total", "gte.100");
        let sql = count(&shop(), &q).unwrap();
        assert_eq!(
            sql.text,
            r#"select 1 from "users" as "z0" where exists (select 1 from "orders" as "z1" where "z1"."user_id" = "z0"."id" AND "z1"."total" >= $1)"#
        );
        assert_eq!(sql.params, vec!["100"]);

        // A filter on the left joined embed stays out of the count.
        let mut q = query("users", "id,orders(id)");
        filt(&mut q, "orders.total", "gte.100");
        let sql = count(&shop(), &q).unwrap();
        assert_eq!(sql.text, r#"select 1 from "users" as "z0""#);
        assert!(sql.params.is_empty());
    }

    #[test]
    fn what_the_planner_refuses() {
        let q = query("orders", "id,addresses(id)");
        let PlanError::Embed(e) = fails(&q) else {
            panic!("expected the ambiguity to surface")
        };
        assert_eq!(e.code, "PGRST201");

        let q = query("orders", "id::in;valid");
        assert!(fails(&q).to_string().contains("unsupported cast"));

        // The grammar cannot spell an embedded field inside a logic
        // group, so the planner's guard needs a hand built node.
        let mut q = query("users", "id,orders(id)");
        let Parsed::Filter(c) = parse_pair("orders.total", "gte.1").unwrap() else {
            panic!()
        };
        q.filters.push((
            Vec::new(),
            Node::Group {
                op: crate::filter::LogicOp::Or,
                negated: false,
                kids: vec![Node::Cond(c)],
            },
        ));
        assert!(fails(&q).to_string().contains("inside a logic group"));
    }
}
