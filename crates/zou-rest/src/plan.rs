//! The embedding planner: a select tree to one SQL statement.
//!
//! Every embedded resource becomes a lateral subquery against the
//! target table, to many aggregated with jsonb_agg over to_jsonb, to
//! one carrying the child's own columns and folded to json on the way
//! out, and spreads merged into the parent's column list. Spreading a
//! to many merges a list per column instead: the lateral aggregates
//! each of the child's columns on its own, so `...processes(name)`
//! gives the parent one `name` holding every child's name in the
//! order the embed asked for. The default join is left, so parents
//! keep their rows and an absent embed reads as `[]` or `null`;
//! `!inner` swaps the lateral in as a plain join, which is what makes
//! a filter on the embed remove parent rows.
//!
//! The level around a lateral can still point at it, which is what
//! `order=clients(name)` and `clients=is.null` need: the first orders
//! by a column of the lateral, the second asks whether the lateral
//! found anything at all. Only a to one can be ordered by, since a
//! list has no one value to sort a parent row by, and only `is.null`
//! and `not.is.null` reach an embed by name at all, every other
//! operator reading the name as a column of the level itself.
//!
//! Nested filters, order, limit, and offset route to their embed by
//! path: the query pair `orders.status=eq.x` carries its path in the
//! filter's field, `orders.order=total.desc` and `orders.limit=2`
//! carry it in the parameter name and arrive here as explicit routes.
//! A route that names no embed in the select tree is refused the way
//! PostgREST refuses it, since silently dropping a filter is the one
//! thing a REST layer must never do.
//!
//! Every level reads from exactly one table under an alias, the
//! table's own name at the root and a generated z{n} per embed, so a
//! self referencing embed joins two aliases of the same table instead
//! of colliding, a message from postgres about a column that is not
//! there still names the table the caller asked for, and a
//! many to many walks its junction through an IN subquery rather
//! than a second FROM entry. All literals still bind as parameters
//! through the WHERE compiler, the planner itself splices only
//! identifiers it quotes and numbers it formats.
//!
//! Aggregates group by every non aggregated output expression, the
//! PostgREST rule, and jsonb embed columns take part since jsonb has
//! equality. What the planner refuses, it refuses loudly: mixing an
//! aggregate with `*` or with a spread, aggregating inside a spread
//! to many, casting to a type that is not a plain identifier.

use std::fmt;

use crate::catalog::{Catalog, Column, Details, EmbedError, Kind, Rel, Relation};
use crate::filter::{Node, Op, Value};
use crate::order::{Direction, Nulls, Term};
use crate::select::{Col, Embed, Item, Join};
use crate::sql::{CompileError, EmbedTest, Sql, field_expr, quote_ident, where_clause_over};

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
        check_route(&routes, route)?;
    }
    for (route, _) in q.limit.iter().filter(|(r, _)| !r.is_empty()) {
        check_route(&routes, route)?;
    }
    for (route, _) in q.offset.iter().filter(|(r, _)| !r.is_empty()) {
        check_route(&routes, route)?;
    }

    let mut p = Planner {
        catalog,
        q,
        filters,
        params,
        next: 0,
    };
    let root = p.root_alias();
    let text = p
        .level(
            &q.table,
            &root,
            &q.select,
            &[],
            Link::Root,
            Wanted::default(),
        )?
        .sql;
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
    count_from(catalog, q, Vec::new())
}

/// The same, numbered from wherever `params` already stands. A call
/// binds its arguments first and both the rows and the total are
/// read out of one statement, so the count's own placeholders carry
/// on from the ones the representation already took.
pub fn count_from(catalog: &Catalog, q: &Query, params: Vec<String>) -> Result<Sql, PlanError> {
    let routes = collect_routes(&q.select);
    let filters = route_filters(q, &routes)?;
    let mut p = Planner {
        catalog,
        q,
        filters,
        params,
        next: 0,
    };
    let root = p.root_alias();
    let text = p.count_level(&q.table, &root, &q.select, &[], Link::Root)?;
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

/// PostgREST's PGRST108: a filter, an order, or a page was addressed
/// to something the select tree does not embed. The message names one
/// resource rather than the path it was written as, and upstream
/// names the first segment that has nowhere to go, so
/// `nope.deeper.id=eq.1` is about `nope`.
fn not_embedded(name: &str) -> EmbedError {
    EmbedError {
        code: "PGRST108",
        message: format!("'{name}' is not an embedded resource in this request"),
        details: None,
        hint: Some(format!(
            "Verify that '{name}' is included in the 'select' query parameter."
        )),
    }
}

/// PostgREST's PGRST127: a shape the planner reads and does not
/// build. Aggregating inside a spread of a to many is upstream's own
/// gap, and zou keeps the same hole so the answer is the same answer.
fn not_implemented(details: &str) -> EmbedError {
    EmbedError {
        code: "PGRST127",
        message: "Feature not implemented".into(),
        details: Some(Details::Text(details.to_string())),
        hint: None,
    }
}

/// PostgREST's PGRST118: an order term reached into an embed that
/// brings back a list, and a list has no one value to sort a parent
/// row by.
fn unordered_relationship(parent: &str, key: &str) -> EmbedError {
    EmbedError {
        code: "PGRST118",
        message: format!("A related order on '{key}' is not possible"),
        details: Some(Details::Text(format!(
            "'{parent}' and '{key}' do not form a many-to-one or one-to-one relationship"
        ))),
        hint: None,
    }
}

fn check_route(routes: &[Vec<String>], route: &[String]) -> Result<(), PlanError> {
    for n in 1..=route.len() {
        if !routes.iter().any(|r| r.as_slice() == &route[..n]) {
            return Err(not_embedded(&route[n - 1]).into());
        }
    }
    Ok(())
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
        check_route(routes, &full)?;
        out.push((full, node));
    }
    Ok(out)
}

/// Whether a filter tree asks an embed by name whether it found
/// anything. `is.null` and `not.is.null` are the only tests that reach
/// an embed, so they are the only ones the count query has to turn
/// into an EXISTS and the only ones the planner has to hand the read
/// query a predicate for.
fn asks_null(node: &Node, key: &str) -> bool {
    match node {
        Node::Cond(c) => {
            c.field.column == key
                && c.field.path.is_empty()
                && c.op == Op::Is
                && c.quant.is_none()
                && matches!(&c.value, Value::Lit(w) if w == "null")
        }
        Node::Group { kids, .. } => kids.iter().any(|k| asks_null(k, key)),
    }
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
    /// The names this item answers to in the response. One for an
    /// ordinary column, the whole of a spread's column list for a
    /// spread, and none for a star nobody asked to have spelled out.
    keys: Vec<String>,
}

/// What the thing around a level needs from it beyond the SQL.
///
/// A spread of a to many reads its body one column at a time, so the
/// body has to spell out every column it selects, stars and all, and
/// its order has to come back out rather than be written into it,
/// because the order belongs inside the aggregate. A spread of a to
/// one nested in one has to spell its columns out too, since they
/// become the outer spread's columns, but it keeps its own order.
#[derive(Clone, Copy, Default)]
struct Wanted {
    names: bool,
    order: bool,
}

/// A planned level: the SELECT, the keys its select list answers to,
/// and the order terms it handed back instead of applying.
struct Level {
    sql: String,
    keys: Vec<String>,
    order: Vec<String>,
}

/// An embedded resource as the level around it sees it: the name it
/// answers to, the lateral alias its columns hang off, and whether it
/// brings back one row or a list. A filter and an order term at this
/// level can both name it, and what they are allowed to do with it
/// depends on the kind.
struct Embedded {
    key: String,
    alias: String,
    kind: Kind,
    /// The relation underneath, which an order term on it needs in
    /// order to know how to read a column of it.
    table: String,
}

/// How a level hangs off the one around it.
///
/// A foreign key gives a condition to put in the where clause and
/// leaves the from clause alone. A computed relationship is the other
/// way round: the parent row is the function's argument, so the link
/// is in the from clause and there is no condition to add anywhere.
enum Link {
    /// The root, with nothing around it.
    Root,
    /// A join condition, already rendered against both aliases.
    On(String),
    /// A call taking the parent row, already rendered, which the
    /// level reads from instead of from its table.
    Call(String),
}

impl Planner<'_> {
    /// The name the root level goes by.
    ///
    /// It is the table's own name rather than a generated alias,
    /// because it is the word the caller wrote and the word postgres
    /// puts in a message about a column that is not there: a request
    /// for a column items does not have should hear about items. Two
    /// roots keep the generated alias anyway. A mutation's root is the
    /// CTE holding the rows it just wrote rather than the table, and a
    /// table whose name is already shaped like one of these aliases
    /// would collide with the first embed.
    fn root_alias(&mut self) -> String {
        let table = &self.q.table;
        let alias_shaped = table.len() > 1
            && table.starts_with('z')
            && table[1..].bytes().all(|b| b.is_ascii_digit());
        if self.q.source.is_some() || alias_shaped {
            return self.next_alias();
        }
        let root = table.clone();
        // The embeds carry on from z1 as if the root had taken z0, so
        // that the alias of a level still says how deep it is.
        self.next = 1;
        root
    }

    fn next_alias(&mut self) -> String {
        let a = format!("z{}", self.next);
        self.next += 1;
        a
    }

    /// One SELECT over one table: the root or the inside of an embed
    /// lateral. `link` is how this level reaches the one around it.
    fn level(
        &mut self,
        table: &str,
        alias: &str,
        items: &[Item],
        path: &[String],
        link: Link,
        wanted: Wanted,
    ) -> Result<Level, PlanError> {
        let mut cols: Vec<OutCol> = Vec::new();
        let mut laterals: Vec<String> = Vec::new();
        let mut embeds: Vec<Embedded> = Vec::new();

        let rel = self.catalog.relation(table);
        for item in items {
            match item {
                // A relation with a column that is written out
                // through a cast has its star spelled out, because
                // the call has to go somewhere and `t.*` has no room
                // for one. A star inside a spread of a to many is
                // spelled out too, since every column of it has to be
                // aggregated by name. Every other star stays a star.
                Item::Star => match rel.filter(|r| r.represented() || wanted.names) {
                    Some(rel) => cols.extend(rel.columns.iter().map(|c| {
                        let expr = represented(alias, c);
                        OutCol {
                            rendered: format!("{expr} as {}", quote_ident(&c.name)),
                            expr,
                            aggregated: false,
                            splat: false,
                            keys: vec![c.name.clone()],
                        }
                    })),
                    None => cols.push(OutCol {
                        expr: format!("{}.*", quote_ident(alias)),
                        rendered: format!("{}.*", quote_ident(alias)),
                        aggregated: false,
                        splat: true,
                        keys: Vec::new(),
                    }),
                },
                Item::Col(c) => cols.push(self.col(alias, rel, c)?),
                Item::Embed(e) => self.embed(
                    table,
                    alias,
                    e,
                    path,
                    false,
                    wanted,
                    &mut cols,
                    &mut laterals,
                    &mut embeds,
                )?,
                Item::Spread(e) => self.embed(
                    table,
                    alias,
                    e,
                    path,
                    true,
                    wanted,
                    &mut cols,
                    &mut laterals,
                    &mut embeds,
                )?,
            }
        }
        if cols.is_empty() {
            // An empty embed selects nothing of its own but still
            // joins, the inner flag is its whole point, and the level
            // around it can still order by one of its columns. So the
            // body carries every column and the embed drops the value.
            cols.push(OutCol {
                expr: format!("{}.*", quote_ident(alias)),
                rendered: format!("{}.*", quote_ident(alias)),
                aggregated: false,
                splat: true,
                keys: Vec::new(),
            });
        }

        let grouped = cols.iter().any(|c| c.aggregated);
        if grouped && cols.iter().any(|c| c.splat) {
            return refuse("an aggregate cannot mix with * or a spread in one select list");
        }
        if grouped && wanted.order {
            return Err(not_implemented(
                "Aggregates are not implemented for one-to-many or many-to-many spreads.",
            )
            .into());
        }
        let keys: Vec<String> = cols.iter().flat_map(|c| c.keys.clone()).collect();

        // The order of a spread of a to many is not this level's to
        // apply: it goes inside the aggregate that folds each column
        // up. The columns it names have to be selected here anyway,
        // under names of their own, because nothing can be ordered by
        // what the subquery did not carry out.
        let mut lifted: Vec<String> = Vec::new();
        let terms = self.q.order.iter().find(|(r, _)| r == path);
        if let (true, Some((_, terms))) = (wanted.order, terms) {
            for (i, (expr, dir)) in order_terms(self.catalog, table, alias, terms, &embeds)?
                .into_iter()
                .enumerate()
            {
                let name = quote_ident(&format!("{alias}_o{}", i + 1));
                cols.push(OutCol {
                    rendered: format!("{expr} as {name}"),
                    expr,
                    aggregated: false,
                    splat: false,
                    keys: Vec::new(),
                });
                lifted.push(format!("{name}{dir}"));
            }
        }

        let mut sql = String::from("select ");
        sql.push_str(
            &cols
                .iter()
                .map(|c| c.rendered.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        );
        sql.push_str(&format!(
            " from {} as {}",
            from_sql(&link, &self.q.source, table, path),
            quote_ident(alias)
        ));
        for l in &laterals {
            sql.push(' ');
            sql.push_str(l);
        }

        let mut conjuncts: Vec<String> = Vec::new();
        if let Link::On(link) = link {
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
            // A read query has the laterals in front of it, so asking
            // whether an embed found anything is a test on the lateral
            // itself: the whole row for a to one, the aggregate for a
            // to many, which is null when jsonb_agg saw no rows.
            let tests: Vec<EmbedTest> = embeds
                .iter()
                .map(|e| {
                    let w = quote_ident(&e.alias);
                    match e.kind {
                        Kind::ToOne => EmbedTest {
                            key: e.key.clone(),
                            present: format!("{w} is distinct from null"),
                            absent: format!("{w} is not distinct from null"),
                        },
                        Kind::ToMany => EmbedTest {
                            key: e.key.clone(),
                            present: format!("{w}.\"j\" is not null"),
                            absent: format!("{w}.\"j\" is null"),
                        },
                    }
                })
                .collect();
            let compiled = where_clause_over(
                &mine,
                Some(alias),
                std::mem::take(&mut self.params),
                rel,
                &tests,
            )?;
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

        if let (false, Some((_, terms))) = (wanted.order, terms) {
            sql.push_str(" order by ");
            sql.push_str(&order_sql(self.catalog, table, alias, terms, &embeds)?);
        }
        if let Some((_, n)) = self.q.limit.iter().find(|(r, _)| r == path) {
            sql.push_str(&format!(" limit {n}"));
        }
        if let Some((_, n)) = self.q.offset.iter().find(|(r, _)| r == path) {
            sql.push_str(&format!(" offset {n}"));
        }
        Ok(Level {
            sql,
            keys,
            order: lifted,
        })
    }

    /// One level of the count query: `select 1` from this table with
    /// the filters routed here, inner embeds recursing as EXISTS.
    fn count_level(
        &mut self,
        table: &str,
        alias: &str,
        items: &[Item],
        path: &[String],
        link: Link,
    ) -> Result<String, PlanError> {
        let mut sql = format!(
            "select 1 from {} as {}",
            from_sql(&link, &self.q.source, table, path),
            quote_ident(alias)
        );

        let mut conjuncts: Vec<String> = Vec::new();
        if let Link::On(link) = link {
            conjuncts.push(link);
        }
        let mine: Vec<Node> = self
            .filters
            .iter()
            .filter(|(r, _)| r == path)
            .map(|(_, n)| n.clone())
            .collect();
        if !mine.is_empty() {
            // The count query has no laterals, so a filter that asks
            // an embed whether it found anything is answered by an
            // EXISTS over the same child level, its own filters and
            // its own link included.
            let mut tests: Vec<EmbedTest> = Vec::new();
            for item in items {
                let (Item::Embed(e) | Item::Spread(e)) = item else {
                    continue;
                };
                let key = key_of(e).to_string();
                if !mine.iter().any(|n| asks_null(n, &key)) {
                    continue;
                }
                let sub = self.exists_level(table, alias, e, path)?;
                tests.push(EmbedTest {
                    key,
                    present: format!("exists ({sub})"),
                    absent: format!("not exists ({sub})"),
                });
            }
            let rel = self.catalog.relation(table);
            let compiled = where_clause_over(
                &mine,
                Some(alias),
                std::mem::take(&mut self.params),
                rel,
                &tests,
            )?;
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
            let sub = self.exists_level(table, alias, e, path)?;
            conjuncts.push(format!("exists ({sub})"));
        }
        if !conjuncts.is_empty() {
            sql.push_str(" where ");
            sql.push_str(&conjuncts.join(" AND "));
        }
        Ok(sql)
    }

    /// The count query's child level for one embed, the body an EXISTS
    /// takes: linked to its parent, carrying its own filters.
    fn exists_level(
        &mut self,
        table: &str,
        alias: &str,
        e: &Embed,
        path: &[String],
    ) -> Result<String, PlanError> {
        let rel = self
            .catalog
            .resolve(table, &e.relation, e.hint.as_deref())?;
        let child = self.next_alias();
        let junction = rel.via.as_ref().map(|_| self.next_alias());
        let mut child_path = path.to_vec();
        child_path.push(key_of(e).to_string());
        let link = link_sql(&rel, alias, &child, junction.as_deref());
        self.count_level(&rel.table, &child, &e.items, &child_path, link)
    }

    /// One column pick: the expression, its casts, its aggregate,
    /// and the output key PostgREST would use.
    ///
    /// A data representation goes on first and the request's own
    /// cast on top of it, so `label_color::text` is the text of what
    /// the client would have been shown rather than the text of what
    /// the column holds. An aggregate takes the column bare: nothing
    /// sums a json value, and upstream has no case for it either.
    ///
    /// A cast over a json path takes brackets, because `::` binds
    /// tighter than `->>` and `data->>0::int` would otherwise ask
    /// postgres for the key `0::int` and hand back the text it found
    /// there. Upstream writes `CAST( ... AS ... )` around the whole
    /// field for the same reason.
    fn col(&self, alias: &str, rel: Option<&Relation>, c: &Col) -> Result<OutCol, PlanError> {
        let mut expr = match &c.field {
            Some(f) => {
                let json = rel.is_none_or(|r| r.steps_as_json(&f.name));
                let mut e = match rel.filter(|_| c.agg.is_none() && f.path.is_empty()) {
                    Some(rel) => match rel.column(&f.name) {
                        Some(col) => represented(alias, col),
                        None => field_expr(Some(alias), &f.name, &f.path, json),
                    },
                    None => field_expr(Some(alias), &f.name, &f.path, json),
                };
                if let Some(cast) = &f.cast {
                    if !f.path.is_empty() {
                        e = format!("({e})");
                    }
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
            keys: vec![key],
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
        wanted: Wanted,
        cols: &mut Vec<OutCol>,
        laterals: &mut Vec<String>,
        embeds: &mut Vec<Embedded>,
    ) -> Result<(), PlanError> {
        let rel = self
            .catalog
            .resolve(parent_table, &e.relation, e.hint.as_deref())?;

        let child = self.next_alias();
        let junction = rel.via.as_ref().map(|_| self.next_alias());
        let mut child_path = path.to_vec();
        child_path.push(key_of(e).to_string());
        let link = link_sql(&rel, parent_alias, &child, junction.as_deref());
        let child_wanted = match (spread, rel.kind) {
            (false, _) => Wanted::default(),
            (true, Kind::ToMany) => Wanted {
                names: true,
                order: true,
            },
            // A spread of a to one hands its columns straight up, so
            // it owes its own parent whatever its parent was owed.
            (true, Kind::ToOne) => Wanted {
                names: wanted.names,
                order: false,
            },
        };
        let body = self.level(
            &rel.table,
            &child,
            &e.items,
            &child_path,
            link,
            child_wanted,
        )?;

        let name = format!("e_{child}");
        let wrap = quote_ident(&name);
        let inner = e.join == Some(Join::Inner);
        let key = quote_ident(key_of(e));
        embeds.push(Embedded {
            key: key_of(e).to_string(),
            alias: name,
            kind: rel.kind,
            table: rel.table.clone(),
        });
        if spread {
            // A spread of a to many has no one row to merge in, so
            // each column of the child comes back as the list of that
            // column over every child row, ordered the way the embed
            // asked to be. The row aggregate rides along beside them
            // to answer whether the embed found anything at all.
            if rel.kind == Kind::ToMany {
                let sub = quote_ident(&format!("s_{child}"));
                let order = match body.order.is_empty() {
                    true => String::new(),
                    false => format!(
                        " order by {}",
                        body.order
                            .iter()
                            .map(|t| format!("{sub}.{t}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                };
                let mut picks = vec![format!("json_agg({sub})::jsonb as \"j\"")];
                for k in &body.keys {
                    let col = quote_ident(k);
                    picks.push(format!(
                        "coalesce(json_agg({sub}.{col}{order}), '[]')::jsonb as {col}"
                    ));
                    cols.push(OutCol {
                        expr: format!("{wrap}.{col}"),
                        rendered: format!("{wrap}.{col} as {col}"),
                        aggregated: false,
                        splat: false,
                        keys: vec![k.clone()],
                    });
                }
                let lateral = format!(
                    "(select {} from ({}) as {sub}) as {wrap}",
                    picks.join(", "),
                    body.sql
                );
                if inner {
                    laterals.push(format!(
                        "join lateral {lateral} on {wrap}.\"j\" is not null"
                    ));
                } else {
                    laterals.push(format!("left join lateral {lateral} on true"));
                }
                return Ok(());
            }
            let joiner = if inner { "join" } else { "left join" };
            laterals.push(format!("{joiner} lateral ({}) as {wrap} on true", body.sql));
            if !e.items.is_empty() {
                cols.push(OutCol {
                    expr: format!("{wrap}.*"),
                    rendered: format!("{wrap}.*"),
                    aggregated: false,
                    splat: true,
                    keys: body.keys,
                });
            }
            return Ok(());
        }

        match rel.kind {
            Kind::ToMany => {
                let row = quote_ident(&format!("r_{child}"));
                let lateral = format!(
                    "(select jsonb_agg(to_jsonb({row})) as \"j\" from ({}) as {row}) as {wrap}",
                    body.sql
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
                        keys: vec![key_of(e).to_string()],
                    });
                }
            }
            // The lateral carries the child's columns rather than the
            // json it becomes, so the level around it can order by one
            // of them and can ask whether the whole row is there. The
            // json is built on the way out instead.
            Kind::ToOne => {
                let joiner = if inner { "join" } else { "left join" };
                laterals.push(format!("{joiner} lateral ({}) as {wrap} on true", body.sql));
                if !e.items.is_empty() {
                    let expr = format!("to_jsonb({wrap})");
                    cols.push(OutCol {
                        rendered: format!("{expr} as {key}"),
                        expr,
                        aggregated: false,
                        splat: false,
                        keys: vec![key_of(e).to_string()],
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

/// What a level reads from: its table, the source relation when the
/// query is a representation over a mutation or an rpc CTE, or the
/// call of a computed relationship. Only the root reads a source, the
/// embeds under it keep reading their real tables.
fn from_sql(link: &Link, source: &Option<String>, table: &str, path: &[String]) -> String {
    match (link, source) {
        (Link::Call(call), _) => call.clone(),
        (_, Some(s)) if path.is_empty() => quote_ident(s),
        _ => quote_ident(table),
    }
}

/// How a child level reaches its parent: the call of a computed
/// relationship, or else a join condition, straight column pairs or
/// through the junction of a many to many so the child level still
/// reads from exactly one table.
///
/// The parent row goes into the call cast to the relation the
/// function takes. That cast is what picks one of two functions of
/// the same name apart, and it is also what lets the row of a
/// mutation's CTE go in, since a CTE has a row type of its own that
/// no function was declared over.
fn link_sql(rel: &Rel, parent: &str, child: &str, junction: Option<&str>) -> Link {
    if let Some(call) = &rel.call {
        return Link::Call(format!(
            "{}({}::{})",
            quote_ident(&call.function),
            quote_ident(parent),
            quote_ident(&call.arg)
        ));
    }
    Link::On(join_sql(rel, parent, child, junction))
}

/// The join condition itself, once the relationship is known to have
/// one.
fn join_sql(rel: &Rel, parent: &str, child: &str, junction: Option<&str>) -> String {
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

/// The ORDER BY of one level. A term that names an embedded resource
/// orders by that embed's lateral alias, which is why it only works
/// for a to one: a list has no single value to sort a parent row by,
/// and upstream answers PGRST118 rather than picking one.
fn order_sql(
    catalog: &Catalog,
    table: &str,
    alias: &str,
    terms: &[Term],
    embeds: &[Embedded],
) -> Result<String, PlanError> {
    Ok(order_terms(catalog, table, alias, terms, embeds)?
        .into_iter()
        .map(|(expr, dir)| format!("{expr}{dir}"))
        .collect::<Vec<_>>()
        .join(", "))
}

/// Each order term as what it sorts and how, kept apart because a
/// spread of a to many selects the what under a name of its own and
/// carries only the how into the aggregate.
fn order_terms(
    catalog: &Catalog,
    table: &str,
    alias: &str,
    terms: &[Term],
    embeds: &[Embedded],
) -> Result<Vec<(String, String)>, PlanError> {
    let mut out = Vec::with_capacity(terms.len());
    for t in terms {
        let mut owner = table;
        let qualifier = match &t.relation {
            None => alias,
            Some(name) => {
                let e = embeds
                    .iter()
                    .find(|e| &e.key == name)
                    .ok_or_else(|| not_embedded(name))?;
                if e.kind == Kind::ToMany {
                    return Err(unordered_relationship(table, name).into());
                }
                owner = &e.table;
                &e.alias
            }
        };
        let mut dir = String::new();
        match t.direction {
            Some(Direction::Asc) => dir.push_str(" asc"),
            Some(Direction::Desc) => dir.push_str(" desc"),
            None => {}
        }
        match t.nulls {
            Some(Nulls::First) => dir.push_str(" nulls first"),
            Some(Nulls::Last) => dir.push_str(" nulls last"),
            None => {}
        }
        let to_json = catalog
            .relation(owner)
            .is_none_or(|r| r.steps_as_json(&t.name));
        out.push((field_expr(Some(qualifier), &t.name, &t.path, to_json), dir));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnRow, ComputedRow, FkRow};
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
                base_type: "test.color".into(),
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
        let jsonb = |table: &str, name: &str| ColumnRow {
            table: table.into(),
            column: Column {
                name: name.into(),
                type_name: "jsonb".into(),
                base_type: "jsonb".into(),
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
                jsonb("todos", "data"),
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
            painted_text("todos", "id,label_color").contains(
                r#""todos"."id" as "id", test.json("todos"."label_color") as "label_color""#
            ),
            "{}",
            painted_text("todos", "id,label_color")
        );
        // The request's own cast goes on top of the representation
        // rather than instead of it, so this is the text of what the
        // client would have been shown.
        assert!(
            painted_text("todos", "label_color::text")
                .contains(r#"test.json("todos"."label_color")::text as "label_color""#),
            "{}",
            painted_text("todos", "label_color::text")
        );
    }

    #[test]
    fn a_star_is_spelled_out_only_where_a_cast_needs_the_room() {
        assert_eq!(
            painted_text("todos", "*"),
            r#"select "todos"."id" as "id", test.json("todos"."label_color") as "label_color", "todos"."data" as "data" from "todos" as "todos""#
        );
        // A relation with nothing to represent keeps its star, which
        // is what upstream leaves alone too.
        assert_eq!(
            painted_text("users", "*"),
            r#"select "users".* from "users" as "users""#
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
            painted_text("todos", "label_color.count()")
                .contains(r#"count("todos"."label_color")"#),
            "{}",
            painted_text("todos", "label_color.count()")
        );
        assert!(
            painted_text("todos", "label_color->shade")
                .contains(r#"to_jsonb("todos"."label_color")->'shade'"#),
            "{}",
            painted_text("todos", "label_color->shade")
        );
    }

    #[test]
    fn a_cast_over_a_json_path_takes_brackets() {
        // `::` binds tighter than `->>`, so without them the cast
        // lands on the key and the value comes back as text.
        assert!(
            painted_text("todos", "data->>0::int")
                .contains(r#"("todos"."data"->>0)::int as "data""#),
            "{}",
            painted_text("todos", "data->>0::int")
        );
    }

    #[test]
    fn a_filter_reads_its_value_through_the_relation_it_names() {
        let mut q = query("todos", "id");
        filt(&mut q, "label_color", "eq.red");
        let s = plan(&painted(), &q).unwrap_or_else(|e| panic!("{e}"));
        assert!(
            s.text
                .ends_with(r#" where "todos"."label_color" = test.color($1)"#),
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
            r#"select "users"."id" as "id", coalesce("e_z1"."j", '[]'::jsonb) as "orders" from "users" as "users" left join lateral (select jsonb_agg(to_jsonb("r_z1")) as "j" from (select "z1"."id" as "id" from "orders" as "z1" where "z1"."user_id" = "users"."id") as "r_z1") as "e_z1" on true"#
        );
    }

    #[test]
    fn a_to_one_embed_carries_one_jsonb_value() {
        let q = query("orders", "id,users(id)");
        assert_eq!(
            text(&q),
            r#"select "orders"."id" as "id", to_jsonb("e_z1") as "users" from "orders" as "orders" left join lateral (select "z1"."id" as "id" from "users" as "z1" where "z1"."id" = "orders"."user_id") as "e_z1" on true"#
        );
    }

    /// The shop with three functions over it: one listing a user's
    /// orders, one naming an order's user, and one called `users`,
    /// which is the name the foreign key between them already
    /// answered to.
    fn computed() -> Catalog {
        let row = |function: &str, table: &str, ftable: &str, single: bool| ComputedRow {
            function: function.into(),
            table: table.into(),
            ftable: ftable.into(),
            single,
        };
        shop().with_computed(vec![
            row("recent_orders", "users", "orders", false),
            row("buyer", "orders", "users", true),
            row("users", "orders", "users", true),
        ])
    }

    fn computed_text(table: &str, sel: &str) -> String {
        let q = query(table, sel);
        plan(&computed(), &q).unwrap_or_else(|e| panic!("{e}")).text
    }

    #[test]
    fn a_computed_embed_calls_the_function_on_the_parent_row() {
        // No join condition anywhere: the argument is the join, and
        // the cast on it is what tells two functions of one name
        // apart.
        assert_eq!(
            computed_text("users", "id,recent_orders(id)"),
            r#"select "users"."id" as "id", coalesce("e_z1"."j", '[]'::jsonb) as "recent_orders" from "users" as "users" left join lateral (select jsonb_agg(to_jsonb("r_z1")) as "j" from (select "z1"."id" as "id" from "recent_orders"("users"::"users") as "z1") as "r_z1") as "e_z1" on true"#
        );
    }

    #[test]
    fn a_function_that_gives_back_one_row_is_a_to_one() {
        assert_eq!(
            computed_text("orders", "id,buyer(id)"),
            r#"select "orders"."id" as "id", to_jsonb("e_z1") as "buyer" from "orders" as "orders" left join lateral (select "z1"."id" as "id" from "buyer"("orders"::"orders") as "z1") as "e_z1" on true"#
        );
    }

    #[test]
    fn a_function_named_after_a_table_takes_the_embed_over() {
        // The foreign key is still there and `users` still reaches
        // the users table, but through the function now.
        assert!(
            computed_text("orders", "users(id)")
                .contains(r#"from "users"("orders"::"orders") as "z1""#),
            "{}",
            computed_text("orders", "users(id)")
        );
        // And the key it replaced is gone rather than left as a
        // second answer, so neither of the key's other spellings
        // reaches it either.
        let q = query("orders", "orders_user_id_fkey(id)");
        assert!(matches!(plan(&computed(), &q), Err(PlanError::Embed(_))));
    }

    #[test]
    fn the_count_query_calls_it_too() {
        let q = query("users", "id,recent_orders!inner(id)");
        assert_eq!(
            count(&computed(), &q).unwrap().text,
            r#"select 1 from "users" as "users" where exists (select 1 from "recent_orders"("users"::"users") as "z1")"#
        );
    }

    #[test]
    fn inner_join_and_the_routed_filter() {
        let mut q = query("users", "id,orders!inner(id)");
        filt(&mut q, "orders.total", "gte.100");
        let sql = plan(&shop(), &q).unwrap();
        assert_eq!(
            sql.text,
            r#"select "users"."id" as "id", coalesce("e_z1"."j", '[]'::jsonb) as "orders" from "users" as "users" join lateral (select jsonb_agg(to_jsonb("r_z1")) as "j" from (select "z1"."id" as "id" from "orders" as "z1" where "z1"."user_id" = "users"."id" AND "z1"."total" >= $1) as "r_z1") as "e_z1" on "e_z1"."j" is not null"#
        );
        assert_eq!(sql.params, vec!["100"]);
    }

    #[test]
    fn many_to_many_walks_the_junction_in_a_subquery() {
        let q = query("orders", "id,products(id)");
        let t = text(&q);
        assert!(
            t.contains(
                r#"where ("z1"."id") in (select "z2"."product_id" from "order_items" as "z2" where "z2"."order_id" = "orders"."id")"#
            ),
            "{t}"
        );
    }

    #[test]
    fn a_spread_merges_the_columns() {
        let q = query("orders", "id,...users(id)");
        assert_eq!(
            text(&q),
            r#"select "orders"."id" as "id", "e_z1".* from "orders" as "orders" left join lateral (select "z1"."id" as "id" from "users" as "z1" where "z1"."id" = "orders"."user_id") as "e_z1" on true"#
        );
    }

    /// Spreading a list gives the parent one column per column of the
    /// child, each holding the list of that column, and the order the
    /// embed asked for rides inside the aggregate rather than being
    /// applied to rows that are about to be folded up.
    #[test]
    fn a_spread_of_a_list_merges_a_list_per_column() {
        let q = query("users", "id,...orders(total)");
        assert_eq!(
            text(&q),
            r#"select "users"."id" as "id", "e_z1"."total" as "total" from "users" as "users" left join lateral (select json_agg("s_z1")::jsonb as "j", coalesce(json_agg("s_z1"."total"), '[]')::jsonb as "total" from (select "z1"."total" as "total" from "orders" as "z1" where "z1"."user_id" = "users"."id") as "s_z1") as "e_z1" on true"#
        );

        let mut q = query("users", "id,...orders(total)");
        q.order
            .push((vec!["orders".into()], order::parse("id.desc").unwrap()));
        let t = text(&q);
        assert!(t.contains(r#""z1"."id" as "z1_o1" from "orders""#), "{t}");
        assert!(
            t.contains(r#"json_agg("s_z1"."total" order by "s_z1"."z1_o1" desc)"#),
            "{t}"
        );
        assert!(!t.contains("order by \"z1\""), "{t}");
    }

    /// An aggregate inside a spread of a list is upstream's own gap,
    /// and answering it with the same code is the compatible answer.
    #[test]
    fn an_aggregate_cannot_ride_inside_a_spread_of_a_list() {
        let q = query("users", "id,...orders(total.sum())");
        let PlanError::Embed(e) = fails(&q) else {
            panic!("expected an embed error");
        };
        assert_eq!(e.code, "PGRST127");
        assert_eq!(e.message, "Feature not implemented");
        assert_eq!(
            e.details.as_ref().and_then(Details::text),
            Some("Aggregates are not implemented for one-to-many or many-to-many spreads.")
        );
    }

    #[test]
    fn nested_order_and_limit_land_in_the_subquery() {
        let mut q = query("users", "id,orders(id)");
        q.order
            .push((vec!["orders".into()], order::parse("total.desc").unwrap()));
        q.limit.push((vec!["orders".into()], 2));
        let t = text(&q);
        assert!(
            t.contains(r#""users"."id" order by "z1"."total" desc limit 2) as "r_z1""#),
            "{t}"
        );
    }

    #[test]
    fn aggregates_group_by_the_plain_columns() {
        let q = query("orders", "user_id,total.sum()");
        assert_eq!(
            text(&q),
            r#"select "orders"."user_id" as "user_id", sum("orders"."total") as "sum" from "orders" as "orders" group by "orders"."user_id""#
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
    fn an_order_term_can_name_a_to_one_embed() {
        let mut q = query("orders", "id,users(name)");
        q.order
            .push((Vec::new(), order::parse("users(name).desc").unwrap()));
        let t = text(&q);
        assert!(t.ends_with(r#"order by "e_z1"."name" desc"#), "{t}");

        // An empty embed still exposes its columns, which is how
        // ordering by something the client never asked to see works.
        let mut q = query("orders", "id,users()");
        q.order
            .push((Vec::new(), order::parse("users(name)").unwrap()));
        let t = text(&q);
        assert!(t.contains(r#"select "z1".* from "users""#), "{t}");
        assert!(t.ends_with(r#"order by "e_z1"."name""#), "{t}");

        // The alias is the name, when there is one.
        let mut q = query("orders", "id,who:users(name)");
        q.order
            .push((Vec::new(), order::parse("who(name)").unwrap()));
        assert!(text(&q).ends_with(r#"order by "e_z1"."name""#));
    }

    #[test]
    fn a_related_order_needs_a_to_one_that_is_embedded() {
        let mut q = query("users", "id,orders(id)");
        q.order
            .push((Vec::new(), order::parse("orders(id)").unwrap()));
        let PlanError::Embed(e) = fails(&q) else {
            panic!("not an embed error")
        };
        assert_eq!(e.code, "PGRST118");
        assert_eq!(e.message, "A related order on 'orders' is not possible");
        assert_eq!(
            e.details.as_ref().and_then(Details::text),
            Some("'users' and 'orders' do not form a many-to-one or one-to-one relationship")
        );

        let mut q = query("orders", "id,users(name)");
        q.order
            .push((Vec::new(), order::parse("usersx(name)").unwrap()));
        let PlanError::Embed(e) = fails(&q) else {
            panic!("not an embed error")
        };
        assert_eq!(e.code, "PGRST108");
        assert_eq!(
            e.message,
            "'usersx' is not an embedded resource in this request"
        );
    }

    #[test]
    fn a_filter_can_ask_whether_an_embed_found_anything() {
        // A to one is the whole row of its lateral.
        let mut q = query("orders", "id,users()");
        filt(&mut q, "users", "not.is.null");
        let t = text(&q);
        assert!(t.contains(r#"where "e_z1" is distinct from null"#), "{t}");

        let mut q = query("orders", "id,users()");
        filt(&mut q, "users", "is.null");
        assert!(text(&q).contains(r#"where "e_z1" is not distinct from null"#));

        // A to many is the aggregate, which is null when jsonb_agg
        // saw no rows at all.
        let mut q = query("users", "id,orders()");
        filt(&mut q, "orders", "not.is.null");
        assert!(text(&q).contains(r#"where "e_z1"."j" is not null"#));

        // Every other operator reads the name as a column.
        let mut q = query("orders", "id,users()");
        filt(&mut q, "users", "eq.2");
        assert!(text(&q).contains(r#""orders"."users" = $1"#));
    }

    #[test]
    fn the_count_query_answers_an_embed_test_with_an_exists() {
        let mut q = query("users", "id,orders()");
        filt(&mut q, "orders", "is.null");
        filt(&mut q, "orders.total", "gte.100");
        let sql = count(&shop(), &q).unwrap();
        assert_eq!(
            sql.text,
            r#"select 1 from "users" as "users" where not exists (select 1 from "orders" as "z1" where "z1"."user_id" = "users"."id" AND "z1"."total" >= $1)"#
        );
        assert_eq!(sql.params, vec!["100"]);
    }

    #[test]
    fn an_empty_inner_embed_is_pure_existence() {
        let mut q = query("users", "id,orders!inner()");
        filt(&mut q, "orders.total", "gte.100");
        let t = text(&q);
        assert!(t.starts_with(r#"select "users"."id" as "id" from"#), "{t}");
        assert!(t.contains(r#"select "z1".* from "orders""#), "{t}");
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
            r#"select 1 from "users" as "users" where "users"."id" >= $1"#
        );
        assert_eq!(sql.params, vec!["5"]);

        // An inner embed folds to EXISTS and carries its filter.
        let mut q = query("users", "id,orders!inner(id)");
        filt(&mut q, "orders.total", "gte.100");
        let sql = count(&shop(), &q).unwrap();
        assert_eq!(
            sql.text,
            r#"select 1 from "users" as "users" where exists (select 1 from "orders" as "z1" where "z1"."user_id" = "users"."id" AND "z1"."total" >= $1)"#
        );
        assert_eq!(sql.params, vec!["100"]);

        // A filter on the left joined embed stays out of the count.
        let mut q = query("users", "id,orders(id)");
        filt(&mut q, "orders.total", "gte.100");
        let sql = count(&shop(), &q).unwrap();
        assert_eq!(sql.text, r#"select 1 from "users" as "users""#);
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
