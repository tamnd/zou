//! What a database's schema is, read out of the catalog, and what it
//! would take to turn one into another.
//!
//! This is the half of `zou db diff` that does not care where either
//! database came from. One of them is the project's, the other is a
//! throwaway with the migrations replayed into it, and the difference
//! between the two is the migration nobody has written yet.
//!
//! Everything here is keyed by the name a person would use, not by an
//! oid, because the two databases are different databases and an oid
//! in one means nothing in the other. Definitions come back from
//! postgres's own `pg_get_*def` functions rather than being assembled
//! from columns, so the comparison is between two things postgres
//! wrote and a difference is a real difference rather than a
//! disagreement about how to spell one.
//!
//! What it does not compare is listed in [`UNREAD`] and printed, so a
//! diff that comes back empty says what it looked at.

use std::collections::{BTreeMap, BTreeSet};

use tokio_postgres::Client;

/// Object kinds this does not look at yet. Printed with every diff,
/// because "no changes" from a tool that never looked is worse than
/// no tool.
pub const UNREAD: &[&str] = &[
    "default privileges",
    "column privileges",
    "ownership",
    "extensions",
    "publications and subscriptions",
    "event triggers",
    "domains and composite types",
    "sequences that no column owns",
    "foreign tables and foreign data wrappers",
];

const SCHEMAS_SQL: &str = "\
select nspname from pg_namespace where nspname = any($1)";

const ENUMS_SQL: &str = "\
select n.nspname || '.' || t.typname as id, e.enumlabel::text as label
from pg_type t
  join pg_namespace n on n.oid = t.typnamespace
  join pg_enum e on e.enumtypid = t.oid
where n.nspname = any($1)
order by n.nspname, t.typname, e.enumsortorder";

const TABLES_SQL: &str = "\
select n.nspname as schema, c.relname as name, c.relkind::text as kind, c.relrowsecurity as rls
from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
where n.nspname = any($1) and c.relkind in ('r', 'p', 'S')";

const COLUMNS_SQL: &str = "\
select
  n.nspname as schema,
  c.relname as \"table\",
  a.attname as name,
  a.attnum::int4 as position,
  format_type(a.atttypid, a.atttypmod) as type,
  a.attnotnull as not_null,
  coalesce(pg_get_expr(d.adbin, d.adrelid), '') as \"default\",
  a.attidentity::text as identity,
  a.attgenerated::text as generated
from pg_attribute a
  join pg_class c on c.oid = a.attrelid
  join pg_namespace n on n.oid = c.relnamespace
  left join pg_attrdef d on d.adrelid = c.oid and d.adnum = a.attnum
where n.nspname = any($1)
  and c.relkind in ('r', 'p')
  and a.attnum > 0
  and not a.attisdropped
order by n.nspname, c.relname, a.attnum";

/// Not null arrives twice in postgres 18: once as the column's
/// `attnotnull`, which is where a person put it, and once as a
/// constraint of its own in `pg_constraint`. Writing it from here as
/// well would put a second, differently spelled copy of the same thing
/// in every migration this generates.
const CONSTRAINTS_SQL: &str = "\
select n.nspname as schema, c.relname as \"table\", con.conname as name,
       pg_get_constraintdef(con.oid) as def
from pg_constraint con
  join pg_class c on c.oid = con.conrelid
  join pg_namespace n on n.oid = c.relnamespace
where n.nspname = any($1) and con.contype <> 'n'";

/// Indexes that no constraint owns. The ones a primary key or a unique
/// constraint brought with them arrive as constraints instead, and
/// writing both would be writing the same index twice.
const INDEXES_SQL: &str = "\
select n.nspname as schema, ci.relname as name, pg_get_indexdef(i.indexrelid) as def
from pg_index i
  join pg_class ci on ci.oid = i.indexrelid
  join pg_class c on c.oid = i.indrelid
  join pg_namespace n on n.oid = c.relnamespace
where n.nspname = any($1)
  and not exists (select 1 from pg_constraint con where con.conindid = i.indexrelid)";

const VIEWS_SQL: &str = "\
select n.nspname as schema, c.relname as name, c.relkind::text as kind,
       pg_get_viewdef(c.oid, true) as def
from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
where n.nspname = any($1) and c.relkind in ('v', 'm')";

/// Which view reads which view, so the drops go deepest first and the
/// creates go the other way.
const VIEW_DEPS_SQL: &str = "\
select distinct
  dn.nspname || '.' || dc.relname as dependent,
  sn.nspname || '.' || sc.relname as source
from pg_depend d
  join pg_rewrite r on r.oid = d.objid
  join pg_class dc on dc.oid = r.ev_class
  join pg_namespace dn on dn.oid = dc.relnamespace
  join pg_class sc on sc.oid = d.refobjid
  join pg_namespace sn on sn.oid = sc.relnamespace
where d.classid = 'pg_rewrite'::regclass
  and d.refclassid = 'pg_class'::regclass
  and dc.relkind in ('v', 'm')
  and sc.relkind in ('v', 'm')
  and dc.oid <> sc.oid
  and dn.nspname = any($1)
  and sn.nspname = any($1)";

const FUNCTIONS_SQL: &str = "\
select n.nspname as schema,
       p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')' as name,
       pg_get_functiondef(p.oid) as def
from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
where n.nspname = any($1) and p.prokind in ('f', 'p')";

const TRIGGERS_SQL: &str = "\
select n.nspname as schema, c.relname as \"table\", t.tgname as name,
       pg_get_triggerdef(t.oid, true) as def
from pg_trigger t
  join pg_class c on c.oid = t.tgrelid
  join pg_namespace n on n.oid = c.relnamespace
where n.nspname = any($1) and not t.tgisinternal";

/// Role 0 in `polroles` is PUBLIC, which `pg_get_userbyid` has no name
/// for, so it is spelled here.
const POLICIES_SQL: &str = "\
select n.nspname as schema, c.relname as \"table\", pol.polname as name,
       pol.polcmd::text as command,
       pol.polpermissive as permissive,
       (select coalesce(string_agg(
                 case when r = 0 then 'public' else quote_ident(pg_get_userbyid(r)) end, ', '
                 order by r), 'public')
        from unnest(pol.polroles) r) as roles,
       coalesce(pg_get_expr(pol.polqual, pol.polrelid), '') as \"using\",
       coalesce(pg_get_expr(pol.polwithcheck, pol.polrelid), '') as \"check\"
from pg_policy pol
  join pg_class c on c.oid = pol.polrelid
  join pg_namespace n on n.oid = c.relnamespace
where n.nspname = any($1)";

const TABLE_GRANTS_SQL: &str = "\
select n.nspname as schema, c.relname as name,
       case when a.grantee = 0 then 'public' else quote_ident(pg_get_userbyid(a.grantee)) end
         as grantee,
       a.privilege_type as privilege
from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  cross join lateral aclexplode(c.relacl) a
where n.nspname = any($1)
  and c.relkind in ('r', 'p', 'v', 'm', 'S')
  and a.grantee <> c.relowner";

const SCHEMA_GRANTS_SQL: &str = "\
select n.nspname as schema,
       case when a.grantee = 0 then 'public' else quote_ident(pg_get_userbyid(a.grantee)) end
         as grantee,
       a.privilege_type as privilege
from pg_namespace n
  cross join lateral aclexplode(n.nspacl) a
where n.nspname = any($1) and a.grantee <> n.nspowner";

const COMMENTS_SQL: &str = "\
select n.nspname as schema, c.relname as name, '' as column, d.description
from pg_description d
  join pg_class c on c.oid = d.objoid and d.classoid = 'pg_class'::regclass and d.objsubid = 0
  join pg_namespace n on n.oid = c.relnamespace
where n.nspname = any($1)
union all
select n.nspname, c.relname, a.attname, d.description
from pg_description d
  join pg_class c on c.oid = d.objoid and d.classoid = 'pg_class'::regclass and d.objsubid > 0
  join pg_attribute a on a.attrelid = c.oid and a.attnum = d.objsubid
  join pg_namespace n on n.oid = c.relnamespace
where n.nspname = any($1)";

#[derive(Debug, Default, PartialEq)]
pub struct Column {
    pub position: i32,
    pub kind: String,
    pub not_null: bool,
    pub default: String,
    pub identity: String,
    pub generated: String,
}

#[derive(Debug, Default, PartialEq)]
pub struct Table {
    pub partitioned: bool,
    pub rls: bool,
}

#[derive(Debug, Default, PartialEq)]
pub struct Policy {
    pub command: String,
    pub permissive: bool,
    pub roles: String,
    pub using: String,
    pub check: String,
}

#[derive(Debug, Default, PartialEq)]
pub struct View {
    pub materialized: bool,
    pub def: String,
}

/// Something named inside a table: the qualified table and the name,
/// kept apart rather than joined with a dot, because a constraint
/// called `a.b` is legal and a key that ran them together could not be
/// taken back apart.
type Named = (String, String);

/// A database's schema, as far as this compares them. Every map is
/// keyed by the name a person would write, so two catalogs from two
/// databases line up on the keys and the values are what differ.
#[derive(Debug, Default)]
pub struct Catalog {
    pub schemas: BTreeSet<String>,
    /// `schema.type` to its labels in sort order.
    pub enums: BTreeMap<String, Vec<String>>,
    /// `schema.table`.
    pub tables: BTreeMap<String, Table>,
    /// `schema.sequence`, which nothing here writes and grants ask
    /// about.
    pub sequences: BTreeSet<String>,
    /// `schema.table` to its columns by name.
    pub columns: BTreeMap<String, BTreeMap<String, Column>>,
    /// Table and constraint name to its definition.
    pub constraints: BTreeMap<Named, String>,
    /// `schema.index` to its definition.
    pub indexes: BTreeMap<String, String>,
    /// `schema.view`.
    pub views: BTreeMap<String, View>,
    /// Which view reads which, dependent first.
    pub view_deps: Vec<(String, String)>,
    /// `schema.name(argument types)` to its definition.
    pub functions: BTreeMap<String, String>,
    /// Table and trigger name to its definition.
    pub triggers: BTreeMap<Named, String>,
    /// Table and policy name.
    pub policies: BTreeMap<Named, Policy>,
    /// `schema.table` or `schema` to the privileges each role holds.
    pub grants: BTreeMap<String, BTreeSet<(String, String)>>,
    /// An object and a column of it, empty for the object itself, to
    /// the comment on it.
    pub comments: BTreeMap<Named, String>,
}

impl Catalog {
    pub async fn read(client: &Client, schemas: &[String]) -> Result<Catalog, String> {
        let mut out = Catalog::default();
        let q = |sql: &'static str| async move {
            client
                .query(sql, &[&schemas])
                .await
                .map_err(|e| format!("reading the catalog: {e}"))
        };

        for row in q(SCHEMAS_SQL).await? {
            out.schemas.insert(row.get("nspname"));
        }
        for row in q(ENUMS_SQL).await? {
            out.enums
                .entry(row.get("id"))
                .or_default()
                .push(row.get("label"));
        }
        for row in q(TABLES_SQL).await? {
            let id = qualified(
                &row.get::<_, String>("schema"),
                &row.get::<_, String>("name"),
            );
            let kind: String = row.get("kind");
            if kind == "S" {
                out.sequences.insert(id);
                continue;
            }
            out.tables.insert(
                id.clone(),
                Table {
                    partitioned: kind == "p",
                    rls: row.get("rls"),
                },
            );
            out.columns.entry(id).or_default();
        }
        for row in q(COLUMNS_SQL).await? {
            let id = qualified(
                &row.get::<_, String>("schema"),
                &row.get::<_, String>("table"),
            );
            out.columns.entry(id).or_default().insert(
                row.get("name"),
                Column {
                    position: row.get("position"),
                    kind: row.get("type"),
                    not_null: row.get("not_null"),
                    default: row.get("default"),
                    identity: row.get("identity"),
                    generated: row.get("generated"),
                },
            );
        }
        for row in q(CONSTRAINTS_SQL).await? {
            out.constraints.insert(named(&row), row.get("def"));
        }
        for row in q(INDEXES_SQL).await? {
            let id = qualified(
                &row.get::<_, String>("schema"),
                &row.get::<_, String>("name"),
            );
            out.indexes.insert(id, row.get("def"));
        }
        for row in q(VIEWS_SQL).await? {
            let id = qualified(
                &row.get::<_, String>("schema"),
                &row.get::<_, String>("name"),
            );
            out.views.insert(
                id,
                View {
                    materialized: row.get::<_, String>("kind") == "m",
                    def: row.get("def"),
                },
            );
        }
        for row in q(VIEW_DEPS_SQL).await? {
            out.view_deps
                .push((row.get("dependent"), row.get("source")));
        }
        for row in q(FUNCTIONS_SQL).await? {
            let id = qualified(
                &row.get::<_, String>("schema"),
                &row.get::<_, String>("name"),
            );
            out.functions.insert(id, row.get("def"));
        }
        for row in q(TRIGGERS_SQL).await? {
            out.triggers.insert(named(&row), row.get("def"));
        }
        for row in q(POLICIES_SQL).await? {
            out.policies.insert(
                named(&row),
                Policy {
                    command: row.get("command"),
                    permissive: row.get("permissive"),
                    roles: row.get("roles"),
                    using: row.get("using"),
                    check: row.get("check"),
                },
            );
        }
        for row in q(TABLE_GRANTS_SQL).await? {
            let id = qualified(
                &row.get::<_, String>("schema"),
                &row.get::<_, String>("name"),
            );
            out.grants
                .entry(id)
                .or_default()
                .insert((row.get("grantee"), row.get("privilege")));
        }
        for row in q(SCHEMA_GRANTS_SQL).await? {
            out.grants
                .entry(row.get::<_, String>("schema"))
                .or_default()
                .insert((row.get("grantee"), row.get("privilege")));
        }
        for row in q(COMMENTS_SQL).await? {
            let id = qualified(
                &row.get::<_, String>("schema"),
                &row.get::<_, String>("name"),
            );
            let column: String = row.get("column");
            let column = if column.is_empty() {
                String::new()
            } else {
                ident(&column)
            };
            out.comments.insert((id, column), row.get("description"));
        }
        Ok(out)
    }

    /// Forget every grant these roles hold, so a comparison says
    /// nothing about them either way.
    ///
    /// A database the server has never answered from does not have the
    /// api roles yet, because the server creates them on its first
    /// request. The shadow always has them, so without this every
    /// grant the bootstrap would eventually make reads as a change the
    /// project made, which is the opposite of the truth.
    pub fn forget_grants_to(&mut self, roles: &[&str]) {
        for held in self.grants.values_mut() {
            held.retain(|(role, _)| !roles.contains(&role.as_str()));
        }
    }
}

/// A qualified name with each half quoted only where it has to be, so
/// the keys read like the sql a person would write.
fn qualified(schema: &str, name: &str) -> String {
    format!("{}.{}", ident(schema), ident(name))
}

/// The table a row is about and what the row calls itself.
fn named(row: &tokio_postgres::Row) -> Named {
    (
        qualified(
            &row.get::<_, String>("schema"),
            &row.get::<_, String>("table"),
        ),
        ident(&row.get::<_, String>("name")),
    )
}

/// A name as sql spells it. Lowercase letters, digits and underscores
/// not starting with a digit go through untouched, everything else is
/// quoted, which is what `quote_ident` does.
pub fn ident(name: &str) -> String {
    let plain = !name.is_empty()
        && !name.starts_with(|c: char| c.is_ascii_digit())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !RESERVED.contains(&name);
    if plain {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// Words a bare name cannot be. Not the whole list postgres has, the
/// ones a table or a column is actually called by somebody who has not
/// hit the error yet.
const RESERVED: &[&str] = &[
    "all",
    "and",
    "any",
    "as",
    "asc",
    "case",
    "check",
    "column",
    "constraint",
    "create",
    "default",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "false",
    "for",
    "from",
    "grant",
    "group",
    "having",
    "in",
    "index",
    "into",
    "join",
    "limit",
    "not",
    "null",
    "offset",
    "on",
    "or",
    "order",
    "primary",
    "references",
    "select",
    "table",
    "then",
    "to",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "when",
    "where",
    "with",
];

/// A string as a sql literal.
fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// The statements that turn `from` into `to`. `from` is the shadow,
/// which has the migrations in it, and `to` is the database somebody
/// has been changing, so what comes back is the migration that was
/// never written.
pub fn diff(from: &Catalog, to: &Catalog) -> Vec<String> {
    let mut out = Vec::new();
    schemas(from, to, &mut out);
    enums(from, to, &mut out);
    // Everything that stands on a table comes off before the table
    // underneath it is touched, and goes back on after.
    drops(from, to, &mut out);
    tables(from, to, &mut out);
    creates(from, to, &mut out);
    rls(from, to, &mut out);
    grants(from, to, &mut out);
    comments(from, to, &mut out);
    out
}

fn schemas(from: &Catalog, to: &Catalog, out: &mut Vec<String>) {
    for name in to.schemas.difference(&from.schemas) {
        out.push(format!("create schema {};", ident(name)));
    }
    for name in from.schemas.difference(&to.schemas) {
        out.push(format!("drop schema {};", ident(name)));
    }
}

fn enums(from: &Catalog, to: &Catalog, out: &mut Vec<String>) {
    for (id, labels) in &to.enums {
        match from.enums.get(id) {
            None => out.push(format!(
                "create type {id} as enum ({});",
                labels
                    .iter()
                    .map(|l| literal(l))
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            Some(had) if had == labels => {}
            // An enum that only grew takes the new labels in place. One
            // that lost or reordered them cannot be altered, and
            // dropping a type a column uses would take the column with
            // it, so that is said rather than done.
            Some(had) if labels.starts_with(had) => {
                for label in &labels[had.len()..] {
                    out.push(format!("alter type {id} add value {};", literal(label)));
                }
            }
            Some(had) => out.push(format!(
                "-- the labels of {id} changed from ({}) to ({}), \
                 which no alter can do, so write it by hand",
                had.join(", "),
                labels.join(", ")
            )),
        }
    }
    for id in from.enums.keys() {
        if !to.enums.contains_key(id) {
            out.push(format!("drop type {id};"));
        }
    }
}

/// Everything that has to come off before a table can change: the
/// policies, triggers, views, functions, indexes and constraints that
/// are gone or that changed. A changed one is dropped here and made
/// again in `creates`, which is what postgres would make you do.
fn drops(from: &Catalog, to: &Catalog, out: &mut Vec<String>) {
    for (id, policy) in &from.policies {
        let (table, name) = id;
        if to.policies.get(id) != Some(policy) && to.tables.contains_key(table) {
            out.push(format!("drop policy {name} on {table};"));
        }
    }
    for (id, def) in &from.triggers {
        let (table, name) = id;
        if to.triggers.get(id) != Some(def) && to.tables.contains_key(table) {
            out.push(format!("drop trigger {name} on {table};"));
        }
    }
    for id in ordered_views(from, Deepest::First, |id| {
        from.views.get(id) != to.views.get(id)
    }) {
        let view = &from.views[&id];
        let kind = if view.materialized {
            "materialized view"
        } else {
            "view"
        };
        out.push(format!("drop {kind} {id};"));
    }
    for (id, def) in &from.functions {
        if to.functions.get(id) != Some(def) {
            out.push(format!("drop function {id};"));
        }
    }
    for (id, def) in &from.indexes {
        if to.indexes.get(id) != Some(def) {
            out.push(format!("drop index {id};"));
        }
    }
    for (id, def) in &from.constraints {
        let (table, name) = id;
        // The table may be going away underneath it, and then the
        // constraint goes with it.
        if to.constraints.get(id) != Some(def) && to.tables.contains_key(table) {
            out.push(format!("alter table {table} drop constraint {name};"));
        }
    }
}

/// The tables themselves: the new ones whole, the gone ones dropped,
/// and the ones that kept their name column by column.
fn tables(from: &Catalog, to: &Catalog, out: &mut Vec<String>) {
    for (id, table) in &to.tables {
        let Some(columns) = to.columns.get(id) else {
            continue;
        };
        if !from.tables.contains_key(id) {
            let mut lines: Vec<(i32, String)> = columns
                .iter()
                .map(|(name, c)| (c.position, format!("    {} {}", ident(name), column(c))))
                .collect();
            lines.sort();
            out.push(format!(
                "create table {id} (\n{}\n);",
                lines
                    .into_iter()
                    .map(|(_, line)| line)
                    .collect::<Vec<_>>()
                    .join(",\n")
            ));
            let _ = table;
            continue;
        }
        let empty = BTreeMap::new();
        let had = from.columns.get(id).unwrap_or(&empty);
        for (name, want) in columns {
            match had.get(name) {
                None => out.push(format!(
                    "alter table {id} add column {} {};",
                    ident(name),
                    column(want)
                )),
                Some(have) if have == want => {}
                Some(have) => alter_column(id, name, have, want, out),
            }
        }
        for name in had.keys() {
            if !columns.contains_key(name) {
                out.push(format!("alter table {id} drop column {};", ident(name)));
            }
        }
    }
    for id in from.tables.keys() {
        if !to.tables.contains_key(id) {
            out.push(format!("drop table {id};"));
        }
    }
}

/// A column as `create table` spells it.
fn column(c: &Column) -> String {
    let mut out = c.kind.clone();
    if !c.generated.is_empty() {
        out.push_str(&format!(" generated always as ({}) stored", c.default));
    } else if !c.identity.is_empty() {
        let when = if c.identity == "a" {
            "always"
        } else {
            "by default"
        };
        out.push_str(&format!(" generated {when} as identity"));
    } else if !c.default.is_empty() {
        out.push_str(&format!(" default {}", c.default));
    }
    if c.not_null {
        out.push_str(" not null");
    }
    out
}

fn alter_column(table: &str, name: &str, have: &Column, want: &Column, out: &mut Vec<String>) {
    let name = ident(name);
    if have.kind != want.kind {
        out.push(format!(
            "alter table {table} alter column {name} set data type {};",
            want.kind
        ));
    }
    if have.default != want.default && want.generated.is_empty() {
        if want.default.is_empty() {
            out.push(format!(
                "alter table {table} alter column {name} drop default;"
            ));
        } else {
            out.push(format!(
                "alter table {table} alter column {name} set default {};",
                want.default
            ));
        }
    }
    if have.not_null != want.not_null {
        let what = if want.not_null { "set" } else { "drop" };
        out.push(format!(
            "alter table {table} alter column {name} {what} not null;"
        ));
    }
    if have.identity != want.identity {
        out.push(format!(
            "-- {table}.{name} changed how it generates its values, \
             from {:?} to {:?}, which is a rewrite rather than an alter",
            have.identity, want.identity
        ));
    }
    if have.generated != want.generated {
        out.push(format!(
            "-- {table}.{name} is generated in one database and not in the other, \
             which no alter can do, so write it by hand"
        ));
    }
}

/// Everything that stands on a table, put back in an order postgres
/// will accept: constraints, then indexes, functions, views, triggers
/// and policies.
fn creates(from: &Catalog, to: &Catalog, out: &mut Vec<String>) {
    for (id, def) in &to.constraints {
        if from.constraints.get(id) != Some(def) {
            let (table, name) = id;
            out.push(format!("alter table {table} add constraint {name} {def};"));
        }
    }
    for (id, def) in &to.indexes {
        if from.indexes.get(id) != Some(def) {
            out.push(format!("{def};"));
        }
    }
    for (id, def) in &to.functions {
        if from.functions.get(id) != Some(def) {
            out.push(format!("{};", def.trim_end().trim_end_matches(';')));
        }
    }
    for id in ordered_views(to, Deepest::Last, |id| {
        from.views.get(id) != to.views.get(id)
    }) {
        let view = &to.views[&id];
        let kind = if view.materialized {
            "materialized view"
        } else {
            "view"
        };
        out.push(format!(
            "create {kind} {id} as\n{};",
            view.def.trim_end().trim_end_matches(';')
        ));
    }
    for (id, def) in &to.triggers {
        if from.triggers.get(id) != Some(def) {
            out.push(format!("{def};"));
        }
    }
    for (id, policy) in &to.policies {
        if from.policies.get(id) != Some(policy) {
            let (table, name) = id;
            let mut stmt = format!("create policy {name} on {table}");
            if !policy.permissive {
                stmt.push_str(" as restrictive");
            }
            stmt.push_str(&format!(
                " for {} to {}",
                command(&policy.command),
                policy.roles
            ));
            if !policy.using.is_empty() {
                stmt.push_str(&format!(" using ({})", policy.using));
            }
            if !policy.check.is_empty() {
                stmt.push_str(&format!(" with check ({})", policy.check));
            }
            out.push(format!("{stmt};"));
        }
    }
}

/// What `polcmd` means in words.
fn command(cmd: &str) -> &'static str {
    match cmd {
        "r" => "select",
        "a" => "insert",
        "w" => "update",
        "d" => "delete",
        _ => "all",
    }
}

fn rls(from: &Catalog, to: &Catalog, out: &mut Vec<String>) {
    for (id, table) in &to.tables {
        let had = from.tables.get(id).map(|t| t.rls);
        // A table that is new arrives without row level security, so
        // the statement is only skipped when the answer already
        // matches.
        if had.unwrap_or(false) != table.rls {
            let what = if table.rls { "enable" } else { "disable" };
            out.push(format!("alter table {id} {what} row level security;"));
        }
    }
}

/// Privileges on the objects both databases have.
///
/// An object one of them is missing is left out on purpose: it is being
/// created or dropped a few statements up, and a created one arrives
/// with whatever the default privileges say, which for a Supabase
/// project is a grant to all three api roles on everything. Writing
/// those out would bury the one line somebody actually changed under a
/// page of grants that were going to happen anyway.
fn grants(from: &Catalog, to: &Catalog, out: &mut Vec<String>) {
    let empty = BTreeSet::new();
    for (id, want) in &to.grants {
        if !both(from, to, id) {
            continue;
        }
        let have = from.grants.get(id).unwrap_or(&empty);
        for (role, privilege) in want.difference(have) {
            out.push(grant("grant", "to", id, to, role, privilege));
        }
    }
    for (id, have) in &from.grants {
        if !both(from, to, id) {
            continue;
        }
        let want = to.grants.get(id).unwrap_or(&empty);
        for (role, privilege) in have.difference(want) {
            out.push(grant("revoke", "from", id, from, role, privilege));
        }
    }
}

/// Whether both databases have the thing this key names.
fn both(from: &Catalog, to: &Catalog, id: &str) -> bool {
    let has = |side: &Catalog| {
        side.tables.contains_key(id)
            || side.views.contains_key(id)
            || side.sequences.contains(id)
            || side.schemas.contains(id)
    };
    has(from) && has(to)
}

/// A grant or a revoke, spelled for whichever kind of thing it is on.
fn grant(verb: &str, way: &str, id: &str, side: &Catalog, role: &str, privilege: &str) -> String {
    let privilege = privilege.to_lowercase();
    let what = if side.sequences.contains(id) {
        "sequence"
    } else if side.schemas.contains(id) {
        "schema"
    } else {
        // Views take the table spelling, which is what postgres calls
        // them here.
        "table"
    };
    format!("{verb} {privilege} on {what} {id} {way} {role};")
}

fn comments(from: &Catalog, to: &Catalog, out: &mut Vec<String>) {
    for (id, text) in &to.comments {
        if from.comments.get(id) != Some(text) {
            out.push(format!("{} is {};", comment_on(id, to), literal(text)));
        }
    }
    for id in from.comments.keys() {
        if !to.comments.contains_key(id) {
            out.push(format!("{} is null;", comment_on(id, from)));
        }
    }
}

/// A comment names the object it is on, and a column comment names the
/// column too, which is the second half of the key.
fn comment_on(id: &Named, side: &Catalog) -> String {
    let (object, column) = id;
    if !column.is_empty() {
        return format!("comment on column {object}.{column}");
    }
    let what = match side.views.get(object) {
        Some(view) if view.materialized => "materialized view",
        Some(_) => "view",
        None if side.sequences.contains(object) => "sequence",
        None => "table",
    };
    format!("comment on {what} {object}")
}

/// Which end of the dependency chain comes first. A drop takes the
/// reader before the thing it reads, a create is the other way round.
#[derive(Clone, Copy, PartialEq)]
enum Deepest {
    First,
    Last,
}

/// The views a predicate picks, in an order postgres will accept.
fn ordered_views(side: &Catalog, end: Deepest, mut pick: impl FnMut(&str) -> bool) -> Vec<String> {
    let chosen: Vec<String> = side
        .views
        .keys()
        .filter(|id| pick(id))
        .cloned()
        .collect::<Vec<_>>();
    // Depth is how many views stand between this one and a table, so a
    // view that reads a view is deeper than the one it reads.
    let mut depth: BTreeMap<&String, usize> = side.views.keys().map(|id| (id, 0)).collect();
    for _ in 0..side.views.len() {
        let mut moved = false;
        for (dependent, source) in &side.view_deps {
            let (Some(&d), Some(&s)) = (depth.get(dependent), depth.get(source)) else {
                continue;
            };
            if d <= s
                && let Some(slot) = depth.get_mut(dependent)
            {
                *slot = s + 1;
                moved = true;
            }
        }
        if !moved {
            break;
        }
    }
    let mut out = chosen;
    out.sort_by_key(|id| {
        let d = depth.get(id).copied().unwrap_or(0);
        let d = if end == Deepest::First {
            usize::MAX - d
        } else {
            d
        };
        (d, id.clone())
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(kind: &str) -> Column {
        Column {
            position: 1,
            kind: kind.into(),
            ..Column::default()
        }
    }

    fn with_table(id: &str, columns: Vec<(&str, Column)>) -> Catalog {
        let mut c = Catalog::default();
        c.schemas.insert("public".into());
        c.tables.insert(id.into(), Table::default());
        c.columns.insert(
            id.into(),
            columns
                .into_iter()
                .map(|(n, col)| (n.to_string(), col))
                .collect(),
        );
        c
    }

    #[test]
    fn a_name_is_quoted_only_when_it_has_to_be() {
        assert_eq!(ident("widgets"), "widgets");
        assert_eq!(ident("w1"), "w1");
        assert_eq!(ident("Widgets"), "\"Widgets\"");
        assert_eq!(ident("my table"), "\"my table\"");
        assert_eq!(ident("order"), "\"order\"", "and a word sql has taken");
        assert_eq!(ident("1st"), "\"1st\"");
        assert_eq!(ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn two_databases_that_agree_have_nothing_to_say() {
        let a = with_table("public.widgets", vec![("id", col("integer"))]);
        let b = with_table("public.widgets", vec![("id", col("integer"))]);
        assert_eq!(diff(&a, &b), Vec::<String>::new());
    }

    #[test]
    fn a_table_that_is_only_on_one_side_is_created_or_dropped() {
        let none = Catalog {
            schemas: ["public".to_string()].into_iter().collect(),
            ..Catalog::default()
        };
        let one = with_table(
            "public.widgets",
            vec![
                ("id", col("bigint")),
                (
                    "name",
                    Column {
                        position: 2,
                        kind: "text".into(),
                        not_null: true,
                        ..Column::default()
                    },
                ),
            ],
        );
        assert_eq!(
            diff(&none, &one),
            ["create table public.widgets (\n    id bigint,\n    name text not null\n);"]
        );
        assert_eq!(diff(&one, &none), ["drop table public.widgets;"]);
    }

    #[test]
    fn a_column_that_changed_is_altered_rather_than_rebuilt() {
        let before = with_table("public.widgets", vec![("name", col("text"))]);
        let after = with_table(
            "public.widgets",
            vec![(
                "name",
                Column {
                    position: 1,
                    kind: "character varying(20)".into(),
                    not_null: true,
                    default: "'x'::text".into(),
                    ..Column::default()
                },
            )],
        );
        assert_eq!(
            diff(&before, &after),
            [
                "alter table public.widgets alter column name set data type character varying(20);",
                "alter table public.widgets alter column name set default 'x'::text;",
                "alter table public.widgets alter column name set not null;",
            ]
        );
    }

    #[test]
    fn a_column_that_went_away_is_dropped_and_a_new_one_is_added() {
        let before = with_table("public.widgets", vec![("id", col("bigint"))]);
        let mut after = with_table("public.widgets", vec![("id", col("bigint"))]);
        after.columns.get_mut("public.widgets").unwrap().insert(
            "colour".into(),
            Column {
                position: 2,
                kind: "text".into(),
                ..Column::default()
            },
        );
        assert_eq!(
            diff(&before, &after),
            ["alter table public.widgets add column colour text;"]
        );
        assert_eq!(
            diff(&after, &before),
            ["alter table public.widgets drop column colour;"]
        );
    }

    #[test]
    fn an_enum_that_grew_takes_the_new_labels_in_place() {
        let mut before = Catalog::default();
        before
            .enums
            .insert("public.mood".into(), vec!["ok".into(), "good".into()]);
        let mut after = Catalog::default();
        after.enums.insert(
            "public.mood".into(),
            vec!["ok".into(), "good".into(), "great".into()],
        );
        assert_eq!(
            diff(&before, &after),
            ["alter type public.mood add value 'great';"]
        );
    }

    #[test]
    fn an_enum_that_lost_a_label_says_so_rather_than_dropping_a_type_in_use() {
        let mut before = Catalog::default();
        before
            .enums
            .insert("public.mood".into(), vec!["ok".into(), "bad".into()]);
        let mut after = Catalog::default();
        after.enums.insert("public.mood".into(), vec!["ok".into()]);
        let out = diff(&before, &after);
        assert_eq!(out.len(), 1);
        assert!(
            out[0].starts_with("-- the labels of public.mood changed"),
            "{out:?}"
        );
    }

    #[test]
    fn a_changed_constraint_comes_off_before_it_goes_back_on() {
        let mut before = with_table("public.widgets", vec![("id", col("bigint"))]);
        before.constraints.insert(
            ("public.widgets".into(), "widgets_id_check".into()),
            "CHECK ((id > 0))".into(),
        );
        let mut after = with_table("public.widgets", vec![("id", col("bigint"))]);
        after.constraints.insert(
            ("public.widgets".into(), "widgets_id_check".into()),
            "CHECK ((id > 10))".into(),
        );
        assert_eq!(
            diff(&before, &after),
            [
                "alter table public.widgets drop constraint widgets_id_check;",
                "alter table public.widgets add constraint widgets_id_check CHECK ((id > 10));",
            ]
        );
    }

    #[test]
    fn a_constraint_on_a_table_that_is_going_away_is_not_dropped_twice() {
        let mut before = with_table("public.widgets", vec![("id", col("bigint"))]);
        before.constraints.insert(
            ("public.widgets".into(), "widgets_pkey".into()),
            "PRIMARY KEY (id)".into(),
        );
        let after = Catalog {
            schemas: ["public".to_string()].into_iter().collect(),
            ..Catalog::default()
        };
        assert_eq!(diff(&before, &after), ["drop table public.widgets;"]);
    }

    #[test]
    fn a_policy_is_written_the_way_it_was_read() {
        let before = with_table("public.widgets", vec![("id", col("bigint"))]);
        let mut after = with_table("public.widgets", vec![("id", col("bigint"))]);
        after.tables.insert(
            "public.widgets".into(),
            Table {
                partitioned: false,
                rls: true,
            },
        );
        after.policies.insert(
            ("public.widgets".into(), "mine".into()),
            Policy {
                command: "r".into(),
                permissive: true,
                roles: "authenticated".into(),
                using: "(owner = auth.uid())".into(),
                check: String::new(),
            },
        );
        assert_eq!(
            diff(&before, &after),
            [
                "create policy mine on public.widgets for select to authenticated \
                 using ((owner = auth.uid()));",
                "alter table public.widgets enable row level security;",
            ]
        );
    }

    #[test]
    fn a_view_that_reads_a_view_is_dropped_first_and_created_last() {
        let mut before = Catalog::default();
        for (id, def) in [("public.a", "select 1"), ("public.b", "select * from a")] {
            before.views.insert(
                id.into(),
                View {
                    materialized: false,
                    def: def.into(),
                },
            );
        }
        before
            .view_deps
            .push(("public.b".into(), "public.a".into()));
        let after = Catalog::default();
        assert_eq!(
            diff(&before, &after),
            ["drop view public.b;", "drop view public.a;"],
            "the reader goes first"
        );
        let out = diff(&after, &before);
        let a = out
            .iter()
            .position(|s| s.contains("view public.a"))
            .unwrap();
        let b = out
            .iter()
            .position(|s| s.contains("view public.b"))
            .unwrap();
        assert!(a < b, "and is created last, {out:?}");
    }

    #[test]
    fn a_grant_that_is_only_on_one_side_is_granted_or_revoked() {
        let mut before = with_table("public.widgets", vec![("id", col("bigint"))]);
        let mut after = with_table("public.widgets", vec![("id", col("bigint"))]);
        before.grants.insert(
            "public.widgets".into(),
            [("anon".to_string(), "SELECT".to_string())]
                .into_iter()
                .collect(),
        );
        after.grants.insert(
            "public.widgets".into(),
            [("authenticated".to_string(), "INSERT".to_string())]
                .into_iter()
                .collect(),
        );
        assert_eq!(
            diff(&before, &after),
            [
                "grant insert on table public.widgets to authenticated;",
                "revoke select on table public.widgets from anon;",
            ]
        );
    }

    #[test]
    fn a_role_that_is_forgotten_takes_its_grants_with_it() {
        let mut before = with_table("public.widgets", vec![("id", col("bigint"))]);
        let after = with_table("public.widgets", vec![("id", col("bigint"))]);
        before.grants.insert(
            "public.widgets".into(),
            [
                ("anon".to_string(), "SELECT".to_string()),
                ("reader".to_string(), "SELECT".to_string()),
            ]
            .into_iter()
            .collect(),
        );
        before.forget_grants_to(&["anon", "authenticated", "service_role"]);
        assert_eq!(
            diff(&before, &after),
            ["revoke select on table public.widgets from reader;"],
            "a role of the project's own is still compared"
        );
    }

    #[test]
    fn a_comment_knows_whether_it_is_on_a_table_or_a_column() {
        let before = with_table("public.widgets", vec![("id", col("bigint"))]);
        let mut after = with_table("public.widgets", vec![("id", col("bigint"))]);
        after.comments.insert(
            ("public.widgets".into(), String::new()),
            "the widgets".into(),
        );
        after
            .comments
            .insert(("public.widgets".into(), "id".into()), "the id".into());
        assert_eq!(
            diff(&before, &after),
            [
                "comment on table public.widgets is 'the widgets';",
                "comment on column public.widgets.id is 'the id';",
            ]
        );
    }
}
