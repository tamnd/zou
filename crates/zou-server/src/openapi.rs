//! The OpenAPI document PostgREST serves at the API root.
//!
//! It is a Swagger 2.0 document built from the exposed schema:
//! every accessible table becomes a definition and a path item,
//! every accessible function becomes an rpc path item, and the
//! query grammar rides along as reusable parameter definitions the
//! path items reference. Clients read it to discover the surface,
//! so the shape here is transcribed from PostgREST's
//! Response/OpenAPI.hs rather than invented, down to the `<pk/>`
//! and `<fk .../>` markers Swagger UI forks look for.
//!
//! The three introspection queries live here next to the builder
//! they feed. They run on the request's own transaction, under the
//! request's role, and carry the privilege predicates upstream's
//! accessibleTables and accessibleFuncs use, which is what makes
//! the document show a caller only what that caller may touch.

use serde_json::{Map, Value, json};
use zou_rest::catalog::Catalog;

/// Tables, views, materialized views, foreign tables and partitioned
/// tables in the schema, with what the caller may do to each. Bind
/// the schema name as $1. The insertable, updatable and deletable
/// bits are pg_relation_is_updatable's CMD_INSERT, CMD_UPDATE and
/// CMD_DELETE masks, which is how upstream decides whether a view
/// gets the write path items.
pub const TABLES_SQL: &str = "\
select c.relname::text,
       d.description,
       c.relkind in ('r', 'p')
         or (c.relkind in ('v', 'f')
             and (pg_relation_is_updatable(c.oid::regclass, true) & 8) = 8),
       c.relkind in ('r', 'p')
         or (c.relkind in ('v', 'f')
             and (pg_relation_is_updatable(c.oid::regclass, true) & 4) = 4),
       c.relkind in ('r', 'p')
         or (c.relkind in ('v', 'f')
             and (pg_relation_is_updatable(c.oid::regclass, true) & 16) = 16),
       coalesce((select array_agg(a.attname::text order by a.attname)
                   from pg_constraint pk
                   join pg_attribute a
                     on a.attrelid = c.oid and a.attnum = any(pk.conkey)
                  where pk.conrelid = c.oid and pk.contype = 'p'
                    and not a.attisdropped), '{}')
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  left join pg_description d
    on d.objoid = c.oid and d.objsubid = 0 and d.classoid = 'pg_class'::regclass
 where c.relkind in ('r', 'p', 'v', 'm', 'f')
   and n.nspname = $1
   and not c.relispartition
   and (pg_has_role(c.relowner, 'usage')
        or has_table_privilege(
             c.oid,
             'select, insert, update, delete, truncate, references, trigger')
        or has_any_column_privilege(c.oid, 'select, insert, update, references'))
 order by c.relname";

/// Every column of those relations in attribute order, with the
/// pieces the property schema needs: comment, nullability, the type
/// as format_type renders it, the char maximum length, the default
/// expression, and the labels when the type is an enum. Bind the
/// schema name as $1.
///
/// A builtin type renders without its modifier, so varchar(20) is
/// "character varying" and the maxLength carries the 20 separately,
/// while a type from another schema keeps its qualified spelling.
/// One level of domain resolves to its base type, which is as far
/// as upstream's recursion matters in practice.
pub const COLUMNS_SQL: &str = "\
select c.relname::text,
       a.attname::text,
       d.description,
       not (a.attnotnull or (t.typtype = 'd' and t.typnotnull)),
       case
         when t.typtype = 'd' then
           case when bt.typnamespace = 'pg_catalog'::regnamespace
                then format_type(t.typbasetype, null::integer)
                else format_type(a.atttypid, a.atttypmod) end
         when t.typnamespace = 'pg_catalog'::regnamespace
           then format_type(a.atttypid, null::integer)
         else format_type(a.atttypid, a.atttypmod)
       end::text,
       information_schema._pg_char_max_length(
         information_schema._pg_truetypid(a.*, t.*),
         information_schema._pg_truetypmod(a.*, t.*))::int,
       case when a.attgenerated = 's' then null
            else pg_get_expr(ad.adbin, ad.adrelid)::text end,
       coalesce((select array_agg(e.enumlabel::text order by e.enumsortorder)
                   from pg_enum e
                  where e.enumtypid = coalesce(nullif(t.typbasetype, 0), t.oid)),
                '{}')
  from pg_attribute a
  join pg_class c on c.oid = a.attrelid
  join pg_namespace n on n.oid = c.relnamespace
  join pg_type t on t.oid = a.atttypid
  left join pg_type bt on bt.oid = t.typbasetype
  left join pg_attrdef ad on ad.adrelid = a.attrelid and ad.adnum = a.attnum
  left join pg_description d
    on d.objoid = a.attrelid and d.objsubid = a.attnum
   and d.classoid = 'pg_class'::regclass
 where c.relkind in ('r', 'p', 'v', 'm', 'f')
   and n.nspname = $1
   and not c.relispartition
   and a.attnum > 0
   and not a.attisdropped
 order by c.relname, a.attnum";

/// Every function the caller may execute in the schema, with its
/// input arguments in declaration order. Bind the schema name as
/// $1. Required means the argument has no default, which is
/// upstream's `idx <= pronargs - pronargdefaults`, and the types
/// that ignore their length declaration are widened the way
/// upstream widens them so the document does not promise a limit
/// the database will not enforce.
pub const FUNCS_SQL: &str = "\
with args as (
  select p.oid,
         array_agg(coalesce(a.name, '') order by a.idx) as names,
         array_agg(case a.type
                     when 'bit'::regtype then 'bit varying'
                     when 'bit[]'::regtype then 'bit varying[]'
                     when 'character'::regtype then 'character varying'
                     when 'character[]'::regtype then 'character varying[]'
                     else a.type::regtype::text
                   end order by a.idx) as types,
         array_agg(a.idx <= (p.pronargs - p.pronargdefaults) order by a.idx)
           as required,
         array_agg(coalesce(a.mode = 'v', false) order by a.idx) as variadic
    from pg_proc p,
         lateral unnest(p.proargnames, p.proargtypes, p.proargmodes)
           with ordinality as a(name, type, mode, idx)
   where a.type is not null
   group by p.oid
)
select p.proname::text,
       d.description,
       p.provolatile = 'v',
       coalesce(args.names, '{}'),
       coalesce(args.types, '{}'),
       coalesce(args.required, '{}'),
       coalesce(args.variadic, '{}')
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  left join args on args.oid = p.oid
  left join pg_description d
    on d.objoid = p.oid and d.classoid = 'pg_proc'::regclass
 where n.nspname = $1
   and p.prokind = 'f'
   and p.prorettype <> 'trigger'::regtype
   and has_function_privilege(p.oid, 'execute')
 order by p.proname";

/// The schema's own comment, which becomes the document title and
/// description. Bind the schema name as $1.
/// The cast goes through text so the parameter binds as text rather
/// than as a regnamespace the driver cannot serialize.
pub const SCHEMA_SQL: &str = "select obj_description(($1::text)::regnamespace, 'pg_namespace')";

#[derive(Debug, Clone, Default)]
pub struct Column {
    pub name: String,
    pub description: Option<String>,
    pub nullable: bool,
    pub data_type: String,
    pub max_len: Option<i32>,
    pub default: Option<String>,
    pub enum_labels: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Table {
    pub name: String,
    pub description: Option<String>,
    pub insertable: bool,
    pub updatable: bool,
    pub deletable: bool,
    pub pk: Vec<String>,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Default)]
pub struct Param {
    pub name: String,
    pub type_name: String,
    pub required: bool,
    pub variadic: bool,
}

#[derive(Debug, Clone, Default)]
pub struct Func {
    pub name: String,
    pub description: Option<String>,
    pub volatile: bool,
    pub params: Vec<Param>,
}

/// Everything outside the schema that shapes the document: where the
/// API answers and which version says so.
#[derive(Debug, Clone)]
pub struct Site {
    pub scheme: String,
    pub host: String,
    pub base_path: String,
    pub version: String,
}

/// The version the document reports, upstream's prettyVersion: the
/// first two components of the build's own version and nothing else.
pub fn version() -> String {
    env!("CARGO_PKG_VERSION")
        .split('.')
        .take(2)
        .collect::<Vec<_>>()
        .join(".")
}

/// The media types the surface produces and consumes, upstream's
/// list in upstream's order.
const MIMES: [&str; 4] = [
    "application/json",
    "application/vnd.pgrst.object+json;nulls=stripped",
    "application/vnd.pgrst.object+json",
    "text/csv",
];

/// The swagger type for a postgres type name, None where upstream
/// leaves it open: json and jsonb accept anything, so they get a
/// format and no type at all.
fn swagger_type(pg: &str) -> Option<&'static str> {
    match pg {
        "character varying" | "character" | "text" => Some("string"),
        "boolean" => Some("boolean"),
        "smallint" | "integer" | "bigint" => Some("integer"),
        "numeric" | "real" | "double precision" => Some("number"),
        "json" | "jsonb" => None,
        _ if pg.ends_with("[]") => Some("array"),
        _ => Some("string"),
    }
}

/// The swagger format, which is the integer width for the integer
/// types and the postgres type name itself for everything else.
fn swagger_format(pg: &str) -> &str {
    match pg {
        "smallint" | "integer" => "int32",
        "bigint" => "int64",
        other => other,
    }
}

/// A default expression rendered as the json value it stands for, or
/// None when it is not a literal. String typed defaults lose their
/// `::type` cast and their quotes first, everything else is read as
/// json, so `42` and `false` land as numbers and booleans while
/// `now()` lands as nothing.
fn parse_default(pg_type: &str, expr: &str) -> Option<Value> {
    let text = if swagger_type(pg_type) == Some("string") {
        let stripped = expr
            .strip_suffix(&format!("::{pg_type}"))
            .map(|d| d.trim_matches('\'').to_string())
            .unwrap_or_else(|| expr.to_string());
        format!("\"{stripped}\"")
    } else {
        expr.to_string()
    };
    serde_json::from_str(&text).ok()
}

/// PostgREST splits a comment at its first newline: the first line
/// is the summary, the rest is the description with leading blank
/// lines dropped, and an empty rest is no description at all.
fn split_comment(comment: Option<&String>) -> (Option<String>, Option<String>) {
    match comment {
        None => (None, None),
        Some(c) => match c.split_once('\n') {
            None => (Some(c.clone()), None),
            Some((head, rest)) => {
                let rest = rest.trim_start_matches('\n');
                (
                    Some(head.to_string()),
                    (!rest.is_empty()).then(|| rest.to_string()),
                )
            }
        },
    }
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert(k.to_string(), v);
    }
    Value::Object(m)
}

/// The property schema for one column, notes included. The notes are
/// what Swagger UI forks parse to draw key icons, so the wording and
/// the pseudo tags are upstream's exactly.
fn property(table: &Table, col: &Column, catalog: &Catalog) -> Value {
    let mut s = Map::new();
    if let Some(expr) = &col.default
        && let Some(v) = parse_default(&col.data_type, expr)
    {
        s.insert("default".to_string(), v);
    }

    let fk = catalog
        .fks()
        .iter()
        .find(|fk| fk.table == table.name && fk.columns.len() == 1 && fk.columns[0] == col.name);
    let mut notes: Vec<String> = Vec::new();
    if table.pk.contains(&col.name) {
        notes.push("This is a Primary Key.<pk/>".to_string());
    }
    if let Some(fk) = fk {
        let t = &fk.ref_table;
        let c = &fk.ref_columns[0];
        notes.push(format!(
            "This is a Foreign Key to `{t}.{c}`.<fk table='{t}' column='{c}'/>"
        ));
    }
    let description = if notes.is_empty() {
        col.description.clone()
    } else {
        let head = col
            .description
            .as_ref()
            .map(|d| format!("{d}\n\n"))
            .unwrap_or_default();
        Some(format!("{head}Note:\n{}", notes.join("\n")))
    };
    if let Some(d) = description {
        s.insert("description".to_string(), Value::from(d));
    }
    if !col.enum_labels.is_empty() {
        s.insert("enum".to_string(), json!(col.enum_labels));
    }
    s.insert(
        "format".to_string(),
        Value::from(swagger_format(&col.data_type)),
    );
    if let Some(n) = col.max_len {
        s.insert("maxLength".to_string(), Value::from(n));
    }
    if let Some(t) = swagger_type(&col.data_type) {
        s.insert("type".to_string(), Value::from(t));
        if t == "array" {
            let elem = &col.data_type[..col.data_type.len() - 2];
            s.insert(
                "items".to_string(),
                match swagger_type(elem) {
                    Some(e) => json!({ "type": e }),
                    None => json!({}),
                },
            );
        }
    }
    Value::Object(s)
}

fn definition(table: &Table, catalog: &Catalog) -> Value {
    let mut props = Map::new();
    for col in &table.columns {
        props.insert(col.name.clone(), property(table, col, catalog));
    }
    let required: Vec<&String> = table
        .columns
        .iter()
        .filter(|c| !c.nullable)
        .map(|c| &c.name)
        .collect();
    let mut d = Map::new();
    if let Some(desc) = &table.description {
        d.insert("description".to_string(), Value::from(desc.clone()));
    }
    d.insert("type".to_string(), Value::from("object"));
    d.insert("properties".to_string(), Value::Object(props));
    if !required.is_empty() {
        d.insert("required".to_string(), json!(required));
    }
    Value::Object(d)
}

fn prefer_param(tokens: &[&str]) -> Value {
    let mut values: Vec<&str> = Vec::new();
    for t in tokens {
        match *t {
            "count" => values.push("count=none"),
            "return" => values.extend(["return=representation", "return=minimal", "return=none"]),
            "resolution" => values.extend([
                "resolution=ignore-duplicates",
                "resolution=merge-duplicates",
            ]),
            _ => {}
        }
    }
    let mut p = Map::new();
    p.insert("name".to_string(), Value::from("Prefer"));
    p.insert("description".to_string(), Value::from("Preference"));
    p.insert("required".to_string(), Value::from(false));
    p.insert("in".to_string(), Value::from("header"));
    p.insert("type".to_string(), Value::from("string"));
    if !values.is_empty() {
        p.insert("enum".to_string(), json!(values));
    }
    Value::Object(p)
}

fn simple_param(name: &str, description: &str, in_: &str) -> Value {
    obj(vec![
        ("name", Value::from(name)),
        ("description", Value::from(description)),
        ("required", Value::from(false)),
        ("in", Value::from(in_)),
        ("type", Value::from("string")),
    ])
}

fn parameters(tables: &[Table]) -> Value {
    let mut p = Map::new();
    p.insert("preferParams".to_string(), prefer_param(&["params"]));
    p.insert("preferReturn".to_string(), prefer_param(&["return"]));
    p.insert("preferCount".to_string(), prefer_param(&["count"]));
    p.insert(
        "preferPost".to_string(),
        prefer_param(&["return", "resolution"]),
    );
    p.insert(
        "select".to_string(),
        simple_param("select", "Filtering Columns", "query"),
    );
    p.insert(
        "on_conflict".to_string(),
        simple_param("on_conflict", "On Conflict", "query"),
    );
    p.insert(
        "order".to_string(),
        simple_param("order", "Ordering", "query"),
    );
    p.insert(
        "range".to_string(),
        simple_param("Range", "Limiting and Pagination", "header"),
    );
    let mut unit = simple_param("Range-Unit", "Limiting and Pagination", "header");
    unit["default"] = Value::from("items");
    p.insert("rangeUnit".to_string(), unit);
    p.insert(
        "offset".to_string(),
        simple_param("offset", "Limiting and Pagination", "query"),
    );
    p.insert(
        "limit".to_string(),
        simple_param("limit", "Limiting and Pagination", "query"),
    );
    for t in tables {
        p.insert(
            format!("body.{}", t.name),
            obj(vec![
                ("name", Value::from(t.name.clone())),
                ("description", Value::from(t.name.clone())),
                ("required", Value::from(false)),
                ("in", Value::from("body")),
                (
                    "schema",
                    json!({ "$ref": format!("#/definitions/{}", t.name) }),
                ),
            ]),
        );
        for c in &t.columns {
            let mut f = Map::new();
            f.insert("name".to_string(), Value::from(c.name.clone()));
            if let Some(d) = &c.description {
                f.insert("description".to_string(), Value::from(d.clone()));
            }
            f.insert("required".to_string(), Value::from(false));
            f.insert("in".to_string(), Value::from("query"));
            f.insert("type".to_string(), Value::from("string"));
            p.insert(format!("rowFilter.{}.{}", t.name, c.name), Value::Object(f));
        }
    }
    Value::Object(p)
}

fn refs(names: &[String]) -> Value {
    Value::Array(
        names
            .iter()
            .map(|n| json!({ "$ref": format!("#/parameters/{n}") }))
            .collect(),
    )
}

fn table_path(table: &Table) -> Value {
    let (summary, description) = split_comment(table.description.as_ref());
    let base = |params: Vec<String>, responses: Value| {
        let mut op = Map::new();
        op.insert("tags".to_string(), json!([table.name.clone()]));
        if let Some(s) = &summary {
            op.insert("summary".to_string(), Value::from(s.clone()));
        }
        if let Some(d) = &description {
            op.insert("description".to_string(), Value::from(d.clone()));
        }
        op.insert("parameters".to_string(), refs(&params));
        op.insert("responses".to_string(), responses);
        Value::Object(op)
    };
    let filters: Vec<String> = table
        .columns
        .iter()
        .map(|c| format!("rowFilter.{}.{}", table.name, c.name))
        .collect();
    let with = |extra: &[&str]| {
        let mut v = filters.clone();
        v.extend(extra.iter().map(|s| s.to_string()));
        v
    };

    let mut item = Map::new();
    item.insert(
        "get".to_string(),
        base(
            with(&[
                "select",
                "order",
                "range",
                "rangeUnit",
                "offset",
                "limit",
                "preferCount",
            ]),
            json!({
                "200": {
                    "description": "OK",
                    "schema": {
                        "items": { "$ref": format!("#/definitions/{}", table.name) },
                        "type": "array",
                    },
                },
                "206": { "description": "Partial Content" },
            }),
        ),
    );
    // Upstream gates the whole write trio on the same or, so a view
    // that is only updatable still advertises post and delete.
    if table.insertable || table.updatable || table.deletable {
        item.insert(
            "post".to_string(),
            base(
                vec![
                    format!("body.{}", table.name),
                    "select".to_string(),
                    "preferPost".to_string(),
                ],
                json!({ "201": { "description": "Created" } }),
            ),
        );
        item.insert(
            "patch".to_string(),
            base(
                with(&[&format!("body.{}", table.name), "preferReturn"]),
                json!({ "204": { "description": "No Content" } }),
            ),
        );
        item.insert(
            "delete".to_string(),
            base(
                with(&["preferReturn"]),
                json!({ "204": { "description": "No Content" } }),
            ),
        );
    }
    Value::Object(item)
}

/// One query string argument of an rpc GET. A variadic argument
/// takes the multi collection format, everything else collapses to
/// a string when its type has no scalar swagger spelling, because
/// arrays and json arrive as text in a query string.
fn proc_get_param(p: &Param) -> Value {
    let mut v = Map::new();
    v.insert("name".to_string(), Value::from(p.name.clone()));
    v.insert("required".to_string(), Value::from(p.required));
    v.insert("in".to_string(), Value::from("query"));
    if p.variadic {
        v.insert(
            "type".to_string(),
            Value::from(swagger_type(&p.type_name).unwrap_or("string")),
        );
        let elem = p.type_name.strip_suffix("[]").unwrap_or(&p.type_name);
        let mut items = Map::new();
        if let Some(t) = swagger_type(elem) {
            items.insert("type".to_string(), Value::from(t));
        }
        items.insert("format".to_string(), Value::from(swagger_format(elem)));
        v.insert("collectionFormat".to_string(), Value::from("multi"));
        v.insert("items".to_string(), Value::Object(items));
    } else {
        v.insert(
            "format".to_string(),
            Value::from(swagger_format(&p.type_name)),
        );
        v.insert(
            "type".to_string(),
            Value::from(match swagger_type(&p.type_name) {
                Some("array") | None => "string",
                Some(t) => t,
            }),
        );
    }
    Value::Object(v)
}

/// The body schema of an rpc POST: one object property per argument,
/// required listing the ones without defaults.
fn proc_schema(f: &Func) -> Value {
    let mut props = Map::new();
    for p in &f.params {
        let mut s = Map::new();
        s.insert(
            "format".to_string(),
            Value::from(swagger_format(&p.type_name)),
        );
        if let Some(t) = swagger_type(&p.type_name) {
            s.insert("type".to_string(), Value::from(t));
            if t == "array" {
                let elem = &p.type_name[..p.type_name.len() - 2];
                s.insert(
                    "items".to_string(),
                    match swagger_type(elem) {
                        Some(e) => json!({ "type": e }),
                        None => json!({}),
                    },
                );
            }
        }
        props.insert(p.name.clone(), Value::Object(s));
    }
    let required: Vec<&String> = f
        .params
        .iter()
        .filter(|p| p.required)
        .map(|p| &p.name)
        .collect();
    let mut s = Map::new();
    if let Some(d) = &f.description {
        s.insert("description".to_string(), Value::from(d.clone()));
    }
    s.insert("type".to_string(), Value::from("object"));
    s.insert("properties".to_string(), Value::Object(props));
    if !required.is_empty() {
        s.insert("required".to_string(), json!(required));
    }
    Value::Object(s)
}

fn proc_path(f: &Func) -> Value {
    let (summary, description) = split_comment(f.description.as_ref());
    let base = || {
        let mut op = Map::new();
        op.insert("tags".to_string(), json!([format!("(rpc) {}", f.name)]));
        if let Some(s) = &summary {
            op.insert("summary".to_string(), Value::from(s.clone()));
        }
        if let Some(d) = &description {
            op.insert("description".to_string(), Value::from(d.clone()));
        }
        op.insert(
            "produces".to_string(),
            json!([
                "application/json",
                "application/vnd.pgrst.object+json;nulls=stripped",
                "application/vnd.pgrst.object+json",
            ]),
        );
        op.insert(
            "responses".to_string(),
            json!({ "200": { "description": "OK" } }),
        );
        op
    };
    let mut item = Map::new();
    // A volatile function cannot answer a GET, so it only gets the
    // post path item.
    if !f.volatile {
        let mut get = base();
        get.insert(
            "parameters".to_string(),
            Value::Array(f.params.iter().map(proc_get_param).collect()),
        );
        item.insert("get".to_string(), Value::Object(get));
    }
    let mut post = base();
    post.insert(
        "parameters".to_string(),
        json!([
            {
                "name": "args",
                "required": true,
                "in": "body",
                "schema": proc_schema(f),
            },
            { "$ref": "#/parameters/preferParams" },
        ]),
    );
    item.insert("post".to_string(), Value::Object(post));
    Value::Object(item)
}

fn root_path() -> Value {
    json!({
        "get": {
            "tags": ["Introspection"],
            "summary": "OpenAPI description (this document)",
            "produces": ["application/openapi+json", "application/json"],
            "responses": { "200": { "description": "OK" } },
        }
    })
}

/// The whole document. `schema_comment` is the exposed schema's own
/// comment, which overrides the title and description the way
/// upstream lets it.
pub fn document(
    site: &Site,
    schema_comment: Option<&String>,
    tables: &[Table],
    funcs: &[Func],
    catalog: &Catalog,
) -> Value {
    let (title, description) = split_comment(schema_comment);
    let mut defs = Map::new();
    let mut paths = Map::new();
    paths.insert("/".to_string(), root_path());
    for t in tables {
        defs.insert(t.name.clone(), definition(t, catalog));
        paths.insert(format!("/{}", t.name), table_path(t));
    }
    for f in funcs {
        paths.insert(format!("/rpc/{}", f.name), proc_path(f));
    }
    json!({
        "swagger": "2.0",
        "info": {
            "version": site.version,
            "title": title.unwrap_or_else(|| "PostgREST API".to_string()),
            "description": description
                .unwrap_or_else(|| "This is a dynamic API generated by PostgREST".to_string()),
        },
        "host": site.host,
        "basePath": site.base_path,
        "schemes": [site.scheme],
        "consumes": MIMES,
        "produces": MIMES,
        "externalDocs": {
            "description": "PostgREST Documentation",
            "url": "https://postgrest.org/en/latest/references/api.html",
        },
        "definitions": Value::Object(defs),
        "parameters": parameters(tables),
        "paths": Value::Object(paths),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use zou_rest::catalog::FkRow;

    fn col(name: &str, ty: &str) -> Column {
        Column {
            name: name.to_string(),
            data_type: ty.to_string(),
            nullable: true,
            ..Column::default()
        }
    }

    fn site() -> Site {
        Site {
            scheme: "http".to_string(),
            host: "localhost:54321".to_string(),
            base_path: "/rest/v1/".to_string(),
            version: "0.0.1".to_string(),
        }
    }

    #[test]
    fn postgres_types_map_the_way_postgrest_maps_them() {
        let mut t = Table {
            name: "openapi_types".to_string(),
            columns: vec![
                col("a_text", "text"),
                col("a_varchar", "character varying"),
                col("a_bool", "boolean"),
                col("a_smallint", "smallint"),
                col("a_bigint", "bigint"),
                col("a_numeric", "numeric"),
                col("a_json", "json"),
                col("a_text_arr", "text[]"),
                col("a_json_arr", "json[]"),
            ],
            ..Table::default()
        };
        t.columns[1].max_len = Some(20);
        let cat = Catalog::new(vec![]);
        let d = definition(&t, &cat);
        let p = &d["properties"];
        assert_eq!(p["a_text"], json!({"format": "text", "type": "string"}));
        assert_eq!(
            p["a_varchar"],
            json!({"format": "character varying", "maxLength": 20, "type": "string"})
        );
        assert_eq!(p["a_bool"], json!({"format": "boolean", "type": "boolean"}));
        assert_eq!(
            p["a_smallint"],
            json!({"format": "int32", "type": "integer"})
        );
        assert_eq!(p["a_bigint"], json!({"format": "int64", "type": "integer"}));
        assert_eq!(
            p["a_numeric"],
            json!({"format": "numeric", "type": "number"})
        );
        assert_eq!(
            p["a_json"],
            json!({"format": "json"}),
            "json takes any shape, so it carries no type"
        );
        assert_eq!(
            p["a_text_arr"],
            json!({"format": "text[]", "type": "array", "items": {"type": "string"}})
        );
        assert_eq!(
            p["a_json_arr"],
            json!({"format": "json[]", "type": "array", "items": {}})
        );
        assert!(
            d.get("required").is_none(),
            "every column nullable means no required list at all"
        );
    }

    #[test]
    fn keys_and_comments_become_the_property_notes() {
        let t = Table {
            name: "child_entities".to_string(),
            description: Some("child_entities comment".to_string()),
            pk: vec!["id".to_string()],
            columns: vec![
                Column {
                    description: Some("child_entities id comment".to_string()),
                    nullable: false,
                    ..col("id", "integer")
                },
                Column {
                    nullable: false,
                    ..col("parent_id", "integer")
                },
                col("name", "text"),
            ],
            ..Table::default()
        };
        let cat = Catalog::new(vec![FkRow {
            constraint: "child_entities_parent_id_fkey".to_string(),
            table: "child_entities".to_string(),
            columns: vec!["parent_id".to_string()],
            ref_table: "entities".to_string(),
            ref_columns: vec!["id".to_string()],
            unique: false,
            in_pk: false,
        }]);
        let d = definition(&t, &cat);
        assert_eq!(d["description"], "child_entities comment");
        assert_eq!(
            d["properties"]["id"]["description"],
            "child_entities id comment\n\nNote:\nThis is a Primary Key.<pk/>"
        );
        assert_eq!(
            d["properties"]["parent_id"]["description"],
            "Note:\nThis is a Foreign Key to `entities.id`.<fk table='entities' column='id'/>"
        );
        assert!(d["properties"]["name"].get("description").is_none());
        assert_eq!(d["required"], json!(["id", "parent_id"]));
    }

    #[test]
    fn defaults_come_back_as_the_json_they_stand_for() {
        assert_eq!(parse_default("integer", "42"), Some(json!(42)));
        assert_eq!(parse_default("boolean", "false"), Some(json!(false)));
        assert_eq!(parse_default("numeric", "42.2"), Some(json!(42.2)));
        assert_eq!(
            parse_default("text", "'default'::text"),
            Some(json!("default"))
        );
        assert_eq!(
            parse_default("date", "'1900-01-01'::date"),
            Some(json!("1900-01-01")),
            "a type with no scalar spelling is a string, so its default is quoted"
        );
        assert_eq!(
            parse_default("timestamp with time zone", "now()"),
            Some(json!("now()")),
            "a string typed column quotes whatever the expression says, upstream's own shortcut"
        );
        assert_eq!(
            parse_default("integer", "nextval('s'::regclass)"),
            None,
            "an expression that is not json for a non string column carries no default"
        );
    }

    #[test]
    fn a_comment_splits_into_summary_and_description() {
        assert_eq!(
            split_comment(Some(&"one line".to_string())),
            (Some("one line".to_string()), None)
        );
        assert_eq!(
            split_comment(Some(&"summary\n\nrest\nmore".to_string())),
            (Some("summary".to_string()), Some("rest\nmore".to_string()))
        );
        assert_eq!(
            split_comment(Some(&"summary\n".to_string())),
            (Some("summary".to_string()), None)
        );
        assert_eq!(split_comment(None), (None, None));
    }

    #[test]
    fn path_items_carry_the_grammar_as_parameter_refs() {
        let t = Table {
            name: "items".to_string(),
            insertable: true,
            updatable: true,
            deletable: true,
            columns: vec![col("id", "integer"), col("name", "text")],
            ..Table::default()
        };
        let p = table_path(&t);
        assert_eq!(
            p["get"]["parameters"],
            json!([
                {"$ref": "#/parameters/rowFilter.items.id"},
                {"$ref": "#/parameters/rowFilter.items.name"},
                {"$ref": "#/parameters/select"},
                {"$ref": "#/parameters/order"},
                {"$ref": "#/parameters/range"},
                {"$ref": "#/parameters/rangeUnit"},
                {"$ref": "#/parameters/offset"},
                {"$ref": "#/parameters/limit"},
                {"$ref": "#/parameters/preferCount"},
            ])
        );
        assert_eq!(
            p["get"]["responses"]["200"]["schema"],
            json!({"items": {"$ref": "#/definitions/items"}, "type": "array"})
        );
        assert_eq!(
            p["get"]["responses"]["206"]["description"],
            "Partial Content"
        );
        assert_eq!(
            p["post"]["parameters"],
            json!([
                {"$ref": "#/parameters/body.items"},
                {"$ref": "#/parameters/select"},
                {"$ref": "#/parameters/preferPost"},
            ])
        );
        assert_eq!(p["post"]["responses"]["201"]["description"], "Created");
        assert_eq!(p["patch"]["responses"]["204"]["description"], "No Content");
        assert_eq!(p["delete"]["responses"]["204"]["description"], "No Content");

        let read_only = Table {
            name: "v".to_string(),
            ..Table::default()
        };
        let p = table_path(&read_only);
        assert!(p.get("post").is_none() && p.get("patch").is_none());
    }

    #[test]
    fn rpc_path_items_follow_volatility_and_argument_shape() {
        let stable = Func {
            name: "getallusers".to_string(),
            description: Some("An RPC function\n\nJust a test".to_string()),
            volatile: false,
            params: vec![
                Param {
                    name: "num".to_string(),
                    type_name: "integer".to_string(),
                    required: true,
                    variadic: false,
                },
                Param {
                    name: "arr".to_string(),
                    type_name: "text[]".to_string(),
                    required: false,
                    variadic: false,
                },
                Param {
                    name: "v".to_string(),
                    type_name: "text[]".to_string(),
                    required: false,
                    variadic: true,
                },
            ],
        };
        let p = proc_path(&stable);
        assert_eq!(p["get"]["tags"], json!(["(rpc) getallusers"]));
        assert_eq!(p["get"]["summary"], "An RPC function");
        assert_eq!(p["get"]["description"], "Just a test");
        assert_eq!(
            p["get"]["parameters"][0],
            json!({"name": "num", "required": true, "in": "query", "format": "int32", "type": "integer"})
        );
        assert_eq!(
            p["get"]["parameters"][1],
            json!({"name": "arr", "required": false, "in": "query", "format": "text[]", "type": "string"}),
            "an array argument arrives as text in a query string"
        );
        assert_eq!(
            p["get"]["parameters"][2],
            json!({
                "name": "v",
                "required": false,
                "in": "query",
                "type": "array",
                "collectionFormat": "multi",
                "items": {"type": "string", "format": "text"},
            })
        );
        let body = &p["post"]["parameters"][0];
        assert_eq!(body["name"], "args");
        assert_eq!(body["in"], "body");
        assert_eq!(body["schema"]["required"], json!(["num"]));
        assert_eq!(
            body["schema"]["properties"]["arr"],
            json!({"format": "text[]", "type": "array", "items": {"type": "string"}})
        );
        assert_eq!(
            p["post"]["parameters"][1],
            json!({"$ref": "#/parameters/preferParams"})
        );

        let volatile = Func {
            name: "reset".to_string(),
            volatile: true,
            ..Func::default()
        };
        let p = proc_path(&volatile);
        assert!(p.get("get").is_none(), "a volatile function has no GET");
        assert!(p.get("post").is_some());
        assert!(
            p["post"]["parameters"][0]["schema"]
                .get("required")
                .is_none(),
            "no argument means no required list"
        );
    }

    #[test]
    fn the_document_carries_the_surface_and_the_defaults() {
        let t = Table {
            name: "items".to_string(),
            columns: vec![col("id", "integer")],
            ..Table::default()
        };
        let d = document(&site(), None, &[t], &[], &Catalog::new(vec![]));
        assert_eq!(d["swagger"], "2.0");
        assert_eq!(d["info"]["title"], "PostgREST API");
        assert_eq!(
            d["info"]["description"],
            "This is a dynamic API generated by PostgREST"
        );
        assert_eq!(d["basePath"], "/rest/v1/");
        assert_eq!(d["host"], "localhost:54321");
        assert_eq!(d["schemes"], json!(["http"]));
        assert_eq!(d["produces"], json!(MIMES));
        assert_eq!(d["consumes"], json!(MIMES));
        assert!(d["paths"]["/"]["get"]["tags"] == json!(["Introspection"]));
        assert!(d["paths"]["/items"]["get"].is_object());
        assert!(d["definitions"]["items"].is_object());
        assert!(
            d.get("security").is_none() && d.get("securityDefinitions").is_none(),
            "upstream ships no security section by default"
        );
        assert_eq!(
            d["parameters"]["preferCount"]["enum"],
            json!(["count=none"])
        );
        assert!(
            d["parameters"]["preferParams"].get("enum").is_none(),
            "an empty enum is left out entirely"
        );
        assert_eq!(d["parameters"]["rangeUnit"]["default"], "items");

        let commented = document(
            &site(),
            Some(&"My API title\n\nMy API description".to_string()),
            &[],
            &[],
            &Catalog::new(vec![]),
        );
        assert_eq!(commented["info"]["title"], "My API title");
        assert_eq!(commented["info"]["description"], "My API description");
    }
}
