//! What the generator reads out of a database before it writes
//! anything: schemas, the things with columns, the columns, the foreign
//! keys, the functions, and the types.
//!
//! The five subtle queries are postgres-meta's own, kept in
//! `sql/*.sql` and pulled in whole rather than paraphrased, because a
//! paraphrase of "is this column nullable" is a bug waiting for a
//! domain over a not null base type. The five simple ones are written
//! out here, since a relkind and a privilege check paraphrase fine.

use super::sort::locale_cmp;
use serde_json::Value;
use tokio_postgres::{Client, NoTls, Row};

/// A schema in the generated file, which is one the caller asked for
/// and can actually see.
const SCHEMAS_SQL: &str = "\
select n.oid::int8 as id, n.nspname as name
from pg_namespace n
where n.nspname = any($1)
  and (pg_has_role(n.nspowner, 'USAGE') or has_schema_privilege(n.oid, 'CREATE, USAGE'))";

/// Tables and partitioned tables, then foreign tables, views and
/// materialized views, which differ only in relkind and in whether a
/// write can reach through them.
const TABLES_SQL: &str = "\
select c.oid::int8 as id, n.nspname as schema, c.relname as name, false as is_updatable
from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
where n.nspname = any($1)
  and c.relkind::text = any($2)
  and (
    pg_has_role(c.relowner, 'USAGE')
    or has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
    or has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES')
  )";

/// The same, for views, where information_schema's own definition of
/// updatable is the one that decides whether an Insert type is written.
const VIEWS_SQL: &str = "\
select
  c.oid::int8 as id,
  n.nspname as schema,
  c.relname as name,
  (pg_relation_is_updatable(c.oid, false) & 20) = 20 as is_updatable
from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
where n.nspname = any($1) and c.relkind = 'v'";

const COLUMNS_SQL: &str = include_str!("sql/columns.sql");
const TYPES_SQL: &str = include_str!("sql/types.sql");
const FUNCTIONS_SQL: &str = include_str!("sql/functions.sql");
const TABLE_RELATIONSHIPS_SQL: &str = include_str!("sql/table-rel.sql");
const VIEW_KEY_DEPENDENCIES_SQL: &str = include_str!("sql/view-deps.sql");

pub struct Catalog {
    pub schemas: Vec<Schema>,
    pub tables: Vec<Relation>,
    pub foreign_tables: Vec<Relation>,
    pub views: Vec<Relation>,
    pub materialized_views: Vec<Relation>,
    pub columns: Vec<Column>,
    pub relationships: Vec<Relationship>,
    pub functions: Vec<Function>,
    pub types: Vec<PgType>,
}

pub struct Schema {
    pub name: String,
}

pub struct Relation {
    pub id: i64,
    pub schema: String,
    pub name: String,
    pub is_updatable: bool,
}

pub struct Column {
    pub table_id: i64,
    pub name: String,
    pub format: String,
    pub default_value: Option<String>,
    pub is_identity: bool,
    pub identity_generation: Option<String>,
    pub is_nullable: bool,
    pub is_updatable: bool,
}

pub struct Relationship {
    pub foreign_key_name: String,
    pub schema: String,
    pub relation: String,
    pub columns: Vec<String>,
    pub referenced_schema: String,
    pub referenced_relation: String,
    pub referenced_columns: Vec<String>,
    pub is_one_to_one: bool,
}

pub struct Function {
    pub schema: String,
    pub name: String,
    pub args: Vec<Arg>,
    pub argument_types: String,
    pub return_type: String,
    pub return_type_id: i64,
    pub return_type_relation_id: Option<i64>,
    pub is_set_returning_function: bool,
    pub rows: Option<f32>,
}

pub struct Arg {
    pub mode: String,
    pub name: String,
    pub type_id: i64,
    pub has_default: bool,
}

pub struct PgType {
    pub id: i64,
    pub name: String,
    pub schema: String,
    pub format: String,
    pub enums: Vec<String>,
    pub attributes: Vec<Attribute>,
    pub type_relation_id: Option<i64>,
}

pub struct Attribute {
    pub name: String,
    pub type_id: i64,
}

/// A key dependency is one view column that leads back to one table
/// column, which is how a foreign key on a table becomes a
/// relationship on the views that select from it.
struct KeyDependency {
    table_schema: String,
    table_name: String,
    view_schema: String,
    view_name: String,
    constraint_name: String,
    constraint_type: String,
    column_dependencies: Vec<ColumnDependency>,
}

struct ColumnDependency {
    view_columns: Vec<String>,
}

pub async fn read(url: &str, schemas: &[String]) -> Result<Catalog, String> {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .map_err(|e| format!("cannot connect: {e}"))?;
    // The connection is the half that drives the socket. It ends when
    // the client is dropped, and an error there shows up as an error on
    // the next query, so there is nothing to report from in here.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            log::debug!("connection ended: {e}");
        }
    });

    let wanted = schemas.to_vec();
    let schema_rows = query(&client, SCHEMAS_SQL, &[&wanted], "schemas").await?;
    let tables = relations(&client, &wanted, &["r", "p"]).await?;
    let foreign_tables = relations(&client, &wanted, &["f"]).await?;
    let materialized_views = relations(&client, &wanted, &["m"]).await?;
    let view_rows = query(&client, VIEWS_SQL, &[&wanted], "views").await?;
    let column_rows = query(&client, COLUMNS_SQL, &[&wanted], "columns").await?;
    let type_rows = query(&client, TYPES_SQL, &[], "types").await?;
    let function_rows = query(&client, FUNCTIONS_SQL, &[&wanted], "functions").await?;
    let relationship_rows = query(&client, TABLE_RELATIONSHIPS_SQL, &[], "relationships").await?;
    let dependency_rows =
        query(&client, VIEW_KEY_DEPENDENCIES_SQL, &[], "view dependencies").await?;

    let relationships = table_relationships(&relationship_rows);
    let dependencies = key_dependencies(&dependency_rows);
    let relationships = with_view_relationships(relationships, &dependencies);

    Ok(Catalog {
        schemas: schema_rows
            .iter()
            .map(|row| Schema {
                name: row.get("name"),
            })
            .collect(),
        tables,
        foreign_tables,
        views: view_rows.iter().map(relation).collect(),
        materialized_views,
        columns: column_rows.iter().map(column).collect(),
        relationships,
        functions: function_rows.iter().filter_map(function).collect(),
        types: type_rows.iter().map(pg_type).collect(),
    })
}

async fn query(
    client: &Client,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    what: &str,
) -> Result<Vec<Row>, String> {
    client
        .query(sql, params)
        .await
        .map_err(|e| format!("cannot read {what}: {e}"))
}

async fn relations(
    client: &Client,
    schemas: &Vec<String>,
    kinds: &[&str],
) -> Result<Vec<Relation>, String> {
    let kinds: Vec<String> = kinds.iter().map(|k| k.to_string()).collect();
    let rows = query(client, TABLES_SQL, &[schemas, &kinds], "tables").await?;
    Ok(rows.iter().map(relation).collect())
}

fn relation(row: &Row) -> Relation {
    Relation {
        id: row.get("id"),
        schema: row.get("schema"),
        name: row.get("name"),
        is_updatable: row.get("is_updatable"),
    }
}

fn column(row: &Row) -> Column {
    Column {
        table_id: row.get("table_id"),
        name: row.get("name"),
        format: row.get("format"),
        default_value: row.get("default_value"),
        is_identity: row.get("is_identity"),
        identity_generation: row.get("identity_generation"),
        is_nullable: row.get("is_nullable"),
        is_updatable: row.get("is_updatable"),
    }
}

/// Triggers are functions the catalog knows about and postgrest will
/// never call, so they are dropped here the way postgres-meta drops
/// them before it reaches its template.
fn function(row: &Row) -> Option<Function> {
    let return_type: String = row.get("return_type");
    if return_type == "trigger" || return_type == "event_trigger" {
        return None;
    }
    let args: Value = row.get("args");
    // Arguments come out of the catalog in the order they were
    // declared and go into the file in the order of their names, which
    // is also the order the returns table columns come out in.
    let mut sorted: Vec<Arg> = args
        .as_array()
        .map(|args| args.iter().map(arg).collect())
        .unwrap_or_default();
    sorted.sort_by(|a, b| locale_cmp(&a.name, &b.name));
    Some(Function {
        schema: row.get("schema"),
        name: row.get("name"),
        args: sorted,
        argument_types: row.get("argument_types"),
        return_type,
        return_type_id: row.get("return_type_id"),
        return_type_relation_id: row.get("return_type_relation_id"),
        is_set_returning_function: row.get("is_set_returning_function"),
        rows: row.get("prorows"),
    })
}

fn arg(value: &Value) -> Arg {
    Arg {
        mode: string_at(value, "mode"),
        name: string_at(value, "name"),
        type_id: value.get("type_id").and_then(Value::as_i64).unwrap_or(0),
        has_default: value
            .get("has_default")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    }
}

fn pg_type(row: &Row) -> PgType {
    let enums: Value = row.get("enums");
    let attributes: Value = row.get("attributes");
    PgType {
        id: row.get("id"),
        name: row.get("name"),
        schema: row.get("schema"),
        format: row.get("format"),
        enums: strings(&enums),
        attributes: attributes
            .as_array()
            .map(|attributes| {
                attributes
                    .iter()
                    .map(|a| Attribute {
                        name: string_at(a, "name"),
                        type_id: a.get("type_id").and_then(Value::as_i64).unwrap_or(0),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        type_relation_id: row.get("type_relation_id"),
    }
}

fn table_relationships(rows: &[Row]) -> Vec<Relationship> {
    rows.iter()
        .map(|row| {
            let columns: Value = row.get("columns");
            let referenced_columns: Value = row.get("referenced_columns");
            Relationship {
                foreign_key_name: row.get("foreign_key_name"),
                schema: row.get("schema"),
                relation: row.get("relation"),
                columns: strings(&columns),
                referenced_schema: row.get("referenced_schema"),
                referenced_relation: row.get("referenced_relation"),
                referenced_columns: strings(&referenced_columns),
                is_one_to_one: row.get::<_, Option<bool>>("is_one_to_one").unwrap_or(false),
            }
        })
        .collect()
}

fn key_dependencies(rows: &[Row]) -> Vec<KeyDependency> {
    rows.iter()
        .map(|row| {
            let dependencies: Value = row.get("column_dependencies");
            KeyDependency {
                table_schema: row.get("table_schema"),
                table_name: row.get("table_name"),
                view_schema: row.get("view_schema"),
                view_name: row.get("view_name"),
                constraint_name: row.get("constraint_name"),
                constraint_type: row.get("constraint_type"),
                column_dependencies: dependencies
                    .as_array()
                    .map(|deps| {
                        deps.iter()
                            .map(|d| ColumnDependency {
                                view_columns: d
                                    .get("view_columns")
                                    .map(strings)
                                    .unwrap_or_default(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }
        })
        .collect()
}

/// A foreign key between two tables is also a relationship between any
/// views that carry both ends of it, in all three combinations. This is
/// postgrest's rule, and clients rely on it to embed through a view.
fn with_view_relationships(
    table_relationships: Vec<Relationship>,
    dependencies: &[KeyDependency],
) -> Vec<Relationship> {
    let mut through_views = Vec::new();
    for r in &table_relationships {
        let from_view: Vec<&KeyDependency> = dependencies
            .iter()
            .filter(|d| {
                d.table_schema == r.schema
                    && d.table_name == r.relation
                    && d.constraint_name == r.foreign_key_name
                    && d.constraint_type == "f"
            })
            .collect();
        let to_view: Vec<&KeyDependency> = dependencies
            .iter()
            .filter(|d| {
                d.table_schema == r.referenced_schema
                    && d.table_name == r.referenced_relation
                    && d.constraint_name == r.foreign_key_name
                    && d.constraint_type == "f_ref"
            })
            .collect();

        for d in &from_view {
            for columns in expand(d) {
                through_views.push(Relationship {
                    foreign_key_name: r.foreign_key_name.clone(),
                    schema: d.view_schema.clone(),
                    relation: d.view_name.clone(),
                    columns,
                    referenced_schema: r.referenced_schema.clone(),
                    referenced_relation: r.referenced_relation.clone(),
                    referenced_columns: r.referenced_columns.clone(),
                    is_one_to_one: r.is_one_to_one,
                });
            }
        }
        for d in &to_view {
            for referenced_columns in expand(d) {
                through_views.push(Relationship {
                    foreign_key_name: r.foreign_key_name.clone(),
                    schema: r.schema.clone(),
                    relation: r.relation.clone(),
                    columns: r.columns.clone(),
                    referenced_schema: d.view_schema.clone(),
                    referenced_relation: d.view_name.clone(),
                    referenced_columns,
                    is_one_to_one: r.is_one_to_one,
                });
            }
        }
        for from in &from_view {
            for columns in expand(from) {
                for to in &to_view {
                    for referenced_columns in expand(to) {
                        through_views.push(Relationship {
                            foreign_key_name: r.foreign_key_name.clone(),
                            schema: from.view_schema.clone(),
                            relation: from.view_name.clone(),
                            columns: columns.clone(),
                            referenced_schema: to.view_schema.clone(),
                            referenced_relation: to.view_name.clone(),
                            referenced_columns,
                            is_one_to_one: r.is_one_to_one,
                        });
                    }
                }
            }
        }
    }
    let mut all = table_relationships;
    all.extend(through_views);
    all
}

/// One view can select the same table column twice, so a key of n
/// columns can land on more than one set of view columns. Every
/// combination is a relationship of its own.
fn expand(dependency: &KeyDependency) -> Vec<Vec<String>> {
    let mut combinations: Vec<Vec<String>> = vec![Vec::new()];
    for dependency in &dependency.column_dependencies {
        let mut next = Vec::new();
        for so_far in &combinations {
            for column in &dependency.view_columns {
                let mut one = so_far.clone();
                one.push(column.clone());
                next.push(one);
            }
        }
        combinations = next;
    }
    combinations
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|item| item.as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn string_at(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}
