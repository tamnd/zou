//! The catalog turned into the file supabase-js is written against.
//!
//! This is a port of postgres-meta's typescript template, kept close
//! enough to it that the two can be diffed by eye when supabase moves.
//! What it does not port is the string building: the template writes a
//! rough file and lets prettier lay it out, so the shapes here are
//! types rather than text, and `pretty` lays them out at the end.
//!
//! The one thing to know while reading it: an object written with a
//! newline after its brace stays open no matter how short it is,
//! because prettier keeps an object the way the source had it. That is
//! why `Row` is always several lines and `{ Args: never; Returns:
//! number[] }` is one.

use std::collections::HashMap;

use super::catalog::{Arg, Catalog, Column, Function, PgType, Relation, Relationship};
use super::pretty::{
    Doc, concat, group, group_broken, if_break, indent, join, line, print, soft, text,
};
use super::sort::locale_cmp;

/// prettier's default, and the width the file supabase ships was laid
/// out at.
const WIDTH: usize = 80;

/// json, jsonb and text: the only types postgrest will accept as the
/// single unnamed argument of a function.
const CALLABLE_UNNAMED_ARGS: [i64; 3] = [114, 3802, 25];

/// Written out rather than built, because it never varies.
const JSON_TYPE: &str = "\
export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[]";

/// The helper types supabase-js reaches for. They mention the default
/// schema by name, which is always public.
const HELPERS: &str = include_str!("helpers.ts");

/// A TypeScript type, only as far as this file needs one.
enum Ts {
    Name(String),
    Array(Box<Ts>),
    Union(Vec<Ts>),
    /// A type literal, and whether it is one of the ones that stays
    /// open at any width.
    Object(Vec<Field>, bool),
    /// An object of values rather than of types, which is the Constants
    /// table at the end of the file.
    Record(Vec<Field>),
    Tuple(Vec<Ts>),
    Intersection(Vec<Ts>),
}

struct Field {
    key: String,
    optional: bool,
    value: Ts,
}

fn name(text: impl Into<String>) -> Ts {
    Ts::Name(text.into())
}

fn field(key: &str, optional: bool, value: Ts) -> Field {
    Field {
        key: key_text(key),
        optional,
        value,
    }
}

/// A key that is already in the shape it should be printed in, which
/// is the mapped type an empty section is written as.
fn raw_field(key: &str, value: Ts) -> Field {
    Field {
        key: key.to_string(),
        optional: false,
        value,
    }
}

/// The section an empty Tables, Views, Functions, Enums or
/// CompositeTypes gets, which reads as "no keys" to the compiler.
fn nothing() -> Ts {
    Ts::Object(vec![raw_field("[_ in never]", name("never"))], true)
}

fn key_text(key: &str) -> String {
    let mut chars = key.chars();
    let identifier = match chars.next() {
        None => false,
        Some(first) => {
            (first.is_ascii_alphabetic() || first == '_' || first == '$')
                && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
        }
    };
    match identifier {
        true => key.to_string(),
        false => quote(key),
    }
}

/// Double quotes unless the string has more of them than single ones,
/// which is the rule prettier picks a quote by.
fn quote(value: &str) -> String {
    let quote = match value.matches('"').count() > value.matches('\'').count() {
        true => '\'',
        false => '"',
    };
    let mut out = String::with_capacity(value.len() + 2);
    out.push(quote);
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

fn doc_of(ts: &Ts) -> Doc {
    match ts {
        Ts::Name(text_) => text(text_.clone()),
        Ts::Array(element) => match element.as_ref() {
            // A union is the one element type that needs its parens
            // kept, and it is short enough to leave on one line.
            Ts::Union(_) => text(format!("({})[]", print(&doc_of(element), usize::MAX / 4))),
            _ => concat(vec![doc_of(element), text("[]")]),
        },
        // A union that is not a field value has nowhere to wrap to,
        // since the thing holding it is on one line by then. The ones
        // that do wrap are laid out by `field_doc`.
        Ts::Union(members) => join(text(" | "), members.iter().map(doc_of).collect()),
        Ts::Object(fields, open) => object_doc(fields, *open),
        Ts::Record(fields) => record_doc(fields),
        Ts::Tuple(items) => tuple_doc(items),
        Ts::Intersection(members) => join(text(" & "), members.iter().map(doc_of).collect()),
    }
}

/// A union on one line after its key, or one member to a line with a
/// leading bar, which is what a long enum column looks like.
fn union_tail(members: &[Ts]) -> Doc {
    let bar = concat(vec![line(), text("| ")]);
    let members = members.iter().map(|m| indent(2, doc_of(m))).collect();
    group(indent(
        2,
        concat(vec![line(), if_break("| ", ""), join(bar, members)]),
    ))
}

fn object_doc(fields: &[Field], open: bool) -> Doc {
    if fields.is_empty() {
        return text("{}");
    }
    let separator = concat(vec![if_break("", ";"), line()]);
    let body = concat(vec![
        text("{"),
        indent(
            2,
            concat(vec![
                line(),
                join(separator, fields.iter().map(field_doc).collect()),
            ]),
        ),
        line(),
        text("}"),
    ]);
    match open {
        true => group_broken(body),
        false => group(body),
    }
}

fn record_doc(fields: &[Field]) -> Doc {
    if fields.is_empty() {
        return text("{}");
    }
    let separator = concat(vec![text(","), line()]);
    group_broken(concat(vec![
        text("{"),
        indent(
            2,
            concat(vec![
                line(),
                join(separator, fields.iter().map(field_doc).collect()),
            ]),
        ),
        if_break(",", ""),
        line(),
        text("}"),
    ]))
}

fn tuple_doc(items: &[Ts]) -> Doc {
    if items.is_empty() {
        return text("[]");
    }
    let separator = concat(vec![text(","), line()]);
    group(concat(vec![
        text("["),
        indent(
            2,
            concat(vec![
                soft(),
                join(separator, items.iter().map(doc_of).collect()),
            ]),
        ),
        if_break(",", ""),
        soft(),
        text("]"),
    ]))
}

fn field_doc(field: &Field) -> Doc {
    let head = concat(vec![
        text(field.key.clone()),
        text(match field.optional {
            true => "?:",
            false => ":",
        }),
    ]);
    match &field.value {
        // A union carries its own leading space, since when it breaks
        // the key keeps the line to itself.
        Ts::Union(members) if members.len() > 1 => concat(vec![head, union_tail(members)]),
        value => concat(vec![head, text(" "), doc_of(value)]),
    }
}

pub fn render(catalog: &Catalog) -> String {
    let generator = Generator::new(catalog);
    let database = concat(vec![
        text("export type Database = "),
        doc_of(&generator.database()),
    ]);
    let constants = concat(vec![
        text("export const Constants = "),
        doc_of(&generator.constants()),
        text(" as const"),
    ]);
    // The trailing blank line is the one supabase's cli leaves behind:
    // prettier ends the file with a newline and the cli prints it with
    // another. A file that differs from theirs only in whitespace is
    // still a file that shows up in a diff.
    format!(
        "{}\n\n{}\n\n{}\n\n{}\n\n",
        JSON_TYPE,
        print(&database, WIDTH),
        HELPERS.trim_end(),
        print(&constants, WIDTH)
    )
}

/// One function of a schema, with the arguments a caller can pass
/// picked out of the ones the catalog lists.
struct Signature<'a> {
    function: &'a Function,
    in_args: Vec<&'a Arg>,
}

struct Parts<'a> {
    name: &'a str,
    tables: Vec<(&'a Relation, Vec<&'a Relationship>)>,
    views: Vec<(&'a Relation, bool, Vec<&'a Relationship>)>,
    functions: Vec<Signature<'a>>,
    enums: Vec<&'a PgType>,
    composites: Vec<&'a PgType>,
}

struct Generator<'a> {
    catalog: &'a Catalog,
    schema_names: Vec<&'a str>,
    columns: HashMap<i64, Vec<&'a Column>>,
    relation_names: HashMap<i64, &'a str>,
    types: HashMap<i64, &'a PgType>,
    /// The subset of types that are the row type of a table or a view,
    /// which is how a function that takes or returns a whole row is
    /// recognised.
    row_types: HashMap<i64, &'a PgType>,
    schemas: Vec<Parts<'a>>,
}

impl<'a> Generator<'a> {
    fn new(catalog: &'a Catalog) -> Self {
        let mut names: Vec<&'a str> = catalog.schemas.iter().map(|s| s.name.as_str()).collect();
        names.sort_by(|a, b| locale_cmp(a, b));

        let mut types = HashMap::new();
        let mut row_types = HashMap::new();
        for pg_type in &catalog.types {
            types.insert(pg_type.id, pg_type);
            if pg_type.type_relation_id.is_some() {
                row_types.insert(pg_type.id, pg_type);
            }
        }

        let relations = || {
            catalog
                .tables
                .iter()
                .chain(&catalog.foreign_tables)
                .chain(&catalog.views)
                .chain(&catalog.materialized_views)
        };
        let mut columns: HashMap<i64, Vec<&Column>> = HashMap::new();
        let mut relation_names = HashMap::new();
        for relation in relations() {
            columns.insert(relation.id, Vec::new());
            relation_names.insert(relation.id, relation.name.as_str());
        }
        for column in &catalog.columns {
            if let Some(list) = columns.get_mut(&column.table_id) {
                list.push(column);
            }
        }
        for list in columns.values_mut() {
            list.sort_by(|a, b| locale_cmp(&a.name, &b.name));
        }

        let mut relationships: Vec<&Relationship> = catalog.relationships.iter().collect();
        relationships.sort_by(|a, b| {
            locale_cmp(&a.foreign_key_name, &b.foreign_key_name)
                .then_with(|| locale_cmp(&a.referenced_relation, &b.referenced_relation))
                .then_with(|| {
                    locale_cmp(
                        &as_json(&a.referenced_columns),
                        &as_json(&b.referenced_columns),
                    )
                })
        });

        let mut generator = Generator {
            catalog,
            schema_names: names.clone(),
            columns,
            relation_names,
            types,
            row_types,
            schemas: Vec::new(),
        };

        let of_schema = |schema: &str, relation: &Relation| -> Vec<&'a Relationship> {
            relationships
                .iter()
                .filter(|r| {
                    r.schema == schema
                        && r.referenced_schema == schema
                        && r.relation == relation.name
                })
                .copied()
                .collect()
        };

        for schema in &names {
            let mut parts = Parts {
                name: schema,
                tables: Vec::new(),
                views: Vec::new(),
                functions: Vec::new(),
                enums: Vec::new(),
                composites: Vec::new(),
            };
            for table in catalog.tables.iter().chain(&catalog.foreign_tables) {
                if table.schema == *schema {
                    parts.tables.push((table, of_schema(schema, table)));
                }
            }
            for view in &catalog.views {
                if view.schema == *schema {
                    parts
                        .views
                        .push((view, view.is_updatable, of_schema(schema, view)));
                }
            }
            // A materialized view has columns and relationships like a
            // view, and no way to write through it.
            for view in &catalog.materialized_views {
                if view.schema == *schema {
                    parts.views.push((view, false, of_schema(schema, view)));
                }
            }
            for pg_type in &catalog.types {
                if pg_type.schema == *schema {
                    if !pg_type.enums.is_empty() {
                        parts.enums.push(pg_type);
                    }
                    if !pg_type.attributes.is_empty() {
                        parts.composites.push(pg_type);
                    }
                }
            }
            for function in &catalog.functions {
                if function.schema != *schema {
                    continue;
                }
                let in_args: Vec<&Arg> = function
                    .args
                    .iter()
                    .filter(|a| matches!(a.mode.as_str(), "in" | "inout" | "variadic"))
                    .collect();
                if generator.is_callable(&in_args) {
                    parts.functions.push(Signature { function, in_args });
                }
            }
            parts
                .tables
                .sort_by(|a, b| locale_cmp(&a.0.name, &b.0.name));
            parts.views.sort_by(|a, b| locale_cmp(&a.0.name, &b.0.name));
            parts
                .functions
                .sort_by(|a, b| locale_cmp(&a.function.name, &b.function.name));
            parts.enums.sort_by(|a, b| locale_cmp(&a.name, &b.name));
            parts
                .composites
                .sort_by(|a, b| locale_cmp(&a.name, &b.name));
            generator.schemas.push(parts);
        }
        generator
    }

    /// Whether postgrest would ever call this function. A function
    /// whose unnamed argument it cannot fill in is left out of the file
    /// rather than written down as callable.
    fn is_callable(&self, in_args: &[&Arg]) -> bool {
        // Every argument named is every argument postgrest can pass by
        // name, which includes a function that takes none at all.
        if !in_args.iter().any(|a| a.name.is_empty()) {
            return true;
        }
        let unnamed_are_fillable = in_args.iter().all(|a| match a.name.is_empty() {
            // An unnamed argument postgrest can put a body in is one it
            // can leave out as well, so long as there is a default.
            true => a.has_default && CALLABLE_UNNAMED_ARGS.contains(&a.type_id),
            false => true,
        });
        if unnamed_are_fillable {
            return true;
        }
        // Past here there is an unnamed argument with no default, and
        // the only shape postgrest calls is exactly one of them.
        match in_args {
            [only] => {
                CALLABLE_UNNAMED_ARGS.contains(&only.type_id)
                    // A single unnamed row argument is a computed
                    // column, which is a call postgrest makes on the
                    // caller's behalf, and it is written down even when
                    // it does not qualify so that the reason can be.
                    || self.row_types.contains_key(&only.type_id)
            }
            _ => false,
        }
    }

    fn parts(&self, schema: &str) -> Option<&Parts<'a>> {
        self.schemas.iter().find(|p| p.name == schema)
    }

    fn columns_of(&self, relation: i64) -> &[&'a Column] {
        match self.columns.get(&relation) {
            Some(columns) => columns,
            None => &[],
        }
    }

    /// The name of the relation a function returns rows of, which is
    /// what lets a caller select columns through an rpc call.
    fn returned_relation(&self, relation_id: Option<i64>, return_type_id: i64) -> Option<&'a str> {
        let relation_id = relation_id?;
        if let Some(name) = self.relation_names.get(&relation_id) {
            return Some(name);
        }
        // A composite type has no relation of its own, so its own name
        // stands in and sub fields stay selectable.
        self.row_types.get(&return_type_id).map(|t| t.name.as_str())
    }

    fn database(&self) -> Ts {
        let schemas = self
            .schemas
            .iter()
            .map(|parts| field(parts.name, false, self.schema(parts)))
            .collect();
        Ts::Object(schemas, true)
    }

    fn schema(&self, parts: &Parts<'a>) -> Ts {
        let tables = match parts.tables.is_empty() {
            true => nothing(),
            false => Ts::Object(
                parts
                    .tables
                    .iter()
                    .map(|(table, relationships)| {
                        field(&table.name, false, self.table(parts, table, relationships))
                    })
                    .collect(),
                true,
            ),
        };
        let views = match parts.views.is_empty() {
            true => nothing(),
            false => Ts::Object(
                parts
                    .views
                    .iter()
                    .map(|(view, updatable, relationships)| {
                        field(
                            &view.name,
                            false,
                            self.view(parts, view, *updatable, relationships),
                        )
                    })
                    .collect(),
                true,
            ),
        };
        let functions = match parts.functions.is_empty() {
            true => nothing(),
            false => Ts::Object(self.functions(parts), true),
        };
        let enums = match parts.enums.is_empty() {
            true => nothing(),
            false => Ts::Object(
                parts
                    .enums
                    .iter()
                    .map(|e| {
                        field(
                            &e.name,
                            false,
                            Ts::Union(e.enums.iter().map(|v| name(quote(v))).collect()),
                        )
                    })
                    .collect(),
                true,
            ),
        };
        let composites = match parts.composites.is_empty() {
            true => nothing(),
            false => Ts::Object(
                parts
                    .composites
                    .iter()
                    .map(|c| {
                        let attributes = c
                            .attributes
                            .iter()
                            .map(|a| {
                                let ts = match self.types.get(&a.type_id) {
                                    Some(t) => self.pg_type(parts.name, &t.name),
                                    None => name("unknown"),
                                };
                                field(&a.name, false, nullable(ts, true))
                            })
                            .collect();
                        field(&c.name, false, Ts::Object(attributes, true))
                    })
                    .collect(),
                true,
            ),
        };
        Ts::Object(
            vec![
                field("Tables", false, tables),
                field("Views", false, views),
                field("Functions", false, functions),
                field("Enums", false, enums),
                field("CompositeTypes", false, composites),
            ],
            true,
        )
    }

    fn table(&self, parts: &Parts<'a>, table: &Relation, relationships: &[&'a Relationship]) -> Ts {
        let insert = self
            .columns_of(table.id)
            .iter()
            .map(|column| match column.identity_generation.as_deref() {
                Some("ALWAYS") => field(&column.name, true, name("never")),
                _ => self.column(
                    parts.name,
                    column,
                    column.is_nullable || column.is_identity || column.default_value.is_some(),
                ),
            })
            .collect();
        let update = self
            .columns_of(table.id)
            .iter()
            .map(|column| match column.identity_generation.as_deref() {
                Some("ALWAYS") => field(&column.name, true, name("never")),
                _ => self.column(parts.name, column, true),
            })
            .collect();
        Ts::Object(
            vec![
                field("Row", false, self.row(parts, table)),
                field("Insert", false, Ts::Object(insert, true)),
                field("Update", false, Ts::Object(update, true)),
                relationships_field(relationships),
            ],
            true,
        )
    }

    fn view(
        &self,
        parts: &Parts<'a>,
        view: &Relation,
        updatable: bool,
        relationships: &[&'a Relationship],
    ) -> Ts {
        let mut fields = vec![field("Row", false, self.row(parts, view))];
        if updatable {
            // Everything a view can be written through is optional and
            // nullable, since the view is not the thing holding the
            // constraint.
            let writable = || -> Vec<Field> {
                self.columns_of(view.id)
                    .iter()
                    .map(|column| match column.is_updatable {
                        false => field(&column.name, true, name("never")),
                        true => Field {
                            key: key_text(&column.name),
                            optional: true,
                            value: nullable(self.pg_type(parts.name, &column.format), true),
                        },
                    })
                    .collect()
            };
            fields.push(field("Insert", false, Ts::Object(writable(), true)));
            fields.push(field("Update", false, Ts::Object(writable(), true)));
        }
        fields.push(relationships_field(relationships));
        Ts::Object(fields, true)
    }

    /// The columns, and then the functions of one row, which postgrest
    /// serves as though they were columns too.
    fn row(&self, parts: &Parts<'a>, relation: &Relation) -> Ts {
        let mut fields: Vec<Field> = self
            .columns_of(relation.id)
            .iter()
            .map(|column| self.column(parts.name, column, false))
            .collect();
        for signature in &parts.functions {
            if signature.function.argument_types == relation.name {
                let returns = self.return_type(parts.name, signature.function);
                fields.push(field(
                    &signature.function.name,
                    false,
                    nullable(returns, true),
                ));
            }
        }
        Ts::Object(fields, true)
    }

    fn column(&self, schema: &str, column: &Column, optional: bool) -> Field {
        Field {
            key: key_text(&column.name),
            optional,
            value: nullable(self.pg_type(schema, &column.format), column.is_nullable),
        }
    }

    fn functions(&self, parts: &Parts<'a>) -> Vec<Field> {
        let mut groups: Vec<(&str, Vec<&Signature<'a>>)> = Vec::new();
        for signature in &parts.functions {
            match groups.last_mut() {
                Some((name, group)) if *name == signature.function.name => group.push(signature),
                _ => groups.push((&signature.function.name, vec![signature])),
            }
        }
        groups
            .into_iter()
            .map(|(name, mut group)| {
                group.sort_by(|a, b| {
                    locale_cmp(&a.function.argument_types, &b.function.argument_types)
                        .then_with(|| locale_cmp(&a.function.return_type, &b.function.return_type))
                });
                let overloads: Vec<Ts> = group
                    .iter()
                    .map(|signature| self.signature(parts, &group, signature))
                    .collect();
                field(name, false, Ts::Union(overloads))
            })
            .collect()
    }

    fn signature(
        &self,
        parts: &Parts<'a>,
        group: &[&Signature<'a>],
        signature: &Signature<'a>,
    ) -> Ts {
        let function = signature.function;
        let mut args = name("never");
        let mut returns = self.return_type(parts.name, function);

        if let Some(conflict) = self.conflict(parts, group, signature) {
            if !signature.in_args.is_empty() {
                args = self.args(parts.name, signature);
            }
            returns = refused(conflict);
        } else if self.is_unusable_row_argument(function, &signature.in_args) {
            if !signature.in_args.is_empty() {
                args = self.args(parts.name, signature);
            }
            returns = refused(format!(
                "the function {}.{} with parameter or with a single unnamed json/jsonb parameter, but no matches were found in the schema cache",
                parts.name, function.name
            ));
        } else if !signature.in_args.is_empty() {
            args = self.args(parts.name, signature);
        }

        // Rows come back as an array only when the planner expects more
        // than one of them, which is what postgrest reads too.
        let many = function.rows.is_some_and(|rows| rows > 1.0);
        if function.is_set_returning_function && many {
            returns = Ts::Array(Box::new(returns));
        }
        let mut fields = vec![field("Args", false, args), field("Returns", false, returns)];
        if let Some(setof) = self.setof_options(function, many) {
            fields.push(field("SetofOptions", false, setof));
        }
        Ts::Object(fields, false)
    }

    fn args(&self, schema: &str, signature: &Signature<'a>) -> Ts {
        Ts::Object(
            signature
                .in_args
                .iter()
                .map(|arg| {
                    let ts = match self.types.get(&arg.type_id) {
                        Some(t) => self.pg_type(schema, &t.name),
                        None => name("unknown"),
                    };
                    field(&arg.name, arg.has_default, ts)
                })
                .collect(),
            false,
        )
    }

    /// What a caller can select through this call: the relation it
    /// reads from and the relation it lands on.
    fn setof_options(&self, function: &Function, many: bool) -> Option<Ts> {
        let returned =
            self.returned_relation(function.return_type_relation_id, function.return_type_id);
        let returns_rows_of_relation =
            function.is_set_returning_function && function.return_type_relation_id.is_some();
        let mut options =
            returned.map(|to| setof("*", to, !many, function.is_set_returning_function));
        // A function that takes one whole row reads from that row's
        // relation rather than from anywhere.
        if let [only] = function.args.as_slice()
            && let Some(row_type) = self.row_types.get(&only.type_id)
            && let Some(to) = returned
        {
            options = match returns_rows_of_relation {
                true => Some(setof(&row_type.format, to, !many, true)),
                false => Some(setof(&row_type.format, to, true, false)),
            };
        }
        options
    }

    /// The return type as a caller sees it: the columns of a returns
    /// table, the row of a relation, or a plain type.
    fn return_type(&self, schema: &str, function: &Function) -> Ts {
        let table_args: Vec<&Arg> = function.args.iter().filter(|a| a.mode == "table").collect();
        if !table_args.is_empty() {
            return Ts::Object(
                table_args
                    .iter()
                    .map(|arg| {
                        let ts = match self.types.get(&arg.type_id) {
                            Some(t) => self.pg_type(schema, &t.name),
                            None => name("unknown"),
                        };
                        field(&arg.name, false, ts)
                    })
                    .collect(),
                true,
            );
        }
        if let Some(relation_id) = function.return_type_relation_id
            && let Some(parts) = self.parts(schema)
        {
            let relation = parts
                .tables
                .iter()
                .map(|(table, _)| *table)
                .chain(parts.views.iter().map(|(view, _, _)| *view))
                .find(|relation| relation.id == relation_id);
            if let Some(relation) = relation {
                return Ts::Object(
                    self.columns_of(relation.id)
                        .iter()
                        .map(|column| self.column(schema, column, false))
                        .collect(),
                    true,
                );
            }
        }
        match self.types.get(&function.return_type_id) {
            Some(t) => self.pg_type(schema, &t.name),
            None => name("unknown"),
        }
    }

    /// A single unnamed row argument that leads nowhere. postgrest
    /// cannot call it, and saying so in the type is more use than
    /// leaving it out.
    fn is_unusable_row_argument(&self, function: &Function, in_args: &[&Arg]) -> bool {
        match in_args {
            [only] if only.name.is_empty() => {
                self.row_types.contains_key(&only.type_id)
                    && self
                        .returned_relation(
                            function.return_type_relation_id,
                            function.return_type_id,
                        )
                        .is_none()
            }
            _ => false,
        }
    }

    /// Two functions of one name that postgrest cannot tell apart. The
    /// type says which two, since the fix is in the database.
    fn conflict(
        &self,
        parts: &Parts<'a>,
        group: &[&Signature<'a>],
        signature: &Signature<'a>,
    ) -> Option<String> {
        if group.len() <= 1 {
            return None;
        }
        let function = signature.function;
        if signature.in_args.is_empty() {
            let against = group.iter().find(|other| {
                !std::ptr::eq(other.function, function)
                    && matches!(other.in_args.as_slice(), [only] if only.name.is_empty() && only.has_default)
            });
            if let Some(against) = against {
                let returns = match self.types.get(&against.function.return_type_id) {
                    Some(t) => t.name.as_str(),
                    None => "unknown",
                };
                return Some(format!(
                    "Could not choose the best candidate function between: {schema}.{name}(), {schema}.{name}( => {returns}). Try renaming the parameters or the function itself in the database so function overloading can be resolved",
                    schema = parts.name,
                    name = function.name,
                ));
            }
        }
        if let [only] = signature.in_args.as_slice()
            && !only.name.is_empty()
        {
            let mut against: Vec<&&Signature<'a>> = group
                .iter()
                .filter(|other| {
                    !std::ptr::eq(other.function, function)
                        && matches!(other.in_args.as_slice(), [theirs] if theirs.name == only.name && theirs.type_id != only.type_id)
                })
                .collect();
            if !against.is_empty() {
                let mut all = vec![&signature];
                all.append(&mut against);
                all.sort_by_key(|s| s.in_args.first().map(|a| a.type_id).unwrap_or(0));
                let list: Vec<String> = all
                    .iter()
                    .map(|other| {
                        let args: Vec<String> = other
                            .in_args
                            .iter()
                            .map(|arg| {
                                let type_name = match self.types.get(&arg.type_id) {
                                    Some(t) => t.name.as_str(),
                                    None => "unknown",
                                };
                                format!("{} => {}", arg.name, type_name)
                            })
                            .collect();
                        format!("{}.{}({})", parts.name, function.name, args.join(", "))
                    })
                    .collect();
                return Some(format!(
                    "Could not choose the best candidate function between: {}. Try renaming the parameters or the function itself in the database so function overloading can be resolved",
                    list.join(", ")
                ));
            }
        }
        None
    }

    /// A postgres type as TypeScript. Anything with no counterpart is
    /// unknown rather than a guess, so a client has to say what it
    /// meant.
    fn pg_type(&self, schema: &str, pg_type: &str) -> Ts {
        match pg_type {
            "bool" => return name("boolean"),
            "int2" | "int4" | "int8" | "float4" | "float8" | "numeric" => return name("number"),
            "bytea" | "bpchar" | "varchar" | "date" | "text" | "citext" | "time" | "timetz"
            | "timestamp" | "timestamptz" | "uuid" | "vector" | "interval" => {
                return name("string");
            }
            "json" | "jsonb" => return name("Json"),
            "void" => return name("undefined"),
            "record" => return name("Record<string, unknown>"),
            _ => {}
        }
        if let Some(element) = pg_type.strip_prefix('_') {
            return Ts::Array(Box::new(self.pg_type(schema, element)));
        }
        let pick = |candidates: Vec<&'a PgType>| -> Option<&'a PgType> {
            candidates
                .iter()
                .find(|t| t.schema == schema)
                .or(candidates.first())
                .copied()
        };
        let named = |kind: &str, t: &PgType| {
            name(format!(
                "Database[{}][\"{}\"][{}]",
                quote(&t.schema),
                kind,
                quote(&t.name)
            ))
        };

        let enums = self
            .catalog
            .types
            .iter()
            .filter(|t| t.name == pg_type && !t.enums.is_empty())
            .collect();
        if let Some(chosen) = pick(enums) {
            if self.schema_names.contains(&chosen.schema.as_str()) {
                return named("Enums", chosen);
            }
            // An enum outside the generated schemas still has variants,
            // and they are more use than unknown.
            return Ts::Union(chosen.enums.iter().map(|v| name(quote(v))).collect());
        }
        let composites = self
            .catalog
            .types
            .iter()
            .filter(|t| t.name == pg_type && !t.attributes.is_empty())
            .collect();
        if let Some(chosen) = pick(composites) {
            return match self.schema_names.contains(&chosen.schema.as_str()) {
                true => named("CompositeTypes", chosen),
                false => name("unknown"),
            };
        }
        let tables = self.catalog.tables.iter().filter(|t| t.name == pg_type);
        if let Some(chosen) = tables
            .clone()
            .find(|t| t.schema == schema)
            .or_else(|| tables.clone().next())
        {
            return match self.schema_names.contains(&chosen.schema.as_str()) {
                true => name(format!(
                    "Database[{}][\"Tables\"][{}][\"Row\"]",
                    quote(&chosen.schema),
                    quote(&chosen.name)
                )),
                false => name("unknown"),
            };
        }
        let views = self.catalog.views.iter().filter(|v| v.name == pg_type);
        if let Some(chosen) = views
            .clone()
            .find(|v| v.schema == schema)
            .or_else(|| views.clone().next())
        {
            return match self.schema_names.contains(&chosen.schema.as_str()) {
                true => name(format!(
                    "Database[{}][\"Views\"][{}][\"Row\"]",
                    quote(&chosen.schema),
                    quote(&chosen.name)
                )),
                false => name("unknown"),
            };
        }
        name("unknown")
    }

    fn constants(&self) -> Ts {
        Ts::Record(
            self.schemas
                .iter()
                .map(|parts| {
                    let enums = Ts::Record(
                        parts
                            .enums
                            .iter()
                            .map(|e| {
                                field(
                                    &e.name,
                                    false,
                                    Ts::Tuple(e.enums.iter().map(|v| name(quote(v))).collect()),
                                )
                            })
                            .collect(),
                    );
                    field(
                        parts.name,
                        false,
                        Ts::Record(vec![field("Enums", false, enums)]),
                    )
                })
                .collect(),
        )
    }
}

fn setof(from: &str, to: &str, one_to_one: bool, set_return: bool) -> Ts {
    Ts::Object(
        vec![
            field("from", false, name(quote(from))),
            field("to", false, name(quote(to))),
            field("isOneToOne", false, name(one_to_one.to_string())),
            field("isSetofReturn", false, name(set_return.to_string())),
        ],
        true,
    )
}

fn refused(reason: String) -> Ts {
    Ts::Intersection(vec![
        Ts::Object(vec![field("error", false, name("true"))], false),
        name(quote(&reason)),
    ])
}

fn relationships_field(relationships: &[&Relationship]) -> Field {
    field(
        "Relationships",
        false,
        Ts::Tuple(
            relationships
                .iter()
                .map(|r| {
                    Ts::Object(
                        vec![
                            field("foreignKeyName", false, name(quote(&r.foreign_key_name))),
                            field(
                                "columns",
                                false,
                                Ts::Tuple(r.columns.iter().map(|c| name(quote(c))).collect()),
                            ),
                            field("isOneToOne", false, name(r.is_one_to_one.to_string())),
                            field(
                                "referencedRelation",
                                false,
                                name(quote(&r.referenced_relation)),
                            ),
                            field(
                                "referencedColumns",
                                false,
                                Ts::Tuple(
                                    r.referenced_columns
                                        .iter()
                                        .map(|c| name(quote(c)))
                                        .collect(),
                                ),
                            ),
                        ],
                        true,
                    )
                })
                .collect(),
        ),
    )
}

/// Null is a value a column can hold, so it is a value the type has to
/// admit. unknown already admits it.
fn nullable(ts: Ts, is_nullable: bool) -> Ts {
    if !is_nullable {
        return ts;
    }
    match ts {
        Ts::Name(text) if text == "unknown" || text == "any" => Ts::Name(text),
        // An enum written out in place is already a union, and null
        // joins it rather than wrapping it, so that a long one breaks
        // one variant to a line.
        Ts::Union(mut members) => {
            members.push(name("null"));
            Ts::Union(members)
        }
        ts => Ts::Union(vec![ts, name("null")]),
    }
}

fn as_json(values: &[String]) -> String {
    let quoted: Vec<String> = values.iter().map(|v| quote(v)).collect();
    format!("[{}]", quoted.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codegen::catalog::{Attribute, Schema};

    /// A catalog with one table in it, which is enough to see what
    /// happens to a column, and the hooks to add the rest.
    fn catalog() -> Catalog {
        Catalog {
            schemas: vec![Schema {
                name: "public".to_string(),
            }],
            tables: vec![Relation {
                id: 1,
                schema: "public".to_string(),
                name: "users".to_string(),
                is_updatable: false,
            }],
            foreign_tables: Vec::new(),
            views: Vec::new(),
            materialized_views: Vec::new(),
            columns: Vec::new(),
            relationships: Vec::new(),
            functions: Vec::new(),
            types: Vec::new(),
        }
    }

    fn column(name: &str, format: &str) -> Column {
        Column {
            table_id: 1,
            name: name.to_string(),
            format: format.to_string(),
            default_value: None,
            is_identity: false,
            identity_generation: None,
            is_nullable: false,
            is_updatable: true,
        }
    }

    /// The Row block of the one table, which is where a column ends up.
    fn row_of(catalog: &Catalog) -> String {
        let file = render(catalog);
        let start = file.find("Row: {").expect("a row");
        let end = file[start..].find("\n        }").expect("an end");
        file[start..start + end].to_string()
    }

    #[test]
    fn a_column_carries_its_type_and_a_null_when_it_can_be_one() {
        let mut catalog = catalog();
        catalog.columns = vec![column("id", "int8"), {
            let mut email = column("email", "text");
            email.is_nullable = true;
            email
        }];
        let row = row_of(&catalog);
        assert!(row.contains("id: number"), "{row}");
        assert!(row.contains("email: string | null"), "{row}");
    }

    #[test]
    fn a_type_with_no_counterpart_is_unknown_rather_than_a_guess() {
        let mut catalog = catalog();
        catalog.columns = vec![column("spot", "point")];
        assert!(row_of(&catalog).contains("spot: unknown"));
    }

    /// unknown already admits null, so saying so again would only be
    /// noise in the diff.
    #[test]
    fn unknown_does_not_pick_up_a_null() {
        let mut catalog = catalog();
        catalog.columns = vec![{
            let mut spot = column("spot", "point");
            spot.is_nullable = true;
            spot
        }];
        let row = row_of(&catalog);
        assert!(row.contains("spot: unknown"), "{row}");
        assert!(!row.contains("null"), "{row}");
    }

    #[test]
    fn an_array_column_is_an_array_of_what_it_holds() {
        let mut catalog = catalog();
        catalog.columns = vec![column("tags", "_text")];
        assert!(row_of(&catalog).contains("tags: string[]"));
    }

    /// A name that is not an identifier has to be quoted, or the file
    /// does not parse.
    #[test]
    fn a_name_that_is_not_an_identifier_is_quoted() {
        let mut catalog = catalog();
        catalog.columns = vec![column("full name", "text"), column("2fa", "bool")];
        let row = row_of(&catalog);
        assert!(row.contains("\"full name\": string"), "{row}");
        assert!(row.contains("\"2fa\": boolean"), "{row}");
    }

    #[test]
    fn columns_come_out_in_the_order_a_reader_expects() {
        let mut catalog = catalog();
        catalog.columns = vec![
            column("posts", "int4"),
            column("post_stats", "int4"),
            column("Alias", "text"),
            column("alias", "text"),
        ];
        let row = row_of(&catalog);
        let at = |name: &str| row.find(name).expect(name);
        assert!(at("alias: ") < at("Alias: "), "{row}");
        assert!(
            at("Alias: ") < at("post_stats") && at("post_stats") < at("posts:"),
            "{row}"
        );
    }

    /// What a column can be left out of on insert is the whole of what
    /// a client is allowed to leave out, so it is worth its own test.
    #[test]
    fn insert_leaves_out_what_the_database_can_fill_in() {
        let mut catalog = catalog();
        catalog.columns = vec![
            column("required", "text"),
            {
                let mut with_default = column("with_default", "text");
                with_default.default_value = Some("'x'".to_string());
                with_default
            },
            {
                let mut always = column("always", "int8");
                always.is_identity = true;
                always.identity_generation = Some("ALWAYS".to_string());
                always
            },
            {
                let mut by_default = column("by_default", "int8");
                by_default.is_identity = true;
                by_default.identity_generation = Some("BY DEFAULT".to_string());
                by_default
            },
        ];
        let file = render(&catalog);
        let start = file.find("Insert: {").expect("an insert");
        let insert = &file[start..start + file[start..].find("\n        }").expect("an end")];
        assert!(insert.contains("required: string"), "{insert}");
        assert!(insert.contains("with_default?: string"), "{insert}");
        assert!(insert.contains("by_default?: number"), "{insert}");
        // Generated always is not a column a client may write at all.
        assert!(insert.contains("always?: never"), "{insert}");
    }

    #[test]
    fn update_asks_for_nothing_and_allows_everything() {
        let mut catalog = catalog();
        catalog.columns = vec![column("required", "text")];
        let file = render(&catalog);
        let start = file.find("Update: {").expect("an update");
        let update = &file[start..start + file[start..].find("\n        }").expect("an end")];
        assert!(update.contains("required?: string"), "{update}");
    }

    #[test]
    fn a_schema_with_nothing_in_it_still_has_every_section() {
        let mut catalog = catalog();
        catalog.tables.clear();
        let file = render(&catalog);
        for section in ["Tables", "Views", "Functions", "Enums", "CompositeTypes"] {
            assert!(
                file.contains(&format!("{section}: {{\n      [_ in never]: never")),
                "{file}"
            );
        }
        assert!(file.contains("export const Constants = {\n  public: {\n    Enums: {},\n  },\n}"));
    }

    #[test]
    fn an_enum_is_a_union_in_the_types_and_an_array_in_the_constants() {
        let mut catalog = catalog();
        catalog.tables.clear();
        catalog.types = vec![PgType {
            id: 100,
            name: "mood".to_string(),
            schema: "public".to_string(),
            format: "mood".to_string(),
            enums: vec!["sad".to_string(), "ok".to_string()],
            attributes: Vec::new(),
            type_relation_id: None,
        }];
        let file = render(&catalog);
        assert!(file.contains("      mood: \"sad\" | \"ok\"\n"), "{file}");
        assert!(file.contains("      mood: [\"sad\", \"ok\"],\n"), "{file}");
    }

    #[test]
    fn a_composite_type_is_an_object_of_columns_that_may_all_be_null() {
        let mut catalog = catalog();
        catalog.tables.clear();
        catalog.types = vec![PgType {
            id: 200,
            name: "posted_at".to_string(),
            schema: "public".to_string(),
            format: "posted_at".to_string(),
            enums: Vec::new(),
            attributes: vec![Attribute {
                name: "city".to_string(),
                type_id: 25,
            }],
            type_relation_id: None,
        }];
        catalog.types.push(PgType {
            id: 25,
            name: "text".to_string(),
            schema: "pg_catalog".to_string(),
            format: "text".to_string(),
            enums: Vec::new(),
            attributes: Vec::new(),
            type_relation_id: None,
        });
        let file = render(&catalog);
        assert!(
            file.contains("posted_at: {\n        city: string | null\n      }"),
            "{file}"
        );
    }

    /// The relationships block is what a client follows to embed, so
    /// the names in it have to be the ones postgrest answers to.
    #[test]
    fn a_foreign_key_becomes_a_relationship_the_client_can_follow() {
        let mut catalog = catalog();
        catalog.columns = vec![column("id", "int8")];
        catalog.relationships = vec![Relationship {
            foreign_key_name: "users_team_id_fkey".to_string(),
            schema: "public".to_string(),
            relation: "users".to_string(),
            columns: vec!["team_id".to_string()],
            referenced_schema: "public".to_string(),
            referenced_relation: "teams".to_string(),
            referenced_columns: vec!["id".to_string()],
            is_one_to_one: false,
        }];
        let file = render(&catalog);
        assert!(
            file.contains(
                "        Relationships: [\n          {\n            foreignKeyName: \"users_team_id_fkey\"\n            columns: [\"team_id\"]\n            isOneToOne: false\n            referencedRelation: \"teams\"\n            referencedColumns: [\"id\"]\n          },\n        ]"
            ),
            "{file}"
        );
    }

    /// A relationship that leaves the schemas being generated is one a
    /// client cannot follow, so it is not written down.
    #[test]
    fn a_relationship_into_another_schema_is_left_out() {
        let mut catalog = catalog();
        catalog.columns = vec![column("id", "int8")];
        catalog.relationships = vec![Relationship {
            foreign_key_name: "users_secret_id_fkey".to_string(),
            schema: "public".to_string(),
            relation: "users".to_string(),
            columns: vec!["secret_id".to_string()],
            referenced_schema: "hidden".to_string(),
            referenced_relation: "secrets".to_string(),
            referenced_columns: vec!["id".to_string()],
            is_one_to_one: false,
        }];
        assert!(render(&catalog).contains("Relationships: []"));
    }

    #[test]
    fn the_file_ends_the_way_the_supabase_cli_ends_it() {
        let file = render(&catalog());
        assert!(file.starts_with("export type Json =\n"), "{file}");
        assert!(file.ends_with("} as const\n\n"), "{file}");
        assert!(
            file.contains("\ntype DatabaseWithoutInternals = Omit<Database, "),
            "{file}"
        );
    }

    #[test]
    fn a_string_is_quoted_the_way_prettier_would_quote_it() {
        assert_eq!(quote("plain"), "\"plain\"");
        assert_eq!(quote("it's"), "\"it's\"");
        // More double quotes than single ones, so single wins and only
        // the single one needs escaping.
        assert_eq!(quote("\"a\" 'b' \"c\""), "'\"a\" \\'b\\' \"c\"'");
        assert_eq!(quote("a\\b\nc"), "\"a\\\\b\\nc\"");
    }

    #[test]
    fn a_key_is_quoted_only_when_it_has_to_be() {
        assert_eq!(key_text("plain"), "plain");
        assert_eq!(key_text("_private"), "_private");
        assert_eq!(key_text("a1"), "a1");
        assert_eq!(key_text("1a"), "\"1a\"");
        assert_eq!(key_text("order status"), "\"order status\"");
        assert_eq!(key_text(""), "\"\"");
    }
}
