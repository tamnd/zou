//! Function calls, the /rest/v1/rpc/{fn} surface.
//!
//! PostgREST turns a request into a call with named notation, so
//! callers hit overloads, defaults, and variadic parameters exactly
//! as SQL would resolve them. The work splits
//! three ways: [`INTROSPECT_SQL`] pulls every overload of a name
//! out of pg_proc, [`choose`] picks the one the supplied argument
//! names identify with PostgREST's PGRST202 and PGRST203 errors
//! when the answer is none or several, and the call builders bind
//! arguments the way each method carries them, a json body as one
//! parameter unpacked per argument, query string values as one text
//! parameter each cast to the declared type.
//!
//! What the call returns decides the response shape downstream, so
//! [`Routine`] also carries the return contract: void, a scalar, a
//! set of scalars, or rows, and for rows whether the type is a real
//! table's, which is what lets embeds resolve on the result.

use crate::catalog::Catalog;
use crate::mutate::Represented;
use crate::plan::{PlanError, Query, plan_from};
use crate::sql::{Sql, quote_ident};

/// The CTE alias function results travel under when the select
/// grammar applies to them.
pub const SOURCE: &str = "_zou_rpc";

/// The column a call that hands back a value rather than a row
/// travels under. Naming it here rather than letting postgres name
/// the output column after the function keeps the wrap independent
/// of what the function was called.
pub const VALUE: &str = "_zou_val";

/// One input argument of one overload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arg {
    /// Empty when the argument is unnamed, which only the single
    /// json parameter calling convention can reach.
    pub name: String,
    /// format_type output, spliced into casts verbatim. It comes
    /// from the catalog, never from the client.
    pub type_name: String,
    pub has_default: bool,
    pub variadic: bool,
}

/// What a function hands back, which picks the response shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetKind {
    Void,
    Scalar,
    /// Rows. When the return type is a table's own rowtype the name
    /// rides along and embeds resolve against it.
    Composite {
        table: Option<String>,
    },
}

/// One overload of the requested function, straight off
/// [`INTROSPECT_SQL`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Routine {
    pub args: Vec<Arg>,
    pub kind: RetKind,
    pub returns_set: bool,
    pub volatile: bool,
}

/// Every overload of one function name in one schema. Bind the
/// schema as $1 and the function name as $2. Nine columns per row:
/// input argument names (empty string for unnamed), their types via
/// format_type, a variadic flag per argument, the count of trailing
/// defaults, then proretset, provolatile = 'v', the return type's
/// name, the table whose rowtype it is when there is one, and
/// whether the result is a row rather than a value.
///
/// proallargtypes and proargmodes only exist once OUT or TABLE
/// arguments appear, so both coalesce back to the plain input
/// columns, and the mode filter keeps i, b, and v: IN, INOUT, and
/// VARIADIC are what a caller can supply.
///
/// A domain is not a type of its own here. `bases` walks typbasetype
/// down to whatever the domain was declared over, and every question
/// about the return type is asked of that, because a domain over a
/// table's rowtype returns rows and postgres will not say so.
pub const INTROSPECT_SQL: &str = "\
with recursive bases as (
  select oid, typbasetype, coalesce(nullif(typbasetype, 0), oid) as base
    from pg_type
  union
  select t.oid, b.typbasetype, coalesce(nullif(b.typbasetype, 0), b.oid)
    from bases t
    join pg_type b on b.oid = t.typbasetype
),
base_types as (select oid, base from bases where typbasetype = 0),
fns as (
  select p.oid, p.pronargdefaults, p.proretset, p.provolatile, p.prorettype,
         coalesce(p.proallargtypes, p.proargtypes::oid[]) as types,
         coalesce(p.proargmodes,
                  array_fill('i'::\"char\",
                             array[coalesce(cardinality(p.proargtypes::oid[]), 0)])) as modes,
         p.proargnames as names
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
   where n.nspname = $1 and p.proname = $2 and p.prokind = 'f'
)
select coalesce((select array_agg(coalesce(f.names[u.ord], '') order by u.ord)
                   from unnest(f.types) with ordinality u(t, ord)
                  where f.modes[u.ord] in ('i', 'b', 'v')), '{}'::text[]),
       coalesce((select array_agg(format_type(u.t, null) order by u.ord)
                   from unnest(f.types) with ordinality u(t, ord)
                  where f.modes[u.ord] in ('i', 'b', 'v')), '{}'::text[]),
       coalesce((select array_agg((f.modes[u.ord] = 'v') order by u.ord)
                   from unnest(f.types) with ordinality u(t, ord)
                  where f.modes[u.ord] in ('i', 'b', 'v')), '{}'::bool[]),
       f.pronargdefaults::int,
       f.proretset,
       f.provolatile = 'v',
       format_type(t.oid, null)::text,
       (select c.relname::text
          from pg_class c
         where c.oid = t.typrelid
           and c.relkind in ('r', 'v', 'p', 'm', 'f')),
       (t.typtype = 'c'
        or coalesce((select bool_or(m in ('o', 'b', 't')) from unnest(f.modes) m), false))
  from fns f
  join base_types b on b.oid = f.prorettype
  join pg_type t on t.oid = b.base
 order by f.oid";

/// One overload as [`INTROSPECT_SQL`] hands it over, the raw
/// catalog shape [`routine`] folds into a [`Routine`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineRow {
    pub arg_names: Vec<String>,
    pub arg_types: Vec<String>,
    pub arg_variadic: Vec<bool>,
    pub defaults: i32,
    pub returns_set: bool,
    pub volatile: bool,
    pub rettype: String,
    pub return_table: Option<String>,
    /// Whether the result is a row: a composite type, a table's own
    /// rowtype, or a result named in OUT, INOUT or TABLE arguments.
    pub composite: bool,
}

/// Assemble one overload from an [`INTROSPECT_SQL`] row.
pub fn routine(row: RoutineRow) -> Routine {
    let n = row.arg_names.len();
    let first_default = n.saturating_sub(row.defaults.max(0) as usize);
    let args = row
        .arg_names
        .into_iter()
        .zip(row.arg_types)
        .zip(row.arg_variadic)
        .enumerate()
        .map(|(i, ((name, type_name), variadic))| Arg {
            name,
            type_name,
            has_default: i >= first_default,
            variadic,
        })
        .collect();
    // A function with OUT, INOUT or TABLE arguments returns rows
    // whatever its return type says, because postgres names the
    // output columns after them. One INOUT argument is the case that
    // reads wrong from the return type alone: it collapses to that
    // argument's own type and still comes back as an object.
    //
    // `record` is not a row here. It has no columns to expand, which
    // is what postgres means by "a column definition list is
    // required", so it travels as a value and the value happens to
    // write itself out as an object.
    let kind = if row.rettype == "void" {
        RetKind::Void
    } else if row.composite {
        RetKind::Composite {
            table: row.return_table,
        }
    } else {
        RetKind::Scalar
    };
    Routine {
        args,
        kind,
        returns_set: row.returns_set,
        volatile: row.volatile,
    }
}

/// The PostgREST error shapes for function resolution, PGRST202
/// when nothing matches and PGRST203 when several overloads do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcError {
    pub code: &'static str,
    pub message: String,
    pub details: Option<String>,
}

/// The winning overload plus how to bind it: `unnamed_json` means
/// the whole body passes as the function's single json parameter.
#[derive(Debug)]
pub struct Choice<'a> {
    pub routine: &'a Routine,
    pub unnamed_json: bool,
}

/// Pick the overload the supplied argument names identify. A
/// candidate matches when every supplied name is one of its
/// arguments and everything it cannot default is supplied, the same
/// resolution named notation would do. When nothing matches and the
/// body is json, a single unnamed json parameter takes the whole
/// body, PostgREST's fallback convention.
pub fn choose<'a>(
    schema: &str,
    name: &str,
    overloads: &'a [Routine],
    supplied: &[String],
    json_body: bool,
) -> Result<Choice<'a>, RpcError> {
    let matches: Vec<&Routine> = overloads
        .iter()
        .filter(|r| {
            supplied.iter().all(|s| r.args.iter().any(|a| a.name == *s))
                && r.args
                    .iter()
                    .all(|a| a.has_default || a.variadic || supplied.contains(&a.name))
                && r.args.iter().all(|a| !a.name.is_empty())
        })
        .collect();
    match matches.len() {
        1 => Ok(Choice {
            routine: matches[0],
            unnamed_json: false,
        }),
        0 => {
            if json_body
                && let Some(r) = overloads.iter().find(|r| {
                    r.args.len() == 1
                        && r.args[0].name.is_empty()
                        && matches!(r.args[0].type_name.as_str(), "json" | "jsonb")
                })
            {
                return Ok(Choice {
                    routine: r,
                    unnamed_json: true,
                });
            }
            let mut keys = supplied.to_vec();
            keys.sort();
            let prms = keys.join(", ");
            let spelled = if keys.is_empty() {
                format!("{schema}.{name} without parameters")
            } else {
                format!("{schema}.{name}({prms})")
            };
            let searched = if keys.is_empty() {
                format!("{schema}.{name} without parameters")
            } else {
                let plural = if keys.len() > 1 { "s" } else { "" };
                format!("{schema}.{name} with parameter{plural} {prms}")
            };
            let tail = if json_body {
                " or with a single unnamed json/jsonb parameter"
            } else {
                ""
            };
            Err(RpcError {
                code: "PGRST202",
                message: format!("Could not find the function {spelled} in the schema cache"),
                details: Some(format!(
                    "Searched for the function {searched}{tail}, but no matches were found in the schema cache."
                )),
            })
        }
        _ => {
            let spellings: Vec<String> = matches
                .iter()
                .map(|r| {
                    let args: Vec<String> = r
                        .args
                        .iter()
                        .map(|a| format!("{} => {}", a.name, a.type_name))
                        .collect();
                    format!("{schema}.{name}({})", args.join(", "))
                })
                .collect();
            Err(RpcError {
                code: "PGRST203",
                message: format!(
                    "Could not choose the best candidate function between: {}",
                    spellings.join(", ")
                ),
                details: None,
            })
        }
    }
}

fn qualified(schema: &str, name: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(name))
}

/// How the call itself is spelled once the arguments are bound. A
/// result that is a row goes in the from clause, where postgres
/// expands it into the columns the select grammar then works on. A
/// result that is a value goes in the select list under [`VALUE`],
/// because there is nothing to expand: `record` has no columns
/// until somebody writes a column definition list, and a set of
/// values in a select list is as many rows as the set has.
fn call_sql(kind: &RetKind, f: &str, args: &str) -> String {
    match kind {
        RetKind::Scalar => format!("select {f}({args}) as {}", quote_ident(VALUE)),
        _ => format!("select * from {f}({args})"),
    }
}

/// The expression pulling one named argument out of the json body
/// parameter. json stays json, an array type unpacks element by
/// element because a json array's text is not a postgres array
/// literal, and everything else goes through text into the declared
/// type, the same trust-the-server stance the filters take.
fn json_arg(arg: &Arg) -> String {
    let key = arg.name.replace('\'', "''");
    match arg.type_name.as_str() {
        "json" | "jsonb" => format!("(($1::jsonb)->'{key}')::{}", arg.type_name),
        t if t.ends_with("[]") => format!(
            "coalesce((select array_agg(\"_zou_el\".v) \
             from jsonb_array_elements_text(($1::jsonb)->'{key}') as \"_zou_el\"(v)), \
             array[]::text[])::{t}"
        ),
        t => format!("(($1::jsonb)->>'{key}')::{t}"),
    }
}

/// Build the call for a json body: the body is $1 and each supplied
/// argument unpacks from it by name, or the whole body passes when
/// the function takes one unnamed json parameter.
pub fn call_json(
    schema: &str,
    name: &str,
    choice: &Choice<'_>,
    supplied: &[String],
    body: String,
) -> Sql {
    let f = qualified(schema, name);
    if choice.unnamed_json {
        let t = &choice.routine.args[0].type_name;
        return Sql {
            text: call_sql(&choice.routine.kind, &f, &format!("$1::{t}")),
            params: vec![body],
        };
    }
    let mut parts = Vec::new();
    for arg in &choice.routine.args {
        if !supplied.contains(&arg.name) {
            continue;
        }
        let prefix = if arg.variadic { "variadic " } else { "" };
        parts.push(format!(
            "{prefix}{} := {}",
            quote_ident(&arg.name),
            json_arg(arg)
        ));
    }
    // Every part reads from $1, so the payload only binds when at
    // least one argument was supplied; a bare call takes nothing.
    let params = if parts.is_empty() {
        Vec::new()
    } else {
        vec![body]
    };
    Sql {
        text: call_sql(&choice.routine.kind, &f, &parts.join(", ")),
        params,
    }
}

/// Build the call for query string arguments: each value binds as
/// its own text parameter cast to the declared type, and a variadic
/// argument collects every repetition of its name into one array.
pub fn call_get(schema: &str, name: &str, routine: &Routine, args: &[(String, String)]) -> Sql {
    let f = qualified(schema, name);
    let mut parts = Vec::new();
    let mut params: Vec<String> = Vec::new();
    for arg in &routine.args {
        let values: Vec<&str> = args
            .iter()
            .filter(|(k, _)| *k == arg.name)
            .map(|(_, v)| v.as_str())
            .collect();
        if values.is_empty() {
            continue;
        }
        let ident = quote_ident(&arg.name);
        if arg.variadic {
            let elems: Vec<String> = values
                .iter()
                .map(|v| {
                    params.push((*v).to_string());
                    format!("${}", params.len())
                })
                .collect();
            parts.push(format!(
                "variadic {ident} := array[{}]::{}",
                elems.join(", "),
                arg.type_name
            ));
        } else {
            params.push(values[0].to_string());
            parts.push(format!("{ident} := ${}::{}", params.len(), arg.type_name));
        }
    }
    Sql {
        text: call_sql(&routine.kind, &f, &parts.join(", ")),
        params,
    }
}

/// Wrap a value returning call so the value rides out as json text:
/// one row's bare value, or a set folded to a json array. The value
/// is under [`VALUE`], which is what the wrap reads.
///
/// The second column is how many rows there were, which is worth
/// carrying because a folded set has no rows left to count and
/// max-affected has to count them.
pub fn scalar_wrap(call: Sql, set: bool) -> Sql {
    let src = quote_ident(SOURCE);
    let col = quote_ident(VALUE);
    let text = if set {
        format!(
            "with {src} as ({}) select coalesce(jsonb_agg(to_jsonb({src}.{col})), '[]'::jsonb)::text, count(*)::bigint from {src}",
            call.text
        )
    } else {
        format!(
            "with {src} as ({}) select to_jsonb({src}.{col})::text, 1::bigint from {src}",
            call.text
        )
    };
    Sql {
        text,
        params: call.params,
    }
}

/// Plan the request's select tree over the rows a call returns,
/// the same two piece assembly mutations use, just under the
/// [`SOURCE`] alias and with an ordinary CTE instead of a data
/// modifying one.
pub fn representation(
    catalog: &Catalog,
    call: Sql,
    q: &mut Query,
) -> Result<Represented, PlanError> {
    let Sql { text, params } = call;
    q.source = Some(SOURCE.to_string());
    let select = plan_from(catalog, q, params)?;
    Ok(Represented {
        cte: format!("{} as ({text})", quote_ident(SOURCE)),
        select,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(names: &[&str], types: &[&str], defaults: i32) -> RoutineRow {
        RoutineRow {
            arg_names: names.iter().map(|s| s.to_string()).collect(),
            arg_types: types.iter().map(|s| s.to_string()).collect(),
            arg_variadic: vec![false; names.len()],
            defaults,
            returns_set: false,
            volatile: false,
            rettype: "integer".into(),
            return_table: None,
            composite: false,
        }
    }

    fn plain(names: &[&str], types: &[&str], defaults: i32) -> Routine {
        routine(row(names, types, defaults))
    }

    #[test]
    fn defaults_count_from_the_tail() {
        let r = plain(&["a", "b", "c"], &["integer", "text", "text"], 2);
        assert!(!r.args[0].has_default);
        assert!(r.args[1].has_default);
        assert!(r.args[2].has_default);
    }

    #[test]
    fn the_return_contract_sorts_into_kinds() {
        let kind = |ret: &str, table: Option<&str>, composite: bool| {
            routine(RoutineRow {
                rettype: ret.into(),
                return_table: table.map(str::to_string),
                composite,
                ..row(&[], &[], 0)
            })
            .kind
        };
        assert_eq!(kind("void", None, false), RetKind::Void);
        assert_eq!(kind("integer", None, false), RetKind::Scalar);
        // `record` is a value, not a row: there is nothing to expand
        // until somebody names the columns.
        assert_eq!(kind("record", None, false), RetKind::Scalar);
        // A composite type nobody can embed on is still a row.
        assert_eq!(
            kind("point_2d", None, true),
            RetKind::Composite { table: None }
        );
        assert_eq!(
            kind("books", Some("books"), true),
            RetKind::Composite {
                table: Some("books".into())
            }
        );
    }

    #[test]
    fn a_value_is_selected_and_a_row_is_expanded() {
        let scalar = plain(&["a"], &["integer"], 0);
        let s = call_get("public", "f", &scalar, &[("a".into(), "1".into())]);
        assert_eq!(
            s.text,
            "select \"public\".\"f\"(\"a\" := $1::integer) as \"_zou_val\""
        );

        let composite = routine(RoutineRow {
            rettype: "books".into(),
            return_table: Some("books".into()),
            composite: true,
            ..row(&["a"], &["integer"], 0)
        });
        let s = call_get("public", "f", &composite, &[("a".into(), "1".into())]);
        assert_eq!(
            s.text,
            "select * from \"public\".\"f\"(\"a\" := $1::integer)"
        );
    }

    #[test]
    fn choosing_honors_names_and_defaults() {
        let overloads = vec![
            plain(&["a"], &["integer"], 0),
            plain(&["a", "b"], &["integer", "text"], 0),
        ];
        let c = choose("public", "f", &overloads, &["a".into()], false).unwrap();
        assert_eq!(c.routine.args.len(), 1);
        let c = choose("public", "f", &overloads, &["a".into(), "b".into()], false).unwrap();
        assert_eq!(c.routine.args.len(), 2);

        // A defaulted tail makes one call shape hit two overloads.
        let overloads = vec![
            plain(&["a"], &["integer"], 0),
            plain(&["a", "b"], &["integer", "text"], 1),
        ];
        let e = choose("public", "f", &overloads, &["a".into()], false).unwrap_err();
        assert_eq!(e.code, "PGRST203");
        assert!(e.message.contains("public.f(a => integer)"));
        assert!(e.message.contains("public.f(a => integer, b => text)"));
    }

    #[test]
    fn a_miss_is_pgrst202_with_the_postgrest_spelling() {
        let overloads = vec![plain(&["a"], &["integer"], 0)];
        let e = choose("public", "f", &overloads, &["b".into(), "a".into()], true).unwrap_err();
        assert_eq!(e.code, "PGRST202");
        assert_eq!(
            e.message,
            "Could not find the function public.f(a, b) in the schema cache"
        );
        assert_eq!(
            e.details.as_deref(),
            Some(
                "Searched for the function public.f with parameters a, b \
                 or with a single unnamed json/jsonb parameter, \
                 but no matches were found in the schema cache."
            )
        );

        let e = choose("public", "f", &[], &[], false).unwrap_err();
        assert_eq!(
            e.message,
            "Could not find the function public.f without parameters in the schema cache"
        );
    }

    #[test]
    fn the_unnamed_json_fallback_takes_the_whole_body() {
        let unnamed = plain(&[""], &["jsonb"], 0);
        let overloads = vec![unnamed];
        let c = choose("public", "f", &overloads, &["x".into()], true).unwrap();
        assert!(c.unnamed_json);
        let s = call_json("public", "f", &c, &["x".into()], r#"{"x": 1}"#.into());
        assert_eq!(s.text, r#"select "public"."f"($1::jsonb) as "_zou_val""#);
        assert_eq!(s.params, vec![r#"{"x": 1}"#.to_string()]);

        // Without a json body the same miss is the plain 404.
        let e = choose("public", "f", &overloads, &["x".into()], false).unwrap_err();
        assert_eq!(e.code, "PGRST202");
    }

    #[test]
    fn a_json_call_unpacks_the_body_per_argument() {
        let r = plain(
            &["n", "tag", "opts", "ids"],
            &["integer", "text", "jsonb", "integer[]"],
            0,
        );
        let overloads = vec![r];
        let supplied: Vec<String> = ["n", "tag", "opts", "ids"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let c = choose("public", "f", &overloads, &supplied, true).unwrap();
        let s = call_json("public", "f", &c, &supplied, "{}".into());
        assert_eq!(
            s.text,
            "select \"public\".\"f\"(\
             \"n\" := (($1::jsonb)->>'n')::integer, \
             \"tag\" := (($1::jsonb)->>'tag')::text, \
             \"opts\" := (($1::jsonb)->'opts')::jsonb, \
             \"ids\" := coalesce((select array_agg(\"_zou_el\".v) \
             from jsonb_array_elements_text(($1::jsonb)->'ids') as \"_zou_el\"(v)), \
             array[]::text[])::integer[]) as \"_zou_val\""
        );
        assert_eq!(s.params.len(), 1);
    }

    #[test]
    fn a_bare_call_binds_no_parameters() {
        let overloads = vec![plain(&["min_id"], &["integer"], 1)];
        let c = choose("public", "f", &overloads, &[], true).unwrap();
        let s = call_json("public", "f", &c, &[], "{}".into());
        assert_eq!(s.text, r#"select "public"."f"() as "_zou_val""#);
        assert!(s.params.is_empty());
    }

    #[test]
    fn a_get_call_binds_each_value_and_gathers_variadics() {
        let r = routine(RoutineRow {
            arg_variadic: vec![false, true],
            ..row(&["a", "v"], &["integer", "integer[]"], 0)
        });
        let s = call_get(
            "public",
            "f",
            &r,
            &[
                ("a".into(), "1".into()),
                ("v".into(), "2".into()),
                ("v".into(), "3".into()),
            ],
        );
        assert_eq!(
            s.text,
            "select \"public\".\"f\"(\
             \"a\" := $1::integer, \
             variadic \"v\" := array[$2, $3]::integer[]) as \"_zou_val\""
        );
        assert_eq!(s.params, vec!["1".to_string(), "2".into(), "3".into()]);
    }
}
