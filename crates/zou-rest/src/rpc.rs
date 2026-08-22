//! Function calls, the /rest/v1/rpc/{fn} surface.
//!
//! PostgREST turns a request into a call with named notation, so
//! callers hit overloads, defaults, and variadic parameters exactly
//! as SQL would resolve them. The work splits
//! three ways: [`ROUTINES_SQL`] pulls every overload of every
//! callable name out of pg_proc into the schema cache, [`choose`]
//! picks the one the supplied argument
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

use crate::catalog::{Catalog, closest};
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

/// The row a json body unpacks into, which the arguments then read.
const BODY: &str = "_zou_arg";

/// The call itself when the body sits beside it in the from clause
/// and the two have to be told apart.
const CALL: &str = "_zou_call";

/// One input argument of one overload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arg {
    /// Empty when the argument is unnamed, which only the single
    /// json parameter calling convention can reach.
    pub name: String,
    /// format_type output, spliced into casts verbatim. It comes
    /// from the catalog, never from the client.
    pub type_name: String,
    /// The type an incoming value is cast to, which is the declared
    /// one except for the two that carry a length nobody wrote.
    /// `character` and `bit` are `character(1)` and `bit(1)`, so a
    /// cast to either pads or truncates the value on the way in, and
    /// upstream casts to the varying form instead, which takes what
    /// it is given up to the maximum. The declared type is still what
    /// the argument is, so it is what an ambiguous call reports and
    /// what a single unnamed parameter takes the body as.
    pub cast_type: String,
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
/// [`ROUTINES_SQL`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Routine {
    pub args: Vec<Arg>,
    pub kind: RetKind,
    pub returns_set: bool,
    pub volatile: bool,
    /// The media type the call answers in when the client asks for
    /// it by name, which a function declares by returning a domain
    /// named after one. Nothing else on this surface can produce it,
    /// so it is the function's own and travels with the overload
    /// rather than with the request.
    pub media: Option<String>,
    /// Whether that media type sits on bytea, so the value is bytes
    /// and reading it back as text would hand over a hex literal
    /// rather than the body the function wrote.
    pub media_bytes: bool,
}

/// Every overload of every function one schema can be called on.
/// Bind the schema as $1. The first column is the function's own
/// name, and the eleven after it are one [`RoutineRow`]: input
/// argument names (empty string for unnamed), their types via
/// format_type, the type each one casts an incoming value to, a
/// variadic flag per argument, the count of trailing defaults, then
/// proretset, provolatile = 'v', the return type's name, the table
/// whose rowtype it is when there is one, whether the result is a row
/// rather than a value, and the media type the return type names when
/// it names one.
///
/// The whole schema rather than the one name a request asked for,
/// because this is read once per catalog epoch and held, the way
/// upstream holds its own schema cache. Asking per call cost every
/// call a round trip and a recursive walk of `pg_type`, which is
/// what #178 measured as 1.4 ms the read path was not paying.
///
/// The cast column is the declared type for all but four entries.
/// `character` and `bit` written without a length are `character(1)`
/// and `bit(1)`, so casting a value to one of them pads or truncates
/// it, which is not what a caller who wrote no length asked for.
/// Upstream casts to the varying form and so does this, arrays
/// included.
///
/// proallargtypes and proargmodes only exist once OUT or TABLE
/// arguments appear, so both coalesce back to the plain input
/// columns, and the mode filter keeps i, b, and v: IN, INOUT, and
/// VARIADIC are what a caller can supply.
///
/// A domain is not a type of its own here. `bases` walks typbasetype
/// down to whatever the domain was declared over, and every question
/// about the return type is asked of that, because a domain over a
/// table's rowtype returns rows and postgres will not say so. The
/// walk is seeded from the return types these functions actually
/// have rather than from all of `pg_type`, which is the same answer
/// off a few rows instead of every type the database knows.
///
/// Except one question. The domain's own name is how a function
/// declares what media type it answers in, so the last column asks
/// the undropped return type whether its name is a media type name.
/// A set of them is not one: a media type is one body and a function
/// returning a set has as many as it has rows, which is why upstream
/// registers no handler for those.
///
/// The last clause drops what no request could call. An argument
/// with no name cannot be named in a call, so a function may have at
/// most one, and only of a type some content type arrives as. A
/// function with an unnamed integer argument is not a function this
/// surface has, which is why one is answered as a name nobody has
/// rather than as a call that got its arguments wrong.
pub const ROUTINES_SQL: &str = "\
with recursive fns as (
  select p.oid, p.proname::text as name,
         p.pronargdefaults, p.proretset, p.provolatile, p.prorettype,
         coalesce(p.proallargtypes, p.proargtypes::oid[]) as types,
         coalesce(p.proargmodes,
                  array_fill('i'::\"char\",
                             array[coalesce(cardinality(p.proargtypes::oid[]), 0)])) as modes,
         p.proargnames as names
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
   where n.nspname = $1 and p.prokind = 'f'
),
bases as (
  select t.oid, t.typbasetype, coalesce(nullif(t.typbasetype, 0), t.oid) as base
    from pg_type t
   where t.oid in (select f.prorettype from fns f)
  union
  select t.oid, b.typbasetype, coalesce(nullif(b.typbasetype, 0), b.oid)
    from bases t
    join pg_type b on b.oid = t.typbasetype
),
base_types as (select oid, base from bases where typbasetype = 0),
args as (
  select f.oid,
         coalesce((select array_agg(coalesce(f.names[u.ord], '') order by u.ord)
                     from unnest(f.types) with ordinality u(t, ord)
                    where f.modes[u.ord] in ('i', 'b', 'v')), '{}'::text[]) as names,
         coalesce((select array_agg(format_type(u.t, null) order by u.ord)
                     from unnest(f.types) with ordinality u(t, ord)
                    where f.modes[u.ord] in ('i', 'b', 'v')), '{}'::text[]) as types,
         coalesce((select array_agg(case u.t
                                      when 'bit'::regtype then 'bit varying'
                                      when 'bit[]'::regtype then 'bit varying[]'
                                      when 'character'::regtype then 'character varying'
                                      when 'character[]'::regtype then 'character varying[]'
                                      else format_type(u.t, null)
                                    end order by u.ord)
                     from unnest(f.types) with ordinality u(t, ord)
                    where f.modes[u.ord] in ('i', 'b', 'v')), '{}'::text[]) as casts,
         coalesce((select array_agg((f.modes[u.ord] = 'v') order by u.ord)
                     from unnest(f.types) with ordinality u(t, ord)
                    where f.modes[u.ord] in ('i', 'b', 'v')), '{}'::bool[]) as variadic
    from fns f
)
select f.name,
       a.names,
       a.types,
       a.casts,
       a.variadic,
       f.pronargdefaults::int,
       f.proretset,
       f.provolatile = 'v',
       format_type(t.oid, null)::text,
       (select c.relname::text
          from pg_class c
         where c.oid = t.typrelid
           and c.relkind in ('r', 'v', 'p', 'm', 'f')),
       (t.typtype = 'c'
        or coalesce((select bool_or(m in ('o', 'b', 't')) from unnest(f.modes) m), false)),
       (select lower(d.typname)::text
          from pg_type d
         where d.oid = f.prorettype
           and not f.proretset
           and (d.typname ~ '^[A-Za-z0-9.-]+/[A-Za-z0-9.+-]+$' or d.typname = '*/*'))
  from fns f
  join args a on a.oid = f.oid
  join base_types b on b.oid = f.prorettype
  join pg_type t on t.oid = b.base
 where cardinality(array_positions(a.names, '')) = 0
    or (cardinality(array_positions(a.names, '')) = 1
        and a.types[(array_positions(a.names, ''))[1]]
            in ('bytea', 'json', 'jsonb', 'text', 'xml'))
 order by f.name, f.oid";

/// Every function name in one schema this surface can call, which is
/// what a name nobody has is compared against for the suggestion.
/// Bind the schema as $1. The two clauses are [`ROUTINES_SQL`]'s:
/// a trigger is not callable over http, and neither is a function
/// whose unnamed arguments no body can fill.
pub const NAMES_SQL: &str = "\
select distinct p.proname::text
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  left join lateral (
    select count(*) filter (where coalesce(p.proargnames[u.ord], '') = '') as unnamed,
           bool_and(format_type(u.t, null) in ('bytea', 'json', 'jsonb', 'text', 'xml'))
             filter (where coalesce(p.proargnames[u.ord], '') = '') as spoken
      from unnest(p.proargtypes::oid[]) with ordinality u(t, ord)
  ) a on true
 where n.nspname = $1
   and p.prokind = 'f'
   and p.prorettype <> 'trigger'::regtype
   and (coalesce(a.unnamed, 0) = 0 or (a.unnamed = 1 and a.spoken))";

/// One overload as [`ROUTINES_SQL`] hands it over, the raw
/// catalog shape [`routine`] folds into a [`Routine`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineRow {
    pub arg_names: Vec<String>,
    pub arg_types: Vec<String>,
    /// The type each argument casts an incoming value to, which is
    /// [`Arg::cast_type`].
    pub arg_casts: Vec<String>,
    pub arg_variadic: Vec<bool>,
    pub defaults: i32,
    pub returns_set: bool,
    pub volatile: bool,
    pub rettype: String,
    pub return_table: Option<String>,
    /// Whether the result is a row: a composite type, a table's own
    /// rowtype, or a result named in OUT, INOUT or TABLE arguments.
    pub composite: bool,
    /// The return type's own name when that name is a media type.
    pub media: Option<String>,
}

/// Assemble one overload from a [`ROUTINES_SQL`] row.
pub fn routine(row: RoutineRow) -> Routine {
    let n = row.arg_names.len();
    let first_default = n.saturating_sub(row.defaults.max(0) as usize);
    let args = row
        .arg_names
        .into_iter()
        .zip(row.arg_types)
        .zip(row.arg_casts)
        .zip(row.arg_variadic)
        .enumerate()
        .map(|(i, (((name, type_name), cast_type), variadic))| Arg {
            name,
            type_name,
            cast_type,
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
    // A row is many values and a value is one body, so only the
    // second can be handed over as a media type of its own.
    let value = kind == RetKind::Scalar;
    let media_bytes = row.rettype == "bytea";
    Routine {
        args,
        kind,
        returns_set: row.returns_set,
        volatile: row.volatile,
        media: row.media.filter(|_| value),
        media_bytes,
    }
}

/// The PostgREST error shapes for function resolution, PGRST202
/// when nothing matches and PGRST203 when several overloads do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcError {
    pub code: &'static str,
    pub message: String,
    pub details: Option<String>,
    pub hint: Option<String>,
    /// Whether the hint is still owed a look through the schema's
    /// other function names. Nothing here has that list, and only a
    /// name the schema has no function of at all is worth spending a
    /// query on, so [`choose`] says when to ask and [`name_hint`]
    /// answers once the caller has been.
    pub unknown_name: bool,
}

/// What the request body is, which decides whether the whole of it
/// can pass as one unnamed parameter and which parameter that is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Payload {
    Json,
    Text,
    Xml,
    Bytes,
    /// A form, a csv, anything else: a body with no single parameter
    /// waiting for it.
    Other,
}

/// The winning overload plus how to bind it: `unnamed` means the
/// whole body passes as the function's one unnamed parameter.
#[derive(Debug)]
pub struct Choice<'a> {
    pub routine: &'a Routine,
    pub unnamed: bool,
}

/// Pick the overload the supplied argument names identify.
///
/// The overloads sort into two piles. One holds those the names fit,
/// which is the resolution named notation would do. The
/// other holds those taking a single unnamed parameter the body
/// could pass whole, which only a POST can reach and only for a
/// content type the parameter's type answers to. The named pile
/// decides on its own if it has anything in it, and the unnamed pile
/// is what a call with no named match falls back to. Either pile
/// with more than one candidate in it is PGRST203, and both empty is
/// PGRST202.
pub fn choose<'a>(
    schema: &str,
    name: &str,
    overloads: &'a [Routine],
    keys: &[String],
    payload: Payload,
    is_post: bool,
) -> Result<Choice<'a>, RpcError> {
    let mut named: Vec<&Routine> = Vec::new();
    let mut whole: Vec<&Routine> = Vec::new();
    for r in overloads {
        if fits_keys(r, keys, payload, is_post) {
            named.push(r);
        } else if takes_body(r, payload, is_post) {
            whole.push(r);
        }
    }
    let ambiguous = |rs: &[&Routine]| RpcError {
        code: "PGRST203",
        message: format!(
            "Could not choose the best candidate function between: {}",
            rs.iter()
                .map(|r| {
                    let args: Vec<String> = r
                        .args
                        .iter()
                        .map(|a| format!("{} => {}", a.name, a.type_name))
                        .collect();
                    format!("{schema}.{name}({})", args.join(", "))
                })
                .collect::<Vec<_>>()
                .join(", ")
        ),
        details: None,
        hint: Some(
            "Try renaming the parameters or the function itself in the database \
             so function overloading can be resolved"
                .into(),
        ),
        unknown_name: false,
    };
    match (named.len(), whole.len()) {
        (0, 0) => Err(no_such(schema, name, keys, payload, is_post, overloads)),
        (0, 1) => Ok(Choice {
            routine: whole[0],
            unnamed: true,
        }),
        (0, _) => Err(ambiguous(&whole)),
        (1, _) => Ok(Choice {
            routine: named[0],
            unnamed: false,
        }),
        _ => Err(ambiguous(&named)),
    }
}

/// Whether these argument names call this overload. A parameter with
/// a default may be left out and anything else may not, and a name
/// that is not a parameter at all rules the overload out however well
/// the rest fit, which is what makes an unknown argument a 404 rather
/// than something quietly ignored.
///
/// A function with no parameters is called by supplying none, except
/// by a POST whose body is one value: that body is asking for the
/// parameter it would pass as, and a function taking nothing is not
/// it.
fn fits_keys(r: &Routine, keys: &[String], payload: Payload, is_post: bool) -> bool {
    if r.args.is_empty() {
        return keys.is_empty() && !value_body(payload, is_post);
    }
    let has = |args: &[&Arg], k: &String| args.iter().any(|a| a.name == *k);
    let (required, optional): (Vec<&Arg>, Vec<&Arg>) = r.args.iter().partition(|a| !a.has_default);
    let supplied: Vec<&String> = keys.iter().filter(|k| !has(&optional, k)).collect();
    supplied.len() == required.len() && required.iter().all(|a| supplied.contains(&&a.name))
}

/// Whether the body can pass to this overload whole. The parameter
/// has to be the only one, unnamed, and of the type the content type
/// arrives as, and the request has to be a POST, since a body is the
/// only thing there is to pass.
fn takes_body(r: &Routine, payload: Payload, is_post: bool) -> bool {
    let Some(wanted) = lone_param_type(payload) else {
        return false;
    };
    is_post
        && matches!(r.args.as_slice(), [a] if a.name.is_empty()
            && wanted.contains(&a.type_name.as_str()))
}

/// The parameter types a body of that content type can pass to
/// whole, or none for a content type no parameter answers to. json
/// is the odd one: it names the arguments as well, so a function
/// with named parameters is still in the running beside it.
fn lone_param_type(payload: Payload) -> Option<&'static [&'static str]> {
    match payload {
        Payload::Json => Some(&["json", "jsonb"]),
        Payload::Text => Some(&["text"]),
        Payload::Xml => Some(&["xml"]),
        Payload::Bytes => Some(&["bytea"]),
        Payload::Other => None,
    }
}

/// A body that is one value carries no argument names, so the error
/// it gets names none either: there is nothing the caller wrote that
/// could have been meant differently, only a parameter the function
/// does not have.
fn value_body(payload: Payload, is_post: bool) -> bool {
    is_post && matches!(payload, Payload::Text | Payload::Xml | Payload::Bytes)
}

fn no_such(
    schema: &str,
    name: &str,
    keys: &[String],
    payload: Payload,
    is_post: bool,
    overloads: &[Routine],
) -> RpcError {
    let mut keys = keys.to_vec();
    keys.sort();
    let func = format!("{schema}.{name}");
    let spelled = match keys.is_empty() {
        true => " without parameters".to_string(),
        false => format!("({})", keys.join(", ")),
    };
    let searched = match keys.is_empty() {
        true => " without parameters".to_string(),
        false => {
            let plural = if keys.len() > 1 { "s" } else { "" };
            format!(" with parameter{plural} {}", keys.join(", "))
        }
    };
    let looked_for = match (is_post, payload) {
        (true, Payload::Text) => " with a single unnamed text parameter".into(),
        (true, Payload::Xml) => " with a single unnamed xml parameter".into(),
        (true, Payload::Bytes) => " with a single unnamed bytea parameter".into(),
        (true, Payload::Json) => {
            format!("{searched} or with a single unnamed json/jsonb parameter")
        }
        _ => searched,
    };
    let told = value_body(payload, is_post);
    RpcError {
        code: "PGRST202",
        message: format!(
            "Could not find the function {func}{} in the schema cache",
            if told { "" } else { &spelled }
        ),
        details: Some(format!(
            "Searched for the function {func}{looked_for}, \
             but no matches were found in the schema cache."
        )),
        // A name the schema does have functions of is a call that got
        // the arguments wrong, so the suggestion is the argument list
        // of whichever overload the caller came nearest to writing.
        hint: match told || overloads.is_empty() {
            true => None,
            false => {
                let spellings: Vec<String> = overloads.iter().map(spell_params).collect();
                closest(spellings.iter().map(String::as_str), &spell(&keys), 0.33).map(|best| {
                    format!("Perhaps you meant to call the function {schema}.{name}{best}")
                })
            }
        },
        unknown_name: !told && overloads.is_empty(),
    }
}

/// A parameter list the way the suggestion compares them, which is
/// by name alone and in sorted order, since named notation makes the
/// declared order nothing to the caller.
fn spell_params(r: &Routine) -> String {
    let mut names: Vec<String> = r.args.iter().map(|a| a.name.clone()).collect();
    names.sort();
    spell(&names)
}

fn spell(names: &[String]) -> String {
    format!("({})", names.join(", "))
}

/// The suggestion for a name the schema has no function of, which is
/// the nearest name it does have. The bar is three quarters, the
/// same one a misspelled table is held to and for the same reason:
/// the field is every function in the schema, and a loose match
/// there suggests functions that have nothing to do with the call.
pub fn name_hint(schema: &str, name: &str, names: &[String]) -> Option<String> {
    let best = closest(names.iter().map(String::as_str), name, 0.75)?;
    Some(format!(
        "Perhaps you meant to call the function {schema}.{best}"
    ))
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

/// The call itself with the body beside it in the from clause, which
/// is the shape a json body needs: the arguments are columns of a row
/// postgres unpacked, so the call has to be lateral to it.
///
/// A result that is a value is wrapped in a select of its own so that
/// it has a name to be read by, the same name [`call_sql`] gives it in
/// the select list, since the from clause names a set of values after
/// the function rather than after anything this could ask for.
fn call_beside(kind: &RetKind, f: &str, from: &str, args: &str) -> String {
    let call = quote_ident(CALL);
    match kind {
        RetKind::Scalar => format!(
            "select {call}.{v} from {from}, lateral (select {f}({args}) as {v}) as {call}",
            v = quote_ident(VALUE)
        ),
        _ => format!("select {call}.* from {from}, lateral {f}({args}) as {call}"),
    }
}

/// Build the call for a json body: the body is $1 and the supplied
/// arguments are unpacked out of it under a column definition list,
/// or the whole body passes when the function takes one unnamed json
/// parameter.
///
/// The definition list is what makes a body value arrive as the type
/// the argument is. Postgres reads a json value into a declared type
/// with the same rules it reads any json into a record with, so an
/// array argument takes a json array and takes a string holding a
/// postgres array literal, and neither of those is a rule written
/// here. Upstream builds the same from clause for the same reason.
///
/// The body is one object by the time it gets here. Upstream reaches
/// that with `LIMIT 1` over the unpacked array, which is the same
/// answer arrived at earlier.
pub fn call_json(
    schema: &str,
    name: &str,
    choice: &Choice<'_>,
    supplied: &[String],
    body: String,
) -> Sql {
    let f = qualified(schema, name);
    if choice.unnamed {
        let t = &choice.routine.args[0].type_name;
        return Sql {
            text: call_sql(&choice.routine.kind, &f, &format!("$1::{t}")),
            params: vec![body],
        };
    }
    let src = quote_ident(BODY);
    let mut defs = Vec::new();
    let mut parts = Vec::new();
    for arg in &choice.routine.args {
        if !supplied.contains(&arg.name) {
            continue;
        }
        let ident = quote_ident(&arg.name);
        defs.push(format!("{ident} {}", arg.cast_type));
        let prefix = if arg.variadic { "variadic " } else { "" };
        parts.push(format!("{prefix}{ident} := {src}.{ident}"));
    }
    // The body only binds when at least one argument was supplied,
    // since a call taking none has nothing to unpack it for.
    if parts.is_empty() {
        return Sql {
            text: call_sql(&choice.routine.kind, &f, ""),
            params: Vec::new(),
        };
    }
    let from = format!("json_to_record($1) as {src}({})", defs.join(", "));
    Sql {
        text: call_beside(&choice.routine.kind, &f, &from, &parts.join(", ")),
        params: vec![body],
    }
}

/// Build the call for query string arguments: each value binds as
/// its own text parameter cast to the argument's type, and a variadic
/// argument collects every repetition of its name into one array.
///
/// A name repeated for an argument that is not variadic is the last
/// of them. There is one parameter to fill and postgres has no say
/// in which value fills it, so the rule is only a convention, and
/// the convention is that the last write wins.
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
                arg.cast_type
            ));
        } else {
            params.push(values[values.len() - 1].to_string());
            parts.push(format!("{ident} := ${}::{}", params.len(), arg.cast_type));
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
            "with {src} as ({}) select coalesce(json_agg(to_json({src}.{col})), '[]'::json)::text, count(*)::bigint from {src}",
            call.text
        )
    } else {
        format!(
            "with {src} as ({}) select to_json({src}.{col})::text, 1::bigint from {src}",
            call.text
        )
    };
    Sql {
        text,
        params: call.params,
    }
}

/// Wrap a value returning call so the value rides out as itself.
///
/// This is the shape a function gets when it declares a media type
/// of its own: nothing wraps the value, no quotes go around it and no
/// brackets go around that, the body is what the function returned.
/// A null returned nothing, which is an empty body rather than the
/// four letters, and the caller reads that as null too.
///
/// The count is one because a value is one row by definition. Only a
/// value can carry a media type this way, a row being many values and
/// so many bodies.
///
/// The value comes back as bytes either way. A domain over bytea has
/// to, since its text is the hex literal and not the body, and the
/// rest go through convert_to so that one column type answers for
/// every media type a function can declare.
pub fn value_wrap(call: Sql, bytes: bool) -> Sql {
    let src = quote_ident(SOURCE);
    let col = quote_ident(VALUE);
    let body = if bytes {
        format!("{src}.{col}::bytea")
    } else {
        format!("convert_to({src}.{col}::text, 'UTF8')")
    };
    Sql {
        text: format!(
            "with {src} as ({}) select {body}, 1::bigint from {src}",
            call.text
        ),
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
            arg_casts: types.iter().map(|s| s.to_string()).collect(),
            arg_variadic: vec![false; names.len()],
            defaults,
            returns_set: false,
            volatile: false,
            rettype: "integer".into(),
            return_table: None,
            composite: false,
            media: None,
        }
    }

    fn plain(names: &[&str], types: &[&str], defaults: i32) -> Routine {
        routine(row(names, types, defaults))
    }

    fn call<'a>(
        overloads: &'a [Routine],
        keys: &[String],
        payload: Payload,
        is_post: bool,
    ) -> Result<Choice<'a>, RpcError> {
        choose("public", "f", overloads, keys, payload, is_post)
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
    fn only_a_value_carries_a_media_type_of_its_own() {
        let media = |composite: bool| {
            routine(RoutineRow {
                media: Some("text/plain".into()),
                rettype: "text".into(),
                composite,
                ..row(&[], &[], 0)
            })
            .media
        };
        assert_eq!(media(false), Some("text/plain".to_string()));
        // A row is written out as one body per column layout, not as
        // one body, so the name on the return type says nothing.
        assert_eq!(media(true), None);
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
        let c = call(&overloads, &["a".into()], Payload::Other, false).unwrap();
        assert_eq!(c.routine.args.len(), 1);
        let c = call(&overloads, &["a".into(), "b".into()], Payload::Other, false).unwrap();
        assert_eq!(c.routine.args.len(), 2);

        // A defaulted tail makes one call shape hit two overloads.
        let overloads = vec![
            plain(&["a"], &["integer"], 0),
            plain(&["a", "b"], &["integer", "text"], 1),
        ];
        let e = call(&overloads, &["a".into()], Payload::Other, false).unwrap_err();
        assert_eq!(e.code, "PGRST203");
        assert!(e.message.contains("public.f(a => integer)"));
        assert!(e.message.contains("public.f(a => integer, b => text)"));
        assert_eq!(
            e.hint.as_deref(),
            Some(
                "Try renaming the parameters or the function itself in the database \
                 so function overloading can be resolved"
            )
        );
    }

    #[test]
    fn an_argument_the_function_has_not_got_rules_it_out() {
        // Every supplied name has to be a parameter, so the overload
        // taking a superset is no more a match than one taking none of
        // them.
        let overloads = vec![plain(&["a", "b"], &["integer", "text"], 1)];
        assert!(call(&overloads, &["a".into(), "c".into()], Payload::Other, false).is_err());
        // And a required parameter left out is a miss however few the
        // names are.
        assert!(call(&overloads, &[], Payload::Other, false).is_err());
    }

    #[test]
    fn a_miss_is_pgrst202_with_the_postgrest_spelling() {
        let overloads = vec![plain(&["a"], &["integer"], 0)];
        let e = call(&overloads, &["b".into(), "a".into()], Payload::Json, true).unwrap_err();
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

        let e = call(&[], &[], Payload::Other, false).unwrap_err();
        assert_eq!(
            e.message,
            "Could not find the function public.f without parameters in the schema cache"
        );
        assert_eq!(
            e.details.as_deref(),
            Some(
                "Searched for the function public.f without parameters, \
                 but no matches were found in the schema cache."
            )
        );
        // Nothing in the schema goes by that name, so the caller wants
        // a name suggested rather than an argument list.
        assert!(e.unknown_name);
    }

    #[test]
    fn a_body_that_is_one_value_names_no_parameters_in_the_error() {
        let overloads = vec![plain(&["a"], &["integer"], 0)];
        let e = call(&overloads, &[], Payload::Text, true).unwrap_err();
        assert_eq!(
            e.message,
            "Could not find the function public.f in the schema cache"
        );
        assert_eq!(
            e.details.as_deref(),
            Some(
                "Searched for the function public.f with a single unnamed text parameter, \
                 but no matches were found in the schema cache."
            )
        );
        // There is no argument list to have meant instead, and no name
        // to look up either, since the schema has the name already.
        assert!(e.hint.is_none());
        assert!(!e.unknown_name);
    }

    #[test]
    fn the_hint_names_the_argument_list_nearest_what_was_written() {
        let overloads = vec![
            plain(&["a", "b"], &["integer", "integer"], 0),
            plain(&["a", "b", "c"], &["integer", "integer", "integer"], 0),
        ];
        let keys = ["a".to_string(), "b".into(), "wrong_arg".into()];
        let e = call(&overloads, &keys, Payload::Json, true).unwrap_err();
        assert_eq!(
            e.hint.as_deref(),
            Some("Perhaps you meant to call the function public.f(a, b, c)")
        );

        // Nothing close enough is no hint at all.
        let overloads = vec![plain(&["name"], &["text"], 0)];
        let e = call(&overloads, &["any_arg".into()], Payload::Json, true).unwrap_err();
        assert!(e.hint.is_none());
    }

    #[test]
    fn the_unnamed_fallback_takes_the_whole_body() {
        let overloads = vec![plain(&[""], &["jsonb"], 0)];
        let c = call(&overloads, &["x".into()], Payload::Json, true).unwrap();
        assert!(c.unnamed);
        let s = call_json("public", "f", &c, &["x".into()], r#"{"x": 1}"#.into());
        assert_eq!(s.text, r#"select "public"."f"($1::jsonb) as "_zou_val""#);
        assert_eq!(s.params, vec![r#"{"x": 1}"#.to_string()]);

        // A GET has no body to pass, so the same miss is the plain 404.
        let e = call(&overloads, &["x".into()], Payload::Json, false).unwrap_err();
        assert_eq!(e.code, "PGRST202");

        // The parameter's type has to be what the content type
        // arrives as. A text body does not go into a jsonb parameter.
        assert!(call(&overloads, &[], Payload::Text, true).is_err());
        let overloads = vec![plain(&[""], &["text"], 0)];
        assert!(call(&overloads, &[], Payload::Text, true).unwrap().unnamed);

        // A named overload wins over the fallback rather than
        // competing with it.
        let overloads = vec![plain(&[""], &["jsonb"], 0), plain(&["x"], &["integer"], 0)];
        let c = call(&overloads, &["x".into()], Payload::Json, true).unwrap();
        assert!(!c.unnamed);
    }

    #[test]
    fn two_fallbacks_for_the_one_body_are_pgrst203() {
        let overloads = vec![plain(&[""], &["json"], 0), plain(&[""], &["jsonb"], 0)];
        let e = call(&overloads, &["x".into()], Payload::Json, true).unwrap_err();
        assert_eq!(e.code, "PGRST203");
        assert!(e.message.contains("public.f( => json)"));
        assert!(e.message.contains("public.f( => jsonb)"));
    }

    #[test]
    fn a_body_that_is_one_value_rules_out_the_function_taking_nothing() {
        let overloads = vec![plain(&[], &[], 0)];
        assert!(call(&overloads, &[], Payload::Json, true).is_ok());
        assert!(call(&overloads, &[], Payload::Other, false).is_ok());
        // A text body is asking to be passed as a parameter, and this
        // function has none to pass it to.
        assert!(call(&overloads, &[], Payload::Text, true).is_err());
    }

    /// The body unpacks once, under a column definition list, and the
    /// call reads the row it made. Nothing here says what a json array
    /// or a json object becomes, because postgres already knows: the
    /// declared type of the column is the whole instruction.
    #[test]
    fn a_json_call_unpacks_the_body_under_its_argument_types() {
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
        let c = call(&overloads, &supplied, Payload::Json, true).unwrap();
        let s = call_json("public", "f", &c, &supplied, "{}".into());
        assert_eq!(
            s.text,
            "select \"_zou_call\".\"_zou_val\" \
             from json_to_record($1) as \"_zou_arg\"(\
             \"n\" integer, \"tag\" text, \"opts\" jsonb, \"ids\" integer[]), \
             lateral (select \"public\".\"f\"(\
             \"n\" := \"_zou_arg\".\"n\", \
             \"tag\" := \"_zou_arg\".\"tag\", \
             \"opts\" := \"_zou_arg\".\"opts\", \
             \"ids\" := \"_zou_arg\".\"ids\") as \"_zou_val\") as \"_zou_call\""
        );
        assert_eq!(s.params.len(), 1);
    }

    #[test]
    fn a_bare_call_binds_no_parameters() {
        let overloads = vec![plain(&["min_id"], &["integer"], 1)];
        let c = call(&overloads, &[], Payload::Json, true).unwrap();
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

    /// `character` is `character(1)`, so casting to it keeps one
    /// letter of whatever was sent. The value is cast to the varying
    /// form instead, from the query string and from a json body
    /// alike, and the declared type is still what the argument is.
    #[test]
    fn a_length_less_char_takes_the_value_it_was_given() {
        let r = routine(RoutineRow {
            arg_casts: vec!["character varying".into(), "character varying[]".into()],
            ..row(&["c", "arr"], &["character", "character[]"], 0)
        });
        let s = call_get(
            "public",
            "f",
            &r,
            &[("c".into(), "abcdefg".into()), ("arr".into(), "{a}".into())],
        );
        assert_eq!(
            s.text,
            "select \"public\".\"f\"(\
             \"c\" := $1::character varying, \
             \"arr\" := $2::character varying[]) as \"_zou_val\""
        );
        let overloads = [r];
        let supplied = ["c".to_string(), "arr".to_string()];
        let c = call(&overloads, &supplied, Payload::Json, true).unwrap();
        let s = call_json("public", "f", &c, &supplied, r#"{"c": "abcdefg"}"#.into());
        assert!(
            s.text
                .contains("as \"_zou_arg\"(\"c\" character varying, \"arr\" character varying[])"),
            "{}",
            s.text
        );
        // The two the substitution is for are the only ones it
        // touches, and the catalog is where it happens.
        assert!(ROUTINES_SQL.contains("when 'character'::regtype then 'character varying'"));
        assert!(ROUTINES_SQL.contains("when 'bit'::regtype then 'bit varying'"));
    }

    #[test]
    fn a_name_written_twice_binds_the_last_of_them() {
        // Only a variadic parameter gathers the repeats. Anywhere else
        // the last one written is the value, the way a query string is
        // read everywhere else.
        let r = plain(&["a"], &["integer"], 0);
        let s = call_get(
            "public",
            "f",
            &r,
            &[("a".into(), "1".into()), ("a".into(), "2".into())],
        );
        assert_eq!(
            s.text,
            "select \"public\".\"f\"(\"a\" := $1::integer) as \"_zou_val\""
        );
        assert_eq!(s.params, vec!["2".to_string()]);
    }

    #[test]
    fn a_variadic_parameter_is_required_unless_it_has_a_default() {
        let r = routine(RoutineRow {
            arg_variadic: vec![true],
            ..row(&["v"], &["integer[]"], 0)
        });
        let overloads = vec![r];
        assert!(call(&overloads, &[], Payload::Json, true).is_err());
        assert!(call(&overloads, &["v".into()], Payload::Json, true).is_ok());
    }
}
