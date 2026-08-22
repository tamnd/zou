//! The relationship graph resource embedding runs on.
//!
//! PostgREST embeds one table inside another by walking foreign
//! keys, so the planner needs to know every fk in the exposed
//! schema, which side holds it, whether its columns are covered by
//! a unique constraint, and whether they sit inside the child's
//! primary key. That last bit is how junction tables are detected:
//! a table whose primary key is made of fks to both ends is a
//! many to many bridge.
//!
//! The crate stays free of database dependencies, [`INTROSPECT_SQL`]
//! is the query and the caller feeds the resulting rows back in as
//! [`FkRow`] values. Resolution then answers the planner's question:
//! given the table a request is rooted on and the word an embed
//! names, which relationship is meant, with the PGRST200 and
//! PGRST201 errors PostgREST clients branch on when the answer is
//! none or several.
//!
//! That word is a name for the relationship rather than for the
//! relation on the other end. A relationship answers to the foreign
//! table, `clients`, to the constraint that makes it,
//! `projects_client_id_fkey`, and to the foreign key column the
//! constraint sits on, `client_id`, and a hint after `!` may name
//! any of those or the junction table of a many to many. The one
//! place the name cannot say which way the join runs is a table
//! pointing at itself, where the convention decides: the table name
//! is the list pointing back, the column name is the one row it
//! points at.
//!
//! Not every relationship is a foreign key. A function of one
//! argument taking a row of one relation and returning rows of
//! another is a relationship too, [`COMPUTED_SQL`], and the embed
//! names it by the function's name. There is no join condition to
//! write for one: the parent row goes in as the argument, and that
//! is the whole of the link.
//!
//! Alongside the graph the catalog holds the relations themselves
//! and their columns, [`RELATIONS_SQL`], [`COLUMNS_SQL`] and
//! [`Relation`]. That list is the difference between a request
//! answered and a request refused before any SQL is written: a table
//! nobody has is a 404 naming the schema, and a column a write names
//! but the table does not have is a 400, rather than whatever
//! postgres would have said about the statement that got built
//! anyway.
//!
//! Each column also carries the casts its type was given to and from
//! json, which is all a data representation is: a domain with a
//! `json(the_domain)` function behind a cast is written out through
//! that function instead of by its own output, and one with a
//! `the_domain(text)` function reads the values a url carries
//! through that, and one with a `the_domain(json)` function reads
//! the values a body carries. The planner asks [`Column`] for them
//! and splices the function name postgres quoted. The type name is
//! there for the same reason: a write has to spell out what it is
//! unpacking a body into before either cast can be called.

use crate::rpc::Routine;
use std::collections::{HashMap, HashSet};
use std::fmt;

/// One foreign key, straight off [`INTROSPECT_SQL`]. `table` is the
/// side that holds the fk columns, `ref_table` the side they point
/// at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FkRow {
    pub constraint: String,
    pub table: String,
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    /// The fk columns are covered exactly by a unique or primary
    /// key constraint, which turns a to many into a one to one.
    pub unique: bool,
    /// The fk columns all sit inside the table's primary key, the
    /// junction table signature.
    pub in_pk: bool,
}

/// The catalog side of the introspection: every fk in one schema.
/// Bind the schema name as $1.
///
/// A partition is not a table the api has, [`RELATIONS_SQL`] leaves
/// them out, so neither are the keys it inherited: postgres copies
/// every key of a partitioned table onto each partition and records
/// the copy with `conparentid` pointing back at the original, and the
/// original is the only one that names a relationship anybody can ask
/// for. Without that a partitioned junction is as many junctions as
/// it has partitions, which is an ambiguous embed where upstream has
/// one relationship.
pub const INTROSPECT_SQL: &str = "\
select c.conname::text,
       child.relname::text,
       (select array_agg(a.attname::text order by k.ord)
          from unnest(c.conkey) with ordinality k(attnum, ord)
          join pg_attribute a
            on a.attrelid = c.conrelid and a.attnum = k.attnum),
       parent.relname::text,
       (select array_agg(a.attname::text order by k.ord)
          from unnest(c.confkey) with ordinality k(attnum, ord)
          join pg_attribute a
            on a.attrelid = c.confrelid and a.attnum = k.attnum),
       exists (select 1 from pg_constraint u
                where u.conrelid = c.conrelid
                  and u.contype in ('p', 'u')
                  and (select array_agg(x order by x) from unnest(u.conkey) x)
                    = (select array_agg(x order by x) from unnest(c.conkey) x)),
       coalesce((select c.conkey <@ p.conkey from pg_constraint p
                  where p.conrelid = c.conrelid and p.contype = 'p'),
                false)
  from pg_constraint c
  join pg_class child on child.oid = c.conrelid
  join pg_namespace cns on cns.oid = child.relnamespace
  join pg_class parent on parent.oid = c.confrelid
  join pg_namespace pns on pns.oid = parent.relnamespace
 where c.contype = 'f'
   and c.conparentid = 0
   and cns.nspname = $1
   and pns.nspname = $1
 order by c.conname";

/// One relation the schema exposes and the columns it has. Views and
/// foreign tables are relations here for the same reason they are in
/// PostgREST: a client asks for them by name over the same url and
/// does not care what backs them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub name: String,
    pub columns: Vec<Column>,
    /// The columns of its primary key, in the order the key was
    /// declared in. A view has one of these too, borrowed from the
    /// table under it the way its foreign keys are.
    pub keys: Vec<String>,
}

impl Relation {
    /// The column of that name, if the relation has one.
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Whether the relation has a column of that name at all, which
    /// is the question a write's column list asks.
    pub fn has(&self, name: &str) -> bool {
        self.column(name).is_some()
    }

    /// Whether any column here is written out through a cast, which
    /// is what makes `*` worth expanding into a column list: the
    /// expansion exists to have somewhere to put the call.
    pub fn represented(&self) -> bool {
        self.columns.iter().any(|c| c.to_json.is_some())
    }

    /// Whether an arrow into this column has to read it as json
    /// first. Everything does except json and jsonb themselves, and
    /// a column nobody here has is a column of a type nobody looked
    /// up, which upstream reads as json too.
    pub fn steps_as_json(&self, column: &str) -> bool {
        match self.column(column) {
            Some(c) => !matches!(c.type_name.as_str(), "json" | "jsonb"),
            None => true,
        }
    }

    /// Whether a text search over this column has to make the vector
    /// itself. A tsvector already is one, and a column nobody here
    /// has is left alone, because upstream has no type to decide
    /// against and will not guess one.
    pub fn searches_as_text(&self, column: &str) -> bool {
        self.column(column)
            .is_some_and(|c| c.base_type != "tsvector")
    }
}

/// One column of a relation, and the two functions its type was
/// given for crossing into json and back out of a url.
///
/// A type with neither is every ordinary column, and both names are
/// already quoted the way postgres quotes an identifier, so a
/// planner splices them and nothing here has to know what characters
/// a function name may hold.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Column {
    pub name: String,
    /// What postgres calls the type, spelled the way a column
    /// definition list wants it. Only a write needs it.
    pub type_name: String,
    /// The same type with every domain over it resolved away, so a
    /// domain over tsvector reads as `tsvector` here and as its own
    /// name above. A rule about what a type can do asks this one, a
    /// rule about how a value is spelled asks the other.
    pub base_type: String,
    /// Writes a value of this type as json, the cast to json.
    pub to_json: Option<String>,
    /// Reads one out of the text a url carries, the cast from text.
    pub from_text: Option<String>,
    /// Reads one out of the json a body carries, the cast from json.
    pub from_json: Option<String>,
    /// What postgres would put here if a write said nothing, the
    /// default expression as postgres spells it back. Only
    /// `Prefer: missing=default` reads it.
    pub default_expr: Option<String>,
}

/// One row of [`COLUMNS_SQL`], a column and the relation it sits in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRow {
    pub table: String,
    pub column: Column,
}

/// The relations one schema exposes, one name per row. Bind the
/// schema name as $1.
///
/// Partitions are left out, which is upstream's rule and not an
/// accident of the query: a partition is reachable through the table
/// it partitions, and naming one directly is a 404 from PostgREST
/// even though pg_class has it.
pub const RELATIONS_SQL: &str = "\
select c.relname::text
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
 where n.nspname = $1
   and c.relkind in ('r', 'v', 'm', 'f', 'p')
   and not c.relispartition
 order by c.relname";

/// The primary key of each of those relations, one row per table,
/// the columns in the order the key declares them. Bind the schema
/// name as $1, the same one [`RELATIONS_SQL`] took.
///
/// Only tables answer here. A view has no key postgres would record
/// and gets one from [`crate::origin`] instead, out of the columns
/// it borrowed, which is the same place its foreign keys come from.
pub const KEYS_SQL: &str = "\
select t.relname::text,
       (select array_agg(a.attname::text order by k.ord)
          from unnest(c.conkey) with ordinality k(attnum, ord)
          join pg_attribute a
            on a.attrelid = t.oid and a.attnum = k.attnum)
  from pg_constraint c
  join pg_class t on t.oid = c.conrelid
  join pg_namespace n on n.oid = t.relnamespace
 where c.contype = 'p' and n.nspname = $1
 order by t.relname";

/// The columns of those relations, one per row, each with the cast
/// functions its type carries. Bind the schema name as $1, the same
/// one [`RELATIONS_SQL`] took.
///
/// Dropped columns are left out because attnum keeps their slot and
/// nothing else does. Only casts written as a function count, since
/// a binary or i/o cast has no name to call, and postgres quotes the
/// names it hands back so the planner can splice them.
///
/// The type name comes back from format_type, which spells it the
/// way a column definition list has to have it, schema qualified
/// when the search path would not find it and with its modifiers
/// carried along.
///
/// The base type next to it is the same type with the domains taken
/// off, which takes a recursive walk because a domain may be over a
/// domain. A type postgres itself owns is spelled without modifiers,
/// since what asks for this name asks what the type can do and not
/// how wide it is, and a type from anywhere else keeps the nominal
/// spelling, because that is the only name a query may use.
///
/// The default expression is where postgres keeps four different
/// things. A plain default is in pg_attrdef. A domain's default is
/// on the type and only counts when the column has none of its own.
/// An identity column has no default row at all and its sequence is
/// found through the dependency postgres recorded, spelled back as
/// the nextval call that would have been there. A stored generated
/// column has an expression nobody may write to, so it has none
/// here.
pub const COLUMNS_SQL: &str = "\
with recursive base_types as (
select oid,
       typbasetype,
       typnamespace as base_namespace,
       coalesce(nullif(typbasetype, 0), oid) as base_type
  from pg_type
 union
select t.oid,
       b.typbasetype,
       b.typnamespace,
       coalesce(nullif(b.typbasetype, 0), b.oid)
  from base_types t
  join pg_type b on b.oid = t.typbasetype
)
select c.relname::text,
       a.attname::text,
       (select quote_ident(fn.nspname) || '.' || quote_ident(f.proname)
          from pg_cast ct
          join pg_proc f on f.oid = ct.castfunc
          join pg_namespace fn on fn.oid = f.pronamespace
         where ct.castsource = a.atttypid
           and ct.casttarget = 'json'::regtype
           and ct.castmethod = 'f'),
       (select quote_ident(fn.nspname) || '.' || quote_ident(f.proname)
          from pg_cast ct
          join pg_proc f on f.oid = ct.castfunc
          join pg_namespace fn on fn.oid = f.pronamespace
         where ct.castsource = 'text'::regtype
           and ct.casttarget = a.atttypid
           and ct.castmethod = 'f'),
       (select quote_ident(fn.nspname) || '.' || quote_ident(f.proname)
          from pg_cast ct
          join pg_proc f on f.oid = ct.castfunc
          join pg_namespace fn on fn.oid = f.pronamespace
         where ct.castsource = 'json'::regtype
           and ct.casttarget = a.atttypid
           and ct.castmethod = 'f'),
       format_type(a.atttypid, a.atttypmod),
       case when t.typtype = 'd'
              then case when bt.base_namespace = 'pg_catalog'::regnamespace
                          then format_type(bt.base_type, null)
                        else format_type(a.atttypid, a.atttypmod)
                   end
            when t.typnamespace = 'pg_catalog'::regnamespace
              then format_type(a.atttypid, null)
            else format_type(a.atttypid, a.atttypmod)
       end,
       case when t.typbasetype <> 0 and ad.adbin is null
              then pg_get_expr(t.typdefaultbin, 0)
            when a.attidentity = 'd'
              then format('nextval(%L)', seq.objid::regclass)
            when a.attgenerated = 's' then null
            else pg_get_expr(ad.adbin, ad.adrelid)
       end
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  join pg_attribute a on a.attrelid = c.oid
  join pg_type t on t.oid = a.atttypid
  left join base_types bt on bt.oid = a.atttypid and bt.typbasetype = 0
  left join pg_attrdef ad
    on ad.adrelid = a.attrelid and ad.adnum = a.attnum
  left join pg_depend seq
    on seq.refobjid = a.attrelid
   and seq.refobjsubid = a.attnum
   and seq.deptype = 'i'
 where n.nspname = $1
   and c.relkind in ('r', 'v', 'm', 'f', 'p')
   and not c.relispartition
   and a.attnum > 0
   and not a.attisdropped
 order by c.relname, a.attnum";

/// One computed relationship, straight off [`COMPUTED_SQL`].
/// `function` is the name an embed calls it by, `table` the relation
/// whose row it takes, `ftable` the relation whose rows it gives
/// back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputedRow {
    pub function: String,
    pub table: String,
    pub ftable: String,
    /// The function gives back at most one row, either because it
    /// returns a bare row rather than a set or because it told the
    /// planner it returns exactly one. This is the whole of the
    /// cardinality: there is no key to look at.
    pub single: bool,
}

/// The functions of one schema that read as relationships: one
/// argument, a relation's row type in and a relation's row type out.
/// Bind the schema name as $1.
///
/// A row type is how a relation appears in a function signature, so
/// the join back to pg_class is on reltype rather than on any name,
/// and it is what decides the question: a function over an integer or
/// over a composite type nobody has a table for is an ordinary
/// function and not a relationship.
///
/// Rows rather than sets is the cardinality. A function that does not
/// return a set gives back one row by construction, and one that
/// declares it returns a single row is taken at its word, which is
/// how `ROWS 1` on a `SETOF` is written and read.
pub const COMPUTED_SQL: &str = "\
select p.proname::text,
       arg.relname::text,
       ret.relname::text,
       not p.proretset or p.prorows = 1
  from pg_proc p
  join pg_namespace pn on pn.oid = p.pronamespace
  join pg_class arg on arg.reltype = p.proargtypes[0]
  join pg_namespace an on an.oid = arg.relnamespace
  join pg_class ret on ret.reltype = p.prorettype
  join pg_namespace rn on rn.oid = ret.relnamespace
 where p.pronargs = 1
   and arg.relkind in ('r', 'v', 'm', 'f', 'p')
   and ret.relkind in ('r', 'v', 'm', 'f', 'p')
   and pn.nspname = $1
   and an.nspname = $1
   and rn.nspname = $1
 order by p.proname, arg.relname";

/// The media type name a handler answers under when it answers under
/// every one of them.
pub const ANY_MEDIA: &str = "*/*";

/// One media type handler: an aggregate the schema declared over a
/// domain whose name is a media type.
///
/// `target` is the relation whose rows the aggregate takes, and none
/// of it means `anyelement`, which is the same handler offered for
/// every relation. `binary` says the domain sits on bytea, so the
/// value it builds is bytes rather than text and casting it to text
/// would hand back a hex literal instead of the body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handler {
    pub media: String,
    pub target: Option<String>,
    pub aggregate: String,
    pub binary: bool,
}

/// Every media type handler one schema declares. Bind the schema
/// name as $1.
///
/// A handler is an aggregate of one argument whose result type is a
/// domain named after a media type, which is the whole of the
/// registration: there is no table of them to write to, the name of
/// the type is the declaration. The argument says which relations it
/// answers for, a rowtype naming one and `anyelement` naming all of
/// them.
///
/// The vendored names are left out. A schema may well declare a
/// domain called `application/vnd.pgrst.object+json` and upstream
/// goes on answering those itself, because the vendored names say
/// what shape the json is in rather than what the project wanted to
/// render, and a handler cannot take that over.
///
/// `bases` walks the domain down to what it was declared over, since
/// what a handler builds is text of some sort or bytes and only the
/// second may not be written out as text.
pub const HANDLERS_SQL: &str = "\
with recursive aggs as (
  select p.oid, p.prorettype, p.proargtypes[0] as argtype,
         (quote_ident(n.nspname) || '.' || quote_ident(p.proname))::text as call
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
   where n.nspname = $1 and p.prokind = 'a' and p.pronargs = 1
),
bases as (
  select t.oid, t.typbasetype, coalesce(nullif(t.typbasetype, 0), t.oid) as base
    from pg_type t
   where t.oid in (select a.prorettype from aggs a)
  union
  select t.oid, b.typbasetype, coalesce(nullif(b.typbasetype, 0), b.oid)
    from bases t
    join pg_type b on b.oid = t.typbasetype
),
base_types as (select oid, base from bases where typbasetype = 0)
select lower(d.typname)::text,
       (select c.relname::text
          from pg_class c
         where c.oid = at.typrelid
           and c.relkind in ('r', 'v', 'p', 'm', 'f')),
       a.call,
       b.typname = 'bytea'
  from aggs a
  join pg_type d on d.oid = a.prorettype
  join base_types bt on bt.oid = a.prorettype
  join pg_type b on b.oid = bt.base
  join pg_type at on at.oid = a.argtype
 where d.typtype = 'd'
   and (d.typname ~ '^[A-Za-z0-9.-]+/[A-Za-z0-9.+-]+$' or d.typname = '*/*')
   and d.typname not like 'application/vnd.pgrst.%'
   and (at.oid = 'anyelement'::regtype
        or exists (select 1
                     from pg_class c
                    where c.oid = at.typrelid
                      and c.relkind in ('r', 'v', 'p', 'm', 'f')))
 order by d.typname, a.call";

/// How the embedded rows relate to the outer ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// The embed is a single row: the outer table holds the fk, or
    /// the embedded table holds it under a unique constraint.
    ToOne,
    /// The embed is an array of rows.
    ToMany,
}

/// A resolved relationship, everything the join codegen needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rel {
    pub kind: Kind,
    /// The relation on the other side of the join. Not always the
    /// word the embed used: a constraint name or a foreign key
    /// column name names a relationship too, and the client keeps
    /// its own word for the key it gets back.
    pub table: String,
    pub constraint: String,
    /// Column pairs joining the outer table to the embedded table,
    /// or to the junction when `via` is set.
    pub join: Vec<(String, String)>,
    /// The junction of a many to many: its name, the constraint to
    /// the embedded side, and the pairs joining junction columns to
    /// embedded table columns.
    pub via: Option<Junction>,
    /// The call a computed relationship is made of. When it is set
    /// `join` is empty and stays empty: the argument is the join.
    pub call: Option<Call>,
}

/// The function behind a computed relationship, and the relation
/// whose row it takes. Both are needed to write the call: the row
/// goes in cast to that relation, which is what tells postgres which
/// of two functions of the same name was meant, and what lets the row
/// of a mutation's CTE go in at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    pub function: String,
    pub arg: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Junction {
    pub table: String,
    pub constraint: String,
    pub join: Vec<(String, String)>,
}

/// The PostgREST error shape for embedding failures, PGRST200 when
/// no relationship exists and PGRST201 when several do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbedError {
    pub code: &'static str,
    pub message: String,
    pub details: Option<Details>,
    pub hint: Option<String>,
}

/// What an error's details key carries. Most of them are a sentence.
/// An ambiguous embed is the exception: it answers with the list of
/// relationships it found, one object each, since the client that has
/// to choose between them needs to read them apart rather than out of
/// a sentence. This crate has no json in it, so the shape is a type
/// and the server writes it out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Details {
    Text(String),
    Rels(Vec<RelDetail>),
}

impl Details {
    /// The sentence, when the details are one.
    pub fn text(&self) -> Option<&str> {
        match self {
            Details::Text(s) => Some(s),
            Details::Rels(_) => None,
        }
    }
}

/// One relationship as an ambiguity error describes it: the pair being
/// embedded, the cardinality named the way upstream names it, and the
/// relationship written out with the columns it joins on, which is
/// what tells two keys between the same two tables apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelDetail {
    pub cardinality: &'static str,
    pub embedding: String,
    pub relationship: String,
}

impl fmt::Display for EmbedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for EmbedError {}

#[derive(Debug, Default)]
pub struct Catalog {
    fks: Vec<FkRow>,
    computed: Vec<ComputedRow>,
    rels: Vec<Relation>,
    views: HashSet<String>,
    routines: HashMap<String, Vec<Routine>>,
    handlers: Vec<Handler>,
    schema: String,
}

impl Catalog {
    /// A catalog of the foreign keys alone, which is all the embed
    /// planner reads. A catalog built this way knows of no relations
    /// at all, so [`Catalog::relation`] answers none for every name
    /// and the caller must not read that as a missing table.
    pub fn new(fks: Vec<FkRow>) -> Catalog {
        Catalog {
            fks,
            computed: Vec::new(),
            rels: Vec::new(),
            views: HashSet::new(),
            routines: HashMap::new(),
            handlers: Vec::new(),
            schema: String::new(),
        }
    }

    /// The same catalog with the schema's computed relationships in
    /// it, the answer to [`COMPUTED_SQL`]. They are not added to the
    /// foreign keys, they replace them: a function named after a
    /// table the parent already has a key to is how a schema takes
    /// that embed over, and the key it overrides goes away entirely
    /// rather than becoming a second answer.
    pub fn with_computed(self, rows: Vec<ComputedRow>) -> Catalog {
        Catalog {
            computed: rows,
            ..self
        }
    }

    /// The same catalog knowing which of its relations are views,
    /// which resolution reads and nothing else does: a relationship
    /// whose far end is a view can only be embedded under that
    /// view's name, never under the constraint or the column the key
    /// is on. Those two spellings belong to the table the view
    /// borrowed the key from, and upstream keeps them there.
    pub fn with_views(self, names: Vec<String>) -> Catalog {
        Catalog {
            views: names.into_iter().collect(),
            ..self
        }
    }

    /// Whether that name is a view rather than a table.
    pub fn is_view(&self, name: &str) -> bool {
        self.views.contains(name)
    }

    /// The same catalog with the schema's callable functions in it,
    /// the answer to [`crate::rpc::ROUTINES_SQL`] as name and overload
    /// pairs. They arrive in the order the query put them in and stay
    /// in it, since that is the order an ambiguous call reports its
    /// candidates in.
    ///
    /// Functions live here rather than being asked for per call for
    /// the reason the rest of this catalog does: a call needs the
    /// same answer every time until the schema changes, and the epoch
    /// is what says it has.
    pub fn with_routines(self, rows: Vec<(String, Routine)>) -> Catalog {
        let mut routines: HashMap<String, Vec<Routine>> = HashMap::new();
        for (name, routine) in rows {
            routines.entry(name).or_default().push(routine);
        }
        Catalog { routines, ..self }
    }

    /// Every overload of that function this surface can call, which
    /// is none when the schema has no such function, and none as well
    /// when it has one no request could supply the arguments of.
    pub fn routines(&self, name: &str) -> &[Routine] {
        self.routines.get(name).map_or(&[], Vec::as_slice)
    }

    /// The same catalog with the schema's media type handlers in it,
    /// the answer to [`HANDLERS_SQL`].
    pub fn with_handlers(self, handlers: Vec<Handler>) -> Catalog {
        Catalog { handlers, ..self }
    }

    /// The handler that answers a request for that media type on that
    /// relation, if the schema declared one.
    ///
    /// Four lookups in the order upstream weighs them. A handler
    /// written for this relation beats one written for every
    /// relation, and a handler written for the media type asked for
    /// beats one written for all of them, which is what a domain
    /// named `*/*` is: a project saying it will answer whatever the
    /// request wanted and set the content type itself.
    ///
    /// The name has to arrive canonical, type and subtype and nothing
    /// else, since a domain name carries no parameters to compare
    /// against.
    pub fn handler(&self, table: &str, media: &str) -> Option<&Handler> {
        let pick = |name: &str, own: bool| {
            self.handlers.iter().find(|h| {
                h.media == name
                    && match &h.target {
                        Some(t) => own && t == table,
                        None => !own,
                    }
            })
        };
        pick(media, true)
            .or_else(|| pick(media, false))
            .or_else(|| pick(ANY_MEDIA, true))
            .or_else(|| pick(ANY_MEDIA, false))
    }

    /// The same catalog knowing which schema it was read out of,
    /// which only the PGRST200 details say out loud.
    pub fn with_schema(self, schema: &str) -> Catalog {
        Catalog {
            schema: schema.to_string(),
            ..self
        }
    }

    /// The same catalog with the schema's relations in it: the names
    /// [`RELATIONS_SQL`] answered with, and the columns
    /// [`COLUMNS_SQL`] answered with sorted onto them. Both go in at
    /// once because a name with no columns is a relation and a
    /// column with no name to hang on is nothing, and taking them
    /// one at a time would let a caller do it in the order that
    /// loses the columns.
    pub fn with_relations(self, names: Vec<String>, columns: Vec<ColumnRow>) -> Catalog {
        let mut rels: Vec<Relation> = names
            .into_iter()
            .map(|name| Relation {
                name,
                columns: Vec::new(),
                keys: Vec::new(),
            })
            .collect();
        for row in columns {
            if let Some(rel) = rels.iter_mut().find(|r| r.name == row.table) {
                rel.columns.push(row.column);
            }
        }
        Catalog { rels, ..self }
    }

    /// The same catalog with each relation's primary key on it, the
    /// answer to [`KEYS_SQL`] and the one [`crate::origin`] traced
    /// for the views, in either order. A name the catalog does not
    /// have is dropped rather than added, since a key belongs to a
    /// relation and there is no relation here to hang it on.
    pub fn with_keys(mut self, keys: Vec<(String, Vec<String>)>) -> Catalog {
        for (name, columns) in keys {
            if let Some(rel) = self.rels.iter_mut().find(|r| r.name == name) {
                rel.keys = columns;
            }
        }
        self
    }

    /// The foreign keys as introspected, which the OpenAPI document
    /// reads to write its `<fk .../>` column notes.
    pub fn fks(&self) -> &[FkRow] {
        &self.fks
    }

    /// The relation of that name, if the schema has one.
    pub fn relation(&self, name: &str) -> Option<&Relation> {
        self.rels.iter().find(|r| r.name == name)
    }

    /// The relation this name is close enough to be a typo for, when
    /// there is one. PostgREST offers the suggestion at a similarity
    /// of 75% and keeps quiet below it, which is the difference
    /// between `projecxx`, near enough to `projects` to be worth
    /// saying, and `projxxxx`, which is somebody asking for
    /// something else.
    pub fn nearest(&self, name: &str) -> Option<&str> {
        closest(self.rels.iter().map(|r| r.name.as_str()), name, 0.75)
    }

    /// The suggestion an embed that found nothing gets, which depends
    /// on which half of it was wrong. A parent nothing hangs off is
    /// the misspelling, and the schema's related tables are what it
    /// might have meant. A parent that does have relationships is
    /// taken at its word and the target is the misspelling, against
    /// the tables that parent's own relationships reach.
    ///
    /// A target that is already one of those names gets no suggestion
    /// out of it, since then the embed did find the relation and it
    /// is the hint after `!` that went wrong. Upstream drops the
    /// exact match rather than the whole answer, so a second name
    /// close enough to the first can still be offered.
    ///
    /// The two halves are held to different bars. A parent is a table
    /// name and is judged the way [`Catalog::nearest`] judges one, at
    /// three quarters, since the field it is matched against is every
    /// related table in the schema and a loose match there suggests
    /// tables that have nothing to do with the request. A target is
    /// judged more loosely because the field is much smaller: the
    /// tables one table's own relationships reach, where being the
    /// nearest of them means something.
    fn misspelling(&self, parent: &str, target: &str) -> Option<String> {
        let cands = self.candidates(parent);
        let (wrong, names, min) = if cands.is_empty() {
            (parent, self.related(), 0.75)
        } else {
            (
                target,
                cands.iter().map(|c| c.rel.table.clone()).collect(),
                0.5,
            )
        };
        let best = closest(
            names.iter().map(String::as_str).filter(|n| *n != wrong),
            wrong,
            min,
        )?;
        Some(format!("Perhaps you meant '{best}' instead of '{wrong}'."))
    }

    /// Every table in the schema that some relationship starts or
    /// ends at, which is the field a misspelled parent is matched
    /// against. A table no key and no function reaches is not one an
    /// embed could have meant.
    fn related(&self) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        for fk in &self.fks {
            names.push(fk.table.clone());
            names.push(fk.ref_table.clone());
        }
        for c in &self.computed {
            names.push(c.table.clone());
        }
        names.sort();
        names.dedup();
        names
    }

    /// Every relationship leading out of one relation, each with the
    /// name of what sits on the other end. Which of them an embed
    /// means is [`Catalog::resolve`]'s question, and it is a separate
    /// one: a relationship is reachable under its foreign table's
    /// name, under the constraint that makes it, and under the
    /// foreign key column, so the list has to be built before any of
    /// those names is tried.
    fn candidates(&self, parent: &str) -> Vec<Cand> {
        let mut cands: Vec<Cand> = Vec::new();

        for fk in &self.fks {
            // The embedded table holds the fk: to many, or to one
            // behind a unique constraint.
            if fk.ref_table == parent {
                cands.push(Cand {
                    card: if fk.unique { Card::O2O } else { Card::O2M },
                    view: self.is_view(&fk.table),
                    rel: Rel {
                        kind: if fk.unique { Kind::ToOne } else { Kind::ToMany },
                        table: fk.table.clone(),
                        constraint: fk.constraint.clone(),
                        join: pairs(&fk.ref_columns, &fk.columns),
                        via: None,
                        call: None,
                    },
                });
            }
            // The outer table holds the fk: always to one.
            if fk.table == parent {
                cands.push(Cand {
                    card: if fk.unique { Card::O2O } else { Card::M2O },
                    view: self.is_view(&fk.ref_table),
                    rel: Rel {
                        kind: Kind::ToOne,
                        table: fk.ref_table.clone(),
                        constraint: fk.constraint.clone(),
                        join: pairs(&fk.columns, &fk.ref_columns),
                        via: None,
                        call: None,
                    },
                });
            }
        }

        // Junctions: a table whose primary key is fks to both ends.
        for a in &self.fks {
            if !(a.in_pk && a.ref_table == parent) {
                continue;
            }
            for b in &self.fks {
                if !(b.in_pk && b.table == a.table) || std::ptr::eq(a, b) {
                    continue;
                }
                cands.push(Cand {
                    card: Card::M2M,
                    view: self.is_view(&b.ref_table),
                    rel: Rel {
                        kind: Kind::ToMany,
                        table: b.ref_table.clone(),
                        constraint: a.constraint.clone(),
                        join: pairs(&a.ref_columns, &a.columns),
                        via: Some(Junction {
                            table: a.table.clone(),
                            constraint: b.constraint.clone(),
                            join: pairs(&b.columns, &b.ref_columns),
                        }),
                        call: None,
                    },
                });
            }
        }

        // A computed relationship takes over the name it is called
        // by. Every key to a table of that name goes, however the
        // embed would have spelled it, because upstream holds the
        // relationships of a pair under one key and the function
        // replaces what is under it rather than joining it. What the
        // function itself gives back does not come into it: two
        // functions over the same table are two names and never one
        // another's key.
        for c in self.computed.iter().filter(|c| c.table == parent) {
            cands.retain(|cand| cand.rel.call.is_some() || cand.rel.table != c.function);
            cands.push(Cand {
                card: if c.single { Card::M2O } else { Card::O2M },
                view: self.is_view(&c.ftable),
                rel: Rel {
                    kind: if c.single { Kind::ToOne } else { Kind::ToMany },
                    table: c.ftable.clone(),
                    constraint: c.function.clone(),
                    join: Vec::new(),
                    via: None,
                    call: Some(Call {
                        function: c.function.clone(),
                        arg: c.table.clone(),
                    }),
                },
            });
        }

        cands
    }

    /// Resolve the relationship an embed means: `parent` is the
    /// table the request is rooted on, `target` the word the embed
    /// names, `hint` the word after `!` when the client
    /// disambiguates by constraint, fk column, or junction table.
    ///
    /// The target is a name for the relationship rather than for the
    /// relation: `clients`, the foreign table, but equally
    /// `projects_client_id_fkey`, the constraint, or `client_id`,
    /// the column the constraint is on. The last two are refused
    /// when the foreign table is a view, which is what keeps a view
    /// over one end of a key from making every embed by column name
    /// ambiguous.
    ///
    /// A self relationship is the one place the target alone cannot
    /// say which way the join runs, since both ends carry the same
    /// name. The convention is that the table name means the one to
    /// many and the foreign key column means the many to one, and a
    /// hint on the table name means the one to many named by the
    /// column it comes back through.
    pub fn resolve(
        &self,
        parent: &str,
        target: &str,
        hint: Option<&str>,
    ) -> Result<Rel, EmbedError> {
        let mut cands = self.candidates(parent);
        cands.retain(|c| c.wanted(parent, target, hint));
        // Only an error reads the order, and two parts of it do: the
        // details and the hint list them, and a client comparing the
        // two side by side is entitled to find them in the same order.
        // Upstream's is the order its relationships sort in, which is
        // what [`Cand::order`] spells out.
        cands.sort_by_key(Cand::order);

        match cands.len() {
            0 => Err(EmbedError {
                code: "PGRST200",
                message: format!(
                    "Could not find a relationship between '{parent}' and '{target}' in the schema cache"
                ),
                details: Some(Details::Text(format!(
                    "Searched for a foreign key relationship between '{parent}' and '{target}'{} in the schema '{}', but no matches were found.",
                    hint.map(|h| format!(" using the hint '{h}'"))
                        .unwrap_or_default(),
                    self.schema,
                ))),
                hint: self.misspelling(parent, target),
            }),
            1 => Ok(cands.pop().expect("checked len").rel),
            _ => {
                let spellings: Vec<String> = cands.iter().map(Cand::spelled).collect();
                Err(EmbedError {
                    code: "PGRST201",
                    message: format!(
                        "Could not embed because more than one relationship was found for '{parent}' and '{target}'"
                    ),
                    details: Some(Details::Rels(
                        cands.iter().filter_map(|c| c.detail(parent)).collect(),
                    )),
                    hint: Some(format!(
                        "Try changing '{target}' to one of the following: {}. Find the desired relationship in the 'details' key.",
                        spellings
                            .iter()
                            .map(|s| format!("'{s}'"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )),
                })
            }
        }
    }
}

/// One relationship out of the parent, with the cardinality spelled
/// the way upstream spells it. [`Kind`] is coarser on purpose, since
/// codegen only cares whether it is building one row or a list, but
/// resolution reads the difference: the self relationship convention
/// is a rule about direction and nothing else says which way a
/// relationship between a table and itself runs.
struct Cand {
    rel: Rel,
    card: Card,
    /// Whether the far end is a view, which narrows what the embed
    /// may call this relationship.
    view: bool,
}

/// The far table, the cardinality, the junction, the two constraints
/// and the two column lists, which is [`Cand::order`]'s answer written
/// down once rather than three times.
type Order = (
    String,
    u8,
    String,
    String,
    String,
    Vec<(String, String)>,
    Vec<(String, String)>,
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Card {
    O2M,
    M2O,
    O2O,
    M2M,
}

impl Cand {
    /// The one column on the parent's side, when the relationship
    /// joins on exactly one. A junction has none: the columns it
    /// joins on belong to the bridge rather than to either end, and
    /// upstream never matches a name against them.
    fn near_col(&self) -> Option<&str> {
        match (&self.rel.via, self.rel.join.as_slice()) {
            (None, [(near, _)]) => Some(near),
            _ => None,
        }
    }

    /// The one column on the far side, the other half of the same
    /// pair. A hint may name either end, `clients!client_id` and
    /// `clients!id` both reaching the same relationship.
    fn far_col(&self) -> Option<&str> {
        match (&self.rel.via, self.rel.join.as_slice()) {
            (None, [(_, far)]) => Some(far),
            _ => None,
        }
    }

    /// The constraint, when the relationship is made by one. A many
    /// to many is made by two and is named by its junction instead,
    /// so nothing here answers to a constraint name.
    fn cons(&self) -> Option<&str> {
        (self.card != Card::M2M).then_some(self.rel.constraint.as_str())
    }

    /// Whether this is the relationship the embed named.
    fn wanted(&self, parent: &str, target: &str, hint: Option<&str>) -> bool {
        // A computed relationship answers to the function's name and
        // to nothing else. It has no constraint and no column for the
        // other spellings to name, and a hint on one is not an error
        // upstream, it simply makes no difference.
        if let Some(call) = &self.rel.call {
            return target == call.function;
        }
        if self.rel.table == parent {
            // A self relationship, where the table name is the same
            // on both ends and the direction has to come from
            // somewhere else. One to one and many to many are not
            // covered by the convention and upstream leaves them
            // unreachable.
            return match hint {
                None => {
                    (target == self.rel.table && self.card == Card::O2M)
                        || (self.card == Card::M2O && self.near_col() == Some(target))
                }
                Some(h) => {
                    target == self.rel.table && self.card == Card::O2M && self.far_col() == Some(h)
                }
            };
        }
        match hint {
            None => {
                target == self.rel.table
                    // The constraint and the column are the table's
                    // names for the key, and a view that borrowed
                    // the key did not borrow them. Without this a
                    // view over either end of a key would make every
                    // embed by column name ambiguous, since the view
                    // answers to the same column as the table it
                    // took it from.
                    || (!self.view
                        && (self.cons() == Some(target) || self.near_col() == Some(target)))
            }
            Some(h) => {
                target == self.rel.table
                    && (self.cons() == Some(h)
                        || self.near_col() == Some(h)
                        || self.far_col() == Some(h)
                        || self.rel.via.as_ref().is_some_and(|j| j.table == h))
            }
        }
    }

    /// The spelling the ambiguity error offers, the target and the
    /// hint that would have picked this one out.
    fn spelled(&self) -> String {
        // A computed relationship is already spelled the only way it
        // can be, by the function it calls.
        if let Some(call) = &self.rel.call {
            return call.function.clone();
        }
        let by = match &self.rel.via {
            Some(j) => &j.table,
            None => &self.rel.constraint,
        };
        format!("{}!{by}", self.rel.table)
    }

    /// This relationship as the ambiguity error describes it. A key is
    /// named by the constraint and written out from both ends, the
    /// parent's columns and the foreign table's. A many to many is
    /// named by the junction instead and written out as the two keys
    /// the junction holds, so its columns are the bridge's on both
    /// sides rather than either end's.
    ///
    /// A computed relationship has none of that to say and never has
    /// to: it takes over the name it is called by, so it is never one
    /// of several answers. Upstream leaves an empty object here for
    /// the same reason.
    fn detail(&self, parent: &str) -> Option<RelDetail> {
        if self.rel.call.is_some() {
            return None;
        }
        let cols = |names: Vec<&String>| {
            names
                .into_iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        };
        let relationship = match &self.rel.via {
            Some(j) => format!(
                "{} using {}({}) and {}({})",
                j.table,
                self.rel.constraint,
                cols(self.rel.join.iter().map(|(_, far)| far).collect()),
                j.constraint,
                cols(j.join.iter().map(|(near, _)| near).collect()),
            ),
            None => format!(
                "{} using {parent}({}) and {}({})",
                self.rel.constraint,
                cols(self.rel.join.iter().map(|(near, _)| near).collect()),
                self.rel.table,
                cols(self.rel.join.iter().map(|(_, far)| far).collect()),
            ),
        };
        Some(RelDetail {
            cardinality: match self.card {
                Card::O2M => "one-to-many",
                Card::M2O => "many-to-one",
                Card::O2O => "one-to-one",
                Card::M2M => "many-to-many",
            },
            embedding: format!("{parent} with {}", self.rel.table),
            relationship,
        })
    }

    /// Where this relationship falls in the order an error lists them
    /// in: the table on the far end, then the cardinality, then
    /// whatever makes the relationship, the constraint and its columns
    /// for a key and the junction and both of its keys for a many to
    /// many. That is upstream's derived ordering on a relationship,
    /// field by field, and the cardinality is part of it because the
    /// constructors are written one to many, many to one, one to one,
    /// many to many, in that order.
    fn order(&self) -> Order {
        let card = match self.card {
            Card::O2M => 0,
            Card::M2O => 1,
            Card::O2O => 2,
            Card::M2M => 3,
        };
        let (junction, second, target) = match &self.rel.via {
            Some(j) => (j.table.clone(), j.constraint.clone(), j.join.clone()),
            None => (String::new(), String::new(), Vec::new()),
        };
        (
            self.rel.table.clone(),
            card,
            junction,
            self.rel.constraint.clone(),
            second,
            self.rel.join.clone(),
            target,
        )
    }
}

/// The name in the list this word is closest to, when one of them is
/// close enough to be worth suggesting. Ties keep the earlier name,
/// so the suggestion does not depend on the order rows landed in.
/// The name a word is likeliest to be a misspelling of, or none when
/// nothing is close enough.
///
/// A name only runs against the word at all if the two share a run of
/// letters, and then it is scored by [`similarity`]. That is upstream's
/// fuzzy set in the two parts it comes in: an index of letter runs
/// finds the candidates, and the edit distance ranks them. Both parts
/// matter, because the runs are read off the names with punctuation
/// stripped and the distance is not, so `(any_arg)` and `(name)` are
/// three edits apart and still share nothing worth suggesting.
///
/// Runs of three are tried before runs of two, and a suggestion from
/// the longer run wins outright, since a name that shares a whole
/// three letters with the word is a likelier typo than one that
/// happens to share a pair.
pub(crate) fn closest<'a>(
    names: impl Iterator<Item = &'a str> + Clone,
    word: &str,
    min: f64,
) -> Option<&'a str> {
    let lowered = word.to_lowercase();
    for run in [3, 2] {
        let mut best: Option<(f64, &str)> = None;
        for name in names.clone() {
            if !shares_run(&lowered, name, run) {
                continue;
            }
            let score = similarity(&lowered, &name.to_lowercase());
            if score >= min && best.is_none_or(|(seen, _)| score > seen) {
                best = Some((score, name));
            }
        }
        if best.is_some() {
            return best.map(|(_, name)| name);
        }
    }
    None
}

/// Whether two names have any run of that many letters in common.
/// The runs come off the names with everything that is not a letter,
/// a digit, a space or a comma dropped, and with a dash on each end
/// so that a shared beginning or ending counts for something.
fn shares_run(a: &str, b: &str, run: usize) -> bool {
    let held = runs(a, run);
    runs(b, run).iter().any(|r| held.contains(r))
}

fn runs(name: &str, run: usize) -> Vec<String> {
    let kept: String = name
        .to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace() || *c == ',')
        .collect();
    let padded: Vec<char> = format!("-{kept}-").chars().collect();
    (0..padded.len().saturating_sub(run - 1))
        .map(|i| padded[i..i + run].iter().collect())
        .collect()
}

fn pairs(outer: &[String], inner: &[String]) -> Vec<(String, String)> {
    outer.iter().cloned().zip(inner.iter().cloned()).collect()
}

/// How alike two names are, one minus the edit distance over the
/// longer of them, so it runs from 0 for nothing in common to 1 for
/// the same word. Nothing here needs the distance itself, only
/// whether a typo is close enough to be worth suggesting, and a
/// distance on its own cannot say that: two edits is most of a short
/// name and nothing much of a long one.
fn similarity(a: &str, b: &str) -> f64 {
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    let longer = a.len().max(b.len());
    if longer == 0 {
        return 1.0;
    }
    // One row of the Levenshtein matrix at a time, which is all the
    // next row reads.
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut row = vec![0; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        row[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let substitute = prev[j] + usize::from(ca != cb);
            row[j + 1] = substitute.min(prev[j + 1] + 1).min(row[j] + 1);
        }
        std::mem::swap(&mut prev, &mut row);
    }
    1.0 - prev[b.len()] as f64 / longer as f64
}

#[cfg(test)]
mod tests {
    use super::*;

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
                "profiles_user_id_fkey",
                "profiles",
                &["user_id"],
                "users",
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
            fk(
                "employees_manager_id_fkey",
                "employees",
                &["manager_id"],
                "employees",
                &["id"],
            ),
        ];
        // profiles.user_id is unique: users have one profile.
        fks[3].unique = true;
        // order_items is a junction: its pk is (order_id, product_id).
        fks[4].in_pk = true;
        fks[5].in_pk = true;
        Catalog::new(fks)
    }

    #[test]
    fn to_many_and_to_one_by_direction() {
        let c = shop();

        let r = c.resolve("users", "orders", None).unwrap();
        assert_eq!(r.kind, Kind::ToMany);
        assert_eq!(r.constraint, "orders_user_id_fkey");
        assert_eq!(r.join, vec![("id".into(), "user_id".into())]);
        assert!(r.via.is_none());

        let r = c.resolve("orders", "users", None).unwrap();
        assert_eq!(r.kind, Kind::ToOne);
        assert_eq!(r.join, vec![("user_id".into(), "id".into())]);
    }

    #[test]
    fn unique_fk_makes_one_to_one() {
        let c = shop();
        let r = c.resolve("users", "profiles", None).unwrap();
        assert_eq!(r.kind, Kind::ToOne);
        assert_eq!(r.constraint, "profiles_user_id_fkey");
    }

    #[test]
    fn many_to_many_through_the_junction() {
        let c = shop();
        let r = c.resolve("orders", "products", None).unwrap();
        assert_eq!(r.kind, Kind::ToMany);
        assert_eq!(r.join, vec![("id".into(), "order_id".into())]);
        let j = r.via.expect("a junction");
        assert_eq!(j.table, "order_items");
        assert_eq!(j.constraint, "order_items_product_id_fkey");
        assert_eq!(j.join, vec![("product_id".into(), "id".into())]);
    }

    #[test]
    fn ambiguity_is_pgrst201_with_usable_hints() {
        let c = shop();
        let e = c.resolve("orders", "addresses", None).unwrap_err();
        assert_eq!(e.code, "PGRST201");
        let hint = e.hint.expect("a hint");
        assert!(hint.contains("addresses!orders_billing_address_id_fkey"));
        assert!(hint.contains("addresses!orders_shipping_address_id_fkey"));
    }

    /// The details are the list the client has to choose from, so
    /// each relationship is written out from both ends rather than
    /// named. Two keys between the same two tables have the same
    /// everything except the columns, which is the whole reason the
    /// columns are in there.
    #[test]
    fn the_details_write_each_relationship_out_from_both_ends() {
        let c = shop();
        let e = c.resolve("orders", "addresses", None).unwrap_err();
        let Some(Details::Rels(rels)) = e.details else {
            panic!("expected a list of relationships");
        };
        assert_eq!(
            rels,
            vec![
                RelDetail {
                    cardinality: "many-to-one",
                    embedding: "orders with addresses".into(),
                    relationship:
                        "orders_billing_address_id_fkey using orders(billing_address_id) and addresses(id)"
                            .into(),
                },
                RelDetail {
                    cardinality: "many-to-one",
                    embedding: "orders with addresses".into(),
                    relationship:
                        "orders_shipping_address_id_fkey using orders(shipping_address_id) and addresses(id)"
                            .into(),
                },
            ]
        );
    }

    /// A many to many is named by its junction and written out as the
    /// two keys the junction holds, so both column lists are the
    /// bridge's rather than either end's.
    #[test]
    fn a_junction_is_written_out_as_the_two_keys_it_holds() {
        let mut fks = vec![
            fk(
                "main_project",
                "sites",
                &["main_project_id"],
                "big_projects",
                &["big_project_id"],
            ),
            fk("jobs_site_id_fkey", "jobs", &["site_id"], "sites", &["id"]),
            fk(
                "jobs_big_project_id_fkey",
                "jobs",
                &["big_project_id"],
                "big_projects",
                &["big_project_id"],
            ),
        ];
        fks[1].in_pk = true;
        fks[2].in_pk = true;
        let e = Catalog::new(fks)
            .resolve("sites", "big_projects", None)
            .unwrap_err();
        let Some(Details::Rels(rels)) = e.details else {
            panic!("expected a list of relationships");
        };
        assert_eq!(
            rels,
            vec![
                RelDetail {
                    cardinality: "many-to-one",
                    embedding: "sites with big_projects".into(),
                    relationship:
                        "main_project using sites(main_project_id) and big_projects(big_project_id)"
                            .into(),
                },
                RelDetail {
                    cardinality: "many-to-many",
                    embedding: "sites with big_projects".into(),
                    relationship:
                        "jobs using jobs_site_id_fkey(site_id) and jobs_big_project_id_fkey(big_project_id)"
                            .into(),
                },
            ]
        );
    }

    /// Cardinality decides the order before anything the schema is
    /// named after does, and the details and the hint are listed in
    /// the same one: a client reading them side by side is matching
    /// the second entry of the list to the second word of the hint.
    #[test]
    fn the_cardinality_orders_the_list_and_the_hint_follows_it() {
        let c = Catalog::new(vec![
            fk(
                "agents_department_id_fkey",
                "agents",
                &["department_id"],
                "departments",
                &["id"],
            ),
            fk(
                "departments_head_id_fkey",
                "departments",
                &["head_id"],
                "agents",
                &["id"],
            ),
        ]);
        let e = c.resolve("agents", "departments", None).unwrap_err();
        let Some(Details::Rels(rels)) = e.details else {
            panic!("expected a list of relationships");
        };
        assert_eq!(
            rels.iter().map(|r| r.cardinality).collect::<Vec<_>>(),
            vec!["one-to-many", "many-to-one"]
        );
        assert_eq!(
            rels[0].relationship,
            "departments_head_id_fkey using agents(id) and departments(head_id)"
        );
        assert_eq!(
            e.hint.expect("a hint"),
            "Try changing 'departments' to one of the following: \
             'departments!departments_head_id_fkey', \
             'departments!agents_department_id_fkey'. \
             Find the desired relationship in the 'details' key."
        );
    }

    #[test]
    fn hints_resolve_by_constraint_and_by_column() {
        let c = shop();

        let r = c
            .resolve(
                "orders",
                "addresses",
                Some("orders_billing_address_id_fkey"),
            )
            .unwrap();
        assert_eq!(r.join, vec![("billing_address_id".into(), "id".into())]);

        let r = c
            .resolve("orders", "addresses", Some("shipping_address_id"))
            .unwrap();
        assert_eq!(r.join, vec![("shipping_address_id".into(), "id".into())]);

        let r = c
            .resolve("orders", "products", Some("order_items"))
            .unwrap();
        assert!(r.via.is_some());
    }

    #[test]
    fn missing_is_pgrst200() {
        let c = shop();
        let e = c.resolve("users", "products", None).unwrap_err();
        assert_eq!(e.code, "PGRST200");
        assert!(e.message.contains("'users' and 'products'"));

        let e = c
            .resolve("orders", "addresses", Some("no_such_fk"))
            .unwrap_err();
        assert_eq!(e.code, "PGRST200");
    }

    /// Which half of the embed the suggestion is about depends on
    /// which half the schema recognises.
    #[test]
    fn a_relationship_nobody_has_suggests_the_name_that_is_near_it() {
        let c = shop();

        assert_eq!(
            c.resolve("users", "order", None).unwrap_err().hint,
            Some("Perhaps you meant 'orders' instead of 'order'.".to_string())
        );

        // Nothing hangs off 'userss', so it is the word that is
        // wrong, and the field is every table a relationship reaches.
        assert_eq!(
            c.resolve("userss", "orders", None).unwrap_err().hint,
            Some("Perhaps you meant 'users' instead of 'userss'.".to_string())
        );

        // Not every miss is a typo. 'product' is nothing like the
        // tables users reaches, and a suggestion would be noise.
        assert_eq!(c.resolve("users", "product", None).unwrap_err().hint, None);

        // A parent is matched against every related table in the
        // schema, so it is held to the bar a table name is held to.
        // Halfway to 'order_items' is not a misspelling of it.
        assert_eq!(
            c.resolve("produc_items", "orders", None).unwrap_err().hint,
            None
        );
    }

    /// A target that is a relationship of the parent is not the
    /// misspelling, whatever else the schema is named: the embed
    /// found it and the hint after `!` is what went wrong.
    #[test]
    fn a_target_that_exists_is_never_the_suggestion() {
        let c = shop();
        let e = c
            .resolve("orders", "users", Some("no_such_fk"))
            .unwrap_err();
        assert_eq!(e.code, "PGRST200");
        assert_eq!(e.hint, None);
    }

    #[test]
    fn a_self_reference_takes_its_direction_by_convention() {
        let c = shop();

        // The table name is the list of employees who report here.
        let r = c.resolve("employees", "employees", None).unwrap();
        assert_eq!(r.kind, Kind::ToMany);
        assert_eq!(r.join, vec![("id".into(), "manager_id".into())]);

        // The column name is the one they report to.
        let r = c.resolve("employees", "manager_id", None).unwrap();
        assert_eq!(r.kind, Kind::ToOne);
        assert_eq!(r.table, "employees");
        assert_eq!(r.join, vec![("manager_id".into(), "id".into())]);

        // A hint on the table name names the column the list comes
        // back through, which is the only spelling that survives
        // more than one self reference.
        let r = c
            .resolve("employees", "employees", Some("manager_id"))
            .unwrap();
        assert_eq!(r.kind, Kind::ToMany);
        let e = c.resolve("employees", "employees", Some("id")).unwrap_err();
        assert_eq!(e.code, "PGRST200");
    }

    #[test]
    fn a_computed_relationship_answers_to_the_function_and_nothing_else() {
        let c = shop().with_computed(vec![ComputedRow {
            function: "recent_orders".into(),
            table: "users".into(),
            ftable: "orders".into(),
            single: false,
        }]);

        let r = c.resolve("users", "recent_orders", None).unwrap();
        assert_eq!(r.kind, Kind::ToMany);
        assert_eq!(r.table, "orders");
        assert_eq!(r.call.unwrap().arg, "users");
        assert!(r.join.is_empty());

        // A hint makes no difference to one, which is upstream's
        // answer too: the filter that picks a computed relationship
        // out reads the target and never looks at the hint.
        assert!(
            c.resolve("users", "recent_orders", Some("nonsense"))
                .is_ok()
        );

        // The function is a name the parent has, not one the schema
        // has: the same word from anywhere else is a 400 as before.
        assert_eq!(
            c.resolve("orders", "recent_orders", None).unwrap_err().code,
            "PGRST200"
        );
    }

    /// A function whose name is a table the parent already reaches
    /// replaces that whole relationship, so the key it overrides
    /// stops answering to its constraint and its column as well.
    #[test]
    fn a_function_named_after_a_table_replaces_the_key_to_it() {
        let c = shop().with_computed(vec![ComputedRow {
            function: "users".into(),
            table: "orders".into(),
            ftable: "users".into(),
            single: true,
        }]);

        let r = c.resolve("orders", "users", None).unwrap();
        assert_eq!(r.kind, Kind::ToOne);
        assert!(r.call.is_some());
        for spelling in ["orders_user_id_fkey", "user_id"] {
            assert_eq!(
                c.resolve("orders", spelling, None).unwrap_err().code,
                "PGRST200",
                "{spelling}"
            );
        }

        // Only the key of that pair goes. The other way round is a
        // different key and the function does not reach it.
        assert!(c.resolve("users", "orders", None).is_ok());
    }

    #[test]
    fn a_relationship_answers_to_its_constraint_and_its_column() {
        let c = shop();

        let r = c.resolve("orders", "orders_user_id_fkey", None).unwrap();
        assert_eq!(r.kind, Kind::ToOne);
        assert_eq!(r.table, "users");

        let r = c.resolve("orders", "user_id", None).unwrap();
        assert_eq!(r.kind, Kind::ToOne);
        assert_eq!(r.table, "users");
        assert_eq!(r.join, vec![("user_id".into(), "id".into())]);

        // The other side answers to its own constraint, and the
        // column that names it there is the one the parent joins on.
        let r = c.resolve("users", "orders_user_id_fkey", None).unwrap();
        assert_eq!(r.kind, Kind::ToMany);
        assert_eq!(r.table, "orders");

        // A hint may name either end of the pair.
        let r = c.resolve("orders", "users", Some("id")).unwrap();
        assert_eq!(r.join, vec![("user_id".into(), "id".into())]);
    }

    /// A view over one end of a key answers to its own name and to
    /// nothing else, or every embed by column name would be
    /// ambiguous the moment somebody wrote a view.
    #[test]
    fn a_view_does_not_answer_to_the_key_the_table_lent_it() {
        let c = Catalog::new(vec![
            fk(
                "orders_user_id_fkey",
                "orders",
                &["user_id"],
                "users",
                &["id"],
            ),
            // What deriving a view relationship adds: the same key
            // again, pointing at a view over the users table.
            fk(
                "orders_user_id_fkey",
                "orders",
                &["user_id"],
                "user_list",
                &["id"],
            ),
        ])
        .with_views(names(&["user_list"]));

        // Both are reachable by name.
        assert_eq!(c.resolve("orders", "users", None).unwrap().table, "users");
        assert_eq!(
            c.resolve("orders", "user_list", None).unwrap().table,
            "user_list"
        );

        // The constraint and the column pick the table out, and the
        // view sitting on the same key is not a second answer.
        assert_eq!(
            c.resolve("orders", "orders_user_id_fkey", None)
                .unwrap()
                .table,
            "users"
        );
        assert_eq!(c.resolve("orders", "user_id", None).unwrap().table, "users");

        // Without knowing which is the view there is no way to
        // choose, and both spellings are ambiguous.
        let blind = Catalog::new(c.fks().to_vec());
        assert_eq!(
            blind.resolve("orders", "user_id", None).unwrap_err().code,
            "PGRST201"
        );
    }

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|n| n.to_string()).collect()
    }

    fn col(table: &str, column: &str) -> ColumnRow {
        ColumnRow {
            table: table.to_string(),
            column: Column {
                name: column.to_string(),
                ..Column::default()
            },
        }
    }

    #[test]
    fn a_relation_is_looked_up_by_name_with_its_columns() {
        let c = Catalog::new(Vec::new()).with_relations(
            names(&["projects", "tasks"]),
            vec![
                col("projects", "id"),
                col("projects", "name"),
                col("tasks", "id"),
                // A column of something outside the list has nowhere
                // to go and goes nowhere.
                col("secrets", "token"),
            ],
        );
        let projects = c.relation("projects").expect("the relation");
        assert!(projects.has("id") && projects.has("name"));
        assert!(!projects.has("token"));
        assert_eq!(c.relation("tasks").map(|r| r.columns.len()), Some(1));
        assert!(c.relation("nope").is_none());
        // A catalog nobody gave relations to has none, rather than
        // claiming every name is missing.
        assert!(Catalog::new(Vec::new()).relation("projects").is_none());
    }

    #[test]
    fn a_column_carries_the_casts_its_type_was_given() {
        let c = Catalog::new(Vec::new()).with_relations(
            names(&["todos"]),
            vec![
                ColumnRow {
                    table: "todos".into(),
                    column: Column {
                        name: "label_color".into(),
                        to_json: Some("test.json".into()),
                        from_text: Some("test.color".into()),
                        from_json: Some("test.color".into()),
                        type_name: "test.color".into(),
                        base_type: "test.color".into(),
                        default_expr: None,
                    },
                },
                col("todos", "name"),
            ],
        );
        let todos = c.relation("todos").expect("the relation");
        assert_eq!(
            todos
                .column("label_color")
                .and_then(|c| c.to_json.as_deref()),
            Some("test.json")
        );
        assert!(todos.column("name").expect("a column").to_json.is_none());
        assert!(todos.represented());

        let plain =
            Catalog::new(Vec::new()).with_relations(names(&["todos"]), vec![col("todos", "name")]);
        assert!(!plain.relation("todos").expect("the relation").represented());
    }

    #[test]
    fn a_near_enough_name_is_worth_suggesting() {
        // The three the recordings pin down, one edit, two edits,
        // and four, against a name of eight characters.
        let c = Catalog::new(Vec::new()).with_relations(
            names(&["big_projects", "products", "profiles", "projects"]),
            Vec::new(),
        );
        assert_eq!(c.nearest("projectx"), Some("projects"));
        assert_eq!(c.nearest("projecxx"), Some("projects"));
        assert_eq!(c.nearest("projxxxx"), None);
        assert_eq!(c.nearest("projects"), Some("projects"));
    }

    #[test]
    fn similarity_is_the_edit_distance_over_the_longer_name() {
        assert_eq!(similarity("same", "same"), 1.0);
        assert_eq!(similarity("", ""), 1.0);
        assert_eq!(similarity("abcd", "abcx"), 0.75);
        assert_eq!(similarity("abcd", "xyzw"), 0.0);
        // The same two edits against a longer name, which is why the
        // distance alone cannot decide anything.
        assert_eq!(similarity("ab", "abcd"), 0.5);
        assert_eq!(similarity("abcdefgh", "abcdefxy"), 0.75);
    }

    #[test]
    fn a_suggestion_needs_a_shared_run_before_it_is_scored() {
        let names = ["(name)"];
        // Three edits over six letters is halfway, well over the bar a
        // parameter list is held to, and still no suggestion: the two
        // share no run of letters once the punctuation is dropped.
        assert!(similarity("(any_arg)", "(name)") > 0.33);
        assert_eq!(closest(names.into_iter(), "(any_arg)", 0.33), None);

        // A run of two is enough when nothing shares a run of three.
        assert_eq!(
            closest(["(x, y)"].into_iter(), "(a, b)", 0.33),
            Some("(x, y)")
        );
    }

    #[test]
    fn the_longer_run_decides_before_the_shorter_one_is_read() {
        // `(x, y)` is the closer of the two by score, and it never
        // gets compared: `(a, bcdef)` shares a run of three, which
        // settles the suggestion before any run of two is looked at.
        let names = ["(a, bcdef)", "(x, y)"];
        assert!(similarity("(a, b)", "(x, y)") > similarity("(a, b)", "(a, bcdef)"));
        assert_eq!(
            closest(names.into_iter(), "(a, b)", 0.33),
            Some("(a, bcdef)")
        );
    }

    #[test]
    fn ties_and_scores_pick_among_what_the_run_turned_up() {
        // Two overloads sharing a run with the call, the nearer one
        // suggested.
        let names = ["(a, b)", "(a, b, c)"];
        assert_eq!(
            closest(names.into_iter(), "(a, b, wrong_arg)", 0.33),
            Some("(a, b, c)")
        );
        // Nothing close enough is no suggestion, however many share a
        // run.
        assert_eq!(
            closest(names.into_iter(), "(a, b, c, d, e, f, g)", 0.75),
            None
        );
    }
}
