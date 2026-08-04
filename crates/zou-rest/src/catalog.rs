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

use std::collections::HashSet;
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
/// The default expression is where postgres keeps four different
/// things. A plain default is in pg_attrdef. A domain's default is
/// on the type and only counts when the column has none of its own.
/// An identity column has no default row at all and its sequence is
/// found through the dependency postgres recorded, spelled back as
/// the nextval call that would have been there. A stored generated
/// column has an expression nobody may write to, so it has none
/// here.
pub const COLUMNS_SQL: &str = "\
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

/// Every timezone name postgres will accept, which is what decides
/// whether `Prefer: timezone=` names a real one. It takes no
/// parameter: the list is the installation's and not a schema's, and
/// it is here rather than anywhere else because upstream's schema
/// cache holds it too, loaded and expired with everything else the
/// cache holds.
pub const TIMEZONES_SQL: &str = "select name::text from pg_timezone_names";

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
    pub details: Option<String>,
    pub hint: Option<String>,
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
    rels: Vec<Relation>,
    timezones: HashSet<String>,
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
            rels: Vec::new(),
            timezones: HashSet::new(),
            schema: String::new(),
        }
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
            })
            .collect();
        for row in columns {
            if let Some(rel) = rels.iter_mut().find(|r| r.name == row.table) {
                rel.columns.push(row.column);
            }
        }
        Catalog { rels, ..self }
    }

    /// The same catalog knowing which timezone names postgres has,
    /// the answer to [`TIMEZONES_SQL`].
    pub fn with_timezones(self, names: Vec<String>) -> Catalog {
        Catalog {
            timezones: names.into_iter().collect(),
            ..self
        }
    }

    /// Whether postgres would take this as a timezone. The names are
    /// case sensitive here because they are case sensitive there: a
    /// `SET timezone` to `utc` is not the `UTC` pg_timezone_names
    /// lists, and upstream refuses it on exactly that ground.
    pub fn has_timezone(&self, name: &str) -> bool {
        self.timezones.contains(name)
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
        let mut best: Option<(f64, &str)> = None;
        for rel in &self.rels {
            let score = similarity(name, &rel.name);
            // Strictly better, so a tie keeps the earlier name and
            // the suggestion does not depend on how the rows landed.
            if score >= 0.75 && best.is_none_or(|(seen, _)| score > seen) {
                best = Some((score, &rel.name));
            }
        }
        best.map(|(_, name)| name)
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
                    rel: Rel {
                        kind: if fk.unique { Kind::ToOne } else { Kind::ToMany },
                        table: fk.table.clone(),
                        constraint: fk.constraint.clone(),
                        join: pairs(&fk.ref_columns, &fk.columns),
                        via: None,
                    },
                });
            }
            // The outer table holds the fk: always to one.
            if fk.table == parent {
                cands.push(Cand {
                    card: if fk.unique { Card::O2O } else { Card::M2O },
                    rel: Rel {
                        kind: Kind::ToOne,
                        table: fk.ref_table.clone(),
                        constraint: fk.constraint.clone(),
                        join: pairs(&fk.columns, &fk.ref_columns),
                        via: None,
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
                    },
                });
            }
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
    /// the column the constraint is on. Upstream refuses the last
    /// two when the foreign table is a view, which cannot arise
    /// here because a foreign key never points at one.
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

        match cands.len() {
            0 => Err(EmbedError {
                code: "PGRST200",
                message: format!(
                    "Could not find a relationship between '{parent}' and '{target}' in the schema cache"
                ),
                details: Some(format!(
                    "Searched for a foreign key relationship between '{parent}' and '{target}'{} in the schema '{}', but no matches were found.",
                    hint.map(|h| format!(" using the hint '{h}'"))
                        .unwrap_or_default(),
                    self.schema,
                )),
                hint: None,
            }),
            1 => Ok(cands.pop().expect("checked len").rel),
            _ => {
                let spellings: Vec<String> = cands.iter().map(Cand::spelled).collect();
                Err(EmbedError {
                    code: "PGRST201",
                    message: format!(
                        "Could not embed because more than one relationship was found for '{parent}' and '{target}'"
                    ),
                    details: Some(spellings.join(", ")),
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
}

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
                    || self.cons() == Some(target)
                    || self.near_col() == Some(target)
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
        let by = match &self.rel.via {
            Some(j) => &j.table,
            None => &self.rel.constraint,
        };
        format!("{}!{by}", self.rel.table)
    }
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
}
