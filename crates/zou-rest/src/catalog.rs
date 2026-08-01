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
//! given the table a request is rooted on and the relation an embed
//! names, which relationship is meant, with the PGRST200 and
//! PGRST201 errors PostgREST clients branch on when the answer is
//! none or several.

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
}

impl Catalog {
    pub fn new(fks: Vec<FkRow>) -> Catalog {
        Catalog { fks }
    }

    /// Resolve the relationship an embed means: `parent` is the
    /// table the request is rooted on, `target` the relation the
    /// embed names, `hint` the word after `!` when the client
    /// disambiguates by constraint, fk column, or junction table.
    pub fn resolve(
        &self,
        parent: &str,
        target: &str,
        hint: Option<&str>,
    ) -> Result<Rel, EmbedError> {
        let mut cands: Vec<Cand> = Vec::new();

        for fk in &self.fks {
            // The embedded table holds the fk: to many, or to one
            // behind a unique constraint.
            if fk.table == target && fk.ref_table == parent {
                cands.push(Cand {
                    rel: Rel {
                        kind: if fk.unique { Kind::ToOne } else { Kind::ToMany },
                        constraint: fk.constraint.clone(),
                        join: pairs(&fk.ref_columns, &fk.columns),
                        via: None,
                    },
                    fk_columns: fk.columns.clone(),
                    spelled: format!("{target}!{}", fk.constraint),
                });
            }
            // The outer table holds the fk: always to one.
            if fk.table == parent && fk.ref_table == target {
                cands.push(Cand {
                    rel: Rel {
                        kind: Kind::ToOne,
                        constraint: fk.constraint.clone(),
                        join: pairs(&fk.columns, &fk.ref_columns),
                        via: None,
                    },
                    fk_columns: fk.columns.clone(),
                    spelled: format!("{target}!{}", fk.constraint),
                });
            }
        }

        // Junctions: a table whose primary key is fks to both ends.
        for a in &self.fks {
            if !(a.in_pk && a.ref_table == parent) {
                continue;
            }
            for b in &self.fks {
                if !(b.in_pk && b.table == a.table && b.ref_table == target) {
                    continue;
                }
                if std::ptr::eq(a, b) {
                    continue;
                }
                cands.push(Cand {
                    rel: Rel {
                        kind: Kind::ToMany,
                        constraint: a.constraint.clone(),
                        join: pairs(&a.ref_columns, &a.columns),
                        via: Some(Junction {
                            table: a.table.clone(),
                            constraint: b.constraint.clone(),
                            join: pairs(&b.columns, &b.ref_columns),
                        }),
                    },
                    fk_columns: Vec::new(),
                    spelled: format!("{target}!{}", a.table),
                });
            }
        }

        if let Some(h) = hint {
            cands.retain(|c| {
                c.rel.constraint == h
                    || (c.fk_columns.len() == 1 && c.fk_columns[0] == h)
                    || c.rel.via.as_ref().is_some_and(|j| j.table == h)
            });
        }

        match cands.len() {
            0 => Err(EmbedError {
                code: "PGRST200",
                message: format!(
                    "Could not find a relationship between '{parent}' and '{target}' in the schema cache"
                ),
                details: Some(format!(
                    "Searched for a foreign key relationship between '{parent}' and '{target}' in the schema cache"
                )),
                hint: None,
            }),
            1 => Ok(cands.pop().expect("checked len").rel),
            _ => {
                let spellings: Vec<String> = cands.iter().map(|c| c.spelled.clone()).collect();
                Err(EmbedError {
                    code: "PGRST201",
                    message: format!(
                        "Could not embed because more than one relationship was found for '{parent}' and '{target}'"
                    ),
                    details: Some(spellings.join(", ")),
                    hint: Some(format!(
                        "Try changing '{target}' to one of the following: {}. Find the desired relationship in the 'details' key.",
                        spellings.join(", ")
                    )),
                })
            }
        }
    }
}

struct Cand {
    rel: Rel,
    /// The fk columns on whichever side holds them, what a column
    /// hint matches.
    fk_columns: Vec<String>,
    /// The hint spelling offered in the ambiguity error.
    spelled: String,
}

fn pairs(outer: &[String], inner: &[String]) -> Vec<(String, String)> {
    outer.iter().cloned().zip(inner.iter().cloned()).collect()
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
    fn self_reference_needs_a_direction() {
        let c = shop();
        // Both directions of manager_id match, even the column hint
        // cannot split them.
        let e = c.resolve("employees", "employees", None).unwrap_err();
        assert_eq!(e.code, "PGRST201");
    }
}
