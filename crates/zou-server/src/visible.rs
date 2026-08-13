//! What a subscriber may see of a change, asked of the database.
//!
//! A change feed that ignored row level security would be a way to read
//! every table in a project through a socket, which is not a missing
//! feature but a hole in the ordinary sql permission model: the same
//! person hitting `/rest/v1/todos` gets their own rows and would get
//! everybody's by subscribing instead. So a row is not sent to anybody
//! the database would not have shown it to, and it is the database that
//! is asked rather than a rule written here.
//!
//! Upstream asks the same question the same way. Walrus becomes the
//! subscriber, with their claims in `request.jwt.claims` where
//! `auth.uid()` reads them, and selects the changed row back by its
//! primary key: if a policy lets it through, the subscriber may see it.
//! That is the check here, statement for statement, because a policy
//! written for Supabase has to mean the same thing.
//!
//! Three parts of the answer, and none of them is a yes or no about the
//! whole change.
//!
//! Which columns. `has_column_privilege` per column per role, which is
//! what a `grant select (id, title)` means, and the answer shapes both
//! the record and the columns metadata a client reads.
//!
//! Whether the row. The exists check above, and only when the table has
//! row level security enabled, since a table without it is readable by
//! anybody with select on it.
//!
//! And what a delete says. There is no row left to check a policy
//! against, so upstream publishes deletes to every subscriber and cuts
//! the old record down to the primary key when the table has row level
//! security on. What a subscriber learns is that a row with that key is
//! gone, which is the compromise upstream made and one this server has
//! to make the same way or a project's policies mean something
//! different here.
//!
//! The key that cut is made on comes from the catalog and not from the
//! change, because the two are not the same thing on a table with
//! `replica identity full`: pgoutput flags every column of such a table
//! as part of the identity, so a cut made on the flags would keep the
//! whole deleted row and publish it to subscribers a policy hides it
//! from. Upstream cuts on `(c).is_pkey` for the same reason, with the
//! same comment next to it about not being able to secure a delete any
//! other way.
//!
//! Two things this cannot fix, both upstream's too and both worth
//! knowing. The check reads the table as it is now, so a row an update
//! moved out of a policy's view is checked in its new state, and a row
//! deleted immediately after being updated is gone before the check
//! runs. And a table with no primary key cannot be checked at all,
//! which is an error in the payload rather than a silence.

use serde_json::Value;

use crate::payload::{NO_KEY, Seen, UNAUTHORIZED};
use crate::pgoutput::{Cell, Change, Op, Relation, Replica};
use crate::sql::{Error, Pool, Session};

/// Who is asking, which is the role and the claims a policy reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Asker {
    /// The postgres role the claims name, which is `anon` when they
    /// name none.
    pub role: String,
    /// The whole claim set, which is what `auth.uid()` and anything
    /// else a policy reads comes out of.
    pub claims: Value,
}

impl Asker {
    fn sub(&self) -> String {
        self.claims
            .get("sub")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    }
}

/// What this asker may see of this change.
///
/// One database round trip when the table has no row level security on
/// it, two when it has, and the second one is a primary key lookup as
/// the subscriber. That cost is per asker per change, which is what
/// upstream pays as well: a policy is a function of the row and of who
/// is asking, so there is no answer to cache across either.
pub async fn seen(pool: &Pool, asker: &Asker, change: &Change) -> Result<Seen, String> {
    let sess = pool.admin().await.map_err(unreachable)?;
    let seen = looking(&sess, asker, change).await;
    // Always, whatever happened: the role and the claims are set with
    // `set_config` local to this transaction, so the rollback is what
    // puts the connection back the way the pool handed it over.
    if let Err(e) = sess.rollback().await {
        log::warn!("realtime: a visibility check would not roll back, {e}");
    }
    seen
}

async fn looking(sess: &Session, asker: &Asker, change: &Change) -> Result<Seen, String> {
    let relation = &change.relation;
    let (rls, columns, keys) = privileges(sess, asker, relation).await?;
    let mut seen = Seen {
        row: true,
        columns,
        keys: identifying(relation, keys),
        keys_only: false,
        error: None,
    };

    // A delete is published to everybody, because the row it was about
    // is gone and no policy can be asked about it. What it says is cut
    // down instead.
    if change.op == Op::Delete {
        seen.keys_only = rls;
        return Ok(seen);
    }

    let keys: Vec<usize> = seen
        .keys
        .iter()
        .enumerate()
        .filter(|(_, key)| **key)
        .map(|(at, _)| at)
        .collect();
    if keys.is_empty() {
        // Nothing can name the row, so nothing can check it. Upstream
        // sends the reason rather than the row.
        seen.error = Some(NO_KEY);
        return Ok(seen);
    }
    if keys.iter().any(|at| !seen.columns[*at]) {
        seen.error = Some(UNAUTHORIZED);
        return Ok(seen);
    }
    if !rls {
        return Ok(seen);
    }

    seen.row = allowed(sess, asker, relation, &keys, change).await?;
    Ok(seen)
}

/// Which columns name the row, which is the primary key.
///
/// The one case where there is no primary key and something still names
/// the row is a table with `replica identity index`: postgres will not
/// publish an update or a delete of one without the index's columns, so
/// they identify a row as exactly as a key would and are what the
/// change carries. Flags are read for that and only that, because under
/// `replica identity full` every column carries the flag and none of
/// them names anything.
fn identifying(relation: &Relation, keys: Vec<bool>) -> Vec<bool> {
    if keys.iter().any(|key| *key) || relation.replica == Replica::Full {
        return keys;
    }
    relation.columns.iter().map(|column| column.key).collect()
}

/// Whether row level security is on, which columns this role may
/// select, and which of them are the primary key.
///
/// All three come from the catalog rather than from a check as the
/// user, which is what `has_column_privilege` taking a role is for: a
/// privilege is a fact about a grant and does not need anybody to be
/// impersonated to be read.
///
/// A role the database does not have answers null, which is read as a
/// no. That is a token naming a role somebody dropped, and a subscriber
/// with one should see nothing rather than everything.
async fn privileges(
    sess: &Session,
    asker: &Asker,
    relation: &Relation,
) -> Result<(bool, Vec<bool>, Vec<bool>), String> {
    let names: Vec<String> = relation.columns.iter().map(|c| c.name.clone()).collect();
    let oid = relation.oid as i64;
    // The left join is what keeps a column dropped since the relation
    // message was written from raising: `has_column_privilege` is
    // strict, so a column that is not there answers null and the
    // coalesce reads that as a no.
    let rows = sess
        .query(
            "select c.relrowsecurity,
                    array(
                        select coalesce(
                            has_column_privilege(to_regrole($2)::oid, a.attrelid, a.attnum, 'SELECT'),
                            false
                        )
                        from unnest($3::text[]) with ordinality as n(name, at)
                        left join pg_attribute a
                          on a.attrelid = c.oid and a.attname = n.name
                         and a.attnum > 0 and not a.attisdropped
                        order by n.at
                    ),
                    array(
                        select coalesce(a.attnum = any(k.keys), false)
                        from unnest($3::text[]) with ordinality as n(name, at)
                        left join pg_attribute a
                          on a.attrelid = c.oid and a.attname = n.name
                         and a.attnum > 0 and not a.attisdropped
                        order by n.at
                    )
             from pg_class c
             left join lateral (
                 select i.indkey::int2[] as keys from pg_index i
                  where i.indrelid = c.oid and i.indisprimary limit 1
             ) k on true
             where c.oid = $1::int8::oid",
            &[&oid, &asker.role, &names],
        )
        .await
        .map_err(refused)?;
    let Some(row) = rows.first() else {
        // The relation is gone, which is a table dropped between the
        // change and this question. Nobody may see it.
        let none = vec![false; relation.columns.len()];
        return Ok((true, none.clone(), none));
    };
    Ok((row.get(0), row.get(1), row.get(2)))
}

/// Whether the policies on the table let this asker select the row that
/// changed, which is asked by becoming them and selecting it.
async fn allowed(
    sess: &Session,
    asker: &Asker,
    relation: &Relation,
    keys: &[usize],
    change: &Change,
) -> Result<bool, String> {
    // The type each key is compared as, which has to come from the
    // catalog: a value arrives as text and comparing it as text would
    // both miss an index and be wrong about anything postgres prints
    // more than one way.
    let types = key_types(sess, relation, keys).await?;
    let mut where_ = String::new();
    let mut values: Vec<Option<String>> = Vec::new();
    for (nth, at) in keys.iter().enumerate() {
        if nth > 0 {
            where_.push_str(" and ");
        }
        // The identifier is quoted and the type comes from the catalog,
        // so neither is a value anybody outside the database chose. The
        // value itself is a parameter.
        where_.push_str(&format!(
            "{} = cast(${}::text as {})",
            ident(&relation.columns[*at].name),
            nth + 1,
            types[nth]
        ));
        // A key postgres left out is a key nothing wrote, so the row
        // still has whatever it had, and the old tuple has it at the
        // same place. A key nothing at all has is null, and a
        // comparison against null is null, which is the no that
        // upstream's generated statement gives as well.
        let cell = match change.record.get(*at) {
            Some(Cell::Unchanged) | None => change.old.as_ref().and_then(|old| old.get(*at)),
            cell => cell,
        };
        values.push(match cell {
            Some(Cell::Text(text)) => Some(text.clone()),
            Some(Cell::Bytes(bytes)) => Some(format!("\\x{}", hex(bytes))),
            _ => None,
        });
    }
    let sql = format!(
        "select exists (select 1 from {}.{} where {where_})",
        ident(&relation.schema),
        ident(&relation.table)
    );

    if let Err(e) = become_user(sess, asker).await {
        // A role that cannot be set is a role that cannot see
        // anything, which is the same answer as a policy saying no and
        // a safer one than an error that skips the check.
        log::warn!("realtime: no such role {}, {e}", asker.role);
        return Ok(false);
    }
    let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = values
        .iter()
        .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    // Nothing else runs on this session as the subscriber: this is the
    // last question, and the rollback the caller always does is what
    // puts the role and the claims back.
    match sess.query(&sql, &params).await {
        Ok(rows) => Ok(rows.first().map(|row| row.get(0)).unwrap_or(false)),
        // A select the role has no privilege for at all, which is a
        // no rather than a failure: `revoke select` is how a project
        // says nobody may read this.
        Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE) => {
            Ok(false)
        }
        Err(e) => Err(refused(e)),
    }
}

/// What each key column is declared as, in the form a cast can use.
///
/// `format_type` is the catalog's own rendering of a type, so it
/// carries the length of a `varchar(10)` and the schema of a type that
/// is not on the search path, and it is what postgres itself prints in
/// `\d`.
async fn key_types(
    sess: &Session,
    relation: &Relation,
    keys: &[usize],
) -> Result<Vec<String>, String> {
    let names: Vec<String> = keys
        .iter()
        .map(|at| relation.columns[*at].name.clone())
        .collect();
    let oid = relation.oid as i64;
    let rows = sess
        .query(
            "select format_type(a.atttypid, a.atttypmod)
             from unnest($2::text[]) with ordinality as n(name, at)
             join pg_attribute a on a.attrelid = $1::int8::oid and a.attname = n.name
             order by n.at",
            &[&oid, &names],
        )
        .await
        .map_err(refused)?;
    if rows.len() != keys.len() {
        return Err("realtime: the changed table's key columns are not in the catalog".to_string());
    }
    Ok(rows.iter().map(|row| row.get(0)).collect())
}

/// Everything a policy reads about who is asking, all of it local to
/// the transaction and so gone with the rollback.
///
/// The names are upstream's, and they have to be: a policy written for
/// Supabase reads `auth.uid()`, which reads `request.jwt.claims`.
async fn become_user(sess: &Session, asker: &Asker) -> Result<(), Error> {
    let claims = asker.claims.to_string();
    let sub = asker.sub();
    sess.query(
        "select set_config('role', $1, true),
                set_config('request.jwt.claims', $2, true),
                set_config('request.jwt.claim.sub', $3, true),
                set_config('request.jwt.claim.role', $4, true)",
        &[&asker.role, &claims, &sub, &asker.role],
    )
    .await?;
    Ok(())
}

/// A quoted identifier, which is what makes a table named by the
/// database safe to put in a statement.
fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn unreachable(e: Error) -> String {
    log::warn!("realtime: a visibility check could not run, {e}");
    "Realtime was unable to connect to the project database".to_string()
}

fn refused(e: Error) -> String {
    log::warn!("realtime: a visibility check would not run, {e}");
    "Realtime was unable to check what this subscriber may see".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_identifier_is_quoted_the_way_postgres_quotes_one() {
        assert_eq!(ident("todos"), "\"todos\"");
        assert_eq!(
            ident("a \"table\""),
            "\"a \"\"table\"\"\"",
            "a quote in a name is doubled, which is the only escape postgres has"
        );
    }

    #[test]
    fn the_sub_claim_is_what_auth_uid_reads() {
        let asker = Asker {
            role: "authenticated".into(),
            claims: serde_json::json!({"sub": "a-user", "role": "authenticated"}),
        };
        assert_eq!(asker.sub(), "a-user");
        let anon = Asker {
            role: "anon".into(),
            claims: serde_json::json!({}),
        };
        assert_eq!(anon.sub(), "", "a token with no subject is not a user");
    }
}
