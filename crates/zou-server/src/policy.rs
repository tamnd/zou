//! What a private channel is allowed to do, asked of the project's own
//! database.
//!
//! There is no permission model here, and that is the whole design.
//! Supabase's convention is that a channel's rules are ordinary row
//! level security policies on `realtime.messages`, written by the
//! project in sql, with the room name in `realtime.topic()` and the
//! user in `auth.uid()`. So the way to find out whether somebody may
//! read a room is to ask postgres to try, which is what this does:
//!
//!   * read: write two rows into `realtime.messages`, one per
//!     extension, become the user, and select them back. What comes
//!     back is what the select policies let through.
//!   * write: become the user and try to insert. An insert that is
//!     refused for insufficient privilege is a no, anything else is a
//!     yes.
//!
//! Both run in a transaction that is always rolled back, so nothing is
//! ever kept: the rows exist for the length of the question. That is
//! upstream's own method, down to the two extensions and the rollback,
//! because a check that worked differently would answer differently
//! for policies that are written to be read by upstream.

use serde_json::Value;
use tokio_postgres::error::SqlState;
use zou_realtime::Grant;

use crate::sql::{Error, Pool, Session};

/// Who is asking, which is the role and claims a policy sees.
pub struct Who<'a> {
    pub role: &'a str,
    pub claims: &'a Value,
}

impl Who<'_> {
    fn sub(&self) -> String {
        self.claims
            .get("sub")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    }
}

/// The two extensions a channel has policies for, in the order the
/// probes use.
const EXTENSIONS: [&str; 2] = ["broadcast", "presence"];

/// What the select policies on `realtime.messages` say about `topic`.
///
/// The rows go in before the role is set, as the connecting role,
/// which owns the table and is therefore not filtered by its policies.
/// Then the role changes and the same rows are selected back: whatever
/// survives is what this user may read.
pub async fn reads(pool: &Pool, who: &Who<'_>, topic: &str) -> Result<Grant, String> {
    let sess = pool.admin().await.map_err(unreachable)?;
    let probes = sess
        .query(
            "insert into realtime.messages (topic, extension)
             values ($1, 'broadcast'), ($1, 'presence')
             returning id::text, extension",
            &[&topic],
        )
        .await;
    let probes = match probes {
        Ok(rows) => rows,
        Err(e) => return finish(sess, Err(refused_probe(&e))).await,
    };
    let ids: Vec<String> = probes.iter().map(|row| row.get(0)).collect();
    if let Err(e) = become_user(&sess, who, topic).await {
        return finish(sess, Err(unreachable(e))).await;
    }
    let seen = sess
        .query(
            "select extension from realtime.messages where id::text = any($1)",
            &[&ids],
        )
        .await;
    let granted = match seen {
        Ok(rows) => {
            let readable =
                |extension: &str| rows.iter().any(|row| row.get::<_, String>(0) == extension);
            Ok(Grant {
                broadcast: readable("broadcast"),
                presence: readable("presence"),
            })
        }
        Err(e) => Err(refused_probe(&e)),
    };
    finish(sess, granted).await
}

/// What the insert policies say, which is asked by trying one.
///
/// A savepoint per extension, because an insert that a policy refuses
/// takes the transaction down with it and there are two questions to
/// ask on one connection.
pub async fn writes(pool: &Pool, who: &Who<'_>, topic: &str) -> Result<Grant, String> {
    let sess = pool.admin().await.map_err(unreachable)?;
    if let Err(e) = become_user(&sess, who, topic).await {
        return finish(sess, Err(unreachable(e))).await;
    }
    let mut granted = Grant::default();
    for extension in EXTENSIONS {
        if let Err(e) = sess.execute("savepoint probe", &[]).await {
            return finish(sess, Err(unreachable(e))).await;
        }
        let tried = sess
            .execute(
                "insert into realtime.messages (topic, extension) values ($1, $2)",
                &[&topic, &extension],
            )
            .await;
        let allowed = match tried {
            Ok(_) => true,
            // What a policy that says no looks like from here, and
            // also what a role with no insert grant at all looks like.
            // Both mean the same thing to the caller.
            Err(e) if e.code() == Some(&SqlState::INSUFFICIENT_PRIVILEGE) => false,
            Err(e) => return finish(sess, Err(refused_probe(&e))).await,
        };
        match extension {
            "presence" => granted.presence = allowed,
            _ => granted.broadcast = allowed,
        }
        if let Err(e) = sess.execute("rollback to savepoint probe", &[]).await {
            return finish(sess, Err(unreachable(e))).await;
        }
    }
    finish(sess, Ok(granted)).await
}

/// Everything a policy reads about who is asking.
///
/// The names are upstream's, and they have to be: a policy written for
/// Supabase reads `realtime.topic()` and `auth.uid()`, and those read
/// these settings. All of them are local to the transaction, so they
/// are gone with the rollback.
async fn become_user(sess: &Session, who: &Who<'_>, topic: &str) -> Result<(), Error> {
    let claims = who.claims.to_string();
    let sub = who.sub();
    sess.query(
        "select set_config('role', $1, true),
                set_config('realtime.topic', $2, true),
                set_config('request.jwt.claims', $3, true),
                set_config('request.jwt.claim.sub', $4, true),
                set_config('request.jwt.claim.role', $5, true)",
        &[&who.role, &topic, &claims, &sub, &who.role],
    )
    .await?;
    Ok(())
}

/// Roll the probe back, whatever it found, and hand the answer on.
///
/// The rollback is the point of the whole shape: a check that left
/// rows behind would be a check that filled a table nobody reads.
async fn finish(sess: Session, granted: Result<Grant, String>) -> Result<Grant, String> {
    if let Err(e) = sess.rollback().await {
        log::warn!("realtime: a policy check would not roll back, {e}");
    }
    granted
}

/// A database that is not there to be asked. The client is told this
/// rather than told no, because no is an answer about its policies and
/// this is not.
fn unreachable(e: Error) -> String {
    log::warn!("realtime: a policy check could not run, {e}");
    "Realtime was unable to connect to the project database".to_string()
}

/// A probe that would not run. The likeliest cause by far is somebody
/// else's realtime schema: upstream partitions `realtime.messages` by
/// day and keeps a janitor creating the partitions, and a database
/// that was Supabase's before it was this server's has the partitioned
/// table with nothing to keep it fed.
fn refused_probe(e: &Error) -> String {
    log::warn!("realtime: a policy check would not run, {e}");
    if e.code() == Some(&SqlState::CHECK_VIOLATION) {
        return "Realtime was unable to find the expected messages partition".to_string();
    }
    "Realtime was unable to check the policies on this Channel topic".to_string()
}
