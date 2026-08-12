//! Where the changes come from.
//!
//! [`crate::pgoutput`] turns postgres's bytes into rows and has no idea
//! where the bytes came from. This is the other half: a connection, a
//! slot, and a poll that hands those bytes over.
//!
//! The slot is temporary, which is the decision everything else here
//! follows from. A temporary logical slot lives for exactly as long as
//! the session that made it, so a server with nobody subscribed holds
//! no slot, retains no write ahead log, and leaves nothing behind when
//! it is killed. The cost is that a tap sees what happened after it
//! opened and nothing before, so a reconnecting subscriber has a gap
//! rather than a replay. That is upstream's trade too: Realtime's
//! postgres changes extension creates a temporary slot by default.
//!
//! The reading is a poll rather than a replication stream on purpose.
//! `pg_logical_slot_get_binary_changes` is an ordinary function on an
//! ordinary connection, so this needs no replication protocol, no
//! second dsn with `replication=database` on it, and no separate
//! authentication story. What it costs is a round trip per batch
//! instead of a push, which is a latency floor of however often the
//! caller asks rather than a throughput one, since a batch is as many
//! changes as postgres has.
//!
//! Two things a database has to have, and neither is this module's to
//! decide silently. `wal_level` must be `logical`, which is a
//! postmaster setting and a restart, and the publication must exist,
//! which is the bootstrap contract's. A tap says which one is missing
//! rather than reporting that there were no changes, because a change
//! feed that is quiet for a fixable reason is the worst possible
//! failure to have.

use crate::pgoutput::{Change, Decoder};
use tokio_postgres::NoTls;

/// What Supabase calls the publication a project's changed tables are
/// added to, and so what a table has to be in before anything here
/// hears about it.
///
/// This is not a convenience default that could have been anything.
/// `alter publication supabase_realtime add table todos` is what every
/// Supabase project runs, by hand or through a dashboard, and a server
/// reading a differently named publication would be a server where
/// that line does nothing.
pub const PUBLICATION: &str = "supabase_realtime";

/// Why there is no tap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Closed {
    /// The server is not running with `wal_level = logical`, so it
    /// wrote no logical decoding information to begin with and there is
    /// nothing to read. Carries what the level actually is. Fixing it
    /// is a postmaster setting and a restart, which is why this is its
    /// own answer rather than an error string: a caller that starts its
    /// own postgres can fix it, and a test can skip on it.
    NotLogical(String),
    /// The publication does not exist, so postgres would refuse the
    /// first read. A database this server bootstrapped has one.
    NoPublication(String),
    /// The connection, the slot, or the read.
    Failed(String),
}

impl std::fmt::Display for Closed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Closed::NotLogical(level) => {
                write!(f, "wal_level is {level} and logical decoding needs logical")
            }
            Closed::NoPublication(name) => write!(f, "publication {name} does not exist"),
            Closed::Failed(why) => write!(f, "{why}"),
        }
    }
}

/// A connection holding a temporary slot, and what it has decoded so
/// far.
///
/// One per tap, and the connection is not the pool's: a temporary slot
/// belongs to the session that created it and cannot be read from any
/// other, so this connection cannot be shared, returned, or reopened
/// without losing the slot with it.
pub struct Tap {
    client: tokio_postgres::Client,
    connection: tokio::task::JoinHandle<()>,
    slot: String,
    publication: String,
    decoder: Decoder,
}

impl Tap {
    /// Dial, check what the database can do, and take a slot.
    ///
    /// The slot is named for the backend holding it, which is unique
    /// while it exists and is the name that shows up in
    /// `pg_replication_slots` next to that backend's pid, so somebody
    /// looking at a database can tell which connection is reading it.
    pub async fn open(dsn: &str, publication: &str) -> Result<Tap, Closed> {
        let cfg: tokio_postgres::Config = dsn.parse().map_err(failed)?;
        let (client, connection) = cfg.connect(NoTls).await.map_err(failed)?;
        let handle = tokio::spawn(async move {
            // A dead connection shows up as the next read failing,
            // which is where it can be answered. Nothing to do here.
            let _ = connection.await;
        });
        match Tap::taking(client, publication).await {
            Ok((client, slot)) => Ok(Tap {
                client,
                connection: handle,
                slot,
                publication: publication.to_string(),
                decoder: Decoder::new(),
            }),
            Err(closed) => {
                handle.abort();
                Err(closed)
            }
        }
    }

    async fn taking(
        client: tokio_postgres::Client,
        publication: &str,
    ) -> Result<(tokio_postgres::Client, String), Closed> {
        let level: String = client
            .query_one("show wal_level", &[])
            .await
            .map_err(failed)?
            .get(0);
        if level != "logical" {
            return Err(Closed::NotLogical(level));
        }
        let published: bool = client
            .query_one(
                "select exists (select 1 from pg_publication where pubname = $1)",
                &[&publication],
            )
            .await
            .map_err(failed)?
            .get(0);
        if !published {
            return Err(Closed::NoPublication(publication.to_string()));
        }
        let pid: i32 = client
            .query_one("select pg_backend_pid()", &[])
            .await
            .map_err(failed)?
            .get(0);
        let slot = format!("zou_cdc_{pid}");
        client
            .query(
                "select pg_create_logical_replication_slot($1::name, 'pgoutput'::name, true)",
                &[&slot],
            )
            .await
            .map_err(failed)?;
        Ok((client, slot))
    }

    /// Everything postgres has written since the last time this asked,
    /// up to `most` messages. Zero is no limit.
    ///
    /// The limit is on messages rather than on changes, and a
    /// transaction's begin, its relations and its commit are messages,
    /// so a batch capped at a hundred is a hundred messages and rather
    /// fewer rows. That is postgres's own counting and the honest thing
    /// to expose: what a caller is bounding is how much it reads, not
    /// how much it hands on.
    ///
    /// A batch stops at a transaction boundary whatever the limit says,
    /// because postgres does not hand over half a transaction.
    ///
    /// Reading consumes: these changes are gone from the slot whether
    /// or not whoever asked does anything with them. That is what makes
    /// a decode failure worth stepping over rather than failing the
    /// batch on, since the alternative is throwing away the changes
    /// that did decode along with the one that did not.
    pub async fn changes(&mut self, most: i32) -> Result<Vec<Change>, String> {
        let limit = (most > 0).then_some(most);
        let rows = self
            .client
            .query(
                "select lsn::text, data from pg_logical_slot_get_binary_changes(\
                 $1::name, null, $2::int, 'proto_version', '1', 'publication_names', $3)",
                &[&self.slot, &limit, &self.publication],
            )
            .await
            .map_err(|e| e.to_string())?;
        let mut changes = Vec::new();
        for row in &rows {
            let at: &str = row.get(0);
            let bytes: &[u8] = row.get(1);
            let lsn = lsn(at)?;
            match self.decoder.message(lsn, bytes) {
                Ok(Some(change)) => changes.push(change),
                Ok(None) => {}
                // One message this could not read, out of a batch that
                // is already consumed. Saying so and carrying on loses
                // one change; stopping loses the rest of the batch as
                // well and then every batch after it.
                Err(why) => log::warn!("logical decoding at {at}: {why}"),
            }
        }
        Ok(changes)
    }

    /// The slot this holds, which is the name in `pg_replication_slots`
    /// until this is dropped.
    pub fn slot(&self) -> &str {
        &self.slot
    }
}

impl Drop for Tap {
    fn drop(&mut self) {
        // Dropping the client closes the connection, which drops the
        // temporary slot with it. The abort is so the connection's task
        // goes now rather than whenever it next notices.
        self.connection.abort();
    }
}

fn failed<E: std::fmt::Display>(e: E) -> Closed {
    Closed::Failed(e.to_string())
}

/// A log position the way postgres writes one: two hex halves with a
/// slash between them.
fn lsn(text: &str) -> Result<u64, String> {
    let (high, low) = text
        .split_once('/')
        .ok_or_else(|| format!("{text} is not a log position"))?;
    let half = |part: &str| {
        u64::from_str_radix(part, 16).map_err(|_| format!("{text} is not a log position"))
    };
    Ok(half(high)? << 32 | half(low)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_log_position_is_two_halves_of_hex() {
        assert_eq!(lsn("0/0"), Ok(0));
        assert_eq!(lsn("0/16B4E38"), Ok(0x016b_4e38));
        assert_eq!(lsn("1/0"), Ok(0x0000_0001_0000_0000));
        assert_eq!(lsn("2A/FFFFFFFF"), Ok(0x0000_002a_ffff_ffff));
    }

    #[test]
    fn a_log_position_that_is_not_one_says_so() {
        assert!(lsn("").is_err());
        assert!(lsn("16B4E38").is_err());
        assert!(lsn("0/zz").is_err());
    }

    /// The three reasons a tap does not exist read as three different
    /// sentences, because two of them are somebody's to fix.
    #[test]
    fn a_tap_that_did_not_open_says_which_of_the_three_it_was() {
        assert_eq!(
            Closed::NotLogical("replica".into()).to_string(),
            "wal_level is replica and logical decoding needs logical"
        );
        assert_eq!(
            Closed::NoPublication(PUBLICATION.into()).to_string(),
            "publication supabase_realtime does not exist"
        );
        assert_eq!(
            Closed::Failed("no route to host".into()).to_string(),
            "no route to host"
        );
    }
}
