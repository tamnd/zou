//! `zou check <target> [ref]`: read every page of every table back out
//! of a store, and say so.
//!
//! `zou doctor` asks whether a store can hold a database, by doing to a
//! scratch prefix the things the engine does. This asks the other
//! question, which is whether the database that is already in one still
//! reads. They are different failures: a store that never could have
//! worked, against a store that worked and has since lost something.
//!
//! The check is a restore and then a sequential scan of every ordinary
//! table in every database. That is deliberately the crudest possible
//! query, because on this engine a page comes out of a checkpoint run,
//! the wal after it, or a block object, and a scan is the one thing
//! that asks for all of them. A page whose stored bytes are damaged is
//! a page postgres refuses, and a refusal is what this is for. Index
//! only scans are turned off for the same reason, since a count
//! answered out of an index would prove nothing about the heap it was
//! counting.
//!
//! What this cannot see is a page that reads back as zeros. A block no
//! tier holds is read as zeros the way a hole in a file is, and an all
//! zero page is one postgres accepts as empty, so a relation that lost
//! a page that way scans clean and comes up short by the rows it held.
//! The per table counts printed below are therefore the part of the
//! output worth keeping and comparing between runs, rather than the ok
//! line. Issue #546 is about closing that off in the read path, where
//! it belongs.
//!
//! There is no server anywhere in it. The restore lands in a temporary
//! directory that goes away on the way out, and the SQL runs through
//! the single user backend, which reaches a first answered query in
//! about a third of the time a postmaster takes and does not have to
//! be waited for or shut down. See [`zou_pg::single`].
//!
//! Attaching to write is what any attach is, so the writer lease is
//! taken by the backend itself through the ordinary protocol, and a
//! tenant a server is serving right now refuses this with the lease
//! error rather than with a check of its own. That is on purpose: one
//! rule about who may write a tenant, in one place.

use std::path::Path;
use std::sync::Arc;

use zou_pg::single::{Rows, Session};
use zou_pg::{install, restore};
use zou_store::layout::TenantLayout;
use zou_store::{CasStore, Manifest, open_store};

pub const USAGE: &str = "usage: zou check <target> [ref] [--pg-bin <dir>]";

/// Databases that exist in every cluster and hold nothing anybody put
/// there. `template0` refuses connections at all, and `template1` is
/// the mould the others were cut from, so scanning it says nothing the
/// scan of a real database does not.
const TEMPLATES: &[&str] = &["template0", "template1"];

/// Schemas whose tables belong to postgres rather than to the store.
/// The catalog is read on the way in by every query here, so a broken
/// one fails this long before a scan of it would.
const NOT_DATA: &str = "'pg_catalog', 'information_schema', 'pg_toast'";

#[derive(Debug)]
pub struct Args {
    pub target: String,
    pub tenant: String,
    pub pg_bin: Option<String>,
}

pub fn parse(argv: &[String]) -> Result<Args, String> {
    let mut target = None;
    let mut tenant = None;
    let mut pg_bin = None;
    let mut rest = argv.iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--pg-bin" => {
                pg_bin = Some(rest.next().ok_or("--pg-bin needs a directory")?.clone());
            }
            other if other.starts_with('-') => {
                return Err(format!("unknown flag {other}\n{USAGE}"));
            }
            other if target.is_none() => target = Some(other.to_string()),
            other if tenant.is_none() => tenant = Some(other.to_string()),
            _ => return Err(USAGE.into()),
        }
    }
    Ok(Args {
        target: target.ok_or(USAGE)?,
        tenant: tenant.unwrap_or_else(|| "local".to_string()),
        pg_bin,
    })
}

/// The directory the restore lands in, removed on the way out of the
/// command whether the check passed, failed or panicked.
///
/// A restored pgdata is a copy of somebody's database, so leaving one
/// behind in the temporary directory is both a surprise and a disk
/// filling up over a week of cron runs.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new() -> Result<Self, String> {
        let dir = std::env::temp_dir().join(format!("zou-check-{}", std::process::id()));
        // A leftover from a killed run of this same pid is this run's
        // problem, since the restore would otherwise land on top of it.
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        Ok(Self(dir))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Look before restoring, so a ref that is not there is a sentence
/// rather than a restore that fails halfway with a key.
///
/// The store is opened and closed inside this function on purpose, and
/// the reason is a `.zou` target: the single file backend admits one
/// process at a time through an OS lock, and every stage after this one
/// opens the store for itself, the restore in this process and then the
/// backend in a child. A handle held across them is this command
/// refusing itself, which is the confusing half of a real limitation.
/// See #44.
fn read_the_manifest(args: &Args) -> Result<Manifest, String> {
    let store: Arc<dyn CasStore> = Arc::from(open_store(&args.target)?);
    let layout = TenantLayout::new(&args.tenant);
    let (data, _) = store
        .get(&layout.manifest())
        .map_err(|e| format!("store: {e}"))?
        .ok_or_else(|| {
            format!(
                "{} has no database at ref {}, `zou tenant {} list` shows what is there",
                args.target, args.tenant, args.target
            )
        })?;
    let manifest = Manifest::from_json(&data).map_err(|e| format!("manifest: {e}"))?;
    if manifest.checkpoints.is_empty() {
        return Err(format!(
            "{} at ref {} has no checkpoint yet, there is no database in it to check",
            args.target, args.tenant
        ));
    }
    Ok(manifest)
}

/// One table and what reading all of it did.
struct Scanned {
    database: String,
    table: String,
    rows: Option<u64>,
    refused: Option<String>,
}

pub fn run(argv: &[String]) -> Result<(), String> {
    let args = parse(argv)?;
    read_the_manifest(&args)?;

    let pg_bin = install::pg_bin(args.pg_bin.as_ref().map(Into::into));
    let work = Scratch::new()?;
    let pgdata = work.path().join("pgdata");

    say!("checking {} at ref {}", args.target, args.tenant);
    let stats = restore::restore(&args.target, &args.tenant, &pgdata)?;
    say!(
        "restored {} files and replayed {} wal records",
        stats.files,
        stats.wal_records
    );

    let scanned = scan_everything(&pg_bin, &pgdata, &args)?;
    let mut failed = 0;
    let mut rows = 0u64;
    for one in &scanned {
        match (&one.refused, one.rows) {
            (Some(said), _) => {
                failed += 1;
                say!("  {}.{} refused: {said}", one.database, one.table);
            }
            (None, Some(n)) => {
                rows += n;
                say!("  {}.{} {n} rows", one.database, one.table);
            }
            (None, None) => {
                failed += 1;
                say!("  {}.{} answered nothing", one.database, one.table);
            }
        }
    }
    if failed > 0 {
        return Err(format!(
            "{failed} of {} tables could not be read out of {}",
            scanned.len(),
            args.target
        ));
    }
    say!(
        "ok: {} tables read, {rows} rows, nothing refused",
        scanned.len()
    );
    Ok(())
}

/// Scan every table of every database that allows connections.
///
/// One session per database, because the backend opens exactly one and
/// there is no such thing as changing to another inside it.
fn scan_everything(pg_bin: &Path, pgdata: &Path, args: &Args) -> Result<Vec<Scanned>, String> {
    let session = |database: &str| {
        Session::new(pg_bin, pgdata)
            .database(database)
            .env("ZOU_TARGET", &args.target)
            .env("ZOU_TENANT", &args.tenant)
            // Nothing is serving this tenant, so there is no page
            // service to read through and the objects are the path
            // under test, which is the path being checked.
            .env("ZOU_PAGESERVE", "0")
    };

    let databases = one_column(
        &session("postgres").run(&format!(
            "select datname from pg_database where datallowconn and datname not in ({}) order by 1;",
            TEMPLATES
                .iter()
                .map(|t| format!("'{t}'"))
                .collect::<Vec<_>>()
                .join(", ")
        ))?,
    );
    let mut out = Vec::new();
    for database in databases {
        let tables = one_column(&session(&database).run(&format!(
            "select quote_ident(n.nspname) || '.' || quote_ident(c.relname) \
             from pg_class c join pg_namespace n on n.oid = c.relnamespace \
             where c.relkind in ('r', 'm') and n.nspname not in ({NOT_DATA}) order by 1;"
        ))?);
        for table in tables {
            out.push(scan_one(&session(&database), &database, &table));
        }
    }
    Ok(out)
}

/// Count every row of one table, in its own session.
///
/// Its own session because a refusal has to be attributable: the
/// backend runs the statement after a failed one and exits zero either
/// way, so a batch of counts that came back one short would not say
/// which table was missing.
fn scan_one(session: &Session, database: &str, table: &str) -> Scanned {
    // Off so the count reads the heap. An index only scan would answer
    // out of the visibility map and the index, and this is a question
    // about the pages of the table.
    let sql = format!(
        "set enable_indexonlyscan = off; \
         set enable_indexscan = off; \
         set enable_bitmapscan = off; \
         select count(*) as n from {table};"
    );
    match session.run(&sql) {
        Err(said) => Scanned {
            database: database.to_string(),
            table: table.to_string(),
            rows: None,
            refused: Some(said.replace('\n', "; ")),
        },
        Ok(sets) => Scanned {
            database: database.to_string(),
            table: table.to_string(),
            rows: sets
                .last()
                .and_then(Rows::scalar)
                .and_then(|n| n.parse().ok()),
            refused: None,
        },
    }
}

/// The first column of the last answer, which is the shape every query
/// above asks in.
fn one_column(sets: &[Rows]) -> Vec<String> {
    sets.last().map_or_else(Vec::new, |rows| {
        rows.rows
            .iter()
            .filter_map(|row| row.first().cloned().flatten())
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn the_ref_is_optional_and_local_by_default() {
        let args = parse(&argv(&["/tmp/store"])).expect("parse");
        assert_eq!(args.target, "/tmp/store");
        assert_eq!(args.tenant, "local");
        assert!(args.pg_bin.is_none());

        let args =
            parse(&argv(&["/tmp/store", "yesterday", "--pg-bin", "/pg/bin"])).expect("parse");
        assert_eq!(args.tenant, "yesterday");
        assert_eq!(args.pg_bin.as_deref(), Some("/pg/bin"));
    }

    #[test]
    fn a_typo_is_refused_rather_than_read_as_a_ref() {
        assert!(parse(&argv(&[])).is_err());
        assert!(parse(&argv(&["/tmp/store", "--tenat", "x"])).is_err());
        assert!(parse(&argv(&["/tmp/store", "a", "b"])).is_err());
    }

    #[test]
    fn the_first_column_of_the_last_answer_is_what_a_list_query_asks_for() {
        let sets = vec![
            Rows {
                columns: vec!["ignored".into()],
                rows: vec![vec![Some("set".into())]],
            },
            Rows {
                columns: vec!["datname".into()],
                rows: vec![
                    vec![Some("postgres".into())],
                    // A null here would be a column that is not there
                    // rather than a name, and a name it is not.
                    vec![None],
                    vec![Some("app".into())],
                ],
            },
        ];
        assert_eq!(one_column(&sets), ["postgres", "app"]);
        assert!(one_column(&[]).is_empty());
    }
}
