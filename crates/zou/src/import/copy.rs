//! The copy half of `zou import supabase`: move the project here.
//!
//! The survey next door decides what is possible. This decides nothing
//! and moves what the survey found, in an order chosen so that a run
//! killed anywhere can be started again with the same command.
//!
//! Resume is a ledger in the target rather than a guess. Every step is
//! one transaction with its own ledger row written inside it, so a step
//! either happened and is written down or did not happen and left
//! nothing behind. There is no half copied table to detect and no
//! partial state to clean up, which is why the second run of a killed
//! import is the same command as the first.
//!
//! What it will not do is merge into a database that already has the
//! project's tables in it. A schema that is already there is either
//! this import resuming, which the ledger says, or somebody else's
//! data, and guessing wrong overwrites the second one. So the first
//! run wants the target's own schemas empty and says so plainly when
//! they are not.

use std::collections::{BTreeMap, BTreeSet};

use futures_util::{SinkExt, StreamExt, pin_mut};
use tokio_postgres::Client;

use crate::schema::{self, Catalog, ident};

use super::{EMULATED, NATIVE, PLATFORM_ROLES, SYSTEM_SCHEMAS, Survey};

/// The two schemas this server owns rather than the project. Their
/// tables are here already and the rows go into them, which is a
/// different job from the project's own schemas and is done separately
/// below.
const PLATFORM: &[&str] = &["auth", "storage"];

/// Schemas that belong to the server or to postgres, so neither their
/// definitions nor their rows come over. `auth` and `storage` are not
/// in it, because their rows do, one table at a time and only into a
/// table that is already here.
const NOT_THE_PROJECTS: &[&str] = &[
    "cron",
    "extensions",
    "graphql",
    "graphql_public",
    "net",
    "pgbouncer",
    "pgsodium",
    "realtime",
    "supabase_functions",
    "supabase_migrations",
    "vault",
    "zou",
];

/// The three roles the api answers as, which is who the grants at the
/// end are for.
const API_ROLES: &[&str] = &["anon", "authenticated", "service_role"];

/// Tables in the platform's two schemas that are deliberately left
/// where they are, and why.
///
/// Three kinds. A session, which is not carried because no token the
/// old project minted is honoured by this one, so everybody signs in
/// again once and that is the whole of the cutover for a signed in
/// user. Something in flight, a half finished sign in or a challenge
/// or an upload, which was going to expire in minutes anyway and whose
/// other half is on a server nobody is going to talk to again. And the
/// platform's own bookkeeping about the project, which is not the
/// project's data and which this server keeps its own version of.
///
/// Every one of them gets a line in the run's output, because a table
/// the survey counted and the copy skipped is exactly the kind of thing
/// somebody finds out about later.
const NOT_CARRIED: &[(&str, &str)] = &[
    (
        "auth.refresh_tokens",
        "a session on the old project, and none of its tokens are honoured here, so everybody signs in again once",
    ),
    ("auth.sessions", "the other half of the refresh tokens"),
    (
        "auth.mfa_amr_claims",
        "how a session was proved, and the sessions do not come",
    ),
    (
        "auth.mfa_challenges",
        "a factor being proved right now, which expires in minutes",
    ),
    (
        "auth.flow_state",
        "a sign in part way through the old project's redirect",
    ),
    (
        "auth.one_time_tokens",
        "confirmation and recovery links already sent, which point at the old project",
    ),
    ("auth.saml_relay_states", "a saml sign in part way through"),
    (
        "auth.webauthn_challenges",
        "a passkey being proved right now",
    ),
    (
        "auth.oauth_client_states",
        "an oauth sign in part way through",
    ),
    (
        "auth.schema_migrations",
        "GoTrue's own migration history, which is not this server's",
    ),
    (
        "auth.instances",
        "the platform's row about the project rather than anything in it",
    ),
    (
        "storage.migrations",
        "the storage service's own migration history, which is not this server's",
    ),
    (
        "storage.s3_multipart_uploads",
        "an upload part way through, whose parts are on the old project",
    ),
    (
        "storage.s3_multipart_uploads_parts",
        "the parts of those uploads",
    ),
];

/// The ledger, in the schema this server already owns so that a
/// `zou db diff` does not read it as something the project wrote and
/// try to write a migration for it.
const LEDGER: &str = "
set client_min_messages = warning;
create schema if not exists zou;
create table if not exists zou.import_progress (
    step text primary key,
    rows bigint not null default 0,
    at timestamptz not null default now()
);
reset client_min_messages;
";

/// What one run did, which is what gets printed and what the report
/// gets a section of.
#[derive(Debug, Default)]
pub struct Done {
    /// Step name to rows, for the steps this run did.
    pub steps: Vec<(String, u64)>,
    /// Steps the ledger already had, so a resume can say what it
    /// skipped rather than looking like it did nothing.
    pub resumed: Vec<String>,
    /// What did not come over, and why. Never empty by accident: what
    /// no schema read here looks at is in it every run, and so are the
    /// object bytes unless somebody asked for them.
    pub left: Vec<String>,
}

impl Done {
    fn rows(&self) -> u64 {
        self.steps.iter().map(|(_, n)| n).sum()
    }

    /// The summary a person reads at the end of a run.
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "copied {} rows in {}",
            self.rows(),
            super::plural(self.steps.len() as i64, "step", "steps")
        ));
        if !self.resumed.is_empty() {
            out.push_str(&format!(
                ", {} already done",
                super::plural(self.resumed.len() as i64, "step was", "steps were")
            ));
        }
        out.push('\n');
        for line in &self.left {
            out.push_str(&format!("not copied: {line}\n"));
        }
        out
    }
}

/// A table to copy and the columns to copy of it, which is not every
/// column: a generated one is computed here and postgres refuses to be
/// given it.
#[derive(Debug, PartialEq)]
struct Copyable {
    id: String,
    columns: Vec<String>,
}

impl Copyable {
    fn list(&self) -> String {
        self.columns
            .iter()
            .map(|c| ident(c))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// The schemas the project owns, which is everything that is neither
/// postgres's nor this server's.
fn project_schemas(survey: &Survey) -> Vec<String> {
    survey
        .schemas
        .iter()
        .map(|s| s.name.clone())
        .filter(|n| !SYSTEM_SCHEMAS.contains(&n.as_str()))
        .filter(|n| !NOT_THE_PROJECTS.contains(&n.as_str()))
        .filter(|n| !PLATFORM.contains(&n.as_str()))
        .collect()
}

/// The whole copy, in the order that makes each step safe to repeat.
///
/// `bytes` says whether the caller is going on to fetch the storage
/// objects themselves, which is a step of its own over http rather
/// than anything this can do down a postgres connection. It changes
/// nothing here except the sentence about them at the end, and that
/// sentence is worth being right about.
pub async fn run(
    source: &Client,
    target: &mut Client,
    survey: &Survey,
    bytes: bool,
) -> Result<Done, String> {
    let mut done = Done::default();
    // No search path on either side, so every name postgres hands back
    // is written in full and every name sent is read the same way here
    // as it was there.
    for side in [source, &*target] {
        side.batch_execute("set search_path = ''")
            .await
            .map_err(|e| format!("cannot clear the search path: {}", why(&e)))?;
    }
    // "already exists, skipping" from a `create extension if not
    // exists` is this command's own bookkeeping talking rather than
    // anything the project did.
    target
        .batch_execute("set client_min_messages = warning")
        .await
        .map_err(|e| format!("cannot quiet the target: {}", why(&e)))?;
    target
        .batch_execute(LEDGER)
        .await
        .map_err(|e| format!("cannot make the import ledger: {}", why(&e)))?;
    let already = ledger(target).await?;

    extensions(target, survey, &mut done, &already).await?;
    let schemas = project_schemas(survey);
    schemas_first(source, target, &schemas, &mut done, &already).await?;
    sequence_definitions(source, target, &schemas, &mut done, &already).await?;
    definitions(source, target, &schemas, &mut done, &already).await?;
    data(source, target, &schemas, &mut done, &already).await?;
    sequences(source, target, &schemas, &mut done, &already).await?;
    platform(source, target, &mut done, &already).await?;
    grants(target, &schemas, &mut done, &already).await?;

    // Said unless the caller is about to go and get them, because the
    // survey counted these and somebody who read that count will look
    // for them here.
    if !bytes {
        done.left.push(
            "the bytes of the storage objects, the rows that name them are here \
             and the bytes need --store and --service-key"
                .into(),
        );
    }
    // What the schema read does not look at, minus extensions, which
    // it does not and this does.
    for what in schema::UNREAD.iter().filter(|w| **w != "extensions") {
        done.left
            .push(format!("{what}, which no schema read here does"));
    }
    Ok(done)
}

/// A postgres error prints as "db error" and keeps the sentence that
/// says what went wrong in its source, which is no use to somebody
/// reading a failed import.
pub(super) fn why(e: &tokio_postgres::Error) -> String {
    let mut out = e.to_string();
    let mut source = std::error::Error::source(e);
    while let Some(e) = source {
        out.push_str(": ");
        out.push_str(&e.to_string());
        source = e.source();
    }
    out
}

async fn ledger(target: &Client) -> Result<BTreeSet<String>, String> {
    let rows = target
        .query("select step from zou.import_progress", &[])
        .await
        .map_err(|e| format!("cannot read the import ledger: {}", why(&e)))?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// One step: the statements and the ledger row in one transaction, so
/// the step is either written down or was never taken.
async fn step(
    target: &mut Client,
    name: &str,
    sql: &str,
    done: &mut Done,
    already: &BTreeSet<String>,
) -> Result<(), String> {
    if already.contains(name) {
        done.resumed.push(name.to_string());
        return Ok(());
    }
    if sql.trim().is_empty() {
        return Ok(());
    }
    let tx = target
        .transaction()
        .await
        .map_err(|e| format!("{name}: cannot begin: {}", why(&e)))?;
    tx.batch_execute(sql)
        .await
        .map_err(|e| format!("{name}: {}", why(&e)))?;
    tx.execute(
        "insert into zou.import_progress (step) values ($1)",
        &[&name],
    )
    .await
    .map_err(|e| format!("{name}: cannot write the ledger: {}", why(&e)))?;
    tx.commit()
        .await
        .map_err(|e| format!("{name}: cannot commit: {}", why(&e)))?;
    done.steps.push((name.to_string(), 0));
    Ok(())
}

/// `create extension` for the ones built here, and a sentence for the
/// ones that are not. The version is not asked for: what matters is
/// that the functions the project's code calls resolve, and pinning a
/// version that this build does not carry fails for no gain.
async fn extensions(
    target: &mut Client,
    survey: &Survey,
    done: &mut Done,
    already: &BTreeSet<String>,
) -> Result<(), String> {
    let mut sql = String::new();
    for e in &survey.extensions {
        if e.name == "plpgsql" {
            continue;
        }
        if NATIVE.contains(&e.name.as_str()) {
            sql.push_str(&format!(
                "create extension if not exists {} with schema {} cascade;\n",
                literal_name(&e.name),
                ident(&e.schema)
            ));
            continue;
        }
        if let Some((_, how)) = EMULATED.iter().find(|(n, _)| *n == e.name) {
            done.left
                .push(format!("{}, {how}, so it is not installed", e.name));
            continue;
        }
        done.left
            .push(format!("{}, which has no answer here", e.name));
    }
    step(target, "extensions", &sql, done, already).await
}

/// An extension name is an identifier to `create extension`, and the
/// ones with a dash in them have to be quoted.
fn literal_name(name: &str) -> String {
    ident(name)
}

/// The project's own schemas, made here the way they are there.
///
/// The statements come from the same differ `zou db diff` uses, run
/// against a catalog holding only the schemas that already exist, so
/// what comes out is creates and nothing else. Anything else coming out
/// of it would mean the target had objects this is about to change, and
/// that is refused above rather than executed.
async fn definitions(
    source: &Client,
    target: &mut Client,
    schemas: &[String],
    done: &mut Done,
    already: &BTreeSet<String>,
) -> Result<(), String> {
    if already.contains("definitions") {
        done.resumed.push("definitions".into());
        return Ok(());
    }
    let theirs = Catalog::read(source, schemas).await?;
    let ours = Catalog::read(&*target, schemas).await?;
    if !ours.tables.is_empty() {
        let mut names: Vec<&str> = ours.tables.keys().map(String::as_str).collect();
        names.sort();
        return Err(format!(
            "the target already has {}, and an import will not write over a database somebody else is using. \
             Point it at an empty one, or if this is the same import being run again, it resumes from its own ledger and this database has none",
            names.join(", ")
        ));
    }
    // The schemas that are already here, so the diff does not try to
    // make `public` a second time, and so it does not read a schema
    // that is here and not there as something to drop.
    let from = Catalog {
        schemas: ours
            .schemas
            .intersection(&theirs.schemas)
            .cloned()
            .collect(),
        ..Catalog::default()
    };
    let mut theirs = theirs;
    // A hosted project's grants name the roles its platform processes
    // run as. Those roles are not here, so a grant to one of them fails
    // and takes the whole step with it.
    theirs.forget_grants_to(PLATFORM_ROLES);
    let statements = schema::diff(&from, &theirs);
    if let Some(bad) = statements
        .iter()
        .find(|s| s.trim_start().starts_with("drop "))
    {
        return Err(format!(
            "the copy would have run {bad:?}, which an import must never do, so nothing was run"
        ));
    }
    let sql = statements.join("\n");
    step(target, "definitions", &sql, done, already).await
}

/// Every table's rows, one table one transaction.
///
/// Triggers are off for the duration, which is what a restore does and
/// what makes the table order not matter: a foreign key pointing at a
/// table that has not been copied yet would otherwise decide the order,
/// and a cycle between two of them would leave no order at all. The
/// project's own triggers stay off too, because they already ran on the
/// source when these rows were written there.
async fn data(
    source: &Client,
    target: &mut Client,
    schemas: &[String],
    done: &mut Done,
    already: &BTreeSet<String>,
) -> Result<(), String> {
    let tables = copyable(source, schemas).await?;
    if tables.is_empty() {
        return Ok(());
    }
    let quiet = target
        .batch_execute("set session_replication_role = replica")
        .await
        .is_ok();
    if !quiet {
        done.left.push(
            "the foreign keys were checked as the rows landed, this role cannot turn triggers off, \
             so a table whose rows point at a table copied later can fail"
                .into(),
        );
    }
    for table in &tables {
        table_rows(source, target, table, table, done, already).await?;
    }
    if quiet {
        target
            .batch_execute("set session_replication_role = origin")
            .await
            .map_err(|e| format!("cannot turn the triggers back on: {}", why(&e)))?;
    }
    Ok(())
}

/// One table's rows, streamed from the one side into the other without
/// being held anywhere in between.
async fn table_rows(
    source: &Client,
    target: &mut Client,
    from: &Copyable,
    into: &Copyable,
    done: &mut Done,
    already: &BTreeSet<String>,
) -> Result<(), String> {
    let name = format!("data:{}", into.id);
    if already.contains(&name) {
        done.resumed.push(name);
        return Ok(());
    }
    let out = format!("copy (select {} from {}) to stdout", from.list(), from.id);
    let into_sql = format!("copy {} ({}) from stdin", into.id, into.list());
    let tx = target
        .transaction()
        .await
        .map_err(|e| format!("{name}: cannot begin: {}", why(&e)))?;
    let sink = tx
        .copy_in(&into_sql)
        .await
        .map_err(|e| format!("{name}: cannot write: {}", why(&e)))?;
    let stream = source
        .copy_out(&out)
        .await
        .map_err(|e| format!("{name}: cannot read: {}", why(&e)))?;
    pin_mut!(sink);
    pin_mut!(stream);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| format!("{name}: reading: {}", why(&e)))?;
        sink.send(chunk)
            .await
            .map_err(|e| format!("{name}: writing: {}", why(&e)))?;
    }
    let rows = sink
        .finish()
        .await
        .map_err(|e| format!("{name}: finishing: {}", why(&e)))?;
    tx.execute(
        "insert into zou.import_progress (step, rows) values ($1, $2)",
        &[&name, &(rows as i64)],
    )
    .await
    .map_err(|e| format!("{name}: cannot write the ledger: {}", why(&e)))?;
    tx.commit()
        .await
        .map_err(|e| format!("{name}: cannot commit: {}", why(&e)))?;
    done.steps.push((name, rows));
    Ok(())
}

const COPYABLE_SQL: &str = "\
select n.nspname as schema, c.relname as name, a.attname as column, a.attnum
from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
where n.nspname = any($1)
  and c.relkind in ('r', 'p')
  and c.relispartition = false
  and a.attgenerated = ''
order by n.nspname, c.relname, a.attnum";

/// Every ordinary table in those schemas and the columns of it that can
/// be given values. Partitions are left out because their rows arrive
/// through the table they belong to and copying both would double them.
async fn copyable(client: &Client, schemas: &[String]) -> Result<Vec<Copyable>, String> {
    let rows = client
        .query(COPYABLE_SQL, &[&schemas])
        .await
        .map_err(|e| format!("cannot list the tables: {}", why(&e)))?;
    let mut by_table: Vec<Copyable> = Vec::new();
    for row in rows {
        let id = format!(
            "{}.{}",
            ident(&row.get::<_, String>("schema")),
            ident(&row.get::<_, String>("name"))
        );
        let column: String = row.get("column");
        match by_table.last_mut() {
            Some(last) if last.id == id => last.columns.push(column),
            _ => by_table.push(Copyable {
                id,
                columns: vec![column],
            }),
        }
    }
    Ok(by_table
        .into_iter()
        .filter(|t| !t.columns.is_empty())
        .collect())
}

/// The schemas, before anything that goes in one.
///
/// `public` is here already on any database, and a project that made
/// schemas of its own has them made here in the plainest way there is.
/// Their owner is whoever is running the import rather than whoever
/// owned them on the platform, which is the ownership fixup: the roles
/// a hosted project's objects belong to do not exist here.
async fn schemas_first(
    source: &Client,
    target: &mut Client,
    schemas: &[String],
    done: &mut Done,
    already: &BTreeSet<String>,
) -> Result<(), String> {
    let _ = source;
    let here: BTreeSet<String> = target
        .query(
            "select nspname from pg_namespace where nspname = any($1)",
            &[&schemas],
        )
        .await
        .map_err(|e| format!("cannot read the schema list: {}", why(&e)))?
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    let mut sql = String::new();
    for name in schemas {
        if !here.contains(name) {
            sql.push_str(&format!("create schema {};\n", ident(name)));
        }
    }
    step(target, "schemas", &sql, done, already).await
}

/// The sequences to make, which is every one the project has except
/// the ones an identity column owns. Those are made by the column,
/// with the name postgres picks, and making one first would take the
/// name the column is about to want.
const SEQUENCE_DDL_SQL: &str = "\
select
  s.schemaname,
  s.sequencename,
  s.data_type::text as kind,
  s.start_value,
  s.min_value,
  s.max_value,
  s.increment_by,
  s.cycle,
  s.cache_size
from pg_sequences s
  join pg_class c on c.relname = s.sequencename
  join pg_namespace n on n.oid = c.relnamespace and n.nspname = s.schemaname
where s.schemaname = any($1)
  and c.relkind = 'S'
  and not exists (
    select 1 from pg_depend d
    where d.classid = 'pg_class'::regclass and d.objid = c.oid and d.deptype = 'i'
  )";

/// Which column owns which sequence, so that dropping the table takes
/// the sequence with it here the same way it does there. Identity
/// sequences are left out for the same reason as above: the column
/// already owns the one it made.
const SEQUENCE_OWNER_SQL: &str = "\
select
  n.nspname as schema,
  c.relname as sequence,
  tn.nspname as owner_schema,
  t.relname as owner_table,
  a.attname as owner_column
from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  join pg_depend d on d.classid = 'pg_class'::regclass and d.objid = c.oid and d.deptype = 'a'
  join pg_class t on t.oid = d.refobjid
  join pg_namespace tn on tn.oid = t.relnamespace
  join pg_attribute a on a.attrelid = t.oid and a.attnum = d.refobjsubid
where c.relkind = 'S' and n.nspname = any($1)";

const SEQUENCES_SQL: &str = "\
select schemaname, sequencename, last_value
from pg_sequences
where schemaname = any($1) and last_value is not null";

/// The sequences themselves, before the tables, because a serial
/// column's default calls `nextval` on one by name and a table whose
/// default names a sequence that is not there does not get created.
///
/// The differ next door never has to do this: both sides of a
/// `zou db diff` were built from the same migrations, so a sequence
/// exists on both. An import starts from an empty database, where it
/// exists on neither.
async fn sequence_definitions(
    source: &Client,
    target: &mut Client,
    schemas: &[String],
    done: &mut Done,
    already: &BTreeSet<String>,
) -> Result<(), String> {
    let rows = source
        .query(SEQUENCE_DDL_SQL, &[&schemas])
        .await
        .map_err(|e| format!("cannot read the sequences: {}", why(&e)))?;
    let mut sql = String::new();
    for row in rows {
        let id = format!(
            "{}.{}",
            ident(&row.get::<_, String>("schemaname")),
            ident(&row.get::<_, String>("sequencename"))
        );
        let kind: String = row.get("kind");
        let start: i64 = row.get("start_value");
        let min: i64 = row.get("min_value");
        let max: i64 = row.get("max_value");
        let by: i64 = row.get("increment_by");
        let cache: i64 = row.get("cache_size");
        let cycle = if row.get::<_, bool>("cycle") {
            " cycle"
        } else {
            ""
        };
        sql.push_str(&format!(
            "create sequence {id} as {kind} increment by {by} minvalue {min} \
             maxvalue {max} start with {start} cache {cache}{cycle};\n"
        ));
    }
    step(target, "sequence definitions", &sql, done, already).await
}

/// Where each sequence had got to. Without this the tables have their
/// rows and the next insert into one of them collides with the first
/// row that was copied, which is a failure that reads like a bug in the
/// application rather than a step of the import that never ran.
async fn sequences(
    source: &Client,
    target: &mut Client,
    schemas: &[String],
    done: &mut Done,
    already: &BTreeSet<String>,
) -> Result<(), String> {
    let rows = source
        .query(SEQUENCES_SQL, &[&schemas])
        .await
        .map_err(|e| format!("cannot read the sequences: {}", why(&e)))?;
    let mut sql = String::new();
    for row in rows {
        let id = format!(
            "{}.{}",
            ident(&row.get::<_, String>("schemaname")),
            ident(&row.get::<_, String>("sequencename"))
        );
        let last: i64 = row.get("last_value");
        sql.push_str(&format!("select setval('{id}', {last}, true);\n"));
    }
    // The ownership goes on here rather than with the create above,
    // because a sequence cannot be owned by a column of a table that
    // does not exist yet.
    let rows = source
        .query(SEQUENCE_OWNER_SQL, &[&schemas])
        .await
        .map_err(|e| format!("cannot read what owns the sequences: {}", why(&e)))?;
    for row in rows {
        sql.push_str(&format!(
            "alter sequence {}.{} owned by {}.{}.{};\n",
            ident(&row.get::<_, String>("schema")),
            ident(&row.get::<_, String>("sequence")),
            ident(&row.get::<_, String>("owner_schema")),
            ident(&row.get::<_, String>("owner_table")),
            ident(&row.get::<_, String>("owner_column"))
        ));
    }
    step(target, "sequences", &sql, done, already).await
}

/// The rows of the two schemas this server owns.
///
/// Nothing is created here. The tables are already here and their
/// shape is this server's rather than the hosted platform's, so the
/// columns copied are the ones both sides have, and a table on one side
/// and not the other is named in what was left rather than being
/// quietly skipped. A table here that already has rows in it is left
/// alone for the same reason the project's schemas have to be empty.
async fn platform(
    source: &Client,
    target: &mut Client,
    done: &mut Done,
    already: &BTreeSet<String>,
) -> Result<(), String> {
    let here: BTreeMap<String, Copyable> = copyable(&*target, &owned())
        .await?
        .into_iter()
        .map(|t| (t.id.clone(), t))
        .collect();
    let theirs = copyable(source, &owned()).await?;
    if theirs.is_empty() {
        return Ok(());
    }
    let quiet = target
        .batch_execute("set session_replication_role = replica")
        .await
        .is_ok();
    for table in &theirs {
        if let Some((_, why)) = NOT_CARRIED.iter().find(|(id, _)| *id == table.id) {
            done.left.push(format!("{}, which is {why}", table.id));
            continue;
        }
        let Some(ours) = here.get(&table.id) else {
            done.left.push(format!(
                "{}, which the platform has and this server does not",
                table.id
            ));
            continue;
        };
        let shared: Vec<String> = table
            .columns
            .iter()
            .filter(|c| ours.columns.contains(c))
            .cloned()
            .collect();
        if shared.is_empty() {
            done.left.push(format!(
                "{}, whose columns here and there have no name in common",
                table.id
            ));
            continue;
        }
        for column in &table.columns {
            if !shared.contains(column) {
                done.left
                    .push(format!("{}.{column}, which is not a column here", table.id));
            }
        }
        let name = format!("data:{}", table.id);
        if !already.contains(&name) && !empty(&*target, &table.id).await? {
            done.left.push(format!(
                "{}, which already has rows here and is not written over",
                table.id
            ));
            continue;
        }
        let both = Copyable {
            id: table.id.clone(),
            columns: shared,
        };
        table_rows(source, target, &both, &both, done, already).await?;
    }
    if quiet {
        target
            .batch_execute("set session_replication_role = origin")
            .await
            .map_err(|e| format!("cannot turn the triggers back on: {}", why(&e)))?;
    }
    Ok(())
}

fn owned() -> Vec<String> {
    PLATFORM.iter().map(|s| s.to_string()).collect()
}

async fn empty(client: &Client, id: &str) -> Result<bool, String> {
    let row = client
        .query_one(&format!("select exists (select 1 from {id} limit 1)"), &[])
        .await
        .map_err(|e| format!("cannot look at {id}: {}", why(&e)))?;
    Ok(!row.get::<_, bool>(0))
}

/// The grants the api roles need on what just arrived.
///
/// The project's own grants came over with the definitions, minus the
/// ones naming roles that do not exist here. These are the ones a table
/// made on this server would have got from the bootstrap, so a table
/// that arrived by import is reachable through the api the same way a
/// table made here is.
async fn grants(
    target: &mut Client,
    schemas: &[String],
    done: &mut Done,
    already: &BTreeSet<String>,
) -> Result<(), String> {
    let missing = target
        .query(
            "select 1 from pg_roles where rolname = any($1)",
            &[&API_ROLES],
        )
        .await
        .map_err(|e| format!("cannot look for the api roles: {}", why(&e)))?
        .is_empty();
    if missing {
        done.left.push(
            "the grants to anon, authenticated and service_role, because this database does not \
             have those roles yet, which means the server has never served from it"
                .into(),
        );
        return Ok(());
    }
    let roles = API_ROLES.join(", ");
    let mut sql = String::new();
    for schema in schemas {
        let s = ident(schema);
        sql.push_str(&format!("grant usage on schema {s} to {roles};\n"));
        sql.push_str(&format!(
            "grant all on all tables in schema {s} to {roles};\n"
        ));
        sql.push_str(&format!(
            "grant all on all sequences in schema {s} to {roles};\n"
        ));
        sql.push_str(&format!(
            "grant all on all functions in schema {s} to {roles};\n"
        ));
        sql.push_str(&format!(
            "alter default privileges in schema {s} grant all on tables to {roles};\n"
        ));
        sql.push_str(&format!(
            "alter default privileges in schema {s} grant all on sequences to {roles};\n"
        ));
        sql.push_str(&format!(
            "alter default privileges in schema {s} grant all on functions to {roles};\n"
        ));
    }
    step(target, "grants", &sql, done, already).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::import::Schema;

    fn survey_with(schemas: &[&str]) -> Survey {
        Survey {
            schemas: schemas
                .iter()
                .map(|name| Schema {
                    name: name.to_string(),
                    ..Schema::default()
                })
                .collect(),
            ..Survey::default()
        }
    }

    /// The project's schemas are the ones nobody else owns. `auth` and
    /// `storage` are not the project's even though its rows are in
    /// them, because the tables here are this server's.
    #[test]
    fn the_project_owns_what_is_left_over() {
        let s = survey_with(&[
            "public",
            "app",
            "auth",
            "storage",
            "extensions",
            "pg_catalog",
            "information_schema",
            "zou",
            "supabase_migrations",
        ]);
        assert_eq!(project_schemas(&s), vec!["public", "app"]);
    }

    /// A copy of nothing still says what it did not copy, and the
    /// bytes of the objects are in that list every time until they are
    /// implemented.
    #[test]
    fn a_run_that_copied_nothing_still_says_what_it_left() {
        let mut done = Done::default();
        done.left.push("the bytes of the storage objects".into());
        let out = done.render();
        assert!(out.starts_with("copied 0 rows in 0 steps\n"), "{out}");
        assert!(
            out.contains("not copied: the bytes of the storage objects"),
            "{out}"
        );
    }

    #[test]
    fn a_resume_says_how_much_was_already_there() {
        let done = Done {
            steps: vec![("data:public.notes".into(), 5000)],
            resumed: vec!["definitions".into(), "extensions".into()],
            left: Vec::new(),
        };
        let out = done.render();
        assert!(out.contains("copied 5000 rows in 1 step,"), "{out}");
        assert!(out.contains("2 steps were already done"), "{out}");
    }

    /// A column list is written the way postgres would write it, so a
    /// column called `order` is still a column.
    #[test]
    fn the_column_list_is_quoted_where_it_has_to_be() {
        let t = Copyable {
            id: "public.notes".into(),
            columns: vec!["id".into(), "order".into(), "Body".into()],
        };
        assert_eq!(t.list(), "id, \"order\", \"Body\"");
    }

    /// A project with the shapes that break a copy in it: a serial
    /// column whose default names a sequence, an identity column that
    /// makes its own, a generated column postgres refuses to be given,
    /// a foreign key pointing at a table that sorts after it, row level
    /// security and a policy, and the two platform schemas.
    const SOURCE: &str = "
create schema app;
create table app.authors (id bigserial primary key, name text not null);
create table app.books (
    id bigint generated always as identity primary key,
    author bigint not null references app.authors(id),
    title text not null,
    upper_title text generated always as (upper(title)) stored
);
insert into app.authors (name) select 'author ' || g from generate_series(1, 10) g;
insert into app.books (author, title)
select 1 + (g % 10), 'book ' || g from generate_series(1, 100) g;
alter table app.books enable row level security;
create policy readable on app.books for select using (true);
create schema auth;
create table auth.users (id uuid primary key, email text, phone text);
insert into auth.users select gen_random_uuid(), 'u' || g || '@example.com', null
from generate_series(1, 7) g;
create table auth.refresh_tokens (id bigserial primary key, token text);
insert into auth.refresh_tokens (token) select 'token ' || g from generate_series(1, 12) g;
create schema storage;
create table storage.buckets (id text primary key, public boolean default false);
insert into storage.buckets values ('avatars', true);
create table storage.objects (id uuid primary key, bucket_id text, name text);
insert into storage.objects select gen_random_uuid(), 'avatars', 'file' || g
from generate_series(1, 5) g;
";

    /// What this server has of the two platform schemas, which is not
    /// what the platform has: no `phone`, and no `storage.objects` at
    /// all, so the copy has both a column and a table to leave behind.
    /// `auth.refresh_tokens` is here, which is the point of it: the
    /// table exists on both sides and the rows still do not come.
    const TARGET: &str = "
create extension if not exists pgcrypto;
create schema auth;
create table auth.users (id uuid primary key, email text);
create table auth.refresh_tokens (id bigserial primary key, token text);
create schema storage;
create table storage.buckets (id text primary key, public boolean default false);
";

    async fn number(client: &Client, sql: &str) -> i64 {
        client.query_one(sql, &[]).await.expect(sql).get(0)
    }

    /// The whole copy against two real databases, because every failure
    /// this file is about is a failure postgres reports and nothing
    /// else can.
    #[test]
    fn a_project_moves_and_moving_it_twice_changes_nothing() {
        let Ok(dsn) = std::env::var("ZOU_PG_TEST_DSN") else {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            return;
        };
        if dsn.is_empty() {
            eprintln!("skipping: ZOU_PG_TEST_DSN is empty");
            return;
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let base: tokio_postgres::Config = dsn.parse().expect("the dsn parses");
            let admin = crate::import::tests::open(&base).await;
            for name in ["zou_import_from", "zou_import_into"] {
                admin
                    .batch_execute(&format!("drop database if exists {name} with (force)"))
                    .await
                    .expect("drop");
                admin
                    .batch_execute(&format!("create database {name}"))
                    .await
                    .expect("create");
            }
            let mut from = base.clone();
            from.dbname("zou_import_from");
            let source = crate::import::tests::open(&from).await;
            source
                .batch_execute("create extension if not exists pgcrypto")
                .await
                .expect("pgcrypto");
            source.batch_execute(SOURCE).await.expect("seed the source");
            let mut into = base.clone();
            into.dbname("zou_import_into");
            let mut target = crate::import::tests::open(&into).await;
            target.batch_execute(TARGET).await.expect("seed the target");

            let survey = crate::import::survey(&source).await;
            let done = run(&source, &mut target, &survey, false)
                .await
                .expect("the copy");

            assert_eq!(
                number(&target, "select count(*) from app.authors").await,
                10
            );
            assert_eq!(number(&target, "select count(*) from app.books").await, 100);
            assert_eq!(number(&target, "select count(*) from auth.users").await, 7);
            assert_eq!(
                number(&target, "select count(*) from storage.buckets").await,
                1
            );
            // The generated column is not copied and is still right,
            // because this side computes it the same way that side did.
            assert_eq!(
                number(
                    &target,
                    "select count(*) from app.books where upper_title = upper(title)"
                )
                .await,
                100
            );
            // The sequence is where the source left it, so the next
            // insert does not collide with a row that was copied.
            target
                .batch_execute("insert into app.authors (name) values ('eleventh')")
                .await
                .expect("the sequence moved on");
            assert_eq!(number(&target, "select max(id) from app.authors").await, 11);
            // And the identity column's own sequence too.
            target
                .batch_execute("insert into app.books (author, title) values (1, 'next')")
                .await
                .expect("the identity moved on");
            assert_eq!(number(&target, "select max(id) from app.books").await, 101);
            let rls: bool = target
                .query_one(
                    "select relrowsecurity from pg_class where oid = 'app.books'::regclass",
                    &[],
                )
                .await
                .expect("rls")
                .get(0);
            assert!(rls, "row level security came over");
            assert_eq!(number(&target, "select count(*) from pg_policy").await, 1);

            // A session on the old project does not become a session
            // here, even though the table it lives in is on both sides.
            assert_eq!(
                number(&target, "select count(*) from auth.refresh_tokens").await,
                0,
                "everybody signs in again once, which is the documented policy"
            );

            // A column and a table this server does not have are named
            // rather than dropped in silence, and so is every table
            // that was left behind on purpose.
            let left = done.left.join("\n");
            assert!(left.contains("auth.users.phone"), "{left}");
            assert!(left.contains("storage.objects"), "{left}");
            assert!(left.contains("the bytes of the storage objects"), "{left}");
            assert!(
                left.contains("auth.refresh_tokens, which is a session on the old project"),
                "{left}"
            );

            // The same command again does nothing and breaks nothing,
            // which is what resume means when the first run finished.
            let again = run(&source, &mut target, &survey, false)
                .await
                .expect("the second copy");
            assert!(again.steps.is_empty(), "{:?}", again.steps);
            assert!(!again.resumed.is_empty(), "it read its own ledger");
            assert_eq!(number(&target, "select count(*) from app.books").await, 101);

            drop(source);
            drop(target);
            for name in ["zou_import_from", "zou_import_into"] {
                admin
                    .batch_execute(&format!("drop database {name} with (force)"))
                    .await
                    .expect("drop");
            }
        });
    }
}
