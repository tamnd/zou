//! `zou export`: everything this database holds, written out as sql
//! that needs nothing of zou's to read it back.
//!
//! This is the reverse of `zou import supabase` and it exists for the
//! same reason a fire exit does. A backend somebody cannot leave is a
//! backend somebody has to trust, and trust is a poor substitute for a
//! directory of plain files. So what comes out is `schema.sql`,
//! `data.sql` and `platform.sql`, three files that `psql -f` restores
//! into a stock Postgres or into a hosted Supabase project, and a
//! report saying what is in them and what is not.
//!
//! Nothing here is a zou format. No manifest, no lease, no header this
//! program has to be present to parse. The data is Postgres's own copy
//! text format, which is what `pg_dump` writes and what every Postgres
//! since the nineties reads.
//!
//! Two things make the restore work without a superuser on the far
//! side. The tables go out in dependency order, so a foreign key never
//! points at a table whose rows have not landed yet, and each table's
//! user triggers are turned off around its own rows, which is a table
//! owner's privilege rather than a superuser's. A cycle in the foreign
//! keys is the one case ordering cannot fix, and the report names the
//! tables in it instead of leaving somebody to find out.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use futures_util::{StreamExt, pin_mut};
use tokio_postgres::Client;

use crate::import::copy::{Copyable, NOT_CARRIED, NOT_THE_PROJECTS, PLATFORM, copyable, why};
use crate::import::{EMULATED, SYSTEM_SCHEMAS, connect};
use crate::schema::{self, Catalog, ident};

pub const USAGE: &str = "usage: zou export --db-url <url> --to <dir> [--report <path>]";

/// The three files, named for what a person restoring them does in
/// what order rather than for what wrote them.
const SCHEMA_FILE: &str = "schema.sql";
const DATA_FILE: &str = "data.sql";
const PLATFORM_FILE: &str = "platform.sql";
const DEFAULT_REPORT: &str = "export-report.md";

/// Extensions that belong to the platform or to postgres, so a restore
/// somewhere else neither needs them nor should be told to create them.
const NOT_THE_PROJECTS_EXTENSIONS: &[&str] = &["plpgsql"];

#[derive(Debug, Default)]
pub struct Args {
    pub url: Option<String>,
    pub to: Option<PathBuf>,
    pub report: Option<PathBuf>,
}

pub fn parse(argv: &[String]) -> Result<Args, String> {
    let mut args = Args::default();
    let mut rest = argv.iter();
    while let Some(flag) = rest.next() {
        let mut value = || {
            rest.next()
                .cloned()
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        match flag.as_str() {
            "--db-url" => args.url = Some(value()?),
            "--to" => args.to = Some(PathBuf::from(value()?)),
            "--report" => args.report = Some(PathBuf::from(value()?)),
            "-h" | "--help" => return Err(USAGE.into()),
            other => return Err(format!("unknown flag {other}\n{USAGE}")),
        }
    }
    if args.url.is_none() {
        return Err(format!("nothing to export from, pass --db-url\n{USAGE}"));
    }
    if args.to.is_none() {
        return Err(format!(
            "nowhere to write, pass --to with a directory\n{USAGE}"
        ));
    }
    Ok(args)
}

/// One table on its way out, and how many of its rows went.
#[derive(Debug)]
pub struct Sent {
    pub id: String,
    pub rows: u64,
}

/// What one export wrote, which is what gets printed and what the
/// report is rendered from.
#[derive(Debug, Default)]
pub struct Written {
    pub dir: PathBuf,
    pub server: String,
    pub database: String,
    pub schemas: Vec<String>,
    pub statements: usize,
    pub extensions: Vec<String>,
    /// Extensions this server answers for without installing, which the
    /// far side will want the real one of.
    pub emulated: Vec<String>,
    pub tables: Vec<Sent>,
    pub platform: Vec<Sent>,
    pub sequences: usize,
    /// Tables whose foreign keys point in a circle, which no ordering
    /// can satisfy.
    pub tangled: Vec<String>,
    /// What did not come out, and why. Never empty.
    pub left: Vec<String>,
}

impl Written {
    fn rows(&self) -> u64 {
        self.tables.iter().map(|t| t.rows).sum::<u64>()
            + self.platform.iter().map(|t| t.rows).sum::<u64>()
    }

    /// The summary a person reads at the end of a run.
    pub fn render(&self) -> String {
        let mut out = format!(
            "wrote {} in {} and {} to {}\n",
            crate::import::plural(self.rows() as i64, "row", "rows"),
            crate::import::plural(
                (self.tables.len() + self.platform.len()) as i64,
                "table",
                "tables"
            ),
            crate::import::plural(self.statements as i64, "statement", "statements"),
            self.dir.display()
        );
        for line in &self.left {
            let _ = writeln!(out, "not exported: {line}");
        }
        out
    }
}

pub fn run(argv: &[String]) -> Result<(), String> {
    let args = parse(argv)?;
    let url = args.url.clone().ok_or(USAGE)?;
    let dir = args.to.clone().ok_or(USAGE)?;
    std::fs::create_dir_all(&dir).map_err(|e| format!("cannot make {}: {e}", dir.display()))?;
    let report = args
        .report
        .clone()
        .unwrap_or_else(|| dir.join(DEFAULT_REPORT));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("cannot start a runtime: {e}"))?;
    runtime.block_on(async {
        let client = connect(&url).await?;
        let written = write_all(&client, &dir).await?;
        std::fs::write(&report, render_report(&written))
            .map_err(|e| format!("cannot write {}: {e}", report.display()))?;
        put!("{}", written.render());
        say!("the report is {}", report.display());
        Ok(())
    })
}

/// The whole export. Schema first because the data file names the
/// tables the schema file makes.
pub async fn write_all(client: &Client, dir: &Path) -> Result<Written, String> {
    let mut out = Written {
        dir: dir.to_path_buf(),
        ..Written::default()
    };
    if let Ok(row) = client
        .query_one("select version(), current_database()", &[])
        .await
    {
        out.server = row.get(0);
        out.database = row.get(1);
    }
    out.schemas = project_schemas(client).await?;
    schema_file(client, dir, &mut out).await?;
    data_file(client, dir, &mut out).await?;
    platform_file(client, dir, &mut out).await?;
    unread(&mut out);
    Ok(out)
}

const SCHEMAS_SQL: &str = "\
select nspname::text from pg_namespace
where nspname <> all($1) and nspname not like 'pg\\_%'
order by nspname";

/// The schemas the project owns, which is everything that is neither
/// postgres's nor this server's. The same question the import asks of a
/// hosted project, asked here of this one.
async fn project_schemas(client: &Client) -> Result<Vec<String>, String> {
    let mut not_ours: Vec<String> = SYSTEM_SCHEMAS.iter().map(|s| s.to_string()).collect();
    not_ours.extend(NOT_THE_PROJECTS.iter().map(|s| s.to_string()));
    not_ours.extend(PLATFORM.iter().map(|s| s.to_string()));
    let rows = client
        .query(SCHEMAS_SQL, &[&not_ours])
        .await
        .map_err(|e| format!("cannot read the schemas: {}", why(&e)))?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

const EXTENSIONS_SQL: &str = "\
select extname::text from pg_extension order by extname";

/// `schema.sql`: the extensions, then everything the catalog reader
/// sees, diffed against nothing so that only creates can come out.
async fn schema_file(client: &Client, dir: &Path, out: &mut Written) -> Result<(), String> {
    for row in client
        .query(EXTENSIONS_SQL, &[])
        .await
        .map_err(|e| format!("cannot read the extensions: {}", why(&e)))?
    {
        let name: String = row.get(0);
        if !NOT_THE_PROJECTS_EXTENSIONS.contains(&name.as_str()) {
            out.extensions.push(name);
        }
    }
    // The two this server answers for are not rows in pg_extension,
    // because there is nothing installed to be a row. A project using
    // them still depends on them, so they are named rather than left
    // out of a file whose whole job is to be complete.
    for (name, _) in EMULATED {
        if emulated_here(client, name).await {
            out.emulated.push((*name).to_string());
        }
    }

    let catalog = Catalog::read(client, &out.schemas).await?;
    let statements: Vec<String> = schema::diff(&Catalog::default(), &catalog)
        .into_iter()
        .map(|s| softened(&s))
        .collect();
    if let Some(bad) = statements
        .iter()
        .find(|s| s.trim_start().starts_with("drop "))
    {
        return Err(format!(
            "the export would have written {bad:?}, which is a drop, and an export writes nothing that destroys anything"
        ));
    }
    // A serial column's default is a call to a sequence by name, and
    // nothing in the diff makes that sequence: `zou db diff` runs
    // against a database that already has one. An export lands on a
    // database that has nothing, so the sequences are made here, before
    // the tables whose defaults name them, and handed to their columns
    // after those tables exist.
    let (made, owned) = sequences_ddl(client, &out.schemas).await?;
    out.statements = statements.len() + made.len() + owned.len();

    let mut sql = String::new();
    sql.push_str(
        "-- The shape of the database, written by zou export.\n\
         --\n\
         -- Restore it with:\n\
         --\n\
         --   psql -v ON_ERROR_STOP=1 -d <target> -f schema.sql\n\
         --   psql -v ON_ERROR_STOP=1 -d <target> -f data.sql\n\
         --\n\
         -- Nothing in this file is a zou format. It is the sql a stock\n\
         -- Postgres reads, which is the point of it.\n\n",
    );
    for name in &out.extensions {
        let _ = writeln!(sql, "create extension if not exists {};", ident(name));
    }
    if !out.extensions.is_empty() {
        sql.push('\n');
    }
    // The schemas first, then the sequences inside them, then
    // everything the diff had to say, then the sequences handed to the
    // columns that draw from them.
    let (folders, rest): (Vec<&String>, Vec<&String>) = statements
        .iter()
        .partition(|s| s.starts_with("create schema "));
    for statement in folders.into_iter().chain(made.iter()) {
        sql.push_str(statement);
        sql.push('\n');
    }
    for statement in rest.into_iter().chain(owned.iter()) {
        sql.push_str(statement);
        sql.push('\n');
    }
    std::fs::write(dir.join(SCHEMA_FILE), sql)
        .map_err(|e| format!("cannot write {SCHEMA_FILE}: {e}"))?;
    Ok(())
}

const SEQUENCES_DDL_SQL: &str = "\
select
  s.schemaname || '.' || s.sequencename as id,
  s.data_type::text as kind,
  s.start_value, s.min_value, s.max_value, s.increment_by, s.cycle,
  coalesce(s.cache_size, 1) as cache,
  d.deptype::text as dep,
  tn.nspname || '.' || tc.relname as owner_table,
  a.attname as owner_column
from pg_sequences s
  join pg_namespace sn on sn.nspname = s.schemaname
  join pg_class sc on sc.relname = s.sequencename and sc.relnamespace = sn.oid
  left join pg_depend d
    on d.classid = 'pg_class'::regclass and d.objid = sc.oid
   and d.refclassid = 'pg_class'::regclass and d.deptype in ('a', 'i')
  left join pg_class tc on tc.oid = d.refobjid
  left join pg_namespace tn on tn.oid = tc.relnamespace
  left join pg_attribute a on a.attrelid = d.refobjid and a.attnum = d.refobjsubid
where s.schemaname = any($1)
order by id";

/// The sequences to make, and the columns to hand them to afterwards.
///
/// A sequence belonging to an identity column is not in either list. An
/// identity column carries its sequence in its own definition, so the
/// `create table` already makes one, and a second `create sequence` for
/// the same name would collide with it. Postgres marks that dependency
/// internal, which is exactly the distinction wanted here: auto means a
/// serial column, which needs both statements, and internal means an
/// identity column, which needs neither.
async fn sequences_ddl(
    client: &Client,
    schemas: &[String],
) -> Result<(Vec<String>, Vec<String>), String> {
    let rows = client
        .query(SEQUENCES_DDL_SQL, &[&schemas])
        .await
        .map_err(|e| format!("cannot read the sequences: {}", why(&e)))?;
    let mut made = Vec::new();
    let mut owned = Vec::new();
    for row in &rows {
        let dep: Option<String> = row.get("dep");
        if dep.as_deref() == Some("i") {
            continue;
        }
        let id: String = row.get("id");
        let (kind, start, min, max, step, cycle, cache): (String, i64, i64, i64, i64, bool, i64) = (
            row.get("kind"),
            row.get("start_value"),
            row.get("min_value"),
            row.get("max_value"),
            row.get("increment_by"),
            row.get("cycle"),
            row.get("cache"),
        );
        made.push(format!(
            "create sequence if not exists {} as {kind} start with {start} increment by {step} minvalue {min} maxvalue {max} cache {cache}{};",
            qualified(&id),
            if cycle { " cycle" } else { " no cycle" }
        ));
        let (table, column): (Option<String>, Option<String>) =
            (row.get("owner_table"), row.get("owner_column"));
        if let (Some(table), Some(column)) = (table, column) {
            owned.push(format!(
                "alter sequence {} owned by {}.{};",
                qualified(&id),
                qualified(&table),
                ident(&column)
            ));
        }
    }
    Ok((made, owned))
}

/// A `schema.name` with each half quoted the way postgres would write
/// it, so a table somebody called `Order` survives being named.
fn qualified(id: &str) -> String {
    let (schema, name) = id.split_once('.').unwrap_or(("public", id));
    format!("{}.{}", ident(schema), ident(name))
}

/// `create schema public;` with an `if not exists` in it, because every
/// database in the world already has a `public` and the restore would
/// stop on the first line otherwise.
///
/// A migration is right to be strict here: `zou db diff` knows both
/// catalogs, so a schema it says to create is a schema that is not
/// there, and a database that disagrees is a database somebody should
/// hear about. An export knows only one of them, and the other side is
/// a target it has never seen. Creating a schema that is already there
/// is the ordinary case rather than the surprising one, and it is the
/// only statement in the file that has that shape.
fn softened(statement: &str) -> String {
    match statement.strip_prefix("create schema ") {
        Some(rest) => format!("create schema if not exists {rest}"),
        None => statement.to_string(),
    }
}

/// Whether one of the extensions this server answers for is being used
/// here, which is a question about the schema rather than about
/// pg_extension.
async fn emulated_here(client: &Client, name: &str) -> bool {
    let probe = match name {
        "pg_net" => "net.http_request_queue",
        "pg_cron" => "cron.job",
        _ => return false,
    };
    client
        .query_one("select to_regclass($1) is not null", &[&probe])
        .await
        .map(|r| r.get(0))
        .unwrap_or(false)
}

/// `data.sql`: the project's own rows, in an order a restore can
/// actually apply.
async fn data_file(client: &Client, dir: &Path, out: &mut Written) -> Result<(), String> {
    let tables = copyable(client, &out.schemas).await?;
    let (ordered, tangled) = in_dependency_order(client, tables, &out.schemas).await?;
    out.tangled = tangled;
    let mut file = std::fs::File::create(dir.join(DATA_FILE))
        .map_err(|e| format!("cannot write {DATA_FILE}: {e}"))?;
    let mut file = std::io::BufWriter::new(&mut file);
    write_bytes(
        &mut file,
        "-- The rows, written by zou export in postgres's own copy text\n\
         -- format, which is what pg_dump writes.\n\
         --\n\
         -- The tables are in dependency order, so a foreign key never\n\
         -- points at a table whose rows have not landed yet, and each\n\
         -- table's user triggers are off around its own rows, which a\n\
         -- table's owner may do and which a superuser is not needed for.\n\
         -- The triggers already ran on the database these rows came out\n\
         -- of, so running them again would be the second time.\n\
         --\n\
         -- One transaction, so a restore that fails leaves the target\n\
         -- as it found it rather than half loaded with its triggers off.\n\n\
         begin;\n\n",
    )?;
    for table in &ordered {
        let rows = table_rows(client, &mut file, table).await?;
        out.tables.push(Sent {
            id: table.id.clone(),
            rows,
        });
    }
    out.sequences += setvals(client, &mut file, &out.schemas).await?;
    write_bytes(&mut file, "commit;\n")?;
    std::io::Write::flush(&mut file).map_err(|e| format!("cannot finish {DATA_FILE}: {e}"))?;
    Ok(())
}

/// `platform.sql`: the `auth` and `storage` rows, in a file of their
/// own because the tables they go into are not in `schema.sql`. On the
/// far side something else makes them, GoTrue and the storage service
/// on hosted Supabase or this server on another zou, and a restore into
/// a plain Postgres skips this file entirely.
async fn platform_file(client: &Client, dir: &Path, out: &mut Written) -> Result<(), String> {
    let platform: Vec<String> = PLATFORM.iter().map(|s| s.to_string()).collect();
    let tables = copyable(client, &platform).await?;
    let mut file = std::fs::File::create(dir.join(PLATFORM_FILE))
        .map_err(|e| format!("cannot write {PLATFORM_FILE}: {e}"))?;
    let mut file = std::io::BufWriter::new(&mut file);
    write_bytes(
        &mut file,
        "-- The auth and storage rows, written by zou export.\n\
         --\n\
         -- These tables are not in schema.sql, because on the far side\n\
         -- something else makes them: GoTrue and the storage service on\n\
         -- hosted Supabase, this server on another zou. Restore this\n\
         -- file after they exist, and skip it entirely when the target\n\
         -- is a plain Postgres with no auth in it.\n\
         --\n\
         --   psql -v ON_ERROR_STOP=1 -d <target> -f platform.sql\n\n\
         begin;\n\n",
    )?;
    for table in &tables {
        if let Some((_, reason)) = NOT_CARRIED.iter().find(|(id, _)| *id == table.id) {
            out.left.push(format!("{}, which is {reason}", table.id));
            continue;
        }
        let rows = table_rows(client, &mut file, table).await?;
        out.platform.push(Sent {
            id: table.id.clone(),
            rows,
        });
    }
    out.sequences += setvals(client, &mut file, &platform).await?;
    write_bytes(&mut file, "commit;\n")?;
    std::io::Write::flush(&mut file).map_err(|e| format!("cannot finish {PLATFORM_FILE}: {e}"))?;
    Ok(())
}

fn write_bytes(file: &mut impl std::io::Write, text: &str) -> Result<(), String> {
    file.write_all(text.as_bytes())
        .map_err(|e| format!("cannot write: {e}"))
}

/// One table's rows, streamed out of postgres and into the file without
/// being held anywhere in between, so a table larger than this machine's
/// memory still exports.
///
/// Row counting is a count of newlines, which is exact rather than
/// nearly right: copy's text format escapes a newline inside a value as
/// two characters, so the only raw newlines in the stream are the ones
/// that end rows.
async fn table_rows(
    client: &Client,
    file: &mut impl std::io::Write,
    table: &Copyable,
) -> Result<u64, String> {
    let list = table.list();
    write_bytes(
        file,
        &format!(
            "alter table {} disable trigger user;\ncopy {} ({list}) from stdin;\n",
            table.id, table.id
        ),
    )?;
    let stream = client
        .copy_out(&format!("copy (select {list} from {}) to stdout", table.id))
        .await
        .map_err(|e| format!("{}: cannot read: {}", table.id, why(&e)))?;
    pin_mut!(stream);
    let mut rows = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| format!("{}: reading: {}", table.id, why(&e)))?;
        rows += chunk.iter().filter(|b| **b == b'\n').count() as u64;
        file.write_all(&chunk)
            .map_err(|e| format!("{}: writing: {e}", table.id))?;
    }
    write_bytes(
        file,
        &format!("\\.\nalter table {} enable trigger user;\n\n", table.id),
    )?;
    Ok(rows)
}

const EDGES_SQL: &str = "\
select nr.nspname || '.' || cr.relname, nf.nspname || '.' || cf.relname
from pg_constraint k
  join pg_class cr on cr.oid = k.conrelid
  join pg_namespace nr on nr.oid = cr.relnamespace
  join pg_class cf on cf.oid = k.confrelid
  join pg_namespace nf on nf.oid = cf.relnamespace
where k.contype = 'f' and nr.nspname = any($1) and nf.nspname = any($1)";

/// The tables sorted so that every table a foreign key points at comes
/// before the table pointing at it, and the ones left over.
///
/// Left over means a circle: two tables that each point at the other,
/// or a longer ring. No order satisfies those, so they go last and get
/// named in the report. A table pointing at itself is not a circle
/// between tables and does not stop the sort, but its rows can still
/// fail to land in the order they come out, so it counts as tangled
/// too.
async fn in_dependency_order(
    client: &Client,
    tables: Vec<Copyable>,
    schemas: &[String],
) -> Result<(Vec<Copyable>, Vec<String>), String> {
    let rows = client
        .query(EDGES_SQL, &[&schemas])
        .await
        .map_err(|e| format!("cannot read the foreign keys: {}", why(&e)))?;
    let mut edges: Vec<(String, String)> = Vec::new();
    let mut itself: BTreeSet<String> = BTreeSet::new();
    for row in &rows {
        let (child, parent): (String, String) = (row.get(0), row.get(1));
        if child == parent {
            itself.insert(child);
        } else {
            edges.push((parent, child));
        }
    }
    Ok(sort(tables, &edges, itself))
}

/// Kahn's algorithm, with the input order kept for everything the
/// edges do not decide, so two exports of the same database write the
/// same file.
fn sort(
    tables: Vec<Copyable>,
    edges: &[(String, String)],
    itself: BTreeSet<String>,
) -> (Vec<Copyable>, Vec<String>) {
    let here: BTreeSet<&str> = tables.iter().map(|t| t.id.as_str()).collect();
    let mut waiting_on: BTreeMap<&str, usize> = tables.iter().map(|t| (t.id.as_str(), 0)).collect();
    let mut feeds: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (parent, child) in edges {
        if !here.contains(parent.as_str()) || !here.contains(child.as_str()) {
            continue;
        }
        feeds.entry(parent).or_default().push(child);
        *waiting_on.entry(child).or_default() += 1;
    }
    let mut order: Vec<&str> = Vec::new();
    let mut ready: Vec<&str> = tables
        .iter()
        .map(|t| t.id.as_str())
        .filter(|id| waiting_on[id] == 0)
        .collect();
    while let Some(id) = ready.first().copied() {
        ready.remove(0);
        order.push(id);
        for child in feeds.get(id).into_iter().flatten() {
            let left = waiting_on.get_mut(child).expect("a table of this database");
            *left -= 1;
            if *left == 0 {
                ready.push(child);
            }
        }
    }
    let placed: BTreeSet<&str> = order.iter().copied().collect();
    let mut tangled: Vec<String> = itself
        .into_iter()
        .filter(|id| here.contains(id.as_str()))
        .collect();
    for table in &tables {
        if !placed.contains(table.id.as_str()) {
            order.push(table.id.as_str());
            tangled.push(table.id.clone());
        }
    }
    tangled.sort();
    tangled.dedup();
    let mut by_id: BTreeMap<&str, &Copyable> = BTreeMap::new();
    for table in &tables {
        by_id.insert(table.id.as_str(), table);
    }
    let sorted = order
        .iter()
        .map(|id| Copyable {
            id: by_id[id].id.clone(),
            columns: by_id[id].columns.clone(),
        })
        .collect();
    (sorted, tangled)
}

const SEQUENCES_SQL: &str = "\
select schemaname || '.' || sequencename, last_value
from pg_sequences where schemaname = any($1) and last_value is not null
order by schemaname, sequencename";

/// A `setval` per sequence that has been used, so the next insert on
/// the far side does not collide with a row that just arrived. A
/// sequence nobody has drawn from has a null `last_value` and needs no
/// line, because a fresh one is already where it should be.
async fn setvals(
    client: &Client,
    file: &mut impl std::io::Write,
    schemas: &[String],
) -> Result<usize, String> {
    let rows = client
        .query(SEQUENCES_SQL, &[&schemas])
        .await
        .map_err(|e| format!("cannot read the sequences: {}", why(&e)))?;
    if rows.is_empty() {
        return Ok(0);
    }
    write_bytes(
        file,
        "-- Where each sequence had got to, so the next insert on the\n\
         -- other side does not collide with a row that just arrived.\n",
    )?;
    for row in &rows {
        let (id, last): (String, i64) = (row.get(0), row.get(1));
        write_bytes(
            file,
            &format!(
                "select pg_catalog.setval('{}', {last}, true);\n",
                quoted(&id)
            ),
        )?;
    }
    write_bytes(file, "\n")?;
    Ok(rows.len())
}

/// A qualified name inside a sql string literal, quoted the way
/// postgres would write it and with any apostrophe in it doubled.
fn quoted(id: &str) -> String {
    qualified(id).replace('\'', "''")
}

/// What did not come out, said every run whether or not there was
/// anything interesting in it, for the same reason `zou db diff` prints
/// its own list: a section that came back empty from a tool that never
/// looked reads exactly like good news.
fn unread(out: &mut Written) {
    out.left.push(
        "the bytes of the storage objects, the rows that name them are in platform.sql and the bytes are in the object store"
            .into(),
    );
    out.left.push(
        "roles and their passwords, because a role belongs to the cluster and not to this database"
            .into(),
    );
    out.left
        .push("large objects, which nothing here creates".into());
    out.left.push(
        "settings made with alter database or alter role, which belong to the server they were set on"
            .into(),
    );
    out.left.push(
        "publications, subscriptions and replication slots, which name a stream rather than hold data"
            .into(),
    );
    for kind in schema::UNREAD {
        // Two of the diff's blind spots are not this file's. Sequences
        // are written here whether or not a column owns them, and the
        // extensions are the first thing in schema.sql, so repeating
        // either one would be telling somebody a hole is there when
        // they can go and look at it.
        if *kind == "sequences that no column owns" || *kind == "extensions" {
            continue;
        }
        out.left
            .push(format!("{kind}, which the catalog reader does not look at"));
    }
    if !out.tangled.is_empty() {
        out.left.push(format!(
            "no order that satisfies the foreign keys of {}, so those tables are last and their keys are checked as the rows land",
            out.tangled.join(", ")
        ));
    }
    for name in &out.emulated {
        out.left.push(format!(
            "the {name} extension, which this server answers for rather than installs, so the far side needs the real one"
        ));
    }
}

/// The report, which is the thing somebody reads before trusting the
/// three files next to it.
pub fn render_report(out: &Written) -> String {
    let mut md = String::from("# Export report\n\n");
    if !out.database.is_empty() {
        let _ = writeln!(md, "Database `{}`.\n", out.database);
    }
    if !out.server.is_empty() {
        let _ = writeln!(md, "Server `{}`.\n", out.server);
    }

    md.push_str("## What came out\n\n");
    let _ = writeln!(
        md,
        "{} in {} and {} of schema, into `{}`.\n",
        crate::import::plural(out.rows() as i64, "row", "rows"),
        crate::import::plural(
            (out.tables.len() + out.platform.len()) as i64,
            "table",
            "tables"
        ),
        crate::import::plural(out.statements as i64, "statement", "statements"),
        out.dir.display()
    );
    md.push_str("| file | what is in it |\n| --- | --- |\n");
    let _ = writeln!(
        md,
        "| `{SCHEMA_FILE}` | {} of ddl for {} |",
        crate::import::plural(out.statements as i64, "statement", "statements"),
        list(&out.schemas)
    );
    let _ = writeln!(
        md,
        "| `{DATA_FILE}` | the rows of {}, and {} |",
        crate::import::plural(out.tables.len() as i64, "table", "tables"),
        crate::import::plural(out.sequences as i64, "sequence", "sequences")
    );
    let _ = writeln!(
        md,
        "| `{PLATFORM_FILE}` | the rows of {} in auth and storage |\n",
        crate::import::plural(out.platform.len() as i64, "table", "tables")
    );

    md.push_str("## Restoring it\n\n```\npsql -v ON_ERROR_STOP=1 -d <target> -f schema.sql\npsql -v ON_ERROR_STOP=1 -d <target> -f data.sql\npsql -v ON_ERROR_STOP=1 -d <target> -f platform.sql\n```\n\n");
    md.push_str("The third one only after something has made the `auth` and `storage` tables, which on hosted Supabase is the platform and on another zou is the server at startup. A target that is a plain Postgres with no auth in it takes the first two and not the third.\n\n");
    md.push_str("Nothing here needs zou to read it. The ddl is sql and the rows are postgres's own copy text format, which is what `pg_dump` writes.\n\n");

    md.push_str("## The tables\n\n");
    if out.tables.is_empty() && out.platform.is_empty() {
        md.push_str(
            "None, which means this database has no rows in any schema the project owns.\n\n",
        );
    } else {
        md.push_str("| table | rows |\n| --- | --- |\n");
        for table in out.tables.iter().chain(out.platform.iter()) {
            let _ = writeln!(md, "| `{}` | {} |", table.id, table.rows);
        }
        md.push('\n');
    }

    md.push_str("## Extensions\n\n");
    if out.extensions.is_empty() {
        md.push_str("None beyond what a stock Postgres has.\n\n");
    } else {
        let _ = writeln!(
            md,
            "`schema.sql` starts with a `create extension if not exists` for each of: {}.\n",
            list(&out.extensions)
        );
    }
    if !out.emulated.is_empty() {
        let _ = writeln!(
            md,
            "And {} is not in that list, because this server answers for it rather than installing it. A project using it needs the real extension wherever these files are restored.\n",
            list(&out.emulated)
        );
    }

    md.push_str("## What did not come out\n\n");
    for line in &out.left {
        let _ = writeln!(md, "- {line}");
    }
    md.push('\n');
    md.push_str("This section is never empty, which is the point of it. An export that says nothing about what it left behind is an export somebody finds the hole in later.\n");
    md
}

fn list(items: &[String]) -> String {
    if items.is_empty() {
        return "nothing".into();
    }
    items
        .iter()
        .map(|s| format!("`{s}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One of these files put back, the way psql puts it back.
    ///
    /// Everything except a copy block goes through as sql. A copy block
    /// is the one thing a client has to understand rather than forward:
    /// psql sees `from stdin`, takes the lines up to the `\.` itself and
    /// pushes them down the copy protocol, and so does this. Twenty
    /// lines of it here rather than shelling out to psql, so the test
    /// needs nothing installed to run.
    async fn restore(client: &Client, sql: &str) -> Result<(), String> {
        use futures_util::SinkExt;

        let mut pending = String::new();
        let mut lines = sql.lines();
        while let Some(line) = lines.next() {
            if !(line.starts_with("copy ") && line.ends_with("from stdin;")) {
                pending.push_str(line);
                pending.push('\n');
                continue;
            }
            if !pending.trim().is_empty() {
                client
                    .batch_execute(&pending)
                    .await
                    .map_err(|e| format!("{pending}: {}", why(&e)))?;
            }
            pending.clear();
            let sink = client
                .copy_in(line.trim_end_matches(';'))
                .await
                .map_err(|e| format!("{line}: {}", why(&e)))?;
            pin_mut!(sink);
            for row in lines.by_ref() {
                if row == "\\." {
                    break;
                }
                sink.send(bytes::Bytes::from(format!("{row}\n")))
                    .await
                    .map_err(|e| format!("{line}: {}", why(&e)))?;
            }
            sink.finish()
                .await
                .map_err(|e| format!("{line}: {}", why(&e)))?;
        }
        if !pending.trim().is_empty() {
            client
                .batch_execute(&pending)
                .await
                .map_err(|e| format!("{pending}: {}", why(&e)))?;
        }
        Ok(())
    }

    /// A connection with its driver pumped, the same two lines the
    /// import's own live tests use.
    async fn open(config: &tokio_postgres::Config) -> Client {
        let (client, connection) = config
            .connect(tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    fn table(id: &str) -> Copyable {
        Copyable {
            id: id.into(),
            columns: vec!["id".into()],
        }
    }

    fn ids(tables: &[Copyable]) -> Vec<String> {
        tables.iter().map(|t| t.id.clone()).collect()
    }

    #[test]
    fn the_flags_come_apart() {
        let args = parse(&[
            "--db-url".into(),
            "postgresql://localhost/zou".into(),
            "--to".into(),
            "/tmp/out".into(),
            "--report".into(),
            "/tmp/r.md".into(),
        ])
        .expect("parses");
        assert_eq!(args.url.as_deref(), Some("postgresql://localhost/zou"));
        assert_eq!(args.to, Some(PathBuf::from("/tmp/out")));
        assert_eq!(args.report, Some(PathBuf::from("/tmp/r.md")));
    }

    /// Both halves are required, and the refusal says which one is
    /// missing rather than printing the usage and leaving somebody to
    /// diff it against what they typed.
    #[test]
    fn an_export_needs_a_database_and_a_directory() {
        let e = parse(&["--to".into(), "/tmp/out".into()]).expect_err("no source");
        assert!(e.contains("nothing to export from"), "{e}");
        let e = parse(&["--db-url".into(), "postgresql://localhost/zou".into()])
            .expect_err("nowhere to write");
        assert!(e.contains("nowhere to write"), "{e}");
        let e = parse(&["--wat".into()]).expect_err("unknown");
        assert!(e.contains("unknown flag --wat"), "{e}");
    }

    /// A table a foreign key points at is written before the table
    /// pointing at it, whatever order the catalog happened to answer
    /// in.
    #[test]
    fn a_table_arrives_after_the_one_it_points_at() {
        let tables = vec![table("app.books"), table("app.authors")];
        let edges = vec![("app.authors".to_string(), "app.books".to_string())];
        let (sorted, tangled) = sort(tables, &edges, BTreeSet::new());
        assert_eq!(ids(&sorted), vec!["app.authors", "app.books"]);
        assert!(tangled.is_empty(), "{tangled:?}");
    }

    /// A chain sorts all the way down, and a table with no keys at all
    /// keeps the place the catalog gave it, so two exports of one
    /// database write the same file.
    #[test]
    fn a_chain_sorts_and_everything_else_stays_where_it_was() {
        let tables = vec![
            table("app.c"),
            table("app.loose"),
            table("app.b"),
            table("app.a"),
        ];
        let edges = vec![
            ("app.a".to_string(), "app.b".to_string()),
            ("app.b".to_string(), "app.c".to_string()),
        ];
        let (sorted, _) = sort(tables, &edges, BTreeSet::new());
        let order = ids(&sorted);
        let at = |id: &str| order.iter().position(|x| x == id).expect(id);
        assert!(at("app.a") < at("app.b"), "{order:?}");
        assert!(at("app.b") < at("app.c"), "{order:?}");
        assert!(order.contains(&"app.loose".to_string()), "{order:?}");
        assert_eq!(order.len(), 4);
    }

    /// Two tables pointing at each other cannot be ordered, so they are
    /// still written and they are named, because the alternative is an
    /// export that quietly leaves two tables out.
    #[test]
    fn a_circle_is_written_and_named() {
        let tables = vec![table("app.left"), table("app.right")];
        let edges = vec![
            ("app.left".to_string(), "app.right".to_string()),
            ("app.right".to_string(), "app.left".to_string()),
        ];
        let (sorted, tangled) = sort(tables, &edges, BTreeSet::new());
        assert_eq!(sorted.len(), 2, "both tables are still exported");
        assert_eq!(tangled, vec!["app.left", "app.right"]);
    }

    /// A table pointing at itself does not stop the sort, and is still
    /// worth saying, because its own rows can come out in an order that
    /// will not go back in.
    #[test]
    fn a_table_pointing_at_itself_is_named_but_does_not_stop_anything() {
        let tables = vec![table("app.tree")];
        let itself = BTreeSet::from(["app.tree".to_string()]);
        let (sorted, tangled) = sort(tables, &[], itself);
        assert_eq!(ids(&sorted), vec!["app.tree"]);
        assert_eq!(tangled, vec!["app.tree"]);
    }

    /// A name with an apostrophe or a capital in it goes into the
    /// setval literal the way postgres would write it, because a
    /// sequence called `Order's` is a sequence somebody made.
    #[test]
    fn a_sequence_name_survives_being_put_in_a_string() {
        assert_eq!(quoted("public.notes_id_seq"), "public.notes_id_seq");
        assert_eq!(quoted("app.Order"), "app.\"Order\"");
        assert_eq!(quoted("app.it's"), "app.\"it''s\"");
    }

    /// Every database already has a `public`, so the one statement that
    /// would stop a restore on its first line is written to tolerate
    /// finding what it was going to make. Nothing else is softened.
    #[test]
    fn creating_a_schema_that_is_already_there_is_not_an_error() {
        assert_eq!(
            softened("create schema public;"),
            "create schema if not exists public;"
        );
        assert_eq!(
            softened("create table public.notes (id int);"),
            "create table public.notes (id int);"
        );
    }

    /// The list of what did not come out is never empty, even for a
    /// database with nothing in it, and it always names the object
    /// bytes.
    #[test]
    fn a_run_says_what_it_left_behind() {
        let mut out = Written::default();
        unread(&mut out);
        assert!(!out.left.is_empty());
        let all = out.left.join("\n");
        assert!(all.contains("the bytes of the storage objects"), "{all}");
        assert!(all.contains("roles and their passwords"), "{all}");
        assert!(all.contains("large objects"), "{all}");
    }

    /// The report has a row per file and a row per table, because the
    /// two questions somebody has are what to restore and whether their
    /// biggest table is in it.
    #[test]
    fn the_report_names_the_files_and_the_tables() {
        let mut out = Written {
            dir: PathBuf::from("/tmp/out"),
            database: "postgres".into(),
            schemas: vec!["public".into()],
            statements: 12,
            extensions: vec!["pgcrypto".into()],
            tables: vec![Sent {
                id: "public.notes".into(),
                rows: 5000,
            }],
            platform: vec![Sent {
                id: "auth.users".into(),
                rows: 7,
            }],
            sequences: 1,
            ..Written::default()
        };
        unread(&mut out);
        let md = render_report(&out);
        assert!(md.contains("`schema.sql`"), "{md}");
        assert!(md.contains("`data.sql`"), "{md}");
        assert!(md.contains("`platform.sql`"), "{md}");
        assert!(md.contains("| `public.notes` | 5000 |"), "{md}");
        assert!(md.contains("| `auth.users` | 7 |"), "{md}");
        assert!(md.contains("`pgcrypto`"), "{md}");
        assert!(md.contains("## What did not come out"), "{md}");
    }

    /// The console line is one sentence and then everything left
    /// behind, the same shape the copy prints.
    #[test]
    fn the_summary_counts_both_halves() {
        let out = Written {
            dir: PathBuf::from("/tmp/out"),
            statements: 3,
            tables: vec![Sent {
                id: "public.notes".into(),
                rows: 10,
            }],
            platform: vec![Sent {
                id: "auth.users".into(),
                rows: 2,
            }],
            left: vec!["the bytes of the storage objects".into()],
            ..Written::default()
        };
        let line = out.render();
        assert!(
            line.starts_with("wrote 12 rows in 2 tables and 3 statements to /tmp/out\n"),
            "{line}"
        );
        assert!(
            line.contains("not exported: the bytes of the storage objects"),
            "{line}"
        );
    }

    /// The whole export against a real database, then the files put
    /// back into a second one, because a dump nobody has restored is a
    /// dump nobody knows about.
    #[test]
    fn a_database_goes_out_as_sql_and_comes_back_in() {
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
            let admin = open(&base).await;
            for name in ["zou_export_from", "zou_export_into"] {
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
            from.dbname("zou_export_from");
            let source = open(&from).await;
            source
                .batch_execute("create extension if not exists pgcrypto")
                .await
                .expect("pgcrypto");
            source.batch_execute(SEED).await.expect("seed");

            let dir = std::env::temp_dir().join("zou-export-test");
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("make the directory");
            let written = write_all(&source, &dir).await.expect("the export");

            // The books point at the authors, so the authors are
            // written first whatever order the catalog answered in.
            let order: Vec<&str> = written.tables.iter().map(|t| t.id.as_str()).collect();
            let at = |id: &str| order.iter().position(|x| *x == id).expect(id);
            assert!(at("app.authors") < at("app.books"), "{order:?}");
            assert_eq!(
                written
                    .tables
                    .iter()
                    .find(|t| t.id == "app.books")
                    .expect("books")
                    .rows,
                100
            );
            // A body with a newline and a tab and a backslash in it is
            // still one row, which is what makes counting newlines
            // right rather than nearly right.
            assert_eq!(
                written
                    .tables
                    .iter()
                    .find(|t| t.id == "app.notes")
                    .expect("notes")
                    .rows,
                3
            );
            assert_eq!(
                written
                    .platform
                    .iter()
                    .find(|t| t.id == "auth.users")
                    .expect("users")
                    .rows,
                7
            );
            assert!(
                !written
                    .platform
                    .iter()
                    .any(|t| t.id == "auth.refresh_tokens"),
                "a session is not exported either: {:?}",
                written.platform
            );

            let mut into = base.clone();
            into.dbname("zou_export_into");
            let target = open(&into).await;
            // The far side makes its own auth and storage, the way the
            // platform does there and this server does here.
            target
                .batch_execute(PLATFORM_THERE)
                .await
                .expect("platform");
            for file in [SCHEMA_FILE, DATA_FILE, PLATFORM_FILE] {
                let sql = std::fs::read_to_string(dir.join(file)).expect(file);
                restore(&target, &sql)
                    .await
                    .unwrap_or_else(|e| panic!("{file} does not restore: {e}"));
            }

            let count = |sql: &'static str| {
                let target = &target;
                async move {
                    target
                        .query_one(sql, &[])
                        .await
                        .expect(sql)
                        .get::<_, i64>(0)
                }
            };
            assert_eq!(count("select count(*) from app.authors").await, 10);
            assert_eq!(count("select count(*) from app.books").await, 100);
            assert_eq!(count("select count(*) from auth.users").await, 7);
            assert_eq!(count("select count(*) from auth.refresh_tokens").await, 0);
            // The value that had a newline and a backslash in it is the
            // same value on the other side.
            assert_eq!(
                count("select count(*) from app.notes where body like E'one\\ntwo%'").await,
                1
            );
            // The generated column is computed there rather than
            // carried, and it agrees.
            assert_eq!(
                count("select count(*) from app.books where upper_title = upper(title)").await,
                100
            );
            // The sequence is where it was, so the next insert does not
            // land on a row that just arrived.
            target
                .batch_execute("insert into app.authors (name) values ('eleventh')")
                .await
                .expect("the sequence came over");
            assert_eq!(count("select max(id) from app.authors").await, 11);
            // Row level security and the policy on it came too.
            assert_eq!(count("select count(*) from pg_policy").await, 1);

            let report = render_report(&written);
            assert!(report.contains("| `app.books` | 100 |"), "{report}");
            assert!(
                report.contains("the bytes of the storage objects"),
                "{report}"
            );

            let _ = std::fs::remove_dir_all(&dir);
            drop(source);
            drop(target);
            for name in ["zou_export_from", "zou_export_into"] {
                admin
                    .batch_execute(&format!("drop database {name} with (force)"))
                    .await
                    .expect("drop");
            }
        });
    }

    /// A project with the shapes that break a dump: a foreign key that
    /// wants an order, a serial, a generated column, row level security
    /// and a policy, a value with the characters copy has to escape,
    /// and the two platform schemas including one table that is a
    /// session and does not travel.
    const SEED: &str = "
create schema app;
create table app.authors (id bigserial primary key, name text not null);
create table app.books (
    id bigint generated always as identity primary key,
    author bigint not null references app.authors(id),
    title text not null,
    upper_title text generated always as (upper(title)) stored
);
create table app.notes (id int primary key, body text);
insert into app.authors (name) select 'author ' || g from generate_series(1, 10) g;
insert into app.books (author, title)
select 1 + (g % 10), 'book ' || g from generate_series(1, 100) g;
insert into app.notes values (1, E'one\\ntwo\\tthree'), (2, 'back\\\\slash'), (3, null);
alter table app.books enable row level security;
create policy readable on app.books for select using (true);
create schema auth;
create table auth.users (id uuid primary key, email text);
insert into auth.users select gen_random_uuid(), 'u' || g || '@example.com'
from generate_series(1, 7) g;
create table auth.refresh_tokens (id bigserial primary key, token text);
insert into auth.refresh_tokens (token) select 'token ' || g from generate_series(1, 12) g;
create schema storage;
create table storage.buckets (id text primary key, public boolean default false);
insert into storage.buckets values ('avatars', true);
";

    /// What the far side already has when the files land: the two
    /// platform schemas, made by whatever runs auth and storage there.
    const PLATFORM_THERE: &str = "
create extension if not exists pgcrypto;
create schema auth;
create table auth.users (id uuid primary key, email text);
create table auth.refresh_tokens (id bigserial primary key, token text);
create schema storage;
create table storage.buckets (id text primary key, public boolean default false);
";
}
