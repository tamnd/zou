//! `zou db push`, `zou db reset` and `zou migration new`: a project's
//! own migrations, applied to a zou database.
//!
//! A Supabase project keeps its schema as timestamped files under
//! `supabase/migrations` and a ledger of what has been applied in
//! `supabase_migrations.schema_migrations`. Both of those are the CLI's
//! shapes rather than ours, so the same directory works either way and
//! a project can go back and forth while it is deciding.
//!
//! `push` applies what the ledger has not seen, in name order, each
//! file as one implicit transaction, so a file that fails half way
//! leaves nothing behind and nothing in the ledger. `reset` throws away
//! the project's own schemas and replays every migration and the seed
//! over them, which is the loop somebody runs twenty times while they
//! are writing a migration. Neither of them touches the schemas this
//! server manages, `auth` and `storage` above all: on the Supabase CLI
//! those come back because a container recreates them, and here they
//! belong to the running server, so a reset that dropped them would
//! break the api rather than reset the project.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio_postgres::{Client, NoTls};

use crate::config::{self, Project};

pub const DB_USAGE: &str =
    "usage: zou db <push [--dry-run] | reset [--force]> [--db-url <url>] [--config <config.toml>]";
pub const MIGRATION_USAGE: &str = "usage: zou migration new <name> [--config <config.toml>]";

/// The ledger the Supabase CLI reads and writes, so a database pushed
/// to from here is a database it can go on pushing to.
/// The notices are turned down around it, because "already exists,
/// skipping" on every run is our own bookkeeping talking, not
/// something the project did.
const LEDGER: &str = "
set client_min_messages = warning;
create schema if not exists supabase_migrations;
create table if not exists supabase_migrations.schema_migrations (
    version text primary key,
    statements text[],
    name text
);
reset client_min_messages;
";

/// Schemas a reset leaves alone: postgres's own, and the ones this
/// server puts there and depends on.
const MANAGED: &[&str] = &[
    "auth",
    "extensions",
    "graphql",
    "graphql_public",
    "information_schema",
    "net",
    "pgbouncer",
    "pgsodium",
    "realtime",
    "storage",
    "supabase_functions",
    "supabase_migrations",
    "vault",
    // Ours. The event triggers behind catalog invalidation hang off
    // it, and a cascade through them leaves the api reading a stale
    // schema until somebody restarts the server.
    "zou",
];

/// The three roles the api answers as. A database the server has
/// never served from does not have them yet, and the grants below are
/// about them, so their absence decides whether the grants run.
const API_ROLES: &[&str] = &["anon", "authenticated", "service_role"];

/// The grants the server's own bootstrap gives the public schema. A
/// reset drops that schema, so the same grants go back on afterwards,
/// otherwise the api roles would find a schema they cannot see.
const PUBLIC_GRANTS: &str = "
grant usage on schema public to anon, authenticated, service_role;
grant all on all tables in schema public to anon, authenticated, service_role;
grant all on all sequences in schema public to anon, authenticated, service_role;
grant all on all functions in schema public to anon, authenticated, service_role;
alter default privileges in schema public
    grant all on tables to anon, authenticated, service_role;
alter default privileges in schema public
    grant all on sequences to anon, authenticated, service_role;
alter default privileges in schema public
    grant all on functions to anon, authenticated, service_role;
";

struct Args {
    url: Option<String>,
    config: Option<PathBuf>,
    dry_run: bool,
    force: bool,
}

fn parse(argv: &[String]) -> Result<(String, Args), String> {
    let [verb, rest @ ..] = argv else {
        return Err(DB_USAGE.into());
    };
    if verb != "push" && verb != "reset" {
        return Err(format!("no such db command {verb:?}\n{DB_USAGE}"));
    }
    let mut args = Args {
        url: None,
        config: None,
        dry_run: false,
        force: false,
    };
    let mut it = rest.iter();
    while let Some(arg) = it.next() {
        let mut need = |flag: &str| {
            it.next()
                .cloned()
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        match arg.as_str() {
            "--db-url" => args.url = Some(need("--db-url")?),
            "--config" => args.config = Some(PathBuf::from(need("--config")?)),
            "--dry-run" if verb == "push" => args.dry_run = true,
            "--force" if verb == "reset" => args.force = true,
            other => return Err(format!("unexpected argument {other:?}\n{DB_USAGE}")),
        }
    }
    Ok((verb.clone(), args))
}

/// The database to work on: what the command line says, then the
/// environment, then the port the project's own file names, then the
/// port `zou dev` uses when nobody has said anything.
fn url(args: &Args, project: Option<&Project>) -> String {
    if let Some(url) = &args.url {
        return url.clone();
    }
    for name in ["ZOU_DB_URL", "DATABASE_URL"] {
        if let Ok(v) = std::env::var(name)
            && !v.is_empty()
        {
            return v;
        }
    }
    config::local_db_url(project.and_then(|p| p.db).unwrap_or(5432))
}

/// A database somewhere else is not one a reset should be able to
/// walk into by accident, so it has to be said out loud.
fn is_local(url: &str) -> bool {
    let host = url
        .rsplit_once('@')
        .map(|(_, rest)| rest)
        .unwrap_or(url)
        .trim_start_matches("postgresql://")
        .trim_start_matches("postgres://");
    let host = host.split(['/', ':', '?']).next().unwrap_or("");
    host.is_empty() || host == "localhost" || host == "127.0.0.1" || host == "::1"
}

#[derive(Debug, PartialEq)]
struct Migration {
    version: String,
    name: String,
    path: PathBuf,
}

/// Every migration in the directory, in the order the names put them
/// in, which is the order the timestamps put them in.
fn migrations(dir: &Path) -> Result<Vec<Migration>, String> {
    let mut entries: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(read) => read
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "sql"))
            .collect(),
        // A project with no migrations yet is not a broken project.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("read {}: {e}", dir.display())),
    };
    entries.sort();
    let mut out = Vec::new();
    for path in entries {
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| format!("{} is not a name this can read", path.display()))?;
        let (version, name) = stem.split_once('_').unwrap_or((stem, ""));
        if version.is_empty() || !version.chars().all(|c| c.is_ascii_digit()) {
            return Err(format!(
                "{} does not start with a version, migrations are named <timestamp>_<name>.sql",
                path.display()
            ));
        }
        out.push(Migration {
            version: version.to_string(),
            name: name.to_string(),
            path,
        });
    }
    Ok(out)
}

async fn connect(url: &str) -> Result<Client, String> {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .map_err(|e| format!("cannot connect: {}", why(&e)))?;
    // The connection drives the socket and ends when the client is
    // dropped, which is when the command is over.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            log::debug!("connection ended: {e}");
        }
    });
    Ok(client)
}

async fn applied(client: &Client) -> Result<BTreeSet<String>, String> {
    client
        .batch_execute(LEDGER)
        .await
        .map_err(|e| format!("cannot make the migration ledger: {}", why(&e)))?;
    let rows = client
        .query(
            "select version from supabase_migrations.schema_migrations",
            &[],
        )
        .await
        .map_err(|e| format!("cannot read the migration ledger: {}", why(&e)))?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// One migration, as one implicit transaction, with the ledger row
/// written inside it, so a file that fails leaves neither its objects
/// nor its version behind.
///
/// The `statements` column gets the file whole rather than cut into
/// statements the way the CLI cuts it. Nothing reads it back to
/// replay, and a splitter that got a dollar quoted function body
/// wrong would write down something that was never run.
async fn apply(client: &Client, m: &Migration) -> Result<(), String> {
    let sql =
        std::fs::read_to_string(&m.path).map_err(|e| format!("read {}: {e}", m.path.display()))?;
    let statements: Vec<&str> = vec![&sql];
    // The semicolon on its own line is for the file that does not end
    // in one, which postgres would otherwise read as the beginning of
    // the insert below. An extra one after a statement that has its own
    // is an empty statement, which costs nothing.
    let batch = format!(
        "begin;\n{sql}\n;\n\
         insert into supabase_migrations.schema_migrations (version, name, statements) \
         values ('{}', '{}', $zou${}$zou$::text[]);\ncommit;",
        m.version.replace('\'', "''"),
        m.name.replace('\'', "''"),
        pg_array(&statements),
    );
    client
        .batch_execute(&batch)
        .await
        .map_err(|e| format!("{}: {}", m.path.display(), why(&e)))
}

/// A text[] literal. The dollar quoting around it in the statement
/// above is what keeps the sql itself out of harm's way, so all this
/// has to do is the array's own escaping.
fn pg_array(items: &[&str]) -> String {
    let mut out = String::from("{");
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        for c in item.chars() {
            if c == '"' || c == '\\' {
                out.push('\\');
            }
            out.push(c);
        }
        out.push('"');
    }
    out.push('}');
    out
}

/// What the server actually said. A postgres error prints as "db
/// error" on its own and keeps the sentence a person needs one level
/// down, so the chain is walked and joined.
fn why(e: &tokio_postgres::Error) -> String {
    let mut out = e.to_string();
    let mut source = std::error::Error::source(e);
    while let Some(e) = source {
        out.push_str(": ");
        out.push_str(&e.to_string());
        source = e.source();
    }
    out
}

async fn roles_exist(client: &Client) -> Result<bool, String> {
    let rows = client
        .query(
            "select 1 from pg_roles where rolname = any($1)",
            &[&API_ROLES],
        )
        .await
        .map_err(|e| format!("cannot read the role list: {}", why(&e)))?;
    Ok(rows.len() == API_ROLES.len())
}

/// The project's own schemas, which is everything that is neither
/// postgres's nor this server's.
async fn own_schemas(client: &Client) -> Result<Vec<String>, String> {
    let rows = client
        .query(
            "select nspname from pg_namespace \
             where nspname not like 'pg\\_%' and nspname <> all($1) order by nspname",
            &[&MANAGED],
        )
        .await
        .map_err(|e| format!("cannot read the schema list: {}", why(&e)))?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

async fn push(client: &Client, dir: &Path, dry_run: bool) -> Result<(), String> {
    let all = migrations(dir)?;
    if all.is_empty() {
        println!("no migrations in {}", dir.display());
        return Ok(());
    }
    let done = applied(client).await?;
    let todo: Vec<&Migration> = all.iter().filter(|m| !done.contains(&m.version)).collect();
    if todo.is_empty() {
        println!("nothing to apply, all {} are already there", all.len());
        return Ok(());
    }
    for m in &todo {
        if dry_run {
            println!("would apply {}", m.path.display());
            continue;
        }
        apply(client, m).await?;
        println!("applied {}", m.path.display());
    }
    if !dry_run {
        println!("{} applied, {} were already there", todo.len(), done.len());
    }
    Ok(())
}

async fn reset(client: &Client, project: Option<&Project>, dir: &Path) -> Result<(), String> {
    for schema in own_schemas(client).await? {
        // A cascade names every table it takes with it, which on a
        // schema being thrown away on purpose is a page of notices
        // saying what the line below says in one.
        client
            .batch_execute(&format!(
                "set client_min_messages = warning;\
                 drop schema {} cascade;\
                 reset client_min_messages;",
                quoted(&schema)
            ))
            .await
            .map_err(|e| format!("cannot drop schema {schema}: {}", why(&e)))?;
        println!("dropped schema {schema}");
    }
    client
        .batch_execute("create schema public")
        .await
        .map_err(|e| format!("cannot put the public schema back: {}", why(&e)))?;
    // On a database the server has served from, the api roles are
    // there and the grants that went with the dropped schema have to
    // go back on. On one it has not, there is nothing to grant to yet,
    // and its own bootstrap will do this sweep when it first answers.
    if roles_exist(client).await? {
        client
            .batch_execute(PUBLIC_GRANTS)
            .await
            .map_err(|e| format!("cannot grant the public schema back: {}", why(&e)))?;
    } else {
        println!("the api roles are not there yet, the server grants public when it starts");
    }
    // The ledger is emptied rather than dropped, because a database
    // nobody has pushed to yet does not have one to drop, and the
    // create is the same one push would do a moment later.
    client
        .batch_execute(&format!(
            "{LEDGER}\ntruncate supabase_migrations.schema_migrations;"
        ))
        .await
        .map_err(|e| format!("cannot clear the migration ledger: {}", why(&e)))?;
    push(client, dir, false).await?;
    seed(client, project).await
}

/// `seed.sql`, or whatever `[db.seed] sql_paths` names, run after the
/// migrations the way the CLI runs it.
async fn seed(client: &Client, project: Option<&Project>) -> Result<(), String> {
    let Some(project) = project else {
        return Ok(());
    };
    for path in &project.seed {
        // The CLI's own default is written `./seed.sql`, and joining
        // that verbatim gives a path with a dot in the middle of it,
        // which is the same file spelled worse.
        let path = project.dir().join(path.trim_start_matches("./"));
        let sql = match std::fs::read_to_string(&path) {
            Ok(sql) => sql,
            // A project that never wrote a seed still says
            // `./seed.sql` in its config, because the CLI's default
            // says it. Missing is not an error there and is not here.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(format!("read {}: {e}", path.display())),
        };
        client
            .batch_execute(&sql)
            .await
            .map_err(|e| format!("{}: {}", path.display(), why(&e)))?;
        println!("seeded from {}", path.display());
    }
    Ok(())
}

pub fn run(argv: &[String]) -> Result<(), String> {
    let (verb, args) = parse(argv)?;
    let project = config::locate(args.config.as_deref())?;
    let dir = project
        .as_ref()
        .map(Project::migrations)
        .unwrap_or_else(|| PathBuf::from("supabase/migrations"));
    let url = url(&args, project.as_ref());
    if verb == "reset" && !args.force && !is_local(&url) {
        return Err(format!(
            "{url} is not on this machine, and a reset drops every schema the project owns, pass --force if that is what you meant"
        ));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("cannot start a runtime: {e}"))?;
    runtime.block_on(async {
        let client = connect(&url).await?;
        match verb.as_str() {
            "push" => push(&client, &dir, args.dry_run).await,
            _ => reset(&client, project.as_ref(), &dir).await,
        }
    })
}

/// `zou migration new <name>`: an empty file, named the way the CLI
/// names one, in the directory the project keeps them in.
pub fn migration(argv: &[String]) -> Result<(), String> {
    let mut name = None;
    let mut config = None;
    let mut it = argv.iter();
    match it.next().map(String::as_str) {
        Some("new") => {}
        Some(other) => {
            return Err(format!(
                "no such migration command {other:?}\n{MIGRATION_USAGE}"
            ));
        }
        None => return Err(MIGRATION_USAGE.into()),
    }
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--config" => {
                config = Some(PathBuf::from(
                    it.next().ok_or("--config needs a path")?.clone(),
                ));
            }
            other if name.is_none() && !other.starts_with('-') => name = Some(other.to_string()),
            other => return Err(format!("unexpected argument {other:?}\n{MIGRATION_USAGE}")),
        }
    }
    let name = name.ok_or(MIGRATION_USAGE)?;
    let project = config::locate(config.as_deref())?;
    let dir = project
        .as_ref()
        .map(Project::migrations)
        .unwrap_or_else(|| PathBuf::from("supabase/migrations"));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let path = dir.join(format!("{}_{}.sql", stamp(now), slug(&name)));
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    if path.exists() {
        return Err(format!("{} is already there", path.display()));
    }
    std::fs::write(&path, "").map_err(|e| format!("write {}: {e}", path.display()))?;
    println!("{}", path.display());
    Ok(())
}

/// What the CLI puts in front of a migration name: the utc second, no
/// separators. The civil calendar arithmetic is Howard Hinnant's, the
/// same as the copy in zou-store's s3 signing.
fn stamp(unix: i64) -> String {
    let days = unix.div_euclid(86_400);
    let secs = unix.rem_euclid(86_400);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = y + i64::from(m <= 2);
    format!(
        "{y:04}{m:02}{d:02}{:02}{:02}{:02}",
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    )
}

/// A name the way a file wants it, which is also how the CLI writes
/// one: lowercase, and anything that is not a letter or a digit
/// becomes an underscore.
fn slug(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            out.extend(c.to_lowercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    out.trim_matches('_').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn the_verbs_and_their_flags_come_apart() {
        let (verb, args) =
            parse(&argv(&["push", "--dry-run", "--db-url", "postgres:///x"])).unwrap();
        assert_eq!(verb, "push");
        assert!(args.dry_run);
        assert_eq!(args.url.as_deref(), Some("postgres:///x"));
        let (verb, args) = parse(&argv(&["reset", "--force"])).unwrap();
        assert_eq!(verb, "reset");
        assert!(args.force);
        assert!(parse(&argv(&["push", "--force"])).is_err(), "not this verb");
        assert!(parse(&argv(&["reset", "--dry-run"])).is_err());
        assert!(parse(&argv(&["diff"])).is_err(), "not written yet");
        assert!(parse(&argv(&[])).is_err());
    }

    #[test]
    fn a_database_somewhere_else_is_not_local() {
        assert!(is_local(
            "postgresql://postgres:postgres@127.0.0.1:5432/postgres"
        ));
        assert!(is_local("postgres://localhost/postgres"));
        assert!(is_local("postgresql:///postgres"));
        assert!(!is_local(
            "postgresql://user:pw@db.example.com:5432/postgres"
        ));
        assert!(
            !is_local("postgresql://db.example.com/postgres?host=127.0.0.1"),
            "and a loopback in a query parameter is not the host"
        );
    }

    #[test]
    fn migrations_come_back_in_the_order_their_names_put_them_in() {
        let dir = tempfile::tempdir().unwrap();
        for name in [
            "20240102030405_second.sql",
            "20230101000000_first.sql",
            "notes.txt",
        ] {
            std::fs::write(dir.path().join(name), "select 1").unwrap();
        }
        let found = migrations(dir.path()).unwrap();
        assert_eq!(found.len(), 2, "the txt is not a migration");
        assert_eq!(found[0].version, "20230101000000");
        assert_eq!(found[0].name, "first");
        assert_eq!(found[1].name, "second");
    }

    #[test]
    fn a_migration_with_no_version_is_refused_rather_than_applied_last() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("fix_the_thing.sql"), "select 1").unwrap();
        let e = migrations(dir.path()).unwrap_err();
        assert!(e.contains("<timestamp>_<name>.sql"), "{e}");
    }

    #[test]
    fn nowhere_to_read_is_no_migrations_and_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(migrations(&dir.path().join("nope")).unwrap(), []);
    }

    #[test]
    fn the_stamp_is_the_utc_second_the_way_the_cli_writes_it() {
        assert_eq!(stamp(0), "19700101000000");
        assert_eq!(stamp(1_700_000_000), "20231114221320");
        assert_eq!(stamp(1_709_164_800), "20240229000000", "a leap day");
    }

    #[test]
    fn a_name_becomes_a_file_name() {
        assert_eq!(slug("Add users table"), "add_users_table");
        assert_eq!(slug("add-users!!"), "add_users");
        assert_eq!(slug("  "), "");
    }

    #[test]
    fn the_schemas_this_server_lives_in_are_not_the_projects_to_drop() {
        for ours in ["auth", "storage", "zou", "supabase_migrations"] {
            assert!(MANAGED.contains(&ours), "{ours} is not the project's");
        }
        assert!(!MANAGED.contains(&"public"), "public is, and comes back");
        let mut sorted = MANAGED.to_vec();
        sorted.sort_unstable();
        assert_eq!(sorted, MANAGED, "kept in order so a reader can find one");
    }

    #[test]
    fn an_array_literal_survives_the_sql_inside_it() {
        assert_eq!(pg_array(&["select 1"]), "{\"select 1\"}");
        assert_eq!(
            pg_array(&["a \"quoted\" \\ thing"]),
            "{\"a \\\"quoted\\\" \\\\ thing\"}"
        );
    }

    #[test]
    fn a_new_migration_lands_where_the_project_keeps_them() {
        let dir = tempfile::tempdir().unwrap();
        let supabase = dir.path().join("supabase");
        std::fs::create_dir_all(&supabase).unwrap();
        std::fs::write(supabase.join("config.toml"), "project_id = \"x\"\n").unwrap();
        migration(&argv(&[
            "new",
            "Add users table",
            "--config",
            supabase.join("config.toml").to_str().unwrap(),
        ]))
        .unwrap();
        let written: Vec<String> = std::fs::read_dir(supabase.join("migrations"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(written.len(), 1);
        assert!(written[0].ends_with("_add_users_table.sql"), "{written:?}");
        assert!(
            written[0].len() == "20240102030405_add_users_table.sql".len(),
            "with a full timestamp in front, {written:?}"
        );
    }
}
