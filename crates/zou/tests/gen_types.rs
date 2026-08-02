//! `zou gen types typescript` against a live postgres.
//!
//! The file this writes is checked into other people's repositories
//! and shows up in their diffs, so the test is a byte comparison
//! against a file `supabase gen types typescript` produced from the
//! same schema. Anything looser would let a stray line break through,
//! and a stray line break is a diff on every project that upgrades.
//!
//! The fixture is one instance of every shape that has ever been
//! awkward: identity columns, an updatable view and one that is not,
//! a materialized view, computed columns, overloads PostgREST can and
//! cannot resolve, functions PostgREST cannot call at all, types from
//! a schema nobody asked to generate, the same names in two schemas,
//! names that are not identifiers, and an enum long enough to wrap.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.

use std::process::Command;

use tokio_postgres::NoTls;

const FIXTURE: &str = include_str!("fixtures/gen-types.sql");
const EXPECTED: &str = include_str!("fixtures/gen-types.ts");

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

/// The same server, a different database. The generator reads whole
/// schemas, so it needs one nobody else is writing to.
fn with_database(dsn: &str, database: &str) -> String {
    let (head, query) = match dsn.split_once('?') {
        Some((head, query)) => (head, Some(query)),
        None => (dsn, None),
    };
    let after_scheme = head.find("//").map(|at| at + 2).unwrap_or(0);
    let base = match head[after_scheme..].find('/') {
        Some(at) => &head[..after_scheme + at],
        None => head,
    };
    match query {
        Some(query) => format!("{base}/{database}?{query}"),
        None => format!("{base}/{database}"),
    }
}

/// A database of its own with the fixture in it, and the dsn that
/// reaches it. template0 rather than template1, so that whatever the
/// server's template picked up over the years is not in the generated
/// file.
struct Fixture {
    dsn: String,
    database: String,
    admin: tokio_postgres::Client,
}

async fn fixture(name: &str) -> Fixture {
    let dsn = dsn().expect("dsn");
    let database = format!("zou_gen_{name}");
    let (admin, connection) = tokio_postgres::connect(&dsn, NoTls).await.expect("connect");
    tokio::spawn(connection);
    // Force, because a run that failed halfway may have left something
    // connected, and a test that cannot start is worse than one that
    // takes the old database away.
    admin
        .batch_execute(&format!("drop database if exists {database} with (force)"))
        .await
        .expect("drop");
    admin
        .batch_execute(&format!("create database {database} template template0"))
        .await
        .expect("create");

    let child = with_database(&dsn, &database);
    let (client, connection) = tokio_postgres::connect(&child, NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    client.batch_execute(FIXTURE).await.expect("fixture");
    Fixture {
        dsn: child,
        database,
        admin,
    }
}

impl Fixture {
    async fn done(&self) {
        self.admin
            .batch_execute(&format!(
                "drop database if exists {} with (force)",
                self.database
            ))
            .await
            .expect("drop");
    }
}

/// The whole file, not a substring: a line break in the wrong place is
/// the failure this suite is here to catch. Reported a line at a time,
/// since a diff of six hundred lines is not a message anyone reads.
fn assert_same(written: &str) {
    if written == EXPECTED {
        return;
    }
    let mut expected = EXPECTED.lines();
    for (n, line) in written.lines().enumerate() {
        assert_eq!(
            line,
            expected.next().unwrap_or("<end of file>"),
            "line {}",
            n + 1
        );
    }
    panic!(
        "generated {} lines, expected {}",
        written.lines().count(),
        EXPECTED.lines().count()
    );
}

fn generate(dsn: &str, schemas: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_zou"))
        .args([
            "gen",
            "types",
            "typescript",
            "--db-url",
            dsn,
            "--schema",
            schemas,
        ])
        .output()
        .expect("run zou")
}

#[tokio::test]
async fn the_generated_file_is_the_one_supabase_generates() {
    if dsn().is_none() {
        return;
    }
    let fixture = fixture("types").await;
    let out = generate(&fixture.dsn, "public,shop");
    fixture.done().await;
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    let written = String::from_utf8(out.stdout).expect("utf8");
    assert!(out.status.success(), "{stderr}");
    assert_same(&written);
}

/// A generator that writes to the database it reads is a generator
/// nobody can point at production. A read only database makes any
/// write an error rather than a thing to notice later.
#[tokio::test]
async fn generating_types_writes_nothing_to_the_database() {
    if dsn().is_none() {
        return;
    }
    let fixture = fixture("readonly").await;
    fixture
        .admin
        .batch_execute(&format!(
            "alter database {} set default_transaction_read_only = on",
            fixture.database
        ))
        .await
        .expect("read only");

    let out = generate(&fixture.dsn, "public,shop");
    fixture.done().await;
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(out.status.success(), "{stderr}");
    assert_same(&String::from_utf8(out.stdout).expect("utf8"));
}

/// One schema is the default, and the file it writes has one schema in
/// it, which is the shape most projects check in.
#[tokio::test]
async fn a_schema_nobody_asked_for_is_not_in_the_file() {
    if dsn().is_none() {
        return;
    }
    let fixture = fixture("one").await;
    let out = Command::new(env!("CARGO_BIN_EXE_zou"))
        .args(["gen", "types", "typescript", "--db-url", &fixture.dsn])
        .output()
        .expect("run zou");
    fixture.done().await;
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(out.status.success(), "{stderr}");
    let written = String::from_utf8(out.stdout).expect("utf8");
    assert!(written.contains("  public: {"), "{written}");
    assert!(!written.contains("  shop: {"), "{written}");
    assert!(!written.contains("  hidden: {"), "{written}");
    // A type from a schema that is not in the file is still worth
    // something, so an enum keeps its variants.
    assert!(
        written.contains("tier: \"bronze\" | \"silver\" | \"gold\""),
        "{written}"
    );
}

#[tokio::test]
async fn the_file_can_be_written_where_the_project_keeps_it() {
    if dsn().is_none() {
        return;
    }
    let fixture = fixture("output").await;
    let path = std::env::temp_dir().join(format!("zou-gen-{}.ts", std::process::id()));
    let out = Command::new(env!("CARGO_BIN_EXE_zou"))
        .args([
            "gen",
            "types",
            "typescript",
            "--db-url",
            &fixture.dsn,
            "--schema",
            "public,shop",
            "--output",
            path.to_str().expect("path"),
        ])
        .output()
        .expect("run zou");
    fixture.done().await;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.stdout.is_empty(),
        "nothing goes to stdout when a path was given"
    );
    let written = std::fs::read_to_string(&path).expect("read");
    let _ = std::fs::remove_file(&path);
    assert_same(&written);
}

/// The url can come from the environment, which is how a Makefile or a
/// CI job usually has it.
#[tokio::test]
async fn the_url_can_come_from_the_environment() {
    if dsn().is_none() {
        return;
    }
    let fixture = fixture("env").await;
    let out = Command::new(env!("CARGO_BIN_EXE_zou"))
        .args(["gen", "types", "typescript", "--schema", "public,shop"])
        .env("ZOU_DB_URL", &fixture.dsn)
        .output()
        .expect("run zou");
    fixture.done().await;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_same(&String::from_utf8(out.stdout).expect("utf8"));
}

/// A variable that is set to nothing is a variable somebody meant to
/// set and did not, so it reads as absent rather than as a url of no
/// characters.
#[test]
fn an_empty_variable_is_not_a_database_url() {
    let out = Command::new(env!("CARGO_BIN_EXE_zou"))
        .args(["gen", "types", "typescript"])
        .env("ZOU_DB_URL", "")
        .env("DATABASE_URL", "")
        .output()
        .expect("run zou");
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("no database to read"), "{stderr}");
}

#[test]
fn a_database_with_no_server_behind_it_is_an_error_and_not_a_file() {
    let out = generate("postgresql://127.0.0.1:1/nothing", "public");
    assert!(!out.status.success());
    assert!(out.stdout.is_empty(), "no half written file on failure");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("cannot connect"), "{stderr}");
}
