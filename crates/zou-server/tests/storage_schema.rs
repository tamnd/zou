//! The storage schema zou creates is the schema storage-api's own
//! migrations leave behind, and a storage schema zou did not create is
//! never touched.
//!
//! Same argument as the auth schema test next door and the same
//! machinery. The fixture comes out of
//! scripts/storage-schema-refresh.sh, which replays supabase/storage's
//! sixty migrations at a pinned tag against a scratch database and
//! records every column, constraint, index, trigger, enum and comment
//! as one sorted line. So "field compatible with storage-api" is a text
//! comparison against what storage-api itself produces rather than a
//! claim in a readme.
//!
//! The pinned tag is the one the local supabase stack runs, which is
//! what makes this fixture and the storage recording in the conformance
//! repository two views of the same server.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.

use tokio_postgres::NoTls;
use zou_server::sql::{Pool, Session};

/// The same query the refresh script ran to record the fixture. Shared
/// as a file rather than copied, because two drifting copies of it
/// would compare two different things and still pass.
const FINGERPRINT: &str = include_str!("../../../scripts/schema-fingerprint.sql");

/// What replaying storage-api's migrations produces.
const STORAGE_API: &str = include_str!("fixtures/storage-api-fingerprint.txt");

/// The tag the fixture was taken at, for the failure message. Bumping
/// it without rerunning the refresh script is the mistake this names.
const STORAGE_TAG: &str = "v1.67.20";

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

/// The query reads the schema it is fingerprinting out of a setting, so
/// the setting has to be made in the same session first.
async fn fingerprint(sess: &Session) -> Vec<String> {
    sess.execute("set zou.fingerprint_schema = 'storage'", &[])
        .await
        .expect("name the schema");
    sess.query(FINGERPRINT, &[])
        .await
        .expect("fingerprint query")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect()
}

/// A connection outside the pool, for the statements that cannot run
/// inside one: create database and drop database both refuse the
/// extended query protocol, and both have to run before there is
/// anything to pool.
async fn raw(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect to {dsn}: {e}"));
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// Point a keyword and value dsn at another database. The last dbname
/// wins in that form, which is why appending is enough. A url form dsn
/// gets None and the caller skips, rather than a mangled string that
/// would connect somewhere unintended.
fn with_dbname(dsn: &str, db: &str) -> Option<String> {
    if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        return None;
    }
    Some(format!("{dsn} dbname={db}"))
}

#[tokio::test]
async fn the_schema_zou_creates_is_the_schema_storage_api_creates() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 2).expect("dsn parses");
    // Any checkout runs the bootstrap, this one included.
    let sess = pool.unscoped().await.expect("unscoped");
    let ours = fingerprint(&sess).await;
    sess.commit().await.expect("finish");

    let theirs: Vec<String> = STORAGE_API.lines().map(str::to_string).collect();
    assert!(
        theirs.len() > 100,
        "the fixture looks truncated at {} lines",
        theirs.len()
    );

    let missing: Vec<&String> = theirs.iter().filter(|l| !ours.contains(l)).collect();
    let extra: Vec<&String> = ours.iter().filter(|l| !theirs.contains(l)).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "the storage schema drifted from storage-api {STORAGE_TAG}\nmissing {} lines:\n{}\nextra {} lines:\n{}",
        missing.len(),
        missing
            .iter()
            .take(20)
            .map(|l| format!("  - {l}"))
            .collect::<Vec<_>>()
            .join("\n"),
        extra.len(),
        extra
            .iter()
            .take(20)
            .map(|l| format!("  + {l}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

/// The rows that tell a real storage-api it has nothing to run. Sixty
/// one of them, because the migration runner counts its own first
/// migration as well as the sixty in the tenant directory, and a
/// storage-api that found sixty would replay the one it thinks is
/// missing over a schema that already has it.
#[tokio::test]
async fn the_migration_rows_say_the_schema_is_current() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 2).expect("dsn parses");
    let sess = pool.unscoped().await.expect("unscoped");
    let rows = sess
        .query(
            "select id, name, hash from storage.migrations order by id",
            &[],
        )
        .await
        .expect("migration rows");
    let first: (i32, String, String) = (rows[0].get(0), rows[0].get(1), rows[0].get(2));
    let last: (i32, String) = (
        rows[rows.len() - 1].get(0),
        rows[rows.len() - 1].get::<_, String>(1),
    );
    sess.commit().await.expect("finish");

    assert_eq!(rows.len(), 61);
    assert_eq!(
        first,
        (
            0,
            "create-migrations-table".to_string(),
            "e18db593bcde2aca2a408c4d1100f6abba2195df".to_string()
        )
    );
    assert_eq!(last, (60, "optimize-existing-functions-again".to_string()));
}

/// The guard that made the conformance fixture need an escape hatch, so
/// it is worth a test that it is really there rather than a line in a
/// dump that happened to be copied across.
#[tokio::test]
async fn a_delete_that_did_not_come_through_the_api_is_refused() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 2).expect("dsn parses");

    let sess = pool.unscoped().await.expect("unscoped");
    let refused = sess.execute("delete from storage.buckets", &[]).await;
    let code = refused
        .err()
        .and_then(|e| e.code().map(|c| c.code().to_string()));
    sess.commit().await.expect("finish");
    assert_eq!(code.as_deref(), Some("42501"));

    // And the way storage-api itself gets past it. Not set local: an
    // unscoped session has no transaction for a local setting to be
    // local to, and postgres answers that with a warning and no
    // setting rather than an error. What puts it back is the pool
    // scrubbing every connection on its way home.
    let sess = pool.unscoped().await.expect("unscoped");
    sess.execute("set storage.allow_delete_query = 'true'", &[])
        .await
        .expect("the escape hatch");
    // The objects first, because they point at the buckets. Another
    // suite in this crate leaves objects behind in the same database on
    // purpose, and a foreign key failure here would read as the hatch
    // not working when it is the order that is wrong.
    sess.execute("delete from storage.objects", &[])
        .await
        .expect("clear the objects with the hatch open");
    sess.execute("delete from storage.buckets", &[])
        .await
        .expect("delete with the hatch open");
    sess.commit().await.expect("finish");

    // The scrub really happened, so the next request off this pool is
    // not quietly allowed to delete.
    let sess = pool.unscoped().await.expect("unscoped");
    let refused = sess.execute("delete from storage.buckets", &[]).await;
    let code = refused
        .err()
        .and_then(|e| e.code().map(|c| c.code().to_string()));
    sess.commit().await.expect("finish");
    assert_eq!(code.as_deref(), Some("42501"));
}

#[tokio::test]
async fn a_storage_schema_zou_did_not_create_is_left_alone() {
    let Some(dsn) = dsn() else { return };
    let Some(scratch) = with_dbname(&dsn, "zou_storage_squatter") else {
        eprintln!("skipping: needs a keyword and value dsn");
        return;
    };

    let admin = raw(&dsn).await;
    admin
        .batch_execute("drop database if exists zou_storage_squatter with (force)")
        .await
        .expect("drop any leftover");
    admin
        .batch_execute("create database zou_storage_squatter")
        .await
        .expect("create scratch");

    // Somebody else's storage.objects, standing in for a real
    // storage-api's. It has nothing in common with ours beyond the
    // name, so if zou were to apply its ddl over the top the result
    // would be visible.
    {
        let squatter = raw(&scratch).await;
        squatter
            .batch_execute(
                "create schema storage;
                 create table storage.objects (id int primary key, squatter text)",
            )
            .await
            .expect("plant the squatter");
    }

    let squatted = Pool::new(&scratch, 2).expect("scratch dsn parses");
    let sess = squatted.unscoped().await.expect("unscoped on scratch");
    let tables: Vec<String> = sess
        .query(
            "select table_name from information_schema.tables
             where table_schema = 'storage' order by table_name",
            &[],
        )
        .await
        .expect("tables")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    // The bootstrap itself still ran, it is the storage ddl that stood
    // down. Without this the test would also pass if zou had failed to
    // start at all.
    let uid: bool = sess
        .query("select to_regprocedure('auth.uid()') is not null", &[])
        .await
        .expect("uid")[0]
        .get(0);
    sess.commit().await.expect("finish");
    // The pool's connections have to go before the database can, which
    // is what force covers for anything still winding down.
    drop(squatted);
    admin
        .batch_execute("drop database if exists zou_storage_squatter with (force)")
        .await
        .expect("drop scratch");

    assert_eq!(tables, vec!["objects".to_string()]);
    assert!(uid, "the bootstrap did not run");
}
