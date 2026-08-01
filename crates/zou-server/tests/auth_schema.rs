//! The auth schema zou creates is the schema GoTrue's own migrations
//! leave behind, and an auth schema zou did not create is never
//! touched.
//!
//! The fixture this compares against is not written by hand. It comes
//! out of scripts/auth-schema-refresh.sh, which replays supabase/auth's
//! seventy migrations at a pinned tag against a scratch database and
//! records every column, constraint, index, enum, comment and rls flag
//! as one sorted line. So "field compatible with GoTrue" is not a claim
//! in a readme here, it is a text comparison against what GoTrue itself
//! produces.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.

use tokio_postgres::NoTls;
use zou_server::sql::{Pool, Session};

/// The same query the refresh script ran to record the fixture. Shared
/// as a file rather than copied, because two drifting copies of it
/// would compare two different things and still pass.
const FINGERPRINT: &str = include_str!("../../../scripts/auth-schema-fingerprint.sql");

/// What replaying GoTrue's migrations produces.
const GOTRUE: &str = include_str!("fixtures/gotrue-auth-fingerprint.txt");

/// The tag the fixture was taken at, for the failure message. Bumping
/// it without rerunning the refresh script is the mistake this names.
const AUTH_TAG: &str = "v2.194.0";

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

async fn fingerprint(sess: &Session) -> Vec<String> {
    sess.query(FINGERPRINT, &[])
        .await
        .expect("fingerprint query")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect()
}

/// A connection outside the pool, so a test can run statements the
/// pool's own api does not reach. create database and drop database
/// are the reason: both refuse the extended query protocol that
/// Session::execute uses, and both have to run before there is
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
async fn the_schema_zou_creates_is_the_schema_gotrue_creates() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 2).expect("dsn parses");
    // Any checkout runs the bootstrap, this one included.
    let sess = pool.unscoped().await.expect("unscoped");
    let ours = fingerprint(&sess).await;
    sess.commit().await.expect("finish");

    let theirs: Vec<String> = GOTRUE.lines().map(str::to_string).collect();
    assert!(
        theirs.len() > 500,
        "the fixture looks truncated at {} lines",
        theirs.len()
    );

    let missing: Vec<&String> = theirs.iter().filter(|l| !ours.contains(l)).collect();
    let extra: Vec<&String> = ours.iter().filter(|l| !theirs.contains(l)).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "the auth schema drifted from GoTrue {AUTH_TAG}\nmissing {} lines:\n{}\nextra {} lines:\n{}",
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

#[tokio::test]
async fn a_second_bootstrap_changes_nothing() {
    let Some(dsn) = dsn() else { return };
    let first = Pool::new(&dsn, 2).expect("dsn parses");
    let sess = first.unscoped().await.expect("unscoped");
    let before = fingerprint(&sess).await;
    sess.commit().await.expect("finish");

    // A fresh pool has its own OnceCell, so this really does run the
    // bootstrap and the auth schema check a second time rather than
    // remembering that it already did.
    let second = Pool::new(&dsn, 2).expect("dsn parses");
    let sess = second.unscoped().await.expect("unscoped");
    let after = fingerprint(&sess).await;
    sess.commit().await.expect("finish");

    assert_eq!(before, after);
}

#[tokio::test]
async fn an_auth_schema_zou_did_not_create_is_left_alone() {
    let Some(dsn) = dsn() else { return };
    let Some(scratch) = with_dbname(&dsn, "zou_auth_squatter") else {
        eprintln!("skipping: needs a keyword and value dsn");
        return;
    };

    let admin = raw(&dsn).await;
    admin
        .batch_execute("drop database if exists zou_auth_squatter with (force)")
        .await
        .expect("drop any leftover");
    admin
        .batch_execute("create database zou_auth_squatter")
        .await
        .expect("create scratch");

    // Somebody else's auth.users, standing in for a real GoTrue's. It
    // has nothing in common with ours beyond the name, so if zou were
    // to apply its ddl over the top the result would be visible.
    {
        let squatter = raw(&scratch).await;
        squatter
            .batch_execute(
                "create schema auth;
                 create table auth.users (id int primary key, squatter text)",
            )
            .await
            .expect("plant the squatter");
    }

    let squatted = Pool::new(&scratch, 2).expect("scratch dsn parses");
    let sess = squatted.unscoped().await.expect("unscoped on scratch");
    let tables: Vec<String> = sess
        .query(
            "select table_name from information_schema.tables
             where table_schema = 'auth' order by table_name",
            &[],
        )
        .await
        .expect("tables")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    let columns: Vec<String> = sess
        .query(
            "select column_name from information_schema.columns
             where table_schema = 'auth' and table_name = 'users'
             order by column_name",
            &[],
        )
        .await
        .expect("columns")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    // The bootstrap itself still ran, it is the auth ddl that stood
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
        .batch_execute("drop database if exists zou_auth_squatter with (force)")
        .await
        .expect("drop scratch");

    assert_eq!(tables, vec!["users".to_string()]);
    assert_eq!(columns, vec!["id".to_string(), "squatter".to_string()]);
    assert!(uid, "the bootstrap did not run");
}
