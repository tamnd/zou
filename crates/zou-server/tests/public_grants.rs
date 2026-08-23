//! What a table in `public` arrives granted, which is the difference
//! between a project that forgot a policy and a project that leaked.
//!
//! Upstream a new table there is readable and writable by nobody who
//! comes in through the api, and a project grants what it means to
//! expose. zou handed all three api roles everything for a while, which
//! is only invisible while every table has row level security on it,
//! and goes the wrong way the moment one does not.
//!
//! Each test works on a scratch database of its own, because what is
//! under test is the first bootstrap of a database and the shared test
//! database has already had one.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.

use tokio_postgres::NoTls;
use zou_server::sql::{Pool, with_database};

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

/// A connection outside the pool, for the create database and drop
/// database a scratch database needs and for reading privileges back
/// as somebody who is not the pool.
async fn raw(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect to {dsn}: {e}"));
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// A database nobody has opened yet, dropped first so a run that died
/// halfway does not decide the next one.
async fn scratch(dsn: &str, name: &str) -> String {
    let target = with_database(dsn, name);
    let admin = raw(dsn).await;
    admin
        .batch_execute(&format!("drop database if exists {name} with (force)"))
        .await
        .expect("drop any leftover");
    admin
        .batch_execute(&format!("create database {name}"))
        .await
        .expect("create scratch");
    target
}

/// Whether a role may do something to a table, asked of postgres
/// rather than worked out from an acl string.
async fn may(client: &tokio_postgres::Client, role: &str, table: &str, what: &str) -> bool {
    client
        .query_one(
            "select has_table_privilege($1, $2, $3)",
            &[&role, &table, &what],
        )
        .await
        .expect("privilege query")
        .get(0)
}

/// The roles a bootstrap would create, made here first because the
/// stance being planted is an `alter default privileges` that has to
/// name them. Roles are the cluster's, so on a box that has run this
/// suite before they are already there.
async fn roles(client: &tokio_postgres::Client) {
    client
        .batch_execute(
            "do $$
             begin
                 if not exists (select 1 from pg_roles where rolname = 'anon') then
                     create role anon nologin;
                 end if;
                 if not exists (select 1 from pg_roles where rolname = 'authenticated') then
                     create role authenticated nologin;
                 end if;
                 if not exists (select 1 from pg_roles where rolname = 'service_role') then
                     create role service_role nologin bypassrls;
                 end if;
             end
             $$;",
        )
        .await
        .expect("create the api roles");
}

/// The whole point: a table made after the bootstrap is not a table the
/// api can read.
#[tokio::test]
async fn a_new_table_in_public_is_readable_by_nobody_who_came_through_the_api() {
    let Some(dsn) = dsn() else { return };
    let fresh = scratch(&dsn, "zou_public_grants_new").await;

    // A checkout is what bootstraps the database.
    let pool = Pool::new(&fresh, 2).expect("scratch dsn parses");
    let sess = pool.unscoped().await.expect("unscoped on scratch");
    sess.execute("create table after_the_bootstrap (id int primary key)", &[])
        .await
        .expect("create the table");
    sess.commit().await.expect("finish");

    let client = raw(&fresh).await;
    for role in ["anon", "authenticated", "service_role"] {
        for what in ["select", "insert", "update", "delete"] {
            assert!(
                !may(&client, role, "after_the_bootstrap", what).await,
                "{role} may {what} a table nobody granted"
            );
        }
        // The four that are not a way in on their own are granted, so
        // the acl matches upstream's rather than merely being smaller
        // than what it was.
        for what in ["truncate", "references", "trigger", "maintain"] {
            assert!(
                may(&client, role, "after_the_bootstrap", what).await,
                "{role} may not {what}, which upstream grants"
            );
        }
    }
}

/// A grant the project made is the whole way in, and it still works.
#[tokio::test]
async fn a_project_that_grants_a_table_gets_a_table_the_api_can_read() {
    let Some(dsn) = dsn() else { return };
    let fresh = scratch(&dsn, "zou_public_grants_asked").await;

    let pool = Pool::new(&fresh, 2).expect("scratch dsn parses");
    let sess = pool.unscoped().await.expect("unscoped on scratch");
    sess.execute("create table asked_for (id int primary key)", &[])
        .await
        .expect("create the table");
    sess.execute("grant select on asked_for to anon", &[])
        .await
        .expect("grant it");
    sess.commit().await.expect("finish");

    let client = raw(&fresh).await;
    assert!(may(&client, "anon", "asked_for", "select").await);
    assert!(!may(&client, "anon", "asked_for", "insert").await);
    assert!(!may(&client, "authenticated", "asked_for", "select").await);
}

/// A database an older zou opened carries that release's stance in its
/// default privileges, and a database is bootstrapped once, so without
/// this it would hand out every future table forever. What it must not
/// do while fixing that is rewrite a table that already exists, since
/// the acl on one of those is the project's answer and not zou's.
#[tokio::test]
async fn a_database_an_older_zou_opened_stops_handing_out_new_tables() {
    let Some(dsn) = dsn() else { return };
    let fresh = scratch(&dsn, "zou_public_grants_older").await;

    {
        let old = raw(&fresh).await;
        roles(&old).await;
        old.batch_execute(
            "create table from_the_old_days (id int primary key);
             grant all on all tables in schema public to anon, authenticated, service_role;
             alter default privileges in schema public
                 grant all on tables to anon, authenticated, service_role;",
        )
        .await
        .expect("plant the old stance");
    }

    let pool = Pool::new(&fresh, 2).expect("scratch dsn parses");
    let sess = pool.unscoped().await.expect("unscoped on scratch");
    sess.execute(
        "create table made_after_the_upgrade (id int primary key)",
        &[],
    )
    .await
    .expect("create the table");
    sess.commit().await.expect("finish");

    let client = raw(&fresh).await;
    assert!(
        !may(&client, "anon", "made_after_the_upgrade", "select").await,
        "the old default privileges outlived the upgrade"
    );
    assert!(
        may(&client, "anon", "from_the_old_days", "select").await,
        "a table that already existed had its grants taken away"
    );
}
