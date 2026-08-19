//! Leak tests for the SQL session pool against a live postgres.
//!
//! Gated on ZOU_PG_TEST_DSN, unset means every test skips, same
//! pattern as the s3 contract suite. CI runs these against a stock
//! postgres service container, locally point the var at any cluster
//! you can throw roles at:
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test pool
//!
//! Every test builds a pool capped at one connection, so reuse versus
//! replacement is observable through pg_backend_pid.

use zou_server::sql::{Pool, RequestContext};

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

fn pool_of(n: usize) -> Option<Pool> {
    dsn().map(|d| Pool::new(&d, n).expect("dsn parses"))
}

const CLAIMS: &str = r#"{"sub":"user-1","role":"authenticated"}"#;

async fn text(sess: &zou_server::sql::Session, sql: &str) -> String {
    let rows = sess.query(sql, &[]).await.expect("query");
    rows[0].get::<_, String>(0)
}

#[tokio::test]
async fn the_context_exists_inside_the_transaction() {
    let Some(pool) = pool_of(1) else { return };
    let ctx = RequestContext {
        role: "authenticated".to_string(),
        claims: CLAIMS.to_string(),
        method: "GET".to_string(),
        path: "/todos".to_string(),
        headers: r#"{"x-forwarded-for":"10.0.0.1"}"#.to_string(),
        cookies: "{}".to_string(),
        search_path: "\"public\"".to_string(),
    };
    let sess = pool.session(&ctx, false).await.expect("session");
    assert_eq!(
        text(&sess, "select current_user::text").await,
        "authenticated"
    );
    assert_eq!(
        text(&sess, "select current_setting('request.jwt.claims')").await,
        CLAIMS
    );
    assert_eq!(
        text(&sess, "select current_setting('request.method')").await,
        "GET"
    );
    assert_eq!(
        text(&sess, "select current_setting('request.path')").await,
        "/todos"
    );
    assert_eq!(
        text(
            &sess,
            "select current_setting('request.headers')::json ->> 'x-forwarded-for'"
        )
        .await,
        "10.0.0.1"
    );
    sess.commit().await.expect("commit");
}

#[tokio::test]
async fn nothing_leaks_into_the_next_checkout_after_commit() {
    let Some(pool) = pool_of(1) else { return };
    let ctx = RequestContext::bare("anon", "{}");
    let sess = pool.session(&ctx, false).await.expect("session");
    let pid = sess.backend_pid().await.expect("pid");
    sess.commit().await.expect("commit");

    let clean = pool.unscoped().await.expect("unscoped");
    assert_eq!(
        clean.backend_pid().await.expect("pid"),
        pid,
        "same connection reused"
    );
    assert_ne!(text(&clean, "select current_user::text").await, "anon");
    assert_eq!(
        text(
            &clean,
            "select coalesce(nullif(current_setting('request.jwt.claims', true), ''), '<unset>')"
        )
        .await,
        "<unset>"
    );
    clean.commit().await.expect("finish");
}

#[tokio::test]
async fn a_session_level_set_config_is_scrubbed_on_return() {
    let Some(pool) = pool_of(1) else { return };
    let ctx = RequestContext::bare("anon", "{}");
    let sess = pool.session(&ctx, false).await.expect("session");
    // is_local false survives commit, exactly the smuggling the pool
    // must scrub before the connection serves anyone else.
    sess.query("select set_config('app.sticky', 'leaked', false)", &[])
        .await
        .expect("session level set");
    sess.query("select set_config('role', 'service_role', false)", &[])
        .await
        .expect("session level role");
    let pid = sess.backend_pid().await.expect("pid");
    sess.commit().await.expect("commit");

    let clean = pool.unscoped().await.expect("unscoped");
    assert_eq!(
        clean.backend_pid().await.expect("pid"),
        pid,
        "same connection reused"
    );
    assert_ne!(
        text(&clean, "select current_user::text").await,
        "service_role"
    );
    assert_eq!(
        text(
            &clean,
            "select coalesce(nullif(current_setting('app.sticky', true), ''), '<unset>')"
        )
        .await,
        "<unset>"
    );
    clean.commit().await.expect("finish");
}

#[tokio::test]
async fn an_abandoned_session_forfeits_its_connection() {
    let Some(pool) = pool_of(1) else { return };
    let ctx = RequestContext::bare("anon", "{}");
    let sess = pool.session(&ctx, false).await.expect("session");
    sess.query("select set_config('app.sticky', 'leaked', false)", &[])
        .await
        .expect("session level set");
    let pid = sess.backend_pid().await.expect("pid");
    drop(sess);

    let clean = pool.unscoped().await.expect("unscoped");
    assert_ne!(
        clean.backend_pid().await.expect("pid"),
        pid,
        "fresh connection dialed"
    );
    assert_eq!(
        text(
            &clean,
            "select coalesce(nullif(current_setting('app.sticky', true), ''), '<unset>')"
        )
        .await,
        "<unset>"
    );
    clean.commit().await.expect("finish");
}

#[tokio::test]
async fn rollback_returns_a_clean_connection() {
    let Some(pool) = pool_of(1) else { return };
    let ctx = RequestContext::bare("anon", "{}");
    let sess = pool.session(&ctx, false).await.expect("session");
    let pid = sess.backend_pid().await.expect("pid");
    assert!(
        sess.query("select no_such_column from nowhere", &[])
            .await
            .is_err()
    );
    sess.rollback().await.expect("rollback");

    let clean = pool.unscoped().await.expect("unscoped");
    assert_eq!(
        clean.backend_pid().await.expect("pid"),
        pid,
        "same connection reused"
    );
    assert_eq!(
        text(
            &clean,
            "select coalesce(nullif(current_setting('request.jwt.claims', true), ''), '<unset>')"
        )
        .await,
        "<unset>"
    );
    clean.commit().await.expect("finish");
}

#[tokio::test]
async fn read_only_sessions_reject_writes() {
    let Some(pool) = pool_of(1) else { return };
    let ctx = RequestContext::bare("anon", "{}");
    let sess = pool.session(&ctx, true).await.expect("session");
    let err = sess
        .execute("create table zou_pool_never (x int)", &[])
        .await
        .expect_err("write in a read only transaction");
    let msg = err.as_db_error().expect("a db error").message();
    assert!(msg.contains("read-only"), "unexpected error: {msg}");
    sess.rollback().await.expect("rollback");
}

#[tokio::test]
async fn bootstrap_created_the_three_roles() {
    let Some(pool) = pool_of(1) else { return };
    let sess = pool.unscoped().await.expect("unscoped");
    assert_eq!(
        text(
            &sess,
            "select count(*)::text from pg_roles where rolname in ('anon','authenticated','service_role')"
        )
        .await,
        "3"
    );
    assert_eq!(
        text(
            &sess,
            "select rolbypassrls::text from pg_roles where rolname = 'service_role'"
        )
        .await,
        "true"
    );
    sess.commit().await.expect("finish");
}

/// The fourth role, which is not one of the three and is not reached
/// the way they are. A project's migration grants to it, so it has to
/// be there, and nothing in this server ever becomes it.
#[tokio::test]
async fn bootstrap_created_the_role_a_project_migration_grants_to() {
    let Some(pool) = pool_of(1) else { return };
    let sess = pool.unscoped().await.expect("unscoped");
    assert_eq!(
        text(
            &sess,
            "select rolcanlogin::text from pg_roles where rolname = 'supabase_auth_admin'"
        )
        .await,
        "false"
    );
    sess.execute("grant usage on schema public to supabase_auth_admin", &[])
        .await
        .expect("a migration written for a Supabase database applies");
    sess.commit().await.expect("finish");
}

#[tokio::test]
async fn the_injected_role_carries_real_privileges() {
    let Some(pool) = pool_of(1) else { return };
    let admin = pool.unscoped().await.expect("unscoped");
    // The table lives in a schema anon has no usage on. A revoke on a
    // public table would not stick here: every test builds its own
    // pool, and a concurrent bootstrap's blanket grant on all tables
    // in public would re grant it mid test.
    admin
        .execute("create schema if not exists zou_pool_hidden", &[])
        .await
        .expect("schema");
    admin
        .execute(
            "create table if not exists zou_pool_hidden.private (x int)",
            &[],
        )
        .await
        .expect("create");
    admin.commit().await.expect("finish");

    let ctx = RequestContext::bare("anon", "{}");
    let sess = pool.session(&ctx, false).await.expect("session");
    let err = sess
        .query("select * from zou_pool_hidden.private", &[])
        .await
        .expect_err("anon reading an unGRANTed table");
    let msg = err.as_db_error().expect("a db error").message();
    assert!(msg.contains("permission denied"), "unexpected error: {msg}");
    sess.rollback().await.expect("rollback");

    let admin = pool.unscoped().await.expect("unscoped");
    admin
        .execute("drop schema zou_pool_hidden cascade", &[])
        .await
        .expect("drop");
    admin.commit().await.expect("finish");
}

#[tokio::test]
async fn checkouts_past_the_cap_wait_instead_of_failing() {
    let Some(pool) = pool_of(2) else { return };
    let mut tasks = Vec::new();
    for i in 0..6 {
        let pool = pool.clone();
        tasks.push(tokio::spawn(async move {
            let ctx = RequestContext::bare("anon", "{}");
            let sess = pool.session(&ctx, false).await.expect("session");
            let n: String = text(&sess, &format!("select {i}::text")).await;
            sess.commit().await.expect("commit");
            n
        }));
    }
    for (i, t) in tasks.into_iter().enumerate() {
        assert_eq!(t.await.expect("join"), i.to_string());
    }
}

/// The timeout is on the role, and postgres does not apply a role's
/// settings to a role that was arrived at with set role, so this is
/// really a test of the pool reading them and applying them itself.
#[tokio::test]
async fn a_role_setting_reaches_the_transaction_that_set_role_would_not_get() {
    let Some(pool) = pool_of(1) else { return };
    let ctx = RequestContext::bare("anon", "{}");
    let sess = pool.session(&ctx, false).await.expect("session");
    assert_eq!(
        text(&sess, "select current_setting('statement_timeout')").await,
        "3s"
    );
    let err = sess
        .query("select pg_sleep(5)", &[])
        .await
        .expect_err("a query past the timeout");
    let msg = err.as_db_error().expect("a db error").message();
    assert!(msg.contains("statement timeout"), "unexpected error: {msg}");
    sess.rollback().await.expect("rollback");

    // Local to the transaction, like everything else the pool injects.
    let clean = pool.unscoped().await.expect("unscoped");
    assert_ne!(
        text(&clean, "select current_setting('statement_timeout')").await,
        "3s"
    );
    clean.commit().await.expect("finish");
}

/// The one setting a role is not allowed to have, because the schema a
/// request runs against is negotiated per request and a role level
/// search_path would win over the profile the caller asked for.
///
/// A role of its own rather than anon, because every other test in
/// this file may be bootstrapping a pool while this one runs, and two
/// alter role statements against one role at the same time is a tuple
/// concurrently updated rather than a result.
#[tokio::test]
async fn a_role_search_path_does_not_beat_the_requested_profile() {
    let Some(pool) = pool_of(1) else { return };
    let admin = pool.unscoped().await.expect("unscoped");
    admin
        .execute(
            "do $$ begin \
               if not exists (select 1 from pg_roles where rolname = 'zou_pool_profiled') then \
                 create role zou_pool_profiled nologin; \
               end if; \
             end $$",
            &[],
        )
        .await
        .expect("role");
    admin
        .execute(
            "alter role zou_pool_profiled set search_path to pg_catalog",
            &[],
        )
        .await
        .expect("alter role");
    admin.commit().await.expect("finish");

    // A pool of its own, because the settings the one above read are
    // held for a while and were read before any of this.
    let fresh = pool_of(1).expect("dsn");
    let ctx = RequestContext::bare("zou_pool_profiled", "{}");
    let sess = fresh.session(&ctx, false).await.expect("session");
    assert_eq!(
        text(&sess, "select current_setting('search_path')").await,
        "\"public\""
    );
    sess.rollback().await.expect("rollback");

    let admin = pool.unscoped().await.expect("unscoped");
    admin
        .execute("drop role zou_pool_profiled", &[])
        .await
        .expect("drop");
    admin.commit().await.expect("finish");
}

/// The extensions a project's own migrations assume, which are not
/// zou's own: nothing in this server calls either of them. The
/// assertion is that `gen_salt` resolves with nothing in front of it,
/// because that is how a migration copied off the Supabase docs spells
/// it.
#[tokio::test]
async fn bootstrap_left_the_extensions_a_supabase_database_has() {
    let Some(pool) = pool_of(1) else { return };
    let sess = pool.unscoped().await.expect("unscoped");
    assert_eq!(
        text(
            &sess,
            "select count(*)::text from pg_extension e \
             join pg_namespace n on n.oid = e.extnamespace \
             where e.extname in ('pgcrypto','uuid-ossp') and n.nspname = 'extensions'"
        )
        .await,
        "2"
    );
    // Membership rather than the whole array, because the cron
    // schema puts pg_cron's four settings on the database next to
    // this one and a project reads them one at a time.
    assert_eq!(
        text(
            &sess,
            "select (setconfig @> array['search_path=\"$user\", public, extensions'])::text \
               from pg_db_role_setting \
              where setdatabase = (select oid from pg_database where datname = current_database()) \
                and setrole = 0"
        )
        .await,
        "true"
    );
    // A session that started before the alter database landed still
    // has the old path, so this asks the way a new one would. Not set
    // local: an unscoped session is not in a transaction.
    sess.query("set search_path to \"$user\", public, extensions", &[])
        .await
        .expect("search path");
    assert_eq!(
        text(&sess, "select length(gen_salt('bf'))::text").await,
        "29"
    );
    assert_eq!(
        text(&sess, "select length(uuid_generate_v4()::text)::text").await,
        "36"
    );
    sess.commit().await.expect("finish");
}
