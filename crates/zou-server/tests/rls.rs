//! The RLS context contract against a live postgres: the auth.*
//! functions read the injected claims exactly like Supabase's own
//! definitions, policies written against them isolate users, and
//! service_role walks past RLS the way it does on hosted Supabase.
//!
//! Gated on ZOU_PG_TEST_DSN like the pool suite, skips when unset.

use zou_server::sql::{Pool, RequestContext, Session};

const U1: &str = "11111111-1111-1111-1111-111111111111";
const U2: &str = "22222222-2222-2222-2222-222222222222";

fn pool() -> Option<Pool> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(Pool::new(&v, 2).expect("dsn parses")),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

fn user_ctx(sub: &str) -> RequestContext {
    RequestContext::bare(
        "authenticated",
        &format!(r#"{{"sub":"{sub}","role":"authenticated","email":"{sub}@example.com"}}"#),
    )
}

async fn text(sess: &Session, sql: &str) -> String {
    let rows = sess.query(sql, &[]).await.expect("query");
    rows[0].get::<_, String>(0)
}

#[tokio::test]
async fn the_auth_functions_read_the_injected_claims() {
    let Some(pool) = pool() else { return };
    let sess = pool.session(&user_ctx(U1), false).await.expect("session");
    assert_eq!(text(&sess, "select auth.uid()::text").await, U1);
    assert_eq!(text(&sess, "select auth.role()").await, "authenticated");
    assert_eq!(
        text(&sess, "select auth.email()").await,
        format!("{U1}@example.com")
    );
    assert_eq!(text(&sess, "select auth.jwt() ->> 'sub'").await, U1);
    sess.commit().await.expect("commit");
}

#[tokio::test]
async fn the_auth_functions_are_null_without_claims() {
    let Some(pool) = pool() else { return };
    let ctx = RequestContext::bare("anon", "{}");
    let sess = pool.session(&ctx, false).await.expect("session");
    assert_eq!(
        text(&sess, "select coalesce(auth.uid()::text, '<null>')").await,
        "<null>"
    );
    assert_eq!(
        text(&sess, "select coalesce(auth.email(), '<null>')").await,
        "<null>"
    );
    sess.commit().await.expect("commit");
}

#[tokio::test]
async fn policies_on_auth_uid_isolate_users() {
    let Some(pool) = pool() else { return };
    let admin = pool.unscoped().await.expect("unscoped");
    admin
        .execute("drop table if exists zou_rls_docs", &[])
        .await
        .expect("drop");
    admin
        .execute(
            "create table zou_rls_docs (owner uuid not null, body text not null)",
            &[],
        )
        .await
        .expect("create");
    admin
        .execute("alter table zou_rls_docs enable row level security", &[])
        .await
        .expect("enable rls");
    admin
        .execute(
            "create policy owner_only on zou_rls_docs using (auth.uid() = owner)",
            &[],
        )
        .await
        .expect("policy");
    // The grant a project's migrations carry. A table arrives granted
    // to nobody who came in through the api, so the policy below has
    // to be given something to decide about.
    admin
        .execute(
            "grant select, insert, update, delete on zou_rls_docs \
             to anon, authenticated, service_role",
            &[],
        )
        .await
        .expect("grant");
    admin
        .execute(
            &format!("insert into zou_rls_docs values ('{U1}', 'u1 doc'), ('{U2}', 'u2 doc')"),
            &[],
        )
        .await
        .expect("seed");
    admin.commit().await.expect("finish");

    // U1 sees exactly their row, thanks to the default grants from
    // bootstrap plus the policy, no per table grant statements here.
    let sess = pool.session(&user_ctx(U1), false).await.expect("session");
    assert_eq!(
        text(&sess, "select count(*)::text from zou_rls_docs").await,
        "1"
    );
    assert_eq!(text(&sess, "select body from zou_rls_docs").await, "u1 doc");
    // The cross user write attack: updating the other user's row
    // matches nothing, it is filtered before the write.
    let touched = sess
        .execute(
            &format!("update zou_rls_docs set body = 'stolen' where owner = '{U2}'"),
            &[],
        )
        .await
        .expect("update runs");
    assert_eq!(touched, 0);
    sess.commit().await.expect("commit");

    // anon has no sub claim, auth.uid() is null, nothing matches.
    let anon = pool
        .session(&RequestContext::bare("anon", "{}"), false)
        .await
        .expect("session");
    assert_eq!(
        text(&anon, "select count(*)::text from zou_rls_docs").await,
        "0"
    );
    anon.commit().await.expect("commit");

    // service_role carries bypassrls, both rows and the untouched body.
    let service = pool
        .session(&RequestContext::bare("service_role", "{}"), false)
        .await
        .expect("session");
    assert_eq!(
        text(&service, "select count(*)::text from zou_rls_docs").await,
        "2"
    );
    assert_eq!(
        text(
            &service,
            &format!("select body from zou_rls_docs where owner = '{U2}'")
        )
        .await,
        "u2 doc"
    );
    service.commit().await.expect("commit");

    let admin = pool.unscoped().await.expect("unscoped");
    admin
        .execute("drop table zou_rls_docs", &[])
        .await
        .expect("drop");
    admin.commit().await.expect("finish");
}

/// A table nobody granted is a table nobody reads, which is what a
/// project gets upstream. The grant is the project's to make and row
/// level security is the second fence rather than the only one.
#[tokio::test]
async fn a_table_created_after_bootstrap_is_granted_to_nobody() {
    let Some(pool) = pool() else { return };
    let admin = pool.unscoped().await.expect("unscoped");
    admin
        .execute("drop table if exists zou_rls_open", &[])
        .await
        .expect("drop");
    admin
        .execute("create table zou_rls_open (x int)", &[])
        .await
        .expect("create");
    admin.commit().await.expect("finish");

    let anon = pool
        .session(&RequestContext::bare("anon", "{}"), false)
        .await
        .expect("session");
    let refused = anon
        .query("select count(*) from zou_rls_open", &[])
        .await
        .expect_err("a table nobody granted");
    assert!(
        refused.to_string().contains("permission denied"),
        "{refused}"
    );
    // The session is done for once postgres has refused inside it, so
    // this ends the transaction rather than pretending it is still one.
    drop(anon);

    let admin = pool.unscoped().await.expect("unscoped");
    admin
        .execute("grant select on zou_rls_open to anon", &[])
        .await
        .expect("grant");
    admin.commit().await.expect("finish");

    let anon = pool
        .session(&RequestContext::bare("anon", "{}"), false)
        .await
        .expect("session");
    assert_eq!(
        text(&anon, "select count(*)::text from zou_rls_open").await,
        "0"
    );
    anon.commit().await.expect("commit");

    let admin = pool.unscoped().await.expect("unscoped");
    admin
        .execute("drop table zou_rls_open", &[])
        .await
        .expect("drop");
    admin.commit().await.expect("finish");
}
