//! The cross user and cross tenant attack suite.
//!
//! Everything here goes through the front door as a real request and
//! tries to get at data it should not have. The other RLS suite proves
//! the session layer injects the right claims; this one assumes an
//! attacker and asks what they can reach: another user's rows, another
//! tenant's rows, a role they were not given, or the database itself
//! through the query grammar.
//!
//! Gated on ZOU_PG_TEST_DSN like the rest of the live suites, and run
//! in CI against a real postgres.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test attack

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use zou_server::sql::Pool;
use zou_server::{Config, jwt, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";
const OTHER: &[u8] = b"another-secret-that-is-also-at-least-32-characters-long";
const U1: &str = "11111111-1111-1111-1111-111111111111";
const U2: &str = "22222222-2222-2222-2222-222222222222";

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

fn app(dsn: &str) -> axum::Router {
    router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        ..Config::default()
    })
    .expect("router builds")
}

fn anon_key() -> String {
    jwt::mint(&jwt::key_claims("anon"), SECRET)
}

/// A user token the way GoTrue mints one, signed with the secret the
/// server trusts unless `secret` says otherwise.
fn user(sub: &str, tenant: &str, secret: &[u8]) -> String {
    jwt::mint(
        &serde_json::json!({
            "sub": sub,
            "role": "authenticated",
            "email": format!("{sub}@example.com"),
            "tenant": tenant,
        }),
        secret,
    )
}

async fn seed(dsn: &str, statements: &[impl AsRef<str>]) {
    let pool = Pool::new(dsn, 1).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    for stmt in statements {
        let stmt = stmt.as_ref();
        sess.execute(stmt, &[]).await.expect(stmt);
    }
    sess.commit().await.expect("park");
}

async fn body_text(res: axum::response::Response) -> String {
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// A request with an apikey and, when `token` is given, a bearer.
fn as_user(method: &str, uri: &str, token: Option<&str>, body: &str) -> Request<Body> {
    let mut b = Request::builder()
        .method(method)
        .uri(uri)
        .header("apikey", anon_key());
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    if body.is_empty() {
        b.body(Body::empty()).unwrap()
    } else {
        b.header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }
}

/// The seed every test here runs against: one table, RLS on, and one
/// policy tying a row both to the token's own sub and to the token's
/// own tenant claim.
///
/// The table is named per test rather than shared, because these run
/// in parallel and two of them creating the same table at once is a
/// duplicate key in pg_type rather than anything to do with the thing
/// under test.
async fn docs(dsn: &str, table: &str) {
    seed(
        dsn,
        &[
            format!("drop table if exists {table} cascade"),
            format!(
                "create table {table} (\
                   id int primary key, \
                   owner uuid not null, \
                   tenant text not null, \
                   body text)"
            ),
            format!("alter table {table} enable row level security"),
            format!(
                "create policy own_rows on {table} for all to authenticated \
                 using (owner = auth.uid() and tenant = auth.jwt() ->> 'tenant') \
                 with check (owner = auth.uid() and tenant = auth.jwt() ->> 'tenant')"
            ),
            format!(
                "insert into {table} values \
                   (1, '{U1}', 'acme', 'one'), \
                   (2, '{U2}', 'globex', 'two')"
            ),
            format!("grant all on {table} to anon, authenticated, service_role"),
        ],
    )
    .await;
}

/// What the database really holds, read past RLS, so an assertion
/// about a write that should not have happened cannot itself be
/// fooled by RLS.
async fn truth(dsn: &str, table: &str) -> String {
    let pool = Pool::new(dsn, 1).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    let rows = sess
        .query(
            &format!(
                "select coalesce(string_agg(id || ':' || body, ',' order by id), '') \
                 from {table}"
            ),
            &[],
        )
        .await
        .expect("query");
    let out: String = rows[0].get(0);
    sess.commit().await.expect("park");
    out
}

#[tokio::test]
async fn a_user_reaches_only_their_own_rows() {
    let Some(dsn) = dsn() else { return };
    docs(&dsn, "zou_atk_read").await;
    let app = app(&dsn);
    let one = user(U1, "acme", SECRET);

    // The plain read is filtered by the policy.
    let res = app
        .clone()
        .oneshot(as_user("GET", "/rest/v1/zou_atk_read", Some(&one), ""))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let rows: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1);
    assert_eq!(rows[0]["id"], 1);

    // Naming the other row explicitly does not conjure it. Each of
    // these is a legal query that a bare table would answer with row
    // two, so each must come back 200 with row two missing rather than
    // as an error, which is the difference between a policy filtering
    // and a parser refusing.
    for uri in [
        "/rest/v1/zou_atk_read?id=eq.2",
        "/rest/v1/zou_atk_read?owner=eq.22222222-2222-2222-2222-222222222222",
        "/rest/v1/zou_atk_read?tenant=eq.globex",
        "/rest/v1/zou_atk_read?or=(id.eq.1,id.eq.2)",
        "/rest/v1/zou_atk_read?id=not.eq.1",
        "/rest/v1/zou_atk_read?select=body&order=id.desc&limit=100",
        "/rest/v1/zou_atk_read?id=gte.0&order=owner.asc,tenant.desc",
        "/rest/v1/zou_atk_read?body=neq.one",
    ] {
        let res = app
            .clone()
            .oneshot(as_user("GET", uri, Some(&one), ""))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "{uri} did not run");
        let body = body_text(res).await;
        assert!(
            !body.contains("two") && !body.contains("globex") && !body.contains(U2),
            "{uri} leaked another user's row: {body}"
        );
    }

    // A count is a read too, so it counts what the policy allows and
    // not what the table holds.
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/rest/v1/zou_atk_read")
                .header("apikey", anon_key())
                .header("authorization", format!("Bearer {one}"))
                .header("prefer", "count=exact")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        res.headers()["content-range"],
        "0-0/1",
        "an exact count must not report rows the caller cannot see"
    );

    // Anon has no policy at all, so it has no rows.
    let res = app
        .clone()
        .oneshot(as_user("GET", "/rest/v1/zou_atk_read", None, ""))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, "[]");
}

#[tokio::test]
async fn a_user_cannot_write_another_users_rows() {
    let Some(dsn) = dsn() else { return };
    docs(&dsn, "zou_atk_write").await;
    let app = app(&dsn);
    let one = user(U1, "acme", SECRET);

    // An update the policy does not cover matches nothing rather than
    // erroring, which is postgres deciding the row is not there.
    let res = app
        .clone()
        .oneshot(as_user(
            "PATCH",
            "/rest/v1/zou_atk_write?id=eq.2",
            Some(&one),
            r#"{"body":"stolen"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        truth(&dsn, "zou_atk_write").await,
        "1:one,2:two",
        "the patch touched nothing"
    );

    let res = app
        .clone()
        .oneshot(as_user(
            "DELETE",
            "/rest/v1/zou_atk_write?id=eq.2",
            Some(&one),
            "",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        truth(&dsn, "zou_atk_write").await,
        "1:one,2:two",
        "the delete took nothing"
    );

    // Writing a row owned by someone else is the with check clause,
    // which is an error rather than a silent miss.
    let res = app
        .clone()
        .oneshot(as_user(
            "POST",
            "/rest/v1/zou_atk_write",
            Some(&one),
            &format!(r#"{{"id":3,"owner":"{U2}","tenant":"globex","body":"planted"}}"#),
        ))
        .await
        .unwrap();
    assert!(
        res.status().is_client_error(),
        "a row owned by another user must be refused, got {}",
        res.status()
    );
    let e: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(e["code"], "42501");
    assert_eq!(
        truth(&dsn, "zou_atk_write").await,
        "1:one,2:two",
        "nothing was planted"
    );

    // The same user rewriting a row into another tenant is the same
    // refusal, which is what makes the tenant claim a boundary and not
    // a label.
    let res = app
        .clone()
        .oneshot(as_user(
            "PATCH",
            "/rest/v1/zou_atk_write?id=eq.1",
            Some(&one),
            r#"{"tenant":"globex"}"#,
        ))
        .await
        .unwrap();
    assert!(res.status().is_client_error());
    assert_eq!(truth(&dsn, "zou_atk_write").await, "1:one,2:two");
}

#[tokio::test]
async fn the_tenant_comes_from_the_token() {
    let Some(dsn) = dsn() else { return };
    docs(&dsn, "zou_atk_tenant").await;
    let app = app(&dsn);

    // The same user with the wrong tenant claim sees nothing, so the
    // policy really reads the token and not the row.
    let wrong = user(U1, "globex", SECRET);
    let res = app
        .clone()
        .oneshot(as_user("GET", "/rest/v1/zou_atk_tenant", Some(&wrong), ""))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, "[]");

    // And a tenant asserted anywhere other than the token is not a
    // tenant at all.
    let one = user(U1, "acme", SECRET);
    for (name, value) in [
        ("x-tenant", "globex"),
        ("tenant", "globex"),
        ("x-forwarded-user", U2),
    ] {
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/v1/zou_atk_tenant")
                    .header("apikey", anon_key())
                    .header("authorization", format!("Bearer {one}"))
                    .header(name, value)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_text(res).await;
        assert!(
            body.contains("one") && !body.contains("two"),
            "the {name} header moved the tenant: {body}"
        );
    }
}

#[tokio::test]
async fn the_role_comes_from_the_signature() {
    let Some(dsn) = dsn() else { return };
    docs(&dsn, "zou_atk_role").await;
    let app = app(&dsn);

    // A service_role token signed with the wrong secret is not a
    // token.
    let forged = jwt::mint(&jwt::key_claims("service_role"), OTHER);
    let res = app
        .clone()
        .oneshot(as_user("GET", "/rest/v1/zou_atk_role", Some(&forged), ""))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // Neither is a real token with its payload rewritten, since the
    // signature covers the payload.
    let real = user(U1, "acme", SECRET);
    let mut parts: Vec<String> = real.split('.').map(str::to_string).collect();
    parts[1] = parts[1].replace(U1, U2);
    let tampered = parts.join(".");
    if tampered != real {
        let res = app
            .clone()
            .oneshot(as_user("GET", "/rest/v1/zou_atk_role", Some(&tampered), ""))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    // An unsigned token is not a token either, whatever it claims.
    // The two segments are base64url of {"alg":"none","typ":"JWT"}
    // and {"role":"service_role"}, written out so the test does not
    // need an encoder to say something this small.
    let alg_none = "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0\
                    .eyJyb2xlIjoic2VydmljZV9yb2xlIn0.";
    let res = app
        .clone()
        .oneshot(as_user("GET", "/rest/v1/zou_atk_role", Some(alg_none), ""))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // A role asserted beside the token rather than inside it is not a
    // role.
    for (name, value) in [("x-role", "service_role"), ("role", "service_role")] {
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/v1/zou_atk_role")
                    .header("apikey", anon_key())
                    .header(name, value)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(body_text(res).await, "[]", "the {name} header set a role");
    }
    let res = app
        .clone()
        .oneshot(as_user(
            "GET",
            "/rest/v1/zou_atk_role?role=eq.service_role",
            None,
            "",
        ))
        .await
        .unwrap();
    assert!(
        res.status().is_client_error() || body_text(res).await == "[]",
        "a query parameter named role is a filter, never a role"
    );

    // The positive control: the key that is trusted is trusted. A
    // service_role token bypasses RLS the way it does on hosted
    // Supabase, which is what makes every assertion above meaningful.
    let service = jwt::mint(&jwt::key_claims("service_role"), SECRET);
    let res = app
        .clone()
        .oneshot(as_user("GET", "/rest/v1/zou_atk_role", Some(&service), ""))
        .await
        .unwrap();
    let rows: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn the_grammar_does_not_carry_sql() {
    let Some(dsn) = dsn() else { return };
    docs(&dsn, "zou_atk_grammar").await;
    let app = app(&dsn);
    let one = user(U1, "acme", SECRET);

    // Sql where a value goes. These are legal queries whose values
    // happen to be hostile, so each has to run and answer honestly
    // with the caller's own rows, which here is none of them. An error
    // would hide the interesting part; what is being asserted is that
    // the value reached postgres as a parameter and matched nothing.
    for uri in [
        "/rest/v1/zou_atk_grammar?body=eq.';drop%20table%20zou_atk_grammar;--",
        "/rest/v1/zou_atk_grammar?body=eq.'%20or%20'1'='1",
        "/rest/v1/zou_atk_grammar?body=like.*%25'||(select%20version())||'",
        "/rest/v1/zou_atk_grammar?body=fts.'||pg_sleep(0)||'",
        "/rest/v1/zou_atk_grammar?tenant=eq.acme'%20or%20tenant='globex",
    ] {
        let res = app
            .clone()
            .oneshot(as_user("GET", uri, Some(&one), ""))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "{uri} did not run as a value");
        assert_eq!(body_text(res).await, "[]", "{uri} matched something");
    }

    // Sql where a column, a relation, or a number goes. These are not
    // legal, so the answer is a 4xx from the parser or from postgres
    // refusing the value, never a 5xx and never a row.
    for uri in [
        "/rest/v1/zou_atk_grammar?id=eq.1;drop%20table%20zou_atk_grammar",
        "/rest/v1/zou_atk_grammar?select=id,body);drop%20table%20zou_atk_grammar;--",
        "/rest/v1/zou_atk_grammar?select=*&order=id;drop%20table%20zou_atk_grammar",
        "/rest/v1/zou_atk_grammar?select=*&order=(select%20body%20from%20zou_atk_grammar).asc",
        "/rest/v1/zou_atk_grammar?limit=1;drop%20table%20zou_atk_grammar",
        "/rest/v1/zou_atk_grammar?offset=-1",
        "/rest/v1/zou_atk_grammar?id=in.(1,2);--",
        "/rest/v1/zou_atk_grammar?select=*,zou_atk_grammar!inner(*)",
    ] {
        let res = app
            .clone()
            .oneshot(as_user("GET", uri, Some(&one), ""))
            .await
            .unwrap();
        let status = res.status();
        let body = body_text(res).await;
        assert!(
            status.is_client_error(),
            "{uri} was not refused: {status} {body}"
        );
        assert!(
            !body.contains("two") && !body.contains("PostgreSQL"),
            "{uri} returned more than it should: {body}"
        );
    }
    assert_eq!(
        truth(&dsn, "zou_atk_grammar").await,
        "1:one,2:two",
        "the table survived the grammar"
    );

    // The other direction, so none of the above passes because the
    // value never got anywhere: a hostile string written as data comes
    // back as data, byte for byte, and is still just a string when it
    // is used as a filter.
    let nasty = "'; drop table zou_atk_grammar; --";
    let res = app
        .clone()
        .oneshot(as_user(
            "POST",
            "/rest/v1/zou_atk_grammar",
            Some(&one),
            &format!(r#"{{"id":7,"owner":"{U1}","tenant":"acme","body":"{nasty}"}}"#),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let res = app
        .clone()
        .oneshot(as_user(
            "GET",
            "/rest/v1/zou_atk_grammar?body=eq.';%20drop%20table%20zou_atk_grammar;%20--",
            Some(&one),
            "",
        ))
        .await
        .unwrap();
    let rows: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1, "the value round trips");
    assert_eq!(rows[0]["body"], nasty);
    seed(&dsn, &["delete from zou_atk_grammar where id = 7"]).await;

    // on_conflict names a column list, and it only means anything on
    // an upsert, so it gets probed where it is actually read.
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/rest/v1/zou_atk_grammar?on_conflict=id);drop%20table%20zou_atk_grammar;--")
                .header("apikey", anon_key())
                .header("authorization", format!("Bearer {one}"))
                .header("content-type", "application/json")
                .header("prefer", "resolution=merge-duplicates")
                .body(Body::from(format!(
                    r#"{{"id":1,"owner":"{U1}","tenant":"acme","body":"merged"}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        res.status().is_client_error(),
        "a column list that is not a column list must be refused, got {}",
        res.status()
    );

    // The same through a write body, where the column names are
    // attacker chosen too.
    for body in [
        r#"{"id":9,"owner":"11111111-1111-1111-1111-111111111111","tenant":"acme","body\"":"x"}"#,
        r#"{"id":9,"owner":"11111111-1111-1111-1111-111111111111","tenant":"acme","body);drop table zou_atk_grammar;--":"x"}"#,
    ] {
        let res = app
            .clone()
            .oneshot(as_user(
                "POST",
                "/rest/v1/zou_atk_grammar",
                Some(&one),
                body,
            ))
            .await
            .unwrap();
        assert!(
            res.status().is_client_error(),
            "an unknown column must be refused, got {}",
            res.status()
        );
    }
    assert_eq!(truth(&dsn, "zou_atk_grammar").await, "1:one,2:two");
}

#[tokio::test]
async fn a_schema_that_is_not_exposed_stays_unreachable() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop schema if exists zou_atk_hidden cascade",
            "create schema zou_atk_hidden",
            "grant usage on schema zou_atk_hidden to anon, authenticated, service_role",
            "create table zou_atk_hidden.zou_atk_keys (id int primary key, secret text)",
            "insert into zou_atk_hidden.zou_atk_keys values (1, 'top-secret')",
            "grant all on zou_atk_hidden.zou_atk_keys \
             to anon, authenticated, service_role",
            "drop table if exists zou_atk_visible cascade",
            "create table zou_atk_visible (id int primary key)",
            "grant all on zou_atk_visible to anon, authenticated, service_role",
        ],
    )
    .await;
    let app = app(&dsn);
    let service = jwt::mint(&jwt::key_claims("service_role"), SECRET);

    // Even service_role, which walks past RLS, cannot name a schema
    // the deployment did not expose.
    for uri in [
        "/rest/v1/zou_atk_keys",
        "/rest/v1/zou_atk_hidden.zou_atk_keys",
        "/rest/v1/zou_atk_keys?select=secret",
    ] {
        let res = app
            .clone()
            .oneshot(as_user("GET", uri, Some(&service), ""))
            .await
            .unwrap();
        let status = res.status();
        let body = body_text(res).await;
        assert!(
            status.is_client_error(),
            "{uri} was not refused: {status} {body}"
        );
        assert!(
            !body.contains("top-secret"),
            "{uri} reached a hidden schema"
        );
    }

    // A table of the same shape in the exposed schema answers, so the
    // refusals above are about where the table lives and not about
    // this caller being unable to read anything at all.
    let res = app
        .clone()
        .oneshot(as_user(
            "GET",
            "/rest/v1/zou_atk_visible",
            Some(&service),
            "",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // And the profile header only names what the config exposes.
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/rest/v1/zou_atk_keys")
                .header("apikey", anon_key())
                .header("authorization", format!("Bearer {service}"))
                .header("accept-profile", "zou_atk_hidden")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);
    let e: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(e["code"], "PGRST106");
}

#[tokio::test]
async fn the_openapi_document_keeps_the_same_boundary() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_atk_private cascade",
            "create table zou_atk_private (id int primary key, secret text)",
            "revoke all on zou_atk_private from anon, authenticated, service_role",
            "drop table if exists zou_atk_shown cascade",
            "create table zou_atk_shown (id int primary key)",
            "grant all on zou_atk_shown to anon",
        ],
    )
    .await;
    let app = app(&dsn);

    // Discovery is a read, so it answers the same question the reads
    // answer: a table anon may not touch is not in anon's document,
    // while one it may touch is, which is what makes the absence mean
    // something.
    let res = app
        .clone()
        .oneshot(as_user("GET", "/rest/v1/", None, ""))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_text(res).await;
    assert!(body.contains("zou_atk_shown"), "the document is empty");
    assert!(
        !body.contains("zou_atk_private"),
        "the document listed a table anon cannot reach"
    );
}

#[tokio::test]
async fn the_role_switch_is_only_as_narrow_as_the_connection() {
    let Some(dsn) = dsn() else { return };
    docs(&dsn, "zou_atk_switch").await;
    let app = app(&dsn);
    let role = |r: &str| jwt::mint(&serde_json::json!({"role": r, "sub": U1}), SECRET);

    // A role that does not exist cannot be entered, and the session
    // fails before any query runs.
    let res = app
        .clone()
        .oneshot(as_user(
            "GET",
            "/rest/v1/zou_atk_switch",
            Some(&role("no_such_role")),
            "",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    let e: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(e["code"], "22023");

    // A role that exists but was never granted the table gets in and
    // then gets nothing, which is postgres deciding rather than zou.
    let res = app
        .clone()
        .oneshot(as_user(
            "GET",
            "/rest/v1/zou_atk_switch",
            Some(&role("pg_read_server_files")),
            "",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    let e: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(e["code"], "42501");

    // And a role the connection role may become is entered, superuser
    // included. That is issue #92: hosted Supabase connects as
    // authenticator, which was granted exactly anon, authenticated and
    // service_role and so cannot become anything else, while zou
    // connects as whatever the dsn names, which in development is
    // usually the superuser. This assertion pins today's behavior so
    // that closing #92 has to change it on purpose rather than by
    // accident, and it is written against the dsn's own user so it
    // stays true wherever the suite runs.
    let me = std::env::var("ZOU_PG_TEST_DSN")
        .ok()
        .and_then(|d| {
            d.split_whitespace()
                .find_map(|kv| kv.strip_prefix("user=").map(str::to_string))
        })
        .unwrap_or_else(|| "postgres".to_string());
    let res = app
        .clone()
        .oneshot(as_user(
            "GET",
            "/rest/v1/zou_atk_switch",
            Some(&role(&me)),
            "",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let rows: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(
        rows.as_array().unwrap().len(),
        2,
        "the dsn's own role reads past the policy, see #92"
    );
}
