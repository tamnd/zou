//! The admin surface, `/auth/v1/admin/users`, against a live postgres.
//!
//! These endpoints are the service role acting on somebody else's
//! account, so the two things worth pinning are the gate in front of
//! them and the consequence behind them. The gate is not a status code
//! to assert once: an anon key, a signed in person's own token, and no
//! token at all each get a different refusal, and a project that let any
//! of the three through would be handing every account to the internet.
//!
//! Behind the gate the assertions are about the row rather than the 200.
//! A ban is a sign in that fails afterwards, a password change is the
//! old session no longer working, a hard delete is a row that is gone,
//! and a soft delete is a row that is still there with nothing in it
//! that names anybody.
//!
//! The audience header is used throughout to give each test its own
//! corner of auth.users, because the list endpoint is scoped by audience
//! and the suites share a database.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test auth_admin

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use zou_server::sql::Pool;
use zou_server::{Config, jwt, mail, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";
const SITE: &str = "https://app.zou.test";

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

/// A project that confirms its own signups and mails nothing, which is
/// all any of these tests need out of the rest of the surface.
fn app(dsn: &str) -> axum::Router {
    router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        external_url: Some("https://zou.test".to_string()),
        site_url: Some(SITE.to_string()),
        mailer_autoconfirm: true,
        mail: mail::Settings {
            max_frequency: 0,
            ..mail::Settings::default()
        },
        ..Config::default()
    })
    .expect("router builds")
}

fn anon_key() -> String {
    jwt::mint(&jwt::key_claims("anon"), SECRET)
}

fn service_key() -> String {
    jwt::mint(&jwt::key_claims("service_role"), SECRET)
}

struct Answer {
    status: StatusCode,
    body: serde_json::Value,
    headers: axum::http::HeaderMap,
}

impl Answer {
    fn refusal(&self) -> (u16, &str, &str) {
        (
            self.status.as_u16(),
            self.body["error_code"].as_str().unwrap_or("<none>"),
            self.body["msg"].as_str().unwrap_or("<none>"),
        )
    }
    fn header(&self, name: &str) -> String {
        self.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string()
    }
    fn emails(&self) -> Vec<String> {
        self.body["users"]
            .as_array()
            .expect("a list of users")
            .iter()
            .map(|u| u["email"].as_str().unwrap_or_default().to_string())
            .collect()
    }
}

async fn answer(res: axum::response::Response) -> Answer {
    let status = res.status();
    let headers = res.headers().clone();
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let body = match bytes.is_empty() {
        true => serde_json::Value::Null,
        false => serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| panic!("not json: {}", String::from_utf8_lossy(&bytes))),
    };
    Answer {
        status,
        body,
        headers,
    }
}

/// A request from whoever is holding `token`, in the audience `aud`.
/// Every admin route reads the audience header the same way GoTrue's
/// requestAud does, which is what keeps these tests out of each other's
/// way.
async fn sent(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    aud: &str,
    body: serde_json::Value,
) -> Answer {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("apikey", anon_key())
        .header("content-type", "application/json");
    if !token.is_empty() {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    if !aud.is_empty() {
        req = req.header("x-jwt-aud", aud);
    }
    let req = req.body(Body::from(body.to_string())).unwrap();
    answer(app.clone().oneshot(req).await.expect("router answers")).await
}

async fn admin(
    app: &axum::Router,
    method: &str,
    path: &str,
    aud: &str,
    body: serde_json::Value,
) -> Answer {
    sent(app, method, path, &service_key(), aud, body).await
}

async fn create(app: &axum::Router, aud: &str, body: serde_json::Value) -> Answer {
    admin(app, "POST", "/auth/v1/admin/users", aud, body).await
}

async fn fetch(app: &axum::Router, user_id: &str) -> Answer {
    admin(
        app,
        "GET",
        &format!("/auth/v1/admin/users/{user_id}"),
        "",
        serde_json::json!({}),
    )
    .await
}

async fn update(app: &axum::Router, user_id: &str, body: serde_json::Value) -> Answer {
    admin(
        app,
        "PUT",
        &format!("/auth/v1/admin/users/{user_id}"),
        "",
        body,
    )
    .await
}

async fn delete(app: &axum::Router, user_id: &str, body: serde_json::Value) -> Answer {
    admin(
        app,
        "DELETE",
        &format!("/auth/v1/admin/users/{user_id}"),
        "",
        body,
    )
    .await
}

async fn listed(app: &axum::Router, aud: &str, query: &str) -> Answer {
    let path = match query {
        "" => "/auth/v1/admin/users".to_string(),
        q => format!("/auth/v1/admin/users?{q}"),
    };
    admin(app, "GET", &path, aud, serde_json::json!({})).await
}

/// An ordinary signup, for the tests that need a session that was not
/// minted by an admin.
async fn signed_up(app: &axum::Router, email: &str, password: &str) -> (String, String) {
    let req = Request::builder()
        .method("POST")
        .uri("/auth/v1/signup")
        .header("apikey", anon_key())
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({"email": email, "password": password}).to_string(),
        ))
        .unwrap();
    let answer = answer(app.clone().oneshot(req).await.expect("router answers")).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    (
        answer.body["user"]["id"].as_str().expect("id").to_string(),
        answer.body["access_token"]
            .as_str()
            .expect("access_token")
            .to_string(),
    )
}

async fn signs_in(app: &axum::Router, email: &str, password: &str) -> Answer {
    let req = Request::builder()
        .method("POST")
        .uri("/auth/v1/token?grant_type=password")
        .header("apikey", anon_key())
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({"email": email, "password": password}).to_string(),
        ))
        .unwrap();
    answer(app.clone().oneshot(req).await.expect("router answers")).await
}

async fn user_get(app: &axum::Router, token: &str) -> Answer {
    sent(
        app,
        "GET",
        "/auth/v1/user",
        token,
        "",
        serde_json::json!({}),
    )
    .await
}

fn address(tag: &str) -> String {
    format!("{tag}@zou.test")
}

/// A private audience for one test, so the list endpoint sees only what
/// that test put there.
fn audience(tag: &str) -> String {
    format!("admin-{tag}")
}

async fn wipe(pool: &Pool, aud: &str) {
    let sess = pool.unscoped().await.expect("connect");
    sess.execute("delete from auth.users where aud = $1", &[&aud])
        .await
        .expect("clear any leftover");
    sess.commit().await.expect("park");
}

async fn wipe_email(pool: &Pool, email: &str) {
    let sess = pool.unscoped().await.expect("connect");
    sess.execute("delete from auth.users where email = $1", &[&email])
        .await
        .expect("clear any leftover");
    sess.commit().await.expect("park");
}

async fn scalar<T>(pool: &Pool, sql: &str, args: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> T
where
    T: for<'a> tokio_postgres::types::FromSql<'a>,
{
    let sess = pool.unscoped().await.expect("connect");
    let value = sess.query(sql, args).await.expect(sql)[0].get(0);
    sess.commit().await.expect("park");
    value
}

async fn run(pool: &Pool, sql: &str, args: &[&(dyn tokio_postgres::types::ToSql + Sync)]) {
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(sql, args).await.expect(sql);
    sess.commit().await.expect("write");
}

fn claims_of(token: &str) -> serde_json::Value {
    jwt::verify(token, SECRET)
        .expect("the access token verifies")
        .claims
}

/// GoTrue's obfuscateValue, written out again here rather than reached
/// for in the server, because a test that calls the code under test to
/// work out what it expects is not a test.
fn obfuscated(user_id: &str, value: &str) -> String {
    use base64ct::Encoding;
    use sha2::Digest;
    base64ct::Base64UrlUnpadded::encode_string(&sha2::Sha256::digest(
        format!("{user_id}{value}").as_bytes(),
    ))
}

// The gate.

#[tokio::test]
async fn only_an_admin_role_gets_through_the_admin_door() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let aud = audience("gate");

    // No token at all. The apikey got past the front door and no
    // further, and the wording is the one every GoTrue endpoint that
    // needs a bearer uses.
    let none = sent(
        &app,
        "GET",
        "/auth/v1/admin/users",
        "",
        &aud,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        none.refusal(),
        (
            401,
            "no_authorization",
            "This endpoint requires a valid Bearer token"
        )
    );

    // The anon key as a bearer. It is a real signed token and it is
    // public, which is exactly why it is refused here.
    let anon = sent(
        &app,
        "GET",
        "/auth/v1/admin/users",
        &anon_key(),
        &aud,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(anon.refusal(), (403, "not_admin", "User not allowed"));

    // A signed in person's own token. Being somebody is not being an
    // admin, and this is the refusal that stops one account reading
    // another.
    let pool = Pool::new(&dsn, 4).expect("pool");
    let email = address("admin-gate-person");
    wipe_email(&pool, &email).await;
    let (_, token) = signed_up(&app, &email, "correct horse").await;
    let person = sent(
        &app,
        "GET",
        "/auth/v1/admin/users",
        &token,
        &aud,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(person.refusal(), (403, "not_admin", "User not allowed"));

    // And the service role, which is what a dashboard holds.
    let admin = listed(&app, &aud, "").await;
    assert_eq!(admin.status, StatusCode::OK, "{}", admin.body);

    // Every verb on both routes is behind the same door, so a project
    // cannot be one refactor away from an open write endpoint.
    for (method, path) in [
        ("POST", "/auth/v1/admin/users"),
        (
            "GET",
            "/auth/v1/admin/users/00000000-0000-0000-0000-000000000001",
        ),
        (
            "PUT",
            "/auth/v1/admin/users/00000000-0000-0000-0000-000000000001",
        ),
        (
            "DELETE",
            "/auth/v1/admin/users/00000000-0000-0000-0000-000000000001",
        ),
    ] {
        let refused = sent(&app, method, path, &anon_key(), &aud, serde_json::json!({})).await;
        assert_eq!(
            refused.refusal(),
            (403, "not_admin", "User not allowed"),
            "{method} {path}"
        );
    }
}

#[tokio::test]
async fn an_admin_token_that_names_a_gone_session_is_refused() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = Pool::new(&dsn, 4).expect("pool");
    let email = address("admin-holder");
    wipe_email(&pool, &email).await;
    let (user_id, token) = signed_up(&app, &email, "correct horse").await;

    // An account that holds an admin role and a session, which is the
    // one kind of admin token that names a person at all. A service key
    // carries no sub claim, so nothing about it can go stale.
    run(
        &pool,
        "update auth.users set role = 'service_role' where id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    let (_, fresh) = {
        let answer = signs_in(&app, &email, "correct horse").await;
        assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
        (
            answer.body["user"]["id"].as_str().unwrap().to_string(),
            answer.body["access_token"].as_str().unwrap().to_string(),
        )
    };
    assert_eq!(claims_of(&fresh)["role"], "service_role");
    let allowed = sent(
        &app,
        "GET",
        "/auth/v1/admin/users",
        &fresh,
        &audience("holder"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(allowed.status, StatusCode::OK, "{}", allowed.body);

    // Take the session away. The token is still signed and still inside
    // its hour, and it stops working, which is the whole point of the
    // check.
    run(
        &pool,
        "delete from auth.sessions where user_id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    let stale = sent(
        &app,
        "GET",
        "/auth/v1/admin/users",
        &fresh,
        &audience("holder"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        stale.refusal(),
        (
            403,
            "session_not_found",
            "Session from session_id claim in JWT does not exist"
        )
    );
    let _ = token;
}

// Creating.

#[tokio::test]
async fn an_admin_makes_an_account_somebody_else_will_sign_in_to() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = Pool::new(&dsn, 4).expect("pool");
    let email = address("admin-create");
    wipe_email(&pool, &email).await;

    // No audience header, so the account lands in the default one. The
    // password grant looks there and nowhere else, which is what lets
    // this test finish by signing in as the account it just made.
    let made = create(
        &app,
        "",
        serde_json::json!({
            "email": &email,
            "password": "correct horse",
            "email_confirm": true,
            "user_metadata": {"full_name": "Ada"},
            "app_metadata": {"plan": "team"},
        }),
    )
    .await;
    assert_eq!(made.status, StatusCode::OK, "{}", made.body);
    let user = &made.body;
    assert_eq!(user["email"], email.as_str());
    assert_eq!(user["aud"], "authenticated");
    assert_eq!(user["role"], "authenticated");
    assert_eq!(user["user_metadata"]["full_name"], "Ada");
    // The provider keys go on first and the request's own app_metadata
    // is merged over them, so a project can carry its own keys without
    // losing what the account is signed in with.
    assert_eq!(user["app_metadata"]["provider"], "email");
    assert_eq!(user["app_metadata"]["providers"][0], "email");
    assert_eq!(user["app_metadata"]["plan"], "team");
    assert!(user["email_confirmed_at"].is_string(), "{user}");
    // And no confirmed_at, because upstream answers a create with the
    // struct it just built and that column is generated, so it is not
    // in the answer here and is in the answer to a fetch of the same
    // account a moment later.
    assert!(user.get("confirmed_at").is_none(), "{user}");

    // One email identity, and it still says the address is unproven.
    // The admin asserted it, not the provider, and upstream writes the
    // identity unverified and never goes back, so a client reading
    // identity_data here sees what it has always seen. The account
    // itself is confirmed, which is what email_confirm was about.
    let identities = user["identities"].as_array().expect("identities");
    assert_eq!(identities.len(), 1, "{user}");
    assert_eq!(identities[0]["provider"], "email");
    assert_eq!(identities[0]["identity_data"]["email"], email.as_str());
    assert_eq!(identities[0]["identity_data"]["email_verified"], false);

    // And the account works: the password the admin set signs in.
    let session = signs_in(&app, &email, "correct horse").await;
    assert_eq!(session.status, StatusCode::OK, "{}", session.body);
    assert_eq!(session.body["user"]["id"], user["id"]);
}

#[tokio::test]
async fn an_account_made_with_no_password_cannot_be_signed_in_to() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = Pool::new(&dsn, 4).expect("pool");
    let aud = audience("nopassword");
    wipe(&pool, &aud).await;
    let email = address("admin-nopassword");

    let made = create(
        &app,
        &aud,
        serde_json::json!({"email": &email, "email_confirm": true}),
    )
    .await;
    assert_eq!(made.status, StatusCode::OK, "{}", made.body);
    let user_id = made.body["id"].as_str().expect("id").to_string();

    // Upstream writes a hash of a password nobody will ever see rather
    // than leaving the column empty, which is the difference between an
    // account that cannot be signed in to and an account with no lock
    // on it.
    let hash: String = scalar(
        &pool,
        "select coalesce(encrypted_password, '') from auth.users where id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert!(hash.starts_with("$2"), "a bcrypt hash, got {hash:?}");
    let empty = signs_in(&app, &email, "").await;
    assert_eq!(empty.status, StatusCode::BAD_REQUEST, "{}", empty.body);

    // A hash can be handed over directly, which is what a migration off
    // another auth system does, and it signs in as itself.
    let moved = address("admin-migrated");
    wipe_email(&pool, &moved).await;
    let same = create(
        &app,
        &aud,
        serde_json::json!({
            "email": &moved,
            "email_confirm": true,
            // bcrypt of "correct horse", cost 10.
            "password_hash": "$2b$10$Y2nJ1Q0kZ7qYt3l9EpJ5Ue3zZ6QAOBqvUdV5tS6qcgvFj5FQzKZ0O",
        }),
    )
    .await;
    assert_eq!(same.status, StatusCode::OK, "{}", same.body);
    let stored: String = scalar(
        &pool,
        "select encrypted_password from auth.users where email = $1",
        &[&moved],
    )
    .await;
    assert_eq!(
        stored, "$2b$10$Y2nJ1Q0kZ7qYt3l9EpJ5Ue3zZ6QAOBqvUdV5tS6qcgvFj5FQzKZ0O",
        "the hash is stored as it was handed over"
    );
}

#[tokio::test]
async fn creating_refuses_what_upstream_refuses() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = Pool::new(&dsn, 4).expect("pool");
    let aud = audience("createrefuse");
    wipe(&pool, &aud).await;

    let nothing = create(&app, &aud, serde_json::json!({})).await;
    assert_eq!(
        nothing.refusal(),
        (
            400,
            "validation_failed",
            "Cannot create a user without either an email or phone"
        )
    );

    let email = address("admin-dup");
    wipe_email(&pool, &email).await;
    let first = create(&app, &aud, serde_json::json!({"email": &email})).await;
    assert_eq!(first.status, StatusCode::OK, "{}", first.body);
    let again = create(&app, &aud, serde_json::json!({"email": &email})).await;
    assert_eq!(
        again.refusal(),
        (
            422,
            "email_exists",
            "A user with this email address has already been registered"
        )
    );

    let both = create(
        &app,
        &aud,
        serde_json::json!({
            "email": address("admin-both"),
            "password": "correct horse",
            "password_hash": "$2b$10$Y2nJ1Q0kZ7qYt3l9EpJ5Ue3zZ6QAOBqvUdV5tS6qcgvFj5FQzKZ0O",
        }),
    )
    .await;
    assert_eq!(
        both.refusal(),
        (
            400,
            "validation_failed",
            "Only a password or a password hash should be provided"
        )
    );

    let short = create(
        &app,
        &aud,
        serde_json::json!({"email": address("admin-short"), "password": "abc"}),
    )
    .await;
    assert_eq!(
        short.status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        short.body
    );
    assert_eq!(short.body["error_code"], "weak_password");

    let bad_id = create(
        &app,
        &aud,
        serde_json::json!({"email": address("admin-badid"), "id": "not-a-uuid"}),
    )
    .await;
    assert_eq!(
        bad_id.refusal(),
        (
            400,
            "validation_failed",
            "ID must conform to the uuid v4 format"
        )
    );

    let nil = create(
        &app,
        &aud,
        serde_json::json!({
            "email": address("admin-nilid"),
            "id": "00000000-0000-0000-0000-000000000000",
        }),
    )
    .await;
    assert_eq!(
        nil.refusal(),
        (400, "validation_failed", "ID cannot be a nil uuid")
    );

    let ban = create(
        &app,
        &aud,
        serde_json::json!({"email": address("admin-badban"), "ban_duration": "forever"}),
    )
    .await;
    assert_eq!(
        ban.refusal(),
        (
            400,
            "validation_failed",
            "invalid format for ban duration: time: invalid duration \"forever\""
        )
    );

    // The address is validated the same way it is on the open surface,
    // and an account with a phone number is a surface this end does not
    // serve yet rather than one it pretends to.
    let shape = create(&app, &aud, serde_json::json!({"email": "not an address"})).await;
    assert_eq!(shape.status, StatusCode::BAD_REQUEST, "{}", shape.body);
    assert_eq!(shape.body["error_code"], "validation_failed");
    let phone = create(
        &app,
        &aud,
        serde_json::json!({"email": address("admin-phone"), "phone": "+15550100"}),
    )
    .await;
    assert_eq!(phone.status, StatusCode::NOT_IMPLEMENTED, "{}", phone.body);

    // Nothing that was refused left a row behind.
    let count: i64 = scalar(
        &pool,
        "select count(*) from auth.users where aud = $1",
        &[&aud],
    )
    .await;
    assert_eq!(count, 1, "only the one account that was allowed");
}

#[tokio::test]
async fn an_account_can_be_made_with_the_id_and_the_role_it_is_asked_for() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = Pool::new(&dsn, 4).expect("pool");
    let aud = audience("customid");
    wipe(&pool, &aud).await;
    let email = address("admin-customid");
    let wanted = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";

    // A custom id is what a migration from another system needs, so
    // that rows already pointing at the old id keep pointing at
    // something.
    let made = create(
        &app,
        &aud,
        serde_json::json!({"email": &email, "id": wanted, "role": "moderator"}),
    )
    .await;
    assert_eq!(made.status, StatusCode::OK, "{}", made.body);
    assert_eq!(made.body["id"], wanted);
    assert_eq!(made.body["role"], "moderator");
    // And the identity points at the account rather than at whatever id
    // the database would have generated.
    assert_eq!(made.body["identities"][0]["user_id"], wanted);
    assert_eq!(made.body["identities"][0]["identity_data"]["sub"], wanted);

    // The same id twice is the database's own refusal rather than a
    // silent second account.
    let twice = create(
        &app,
        &aud,
        serde_json::json!({"email": address("admin-customid-2"), "id": wanted}),
    )
    .await;
    assert_eq!(
        twice.status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "{}",
        twice.body
    );
}

// Reading.

#[tokio::test]
async fn an_admin_reads_an_account_it_did_not_make() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = Pool::new(&dsn, 4).expect("pool");
    let email = address("admin-read");
    wipe_email(&pool, &email).await;
    let (user_id, _) = signed_up(&app, &email, "correct horse").await;

    let read = fetch(&app, &user_id).await;
    assert_eq!(read.status, StatusCode::OK, "{}", read.body);
    assert_eq!(read.body["id"], user_id.as_str());
    assert_eq!(read.body["email"], email.as_str());
    assert_eq!(read.body["identities"][0]["provider"], "email");

    // An id that was never a uuid is a 404 rather than a 400. That is
    // upstream's, it reads like a slip, and a client that branches on
    // the status sees what it has always seen.
    let shape = fetch(&app, "not-a-uuid").await;
    assert_eq!(
        shape.refusal(),
        (404, "validation_failed", "user_id must be an UUID")
    );

    let missing = fetch(&app, "3f2504e0-4f89-41d3-9a0c-0305e82c3399").await;
    assert_eq!(missing.refusal(), (404, "user_not_found", "User not found"));
}

// Listing.

#[tokio::test]
async fn the_list_is_a_page_of_one_audience_newest_first() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = Pool::new(&dsn, 4).expect("pool");
    let aud = audience("list");
    wipe(&pool, &aud).await;

    // Three accounts, in a known order, each with a distinguishable
    // name so the filter has something to find.
    for (tag, name) in [
        ("alpha", "Ada"),
        ("beta", "Grace"),
        ("gamma", "Adalovelace"),
    ] {
        let made = create(
            &app,
            &aud,
            serde_json::json!({
                "email": address(&format!("admin-list-{tag}")),
                "user_metadata": {"full_name": name},
            }),
        )
        .await;
        assert_eq!(made.status, StatusCode::OK, "{}", made.body);
        // created_at is a timestamp and three rows written inside one
        // millisecond would tie, so the order is made explicit rather
        // than raced for.
        run(
            &pool,
            "update auth.users set created_at = now() - $2::double precision * interval '1 hour'
              where id = $1::text::uuid",
            &[
                &made.body["id"].as_str().unwrap(),
                &match tag {
                    "alpha" => 3.0f64,
                    "beta" => 2.0,
                    _ => 1.0,
                },
            ],
        )
        .await;
    }

    let all = listed(&app, &aud, "").await;
    assert_eq!(all.status, StatusCode::OK, "{}", all.body);
    assert_eq!(all.body["aud"], aud.as_str());
    assert_eq!(
        all.emails(),
        vec![
            address("admin-list-gamma"),
            address("admin-list-beta"),
            address("admin-list-alpha"),
        ],
        "newest first without being asked"
    );
    assert_eq!(all.header("x-total-count"), "3");
    // One page holds all three, so there is no next, and last is this
    // page. The link carries no /auth/v1 because upstream sits behind a
    // gateway that took the prefix off before it saw the request, and a
    // client following the link has to get what upstream's client gets.
    assert_eq!(all.header("link"), "</admin/users?page=1>; rel=\"last\"");

    let ascending = listed(&app, &aud, "sort=created_at+asc").await;
    assert_eq!(
        ascending.emails(),
        vec![
            address("admin-list-alpha"),
            address("admin-list-beta"),
            address("admin-list-gamma"),
        ]
    );

    // A page is a window, and the headers say where the rest of it is.
    let first = listed(&app, &aud, "per_page=2").await;
    assert_eq!(first.emails().len(), 2);
    assert_eq!(first.header("x-total-count"), "3");
    assert_eq!(
        first.header("link"),
        "</admin/users?page=2&per_page=2>; rel=\"next\", \
         </admin/users?page=2&per_page=2>; rel=\"last\""
    );
    let second = listed(&app, &aud, "per_page=2&page=2").await;
    assert_eq!(second.emails(), vec![address("admin-list-alpha")]);
    assert_eq!(
        second.header("link"),
        "</admin/users?page=2&per_page=2>; rel=\"last\"",
        "the last page does not point at a next one"
    );

    // The filter is upstream's: the address matched as written, the
    // full name matched whatever its case.
    let by_email = listed(&app, &aud, "filter=beta").await;
    assert_eq!(by_email.emails(), vec![address("admin-list-beta")]);
    assert_eq!(by_email.header("x-total-count"), "1");
    let by_name = listed(&app, &aud, "filter=ADA").await;
    assert_eq!(
        by_name.emails(),
        vec![address("admin-list-gamma"), address("admin-list-alpha")],
        "the name match ignores case and the address match does not"
    );
    let nothing = listed(&app, &aud, "filter=nobody").await;
    assert_eq!(nothing.emails().len(), 0);
    assert_eq!(nothing.header("x-total-count"), "0");

    // The identity list is filled in, which upstream leaves null here.
    assert_eq!(
        by_email.body["users"][0]["identities"][0]["provider"],
        "email"
    );

    // And nothing from another audience leaks into this one.
    let elsewhere = listed(&app, &audience("list-empty"), "").await;
    assert_eq!(elsewhere.emails().len(), 0);
}

#[tokio::test]
async fn the_list_refuses_parameters_it_cannot_read() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let aud = audience("listrefuse");

    let field = listed(&app, &aud, "sort=email+asc").await;
    assert_eq!(
        field.refusal(),
        (
            400,
            "validation_failed",
            "Bad Sort Parameters: bad field for sort 'email'"
        )
    );

    let dir = listed(&app, &aud, "sort=created_at+sideways").await;
    assert_eq!(
        dir.refusal(),
        (
            400,
            "validation_failed",
            "Bad Sort Parameters: bad direction for sort 'sideways', only 'asc' and 'desc' allowed"
        )
    );

    let page = listed(&app, &aud, "page=many").await;
    assert_eq!(
        page.refusal(),
        (
            400,
            "validation_failed",
            "Bad Pagination Parameters: strconv.ParseUint: parsing \"many\": invalid syntax"
        )
    );

    let per_page = listed(&app, &aud, "per_page=-1").await;
    assert_eq!(
        per_page.refusal(),
        (
            400,
            "validation_failed",
            "Bad Pagination Parameters: strconv.ParseUint: parsing \"-1\": invalid syntax"
        )
    );

    // Both wrong, and the sort is the one that answers, because that is
    // the order upstream parses them in.
    let both = listed(&app, &aud, "sort=email+asc&page=many").await;
    assert_eq!(
        both.refusal(),
        (
            400,
            "validation_failed",
            "Bad Sort Parameters: bad field for sort 'email'"
        )
    );
}

// Updating.

#[tokio::test]
async fn an_admin_changes_an_account_without_asking_it_anything() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = Pool::new(&dsn, 4).expect("pool");
    let email = address("admin-update");
    let moved = address("admin-update-moved");
    wipe_email(&pool, &email).await;
    wipe_email(&pool, &moved).await;

    let made = create(
        &app,
        "",
        serde_json::json!({
            "email": &email,
            "password": "correct horse",
            "email_confirm": true,
            "user_metadata": {"full_name": "Ada", "colour": "green"},
            "app_metadata": {"plan": "free"},
        }),
    )
    .await;
    let user_id = made.body["id"].as_str().expect("id").to_string();

    let changed = update(
        &app,
        &user_id,
        serde_json::json!({
            "email": &moved,
            "email_confirm": true,
            "role": "moderator",
            "user_metadata": {"colour": null, "shape": "round"},
            "app_metadata": {"plan": "team"},
        }),
    )
    .await;
    assert_eq!(changed.status, StatusCode::OK, "{}", changed.body);
    // The address moves without a mail, because the admin asserting it
    // is what confirms it.
    assert_eq!(changed.body["email"], moved.as_str());
    assert_eq!(changed.body["new_email"], serde_json::Value::Null);
    assert_eq!(changed.body["role"], "moderator");
    // A key sent as null is a deletion, the rest is a merge.
    assert_eq!(changed.body["user_metadata"]["full_name"], "Ada");
    assert_eq!(changed.body["user_metadata"]["shape"], "round");
    assert_eq!(
        changed.body["user_metadata"].get("colour"),
        None,
        "a null key is removed rather than stored"
    );
    assert_eq!(changed.body["app_metadata"]["plan"], "team");
    assert_eq!(changed.body["app_metadata"]["provider"], "email");
    // The identity moved with it, so the account is still signed in to
    // through the address it now holds.
    assert_eq!(
        changed.body["identities"][0]["identity_data"]["email"],
        moved.as_str()
    );
    assert_eq!(
        changed.body["identities"][0]["identity_data"]["email_verified"],
        true
    );

    // The new address signs in and the old one does not.
    let now = signs_in(&app, &moved, "correct horse").await;
    assert_eq!(now.status, StatusCode::OK, "{}", now.body);
    assert_eq!(
        claims_of(now.body["access_token"].as_str().unwrap())["role"],
        "moderator"
    );
    let before = signs_in(&app, &email, "correct horse").await;
    assert_eq!(before.status, StatusCode::BAD_REQUEST, "{}", before.body);
}

#[tokio::test]
async fn a_password_set_by_an_admin_takes_the_account_back() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = Pool::new(&dsn, 4).expect("pool");
    let email = address("admin-password");
    wipe_email(&pool, &email).await;
    let (user_id, token) = signed_up(&app, &email, "correct horse").await;

    let before = user_get(&app, &token).await;
    assert_eq!(before.status, StatusCode::OK, "{}", before.body);

    let changed = update(
        &app,
        &user_id,
        serde_json::json!({"password": "battery staple horse"}),
    )
    .await;
    assert_eq!(changed.status, StatusCode::OK, "{}", changed.body);

    // Whoever was holding a session on the account is not any more.
    // This is the case the endpoint exists for: an account being taken
    // back from somebody, and a session that outlived the password
    // would give it straight back.
    let after = user_get(&app, &token).await;
    assert_eq!(
        after.refusal(),
        (
            403,
            "session_not_found",
            "Session from session_id claim in JWT does not exist"
        )
    );
    let old = signs_in(&app, &email, "correct horse").await;
    assert_eq!(old.status, StatusCode::BAD_REQUEST, "{}", old.body);
    let new = signs_in(&app, &email, "battery staple horse").await;
    assert_eq!(new.status, StatusCode::OK, "{}", new.body);

    let weak = update(&app, &user_id, serde_json::json!({"password": "abc"})).await;
    assert_eq!(
        weak.status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        weak.body
    );
    assert_eq!(weak.body["error_code"], "weak_password");
    // And the refusal left the password that was there alone.
    let still = signs_in(&app, &email, "battery staple horse").await;
    assert_eq!(still.status, StatusCode::OK, "{}", still.body);
}

#[tokio::test]
async fn a_ban_is_a_sign_in_that_stops_working_until_it_is_lifted() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = Pool::new(&dsn, 4).expect("pool");
    let email = address("admin-ban");
    wipe_email(&pool, &email).await;

    let made = create(
        &app,
        "",
        serde_json::json!({
            "email": &email,
            "password": "correct horse",
            "email_confirm": true,
            "ban_duration": "24h",
        }),
    )
    .await;
    assert_eq!(made.status, StatusCode::OK, "{}", made.body);
    let user_id = made.body["id"].as_str().expect("id").to_string();
    assert!(made.body["banned_until"].is_string(), "{}", made.body);

    // A day, to the second, read back from the row rather than from the
    // answer, because the answer is the thing being tested.
    let seconds: f64 = scalar(
        &pool,
        "select extract(epoch from (banned_until - now()))::double precision
           from auth.users where id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert!(
        (seconds - 86_400.0).abs() < 60.0,
        "a day from now, got {seconds}"
    );

    // A 400 rather than a 403, which is what upstream's password grant
    // answers even though every other path it bans on answers 403.
    let banned = signs_in(&app, &email, "correct horse").await;
    assert_eq!(banned.refusal(), (400, "user_banned", "User is banned"));

    // "none" is the only way a client has of undoing one.
    let lifted = update(&app, &user_id, serde_json::json!({"ban_duration": "none"})).await;
    assert_eq!(lifted.status, StatusCode::OK, "{}", lifted.body);
    assert_eq!(lifted.body.get("banned_until"), None, "{}", lifted.body);
    let allowed = signs_in(&app, &email, "correct horse").await;
    assert_eq!(allowed.status, StatusCode::OK, "{}", allowed.body);

    // Saying nothing about the ban leaves it where it is, which is what
    // makes every other field on this endpoint safe to send on its own.
    let again = update(&app, &user_id, serde_json::json!({"ban_duration": "1h30m"})).await;
    assert_eq!(again.status, StatusCode::OK, "{}", again.body);
    let quiet = update(&app, &user_id, serde_json::json!({"role": "authenticated"})).await;
    assert_eq!(quiet.status, StatusCode::OK, "{}", quiet.body);
    let left: f64 = scalar(
        &pool,
        "select extract(epoch from (banned_until - now()))::double precision
           from auth.users where id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert!(
        (left - 5_400.0).abs() < 60.0,
        "still an hour and a half, got {left}"
    );

    let bad = update(
        &app,
        &user_id,
        serde_json::json!({"ban_duration": "forever"}),
    )
    .await;
    assert_eq!(
        bad.refusal(),
        (
            400,
            "validation_failed",
            "invalid format for ban duration: time: invalid duration \"forever\""
        )
    );
}

#[tokio::test]
async fn updating_refuses_the_same_things_reading_does() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);

    let shape = update(&app, "not-a-uuid", serde_json::json!({"role": "admin"})).await;
    assert_eq!(
        shape.refusal(),
        (404, "validation_failed", "user_id must be an UUID")
    );
    let missing = update(
        &app,
        "3f2504e0-4f89-41d3-9a0c-0305e82c3398",
        serde_json::json!({"role": "admin"}),
    )
    .await;
    assert_eq!(missing.refusal(), (404, "user_not_found", "User not found"));
}

#[tokio::test]
async fn an_account_with_no_address_gets_one_and_keeps_its_id() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = Pool::new(&dsn, 4).expect("pool");
    let email = address("admin-anon-converted");
    wipe_email(&pool, &email).await;

    // An anonymous account, made here by hand rather than through the
    // endpoint, because this test is about the update and not about the
    // sign in.
    let user_id: String = scalar(
        &pool,
        "insert into auth.users (instance_id, id, aud, role, created_at, updated_at,
                                 is_anonymous, is_sso_user, raw_app_meta_data,
                                 raw_user_meta_data)
         values ('00000000-0000-0000-0000-000000000000', gen_random_uuid(),
                 'authenticated', 'authenticated', now(), now(), true, false,
                 '{}'::jsonb, '{}'::jsonb)
         returning id::text",
        &[],
    )
    .await;

    let given = update(
        &app,
        &user_id,
        serde_json::json!({"email": &email, "email_confirm": true}),
    )
    .await;
    assert_eq!(given.status, StatusCode::OK, "{}", given.body);
    assert_eq!(given.body["id"], user_id.as_str());
    assert_eq!(given.body["email"], email.as_str());
    assert_eq!(given.body["is_anonymous"], false);
    // The account had no identity at all, so one is made rather than
    // updated, and everything already written against the id stays
    // where it is because the id did not move.
    let identities = given.body["identities"].as_array().expect("identities");
    assert_eq!(identities.len(), 1, "{}", given.body);
    assert_eq!(identities[0]["identity_data"]["email"], email.as_str());
    assert_eq!(identities[0]["identity_data"]["email_verified"], true);

    // Without email_confirm the account takes the address and stays
    // anonymous, because nobody has vouched for it.
    let other = address("admin-anon-unconfirmed");
    wipe_email(&pool, &other).await;
    let quiet: String = scalar(
        &pool,
        "insert into auth.users (instance_id, id, aud, role, created_at, updated_at,
                                 is_anonymous, is_sso_user, raw_app_meta_data,
                                 raw_user_meta_data)
         values ('00000000-0000-0000-0000-000000000000', gen_random_uuid(),
                 'authenticated', 'authenticated', now(), now(), true, false,
                 '{}'::jsonb, '{}'::jsonb)
         returning id::text",
        &[],
    )
    .await;
    let taken = update(&app, &quiet, serde_json::json!({"email": &other})).await;
    assert_eq!(taken.status, StatusCode::OK, "{}", taken.body);
    assert_eq!(taken.body["is_anonymous"], true);
    assert_eq!(
        taken.body["identities"][0]["identity_data"]["email_verified"],
        false
    );
    assert_eq!(taken.body.get("email_confirmed_at"), None, "{}", taken.body);
}

// Deleting.

#[tokio::test]
async fn a_hard_delete_leaves_nothing_behind() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = Pool::new(&dsn, 4).expect("pool");
    let email = address("admin-delete");
    wipe_email(&pool, &email).await;
    let (user_id, token) = signed_up(&app, &email, "correct horse").await;

    // No body at all, which is a hard delete rather than a parse error,
    // because upstream only reads the body when there is one.
    let req = Request::builder()
        .method("DELETE")
        .uri(format!("/auth/v1/admin/users/{user_id}"))
        .header("apikey", anon_key())
        .header("authorization", format!("Bearer {}", service_key()))
        .body(Body::empty())
        .unwrap();
    let gone = answer(app.clone().oneshot(req).await.expect("router answers")).await;
    assert_eq!(gone.status, StatusCode::OK, "{}", gone.body);
    assert_eq!(gone.body, serde_json::json!({}));

    let rows: i64 = scalar(
        &pool,
        "select count(*) from auth.users where id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert_eq!(rows, 0, "the row is gone");
    let identities: i64 = scalar(
        &pool,
        "select count(*) from auth.identities where user_id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert_eq!(identities, 0, "and so is everything hanging off it");

    // The session it was holding stops resolving, which is what makes
    // the deletion mean anything to whoever was signed in.
    let after = user_get(&app, &token).await;
    assert_eq!(
        after.refusal(),
        (
            403,
            "user_not_found",
            "User from sub claim in JWT does not exist"
        )
    );

    let twice = delete(&app, &user_id, serde_json::json!({})).await;
    assert_eq!(twice.refusal(), (404, "user_not_found", "User not found"));
}

#[tokio::test]
async fn a_soft_delete_leaves_a_row_that_names_nobody() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = Pool::new(&dsn, 4).expect("pool");
    let email = address("admin-soft");
    wipe_email(&pool, &email).await;
    let (user_id, token) = signed_up(&app, &email, "correct horse").await;
    run(
        &pool,
        "update auth.users set raw_user_meta_data = '{\"full_name\": \"Ada\"}'::jsonb
          where id = $1::text::uuid",
        &[&user_id],
    )
    .await;

    let soft = delete(
        &app,
        &user_id,
        serde_json::json!({"should_soft_delete": true}),
    )
    .await;
    assert_eq!(soft.status, StatusCode::OK, "{}", soft.body);
    assert_eq!(soft.body, serde_json::json!({}));

    // The row is still there, which is the whole reason to ask for this
    // rather than the other one: an application's own rows pointing at
    // auth.users still resolve.
    let deleted_at: bool = scalar(
        &pool,
        "select deleted_at is not null from auth.users where id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert!(deleted_at, "the row is kept and marked");

    // And it names nobody. The hash is over the id and the value
    // together, so the same address on two accounts leaves two
    // different tombstones and neither can be read back.
    let stored: String = scalar(
        &pool,
        "select email from auth.users where id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert_eq!(stored, obfuscated(&user_id, &email));
    assert_ne!(stored, email);
    let password: bool = scalar(
        &pool,
        "select encrypted_password is null from auth.users where id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert!(password, "the password is gone");
    let metadata: String = scalar(
        &pool,
        "select raw_user_meta_data::text from auth.users where id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert_eq!(metadata, "{}", "nothing the account said about itself");
    let tokens: String = scalar(
        &pool,
        "select confirmation_token || recovery_token || email_change_token_current
                || email_change_token_new || phone_change_token
           from auth.users where id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert_eq!(tokens, "", "no live code can be spent against it");

    // The identity keeps its row, because deleting it would take the
    // foreign key with it, and loses everything it said.
    let data: String = scalar(
        &pool,
        "select identity_data::text from auth.identities where user_id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert_eq!(data, "{}");
    let provider_id: String = scalar(
        &pool,
        "select provider_id from auth.identities where user_id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert_eq!(
        provider_id,
        obfuscated(&user_id, &format!("email:{user_id}"))
    );

    // Nobody is signed in to it any more.
    let sessions: i64 = scalar(
        &pool,
        "select count(*) from auth.sessions where user_id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert_eq!(sessions, 0);
    let after = user_get(&app, &token).await;
    assert_eq!(
        after.refusal(),
        (
            403,
            "user_not_found",
            "User from sub claim in JWT does not exist"
        )
    );
    // And the address is free again for somebody else to register.
    let reused = signed_up(&app, &email, "correct horse").await;
    assert_ne!(reused.0, user_id);

    // Asking twice changes nothing, rather than hashing the hash.
    let again = delete(
        &app,
        &user_id,
        serde_json::json!({"should_soft_delete": true}),
    )
    .await;
    assert_eq!(again.status, StatusCode::OK, "{}", again.body);
    let unchanged: String = scalar(
        &pool,
        "select email from auth.users where id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert_eq!(unchanged, stored, "the tombstone is left alone");

    // A soft deleted account is gone as far as the admin surface is
    // concerned only in the sense that it can still be read; it is the
    // sign in and the session that are closed.
    let read = fetch(&app, &user_id).await;
    assert_eq!(read.status, StatusCode::OK, "{}", read.body);
    assert!(read.body["deleted_at"].is_string(), "{}", read.body);
}

#[tokio::test]
async fn deleting_refuses_what_reading_refuses() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);

    let shape = delete(&app, "not-a-uuid", serde_json::json!({})).await;
    assert_eq!(
        shape.refusal(),
        (404, "validation_failed", "user_id must be an UUID")
    );
    let missing = delete(
        &app,
        "3f2504e0-4f89-41d3-9a0c-0305e82c3397",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(missing.refusal(), (404, "user_not_found", "User not found"));
}
