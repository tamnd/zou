//! Sessions, access tokens and the refresh_token grant against a live
//! postgres.
//!
//! What is being pinned here is GoTrue's behaviour, not zou's: the
//! claim set a client and an RLS policy read, the three rows a session
//! is made of, and rotation with reuse detection down to the error_code
//! in the body. A client that branches on any of it sees the same thing
//! it would see against hosted Supabase.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test auth_token

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use zou_server::sql::{Pool, Session};
use zou_server::{Config, auth, jwt, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

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
        rate: None,
        jwks: None,
        schemas: vec![],
        external_url: Some("https://zou.test".to_string()),
        jwt_keys: None,
    })
    .expect("router builds")
}

/// The legacy signer, which is what a project with no signing keys
/// configured issues on.
fn signer() -> jwt::Signer<'static> {
    jwt::Signer::Secret(SECRET)
}

fn anon_key() -> String {
    jwt::mint(&jwt::key_claims("anon"), SECRET)
}

/// The issuer the router above signs with, so a test can assert the
/// claim rather than restate the format.
const ISSUER: &str = "https://zou.test/auth/v1";

/// A user of this suite's own, named after the test that owns it so
/// two tests running at once never touch each other's rows. Returns the
/// id. Every test starts by deleting its own user, which takes the
/// sessions, amr claims and refresh tokens with it through the cascades
/// GoTrue's schema already declares.
async fn seed_user(pool: &Pool, tag: &str) -> String {
    let email = format!("{tag}@zou.test");
    let sess = pool.unscoped().await.expect("connect");
    sess.execute("delete from auth.users where email = $1", &[&email])
        .await
        .expect("clear any leftover");
    let rows = sess
        .query(
            "insert into auth.users
                 (id, instance_id, aud, role, email, encrypted_password,
                  email_confirmed_at, raw_app_meta_data, raw_user_meta_data,
                  created_at, updated_at, is_anonymous)
             values (gen_random_uuid(), '00000000-0000-0000-0000-000000000000',
                     $2, 'authenticated', $1, 'x',
                     now(), '{\"provider\": \"email\", \"providers\": [\"email\"]}'::jsonb,
                     '{\"nickname\": \"tester\"}'::jsonb, now(), now(), false)
             returning id::text",
            &[&email, &auth::AUD],
        )
        .await
        .expect("seed the user");
    let id: String = rows[0].get(0);
    sess.commit().await.expect("park");
    id
}

async fn cleanup(pool: &Pool, tag: &str) {
    let email = format!("{tag}@zou.test");
    let sess = pool.unscoped().await.expect("connect");
    sess.execute("delete from auth.users where email = $1", &[&email])
        .await
        .expect("cleanup");
    sess.commit().await.expect("park");
}

async fn scalar<T>(
    sess: &Session,
    sql: &str,
    args: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> T
where
    T: for<'a> tokio_postgres::types::FromSql<'a>,
{
    sess.query(sql, args).await.expect(sql)[0].get(0)
}

/// POST /auth/v1/token through the whole front door, gate included.
async fn post_token(
    app: &axum::Router,
    grant: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let req = Request::builder()
        .method("POST")
        .uri(format!("/auth/v1/token?grant_type={grant}"))
        .header("apikey", anon_key())
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let res = app.clone().oneshot(req).await.expect("router answers");
    let status = res.status();
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let parsed = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("not json: {}", String::from_utf8_lossy(&bytes)));
    (status, parsed)
}

fn claims_of(token: &str) -> serde_json::Value {
    jwt::verify(token, SECRET)
        .expect("the access token verifies")
        .claims
}

#[tokio::test]
async fn a_session_is_three_rows_and_a_full_claim_set() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let user = seed_user(&pool, "claims").await;

    let issued = auth::issue(&pool, &user, "password", &signer(), ISSUER)
        .await
        .expect("issue");

    let claims = claims_of(&issued.access_token);
    let session_id = claims["session_id"]
        .as_str()
        .expect("session_id")
        .to_string();
    assert_eq!(claims["iss"], ISSUER);
    assert_eq!(claims["sub"], user);
    assert_eq!(claims["aud"], "authenticated");
    assert_eq!(claims["role"], "authenticated");
    assert_eq!(claims["email"], "claims@zou.test");
    assert_eq!(claims["phone"], "");
    assert_eq!(claims["aal"], "aal1");
    assert_eq!(claims["is_anonymous"], false);
    assert_eq!(claims["app_metadata"]["provider"], "email");
    assert_eq!(claims["user_metadata"]["nickname"], "tester");
    assert_eq!(claims["amr"][0]["method"], "password");
    assert!(
        claims["amr"][0]["timestamp"].is_i64(),
        "amr timestamp is a number in GoTrue, got {}",
        claims["amr"][0]["timestamp"]
    );
    let iat = claims["iat"].as_i64().expect("iat");
    let exp = claims["exp"].as_i64().expect("exp");
    assert_eq!(exp - iat, 3600, "GoTrue's JWT_EXP default");
    assert_eq!(issued.expires_in, 3600);
    assert_eq!(issued.expires_at, exp);

    // The response body a supabase-js client parses into a Session.
    let body = issued.json();
    assert_eq!(body["token_type"], "bearer");
    assert_eq!(body["user"]["id"], user);
    assert_eq!(body["user"]["email"], "claims@zou.test");
    assert_eq!(body["user"]["identities"], serde_json::json!([]));
    assert!(
        body["user"]["created_at"].as_str().unwrap().ends_with('Z'),
        "timestamps are RFC 3339 in UTC"
    );
    // Go's omitempty leaves a nil timestamp out entirely, it is not
    // null, and clients test for the key.
    assert!(body["user"].get("email_confirmed_at").is_some());
    assert!(
        body["user"].get("banned_until").is_none(),
        "a user who is not banned has no banned_until key"
    );

    // The three rows the session is made of.
    let sess = pool.unscoped().await.expect("connect");
    let sessions: i64 = scalar(
        &sess,
        "select count(*) from auth.sessions where id = $1::text::uuid and user_id = $2::text::uuid",
        &[&session_id, &user],
    )
    .await;
    let amr: String = scalar(
        &sess,
        "select authentication_method from auth.mfa_amr_claims where session_id = $1::text::uuid",
        &[&session_id],
    )
    .await;
    let (token, parent, revoked): (String, String, bool) = {
        let rows = sess
            .query(
                "select token, coalesce(parent, ''), coalesce(revoked, false)
                 from auth.refresh_tokens where session_id = $1::text::uuid",
                &[&session_id],
            )
            .await
            .expect("the refresh token row");
        assert_eq!(rows.len(), 1, "one refresh token per new session");
        (rows[0].get(0), rows[0].get(1), rows[0].get(2))
    };
    let signed_in: bool = scalar(
        &sess,
        "select last_sign_in_at is not null from auth.users where id = $1::text::uuid",
        &[&user],
    )
    .await;
    sess.commit().await.expect("park");

    assert_eq!(sessions, 1);
    assert_eq!(amr, "password");
    assert_eq!(token, issued.refresh_token);
    assert_eq!(parent, "", "the first token of a session has no parent");
    assert!(!revoked);
    assert!(signed_in, "issuing a session records the sign in");

    cleanup(&pool, "claims").await;
}

#[tokio::test]
async fn the_refresh_grant_rotates_and_keeps_the_session() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let app = app(&dsn);
    let user = seed_user(&pool, "rotate").await;
    let first = auth::issue(&pool, &user, "password", &signer(), ISSUER)
        .await
        .expect("issue");
    let first_session = claims_of(&first.access_token)["session_id"]
        .as_str()
        .unwrap()
        .to_string();

    let (status, body) = post_token(
        &app,
        "refresh_token",
        serde_json::json!({"refresh_token": first.refresh_token}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let second = body["refresh_token"].as_str().expect("a new token");
    assert_ne!(second, first.refresh_token, "rotation issues a new token");
    assert_eq!(body["token_type"], "bearer");
    assert_eq!(body["user"]["id"], user);

    // Same session, so a policy that pinned session_id still matches
    // and a logout still has one thing to revoke.
    let claims = claims_of(body["access_token"].as_str().unwrap());
    assert_eq!(claims["session_id"], first_session);
    assert_eq!(claims["sub"], user);

    let sess = pool.unscoped().await.expect("connect");
    let parent_revoked: bool = scalar(
        &sess,
        "select revoked from auth.refresh_tokens where token = $1",
        &[&first.refresh_token],
    )
    .await;
    let child_parent: String = scalar(
        &sess,
        "select coalesce(parent, '') from auth.refresh_tokens where token = $1",
        &[&second],
    )
    .await;
    let refreshed: bool = scalar(
        &sess,
        "select refreshed_at is not null from auth.sessions where id = $1::text::uuid",
        &[&first_session],
    )
    .await;
    sess.commit().await.expect("park");
    assert!(parent_revoked, "the presented token is revoked by its use");
    assert_eq!(child_parent, first.refresh_token, "the child points at it");
    assert!(refreshed, "the session records when it was refreshed");

    cleanup(&pool, "rotate").await;
}

#[tokio::test]
async fn a_lost_response_is_answered_with_the_token_that_was_issued() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let app = app(&dsn);
    let user = seed_user(&pool, "lost").await;
    let first = auth::issue(&pool, &user, "password", &signer(), ISSUER)
        .await
        .expect("issue");

    let (status, body) = post_token(
        &app,
        "refresh_token",
        serde_json::json!({"refresh_token": first.refresh_token}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let child = body["refresh_token"].as_str().unwrap().to_string();

    // The client never saw that response and retries with the token it
    // still has. It is revoked, but it is the parent of the live one,
    // so it is a lost answer rather than a stolen token and it gets the
    // same answer again.
    let (status, again) = post_token(
        &app,
        "refresh_token",
        serde_json::json!({"refresh_token": first.refresh_token}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{again}");
    assert_eq!(
        again["refresh_token"], child,
        "the same token, not a third one"
    );

    let sess = pool.unscoped().await.expect("connect");
    let live: i64 = scalar(
        &sess,
        "select count(*) from auth.refresh_tokens t
         join auth.sessions s on s.id = t.session_id
         where s.user_id = $1::text::uuid and t.revoked = false",
        &[&user],
    )
    .await;
    sess.commit().await.expect("park");
    assert_eq!(live, 1, "a replay does not fork the session");

    // And the token it was answered with still works.
    let (status, body) = post_token(
        &app,
        "refresh_token",
        serde_json::json!({"refresh_token": child}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    cleanup(&pool, "lost").await;
}

#[tokio::test]
async fn a_stolen_token_takes_the_whole_family_down() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let app = app(&dsn);
    let user = seed_user(&pool, "stolen").await;
    let first = auth::issue(&pool, &user, "password", &signer(), ISSUER)
        .await
        .expect("issue");

    // Two rotations, so the first token is a grandparent: revoked, and
    // not the parent of anything live. Nothing legitimate presents it.
    let (_, second) = post_token(
        &app,
        "refresh_token",
        serde_json::json!({"refresh_token": first.refresh_token}),
    )
    .await;
    let second = second["refresh_token"].as_str().unwrap().to_string();
    let (_, third) = post_token(
        &app,
        "refresh_token",
        serde_json::json!({"refresh_token": second}),
    )
    .await;
    let third = third["refresh_token"].as_str().unwrap().to_string();

    let (status, body) = post_token(
        &app,
        "refresh_token",
        serde_json::json!({"refresh_token": first.refresh_token}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        serde_json::json!({
            "code": 400,
            "error_code": "refresh_token_already_used",
            "msg": "Invalid Refresh Token: Already Used",
        })
    );

    // The theft logs out the thief and the victim both, deliberately:
    // the token the real client is holding is dead too.
    let (status, body) = post_token(
        &app,
        "refresh_token",
        serde_json::json!({"refresh_token": third}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error_code"], "refresh_token_already_used");

    let sess = pool.unscoped().await.expect("connect");
    let live: i64 = scalar(
        &sess,
        "select count(*) from auth.refresh_tokens t
         join auth.sessions s on s.id = t.session_id
         where s.user_id = $1::text::uuid and t.revoked = false",
        &[&user],
    )
    .await;
    sess.commit().await.expect("park");
    assert_eq!(live, 0, "every token in the family is revoked");

    cleanup(&pool, "stolen").await;
}

#[tokio::test]
async fn a_token_whose_session_went_away_is_refused_and_deleted() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let app = app(&dsn);
    let user = seed_user(&pool, "orphan").await;
    let issued = auth::issue(&pool, &user, "password", &signer(), ISSUER)
        .await
        .expect("issue");
    let session_id = claims_of(&issued.access_token)["session_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Detach the token from its session rather than deleting the
    // session, which would cascade the token away with it and leave
    // nothing to present.
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        "update auth.refresh_tokens set session_id = null where token = $1",
        &[&issued.refresh_token],
    )
    .await
    .expect("orphan the token");
    sess.execute(
        "delete from auth.sessions where id = $1::text::uuid",
        &[&session_id],
    )
    .await
    .expect("drop the session");
    sess.commit().await.expect("park");

    let (status, body) = post_token(
        &app,
        "refresh_token",
        serde_json::json!({"refresh_token": issued.refresh_token}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error_code"], "session_not_found");
    assert_eq!(body["msg"], "Invalid Refresh Token: No Valid Session Found");

    // The refusal still wrote: a token with nowhere to point is of no
    // use to anyone and does not survive the request.
    let sess = pool.unscoped().await.expect("connect");
    let left: i64 = scalar(
        &sess,
        "select count(*) from auth.refresh_tokens where token = $1",
        &[&issued.refresh_token],
    )
    .await;
    sess.commit().await.expect("park");
    assert_eq!(left, 0, "the orphan is deleted, and the delete committed");

    cleanup(&pool, "orphan").await;
}

#[tokio::test]
async fn an_expired_session_and_a_banned_user_are_told_apart() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let app = app(&dsn);

    let expired_user = seed_user(&pool, "expired").await;
    let expired = auth::issue(&pool, &expired_user, "password", &signer(), ISSUER)
        .await
        .expect("issue");
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        "update auth.sessions set not_after = now() - interval '1 hour'
         where user_id = $1::text::uuid",
        &[&expired_user],
    )
    .await
    .expect("expire the session");
    sess.commit().await.expect("park");

    let (status, body) = post_token(
        &app,
        "refresh_token",
        serde_json::json!({"refresh_token": expired.refresh_token}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error_code"], "session_expired");
    assert_eq!(body["msg"], "Invalid Refresh Token: Session Expired");

    let banned_user = seed_user(&pool, "banned").await;
    let banned = auth::issue(&pool, &banned_user, "password", &signer(), ISSUER)
        .await
        .expect("issue");
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        "update auth.users set banned_until = now() + interval '1 hour'
         where id = $1::text::uuid",
        &[&banned_user],
    )
    .await
    .expect("ban the user");
    sess.commit().await.expect("park");

    let (status, body) = post_token(
        &app,
        "refresh_token",
        serde_json::json!({"refresh_token": banned.refresh_token}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error_code"], "user_banned");
    assert_eq!(body["msg"], "Invalid Refresh Token: User Banned");

    cleanup(&pool, "expired").await;
    cleanup(&pool, "banned").await;
}

#[tokio::test]
async fn the_grant_refuses_what_it_cannot_serve() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);

    let (status, body) = post_token(
        &app,
        "refresh_token",
        serde_json::json!({"refresh_token": "notatokenatall"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error_code"], "refresh_token_not_found");
    assert_eq!(
        body["msg"],
        "Invalid Refresh Token: Refresh Token Not Found"
    );

    let (status, body) = post_token(&app, "refresh_token", serde_json::json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error_code"], "validation_failed");
    assert_eq!(body["msg"], "refresh_token required");

    // An unknown grant is 400 with GoTrue's own wording, a grant that
    // is coming is 501, and the difference matters to a client that is
    // deciding whether to retry.
    let (status, body) = post_token(&app, "carrier_pigeon", serde_json::json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error_code"], "invalid_credentials");
    assert_eq!(body["msg"], "unsupported_grant_type");

    let req = Request::builder()
        .method("POST")
        .uri("/auth/v1/token?grant_type=password")
        .header("apikey", anon_key())
        .body(Body::from(r#"{"email":"a@b.c","password":"x"}"#))
        .unwrap();
    let res = app.clone().oneshot(req).await.expect("router answers");
    assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);

    // Malformed json is bad_json, not a 500 and not a not found.
    let req = Request::builder()
        .method("POST")
        .uri("/auth/v1/token?grant_type=refresh_token")
        .header("apikey", anon_key())
        .body(Body::from("{not json"))
        .unwrap();
    let res = app.clone().oneshot(req).await.expect("router answers");
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["error_code"], "bad_json");

    // And the endpoint is behind the same gate as everything else.
    let req = Request::builder()
        .method("POST")
        .uri("/auth/v1/token?grant_type=refresh_token")
        .body(Body::from(r#"{"refresh_token":"x"}"#))
        .unwrap();
    let res = app.clone().oneshot(req).await.expect("router answers");
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}
