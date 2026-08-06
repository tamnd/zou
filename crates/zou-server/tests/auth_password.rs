//! Signup and the password grant against a live postgres.
//!
//! The contract being pinned is GoTrue's, not zou's. A signup writes a
//! user, an identity and either a session or a confirmation token, and
//! it says the same things in the same words when it refuses. The
//! password grant answers one message for every bad credential so that
//! nothing about which addresses exist can be read off the response.
//!
//! The hash format is part of the contract too, and the harshest test
//! here is the one that signs in on a hash produced by Go's bcrypt:
//! that is what a project moving its auth.users rows off hosted
//! Supabase actually carries with it.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test auth_password

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use zou_server::sql::{Pool, Session};
use zou_server::{Config, jwt, mail, password, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";
const ISSUER: &str = "https://zou.test/auth/v1";

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

/// The whole front door, with the one knob these tests care about:
/// whether the project confirms its own signups or mails for it.
/// The send frequency limit is off, the way it is in the other suites
/// that are not about it: it belongs to `auth_mail`, and here it would
/// only refuse the second signup this suite exists to make.
fn app(dsn: &str, autoconfirm: bool) -> axum::Router {
    router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        external_url: Some("https://zou.test".to_string()),
        mailer_autoconfirm: autoconfirm,
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

/// What came back: the status, the error_code header GoTrue sets
/// alongside the body, and the body itself.
struct Answer {
    status: StatusCode,
    error_code: Option<String>,
    body: serde_json::Value,
}

impl Answer {
    /// The three things a client reads off a refusal, together, so a
    /// test states them in one assertion rather than three.
    fn refusal(&self) -> (u16, &str, &str) {
        (
            self.status.as_u16(),
            self.body["error_code"].as_str().unwrap_or("<none>"),
            self.body["msg"].as_str().unwrap_or("<none>"),
        )
    }
}

async fn post(app: &axum::Router, path: &str, body: serde_json::Value) -> Answer {
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .header("apikey", anon_key())
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let res = app.clone().oneshot(req).await.expect("router answers");
    let status = res.status();
    let error_code = res
        .headers()
        .get("x-sb-error-code")
        .map(|v| v.to_str().expect("ascii").to_string());
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("not json: {}", String::from_utf8_lossy(&bytes)));
    Answer {
        status,
        error_code,
        body,
    }
}

async fn signup(app: &axum::Router, body: serde_json::Value) -> Answer {
    post(app, "/auth/v1/signup", body).await
}

async fn grant(app: &axum::Router, body: serde_json::Value) -> Answer {
    post(app, "/auth/v1/token?grant_type=password", body).await
}

/// An address of this test's own, so two tests running at once never
/// touch each other's rows.
fn address(tag: &str) -> String {
    format!("{tag}@zou.test")
}

async fn wipe(pool: &Pool, email: &str) {
    let sess = pool.unscoped().await.expect("connect");
    sess.execute("delete from auth.users where email = $1", &[&email])
        .await
        .expect("clear any leftover");
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

fn claims_of(token: &str) -> serde_json::Value {
    jwt::verify(token, SECRET)
        .expect("the access token verifies")
        .claims
}

#[tokio::test]
async fn a_confirmed_signup_writes_a_user_an_identity_and_a_session() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("signup-session");
    wipe(&pool, &email).await;

    let answer = signup(
        &app(&dsn, true),
        serde_json::json!({
            "email": &email,
            "password": "correct horse",
            "data": {"nickname": "tester"},
        }),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK);

    // A session, not just a user: the project confirms its own signups
    // so there is nothing left to wait for.
    let body = &answer.body;
    assert_eq!(body["token_type"], "bearer");
    assert_eq!(body["expires_in"], 3600);
    let claims = claims_of(body["access_token"].as_str().expect("access_token"));
    let user_id = body["user"]["id"].as_str().expect("id").to_string();
    assert_eq!(claims["sub"], user_id.as_str());
    assert_eq!(claims["email"], email.as_str());
    assert_eq!(claims["role"], "authenticated");
    assert_eq!(claims["iss"], ISSUER);
    assert_eq!(claims["amr"][0]["method"], "password");

    assert_eq!(body["user"]["email"], email.as_str());
    assert_eq!(body["user"]["aud"], "authenticated");
    assert_eq!(body["user"]["app_metadata"]["provider"], "email");
    assert_eq!(body["user"]["app_metadata"]["providers"][0], "email");
    assert_eq!(body["user"]["user_metadata"]["nickname"], "tester");
    // Confirming here is the project vouching for the address, and it
    // says so in the metadata the way GoTrue does.
    assert_eq!(body["user"]["user_metadata"]["email_verified"], true);
    assert!(body["user"]["email_confirmed_at"].is_string());
    assert!(
        body["user"].get("confirmation_sent_at").is_none(),
        "nothing was mailed, so there is no sent_at: {}",
        body["user"]
    );

    // The identity is what says this user belongs to the email
    // provider, and email_verified there is what the provider has
    // asserted. Autoconfirm runs the same confirmation a link would
    // have run, so the provider has asserted it. An account an admin
    // makes with email_confirm is the other way round: the admin
    // asserted the address, the provider never did, and the identity
    // there still says false.
    let identity = &body["user"]["identities"][0];
    assert_eq!(identity["provider"], "email");
    assert_eq!(identity["id"], user_id.as_str());
    assert_eq!(identity["user_id"], user_id.as_str());
    assert_eq!(identity["identity_data"]["sub"], user_id.as_str());
    assert_eq!(identity["identity_data"]["email"], email.as_str());
    assert_eq!(identity["identity_data"]["email_verified"], true);
    assert_eq!(identity["identity_data"]["phone_verified"], false);

    let sess = pool.unscoped().await.expect("connect");
    let hash: String = scalar(
        &sess,
        "select encrypted_password from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert!(hash.starts_with("$2a$10$"), "not GoTrue's format: {hash}");
    assert!(password::matches("correct horse", &hash));
    // Nothing to confirm means nothing left lying around to confirm
    // with.
    let tokens: i64 = scalar(
        &sess,
        "select count(*) from auth.one_time_tokens t
         join auth.users u on u.id = t.user_id where u.email = $1",
        &[&email],
    )
    .await;
    assert_eq!(tokens, 0);
    let pending: String = scalar(
        &sess,
        "select confirmation_token from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert_eq!(pending, "");
    sess.commit().await.expect("park");

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_pending_signup_answers_with_a_user_and_a_token_to_confirm_it() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("signup-pending");
    wipe(&pool, &email).await;

    let answer = signup(
        &app(&dsn, false),
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK);
    let body = &answer.body;

    // A user and nothing else. Handing out a session here would sign in
    // whoever typed the address, proved or not.
    assert!(
        body.get("access_token").is_none(),
        "an unconfirmed signup is not a session: {body}"
    );
    assert!(body["confirmation_sent_at"].is_string());
    assert!(
        body.get("email_confirmed_at").is_none(),
        "nothing has confirmed this address yet: {body}"
    );
    assert_eq!(body["email"], email.as_str());
    assert_eq!(body["role"], "authenticated");
    assert_eq!(body["identities"][0]["provider"], "email");

    let sess = pool.unscoped().await.expect("connect");
    // The code itself is never written down, only its hash, in both
    // places GoTrue keeps it.
    let stored: String = scalar(
        &sess,
        "select confirmation_token from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert_eq!(stored.len(), 56, "hex of sha224: {stored}");
    let (kind, hash, relates): (String, String, String) = {
        let row = &sess
            .query(
                "select t.token_type::text, t.token_hash, t.relates_to
                 from auth.one_time_tokens t
                 join auth.users u on u.id = t.user_id where u.email = $1",
                &[&email],
            )
            .await
            .expect("the one time token row")[0];
        (row.get(0), row.get(1), row.get(2))
    };
    assert_eq!(kind, "confirmation_token");
    assert_eq!(hash, stored);
    assert_eq!(relates, email);
    sess.commit().await.expect("park");

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_second_signup_on_a_confirmed_address_is_refused_when_it_is_safe_to_say_so() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("signup-duplicate");
    wipe(&pool, &email).await;

    let app = app(&dsn, true);
    let first = signup(
        &app,
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(first.status, StatusCode::OK);

    let second = signup(
        &app,
        serde_json::json!({"email": &email, "password": "another one"}),
    )
    .await;
    assert_eq!(
        second.refusal(),
        (422, "user_already_exists", "User already registered")
    );
    assert_eq!(second.error_code.as_deref(), Some("user_already_exists"));

    // The refusal is not a password change: the first password still
    // works and the second one never does.
    let signed_in = grant(
        &app,
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(signed_in.status, StatusCode::OK);
    let refused = grant(
        &app,
        serde_json::json!({"email": &email, "password": "another one"}),
    )
    .await;
    assert_eq!(
        refused.refusal(),
        (400, "invalid_credentials", "Invalid login credentials")
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_project_that_mails_confirmations_never_says_an_address_is_taken() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let taken = address("signup-sanitized");
    let free = address("signup-sanitized-free");
    wipe(&pool, &taken).await;
    wipe(&pool, &free).await;

    // Someone holds the first address already and has proved it.
    let confirmed = app(&dsn, true);
    let real = signup(
        &confirmed,
        serde_json::json!({"email": &taken, "password": "correct horse"}),
    )
    .await;
    let real_id = real.body["user"]["id"].as_str().expect("id").to_string();

    let mailing = app(&dsn, false);
    let on_taken = signup(
        &mailing,
        serde_json::json!({"email": &taken, "password": "another one"}),
    )
    .await;
    let on_free = signup(
        &mailing,
        serde_json::json!({"email": &free, "password": "another one"}),
    )
    .await;

    // Whatever a caller can see is the same either way: same status,
    // same set of keys, same shape. Only the address differs.
    assert_eq!(on_taken.status, StatusCode::OK);
    assert_eq!(on_free.status, StatusCode::OK);
    let keys = |v: &serde_json::Value| {
        let mut k: Vec<String> = v.as_object().expect("object").keys().cloned().collect();
        k.sort();
        k
    };
    assert_eq!(
        keys(&on_taken.body),
        keys(&on_free.body),
        "the two answers differ in shape, which is the leak"
    );
    // The id it hands back is not the id of the user who holds the
    // address, so it cannot be used to learn anything about them.
    assert_ne!(on_taken.body["id"].as_str(), Some(real_id.as_str()));
    assert_eq!(on_taken.body["identities"], serde_json::json!([]));
    assert_eq!(on_taken.body["role"], "");

    // And it wrote nothing: the address still belongs to whoever
    // confirmed it, with the password they set.
    let sess = pool.unscoped().await.expect("connect");
    let users: i64 = scalar(
        &sess,
        "select count(*) from auth.users where email = $1",
        &[&taken],
    )
    .await;
    assert_eq!(users, 1);
    sess.commit().await.expect("park");
    let still_in = grant(
        &confirmed,
        serde_json::json!({"email": &taken, "password": "correct horse"}),
    )
    .await;
    assert_eq!(still_in.status, StatusCode::OK);

    wipe(&pool, &taken).await;
    wipe(&pool, &free).await;
}

#[tokio::test]
async fn a_second_signup_on_an_unconfirmed_address_leaves_the_password_alone() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("signup-unconfirmed");
    wipe(&pool, &email).await;

    let mailing = app(&dsn, false);
    let first = signup(
        &mailing,
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    let user_id = first.body["id"].as_str().expect("id").to_string();

    let sess = pool.unscoped().await.expect("connect");
    let first_hash: String = scalar(
        &sess,
        "select encrypted_password from auth.users where email = $1",
        &[&email],
    )
    .await;
    sess.commit().await.expect("park");

    let second = signup(
        &mailing,
        serde_json::json!({"email": &email, "password": "not mine"}),
    )
    .await;
    assert_eq!(second.status, StatusCode::OK);
    assert_eq!(
        second.body["id"].as_str(),
        Some(user_id.as_str()),
        "the unconfirmed row is reused, not duplicated"
    );

    let sess = pool.unscoped().await.expect("connect");
    let second_hash: String = scalar(
        &sess,
        "select encrypted_password from auth.users where email = $1",
        &[&email],
    )
    .await;
    // Whoever asked second has not shown they are the one who started
    // this signup, so they do not get to set the password.
    assert_eq!(first_hash, second_hash);
    assert!(password::matches("correct horse", &second_hash));
    // One live code per user: the second signup replaced the first
    // rather than leaving two that both work.
    let tokens: i64 = scalar(
        &sess,
        "select count(*) from auth.one_time_tokens t
         join auth.users u on u.id = t.user_id where u.email = $1",
        &[&email],
    )
    .await;
    assert_eq!(tokens, 1);
    let identities: i64 = scalar(
        &sess,
        "select count(*) from auth.identities i
         join auth.users u on u.id = i.user_id where u.email = $1",
        &[&email],
    )
    .await;
    assert_eq!(identities, 1);
    sess.commit().await.expect("park");

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn the_password_grant_mints_a_session_of_its_own() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("grant-session");
    wipe(&pool, &email).await;

    let app = app(&dsn, true);
    let signed_up = signup(
        &app,
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    let signed_in = grant(
        &app,
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(signed_in.status, StatusCode::OK);

    let first = claims_of(signed_up.body["access_token"].as_str().unwrap());
    let second = claims_of(signed_in.body["access_token"].as_str().unwrap());
    assert_eq!(second["sub"], first["sub"], "the same user");
    assert_ne!(
        second["session_id"], first["session_id"],
        "signing in is a new session, not the old one handed back"
    );
    assert_eq!(second["amr"][0]["method"], "password");
    assert_ne!(
        signed_in.body["refresh_token"],
        signed_up.body["refresh_token"]
    );

    // The address is matched case insensitively, which is what a person
    // typing it into a form needs.
    let shouting = grant(
        &app,
        serde_json::json!({"email": email.to_uppercase(), "password": "correct horse"}),
    )
    .await;
    assert_eq!(shouting.status, StatusCode::OK);

    let sess = pool.unscoped().await.expect("connect");
    let sessions: i64 = scalar(
        &sess,
        "select count(*) from auth.sessions s
         join auth.users u on u.id = s.user_id where u.email = $1",
        &[&email],
    )
    .await;
    assert_eq!(sessions, 3, "one from the signup and two from the grants");
    sess.commit().await.expect("park");

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn every_bad_credential_gets_the_same_answer() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("grant-refusals");
    wipe(&pool, &email).await;

    let app = app(&dsn, true);
    signup(
        &app,
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;

    // An address nobody holds, and one somebody holds with a different
    // password. Telling these apart is telling a stranger which
    // addresses have accounts.
    let unknown = grant(
        &app,
        serde_json::json!({"email": address("grant-nobody"), "password": "correct horse"}),
    )
    .await;
    let wrong = grant(
        &app,
        serde_json::json!({"email": &email, "password": "correct horse "}),
    )
    .await;
    let expected = (400, "invalid_credentials", "Invalid login credentials");
    assert_eq!(unknown.refusal(), expected);
    assert_eq!(wrong.refusal(), expected);
    assert_eq!(unknown.body, wrong.body, "byte for byte, or it is a tell");
    assert_eq!(wrong.error_code.as_deref(), Some("invalid_credentials"));

    // A user with no password at all is the same answer again, not a
    // way in.
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        "update auth.users set encrypted_password = null where email = $1",
        &[&email],
    )
    .await
    .expect("clear the password");
    sess.commit().await.expect("park");
    let empty = grant(&app, serde_json::json!({"email": &email, "password": ""})).await;
    assert_eq!(empty.refusal(), expected);

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn an_unconfirmed_address_is_only_named_to_whoever_knows_the_password() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("grant-unconfirmed");
    wipe(&pool, &email).await;

    let mailing = app(&dsn, false);
    signup(
        &mailing,
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;

    let wrong = grant(
        &mailing,
        serde_json::json!({"email": &email, "password": "not it"}),
    )
    .await;
    assert_eq!(
        wrong.refusal(),
        (400, "invalid_credentials", "Invalid login credentials"),
        "a stranger learns nothing about the state of this address"
    );

    let right = grant(
        &mailing,
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(
        right.refusal(),
        (400, "email_not_confirmed", "Email not confirmed")
    );

    let sess = pool.unscoped().await.expect("connect");
    let sessions: i64 = scalar(
        &sess,
        "select count(*) from auth.sessions s
         join auth.users u on u.id = s.user_id where u.email = $1",
        &[&email],
    )
    .await;
    assert_eq!(sessions, 0, "a refusal does not start a session");
    sess.commit().await.expect("park");

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_banned_user_is_refused_before_the_password_is_even_checked() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("grant-banned");
    wipe(&pool, &email).await;

    let app = app(&dsn, true);
    signup(
        &app,
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        "update auth.users set banned_until = now() + interval '1 day' where email = $1",
        &[&email],
    )
    .await
    .expect("ban");
    sess.commit().await.expect("park");

    let expected = (400, "user_banned", "User is banned");
    let right = grant(
        &app,
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    let wrong = grant(
        &app,
        serde_json::json!({"email": &email, "password": "not it"}),
    )
    .await;
    assert_eq!(right.refusal(), expected);
    // Upstream checks the ban first, so a banned user is told they are
    // banned whether or not they still remember the password.
    assert_eq!(wrong.refusal(), expected);

    // A ban that has run out is not a ban.
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        "update auth.users set banned_until = now() - interval '1 day' where email = $1",
        &[&email],
    )
    .await
    .expect("unban");
    sess.commit().await.expect("park");
    let back = grant(
        &app,
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(back.status, StatusCode::OK);

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_password_gotrue_wrote_signs_in_here() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("grant-migrated");
    wipe(&pool, &email).await;

    // This hash was produced by golang.org/x/crypto/bcrypt at
    // DefaultCost, the library and cost GoTrue hashes with. A project
    // moving its auth.users rows off hosted Supabase carries hashes
    // that look exactly like this, and every one of its users has to be
    // able to sign in the morning after the move.
    let go = "$2a$10$9mSfsZp2ozwmHOV6.8fl.OtVThXLxCXzN7X26Qou1r28iLAb3odY.";
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        "insert into auth.users
             (id, instance_id, aud, role, email, encrypted_password,
              email_confirmed_at, raw_app_meta_data, raw_user_meta_data,
              created_at, updated_at, is_anonymous)
         values (gen_random_uuid(), '00000000-0000-0000-0000-000000000000',
                 'authenticated', 'authenticated', $1, $2,
                 now(), '{\"provider\": \"email\", \"providers\": [\"email\"]}'::jsonb,
                 '{}'::jsonb, now(), now(), false)",
        &[&email, &go],
    )
    .await
    .expect("seed the migrated user");
    sess.commit().await.expect("park");

    let app = app(&dsn, true);
    let signed_in = grant(
        &app,
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(signed_in.status, StatusCode::OK);
    assert_eq!(signed_in.body["user"]["email"], email.as_str());
    let refused = grant(
        &app,
        serde_json::json!({"email": &email, "password": "correct horses"}),
    )
    .await;
    assert_eq!(
        refused.refusal(),
        (400, "invalid_credentials", "Invalid login credentials")
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_weak_password_is_refused_with_the_reason_it_is_weak() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("signup-weak");
    wipe(&pool, &email).await;

    let app = app(&dsn, true);
    let short = signup(
        &app,
        serde_json::json!({"email": &email, "password": "12345"}),
    )
    .await;
    assert_eq!(
        short.refusal(),
        (
            422,
            "weak_password",
            "Password should be at least 6 characters."
        )
    );
    // The reasons are the machine readable half, which is what lets a
    // client point at the rule instead of parsing the english.
    assert_eq!(
        short.body["weak_password"]["reasons"],
        serde_json::json!(["length"])
    );
    assert_eq!(short.error_code.as_deref(), Some("weak_password"));

    // Past bcrypt's 72 bytes a password is not weak, it is unusable,
    // and upstream calls it what it is.
    let long = signup(
        &app,
        serde_json::json!({"email": &email, "password": "x".repeat(73)}),
    )
    .await;
    assert_eq!(
        long.refusal(),
        (
            400,
            "validation_failed",
            "Password cannot be longer than 72 characters"
        )
    );
    let missing = signup(&app, serde_json::json!({"email": &email})).await;
    assert_eq!(
        missing.refusal(),
        (400, "validation_failed", "Signup requires a valid password")
    );

    // None of it wrote a user.
    let sess = pool.unscoped().await.expect("connect");
    let users: i64 = scalar(
        &sess,
        "select count(*) from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert_eq!(users, 0);
    sess.commit().await.expect("park");
}

#[tokio::test]
async fn an_address_that_is_not_one_is_refused_in_gotrues_words() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn, true);

    let cases = [
        (
            "not-an-address",
            "Unable to validate email address: invalid format",
        ),
        (
            "two@@zou.test",
            "Unable to validate email address: invalid format",
        ),
        (
            "@zou.test",
            "Unable to validate email address: invalid format",
        ),
        (
            "spaced out@zou.test",
            "Unable to validate email address: invalid format",
        ),
    ];
    for (address, msg) in cases {
        let answer = signup(
            &app,
            serde_json::json!({"email": address, "password": "correct horse"}),
        )
        .await;
        assert_eq!(
            answer.refusal(),
            (400, "validation_failed", msg),
            "for {address}"
        );
    }

    let long = format!("{}@zou.test", "x".repeat(250));
    let too_long = signup(
        &app,
        serde_json::json!({"email": long, "password": "correct horse"}),
    )
    .await;
    assert_eq!(
        too_long.refusal(),
        (400, "validation_failed", "An email address is too long")
    );

    // Neither an address nor a phone number is not this signup at all,
    // it is the anonymous one, and this project has not turned it on.
    let neither = signup(&app, serde_json::json!({"password": "correct horse"})).await;
    assert_eq!(
        neither.refusal(),
        (
            422,
            "anonymous_provider_disabled",
            "Anonymous sign-ins are disabled"
        )
    );
    let both = signup(
        &app,
        serde_json::json!({
            "email": address("signup-both"),
            "phone": "+15551234567",
            "password": "correct horse",
        }),
    )
    .await;
    assert_eq!(
        both.refusal(),
        (
            400,
            "validation_failed",
            "Only an email address or phone number should be provided on signup."
        )
    );
}

#[tokio::test]
async fn the_grant_still_says_which_grants_it_does_not_serve() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn, true);

    let missing = grant(&app, serde_json::json!({"password": "correct horse"})).await;
    assert_eq!(
        missing.refusal(),
        (400, "validation_failed", "missing email or phone")
    );
    let both = grant(
        &app,
        serde_json::json!({
            "email": address("grant-both"),
            "phone": "+15551234567",
            "password": "correct horse",
        }),
    )
    .await;
    assert_eq!(
        both.refusal(),
        (
            400,
            "validation_failed",
            "Only an email address or phone number should be provided on login."
        )
    );

    let unsupported = post(
        &app,
        "/auth/v1/token?grant_type=nonsense",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        unsupported.refusal(),
        (400, "invalid_credentials", "unsupported_grant_type")
    );
}
