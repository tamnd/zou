//! Reading the account back, throwing sessions away, sending the last
//! code again, and signing in as nobody, against a live postgres.
//!
//! The contract being pinned is GoTrue's. Two of these endpoints only
//! look simple. `/logout` cannot revoke the access token it is handed,
//! so what it means is that the session behind the token stops being
//! found, and the tests here are about that consequence rather than
//! about the 204. `/resend` refuses to say whether an address is
//! registered or what it is in the middle of, so most of its answers
//! are an empty 200 and the interesting assertion is what did not go in
//! the post.
//!
//! Anonymous sign in is the other half. An account with no address is
//! ordinary in every other way, and the flow that matters is the one
//! that turns it into a real account later without losing the id it was
//! carrying.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test auth_session

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

/// The knobs this suite turns: whether the project confirms its own
/// signups, whether anonymous sign in is on, and whether the send
/// frequency limit is in the way. Everything else is GoTrue's default.
fn project(dsn: &str, autoconfirm: bool, anonymous: bool, max_frequency: u64) -> axum::Router {
    router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        external_url: Some("https://zou.test".to_string()),
        site_url: Some(SITE.to_string()),
        mailer_autoconfirm: autoconfirm,
        anonymous_users: anonymous,
        mail: mail::Settings {
            max_frequency,
            ..mail::Settings::default()
        },
        ..Config::default()
    })
    .expect("router builds")
}

/// A project that confirms its own signups and mails nothing, which is
/// how most of these tests get a session in one request.
fn app(dsn: &str) -> axum::Router {
    project(dsn, true, false, 0)
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
}

impl Answer {
    fn refusal(&self) -> (u16, &str, &str) {
        (
            self.status.as_u16(),
            self.body["error_code"].as_str().unwrap_or("<none>"),
            self.body["msg"].as_str().unwrap_or("<none>"),
        )
    }
}

async fn answer(res: axum::response::Response) -> Answer {
    let status = res.status();
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    if bytes.is_empty() {
        return Answer {
            status,
            body: serde_json::Value::Null,
        };
    }
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("not json: {}", String::from_utf8_lossy(&bytes)));
    Answer { status, body }
}

async fn post(app: &axum::Router, path: &str, body: serde_json::Value) -> Answer {
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .header("apikey", anon_key())
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    answer(app.clone().oneshot(req).await.expect("router answers")).await
}

/// A request from somebody holding a session.
async fn as_user(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: serde_json::Value,
) -> Answer {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("apikey", anon_key())
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    answer(app.clone().oneshot(req).await.expect("router answers")).await
}

async fn user_get(app: &axum::Router, token: &str) -> Answer {
    as_user(app, "GET", "/auth/v1/user", token, serde_json::json!({})).await
}

async fn logout(app: &axum::Router, token: &str, scope: &str) -> Answer {
    let path = match scope {
        "" => "/auth/v1/logout".to_string(),
        s => format!("/auth/v1/logout?scope={s}"),
    };
    as_user(app, "POST", &path, token, serde_json::json!({})).await
}

async fn resend(app: &axum::Router, body: serde_json::Value) -> Answer {
    post(app, "/auth/v1/resend", body).await
}

/// A followed link: no apikey, because a mail client has none.
async fn follow(app: &axum::Router, url: &str) -> axum::response::Response {
    let (_, query) = url.split_once('?').expect("a link carries its token");
    let req = Request::builder()
        .method("GET")
        .uri(format!("/auth/v1/verify?{query}"))
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(req).await.expect("router answers")
}

/// One message as the dev inbox hands it over.
struct Message(serde_json::Value);

impl Message {
    fn to(&self) -> String {
        self.0["to"].as_str().expect("a recipient").to_string()
    }
    fn link(&self) -> String {
        self.0["link"]
            .as_str()
            .unwrap_or_else(|| panic!("no link in: {}", self.0))
            .to_string()
    }
}

async fn inbox(app: &axum::Router) -> Vec<Message> {
    let req = Request::builder()
        .method("GET")
        .uri("/dev/inbox")
        .header("apikey", service_key())
        .body(Body::empty())
        .unwrap();
    let answer = answer(app.clone().oneshot(req).await.expect("router answers")).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    answer.body["messages"]
        .as_array()
        .expect("an array of messages")
        .iter()
        .map(|m| Message(m.clone()))
        .collect()
}

async fn empty_inbox(app: &axum::Router) {
    let req = Request::builder()
        .method("DELETE")
        .uri("/dev/inbox")
        .header("apikey", service_key())
        .body(Body::empty())
        .unwrap();
    let answer = answer(app.clone().oneshot(req).await.expect("router answers")).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
}

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

/// An account with a session, which is where most of these tests start.
async fn signed_up(app: &axum::Router, email: &str) -> (String, String) {
    let answer = post(
        app,
        "/auth/v1/signup",
        serde_json::json!({"email": email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    (
        answer.body["user"]["id"].as_str().expect("id").to_string(),
        answer.body["access_token"]
            .as_str()
            .expect("access_token")
            .to_string(),
    )
}

/// A second session on the same account, which is what the logout
/// scopes are about.
async fn signed_in(app: &axum::Router, email: &str) -> (String, String) {
    let answer = post(
        app,
        "/auth/v1/token?grant_type=password",
        serde_json::json!({"email": email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    (
        answer.body["access_token"]
            .as_str()
            .expect("access_token")
            .to_string(),
        answer.body["refresh_token"]
            .as_str()
            .expect("refresh_token")
            .to_string(),
    )
}

async fn sessions_of(pool: &Pool, user_id: &str) -> i64 {
    scalar(
        pool,
        "select count(*) from auth.sessions where user_id = $1::text::uuid",
        &[&user_id],
    )
    .await
}

#[tokio::test]
async fn the_account_reads_back_the_way_the_signup_described_it() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("session-get");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (user_id, token) = signed_up(&app, &email).await;
    let answer = user_get(&app, &token).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let user = &answer.body;
    assert_eq!(user["id"], user_id.as_str());
    assert_eq!(user["email"], email.as_str());
    assert_eq!(user["aud"], "authenticated");
    assert_eq!(user["role"], "authenticated");
    assert_eq!(user["is_anonymous"], false);
    assert_eq!(user["identities"][0]["provider"], "email");
    assert!(user["email_confirmed_at"].is_string());
    // The account is the answer, not a session wrapped around it.
    assert!(user.get("access_token").is_none(), "{user}");
}

#[tokio::test]
async fn a_token_minted_for_another_audience_describes_nobody_here() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("session-aud");
    wipe(&pool, &email).await;
    let app = app(&dsn);
    let (user_id, token) = signed_up(&app, &email).await;

    // The header is what a gateway in front puts on the request to say
    // which audience it is serving, and a token from another one stops
    // meaning anything.
    let req = Request::builder()
        .method("GET")
        .uri("/auth/v1/user")
        .header("apikey", anon_key())
        .header("authorization", format!("Bearer {token}"))
        .header("x-jwt-aud", "somewhere-else")
        .body(Body::empty())
        .unwrap();
    let elsewhere = answer(app.clone().oneshot(req).await.expect("router answers")).await;
    assert_eq!(
        elsewhere.refusal(),
        (
            400,
            "validation_failed",
            "Token audience doesn't match request audience"
        )
    );

    // And a token that never named an audience at all is the same
    // refusal, which is what a service key gets here.
    let audienceless = jwt::mint(
        &serde_json::json!({
            "sub": user_id,
            "role": "authenticated",
            "iat": 0,
            "exp": 4102444800i64,
        }),
        SECRET,
    );
    assert_eq!(
        user_get(&app, &audienceless).await.refusal(),
        (
            400,
            "validation_failed",
            "Token audience doesn't match request audience"
        )
    );
}

#[tokio::test]
async fn an_account_that_is_gone_is_not_described_to_the_token_it_left_behind() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("session-deleted");
    wipe(&pool, &email).await;
    let app = app(&dsn);
    let (_, token) = signed_up(&app, &email).await;

    wipe(&pool, &email).await;
    assert_eq!(
        user_get(&app, &token).await.refusal(),
        (
            403,
            "user_not_found",
            "User from sub claim in JWT does not exist"
        )
    );
}

#[tokio::test]
async fn logging_out_leaves_the_token_signed_and_worth_nothing() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("session-logout");
    wipe(&pool, &email).await;
    let app = app(&dsn);
    let (user_id, token) = signed_up(&app, &email).await;
    let (_, refresh) = signed_in(&app, &email).await;
    assert_eq!(sessions_of(&pool, &user_id).await, 2);

    let out = logout(&app, &token, "").await;
    assert_eq!(out.status, StatusCode::NO_CONTENT);
    assert!(out.body.is_null(), "a logout carries nothing: {}", out.body);
    assert_eq!(
        sessions_of(&pool, &user_id).await,
        0,
        "global takes them all"
    );

    // The access token still verifies. It was signed and nothing can
    // unsign it, and it is still inside its hour. What stops it is that
    // the session it names is not there any more.
    assert!(jwt::verify(&token, SECRET).is_ok());
    assert_eq!(
        user_get(&app, &token).await.refusal(),
        (
            403,
            "session_not_found",
            "Session from session_id claim in JWT does not exist"
        )
    );
    // And the refresh token that hung off the other session went with
    // it, so nothing can be renewed either.
    let renewed = post(
        &app,
        "/auth/v1/token?grant_type=refresh_token",
        serde_json::json!({"refresh_token": refresh}),
    )
    .await;
    assert_eq!(renewed.status, StatusCode::BAD_REQUEST, "{}", renewed.body);
}

#[tokio::test]
async fn a_local_logout_takes_this_session_and_leaves_the_others() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("session-local");
    wipe(&pool, &email).await;
    let app = app(&dsn);
    let (user_id, token) = signed_up(&app, &email).await;
    let (elsewhere, _) = signed_in(&app, &email).await;

    assert_eq!(
        logout(&app, &token, "local").await.status,
        StatusCode::NO_CONTENT
    );
    assert_eq!(sessions_of(&pool, &user_id).await, 1);
    assert_eq!(
        user_get(&app, &token).await.body["error_code"],
        "session_not_found"
    );
    assert_eq!(user_get(&app, &elsewhere).await.status, StatusCode::OK);
}

#[tokio::test]
async fn an_others_logout_is_the_local_one_turned_around() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("session-others");
    wipe(&pool, &email).await;
    let app = app(&dsn);
    let (user_id, token) = signed_up(&app, &email).await;
    let (elsewhere, _) = signed_in(&app, &email).await;

    assert_eq!(
        logout(&app, &token, "others").await.status,
        StatusCode::NO_CONTENT
    );
    assert_eq!(sessions_of(&pool, &user_id).await, 1);
    assert_eq!(user_get(&app, &token).await.status, StatusCode::OK);
    assert_eq!(
        user_get(&app, &elsewhere).await.body["error_code"],
        "session_not_found"
    );
}

#[tokio::test]
async fn a_logout_scope_nobody_serves_is_named_back_in_the_refusal() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("session-scope");
    wipe(&pool, &email).await;
    let app = app(&dsn);
    let (user_id, token) = signed_up(&app, &email).await;

    assert_eq!(
        logout(&app, &token, "everywhere").await.refusal(),
        (
            400,
            "validation_failed",
            "Unsupported logout scope \"everywhere\""
        )
    );
    assert_eq!(sessions_of(&pool, &user_id).await, 1, "nothing was taken");

    // And nobody at all is refused before the scope is ever read.
    let bare = post(
        &app,
        "/auth/v1/logout?scope=everywhere",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        bare.refusal(),
        (
            401,
            "no_authorization",
            "This endpoint requires a valid Bearer token"
        )
    );
}

#[tokio::test]
async fn a_resend_replaces_the_code_that_was_mailed_rather_than_repeating_it() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("resend-signup");
    wipe(&pool, &email).await;
    // Mails its confirmations, and does not make this test wait a
    // minute between the two of them.
    let app = project(&dsn, false, false, 0);

    let signup = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(signup.status, StatusCode::OK, "{}", signup.body);
    let first = inbox(&app).await.pop().expect("a confirmation").link();
    empty_inbox(&app).await;

    let again = resend(&app, serde_json::json!({"type": "signup", "email": &email})).await;
    assert_eq!(again.status, StatusCode::OK, "{}", again.body);
    assert_eq!(again.body, serde_json::json!({}));
    let sent = inbox(&app).await;
    assert_eq!(sent.len(), 1, "one more confirmation");
    let second = sent[0].link();
    assert_eq!(sent[0].to(), email);
    assert_ne!(first, second, "a fresh code, not the same one again");

    // One live confirmation per account: the link that was mailed first
    // is dead, and the one that replaced it works.
    let stale = follow(&app, &first).await;
    assert_eq!(stale.status(), StatusCode::SEE_OTHER);
    let location = stale
        .headers()
        .get("location")
        .expect("a redirect")
        .to_str()
        .expect("ascii")
        .to_string();
    assert!(location.contains("otp_expired"), "{location}");

    let fresh = follow(&app, &second).await;
    assert_eq!(fresh.status(), StatusCode::SEE_OTHER);
    let landed = fresh
        .headers()
        .get("location")
        .expect("a redirect")
        .to_str()
        .expect("ascii")
        .to_string();
    assert!(landed.contains("access_token="), "{landed}");
}

#[tokio::test]
async fn a_resend_says_nothing_about_who_has_an_account_here() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("resend-quiet");
    wipe(&pool, &email).await;
    let app = project(&dsn, true, false, 0);

    // Nobody holds the address.
    let unknown = resend(
        &app,
        serde_json::json!({"type": "signup", "email": address("resend-nobody")}),
    )
    .await;
    assert_eq!(unknown.status, StatusCode::OK);
    assert_eq!(unknown.body, serde_json::json!({}));

    // Somebody does, and has nothing left to confirm.
    signed_up(&app, &email).await;
    let done = resend(&app, serde_json::json!({"type": "signup", "email": &email})).await;
    assert_eq!(done.status, StatusCode::OK);
    assert_eq!(done.body, serde_json::json!({}));

    // Neither answer is distinguishable from the other, and neither
    // put anything in the post.
    assert!(inbox(&app).await.is_empty(), "nothing was sent");
}

#[tokio::test]
async fn a_resend_of_a_change_of_address_starts_both_halves_over() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let (old, new) = (address("resend-change-old"), address("resend-change-new"));
    wipe(&pool, &old).await;
    wipe(&pool, &new).await;
    let app = project(&dsn, false, false, 0);

    // An account that has proved its address, and a change waiting on
    // both halves of the double confirmation.
    let (user_id, token) = {
        let confirming = project(&dsn, true, false, 0);
        signed_up(&confirming, &old).await
    };
    let asked = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"email": &new}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);
    let first: Vec<String> = inbox(&app).await.iter().map(|m| m.link()).collect();
    assert_eq!(first.len(), 2, "one code to each address");
    empty_inbox(&app).await;

    let again = resend(
        &app,
        serde_json::json!({"type": "email_change", "email": &old}),
    )
    .await;
    assert_eq!(again.status, StatusCode::OK, "{}", again.body);
    let sent = inbox(&app).await;
    let went_to: Vec<String> = sent.iter().map(|m| m.to()).collect();
    assert!(went_to.contains(&old), "{went_to:?}");
    assert!(went_to.contains(&new), "{went_to:?}");
    for link in sent.iter().map(|m| m.link()) {
        assert!(!first.contains(&link), "a fresh pair, not the old one");
    }
    // The change itself did not move, only the codes carrying it.
    let pending: String = scalar(
        &pool,
        "select email_change from auth.users where id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert_eq!(pending, new);

    // An account that holds its address and asked for no change is the
    // silent answer again. Nothing is invented for it to change to, so
    // nothing is mailed and the column stays as empty as it was.
    empty_inbox(&app).await;
    let settled = address("resend-change-settled");
    wipe(&pool, &settled).await;
    let (other_id, _) = {
        let confirming = project(&dsn, true, false, 0);
        signed_up(&confirming, &settled).await
    };
    let quiet = resend(
        &app,
        serde_json::json!({"type": "email_change", "email": &settled}),
    )
    .await;
    assert_eq!(quiet.status, StatusCode::OK);
    assert_eq!(quiet.body, serde_json::json!({}));
    assert!(inbox(&app).await.is_empty(), "nothing was sent");
    let none: String = scalar(
        &pool,
        "select email_change from auth.users where id = $1::text::uuid",
        &[&other_id],
    )
    .await;
    assert_eq!(none, "");
}

#[tokio::test]
async fn a_resend_that_asks_for_something_nobody_serves_says_so() {
    let Some(dsn) = dsn() else { return };
    let app = project(&dsn, false, false, 0);

    assert_eq!(
        resend(
            &app,
            serde_json::json!({"type": "invite", "email": address("resend-bad")})
        )
        .await
        .refusal(),
        (
            400,
            "validation_failed",
            "Missing one of these types: signup, email_change, sms, phone_change"
        )
    );
    assert_eq!(
        resend(&app, serde_json::json!({"type": "signup"}))
            .await
            .refusal(),
        (
            400,
            "validation_failed",
            "Type provided requires an email address"
        )
    );
    assert_eq!(
        resend(&app, serde_json::json!({"type": "sms"}))
            .await
            .refusal(),
        (
            400,
            "validation_failed",
            "Type provided requires a phone number"
        )
    );
    assert_eq!(
        resend(
            &app,
            serde_json::json!({"type": "email_change", "email": address("a"), "phone": "+15551234567"})
        )
        .await
        .refusal(),
        (
            400,
            "validation_failed",
            "Only an email address or phone number should be provided."
        )
    );
    // A number reaches the same refusal a project with no SMS provider
    // gives upstream, which is what this project is.
    assert_eq!(
        resend(
            &app,
            serde_json::json!({"type": "sms", "phone": "+15551234567"})
        )
        .await
        .refusal(),
        (400, "phone_provider_disabled", "Phone logins are disabled")
    );
    assert_eq!(
        resend(&app, serde_json::json!({"type": "email_change"}))
            .await
            .refusal(),
        (
            400,
            "validation_failed",
            "Missing email address or phone number"
        )
    );
}

#[tokio::test]
async fn a_resend_waits_the_same_minute_the_first_send_started() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("resend-too-soon");
    wipe(&pool, &email).await;
    // The default frequency limit, which is a minute.
    let app = project(&dsn, false, false, 60);

    let signup = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(signup.status, StatusCode::OK, "{}", signup.body);

    let again = resend(&app, serde_json::json!({"type": "signup", "email": &email})).await;
    assert_eq!(
        again.status,
        StatusCode::TOO_MANY_REQUESTS,
        "{}",
        again.body
    );
    assert_eq!(again.body["error_code"], "over_email_send_rate_limit");
    assert_eq!(inbox(&app).await.len(), 1, "only the first one went");
}

#[tokio::test]
async fn signing_in_as_nobody_is_off_until_a_project_turns_it_on() {
    let Some(dsn) = dsn() else { return };
    let app = project(&dsn, true, false, 0);

    let refused = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"password": "correct horse"}),
    )
    .await;
    assert_eq!(
        refused.refusal(),
        (
            422,
            "anonymous_provider_disabled",
            "Anonymous sign-ins are disabled"
        )
    );
}

#[tokio::test]
async fn an_anonymous_account_is_one_that_nobody_vouched_for() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let app = project(&dsn, true, true, 0);

    let answer = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"data": {"nickname": "ghost"}}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let user = &answer.body["user"];
    let user_id = user["id"].as_str().expect("id").to_string();
    assert_eq!(user["email"], "");
    assert_eq!(user["is_anonymous"], true);
    assert_eq!(user["aud"], "authenticated");
    assert_eq!(user["role"], "authenticated");
    assert_eq!(user["user_metadata"]["nickname"], "ghost");
    // No identity and no provider. Every other signup writes down who
    // owns the account, and this one has nobody to write down.
    assert_eq!(user["identities"], serde_json::json!([]));
    assert_eq!(user["app_metadata"], serde_json::json!({}));

    let claims = claims_of(answer.body["access_token"].as_str().expect("access_token"));
    assert_eq!(claims["is_anonymous"], true);
    assert_eq!(claims["email"], "");
    assert_eq!(claims["amr"][0]["method"], "anonymous");

    // It is an ordinary session in every other way.
    let read = user_get(&app, answer.body["access_token"].as_str().unwrap()).await;
    assert_eq!(read.status, StatusCode::OK, "{}", read.body);
    assert_eq!(read.body["id"], user_id.as_str());

    run(
        &pool,
        "delete from auth.users where id = $1::text::uuid",
        &[&user_id],
    )
    .await;
}

#[tokio::test]
async fn an_anonymous_account_keeps_its_id_when_it_becomes_a_real_one() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("anon-converted");
    wipe(&pool, &email).await;
    let app = project(&dsn, true, true, 0);

    let started = post(&app, "/auth/v1/signup", serde_json::json!({})).await;
    let user_id = started.body["user"]["id"].as_str().expect("id").to_string();
    let token = started.body["access_token"].as_str().expect("token");

    let became = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        token,
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(became.status, StatusCode::OK, "{}", became.body);
    // The same account, with everything it was carrying, and now with
    // an address it did not have to prove because the project proves
    // its own.
    assert_eq!(became.body["id"], user_id.as_str());
    assert_eq!(became.body["email"], email.as_str());
    assert_eq!(became.body["is_anonymous"], false);
    assert!(
        became.body["email_confirmed_at"].is_string(),
        "{}",
        became.body
    );
    let identity = &became.body["identities"][0];
    assert_eq!(identity["provider"], "email");
    assert_eq!(identity["identity_data"]["email"], email.as_str());
    assert_eq!(identity["identity_data"]["email_verified"], true);
    assert!(
        inbox(&app).await.is_empty(),
        "nothing to confirm, nothing sent"
    );

    // And the password it set now works, on the same account.
    let back = post(
        &app,
        "/auth/v1/token?grant_type=password",
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(back.status, StatusCode::OK, "{}", back.body);
    assert_eq!(back.body["user"]["id"], user_id.as_str());
    assert_eq!(
        claims_of(back.body["access_token"].as_str().unwrap())["is_anonymous"],
        false
    );
}

#[tokio::test]
async fn an_anonymous_account_proves_the_address_when_the_project_mails_for_it() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("anon-mailed");
    wipe(&pool, &email).await;
    let app = project(&dsn, false, true, 0);

    let started = post(&app, "/auth/v1/signup", serde_json::json!({})).await;
    assert_eq!(started.status, StatusCode::OK, "{}", started.body);
    let user_id = started.body["user"]["id"].as_str().expect("id").to_string();
    let token = started.body["access_token"].as_str().expect("token");

    let asked = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        token,
        serde_json::json!({"email": &email}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);
    // Nothing has moved yet. The account is still anonymous and still
    // has no address, which is the difference from the branch above.
    assert_eq!(asked.body["email"], "");
    assert_eq!(asked.body["is_anonymous"], true);
    assert_eq!(asked.body["new_email"], email.as_str());

    // One code, to the new address only: there is no old address to
    // ask, so the second half of the double confirmation has nobody to
    // go to.
    let sent = inbox(&app).await;
    assert_eq!(sent.len(), 1, "one code");
    assert_eq!(sent[0].to(), email);

    let landed = follow(&app, &sent[0].link()).await;
    assert_eq!(landed.status(), StatusCode::SEE_OTHER);
    let location = landed
        .headers()
        .get("location")
        .expect("a redirect")
        .to_str()
        .expect("ascii")
        .to_string();
    assert!(location.contains("access_token="), "{location}");

    let anonymous: bool = scalar(
        &pool,
        "select is_anonymous from auth.users where id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert!(!anonymous, "following the link ends the anonymous account");
    let provider: String = scalar(
        &pool,
        "select coalesce(max(provider), '') from auth.identities where user_id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert_eq!(provider, "email", "the address it proved got an identity");
}

#[tokio::test]
async fn an_anonymous_account_cannot_lock_itself_behind_a_password() {
    let Some(dsn) = dsn() else { return };
    let app = project(&dsn, true, true, 0);

    let started = post(&app, "/auth/v1/signup", serde_json::json!({})).await;
    let token = started.body["access_token"].as_str().expect("token");

    // A password with nothing to present it with is an account nobody
    // can ever sign in to again.
    let refused = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        token,
        serde_json::json!({"password": "correct horse"}),
    )
    .await;
    assert_eq!(
        refused.refusal(),
        (
            422,
            "validation_failed",
            "Updating password of an anonymous user without an email or phone is not allowed"
        )
    );
}
