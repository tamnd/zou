//! Enrolling a second factor, proving it, and what that does to the
//! session, against a live postgres.
//!
//! The contract being pinned is GoTrue's four factor endpoints. Most of
//! what matters here is not the 200s: it is what a verified factor does
//! to the token, which is `aal2` and an `amr` entry that an RLS policy
//! reads through auth.jwt(), and what it does to the other sessions the
//! account was holding, which is take them away.
//!
//! The tests act as the phone as well as the client. The secret comes
//! back in the enroll response, so the code an authenticator would be
//! showing is computable, and every verify here goes through the same
//! HMAC an app would.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test auth_mfa

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use zou_server::sql::Pool;
use zou_server::{Config, jwt, mfa, router, totp};

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

/// A project that confirms its own signups, so a test gets a session in
/// one request, with whatever MFA settings the test is about.
fn project(dsn: &str, mfa: mfa::Settings) -> axum::Router {
    router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        external_url: Some("https://zou.test".to_string()),
        site_url: Some(SITE.to_string()),
        mailer_autoconfirm: true,
        mfa,
        ..Config::default()
    })
    .expect("router builds")
}

fn app(dsn: &str) -> axum::Router {
    project(dsn, mfa::Settings::default())
}

fn anon_key() -> String {
    jwt::mint(&jwt::key_claims("anon"), SECRET)
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

    fn str(&self, key: &str) -> String {
        self.body[key]
            .as_str()
            .unwrap_or_else(|| panic!("no {key} in {}", self.body))
            .to_string()
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

/// A request from somebody holding a session, optionally saying which
/// address it is coming from. `from` of None sends no X-Forwarded-For,
/// which is what a request straight off the loopback looks like.
async fn as_user(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    from: Option<&str>,
    body: serde_json::Value,
) -> Answer {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("apikey", anon_key())
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json");
    if let Some(ip) = from {
        req = req.header("x-forwarded-for", ip);
    }
    let req = req.body(Body::from(body.to_string())).unwrap();
    answer(app.clone().oneshot(req).await.expect("router answers")).await
}

async fn enroll(app: &axum::Router, token: &str, body: serde_json::Value) -> Answer {
    as_user(app, "POST", "/auth/v1/factors", token, None, body).await
}

async fn totp_enroll(app: &axum::Router, token: &str, name: &str) -> Answer {
    enroll(
        app,
        token,
        serde_json::json!({"factor_type": "totp", "friendly_name": name}),
    )
    .await
}

async fn challenge(app: &axum::Router, token: &str, factor: &str, from: Option<&str>) -> Answer {
    as_user(
        app,
        "POST",
        &format!("/auth/v1/factors/{factor}/challenge"),
        token,
        from,
        serde_json::json!({}),
    )
    .await
}

async fn verify(
    app: &axum::Router,
    token: &str,
    factor: &str,
    from: Option<&str>,
    body: serde_json::Value,
) -> Answer {
    as_user(
        app,
        "POST",
        &format!("/auth/v1/factors/{factor}/verify"),
        token,
        from,
        body,
    )
    .await
}

async fn unenroll(app: &axum::Router, token: &str, factor: &str) -> Answer {
    as_user(
        app,
        "DELETE",
        &format!("/auth/v1/factors/{factor}"),
        token,
        None,
        serde_json::json!({}),
    )
    .await
}

async fn user_get(app: &axum::Router, token: &str) -> Answer {
    as_user(
        app,
        "GET",
        "/auth/v1/user",
        token,
        None,
        serde_json::json!({}),
    )
    .await
}

fn claims_of(token: &str) -> serde_json::Value {
    jwt::verify(token, SECRET)
        .expect("the access token verifies")
        .claims
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after 1970")
        .as_secs() as i64
}

/// The whole exchange, as a phone and a client would do it: enroll,
/// challenge, then the code the app is showing right now.
struct Enrolled {
    factor_id: String,
    secret: String,
}

async fn enrolled(app: &axum::Router, token: &str, name: &str) -> Enrolled {
    let answer = totp_enroll(app, token, name).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    Enrolled {
        factor_id: answer.str("id"),
        secret: answer.body["totp"]["secret"]
            .as_str()
            .expect("a secret")
            .to_string(),
    }
}

/// Enroll, challenge and verify in one go, which is what most of these
/// tests need before they can ask about aal2. Answers whatever verify
/// answered.
async fn proved(app: &axum::Router, token: &str, name: &str) -> (Enrolled, Answer) {
    let factor = enrolled(app, token, name).await;
    let challenge = challenge(app, token, &factor.factor_id, None).await;
    assert_eq!(challenge.status, StatusCode::OK, "{}", challenge.body);
    let code = totp::code(&factor.secret, now()).expect("the secret is base32");
    let answer = verify(
        app,
        token,
        &factor.factor_id,
        None,
        serde_json::json!({"challenge_id": challenge.str("id"), "code": code}),
    )
    .await;
    (factor, answer)
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

/// An account with a session, which is where every one of these starts.
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

/// A second session on the same account.
async fn signed_in(app: &axum::Router, email: &str) -> String {
    let answer = post(
        app,
        "/auth/v1/token?grant_type=password",
        serde_json::json!({"email": email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    answer.str("access_token")
}

#[tokio::test]
async fn enrolling_hands_back_a_secret_a_url_and_a_drawing_of_the_url() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-enroll");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (_, token) = signed_up(&app, &email).await;
    let answer = totp_enroll(&app, &token, "phone").await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert_eq!(answer.body["type"], "totp");
    assert_eq!(answer.body["friendly_name"], "phone");
    let secret = answer.body["totp"]["secret"].as_str().expect("a secret");
    assert_eq!(secret.len(), 32, "{secret}");
    // The issuer is the host of the site url, which is what an
    // authenticator writes above the code, and the account name is the
    // address so a person with two accounts can tell them apart.
    assert_eq!(
        answer.body["totp"]["uri"],
        format!(
            "otpauth://totp/app.zou.test:{email}\
             ?algorithm=SHA1&digits=6&issuer=app.zou.test&period=30&secret={secret}"
        )
    );
    let qr = answer.body["totp"]["qr_code"].as_str().expect("a drawing");
    assert!(qr.starts_with("<?xml version=\"1.0\"?>"), "{qr}");
    assert!(qr.ends_with("</svg>\n"));

    // Nothing has been proved yet: the factor is there and unverified,
    // and the session that enrolled it is still aal1.
    let status: String = scalar(
        &pool,
        "select f.status::text from auth.mfa_factors f
           join auth.users u on u.id = f.user_id where u.email = $1",
        &[&email],
    )
    .await;
    assert_eq!(status, "unverified");
    assert_eq!(claims_of(&token)["aal"], "aal1");
}

#[tokio::test]
async fn an_issuer_on_the_request_is_what_the_url_carries() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-issuer");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (_, token) = signed_up(&app, &email).await;
    let answer = enroll(
        &app,
        &token,
        serde_json::json!({"factor_type": "totp", "issuer": "My Company"}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let uri = answer.body["totp"]["uri"].as_str().expect("a uri");
    assert!(uri.starts_with("otpauth://totp/My%20Company:"), "{uri}");
    assert!(uri.contains("issuer=My+Company"), "{uri}");
}

#[tokio::test]
async fn the_factor_shows_up_on_the_user_object() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-user-object");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (_, token) = signed_up(&app, &email).await;
    // An account with no factors has no factors key at all, which is
    // what upstream's omitempty does and what a client checking for
    // one is written against.
    let before = user_get(&app, &token).await;
    assert!(before.body.get("factors").is_none(), "{}", before.body);

    let factor = enrolled(&app, &token, "phone").await;
    let after = user_get(&app, &token).await;
    let factors = after.body["factors"].as_array().expect("an array");
    assert_eq!(factors.len(), 1, "{}", after.body);
    assert_eq!(factors[0]["id"], factor.factor_id);
    assert_eq!(factors[0]["friendly_name"], "phone");
    assert_eq!(factors[0]["factor_type"], "totp");
    assert_eq!(factors[0]["status"], "unverified");
    // The secret is never on the user object. It went out once, in the
    // enroll response, and that is the only time it does.
    assert!(factors[0].get("secret").is_none(), "{}", factors[0]);
}

#[tokio::test]
async fn proving_the_factor_lifts_the_session_to_aal2() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-verify");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (user_id, token) = signed_up(&app, &email).await;
    let before = claims_of(&token);
    assert_eq!(before["aal"], "aal1");

    let (factor, answer) = proved(&app, &token, "phone").await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);

    // The answer is a whole token pair, not an acknowledgement: the
    // access token is the one that says aal2 and the old one does not.
    let claims = claims_of(&answer.str("access_token"));
    assert_eq!(claims["aal"], "aal2");
    assert_eq!(claims["session_id"], before["session_id"]);
    let amr = claims["amr"].as_array().expect("an amr list");
    // Most recent first, so the factor that was just proved leads and
    // the password that started the session is still behind it.
    assert_eq!(amr[0]["method"], "totp", "{claims}");
    assert_eq!(amr[1]["method"], "password", "{claims}");
    assert!(!answer.str("refresh_token").is_empty());

    // The factor is verified and the session names it, which is what
    // unenrolling later reads to know what to put back down.
    let status: String = scalar(
        &pool,
        "select f.status::text from auth.mfa_factors f where f.id = $1::text::uuid",
        &[&factor.factor_id],
    )
    .await;
    assert_eq!(status, "verified");
    let named: String = scalar(
        &pool,
        "select coalesce(s.factor_id::text, '') || ' ' || s.aal::text
           from auth.sessions s where s.user_id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert_eq!(named, format!("{} aal2", factor.factor_id));
}

#[tokio::test]
async fn the_refresh_token_the_session_had_before_is_swapped_for_a_new_one() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-refresh-swap");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let signup = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"email": email, "password": "correct horse"}),
    )
    .await;
    let token = signup.str("access_token");
    let old_refresh = signup.str("refresh_token");

    let (_, answer) = proved(&app, &token, "phone").await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let new_refresh = answer.str("refresh_token");
    assert_ne!(new_refresh, old_refresh, "the token was not swapped");

    // The token the client was holding before is revoked, so presenting
    // it is the lost response case: inside the grace window it hands
    // back the child that was already issued rather than a second live
    // token, and the session it describes is the aal2 one. There is no
    // way back to an aal1 session because the level is on the session
    // rather than on the token.
    let reused = post(
        &app,
        "/auth/v1/token?grant_type=refresh_token",
        serde_json::json!({"refresh_token": old_refresh}),
    )
    .await;
    assert_eq!(reused.status, StatusCode::OK, "{}", reused.body);
    assert_eq!(reused.str("refresh_token"), new_refresh);
    assert_eq!(claims_of(&reused.str("access_token"))["aal"], "aal2");

    // The new one works and still says aal2, so a refresh does not
    // quietly drop the factor.
    let refreshed = post(
        &app,
        "/auth/v1/token?grant_type=refresh_token",
        serde_json::json!({"refresh_token": new_refresh}),
    )
    .await;
    assert_eq!(refreshed.status, StatusCode::OK, "{}", refreshed.body);
    assert_eq!(claims_of(&refreshed.str("access_token"))["aal"], "aal2");
}

#[tokio::test]
async fn the_other_sessions_are_taken_away_when_the_factor_is_proved() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-invalidate");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (user_id, token) = signed_up(&app, &email).await;
    let other = signed_in(&app, &email).await;
    let count: i64 = scalar(
        &pool,
        "select count(*) from auth.sessions where user_id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert_eq!(count, 2, "two sessions to start with");

    let (_, answer) = proved(&app, &token, "phone").await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);

    // The one that proved the factor is aal2 and stays. The other never
    // passed it, so it is gone rather than left as a way in that skips
    // the factor.
    let left: i64 = scalar(
        &pool,
        "select count(*) from auth.sessions where user_id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert_eq!(left, 1, "the aal1 session was not invalidated");
    let refused = user_get(&app, &other).await;
    assert_eq!(
        refused.refusal(),
        (
            403,
            "session_not_found",
            "Session from session_id claim in JWT does not exist"
        )
    );
}

#[tokio::test]
async fn a_wrong_code_is_refused_and_the_factor_stays_unverified() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-wrong-code");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (_, token) = signed_up(&app, &email).await;
    let factor = enrolled(&app, &token, "phone").await;
    let challenge = challenge(&app, &token, &factor.factor_id, None).await;
    assert_eq!(challenge.status, StatusCode::OK, "{}", challenge.body);

    // A code two steps out is a code an app never showed at this
    // instant, which is exactly what a stale screenshot looks like.
    let stale = totp::code(&factor.secret, now() - 120).expect("base32");
    let answer = verify(
        &app,
        &token,
        &factor.factor_id,
        None,
        serde_json::json!({"challenge_id": challenge.str("id"), "code": stale}),
    )
    .await;
    assert_eq!(
        answer.refusal(),
        (422, "mfa_verification_failed", "Invalid TOTP code entered")
    );
    let status: String = scalar(
        &pool,
        "select f.status::text from auth.mfa_factors f where f.id = $1::text::uuid",
        &[&factor.factor_id],
    )
    .await;
    assert_eq!(status, "unverified");

    // The challenge was not spent by the wrong code, so the right one
    // still lands against it.
    let code = totp::code(&factor.secret, now()).expect("base32");
    let good = verify(
        &app,
        &token,
        &factor.factor_id,
        None,
        serde_json::json!({"challenge_id": challenge.str("id"), "code": code}),
    )
    .await;
    assert_eq!(good.status, StatusCode::OK, "{}", good.body);
}

#[tokio::test]
async fn a_challenge_can_only_be_spent_once() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-replay");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (_, token) = signed_up(&app, &email).await;
    let factor = enrolled(&app, &token, "phone").await;
    let challenge = challenge(&app, &token, &factor.factor_id, None).await;
    let code = totp::code(&factor.secret, now()).expect("base32");
    let body = serde_json::json!({"challenge_id": challenge.str("id"), "code": code});

    let first = verify(&app, &token, &factor.factor_id, None, body.clone()).await;
    assert_eq!(first.status, StatusCode::OK, "{}", first.body);
    // The same code inside the same thirty seconds, against the same
    // challenge, is a replay. Upstream calls a spent challenge an
    // address mismatch, which is the same branch.
    let again = verify(
        &app,
        &first.str("access_token"),
        &factor.factor_id,
        None,
        body,
    )
    .await;
    assert_eq!(
        again.refusal(),
        (
            422,
            "mfa_ip_address_mismatch",
            "Challenge and verify IP addresses mismatch."
        )
    );
}

#[tokio::test]
async fn a_challenge_from_another_address_is_refused() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-ip");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (_, token) = signed_up(&app, &email).await;
    let factor = enrolled(&app, &token, "phone").await;
    let challenge = challenge(&app, &token, &factor.factor_id, Some("203.0.113.7")).await;
    assert_eq!(challenge.status, StatusCode::OK, "{}", challenge.body);

    let code = totp::code(&factor.secret, now()).expect("base32");
    let elsewhere = verify(
        &app,
        &token,
        &factor.factor_id,
        Some("198.51.100.9"),
        serde_json::json!({"challenge_id": challenge.str("id"), "code": code.clone()}),
    )
    .await;
    assert_eq!(
        elsewhere.refusal(),
        (
            422,
            "mfa_ip_address_mismatch",
            "Challenge and verify IP addresses mismatch."
        )
    );

    // From the address it was asked for, the same code lands.
    let same = verify(
        &app,
        &token,
        &factor.factor_id,
        Some("203.0.113.7"),
        serde_json::json!({"challenge_id": challenge.str("id"), "code": code}),
    )
    .await;
    assert_eq!(same.status, StatusCode::OK, "{}", same.body);
}

#[tokio::test]
async fn an_expired_challenge_is_deleted_and_says_so() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-expired");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (_, token) = signed_up(&app, &email).await;
    let factor = enrolled(&app, &token, "phone").await;
    let challenge = challenge(&app, &token, &factor.factor_id, None).await;
    let challenge_id = challenge.str("id");
    // Five minutes and a second ago, which is the only way to reach the
    // branch without sleeping for five minutes.
    run(
        &pool,
        "update auth.mfa_challenges set created_at = now() - interval '301 seconds'
          where id = $1::text::uuid",
        &[&challenge_id],
    )
    .await;

    let code = totp::code(&factor.secret, now()).expect("base32");
    let answer = verify(
        &app,
        &token,
        &factor.factor_id,
        None,
        serde_json::json!({"challenge_id": challenge_id, "code": code}),
    )
    .await;
    assert_eq!(
        answer.refusal(),
        (
            422,
            "mfa_challenge_expired",
            format!(
                "MFA challenge {challenge_id} has expired, verify against another \
                 challenge or create a new challenge."
            )
            .as_str()
        )
    );
    let left: i64 = scalar(
        &pool,
        "select count(*) from auth.mfa_challenges where id = $1::text::uuid",
        &[&challenge_id],
    )
    .await;
    assert_eq!(left, 0, "the expired challenge was not deleted");
}

#[tokio::test]
async fn the_challenge_says_when_it_expires_and_stamps_the_factor() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-challenge-shape");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (_, token) = signed_up(&app, &email).await;
    let factor = enrolled(&app, &token, "phone").await;
    let answer = challenge(&app, &token, &factor.factor_id, None).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert_eq!(answer.body["type"], "totp");
    let expires = answer.body["expires_at"].as_i64().expect("an expiry");
    // Five minutes out, give or take the second the test took.
    assert!((expires - (now() + 300)).abs() <= 5, "{expires}");

    // The factor remembers it was challenged, which is what the phone
    // send rate limit reads and what a client shows as last used.
    let stamped: bool = scalar(
        &pool,
        "select f.last_challenged_at is not null from auth.mfa_factors f
          where f.id = $1::text::uuid",
        &[&factor.factor_id],
    )
    .await;
    assert!(stamped);
}

#[tokio::test]
async fn a_factor_belonging_to_somebody_else_is_not_found() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let mine = address("mfa-mine");
    let theirs = address("mfa-theirs");
    wipe(&pool, &mine).await;
    wipe(&pool, &theirs).await;
    let app = app(&dsn);

    let (_, my_token) = signed_up(&app, &mine).await;
    let (_, their_token) = signed_up(&app, &theirs).await;
    let theirs = enrolled(&app, &their_token, "phone").await;

    // Somebody else's factor and an id that was never a uuid are both
    // 404, and neither says which, so the endpoint cannot be used to
    // ask whether an id exists.
    assert_eq!(
        challenge(&app, &my_token, &theirs.factor_id, None)
            .await
            .refusal(),
        (404, "mfa_factor_not_found", "Factor not found")
    );
    assert_eq!(
        challenge(&app, &my_token, "not-a-uuid", None)
            .await
            .refusal(),
        (404, "validation_failed", "factor_id must be an UUID")
    );
    assert_eq!(
        unenroll(&app, &my_token, &theirs.factor_id).await.refusal(),
        (404, "mfa_factor_not_found", "Factor not found")
    );
}

#[tokio::test]
async fn a_challenge_that_was_never_written_is_not_found() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-no-challenge");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (_, token) = signed_up(&app, &email).await;
    let factor = enrolled(&app, &token, "phone").await;
    let code = totp::code(&factor.secret, now()).expect("base32");
    for challenge_id in ["not-a-uuid", "00000000-0000-0000-0000-000000000000", ""] {
        let answer = verify(
            &app,
            &token,
            &factor.factor_id,
            None,
            serde_json::json!({"challenge_id": challenge_id, "code": code.clone()}),
        )
        .await;
        assert_eq!(
            answer.refusal(),
            (
                422,
                "mfa_factor_not_found",
                "MFA factor with the provided challenge ID not found"
            ),
            "{challenge_id:?}"
        );
    }
}

#[tokio::test]
async fn an_empty_code_is_refused_before_anything_is_looked_up() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-empty-code");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (_, token) = signed_up(&app, &email).await;
    let factor = enrolled(&app, &token, "phone").await;
    let answer = verify(
        &app,
        &token,
        &factor.factor_id,
        None,
        serde_json::json!({"challenge_id": "00000000-0000-0000-0000-000000000000"}),
    )
    .await;
    assert_eq!(
        answer.refusal(),
        (400, "validation_failed", "Code needs to be non-empty")
    );
}

#[tokio::test]
async fn the_factor_types_that_are_not_built_say_so_rather_than_pretending() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-types");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (_, token) = signed_up(&app, &email).await;
    for (kind, code, msg) in [
        (
            "phone",
            "mfa_phone_enroll_not_enabled",
            "MFA enroll is disabled for Phone",
        ),
        (
            "webauthn",
            "mfa_webauthn_enroll_not_enabled",
            "MFA enroll is disabled for WebAuthn",
        ),
    ] {
        let answer = enroll(&app, &token, serde_json::json!({"factor_type": kind})).await;
        assert_eq!(answer.refusal(), (422, code, msg), "{kind}");
    }
    // Anything else, including nothing at all, is a bad request rather
    // than a disabled feature.
    for body in [
        serde_json::json!({}),
        serde_json::json!({"factor_type": "sms"}),
    ] {
        let answer = enroll(&app, &token, body.clone()).await;
        assert_eq!(
            answer.refusal(),
            (
                400,
                "validation_failed",
                "factor_type needs to be totp, phone, or webauthn"
            ),
            "{body}"
        );
    }
}

#[tokio::test]
async fn a_project_with_totp_switched_off_refuses_both_ends() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-off");
    wipe(&pool, &email).await;
    let on = app(&dsn);
    let enroll_off = project(
        &dsn,
        mfa::Settings {
            totp_enroll: false,
            ..mfa::Settings::default()
        },
    );
    let verify_off = project(
        &dsn,
        mfa::Settings {
            totp_verify: false,
            ..mfa::Settings::default()
        },
    );

    let (_, token) = signed_up(&on, &email).await;
    assert_eq!(
        totp_enroll(&enroll_off, &token, "phone").await.refusal(),
        (
            422,
            "mfa_totp_enroll_not_enabled",
            "MFA enroll is disabled for TOTP"
        )
    );

    // Verification off is the switch for turning MFA off without
    // deleting anybody's factors: what is already enrolled cannot be
    // used, and the challenge is refused before it is written.
    let factor = enrolled(&on, &token, "phone").await;
    assert_eq!(
        challenge(&verify_off, &token, &factor.factor_id, None)
            .await
            .refusal(),
        (
            422,
            "mfa_totp_verify_not_enabled",
            "MFA verification is disabled for TOTP"
        )
    );
    let written: i64 = scalar(
        &pool,
        "select count(*) from auth.mfa_challenges where factor_id = $1::text::uuid",
        &[&factor.factor_id],
    )
    .await;
    assert_eq!(written, 0, "a refused challenge was still written");

    // A challenge that was written while verification was on is no good
    // once it is off either, which is the check a client holding an
    // in flight challenge runs into.
    let challenge = challenge(&on, &token, &factor.factor_id, None).await;
    assert_eq!(challenge.status, StatusCode::OK, "{}", challenge.body);
    let code = totp::code(&factor.secret, now()).expect("base32");
    let answer = verify(
        &verify_off,
        &token,
        &factor.factor_id,
        None,
        serde_json::json!({"challenge_id": challenge.str("id"), "code": code}),
    )
    .await;
    assert_eq!(
        answer.refusal(),
        (
            422,
            "mfa_totp_verify_not_enabled",
            "MFA verification is disabled for TOTP"
        )
    );
}

#[tokio::test]
async fn two_factors_cannot_share_a_friendly_name() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-name-conflict");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (_, token) = signed_up(&app, &email).await;
    enrolled(&app, &token, "phone").await;
    assert_eq!(
        totp_enroll(&app, &token, "phone").await.refusal(),
        (
            422,
            "mfa_factor_name_conflict",
            "A factor with the friendly name \"phone\" for this user already exists"
        )
    );
    // The empty name is a name too, which is upstream's behaviour and
    // is why a client that never sets one can only ever enroll once at
    // a time.
    enroll(&app, &token, serde_json::json!({"factor_type": "totp"})).await;
    assert_eq!(
        enroll(&app, &token, serde_json::json!({"factor_type": "totp"}))
            .await
            .refusal(),
        (
            422,
            "mfa_factor_name_conflict",
            "A factor with the friendly name \"\" for this user already exists"
        )
    );
}

#[tokio::test]
async fn there_is_a_ceiling_on_how_many_factors_an_account_may_hold() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-ceiling");
    wipe(&pool, &email).await;
    let app = project(
        &dsn,
        mfa::Settings {
            max_enrolled: 2,
            ..mfa::Settings::default()
        },
    );

    let (_, token) = signed_up(&app, &email).await;
    enrolled(&app, &token, "one").await;
    enrolled(&app, &token, "two").await;
    assert_eq!(
        totp_enroll(&app, &token, "three").await.refusal(),
        (
            422,
            "too_many_enrolled_mfa_factors",
            "Maximum number of verified factors reached, unenroll to continue"
        )
    );
}

#[tokio::test]
async fn an_expired_unused_factor_is_cleared_out_of_the_way() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-expired-factor");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (_, token) = signed_up(&app, &email).await;
    let abandoned = enrolled(&app, &token, "phone").await;
    // Somebody who shut the tab halfway through, five minutes ago. The
    // name it was holding is free again.
    run(
        &pool,
        "update auth.mfa_factors set created_at = now() - interval '301 seconds'
          where id = $1::text::uuid",
        &[&abandoned.factor_id],
    )
    .await;

    let answer = totp_enroll(&app, &token, "phone").await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert_ne!(answer.str("id"), abandoned.factor_id);
    let left: i64 = scalar(
        &pool,
        "select count(*) from auth.mfa_factors where id = $1::text::uuid",
        &[&abandoned.factor_id],
    )
    .await;
    assert_eq!(left, 0, "the abandoned factor is still there");
}

#[tokio::test]
async fn a_second_factor_needs_an_aal2_session_to_enroll() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-second-factor");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (_, token) = signed_up(&app, &email).await;
    let (_, verified) = proved(&app, &token, "first").await;
    assert_eq!(verified.status, StatusCode::OK, "{}", verified.body);
    let aal2 = verified.str("access_token");

    // A fresh password sign in is aal1: the account has a factor now,
    // and a password alone does not pass it. That session cannot add a
    // second authenticator, which is the check that stops somebody who
    // learned the password from enrolling their own phone.
    let aal1 = signed_in(&app, &email).await;
    assert_eq!(claims_of(&aal1)["aal"], "aal1");
    assert_eq!(
        totp_enroll(&app, &aal1, "second").await.refusal(),
        (
            403,
            "insufficient_aal",
            "AAL2 required to enroll a new factor"
        )
    );

    // The session that proved the factor can. The level is read off the
    // session row rather than the token, so the aal1 token that session
    // was carrying beforehand works here too.
    let answer = totp_enroll(&app, &aal2, "second").await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let answer = totp_enroll(&app, &token, "third").await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
}

#[tokio::test]
async fn unenrolling_puts_the_sessions_it_lifted_back_down() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-unenroll");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (user_id, token) = signed_up(&app, &email).await;
    let (factor, verified) = proved(&app, &token, "phone").await;
    let aal2 = verified.str("access_token");

    // A session that never passed the factor cannot take it away, or
    // the factor would be worth nothing to whoever learned the
    // password.
    let aal1 = signed_in(&app, &email).await;
    assert_eq!(
        unenroll(&app, &aal1, &factor.factor_id).await.refusal(),
        (
            422,
            "insufficient_aal",
            "AAL2 required to unenroll verified factor"
        )
    );

    let answer = unenroll(&app, &aal2, &factor.factor_id).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert_eq!(answer.str("id"), factor.factor_id);

    // The factor is gone, the amr entry it wrote is gone, and the
    // session is back to aal1 naming nothing.
    let factors: i64 = scalar(
        &pool,
        "select count(*) from auth.mfa_factors where id = $1::text::uuid",
        &[&factor.factor_id],
    )
    .await;
    assert_eq!(factors, 0);
    let lifted = claims_of(&aal2)["session_id"]
        .as_str()
        .expect("a session id")
        .to_string();
    let session: String = scalar(
        &pool,
        "select s.aal::text || ' ' || coalesce(s.factor_id::text, 'none')
           from auth.sessions s where s.id = $1::text::uuid",
        &[&lifted],
    )
    .await;
    assert_eq!(session, "aal1 none");
    let amr: i64 = scalar(
        &pool,
        "select count(*) from auth.mfa_amr_claims a
           join auth.sessions s on s.id = a.session_id
          where s.user_id = $1::text::uuid and a.authentication_method = 'totp'",
        &[&user_id],
    )
    .await;
    assert_eq!(amr, 0, "the totp amr entry outlived the factor");
}

#[tokio::test]
async fn an_unverified_factor_can_be_taken_away_from_an_aal1_session() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-unenroll-unverified");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (_, token) = signed_up(&app, &email).await;
    let factor = enrolled(&app, &token, "phone").await;
    // Nothing was ever proved with it, so there is nothing to protect:
    // a client that started an enrollment and gave up can clear it.
    let answer = unenroll(&app, &token, &factor.factor_id).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let left: i64 = scalar(
        &pool,
        "select count(*) from auth.mfa_factors where id = $1::text::uuid",
        &[&factor.factor_id],
    )
    .await;
    assert_eq!(left, 0);
}

#[tokio::test]
async fn proving_one_factor_clears_the_half_finished_ones() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-leftovers");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let (user_id, token) = signed_up(&app, &email).await;
    let abandoned = enrolled(&app, &token, "laptop").await;
    let (proved, verified) = proved(&app, &token, "phone").await;
    assert_eq!(verified.status, StatusCode::OK, "{}", verified.body);

    // An account that started two enrollments and finished one is left
    // holding the one it finished. The other was never proved and
    // upstream clears it here rather than leaving a name taken by
    // something nobody can use.
    let left: Vec<String> = {
        let sess = pool.unscoped().await.expect("connect");
        let rows = sess
            .query(
                "select f.id::text from auth.mfa_factors f where f.user_id = $1::text::uuid",
                &[&user_id],
            )
            .await
            .expect("read factors");
        sess.commit().await.expect("park");
        rows.iter().map(|row| row.get(0)).collect()
    };
    assert_eq!(left, vec![proved.factor_id], "{}", abandoned.factor_id);
}

#[tokio::test]
async fn the_five_minutes_is_a_floor_rather_than_a_default() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-floor");
    wipe(&pool, &email).await;
    // A project that asks for thirty seconds gets five minutes, both
    // ends, which is what upstream's ApplyDefaults does after reading
    // the environment.
    let app = project(
        &dsn,
        mfa::Settings {
            challenge_expiry: 30,
            factor_expiry: 30,
            ..mfa::Settings::default()
        },
    );

    let (_, token) = signed_up(&app, &email).await;
    let factor = enrolled(&app, &token, "phone").await;
    run(
        &pool,
        "update auth.mfa_factors set created_at = now() - interval '60 seconds'
          where id = $1::text::uuid",
        &[&factor.factor_id],
    )
    .await;

    // A minute old is expired under what the project asked for and well
    // inside the floor, so the sweep the next enrollment runs leaves it
    // alone.
    let answer = totp_enroll(&app, &token, "laptop").await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let left: i64 = scalar(
        &pool,
        "select count(*) from auth.mfa_factors where id = $1::text::uuid",
        &[&factor.factor_id],
    )
    .await;
    assert_eq!(left, 1, "a factor inside the floor was swept");

    let answer = challenge(&app, &token, &factor.factor_id, None).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let expires = answer.body["expires_at"].as_i64().expect("an expiry");
    assert!((expires - (now() + 300)).abs() <= 5, "{expires}");
}

#[tokio::test]
async fn a_token_with_no_bearer_and_a_token_with_no_session_are_both_refused() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mfa-no-session");
    let sessionless_email = address("mfa-sessionless");
    wipe(&pool, &email).await;
    wipe(&pool, &sessionless_email).await;
    let app = app(&dsn);
    let _ = signed_up(&app, &email).await;

    // No bearer at all.
    let answer = post(
        &app,
        "/auth/v1/factors",
        serde_json::json!({"factor_type": "totp"}),
    )
    .await;
    assert_eq!(
        answer.refusal(),
        (
            401,
            "no_authorization",
            "This endpoint requires a valid Bearer token"
        )
    );

    // A token that names an account but no session has nothing to
    // enroll a factor against. Upstream calls that an internal error
    // rather than a refusal, because its own middleware should have
    // made it impossible to reach the handler that way.
    let (user_id, _) = signed_up(&app, &sessionless_email).await;
    let sessionless = jwt::mint(
        &serde_json::json!({
            "sub": user_id,
            "role": "authenticated",
            "aud": "authenticated",
            "iat": now(),
            "exp": now() + 3600,
        }),
        SECRET,
    );
    let answer = enroll(
        &app,
        &sessionless,
        serde_json::json!({"factor_type": "totp"}),
    )
    .await;
    assert_eq!(
        answer.refusal(),
        (
            500,
            "unexpected_failure",
            "A valid session and a registered user are required to enroll a factor"
        )
    );
}

#[tokio::test]
async fn an_anonymous_account_cannot_enroll_a_factor() {
    let Some(dsn) = dsn() else { return };
    let app = router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        external_url: Some("https://zou.test".to_string()),
        site_url: Some(SITE.to_string()),
        mailer_autoconfirm: true,
        anonymous_users: true,
        ..Config::default()
    })
    .expect("router builds");

    let answer = post(&app, "/auth/v1/signup", serde_json::json!({})).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let token = answer.str("access_token");
    assert_eq!(claims_of(&token)["is_anonymous"], true);

    // An account with no way in of its own has nothing to protect with
    // a second factor, and upstream's requireNotAnonymous turns all four
    // of these away.
    for answer in [
        enroll(&app, &token, serde_json::json!({"factor_type": "totp"})).await,
        challenge(&app, &token, "00000000-0000-0000-0000-000000000000", None).await,
        verify(
            &app,
            &token,
            "00000000-0000-0000-0000-000000000000",
            None,
            serde_json::json!({"code": "123456"}),
        )
        .await,
        unenroll(&app, &token, "00000000-0000-0000-0000-000000000000").await,
    ] {
        assert_eq!(
            answer.refusal(),
            (
                403,
                "no_authorization",
                "Anonymous user not allowed to perform these actions"
            )
        );
    }
}
