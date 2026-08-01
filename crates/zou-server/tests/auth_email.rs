//! Confirm, recover, reauthenticate and change email or password,
//! against a live postgres.
//!
//! The contract being pinned is GoTrue's. A code that was mailed is
//! never stored, only the hash of the address and the code together, so
//! these tests do what the mailer will do: draw a code, write down its
//! hash, and then present the code. That is also the reason the flows
//! answer with so little, and the tests check the little they answer as
//! carefully as the rows they write.
//!
//! Two refusals here look identical and are not: a link that cannot be
//! found says "Email link is invalid or has expired" and a posted code
//! that cannot be matched says "Token has expired or is invalid". Both
//! are 403 otp_expired, both are deliberately vague, and both are
//! upstream's exact words, so both are asserted.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test auth_email

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use zou_server::sql::Pool;
use zou_server::{Config, jwt, mail, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

/// Where a followed link lands when the link does not say otherwise.
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

/// A project that mails its confirmations, which is the default and the
/// only interesting setting for this suite.
///
/// The send frequency limit is off here. It is a real part of these
/// flows and it is pinned in `auth_mail`, but with it on a suite that
/// asks the same account twice in a second would be testing the clock
/// instead of the flow.
fn base(dsn: &str) -> Config {
    Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        external_url: Some("https://zou.test".to_string()),
        site_url: Some(SITE.to_string()),
        mail: mail::Settings {
            max_frequency: 0,
            ..mail::Settings::default()
        },
        ..Config::default()
    }
}

fn app(dsn: &str) -> axum::Router {
    router(base(dsn)).expect("router builds")
}

/// The same front door, but confirming its own signups, which is how a
/// test gets a session without going near a mailbox.
fn app_autoconfirm(dsn: &str) -> axum::Router {
    router(Config {
        mailer_autoconfirm: true,
        ..base(dsn)
    })
    .expect("router builds")
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
}

async fn answer(res: axum::response::Response) -> Answer {
    let status = res.status();
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
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

/// A request from someone holding a session, which is what the user
/// endpoints want.
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

/// A followed link: no apikey, because a mail client has none.
async fn follow(app: &axum::Router, query: &str) -> axum::response::Response {
    let req = Request::builder()
        .method("GET")
        .uri(format!("/auth/v1/verify?{query}"))
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(req).await.expect("router answers")
}

/// Where a redirect went and what it carried, which for these flows is
/// everything: the fragment is the whole of the answer.
struct Landing {
    target: String,
    fragment: std::collections::HashMap<String, String>,
}

async fn landed(res: axum::response::Response) -> Landing {
    assert_eq!(res.status(), StatusCode::SEE_OTHER, "not a redirect");
    let location = res
        .headers()
        .get("location")
        .expect("a redirect has a location")
        .to_str()
        .expect("ascii")
        .to_string();
    let (target, raw) = location.split_once('#').expect("the answer is a fragment");
    let mut fragment = std::collections::HashMap::new();
    for pair in raw.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        fragment.insert(unescape(k), unescape(v));
    }
    Landing {
        target: target.to_string(),
        fragment,
    }
}

fn unescape(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).expect("ascii");
                out.push(u8::from_str_radix(hex, 16).expect("hex"));
                i += 3;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).expect("utf8")
}

/// GoTrue's GenerateTokenHash, which is what the columns hold.
fn code_hash(email: &str, code: &str) -> String {
    use sha2::Digest;
    sha2::Sha224::digest(format!("{email}{code}").as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
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

async fn user_id(pool: &Pool, email: &str) -> String {
    scalar(
        pool,
        "select id::text from auth.users where email = $1",
        &[&email],
    )
    .await
}

/// What the mailer will do: draw a code, write its hash into the column
/// the flow reads and into the token table, and hand the code back. The
/// tests use a fixed code so they can present it afterwards.
async fn plant(pool: &Pool, user_id: &str, kind: &str, relates_to: &str, code: &str) -> String {
    let hash = code_hash(relates_to, code);
    let sent = match kind {
        "recovery_token" => "recovery_sent_at",
        "reauthentication_token" => "reauthentication_sent_at",
        "confirmation_token" => "confirmation_sent_at",
        _ => "email_change_sent_at",
    };
    run(
        pool,
        &format!("update auth.users set {kind} = $2, {sent} = now() where id = $1::text::uuid"),
        &[&user_id, &hash],
    )
    .await;
    run(
        pool,
        "delete from auth.one_time_tokens
          where user_id = $1::text::uuid and token_type::text = $2",
        &[&user_id, &kind],
    )
    .await;
    run(
        pool,
        "insert into auth.one_time_tokens
             (id, user_id, token_type, token_hash, relates_to, created_at, updated_at)
         values (gen_random_uuid(), $1::text::uuid, $2::text::auth.one_time_token_type, $3, $4,
                 now(), now())",
        &[&user_id, &kind, &hash, &relates_to],
    )
    .await;
    hash
}

/// Push a timestamp into the past, which is how a test watches
/// something expire without waiting a day for it.
async fn backdate(pool: &Pool, table: &str, id: &str, column: &str, interval: &str) {
    run(
        pool,
        &format!(
            "update auth.{table} set {column} = {column} - interval '{interval}'
              where id = $1::text::uuid"
        ),
        &[&id],
    )
    .await;
}

async fn tokens_left(pool: &Pool, user_id: &str) -> i64 {
    scalar(
        pool,
        "select count(*) from auth.one_time_tokens where user_id = $1::text::uuid",
        &[&user_id],
    )
    .await
}

fn claims_of(token: &str) -> serde_json::Value {
    jwt::verify(token, SECRET)
        .expect("the access token verifies")
        .claims
}

/// A signup that leaves a confirmation waiting, which is where most of
/// these tests start.
async fn unconfirmed(app: &axum::Router, email: &str) -> Answer {
    post(
        app,
        "/auth/v1/signup",
        serde_json::json!({"email": email, "password": "correct horse"}),
    )
    .await
}

/// A session held by someone who proved their address the way a person
/// does: they followed the link. Any project that mails its
/// confirmations gets its sessions this way, and the email change tests
/// need one, because confirming signups locally turns the two ended
/// email change off.
async fn session_by_link(app: &axum::Router, pool: &Pool, email: &str) -> String {
    unconfirmed(app, email).await;
    let id = user_id(pool, email).await;
    plant(pool, &id, "confirmation_token", email, "123456").await;
    let answer = post(
        app,
        "/auth/v1/verify",
        serde_json::json!({"type": "signup", "token": "123456", "email": email}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    answer.body["access_token"]
        .as_str()
        .expect("a session")
        .to_string()
}

/// A signup that hands back a session, which is where the user endpoint
/// tests start.
async fn confirmed(app: &axum::Router, email: &str) -> String {
    let answer = post(
        app,
        "/auth/v1/signup",
        serde_json::json!({"email": email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    answer.body["access_token"]
        .as_str()
        .expect("a session")
        .to_string()
}

#[tokio::test]
async fn a_confirmation_link_confirms_the_user_and_starts_a_session() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("verify-link");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    // Nothing was confirmed, so the signup answered with a user and no
    // session at all.
    let signup = unconfirmed(&app, &email).await;
    assert_eq!(signup.status, StatusCode::OK);
    assert!(signup.body.get("access_token").is_none(), "{}", signup.body);

    let id = user_id(&pool, &email).await;
    let hash = plant(&pool, &id, "confirmation_token", &email, "123456").await;

    let landing = landed(follow(&app, &format!("type=signup&token={hash}")).await).await;
    assert_eq!(landing.target, SITE);
    assert_eq!(landing.fragment["type"], "signup");
    assert_eq!(landing.fragment["token_type"], "bearer");
    assert_eq!(landing.fragment["expires_in"], "3600");
    assert_eq!(landing.fragment["sb"], "");
    assert!(!landing.fragment["refresh_token"].is_empty());

    // The session is a real one, and it says how it was started: a link
    // in an email rather than a password.
    let claims = claims_of(&landing.fragment["access_token"]);
    assert_eq!(claims["sub"], id.as_str());
    assert_eq!(claims["email"], email.as_str());
    assert_eq!(claims["amr"][0]["method"], "otp");

    let confirmed: bool = scalar(
        &pool,
        "select email_confirmed_at is not null and confirmation_token = ''
           from auth.users where id = $1::text::uuid",
        &[&id],
    )
    .await;
    assert!(confirmed, "the address is confirmed and the code is spent");
    let verified: bool = scalar(
        &pool,
        "select (raw_user_meta_data->>'email_verified')::bool
                and (i.identity_data->>'email_verified')::bool
           from auth.users u join auth.identities i on i.user_id = u.id
          where u.id = $1::text::uuid",
        &[&id],
    )
    .await;
    assert!(verified, "the user and its identity both say so");
    assert_eq!(tokens_left(&pool, &id).await, 0, "every code is spent");
}

#[tokio::test]
async fn a_posted_code_and_address_confirm_the_same_way() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("verify-posted");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    unconfirmed(&app, &email).await;
    let id = user_id(&pool, &email).await;
    plant(&pool, &id, "confirmation_token", &email, "123456").await;

    let answer = post(
        &app,
        "/auth/v1/verify",
        serde_json::json!({"type": "signup", "token": "123456", "email": &email}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert_eq!(answer.body["token_type"], "bearer");
    assert_eq!(answer.body["user"]["id"], id.as_str());
    let claims = claims_of(answer.body["access_token"].as_str().expect("a session"));
    assert_eq!(claims["amr"][0]["method"], "otp");
    assert!(answer.body["user"]["email_confirmed_at"].is_string());

    // The code is spent, so the same request again finds nothing.
    let again = post(
        &app,
        "/auth/v1/verify",
        serde_json::json!({"type": "signup", "token": "123456", "email": &email}),
    )
    .await;
    assert_eq!(
        again.refusal(),
        (403, "otp_expired", "Token has expired or is invalid")
    );
}

#[tokio::test]
async fn a_code_that_is_wrong_or_stale_is_refused_in_gotrues_words() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("verify-stale");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    unconfirmed(&app, &email).await;
    let id = user_id(&pool, &email).await;
    let hash = plant(&pool, &id, "confirmation_token", &email, "123456").await;

    let wrong = post(
        &app,
        "/auth/v1/verify",
        serde_json::json!({"type": "signup", "token": "654321", "email": &email}),
    )
    .await;
    assert_eq!(
        wrong.refusal(),
        (403, "otp_expired", "Token has expired or is invalid")
    );

    // An address nobody signed up with is refused in exactly the same
    // words, which is what keeps this endpoint from listing accounts.
    let nobody = post(
        &app,
        "/auth/v1/verify",
        serde_json::json!({"type": "signup", "token": "123456", "email": "nobody@zou.test"}),
    )
    .await;
    assert_eq!(nobody.refusal(), wrong.refusal());

    // A code that was mailed a fortnight ago is not a code any more,
    // and the link path says so in its own wording.
    backdate(&pool, "users", &id, "confirmation_sent_at", "14 days").await;
    let old = post(
        &app,
        "/auth/v1/verify",
        serde_json::json!({"type": "signup", "token": "123456", "email": &email}),
    )
    .await;
    assert_eq!(
        old.refusal(),
        (403, "otp_expired", "Token has expired or is invalid")
    );

    let landing = landed(follow(&app, &format!("type=signup&token={hash}")).await).await;
    assert_eq!(landing.target, SITE);
    assert_eq!(landing.fragment["error"], "access_denied");
    assert_eq!(landing.fragment["error_code"], "otp_expired");
    assert_eq!(
        landing.fragment["error_description"],
        "Email link is invalid or has expired"
    );
    assert!(!landing.fragment.contains_key("access_token"));

    let never = landed(follow(&app, "type=signup&token=deadbeef").await).await;
    assert_eq!(
        never.fragment["error_description"], "Email link is invalid or has expired",
        "a link that never existed reads like one that expired"
    );

    let still: bool = scalar(
        &pool,
        "select email_confirmed_at is null from auth.users where id = $1::text::uuid",
        &[&id],
    )
    .await;
    assert!(still, "nothing was confirmed by any of that");
}

#[tokio::test]
async fn verify_validates_its_request_the_way_gotrue_does() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);

    let cases: Vec<(serde_json::Value, (u16, &str, &str))> = vec![
        (
            serde_json::json!({}),
            (
                400,
                "validation_failed",
                "Verify requires a verification type",
            ),
        ),
        (
            serde_json::json!({"type": "signup"}),
            (
                400,
                "validation_failed",
                "Verify requires either a token or a token hash",
            ),
        ),
        (
            serde_json::json!({"type": "signup", "token": "123456", "token_hash": "abc"}),
            (
                400,
                "validation_failed",
                "Verify requires either a token or a token hash",
            ),
        ),
        (
            serde_json::json!({"type": "signup", "token": "123456"}),
            (
                400,
                "validation_failed",
                "Only an email address or phone number should be provided on verify",
            ),
        ),
        (
            serde_json::json!({"type": "signup", "token": "123456", "email": "not an address"}),
            (422, "validation_failed", "Invalid email format"),
        ),
        (
            serde_json::json!({"type": "signup", "token_hash": "abc", "email": "a@zou.test"}),
            (
                400,
                "validation_failed",
                "Only the token_hash and type should be provided",
            ),
        ),
        (
            serde_json::json!({"type": "banana", "token_hash": "abc"}),
            (400, "validation_failed", "Invalid email verification type"),
        ),
    ];
    for (body, expected) in cases {
        let answer = post(&app, "/auth/v1/verify", body.clone()).await;
        assert_eq!(answer.refusal(), expected, "for {body}");
    }

    // A link with nothing to verify is answered plainly rather than
    // bounced somewhere, because a request this broken never reached
    // the flow and there is nowhere trustworthy to send it.
    let res = follow(&app, "type=signup").await;
    let answer = answer(res).await;
    assert_eq!(
        answer.refusal(),
        (
            400,
            "validation_failed",
            "Verify requires a token or a token hash"
        )
    );
}

#[tokio::test]
async fn a_link_lands_where_the_project_owns_and_nowhere_else() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let app = app(&dsn);

    // The hash rides under either name, because the older mail
    // templates put it in token and the newer ones in token_hash.
    for (tag, carrier, wanted, expected) in [
        (
            "verify-redirect-own",
            "token_hash",
            "https://app.zou.test/welcome",
            "https://app.zou.test/welcome",
        ),
        (
            "verify-redirect-evil",
            "token",
            "https://evil.test/steal",
            SITE,
        ),
        (
            "verify-redirect-scheme",
            "token",
            "http://app.zou.test/welcome",
            SITE,
        ),
    ] {
        let email = address(tag);
        wipe(&pool, &email).await;
        unconfirmed(&app, &email).await;
        let id = user_id(&pool, &email).await;
        let hash = plant(&pool, &id, "confirmation_token", &email, "123456").await;

        let landing = landed(
            follow(
                &app,
                &format!(
                    "type=signup&{carrier}={hash}&redirect_to={}",
                    encoded(wanted)
                ),
            )
            .await,
        )
        .await;
        assert_eq!(landing.target, expected, "asked for {wanted}");
        assert!(landing.fragment.contains_key("access_token"));
    }
}

fn encoded(s: &str) -> String {
    s.replace(':', "%3A").replace('/', "%2F")
}

#[tokio::test]
async fn recover_writes_a_code_and_says_nothing_about_who_has_an_account() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("recover-start");
    wipe(&pool, &email).await;
    let app = app(&dsn);
    unconfirmed(&app, &email).await;
    let id = user_id(&pool, &email).await;

    let known = post(
        &app,
        "/auth/v1/recover",
        serde_json::json!({"email": &email}),
    )
    .await;
    assert_eq!(known.status, StatusCode::OK);
    assert_eq!(known.body, serde_json::json!({}));

    let unknown = post(
        &app,
        "/auth/v1/recover",
        serde_json::json!({"email": "nobody-here@zou.test"}),
    )
    .await;
    assert_eq!(
        (unknown.status, &unknown.body),
        (known.status, &known.body),
        "an address nobody has reads exactly like one somebody has"
    );
    let invented: i64 = scalar(
        &pool,
        "select count(*) from auth.users where email = $1",
        &[&"nobody-here@zou.test"],
    )
    .await;
    assert_eq!(invented, 0, "and it created nothing");

    let staged: bool = scalar(
        &pool,
        "select recovery_token <> '' and recovery_sent_at is not null
           from auth.users where id = $1::text::uuid",
        &[&id],
    )
    .await;
    assert!(staged, "a code is waiting for the address");
    let waiting: i64 = scalar(
        &pool,
        "select count(*) from auth.one_time_tokens
          where user_id = $1::text::uuid and token_type::text = 'recovery_token'",
        &[&id],
    )
    .await;
    assert_eq!(waiting, 1);

    let empty = post(&app, "/auth/v1/recover", serde_json::json!({})).await;
    assert_eq!(
        empty.refusal(),
        (
            400,
            "validation_failed",
            "Password recovery requires an email"
        )
    );
}

#[tokio::test]
async fn a_recovery_code_starts_a_session_and_confirms_the_address() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("recover-verify");
    wipe(&pool, &email).await;
    let app = app(&dsn);
    unconfirmed(&app, &email).await;
    let id = user_id(&pool, &email).await;
    plant(&pool, &id, "recovery_token", &email, "123456").await;

    let answer = post(
        &app,
        "/auth/v1/verify",
        serde_json::json!({"type": "recovery", "token": "123456", "email": &email}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let claims = claims_of(answer.body["access_token"].as_str().expect("a session"));
    assert_eq!(claims["amr"][0]["method"], "otp");

    // Following a recovery link proves the address as surely as a
    // confirmation link does, so an account that never confirmed is
    // confirmed by it.
    let state: bool = scalar(
        &pool,
        "select recovery_token = '' and email_confirmed_at is not null
           from auth.users where id = $1::text::uuid",
        &[&id],
    )
    .await;
    assert!(state);
    assert_eq!(tokens_left(&pool, &id).await, 0);
}

#[tokio::test]
async fn a_password_update_needs_a_bearer_and_ends_every_other_session() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("update-password");
    wipe(&pool, &email).await;
    let app = app_autoconfirm(&dsn);

    // A project key alone is not a session, and the refusal says which
    // of the two the endpoint wanted.
    let req = Request::builder()
        .method("PUT")
        .uri("/auth/v1/user")
        .header("apikey", anon_key())
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();
    let bare = answer(app.clone().oneshot(req).await.expect("router answers")).await;
    assert_eq!(
        bare.refusal(),
        (
            401,
            "no_authorization",
            "This endpoint requires a valid Bearer token"
        )
    );

    let token = confirmed(&app, &email).await;
    let id = user_id(&pool, &email).await;

    // A second sign in, so there is a session to lose.
    let second = post(
        &app,
        "/auth/v1/token?grant_type=password",
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(second.status, StatusCode::OK, "{}", second.body);
    let sessions: i64 = scalar(
        &pool,
        "select count(*) from auth.sessions where user_id = $1::text::uuid",
        &[&id],
    )
    .await;
    assert_eq!(sessions, 2);

    for (password, expected) in [
        (
            "correct horse",
            (
                422,
                "same_password",
                "New password should be different from the old password.",
            ),
        ),
        (
            "short",
            (
                422,
                "weak_password",
                "Password should be at least 6 characters.",
            ),
        ),
    ] {
        let answer = as_user(
            &app,
            "PUT",
            "/auth/v1/user",
            &token,
            serde_json::json!({"password": password}),
        )
        .await;
        assert_eq!(answer.refusal(), expected);
    }
    let long = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"password": "x".repeat(73)}),
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

    let done = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"password": "a different horse"}),
    )
    .await;
    assert_eq!(done.status, StatusCode::OK, "{}", done.body);
    assert_eq!(done.body["id"], id.as_str());

    // The old password is gone and the new one works.
    let stale = post(
        &app,
        "/auth/v1/token?grant_type=password",
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(stale.status, StatusCode::BAD_REQUEST);
    let fresh = post(
        &app,
        "/auth/v1/token?grant_type=password",
        serde_json::json!({"email": &email, "password": "a different horse"}),
    )
    .await;
    assert_eq!(fresh.status, StatusCode::OK, "{}", fresh.body);

    // The session that changed the password survived and the other one
    // did not, which is the point of changing it. The sign in above
    // started one more, hence two.
    let left: i64 = scalar(
        &pool,
        "select count(*) from auth.sessions where user_id = $1::text::uuid",
        &[&id],
    )
    .await;
    assert_eq!(left, 2, "mine and the one the last sign in started");
    let mine = claims_of(&token)["session_id"]
        .as_str()
        .unwrap()
        .to_string();
    let survived: i64 = scalar(
        &pool,
        "select count(*) from auth.sessions where id = $1::text::uuid",
        &[&mine],
    )
    .await;
    assert_eq!(survived, 1, "the session that asked is still good");
    assert_eq!(tokens_left(&pool, &id).await, 0);
}

#[tokio::test]
async fn an_email_change_waits_for_both_addresses() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let (old, new) = (address("change-from"), address("change-to"));
    wipe(&pool, &old).await;
    wipe(&pool, &new).await;
    // Not autoconfirming, because a project that confirms its own
    // signups is also saying it does not need the old address to agree
    // to a change.
    let app = app(&dsn);
    let token = session_by_link(&app, &pool, &old).await;
    let id = user_id(&pool, &old).await;

    let staged = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"email": &new}),
    )
    .await;
    assert_eq!(staged.status, StatusCode::OK, "{}", staged.body);
    assert_eq!(
        staged.body["email"], old,
        "nothing moves until both ends answer"
    );
    assert_eq!(staged.body["new_email"], new.as_str());

    let both: bool = scalar(
        &pool,
        "select email = $2 and email_change = $3
                and email_change_token_current <> '' and email_change_token_new <> ''
                and email_change_confirm_status = 0
           from auth.users where id = $1::text::uuid",
        &[&id, &old, &new],
    )
    .await;
    assert!(both, "two codes are out, one to each address");

    let to_current = plant(&pool, &id, "email_change_token_current", &old, "111111").await;
    let to_new = plant(&pool, &id, "email_change_token_new", &new, "222222").await;

    // The old address answers first. It moves nothing, which is what
    // stops one leaked link from taking an account.
    let first = post(
        &app,
        "/auth/v1/verify",
        serde_json::json!({"type": "email_change", "token_hash": to_current}),
    )
    .await;
    assert_eq!(first.status, StatusCode::OK, "{}", first.body);
    assert_eq!(
        first.body["msg"],
        "Confirmation link accepted. Please proceed to confirm link sent to the other email"
    );
    assert_eq!(first.body["code"], "200");
    assert!(first.body.get("access_token").is_none());
    let waiting: bool = scalar(
        &pool,
        "select email = $2 and email_change_confirm_status = 1
                and email_change_token_current = ''
           from auth.users where id = $1::text::uuid",
        &[&id, &old],
    )
    .await;
    assert!(waiting, "the address has not moved and one code is spent");

    let second = post(
        &app,
        "/auth/v1/verify",
        serde_json::json!({"type": "email_change", "token_hash": to_new}),
    )
    .await;
    assert_eq!(second.status, StatusCode::OK, "{}", second.body);
    assert_eq!(second.body["user"]["email"], new.as_str());
    let claims = claims_of(second.body["access_token"].as_str().expect("a session"));
    assert_eq!(claims["email"], new.as_str());

    let moved: bool = scalar(
        &pool,
        "select u.email = $2 and u.email_change = ''
                and u.email_change_token_new = '' and u.email_change_confirm_status = 0
                and i.identity_data->>'email' = $2
                and (i.identity_data->>'email_verified')::bool
           from auth.users u join auth.identities i on i.user_id = u.id
          where u.id = $1::text::uuid",
        &[&id, &new],
    )
    .await;
    assert!(moved, "the address moved and the identity says so");
    assert_eq!(tokens_left(&pool, &id).await, 0);
}

#[tokio::test]
async fn a_project_that_confirms_one_end_moves_on_the_first_answer() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let (old, new) = (address("single-from"), address("single-to"));
    wipe(&pool, &old).await;
    wipe(&pool, &new).await;
    let app = router(Config {
        secure_email_change: false,
        ..base(&dsn)
    })
    .expect("router builds");
    let token = session_by_link(&app, &pool, &old).await;
    let id = user_id(&pool, &old).await;

    as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"email": &new}),
    )
    .await;
    let one: bool = scalar(
        &pool,
        "select email_change_token_current = '' and email_change_token_new <> ''
           from auth.users where id = $1::text::uuid",
        &[&id],
    )
    .await;
    assert!(one, "only the new address is asked");

    let hash = plant(&pool, &id, "email_change_token_new", &new, "222222").await;
    let answer = post(
        &app,
        "/auth/v1/verify",
        serde_json::json!({"type": "email_change", "token_hash": hash}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert_eq!(answer.body["user"]["email"], new.as_str());
}

#[tokio::test]
async fn reauthentication_gates_a_password_update_when_the_project_asks() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("reauth");
    wipe(&pool, &email).await;
    let app = router(Config {
        mailer_autoconfirm: true,
        reauthentication_required: true,
        ..base(&dsn)
    })
    .expect("router builds");
    let token = confirmed(&app, &email).await;
    let id = user_id(&pool, &email).await;

    // A session started a minute ago is proof enough on its own.
    let fresh = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"password": "a different horse"}),
    )
    .await;
    assert_eq!(fresh.status, StatusCode::OK, "{}", fresh.body);

    // An old one is not, and the refusal says what to do about it.
    let session = claims_of(&token)["session_id"]
        .as_str()
        .expect("a session id")
        .to_string();
    backdate(&pool, "sessions", &session, "created_at", "2 days").await;
    let stale = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"password": "yet another horse"}),
    )
    .await;
    assert_eq!(
        stale.refusal(),
        (
            400,
            "reauthentication_needed",
            "Password update requires reauthentication"
        )
    );

    let asked = as_user(
        &app,
        "POST",
        "/auth/v1/reauthenticate",
        &token,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);
    assert_eq!(asked.body, serde_json::json!({}));
    let staged: bool = scalar(
        &pool,
        "select reauthentication_token <> '' and reauthentication_sent_at is not null
           from auth.users where id = $1::text::uuid",
        &[&id],
    )
    .await;
    assert!(staged);

    let code = plant(&pool, &id, "reauthentication_token", &email, "123456").await;
    assert!(!code.is_empty());
    let wrong = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"password": "yet another horse", "nonce": "654321"}),
    )
    .await;
    assert_eq!(
        wrong.refusal(),
        (
            422,
            "reauthentication_not_valid",
            "Nonce has expired or is invalid"
        )
    );

    let done = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"password": "yet another horse", "nonce": "123456"}),
    )
    .await;
    assert_eq!(done.status, StatusCode::OK, "{}", done.body);
    let spent: bool = scalar(
        &pool,
        "select reauthentication_token = '' from auth.users where id = $1::text::uuid",
        &[&id],
    )
    .await;
    assert!(spent, "the nonce is good once");
}

#[tokio::test]
async fn metadata_merges_and_a_null_removes_a_key() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("update-metadata");
    wipe(&pool, &email).await;
    let app = app_autoconfirm(&dsn);
    let signup = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({
            "email": &email,
            "password": "correct horse",
            "data": {"nickname": "tester", "colour": "blue"},
        }),
    )
    .await;
    let token = signup.body["access_token"].as_str().expect("a session");

    let answer = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        token,
        serde_json::json!({"data": {"colour": serde_json::Value::Null, "city": "hanoi"}}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let data = &answer.body["user_metadata"];
    assert_eq!(data["nickname"], "tester", "an untouched key stays");
    assert_eq!(data["city"], "hanoi", "a new key lands");
    assert!(data.get("colour").is_none(), "a null removes a key: {data}");
    assert_eq!(data["email_verified"], true, "and nothing else moved");

    // app_metadata is what the project asserts about a user, so a user
    // does not get to assert it about themselves.
    let refused = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        token,
        serde_json::json!({"app_metadata": {"role": "admin"}}),
    )
    .await;
    assert_eq!(
        refused.refusal(),
        (
            403,
            "not_admin",
            "Updating app_metadata requires admin privileges"
        )
    );
}

#[tokio::test]
async fn a_magic_link_is_a_recovery_code_under_another_name() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("magic-known");
    wipe(&pool, &email).await;
    let app = app_autoconfirm(&dsn);
    confirmed(&app, &email).await;
    let id = user_id(&pool, &email).await;

    let asked = post(
        &app,
        "/auth/v1/magiclink",
        serde_json::json!({"email": &email}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);
    assert_eq!(asked.body, serde_json::json!({}), "it says nothing at all");

    let staged: bool = scalar(
        &pool,
        "select recovery_token <> '' and recovery_sent_at is not null
           from auth.users where id = $1::text::uuid",
        &[&id],
    )
    .await;
    assert!(staged, "the same column a recovery uses");

    // And the same verify, under the type a client sends for a link it
    // did not ask a password question about.
    plant(&pool, &id, "recovery_token", &email, "123456").await;
    let answer = post(
        &app,
        "/auth/v1/verify",
        serde_json::json!({"type": "magiclink", "token": "123456", "email": &email}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert_eq!(answer.body["user"]["id"], id.as_str());
    let claims = claims_of(answer.body["access_token"].as_str().expect("a session"));
    assert_eq!(claims["amr"][0]["method"], "otp");
}

#[tokio::test]
async fn a_magic_link_signs_up_an_address_nobody_holds() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");

    // Confirming locally: the account is made, confirmed, and the code
    // that signs them in goes out on the spot.
    let quick = address("magic-new-quick");
    wipe(&pool, &quick).await;
    let local = app_autoconfirm(&dsn);
    let asked = post(
        &local,
        "/auth/v1/magiclink",
        serde_json::json!({"email": &quick, "data": {"nickname": "tester"}}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK);
    assert_eq!(asked.body, serde_json::json!({}));
    let ready: bool = scalar(
        &pool,
        "select email_confirmed_at is not null and recovery_token <> ''
                and raw_user_meta_data->>'nickname' = 'tester'
           from auth.users where email = $1",
        &[&quick],
    )
    .await;
    assert!(ready, "signed up, confirmed, and a code is waiting");

    // Mailing confirmations: the confirmation is the link, so there is
    // no second code to send.
    let slow = address("magic-new-slow");
    wipe(&pool, &slow).await;
    let mailing = app(&dsn);
    let asked = post(
        &mailing,
        "/auth/v1/magiclink",
        serde_json::json!({"email": &slow}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK);
    let waiting: bool = scalar(
        &pool,
        "select email_confirmed_at is null and confirmation_token <> '' and recovery_token = ''
           from auth.users where email = $1",
        &[&slow],
    )
    .await;
    assert!(waiting, "one code, and it is the confirmation");

    // An address that is known but never proved is the same situation,
    // so it takes the same path rather than being told to confirm the
    // first link it already lost.
    let asked = post(
        &mailing,
        "/auth/v1/magiclink",
        serde_json::json!({"email": &slow}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK);
    let one: i64 = scalar(
        &pool,
        "select count(*) from auth.users where email = $1",
        &[&slow],
    )
    .await;
    assert_eq!(one, 1, "and it did not sign anybody up twice");
    let still: bool = scalar(
        &pool,
        "select confirmation_token <> '' and recovery_token = ''
           from auth.users where email = $1",
        &[&slow],
    )
    .await;
    assert!(
        still,
        "and it did not send a sign in code to an address nobody has proved"
    );

    let empty = post(&mailing, "/auth/v1/magiclink", serde_json::json!({})).await;
    assert_eq!(
        empty.refusal(),
        (
            422,
            "validation_failed",
            "Password recovery requires an email"
        ),
        "upstream's wording, and its status, which is not recover's"
    );
}

#[tokio::test]
async fn the_otp_endpoint_is_the_magic_link_with_one_more_rule() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let app = app_autoconfirm(&dsn);

    for (body, expected) in [
        (
            serde_json::json!({"email": "a@zou.test", "phone": "+15551234567"}),
            (
                400,
                "validation_failed",
                "Only an email address or phone number should be provided",
            ),
        ),
        (
            serde_json::json!({"email": "a@zou.test", "channel": "sms"}),
            (
                400,
                "validation_failed",
                "Channel should only be specified with Phone OTP",
            ),
        ),
        (
            serde_json::json!({}),
            (
                400,
                "validation_failed",
                "One of email or phone must be set",
            ),
        ),
    ] {
        let answer = post(&app, "/auth/v1/otp", body.clone()).await;
        assert_eq!(answer.refusal(), expected, "for {body}");
    }

    // A phone is a real branch upstream and not here yet, so it says so
    // rather than pretending to have sent anything.
    let phone = post(
        &app,
        "/auth/v1/otp",
        serde_json::json!({"phone": "+15551234567"}),
    )
    .await;
    assert_eq!(phone.status, StatusCode::NOT_IMPLEMENTED);

    // create_user false turns it into a sign in, so an address nobody
    // holds is refused instead of registered.
    let stranger = address("otp-stranger");
    wipe(&pool, &stranger).await;
    let refused = post(
        &app,
        "/auth/v1/otp",
        serde_json::json!({"email": &stranger, "create_user": false}),
    )
    .await;
    assert_eq!(
        refused.refusal(),
        (422, "otp_disabled", "Signups not allowed for otp")
    );
    let none: i64 = scalar(
        &pool,
        "select count(*) from auth.users where email = $1",
        &[&stranger],
    )
    .await;
    assert_eq!(none, 0, "and nobody was created");

    // Without the flag it is a magic link, so the same address is
    // registered instead: upstream defaults create_user to true.
    let asked = post(
        &app,
        "/auth/v1/otp",
        serde_json::json!({"email": &stranger}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);
    let made: i64 = scalar(
        &pool,
        "select count(*) from auth.users where email = $1 and recovery_token <> ''",
        &[&stranger],
    )
    .await;
    assert_eq!(made, 1, "and this time it did sign them up");

    // The same request for somebody who does have an account sends the
    // code, which is the whole point of the flag.
    let known = address("otp-known");
    wipe(&pool, &known).await;
    confirmed(&app, &known).await;
    let answer = post(
        &app,
        "/auth/v1/otp",
        serde_json::json!({"email": &known, "create_user": false}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert_eq!(answer.body, serde_json::json!({}));
    let staged: bool = scalar(
        &pool,
        "select recovery_token <> '' from auth.users where email = $1",
        &[&known],
    )
    .await;
    assert!(staged);
}
