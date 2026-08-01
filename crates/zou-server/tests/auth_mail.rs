//! The email flows with the mail actually going somewhere, against a
//! live postgres.
//!
//! Everything in `auth_email` plants its codes by hand, because until
//! now nothing carried them. These tests never touch the columns: they
//! sign up, read the message out of the dev inbox the way a person
//! reads it in `zou inbox`, and then follow the link or type the code
//! back in. That is the only way to find out whether the hash written
//! to the row is the hash in the link, and it is the whole reason this
//! suite exists next to the other one.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test auth_mail

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use std::sync::Arc;
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

/// A project with nothing configured for mail, which is what gets the
/// dev inbox. The send frequency limit is left at its default of a
/// minute, because here it is one of the things under test.
fn base(dsn: &str) -> Config {
    Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        external_url: Some("https://zou.test".to_string()),
        site_url: Some(SITE.to_string()),
        ..Config::default()
    }
}

fn app(dsn: &str) -> axum::Router {
    router(base(dsn)).expect("router builds")
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
async fn follow(app: &axum::Router, url: &str) -> axum::response::Response {
    let (_, query) = url.split_once('?').expect("a link carries its token");
    let req = Request::builder()
        .method("GET")
        .uri(format!("/auth/v1/verify?{query}"))
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(req).await.expect("router answers")
}

/// One message as the inbox hands it over.
struct Message(serde_json::Value);

impl Message {
    fn to(&self) -> &str {
        self.0["to"].as_str().expect("a message has a recipient")
    }
    fn subject(&self) -> &str {
        self.0["subject"].as_str().expect("a message has a subject")
    }
    fn body(&self) -> &str {
        self.0["body"].as_str().expect("a message has a body")
    }
    fn link(&self) -> String {
        self.0["link"]
            .as_str()
            .unwrap_or_else(|| panic!("no link in: {}", self.body()))
            .to_string()
    }
    /// What the link carries back, which is what verify reads.
    fn field(&self, name: &str) -> String {
        let link = self.link();
        let (_, query) = link.split_once('?').expect("a link has a query");
        query
            .split('&')
            .find_map(|pair| pair.strip_prefix(&format!("{name}=")).map(unescape))
            .unwrap_or_else(|| panic!("no {name} in {}", self.link()))
    }
}

/// Everything the dev inbox is holding, which only the service role
/// may ask for.
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

/// The one message that was sent, which is the usual case and the one
/// worth asserting on its own.
async fn only_message(app: &axum::Router) -> Message {
    let mut kept = inbox(app).await;
    assert_eq!(kept.len(), 1, "expected exactly one message");
    kept.pop().expect("one message")
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

fn claims_of(token: &str) -> serde_json::Value {
    jwt::verify(token, SECRET)
        .expect("the access token verifies")
        .claims
}

/// A signed up, confirmed account with a session, made the way a
/// person would make one: sign up, read the email, click the link.
async fn signed_in(app: &axum::Router, email: &str) -> String {
    let signup = post(
        app,
        "/auth/v1/signup",
        serde_json::json!({"email": email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(signup.status, StatusCode::OK, "{}", signup.body);
    let mail = only_message(app).await;
    let landing = follow(app, &mail.link()).await;
    let location = landing
        .headers()
        .get("location")
        .expect("a followed link redirects")
        .to_str()
        .expect("ascii")
        .to_string();
    empty_inbox(app).await;
    let (_, fragment) = location.split_once('#').expect("the session is a fragment");
    fragment
        .split('&')
        .find_map(|pair| pair.strip_prefix("access_token=").map(unescape))
        .expect("a session came back")
}

#[tokio::test]
async fn a_signup_mails_a_link_that_confirms_the_address() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mail-signup");
    wipe(&pool, &email).await;
    let app = app(&dsn);

    let signup = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(signup.status, StatusCode::OK, "{}", signup.body);
    assert!(
        signup.body["access_token"].is_null(),
        "nothing is confirmed yet, so there is no session"
    );

    let mail = only_message(&app).await;
    assert_eq!(mail.to(), email);
    assert_eq!(mail.subject(), "Confirm your email address");
    assert_eq!(
        mail.field("type"),
        "signup",
        "the type is what verify branches on"
    );
    assert_eq!(
        mail.field("redirect_to"),
        SITE,
        "nobody asked for anywhere else, so it lands on the site url"
    );
    assert!(
        mail.link().starts_with("https://zou.test/auth/v1/verify?"),
        "the link points at this server, not at the site: {}",
        mail.link()
    );

    // The hash in the link is the hash in the row. Nothing else in
    // these tests can tell us that, because everywhere else the test
    // wrote both.
    let held: String = scalar(
        &pool,
        "select confirmation_token from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert_eq!(mail.field("token"), held);

    // And it works, which is the whole point.
    let landing = follow(&app, &mail.link()).await;
    assert_eq!(landing.status(), StatusCode::SEE_OTHER);
    let location = landing
        .headers()
        .get("location")
        .expect("a location")
        .to_str()
        .expect("ascii")
        .to_string();
    assert!(location.starts_with(SITE), "landed at {location}");
    assert!(location.contains("access_token="), "with a session");
    let confirmed: bool = scalar(
        &pool,
        "select email_confirmed_at is not null from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert!(confirmed);
}

#[tokio::test]
async fn recovery_and_magic_link_mail_a_code_that_signs_in() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let (resetting, returning) = (address("mail-reset"), address("mail-return"));
    wipe(&pool, &resetting).await;
    wipe(&pool, &returning).await;
    let app = app(&dsn);

    signed_in(&app, &resetting).await;
    let asked = post(
        &app,
        "/auth/v1/recover",
        serde_json::json!({"email": &resetting}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);
    let mail = only_message(&app).await;
    assert_eq!(mail.to(), resetting);
    assert_eq!(mail.subject(), "Reset your password");
    assert_eq!(mail.field("type"), "recovery");
    let landing = follow(&app, &mail.link()).await;
    assert_eq!(
        landing.status(),
        StatusCode::SEE_OTHER,
        "the mailed link is the one that works"
    );
    empty_inbox(&app).await;

    // A magic link is the same machinery with a different template and
    // a different type, and it signs up the address it has never seen.
    let asked = post(
        &app,
        "/auth/v1/magiclink",
        serde_json::json!({"email": &returning}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);
    // Nobody had that address, so what arrives is the confirmation:
    // the link they have to click is already in their hands.
    let mail = only_message(&app).await;
    assert_eq!(mail.subject(), "Confirm your email address");
    assert_eq!(mail.field("type"), "signup");
    follow(&app, &mail.link()).await;
    empty_inbox(&app).await;

    let asked = post(
        &app,
        "/auth/v1/magiclink",
        serde_json::json!({"email": &returning}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);
    let mail = only_message(&app).await;
    assert_eq!(mail.subject(), "Your sign-in link");
    assert_eq!(mail.field("type"), "magiclink");
    let landing = follow(&app, &mail.link()).await;
    assert_eq!(landing.status(), StatusCode::SEE_OTHER);
    let location = landing
        .headers()
        .get("location")
        .expect("a location")
        .to_str()
        .expect("ascii")
        .to_string();
    let token = location
        .split_once('#')
        .expect("a fragment")
        .1
        .split('&')
        .find_map(|pair| pair.strip_prefix("access_token=").map(unescape))
        .expect("a session");
    assert_eq!(
        claims_of(&token)["amr"][0]["method"],
        "otp",
        "clicking a link is not typing a password"
    );
}

#[tokio::test]
async fn the_reauthentication_code_is_in_the_subject_and_is_the_one_accepted() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mail-reauth");
    wipe(&pool, &email).await;
    let app = router(Config {
        reauthentication_required: true,
        ..base(&dsn)
    })
    .expect("router builds");

    let token = signed_in(&app, &email).await;
    let session = claims_of(&token)["session_id"]
        .as_str()
        .expect("a session id")
        .to_string();
    backdate(&pool, "sessions", &session, "created_at", "2 days").await;

    let asked = as_user(
        &app,
        "POST",
        "/auth/v1/reauthenticate",
        &token,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);

    let mail = only_message(&app).await;
    assert_eq!(mail.to(), email);
    // The code is in the subject line, which is upstream's doing and
    // is the reason it is worth keeping: a phone shows it on the lock
    // screen without the mail being opened.
    let code = mail
        .subject()
        .strip_suffix(" is your verification code")
        .unwrap_or_else(|| panic!("subject was {}", mail.subject()))
        .to_string();
    assert_eq!(code.len(), 6, "six digits, {code}");
    assert!(code.chars().all(|c| c.is_ascii_digit()), "{code}");
    assert!(mail.body().contains(&code), "and it is in the body too");
    assert!(
        mail.0["link"].is_null(),
        "there is nothing to click in this one"
    );

    let updated = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"password": "a different horse", "nonce": code}),
    )
    .await;
    assert_eq!(
        updated.status,
        StatusCode::OK,
        "the code that was mailed is the code that is accepted: {}",
        updated.body
    );
}

#[tokio::test]
async fn a_change_of_address_mails_both_ends() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let (old, new) = (address("mail-change-from"), address("mail-change-to"));
    wipe(&pool, &old).await;
    wipe(&pool, &new).await;
    let app = app(&dsn);

    let token = signed_in(&app, &old).await;
    let asked = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"email": &new}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);

    let kept = inbox(&app).await;
    assert_eq!(kept.len(), 2, "both ends have to answer, so both are told");
    let (to_new, to_old) = (&kept[0], &kept[1]);
    assert_eq!(to_new.to(), new, "the new address hears first");
    assert_eq!(to_old.to(), old);
    assert_eq!(to_new.subject(), "Confirm your new email address");
    assert!(
        to_new.body().contains(new.as_str()),
        "the mail says which address is being moved to: {}",
        to_new.body()
    );
    assert_ne!(
        to_new.field("token"),
        to_old.field("token"),
        "two codes, because the point is that one person can read both"
    );

    // Answering from one end is not enough, and answering from both is.
    follow(&app, &to_new.link()).await;
    let moved: String = scalar(
        &pool,
        "select email from auth.users where id = $1::text::uuid",
        &[&user_id(&pool, &old).await],
    )
    .await;
    assert_eq!(moved, old, "one answer, so nothing has moved yet");
    follow(&app, &to_old.link()).await;
    let moved: bool = scalar(
        &pool,
        "select exists (select 1 from auth.users where email = $1)",
        &[&new],
    )
    .await;
    assert!(moved, "both answered, so the address is theirs");
}

#[tokio::test]
async fn asking_again_too_soon_is_refused_and_nothing_is_sent() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mail-too-soon");
    wipe(&pool, &email).await;
    let app = app(&dsn);
    signed_in(&app, &email).await;

    let first = post(
        &app,
        "/auth/v1/recover",
        serde_json::json!({"email": &email}),
    )
    .await;
    assert_eq!(first.status, StatusCode::OK, "{}", first.body);
    let sent = only_message(&app).await.field("token");

    let again = post(
        &app,
        "/auth/v1/recover",
        serde_json::json!({"email": &email}),
    )
    .await;
    let (status, code, msg) = again.refusal();
    assert_eq!((status, code), (429, "over_email_send_rate_limit"));
    // Upstream's wording, with the seconds left in it, truncated.
    let seconds: u64 = msg
        .strip_prefix("For security purposes, you can only request this after ")
        .and_then(|rest| rest.strip_suffix(" seconds."))
        .unwrap_or_else(|| panic!("wording was {msg}"))
        .parse()
        .expect("a whole number of seconds");
    assert!((1..=60).contains(&seconds), "{seconds} seconds left");

    assert_eq!(inbox(&app).await.len(), 1, "and nothing else went out");
    let held: String = scalar(
        &pool,
        "select recovery_token from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert_eq!(
        held, sent,
        "the code that was mailed is still the live one, so the refusal cost nobody their link"
    );

    // The limit is a minute, not forever.
    let id = user_id(&pool, &email).await;
    backdate(&pool, "users", &id, "recovery_sent_at", "2 minutes").await;
    let later = post(
        &app,
        "/auth/v1/recover",
        serde_json::json!({"email": &email}),
    )
    .await;
    assert_eq!(later.status, StatusCode::OK, "{}", later.body);
    assert_eq!(inbox(&app).await.len(), 2);
}

/// A sender that cannot deliver, which is what a wrong smtp password
/// looks like from in here.
struct Broken;

impl mail::Sender for Broken {
    fn deliver(&self, _mail: &mail::Mail) -> Result<(), String> {
        Err("nobody is listening on port 25".to_string())
    }

    fn describe(&self) -> String {
        "a sender that cannot deliver".to_string()
    }
}

#[tokio::test]
async fn a_send_that_fails_takes_its_code_with_it() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mail-broken");
    wipe(&pool, &email).await;
    // Confirm the account with a working sender, then break the mail.
    let working = app(&dsn);
    signed_in(&working, &email).await;
    let app = router(Config {
        sender: Some(Arc::new(Broken)),
        ..base(&dsn)
    })
    .expect("router builds");

    let asked = post(
        &app,
        "/auth/v1/recover",
        serde_json::json!({"email": &email}),
    )
    .await;
    assert_eq!(
        asked.refusal(),
        (500, "unexpected_failure", "Error sending recovery email"),
        "upstream's words for a recovery that never left"
    );

    // The row is untouched, which is the reason the send happens
    // inside the transaction. A code written down but never sent is a
    // code the account holder cannot use and an attacker can go on
    // guessing.
    let id = user_id(&pool, &email).await;
    let clean: bool = scalar(
        &pool,
        "select recovery_token = '' and recovery_sent_at is null
           from auth.users where id = $1::text::uuid",
        &[&id],
    )
    .await;
    assert!(clean, "no code was left behind");
    let tokens: i64 = scalar(
        &pool,
        "select count(*) from auth.one_time_tokens where user_id = $1::text::uuid",
        &[&id],
    )
    .await;
    assert_eq!(tokens, 0);

    // And because nothing was written, the next attempt is not refused
    // by the send frequency limit either.
    let again = post(
        &app,
        "/auth/v1/recover",
        serde_json::json!({"email": &email}),
    )
    .await;
    assert_eq!(again.status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn the_inbox_belongs_to_the_service_role_and_to_nobody_else() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mail-inbox-gate");
    wipe(&pool, &email).await;
    let app = app(&dsn);
    post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;

    // The anon key is printed in every client bundle, so it is not a
    // bar at all, and a mailbox full of live codes is not something to
    // leave behind one.
    let req = Request::builder()
        .method("GET")
        .uri("/dev/inbox")
        .header("apikey", anon_key())
        .body(Body::empty())
        .unwrap();
    let refused = answer(app.clone().oneshot(req).await.expect("answers")).await;
    assert_eq!(refused.status, StatusCode::NOT_FOUND);
    assert_eq!(
        refused.body["message"], "no Route matched with those values",
        "and it does not admit to being there"
    );

    let req = Request::builder()
        .method("GET")
        .uri("/dev/inbox")
        .body(Body::empty())
        .unwrap();
    let refused = answer(app.clone().oneshot(req).await.expect("answers")).await;
    assert_eq!(
        refused.status,
        StatusCode::UNAUTHORIZED,
        "it is behind the gate like everything else"
    );

    assert_eq!(inbox(&app).await.len(), 1, "the service role may read it");
    empty_inbox(&app).await;
    assert!(inbox(&app).await.is_empty(), "and may throw it away");
}

#[tokio::test]
async fn the_link_carries_the_redirect_the_request_asked_for() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let (wanted, elsewhere) = (address("mail-redirect"), address("mail-redirect-bad"));
    wipe(&pool, &wanted).await;
    wipe(&pool, &elsewhere).await;
    let app = app(&dsn);

    post(
        &app,
        "/auth/v1/signup?redirect_to=https://app.zou.test/welcome",
        serde_json::json!({"email": &wanted, "password": "correct horse"}),
    )
    .await;
    let mail = only_message(&app).await;
    assert_eq!(mail.field("redirect_to"), "https://app.zou.test/welcome");
    let landing = follow(&app, &mail.link()).await;
    let location = landing
        .headers()
        .get("location")
        .expect("a location")
        .to_str()
        .expect("ascii")
        .to_string();
    assert!(
        location.starts_with("https://app.zou.test/welcome#"),
        "landed at {location}"
    );
    empty_inbox(&app).await;

    // Somewhere the project does not own is not carried into the mail
    // at all. A link that will bounce anywhere is a phishing tool, and
    // it is worse in an email than in a redirect because the email is
    // the thing people trust.
    post(
        &app,
        "/auth/v1/signup?redirect_to=https://evil.test/steal",
        serde_json::json!({"email": &elsewhere, "password": "correct horse"}),
    )
    .await;
    let mail = only_message(&app).await;
    assert_eq!(mail.field("redirect_to"), SITE);
}

/// A mail server that takes one message and writes it down. Plaintext
/// on the loopback address, which is what a local catcher is. The wire
/// format and the encrypted paths are pinned in `tests/smtp.rs`, this
/// only has to be enough to receive.
fn catcher() -> (u16, std::thread::JoinHandle<String>) {
    use std::io::{BufRead, BufReader, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
    let port = listener.local_addr().expect("bound").port();
    let handle = std::thread::spawn(move || {
        let (stream, _) = listener.accept().expect("one connection");
        let mut out = stream.try_clone().expect("a writer");
        let mut lines = BufReader::new(stream).lines();
        let mut say = |line: &str| {
            out.write_all(format!("{line}\r\n").as_bytes())
                .expect("write");
        };
        say("220 catcher ESMTP");
        let mut message = String::new();
        while let Some(Ok(line)) = lines.next() {
            match line
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_uppercase()
                .as_str()
            {
                "EHLO" => say("250 catcher"),
                "MAIL" | "RCPT" => say("250 ok"),
                "DATA" => {
                    say("354 go on");
                    for line in lines.by_ref().map_while(Result::ok) {
                        if line == "." {
                            break;
                        }
                        message.push_str(&line);
                    }
                    say("250 queued");
                }
                "QUIT" => {
                    say("221 bye");
                    break;
                }
                _ => say("500 what"),
            }
        }
        message
    });
    (port, handle)
}

#[tokio::test]
async fn with_a_mail_server_configured_the_link_goes_out_on_a_socket() {
    // The whole chain in one test: a signup mints a code, the template
    // is rendered, the transport puts it on a socket, and the link a
    // mail server received is the link that confirms the account.
    // Everything else in this suite reads the message out of memory.
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("mail-smtp");
    wipe(&pool, &email).await;
    let (port, server) = catcher();

    let mut smtp = zou_server::smtp::Smtp::new("127.0.0.1", port);
    smtp.security = zou_server::smtp::Security::None;
    smtp.admin_email = "noreply@zou.test".to_string();
    smtp.sender_name = "Zou".to_string();
    let app = router(Config {
        sender: Some(Arc::new(smtp)),
        ..base(&dsn)
    })
    .expect("router builds");

    let signup = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(signup.status, StatusCode::OK, "{}", signup.body);

    let received = server.join().expect("the mail server finished");
    assert!(
        received.contains(&format!("To: <{email}>")),
        "the envelope reached it: {received}"
    );
    let body = received
        .split_once("Content-Transfer-Encoding: base64")
        .expect("an encoded body")
        .1;
    use base64ct::Encoding;
    let html =
        String::from_utf8(base64ct::Base64::decode_vec(body.trim()).expect("the body is base64"))
            .expect("utf8");
    let link = html
        .split_once("href=\"")
        .expect("a link in the mail")
        .1
        .split('"')
        .next()
        .expect("a quoted link")
        .replace("&amp;", "&");

    // Nothing was kept in the process, because something else is
    // carrying it now.
    let req = Request::builder()
        .method("GET")
        .uri("/dev/inbox")
        .header("apikey", service_key())
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.expect("router answers");
    assert_eq!(
        res.status(),
        StatusCode::NOT_FOUND,
        "there is no mailbox to read once a mail server has the mail"
    );

    let landing = follow(&app, &link).await;
    assert_eq!(landing.status(), StatusCode::SEE_OTHER, "{link}");
    let confirmed: bool = scalar(
        &pool,
        "select email_confirmed_at is not null from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert!(confirmed, "the link that went out on the wire works");
}
