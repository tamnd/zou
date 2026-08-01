//! `/auth/v1/admin/generate_link` and `/auth/v1/invite`, against a live
//! postgres.
//!
//! These two are the service role reaching into the email flows: one
//! writes down everything a flow writes down and hands back the link
//! instead of posting it, the other makes an account for somebody who
//! never asked and posts the link to them. The only assertion worth
//! making about either is whether the link works, so every test here
//! follows the link it was given and looks at what happened to the
//! account afterwards.
//!
//! The code and its hash are recomputed here from the address rather
//! than read back out of the server, so a change to how either is drawn
//! shows up as a failure rather than as agreement with itself.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test auth_admin_links

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use zou_server::sql::Pool;
use zou_server::{Config, jwt, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

/// Where a followed link lands when nothing else says otherwise.
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

fn base(dsn: &str) -> Config {
    Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        external_url: Some("https://zou.test".to_string()),
        site_url: Some(SITE.to_string()),
        ..Config::default()
    }
}

/// A project with nothing configured for mail, so the dev inbox holds
/// what goes out, and one that mails its confirmations rather than
/// confirming them itself, which is the interesting half of this
/// surface.
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
    fn text(&self, key: &str) -> String {
        self.body[key]
            .as_str()
            .unwrap_or_else(|| panic!("no {key} in {}", self.body))
            .to_string()
    }
    fn id(&self) -> String {
        self.text("id")
    }
    /// What the link carries back, which is what verify reads.
    fn carried(&self, name: &str) -> String {
        let link = self.text("action_link");
        let (_, query) = link.split_once('?').expect("a link has a query");
        query
            .split('&')
            .find_map(|pair| pair.strip_prefix(&format!("{name}=")).map(unescape))
            .unwrap_or_else(|| panic!("no {name} in {link}"))
    }
}

async fn answer(res: axum::response::Response) -> Answer {
    let status = res.status();
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let body = match bytes.is_empty() {
        true => serde_json::Value::Null,
        false => serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| panic!("not json: {}", String::from_utf8_lossy(&bytes))),
    };
    Answer { status, body }
}

/// A request from whoever is holding `token`.
async fn sent(app: &axum::Router, path: &str, token: &str, body: serde_json::Value) -> Answer {
    let mut req = Request::builder()
        .method("POST")
        .uri(path)
        .header("apikey", anon_key())
        .header("content-type", "application/json");
    if !token.is_empty() {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let req = req.body(Body::from(body.to_string())).unwrap();
    answer(app.clone().oneshot(req).await.expect("router answers")).await
}

async fn link(app: &axum::Router, body: serde_json::Value) -> Answer {
    sent(app, "/auth/v1/admin/generate_link", &service_key(), body).await
}

async fn invite(app: &axum::Router, body: serde_json::Value) -> Answer {
    sent(app, "/auth/v1/invite", &service_key(), body).await
}

async fn signs_up(app: &axum::Router, email: &str, password: &str) -> Answer {
    sent(
        app,
        "/auth/v1/signup",
        "",
        serde_json::json!({"email": email, "password": password}),
    )
    .await
}

async fn signs_in(app: &axum::Router, email: &str, password: &str) -> Answer {
    sent(
        app,
        "/auth/v1/token?grant_type=password",
        "",
        serde_json::json!({"email": email, "password": password}),
    )
    .await
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

/// Where a followed link put the browser, and what it put in the
/// fragment, which is where the supabase clients read a session from.
async fn landed(app: &axum::Router, url: &str) -> String {
    let res = follow(app, url).await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER, "{:?}", res.body());
    res.headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .expect("a redirect has a location")
        .to_string()
}

/// Everything the dev inbox is holding.
async fn inbox(app: &axum::Router) -> Vec<serde_json::Value> {
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
        .clone()
}

/// How many of the kept messages are invitations, for the tests where
/// something else has been mailed as well.
async fn invitations(app: &axum::Router) -> usize {
    inbox(app)
        .await
        .iter()
        .filter(|m| m["template"].as_str() == Some("invite"))
        .count()
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

/// GoTrue's GenerateTokenHash, written out again here rather than
/// reached for in the server: the hex of sha224 over the address and the
/// code together.
fn token_hash(email: &str, otp: &str) -> String {
    use sha2::Digest;
    sha2::Sha224::digest(format!("{email}{otp}").as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn address(tag: &str) -> String {
    format!("link-{tag}@zou.test")
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

async fn pool(dsn: &str) -> Pool {
    Pool::new(dsn, 4).expect("pool")
}

// The gate, which is the same one the rest of the admin box has and is
// worth pinning again on the endpoint that is not under /admin at all.

#[tokio::test]
async fn neither_endpoint_answers_anybody_but_an_admin() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = pool(&dsn).await;
    let email = address("gate");
    wipe(&pool, &email).await;

    for path in ["/auth/v1/admin/generate_link", "/auth/v1/invite"] {
        let body = serde_json::json!({"type": "magiclink", "email": email});
        let none = sent(&app, path, "", body.clone()).await;
        assert_eq!(
            none.refusal(),
            (
                401,
                "no_authorization",
                "This endpoint requires a valid Bearer token"
            ),
            "{path} with no bearer"
        );
        let anon = sent(&app, path, &anon_key(), body.clone()).await;
        assert_eq!(
            anon.refusal(),
            (403, "not_admin", "User not allowed"),
            "{path} with the anon key"
        );
        // Nothing was made on the way past either refusal.
        let rows: i64 = scalar(
            &pool,
            "select count(*) from auth.users where email = $1",
            &[&email],
        )
        .await;
        assert_eq!(rows, 0, "{path} made an account for a caller it refused");
    }
}

// generate_link.

#[tokio::test]
async fn a_signup_link_is_an_account_the_link_signs_in_to() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = pool(&dsn).await;
    let email = address("signup");
    wipe(&pool, &email).await;

    let made = link(
        &app,
        serde_json::json!({
            "type": "signup",
            "email": email,
            "password": "correct horse battery staple",
            "data": {"full_name": "Ada"},
        }),
    )
    .await;
    assert_eq!(made.status, StatusCode::OK, "{}", made.body);
    assert_eq!(made.text("verification_type"), "signup");
    assert_eq!(made.text("redirect_to"), SITE);
    assert_eq!(made.body["user_metadata"]["full_name"], "Ada");
    // The code in the answer and the hash in the answer are the same
    // code and the hash of it, and the link carries that hash.
    assert_eq!(
        made.text("hashed_token"),
        token_hash(&email, &made.text("email_otp"))
    );
    assert_eq!(made.carried("token"), made.text("hashed_token"));
    assert_eq!(made.carried("type"), "signup");

    // A signup link is a confirmation code, in the column a confirmation
    // is read out of.
    let stored: String = scalar(
        &pool,
        "select confirmation_token from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert_eq!(stored, made.text("hashed_token"));

    // Nothing about the account is settled until the link is followed.
    let confirmed: bool = scalar(
        &pool,
        "select email_confirmed_at is not null from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert!(
        !confirmed,
        "the account was confirmed before anybody clicked"
    );
    assert_eq!(
        signs_in(&app, &email, "correct horse battery staple")
            .await
            .refusal(),
        (400, "email_not_confirmed", "Email not confirmed")
    );

    let landing = landed(&app, &made.text("action_link")).await;
    assert!(
        landing.starts_with(&format!("{SITE}#")) && landing.contains("access_token="),
        "the followed link did not land with a session: {landing}"
    );
    // And now the password that went in with the link works.
    let session = signs_in(&app, &email, "correct horse battery staple").await;
    assert_eq!(session.status, StatusCode::OK, "{}", session.body);
    assert_eq!(
        session.body["user"]["id"].as_str(),
        Some(made.id().as_str())
    );
}

#[tokio::test]
async fn a_magic_link_for_an_address_nobody_has_becomes_a_signup() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = pool(&dsn).await;
    let email = address("stranger");
    wipe(&pool, &email).await;

    let made = link(
        &app,
        serde_json::json!({"type": "magiclink", "email": email}),
    )
    .await;
    assert_eq!(made.status, StatusCode::OK, "{}", made.body);
    // Upstream turns it into a signup and says so, which is what a
    // client branches on to know it invited somebody.
    assert_eq!(made.text("verification_type"), "signup");
    assert_eq!(made.carried("type"), "signup");

    // The password it made is one nobody knows, so the account is real
    // and the link is the only way into it.
    let empty: bool = scalar(
        &pool,
        "select coalesce(encrypted_password, '') = '' from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert!(!empty, "the account was left with no password at all");
    assert_eq!(
        signs_in(&app, &email, "guessing").await.refusal(),
        (400, "invalid_credentials", "Invalid login credentials")
    );

    let landing = landed(&app, &made.text("action_link")).await;
    assert!(landing.contains("access_token="), "{landing}");
    // The account is confirmed now, so a password that matched would be
    // a session. The password is drawn rather than derived from anything
    // in the request, so none of what somebody would try gets in.
    for guess in [" ", "            ", "password", email.as_str()] {
        assert_ne!(
            signs_in(&app, &email, guess).await.status,
            StatusCode::OK,
            "signed in with {guess:?}"
        );
    }
}

#[tokio::test]
async fn a_magic_link_for_an_account_that_exists_stays_one() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = pool(&dsn).await;
    let email = address("known");
    wipe(&pool, &email).await;
    assert_eq!(
        signs_up(&app, &email, "correct horse battery staple")
            .await
            .status,
        StatusCode::OK
    );

    let made = link(
        &app,
        serde_json::json!({"type": "magiclink", "email": email}),
    )
    .await;
    assert_eq!(made.status, StatusCode::OK, "{}", made.body);
    assert_eq!(made.text("verification_type"), "magiclink");
    assert_eq!(made.carried("type"), "magiclink");
    // A magic link is a recovery token, which is where verify looks for
    // it, and it is the hash of the code that came back.
    let stored: String = scalar(
        &pool,
        "select recovery_token from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert_eq!(stored, made.text("hashed_token"));

    let landing = landed(&app, &made.text("action_link")).await;
    assert!(landing.contains("access_token="), "{landing}");
}

#[tokio::test]
async fn a_signup_link_for_an_account_that_never_confirmed_takes_it_over() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = pool(&dsn).await;
    let email = address("halfway");
    wipe(&pool, &email).await;
    let first = signs_up(&app, &email, "correct horse battery staple").await;
    assert_eq!(first.status, StatusCode::OK, "{}", first.body);
    let id = first.body["id"].as_str().expect("an id").to_string();

    let made = link(
        &app,
        serde_json::json!({
            "type": "signup",
            "email": email,
            "password": "another one entirely",
            "data": {"full_name": "Ada"},
        }),
    )
    .await;
    assert_eq!(made.status, StatusCode::OK, "{}", made.body);
    // The same account, not a second one on the same address, and the
    // metadata the link carried is merged into it.
    assert_eq!(made.id(), id);
    assert_eq!(made.body["user_metadata"]["full_name"], "Ada");
    // The password on the row is left as the signup set it, which is
    // upstream's: whoever generated this link did not prove they are the
    // one who started the signup.
    landed(&app, &made.text("action_link")).await;
    assert_eq!(
        signs_in(&app, &email, "another one entirely")
            .await
            .refusal(),
        (400, "invalid_credentials", "Invalid login credentials")
    );
    let session = signs_in(&app, &email, "correct horse battery staple").await;
    assert_eq!(session.status, StatusCode::OK, "{}", session.body);
}

#[tokio::test]
async fn the_links_that_need_an_account_refuse_when_there_is_none() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = pool(&dsn).await;
    let email = address("nobody");
    wipe(&pool, &email).await;

    for kind in ["recovery", "email_change_current", "email_change_new"] {
        let refused = link(
            &app,
            serde_json::json!({
                "type": kind,
                "email": email,
                "new_email": address("nobody-new"),
            }),
        )
        .await;
        assert_eq!(
            refused.refusal(),
            (404, "user_not_found", "User with this email not found"),
            "type {kind}"
        );
    }
    let made: i64 = scalar(
        &pool,
        "select count(*) from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert_eq!(made, 0, "a refused link left an account behind");
}

#[tokio::test]
async fn an_invite_link_leaves_an_account_that_was_invited() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = pool(&dsn).await;
    let email = address("invited-link");
    wipe(&pool, &email).await;

    let made = link(
        &app,
        serde_json::json!({"type": "invite", "email": email, "data": {"team": "core"}}),
    )
    .await;
    assert_eq!(made.status, StatusCode::OK, "{}", made.body);
    assert_eq!(made.text("verification_type"), "invite");
    assert_eq!(made.carried("type"), "invite");
    assert_eq!(made.body["user_metadata"]["team"], "core");
    // An invitation is written down as one, which is the only thing
    // that tells an invited account from a signed up one afterwards.
    let invited: bool = scalar(
        &pool,
        "select invited_at is not null from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert!(invited, "the account does not say it was invited");
    let stored: String = scalar(
        &pool,
        "select confirmation_token from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert_eq!(stored, made.text("hashed_token"));
    // Nothing to sign in with until the invitation is taken up.
    let hashed: String = scalar(
        &pool,
        "select coalesce(encrypted_password, '') from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert_eq!(hashed, "", "an invited account was given a password");

    let landing = landed(&app, &made.text("action_link")).await;
    assert!(landing.contains("access_token="), "{landing}");
    let confirmed: bool = scalar(
        &pool,
        "select email_confirmed_at is not null from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert!(confirmed, "following the invitation did not confirm it");
}

#[tokio::test]
async fn a_change_of_address_takes_both_of_its_links() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = pool(&dsn).await;
    let (old, new) = (address("mover"), address("moved"));
    wipe(&pool, &old).await;
    wipe(&pool, &new).await;
    assert_eq!(
        signs_up(&app, &old, "correct horse battery staple")
            .await
            .status,
        StatusCode::OK
    );
    // Confirmed, because an address in flight is a change from one the
    // account already holds.
    let id: String = scalar(
        &pool,
        "update auth.users set email_confirmed_at = now()
          where email = $1 returning id::text",
        &[&old],
    )
    .await;

    let to_new = link(
        &app,
        serde_json::json!({"type": "email_change_new", "email": old, "new_email": new}),
    )
    .await;
    assert_eq!(to_new.status, StatusCode::OK, "{}", to_new.body);
    // The code the new address is asked for is hashed against the new
    // address, because that is where it gets typed in, while the
    // hashed_token in the answer is upstream's over the address the
    // request named. They differ here, and both are upstream's.
    assert_eq!(
        to_new.carried("token"),
        token_hash(&new, &to_new.text("email_otp"))
    );
    assert_eq!(
        to_new.text("hashed_token"),
        token_hash(&old, &to_new.text("email_otp"))
    );
    assert_eq!(to_new.carried("type"), "email_change");
    // The address in flight is written down, and the code went into the
    // column for the half this link is, not the other one.
    let (staged, current, fresh): (String, String, String) = {
        let sess = pool.unscoped().await.expect("connect");
        let rows = sess
            .query(
                "select email_change, email_change_token_current, email_change_token_new
                   from auth.users where id = $1::text::uuid",
                &[&id],
            )
            .await
            .expect("read the staged change");
        let row = &rows[0];
        sess.commit().await.expect("park");
        (row.get(0), row.get(1), row.get(2))
    };
    assert_eq!(staged, new);
    assert_eq!(fresh, to_new.carried("token"));
    assert_eq!(current, "", "the other half was written as well");

    let to_old = link(
        &app,
        serde_json::json!({"type": "email_change_current", "email": old, "new_email": new}),
    )
    .await;
    assert_eq!(to_old.status, StatusCode::OK, "{}", to_old.body);
    assert_eq!(
        to_old.carried("token"),
        token_hash(&old, &to_old.text("email_otp"))
    );

    // One link is half a change: the address does not move until both
    // mailboxes have been read.
    landed(&app, &to_new.text("action_link")).await;
    let moved: String = scalar(
        &pool,
        "select email from auth.users where id = $1::text::uuid",
        &[&id],
    )
    .await;
    assert_eq!(moved, old, "the address moved on one link alone");

    landed(&app, &to_old.text("action_link")).await;
    let moved: String = scalar(
        &pool,
        "select email from auth.users where id = $1::text::uuid",
        &[&id],
    )
    .await;
    assert_eq!(moved, new, "the address did not move on the second link");
}

#[tokio::test]
async fn a_link_to_the_old_address_needs_the_secure_change_turned_on() {
    let Some(dsn) = dsn() else { return };
    let app = router(Config {
        secure_email_change: false,
        ..base(&dsn)
    })
    .expect("router builds");
    let pool = pool(&dsn).await;
    let (old, new) = (address("insecure"), address("insecure-new"));
    wipe(&pool, &old).await;
    wipe(&pool, &new).await;
    assert_eq!(
        signs_up(&app, &old, "correct horse battery staple")
            .await
            .status,
        StatusCode::OK
    );

    let refused = link(
        &app,
        serde_json::json!({"type": "email_change_current", "email": old, "new_email": new}),
    )
    .await;
    assert_eq!(
        refused.refusal(),
        (
            400,
            "validation_failed",
            "Enable secure email change to generate link for current email"
        )
    );
    // The other half is still generated, because a project that does not
    // confirm the old address only ever needed the one link.
    let made = link(
        &app,
        serde_json::json!({"type": "email_change_new", "email": old, "new_email": new}),
    )
    .await;
    assert_eq!(made.status, StatusCode::OK, "{}", made.body);
}

#[tokio::test]
async fn generating_a_link_refuses_what_upstream_refuses() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = pool(&dsn).await;
    let email = address("refusals");
    let other = address("refusals-taken");
    wipe(&pool, &email).await;
    wipe(&pool, &other).await;
    assert_eq!(
        signs_up(&app, &email, "correct horse battery staple")
            .await
            .status,
        StatusCode::OK
    );
    assert_eq!(
        signs_up(&app, &other, "correct horse battery staple")
            .await
            .status,
        StatusCode::OK
    );

    // A type nobody serves, named back.
    assert_eq!(
        link(
            &app,
            serde_json::json!({"type": "teleport", "email": email})
        )
        .await
        .refusal(),
        (
            400,
            "validation_failed",
            "Invalid email action link type requested: teleport"
        )
    );
    // An address that is not one.
    assert_eq!(
        link(
            &app,
            serde_json::json!({"type": "magiclink", "email": "not-an-address"})
        )
        .await
        .refusal(),
        (
            400,
            "validation_failed",
            "Unable to validate email address: invalid format"
        )
    );
    // Moving to an address somebody else already holds.
    assert_eq!(
        link(
            &app,
            serde_json::json!({"type": "email_change_new", "email": email, "new_email": other})
        )
        .await
        .refusal(),
        (
            422,
            "email_exists",
            "A user with this email address has already been registered"
        )
    );
    // A signup link for a stranger with no password, which upstream
    // validates as a signup because that is what it is about to do.
    let stranger = address("refusals-stranger");
    wipe(&pool, &stranger).await;
    assert_eq!(
        link(
            &app,
            serde_json::json!({"type": "signup", "email": stranger})
        )
        .await
        .refusal(),
        (400, "validation_failed", "Signup requires a valid password")
    );
    let made: i64 = scalar(
        &pool,
        "select count(*) from auth.users where email = $1",
        &[&stranger],
    )
    .await;
    assert_eq!(made, 0, "a refused signup link left an account behind");
}

#[tokio::test]
async fn a_link_for_an_address_that_is_already_taken_is_refused() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = pool(&dsn).await;
    let email = address("taken");
    wipe(&pool, &email).await;
    assert_eq!(
        signs_up(&app, &email, "correct horse battery staple")
            .await
            .status,
        StatusCode::OK
    );
    let id: String = scalar(
        &pool,
        "update auth.users set email_confirmed_at = now()
          where email = $1 returning id::text",
        &[&email],
    )
    .await;

    for kind in ["signup", "invite"] {
        assert_eq!(
            link(
                &app,
                serde_json::json!({
                    "type": kind,
                    "email": email,
                    "password": "correct horse battery staple",
                })
            )
            .await
            .refusal(),
            (
                422,
                "email_exists",
                "A user with this email address has already been registered"
            ),
            "type {kind}"
        );
    }
    // And the account it refused to invite was left exactly as it was.
    let invited: bool = scalar(
        &pool,
        "select invited_at is not null from auth.users where id = $1::text::uuid",
        &[&id],
    )
    .await;
    assert!(!invited, "a refused invitation marked the account anyway");
}

#[tokio::test]
async fn a_generated_link_lands_where_the_request_asks_when_it_is_ours() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = pool(&dsn).await;
    let email = address("landing");
    wipe(&pool, &email).await;
    assert_eq!(
        signs_up(&app, &email, "correct horse battery staple")
            .await
            .status,
        StatusCode::OK
    );

    let ours = link(
        &app,
        serde_json::json!({
            "type": "magiclink",
            "email": email,
            "redirect_to": format!("{SITE}/welcome"),
        }),
    )
    .await;
    assert_eq!(ours.text("redirect_to"), format!("{SITE}/welcome"));
    assert_eq!(ours.carried("redirect_to"), format!("{SITE}/welcome"));
    let landing = landed(&app, &ours.text("action_link")).await;
    assert!(
        landing.starts_with(&format!("{SITE}/welcome#")),
        "{landing}"
    );

    // Somewhere else entirely is dropped rather than refused, because an
    // open redirect here is a phishing hop out of a project's own mail.
    let theirs = link(
        &app,
        serde_json::json!({
            "type": "magiclink",
            "email": email,
            "redirect_to": "https://phish.example.com/take",
        }),
    )
    .await;
    assert_eq!(theirs.text("redirect_to"), SITE);
    assert_eq!(theirs.carried("redirect_to"), SITE);
}

#[tokio::test]
async fn generating_a_link_sends_nothing() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = pool(&dsn).await;
    let email = address("quiet");
    wipe(&pool, &email).await;

    let made = link(&app, serde_json::json!({"type": "invite", "email": email})).await;
    assert_eq!(made.status, StatusCode::OK, "{}", made.body);
    // The whole point of this endpoint is that the project sends its
    // own mail, so nothing may go out from here.
    assert!(
        inbox(&app).await.is_empty(),
        "generate_link posted the mail as well as handing back the link"
    );
}

// invite.

#[tokio::test]
async fn an_invitation_is_mail_with_a_link_that_starts_a_session() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = pool(&dsn).await;
    let email = address("invitation");
    wipe(&pool, &email).await;

    let made = invite(
        &app,
        serde_json::json!({"email": email, "data": {"full_name": "Ada"}}),
    )
    .await;
    assert_eq!(made.status, StatusCode::OK, "{}", made.body);
    assert_eq!(made.body["email"].as_str(), Some(email.as_str()));
    assert_eq!(made.body["user_metadata"]["full_name"], "Ada");
    assert!(
        made.body["invited_at"].is_string(),
        "the answer does not say the account was invited: {}",
        made.body
    );
    // It is the account's own identity, not a session: an invitation is
    // not a way in until somebody follows it.
    assert_eq!(
        made.body["identities"]
            .as_array()
            .expect("identities")
            .len(),
        1
    );

    let kept = inbox(&app).await;
    assert_eq!(kept.len(), 1, "expected exactly one invitation: {kept:?}");
    let message = &kept[0];
    assert_eq!(message["to"].as_str(), Some(email.as_str()));
    assert_eq!(message["subject"].as_str(), Some("You've been invited"));
    let posted = message["link"].as_str().expect("a link in the mail");
    assert!(posted.contains("type=invite"), "{posted}");

    let landing = landed(&app, posted).await;
    assert!(landing.contains("access_token="), "{landing}");
    let confirmed: bool = scalar(
        &pool,
        "select email_confirmed_at is not null from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert!(confirmed, "following the invitation did not confirm it");
}

#[tokio::test]
async fn inviting_an_address_somebody_already_proved_is_refused() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = pool(&dsn).await;
    let email = address("already");
    wipe(&pool, &email).await;
    assert_eq!(
        signs_up(&app, &email, "correct horse battery staple")
            .await
            .status,
        StatusCode::OK
    );
    scalar::<i64>(
        &pool,
        "update auth.users set email_confirmed_at = now() where email = $1 returning 1::bigint",
        &[&email],
    )
    .await;

    let refused = invite(&app, serde_json::json!({"email": email})).await;
    assert_eq!(
        refused.refusal(),
        (
            422,
            "email_exists",
            "A user with this email address has already been registered"
        )
    );
    // The signup's own confirmation is in there, and nothing else: a
    // refused invitation is not posted.
    assert_eq!(
        invitations(&app).await,
        0,
        "a refused invitation was posted anyway"
    );
}

#[tokio::test]
async fn inviting_an_account_that_never_confirmed_replaces_its_code() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let pool = pool(&dsn).await;
    let email = address("unconfirmed");
    wipe(&pool, &email).await;
    let signed_up = signs_up(&app, &email, "correct horse battery staple").await;
    assert_eq!(signed_up.status, StatusCode::OK, "{}", signed_up.body);
    let was: String = scalar(
        &pool,
        "select confirmation_token from auth.users where email = $1",
        &[&email],
    )
    .await;
    let id = signed_up.body["id"].as_str().expect("an id").to_string();

    let made = invite(&app, serde_json::json!({"email": email})).await;
    assert_eq!(made.status, StatusCode::OK, "{}", made.body);
    // The same account, invited rather than a second one on the address.
    assert_eq!(made.id(), id);
    let now: String = scalar(
        &pool,
        "select confirmation_token from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert_ne!(now, was, "the invitation reused the signup's code");
    // And the code the signup mailed is not worth anything any more,
    // which is the rule everywhere: one live code of a kind per account.
    let stale: i64 = scalar(
        &pool,
        "select count(*) from auth.one_time_tokens
          where user_id = $1::text::uuid and token_hash = $2",
        &[&id, &was],
    )
    .await;
    assert_eq!(stale, 0, "the code the signup mailed still works");
}

#[tokio::test]
async fn an_invitation_needs_an_address_that_is_one() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let refused = invite(&app, serde_json::json!({"email": "not-an-address"})).await;
    assert_eq!(
        refused.refusal(),
        (
            400,
            "validation_failed",
            "Unable to validate email address: invalid format"
        )
    );
    assert!(inbox(&app).await.is_empty(), "something was posted anyway");
}
