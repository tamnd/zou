//! Sign up, sign in, verify, resend and change a phone number, with the
//! text messages actually going somewhere, against a live postgres.
//!
//! Nothing here plants a code by hand. Every test signs up or asks for
//! an otp, reads the message out of the dev sink the way a person reads
//! it in `zou inbox`, and types the six digits back in, which is the
//! only way to find out whether the hash written to the row is the hash
//! of the code that went out.
//!
//! Two things about this surface are easy to get backwards and are
//! pinned here for that reason. A phone code is never verified through
//! a bare token_hash, because there is no link to follow and upstream
//! calls that an email verification type. And a phone change is found
//! by the number being moved to rather than the one held, because the
//! account keeps the old number until the code is spent.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test auth_phone

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;
use zou_server::sql::Pool;
use zou_server::{Config, jwt, router, sms};

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

/// A project that texts its codes, with nothing configured to carry
/// them, which is what gets the dev sink.
///
/// The send frequency limit is off. It is a real part of these flows
/// and it has a test of its own below, but with it on every test that
/// asks one account for two codes in a second would be testing the
/// clock instead of the flow.
fn base(dsn: &str) -> Config {
    Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        external_url: Some("https://zou.test".to_string()),
        site_url: Some("https://app.zou.test".to_string()),
        phone_enabled: true,
        sms: sms::Settings {
            max_frequency: 0,
            ..sms::Settings::default()
        },
        ..Config::default()
    }
}

fn app(dsn: &str) -> axum::Router {
    router(base(dsn)).expect("router builds")
}

/// The same front door with phone sign in switched off, which is the
/// default and the thing most of these endpoints refuse on.
fn app_without_phone(dsn: &str) -> axum::Router {
    router(Config {
        phone_enabled: false,
        ..base(dsn)
    })
    .expect("router builds")
}

/// A project that confirms its own phone signups, which is how a test
/// gets a session on a number without reading anything.
fn app_autoconfirm(dsn: &str) -> axum::Router {
    router(Config {
        sms: sms::Settings {
            autoconfirm: true,
            max_frequency: 0,
            ..sms::Settings::default()
        },
        ..base(dsn)
    })
    .expect("router builds")
}

/// A sender that is not the dev sink, so the endpoints that hand back a
/// provider's message id have one to hand back.
struct Recorder {
    kept: Mutex<Vec<sms::Text>>,
}

impl sms::Sender for Recorder {
    fn deliver(&self, text: &sms::Text) -> Result<String, String> {
        self.kept.lock().expect("not poisoned").push(text.clone());
        Ok("SM0123456789".to_string())
    }
    fn describe(&self) -> String {
        "a test recorder".to_string()
    }
}

impl Recorder {
    fn new() -> Arc<Recorder> {
        Arc::new(Recorder {
            kept: Mutex::new(Vec::new()),
        })
    }
    fn kept(&self) -> Vec<sms::Text> {
        self.kept.lock().expect("not poisoned").clone()
    }
}

fn app_with(dsn: &str, texter: Arc<dyn sms::Sender>) -> axum::Router {
    router(Config {
        texter: Some(texter),
        ..base(dsn)
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
}

impl Answer {
    fn refusal(&self) -> (u16, &str, &str) {
        (
            self.status.as_u16(),
            self.body["error_code"].as_str().unwrap_or("<none>"),
            self.body["msg"].as_str().unwrap_or("<none>"),
        )
    }
    fn token(&self) -> String {
        self.body["access_token"]
            .as_str()
            .unwrap_or_else(|| panic!("no session in: {}", self.body))
            .to_string()
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

/// One text as the dev inbox hands it over.
struct Text(serde_json::Value);

impl Text {
    fn to(&self) -> &str {
        self.0["to"].as_str().expect("a text has a recipient")
    }
    fn body(&self) -> &str {
        self.0["body"].as_str().expect("a text has a body")
    }
    fn code(&self) -> String {
        self.0["code"]
            .as_str()
            .expect("a text carries its code")
            .to_string()
    }
    fn channel(&self) -> &str {
        self.0["channel"].as_str().expect("a text has a channel")
    }
}

/// Every text the dev sink is holding, which only the service role may
/// ask for.
async fn texts(app: &axum::Router) -> Vec<Text> {
    let req = Request::builder()
        .method("GET")
        .uri("/dev/inbox")
        .header("apikey", service_key())
        .body(Body::empty())
        .unwrap();
    let answer = answer(app.clone().oneshot(req).await.expect("router answers")).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    answer.body["texts"]
        .as_array()
        .expect("an array of texts")
        .iter()
        .map(|t| Text(t.clone()))
        .collect()
}

/// The one text that went out, which is the usual case.
async fn only_text(app: &axum::Router) -> Text {
    let mut kept = texts(app).await;
    assert_eq!(kept.len(), 1, "expected exactly one text");
    kept.pop().expect("one text")
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

/// A number nobody else in this suite uses. The tag keeps two tests
/// running at once from signing each other's account up.
fn number(tag: u64) -> String {
    format!("1555{tag:07}")
}

async fn wipe(pool: &Pool, phone: &str) {
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        "delete from auth.users where phone = $1 or phone_change = $1",
        &[&phone],
    )
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
    sess.commit().await.expect("park");
}

async fn pool(dsn: &str) -> Pool {
    Pool::new(dsn, 4).expect("dsn parses")
}

/// A confirmed account on a number, with its password, which is what
/// the sign in and change tests start from.
///
/// The signup goes through a project that confirms its own, because
/// getting there any other way is the subject of another test. The
/// account is left behind in the database, so the test carries on
/// against whatever front door it actually wants to try.
async fn confirmed(dsn: &str, phone: &str, password: &str) {
    let app = app_autoconfirm(dsn);
    let up = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"phone": phone, "password": password}),
    )
    .await;
    assert_eq!(up.status, StatusCode::OK, "{}", up.body);
}

#[tokio::test]
async fn a_phone_signup_texts_a_code_and_the_code_confirms_the_number() {
    let Some(dsn) = dsn() else { return };
    let (app, pool) = (app(&dsn), pool(&dsn).await);
    let phone = number(1);
    wipe(&pool, &phone).await;

    let up = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"phone": &phone, "password": "correct horse"}),
    )
    .await;
    assert_eq!(up.status, StatusCode::OK, "{}", up.body);
    assert_eq!(up.body["phone"], serde_json::json!(phone));
    assert!(
        up.body["access_token"].is_null(),
        "a number that has not answered yet is not a session: {}",
        up.body
    );

    let text = only_text(&app).await;
    assert_eq!(text.to(), phone);
    assert_eq!(text.channel(), "sms");
    assert_eq!(text.body(), format!("Your code is {}", text.code()));
    assert_eq!(text.code().len(), 6, "{}", text.body());
    assert!(text.code().chars().all(|c| c.is_ascii_digit()));

    // Nothing about the account is settled until the code comes back.
    let confirmed: bool = scalar(
        &pool,
        "select phone_confirmed_at is not null from auth.users where phone = $1",
        &[&phone],
    )
    .await;
    assert!(!confirmed, "the signup itself confirms nothing");
    let verified: bool = scalar(
        &pool,
        "select (i.identity_data->>'phone_verified')::bool
           from auth.identities i join auth.users u on u.id = i.user_id
          where u.phone = $1 and i.provider = 'phone'",
        &[&phone],
    )
    .await;
    assert!(
        !verified,
        "and the identity has not asserted the number either"
    );

    let done = post(
        &app,
        "/auth/v1/verify",
        serde_json::json!({"type": "sms", "phone": &phone, "token": text.code()}),
    )
    .await;
    assert_eq!(done.status, StatusCode::OK, "{}", done.body);
    assert!(!done.token().is_empty());
    assert_eq!(done.body["user"]["phone"], serde_json::json!(phone));

    let confirmed: bool = scalar(
        &pool,
        "select phone_confirmed_at is not null from auth.users where phone = $1",
        &[&phone],
    )
    .await;
    assert!(confirmed, "the code that went out proves the number");
    let spent: String = scalar(
        &pool,
        "select confirmation_token from auth.users where phone = $1",
        &[&phone],
    )
    .await;
    assert_eq!(spent, "", "a spent code is not a code any more");
    let verified: bool = scalar(
        &pool,
        "select (i.identity_data->>'phone_verified')::bool
           from auth.identities i join auth.users u on u.id = i.user_id
          where u.phone = $1 and i.provider = 'phone'",
        &[&phone],
    )
    .await;
    assert!(verified, "the identity says what the provider asserted");
}

#[tokio::test]
async fn the_same_code_does_not_work_twice_and_a_stale_one_never_did() {
    let Some(dsn) = dsn() else { return };
    let (app, pool) = (app(&dsn), pool(&dsn).await);
    let phone = number(2);
    wipe(&pool, &phone).await;

    post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"phone": &phone, "password": "correct horse"}),
    )
    .await;
    let code = only_text(&app).await.code();
    let verify = serde_json::json!({"type": "sms", "phone": &phone, "token": &code});
    assert_eq!(
        post(&app, "/auth/v1/verify", verify.clone()).await.status,
        StatusCode::OK
    );
    assert_eq!(
        post(&app, "/auth/v1/verify", verify).await.refusal(),
        (403, "otp_expired", "Token has expired or is invalid")
    );

    // A code that went out longer ago than the sms window lives is
    // refused the same way, and the window is the sms one: a minute by
    // default, not the day a mailed code gets.
    empty_inbox(&app).await;
    let again = post(&app, "/auth/v1/otp", serde_json::json!({"phone": &phone})).await;
    assert_eq!(again.status, StatusCode::OK, "{}", again.body);
    let code = only_text(&app).await.code();
    run(
        &pool,
        "update auth.users set confirmation_sent_at = now() - interval '2 minutes'
          where phone = $1",
        &[&phone],
    )
    .await;
    assert_eq!(
        post(
            &app,
            "/auth/v1/verify",
            serde_json::json!({"type": "sms", "phone": &phone, "token": code}),
        )
        .await
        .refusal(),
        (403, "otp_expired", "Token has expired or is invalid")
    );
}

#[tokio::test]
async fn a_code_texted_to_one_number_does_not_verify_another() {
    let Some(dsn) = dsn() else { return };
    let (app, pool) = (app(&dsn), pool(&dsn).await);
    let (mine, yours) = (number(3), number(4));
    wipe(&pool, &mine).await;
    wipe(&pool, &yours).await;

    for phone in [&mine, &yours] {
        post(
            &app,
            "/auth/v1/signup",
            serde_json::json!({"phone": phone, "password": "correct horse"}),
        )
        .await;
    }
    let kept = texts(&app).await;
    assert_eq!(kept.len(), 2);
    let to_mine = kept.iter().find(|t| t.to() == mine).expect("one each");

    // The code is hashed against the number it went to, so presenting
    // it for somebody else's account is not a near miss, it is nothing.
    assert_eq!(
        post(
            &app,
            "/auth/v1/verify",
            serde_json::json!({"type": "sms", "phone": &yours, "token": to_mine.code()}),
        )
        .await
        .refusal(),
        (403, "otp_expired", "Token has expired or is invalid")
    );
}

#[tokio::test]
async fn a_texted_code_has_no_link_and_so_no_bare_hash_verifies_it() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);

    // Upstream's verifyTokenHash is the email path and says so in its
    // own words, whichever of the two phone types is asked for.
    for kind in ["sms", "phone_change"] {
        assert_eq!(
            post(
                &app,
                "/auth/v1/verify",
                serde_json::json!({"type": kind, "token_hash": "0".repeat(56)}),
            )
            .await
            .refusal(),
            (400, "validation_failed", "Invalid email verification type"),
            "{kind}"
        );
    }

    // A number cannot ride along with a token_hash either, because a
    // hash is the whole of what that request is.
    assert_eq!(
        post(
            &app,
            "/auth/v1/verify",
            serde_json::json!({"type": "sms", "token_hash": "0".repeat(56), "phone": number(5)}),
        )
        .await
        .refusal(),
        (
            400,
            "validation_failed",
            "Only the token_hash and type should be provided"
        )
    );

    // And an address next to a number says nothing about which one the
    // code was hashed against, so it is refused rather than guessed at.
    assert_eq!(
        post(
            &app,
            "/auth/v1/verify",
            serde_json::json!({
                "type": "sms",
                "token": "123456",
                "phone": number(5),
                "email": "someone@zou.test",
            }),
        )
        .await
        .refusal(),
        (
            400,
            "validation_failed",
            "Only an email address or phone number should be provided on verify"
        )
    );
}

#[tokio::test]
async fn a_type_no_phone_flow_ever_wrote_a_code_for_verifies_nothing() {
    let Some(dsn) = dsn() else { return };
    let (app, pool) = (app(&dsn), pool(&dsn).await);
    let phone = number(6);
    wipe(&pool, &phone).await;

    post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"phone": &phone, "password": "correct horse"}),
    )
    .await;
    let code = only_text(&app).await.code();

    // The code is real and the number is real. The type is not one that
    // a phone flow ever wrote, so there is nothing here to match it
    // against and the answer is the same vague refusal as a wrong code.
    assert_eq!(
        post(
            &app,
            "/auth/v1/verify",
            serde_json::json!({"type": "recovery", "phone": &phone, "token": code}),
        )
        .await
        .refusal(),
        (403, "otp_expired", "Token has expired or is invalid")
    );
}

#[tokio::test]
async fn the_otp_endpoint_signs_up_a_number_nobody_has_and_texts_it() {
    let Some(dsn) = dsn() else { return };
    let (app, pool) = (app(&dsn), pool(&dsn).await);
    let phone = number(7);
    wipe(&pool, &phone).await;

    // Nothing here has a password, which is the whole point: the
    // account is created with one nobody will ever hold.
    let asked = post(&app, "/auth/v1/otp", serde_json::json!({"phone": &phone})).await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);
    assert_eq!(asked.body, serde_json::json!({}), "the answer says nothing");

    let text = only_text(&app).await;
    assert_eq!(text.to(), phone);
    let done = post(
        &app,
        "/auth/v1/verify",
        serde_json::json!({"type": "sms", "phone": &phone, "token": text.code()}),
    )
    .await;
    assert_eq!(done.status, StatusCode::OK, "{}", done.body);
    assert!(!done.token().is_empty());

    // The second time round the account is confirmed, so this is a sign
    // in rather than a signup, and it still ends in a session.
    empty_inbox(&app).await;
    let asked = post(&app, "/auth/v1/otp", serde_json::json!({"phone": &phone})).await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);
    let text = only_text(&app).await;
    let done = post(
        &app,
        "/auth/v1/verify",
        serde_json::json!({"type": "sms", "phone": &phone, "token": text.code()}),
    )
    .await;
    assert_eq!(done.status, StatusCode::OK, "{}", done.body);
    let sessions: i64 = scalar(
        &pool,
        "select count(*) from auth.sessions s join auth.users u on u.id = s.user_id
          where u.phone = $1",
        &[&phone],
    )
    .await;
    assert_eq!(sessions, 2, "one session for each time the code came back");
}

#[tokio::test]
async fn the_otp_endpoint_hands_back_the_providers_message_id_when_there_is_one() {
    let Some(dsn) = dsn() else { return };
    let recorder = Recorder::new();
    let (app, pool) = (app_with(&dsn, recorder.clone()), pool(&dsn).await);
    let phone = number(8);
    wipe(&pool, &phone).await;

    // A signup sends the code itself, so the otp endpoint has nothing
    // left to send and nothing to say about it.
    assert_eq!(
        post(&app, "/auth/v1/otp", serde_json::json!({"phone": &phone}))
            .await
            .body,
        serde_json::json!({})
    );
    assert_eq!(recorder.kept().len(), 1);

    let code = recorder.kept()[0].code.clone();
    post(
        &app,
        "/auth/v1/verify",
        serde_json::json!({"type": "sms", "phone": &phone, "token": code}),
    )
    .await;

    // Now the number is confirmed, so the endpoint sends the code and
    // the provider's own id for the message comes back with it.
    let asked = post(&app, "/auth/v1/otp", serde_json::json!({"phone": &phone})).await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);
    assert_eq!(
        asked.body,
        serde_json::json!({"message_id": "SM0123456789"})
    );
    assert_eq!(recorder.kept().len(), 2);
}

#[tokio::test]
async fn otp_without_create_user_only_texts_numbers_that_already_answered() {
    let Some(dsn) = dsn() else { return };
    let (app, pool) = (app(&dsn), pool(&dsn).await);
    let phone = number(9);
    wipe(&pool, &phone).await;

    assert_eq!(
        post(
            &app,
            "/auth/v1/otp",
            serde_json::json!({"phone": &phone, "create_user": false}),
        )
        .await
        .refusal(),
        (422, "otp_disabled", "Signups not allowed for otp")
    );
    assert!(texts(&app).await.is_empty(), "nothing went out");

    // The same request against a number with an account goes through,
    // and an account halfway through a signup counts as one.
    post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"phone": &phone, "password": "correct horse"}),
    )
    .await;
    empty_inbox(&app).await;
    let asked = post(
        &app,
        "/auth/v1/otp",
        serde_json::json!({"phone": &phone, "create_user": false}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);
    assert_eq!(only_text(&app).await.to(), phone);
}

#[tokio::test]
async fn the_number_is_judged_before_anything_is_looked_up() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);

    for bad in ["not a number", "+0123456789", "1", "12345678901234567890"] {
        assert_eq!(
            post(&app, "/auth/v1/otp", serde_json::json!({"phone": bad}))
                .await
                .refusal(),
            (
                400,
                "validation_failed",
                "Invalid phone number format (E.164 required)"
            ),
            "{bad}"
        );
    }

    // A channel nobody carries is refused before the number is even
    // read, and the refusal names the provider rather than the channel
    // because that is what a person can do something about.
    let refused = post(
        &app,
        "/auth/v1/otp",
        serde_json::json!({"phone": number(10), "channel": "carrier pigeon"}),
    )
    .await;
    assert_eq!(refused.status, StatusCode::BAD_REQUEST);
    assert_eq!(refused.body["error_code"], "validation_failed");
    assert!(
        refused.body["msg"]
            .as_str()
            .unwrap_or_default()
            .starts_with("Invalid channel, supported values are 'sms' or 'whatsapp'"),
        "{}",
        refused.body
    );

    // WhatsApp is Twilio's alone, and the dev sink is not Twilio.
    assert_eq!(
        post(
            &app,
            "/auth/v1/otp",
            serde_json::json!({"phone": number(10), "channel": "whatsapp"}),
        )
        .await
        .status,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn a_project_with_phone_off_refuses_every_way_in_by_number() {
    let Some(dsn) = dsn() else { return };
    let (app, pool) = (app_without_phone(&dsn), pool(&dsn).await);
    let phone = number(11);
    wipe(&pool, &phone).await;

    // Each of these is upstream's own wording for that endpoint, and
    // they are deliberately not the same sentence.
    assert_eq!(
        post(
            &app,
            "/auth/v1/signup",
            serde_json::json!({"phone": &phone, "password": "correct horse"}),
        )
        .await
        .refusal(),
        (400, "phone_provider_disabled", "Phone signups are disabled")
    );
    assert_eq!(
        post(&app, "/auth/v1/otp", serde_json::json!({"phone": &phone}))
            .await
            .refusal(),
        (400, "phone_provider_disabled", "Unsupported phone provider")
    );
    assert_eq!(
        post(
            &app,
            "/auth/v1/resend",
            serde_json::json!({"type": "sms", "phone": &phone}),
        )
        .await
        .refusal(),
        (400, "phone_provider_disabled", "Phone logins are disabled")
    );
    assert_eq!(
        post(
            &app,
            "/auth/v1/token?grant_type=password",
            serde_json::json!({"phone": &phone, "password": "correct horse"}),
        )
        .await
        .refusal(),
        (422, "phone_provider_disabled", "Phone logins are disabled")
    );

    let count: i64 = scalar(
        &pool,
        "select count(*) from auth.users where phone = $1",
        &[&phone],
    )
    .await;
    assert_eq!(count, 0, "a refused signup writes nothing");

    // The channel is judged first, which is upstream's order: a request
    // naming a channel nobody carries is malformed whether or not phone
    // signups are on at all.
    let refused = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"phone": &phone, "password": "correct horse", "channel": "smoke"}),
    )
    .await;
    assert_eq!(refused.status, StatusCode::BAD_REQUEST);
    assert_eq!(refused.body["error_code"], "validation_failed");
}

#[tokio::test]
async fn resend_texts_the_code_again_and_says_nothing_about_who_has_an_account() {
    let Some(dsn) = dsn() else { return };
    let (app, pool) = (app(&dsn), pool(&dsn).await);
    let phone = number(12);
    wipe(&pool, &phone).await;

    post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"phone": &phone, "password": "correct horse"}),
    )
    .await;
    let first = only_text(&app).await.code();
    empty_inbox(&app).await;

    let again = post(
        &app,
        "/auth/v1/resend",
        serde_json::json!({"type": "sms", "phone": &phone}),
    )
    .await;
    assert_eq!(again.status, StatusCode::OK, "{}", again.body);
    let second = only_text(&app).await;
    assert_eq!(second.to(), phone);
    assert_eq!(second.channel(), "sms", "resend has no channel of its own");

    // The fresh code is the one that works, and it is a fresh one.
    assert_ne!(second.code(), first);
    assert_eq!(
        post(
            &app,
            "/auth/v1/verify",
            serde_json::json!({"type": "sms", "phone": &phone, "token": first}),
        )
        .await
        .refusal(),
        (403, "otp_expired", "Token has expired or is invalid")
    );
    assert_eq!(
        post(
            &app,
            "/auth/v1/verify",
            serde_json::json!({"type": "sms", "phone": &phone, "token": second.code()}),
        )
        .await
        .status,
        StatusCode::OK
    );

    // Now the number is confirmed, so there is nothing to resend, and a
    // number nobody has is answered exactly the same way. Neither
    // answer says which of the two happened.
    empty_inbox(&app).await;
    for asked in [&phone, &number(13)] {
        let quiet = post(
            &app,
            "/auth/v1/resend",
            serde_json::json!({"type": "sms", "phone": asked}),
        )
        .await;
        assert_eq!(quiet.status, StatusCode::OK, "{}", quiet.body);
        assert_eq!(quiet.body, serde_json::json!({}));
    }
    assert!(texts(&app).await.is_empty(), "nothing went out");

    // And the type has to say what it is resending.
    assert_eq!(
        post(&app, "/auth/v1/resend", serde_json::json!({"type": "sms"}))
            .await
            .refusal(),
        (
            400,
            "validation_failed",
            "Type provided requires a phone number"
        )
    );
}

#[tokio::test]
async fn one_account_may_only_be_texted_so_often() {
    let Some(dsn) = dsn() else { return };
    // The frequency limit is on here, at its default minute, because it
    // is the thing under test.
    let app = router(Config {
        sms: sms::Settings::default(),
        ..base(&dsn)
    })
    .expect("router builds");
    let pool = pool(&dsn).await;
    let phone = number(14);
    wipe(&pool, &phone).await;

    post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"phone": &phone, "password": "correct horse"}),
    )
    .await;
    assert_eq!(texts(&app).await.len(), 1);

    let too_soon = post(
        &app,
        "/auth/v1/resend",
        serde_json::json!({"type": "sms", "phone": &phone}),
    )
    .await;
    assert_eq!(too_soon.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(too_soon.body["error_code"], "over_sms_send_rate_limit");
    assert_eq!(texts(&app).await.len(), 1, "and nothing went out");

    // A minute later it goes out, which is what the limit means.
    run(
        &pool,
        "update auth.users set confirmation_sent_at = now() - interval '2 minutes'
          where phone = $1",
        &[&phone],
    )
    .await;
    let ok = post(
        &app,
        "/auth/v1/resend",
        serde_json::json!({"type": "sms", "phone": &phone}),
    )
    .await;
    assert_eq!(ok.status, StatusCode::OK, "{}", ok.body);
    assert_eq!(texts(&app).await.len(), 2);
}

#[tokio::test]
async fn changing_the_number_stages_it_and_the_code_moves_it() {
    let Some(dsn) = dsn() else { return };
    let (old, new) = (number(15), number(16));
    let pool = pool(&dsn).await;
    wipe(&pool, &old).await;
    wipe(&pool, &new).await;
    confirmed(&dsn, &old, "correct horse").await;
    let app = app(&dsn);
    let token = post(
        &app,
        "/auth/v1/token?grant_type=password",
        serde_json::json!({"phone": &old, "password": "correct horse"}),
    )
    .await
    .token();
    empty_inbox(&app).await;

    let asked = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"phone": &new}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);
    assert_eq!(
        asked.body["phone"],
        serde_json::json!(old),
        "the account keeps the old number until the code is spent"
    );
    assert_eq!(
        asked.body["new_phone"],
        serde_json::json!(new),
        "and says which one it is waiting on"
    );

    let text = only_text(&app).await;
    assert_eq!(text.to(), new, "the code goes to the number being proved");

    // The account is found by the number being moved to, not the one it
    // still holds, so this is the request a client actually sends.
    let done = post(
        &app,
        "/auth/v1/verify",
        serde_json::json!({"type": "phone_change", "phone": &new, "token": text.code()}),
    )
    .await;
    assert_eq!(done.status, StatusCode::OK, "{}", done.body);
    assert!(!done.token().is_empty());
    assert_eq!(done.body["user"]["phone"], serde_json::json!(new));

    let staged: String = scalar(
        &pool,
        "select phone_change from auth.users where phone = $1",
        &[&new],
    )
    .await;
    assert_eq!(staged, "", "nothing is left staged");
    let held: String = scalar(
        &pool,
        "select i.identity_data->>'phone' from auth.identities i
           join auth.users u on u.id = i.user_id
          where u.phone = $1 and i.provider = 'phone'",
        &[&new],
    )
    .await;
    assert_eq!(held, new, "the identity moved with the account");

    // And the old number is nobody's now.
    let gone: i64 = scalar(
        &pool,
        "select count(*) from auth.users where phone = $1",
        &[&old],
    )
    .await;
    assert_eq!(gone, 0);
}

#[tokio::test]
async fn a_number_somebody_else_holds_is_refused_before_anything_is_sent() {
    let Some(dsn) = dsn() else { return };
    let (mine, yours) = (number(17), number(18));
    let pool = pool(&dsn).await;
    wipe(&pool, &mine).await;
    wipe(&pool, &yours).await;
    confirmed(&dsn, &mine, "correct horse").await;
    let app = app(&dsn);
    // Somebody else already has the other number.
    confirmed(&dsn, &yours, "correct horse").await;
    let token = post(
        &app,
        "/auth/v1/token?grant_type=password",
        serde_json::json!({"phone": &mine, "password": "correct horse"}),
    )
    .await
    .token();
    empty_inbox(&app).await;

    assert_eq!(
        as_user(
            &app,
            "PUT",
            "/auth/v1/user",
            &token,
            serde_json::json!({"phone": &yours}),
        )
        .await
        .refusal(),
        (
            422,
            "phone_exists",
            "A user with this phone number has already been registered"
        )
    );
    assert!(texts(&app).await.is_empty(), "nothing went out");

    // Asking for the number the account already holds is not a change
    // and does not text anybody.
    let same = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"phone": &mine}),
    )
    .await;
    assert_eq!(same.status, StatusCode::OK, "{}", same.body);
    assert!(texts(&app).await.is_empty());
}

#[tokio::test]
async fn a_phone_change_is_resent_only_while_one_is_waiting() {
    let Some(dsn) = dsn() else { return };
    let (old, new) = (number(19), number(20));
    let pool = pool(&dsn).await;
    wipe(&pool, &old).await;
    wipe(&pool, &new).await;
    confirmed(&dsn, &old, "correct horse").await;
    let app = app(&dsn);
    let token = post(
        &app,
        "/auth/v1/token?grant_type=password",
        serde_json::json!({"phone": &old, "password": "correct horse"}),
    )
    .await
    .token();
    empty_inbox(&app).await;

    // Nothing is staged yet, so there is nothing to say and nothing to
    // send, and the answer is the same silent 200 either way.
    let quiet = post(
        &app,
        "/auth/v1/resend",
        serde_json::json!({"type": "phone_change", "phone": &old}),
    )
    .await;
    assert_eq!(quiet.status, StatusCode::OK, "{}", quiet.body);
    assert!(texts(&app).await.is_empty());

    as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"phone": &new}),
    )
    .await;
    empty_inbox(&app).await;

    // The resend is asked for by the number the account still holds,
    // and the code still goes to the one it is waiting on.
    let again = post(
        &app,
        "/auth/v1/resend",
        serde_json::json!({"type": "phone_change", "phone": &old}),
    )
    .await;
    assert_eq!(again.status, StatusCode::OK, "{}", again.body);
    let text = only_text(&app).await;
    assert_eq!(text.to(), new);
    assert_eq!(
        post(
            &app,
            "/auth/v1/verify",
            serde_json::json!({"type": "phone_change", "phone": &new, "token": text.code()}),
        )
        .await
        .status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn with_autoconfirm_the_number_is_taken_at_its_word() {
    let Some(dsn) = dsn() else { return };
    let (app, pool) = (app_autoconfirm(&dsn), pool(&dsn).await);
    let (phone, moved) = (number(21), number(22));
    wipe(&pool, &phone).await;
    wipe(&pool, &moved).await;

    let up = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"phone": &phone, "password": "correct horse"}),
    )
    .await;
    assert_eq!(up.status, StatusCode::OK, "{}", up.body);
    assert!(
        !up.token().is_empty(),
        "a project that asks for no proof hands back a session"
    );
    assert!(texts(&app).await.is_empty(), "and texts nobody");
    let confirmed: bool = scalar(
        &pool,
        "select phone_confirmed_at is not null from auth.users where phone = $1",
        &[&phone],
    )
    .await;
    assert!(confirmed);

    // A second signup on a confirmed number is a duplicate, and a
    // project that confirms its own signups says so rather than
    // pretending, because there is no code going out to be silent about.
    assert_eq!(
        post(
            &app,
            "/auth/v1/signup",
            serde_json::json!({"phone": &phone, "password": "correct horse"}),
        )
        .await
        .refusal(),
        (422, "user_already_exists", "User already registered")
    );

    // The change moves on the spot for the same reason.
    let token = up.token();
    let changed = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"phone": &moved}),
    )
    .await;
    assert_eq!(changed.status, StatusCode::OK, "{}", changed.body);
    assert_eq!(changed.body["phone"], serde_json::json!(moved));
    assert!(texts(&app).await.is_empty());
    let confirmed: bool = scalar(
        &pool,
        "select phone_confirmed_at is not null from auth.users where phone = $1",
        &[&moved],
    )
    .await;
    assert!(confirmed);
}

#[tokio::test]
async fn the_password_grant_takes_a_number_the_same_way_it_takes_an_address() {
    let Some(dsn) = dsn() else { return };
    let phone = number(23);
    let pool = pool(&dsn).await;
    wipe(&pool, &phone).await;
    confirmed(&dsn, &phone, "correct horse").await;
    let app = app(&dsn);

    let signed = post(
        &app,
        "/auth/v1/token?grant_type=password",
        serde_json::json!({"phone": &phone, "password": "correct horse"}),
    )
    .await;
    assert_eq!(signed.status, StatusCode::OK, "{}", signed.body);
    assert_eq!(signed.body["user"]["phone"], serde_json::json!(phone));

    // The number is only stripped, never judged, because a malformed one
    // is simply a number nobody holds and saying more would answer a
    // question the caller did not get to ask.
    for asked in [format!("+{phone}"), format!("+{phone} ")] {
        assert_eq!(
            post(
                &app,
                "/auth/v1/token?grant_type=password",
                serde_json::json!({"phone": asked, "password": "correct horse"}),
            )
            .await
            .status,
            StatusCode::OK,
            "{asked}"
        );
    }
    assert_eq!(
        post(
            &app,
            "/auth/v1/token?grant_type=password",
            serde_json::json!({"phone": "not a number", "password": "correct horse"}),
        )
        .await
        .refusal(),
        (400, "invalid_credentials", "Invalid login credentials")
    );
    assert_eq!(
        post(
            &app,
            "/auth/v1/token?grant_type=password",
            serde_json::json!({"phone": &phone, "password": "wrong horse"}),
        )
        .await
        .refusal(),
        (400, "invalid_credentials", "Invalid login credentials")
    );
    assert_eq!(
        post(
            &app,
            "/auth/v1/token?grant_type=password",
            serde_json::json!({"phone": &phone, "email": "someone@zou.test", "password": "x"}),
        )
        .await
        .refusal(),
        (
            400,
            "validation_failed",
            "Only an email address or phone number should be provided on login."
        )
    );
}

#[tokio::test]
async fn a_number_that_never_answered_cannot_sign_in_with_its_password() {
    let Some(dsn) = dsn() else { return };
    let (app, pool) = (app(&dsn), pool(&dsn).await);
    let phone = number(24);
    wipe(&pool, &phone).await;

    post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"phone": &phone, "password": "correct horse"}),
    )
    .await;
    // The password is right. The number has not been proved, which is a
    // different thing and gets said out loud, because the person can do
    // something about it.
    assert_eq!(
        post(
            &app,
            "/auth/v1/token?grant_type=password",
            serde_json::json!({"phone": &phone, "password": "correct horse"}),
        )
        .await
        .refusal(),
        (400, "phone_not_confirmed", "Phone not confirmed")
    );
}

#[tokio::test]
async fn reauthentication_texts_the_code_when_the_account_has_only_a_number() {
    let Some(dsn) = dsn() else { return };
    let phone = number(25);
    let pool = pool(&dsn).await;
    wipe(&pool, &phone).await;
    // Autoconfirm gets the account to a confirmed number without a
    // mailbox, and the reauthentication rule is what is under test.
    let app = router(Config {
        reauthentication_required: true,
        sms: sms::Settings {
            autoconfirm: true,
            max_frequency: 0,
            ..sms::Settings::default()
        },
        ..base(&dsn)
    })
    .expect("router builds");
    let token = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"phone": &phone, "password": "correct horse"}),
    )
    .await
    .token();

    // A session started in the last day needs no code, so the session
    // this test holds is aged out of that window first.
    run(
        &pool,
        "update auth.sessions set created_at = now() - interval '2 days',
                                  refreshed_at = now() - interval '2 days'
           from auth.users u where u.id = auth.sessions.user_id and u.phone = $1",
        &[&phone],
    )
    .await;
    assert_eq!(
        as_user(
            &app,
            "PUT",
            "/auth/v1/user",
            &token,
            serde_json::json!({"password": "a different horse"}),
        )
        .await
        .refusal(),
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
    let text = only_text(&app).await;
    assert_eq!(text.to(), phone);

    // A code hashed against somebody else's number is not this nonce.
    assert_eq!(
        as_user(
            &app,
            "PUT",
            "/auth/v1/user",
            &token,
            serde_json::json!({"password": "a different horse", "nonce": "000000"}),
        )
        .await
        .refusal(),
        (
            422,
            "reauthentication_not_valid",
            "Nonce has expired or is invalid"
        )
    );

    // A texted nonce lives as long as a texted code, which is a minute
    // and not the day a mailed one gets.
    run(
        &pool,
        "update auth.users set reauthentication_sent_at = now() - interval '2 minutes'
          where phone = $1",
        &[&phone],
    )
    .await;
    assert_eq!(
        as_user(
            &app,
            "PUT",
            "/auth/v1/user",
            &token,
            serde_json::json!({"password": "a different horse", "nonce": text.code()}),
        )
        .await
        .refusal(),
        (
            422,
            "reauthentication_not_valid",
            "Nonce has expired or is invalid"
        )
    );
    run(
        &pool,
        "update auth.users set reauthentication_sent_at = now() where phone = $1",
        &[&phone],
    )
    .await;

    let done = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"password": "a different horse", "nonce": text.code()}),
    )
    .await;
    assert_eq!(done.status, StatusCode::OK, "{}", done.body);
    assert_eq!(
        post(
            &app,
            "/auth/v1/token?grant_type=password",
            serde_json::json!({"phone": &phone, "password": "a different horse"}),
        )
        .await
        .status,
        StatusCode::OK,
        "the new password is the password now"
    );

    // And the nonce is spent.
    let left: String = scalar(
        &pool,
        "select reauthentication_token from auth.users where phone = $1",
        &[&phone],
    )
    .await;
    assert_eq!(left, "");
}

#[tokio::test]
async fn an_unproved_number_is_not_something_to_reauthenticate_against() {
    let Some(dsn) = dsn() else { return };
    let (app, pool) = (app(&dsn), pool(&dsn).await);
    let phone = number(26);
    wipe(&pool, &phone).await;

    // There is no session on an unconfirmed number, so this one is made
    // by hand: the account holds the number, nothing has proved it, and
    // the question is what reauthenticate says about that.
    post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"phone": &phone, "password": "correct horse"}),
    )
    .await;
    let user_id: String = scalar(
        &pool,
        "select id::text from auth.users where phone = $1",
        &[&phone],
    )
    .await;
    let token = jwt::mint(
        &serde_json::json!({
            "sub": user_id,
            "aud": "authenticated",
            "role": "authenticated",
        }),
        SECRET,
    );
    empty_inbox(&app).await;

    assert_eq!(
        as_user(
            &app,
            "POST",
            "/auth/v1/reauthenticate",
            &token,
            serde_json::json!({}),
        )
        .await
        .refusal(),
        (
            422,
            "phone_not_confirmed",
            "Please verify your phone first."
        )
    );
    assert!(texts(&app).await.is_empty());
}

#[tokio::test]
async fn an_account_with_both_is_asked_at_the_address() {
    let Some(dsn) = dsn() else { return };
    let phone = number(27);
    let email = "both@zou.test";
    let pool = pool(&dsn).await;
    wipe(&pool, &phone).await;
    {
        let sess = pool.unscoped().await.expect("connect");
        sess.execute("delete from auth.users where email = $1", &[&email])
            .await
            .expect("clear any leftover");
        sess.commit().await.expect("park");
    }
    // Both halves confirm themselves here, so the account ends up
    // holding a proved address and a proved number, which is the only
    // interesting case for the order.
    let app = router(Config {
        reauthentication_required: true,
        mailer_autoconfirm: true,
        sms: sms::Settings {
            autoconfirm: true,
            max_frequency: 0,
            ..sms::Settings::default()
        },
        ..base(&dsn)
    })
    .expect("router builds");

    let token = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"email": email, "password": "correct horse"}),
    )
    .await
    .token();
    let added = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"phone": &phone}),
    )
    .await;
    assert_eq!(added.status, StatusCode::OK, "{}", added.body);
    assert_eq!(added.body["phone"], serde_json::json!(phone));
    empty_inbox(&app).await;

    run(
        &pool,
        "update auth.sessions set created_at = now() - interval '2 days',
                                  refreshed_at = now() - interval '2 days'
           from auth.users u where u.id = auth.sessions.user_id and u.email = $1",
        &[&email],
    )
    .await;
    let asked = as_user(
        &app,
        "POST",
        "/auth/v1/reauthenticate",
        &token,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);

    // The address wins, which is upstream's order and not a preference.
    assert!(
        texts(&app).await.is_empty(),
        "nobody is texted when there is an address to write to"
    );
    let req = Request::builder()
        .method("GET")
        .uri("/dev/inbox")
        .header("apikey", service_key())
        .body(Body::empty())
        .unwrap();
    let inbox = answer(app.clone().oneshot(req).await.expect("router answers")).await;
    let messages = inbox.body["messages"].as_array().expect("an array");
    assert_eq!(messages.len(), 1, "{}", inbox.body);
    assert_eq!(messages[0]["to"], serde_json::json!(email));
}

#[tokio::test]
async fn an_anonymous_account_that_proves_a_number_stops_being_anonymous() {
    let Some(dsn) = dsn() else { return };
    let phone = number(28);
    let pool = pool(&dsn).await;
    wipe(&pool, &phone).await;
    let app = router(Config {
        anonymous_users: true,
        ..base(&dsn)
    })
    .expect("router builds");

    let token = post(&app, "/auth/v1/signup", serde_json::json!({}))
        .await
        .token();
    let asked = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"phone": &phone}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);
    let text = only_text(&app).await;
    assert_eq!(text.to(), phone);

    // The number goes back with its plus on, the way a person types it,
    // which is the same number as far as the hash is concerned.
    let done = post(
        &app,
        "/auth/v1/verify",
        serde_json::json!({
            "type": "phone_change",
            "phone": format!("+{phone}"),
            "token": text.code(),
        }),
    )
    .await;
    assert_eq!(done.status, StatusCode::OK, "{}", done.body);

    // Somebody answered on that number, so there is nothing anonymous
    // about the account any more.
    let anonymous: bool = scalar(
        &pool,
        "select is_anonymous from auth.users where phone = $1",
        &[&phone],
    )
    .await;
    assert!(!anonymous);
    let identity: String = scalar(
        &pool,
        "select i.provider from auth.identities i join auth.users u on u.id = i.user_id
          where u.phone = $1",
        &[&phone],
    )
    .await;
    assert_eq!(identity, "phone", "and it has the identity to show for it");
}
