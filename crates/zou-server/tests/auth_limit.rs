//! The per endpoint rate limits, wired up, against a live postgres.
//!
//! The arithmetic is unit tested next to the buckets. What is pinned
//! here is everything the arithmetic cannot say on its own: that the
//! limits are attached to the endpoints upstream attaches them to, that
//! a refused request never reaches the endpoint, that the budgets are
//! per caller and per endpoint, that a server with no way to tell
//! callers apart limits nobody, and that the two send budgets take the
//! flow they refuse down with them rather than leaving an account
//! holding a code that was never posted.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test auth_limit

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use zou_server::sql::Pool;
use zou_server::{Config, jwt, limit, mail, router, sms};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

/// The burst every endpoint but the anonymous one carries, whatever
/// number it is configured with. Upstream's, and the reason a test that
/// wants to see a refusal on /token has to ask thirty one times.
const BURST: usize = 30;

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

/// A project with the budgets `vars` asks for. They go through the same
/// reader the environment does, so a test that sets one is also a test
/// that the variable was understood.
fn app(dsn: &str, vars: &[(&str, &str)]) -> axum::Router {
    let owned: Vec<(String, String)> = vars
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    let limit = limit::configured(&|name| {
        owned
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    })
    .expect("these budgets parse");
    router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        external_url: Some("https://zou.test".to_string()),
        site_url: Some("https://app.zou.test".to_string()),
        anonymous_users: true,
        phone_enabled: true,
        // The per account send frequency is a different limit with a
        // different refusal, and leaving it on would answer some of
        // these tests before the budget under test got the chance.
        mail: mail::Settings {
            max_frequency: 0,
            ..mail::Settings::default()
        },
        sms: sms::Settings {
            max_frequency: 0,
            ..sms::Settings::default()
        },
        limit,
        ..Config::default()
    })
    .expect("router builds")
}

/// The header a caller is counted by in every test that counts them.
const BY_HEADER: (&str, &str) = ("ZOU_RATE_LIMIT_HEADER", "x-forwarded-for");

fn anon_key() -> String {
    jwt::mint(&jwt::key_claims("anon"), SECRET)
}

fn service_key() -> String {
    jwt::mint(&jwt::key_claims("service_role"), SECRET)
}

struct Answer {
    status: StatusCode,
    code: String,
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

    /// What a caller over its budget is told, in upstream's words.
    fn over_budget(&self) -> bool {
        self.refusal() == (429, "over_request_rate_limit", "Request rate limit reached")
    }
}

/// A request from `ip`, which is the only way to be somewhere in a test:
/// nothing sits in front of this server, so the forwarded header is the
/// whole of the caller's identity.
async fn from(
    app: &axum::Router,
    method: &str,
    path: &str,
    ip: &str,
    body: serde_json::Value,
) -> Answer {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("apikey", anon_key())
        .header("content-type", "application/json");
    if !ip.is_empty() {
        req = req.header("x-forwarded-for", ip);
    }
    let res = app
        .clone()
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .expect("router answers");
    answer(res).await
}

async fn answer(res: axum::response::Response) -> Answer {
    let status = res.status();
    let code = res
        .headers()
        .get("x-sb-error-code")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    Answer { status, code, body }
}

async fn post(app: &axum::Router, path: &str, ip: &str, body: serde_json::Value) -> Answer {
    from(app, "POST", path, ip, body).await
}

/// An anonymous sign in, which is the cheapest way to spend a budget
/// whose burst is the number itself rather than thirty.
async fn anonymous(app: &axum::Router, ip: &str) -> Answer {
    post(app, "/auth/v1/signup", ip, serde_json::json!({})).await
}

/// Everything the dev inbox is holding, messages and texts both.
async fn inbox(app: &axum::Router) -> (usize, usize) {
    let req = Request::builder()
        .method("GET")
        .uri("/dev/inbox")
        .header("apikey", service_key())
        .body(Body::empty())
        .unwrap();
    let answer = answer(app.clone().oneshot(req).await.expect("router answers")).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    (
        answer.body["messages"].as_array().expect("messages").len(),
        answer.body["texts"].as_array().expect("texts").len(),
    )
}

fn address(tag: &str) -> String {
    format!("limit-{tag}@zou.test")
}

/// A number in the range set aside for tests, kept apart from the other
/// live suites by the tag.
fn number(tag: u32) -> String {
    format!("1555010{tag:04}")
}

async fn wipe(pool: &Pool, email: &str) {
    let sess = pool.unscoped().await.expect("connect");
    sess.execute("delete from auth.users where email = $1", &[&email])
        .await
        .expect("wipe");
    sess.commit().await.expect("park");
}

async fn wipe_phone(pool: &Pool, phone: &str) {
    let sess = pool.unscoped().await.expect("connect");
    sess.execute("delete from auth.users where phone = $1", &[&phone])
        .await
        .expect("wipe");
    sess.commit().await.expect("park");
}

/// A signed up, confirmed account, which is what /recover needs to
/// find. It is made through a second front door onto the same database,
/// one that confirms its own signups and so posts no mail: an account
/// made through the door under test would spend the very budget the
/// test is about to measure.
async fn confirmed(dsn: &str, email: &str) {
    let setup = router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        mailer_autoconfirm: true,
        ..Config::default()
    })
    .expect("router builds");
    let signed_up = post(
        &setup,
        "/auth/v1/signup",
        "",
        serde_json::json!({"email": email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(signed_up.status, StatusCode::OK, "{}", signed_up.body);
}

async fn column(pool: &Pool, email: &str, column: &str) -> String {
    let sess = pool.unscoped().await.expect("connect");
    let sql = format!("select coalesce({column}::text, '') from auth.users where email = $1");
    let value = sess.query(&sql, &[&email]).await.expect("read")[0].get(0);
    sess.commit().await.expect("park");
    value
}

#[tokio::test]
async fn a_caller_over_its_budget_is_refused_in_upstreams_words() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn, &[BY_HEADER, ("ZOU_RATE_LIMIT_ANONYMOUS_USERS", "2")]);
    let pool = Pool::new(&dsn, 4).expect("dsn parses");

    // The anonymous budget is the one whose burst is the number itself,
    // so two are allowed and the third is not.
    let mut users = Vec::new();
    for _ in 0..2 {
        let signed_in = anonymous(&app, "203.0.113.7").await;
        assert_eq!(signed_in.status, StatusCode::OK, "{}", signed_in.body);
        users.push(signed_in.body["user"]["id"].as_str().unwrap().to_string());
    }
    let refused = anonymous(&app, "203.0.113.7").await;
    assert!(refused.over_budget(), "{:?}", refused.refusal());
    // The machine readable code is on the header too, the way every
    // other refusal on this surface carries it.
    assert_eq!(refused.code, "over_request_rate_limit");

    // Nothing came back with it either: a refusal is a refusal, not a
    // session with a 429 on it.
    assert!(refused.body["access_token"].is_null());
    assert!(refused.body["user"].is_null());

    let sess = pool.unscoped().await.expect("connect");
    for id in &users {
        sess.execute("delete from auth.users where id::text = $1", &[id])
            .await
            .expect("wipe");
    }
    sess.commit().await.expect("park");
}

#[tokio::test]
async fn one_callers_budget_is_not_anothers() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn, &[BY_HEADER, ("ZOU_RATE_LIMIT_ANONYMOUS_USERS", "1")]);
    let pool = Pool::new(&dsn, 4).expect("dsn parses");

    let mine = anonymous(&app, "203.0.113.8").await;
    assert_eq!(mine.status, StatusCode::OK, "{}", mine.body);
    assert!(anonymous(&app, "203.0.113.8").await.over_budget());
    // Somebody else's first request is their first request.
    let yours = anonymous(&app, "198.51.100.8").await;
    assert_eq!(yours.status, StatusCode::OK, "{}", yours.body);

    let sess = pool.unscoped().await.expect("connect");
    for answer in [&mine, &yours] {
        let id = answer.body["user"]["id"].as_str().unwrap();
        sess.execute("delete from auth.users where id::text = $1", &[&id])
            .await
            .expect("wipe");
    }
    sess.commit().await.expect("park");
}

#[tokio::test]
async fn with_no_way_to_tell_callers_apart_nobody_is_limited() {
    let Some(dsn) = dsn() else { return };
    // The GoTrue default: budgets configured, no header named, nothing
    // forwarding. Upstream limits nobody in that state and neither does
    // this, which is the single most surprising thing about the whole
    // feature and so the thing most worth pinning.
    let app = app(&dsn, &[("ZOU_RATE_LIMIT_ANONYMOUS_USERS", "1")]);
    let pool = Pool::new(&dsn, 4).expect("dsn parses");

    let mut ids = Vec::new();
    for _ in 0..4 {
        let signed_in = anonymous(&app, "203.0.113.9").await;
        assert_eq!(signed_in.status, StatusCode::OK, "{}", signed_in.body);
        ids.push(signed_in.body["user"]["id"].as_str().unwrap().to_string());
    }

    let sess = pool.unscoped().await.expect("connect");
    for id in &ids {
        sess.execute("delete from auth.users where id::text = $1", &[id])
            .await
            .expect("wipe");
    }
    sess.commit().await.expect("park");
}

#[tokio::test]
async fn the_platform_header_is_the_first_place_a_caller_is_looked_for() {
    let Some(dsn) = dsn() else { return };
    let app = app(
        &dsn,
        &[
            BY_HEADER,
            ("ZOU_SECURITY_SB_FORWARDED_FOR_ENABLED", "true"),
            ("ZOU_RATE_LIMIT_ANONYMOUS_USERS", "1"),
        ],
    );
    let pool = Pool::new(&dsn, 4).expect("dsn parses");

    // Two requests claiming to be two different callers on the header
    // this server was told to read, and the same caller on the one the
    // platform sets. The platform wins, so the second is refused.
    let first = sb_forwarded(&app, "203.0.113.10", "10.0.0.1").await;
    assert_eq!(first.status, StatusCode::OK, "{}", first.body);
    let second = sb_forwarded(&app, "203.0.113.11", "10.0.0.1").await;
    assert!(second.over_budget(), "{:?}", second.refusal());

    let sess = pool.unscoped().await.expect("connect");
    let id = first.body["user"]["id"].as_str().unwrap();
    sess.execute("delete from auth.users where id::text = $1", &[&id])
        .await
        .expect("wipe");
    sess.commit().await.expect("park");
}

/// An anonymous sign in claiming one address on the header this server
/// reads and another on the one the platform sets.
async fn sb_forwarded(app: &axum::Router, forwarded: &str, platform: &str) -> Answer {
    let req = Request::builder()
        .method("POST")
        .uri("/auth/v1/signup")
        .header("apikey", anon_key())
        .header("content-type", "application/json")
        .header("x-forwarded-for", forwarded)
        .header("sb-forwarded-for", platform)
        .body(Body::from("{}"))
        .unwrap();
    answer(app.clone().oneshot(req).await.expect("router answers")).await
}

#[tokio::test]
async fn a_refused_request_never_reaches_the_endpoint() {
    let Some(dsn) = dsn() else { return };
    // A refill of nothing, so the thirty the burst allows are the
    // thirty there are. The default refill is one every two seconds and
    // thirty password grants take longer than that to hash.
    let app = app(&dsn, &[BY_HEADER, ("ZOU_RATE_LIMIT_TOKEN_REFRESH", "0")]);
    let email = address("token");
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    wipe(&pool, &email).await;
    confirmed(&dsn, &email).await;

    let sign_in = serde_json::json!({"email": &email, "password": "wrong password"});
    // The password is wrong every time, so every one of these is a 400
    // until the budget runs out and they become a 429. The endpoint
    // does not have to succeed to spend from the budget, which is what
    // makes the limit worth anything against someone guessing.
    for i in 0..BURST {
        let answer = post(
            &app,
            "/auth/v1/token?grant_type=password",
            "203.0.113.20",
            sign_in.clone(),
        )
        .await;
        assert_eq!(
            answer.refusal(),
            (400, "invalid_credentials", "Invalid login credentials"),
            "attempt {i}"
        );
    }
    let refused = post(
        &app,
        "/auth/v1/token?grant_type=password",
        "203.0.113.20",
        sign_in,
    )
    .await;
    assert!(refused.over_budget(), "{:?}", refused.refusal());
    // Even the right password is refused now, which is the point: the
    // limit is in front of the endpoint rather than inside it.
    let right = post(
        &app,
        "/auth/v1/token?grant_type=password",
        "203.0.113.20",
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert!(right.over_budget(), "{:?}", right.refusal());
    // And a caller who has spent the token budget can still ask for a
    // code, because that is a different endpoint with a budget of its
    // own.
    let elsewhere = post(
        &app,
        "/auth/v1/recover",
        "203.0.113.20",
        serde_json::json!({"email": &email}),
    )
    .await;
    assert_eq!(elsewhere.status, StatusCode::OK, "{}", elsewhere.body);

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn the_link_in_an_email_is_limited_too() {
    let Some(dsn) = dsn() else { return };
    // Upstream limits both halves of verify, and the GET is the half a
    // browser follows, so it is the half that gets hammered.
    let app = app(&dsn, &[BY_HEADER]);

    for i in 0..BURST {
        let answer = followed(&app, "203.0.113.30").await;
        assert_eq!(
            answer.status,
            StatusCode::SEE_OTHER,
            "attempt {i} answered {}",
            answer.status
        );
    }
    let refused = followed(&app, "203.0.113.30").await;
    assert!(refused.over_budget(), "{:?}", refused.refusal());
}

/// A followed link with a token nobody minted. It lands on the site
/// with an error in the fragment, which is a 303 rather than a refusal,
/// and it spends the same budget either way.
async fn followed(app: &axum::Router, ip: &str) -> Answer {
    let req = Request::builder()
        .method("GET")
        .uri("/auth/v1/verify?type=signup&token=nothing&redirect_to=https://app.zou.test")
        .header("x-forwarded-for", ip)
        .body(Body::empty())
        .unwrap();
    answer(app.clone().oneshot(req).await.expect("router answers")).await
}

#[tokio::test]
async fn the_mail_budget_is_the_whole_servers_and_takes_the_flow_with_it() {
    let Some(dsn) = dsn() else { return };
    // No header, so nobody is limited by endpoint. The send budget is
    // not keyed on anybody, which is the difference under test here.
    let app = app(&dsn, &[("ZOU_RATE_LIMIT_EMAIL_SENT", "1")]);
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let (mine, yours) = (address("mail-first"), address("mail-second"));
    for email in [&mine, &yours] {
        wipe(&pool, email).await;
        confirmed(&dsn, email).await;
    }

    let first = post(
        &app,
        "/auth/v1/recover",
        "203.0.113.40",
        serde_json::json!({"email": &mine}),
    )
    .await;
    assert_eq!(first.status, StatusCode::OK, "{}", first.body);
    assert_eq!(inbox(&app).await.0, 1);

    // A different account, a different caller, and the server has still
    // posted all the mail it is allowed to this hour.
    let second = post(
        &app,
        "/auth/v1/recover",
        "198.51.100.40",
        serde_json::json!({"email": &yours}),
    )
    .await;
    assert_eq!(
        second.refusal(),
        (
            429,
            "over_email_send_rate_limit",
            "email rate limit exceeded"
        )
    );
    assert_eq!(inbox(&app).await.0, 1, "a refused send went out anyway");
    // The code that was drawn for it went with the refusal, so the
    // account is not left holding one nobody was told.
    assert_eq!(column(&pool, &yours, "recovery_token").await, "");
    assert_eq!(column(&pool, &yours, "recovery_sent_at").await, "");
    // The one that did go out is still on the account it went to.
    assert_ne!(column(&pool, &mine, "recovery_token").await, "");

    for email in [&mine, &yours] {
        wipe(&pool, email).await;
    }
}

#[tokio::test]
async fn the_text_budget_is_the_whole_servers_and_takes_the_flow_with_it() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn, &[("ZOU_RATE_LIMIT_SMS_SENT", "1")]);
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let (mine, yours) = (number(1), number(2));
    for phone in [&mine, &yours] {
        wipe_phone(&pool, phone).await;
    }

    let first = post(
        &app,
        "/auth/v1/otp",
        "203.0.113.50",
        serde_json::json!({"phone": &mine}),
    )
    .await;
    assert_eq!(first.status, StatusCode::OK, "{}", first.body);
    assert_eq!(inbox(&app).await.1, 1);

    let second = post(
        &app,
        "/auth/v1/otp",
        "198.51.100.50",
        serde_json::json!({"phone": &yours}),
    )
    .await;
    assert_eq!(
        second.refusal(),
        (429, "over_sms_send_rate_limit", "SMS rate limit exceeded")
    );
    assert_eq!(inbox(&app).await.1, 1, "a refused text went out anyway");
    // The account the otp would have created went with it.
    let sess = pool.unscoped().await.expect("connect");
    let left: i64 = sess
        .query(
            "select count(*) from auth.users where phone = $1",
            &[&yours],
        )
        .await
        .expect("count")[0]
        .get(0);
    sess.commit().await.expect("park");
    assert_eq!(left, 0, "a refused text left an account behind");

    for phone in [&mine, &yours] {
        wipe_phone(&pool, phone).await;
    }
}

#[tokio::test]
async fn a_budget_nobody_configured_is_gotrues_own() {
    let Some(dsn) = dsn() else { return };
    // Nothing set but the header, so every endpoint number is
    // upstream's default. The otp budget is thirty over five minutes
    // with a burst of thirty, and the thirty first ask is refused. The
    // mail budget is moved out of the way because it is the other kind
    // of limit and its default would answer first.
    let app = app(&dsn, &[BY_HEADER, ("ZOU_RATE_LIMIT_EMAIL_SENT", "1000")]);
    let email = address("default");
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    wipe(&pool, &email).await;
    confirmed(&dsn, &email).await;

    let asking = serde_json::json!({"email": &email});
    for i in 0..BURST {
        let answer = post(&app, "/auth/v1/recover", "203.0.113.60", asking.clone()).await;
        assert_eq!(
            answer.status,
            StatusCode::OK,
            "attempt {i}: {}",
            answer.body
        );
    }
    let refused = post(&app, "/auth/v1/recover", "203.0.113.60", asking).await;
    assert!(refused.over_budget(), "{:?}", refused.refusal());

    wipe(&pool, &email).await;
}
