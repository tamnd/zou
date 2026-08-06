//! The audit trail, against a live postgres.
//!
//! What is pinned here is the thing a unit test cannot say: that a flow
//! which claims to write an entry leaves a row behind, that the row says
//! what upstream's row says, and that a flow which writes nothing leaves
//! nothing. Every assertion reads the rows back out of
//! auth.audit_log_entries rather than trusting the handler.
//!
//! The trail is read per account, matching either the actor or the
//! subject, because an admin action names its subject in the traits and
//! files itself against the role. That is also the reason these tests
//! never truncate the table: the suites run against one database at
//! once and an account is the only thing that makes a row findable.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test auth_audit

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use zou_server::sql::Pool;
use zou_server::{Config, jwt, mail, router, sms, totp};

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

/// A project that makes the caller confirm, which is what a stock
/// GoTrue does and so what most of these tests want.
fn confirming(dsn: &str) -> axum::Router {
    project(dsn, false)
}

/// A project that confirms its own signups, so a signup is a session.
fn instant(dsn: &str) -> axum::Router {
    project(dsn, true)
}

fn project(dsn: &str, autoconfirm: bool) -> axum::Router {
    router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        external_url: Some("https://zou.test".to_string()),
        site_url: Some(SITE.to_string()),
        mailer_autoconfirm: autoconfirm,
        phone_enabled: true,
        anonymous_users: true,
        // Only the unlink test needs this, and it costs the others
        // nothing: it opens the two identity endpoints and touches no
        // other flow.
        manual_linking: true,
        // The per account send frequency would answer some of these
        // before the flow under test got to write anything.
        mail: mail::Settings {
            max_frequency: 0,
            ..mail::Settings::default()
        },
        sms: sms::Settings {
            max_frequency: 0,
            autoconfirm,
            ..sms::Settings::default()
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
}

impl Answer {
    fn str(&self, key: &str) -> String {
        self.body[key]
            .as_str()
            .unwrap_or_else(|| panic!("no {key} in {}", self.body))
            .to_string()
    }

    fn refusal(&self) -> (u16, &str) {
        (
            self.status.as_u16(),
            self.body["error_code"].as_str().unwrap_or("<none>"),
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

/// A request, with whatever bearer and whatever forwarded address the
/// test needs. The address is the whole of a caller's location here:
/// nothing sits in front of this server.
async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    from: Option<&str>,
    body: serde_json::Value,
) -> Answer {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("apikey", anon_key())
        .header("content-type", "application/json");
    if let Some(token) = bearer {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    if let Some(ip) = from {
        req = req.header("x-forwarded-for", ip);
    }
    let req = req.body(Body::from(body.to_string())).unwrap();
    answer(app.clone().oneshot(req).await.expect("router answers")).await
}

async fn post(app: &axum::Router, path: &str, body: serde_json::Value) -> Answer {
    call(app, "POST", path, None, None, body).await
}

async fn as_user(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: serde_json::Value,
) -> Answer {
    call(app, method, path, Some(token), None, body).await
}

/// A request holding the service key, which is what an admin endpoint
/// wants and what makes the actor a role rather than a person.
async fn as_admin(app: &axum::Router, method: &str, path: &str, body: serde_json::Value) -> Answer {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("apikey", service_key())
        .header("authorization", format!("Bearer {}", service_key()))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    answer(app.clone().oneshot(req).await.expect("router answers")).await
}

/// One row of the trail, as a reader of the table sees it.
#[derive(Debug)]
struct Entry {
    instance_id: String,
    ip_address: String,
    created_at: String,
    action: String,
    log_type: String,
    actor_id: String,
    actor_username: String,
    actor_via_sso: bool,
    actor_name: Option<String>,
    traits: Option<serde_json::Value>,
}

impl Entry {
    fn trait_str(&self, key: &str) -> String {
        self.traits
            .as_ref()
            .unwrap_or_else(|| panic!("{} carries no traits", self.action))[key]
            .as_str()
            .unwrap_or_else(|| panic!("{} has no {key} trait", self.action))
            .to_string()
    }
}

/// Everything the trail holds about an account, oldest first. An entry
/// is about an account when the account did it or when the account is
/// what it was done to, which is how the admin entries are found: they
/// are filed against the role and name their subject in the traits.
async fn trail(pool: &Pool, user_id: &str) -> Vec<Entry> {
    let sess = pool.unscoped().await.expect("connect");
    let rows = sess
        .query(
            "select instance_id::text, ip_address, created_at::text, payload::text
               from auth.audit_log_entries
              where payload::jsonb ->> 'actor_id' = $1
                 or payload::jsonb -> 'traits' ->> 'user_id' = $1
              order by created_at asc",
            &[&user_id],
        )
        .await
        .expect("read the trail");
    sess.commit().await.expect("park");
    rows.iter()
        .map(|row| {
            let instance_id: String = row.get(0);
            let ip_address: String = row.get(1);
            let created_at: String = row.get(2);
            let payload: String = row.get(3);
            let payload: serde_json::Value =
                serde_json::from_str(&payload).expect("the payload is json");
            Entry {
                instance_id,
                ip_address,
                created_at,
                action: payload["action"].as_str().expect("an action").to_string(),
                log_type: payload["log_type"]
                    .as_str()
                    .expect("a log type")
                    .to_string(),
                actor_id: payload["actor_id"].as_str().expect("an actor").to_string(),
                actor_username: payload["actor_username"]
                    .as_str()
                    .expect("a username")
                    .to_string(),
                actor_via_sso: payload["actor_via_sso"].as_bool().expect("an sso flag"),
                actor_name: payload["actor_name"].as_str().map(str::to_string),
                traits: payload.get("traits").cloned(),
            }
        })
        .collect()
}

/// The actions the trail holds about an account, in order, which is what
/// most of these tests are really asking about.
async fn actions(pool: &Pool, user_id: &str) -> Vec<String> {
    trail(pool, user_id)
        .await
        .into_iter()
        .map(|e| e.action)
        .collect()
}

const NOBODY: &str = "00000000-0000-0000-0000-000000000000";

fn pool(dsn: &str) -> Pool {
    Pool::new(dsn, 4).expect("dsn parses")
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

fn address(tag: &str) -> String {
    format!("audit-{tag}@zou.test")
}

fn number(tag: u32) -> String {
    format!("1555030{tag:04}")
}

/// The id of an account, whether or not anybody handed back a session
/// for it.
async fn id_of(pool: &Pool, email: &str) -> String {
    let sess = pool.unscoped().await.expect("connect");
    let rows = sess
        .query(
            "select id::text from auth.users where email = $1",
            &[&email],
        )
        .await
        .expect("look up");
    sess.commit().await.expect("park");
    rows.first()
        .unwrap_or_else(|| panic!("no account for {email}"))
        .get(0)
}

/// A signed up account with a session, on a project that confirms its
/// own signups.
struct Session {
    user_id: String,
    access: String,
    refresh: String,
}

async fn signed_up(app: &axum::Router, email: &str) -> Session {
    let answer = post(
        app,
        "/auth/v1/signup",
        serde_json::json!({"email": email, "password": "correct horse battery"}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    Session {
        user_id: answer.body["user"]["id"]
            .as_str()
            .expect("an id")
            .to_string(),
        access: answer.str("access_token"),
        refresh: answer.str("refresh_token"),
    }
}

/// The link the dev inbox last took, followed the way a mail client
/// follows one: no apikey, because a mail client has none. The link
/// carries the hashed code, which is what makes this the only way a
/// test can follow one without planting a code of its own first.
async fn follow_last_link(app: &axum::Router) -> StatusCode {
    let req = Request::builder()
        .method("GET")
        .uri("/dev/inbox")
        .header("apikey", service_key())
        .body(Body::empty())
        .unwrap();
    let inbox = answer(app.clone().oneshot(req).await.expect("router answers")).await;
    assert_eq!(inbox.status, StatusCode::OK, "{}", inbox.body);
    let messages = inbox.body["messages"].as_array().expect("messages");
    let last = messages.last().expect("a message was posted");
    let link = last["link"]
        .as_str()
        .unwrap_or_else(|| panic!("no link in {last}"));
    let query = link
        .split_once('?')
        .unwrap_or_else(|| panic!("no query in {link}"))
        .1;
    let req = Request::builder()
        .method("GET")
        .uri(format!("/auth/v1/verify?{query}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.expect("router answers");
    let location = res
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        !location.contains("error"),
        "the link did not take: {location}",
    );
    res.status()
}

#[tokio::test]
async fn a_signup_that_needs_confirming_writes_the_confirmation_request() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("confirm-me");
    wipe(&pool, &email).await;
    let app = confirming(&dsn);

    let answer = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"email": email, "password": "correct horse battery"}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let user_id = id_of(&pool, &email).await;

    let trail = trail(&pool, &user_id).await;
    assert_eq!(
        trail.iter().map(|e| e.action.as_str()).collect::<Vec<_>>(),
        vec!["user_confirmation_requested"],
    );
    let entry = &trail[0];
    // The whole shape of a row, once, so the rest of this file can ask
    // about actions and orderings and take the rest on trust.
    assert_eq!(entry.instance_id, NOBODY, "the instance is always nobody");
    assert_eq!(entry.log_type, "user");
    assert_eq!(entry.actor_id, user_id);
    assert_eq!(entry.actor_username, email);
    assert!(!entry.actor_via_sso);
    assert_eq!(entry.actor_name, None, "no full name, so no actor_name key");
    assert_eq!(entry.trait_str("provider"), "email");
    assert_eq!(entry.ip_address, "", "only the factor entries fill this in");

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_signup_that_confirms_itself_writes_the_signup_and_then_the_login() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("instant");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let session = signed_up(&app, &email).await;
    let trail = trail(&pool, &session.user_id).await;
    assert_eq!(
        trail.iter().map(|e| e.action.as_str()).collect::<Vec<_>>(),
        vec!["user_signedup", "login"],
        "the order is the order they happened in, which is what \
         clock_timestamp buys",
    );
    assert_eq!(trail[0].log_type, "team");
    assert_eq!(trail[0].trait_str("provider"), "email");
    assert_eq!(trail[1].log_type, "account");
    assert_eq!(trail[1].trait_str("provider"), "email");

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_repeated_signup_leaves_an_entry_the_signup_itself_did_not() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("again");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let session = signed_up(&app, &email).await;
    let again = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"email": email, "password": "a different one"}),
    )
    .await;
    assert_eq!(again.refusal(), (422, "user_already_exists"));

    // The entry is the point. The signup it describes was rolled back,
    // and the entry is committed separately so that the trail can say
    // somebody tried.
    assert_eq!(
        actions(&pool, &session.user_id).await,
        vec!["user_signedup", "login", "user_repeated_signup"],
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn the_actor_name_comes_out_of_the_metadata() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("named");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let answer = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({
            "email": email,
            "password": "correct horse battery",
            "data": {"full_name": "Ada Lovelace"},
        }),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let user_id = answer.body["user"]["id"].as_str().expect("an id");

    let trail = trail(&pool, user_id).await;
    assert!(!trail.is_empty());
    for entry in &trail {
        assert_eq!(
            entry.actor_name.as_deref(),
            Some("Ada Lovelace"),
            "{} lost the name",
            entry.action,
        );
    }

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn an_account_with_a_number_is_named_by_the_number() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let phone = number(1);
    wipe_phone(&pool, &phone).await;
    let app = instant(&dsn);

    let answer = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"phone": phone, "password": "correct horse battery"}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let user_id = answer.body["user"]["id"].as_str().expect("an id");

    let trail = trail(&pool, user_id).await;
    assert!(!trail.is_empty());
    for entry in &trail {
        assert_eq!(
            entry.actor_username, phone,
            "{} should name the number, which upstream prefers over the \
             address",
            entry.action,
        );
    }

    wipe_phone(&pool, &phone).await;
}

#[tokio::test]
async fn a_password_grant_writes_a_login() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("grant");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let session = signed_up(&app, &email).await;
    let answer = post(
        &app,
        "/auth/v1/token?grant_type=password",
        serde_json::json!({"email": email, "password": "correct horse battery"}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);

    let trail = trail(&pool, &session.user_id).await;
    assert_eq!(
        trail.iter().map(|e| e.action.as_str()).collect::<Vec<_>>(),
        vec!["user_signedup", "login", "login"],
    );
    assert_eq!(trail[2].trait_str("provider"), "email");

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_refresh_writes_the_refresh_and_then_the_revoke() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("refresh");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let session = signed_up(&app, &email).await;
    let answer = post(
        &app,
        "/auth/v1/token?grant_type=refresh_token",
        serde_json::json!({"refresh_token": session.refresh}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);

    let trail = trail(&pool, &session.user_id).await;
    assert_eq!(
        trail.iter().map(|e| e.action.as_str()).collect::<Vec<_>>(),
        vec!["user_signedup", "login", "token_refreshed", "token_revoked"],
        "upstream writes the refresh before the swap that revokes, and \
         a reader who sorts by created_at should see the same",
    );
    assert_eq!(trail[2].log_type, "token");
    assert_eq!(trail[3].log_type, "token");
    assert_eq!(trail[2].actor_id, session.user_id);
    assert_eq!(trail[3].actor_id, session.user_id);
    // Both of those were written inside one transaction, and they still
    // carry different instants. That is what clock_timestamp buys and
    // it is the whole reason the order above is readable: with the
    // transaction's clock a reader sorting by created_at gets a tie it
    // cannot break.
    assert_ne!(
        trail[2].created_at, trail[3].created_at,
        "two entries from one transaction share an instant",
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_stolen_refresh_token_writes_nothing() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("stolen");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let session = signed_up(&app, &email).await;
    let first = post(
        &app,
        "/auth/v1/token?grant_type=refresh_token",
        serde_json::json!({"refresh_token": session.refresh}),
    )
    .await;
    assert_eq!(first.status, StatusCode::OK, "{}", first.body);
    // Use the child, which puts the parent outside the window where a
    // second use is treated as a retry.
    let second = post(
        &app,
        "/auth/v1/token?grant_type=refresh_token",
        serde_json::json!({"refresh_token": first.str("refresh_token")}),
    )
    .await;
    assert_eq!(second.status, StatusCode::OK, "{}", second.body);
    let before = actions(&pool, &session.user_id).await;

    let reused = post(
        &app,
        "/auth/v1/token?grant_type=refresh_token",
        serde_json::json!({"refresh_token": session.refresh}),
    )
    .await;
    assert_eq!(reused.refusal(), (400, "refresh_token_already_used"));
    assert_eq!(
        actions(&pool, &session.user_id).await,
        before,
        "a refusal that revoked the family should not also claim a \
         token was refreshed",
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn logging_out_writes_a_logout() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("logout");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let session = signed_up(&app, &email).await;
    let answer = as_user(
        &app,
        "POST",
        "/auth/v1/logout",
        &session.access,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::NO_CONTENT, "{}", answer.body);

    let trail = trail(&pool, &session.user_id).await;
    assert_eq!(
        trail.last().map(|e| e.action.as_str()),
        Some("logout"),
        "{trail:?}",
    );
    assert_eq!(trail.last().expect("an entry").log_type, "account");

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_password_change_writes_the_password_and_the_modification() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("newpass");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let session = signed_up(&app, &email).await;
    let answer = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &session.access,
        serde_json::json!({"password": "an entirely different one"}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);

    assert_eq!(
        actions(&pool, &session.user_id).await,
        vec![
            "user_signedup",
            "login",
            "user_updated_password",
            "user_modified"
        ],
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn asking_for_a_recovery_writes_the_request() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("recover");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let session = signed_up(&app, &email).await;
    let answer = post(
        &app,
        "/auth/v1/recover",
        serde_json::json!({"email": email}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);

    let trail = trail(&pool, &session.user_id).await;
    assert_eq!(
        trail.last().map(|e| e.action.as_str()),
        Some("user_recovery_requested"),
        "{trail:?}",
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn reauthenticating_writes_its_own_action() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("reauth");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let session = signed_up(&app, &email).await;
    // A GET, because that is what upstream routes and what the client
    // sends. A POST here answers 405 with Allow: GET.
    let answer = as_user(
        &app,
        "GET",
        "/auth/v1/reauthenticate",
        &session.access,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);

    let trail = trail(&pool, &session.user_id).await;
    assert_eq!(
        trail.last().map(|e| e.action.as_str()),
        Some("user_reauthenticate_requested"),
        "{trail:?}",
    );
    let entry = trail.last().expect("an entry");
    assert_eq!(entry.log_type, "user");
    assert!(
        entry.traits.is_none(),
        "the key is left off entirely when there is nothing to put in \
         it, so that payload ? 'traits' means what it means upstream",
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn following_a_recovery_link_writes_a_login() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("followed");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let session = signed_up(&app, &email).await;
    let asked = post(
        &app,
        "/auth/v1/recover",
        serde_json::json!({"email": email}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);
    assert_eq!(follow_last_link(&app).await, StatusCode::SEE_OTHER);

    assert_eq!(
        actions(&pool, &session.user_id).await,
        vec!["user_signedup", "login", "user_recovery_requested", "login"],
        "an address that was already confirmed logs in rather than \
         signing up a second time",
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn confirming_a_signup_writes_the_signup_rather_than_the_login() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("confirmed");
    wipe(&pool, &email).await;
    let app = confirming(&dsn);

    let answer = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"email": email, "password": "correct horse battery"}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let user_id = id_of(&pool, &email).await;
    assert_eq!(follow_last_link(&app).await, StatusCode::SEE_OTHER);

    let trail = trail(&pool, &user_id).await;
    assert_eq!(
        trail.iter().map(|e| e.action.as_str()).collect::<Vec<_>>(),
        vec!["user_confirmation_requested", "user_signedup"],
    );
    assert_eq!(trail[1].trait_str("provider"), "email");

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn an_anonymous_sign_in_writes_nothing() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let app = instant(&dsn);

    let answer = post(&app, "/auth/v1/signup", serde_json::json!({})).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let user_id = answer.body["user"]["id"].as_str().expect("an id");

    assert!(
        actions(&pool, user_id).await.is_empty(),
        "upstream writes no entry for an anonymous grant, which is the \
         one hole in the trail and is kept",
    );

    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        "delete from auth.users where id = $1::text::uuid",
        &[&user_id],
    )
    .await
    .expect("wipe");
    sess.commit().await.expect("park");
}

#[tokio::test]
async fn an_admin_is_a_role_and_names_who_it_acted_on() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("by-admin");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let made = as_admin(
        &app,
        "POST",
        "/auth/v1/admin/users",
        serde_json::json!({"email": email, "password": "correct horse battery"}),
    )
    .await;
    assert_eq!(made.status, StatusCode::OK, "{}", made.body);
    let user_id = made.str("id");

    let amended = as_admin(
        &app,
        "PUT",
        &format!("/auth/v1/admin/users/{user_id}"),
        serde_json::json!({"user_metadata": {"seat": "window"}}),
    )
    .await;
    assert_eq!(amended.status, StatusCode::OK, "{}", amended.body);

    let removed = as_admin(
        &app,
        "DELETE",
        &format!("/auth/v1/admin/users/{user_id}"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(removed.status, StatusCode::OK, "{}", removed.body);

    let trail = trail(&pool, &user_id).await;
    assert_eq!(
        trail.iter().map(|e| e.action.as_str()).collect::<Vec<_>>(),
        vec!["user_signedup", "user_modified", "user_deleted"],
    );
    for entry in &trail {
        assert_eq!(
            entry.actor_id, NOBODY,
            "{} should be filed against nobody, because an admin is not \
             a person upstream",
            entry.action,
        );
        assert_eq!(entry.actor_username, "service_role");
        assert!(!entry.actor_via_sso);
        assert_eq!(entry.trait_str("user_id"), user_id);
        assert_eq!(entry.trait_str("user_email"), email);
        assert_eq!(entry.trait_str("user_phone"), "");
    }
    assert_eq!(
        trail[0].trait_str("provider"),
        "email",
        "only the signup carries one",
    );
    assert!(trail[2].traits.as_ref().expect("traits")["provider"].is_null());

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn an_invite_carries_the_address_and_no_number() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("invited");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let answer = as_admin(
        &app,
        "POST",
        "/auth/v1/invite",
        serde_json::json!({"email": email}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let user_id = answer.str("id");

    let trail = trail(&pool, &user_id).await;
    assert_eq!(
        trail.iter().map(|e| e.action.as_str()).collect::<Vec<_>>(),
        vec!["user_invited"],
    );
    assert_eq!(trail[0].log_type, "team");
    assert_eq!(trail[0].actor_username, "service_role");
    assert_eq!(trail[0].trait_str("user_email"), email);
    assert!(
        trail[0].traits.as_ref().expect("traits")["user_phone"].is_null(),
        "an invitation goes to an address, and upstream writes two \
         traits rather than three",
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_generated_link_is_filed_against_the_person_or_nobody() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("generated");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    // A signup link, which writes nothing at all: an account can be
    // brought into existence through this endpoint and the trail will
    // not say so. Upstream's, and kept.
    let made = as_admin(
        &app,
        "POST",
        "/auth/v1/admin/generate_link",
        serde_json::json!({
            "type": "signup",
            "email": email,
            "password": "correct horse battery",
        }),
    )
    .await;
    assert_eq!(made.status, StatusCode::OK, "{}", made.body);
    let user_id = id_of(&pool, &email).await;
    assert!(actions(&pool, &user_id).await.is_empty());

    let recovery = as_admin(
        &app,
        "POST",
        "/auth/v1/admin/generate_link",
        serde_json::json!({"type": "recovery", "email": email}),
    )
    .await;
    assert_eq!(recovery.status, StatusCode::OK, "{}", recovery.body);

    let trail = trail(&pool, &user_id).await;
    assert_eq!(
        trail.iter().map(|e| e.action.as_str()).collect::<Vec<_>>(),
        vec!["user_recovery_requested"],
    );
    assert_eq!(
        trail[0].actor_id, user_id,
        "a recovery link an admin generated reads the same as one the \
         person asked for",
    );
    assert_eq!(trail[0].actor_username, email);

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_generated_invite_link_is_filed_against_the_role() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("gen-invite");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let answer = as_admin(
        &app,
        "POST",
        "/auth/v1/admin/generate_link",
        serde_json::json!({"type": "invite", "email": email}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let user_id = id_of(&pool, &email).await;

    let trail = trail(&pool, &user_id).await;
    assert_eq!(
        trail.iter().map(|e| e.action.as_str()).collect::<Vec<_>>(),
        vec!["user_invited"],
    );
    assert_eq!(trail[0].actor_id, NOBODY);
    assert_eq!(trail[0].actor_username, "service_role");

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn the_factor_entries_are_the_only_ones_that_say_where_they_came_from() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("factor");
    wipe(&pool, &email).await;
    let app = instant(&dsn);
    const FROM: &str = "203.0.113.9";

    let session = signed_up(&app, &email).await;
    let enrolled = call(
        &app,
        "POST",
        "/auth/v1/factors",
        Some(&session.access),
        Some(FROM),
        serde_json::json!({"factor_type": "totp", "friendly_name": "phone"}),
    )
    .await;
    assert_eq!(enrolled.status, StatusCode::OK, "{}", enrolled.body);
    let factor_id = enrolled.str("id");
    let secret = enrolled.body["totp"]["secret"]
        .as_str()
        .expect("a secret")
        .to_string();

    let challenged = call(
        &app,
        "POST",
        &format!("/auth/v1/factors/{factor_id}/challenge"),
        Some(&session.access),
        Some(FROM),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(challenged.status, StatusCode::OK, "{}", challenged.body);
    let challenge_id = challenged.str("id");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after 1970")
        .as_secs() as i64;
    let verified = call(
        &app,
        "POST",
        &format!("/auth/v1/factors/{factor_id}/verify"),
        Some(&session.access),
        Some(FROM),
        serde_json::json!({
            "challenge_id": challenge_id,
            "code": totp::code(&secret, now),
        }),
    )
    .await;
    assert_eq!(verified.status, StatusCode::OK, "{}", verified.body);
    let aal2 = verified.str("access_token");

    let unenrolled = call(
        &app,
        "DELETE",
        &format!("/auth/v1/factors/{factor_id}"),
        Some(&aal2),
        Some(FROM),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(unenrolled.status, StatusCode::OK, "{}", unenrolled.body);

    let trail = trail(&pool, &session.user_id).await;
    let factor_entries: Vec<&Entry> = trail.iter().filter(|e| e.log_type == "factor").collect();
    assert_eq!(
        factor_entries
            .iter()
            .map(|e| e.action.as_str())
            .collect::<Vec<_>>(),
        vec![
            "factor_in_progress",
            "challenge_created",
            "verification_attempted",
            "factor_unenrolled"
        ],
    );
    for entry in &factor_entries {
        assert_eq!(
            entry.ip_address, FROM,
            "{} is a factor event and upstream fills these in",
            entry.action,
        );
        assert_eq!(entry.trait_str("factor_id"), factor_id);
    }
    assert!(
        factor_entries[0].traits.as_ref().expect("traits")["factor_type"].is_null(),
        "upstream leaves factor_type off the TOTP enrollment",
    );
    assert_eq!(factor_entries[1].trait_str("factor_status"), "unverified");
    assert_eq!(factor_entries[2].trait_str("challenge_id"), challenge_id);
    assert_eq!(factor_entries[2].trait_str("factor_type"), "totp");
    assert_eq!(factor_entries[3].trait_str("factor_status"), "verified");
    assert!(!factor_entries[3].trait_str("session_id").is_empty());

    // And the entries the same account wrote through the front door say
    // nothing about where they came from, which is upstream's oldest
    // wart and the reason this test is worded the way it is.
    for entry in trail.iter().filter(|e| e.log_type != "factor") {
        assert_eq!(
            entry.ip_address, "",
            "{} filled in an address",
            entry.action
        );
    }

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_wrong_code_writes_no_attempt() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("wrong-code");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let session = signed_up(&app, &email).await;
    let enrolled = as_user(
        &app,
        "POST",
        "/auth/v1/factors",
        &session.access,
        serde_json::json!({"factor_type": "totp", "friendly_name": "phone"}),
    )
    .await;
    assert_eq!(enrolled.status, StatusCode::OK, "{}", enrolled.body);
    let factor_id = enrolled.str("id");
    let challenged = as_user(
        &app,
        "POST",
        &format!("/auth/v1/factors/{factor_id}/challenge"),
        &session.access,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(challenged.status, StatusCode::OK, "{}", challenged.body);

    let refused = as_user(
        &app,
        "POST",
        &format!("/auth/v1/factors/{factor_id}/verify"),
        &session.access,
        serde_json::json!({"challenge_id": challenged.str("id"), "code": "000000"}),
    )
    .await;
    assert_eq!(refused.refusal(), (422, "mfa_verification_failed"));

    assert!(
        !actions(&pool, &session.user_id)
            .await
            .contains(&"verification_attempted".to_string()),
        "upstream only writes the attempt once the code has checked \
         out, so the trail cannot be used to count guesses",
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn every_entry_lands_in_a_family_somebody_groups_by() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("families");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let session = signed_up(&app, &email).await;
    let refreshed = post(
        &app,
        "/auth/v1/token?grant_type=refresh_token",
        serde_json::json!({"refresh_token": session.refresh}),
    )
    .await;
    assert_eq!(refreshed.status, StatusCode::OK, "{}", refreshed.body);
    let asked = post(
        &app,
        "/auth/v1/recover",
        serde_json::json!({"email": email}),
    )
    .await;
    assert_eq!(asked.status, StatusCode::OK, "{}", asked.body);

    let trail = trail(&pool, &session.user_id).await;
    assert!(trail.len() >= 5, "{trail:?}");
    for entry in &trail {
        assert!(
            [
                "account",
                "team",
                "token",
                "user",
                "factor",
                "recovery_codes"
            ]
            .contains(&entry.log_type.as_str()),
            "{} landed in {}",
            entry.action,
            entry.log_type,
        );
        assert_eq!(entry.instance_id, NOBODY);
        assert!(!entry.actor_id.is_empty());
        assert!(!entry.action.is_empty());
    }

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn asking_for_a_magic_link_writes_a_recovery_request() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("magic");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let session = signed_up(&app, &email).await;
    let answer = post(
        &app,
        "/auth/v1/magiclink",
        serde_json::json!({"email": email}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);

    let trail = trail(&pool, &session.user_id).await;
    // Upstream files a magic link under the recovery action rather than
    // giving it one of its own, which is worth knowing before writing a
    // query that counts password resets.
    assert_eq!(
        trail.last().map(|e| e.action.as_str()),
        Some("user_recovery_requested"),
        "{trail:?}",
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_texted_code_says_which_channel_it_went_down() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let phone = number(2);
    wipe_phone(&pool, &phone).await;
    let app = instant(&dsn);

    let answer = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"phone": phone, "password": "correct horse battery"}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let user_id = answer.body["user"]["id"]
        .as_str()
        .expect("an id")
        .to_string();

    let texted = post(&app, "/auth/v1/otp", serde_json::json!({"phone": phone})).await;
    assert_eq!(texted.status, StatusCode::OK, "{}", texted.body);

    let trail = trail(&pool, &user_id).await;
    let last = trail.last().expect("an entry");
    assert_eq!(last.action, "user_recovery_requested");
    assert_eq!(
        last.trait_str("channel"),
        "sms",
        "the channel is the one trait a text carries and an email does \
         not",
    );

    wipe_phone(&pool, &phone).await;
}

#[tokio::test]
async fn a_resent_confirmation_writes_the_request_again() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("resend");
    wipe(&pool, &email).await;
    let app = confirming(&dsn);

    let answer = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"email": email, "password": "correct horse battery"}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let user_id = id_of(&pool, &email).await;

    let again = post(
        &app,
        "/auth/v1/resend",
        serde_json::json!({"type": "signup", "email": email}),
    )
    .await;
    assert_eq!(again.status, StatusCode::OK, "{}", again.body);

    let trail = trail(&pool, &user_id).await;
    assert_eq!(
        trail.iter().map(|e| e.action.as_str()).collect::<Vec<_>>(),
        vec!["user_confirmation_requested", "user_confirmation_requested"],
    );
    assert!(
        trail[1].traits.is_none(),
        "the resend carries no provider, which the signup does, and \
         that asymmetry is upstream's",
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn unlinking_an_identity_names_the_provider_and_the_identity() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("unlink");
    wipe(&pool, &email).await;
    let app = instant(&dsn);

    let session = signed_up(&app, &email).await;
    // A second identity, planted rather than earned: what is under test
    // is the entry the unlink writes, and standing up a provider to
    // link against would only slow that down.
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        "insert into auth.identities
             (id, user_id, provider_id, provider, identity_data,
              created_at, updated_at)
         values (gen_random_uuid(), $1::text::uuid, '4141', 'github',
                 jsonb_build_object('sub', '4141', 'email', $2::text),
                 now(), now())",
        &[&session.user_id, &email],
    )
    .await
    .expect("plant an identity");
    sess.execute(
        "update auth.users
            set raw_app_meta_data =
                    raw_app_meta_data || '{\"providers\": [\"email\", \"github\"]}'::jsonb
          where id = $1::text::uuid",
        &[&session.user_id],
    )
    .await
    .expect("advertise it");
    sess.commit().await.expect("commit");

    let identity: String = {
        let sess = pool.unscoped().await.expect("connect");
        let rows = sess
            .query(
                "select id::text from auth.identities
                  where user_id = $1::text::uuid and provider = 'github'",
                &[&session.user_id],
            )
            .await
            .expect("find it");
        sess.commit().await.expect("park");
        rows[0].get(0)
    };

    let answer = as_user(
        &app,
        "DELETE",
        &format!("/auth/v1/user/identities/{identity}"),
        &session.access,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);

    let trail = trail(&pool, &session.user_id).await;
    let last = trail.last().expect("an entry");
    assert_eq!(last.action, "identity_unlinked");
    assert_eq!(
        last.log_type, "user",
        "upstream files this under the user rather than the account, \
         which is the odd one in its table",
    );
    assert_eq!(last.trait_str("identity_id"), identity);
    assert_eq!(last.trait_str("provider"), "github");
    assert_eq!(last.trait_str("provider_id"), "4141");

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn an_account_holding_both_is_named_by_the_number() {
    let Some(dsn) = dsn() else { return };
    let pool = pool(&dsn);
    let email = address("both");
    let phone = number(3);
    wipe(&pool, &email).await;
    wipe_phone(&pool, &phone).await;
    let app = instant(&dsn);

    // The one case where the order of the coalesce is visible. An
    // account with only a number is named by it either way, so this is
    // the test that says upstream prefers the number to the address
    // rather than merely falling back to it.
    let made = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"phone": phone, "password": "correct horse battery"}),
    )
    .await;
    assert_eq!(made.status, StatusCode::OK, "{}", made.body);
    let user_id = made.body["user"]["id"].as_str().expect("an id").to_string();

    // The address is planted, because nothing zou serves yet puts both
    // on one account, and what is under test is the coalesce rather
    // than the endpoint that would have got there.
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        "update auth.users set email = $2, email_confirmed_at = now()
          where id = $1::text::uuid",
        &[&user_id, &email],
    )
    .await
    .expect("plant an address");
    sess.commit().await.expect("commit");

    let signed_in = post(
        &app,
        "/auth/v1/token?grant_type=password",
        serde_json::json!({"phone": phone, "password": "correct horse battery"}),
    )
    .await;
    assert_eq!(signed_in.status, StatusCode::OK, "{}", signed_in.body);

    let trail = trail(&pool, &user_id).await;
    // The last one, which is the grant just made. The login the signup
    // wrote came before the address was planted and would name the
    // number whichever way round the coalesce ran.
    let login = trail
        .iter()
        .rfind(|e| e.action == "login")
        .expect("a login was written");
    assert_eq!(login.actor_username, phone);
    assert_ne!(login.actor_username, email);

    wipe_phone(&pool, &phone).await;
}
