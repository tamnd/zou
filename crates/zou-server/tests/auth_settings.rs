//! What a client learns about a project before it asks it for anything,
//! and the envelope every refusal leaves in.
//!
//! Two surfaces that look unrelated and are not. A sign in screen reads
//! /settings to know what to draw, and then reads the error bodies to
//! know what went wrong, and both of them are contracts a supabase
//! client already has code branching on. Getting either shape wrong
//! breaks an app that was never changed.
//!
//! Most of this needs no database at all, because a project answering
//! what it is configured for and a refusal being reshaped on the way out
//! both happen without a query. The ones that do need one are gated on
//! ZOU_PG_TEST_DSN like the other live suites and skip when it is unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test auth_settings

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use std::sync::Arc;
use tower::ServiceExt;
use zou_server::{Config, jwt, router, sms};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

/// The header a client sends to ask for the newer error shape.
const VERSION: &str = "x-supabase-api-version";

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

/// A project with nothing configured, which is what /settings has to
/// describe honestly. No database either, because none of the answers
/// under test come from one.
fn base() -> Config {
    Config {
        jwt_secret: SECRET.to_vec(),
        external_url: Some("https://zou.test".to_string()),
        ..Config::default()
    }
}

/// The same project with a database behind it, for the refusals that
/// are decided after the pool is looked at.
fn live(dsn: &str) -> Config {
    Config {
        pg: Some(dsn.to_string()),
        ..base()
    }
}

fn app_of(cfg: Config) -> axum::Router {
    router(cfg).expect("router builds")
}

fn app() -> axum::Router {
    app_of(base())
}

fn anon_key() -> String {
    jwt::mint(&jwt::key_claims("anon"), SECRET)
}

/// A GET with a key on it, which is every read in this file.
fn get(path: &str) -> Request<Body> {
    Request::builder()
        .uri(path)
        .header("apikey", anon_key())
        .body(Body::empty())
        .unwrap()
}

/// A POST with a json body, the shape every refusal here is provoked
/// with. `version` is the api version header when the test is asking for
/// one, and nothing when it is not.
fn post(path: &str, body: serde_json::Value, version: Option<&str>) -> Request<Body> {
    let mut req = Request::builder()
        .method("POST")
        .uri(path)
        .header("apikey", anon_key())
        .header("content-type", "application/json");
    if let Some(v) = version {
        req = req.header(VERSION, v);
    }
    req.body(Body::from(body.to_string())).unwrap()
}

struct Answer {
    status: StatusCode,
    /// What came back in the api version header, which is only ever set
    /// when the newer shape was granted.
    version: Option<String>,
    /// The machine readable code from the header rather than the body,
    /// which is where the newer clients read it from.
    header_code: Option<String>,
    request_id: Option<String>,
    body: serde_json::Value,
}

async fn answer(res: axum::response::Response) -> Answer {
    let status = res.status();
    let head = |name: &str| {
        res.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let (version, header_code, request_id) =
        (head(VERSION), head("x-sb-error-code"), head("x-request-id"));
    let bytes = to_bytes(res.into_body(), 1 << 20).await.expect("a body");
    Answer {
        status,
        version,
        header_code,
        request_id,
        body: serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    }
}

async fn ask(app: &axum::Router, req: Request<Body>) -> Answer {
    answer(app.clone().oneshot(req).await.expect("router answers")).await
}

/// The status, the code and the sentence, in the original shape.
fn refusal(a: &Answer) -> (u16, &str, &str) {
    (
        a.status.as_u16(),
        a.body["error_code"].as_str().unwrap_or("<none>"),
        a.body["msg"].as_str().unwrap_or("<none>"),
    )
}

// ---------------------------------------------------------------- settings

#[tokio::test]
async fn a_fresh_project_signs_people_in_by_address_and_nothing_else() {
    let a = ask(&app(), get("/auth/v1/settings")).await;
    assert_eq!(a.status, StatusCode::OK);
    assert_eq!(a.body["external"]["email"], true, "the one way in there is");
    assert_eq!(a.body["external"]["phone"], false);
    assert_eq!(a.body["external"]["anonymous_users"], false);
    assert_eq!(a.body["external"]["google"], false);
    assert_eq!(a.body["disable_signup"], false);
    assert_eq!(a.body["mailer_autoconfirm"], false);
    assert_eq!(a.body["phone_autoconfirm"], false);
    assert_eq!(
        a.body["sms_provider"], "",
        "nothing carries the codes, so there is no provider to name"
    );
    assert_eq!(a.body["saml_enabled"], false);
    assert_eq!(a.body["saml_private_key_next_configured"], false);
    assert_eq!(a.body["passkeys_enabled"], false);
}

#[tokio::test]
async fn the_providers_a_project_configured_are_the_ones_it_offers() {
    let mut oauth = zou_server::oauth::Providers::default();
    for name in ["google", "github"] {
        let mut provider = zou_server::oauth::Provider::named(name).expect("a provider zou knows");
        provider.client_id = "id".to_string();
        provider.secret = "secret".to_string();
        oauth.insert(provider);
    }
    let cfg = Config { oauth, ..base() };
    let a = ask(&app_of(cfg), get("/auth/v1/settings")).await;
    assert_eq!(a.body["external"]["google"], true);
    assert_eq!(a.body["external"]["github"], true);
    assert_eq!(
        a.body["external"]["apple"], false,
        "a provider this project has no keys for is not one it can offer"
    );
}

#[tokio::test]
async fn a_project_that_texts_says_which_provider_carries_the_codes() {
    let cfg = Config {
        phone_enabled: true,
        texter: Some(Arc::new(sms::Twilio::new("AC123", "token", "MG123"))),
        sms: sms::Settings {
            autoconfirm: true,
            ..Default::default()
        },
        mailer_autoconfirm: true,
        ..base()
    };
    let a = ask(&app_of(cfg), get("/auth/v1/settings")).await;
    assert_eq!(a.body["external"]["phone"], true);
    assert_eq!(a.body["sms_provider"], "twilio");
    assert_eq!(a.body["phone_autoconfirm"], true);
    assert_eq!(a.body["mailer_autoconfirm"], true);
}

#[tokio::test]
async fn a_project_that_is_closed_says_so_before_a_signup_screen_is_drawn() {
    let cfg = Config {
        disable_signup: true,
        email_enabled: false,
        anonymous_users: true,
        ..base()
    };
    let a = ask(&app_of(cfg), get("/auth/v1/settings")).await;
    assert_eq!(a.body["disable_signup"], true);
    assert_eq!(a.body["external"]["email"], false);
    assert_eq!(a.body["external"]["anonymous_users"], true);
}

#[tokio::test]
async fn a_provider_nobody_serves_says_false_rather_than_going_missing() {
    // A client reading a field that is not there cannot tell not
    // offered from not known, so every name upstream publishes is
    // published here even though most of them are years away.
    let a = ask(&app(), get("/auth/v1/settings")).await;
    let external = a.body["external"].as_object().expect("an object");
    for name in [
        "anonymous_users",
        "apple",
        "azure",
        "bitbucket",
        "discord",
        "facebook",
        "snapchat",
        "figma",
        "fly",
        "github",
        "gitlab",
        "google",
        "keycloak",
        "kakao",
        "linkedin",
        "linkedin_oidc",
        "notion",
        "spotify",
        "slack",
        "slack_oidc",
        "workos",
        "twitch",
        "twitter",
        "email",
        "phone",
        "zoom",
    ] {
        assert!(
            external
                .get(name)
                .is_some_and(serde_json::Value::is_boolean),
            "{name} is missing from the settings a client reads"
        );
    }
    assert_eq!(
        external.len(),
        26,
        "a name here that upstream does not have is a name a client will not read"
    );
}

#[tokio::test]
async fn settings_is_behind_the_gate_like_everything_else() {
    let req = Request::builder()
        .uri("/auth/v1/settings")
        .body(Body::empty())
        .unwrap();
    let a = answer(app().oneshot(req).await.expect("router answers")).await;
    assert_eq!(a.status, StatusCode::UNAUTHORIZED);
    assert_eq!(a.body["message"], "No API key found in request");
}

#[tokio::test]
async fn health_names_gotrue_because_that_is_what_is_being_reimplemented() {
    let a = ask(&app(), get("/auth/v1/health")).await;
    assert_eq!(a.status, StatusCode::OK);
    assert_eq!(a.body["name"], "GoTrue");
    assert_eq!(
        a.body["description"],
        "GoTrue is a user registration and authentication API"
    );
    let version = a.body["version"].as_str().expect("a version");
    assert!(
        version.starts_with("zou-"),
        "the name is upstream's and the version is this project's, {version}"
    );
}

// ------------------------------------------------------------- the envelope

/// A refusal that needs no database: a grant nobody serves is turned
/// away before anything is looked up.
fn bad_grant(version: Option<&str>) -> Request<Body> {
    post(
        "/auth/v1/token?grant_type=telepathy",
        serde_json::json!({}),
        version,
    )
}

#[tokio::test]
async fn without_the_version_header_a_refusal_is_the_shape_it_always_was() {
    let a = ask(&app(), bad_grant(None)).await;
    assert_eq!(
        refusal(&a),
        (400, "invalid_credentials", "unsupported_grant_type")
    );
    assert_eq!(
        a.body["code"], 400,
        "the original shape repeats the http status in the body"
    );
    assert_eq!(a.header_code.as_deref(), Some("invalid_credentials"));
    assert_eq!(
        a.version, None,
        "nothing was asked for, so nothing is granted back"
    );
    assert!(a.body.get("message").is_none());
}

#[tokio::test]
async fn asking_for_the_2024_version_gets_the_newer_shape_and_the_version_back() {
    let a = ask(&app(), bad_grant(Some("2024-01-01"))).await;
    assert_eq!(a.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        a.body["code"], "invalid_credentials",
        "code is the machine readable one now, not the status again"
    );
    assert_eq!(a.body["message"], "unsupported_grant_type");
    assert!(
        a.body.get("msg").is_none() && a.body.get("error_code").is_none(),
        "the old field names are gone rather than sitting alongside the new ones"
    );
    assert_eq!(a.version.as_deref(), Some("2024-01-01"));
    assert_eq!(
        a.header_code.as_deref(),
        Some("invalid_credentials"),
        "the header carries the code whichever shape the body is in"
    );
}

#[tokio::test]
async fn a_version_later_than_the_one_there_is_gets_that_one() {
    // Upstream rounds any date down to the newest version it knows, so
    // a client pinned to next year keeps working.
    let a = ask(&app(), bad_grant(Some("2031-06-30"))).await;
    assert_eq!(a.body["code"], "invalid_credentials");
    assert_eq!(a.version.as_deref(), Some("2024-01-01"));
}

#[tokio::test]
async fn a_version_older_than_the_newer_shape_is_the_older_shape() {
    let a = ask(&app(), bad_grant(Some("2023-12-31"))).await;
    assert_eq!(
        refusal(&a),
        (400, "invalid_credentials", "unsupported_grant_type")
    );
    assert_eq!(
        a.version, None,
        "the version that was always there is not one to echo"
    );
}

#[tokio::test]
async fn a_header_that_is_not_a_date_is_the_version_that_was_always_there() {
    // Upstream cannot parse these either and falls back rather than
    // refusing, so a client sending nonsense gets an answer it can read.
    for header in [
        "banana",
        "",
        "2024-1-1",
        "2024-02-31",
        "2024-13-01",
        "2024/01/01",
        "2024-01-01T00:00:00Z",
    ] {
        let a = ask(&app(), bad_grant(Some(header))).await;
        assert_eq!(
            refusal(&a),
            (400, "invalid_credentials", "unsupported_grant_type"),
            "{header:?} should not have bought the newer shape"
        );
        assert_eq!(a.version, None, "{header:?} should not have been echoed");
    }
}

#[tokio::test]
async fn an_answer_that_worked_is_left_alone_whatever_was_asked_for() {
    let req = Request::builder()
        .uri("/auth/v1/settings")
        .header("apikey", anon_key())
        .header(VERSION, "2024-01-01")
        .body(Body::empty())
        .unwrap();
    let a = ask(&app(), req).await;
    assert_eq!(a.status, StatusCode::OK);
    assert_eq!(a.body["external"]["email"], true);
    assert_eq!(
        a.version, None,
        "the version is echoed on a refusal, which this is not"
    );
}

#[tokio::test]
async fn a_failure_of_this_servers_own_carries_the_id_to_search_the_logs_for() {
    // No database configured, so a signup is a 500 rather than a
    // refusal, which is exactly the case upstream fills the id in for.
    let a = ask(
        &app(),
        post(
            "/auth/v1/signup",
            serde_json::json!({"email": "someone@zou.test", "password": "correct horse battery"}),
            None,
        ),
    )
    .await;
    assert_eq!(a.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(a.body["error_code"], "unexpected_failure");
    let id = a.request_id.expect("every answer carries a request id");
    assert_eq!(
        a.body["error_id"], id,
        "the id in the body is the id in the header, or there is nothing to search for"
    );
}

#[tokio::test]
async fn a_refusal_that_is_the_callers_fault_carries_no_id() {
    let a = ask(&app(), bad_grant(None)).await;
    assert!(
        a.body.get("error_id").is_none(),
        "there is nothing in the logs to go and read about a bad request"
    );
}

#[tokio::test]
async fn the_newer_shape_of_a_failure_is_two_fields_and_no_more() {
    let a = ask(
        &app(),
        post(
            "/auth/v1/signup",
            serde_json::json!({"email": "someone@zou.test", "password": "correct horse battery"}),
            Some("2024-01-01"),
        ),
    )
    .await;
    assert_eq!(a.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(a.body["code"], "unexpected_failure");
    assert_eq!(
        a.body
            .as_object()
            .expect("an object")
            .keys()
            .collect::<Vec<_>>(),
        vec!["code", "message"],
        "the id is dropped with the rest of the old shape"
    );
}

#[tokio::test]
async fn a_refusal_from_outside_the_auth_surface_is_not_reshaped() {
    // The REST surface answers in PostgREST's shape and knows nothing
    // about GoTrue's api versions, so a header meant for one must not
    // rewrite the other.
    let req = Request::builder()
        .uri("/rest/v1/todos?select=*")
        .header("apikey", anon_key())
        .header(VERSION, "2024-01-01")
        .body(Body::empty())
        .unwrap();
    let a = ask(&app(), req).await;
    assert_eq!(a.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(a.body["code"], "PGRST000");
    assert_eq!(
        a.body["message"],
        "Database connection error. Retrying the connection."
    );
    assert_eq!(a.version, None);
}

#[tokio::test]
async fn the_gates_own_refusal_is_the_edges_and_stays_that_way() {
    // A missing apikey never reaches GoTrue in a real deployment, it is
    // turned away at the gateway, so it is not a GoTrue error body and
    // is not treated as one.
    let req = Request::builder()
        .method("POST")
        .uri("/auth/v1/token?grant_type=password")
        .header(VERSION, "2024-01-01")
        .body(Body::empty())
        .unwrap();
    let a = ask(&app(), req).await;
    assert_eq!(a.status, StatusCode::UNAUTHORIZED);
    assert_eq!(a.body["message"], "No API key found in request");
    assert_eq!(a.version, None);
}

// -------------------------------------------------- the two settings that bite

#[tokio::test]
async fn a_closed_project_refuses_a_signup_whichever_medium_it_names() {
    let Some(dsn) = dsn() else { return };
    let cfg = Config {
        disable_signup: true,
        phone_enabled: true,
        ..live(&dsn)
    };
    let app = app_of(cfg);
    for body in [
        serde_json::json!({"email": "nobody@zou.test", "password": "correct horse battery"}),
        serde_json::json!({"phone": "15550000001", "password": "correct horse battery"}),
    ] {
        let a = ask(&app, post("/auth/v1/signup", body, None)).await;
        assert_eq!(
            refusal(&a),
            (
                422,
                "signup_disabled",
                "Signups not allowed for this instance"
            )
        );
    }
}

#[tokio::test]
async fn the_password_is_not_even_graded_on_a_project_nobody_can_join() {
    // Upstream asks whether the project is closed before it asks
    // anything about the request, so a weak password on a closed
    // project is turned away for the closure rather than for the
    // password. Two settings, one answer, and it is the useful one.
    let Some(dsn) = dsn() else { return };
    let cfg = Config {
        disable_signup: true,
        ..live(&dsn)
    };
    let a = ask(
        &app_of(cfg),
        post(
            "/auth/v1/signup",
            serde_json::json!({"email": "nobody@zou.test", "password": "x"}),
            None,
        ),
    )
    .await;
    assert_eq!(a.body["error_code"], "signup_disabled");
}

#[tokio::test]
async fn anonymous_sign_in_being_off_is_the_answer_even_when_signups_are_off_too() {
    // Both settings refuse this request and upstream answers with the
    // nearer of the two, because that is the one an operator turns on
    // to fix it.
    let Some(dsn) = dsn() else { return };
    let cfg = Config {
        disable_signup: true,
        anonymous_users: false,
        ..live(&dsn)
    };
    let a = ask(
        &app_of(cfg),
        post("/auth/v1/signup", serde_json::json!({}), None),
    )
    .await;
    assert_eq!(
        refusal(&a),
        (
            422,
            "anonymous_provider_disabled",
            "Anonymous sign-ins are disabled"
        )
    );
}

#[tokio::test]
async fn a_closed_project_refuses_an_anonymous_sign_in_it_would_otherwise_allow() {
    let Some(dsn) = dsn() else { return };
    let cfg = Config {
        disable_signup: true,
        anonymous_users: true,
        ..live(&dsn)
    };
    let a = ask(
        &app_of(cfg),
        post("/auth/v1/signup", serde_json::json!({}), None),
    )
    .await;
    assert_eq!(
        refusal(&a),
        (
            422,
            "signup_disabled",
            "Signups not allowed for this instance"
        )
    );
}

#[tokio::test]
async fn a_project_with_no_email_refuses_every_way_in_by_address() {
    // Five endpoints, four sentences, two statuses, all of them
    // upstream's. A client branching on any one of them keeps working.
    let Some(dsn) = dsn() else { return };
    let cfg = Config {
        email_enabled: false,
        ..live(&dsn)
    };
    let app = app_of(cfg);
    let address = serde_json::json!({"email": "nobody@zou.test"});
    let cases: Vec<(&str, serde_json::Value, (u16, &str, &str))> = vec![
        (
            "/auth/v1/signup",
            serde_json::json!({"email": "nobody@zou.test", "password": "correct horse battery"}),
            (400, "email_provider_disabled", "Email signups are disabled"),
        ),
        (
            "/auth/v1/recover",
            address.clone(),
            (400, "email_provider_disabled", "Email logins are disabled"),
        ),
        (
            "/auth/v1/magiclink",
            address.clone(),
            (422, "email_provider_disabled", "Email logins are disabled"),
        ),
        (
            "/auth/v1/otp",
            address.clone(),
            (422, "email_provider_disabled", "Email logins are disabled"),
        ),
        (
            "/auth/v1/resend",
            serde_json::json!({"type": "signup", "email": "nobody@zou.test"}),
            (400, "email_provider_disabled", "Email logins are disabled"),
        ),
        (
            "/auth/v1/token?grant_type=password",
            serde_json::json!({"email": "nobody@zou.test", "password": "correct horse battery"}),
            (422, "email_provider_disabled", "Email logins are disabled"),
        ),
    ];
    for (path, body, want) in cases {
        let a = ask(&app, post(path, body, None)).await;
        assert_eq!(refusal(&a), want, "{path}");
    }
}

#[tokio::test]
async fn recover_says_why_before_it_has_read_anything() {
    // Upstream guards recover in the route rather than in the handler,
    // so a body it could not have parsed still hears the real reason.
    let Some(dsn) = dsn() else { return };
    let cfg = Config {
        email_enabled: false,
        ..live(&dsn)
    };
    let req = Request::builder()
        .method("POST")
        .uri("/auth/v1/recover")
        .header("apikey", anon_key())
        .header("content-type", "application/json")
        .body(Body::from("{not json at all"))
        .unwrap();
    let a = ask(&app_of(cfg), req).await;
    assert_eq!(
        refusal(&a),
        (400, "email_provider_disabled", "Email logins are disabled")
    );
}

#[tokio::test]
async fn the_otp_endpoint_asks_who_has_an_account_before_it_asks_about_the_medium() {
    // create_user: false is judged before either medium is looked at
    // upstream, so an address nobody holds is refused for that rather
    // than for the provider being off.
    let Some(dsn) = dsn() else { return };
    let cfg = Config {
        email_enabled: false,
        ..live(&dsn)
    };
    let a = ask(
        &app_of(cfg),
        post(
            "/auth/v1/otp",
            serde_json::json!({"email": "nobody-at-all@zou.test", "create_user": false}),
            None,
        ),
    )
    .await;
    assert_eq!(
        refusal(&a),
        (422, "otp_disabled", "Signups not allowed for otp")
    );
}

#[tokio::test]
async fn a_weak_password_keeps_its_reasons_under_the_newer_shape() {
    // The one refusal with more to say than a sentence, and the reason
    // the rewrite carries a payload across rather than dropping to two
    // fields.
    let Some(dsn) = dsn() else { return };
    let app = app_of(live(&dsn));
    let body = serde_json::json!({"email": "weak@zou.test", "password": "short"});
    let old = ask(&app, post("/auth/v1/signup", body.clone(), None)).await;
    assert_eq!(old.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(old.body["error_code"], "weak_password");
    let reasons = old.body["weak_password"]["reasons"].clone();
    assert_eq!(reasons, serde_json::json!(["length"]));

    let new = ask(&app, post("/auth/v1/signup", body, Some("2024-01-01"))).await;
    assert_eq!(new.body["code"], "weak_password");
    assert_eq!(new.body["message"], old.body["msg"]);
    assert_eq!(
        new.body["weak_password"]["reasons"], reasons,
        "a client that asked for the newer shape still gets told which rule was broken"
    );
    assert_eq!(new.version.as_deref(), Some("2024-01-01"));
}
