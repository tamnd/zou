//! The external providers end to end, against a live postgres, with a
//! fake standing in for Google, Github and Apple.
//!
//! Nothing here plants a row by hand. Every test starts at /authorize
//! the way a browser does, or at /user/identities/authorize the way a
//! signed in client does, follows the redirect to the provider, comes
//! back to /callback with a code, and then either reads the session out
//! of the fragment or trades the code for one. The fake is the only
//! thing stubbed, and it answers the documents a provider serves: the
//! token exchange, and the profile for the two providers that have one.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test auth_oauth

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use axum::response::Response;
use tower::ServiceExt;
use zou_server::sql::Pool;
use zou_server::{Config, jwt, oauth, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

/// Where a redirect lands when the request does not ask for anywhere.
const SITE: &str = "https://app.zou.test";

const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const USER_URL: &str = "https://www.googleapis.com/oauth2/v3/userinfo";
const GH_TOKEN_URL: &str = "https://github.com/login/oauth/access_token";
const GH_USER_URL: &str = "https://api.github.com/user";
const GH_EMAIL_URL: &str = "https://api.github.com/user/emails";
const APPLE_TOKEN_URL: &str = "https://appleid.apple.com/auth/token";

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

/// A provider that answers from a script and writes down what it was
/// asked, so a test can assert on the exchange as well as on what came
/// out of it.
#[derive(Default)]
struct Fake {
    answers: Mutex<HashMap<String, (u16, String)>>,
    asked: Mutex<Vec<String>>,
}

impl Fake {
    fn say(&self, url: &str, status: u16, body: &str) {
        self.answers
            .lock()
            .unwrap()
            .insert(url.to_string(), (status, body.to_string()));
    }

    /// What Google answers for somebody.
    fn google(&self, sub: &str, email: &str, verified: bool) {
        self.say(
            TOKEN_URL,
            200,
            r#"{"access_token":"provider-at","refresh_token":"provider-rt"}"#,
        );
        self.say(
            USER_URL,
            200,
            &serde_json::json!({
                "sub": sub,
                "email": email,
                "email_verified": verified,
                "name": "Some One",
                "picture": "https://lh3.zou.test/photo",
            })
            .to_string(),
        );
    }

    /// What Github answers, which is two documents rather than one.
    fn github(&self, id: i64, login: &str, email: &str, verified: bool) {
        self.say(GH_TOKEN_URL, 200, r#"{"access_token":"gh-at"}"#);
        self.say(
            GH_USER_URL,
            200,
            &serde_json::json!({"id": id, "login": login, "name": "Mona"}).to_string(),
        );
        self.say(
            GH_EMAIL_URL,
            200,
            &serde_json::json!([{"email": email, "primary": true, "verified": verified}])
                .to_string(),
        );
    }

    /// What Apple answers, which is one document and no profile
    /// endpoint at all: everything it will say is in the id token.
    fn apple(&self, sub: &str, email: &str, verified: bool) {
        self.say(
            APPLE_TOKEN_URL,
            200,
            &serde_json::json!({
                "access_token": "apple-at",
                "id_token": id_token(serde_json::json!({
                    "sub": sub,
                    "email": email,
                    // Apple sends this as the string rather than the
                    // bool about half the time, so the string is what
                    // the test sends.
                    "email_verified": match verified {
                        true => "true",
                        false => "false",
                    },
                    "is_private_email": "false",
                })),
            })
            .to_string(),
        );
    }

    fn asked(&self) -> Vec<String> {
        self.asked.lock().unwrap().clone()
    }
}

/// A jwt with a signature that is not one. Nothing checks it: an id
/// token that arrives on a connection this process opened to the token
/// endpoint with the client credentials on it is the case OIDC 3.1.3.7
/// exempts, and one that arrives any other way is never read.
fn id_token(claims: serde_json::Value) -> String {
    use base64ct::Encoding;
    let head = base64ct::Base64UrlUnpadded::encode_string(br#"{"alg":"RS256"}"#);
    let body = base64ct::Base64UrlUnpadded::encode_string(claims.to_string().as_bytes());
    format!("{head}.{body}.not-a-signature")
}

impl oauth::Http for Fake {
    fn call(&self, ask: &oauth::Ask) -> Result<oauth::Answer, String> {
        self.asked.lock().unwrap().push(ask.url.clone());
        match self.answers.lock().unwrap().get(&ask.url) {
            Some((status, body)) => Ok(oauth::Answer {
                status: *status,
                body: body.clone(),
            }),
            None => Err(format!("nothing scripted for {}", ask.url)),
        }
    }
}

fn providers() -> oauth::Providers {
    let mut out = oauth::Providers::default();
    for name in ["google", "github", "apple"] {
        let mut provider = oauth::Provider::named(name).expect("a provider zou knows");
        provider.client_id = format!("{name}-client");
        provider.secret = format!("{name}-secret");
        out.insert(provider);
    }
    out
}

/// A project with both providers configured and the fake answering
/// them. `autoconfirm` is the one knob these tests disagree about: it
/// decides whether an address the provider will not vouch for is taken
/// at face value.
fn app(dsn: &str, autoconfirm: bool) -> (axum::Router, Arc<Fake>) {
    project(dsn, autoconfirm, true, false, false)
}

/// The same project with manual linking turned on, which is the one
/// knob the linking endpoints are behind.
fn linking(dsn: &str) -> (axum::Router, Arc<Fake>) {
    project(dsn, true, true, true, false)
}

/// And the same project with signups off, which is the one knob that
/// tells signing in through a provider apart from signing up through
/// one.
fn closed(dsn: &str) -> (axum::Router, Arc<Fake>) {
    project(dsn, true, true, false, true)
}

fn project(
    dsn: &str,
    autoconfirm: bool,
    secure_change: bool,
    manual_linking: bool,
    disable_signup: bool,
) -> (axum::Router, Arc<Fake>) {
    let fake = Arc::new(Fake::default());
    let cfg = Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        external_url: Some("https://zou.test".to_string()),
        site_url: Some(SITE.to_string()),
        mailer_autoconfirm: autoconfirm,
        secure_email_change: secure_change,
        manual_linking,
        disable_signup,
        oauth: providers(),
        http: Some(Arc::clone(&fake) as Arc<dyn oauth::Http>),
        ..Config::default()
    };
    (router(cfg).expect("router builds"), fake)
}

fn anon_key() -> String {
    jwt::mint(&jwt::key_claims("anon"), SECRET)
}

fn service_key() -> String {
    jwt::mint(&jwt::key_claims("service_role"), SECRET)
}

/// A navigation: no apikey, because a browser following a provider's
/// redirect has none to send.
async fn go(app: &axum::Router, uri: &str) -> Response {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(req).await.expect("router answers")
}

/// The same navigation, from somewhere. A browser coming back from a
/// provider is the only caller in this file whose address the server
/// writes down, so this is the only place that needs one.
async fn go_from(app: &axum::Router, uri: &str, from: &str) -> Response {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .header("x-forwarded-for", from)
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(req).await.expect("router answers")
}

async fn post(app: &axum::Router, path: &str, body: serde_json::Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .header("apikey", anon_key())
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let res = app.clone().oneshot(req).await.expect("router answers");
    let status = res.status();
    (status, json(res).await)
}

async fn as_user(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: serde_json::Value,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("apikey", anon_key())
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let res = app.clone().oneshot(req).await.expect("router answers");
    let status = res.status();
    (status, json(res).await)
}

/// The callback as Apple sends it: a form the browser posts, with no
/// apikey and nothing in the url at all.
async fn post_form(app: &axum::Router, path: &str, body: &str) -> Response {
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(body.to_string()))
        .unwrap();
    app.clone().oneshot(req).await.expect("router answers")
}

/// A followed link, which carries no apikey either.
async fn follow(app: &axum::Router, link: &str) -> Response {
    let (_, query) = link.split_once('?').expect("a link carries its token");
    go(app, &format!("/auth/v1/verify?{query}")).await
}

/// What the dev inbox is holding for one address. Tests in this file
/// run at once against the one inbox, so nobody reads the whole of it.
async fn inbox_for(app: &axum::Router, address: &str) -> Vec<Value> {
    let req = Request::builder()
        .method("GET")
        .uri("/dev/inbox")
        .header("apikey", service_key())
        .body(Body::empty())
        .unwrap();
    let body = json(app.clone().oneshot(req).await.expect("router answers")).await;
    body["messages"]
        .as_array()
        .expect("an array of messages")
        .iter()
        .filter(|m| m["to"] == address)
        .cloned()
        .collect()
}

type Value = serde_json::Value;

async fn json(res: Response) -> Value {
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("not json: {}", String::from_utf8_lossy(&bytes)))
}

/// What a token says once this project has checked its signature.
fn claims(token: &str) -> Value {
    jwt::verify(token, SECRET)
        .expect("signed by this project")
        .claims
}

fn location(res: &Response) -> String {
    res.headers()
        .get("location")
        .unwrap_or_else(|| panic!("no location on a {}", res.status()))
        .to_str()
        .expect("a location is ascii")
        .to_string()
}

/// The parameters of a url, from the query and the fragment both,
/// which is where these answers put things.
fn parts(url: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let (head, fragment) = match url.split_once('#') {
        Some((head, fragment)) => (head, fragment),
        None => (url, ""),
    };
    let query = head.split_once('?').map(|(_, q)| q).unwrap_or("");
    for piece in [query, fragment] {
        for pair in piece.split('&').filter(|p| !p.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            out.insert(unescape(key), unescape(value));
        }
    }
    out
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

/// The s256 challenge for a verifier, computed the way a client
/// computes it rather than the way the server checks it.
fn challenge_for(verifier: &str) -> String {
    use base64ct::Encoding;
    use sha2::Digest;
    base64ct::Base64UrlUnpadded::encode_string(&sha2::Sha256::digest(verifier.as_bytes()))
}

/// Start a flow and hand back the state the provider was given.
async fn start(app: &axum::Router, query: &str) -> String {
    let res = go(app, &format!("/auth/v1/authorize?{query}")).await;
    assert_eq!(res.status(), StatusCode::FOUND, "authorize should redirect");
    let sent = location(&res);
    parts(&sent)
        .remove("state")
        .unwrap_or_else(|| panic!("no state in {sent}"))
}

/// Start a linking flow as somebody, and hand back the state. The url
/// is asked for rather than followed, because a client that is already
/// signed in is making a fetch and cannot be redirected out of one.
async fn start_link(app: &axum::Router, token: &str, query: &str) -> String {
    let (status, body) = as_user(
        app,
        "GET",
        &format!("/auth/v1/user/identities/authorize?{query}&skip_http_redirect=true"),
        token,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let sent = body["url"].as_str().expect("a url to open").to_string();
    parts(&sent)
        .remove("state")
        .unwrap_or_else(|| panic!("no state in {sent}"))
}

/// Somebody with an account and a session, made the plain way.
async fn signed_up(app: &axum::Router, email: &str) -> (String, String) {
    let (status, body) = post(
        app,
        "/auth/v1/signup",
        serde_json::json!({"email": email, "password": "a-long-enough-password"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    (
        body["user"]["id"].as_str().expect("an id").to_string(),
        body["access_token"]
            .as_str()
            .expect("a session, this project autoconfirms")
            .to_string(),
    )
}

async fn pool(dsn: &str) -> Pool {
    Pool::new(dsn, 4).expect("pool")
}

async fn wipe(pool: &Pool, emails: &[&str]) {
    let sess = pool.unscoped().await.expect("connect");
    // The identities go first: nothing here declares a cascade, and a
    // leftover identity would decide the next test's linking for it.
    for email in emails {
        sess.execute(
            "delete from auth.identities
              where email = $1
                 or user_id in (select id from auth.users where email = $1)",
            &[email],
        )
        .await
        .expect("clear identities");
        sess.execute("delete from auth.users where email = $1", &[email])
            .await
            .expect("clear users");
    }
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

#[tokio::test]
async fn a_provider_nobody_configured_is_refused_by_name() {
    let Some(dsn) = dsn() else { return };
    let (app, _) = app(&dsn, true);

    let res = go(&app, "/auth/v1/authorize?provider=myspace").await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = json(res).await;
    assert_eq!(body["error_code"], "validation_failed");
    assert_eq!(
        body["msg"], "Unsupported provider: Provider myspace could not be found",
        "a name that is not a provider at all"
    );

    // A provider zou knows but this project did not configure says
    // something different, because the two are fixed differently.
    let bare = router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.clone()),
        ..Config::default()
    })
    .expect("router builds");
    let res = go(&bare, "/auth/v1/authorize?provider=google").await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json(res).await["msg"],
        "Unsupported provider: provider is not enabled"
    );
}

#[tokio::test]
async fn the_authorize_redirect_carries_the_flow_it_just_wrote_down() {
    let Some(dsn) = dsn() else { return };
    let (app, _) = app(&dsn, true);
    let pool = pool(&dsn).await;
    let verifier = "a-verifier-that-is-long-enough-to-be-worth-something";
    let challenge = challenge_for(verifier);

    let res = go(
        &app,
        &format!(
            "/auth/v1/authorize?provider=google&scopes=drive.readonly\
             &code_challenge={challenge}&code_challenge_method=S256\
             &redirect_to=https%3A%2F%2Fapp.zou.test%2Fwelcome"
        ),
    )
    .await;
    assert_eq!(res.status(), StatusCode::FOUND);
    let sent = location(&res);
    assert!(
        sent.starts_with("https://accounts.google.com/o/oauth2/v2/auth?"),
        "{sent}"
    );
    let query = parts(&sent);
    assert_eq!(query["client_id"], "google-client");
    assert_eq!(query["response_type"], "code");
    assert_eq!(
        query["redirect_uri"], "https://zou.test/auth/v1/callback",
        "the provider is told to come back to this server, not to the app"
    );
    assert_eq!(
        query["scope"], "email profile drive.readonly",
        "what the caller asked for on top of what the provider needs"
    );

    // The state is a row, and the row is what the callback trusts
    // rather than anything the callback itself carries.
    let state = &query["state"];
    let (stored, method, referrer, has_code): (String, String, String, bool) = {
        let sess = pool.unscoped().await.expect("connect");
        let rows = sess
            .query(
                "select code_challenge, code_challenge_method::text, referrer,
                        auth_code is not null
                   from auth.flow_state where id = $1::text::uuid",
                &[state],
            )
            .await
            .expect("the flow was written");
        let row = &rows[0];
        (row.get(0), row.get(1), row.get(2), row.get(3))
    };
    assert_eq!(stored, challenge);
    assert_eq!(method, "s256", "stored lowercased, the way upstream does");
    assert_eq!(
        referrer, "https://app.zou.test/welcome",
        "where the redirect_to asked to land, checked against the site url"
    );
    assert!(has_code, "a pkce flow has a code waiting for its verifier");
}

#[tokio::test]
async fn a_pkce_flow_signs_in_and_the_code_is_good_once() {
    let Some(dsn) = dsn() else { return };
    let (app, fake) = app(&dsn, false);
    let pool = pool(&dsn).await;
    let email = "pkce@zou.test";
    wipe(&pool, &[email]).await;
    fake.google("google-sub-1", email, true);

    let verifier = "verifier-for-the-pkce-round-trip-which-is-long";
    let state = start(
        &app,
        &format!(
            "provider=google&code_challenge={}&code_challenge_method=s256",
            challenge_for(verifier)
        ),
    )
    .await;

    let res = go(
        &app,
        &format!("/auth/v1/callback?state={state}&code=from-google"),
    )
    .await;
    assert_eq!(res.status(), StatusCode::FOUND);
    let landed = location(&res);
    assert!(landed.starts_with(SITE), "{landed}");
    let query = landed.split('#').next().expect("a url");
    assert!(
        query.contains("code="),
        "the code rides in the query, where a server side client reads it: {landed}"
    );
    let code = parts(&landed)
        .remove("code")
        .unwrap_or_else(|| panic!("no code in {landed}"));
    assert!(
        !landed.contains("access_token"),
        "a pkce callback hands out nothing until the verifier arrives: {landed}"
    );

    // The provider was asked for the two things it is for, and the
    // exchange was a real one.
    assert_eq!(fake.asked(), vec![TOKEN_URL, USER_URL]);

    let (status, body) = post(
        &app,
        "/auth/v1/token?grant_type=pkce",
        serde_json::json!({"auth_code": code, "code_verifier": verifier}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["token_type"], "bearer");
    assert_eq!(body["user"]["email"], email);
    assert_eq!(body["user"]["app_metadata"]["provider"], "google");
    assert_eq!(
        body["user"]["identities"][0]["id"], "google-sub-1",
        "the identity is keyed by the provider's own id"
    );
    assert_eq!(
        body["provider_token"], "provider-at",
        "a client that wants to call google as this person gets something to call it with"
    );
    assert_eq!(body["provider_refresh_token"], "provider-rt");

    let claims = claims(body["access_token"].as_str().expect("a token"));
    assert_eq!(claims["amr"][0]["method"], "oauth");
    assert_eq!(claims["email"], email);

    let confirmed: bool = scalar(
        &pool,
        "select email_confirmed_at is not null from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert!(
        confirmed,
        "google said the address is verified, so nothing else has to say it"
    );

    // The flow row is gone, so the code cannot be traded twice even by
    // whoever holds the verifier.
    let (status, body) = post(
        &app,
        "/auth/v1/token?grant_type=pkce",
        serde_json::json!({"auth_code": code, "code_verifier": verifier}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error_code"], "flow_state_not_found");
    assert_eq!(body["msg"], "invalid flow state, no valid flow state found");
}

#[tokio::test]
async fn the_verifier_is_the_thing_that_proves_it() {
    let Some(dsn) = dsn() else { return };
    let (app, fake) = app(&dsn, false);
    let pool = pool(&dsn).await;
    let email = "pkce-wrong@zou.test";
    wipe(&pool, &[email]).await;
    fake.google("google-sub-wrong", email, true);

    let state = start(
        &app,
        &format!(
            "provider=google&code_challenge={}&code_challenge_method=s256",
            challenge_for("the-real-verifier-which-is-long-enough")
        ),
    )
    .await;
    let landed = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
    let code = parts(&landed).remove("code").expect("a code");

    let (status, body) = post(
        &app,
        "/auth/v1/token?grant_type=pkce",
        serde_json::json!({"auth_code": code, "code_verifier": "not-the-real-verifier"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error_code"], "bad_code_verifier");
    assert_eq!(
        body["msg"],
        "code challenge does not match previously saved code verifier"
    );

    // Refusing does not spend the code, so the client that does hold
    // the verifier can still use it.
    let (status, _) = post(
        &app,
        "/auth/v1/token?grant_type=pkce",
        serde_json::json!({
            "auth_code": code,
            "code_verifier": "the-real-verifier-which-is-long-enough",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // And the request that carries neither is refused before anything
    // is looked up.
    let (status, body) = post(
        &app,
        "/auth/v1/token?grant_type=pkce",
        serde_json::json!({"auth_code": "", "code_verifier": ""}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error_code"], "validation_failed");
    assert_eq!(
        body["msg"],
        "invalid request: both auth code and code verifier should be non-empty"
    );
}

#[tokio::test]
async fn the_implicit_flow_hands_the_session_back_in_the_fragment() {
    let Some(dsn) = dsn() else { return };
    let (app, fake) = app(&dsn, false);
    let pool = pool(&dsn).await;
    let email = "implicit@zou.test";
    wipe(&pool, &[email]).await;
    fake.google("google-sub-implicit", email, true);

    let state = start(&app, "provider=google").await;
    let res = go(
        &app,
        &format!("/auth/v1/callback?state={state}&code=from-google"),
    )
    .await;
    assert_eq!(res.status(), StatusCode::FOUND);
    let landed = location(&res);
    let (_, fragment) = landed.split_once('#').unwrap_or_else(|| panic!("{landed}"));
    let back = parts(&landed);
    assert!(
        !fragment.is_empty() && back.contains_key("access_token"),
        "the whole session comes back where a browser can read it: {landed}"
    );
    assert_eq!(back["token_type"], "bearer");
    assert_eq!(back["provider_token"], "provider-at");
    assert_eq!(back["provider_refresh_token"], "provider-rt");
    assert!(back.contains_key("sb"), "the supabase marker: {landed}");

    let claims = claims(&back["access_token"]);
    assert_eq!(claims["email"], email);
    assert_eq!(claims["amr"][0]["method"], "oauth");

    // The refresh token in the fragment is a working one, which is the
    // only way to know the session behind it is real.
    let (status, body) = post(
        &app,
        "/auth/v1/token?grant_type=refresh_token",
        serde_json::json!({"refresh_token": back["refresh_token"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // An implicit flow keeps nothing: there is no code to trade, so
    // there is nothing left to expire.
    let left: i64 = scalar(
        &pool,
        "select count(*) from auth.flow_state where id = $1::text::uuid",
        &[&state],
    )
    .await;
    assert_eq!(left, 0);
}

#[tokio::test]
async fn signing_in_twice_finds_the_same_account() {
    let Some(dsn) = dsn() else { return };
    let (app, fake) = app(&dsn, false);
    let pool = pool(&dsn).await;
    let email = "repeat@zou.test";
    wipe(&pool, &[email]).await;
    fake.google("google-sub-repeat", email, true);

    let mut ids = Vec::new();
    for _ in 0..2 {
        let state = start(&app, "provider=google").await;
        let landed = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
        let token = parts(&landed).remove("access_token").expect("a token");
        ids.push(claims(&token)["sub"].clone());
    }
    assert_eq!(ids[0], ids[1], "the same google account is the same person");

    let identities: i64 = scalar(
        &pool,
        "select count(*) from auth.identities i
           join auth.users u on u.id = i.user_id where u.email = $1",
        &[&email],
    )
    .await;
    assert_eq!(identities, 1, "and it is one identity, not two");
}

#[tokio::test]
async fn a_verified_address_links_to_the_account_that_already_has_it() {
    let Some(dsn) = dsn() else { return };
    let (app, fake) = app(&dsn, true);
    let pool = pool(&dsn).await;
    let email = "linked@zou.test";
    wipe(&pool, &[email]).await;

    // A plain signup first, confirmed, with a password.
    let (status, body) = post(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"email": email, "password": "a-long-enough-password"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let signed_up = body["user"]["id"].as_str().expect("an id").to_string();

    // Then github, for an address it will vouch for.
    fake.github(4242, "mona", email, true);
    let state = start(&app, "provider=github").await;
    let landed = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
    let token = parts(&landed).remove("access_token").expect("a token");
    let claims = claims(&token);
    assert_eq!(
        claims["sub"], signed_up,
        "a verified address that somebody already holds links rather than duplicates"
    );

    let providers: serde_json::Value = serde_json::from_str(
        &scalar::<String>(
            &pool,
            "select (raw_app_meta_data->'providers')::text from auth.users where email = $1",
            &[&email],
        )
        .await,
    )
    .expect("json");
    assert_eq!(
        providers,
        serde_json::json!(["email", "github"]),
        "both ways in are advertised, in the order they were added"
    );

    // The password still works, because linking took nothing away.
    let (status, body) = post(
        &app,
        "/auth/v1/token?grant_type=password",
        serde_json::json!({"email": email, "password": "a-long-enough-password"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn an_address_the_provider_will_not_vouch_for_takes_over_nothing() {
    let Some(dsn) = dsn() else { return };
    // Autoconfirm off, because with it on the project has said it takes
    // every address at face value and there is nothing left to test.
    let (app, fake) = app(&dsn, false);
    let pool = pool(&dsn).await;
    let email = "victim@zou.test";
    wipe(&pool, &[email]).await;

    // Somebody holds the address, confirmed.
    let hash = zou_server::password::hash("a-long-enough-password");
    let sess = pool.unscoped().await.expect("connect");
    let held: String = sess
        .query(
            "insert into auth.users
                 (instance_id, id, aud, role, email, encrypted_password,
                  raw_app_meta_data, raw_user_meta_data,
                  confirmation_token, recovery_token, email_change_token_new, email_change,
                  email_confirmed_at, created_at, updated_at, is_anonymous, is_sso_user)
             values ('00000000-0000-0000-0000-000000000000', gen_random_uuid(),
                     'authenticated', 'authenticated', $1, $2,
                     '{\"provider\":\"email\",\"providers\":[\"email\"]}'::jsonb, '{}'::jsonb,
                     '', '', '', '', now(), now(), now(), false, false)
             returning id::text",
            &[&email, &hash],
        )
        .await
        .expect("plant the account")[0]
        .get(0);
    sess.commit().await.expect("write");

    // Github says the same address and will not say it is verified.
    fake.github(9001, "impostor", email, false);
    let state = start(&app, "provider=github").await;
    let landed = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
    let token = parts(&landed)
        .remove("access_token")
        .unwrap_or_else(|| panic!("no session in {landed}"));
    let arrived = claims(&token);

    assert_ne!(
        arrived["sub"], held,
        "an unverified address gets its own account and not somebody else's"
    );
    assert_eq!(
        arrived["email"], "",
        "and it gets no address at all, because the one it claimed is taken"
    );

    // The account that was there is untouched: same password, still
    // only the email provider.
    let (status, body) = post(
        &app,
        "/auth/v1/token?grant_type=password",
        serde_json::json!({"email": email, "password": "a-long-enough-password"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["user"]["id"], held);
    assert_eq!(
        body["user"]["app_metadata"]["providers"],
        serde_json::json!(["email"])
    );

    // Tidy up the account with no address, which no email can find.
    let sess = pool.unscoped().await.expect("connect");
    let orphan = arrived["sub"].as_str().expect("an id");
    sess.execute(
        "delete from auth.identities where user_id = $1::text::uuid",
        &[&orphan],
    )
    .await
    .expect("clear");
    sess.execute(
        "delete from auth.users where id = $1::text::uuid",
        &[&orphan],
    )
    .await
    .expect("clear");
    sess.commit().await.expect("write");
}

#[tokio::test]
async fn an_unverified_address_nobody_holds_is_mailed_rather_than_believed() {
    let Some(dsn) = dsn() else { return };
    let (app, fake) = app(&dsn, false);
    let pool = pool(&dsn).await;
    let email = "unvouched@zou.test";
    wipe(&pool, &[email]).await;
    fake.github(7007, "quiet", email, false);

    let state = start(&app, "provider=github").await;
    let res = go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await;
    assert_eq!(res.status(), StatusCode::FOUND);
    let landed = location(&res);
    let back = parts(&landed);
    assert_eq!(
        back["error"], "access_denied",
        "to the app it is simply that the person did not get in: {landed}"
    );
    assert_eq!(back["error_code"], "provider_email_needs_verification");
    assert!(
        back["error_description"].contains("A confirmation email has been sent"),
        "{landed}"
    );
    assert!(
        !landed.contains("access_token"),
        "and no session came with it: {landed}"
    );

    // The confirmation really went out, and the account exists but is
    // unconfirmed until it is answered.
    let waiting = inbox_for(&app, email).await;
    assert_eq!(
        waiting.len(),
        1,
        "one confirmation, to the address in question"
    );
    assert_eq!(waiting[0]["subject"], "Confirm your email address");

    let confirmed: bool = scalar(
        &pool,
        "select email_confirmed_at is not null from auth.users where email = $1",
        &[&email],
    )
    .await;
    assert!(!confirmed);
}

#[tokio::test]
async fn a_closed_project_lets_in_the_people_it_knows_and_nobody_else() {
    let Some(dsn) = dsn() else { return };
    let (open, _) = app(&dsn, true);
    let (shut, fake) = closed(&dsn);
    let pool = pool(&dsn).await;
    let known = "known@zou.test";
    let stranger = "stranger@zou.test";
    wipe(&pool, &[known, stranger]).await;

    // Somebody the project has never seen, arriving through a provider
    // it does trust. The provider is not the question: the question is
    // whether an account may be made, and on this project it may not.
    fake.google("google-sub-stranger", stranger, true);
    let state = start(&shut, "provider=google").await;
    let landed = location(&go(&shut, &format!("/auth/v1/callback?state={state}&code=c")).await);
    let back = parts(&landed);
    assert_eq!(back["error"], "access_denied", "{landed}");
    assert_eq!(back["error_code"], "signup_disabled");
    assert_eq!(
        back["error_description"],
        "Signups not allowed for this instance"
    );
    let made: i64 = scalar(
        &pool,
        "select count(*) from auth.users where email = $1",
        &[&stranger],
    )
    .await;
    assert_eq!(made, 0, "and the refusal left nothing behind");

    // Somebody who already has an account, made while the project was
    // open. Signing in is not signing up, so the same closed project
    // hands them a session and hangs the identity off the account they
    // already had.
    signed_up(&open, known).await;
    fake.google("google-sub-known", known, true);
    let state = start(&shut, "provider=google").await;
    let landed = location(&go(&shut, &format!("/auth/v1/callback?state={state}&code=c")).await);
    let back = parts(&landed);
    assert!(
        !back.contains_key("error"),
        "an account that exists is let in: {landed}"
    );
    let token = back.get("access_token").expect("a token");
    let email = claims(token)["email"]
        .as_str()
        .expect("an address")
        .to_string();
    assert_eq!(email, known);

    let identities: i64 = scalar(
        &pool,
        "select count(*) from auth.identities i
           join auth.users u on u.id = i.user_id where u.email = $1",
        &[&known],
    )
    .await;
    assert_eq!(identities, 2, "the password one and the google one");
}

#[tokio::test]
async fn a_state_is_only_good_once_and_only_if_it_is_one() {
    let Some(dsn) = dsn() else { return };
    let (app, fake) = app(&dsn, false);
    let pool = pool(&dsn).await;
    let email = "state@zou.test";
    wipe(&pool, &[email]).await;
    fake.google("google-sub-state", email, true);

    // Missing, malformed, and unknown each say something different.
    let cases = [
        ("", "bad_oauth_callback", "OAuth state parameter missing"),
        (
            "not-a-uuid",
            "bad_oauth_state",
            "OAuth state parameter is invalid",
        ),
        (
            "00000000-0000-0000-0000-000000000000",
            "bad_oauth_state",
            "OAuth state not found or expired",
        ),
    ];
    for (state, code, msg) in cases {
        let landed = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
        let back = parts(&landed);
        assert!(
            landed.starts_with(SITE),
            "a state that does not load has nowhere of its own to go: {landed}"
        );
        assert_eq!(back["error_code"], code, "{landed}");
        assert_eq!(back["error_description"], msg, "{landed}");
    }

    // A pkce state that has already been through the callback is spent,
    // even though the code it produced has not been traded.
    let verifier = "a-verifier-long-enough-for-the-replay-case";
    let state = start(
        &app,
        &format!(
            "provider=google&code_challenge={}&code_challenge_method=s256",
            challenge_for(verifier)
        ),
    )
    .await;
    let first = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
    assert!(parts(&first).contains_key("code"), "{first}");
    let again = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
    let back = parts(&again);
    assert_eq!(back["error_code"], "flow_state_already_used");
    assert_eq!(back["error_description"], "State has already been used");

    // An expired flow is refused too, which is the one case that cannot
    // be reached by waiting.
    let state = start(&app, "provider=google").await;
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        "update auth.flow_state set created_at = now() - interval '10 minutes'
          where id = $1::text::uuid",
        &[&state],
    )
    .await
    .expect("age it");
    sess.commit().await.expect("write");
    let landed = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
    assert_eq!(
        parts(&landed)["error_description"],
        "OAuth state has expired"
    );
}

#[tokio::test]
async fn what_the_provider_says_when_it_says_no_is_what_goes_back() {
    let Some(dsn) = dsn() else { return };
    let (app, fake) = app(&dsn, false);

    let state = start(
        &app,
        "provider=google&redirect_to=https%3A%2F%2Fapp.zou.test%2Fdone",
    )
    .await;
    let landed = location(
        &go(
            &app,
            &format!(
                "/auth/v1/callback?state={state}&error=access_denied\
                 &error_description=The+user+refused+consent"
            ),
        )
        .await,
    );
    assert!(
        landed.starts_with("https://app.zou.test/done"),
        "back where the flow said, not to the site url: {landed}"
    );
    let back = parts(&landed);
    assert_eq!(back["error"], "access_denied");
    assert_eq!(back["error_description"], "The user refused consent");
    assert!(
        fake.asked().is_empty(),
        "and nothing was exchanged, because there was nothing to exchange"
    );

    // A callback with neither an error nor a code is the provider or a
    // proxy getting it wrong, and says so.
    let state = start(&app, "provider=google").await;
    let landed = location(&go(&app, &format!("/auth/v1/callback?state={state}")).await);
    assert_eq!(
        parts(&landed)["error_code"],
        "bad_oauth_callback",
        "{landed}"
    );
}

#[tokio::test]
async fn the_pkce_parameters_are_judged_the_way_upstream_judges_them() {
    let Some(dsn) = dsn() else { return };
    let (app, _) = app(&dsn, true);

    let cases = [
        (
            "code_challenge=abc".to_string(),
            "PKCE flow requires code_challenge_method and code_challenge",
        ),
        (
            "code_challenge_method=s256".to_string(),
            "PKCE flow requires code_challenge_method and code_challenge",
        ),
        (
            "code_challenge=tooshort&code_challenge_method=s256".to_string(),
            "code challenge has to be between 43 and 128 characters",
        ),
        (
            format!(
                "code_challenge={}&code_challenge_method=s256",
                "!".repeat(44)
            ),
            "code challenge can only contain alphanumeric characters, hyphens, periods, underscores and tildes",
        ),
        (
            format!(
                "code_challenge={}&code_challenge_method=md5",
                "a".repeat(44)
            ),
            "Invalid code_challenge_method",
        ),
    ];
    for (query, msg) in cases {
        let res = go(&app, &format!("/auth/v1/authorize?provider=google&{query}")).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST, "{query}");
        let body = json(res).await;
        assert_eq!(body["error_code"], "validation_failed", "{query}");
        assert_eq!(body["msg"], msg, "{query}");
    }
}

#[tokio::test]
async fn an_address_the_person_has_moved_away_from_still_finds_them() {
    let Some(dsn) = dsn() else { return };
    // One confirmation to move, because the point here is the identity
    // and not the change of address.
    let (app, fake) = project(&dsn, false, false, false, false);
    let pool = pool(&dsn).await;
    let (old, new) = ("moved-from@zou.test", "moved-to@zou.test");
    wipe(&pool, &[old, new]).await;
    fake.google("google-sub-moved", old, true);

    let state = start(&app, "provider=google").await;
    let landed = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
    let token = parts(&landed).remove("access_token").expect("a token");
    let signed_in = claims(&token)["sub"].as_str().expect("an id").to_string();

    // The person moves their account to another address and answers the
    // mail. The google identity is left holding the old one, because an
    // identity says what the provider said and nothing else.
    let (status, body) = as_user(
        &app,
        "PUT",
        "/auth/v1/user",
        &token,
        serde_json::json!({"email": new}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mail = inbox_for(&app, new).await;
    assert_eq!(mail.len(), 1, "one confirmation to the new address");
    follow(&app, mail[0]["link"].as_str().expect("a link")).await;
    let moved: String = scalar(
        &pool,
        "select email from auth.users where id = $1::text::uuid",
        &[&signed_in],
    )
    .await;
    assert_eq!(moved, new, "the account is at the new address now");
    let held: String = scalar(
        &pool,
        "select email from auth.identities
          where user_id = $1::text::uuid and provider = 'google'",
        &[&signed_in],
    )
    .await;
    assert_eq!(held, old, "and the google identity still holds the old one");
    // Proving an address gives the account an email identity if it had
    // none, which is what upstream does on this branch: the account can
    // be signed in to with a password now, so something has to say the
    // email provider owns it.
    let proved: String = scalar(
        &pool,
        "select identity_data->>'email' from auth.identities
          where user_id = $1::text::uuid and provider = 'email'",
        &[&signed_in],
    )
    .await;
    assert_eq!(proved, new);

    // Github now turns up vouching for the old address, which no
    // account carries any more. The identity does, and that is enough.
    fake.github(1234, "moved", old, true);
    let state = start(&app, "provider=github").await;
    let landed = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
    let token = parts(&landed).remove("access_token").expect("a token");
    assert_eq!(
        claims(&token)["sub"],
        signed_in,
        "the same person, found by what their identity says rather than by their account"
    );
    let accounts: i64 = scalar(
        &pool,
        "select count(*) from auth.identities where user_id = $1::text::uuid",
        &[&signed_in],
    )
    .await;
    assert_eq!(accounts, 3, "google, github and email, one account");
}

#[tokio::test]
async fn apple_comes_back_as_a_form_and_says_who_it_is_in_the_id_token() {
    let Some(dsn) = dsn() else { return };
    let (app, fake) = app(&dsn, false);
    let pool = pool(&dsn).await;
    let email = "apple@zou.test";
    wipe(&pool, &[email]).await;
    fake.apple("apple-sub-1", email, true);

    // Asking for a name and an address is what makes Apple answer with
    // a form post rather than a redirect, so the request for it is part
    // of the contract.
    let res = go(&app, "/auth/v1/authorize?provider=apple").await;
    assert_eq!(res.status(), StatusCode::FOUND);
    let sent = location(&res);
    assert!(
        sent.starts_with("https://appleid.apple.com/auth/authorize?"),
        "{sent}"
    );
    let query = parts(&sent);
    assert_eq!(query["response_mode"], "form_post", "{sent}");
    assert_eq!(query["scope"], "email name");
    let state = query["state"].clone();

    // Apple posts the code, the state, and on the very first sign in a
    // name, which is the only time anybody will ever hear it.
    let res = post_form(
        &app,
        "/auth/v1/callback",
        &format!(
            "state={state}&code=from-apple\
             &user=%7B%22name%22%3A%7B%22firstName%22%3A%22Ada%22%2C\
             %22lastName%22%3A%22Lovelace%22%7D%7D"
        ),
    )
    .await;
    assert_eq!(res.status(), StatusCode::FOUND);
    let landed = location(&res);
    let back = parts(&landed);
    let token = back
        .get("access_token")
        .unwrap_or_else(|| panic!("no session in {landed}"));
    let claims = claims(token);
    assert_eq!(claims["email"], email);
    assert_eq!(
        claims["user_metadata"]["full_name"], "Ada Lovelace",
        "the name Apple posts once is kept"
    );

    // There is no profile endpoint to ask, so the exchange is the whole
    // conversation.
    assert_eq!(fake.asked(), vec![APPLE_TOKEN_URL]);

    let identity: String = scalar(
        &pool,
        "select provider_id from auth.identities i
           join auth.users u on u.id = i.user_id
          where u.email = $1 and i.provider = 'apple'",
        &[&email],
    )
    .await;
    assert_eq!(identity, "apple-sub-1");
}

#[tokio::test]
async fn manual_linking_is_not_there_until_a_project_turns_it_on() {
    let Some(dsn) = dsn() else { return };
    let (app, _) = app(&dsn, true);
    let pool = pool(&dsn).await;
    let email = "linking-off@zou.test";
    wipe(&pool, &[email]).await;
    let (_, token) = signed_up(&app, email).await;

    for (method, path) in [
        ("GET", "/auth/v1/user/identities/authorize?provider=github"),
        (
            "DELETE",
            "/auth/v1/user/identities/00000000-0000-0000-0000-000000000000",
        ),
    ] {
        let (status, body) = as_user(&app, method, path, &token, serde_json::json!({})).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{method} {path}: {body}");
        assert_eq!(body["error_code"], "manual_linking_disabled");
        assert_eq!(body["msg"], "Manual linking is disabled");
    }
}

#[tokio::test]
async fn a_second_provider_joins_the_account_that_asked_for_it() {
    let Some(dsn) = dsn() else { return };
    let (app, fake) = linking(&dsn);
    let pool = pool(&dsn).await;
    let (email, elsewhere) = ("joins@zou.test", "joins-elsewhere@zou.test");
    wipe(&pool, &[email, elsewhere]).await;
    let (user_id, token) = signed_up(&app, email).await;

    // Github vouches for an address that is not this account's, which
    // is the ordinary case: people use different addresses in different
    // places. Linking is what the session says, not what the address
    // says, so it goes ahead and the account keeps its own address.
    fake.github(777, "joiner", elsewhere, true);
    let state = start_link(&app, &token, "provider=github").await;
    let target: String = scalar(
        &pool,
        "select linking_target_id::text from auth.flow_state where id = $1::text::uuid",
        &[&state],
    )
    .await;
    assert_eq!(
        target, user_id,
        "the row remembers who asked, because the callback carries nothing that says so"
    );

    let res = go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await;
    assert_eq!(res.status(), StatusCode::FOUND);
    let landed = location(&res);
    let back = parts(&landed);
    assert!(
        !back.contains_key("error"),
        "the link went through: {landed}"
    );
    assert_eq!(
        claims(&back["access_token"])["sub"],
        user_id,
        "the session that comes back is the account that asked, not a new one"
    );

    let held: String = scalar(
        &pool,
        "select coalesce(email, '') from auth.users where id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    assert_eq!(
        held, email,
        "an account that has an address keeps it, whatever the provider says"
    );
    let providers: serde_json::Value = serde_json::from_str(
        &scalar::<String>(
            &pool,
            "select (raw_app_meta_data->'providers')::text from auth.users where id = $1::text::uuid",
            &[&user_id],
        )
        .await,
    )
    .expect("json");
    assert_eq!(providers, serde_json::json!(["email", "github"]));
    let elsewhere_accounts: i64 = scalar(
        &pool,
        "select count(*) from auth.users where email = $1",
        &[&elsewhere],
    )
    .await;
    assert_eq!(
        elsewhere_accounts, 0,
        "and no second account was made for the address the provider named"
    );
}

#[tokio::test]
async fn a_link_is_written_to_the_trail_with_the_address_it_came_from() {
    let Some(dsn) = dsn() else { return };
    let (app, fake) = linking(&dsn);
    let pool = pool(&dsn).await;
    let (email, elsewhere) = ("audited@zou.test", "audited-elsewhere@zou.test");
    wipe(&pool, &[email, elsewhere]).await;
    let (user_id, token) = signed_up(&app, email).await;

    fake.github(778, "audited", elsewhere, true);
    let state = start_link(&app, &token, "provider=github").await;
    // The address is on the callback rather than on the start, because
    // the callback is the request that writes the entry.
    let res = go_from(
        &app,
        &format!("/auth/v1/callback?state={state}&code=c"),
        "198.51.100.9",
    )
    .await;
    assert_eq!(res.status(), StatusCode::FOUND);
    let landed = location(&res);
    assert!(
        !parts(&landed).contains_key("error"),
        "the link went through: {landed}"
    );

    let identity: String = scalar(
        &pool,
        "select id::text from auth.identities
          where user_id = $1::text::uuid and provider = 'github'",
        &[&user_id],
    )
    .await;
    let (payload, ip): (String, String) = {
        let sess = pool.unscoped().await.expect("connect");
        let rows = sess
            .query(
                "select payload::text, ip_address from auth.audit_log_entries
                  where payload::jsonb ->> 'actor_id' = $1
                    and payload::jsonb ->> 'action' = 'identity_linked'",
                &[&user_id],
            )
            .await
            .expect("read the trail");
        sess.commit().await.expect("park");
        assert_eq!(rows.len(), 1, "one link, one entry");
        (rows[0].get(0), rows[0].get(1))
    };
    let payload: Value = serde_json::from_str(&payload).expect("json");
    assert_eq!(payload["log_type"], "user");
    assert_eq!(payload["actor_username"], email);
    assert_eq!(payload["traits"]["identity_id"], identity);
    assert_eq!(payload["traits"]["provider"], "github");
    assert_eq!(
        payload["traits"]["provider_id"], "778",
        "the provider's own id for the person, not zou's"
    );
    assert_eq!(
        ip, "198.51.100.9",
        "the linking entries are two of the six upstream fills the column for"
    );

    wipe(&pool, &[email, elsewhere]).await;
}

#[tokio::test]
async fn an_identity_that_is_already_spoken_for_does_not_move() {
    let Some(dsn) = dsn() else { return };
    let (app, fake) = linking(&dsn);
    let pool = pool(&dsn).await;
    let (first, second) = ("holder@zou.test", "would-take@zou.test");
    wipe(&pool, &[first, second]).await;
    let (_, mine) = signed_up(&app, first).await;
    let (_, theirs) = signed_up(&app, second).await;

    fake.github(888, "contested", "contested@zou.test", true);
    let state = start_link(&app, &mine, "provider=github").await;
    let landed = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
    assert!(parts(&landed).contains_key("access_token"), "{landed}");

    // The same github account, asked for by somebody else.
    let state = start_link(&app, &theirs, "provider=github").await;
    let landed = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
    let back = parts(&landed);
    assert_eq!(back["error_code"], "identity_already_exists");
    assert_eq!(
        back["error_description"],
        "Identity is already linked to another user"
    );

    // And asked for again by the person who already has it, which is a
    // different sentence because it is a different mistake.
    let state = start_link(&app, &mine, "provider=github").await;
    let landed = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
    let back = parts(&landed);
    assert_eq!(back["error_code"], "identity_already_exists");
    assert_eq!(back["error_description"], "Identity is already linked");

    let copies: i64 = scalar(
        &pool,
        "select count(*) from auth.identities where provider_id = '888' and provider = 'github'",
        &[],
    )
    .await;
    assert_eq!(copies, 1, "one identity, wherever it was asked for");
}

#[tokio::test]
async fn a_link_that_outlives_the_account_it_was_for_has_nowhere_to_go() {
    let Some(dsn) = dsn() else { return };
    let (app, fake) = linking(&dsn);
    let pool = pool(&dsn).await;
    let email = "vanishes@zou.test";
    wipe(&pool, &[email]).await;
    let (_, token) = signed_up(&app, email).await;
    fake.github(999, "ghost", "ghost@zou.test", true);

    let state = start_link(&app, &token, "provider=github").await;
    wipe(&pool, &[email]).await;

    let landed = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
    let back = parts(&landed);
    assert_eq!(back["error_code"], "user_not_found");
    assert_eq!(back["error_description"], "Linking target user not found");
    assert!(
        fake.asked().is_empty(),
        "and nothing was traded with the provider for it: {:?}",
        fake.asked()
    );
}

#[tokio::test]
async fn unlinking_takes_the_provider_back_off_the_account() {
    let Some(dsn) = dsn() else { return };
    let (app, fake) = linking(&dsn);
    let pool = pool(&dsn).await;
    let email = "unlinks@zou.test";
    wipe(&pool, &[email]).await;
    let (user_id, token) = signed_up(&app, email).await;
    fake.github(4141, "goes", "goes@zou.test", true);
    let state = start_link(&app, &token, "provider=github").await;
    let landed = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
    assert!(parts(&landed).contains_key("access_token"), "{landed}");

    let identity: String = scalar(
        &pool,
        "select id::text from auth.identities
          where user_id = $1::text::uuid and provider = 'github'",
        &[&user_id],
    )
    .await;

    // An id that is not one, and an id that is one and is nobody's.
    let (status, body) = as_user(
        &app,
        "DELETE",
        "/auth/v1/user/identities/not-a-uuid",
        &token,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error_code"], "validation_failed");
    assert_eq!(body["msg"], "identity_id must be an UUID");

    let (status, body) = as_user(
        &app,
        "DELETE",
        "/auth/v1/user/identities/00000000-0000-0000-0000-000000000000",
        &token,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error_code"], "identity_not_found");
    assert_eq!(body["msg"], "Identity doesn't exist");

    let (status, body) = as_user(
        &app,
        "DELETE",
        &format!("/auth/v1/user/identities/{identity}"),
        &token,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, serde_json::json!({}));

    let providers: serde_json::Value = serde_json::from_str(
        &scalar::<String>(
            &pool,
            "select (raw_app_meta_data->'providers')::text from auth.users where id = $1::text::uuid",
            &[&user_id],
        )
        .await,
    )
    .expect("json");
    assert_eq!(
        providers,
        serde_json::json!(["email"]),
        "what is advertised is what is left, not what was ever there"
    );

    // The last one stays, because an account with no identity is one
    // nobody can sign in to again.
    let last: String = scalar(
        &pool,
        "select id::text from auth.identities where user_id = $1::text::uuid",
        &[&user_id],
    )
    .await;
    let (status, body) = as_user(
        &app,
        "DELETE",
        &format!("/auth/v1/user/identities/{last}"),
        &token,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error_code"], "single_identity_not_deletable");
    assert_eq!(
        body["msg"],
        "User must have at least 1 identity after unlinking"
    );
}

#[tokio::test]
async fn an_account_with_no_address_takes_one_from_the_identity_that_joins_it() {
    let Some(dsn) = dsn() else { return };
    let (app, fake) = linking(&dsn);
    let pool = pool(&dsn).await;
    let (unverified, verified) = ("no-address-yet@zou.test", "vouched@zou.test");
    wipe(&pool, &[unverified, verified]).await;

    // An account with no address of its own: what an oauth signup
    // leaves behind when the provider would not vouch for the address
    // it named and somebody else already holds it. Made here directly,
    // because the point is what linking does to it.
    let user_id: String = scalar(
        &pool,
        "insert into auth.users
             (instance_id, id, aud, role, raw_app_meta_data, raw_user_meta_data,
              confirmation_token, recovery_token, email_change_token_new, email_change,
              created_at, updated_at, is_anonymous, is_sso_user)
         values ('00000000-0000-0000-0000-000000000000', gen_random_uuid(),
                 'authenticated', 'authenticated', '{\"providers\": []}'::jsonb,
                 '{}'::jsonb, '', '', '', '', now(), now(), true, false)
         returning id::text",
        &[],
    )
    .await;
    let token = jwt::mint(
        &serde_json::json!({
            "sub": user_id,
            "aud": "authenticated",
            "role": "authenticated",
            "iss": "https://zou.test/auth/v1",
            "exp": 4102444800u64,
            "is_anonymous": true,
        }),
        SECRET,
    );

    // First an address the provider will not vouch for. The account
    // takes it, because it had none, and then has to prove it: no
    // session comes back and a confirmation goes out instead.
    fake.google("google-sub-joins", unverified, false);
    let state = start_link(&app, &token, "provider=google").await;
    let landed = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
    let back = parts(&landed);
    assert_eq!(back["error_code"], "email_not_confirmed");
    assert_eq!(
        back["error_description"],
        "Unverified email with google. A confirmation email has been sent to your google email",
        "the sentence upstream sends, with upstream's other error code on this path"
    );
    assert_eq!(
        inbox_for(&app, unverified).await.len(),
        1,
        "the confirmation survived the refusal, which is why the refusal commits first"
    );
    let (address, confirmed): (String, bool) = {
        let sess = pool.unscoped().await.expect("connect");
        let rows = sess
            .query(
                "select coalesce(email, ''), email_confirmed_at is not null
                   from auth.users where id = $1::text::uuid",
                &[&user_id],
            )
            .await
            .expect("the account");
        sess.commit().await.expect("park");
        (rows[0].get(0), rows[0].get(1))
    };
    assert_eq!(address, unverified, "the account took the address");
    assert!(!confirmed, "and has not proved it");

    wipe(&pool, &[unverified]).await;
}

#[tokio::test]
async fn an_account_that_has_proved_an_address_does_not_prove_the_provider_s() {
    let Some(dsn) = dsn() else { return };
    let (app, fake) = linking(&dsn);
    let pool = pool(&dsn).await;
    let (email, unvouched) = ("proved@zou.test", "unvouched@zou.test");
    wipe(&pool, &[email, unvouched]).await;
    let (user_id, token) = signed_up(&app, email).await;

    // Google will not say the address on this profile is verified. On a
    // signup that would be the end of it, but this account already has
    // an address it has proved, so there is nothing here to prove: the
    // person is signed in and said to link it.
    fake.google("google-sub-proved", unvouched, false);
    let state = start_link(&app, &token, "provider=google").await;
    let landed = location(&go(&app, &format!("/auth/v1/callback?state={state}&code=c")).await);
    let back = parts(&landed);
    assert!(
        !back.contains_key("error"),
        "an unverified address on the provider's side stops nothing here: {landed}"
    );
    assert_eq!(claims(&back["access_token"])["sub"], user_id);
    assert!(
        inbox_for(&app, unvouched).await.is_empty(),
        "and nothing was mailed to an address nobody is claiming"
    );

    let (address, confirmed): (String, bool) = {
        let sess = pool.unscoped().await.expect("connect");
        let rows = sess
            .query(
                "select coalesce(email, ''), email_confirmed_at is not null
                   from auth.users where id = $1::text::uuid",
                &[&user_id],
            )
            .await
            .expect("the account");
        sess.commit().await.expect("park");
        (rows[0].get(0), rows[0].get(1))
    };
    assert_eq!(address, email, "the account is where it was");
    assert!(confirmed, "and is still confirmed");
}
