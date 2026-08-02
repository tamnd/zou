//! The custom access token hook against a live postgres.
//!
//! This is the one place a project gets to change what every other
//! part of it sees. The claims the hook hands back are the claims that
//! get signed, and those are the claims auth.jwt() reads out again
//! inside every RLS policy, so what is pinned here is the whole
//! contract: what the function is handed, what it has to hand back,
//! what happens when it refuses, what happens when it breaks, and what
//! it leaves behind either way.
//!
//! The hook runs inside the grant's own transaction, which is the part
//! with teeth. A hook that writes commits with the sign in. A hook that
//! refuses takes the sign in down with it, including the refresh token
//! rotation that had already happened.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test auth_hook

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use zou_server::sql::Pool;
use zou_server::{Config, hook, jwt, mail, router};

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

/// The whole front door with a hook pointed at `uri`. The URI goes
/// through the same reader the environment does, so a test that names
/// a function is also a test that the URI was understood.
fn app(dsn: &str, uri: &str) -> axum::Router {
    with(dsn, hooked(uri, true))
}

fn with(dsn: &str, hook: hook::Settings) -> axum::Router {
    router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        external_url: Some("https://zou.test".to_string()),
        mailer_autoconfirm: true,
        anonymous_users: true,
        mail: mail::Settings {
            max_frequency: 0,
            ..mail::Settings::default()
        },
        hook,
        ..Config::default()
    })
    .expect("router builds")
}

fn hooked(uri: &str, enabled: bool) -> hook::Settings {
    hook::configured(&|name| match name {
        "ZOU_HOOK_CUSTOM_ACCESS_TOKEN_URI" => uri.to_string(),
        "ZOU_HOOK_CUSTOM_ACCESS_TOKEN_ENABLED" => enabled.to_string(),
        _ => String::new(),
    })
    .expect("the hook uri is one this end understands")
}

fn anon_key() -> String {
    jwt::mint(&jwt::key_claims("anon"), SECRET)
}

/// A hook function of this test's own, named after the test that owns
/// it so two tests running at once never call each other's. The body is
/// plpgsql, everything between begin and end.
async fn hook_fn(pool: &Pool, name: &str, body: &str) -> String {
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        &format!(
            "create or replace function public.{name}(event jsonb) returns jsonb
             language plpgsql as $fn$ begin {body} end $fn$"
        ),
        &[],
    )
    .await
    .expect("create the hook function");
    sess.commit().await.expect("park");
    format!("pg-functions://postgres/public/{name}")
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

    /// The claims of the access token that came back.
    fn claims(&self) -> serde_json::Value {
        let token = self.body["access_token"]
            .as_str()
            .unwrap_or_else(|| panic!("no access token in {}", self.body));
        jwt::verify(token, SECRET)
            .expect("the access token verifies")
            .claims
    }

    fn str(&self, key: &str) -> String {
        self.body[key]
            .as_str()
            .unwrap_or_else(|| panic!("no {key} in {}", self.body))
            .to_string()
    }
}

async fn post(app: &axum::Router, path: &str, body: serde_json::Value) -> Answer {
    from(app, path, body, "").await
}

/// The same, from an address. Nothing sits in front of this server in a
/// test, so the forwarded header is the only way to be somewhere.
async fn from(app: &axum::Router, path: &str, body: serde_json::Value, ip: &str) -> Answer {
    let mut req = Request::builder()
        .method("POST")
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
    let status = res.status();
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("not json: {}", String::from_utf8_lossy(&bytes)));
    Answer { status, body }
}

/// A signup, which is a grant of its own: the project confirms its own
/// addresses here, so the answer carries a session.
async fn signup(app: &axum::Router, email: &str) -> Answer {
    post(
        app,
        "/auth/v1/signup",
        serde_json::json!({"email": email, "password": "correct horse"}),
    )
    .await
}

async fn grant(app: &axum::Router, body: serde_json::Value) -> Answer {
    post(app, "/auth/v1/token?grant_type=password", body).await
}

async fn refresh(app: &axum::Router, token: &str) -> Answer {
    post(
        app,
        "/auth/v1/token?grant_type=refresh_token",
        serde_json::json!({"refresh_token": token}),
    )
    .await
}

fn address(tag: &str) -> String {
    format!("hook-{tag}@zou.test")
}

async fn wipe(pool: &Pool, email: &str) {
    let sess = pool.unscoped().await.expect("connect");
    sess.execute("delete from auth.users where email = $1", &[&email])
        .await
        .expect("wipe");
    sess.commit().await.expect("park");
}

#[tokio::test]
async fn a_hook_puts_its_own_claim_in_the_token() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("adds");
    wipe(&pool, &email).await;
    // The shape Supabase's own documentation uses: read the claims out
    // of the event, put them back changed, return the event.
    let uri = hook_fn(
        &pool,
        "zou_hook_adds",
        "return jsonb_set(event, '{claims,plan}', '\"gold\"');",
    )
    .await;
    let app = app(&dsn, &uri);

    let signed_up = signup(&app, &email).await;
    assert_eq!(signed_up.status, StatusCode::OK);
    assert_eq!(signed_up.claims()["plan"], "gold");
    // Everything else about the token is still what it was. The hook
    // adds a claim, it does not take the session over.
    assert_eq!(signed_up.claims()["role"], "authenticated");
    assert_eq!(signed_up.claims()["email"], email.as_str());

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn every_grant_that_mints_a_token_goes_through_it() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("every-grant");
    wipe(&pool, &email).await;
    let uri = hook_fn(
        &pool,
        "zou_hook_every_grant",
        "return jsonb_set(event, '{claims,seen}', to_jsonb(event->>'authentication_method'));",
    )
    .await;
    let app = app(&dsn, &uri);

    let signed_up = signup(&app, &email).await;
    assert_eq!(signed_up.claims()["seen"], "password");

    let signed_in = grant(
        &app,
        serde_json::json!({"email": &email, "password": "correct horse"}),
    )
    .await;
    assert_eq!(signed_in.status, StatusCode::OK);
    assert_eq!(signed_in.claims()["seen"], "password");

    // A refresh proves nothing new about who is asking, and the hook is
    // told exactly that rather than what the session was first proved
    // with.
    let refreshed = refresh(&app, &signed_in.str("refresh_token")).await;
    assert_eq!(refreshed.status, StatusCode::OK);
    assert_eq!(refreshed.claims()["seen"], "token_refresh");

    let nobody = post(&app, "/auth/v1/signup", serde_json::json!({})).await;
    assert_eq!(nobody.status, StatusCode::OK);
    assert_eq!(nobody.claims()["seen"], "anonymous");
    assert_eq!(nobody.claims()["is_anonymous"], true);

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn the_hook_is_handed_what_upstream_hands_it() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("input");
    wipe(&pool, &email).await;
    // Everything but the claims, put back inside the claims, so the
    // token itself says what the function was given.
    let uri = hook_fn(
        &pool,
        "zou_hook_input",
        "return jsonb_set(event, '{claims,given}', event - 'claims');",
    )
    .await;
    let app = app(&dsn, &uri);

    let signed_up = from(
        &app,
        "/auth/v1/signup",
        serde_json::json!({"email": &email, "password": "correct horse"}),
        "203.0.113.7",
    )
    .await;
    assert_eq!(signed_up.status, StatusCode::OK);
    let claims = signed_up.claims();
    let given = &claims["given"];

    assert_eq!(given["metadata"]["name"], "customize-access-token");
    assert_eq!(given["metadata"]["ip_address"], "203.0.113.7");
    let stamped = given["metadata"]["uuid"].as_str().expect("a uuid");
    assert_eq!(stamped.len(), 36, "not a uuid: {stamped}");
    assert_eq!(
        stamped.chars().nth(14),
        Some('4'),
        "not version 4: {stamped}"
    );
    let time = given["metadata"]["time"].as_str().expect("a time");
    assert!(
        time.len() == 20 && time.ends_with('Z') && time.contains('T'),
        "not an RFC 3339 instant: {time}"
    );
    assert_eq!(given["user_id"], claims["sub"]);
    assert_eq!(given["authentication_method"], "password");
    // The claims it was handed are the ones this server was about to
    // sign, which is why returning the event unchanged is a no op.
    assert_eq!(claims["session_id"].as_str().expect("a session").len(), 36);

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_hook_that_refuses_refuses_the_request() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("refuses");
    wipe(&pool, &email).await;
    // Upstream's own example of a hook that refuses: this address is
    // not one the company lets in.
    let uri = hook_fn(
        &pool,
        "zou_hook_refuses",
        "return jsonb_build_object('error', jsonb_build_object(
             'http_code', 403, 'message', 'only members of the company may sign in'));",
    )
    .await;
    let app = app(&dsn, &uri);

    let refused = signup(&app, &email).await;
    assert_eq!(
        refused.refusal(),
        (403, "unknown", "only members of the company may sign in")
    );

    // The signup wrote a user and then the hook refused, so the whole
    // transaction went back: there is nobody to sign in as afterwards.
    let sess = pool.unscoped().await.expect("connect");
    let left: i64 = sess
        .query(
            "select count(*) from auth.users where email = $1",
            &[&email],
        )
        .await
        .expect("count")[0]
        .get(0);
    sess.commit().await.expect("park");
    assert_eq!(left, 0, "a refused signup left a user behind");

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_refusal_with_no_status_of_its_own_is_a_500() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("no-status");
    wipe(&pool, &email).await;
    let uri = hook_fn(
        &pool,
        "zou_hook_no_status",
        "return jsonb_build_object('error', jsonb_build_object('message', 'not today'));",
    )
    .await;
    let app = app(&dsn, &uri);

    // A hook that refuses without saying at what status is a hook that
    // broke rather than a request that was wrong, which is upstream's
    // reading of it.
    assert_eq!(
        signup(&app, &email).await.refusal(),
        (500, "unexpected_failure", "not today")
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn an_error_with_nothing_to_say_is_not_a_refusal() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("empty-error");
    wipe(&pool, &email).await;
    // An empty message is not a refusal upstream, deliberately, so the
    // claims alongside it are used and the sign in goes through.
    let uri = hook_fn(
        &pool,
        "zou_hook_empty_error",
        "return jsonb_set(event, '{error}', jsonb_build_object('message', ''));",
    )
    .await;
    let app = app(&dsn, &uri);

    let signed_up = signup(&app, &email).await;
    assert_eq!(signed_up.status, StatusCode::OK);
    assert_eq!(signed_up.claims()["email"], email.as_str());

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_refusal_on_a_refresh_leaves_the_token_that_was_presented_alone() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("refresh-refusal");
    wipe(&pool, &email).await;
    let uri = hook_fn(
        &pool,
        "zou_hook_refresh_refusal",
        "return jsonb_build_object('error', jsonb_build_object(
             'http_code', 403, 'message', 'no more tokens for you'));",
    )
    .await;
    let quiet = with(&dsn, hook::Settings::none());
    let hooked = app(&dsn, &uri);

    let signed_in = signup(&quiet, &email).await;
    let token = signed_in.str("refresh_token");
    let refused = refresh(&hooked, &token).await;
    assert_eq!(refused.refusal().0, 403);

    // The rotation had already happened when the hook refused, and it
    // went back with it. Nothing was spent and nothing was written:
    // the token the client is holding is still the only one on the
    // session and it is still good.
    //
    // Asking again is not enough to see this on its own, because a
    // token that was rotated a moment ago is still answered inside the
    // reuse interval. The row is what says whether it was spent.
    let sess = pool.unscoped().await.expect("connect");
    let rows = sess
        .query(
            "select t.token, t.revoked from auth.refresh_tokens t
               join auth.users u on u.id::text = t.user_id
              where u.email = $1",
            &[&email],
        )
        .await
        .expect("read the tokens");
    assert_eq!(rows.len(), 1, "the rotation committed anyway");
    assert_eq!(rows[0].get::<_, String>(0), token);
    assert!(!rows[0].get::<_, bool>(1), "the token was spent anyway");
    sess.commit().await.expect("park");

    // And it still works, which is the difference between a hook that
    // broke and a logout.
    let after = refresh(&quiet, &token).await;
    assert_eq!(after.status, StatusCode::OK);
    assert_ne!(after.str("refresh_token"), token);

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_claim_the_token_cannot_do_without_cannot_be_dropped() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("drops-role");
    wipe(&pool, &email).await;
    let uri = hook_fn(
        &pool,
        "zou_hook_drops_role",
        "return jsonb_set(event, '{claims}', (event->'claims') - 'role');",
    )
    .await;
    let app = app(&dsn, &uri);

    let refused = signup(&app, &email).await;
    let (status, code, msg) = refused.refusal();
    assert_eq!((status, code), (500, "unexpected_failure"));
    assert_eq!(
        msg,
        "output claims do not conform to the expected schema: \n- (root): role is required\n"
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_claim_nothing_depends_on_can_be_dropped() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("drops-metadata");
    wipe(&pool, &email).await;
    let uri = hook_fn(
        &pool,
        "zou_hook_drops_metadata",
        "return jsonb_set(event, '{claims}', (event->'claims') - 'user_metadata');",
    )
    .await;
    let app = app(&dsn, &uri);

    let signed_up = signup(&app, &email).await;
    assert_eq!(signed_up.status, StatusCode::OK);
    assert!(
        signed_up.claims().get("user_metadata").is_none(),
        "the claim came back anyway: {}",
        signed_up.claims()
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_claim_that_is_the_wrong_type_is_named_and_so_is_the_type() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("wrong-type");
    wipe(&pool, &email).await;
    let uri = hook_fn(
        &pool,
        "zou_hook_wrong_type",
        "return jsonb_set(event, '{claims,exp}', '\"tomorrow\"');",
    )
    .await;
    let app = app(&dsn, &uri);

    let refused = signup(&app, &email).await;
    let (status, _, msg) = refused.refusal();
    assert_eq!(status, 500);
    assert_eq!(
        msg,
        "output claims do not conform to the expected schema: \n\
         - exp: Invalid type. Expected: integer, but got: string\n"
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn the_amr_entries_may_be_strings_or_objects() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("amr");
    wipe(&pool, &email).await;
    let words = hook_fn(
        &pool,
        "zou_hook_amr_words",
        "return jsonb_set(event, '{claims,amr}', '[\"password\", \"totp\"]');",
    )
    .await;
    let rows = hook_fn(
        &pool,
        "zou_hook_amr_rows",
        "return jsonb_set(event, '{claims,amr}',
             '[{\"method\": \"password\", \"timestamp\": 1700000000}]');",
    )
    .await;
    let numbers = hook_fn(
        &pool,
        "zou_hook_amr_numbers",
        "return jsonb_set(event, '{claims,amr}', '[1]');",
    )
    .await;

    let signed_up = signup(&app(&dsn, &words), &email).await;
    assert_eq!(signed_up.claims()["amr"][0], "password");
    wipe(&pool, &email).await;

    let signed_up = signup(&app(&dsn, &rows), &email).await;
    assert_eq!(signed_up.claims()["amr"][0]["method"], "password");
    wipe(&pool, &email).await;

    let refused = signup(&app(&dsn, &numbers), &email).await;
    let (status, _, msg) = refused.refusal();
    assert_eq!(status, 500);
    assert_eq!(
        msg,
        "output claims do not conform to the expected schema: \n\
         - amr.0: Must validate at least one schema (anyOf)\n"
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_hook_that_answers_the_wrong_shape_says_which_way() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("shapes");
    wipe(&pool, &email).await;
    let empty = hook_fn(&pool, "zou_hook_empty", "return '{}'::jsonb;").await;
    let nothing = hook_fn(&pool, "zou_hook_nothing", "return null;").await;
    let number = hook_fn(&pool, "zou_hook_number", "return '5'::jsonb;").await;
    let not_claims = hook_fn(
        &pool,
        "zou_hook_not_claims",
        "return jsonb_build_object('claims', 'gold');",
    )
    .await;

    assert_eq!(
        signup(&app(&dsn, &empty), &email).await.refusal(),
        (500, "unexpected_failure", "output claims field is missing")
    );
    wipe(&pool, &email).await;

    assert_eq!(
        signup(&app(&dsn, &nothing), &email).await.refusal(),
        (
            500,
            "unexpected_failure",
            "output claims do not conform to the expected schema: \n\
             - (root): Invalid type. Expected: object, but got: null\n"
        )
    );
    wipe(&pool, &email).await;

    assert_eq!(
        signup(&app(&dsn, &number), &email).await.refusal(),
        (500, "unexpected_failure", "Error unmarshaling JSON output.")
    );
    wipe(&pool, &email).await;

    assert_eq!(
        signup(&app(&dsn, &not_claims), &email).await.refusal(),
        (500, "unexpected_failure", "Error unmarshaling JSON output.")
    );

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_hook_that_raises_takes_the_grant_down_with_it() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("raises");
    wipe(&pool, &email).await;
    let uri = hook_fn(
        &pool,
        "zou_hook_raises",
        "raise exception 'the hook is broken';",
    )
    .await;
    let app = app(&dsn, &uri);

    // A function that raises is a project bug, and it is answered the
    // way every other unexpected failure is: a 500 that says nothing,
    // with the detail in the server log.
    assert_eq!(
        signup(&app, &email).await.refusal(),
        (
            500,
            "unexpected_failure",
            "Unexpected failure, please check server logs for more information"
        )
    );
    let sess = pool.unscoped().await.expect("connect");
    let left: i64 = sess
        .query(
            "select count(*) from auth.users where email = $1",
            &[&email],
        )
        .await
        .expect("count")[0]
        .get(0);
    sess.commit().await.expect("park");
    assert_eq!(left, 0, "a broken hook left a user behind");

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn what_a_hook_writes_commits_with_the_grant() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("writes");
    wipe(&pool, &email).await;
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        "create table if not exists public.zou_hook_ledger
             (at timestamptz default now(), who text, method text)",
        &[],
    )
    .await
    .expect("the ledger");
    sess.execute("delete from public.zou_hook_ledger", &[])
        .await
        .expect("clear the ledger");
    sess.commit().await.expect("park");

    let uri = hook_fn(
        &pool,
        "zou_hook_writes",
        "insert into public.zou_hook_ledger (who, method)
         values (event->>'user_id', event->>'authentication_method');
         return event;",
    )
    .await;
    let app = app(&dsn, &uri);

    let signed_up = signup(&app, &email).await;
    assert_eq!(signed_up.status, StatusCode::OK);
    let sess = pool.unscoped().await.expect("connect");
    let rows = sess
        .query("select who, method from public.zou_hook_ledger", &[])
        .await
        .expect("read the ledger");
    assert_eq!(rows.len(), 1, "the hook's row did not commit");
    assert_eq!(rows[0].get::<_, String>(0), signed_up.claims()["sub"]);
    assert_eq!(rows[0].get::<_, String>(1), "password");
    sess.commit().await.expect("park");

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_hook_that_will_not_finish_is_cut_off() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("slow");
    wipe(&pool, &email).await;
    let uri = hook_fn(
        &pool,
        "zou_hook_slow",
        "perform pg_sleep(30); return event;",
    )
    .await;
    let app = app(&dsn, &uri);

    let started = std::time::Instant::now();
    let refused = signup(&app, &email).await;
    let (status, code, _) = refused.refusal();
    let took = started.elapsed();
    assert_eq!((status, code), (500, "unexpected_failure"));
    // Two seconds is what upstream gives a hook, and the point of
    // giving it any is that a hook nobody wrote carefully cannot hold
    // the request open for as long as it likes.
    assert!(took.as_secs() < 10, "waited {took:?} for a hook");

    wipe(&pool, &email).await;
}

#[tokio::test]
async fn a_hook_that_is_configured_and_switched_off_does_not_run() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let email = address("dormant");
    wipe(&pool, &email).await;
    let uri = hook_fn(
        &pool,
        "zou_hook_dormant",
        "return jsonb_set(event, '{claims,plan}', '\"gold\"');",
    )
    .await;
    let app = with(&dsn, hooked(&uri, false));

    let signed_up = signup(&app, &email).await;
    assert_eq!(signed_up.status, StatusCode::OK);
    assert!(
        signed_up.claims().get("plan").is_none(),
        "a hook that is switched off ran anyway"
    );

    wipe(&pool, &email).await;
}
