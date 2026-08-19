//! What a user carried off a hosted project can do the morning after,
//! against a live postgres.
//!
//! `zou import supabase` copies the `auth` rows into the tables this
//! server made at startup, and copies them column by column without
//! rewriting anything: the same uuid, the same bcrypt hash, the same
//! identity rows, the same TOTP secret. The other suites here prove the
//! auth api against rows this server wrote itself. This one proves it
//! against rows it did not write, seeded in the shape the platform's
//! `auth` schema actually has them, because those are the rows an
//! import puts in front of it.
//!
//! Two of the sentences the import prints are contracts, and both are
//! pinned here. Passwords come over, so a user signs in with the one
//! they had and nothing is reset. Sessions do not, so a refresh token
//! the old project minted is refused, and the first sign in here is the
//! whole of the cutover for a signed in user.
//!
//! The rows below are hand written rather than produced by running the
//! importer, which keeps this suite about what the server does with
//! them. What the importer writes is its own tests' business.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test auth_imported

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use zou_server::sql::Pool;
use zou_server::{Config, jwt, router, totp};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

/// The password behind every hash in this file, which is the one the
/// user typed on the old project and types again on this one.
const PASSWORD: &str = "correct horse";

/// A hash produced by golang.org/x/crypto/bcrypt at DefaultCost, which
/// is the library and the cost GoTrue hashes with. This is the literal
/// shape of the `encrypted_password` column a project carries with it.
const GO_BCRYPT: &str = "$2a$10$9mSfsZp2ozwmHOV6.8fl.OtVThXLxCXzN7X26Qou1r28iLAb3odY.";

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

fn app(dsn: &str) -> axum::Router {
    router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        external_url: Some("https://zou.test".to_string()),
        mailer_autoconfirm: true,
        ..Config::default()
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

async fn grant(app: &axum::Router, email: &str) -> Answer {
    post(
        app,
        "/auth/v1/token?grant_type=password",
        serde_json::json!({"email": email, "password": PASSWORD}),
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

async fn count(pool: &Pool, sql: &str, arg: &str) -> i64 {
    let sess = pool.unscoped().await.expect("connect");
    let n: i64 = sess.query(sql, &[&arg]).await.expect(sql)[0].get(0);
    sess.commit().await.expect("park");
    n
}

/// The rows the import leaves behind for one account, written the way
/// the platform's `auth` schema has them rather than the way this
/// server's signup would write them.
///
/// The account signed up with a password and later linked GitHub, which
/// is the ordinary shape and the one that makes `providers` a list of
/// two. `id` is given rather than generated, because carrying the uuid
/// is the whole reason an RLS policy written against `auth.uid()` keeps
/// working after the move, and a test that let the database pick one
/// could not say that.
///
/// What is deliberately absent is a session and a refresh token. The
/// copy leaves those on the old project, so this is what the rows
/// actually look like on the other side, not a simplification.
struct Carried {
    id: &'static str,
    email: String,
    /// The TOTP secret as GoTrue stored it, which with encryption off
    /// is the base32 an authenticator app was shown at enrollment.
    factor_secret: &'static str,
    factor_id: &'static str,
    /// Who this account is at GitHub. One per test, because upstream
    /// holds a provider and a provider_id unique across the project and
    /// two tests running at once would collide on a shared one.
    github_id: &'static str,
}

impl Carried {
    async fn seed(&self, pool: &Pool) {
        let sess = pool.unscoped().await.expect("connect");
        sess.execute(
            "insert into auth.users
                 (id, instance_id, aud, role, email, encrypted_password,
                  email_confirmed_at, last_sign_in_at,
                  raw_app_meta_data, raw_user_meta_data,
                  created_at, updated_at, is_anonymous)
             values ($1::text::uuid, '00000000-0000-0000-0000-000000000000',
                     'authenticated', 'authenticated', $2, $3,
                     now() - interval '400 days', now() - interval '2 days',
                     '{\"provider\": \"email\", \"providers\": [\"email\", \"github\"]}'::jsonb,
                     '{\"nickname\": \"already here\"}'::jsonb,
                     now() - interval '400 days', now() - interval '2 days', false)",
            &[&self.id, &self.email, &GO_BCRYPT],
        )
        .await
        .expect("the user row");

        // Two identities, because the account has two ways in and the
        // user payload has to say both. `provider_id` is the id at the
        // provider, which for email is the user's own uuid and for an
        // oauth provider is whatever that provider calls them.
        sess.execute(
            "insert into auth.identities
                 (id, user_id, provider, provider_id, identity_data,
                  last_sign_in_at, created_at, updated_at)
             values (gen_random_uuid(), $1::text::uuid, 'email', $1,
                     jsonb_build_object('sub', $1, 'email', $2::text,
                                        'email_verified', true,
                                        'phone_verified', false),
                     now() - interval '400 days', now() - interval '400 days',
                     now() - interval '400 days'),
                    (gen_random_uuid(), $1::text::uuid, 'github', $3,
                     jsonb_build_object('sub', $3::text, 'email', $2::text,
                                        'user_name', 'already-here',
                                        'email_verified', true),
                     now() - interval '2 days', now() - interval '200 days',
                     now() - interval '2 days')",
            &[&self.id, &self.email, &self.github_id],
        )
        .await
        .expect("the identity rows");

        // A factor the user enrolled on the old project, verified there
        // and never touched again. `last_challenged_at` is left null
        // rather than given a time, because that column carries a
        // unique constraint upstream and two seeded factors would
        // collide on it.
        sess.execute(
            "insert into auth.mfa_factors
                 (id, user_id, friendly_name, factor_type, status,
                  created_at, updated_at, secret)
             values ($1::text::uuid, $2::text::uuid, 'the old phone',
                     'totp', 'verified',
                     now() - interval '300 days', now() - interval '300 days', $3)",
            &[&self.factor_id, &self.id, &self.factor_secret],
        )
        .await
        .expect("the factor row");
        sess.commit().await.expect("park");
    }
}

#[tokio::test]
async fn a_user_the_import_carried_signs_in_with_the_password_they_had() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let who = Carried {
        id: "3f2b6a10-0000-4000-8000-00000000c001",
        email: address("imported-password"),
        factor_secret: "JBSWY3DPEHPK3PXP",
        factor_id: "3f2b6a10-0000-4000-8000-00000000f001",
        github_id: "90210",
    };
    wipe(&pool, &who.email).await;
    who.seed(&pool).await;

    let app = app(&dsn);
    let signed_in = grant(&app, &who.email).await;
    assert_eq!(signed_in.status, StatusCode::OK, "{}", signed_in.body);

    // The uuid is the one the old project had. An RLS policy written
    // against auth.uid() keeps matching the rows it matched there, and
    // that is only true because the import carries the id rather than
    // making a new one.
    let user = &signed_in.body["user"];
    assert_eq!(user["id"], who.id);
    let claims = claims_of(&signed_in.str("access_token"));
    assert_eq!(claims["sub"], who.id);
    assert_eq!(claims["role"], "authenticated");
    assert_eq!(claims["amr"][0]["method"], "password");

    // The address was confirmed there and is confirmed here, so nobody
    // is asked to click a link they already clicked in 2024.
    assert!(
        user["email_confirmed_at"].is_string(),
        "the confirmation came over: {user}"
    );
    assert_eq!(user["user_metadata"]["nickname"], "already here");

    // Both identities, in the order the survey reports them, and the
    // app_metadata that says which providers this account has. A client
    // reading `providers` to decide which buttons to show sees what it
    // saw before the move.
    assert_eq!(user["app_metadata"]["providers"][0], "email");
    assert_eq!(user["app_metadata"]["providers"][1], "github");
    let mut providers: Vec<String> = user["identities"]
        .as_array()
        .expect("identities")
        .iter()
        .map(|i| i["provider"].as_str().expect("provider").to_string())
        .collect();
    providers.sort();
    assert_eq!(providers, vec!["email".to_string(), "github".to_string()]);
    let github = user["identities"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["provider"] == "github")
        .expect("the github identity");
    assert_eq!(github["identity_data"]["user_name"], "already-here");
    assert_eq!(github["identity_data"]["sub"], who.github_id);

    // The other half of the contract: the hash is the one GoTrue wrote
    // and this server did not quietly rewrite it on a successful sign
    // in, because a rewrite is a change nobody asked for to the one
    // column a rollback would need.
    let sess = pool.unscoped().await.expect("connect");
    let held: String = sess
        .query(
            "select encrypted_password from auth.users where id = $1::text::uuid",
            &[&who.id],
        )
        .await
        .expect("read the hash")[0]
        .get(0);
    sess.commit().await.expect("park");
    assert_eq!(held, GO_BCRYPT);

    let wrong = post(
        &app,
        "/auth/v1/token?grant_type=password",
        serde_json::json!({"email": &who.email, "password": "correct horses"}),
    )
    .await;
    assert_eq!(
        wrong.refusal(),
        (400, "invalid_credentials", "Invalid login credentials")
    );

    wipe(&pool, &who.email).await;
}

#[tokio::test]
async fn a_factor_the_import_carried_still_proves_the_session() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let who = Carried {
        id: "3f2b6a10-0000-4000-8000-00000000c002",
        email: address("imported-factor"),
        factor_secret: "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ",
        factor_id: "3f2b6a10-0000-4000-8000-00000000f002",
        github_id: "90211",
    };
    wipe(&pool, &who.email).await;
    who.seed(&pool).await;

    let app = app(&dsn);
    let signed_in = grant(&app, &who.email).await;
    assert_eq!(signed_in.status, StatusCode::OK, "{}", signed_in.body);
    let token = signed_in.str("access_token");

    // The factor is eager loaded with the user, the way GoTrue loads
    // it, so a client that lists factors to decide whether to ask for a
    // code sees the imported one without a second request.
    let factor = &signed_in.body["user"]["factors"][0];
    assert_eq!(factor["id"], who.factor_id);
    assert_eq!(factor["status"], "verified");
    assert_eq!(factor["factor_type"], "totp");
    assert_eq!(factor["friendly_name"], "the old phone");

    // A password alone is aal1 even with a verified factor on the
    // account, which is the level an RLS policy reading auth.jwt() sees
    // before the code is given.
    assert_eq!(claims_of(&token)["aal"], "aal1");

    // Now the part that only works if the secret came over byte for
    // byte: the code the user's authenticator is showing right now,
    // computed from the secret that was enrolled on the old project.
    let challenge = as_user(
        &app,
        "POST",
        &format!("/auth/v1/factors/{}/challenge", who.factor_id),
        &token,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(challenge.status, StatusCode::OK, "{}", challenge.body);
    let code = totp::code(who.factor_secret, now()).expect("the secret is base32");
    let proved = as_user(
        &app,
        "POST",
        &format!("/auth/v1/factors/{}/verify", who.factor_id),
        &token,
        serde_json::json!({"challenge_id": challenge.str("id"), "code": code}),
    )
    .await;
    assert_eq!(proved.status, StatusCode::OK, "{}", proved.body);

    let raised = claims_of(&proved.str("access_token"));
    assert_eq!(raised["aal"], "aal2");
    let methods: Vec<&str> = raised["amr"]
        .as_array()
        .expect("amr")
        .iter()
        .map(|m| m["method"].as_str().expect("method"))
        .collect();
    assert!(methods.contains(&"totp"), "{raised}");

    wipe(&pool, &who.email).await;
}

#[tokio::test]
async fn no_session_came_over_so_the_old_refresh_token_is_refused() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let who = Carried {
        id: "3f2b6a10-0000-4000-8000-00000000c003",
        email: address("imported-session"),
        factor_secret: "MFRGGZDFMZTWQ2LKNNWG23TP",
        factor_id: "3f2b6a10-0000-4000-8000-00000000f003",
        github_id: "90212",
    };
    wipe(&pool, &who.email).await;
    who.seed(&pool).await;

    // Nothing was carried into either table, which is what the copy
    // says it does and now what the database says it did.
    assert_eq!(
        count(
            &pool,
            "select count(*) from auth.sessions where user_id = $1::text::uuid",
            who.id
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &pool,
            // `user_id` on this one is text upstream rather than a
            // uuid, which is one of the shapes the copy has to carry
            // as it found it.
            "select count(*) from auth.refresh_tokens where user_id = $1",
            who.id
        )
        .await,
        0
    );

    // A client that still had a token in its local storage when the
    // project moved presents it here. It is refused, in GoTrue's words,
    // and the client's usual answer to that message is to send the user
    // to the sign in page, which is exactly the once that the cutover
    // costs them.
    let app = app(&dsn);
    let stale = post(
        &app,
        "/auth/v1/token?grant_type=refresh_token",
        serde_json::json!({"refresh_token": "hh4hq3tqzqxk"}),
    )
    .await;
    assert_eq!(
        stale.refusal(),
        (
            400,
            "refresh_token_not_found",
            "Invalid Refresh Token: Refresh Token Not Found"
        )
    );

    // Signing in again is all it takes, and that sign in is what makes
    // the session and the token this server will honour.
    let signed_in = grant(&app, &who.email).await;
    assert_eq!(signed_in.status, StatusCode::OK, "{}", signed_in.body);
    assert_eq!(
        count(
            &pool,
            "select count(*) from auth.sessions where user_id = $1::text::uuid",
            who.id
        )
        .await,
        1
    );

    let fresh = post(
        &app,
        "/auth/v1/token?grant_type=refresh_token",
        serde_json::json!({"refresh_token": signed_in.str("refresh_token")}),
    )
    .await;
    assert_eq!(fresh.status, StatusCode::OK, "{}", fresh.body);
    assert_eq!(claims_of(&fresh.str("access_token"))["sub"], who.id);

    wipe(&pool, &who.email).await;
}
