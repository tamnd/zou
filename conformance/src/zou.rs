//! zou as a target, started here rather than found running.
//!
//! The other targets are urls somebody else brought up. zou is linked
//! in, so the harness binds a port, starts the server on a thread, and
//! waits for it to answer. That is one command to run a conformance
//! pass instead of three, and it means CI has nothing to leave behind
//! when a case fails halfway.
//!
//! The thread is never joined. The process exits when the run is over
//! and the server goes with it, which is the whole of the lifecycle a
//! test binary needs.

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::time::Duration;

use zou_server::s3::Credentials;
use zou_server::{Config, jwt, serve_blocking};

pub struct Served {
    /// Where it answers, with no trailing slash.
    pub url: String,
}

/// A project key signed the way zou dev signs one, so the same key
/// works against zou and against a PostgREST configured with the same
/// secret.
pub fn key(role: &str, secret: &str) -> String {
    jwt::mint(&jwt::key_claims(role), secret.as_bytes())
}

/// An access token for the person a suite seeded, carrying the claims
/// GoTrue puts in one.
///
/// Not a project key with a `sub` bolted on: the endpoints that read a
/// token read more of it than the role. `session_id` is what the
/// endpoints that can end a session look the session up by, `aal` and
/// `amr` are what a factor check compares against, and `aud` is
/// checked before any of them.
pub fn user_key(user: &crate::suite::User, secret: &str) -> String {
    user_key_shifted(user, secret, &BTreeMap::new())
        .expect("no claim is moved, so nothing here can be refused")
}

/// The same token with some of its time claims moved off now.
///
/// The offsets are seconds and they are read at the moment the case is
/// asked rather than when the suite was loaded, so a long run does not
/// drift into asking a different question than the one written down.
/// Only the three claims that are about time can be moved: everything
/// else in the token is what the seeded person is, and a case that
/// wanted to change one of those would be asking about a different
/// person rather than about a different time.
pub fn user_key_shifted(
    user: &crate::suite::User,
    secret: &str,
    shift: &BTreeMap<String, i64>,
) -> Result<String, String> {
    let iat = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    // Every offset is from now, exp included, so a case that says
    // `{"exp": -10}` is a token that ran out ten seconds ago rather
    // than one that runs out ten seconds before it would have. A claim
    // nobody moved keeps the value the ordinary token has.
    let at = |claim: &str, unmoved: u64| -> Result<u64, String> {
        match shift.get(claim) {
            Some(by) => (iat as i64)
                .checked_add(*by)
                .filter(|t| *t >= 0)
                .map(|t| t as u64)
                .ok_or_else(|| format!("{claim} moved by {by} is not a time")),
            None => Ok(unmoved),
        }
    };
    if let Some(claim) = shift
        .keys()
        .find(|c| !matches!(c.as_str(), "iat" | "nbf" | "exp"))
    {
        return Err(format!(
            "{claim} is not a claim about time, so there is no offset to give it"
        ));
    }
    let mut claims = serde_json::json!({
        "iss": "zou",
        "sub": user.id,
        "aud": "authenticated",
        "role": "authenticated",
        "email": user.email,
        "phone": "",
        "session_id": user.session_id,
        "is_anonymous": false,
        "aal": "aal1",
        "amr": [{"method": "password", "timestamp": iat}],
        "app_metadata": {"provider": "email", "providers": ["email"]},
        "user_metadata": {},
        "iat": iat,
        "exp": iat + 3600,
    });
    claims["iat"] = at("iat", iat)?.into();
    claims["exp"] = at("exp", iat + 3600)?.into();
    // Unlike the other two, a token that says nothing about when it
    // starts working is the ordinary token, so this one is only there
    // when a case asks for it.
    if shift.contains_key("nbf") {
        claims["nbf"] = at("nbf", iat)?.into();
    }
    Ok(jwt::mint(&claims, secret.as_bytes()))
}

/// What a suite needs the project configured as, over and above the
/// database it is pointed at.
///
/// Every field here is something the reference was configured with when
/// the suite was recorded, so a target told anything else would be
/// answering a different question. They travel together because they
/// arrive together, out of the suite's own `cases.json`, and because a
/// bare `true` at the end of an argument list says nothing at the call
/// site about which flag it is.
pub struct Shape<'a> {
    /// Where a suite keeps its tables, first one first, since the first
    /// is the one a request that names no schema gets. A suite derived
    /// from upstream's fixtures keeps them where upstream keeps them.
    pub schemas: &'a [String],
    /// What a suite calls its unauthenticated role, which is upstream's
    /// name for it in a suite derived from upstream's fixtures.
    pub anon_role: &'a str,
    /// Whether the project this suite is asked as lets somebody sign in
    /// without saying who they are. Off in every suite but the one that
    /// is about it, because it is off in a Supabase project that has
    /// changed nothing, and a signup with no identifier is read as an
    /// anonymous one either way: with the flag off that is a refusal,
    /// and the refusal is a case in the auth suite already.
    pub anonymous_users: bool,
    /// The provider and the written down codes a phone suite is asked
    /// with, and nothing at all in every other suite, which is phone
    /// sign in off.
    pub sms: Option<&'a crate::suite::Sms>,
}

/// Start zou against `dsn` on a free port and wait until it answers.
///
/// `s3` is the pair the S3 surface is asked with. It is the one piece
/// of configuration that is not derived from the secret: a signature is
/// checked against a key pair rather than minted from a JWT secret, so
/// both targets in a diff have to be told the same pair by hand.
pub fn start(
    dsn: &str,
    secret: &[u8],
    shape: Shape<'_>,
    s3: Credentials,
) -> Result<Served, String> {
    start_at(0, dsn, None, secret, shape, s3)
}

/// The same, on a port somebody has to know in advance.
///
/// A suite written here is asked over a client this harness controls,
/// so it does not care what port zou got. A suite written in another
/// language is asked over a client somebody else wrote, and all that
/// one takes is a url, so it has to be a url that was agreed on.
///
/// `holder` is the node that writes the project, when this one is a fan
/// out node in front of it. Set, and every question a socket asks that
/// only the tenant or the database can answer goes up a link to that
/// url, which is the thing a suite run against this one is asking about.
pub fn start_at(
    port: u16,
    dsn: &str,
    holder: Option<&str>,
    secret: &[u8],
    shape: Shape<'_>,
    s3: Credentials,
) -> Result<Served, String> {
    // Bound here rather than inside the thread, so a port that cannot
    // be had is an error the caller sees instead of a run that hangs.
    let listener =
        TcpListener::bind(("127.0.0.1", port)).map_err(|e| format!("binding a port: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("the port it got: {e}"))?
        .port();
    let url = format!("http://127.0.0.1:{port}");
    let texter = match shape.sms {
        None => None,
        Some(sms) if sms.provider == "twilio" => {
            // The credentials are the reference's, which are not
            // credentials: every number a phone suite uses has its code
            // written down, so the send path is never taken and nothing
            // is ever signed with them. The api root is moved off
            // Twilio all the same, so a case that did fall through
            // fails against a closed port here rather than reaching out
            // of the machine the suite is running on.
            let mut twilio =
                zou_server::sms::Twilio::new("ACconformance", "conformance", "MGconformance");
            twilio.base = "http://127.0.0.1:1".to_string();
            Some(std::sync::Arc::new(twilio) as std::sync::Arc<dyn zou_server::sms::Sender>)
        }
        Some(sms) => return Err(format!("no provider here is called {}", sms.provider)),
    };
    let cfg = Config {
        // The key a project derives from its own secret, which every
        // other way of running zou sets too. A target that published an
        // empty key set here would be a target the demos cannot use and
        // the suites would be asking a server nobody runs.
        jwt_keys: Some(zou_server::jwt::derived_keys(secret)),
        jwt_secret: secret.to_vec(),
        pg: Some(dsn.to_string()),
        holder: holder.map(str::to_string),
        external_url: Some(url.clone()),
        // The one piece of configuration that is read from the
        // environment here, because it is the one a suite cannot carry:
        // a provider is a client id, a secret and somewhere to send the
        // browser, and none of the three belong in a file. A run with
        // nothing set gets no providers, which is what every suite in
        // this repository is recorded against.
        oauth: zou_server::oauth::from_env()?,
        // And the second, for the same reason from the other end: a
        // project's own postgres function, named in the project's own
        // config.toml, which this harness has no config.toml to read
        // and could not invent. A run with nothing set gets no hooks,
        // which is what every suite here is recorded against. The chat
        // demo sets both variables, because the claim its policies
        // read is minted by the function its migration installs.
        hook: zou_server::hook::from_env()?,
        // What the reference is configured with, in the same order,
        // since the first is the one a request that names no schema
        // gets.
        schemas: shape.schemas.to_vec(),
        anon_role: shape.anon_role.to_string(),
        anonymous_users: shape.anonymous_users,
        mailer_autoconfirm: true,
        phone_enabled: shape.sms.is_some(),
        sms: zou_server::sms::Settings {
            test_otp: shape.sms.map(|s| s.test_otp.clone()).unwrap_or_default(),
            // The reference is started with no wait between codes, so a
            // suite can ask for two in the time one case takes. Every
            // other setting is the one a project that changed nothing
            // has, which is what the reference was started with too.
            max_frequency: 0,
            ..zou_server::sms::Settings::default()
        },
        texter,
        // Somewhere to put object bytes, under the port it answers on
        // so that two runs on one machine cannot read each other's.
        // A directory rather than a bucket: the suite is about what
        // the api says, and what the store is is the one thing an
        // answer never mentions.
        objects: Some(
            std::env::temp_dir()
                .join(format!("zou-conformance-objects-{port}"))
                .to_string_lossy()
                .to_string(),
        ),
        s3: Some(s3),
        ..Config::default()
    };
    std::thread::spawn(move || {
        if let Err(message) = serve_blocking(listener, cfg) {
            eprintln!("zou: {message}");
        }
    });
    wait_for(&url)?;
    warm(&url, secret)?;
    Ok(Served { url })
}

/// Poll until something answers, which is quick, but not instant: the
/// runtime and the pool are built after the thread starts.
fn wait_for(url: &str) -> Result<(), String> {
    // A second, not thirty: a poll that hangs is a poll that should be
    // tried again rather than waited on.
    let agent = agent(1);
    let health = format!("{url}/auth/v1/health");
    for _ in 0..100 {
        if agent.get(&health).call().is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(format!("{url} never answered"))
}

/// Answering is not the same as being ready. zou installs the auth
/// schema on the first connection it takes out of the pool, and the
/// health endpoint answers without taking one, so a server that has
/// said hello may still be a server with no auth.users in the database
/// behind it. Anything applied in between, a fixture with a foreign key
/// into auth.users for instance, would fail on a race nobody can see.
///
/// So ask it for something that has to reach postgres, and treat any
/// answer at all as the pool having been used. The status does not
/// matter here: an empty schema is a legitimate 200 and an unreadable
/// one is a legitimate error, and either way the bootstrap has run by
/// the time the response comes back.
fn warm(url: &str, secret: &[u8]) -> Result<(), String> {
    let secret = String::from_utf8_lossy(secret).into_owned();
    let anon = key("anon", &secret);
    // Thirty seconds, because this one is allowed to be slow: it is the
    // request that dials postgres, takes the advisory lock and applies
    // the whole auth schema.
    agent(30)
        .get(&format!("{url}/rest/v1/"))
        .header("apikey", &anon)
        .call()
        .map_err(|e| format!("{url} answered health but not rest: {e}"))?;
    Ok(())
}

fn agent(seconds: u64) -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_global(Some(Duration::from_secs(seconds)))
        .build()
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The keys are the point of minting them here: a target told to
    /// use the same secret hands out the same key, so a case sent to
    /// two targets is the same request twice.
    #[test]
    fn the_same_secret_and_role_is_the_same_key() {
        let a = key(
            "anon",
            "super-secret-jwt-token-with-at-least-32-characters-long",
        );
        let b = key(
            "anon",
            "super-secret-jwt-token-with-at-least-32-characters-long",
        );
        assert_eq!(a, b);
        assert_ne!(
            a,
            key(
                "service_role",
                "super-secret-jwt-token-with-at-least-32-characters-long"
            )
        );
    }

    fn person() -> crate::suite::User {
        crate::suite::User {
            id: "f0a2c7d4-9b31-4e58-8c76-2a5d1e3f4b60".to_string(),
            email: "person@zou.test".to_string(),
            session_id: "a3f5c108-2b64-4e97-83d1-6c0a9e7b2d45".to_string(),
        }
    }

    fn claims_of(token: &str) -> serde_json::Value {
        let payload = token.split('.').nth(1).expect("three parts");
        serde_json::from_slice(&base64_url(payload)).expect("json claims")
    }

    /// An ordinary token says nothing about when it starts working,
    /// which is what makes a case that moves `nbf` a case at all.
    #[test]
    fn a_token_nobody_moved_has_no_nbf_on_it() {
        let claims = claims_of(&user_key(&person(), "s"));
        assert!(claims.get("nbf").is_none(), "{claims}");
        assert_eq!(
            claims["exp"].as_i64().unwrap() - claims["iat"].as_i64().unwrap(),
            3600
        );
    }

    #[test]
    fn an_offset_moves_the_claim_it_names_and_leaves_the_others() {
        let shift = BTreeMap::from([("nbf".to_string(), 3600)]);
        let claims = claims_of(&user_key_shifted(&person(), "s", &shift).unwrap());
        let iat = claims["iat"].as_i64().unwrap();
        assert_eq!(claims["nbf"].as_i64().unwrap(), iat + 3600);
        assert_eq!(claims["exp"].as_i64().unwrap(), iat + 3600);
        // Still the same person, since a case about a time claim is not
        // a case about somebody else.
        assert_eq!(claims["sub"], person().id);
        assert_eq!(claims["role"], "authenticated");
    }

    /// Every offset is from now, so this is ten seconds past its exp
    /// rather than ten seconds short of the hour it would have had.
    #[test]
    fn an_offset_backwards_is_a_token_that_ran_out() {
        let shift = BTreeMap::from([("exp".to_string(), -10)]);
        let claims = claims_of(&user_key_shifted(&person(), "s", &shift).unwrap());
        assert_eq!(
            claims["exp"].as_i64().unwrap() - claims["iat"].as_i64().unwrap(),
            -10
        );
    }

    /// Everything else in the token says who the caller is, and a case
    /// that moved one of those would be asking about a different person
    /// rather than about a different moment. Better refused than
    /// quietly written into a recording.
    #[test]
    fn nothing_but_the_three_claims_about_time_can_be_moved() {
        let shift = BTreeMap::from([("role".to_string(), 1)]);
        let why = user_key_shifted(&person(), "s", &shift).unwrap_err();
        assert!(why.contains("not a claim about time"), "{why}");
    }

    #[test]
    fn a_key_carries_the_role_it_was_asked_for() {
        let key = key("service_role", "s");
        let payload = key.split('.').nth(1).expect("three parts");
        let bytes = base64_url(payload);
        let claims: serde_json::Value = serde_json::from_slice(&bytes).expect("json claims");
        assert_eq!(claims["role"], "service_role");
    }

    fn base64_url(text: &str) -> Vec<u8> {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut bits = 0u32;
        let mut have = 0;
        let mut out = Vec::new();
        for byte in text.bytes() {
            let at = ALPHABET.iter().position(|c| *c == byte).expect("base64url");
            bits = (bits << 6) | at as u32;
            have += 6;
            if have >= 8 {
                have -= 8;
                out.push((bits >> have) as u8);
            }
        }
        out
    }
}
