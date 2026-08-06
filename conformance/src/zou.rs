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

use std::net::TcpListener;
use std::time::Duration;

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
    let iat = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let claims = serde_json::json!({
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
    jwt::mint(&claims, secret.as_bytes())
}

/// Start zou against `dsn` on a free port and wait until it answers.
///
/// `schemas` and `anon` come from the suite, because a suite derived
/// from upstream's fixtures keeps its tables where upstream keeps them
/// and calls its unauthenticated role what upstream calls it, and the
/// reference is configured to match both.
pub fn start(dsn: &str, secret: &[u8], schemas: &[String], anon: &str) -> Result<Served, String> {
    start_at(0, dsn, secret, schemas, anon)
}

/// The same, on a port somebody has to know in advance.
///
/// A suite written here is asked over a client this harness controls,
/// so it does not care what port zou got. A suite written in another
/// language is asked over a client somebody else wrote, and all that
/// one takes is a url, so it has to be a url that was agreed on.
pub fn start_at(
    port: u16,
    dsn: &str,
    secret: &[u8],
    schemas: &[String],
    anon: &str,
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
    let cfg = Config {
        jwt_secret: secret.to_vec(),
        pg: Some(dsn.to_string()),
        external_url: Some(url.clone()),
        // The one piece of configuration that is read from the
        // environment here, because it is the one a suite cannot carry:
        // a provider is a client id, a secret and somewhere to send the
        // browser, and none of the three belong in a file. A run with
        // nothing set gets no providers, which is what every suite in
        // this repository is recorded against.
        oauth: zou_server::oauth::from_env()?,
        // What the reference is configured with, in the same order,
        // since the first is the one a request that names no schema
        // gets.
        schemas: schemas.to_vec(),
        anon_role: anon.to_string(),
        mailer_autoconfirm: true,
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
