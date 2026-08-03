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

/// Start zou against `dsn` on a free port and wait until it answers.
///
/// `schemas` comes from the suite, because a suite derived from
/// upstream's fixtures keeps its tables where upstream keeps them and
/// the reference is configured to match.
pub fn start(dsn: &str, secret: &[u8], schemas: &[String]) -> Result<Served, String> {
    // Bound here rather than inside the thread, so a port that cannot
    // be had is an error the caller sees instead of a run that hangs.
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| format!("binding a port: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("the port it got: {e}"))?
        .port();
    let url = format!("http://127.0.0.1:{port}");
    let cfg = Config {
        jwt_secret: secret.to_vec(),
        pg: Some(dsn.to_string()),
        external_url: Some(url.clone()),
        // What the reference is configured with, in the same order,
        // since the first is the one a request that names no schema
        // gets.
        schemas: schemas.to_vec(),
        mailer_autoconfirm: true,
        ..Config::default()
    };
    std::thread::spawn(move || {
        if let Err(message) = serve_blocking(listener, cfg) {
            eprintln!("zou: {message}");
        }
    });
    wait_for(&url)?;
    Ok(Served { url })
}

/// Poll until something answers, which is quick, but not instant: the
/// runtime and the pool are built after the thread starts.
fn wait_for(url: &str) -> Result<(), String> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_global(Some(Duration::from_secs(1)))
        .build()
        .into();
    let health = format!("{url}/auth/v1/health");
    for _ in 0..100 {
        if agent.get(&health).call().is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(format!("{url} never answered"))
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
