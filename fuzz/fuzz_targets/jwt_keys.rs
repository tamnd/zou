//! Run with: cargo +nightly fuzz run jwt_keys
//!
//! Two json documents an operator hands the server: GOTRUE_JWT_KEYS,
//! which is the private set, and the jwks a project mid rotation is
//! configured to verify against. Neither is request data, but both are
//! parsed at startup and a panic there is a server that will not boot
//! over a typo, so no input may panic either.
//!
//! A key set that parses also has to hold together: what the jwks
//! endpoint publishes has to parse back as a verification set, and a
//! token the set signs has to verify against the set's own verifiers.
//! A configuration that parses and then cannot sign anything anybody
//! can check is worse than one that refused to load.

#![no_main]

use libfuzzer_sys::fuzz_target;
use zou_server::jwt::{self, Jwks, KeySet};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

fuzz_target!(|data: &str| {
    let _ = Jwks::parse(data);
    let Ok(keys) = KeySet::parse(data) else {
        return;
    };
    // What the jwks endpoint publishes has to parse back as a
    // verification set, except when there is nothing to publish: a set
    // of symmetric keys publishes none of them, because handing out an
    // hmac secret hands out the ability to sign, and a jwks with no
    // keys in it is a configuration error rather than a set.
    let published = keys.published();
    let empty = published["keys"].as_array().is_none_or(Vec::is_empty);
    let parsed = Jwks::parse(&published.to_string());
    assert_eq!(
        parsed.is_ok(),
        !empty,
        "the published set and what reads it disagree: {published}"
    );

    // No exp, so nothing here depends on the clock.
    let claims = serde_json::json!({"role": "authenticated", "sub": "fuzz"});
    let token = keys.sign(&claims);
    let verifiers = keys.verifiers();
    let back = jwt::verify_any(&token, SECRET, Some(&verifiers))
        .expect("a token the set signed has to verify against the set");
    assert_eq!(back.claims, claims, "the claims did not round trip");
});
