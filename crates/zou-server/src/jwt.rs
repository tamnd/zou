//! HS256 JWTs, the only algorithm the Supabase legacy key format uses.
//!
//! The anon and service_role keys an app configures are JWTs signed
//! with the project's jwt_secret, and every user access token GoTrue
//! issues is another one. Verification here is deliberately small:
//! split, decode, recompute the mac, compare through the hmac crate's
//! constant time verify, then check expiry. The header must name HS256
//! exactly, so alg none and algorithm confusion never get past the
//! first field. JWKS and asymmetric keys come later with the auth
//! service and get their own path.

use base64ct::{Base64UrlUnpadded, Encoding};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

/// A verified token: the claim set as parsed JSON plus the fields the
/// request context needs pulled out.
#[derive(Debug)]
pub struct Verified {
    pub claims: serde_json::Value,
    pub role: Option<String>,
}

/// Why a token was rejected. The strings are for logs and error
/// bodies, callers branch on nothing finer than pass or fail.
#[derive(Debug, PartialEq)]
pub enum Reject {
    Malformed,
    WrongAlgorithm,
    BadSignature,
    Expired,
}

impl Reject {
    pub fn as_str(&self) -> &'static str {
        match self {
            Reject::Malformed => "malformed JWT",
            Reject::WrongAlgorithm => "unsupported JWT algorithm, only HS256 is accepted",
            Reject::BadSignature => "invalid JWT signature",
            Reject::Expired => "JWT expired",
        }
    }
}

/// Seconds since the epoch, the clock exp is checked against.
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Verify `token` against `secret` and hand back its claims.
pub fn verify(token: &str, secret: &[u8]) -> Result<Verified, Reject> {
    let mut parts = token.split('.');
    let (Some(header), Some(payload), Some(sig), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(Reject::Malformed);
    };

    let header_json = Base64UrlUnpadded::decode_vec(header).map_err(|_| Reject::Malformed)?;
    let header_json: serde_json::Value =
        serde_json::from_slice(&header_json).map_err(|_| Reject::Malformed)?;
    if header_json.get("alg").and_then(|a| a.as_str()) != Some("HS256") {
        return Err(Reject::WrongAlgorithm);
    }

    let sig = Base64UrlUnpadded::decode_vec(sig).map_err(|_| Reject::Malformed)?;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).map_err(|_| Reject::BadSignature)?;
    mac.update(header.as_bytes());
    mac.update(b".");
    mac.update(payload.as_bytes());
    mac.verify_slice(&sig).map_err(|_| Reject::BadSignature)?;

    let claims = Base64UrlUnpadded::decode_vec(payload).map_err(|_| Reject::Malformed)?;
    let claims: serde_json::Value =
        serde_json::from_slice(&claims).map_err(|_| Reject::Malformed)?;
    if let Some(exp) = claims.get("exp").and_then(|e| e.as_u64())
        && exp <= now()
    {
        return Err(Reject::Expired);
    }
    let role = claims
        .get("role")
        .and_then(|r| r.as_str())
        .map(str::to_string);
    Ok(Verified { claims, role })
}

/// Sign `claims` into a token. zou dev mints the anon and service_role
/// keys with this at boot, and the tests mint everything else.
pub fn mint(claims: &serde_json::Value, secret: &[u8]) -> String {
    let header = Base64UrlUnpadded::encode_string(br#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = Base64UrlUnpadded::encode_string(claims.to_string().as_bytes());
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(header.as_bytes());
    mac.update(b".");
    mac.update(payload.as_bytes());
    let sig = Base64UrlUnpadded::encode_string(&mac.finalize().into_bytes());
    format!("{header}.{payload}.{sig}")
}

/// The claim set for a project key, shaped like the Supabase legacy
/// anon and service_role keys: iss, role, iat, and a ten year exp.
pub fn key_claims(role: &str) -> serde_json::Value {
    let iat = now();
    serde_json::json!({
        "iss": "zou",
        "role": role,
        "iat": iat,
        "exp": iat + 10 * 365 * 24 * 3600,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

    #[test]
    fn mint_then_verify_round_trips_the_claims() {
        let token = mint(&key_claims("anon"), SECRET);
        let v = verify(&token, SECRET).unwrap();
        assert_eq!(v.role.as_deref(), Some("anon"));
        assert_eq!(v.claims["iss"], "zou");
    }

    #[test]
    fn the_wrong_secret_is_a_bad_signature() {
        let token = mint(&key_claims("anon"), SECRET);
        assert_eq!(
            verify(&token, b"another-secret").unwrap_err(),
            Reject::BadSignature
        );
    }

    #[test]
    fn a_tampered_payload_is_a_bad_signature() {
        let token = mint(&key_claims("anon"), SECRET);
        let mut parts: Vec<&str> = token.split('.').collect();
        let forged = Base64UrlUnpadded::encode_string(br#"{"role":"service_role"}"#);
        parts[1] = &forged;
        assert_eq!(
            verify(&parts.join("."), SECRET).unwrap_err(),
            Reject::BadSignature
        );
    }

    #[test]
    fn expired_tokens_are_rejected() {
        let claims = serde_json::json!({"role": "anon", "exp": 1});
        let token = mint(&claims, SECRET);
        assert_eq!(verify(&token, SECRET).unwrap_err(), Reject::Expired);
    }

    #[test]
    fn alg_none_never_gets_in() {
        let header = Base64UrlUnpadded::encode_string(br#"{"alg":"none","typ":"JWT"}"#);
        let payload = Base64UrlUnpadded::encode_string(br#"{"role":"service_role"}"#);
        let token = format!("{header}.{payload}.");
        assert_eq!(verify(&token, SECRET).unwrap_err(), Reject::WrongAlgorithm);
    }

    #[test]
    fn noise_is_malformed_not_a_panic() {
        for bad in ["", "a", "a.b", "a.b.c.d", "ยง.!!.??", "a.b.c"] {
            assert!(matches!(
                verify(bad, SECRET),
                Err(Reject::Malformed | Reject::WrongAlgorithm | Reject::BadSignature)
            ));
        }
    }

    #[test]
    fn tokens_without_exp_pass_signature_checks() {
        let token = mint(&serde_json::json!({"role": "anon"}), SECRET);
        assert!(verify(&token, SECRET).is_ok());
    }
}
