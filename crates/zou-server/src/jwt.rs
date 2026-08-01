//! JWT verification for both key formats Supabase runs today.
//!
//! The legacy format is HS256: the anon and service_role keys an app
//! configures are JWTs signed with the project's jwt_secret, and so is
//! every user access token GoTrue issues under it. The new signing
//! keys are asymmetric, ES256 on P-256, published as a JWKS, which is
//! what [`Jwks`] and [`verify_any`] handle.
//!
//! Verification is deliberately small: split, decode, check the
//! signature, check expiry. The algorithm branch is taken from the
//! header exactly once and each branch only accepts its own key
//! material, so alg none and confusion attacks (an ES256 public key
//! replayed as an HMAC secret) die at the first field. Apikeys stay
//! HS256 only through [`verify`], because the legacy key format is the
//! only JWT shaped apikey Supabase ever issued.

use base64ct::{Base64UrlUnpadded, Encoding};
use hmac::{Hmac, KeyInit, Mac};
use p256::ecdsa::signature::Verifier as _;
use p256::ecdsa::{Signature, VerifyingKey};
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
    UnknownKey,
}

impl Reject {
    pub fn as_str(&self) -> &'static str {
        match self {
            Reject::Malformed => "malformed JWT",
            Reject::WrongAlgorithm => "unsupported JWT algorithm",
            Reject::BadSignature => "invalid JWT signature",
            Reject::Expired => "JWT expired",
            Reject::UnknownKey => "no JWKS key matches this token",
        }
    }
}

/// The published public keys of a project on the new signing key
/// format: ES256 on P-256, the shape Supabase serves from its jwks
/// endpoint. Unsupported key types in the set are skipped, a project
/// mid rotation can carry entries this verifier never picks.
pub struct Jwks {
    keys: Vec<Jwk>,
}

struct Jwk {
    kid: Option<String>,
    key: VerifyingKey,
}

impl Jwks {
    /// Parse standard JWKS JSON. Errors are configuration problems in
    /// the operator's words, not request time rejects.
    pub fn parse(json: &str) -> Result<Jwks, String> {
        let doc: serde_json::Value =
            serde_json::from_str(json).map_err(|e| format!("jwks is not json: {e}"))?;
        let entries = doc
            .get("keys")
            .and_then(|k| k.as_array())
            .ok_or("jwks has no keys array")?;
        let mut keys = Vec::new();
        for entry in entries {
            let kty = entry.get("kty").and_then(|v| v.as_str());
            let crv = entry.get("crv").and_then(|v| v.as_str());
            if kty != Some("EC") || crv != Some("P-256") {
                continue;
            }
            let x = coord(entry, "x")?;
            let y = coord(entry, "y")?;
            let mut sec1 = vec![0x04];
            sec1.extend_from_slice(&x);
            sec1.extend_from_slice(&y);
            let key = VerifyingKey::from_sec1_bytes(&sec1)
                .map_err(|_| "jwks key is not a valid P-256 point".to_string())?;
            let kid = entry
                .get("kid")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            keys.push(Jwk { kid, key });
        }
        if keys.is_empty() {
            return Err("jwks carries no usable P-256 keys".to_string());
        }
        Ok(Jwks { keys })
    }

    /// The keys worth trying for a token: a kid match when the header
    /// names one, every key when it does not.
    fn candidates(&self, kid: Option<&str>) -> Vec<&VerifyingKey> {
        match kid {
            Some(kid) => self
                .keys
                .iter()
                .filter(|k| k.kid.as_deref() == Some(kid))
                .map(|k| &k.key)
                .collect(),
            None => self.keys.iter().map(|k| &k.key).collect(),
        }
    }
}

fn coord(entry: &serde_json::Value, name: &str) -> Result<Vec<u8>, String> {
    let v = entry
        .get(name)
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("jwks EC key misses {name}"))?;
    let bytes =
        Base64UrlUnpadded::decode_vec(v).map_err(|_| format!("jwks {name} is not base64url"))?;
    if bytes.len() != 32 {
        return Err(format!("jwks {name} is not 32 bytes"));
    }
    Ok(bytes)
}

/// Seconds since the epoch, the clock exp is checked against.
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The three dot separated parts plus the parsed header.
struct Parts<'a> {
    header: serde_json::Value,
    signed: &'a str,
    payload: &'a str,
    sig: Vec<u8>,
}

fn split(token: &str) -> Result<Parts<'_>, Reject> {
    let mut parts = token.split('.');
    let (Some(header), Some(payload), Some(sig), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(Reject::Malformed);
    };
    let header_json = Base64UrlUnpadded::decode_vec(header).map_err(|_| Reject::Malformed)?;
    let header_json: serde_json::Value =
        serde_json::from_slice(&header_json).map_err(|_| Reject::Malformed)?;
    let sig = Base64UrlUnpadded::decode_vec(sig).map_err(|_| Reject::Malformed)?;
    let signed_len = header.len() + 1 + payload.len();
    Ok(Parts {
        header: header_json,
        signed: &token[..signed_len],
        payload,
        sig,
    })
}

/// Decode the payload and finish: expiry check, role extraction.
fn accept(payload: &str) -> Result<Verified, Reject> {
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

fn verify_hs256(parts: &Parts, secret: &[u8]) -> Result<(), Reject> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).map_err(|_| Reject::BadSignature)?;
    mac.update(parts.signed.as_bytes());
    mac.verify_slice(&parts.sig)
        .map_err(|_| Reject::BadSignature)
}

fn verify_es256(parts: &Parts, jwks: &Jwks) -> Result<(), Reject> {
    let sig = Signature::from_slice(&parts.sig).map_err(|_| Reject::BadSignature)?;
    let kid = parts.header.get("kid").and_then(|v| v.as_str());
    let candidates = jwks.candidates(kid);
    if candidates.is_empty() {
        return Err(Reject::UnknownKey);
    }
    for key in candidates {
        if key.verify(parts.signed.as_bytes(), &sig).is_ok() {
            return Ok(());
        }
    }
    Err(Reject::BadSignature)
}

/// Verify `token` against `secret`, HS256 only. This is the apikey
/// path, the legacy key format is the only JWT shaped apikey there is.
pub fn verify(token: &str, secret: &[u8]) -> Result<Verified, Reject> {
    let parts = split(token)?;
    if parts.header.get("alg").and_then(|a| a.as_str()) != Some("HS256") {
        return Err(Reject::WrongAlgorithm);
    }
    verify_hs256(&parts, secret)?;
    accept(parts.payload)
}

/// Verify `token` against whichever key material its header names:
/// HS256 through the project secret, ES256 through the JWKS when one
/// is configured. This is the bearer path, a user access token can be
/// on either format depending on the project's signing key migration.
pub fn verify_any(token: &str, secret: &[u8], jwks: Option<&Jwks>) -> Result<Verified, Reject> {
    let parts = split(token)?;
    match parts.header.get("alg").and_then(|a| a.as_str()) {
        Some("HS256") => verify_hs256(&parts, secret)?,
        Some("ES256") => match jwks {
            Some(jwks) => verify_es256(&parts, jwks)?,
            None => return Err(Reject::WrongAlgorithm),
        },
        _ => return Err(Reject::WrongAlgorithm),
    }
    accept(parts.payload)
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
    use p256::ecdsa::SigningKey;
    use p256::ecdsa::signature::Signer as _;

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
        assert_eq!(
            verify_any(&token, SECRET, None).unwrap_err(),
            Reject::WrongAlgorithm
        );
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

    /// A deterministic ES256 keypair, its JWKS, and a signer for it.
    fn es256_fixture() -> (SigningKey, String) {
        let sk = SigningKey::from_slice(&[7u8; 32]).expect("a small scalar is valid");
        let point = sk.verifying_key().to_sec1_point(false);
        let jwks = serde_json::json!({
            "keys": [{
                "kty": "EC",
                "crv": "P-256",
                "kid": "key-2026",
                "x": Base64UrlUnpadded::encode_string(point.x().unwrap()),
                "y": Base64UrlUnpadded::encode_string(point.y().unwrap()),
            }]
        });
        (sk, jwks.to_string())
    }

    fn mint_es256(claims: &serde_json::Value, sk: &SigningKey, kid: &str) -> String {
        let header = serde_json::json!({"alg": "ES256", "typ": "JWT", "kid": kid});
        let header = Base64UrlUnpadded::encode_string(header.to_string().as_bytes());
        let payload = Base64UrlUnpadded::encode_string(claims.to_string().as_bytes());
        let signed = format!("{header}.{payload}");
        let sig: Signature = sk.sign(signed.as_bytes());
        format!(
            "{signed}.{}",
            Base64UrlUnpadded::encode_string(&sig.to_bytes())
        )
    }

    #[test]
    fn an_es256_token_verifies_through_the_jwks() {
        let (sk, jwks) = es256_fixture();
        let jwks = Jwks::parse(&jwks).unwrap();
        let token = mint_es256(
            &serde_json::json!({"role": "authenticated", "sub": "user-9"}),
            &sk,
            "key-2026",
        );
        let v = verify_any(&token, SECRET, Some(&jwks)).unwrap();
        assert_eq!(v.role.as_deref(), Some("authenticated"));
        assert_eq!(v.claims["sub"], "user-9");
    }

    #[test]
    fn an_unknown_kid_is_refused_before_any_crypto() {
        let (sk, jwks) = es256_fixture();
        let jwks = Jwks::parse(&jwks).unwrap();
        let token = mint_es256(&serde_json::json!({"role": "anon"}), &sk, "rotated-away");
        assert_eq!(
            verify_any(&token, SECRET, Some(&jwks)).unwrap_err(),
            Reject::UnknownKey
        );
    }

    #[test]
    fn a_tampered_es256_payload_is_a_bad_signature() {
        let (sk, jwks) = es256_fixture();
        let jwks = Jwks::parse(&jwks).unwrap();
        let token = mint_es256(&serde_json::json!({"role": "anon"}), &sk, "key-2026");
        let mut parts: Vec<&str> = token.split('.').collect();
        let forged = Base64UrlUnpadded::encode_string(br#"{"role":"service_role"}"#);
        parts[1] = &forged;
        assert_eq!(
            verify_any(&parts.join("."), SECRET, Some(&jwks)).unwrap_err(),
            Reject::BadSignature
        );
    }

    #[test]
    fn es256_without_a_jwks_is_refused_not_downgraded() {
        let (sk, _) = es256_fixture();
        let token = mint_es256(&serde_json::json!({"role": "anon"}), &sk, "key-2026");
        assert_eq!(
            verify_any(&token, SECRET, None).unwrap_err(),
            Reject::WrongAlgorithm
        );
    }

    #[test]
    fn the_apikey_path_never_accepts_es256() {
        let (sk, _) = es256_fixture();
        let token = mint_es256(
            &serde_json::json!({"role": "service_role"}),
            &sk,
            "key-2026",
        );
        assert_eq!(verify(&token, SECRET).unwrap_err(), Reject::WrongAlgorithm);
    }

    #[test]
    fn an_hs256_forgery_with_the_public_key_as_secret_fails() {
        // The classic confusion attack: sign HS256 with the public
        // key bytes and hope the verifier feeds them to hmac. The alg
        // branch sends HS256 to the project secret only.
        let (sk, jwks_json) = es256_fixture();
        let point = sk.verifying_key().to_sec1_point(false);
        let forged = mint(
            &serde_json::json!({"role": "service_role"}),
            point.as_bytes(),
        );
        let jwks = Jwks::parse(&jwks_json).unwrap();
        assert_eq!(
            verify_any(&forged, SECRET, Some(&jwks)).unwrap_err(),
            Reject::BadSignature
        );
    }

    #[test]
    fn garbage_jwks_is_a_config_error() {
        assert!(Jwks::parse("not json").is_err());
        assert!(Jwks::parse(r#"{"keys": []}"#).is_err());
        assert!(Jwks::parse(r#"{"keys": [{"kty": "RSA"}]}"#).is_err());
        assert!(
            Jwks::parse(r#"{"keys": [{"kty": "EC", "crv": "P-256", "x": "AAAA", "y": "AAAA"}]}"#)
                .is_err()
        );
    }
}
