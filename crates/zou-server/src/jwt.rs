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
use p256::ecdsa::signature::{Signer as _, Verifier as _};
use p256::ecdsa::{Signature, SigningKey, VerifyingKey};
use sha2::{Digest as _, Sha256};

/// A verified token: the claim set as parsed JSON plus the fields the
/// request context needs pulled out.
#[derive(Debug)]
pub struct Verified {
    pub claims: serde_json::Value,
    pub role: Option<String>,
}

/// How strictly the time claims are read, which is not one question but
/// two, because the two servers zou answers as do not agree.
///
/// Both numbers were measured rather than read off a README, against
/// the pinned references in the conformance stack, and both libraries
/// have moved their defaults across versions so the measurement is the
/// only thing worth trusting. PostgREST 14.15 verifies with jose, which
/// allows thirty seconds either side of `exp`, `nbf` and `iat`: a token
/// expired half a minute ago is still answered 200, and one issued a
/// minute from now is refused with `JWT issued at future`. GoTrue
/// 2.194.0 verifies with golang-jwt, whose validator has no leeway at
/// all and does not look at `iat`: a token one second past `exp` is
/// refused, and one issued an hour from now is fine.
///
/// The difference is not cosmetic. A phone with a clock thirty seconds
/// slow is an ordinary thing, and a project moving to zou would find
/// its rest calls starting to fail where they used to work if this were
/// rounded to one number for the whole server.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Clock {
    /// PostgREST's reading, for `/rest/v1`.
    Postgrest,
    /// GoTrue's, which is also what everything else here uses: a signed
    /// url, a storage request and a realtime socket are all better off
    /// with the strict reading, and none of them is PostgREST.
    Exact,
}

impl Clock {
    /// Seconds a claim may be out by before it counts.
    fn leeway(&self) -> u64 {
        match self {
            Clock::Postgrest => 30,
            Clock::Exact => 0,
        }
    }

    /// Whether a token issued in the future is refused. Only jose reads
    /// `iat` as a thing that can be wrong.
    fn reads_iat(&self) -> bool {
        *self == Clock::Postgrest
    }
}

/// Why a token was rejected. The strings are for logs and error
/// bodies, callers branch on nothing finer than pass or fail.
#[derive(Debug, PartialEq)]
pub enum Reject {
    Malformed,
    /// Carrying the algorithm the header named, because the message a
    /// caller of the auth surface gets quotes it back.
    WrongAlgorithm(String),
    BadSignature,
    Expired,
    /// `nbf` says the token does not start working until later.
    TooEarly,
    /// `iat` says the token was issued in the future, which only the
    /// rest surface treats as a reason to refuse.
    IssuedLater,
    UnknownKey,
}

impl Reject {
    pub fn as_str(&self) -> &'static str {
        match self {
            Reject::Malformed => "malformed JWT",
            Reject::WrongAlgorithm(_) => "unsupported JWT algorithm",
            Reject::BadSignature => "invalid JWT signature",
            Reject::Expired => "JWT expired",
            // Both of these are PostgREST's own wording for the two
            // claims it reads besides exp, under the same PGRST303.
            Reject::TooEarly => "JWT not yet valid",
            Reject::IssuedLater => "JWT issued at future",
            Reject::UnknownKey => "no JWKS key matches this token",
        }
    }

    /// The same refusal in the words GoTrue uses, which is golang-jwt's
    /// error chain with a sentence of GoTrue's own in front of it.
    ///
    /// The auth surface answers with this and the rest of zou does not,
    /// because a client talking to /auth/v1 is talking to something
    /// that has always been GoTrue, and one talking to /rest/v1 has
    /// always been talking to PostgREST.
    ///
    /// Two of these are broader than upstream's. A token that has three
    /// segments but does not decode is a malformed token here and
    /// upstream says which part failed to decode, and an unknown kid is
    /// a bad signature here, which is what it amounts to: nothing this
    /// project trusts signed it.
    pub fn gotrue(&self) -> String {
        let why = match self {
            Reject::Malformed => {
                "token is malformed: token contains an invalid number of segments".to_string()
            }
            Reject::WrongAlgorithm(alg) => {
                format!("token signature is invalid: signing method {alg} is invalid")
            }
            Reject::BadSignature | Reject::UnknownKey => {
                "token signature is invalid: signature is invalid".to_string()
            }
            Reject::Expired => "token has invalid claims: token is expired".to_string(),
            Reject::TooEarly => "token has invalid claims: token is not valid yet".to_string(),
            // The auth surface never raises this one, golang-jwt not
            // reading iat at all, but the enum is shared and a match
            // that guessed would be worse than a sentence that is
            // never printed.
            Reject::IssuedLater => "token has invalid claims: token is not valid yet".to_string(),
        };
        format!("invalid JWT: unable to parse or verify signature, {why}")
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
    key: Public,
}

/// What a key in the verification set can check a signature with. An
/// hmac secret is here because a project mid rotation names its old
/// symmetric key by kid like any other, and GoTrue honours that kid
/// rather than always reaching for the project secret.
enum Public {
    Ec(Box<VerifyingKey>),
    Oct(Vec<u8>),
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
            keys.push(Jwk {
                kid,
                key: Public::Ec(Box::new(key)),
            });
        }
        if keys.is_empty() {
            return Err("jwks carries no usable P-256 keys".to_string());
        }
        Ok(Jwks { keys })
    }

    /// Everything a project can verify with: the keys an operator
    /// pointed at, plus the public half of the keys this server signs
    /// with. Both are needed at once during a rotation, and neither
    /// replaces the other.
    pub fn and(mut self, more: Jwks) -> Jwks {
        self.keys.extend(more.keys);
        self
    }

    /// The keys worth trying for a token: a kid match when the header
    /// names one, every key when it does not.
    fn candidates(&self, kid: Option<&str>) -> Vec<&VerifyingKey> {
        self.keys
            .iter()
            .filter(|k| kid.is_none() || k.kid.as_deref() == kid)
            .filter_map(|k| match &k.key {
                Public::Ec(key) => Some(key.as_ref()),
                Public::Oct(_) => None,
            })
            .collect()
    }

    /// The hmac secret a kid names, when it names one in this set.
    /// Without a match the caller falls back to the project secret,
    /// which is what a token minted before any of this was configured
    /// was signed with.
    fn secret_for(&self, kid: Option<&str>) -> Option<&[u8]> {
        let kid = kid?;
        self.keys
            .iter()
            .filter(|k| k.kid.as_deref() == Some(kid))
            .find_map(|k| match &k.key {
                Public::Oct(secret) => Some(secret.as_slice()),
                Public::Ec(_) => None,
            })
    }
}

/// The project's own signing keys, GoTrue's GOTRUE_JWT_KEYS: a json
/// array of private JWKs, of which exactly one carries `sign` in its
/// key_ops and is therefore the key new tokens are signed with. The
/// others are the standby key and the ones rotated away from, kept so
/// that tokens still in flight verify until they expire.
///
/// Supabase's console calls those states standby, in use, previously
/// used and revoked. Revoked is the absence of the key from this array,
/// everything else is a key with `verify` alone.
pub struct KeySet {
    keys: Vec<Private>,
    /// Index into `keys` of the one that signs. Fixed at parse time,
    /// because a set with none or several is a configuration error and
    /// not something to discover while answering a request.
    signing: usize,
}

struct Private {
    kid: String,
    material: Secret,
}

enum Secret {
    /// A P-256 keypair. The public half is derived from the private
    /// scalar rather than read from x and y, so a jwk whose coordinates
    /// disagree with its d cannot publish a key that verifies nothing.
    Ec(Box<SigningKey>),
    Oct(Vec<u8>),
}

impl KeySet {
    /// Parse GOTRUE_JWT_KEYS. Errors are the operator's to fix and are
    /// worded for them, and they happen at startup rather than at the
    /// first request.
    pub fn parse(json: &str) -> Result<KeySet, String> {
        let doc: serde_json::Value =
            serde_json::from_str(json).map_err(|e| format!("jwt keys are not json: {e}"))?;
        let entries = doc
            .as_array()
            .ok_or("jwt keys must be a json array of private jwks")?;
        let mut keys = Vec::new();
        let mut signing = Vec::new();
        for entry in entries {
            let kid = entry
                .get("kid")
                .and_then(|v| v.as_str())
                .ok_or("a jwt key has no kid, and a key that cannot be named cannot be rotated")?
                .to_string();
            let ops: Vec<&str> = entry
                .get("key_ops")
                .and_then(|v| v.as_array())
                .map(|v| v.iter().filter_map(|o| o.as_str()).collect())
                .unwrap_or_default();
            let material = material_of(entry, &kid)?;
            if ops.contains(&"sign") {
                signing.push(keys.len());
            }
            keys.push(Private { kid, material });
        }
        match signing.len() {
            1 => Ok(KeySet {
                keys,
                signing: signing[0],
            }),
            0 => Err("no signing key detected, one jwt key needs sign in its key_ops".to_string()),
            _ => Err("multiple signing keys detected, only 1 signing key is supported".to_string()),
        }
    }

    /// The public half of the set, as the jwks endpoint serves it.
    /// Symmetric keys are left out: publishing an hmac secret hands out
    /// the ability to sign, which is the whole reason the asymmetric
    /// format exists.
    pub fn published(&self) -> serde_json::Value {
        let keys: Vec<serde_json::Value> = self
            .keys
            .iter()
            .filter_map(|k| match &k.material {
                Secret::Ec(sk) => {
                    let point = sk.verifying_key().to_sec1_point(false);
                    Some(serde_json::json!({
                        "kty": "EC",
                        "crv": "P-256",
                        "kid": k.kid,
                        "alg": "ES256",
                        "use": "sig",
                        "key_ops": ["verify"],
                        "x": Base64UrlUnpadded::encode_string(point.x().expect("an uncompressed point has x")),
                        "y": Base64UrlUnpadded::encode_string(point.y().expect("an uncompressed point has y")),
                    }))
                }
                Secret::Oct(_) => None,
            })
            .collect();
        serde_json::json!({ "keys": keys })
    }

    /// The same keys as something [`verify_any`] can check against, the
    /// symmetric ones included: this end holds them already, and a
    /// token naming one by kid was signed with it.
    pub fn verifiers(&self) -> Jwks {
        Jwks {
            keys: self
                .keys
                .iter()
                .map(|k| Jwk {
                    kid: Some(k.kid.clone()),
                    key: match &k.material {
                        Secret::Ec(sk) => Public::Ec(Box::new(*sk.verifying_key())),
                        Secret::Oct(secret) => Public::Oct(secret.clone()),
                    },
                })
                .collect(),
        }
    }

    /// Sign with the one key that signs, naming it in the header so a
    /// verifier picks the right one out of the published set without
    /// trying all of them.
    pub fn sign(&self, claims: &serde_json::Value) -> String {
        let key = &self.keys[self.signing];
        match &key.material {
            Secret::Ec(sk) => {
                let header = serde_json::json!({"alg": "ES256", "typ": "JWT", "kid": key.kid});
                let signed = signing_input(&header, claims);
                let sig: Signature = sk.sign(signed.as_bytes());
                format!(
                    "{signed}.{}",
                    Base64UrlUnpadded::encode_string(&sig.to_bytes())
                )
            }
            Secret::Oct(secret) => {
                let header = serde_json::json!({"alg": "HS256", "typ": "JWT", "kid": key.kid});
                let signed = signing_input(&header, claims);
                format!("{signed}.{}", hs256(&signed, secret))
            }
        }
    }
}

/// The signing key a project has when nobody handed it one: a P-256
/// keypair derived from the project's own secret, in the shape
/// GOTRUE_JWT_KEYS takes, so a project that was created with a secret
/// and nothing else still signs its access tokens asymmetrically and
/// still publishes a jwks somebody can verify against.
///
/// Derived rather than generated and stored for two reasons. Every
/// node serving the project arrives at the same key from the one fact
/// they all already have, so there is no key to distribute and no
/// registry format to migrate. And the key is exactly as strong a
/// credential as the secret it comes out of, which is the truth of the
/// arrangement anyway: whoever holds a project's secret can already
/// mint that project's service_role key.
///
/// The scalar is a counter appended to a labelled hash of the secret,
/// stepped until it lands in range, which it does on the first try for
/// all but a vanishing fraction of secrets. The kid names the public
/// half, so it changes when and only when the key does.
pub fn derived_keys(secret: &[u8]) -> String {
    let sk = derived_signing_key(secret);
    let point = sk.verifying_key().to_sec1_point(false);
    let x = point.x().expect("an uncompressed point has x");
    let y = point.y().expect("an uncompressed point has y");
    let named = Sha256::new()
        .chain_update(b"zou jwt kid v1")
        .chain_update(x)
        .chain_update(y)
        .finalize();
    let kid: String = named.iter().take(16).map(|b| format!("{b:02x}")).collect();
    serde_json::json!([{
        "kty": "EC",
        "crv": "P-256",
        "kid": kid,
        "alg": "ES256",
        "use": "sig",
        "key_ops": ["sign", "verify"],
        "d": Base64UrlUnpadded::encode_string(&sk.to_bytes()),
        "x": Base64UrlUnpadded::encode_string(x),
        "y": Base64UrlUnpadded::encode_string(y),
    }])
    .to_string()
}

fn derived_signing_key(secret: &[u8]) -> SigningKey {
    for counter in 0u8..=255 {
        let candidate = Sha256::new()
            .chain_update(b"zou jwt signing key v1")
            .chain_update(secret)
            .chain_update([counter])
            .finalize();
        if let Ok(sk) = SigningKey::from_slice(&candidate) {
            return sk;
        }
    }
    unreachable!("a P-256 scalar is found long before 256 tries")
}

fn material_of(entry: &serde_json::Value, kid: &str) -> Result<Secret, String> {
    match entry.get("kty").and_then(|v| v.as_str()) {
        Some("EC") => {
            if entry.get("crv").and_then(|v| v.as_str()) != Some("P-256") {
                return Err(format!(
                    "jwt key {kid} is not on P-256, which is all ES256 has"
                ));
            }
            let d = coord(entry, "d")?;
            let sk = SigningKey::from_slice(&d)
                .map_err(|_| format!("jwt key {kid} has a d that is not a P-256 scalar"))?;
            Ok(Secret::Ec(Box::new(sk)))
        }
        Some("oct") => {
            let k = entry
                .get("k")
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("jwt key {kid} is an oct key with no k"))?;
            let secret = Base64UrlUnpadded::decode_vec(k)
                .map_err(|_| format!("jwt key {kid} has a k that is not base64url"))?;
            Ok(Secret::Oct(secret))
        }
        other => Err(format!(
            "jwt key {kid} is {}, and zou signs and verifies EC on P-256 and oct",
            other.unwrap_or("missing its kty")
        )),
    }
}

fn signing_input(header: &serde_json::Value, claims: &serde_json::Value) -> String {
    format!(
        "{}.{}",
        Base64UrlUnpadded::encode_string(header.to_string().as_bytes()),
        Base64UrlUnpadded::encode_string(claims.to_string().as_bytes())
    )
}

fn hs256(signed: &str, secret: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(signed.as_bytes());
    Base64UrlUnpadded::encode_string(&mac.finalize().into_bytes())
}

/// What signs the access tokens this server issues: the project secret
/// on the legacy format, or the one key in the set that signs. Both are
/// verifiable by anything holding the project's keys, the difference is
/// whether verifying needs the secret or only the published half.
pub enum Signer<'a> {
    Secret(&'a [u8]),
    Keys(&'a KeySet),
}

impl Signer<'_> {
    pub fn sign(&self, claims: &serde_json::Value) -> String {
        match self {
            Signer::Secret(secret) => mint(claims, secret),
            Signer::Keys(keys) => keys.sign(claims),
        }
    }
}

fn coord(entry: &serde_json::Value, name: &str) -> Result<Vec<u8>, String> {
    let v = entry
        .get(name)
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("jwk EC key misses {name}"))?;
    let bytes =
        Base64UrlUnpadded::decode_vec(v).map_err(|_| format!("jwk {name} is not base64url"))?;
    if bytes.len() != 32 {
        return Err(format!("jwk {name} is not 32 bytes"));
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

/// What a token says it was signed with, for a caller that has to
/// choose its words by the algorithm rather than by the failure.
///
/// The functions surface needs it: the reference has one refusal for a
/// legacy HS256 token that does not verify and another for an
/// asymmetric one, and both of them arrive here as the same
/// [`Reject::BadSignature`]. None for anything that is not three
/// segments with a json header on the front, which is a token that
/// never got as far as having an algorithm.
///
/// This reads the header itself rather than going through `split`,
/// and the difference is the point: split decodes the signature too, so
/// a token whose signature is not base64 at all has no parts, while it
/// very much has an algorithm. The reference reads the header of
/// `<hs256 header>.<payload>.WRONG` and refuses it as an HS256 token.
///
/// An `alg` that is not a string still has a value, and the reference
/// quotes it back: `{"alg":123}` earns `Unsupported JWT algorithm 123`.
/// So this renders whatever is there rather than insisting on a string
/// and calling the rest nothing.
pub fn algorithm(token: &str) -> Option<String> {
    let mut parts = token.split('.');
    let (Some(header), Some(_), Some(_), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    let header = Base64UrlUnpadded::decode_vec(header).ok()?;
    let header: serde_json::Value = serde_json::from_slice(&header).ok()?;
    match header.get("alg")? {
        serde_json::Value::String(alg) => Some(alg.clone()),
        other => Some(other.to_string()),
    }
}

/// Decode the payload and finish: the time claims, then role
/// extraction.
///
/// A claim that is not a number is not read at all, by either library
/// and so not here, which is why each of these is an `as_u64` that
/// falls through when it fails rather than a refusal.
fn accept(payload: &str, clock: Clock) -> Result<Verified, Reject> {
    let claims = Base64UrlUnpadded::decode_vec(payload).map_err(|_| Reject::Malformed)?;
    let claims: serde_json::Value =
        serde_json::from_slice(&claims).map_err(|_| Reject::Malformed)?;
    let now = now();
    let leeway = clock.leeway();
    if let Some(exp) = claims.get("exp").and_then(|e| e.as_u64())
        && exp + leeway <= now
    {
        return Err(Reject::Expired);
    }
    if let Some(nbf) = claims.get("nbf").and_then(|e| e.as_u64())
        && nbf > now + leeway
    {
        return Err(Reject::TooEarly);
    }
    if clock.reads_iat()
        && let Some(iat) = claims.get("iat").and_then(|e| e.as_u64())
        && iat > now + leeway
    {
        return Err(Reject::IssuedLater);
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
    verify_by(token, secret, Clock::Exact)
}

/// The same, reading the time claims the way `clock` says.
pub fn verify_by(token: &str, secret: &[u8], clock: Clock) -> Result<Verified, Reject> {
    let parts = split(token)?;
    let alg = parts.header.get("alg").and_then(|a| a.as_str());
    if alg != Some("HS256") {
        return Err(Reject::WrongAlgorithm(alg.unwrap_or_default().to_string()));
    }
    verify_hs256(&parts, secret)?;
    accept(parts.payload, clock)
}

/// Verify `token` against whichever key material its header names:
/// HS256 through the project secret, ES256 through the JWKS when one
/// is configured. This is the bearer path, a user access token can be
/// on either format depending on the project's signing key migration.
pub fn verify_any(token: &str, secret: &[u8], jwks: Option<&Jwks>) -> Result<Verified, Reject> {
    verify_any_by(token, secret, jwks, Clock::Exact)
}

/// The same, reading the time claims the way `clock` says.
pub fn verify_any_by(
    token: &str,
    secret: &[u8],
    jwks: Option<&Jwks>,
    clock: Clock,
) -> Result<Verified, Reject> {
    let parts = split(token)?;
    let alg = parts.header.get("alg").and_then(|a| a.as_str());
    match alg {
        Some("HS256") => {
            // A kid naming a symmetric key in the set wins, and the
            // project secret is what a token carrying no kid, or one
            // this set never heard of, was signed with. That fallback
            // is what keeps tokens issued before a rotation working.
            let kid = parts.header.get("kid").and_then(|v| v.as_str());
            let secret = jwks.and_then(|j| j.secret_for(kid)).unwrap_or(secret);
            verify_hs256(&parts, secret)?
        }
        Some("ES256") => match jwks {
            Some(jwks) => verify_es256(&parts, jwks)?,
            None => return Err(Reject::WrongAlgorithm("ES256".to_string())),
        },
        _ => return Err(Reject::WrongAlgorithm(alg.unwrap_or_default().to_string())),
    }
    accept(parts.payload, clock)
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
    fn the_leeway_is_thirty_seconds_on_one_clock_and_none_on_the_other() {
        let then = now();
        // Ten seconds past is inside jose's window and outside
        // golang-jwt's, which is the whole of the difference.
        let token = mint(
            &serde_json::json!({"role": "anon", "exp": then - 10}),
            SECRET,
        );
        assert!(verify_by(&token, SECRET, Clock::Postgrest).is_ok());
        assert_eq!(
            verify_by(&token, SECRET, Clock::Exact).unwrap_err(),
            Reject::Expired
        );

        // A minute past is past both.
        let token = mint(
            &serde_json::json!({"role": "anon", "exp": then - 60}),
            SECRET,
        );
        assert_eq!(
            verify_by(&token, SECRET, Clock::Postgrest).unwrap_err(),
            Reject::Expired
        );
    }

    #[test]
    fn a_token_that_has_not_started_yet_is_refused_on_both_clocks() {
        let then = now();
        let token = mint(
            &serde_json::json!({"role": "anon", "nbf": then + 10}),
            SECRET,
        );
        assert!(verify_by(&token, SECRET, Clock::Postgrest).is_ok());
        assert_eq!(
            verify_by(&token, SECRET, Clock::Exact).unwrap_err(),
            Reject::TooEarly
        );

        let token = mint(
            &serde_json::json!({"role": "anon", "nbf": then + 600}),
            SECRET,
        );
        assert_eq!(
            verify_by(&token, SECRET, Clock::Postgrest).unwrap_err(),
            Reject::TooEarly
        );
        assert_eq!(Reject::TooEarly.as_str(), "JWT not yet valid");
    }

    #[test]
    fn only_one_clock_reads_iat() {
        let token = mint(
            &serde_json::json!({"role": "anon", "iat": now() + 600}),
            SECRET,
        );
        assert_eq!(
            verify_by(&token, SECRET, Clock::Postgrest).unwrap_err(),
            Reject::IssuedLater
        );
        assert!(
            verify_by(&token, SECRET, Clock::Exact).is_ok(),
            "golang-jwt does not look at iat"
        );
        assert_eq!(Reject::IssuedLater.as_str(), "JWT issued at future");
    }

    #[test]
    fn the_default_clock_is_the_exact_one() {
        let token = mint(
            &serde_json::json!({"role": "anon", "exp": now() - 10}),
            SECRET,
        );
        assert_eq!(verify(&token, SECRET).unwrap_err(), Reject::Expired);
        assert_eq!(
            verify_any(&token, SECRET, None).unwrap_err(),
            Reject::Expired
        );
    }

    #[test]
    fn alg_none_never_gets_in() {
        let header = Base64UrlUnpadded::encode_string(br#"{"alg":"none","typ":"JWT"}"#);
        let payload = Base64UrlUnpadded::encode_string(br#"{"role":"service_role"}"#);
        let token = format!("{header}.{payload}.");
        let named = Reject::WrongAlgorithm("none".to_string());
        assert_eq!(verify(&token, SECRET).unwrap_err(), named);
        assert_eq!(verify_any(&token, SECRET, None).unwrap_err(), named);
    }

    #[test]
    fn noise_is_malformed_not_a_panic() {
        for bad in ["", "a", "a.b", "a.b.c.d", "ยง.!!.??", "a.b.c"] {
            assert!(matches!(
                verify(bad, SECRET),
                Err(Reject::Malformed | Reject::WrongAlgorithm(_) | Reject::BadSignature)
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
            Reject::WrongAlgorithm("ES256".to_string())
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
        assert_eq!(
            verify(&token, SECRET).unwrap_err(),
            Reject::WrongAlgorithm("ES256".to_string())
        );
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

    /// A private ES256 jwk, the shape Supabase hands an operator when
    /// it creates a signing key. `ops` is what decides whether it is
    /// the key in use or one being kept around to verify with.
    fn private_ec(kid: &str, d: u8, ops: &[&str]) -> serde_json::Value {
        let sk = SigningKey::from_slice(&[d; 32]).expect("a small scalar is valid");
        let point = sk.verifying_key().to_sec1_point(false);
        serde_json::json!({
            "kty": "EC",
            "crv": "P-256",
            "kid": kid,
            "alg": "ES256",
            "key_ops": ops,
            "d": Base64UrlUnpadded::encode_string(&sk.to_bytes()),
            "x": Base64UrlUnpadded::encode_string(point.x().unwrap()),
            "y": Base64UrlUnpadded::encode_string(point.y().unwrap()),
        })
    }

    #[test]
    fn a_key_set_signs_with_the_one_key_that_signs() {
        let keys = KeySet::parse(
            &serde_json::json!([
                private_ec("standby", 3, &["verify"]),
                private_ec("in-use", 4, &["sign", "verify"]),
            ])
            .to_string(),
        )
        .unwrap();

        let token = keys.sign(&serde_json::json!({"role": "authenticated"}));
        let header: serde_json::Value = serde_json::from_slice(
            &Base64UrlUnpadded::decode_vec(token.split('.').next().unwrap()).unwrap(),
        )
        .unwrap();
        assert_eq!(header["alg"], "ES256");
        assert_eq!(header["kid"], "in-use", "the standby key does not sign");

        let v = verify_any(&token, SECRET, Some(&keys.verifiers())).unwrap();
        assert_eq!(v.role.as_deref(), Some("authenticated"));
    }

    #[test]
    fn a_rotation_keeps_verifying_what_the_old_key_signed() {
        let before = KeySet::parse(
            &serde_json::json!([private_ec("2025", 5, &["sign", "verify"])]).to_string(),
        )
        .unwrap();
        let old_token = before.sign(&serde_json::json!({"role": "authenticated"}));

        // What a rotation looks like in this config: the same array,
        // with sign moved to the new key and the old one left in.
        let after = KeySet::parse(
            &serde_json::json!([
                private_ec("2025", 5, &["verify"]),
                private_ec("2026", 6, &["sign", "verify"]),
            ])
            .to_string(),
        )
        .unwrap();
        let new_token = after.sign(&serde_json::json!({"role": "authenticated"}));
        let verifiers = after.verifiers();

        assert!(
            verify_any(&old_token, SECRET, Some(&verifiers)).is_ok(),
            "a token in flight when the key rotated still verifies"
        );
        assert!(verify_any(&new_token, SECRET, Some(&verifiers)).is_ok());

        // Revoking the old key is dropping it from the array, and then
        // its tokens stop verifying. That is the point of revoking.
        let revoked = KeySet::parse(
            &serde_json::json!([private_ec("2026", 6, &["sign", "verify"])]).to_string(),
        )
        .unwrap();
        assert_eq!(
            verify_any(&old_token, SECRET, Some(&revoked.verifiers())).unwrap_err(),
            Reject::UnknownKey
        );
    }

    #[test]
    fn the_published_set_carries_public_halves_only() {
        let keys = KeySet::parse(
            &serde_json::json!([
                private_ec("ec", 8, &["sign", "verify"]),
                {"kty": "oct", "kid": "legacy", "alg": "HS256", "key_ops": ["verify"],
                 "k": Base64UrlUnpadded::encode_string(SECRET)},
            ])
            .to_string(),
        )
        .unwrap();

        let published = keys.published();
        let entries = published["keys"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "an hmac secret is never published");
        assert_eq!(entries[0]["kid"], "ec");
        assert_eq!(entries[0]["alg"], "ES256");
        assert_eq!(entries[0]["use"], "sig");
        assert_eq!(entries[0]["key_ops"], serde_json::json!(["verify"]));
        assert!(entries[0].get("d").is_none(), "the private scalar stays");
        assert!(entries[0].get("k").is_none());

        // And what it publishes is what verifies its tokens, which is
        // the only reason to publish anything.
        let token = keys.sign(&serde_json::json!({"role": "authenticated"}));
        let public = Jwks::parse(&published.to_string()).unwrap();
        assert!(verify_any(&token, b"not the secret", Some(&public)).is_ok());
    }

    /// A project that was handed nothing but a secret still has a
    /// signing key, and it is the same key every time, because a key
    /// that changed on a restart would end every session on the
    /// restart.
    #[test]
    fn the_key_derived_from_a_secret_is_the_same_key_every_time() {
        let one = derived_keys(b"a-project-secret");
        assert_eq!(one, derived_keys(b"a-project-secret"));
        assert_ne!(one, derived_keys(b"another-project-secret"));

        let keys = KeySet::parse(&one).expect("a derived set parses as a set");
        let token = keys.sign(&serde_json::json!({"role": "authenticated", "sub": "u-1"}));
        let published = Jwks::parse(&keys.published().to_string()).expect("a public set");
        assert!(
            verify_any(&token, b"not the secret", Some(&published)).is_ok(),
            "the published half verifies what the private half signed"
        );

        // ES256 with the kid in the header, which is what a verifier
        // that holds several keys picks by, and what a library that
        // reads a jwks refuses a token for not having.
        let header = split(&token).unwrap().header;
        assert_eq!(header["alg"], "ES256");
        assert!(header["kid"].as_str().is_some_and(|kid| kid.len() == 32));

        // Two secrets, two keys, and neither project's tokens verify
        // against the other's published set.
        let other = KeySet::parse(&derived_keys(b"another-project-secret")).unwrap();
        let elsewhere = Jwks::parse(&other.published().to_string()).unwrap();
        assert_eq!(
            verify_any(&token, b"not the secret", Some(&elsewhere)).unwrap_err(),
            Reject::UnknownKey
        );
    }

    /// The secret is the credential the key comes out of, and it must
    /// not be readable back out of anything the key publishes.
    #[test]
    fn a_derived_key_publishes_no_secret() {
        let keys = KeySet::parse(&derived_keys(b"a-project-secret")).unwrap();
        let published = keys.published().to_string();
        assert!(!published.contains("a-project-secret"));
        assert!(!published.contains(&Base64UrlUnpadded::encode_string(b"a-project-secret")));
        assert!(
            !published.contains("\"d\""),
            "the private scalar is not published either"
        );
    }

    #[test]
    fn a_symmetric_key_is_still_named_by_its_kid() {
        // GoTrue turns a plain jwt secret into an oct jwk, and a token
        // naming that kid is verified with that key rather than with
        // whatever the project secret happens to be now.
        let keys = KeySet::parse(
            &serde_json::json!([
                {"kty": "oct", "kid": "legacy", "alg": "HS256", "key_ops": ["sign", "verify"],
                 "k": Base64UrlUnpadded::encode_string(b"the-old-project-secret")},
            ])
            .to_string(),
        )
        .unwrap();
        let token = keys.sign(&serde_json::json!({"role": "authenticated"}));
        assert!(verify_any(&token, SECRET, Some(&keys.verifiers())).is_ok());
        assert_eq!(
            verify_any(&token, SECRET, None).unwrap_err(),
            Reject::BadSignature,
            "without the set it falls back to the project secret, which is not it"
        );

        // A token with no kid still lands on the project secret, which
        // is what everything issued before any of this was configured
        // was signed with.
        let legacy = mint(&serde_json::json!({"role": "anon"}), SECRET);
        assert!(verify_any(&legacy, SECRET, Some(&keys.verifiers())).is_ok());
    }

    #[test]
    fn a_key_set_that_cannot_sign_is_a_config_error() {
        let one = |ops: &[&str]| serde_json::json!([private_ec("a", 9, ops)]).to_string();
        assert!(KeySet::parse(&one(&["verify"])).is_err());
        assert!(
            KeySet::parse(
                &serde_json::json!([
                    private_ec("a", 9, &["sign", "verify"]),
                    private_ec("b", 10, &["sign", "verify"]),
                ])
                .to_string()
            )
            .is_err()
        );
        assert!(KeySet::parse("not json").is_err());
        assert!(KeySet::parse(r#"{"keys": []}"#).is_err(), "not an array");
        // No kid, an unsupported curve, an unsupported key type, and a
        // d that is not a scalar. Each names the key in the message.
        assert!(KeySet::parse(r#"[{"kty": "EC", "crv": "P-256", "key_ops": ["sign"]}]"#).is_err());
        assert!(
            KeySet::parse(r#"[{"kty": "EC", "crv": "P-521", "kid": "a", "key_ops": ["sign"]}]"#)
                .is_err()
        );
        assert!(KeySet::parse(r#"[{"kty": "RSA", "kid": "a", "key_ops": ["sign"]}]"#).is_err());
        assert!(
            KeySet::parse(r#"[{"kty": "EC", "crv": "P-256", "kid": "a", "d": "AAAA"}]"#).is_err()
        );
    }

    #[test]
    fn the_signer_picks_the_key_set_over_the_secret() {
        let keys = KeySet::parse(&serde_json::json!([private_ec("k", 11, &["sign"])]).to_string())
            .unwrap();
        let claims = serde_json::json!({"role": "authenticated"});
        let by_secret = Signer::Secret(SECRET).sign(&claims);
        let by_key = Signer::Keys(&keys).sign(&claims);
        assert!(verify(&by_secret, SECRET).is_ok(), "still plain HS256");
        assert_eq!(
            verify(&by_key, SECRET).unwrap_err(),
            Reject::WrongAlgorithm("ES256".to_string()),
            "the apikey path never takes an ES256 token"
        );
        assert!(verify_any(&by_key, SECRET, Some(&keys.verifiers())).is_ok());
    }
}
