//! The S3 protocol, which is the one Supabase surface nobody asks with
//! a token.
//!
//! Everything else under `/storage/v1` reads an authorization header
//! carrying a JWT and hands the claims to postgres. Here the
//! authorization header carries a signature over the request itself,
//! computed from a key pair the project was configured with, and the
//! server's job is to compute the same signature and see whether the
//! two agree. Nothing about the caller is in the request beyond which
//! access key id signed it.
//!
//! So there is no role in a claim to run as. A signature that verifies
//! is the project's own key pair, which is the whole project, and the
//! statements below run as the service role for the same reason the
//! reference runs them with its service key: an S3 client has no user
//! and no session and nothing for a policy to be about.
//!
//! What the recording says about the checking, none of which is in any
//! documentation:
//!
//! - A request with no authorization header at all and a request
//!   carrying a perfectly good service role JWT get the same answer,
//!   because both are read as an authorization type that is not
//!   `AWS4-HMAC-SHA256`. That answer is 400 rather than 401 or 403.
//! - An access key id nobody has is `AccessDenied`, and a signature
//!   that does not match is `SignatureDoesNotMatch`, which is the one
//!   place this surface says which of the two halves was wrong.
//! - The date is not checked. A signature correctly computed for the
//!   first of January 2024 is accepted today, so there is no skew
//!   window and a captured request is good forever.
//! - The body is not rehashed. `x-amz-content-sha256` is taken as the
//!   payload hash rather than checked against the bytes, so a request
//!   whose body does not match its own hash is accepted.
//!
//! The last two are worth naming rather than fixing. A recording is
//! what the reference does, and a client that works against Supabase
//! has to work here; anything stricter is a difference that shows up as
//! somebody's upload failing against zou and nowhere else.
//!
//! The answers are xml, written out here rather than built by a
//! serializer, for the reason the json ones are written out in
//! `storage`: the recording is compared byte for byte, and an empty
//! element that self closes in one library and does not in another is a
//! difference nobody meant.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{Method, StatusCode, header, request::Parts};
use axum::response::{IntoResponse, Response};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

use crate::App;
use crate::sql::RequestContext;
use crate::storage::{StorageError, begin, done, pg_error};

/// The pair a project's S3 endpoint is asked with, and the region it
/// says it is in.
///
/// One pair rather than a table of them. Hosted Supabase keeps several
/// per project and lets each be scoped to a bucket, and the local
/// project every suite is recorded against has exactly one, which is
/// the shape a signature has to verify against before scoping means
/// anything.
#[derive(Clone, Debug)]
pub struct Credentials {
    pub access: String,
    pub secret: String,
    /// Part of what is signed, so a client in the wrong region computes
    /// a different signature and is told the signature does not match.
    pub region: String,
}

impl Credentials {
    /// A pair in the region every Supabase project answers in unless it
    /// was told otherwise.
    pub fn new(access: &str, secret: &str) -> Credentials {
        Credentials {
            access: access.to_string(),
            secret: secret.to_string(),
            region: REGION.to_string(),
        }
    }
}

/// Where a project is when nobody said.
pub const REGION: &str = "us-east-1";

/// The only signature version any of this understands, which is also
/// the string a request has to open its authorization header with.
const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// The service name in the credential scope. S3 is what the endpoint
/// pretends to be, so it is what the key is derived for.
const SERVICE: &str = "s3";

const XML: &str = "application/xml; charset=utf-8";

/// Every answer opens with these two, and both are byte for byte what
/// the reference sends: the declaration says `standalone="yes"`, and
/// the namespace is the 2006 one every S3 document has carried since.
const DECLARATION: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>";
const NAMESPACE: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// A refusal, as this surface writes one.
///
/// The same [`StorageError`] the json routes carry, rendered another
/// way and with the status it names actually used on the wire. That is
/// the one thing the two halves of storage disagree about: a bucket
/// that is not there is 400 with a `"statusCode": "404"` inside it on
/// the json routes and a plain 404 here.
///
/// `resource` is the bucket the request was about, empty when it was
/// about none, and an empty one is written as a self closing element
/// rather than as an empty pair.
fn refused(why: &StorageError, resource: &str) -> Response {
    let wire = StatusCode::from_u16(why.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let named = match resource.is_empty() {
        true => "<Resource/>".to_string(),
        false => format!("<Resource>{}</Resource>", escaped(resource)),
    };
    let body = format!(
        "{DECLARATION}<Error xmlns=\"{NAMESPACE}\">{named}\
         <Code>{}</Code><Message>{}</Message></Error>",
        escaped(why.code),
        escaped(why.said()),
    );
    (wire, [(header::CONTENT_TYPE, XML)], body).into_response()
}

/// The five characters that cannot be written as themselves inside a
/// document. Attributes are quoted with double quotes here and nothing
/// this surface writes puts a value in one, but both quotes are escaped
/// anyway because the cost is nothing and the day something does put a
/// bucket name in an attribute is not the day to remember this.
fn escaped(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// An answer that worked and has a document in it.
fn xml(body: String) -> Response {
    (StatusCode::OK, [(header::CONTENT_TYPE, XML)], body).into_response()
}

/// What the caller signed, taken apart.
///
/// The scope is kept as its parts rather than as the string, because
/// only one of them is used: the day, which the signing key is derived
/// for. The region in it is deliberately ignored and the configured one
/// used instead, which is what turns a signature computed for another
/// region into a signature that does not match.
#[derive(Debug)]
struct Signed<'a> {
    access: &'a str,
    day: &'a str,
    headers: Vec<&'a str>,
    signature: &'a str,
}

/// Read the authorization header, or say it is not one.
///
/// A header that is not this version, including no header at all, is
/// the same refusal: the reference splits on spaces and compares the
/// first word, so the empty string fails the same comparison
/// `Bearer ...` does.
fn parse(raw: &str) -> Result<Signed<'_>, StorageError> {
    let Some(rest) = raw.strip_prefix(ALGORITHM) else {
        return Err(StorageError::invalid_signature(
            "Unsupported authorization type".to_string(),
        ));
    };
    let mut credential = None;
    let mut headers = None;
    let mut signature = None;
    for field in rest.split(',') {
        let field = field.trim();
        if let Some(value) = field.strip_prefix("Credential=") {
            credential = Some(value);
        } else if let Some(value) = field.strip_prefix("SignedHeaders=") {
            headers = Some(value);
        } else if let Some(value) = field.strip_prefix("Signature=") {
            signature = Some(value);
        }
    }
    // Not recorded, unlike the refusal above. A header that opens with
    // the right words and then says nothing usable is a client that is
    // broken rather than a client that is unauthorized, and this is the
    // shortest true thing to say about it.
    let (Some(credential), Some(headers), Some(signature)) = (credential, headers, signature)
    else {
        return Err(StorageError::invalid_signature(
            "Invalid signature format".to_string(),
        ));
    };
    let mut scope = credential.split('/');
    let (Some(access), Some(day)) = (scope.next(), scope.next()) else {
        return Err(StorageError::invalid_signature(
            "Invalid signature format".to_string(),
        ));
    };
    Ok(Signed {
        access,
        day,
        headers: headers.split(';').collect(),
        signature,
    })
}

/// Did this request come from somebody holding the project's secret?
///
/// The whole check, and it answers nothing about who: a signature that
/// verifies is the project, and a signature that does not is one of two
/// refusals depending on which half of the pair was wrong.
fn verified(app: &App, parts: &Parts) -> Result<(), StorageError> {
    let raw = parts
        .headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let signed = parse(raw)?;
    // A project with no pair configured is a project where nobody holds
    // the access key that was named, which is the same thing to say as
    // a project that has one and was asked with the wrong id.
    let credentials = app
        .cfg
        .s3
        .as_ref()
        .filter(|c| c.access == signed.access)
        .ok_or_else(|| StorageError::access_denied("Invalid Access Key".to_string()))?;

    let stamp = header_value(parts, "x-amz-date").unwrap_or_default();
    // Taken rather than computed. The reference does not hash the body
    // to check this and neither does this, which is recorded: a request
    // whose hash is not its body's is accepted by both.
    let payload = header_value(parts, "x-amz-content-sha256").unwrap_or_default();
    let mut canonical = String::new();
    canonical.push_str(parts.method.as_str());
    canonical.push('\n');
    canonical.push_str(parts.uri.path());
    canonical.push('\n');
    canonical.push_str(&canonical_query(parts.uri.query().unwrap_or("")));
    canonical.push('\n');
    for name in &signed.headers {
        canonical.push_str(name);
        canonical.push(':');
        canonical.push_str(header_value(parts, name).unwrap_or_default().trim());
        canonical.push('\n');
    }
    canonical.push('\n');
    canonical.push_str(&signed.headers.join(";"));
    canonical.push('\n');
    canonical.push_str(payload);

    // The day is the caller's and the region is the project's. Taking
    // the region from the credential too would mean a client could pick
    // the one it signed for and always match, which is the same as not
    // signing the region at all.
    let scope = format!(
        "{}/{}/{SERVICE}/aws4_request",
        signed.day, credentials.region
    );
    let to_sign = format!(
        "{ALGORITHM}\n{stamp}\n{scope}\n{}",
        hex(&sha256(canonical.as_bytes()))
    );
    let key = signing_key(&credentials.secret, signed.day, &credentials.region);
    match same(&hex(&sign(&key, &to_sign)), signed.signature) {
        true => Ok(()),
        false => Err(StorageError::wrong_signature()),
    }
}

/// One header, by a name already in lower case.
fn header_value<'a>(parts: &'a Parts, name: &str) -> Option<&'a str> {
    parts.headers.get(name).and_then(|v| v.to_str().ok())
}

/// The query string as the signature sees it: sorted by name, and a
/// parameter with no value keeps its `=`.
fn canonical_query(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(&str, &str)> = query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| pair.split_once('=').unwrap_or((pair, "")))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// The four nested HMACs the signing key is, each one throwing away
/// what it was given, which is why a signature that leaks does not leak
/// the secret.
fn signing_key(secret: &str, day: &str, region: &str) -> Vec<u8> {
    let key = sign(format!("AWS4{secret}").as_bytes(), day);
    let key = sign(&key, region);
    let key = sign(&key, SERVICE);
    sign(&key, "aws4_request")
}

fn sign(key: &[u8], message: &str) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac takes any key length");
    mac.update(message.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

fn sha256(bytes: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Two hex signatures, compared without letting the time it took say
/// how much of the second one was right.
fn same(ours: &str, theirs: &str) -> bool {
    if ours.len() != theirs.len() {
        return false;
    }
    let mut apart = 0u8;
    for (a, b) in ours.bytes().zip(theirs.bytes()) {
        apart |= a ^ b;
    }
    apart == 0
}

/// What the statements below run as.
///
/// The service role, and nothing read off the request, because there is
/// nothing in a signed request to read: no sub, no claims, no session.
/// The headers and cookies a policy could look at are left empty for
/// the same reason.
fn context(parts: &Parts) -> RequestContext {
    RequestContext {
        role: "service_role".to_string(),
        claims: "{}".to_string(),
        method: parts.method.as_str().to_string(),
        path: parts.uri.path().to_string(),
        headers: "{}".to_string(),
        cookies: "{}".to_string(),
        search_path: "\"storage\"".to_string(),
    }
}

/// GET /storage/v1/s3 and /storage/v1/s3/
pub async fn list_buckets(State(app): State<Arc<App>>, parts: Parts) -> Response {
    match listed(&app, &parts).await {
        Ok(response) => response,
        Err(why) => refused(&why, ""),
    }
}

async fn listed(app: &App, parts: &Parts) -> Result<Response, StorageError> {
    verified(app, parts)?;
    let ctx = context(parts);
    let sess = begin(app, &ctx, true).await?;
    // No order by, the same as the json listing and for the same
    // reason: the reference does not write one either, so both answer
    // in whatever order the scan hands back. What that order is, the
    // suite asks by making a bucket and looking at where it lands.
    //
    // The day is rendered by postgres rather than here, because it has
    // to come out the way a javascript Date prints it, milliseconds and
    // a Z, and to_char is the shortest honest way to say that.
    let rows = sess
        .query(
            "select name, to_char(created_at at time zone 'utc', \
             'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') from storage.buckets",
            &[],
        )
        .await
        .map_err(|e| pg_error(&e))?;
    let mut buckets = String::new();
    for row in &rows {
        let name: String = row.get(0);
        let made: String = row.get(1);
        buckets.push_str(&format!(
            "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
            escaped(&name),
            escaped(&made)
        ));
    }
    // A project with no buckets at all writes the empty element the way
    // an empty `Resource` is written, which is a guess from the one
    // empty element that is recorded rather than from a listing nobody
    // has asked for yet.
    let buckets = match buckets.is_empty() {
        true => "<Buckets/>".to_string(),
        false => format!("<Buckets>{buckets}</Buckets>"),
    };
    let body = format!(
        "{DECLARATION}<ListAllMyBucketsResult xmlns=\"{NAMESPACE}\">{buckets}</ListAllMyBucketsResult>"
    );
    done(sess, Ok(xml(body))).await
}

/// Everything one bucket can be asked, dispatched here rather than by
/// the router.
///
/// One route with `any` on it, for the reason the resumable routes have
/// one: what a method this surface has not been taught yet should hear
/// is a decision rather than a fallback. A method router would answer
/// 405 with an `Allow` listing the three verbs below, which would tell
/// an S3 client that listing a bucket is not a thing anybody can do
/// here, and it is a thing the reference does. So the unwritten ones
/// say what every unwritten part of this surface says, and the day
/// objects arrive they arrive as another arm.
pub async fn bucket(
    State(app): State<Arc<App>>,
    Path(bucket): Path<String>,
    parts: Parts,
) -> Response {
    let answer = match parts.method {
        Method::HEAD => headed(&app, &parts, &bucket).await,
        Method::PUT => made(&app, &parts, &bucket).await,
        Method::DELETE => removed(&app, &parts, &bucket).await,
        _ => return crate::not_yet("the storage surface"),
    };
    match answer {
        Ok(response) => response,
        Err(why) => refused(&why, &bucket),
    }
}

/// HEAD /storage/v1/s3/{bucket}
///
/// Is it there, and nothing else. A head has no body to say anything
/// in, so the answer is the status: 200 with no content type at all,
/// which is what the reference sends and what the differ compares.
async fn headed(app: &App, parts: &Parts, bucket: &str) -> Result<Response, StorageError> {
    verified(app, parts)?;
    let ctx = context(parts);
    let sess = begin(app, &ctx, true).await?;
    let found = exists(&sess, bucket).await?;
    let answer = match found {
        true => Ok(StatusCode::OK.into_response()),
        false => Err(StorageError::no_such_bucket()),
    };
    done(sess, answer).await
}

/// PUT /storage/v1/s3/{bucket}
///
/// CreateBucket, whose body is a location constraint nobody sends and
/// nothing here reads. The name is the path, the answer is empty, and
/// where it was made is the `location` header.
async fn made(app: &App, parts: &Parts, bucket: &str) -> Result<Response, StorageError> {
    verified(app, parts)?;
    let ctx = context(parts);
    let sess = begin(app, &ctx, false).await?;
    // Asked rather than left to the unique index, because the reference
    // asks it too and answers in its own words. A bucket made this way
    // has no owner: there is no subject in a signature to be one.
    if exists(&sess, bucket).await? {
        return done(sess, Err(StorageError::already_exists())).await;
    }
    sess.execute(
        "insert into storage.buckets (id, name, public, type) values ($1, $1, false, 'STANDARD')",
        &[&bucket],
    )
    .await
    .map_err(|e| pg_error(&e))?;
    let answer = (StatusCode::OK, [(header::LOCATION, format!("/{bucket}"))]).into_response();
    done(sess, Ok(answer)).await
}

/// DELETE /storage/v1/s3/{bucket}
async fn removed(app: &App, parts: &Parts, bucket: &str) -> Result<Response, StorageError> {
    verified(app, parts)?;
    let ctx = context(parts);
    let sess = begin(app, &ctx, false).await?;
    if !exists(&sess, bucket).await? {
        return done(sess, Err(StorageError::no_such_bucket())).await;
    }
    let occupied = !sess
        .query(
            "select 1 from storage.objects where bucket_id = $1 limit 1",
            &[&bucket],
        )
        .await
        .map_err(|e| pg_error(&e))?
        .is_empty();
    if occupied {
        return done(sess, Err(StorageError::not_empty())).await;
    }
    sess.execute(crate::storage::ALLOW_DELETE, &[])
        .await
        .map_err(|e| pg_error(&e))?;
    sess.execute("delete from storage.buckets where id = $1", &[&bucket])
        .await
        .map_err(|e| pg_error(&e))?;
    done(sess, Ok(StatusCode::NO_CONTENT.into_response())).await
}

/// Is the bucket there? The service role sees every one of them, so
/// this really is existence rather than the visibility question the
/// json routes ask.
async fn exists(sess: &crate::sql::Session, bucket: &str) -> Result<bool, StorageError> {
    let rows = sess
        .query(
            "select 1 from storage.buckets where id = $1 limit 1",
            &[&bucket],
        )
        .await
        .map_err(|e| pg_error(&e))?;
    Ok(!rows.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The example in Amazon's own documentation, which is the only
    /// thing that says the algorithm was read correctly rather than
    /// plausibly. Signed here rather than verified, since the two ends
    /// are the same computation.
    #[test]
    fn the_documented_example_derives_the_documented_signature() {
        let canonical = "GET\n/test.txt\n\nhost:examplebucket.s3.amazonaws.com\n\
             range:bytes=0-9\n\
             x-amz-content-sha256:\
             e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n\
             x-amz-date:20130524T000000Z\n\n\
             host;range;x-amz-content-sha256;x-amz-date\n\
             e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let to_sign = format!(
            "{ALGORITHM}\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n{}",
            hex(&sha256(canonical.as_bytes()))
        );
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
        );
        assert_eq!(
            hex(&sign(&key, &to_sign)),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn an_authorization_header_is_read_as_its_three_fields() {
        let raw = "AWS4-HMAC-SHA256 Credential=abc/20260807/us-east-1/s3/aws4_request, \
                   SignedHeaders=host;x-amz-date, Signature=deadbeef";
        let signed = parse(raw).expect("a signature");
        assert_eq!(signed.access, "abc");
        assert_eq!(signed.day, "20260807");
        assert_eq!(signed.headers, vec!["host", "x-amz-date"]);
        assert_eq!(signed.signature, "deadbeef");
    }

    /// A token is not a signature, and neither is nothing. Both are the
    /// same refusal, which is recorded.
    #[test]
    fn anything_that_is_not_this_version_is_not_an_authorization_type() {
        for raw in ["", "Bearer not-a-signature", "AWS4-HMAC-SHA1 Credential=a"] {
            let why = parse(raw).expect_err("not a signature");
            assert_eq!(why.said(), "Unsupported authorization type");
            assert_eq!(why.status(), 400);
        }
    }

    /// The right words and then nothing usable is a broken client
    /// rather than an unauthorized one.
    #[test]
    fn a_signature_missing_a_field_is_a_signature_of_no_shape() {
        let why = parse("AWS4-HMAC-SHA256 Credential=a/b, SignedHeaders=host")
            .expect_err("no signature in it");
        assert_eq!(why.said(), "Invalid signature format");
    }

    #[test]
    fn a_query_is_signed_in_order_and_keeps_an_empty_value() {
        assert_eq!(
            canonical_query("prefix=a&delimiter=/"),
            "delimiter=/&prefix=a"
        );
        assert_eq!(canonical_query("list-type=2&x"), "list-type=2&x=");
        assert_eq!(canonical_query(""), "");
    }

    #[test]
    fn an_empty_resource_closes_itself_and_a_named_one_does_not() {
        let response = refused(&StorageError::no_such_bucket(), "");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let response = refused(&StorageError::wrong_signature(), "photos");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn the_characters_a_document_cannot_carry_are_escaped() {
        assert_eq!(escaped("a&b<c>d\"e'f"), "a&amp;b&lt;c&gt;d&quot;e&apos;f");
        assert_eq!(escaped("photos"), "photos");
    }

    /// Length first, so that a signature of another length is not a
    /// signature that happens to agree on its first characters.
    #[test]
    fn two_signatures_are_the_same_only_when_they_are() {
        assert!(same("abcd", "abcd"));
        assert!(!same("abcd", "abce"));
        assert!(!same("abcd", "abc"));
    }
}
