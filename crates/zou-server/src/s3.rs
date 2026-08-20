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

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{Method, StatusCode, header, request::Parts};
use axum::response::{IntoResponse, Response};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

use crate::App;
use crate::object;
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
    /// Where the project says it is, which is what a bucket answers
    /// when asked for its location.
    ///
    /// Part of what is signed, so it is also half of which signatures
    /// verify. The other half is [`REGION`]: a signature made in either
    /// is taken and one made in a third is not, which is recorded three
    /// times over against a project whose region is `local`. A client
    /// that was never told where it is signs in the default and works,
    /// and a client that read the location and signed in that works
    /// too, and those are the two things a client does.
    pub region: String,
}

impl Credentials {
    /// A pair for a project that did not say where it is, which puts it
    /// where every S3 client assumes it is.
    pub fn new(access: &str, secret: &str) -> Credentials {
        Credentials {
            access: access.to_string(),
            secret: secret.to_string(),
            region: REGION.to_string(),
        }
    }

    /// Is this a region a signature may have been computed in?
    fn takes(&self, region: &str) -> bool {
        region == self.region || region == REGION
    }
}

/// Where an S3 client signs when nobody told it otherwise, and where a
/// project is when nobody told it either.
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

/// The three characters that cannot be written as themselves inside an
/// element.
///
/// Three rather than five: an etag is a hex sum in double quotes and
/// the reference writes those quotes as themselves, so a document that
/// escaped them would carry a different etag from the one it means.
/// Quotes matter inside an attribute value and nothing this surface
/// writes puts anything in one.
fn escaped(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
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
/// two of them are used and the other two are constants. The day is
/// taken as the caller wrote it, since nothing checks the date. The
/// region is taken and then checked against the pair, rather than used
/// as it stands: a region read straight out of the credential would
/// always agree with itself, which is the same as not signing the
/// region at all.
#[derive(Debug)]
struct Signed<'a> {
    access: &'a str,
    day: &'a str,
    region: &'a str,
    headers: Vec<&'a str>,
    signature: &'a str,
}

/// Read the authorization header, or say it is not one.
///
/// A header that is not this version, including no header at all, is
/// the same refusal: the reference splits on spaces and compares the
/// first word, so the empty string fails the same comparison
/// `Bearer ...` does. What that refusal is changed in storage-api 1.69,
/// from a 400 saying the authorization type is unsupported to a 403
/// saying the signature is missing.
fn parse(raw: &str) -> Result<Signed<'_>, StorageError> {
    let Some(rest) = raw.strip_prefix(ALGORITHM) else {
        return Err(StorageError::missing_signature());
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
    let (Some(access), Some(day), Some(region)) = (scope.next(), scope.next(), scope.next()) else {
        return Err(StorageError::invalid_signature(
            "Invalid signature format".to_string(),
        ));
    };
    Ok(Signed {
        access,
        day,
        region,
        headers: headers.split(';').collect(),
        signature,
    })
}

/// Did this request come from somebody holding the project's secret?
///
/// The whole check, and it answers nothing about who: a signature that
/// verifies is the project, and a signature that does not is one of two
/// refusals depending on which half of the pair was wrong. What comes
/// back is the pair it verified against, because one answer on this
/// surface is the region out of it.
fn verified<'a>(app: &'a App, parts: &Parts) -> Result<&'a Credentials, StorageError> {
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

    // The day is the caller's and the region is the caller's out of a
    // set of two. Checking membership and then signing with what the
    // caller wrote is the same computation as trying both and taking
    // whichever matched, said once.
    if !credentials.takes(signed.region) {
        return Err(StorageError::wrong_signature());
    }
    let scope = format!("{}/{}/{SERVICE}/aws4_request", signed.day, signed.region);
    let to_sign = format!(
        "{ALGORITHM}\n{stamp}\n{scope}\n{}",
        hex(&sha256(canonical.as_bytes()))
    );
    let key = signing_key(&credentials.secret, signed.day, signed.region);
    match same(&hex(&sign(&key, &to_sign)), signed.signature) {
        true => Ok(credentials),
        false => Err(StorageError::wrong_signature()),
    }
}

/// One header, by a name already in lower case.
fn header_value<'a>(parts: &'a Parts, name: &str) -> Option<&'a str> {
    parts.headers.get(name).and_then(|v| v.to_str().ok())
}

/// The query string taken apart, with the values decoded.
///
/// Decoded here rather than by an extractor, because the signature is
/// computed over the string as it arrived and reading it twice for two
/// purposes is the only way to have both. A parameter with no value is
/// an empty one, which is what `?location` is and what tells this
/// surface which of the several documents a get is asking for.
fn query_of(parts: &Parts) -> BTreeMap<String, String> {
    let mut asked = BTreeMap::new();
    for pair in parts.uri.query().unwrap_or("").split('&') {
        if pair.is_empty() {
            continue;
        }
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        asked.insert(name.to_string(), decoded(value));
    }
    asked
}

/// A percent encoded value, as itself.
///
/// A plus is a space here, which is the html form rule rather than the
/// url one, and it is the rule every query string parser in this
/// business applies. A percent that is not followed by two hex digits
/// is left as a percent rather than refused: this runs before anything
/// has been checked, and a query nobody can read is a query about an
/// object nobody has.
fn decoded(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        match bytes[at] {
            b'+' => out.push(b' '),
            b'%' if at + 2 < bytes.len() => match u8::from_str_radix(&value[at + 1..at + 3], 16) {
                Ok(byte) => {
                    out.push(byte);
                    at += 2;
                }
                Err(_) => out.push(b'%'),
            },
            byte => out.push(byte),
        }
        at += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
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
/// 405 with an `Allow` listing the verbs below, and the things this
/// surface still owes a client are things a method router would be
/// answering for. So the unwritten ones say what every unwritten part
/// of this surface says.
///
/// A get is four questions rather than one, told apart by the query:
/// where the bucket is, whether it keeps versions, and the two versions
/// of the listing. Which is the protocol's own doing, not something
/// this dispatch invented.
pub async fn bucket(
    State(app): State<Arc<App>>,
    Path(bucket): Path<String>,
    parts: Parts,
    body: Body,
) -> Response {
    let answer = match parts.method {
        Method::GET => asked(&app, &parts, &bucket).await,
        Method::HEAD => headed(&app, &parts, &bucket).await,
        Method::PUT => made(&app, &parts, &bucket).await,
        Method::DELETE => removed(&app, &parts, &bucket).await,
        // The one post a bucket answers, told from the ones it does not
        // by a parameter with no value on it. A post carrying anything
        // else is a multipart upload being started or finished, which is
        // still owed.
        Method::POST if query_of(&parts).contains_key("delete") => {
            dropped_many(&app, &parts, &bucket, body).await
        }
        _ => return crate::not_yet("the storage surface", crate::tracked::STORAGE),
    };
    match answer {
        Ok(response) => response,
        Err(why) => refused(&why, &bucket),
    }
}

/// GET /storage/v1/s3/{bucket}
///
/// Four questions on one verb, told apart by which parameter is in the
/// query. The two that are not listings are answered before the bucket
/// is looked for at all: whether the reference checks that a bucket
/// exists before telling a client where it is is not recorded, and
/// answering out of the configuration is the cheaper of the two guesses
/// and the one that cannot be wrong about a bucket that does exist.
async fn asked(app: &App, parts: &Parts, bucket: &str) -> Result<Response, StorageError> {
    let credentials = verified(app, parts)?;
    let query = query_of(parts);
    if query.contains_key("location") {
        return Ok(xml(format!(
            "{DECLARATION}<LocationConstraint xmlns=\"{NAMESPACE}\">{}</LocationConstraint>",
            escaped(&credentials.region)
        )));
    }
    // Suspended rather than Disabled, which is the difference between a
    // bucket that never kept old versions and one that stopped. Neither
    // is true of storage, and this is what the reference says.
    if query.contains_key("versioning") {
        return Ok(xml(format!(
            "{DECLARATION}<VersioningConfiguration xmlns=\"{NAMESPACE}\">\
             <Status>Suspended</Status><MfaDelete>Disabled</MfaDelete>\
             </VersioningConfiguration>"
        )));
    }
    if query.contains_key("uploads") {
        return unfinished(app, parts, bucket).await;
    }
    listing(app, parts, bucket, &query).await
}

/// GET /storage/v1/s3/{bucket}?uploads
///
/// ListMultipartUploads: what somebody began in this bucket and never
/// finished. An upload nobody finishes stays in the table until
/// somebody drops it, so this is the only way to find out that there is
/// anything to clean up.
///
/// No prefix, no delimiter and no paging, which is three of S3's
/// parameters not read. Every upload the bucket has comes back and the
/// answer says so. The reference sends `<Prefix/>` whether or not one
/// was asked for and this sends it too, empty, because it is.
async fn unfinished(app: &App, parts: &Parts, bucket: &str) -> Result<Response, StorageError> {
    let ctx = context(parts);
    let sess = begin(app, &ctx, true).await?;
    if !exists(&sess, bucket).await? {
        return done(sess, Err(StorageError::no_such_bucket())).await;
    }
    let found = sess
        .query(
            "select key, id,
                    to_char(created_at at time zone 'utc',
                            'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')
               from storage.s3_multipart_uploads
              where bucket_id = $1
              order by key collate \"C\", created_at",
            &[&bucket],
        )
        .await
        .map_err(|e| pg_error(&e));
    let rows = done(sess, found).await?;
    let mut uploads = String::new();
    for row in &rows {
        let key: String = row.get(0);
        let id: String = row.get(1);
        let when: String = row.get(2);
        uploads.push_str(&format!(
            "<Upload><Key>{}</Key><Initiated>{}</Initiated><UploadId>{}</UploadId>\
             <StorageClass>STANDARD</StorageClass></Upload>",
            escaped(&key),
            escaped(&when),
            escaped(&id),
        ));
    }
    Ok(xml(format!(
        "{DECLARATION}<ListMultipartUploadsResult xmlns=\"{NAMESPACE}\"><Name>{}</Name>\
         <Prefix/>{uploads}<IsTruncated>false</IsTruncated><MaxUploads>{MOST_UPLOADS}</MaxUploads>\
         <KeyCount>{}</KeyCount></ListMultipartUploadsResult>",
        escaped(bucket),
        rows.len(),
    )))
}

/// The most keys one page can hold, which is also how many it holds
/// when nobody said.
const MOST_KEYS: usize = 1000;

/// What the two multipart listings say they would have stopped at.
///
/// Neither of them pages: `max-parts`, `part-number-marker`,
/// `max-uploads` and the two upload markers are all unread, and every
/// row comes back. The number is in the answers because the reference
/// puts it there, and an upload with more pieces than this in it is a
/// thing neither server has been asked about.
const MOST_PARTS: usize = 1000;
const MOST_UPLOADS: usize = 1000;

/// The most pieces an upload is allowed, which is S3's own limit and
/// the one the reference checks the number against.
const MOST_PIECES: i32 = 10_000;

/// One row of a listing, which is either an object or a folder that
/// several objects are under.
struct Listed {
    /// The key as the document writes it, which for a folder is the
    /// common prefix with its delimiter still on.
    key: String,
    folder: bool,
    /// Empty for a row whose metadata has no etag in it, which is what
    /// a row somebody inserted by hand looks like. The element is left
    /// out entirely for those rather than written empty.
    etag: String,
    size: i64,
    when: String,
}

/// ListObjects and ListObjectsV2, which are one query and two
/// documents.
///
/// The two disagree about the order of their fields, about what a page
/// is asked for with, and about whether the count of what came back is
/// worth saying. They agree about every row, so the rows are read once
/// and written twice.
async fn listing(
    app: &App,
    parts: &Parts,
    bucket: &str,
    query: &BTreeMap<String, String>,
) -> Result<Response, StorageError> {
    let v2 = query.get("list-type").map(String::as_str) == Some("2");
    let prefix = query.get("prefix").cloned().unwrap_or_default();
    let delimiter = query.get("delimiter").cloned().unwrap_or_default();
    let most = query
        .get("max-keys")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(MOST_KEYS)
        .min(MOST_KEYS);
    // The second version pages with a token it minted and the first
    // with a key the client already had. A token that says nothing this
    // server would have said is read as starting from nowhere, which is
    // the same as starting from the beginning: a page after a token
    // nobody issued is a page nobody can be wrong about.
    let after = match v2 {
        true => match query.get("continuation-token") {
            Some(token) => spent(token),
            None => query.get("start-after").cloned().unwrap_or_default(),
        },
        false => query.get("marker").cloned().unwrap_or_default(),
    };

    let ctx = context(parts);
    let sess = begin(app, &ctx, true).await?;
    if !exists(&sess, bucket).await? {
        return done(sess, Err(StorageError::no_such_bucket())).await;
    }
    // One more than the page, which is how the answer knows whether to
    // say there is another one.
    let found = page(&sess, bucket, &prefix, &delimiter, &after, most + 1).await;
    let mut rows = done(sess, found).await?;
    let truncated = rows.len() > most;
    rows.truncate(most);

    let mut contents = String::new();
    let mut folders = String::new();
    for row in &rows {
        match row.folder {
            true => folders.push_str(&format!(
                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                escaped(&row.key)
            )),
            false => contents.push_str(&entry(row)),
        }
    }
    let named = match prefix.is_empty() {
        true => "<Prefix/>".to_string(),
        false => format!("<Prefix>{}</Prefix>", escaped(&prefix)),
    };
    // The key the next page starts after, which both versions send and
    // only one of them wraps up. A page that is not truncated sends
    // neither, including the first version's marker: it names where to
    // go next rather than echoing where this page began, so there is
    // nothing to name when there is no next page.
    let last = rows.last().map(|row| row.key.clone()).unwrap_or_default();

    let body = match v2 {
        true => {
            let counted = rows.len();
            let delimited = match delimiter.is_empty() {
                true => String::new(),
                false => format!("<Delimiter>{}</Delimiter>", escaped(&delimiter)),
            };
            let token = match truncated {
                true => format!(
                    "<NextContinuationToken>{}</NextContinuationToken>",
                    escaped(&minted(&last))
                ),
                false => String::new(),
            };
            format!(
                "{DECLARATION}<ListBucketResult xmlns=\"{NAMESPACE}\">\
                 <Name>{}</Name>{named}{contents}<IsTruncated>{truncated}</IsTruncated>\
                 <MaxKeys>{most}</MaxKeys>{delimited}<KeyCount>{counted}</KeyCount>\
                 {token}{folders}</ListBucketResult>",
                escaped(bucket)
            )
        }
        false => {
            let marker = match truncated {
                true => format!("<Marker>{}</Marker>", escaped(&last)),
                false => String::new(),
            };
            format!(
                "{DECLARATION}<ListBucketResult xmlns=\"{NAMESPACE}\">\
                 <Name>{}</Name>{named}{marker}<MaxKeys>{most}</MaxKeys>\
                 <IsTruncated>{truncated}</IsTruncated>{contents}{folders}\
                 </ListBucketResult>",
                escaped(bucket)
            )
        }
    };
    Ok(xml(body))
}

/// One object, as both documents write it.
///
/// No owner element, which is a thing S3 sends and this does not: there
/// is no owner in the table for a row a signature wrote. The etag is
/// left out rather than written empty when the metadata has none, which
/// is what a row that was inserted rather than uploaded looks like and
/// what the fixture's four rows are.
fn entry(row: &Listed) -> String {
    let etag = match row.etag.is_empty() {
        true => String::new(),
        false => format!("<ETag>{}</ETag>", escaped(&row.etag)),
    };
    format!(
        "<Contents><Key>{}</Key><LastModified>{}</LastModified>{etag}\
         <Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
        escaped(&row.key),
        escaped(&row.when),
        row.size,
    )
}

/// A page of keys, with folders in it or without.
///
/// With a delimiter this is the function storage-api's own listing goes
/// through, which walks the index skipping whole folders rather than
/// reading every row under one, and which hands folders back with their
/// delimiter trimmed off and everything else null. Without one there is
/// nothing to skip and the rows are the rows.
///
/// Both are ordered by the C collation, which is the order S3 promises
/// and not the order a database in an ordinary locale sorts in.
async fn page(
    sess: &crate::sql::Session,
    bucket: &str,
    prefix: &str,
    delimiter: &str,
    after: &str,
    most: usize,
) -> Result<Vec<Listed>, StorageError> {
    let when = "to_char(updated_at at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')";
    let rows = match delimiter.is_empty() {
        true => {
            let sql = format!(
                "select name, false,
                        coalesce(metadata->>'eTag', ''),
                        coalesce((metadata->>'size')::bigint, 0),
                        coalesce({when}, '')
                   from storage.objects
                  where bucket_id = $1
                    and starts_with(name, $2)
                    and ($3 = '' or name collate \"C\" > $3)
                  order by name collate \"C\"
                  limit $4"
            );
            sess.query(&sql, &[&bucket, &prefix, &after, &(most as i64)])
                .await
        }
        false => {
            let sql = format!(
                "select name, id is null,
                        coalesce(metadata->>'eTag', ''),
                        coalesce((metadata->>'size')::bigint, 0),
                        coalesce({when}, '')
                   from storage.list_objects_with_delimiter($1, $2, $3, $4, $5, '', 'asc')"
            );
            sess.query(
                &sql,
                &[&bucket, &prefix, &delimiter, &(most as i32), &after],
            )
            .await
        }
    }
    .map_err(|e| pg_error(&e))?;
    Ok(rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let folder: bool = row.get(1);
            Listed {
                // The function trims the delimiter off a folder and the
                // document wants it on, since a common prefix is the
                // string a client puts back in the next request as its
                // prefix.
                key: match folder {
                    true => format!("{name}{delimiter}"),
                    false => name,
                },
                folder,
                etag: row.get(2),
                size: row.get(3),
                when: row.get(4),
            }
        })
        .collect())
}

/// The token a truncated page hands back.
///
/// The key it left off at, with a letter in front of it saying that is
/// what it is, base64ed so that nothing in a key has to be escaped into
/// a query string. Not a secret and not signed: a client that makes one
/// up gets a page starting wherever it said, which is a page it could
/// have asked for with start-after anyway.
fn minted(key: &str) -> String {
    use base64ct::{Base64, Encoding};
    Base64::encode_string(format!("l:{key}").as_bytes())
}

/// A token, back to the key inside it, or nothing.
fn spent(token: &str) -> String {
    use base64ct::{Base64, Encoding};
    let Ok(bytes) = Base64::decode_vec(token) else {
        return String::new();
    };
    String::from_utf8_lossy(&bytes)
        .strip_prefix("l:")
        .unwrap_or_default()
        .to_string()
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

/// POST /storage/v1/s3/{bucket}?delete
///
/// DeleteObjects, which is the only request on this surface whose body
/// is a document the server reads rather than one it writes.
///
/// Every key the body named comes back as deleted, in the order the body
/// named them rather than sorted, and one that nothing was under comes
/// back as deleted too. Both are recorded, and both are S3's rule rather
/// than the storage api's: the state the caller asked for is the state
/// afterwards, so there is nothing to report about a key that was
/// already in it.
async fn dropped_many(
    app: &App,
    parts: &Parts,
    bucket: &str,
    body: Body,
) -> Result<Response, StorageError> {
    verified(app, parts)?;
    let raw = axum::body::to_bytes(body, object::UPLOAD_LIMIT)
        .await
        .map_err(|_| StorageError::too_large())?;
    // Read before the bucket is looked for, because upstream validates a
    // body against its schema before its handler runs. A body with
    // nothing in it earns the same sentence whether or not the bucket in
    // the path is there.
    let Some(keys) = keys_in(&String::from_utf8_lossy(&raw)) else {
        return Err(StorageError::not_valid(
            "Body, Delete must be object".to_string(),
        ));
    };
    let ctx = context(parts);
    let sess = begin(app, &ctx, false).await?;
    if !exists(&sess, bucket).await? {
        return done(sess, Err(StorageError::no_such_bucket())).await;
    }
    // The versions about to lose the rows that point at them, read
    // before the delete so that their bytes can go after the commit.
    let rows = sess
        .query(
            "select id::text, coalesce(version, '') from storage.objects
              where bucket_id = $1 and name = any($2)",
            &[&bucket, &keys],
        )
        .await
        .map_err(|e| pg_error(&e))?;
    // A row with no version in it points at nothing, which is what a row
    // somebody inserted rather than uploaded looks like. There are no
    // bytes under it to go and looking for them would be asking the
    // store about a key it was never given.
    let orphaned: Vec<String> = rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .filter(|(_, version)| !version.is_empty())
        .map(|(id, version)| crate::blob::key(&id, &version))
        .collect();
    sess.execute(crate::storage::ALLOW_DELETE, &[])
        .await
        .map_err(|e| pg_error(&e))?;
    sess.execute(
        "delete from storage.objects where bucket_id = $1 and name = any($2)",
        &[&bucket, &keys],
    )
    .await
    .map_err(|e| pg_error(&e))?;
    done(sess, Ok(())).await?;
    if let Ok(blobs) = object::blobs(app) {
        for key in orphaned {
            let _ = blobs.delete(key).await;
        }
    }

    let mut deleted = String::new();
    for key in &keys {
        deleted.push_str(&format!(
            "<Deleted><Key>{}</Key></Deleted>",
            escaped(key.as_str())
        ));
    }
    Ok(xml(format!(
        "{DECLARATION}<DeleteResult xmlns=\"{NAMESPACE}\">{deleted}</DeleteResult>"
    )))
}

/// The keys a delete's body names, in the order it named them, or
/// nothing when there is no `Delete` object in it at all.
///
/// Read by finding the tags rather than by parsing the document, for the
/// reason the answers are written out rather than serialized: a parser
/// would bring its own idea of what a document means, and everything
/// that has to be read out of this one is one element deep.
///
/// `Quiet` is not looked for. It asks the server to say nothing about
/// the keys that worked, which is what a client deleting a thousand of
/// them sends, and the reference answers all thousand anyway. Recorded,
/// and copied rather than improved on, because a client that reads the
/// answer expecting silence reads it wrong against both.
fn keys_in(body: &str) -> Option<Vec<String>> {
    let at = body.find("<Delete")?;
    let rest = &body[at + "<Delete".len()..];
    // Attributes are allowed on the tag and every client sends one, so
    // the name ends at a space as well as at the bracket. Anything else
    // after it is a different element whose name merely starts the same
    // way.
    if !rest.starts_with('>') && !rest.starts_with(char::is_whitespace) {
        return None;
    }
    let mut inside = &rest[rest.find('>')? + 1..];
    // An element that closes itself or holds nothing but text is not an
    // object, which is the word the refusal uses: the reference reads
    // this body into a value and its schema says that value is an
    // object, and an empty element reads as a string.
    inside = &inside[..inside.find("</Delete>")?];
    if !inside.contains('<') {
        return None;
    }
    let mut keys = Vec::new();
    while let Some(at) = inside.find("<Key>") {
        let value = &inside[at + "<Key>".len()..];
        let Some(end) = value.find("</Key>") else {
            break;
        };
        keys.push(plain(&value[..end]));
        inside = &value[end + "</Key>".len()..];
    }
    Some(keys)
}

/// The three escapes [`escaped`] writes, read back. A key really can
/// hold an ampersand, and a client that sent one wrote it this way.
fn plain(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

/// Everything one object can be asked, dispatched the way the bucket
/// route dispatches its own.
///
/// The key is the whole rest of the path, slashes and all, because a
/// key with slashes in it is what makes a listing with a delimiter mean
/// anything. Nothing here treats those slashes as anything but
/// characters in a name.
///
/// Half of these are about an upload in pieces rather than about the
/// object, and what tells them apart is the query: `uploads` begins one
/// and `uploadId` names one that was begun. A put needs both `uploadId`
/// and `partNumber` to be a piece of one, which is recorded rather than
/// reasoned: a put naming an upload and no number writes an ordinary
/// object at that key and answers an ordinary object's etag.
pub async fn object(
    State(app): State<Arc<App>>,
    Path((bucket, key)): Path<(String, String)>,
    parts: Parts,
    body: Body,
) -> Response {
    let query = query_of(&parts);
    let answer = match parts.method {
        Method::GET if query.contains_key("uploadId") => {
            pieces(&app, &parts, &bucket, &key, &query).await
        }
        Method::GET => fetched(&app, &parts, &bucket, &key).await,
        Method::HEAD => described(&app, &parts, &bucket, &key).await,
        Method::POST if query.contains_key("uploads") => begun(&app, &parts, &bucket, &key).await,
        Method::POST if query.contains_key("uploadId") => {
            assembled(&app, &parts, &bucket, &key, &query, body).await
        }
        // UploadPartCopy, whose bytes are some other key's rather than
        // its own. Told apart from the piece below by the header alone,
        // and taken first, because a request carrying both a source and
        // a body is the copy: the body is ignored and the source is
        // what the piece is made of. Recorded.
        Method::PUT
            if query.contains_key("uploadId")
                && query.contains_key("partNumber")
                && let Some(source) = header_value(&parts, "x-amz-copy-source") =>
        {
            piece_copied(&app, &parts, &bucket, &key, &query, source).await
        }
        Method::PUT if query.contains_key("uploadId") && query.contains_key("partNumber") => {
            piece(&app, &parts, &bucket, &key, &query, body).await
        }
        // One verb and two requests, told apart by a header. A put
        // carrying bytes writes them and a put naming somewhere else's
        // reads them from there, and the second answers a document where
        // the first answers nothing.
        Method::PUT => match header_value(&parts, "x-amz-copy-source") {
            Some(source) => copied(&app, &parts, &bucket, &key, source).await,
            None => put(&app, &parts, &bucket, &key, body).await,
        },
        Method::DELETE if query.contains_key("uploadId") => abandoned(&app, &parts, &query).await,
        Method::DELETE => dropped(&app, &parts, &bucket, &key).await,
        _ => return crate::not_yet("the storage surface", crate::tracked::STORAGE),
    };
    match answer {
        Ok(response) => response,
        // A key on this surface is named as the bucket and the key
        // together, which is what a client sent and what an S3 error
        // means by a resource.
        Err(why) => refused(&why, &format!("{bucket}/{key}")),
    }
}

/// GET /storage/v1/s3/{bucket}/{key}
///
/// The bytes, whole or in part. Almost the json download route, and not
/// quite: no cache control, no last-modified, and nothing asked about
/// what the client already has. The two halves of storage answer bytes
/// through two different renderers upstream and this is the shorter of
/// them.
async fn fetched(
    app: &App,
    parts: &Parts,
    bucket: &str,
    key: &str,
) -> Result<Response, StorageError> {
    verified(app, parts)?;
    let row = row_of(app, parts, bucket, key).await?;
    let size = row.meta("size").as_u64().unwrap_or_default();
    let asked = parts
        .headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|header| object::wanted_range(header, size));

    let blobs = object::blobs(app)?;
    let (status, bytes, range) = match asked {
        Some((first, last)) => {
            let bytes = blobs
                .get_range(row.key(), first, last - first + 1)
                .await
                .map_err(|e| StorageError::internal(e.to_string()))?;
            let range = format!("bytes {first}-{last}/{size}");
            (StatusCode::PARTIAL_CONTENT, bytes, Some(range))
        }
        None => {
            let bytes = blobs
                .get(row.key())
                .await
                .map_err(|e| StorageError::internal(e.to_string()))?;
            (StatusCode::OK, bytes, None)
        }
    };
    // A row with no bytes behind it is the store disagreeing with the
    // table, which is a failure rather than a miss, and the same
    // failure the json route answers. Recorded on both: the fixture's
    // rows point at bytes nobody ever wrote and asking for one is 500.
    let Some(bytes) = bytes else {
        return Err(StorageError::internal("Internal Server Error".to_string()));
    };

    let mut answer = Response::builder().status(status);
    answer = typed(answer, &row);
    if let Some(range) = range {
        answer = answer.header(header::CONTENT_RANGE, range);
    }
    answer
        .body(Body::from(bytes))
        .map_err(|e| StorageError::internal(e.to_string()))
}

/// HEAD /storage/v1/s3/{bucket}/{key}
///
/// The same answer with nothing in it, and answered out of the table
/// alone. That is the difference worth having a second handler for: a
/// head never opens the store, so a row whose bytes are gone is 200
/// here and 500 through the get above. Recorded.
async fn described(
    app: &App,
    parts: &Parts,
    bucket: &str,
    key: &str,
) -> Result<Response, StorageError> {
    verified(app, parts)?;
    let row = row_of(app, parts, bucket, key).await?;
    typed(Response::builder().status(StatusCode::OK), &row)
        .body(Body::empty())
        .map_err(|e| StorageError::internal(e.to_string()))
}

/// The two headers both reads carry: what the object is, and what it
/// was when it was written. A row with no etag in its metadata sends
/// none, which is a row somebody inserted rather than uploaded.
fn typed(
    answer: axum::http::response::Builder,
    row: &object::ObjectRow,
) -> axum::http::response::Builder {
    let mime = row.meta("mimetype");
    let mut answer = answer.header(
        header::CONTENT_TYPE,
        object::served_type(mime.as_str().unwrap_or_default()),
    );
    if let Some(etag) = row.meta("eTag").as_str() {
        answer = answer.header(header::ETAG, etag);
    }
    answer
}

/// The row, or the refusal that there is not one.
async fn row_of(
    app: &App,
    parts: &Parts,
    bucket: &str,
    key: &str,
) -> Result<object::ObjectRow, StorageError> {
    let ctx = context(parts);
    let sess = begin(app, &ctx, true).await?;
    let found = object::find(&sess, bucket, key).await;
    match done(sess, found).await? {
        Some(row) => Ok(row),
        None => Err(StorageError::no_such_key()),
    }
}

/// PUT /storage/v1/s3/{bucket}/{key}
///
/// Which is an upload with no upsert header to argue about. S3 has no
/// put that refuses to overwrite, so this one always does, and that is
/// recorded rather than assumed: the same path put twice answers two
/// etags and reads back as the second.
async fn put(
    app: &App,
    parts: &Parts,
    bucket: &str,
    key: &str,
    body: Body,
) -> Result<Response, StorageError> {
    verified(app, parts)?;
    let mime = header_value(parts, "content-type").unwrap_or_default();
    let upload = object::Upload {
        bytes: axum::body::to_bytes(body, object::UPLOAD_LIMIT)
            .await
            .map_err(|_| StorageError::too_large())?
            .to_vec(),
        // A client that says nothing is sending bytes. The json route
        // decides the same thing the same way, and what it reads back
        // as is recorded on both.
        mime: match mime.is_empty() {
            true => object::OCTET_STREAM.to_string(),
            false => mime.to_string(),
        },
        cache: header_value(parts, "cache-control")
            .unwrap_or(object::NO_CACHE)
            .to_string(),
    };
    let ctx = context(parts);
    let sess = begin(app, &ctx, false).await?;
    // No role to put aside for the bucket lookup: this connection is
    // the service role already, so there are no policies in the way of
    // the question or of the insert.
    //
    // An empty object in the metadata column rather than nothing, which
    // is what an ordinary upload writes and not what a resumable one
    // does. Recorded: the object this route put with no metadata
    // headers on it reads back with `{}` on the json side, so an S3 put
    // and a form upload leave the same row.
    let attached = Some("{}".to_string());
    let written = object::write(app, sess, None, bucket, key, upload, attached, "", true).await?;
    let mut answer = StatusCode::OK.into_response();
    if let Ok(value) = written.etag.parse() {
        answer.headers_mut().insert(header::ETAG, value);
    }
    Ok(answer)
}

/// PUT /storage/v1/s3/{bucket}/{key} with `x-amz-copy-source` on it.
///
/// A second row and a second set of bytes, which is what the json copy
/// route writes too and for the same reason: two rows pointing at one
/// key would make a delete of either of them a delete of both, and
/// nothing in the store counts what points at it.
///
/// A copy onto a key that is already there replaces it, including the
/// key it came from. S3 refuses a copy onto itself unless the request
/// changes something, because that is how metadata is rewritten there;
/// this endpoint has no such rule and answers the same document it
/// answers for any other copy. Recorded, because a client that rewrites
/// metadata that way is a client that would have got an error.
async fn copied(
    app: &App,
    parts: &Parts,
    bucket: &str,
    key: &str,
    source: &str,
) -> Result<Response, StorageError> {
    verified(app, parts)?;
    let (from_bucket, from_key) = named(source);
    let blobs = object::blobs(app)?;
    let ctx = context(parts);
    let sess = begin(app, &ctx, false).await?;
    // A source that is not there and a source in a bucket that is not
    // there are the same refusal, because one query asks both questions
    // and nothing asks a second. The resource it names is the
    // destination rather than the source, which is the path the request
    // was made to and not the key that is missing. Recorded twice over,
    // and not something anybody would write on purpose.
    let Some(from) = object::find(&sess, from_bucket, from_key).await? else {
        return done(sess, Err(StorageError::no_such_key())).await;
    };
    // Copied by the statement that writes it rather than read out here
    // and sent back in. Everything the source said about its bytes is
    // still true of the copy, so the only columns that are the copy's
    // own are where it is and which version its bytes are stored under.
    let rows = sess
        .query(
            "insert into storage.objects (bucket_id, name, version, metadata, user_metadata)
             select $3, $4, gen_random_uuid()::text, metadata, user_metadata
               from storage.objects where bucket_id = $1 and name = $2
             on conflict (bucket_id, name) do update
                set version = excluded.version,
                    metadata = excluded.metadata,
                    user_metadata = excluded.user_metadata,
                    updated_at = now()
             returning id::text, version, coalesce(metadata->>'eTag', ''),
                       to_char(updated_at at time zone 'utc',
                               'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')",
            &[&from_bucket, &from_key, &bucket, &key],
        )
        .await
        .map_err(|e| missing_parent(&e))?;
    let Some(made) = rows.first() else {
        // Nothing on this surface can earn this: the connection is the
        // service role, so there are no policies for a row to fall foul
        // of. It is the answer the json copy route gives when one does.
        return done(
            sess,
            Err(StorageError::access_denied(
                "new row violates row-level security policy".to_string(),
            )),
        )
        .await;
    };
    let id: String = made.get(0);
    let version: String = made.get(1);
    let etag: String = made.get(2);
    let when: String = made.get(3);

    // The bytes before the commit, for the reason an upload has: a row
    // pointing at bytes that were never written is the one state a
    // client can see and cannot explain. A source whose bytes are not
    // there leaves the copy in that state anyway, which is what the json
    // route does and what the fixture's four rows are already in.
    let bytes = blobs
        .get(from.key())
        .await
        .map_err(|e| StorageError::internal(e.to_string()))?;
    if let Some(bytes) = bytes
        && let Err(e) = blobs.put(crate::blob::key(&id, &version), bytes).await
    {
        let _ = sess.rollback().await;
        return Err(StorageError::internal(e.to_string()));
    }
    done(sess, Ok(())).await?;

    // The etag is left out when the source had none rather than written
    // empty, which is the rule a listing follows and for the same
    // reason: a row somebody inserted by hand has nothing to say here.
    let tag = match etag.is_empty() {
        true => String::new(),
        false => format!("<ETag>{}</ETag>", escaped(&etag)),
    };
    Ok(xml(format!(
        "{DECLARATION}<CopyObjectResult xmlns=\"{NAMESPACE}\">{tag}\
         <LastModified>{}</LastModified></CopyObjectResult>",
        escaped(&when)
    )))
}

/// The bucket and the key a copy's source header names.
///
/// The leading slash is optional. Amazon's own documentation writes one
/// and several clients do not, and both spellings are recorded as
/// working, which is the sort of thing a client's choice of library
/// otherwise decides for it.
///
/// Nothing is decoded. A source with a percent in it is not recorded
/// either way, and a key really can hold one, so guessing here would be
/// guessing about the one case that would tell.
fn named(source: &str) -> (&str, &str) {
    let source = source.strip_prefix('/').unwrap_or(source);
    source.split_once('/').unwrap_or((source, ""))
}

/// A row that named a bucket nobody has.
///
/// Postgres refuses it as a foreign key and this surface answers in the
/// general rather than about buckets, which is recorded: a copy into a
/// bucket that is not there is not `NoSuchBucket`, because nothing
/// looked the bucket up to be able to say that.
fn missing_parent(e: &tokio_postgres::Error) -> StorageError {
    match e.as_db_error().map(|db| db.code().code()) {
        Some("23503") => StorageError::related_missing(),
        _ => pg_error(e),
    }
}

/// DELETE /storage/v1/s3/{bucket}/{key}
///
/// 204 whether or not there was anything there, which is S3's rule and
/// not the storage api's: the two halves disagree about a key that is
/// not there and this endpoint can only answer one of them. Recorded,
/// because guessing which of its two parents this route takes after is
/// exactly the kind of thing a recording is for.
async fn dropped(
    app: &App,
    parts: &Parts,
    bucket: &str,
    key: &str,
) -> Result<Response, StorageError> {
    verified(app, parts)?;
    let ctx = context(parts);
    let sess = begin(app, &ctx, false).await?;
    let Some(row) = object::find(&sess, bucket, key).await? else {
        return done(sess, Ok(StatusCode::NO_CONTENT.into_response())).await;
    };
    sess.execute(crate::storage::ALLOW_DELETE, &[])
        .await
        .map_err(|e| pg_error(&e))?;
    sess.execute(
        "delete from storage.objects where bucket_id = $1 and name = $2",
        &[&bucket, &key],
    )
    .await
    .map_err(|e| pg_error(&e))?;
    done(sess, Ok(())).await?;
    // After the commit, for the reason a replaced version's bytes go
    // after one: a reader that started before it is still reading them.
    if let Ok(blobs) = object::blobs(app) {
        let _ = blobs.delete(row.key()).await;
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// POST /storage/v1/s3/{bucket}/{key}?uploads
///
/// CreateMultipartUpload, which writes nothing anybody can read and
/// hands back the id every request after it carries. Nothing is at the
/// key until the pieces are put together, and the recording says so:
/// the object is not there and not in a listing while an upload for it
/// is in progress.
///
/// What the request says about the object it is going to make is kept
/// on the upload, because the requests that carry the bytes say nothing
/// about it. The shape of that column is nobody's business but this
/// server's: no route reads it back, so what upstream writes there is
/// not recorded and cannot be.
async fn begun(
    app: &App,
    parts: &Parts,
    bucket: &str,
    key: &str,
) -> Result<Response, StorageError> {
    verified(app, parts)?;
    let mime = match header_value(parts, "content-type").unwrap_or_default() {
        "" => object::OCTET_STREAM,
        given => given,
    };
    let cache = header_value(parts, "cache-control").unwrap_or(object::NO_CACHE);
    let held = serde_json::json!({"contentType": mime, "cacheControl": cache}).to_string();
    let ctx = context(parts);
    let sess = begin(app, &ctx, false).await?;
    if !exists(&sess, bucket).await? {
        return done(sess, Err(StorageError::no_such_bucket())).await;
    }
    // `upload_signature` is left empty. Upstream puts a signature over
    // the upload's own bucket, key and version there and refuses a
    // request whose path does not match it; the same question is asked
    // below by comparing the columns, which needs no secret and cannot
    // be answered differently by a server holding a different one.
    //
    // `version` is written and never read. The object's bytes are keyed
    // by the version the row that ends up in `storage.objects` gets,
    // and that row is not this one.
    let found = sess
        .query(
            "insert into storage.s3_multipart_uploads
                 (id, upload_signature, bucket_id, key, version, metadata)
             values (gen_random_uuid()::text, '', $1, $2,
                     gen_random_uuid()::text, $3::text::jsonb)
             returning id",
            &[&bucket, &key, &held],
        )
        .await
        .map_err(|e| pg_error(&e));
    let rows = done(sess, found).await?;
    let id: String = match rows.first() {
        Some(row) => row.get(0),
        None => return Err(StorageError::internal("upload was not written".to_string())),
    };
    Ok(xml(format!(
        "{DECLARATION}<InitiateMultipartUploadResult xmlns=\"{NAMESPACE}\"><Bucket>{}</Bucket>\
         <Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>",
        escaped(bucket),
        escaped(key),
        escaped(&id),
    )))
}

/// PUT /storage/v1/s3/{bucket}/{key}?partNumber=N&uploadId=U
///
/// UploadPart. The bytes are kept as they arrived and nothing is joined
/// up until the upload is put together, which is what lets a client
/// send them at once and in any order.
///
/// The etag comes back without the quotes an object's etag has. That is
/// upstream's inconsistency and it is load bearing: a client that puts
/// the quotes back when it names the piece in the completion is refused
/// with `InvalidChecksum`, which is recorded.
async fn piece(
    app: &App,
    parts: &Parts,
    bucket: &str,
    key: &str,
    query: &BTreeMap<String, String>,
    body: Body,
) -> Result<Response, StorageError> {
    verified(app, parts)?;
    // Before anything is looked up, which is where the reference checks
    // it: a number out of range is refused on an upload that exists,
    // and the sentence names the querystring rather than the upload. A
    // number that is not a number at all is not recorded, and reads as
    // zero here, which is the refusal a client that sent one deserves
    // for the same reason a client that sent zero does.
    let number = query
        .get("partNumber")
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or_default();
    if number < 1 {
        return Err(StorageError::not_valid(
            "Querystring, partNumber must be >= 1".to_string(),
        ));
    }
    if number > MOST_PIECES {
        return Err(StorageError::not_valid(format!(
            "Querystring, partNumber must be <= {MOST_PIECES}"
        )));
    }
    let upload = query.get("uploadId").cloned().unwrap_or_default();
    let bytes = axum::body::to_bytes(body, object::UPLOAD_LIMIT)
        .await
        .map_err(|_| StorageError::too_large())?
        .to_vec();
    let size = bytes.len() as i64;
    let etag = object::etag_of(&bytes).trim_matches('"').to_string();
    let blobs = object::blobs(app)?;

    let ctx = context(parts);
    let sess = begin(app, &ctx, false).await?;
    let Some(going) = upload_of(&sess, &upload).await? else {
        return done(sess, Err(StorageError::no_such_upload())).await;
    };
    // A piece sent to an upload that was begun for another key is 500
    // with `Internal Server Error` on it, which is not a decision
    // anybody made: upstream compares the path against the signature it
    // stored and throws where nothing is catching. Copied, because the
    // alternative is a client that gets a refusal here and a crash
    // there, and a client cannot tell a deliberate 500 from an
    // accidental one anyway.
    if going.bucket != bucket || going.key != key {
        return done(
            sess,
            Err(StorageError::internal("Internal Server Error".to_string())),
        )
        .await;
    }
    // A row of its own every time, even under a number that already has
    // one. Both sends are still there afterwards and the listing shows
    // both, which is recorded, and which is what makes the etag in the
    // completion a choice between them rather than a check on the
    // survivor.
    let rows = sess
        .query(
            "insert into storage.s3_multipart_uploads_parts
                 (upload_id, part_number, bucket_id, key, etag, version, size)
             values ($1, $2, $3, $4, $5, $6, $7)
             returning id::text",
            &[
                &upload,
                &number,
                &bucket,
                &key,
                &etag,
                &going.version,
                &size,
            ],
        )
        .await
        .map_err(|e| pg_error(&e))?;
    let part: String = match rows.first() {
        Some(row) => row.get(0),
        None => return Err(StorageError::internal("piece was not written".to_string())),
    };
    // What the upload is holding, kept as it goes rather than counted
    // at the end. Nothing reads it here; it is the column upstream
    // charges a project's storage against.
    sess.execute(
        "update storage.s3_multipart_uploads
            set in_progress_size = in_progress_size + $2
          where id = $1",
        &[&upload, &size],
    )
    .await
    .map_err(|e| pg_error(&e))?;

    // The bytes before the commit, for the reason an upload has one: a
    // row saying a piece is there is a row a completion will look for.
    if let Err(e) = blobs
        .put(crate::blob::piece_key(&upload, &part), bytes)
        .await
    {
        let _ = sess.rollback().await;
        return Err(StorageError::internal(e.to_string()));
    }
    done(sess, Ok(())).await?;

    let mut answer = StatusCode::OK.into_response();
    if let Ok(value) = etag.parse() {
        answer.headers_mut().insert(header::ETAG, value);
    }
    Ok(answer)
}

/// PUT /storage/v1/s3/{bucket}/{key}?partNumber=N&uploadId=U with
/// `x-amz-copy-source` on it.
///
/// UploadPartCopy: a piece whose bytes are already on the server, so
/// that a client joining objects together does not have to move them
/// through itself. The same put a piece is sent with, told apart by the
/// header, and answering a document where that one answers a header.
///
/// The etag in the document has no quotes on it, the same way the sent
/// piece's header has none, so a client can hand it straight back to
/// the completion. That is worth saying because the copy of a whole
/// object answers the object's own etag, which is quoted everywhere
/// else it appears. Recorded.
///
/// The order of the checks is recorded rather than chosen, and it is
/// not the order the request reads in: the number first, then the
/// source, then the range, then the upload. A request with a bad range
/// and a source that is not there is told about the source, and one
/// with a bad range and an upload that is not there is told about the
/// range, which pins the middle two either side of the upload.
async fn piece_copied(
    app: &App,
    parts: &Parts,
    bucket: &str,
    key: &str,
    query: &BTreeMap<String, String>,
    source: &str,
) -> Result<Response, StorageError> {
    verified(app, parts)?;
    let number = query
        .get("partNumber")
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or_default();
    if number < 1 {
        return Err(StorageError::not_valid(
            "Querystring, partNumber must be >= 1".to_string(),
        ));
    }
    if number > MOST_PIECES {
        return Err(StorageError::not_valid(format!(
            "Querystring, partNumber must be <= {MOST_PIECES}"
        )));
    }
    let upload = query.get("uploadId").cloned().unwrap_or_default();
    // The same reading a copy of a whole object gives the header, since
    // it is the same header: the leading slash is optional and nothing
    // else is touched, so a name spelled with %20 in it is a name with
    // a percent in it and a version in a query string is part of the
    // name. Both recorded here, as `NoSuchKey`.
    let (from_bucket, from_key) = named(source);
    let asked = header_value(parts, "x-amz-copy-source-range")
        .unwrap_or_default()
        .to_string();
    let blobs = object::blobs(app)?;

    let ctx = context(parts);
    let sess = begin(app, &ctx, false).await?;
    let Some(from) = object::find(&sess, from_bucket, from_key).await? else {
        return done(sess, Err(StorageError::no_such_key())).await;
    };
    let whole = from.meta("size").as_u64().unwrap_or_default();
    let wanted = match copied_range(&asked, whole) {
        Ok(wanted) => wanted,
        Err(why) => return done(sess, Err(why)).await,
    };
    let Some(going) = upload_of(&sess, &upload).await? else {
        return done(sess, Err(StorageError::no_such_upload())).await;
    };
    // The same 500 a sent piece earns under another key, and for the
    // same reason: upstream compares the path against the signature it
    // stored over the upload and throws. Recorded on this request as
    // well as on that one, so the copy goes through the check rather
    // than around it.
    if going.bucket != bucket || going.key != key {
        return done(
            sess,
            Err(StorageError::internal("Internal Server Error".to_string())),
        )
        .await;
    }

    let bytes = match wanted {
        Some((first, last)) => blobs.get_range(from.key(), first, last - first + 1).await,
        None => blobs.get(from.key()).await,
    }
    .map_err(|e| StorageError::internal(e.to_string()))?;
    // A source whose row is there and whose bytes are not is 500, the
    // same as reading it would be. Recorded against one of the
    // fixture's four rows, which point at bytes nobody ever wrote.
    let Some(bytes) = bytes else {
        let _ = sess.rollback().await;
        return Err(StorageError::internal("Internal Server Error".to_string()));
    };
    let size = bytes.len() as i64;
    let etag = object::etag_of(&bytes).trim_matches('"').to_string();

    // A row of its own, under a number that already has one as much as
    // under one that does not, which is the rule a sent piece follows
    // and is recorded again here.
    let rows = sess
        .query(
            "insert into storage.s3_multipart_uploads_parts
                 (upload_id, part_number, bucket_id, key, etag, version, size)
             values ($1, $2, $3, $4, $5, $6, $7)
             returning id::text,
                       to_char(created_at at time zone 'utc',
                               'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')",
            &[
                &upload,
                &number,
                &bucket,
                &key,
                &etag,
                &going.version,
                &size,
            ],
        )
        .await
        .map_err(|e| pg_error(&e))?;
    let Some(made) = rows.first() else {
        return Err(StorageError::internal("piece was not written".to_string()));
    };
    let part: String = made.get(0);
    let when: String = made.get(1);
    sess.execute(
        "update storage.s3_multipart_uploads
            set in_progress_size = in_progress_size + $2
          where id = $1",
        &[&upload, &size],
    )
    .await
    .map_err(|e| pg_error(&e))?;

    if let Err(e) = blobs
        .put(crate::blob::piece_key(&upload, &part), bytes)
        .await
    {
        let _ = sess.rollback().await;
        return Err(StorageError::internal(e.to_string()));
    }
    done(sess, Ok(())).await?;

    Ok(xml(format!(
        "{DECLARATION}<CopyPartResult xmlns=\"{NAMESPACE}\"><ETag>{}</ETag>\
         <LastModified>{}</LastModified></CopyPartResult>",
        escaped(&etag),
        escaped(&when),
    )))
}

/// Which bytes of the source a copied piece is made of.
///
/// `None` when the header is not there and when it is there and empty,
/// which are the same request as far as this endpoint is concerned and
/// both recorded.
///
/// Everything else is one spelling: `bytes=` and two numbers, the
/// second of them a byte the object has. A suffix range, two ranges in
/// one header, a range with no unit, a range that ends before it starts
/// and a range that ends one past the last byte are all refused, and
/// all with the same sentence. This is where the two range headers on
/// this surface part company: the one on a download clamps to the end
/// and answers what there is.
fn copied_range(asked: &str, size: u64) -> Result<Option<(u64, u64)>, StorageError> {
    let asked = asked.trim();
    if asked.is_empty() {
        return Ok(None);
    }
    let Some(spans) = asked.strip_prefix("bytes=") else {
        return Err(StorageError::bad_range());
    };
    let Some((first, last)) = spans.split_once('-') else {
        return Err(StorageError::bad_range());
    };
    let (Ok(first), Ok(last)) = (first.trim().parse::<u64>(), last.trim().parse::<u64>()) else {
        return Err(StorageError::bad_range());
    };
    if first > last || last >= size {
        return Err(StorageError::bad_range());
    }
    Ok(Some((first, last)))
}

/// GET /storage/v1/s3/{bucket}/{key}?uploadId=U
///
/// ListParts, which a client that lost its place asks before sending
/// the rest.
///
/// The bucket and the key in the answer are the ones in the path rather
/// than the upload's own, and the pieces are every piece the id has
/// whatever key they were sent under. Both are recorded, by asking for
/// an upload under a key that is not its.
///
/// In order of the number on them, not in the order they arrived. Two
/// pieces sent under one number are two rows and both are listed, the
/// earlier send first.
async fn pieces(
    app: &App,
    parts: &Parts,
    bucket: &str,
    key: &str,
    query: &BTreeMap<String, String>,
) -> Result<Response, StorageError> {
    verified(app, parts)?;
    let upload = query.get("uploadId").cloned().unwrap_or_default();
    let ctx = context(parts);
    let sess = begin(app, &ctx, true).await?;
    if upload_of(&sess, &upload).await?.is_none() {
        return done(sess, Err(StorageError::no_such_upload())).await;
    }
    let found = sess
        .query(
            "select part_number, etag,
                    to_char(created_at at time zone 'utc',
                            'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')
               from storage.s3_multipart_uploads_parts
              where upload_id = $1
              order by part_number, created_at",
            &[&upload],
        )
        .await
        .map_err(|e| pg_error(&e));
    let rows = done(sess, found).await?;
    let mut listed = String::new();
    for row in &rows {
        let number: i32 = row.get(0);
        let etag: String = row.get(1);
        let when: String = row.get(2);
        listed.push_str(&format!(
            "<Part><PartNumber>{number}</PartNumber><LastModified>{}</LastModified>\
             <ETag>{}</ETag></Part>",
            escaped(&when),
            escaped(&etag),
        ));
    }
    Ok(xml(format!(
        "{DECLARATION}<ListPartsResult xmlns=\"{NAMESPACE}\"><Bucket>{}</Bucket><Key>{}</Key>\
         <UploadId>{}</UploadId><MaxParts>{MOST_PARTS}</MaxParts>\
         <IsTruncated>false</IsTruncated>{listed}</ListPartsResult>",
        escaped(bucket),
        escaped(key),
        escaped(&upload),
    )))
}

/// DELETE /storage/v1/s3/{bucket}/{key}?uploadId=U
///
/// AbortMultipartUpload. The client gave up, and the pieces it sent are
/// not going to be an object, so they are nobody's.
///
/// 200 with no body and no content type, which is S3's 204 answered
/// differently and recorded.
async fn abandoned(
    app: &App,
    parts: &Parts,
    query: &BTreeMap<String, String>,
) -> Result<Response, StorageError> {
    verified(app, parts)?;
    let upload = query.get("uploadId").cloned().unwrap_or_default();
    let ctx = context(parts);
    let sess = begin(app, &ctx, false).await?;
    if upload_of(&sess, &upload).await?.is_none() {
        return done(sess, Err(StorageError::no_such_upload())).await;
    }
    let kept = held_pieces(&sess, &upload).await?;
    // The parts rows go with the upload, which is the foreign key's
    // doing rather than this statement's.
    let gone = sess
        .execute(
            "delete from storage.s3_multipart_uploads where id = $1",
            &[&upload],
        )
        .await
        .map_err(|e| pg_error(&e));
    done(sess, gone.map(|_| ())).await?;
    if let Ok(blobs) = object::blobs(app) {
        for part in &kept {
            let _ = blobs
                .delete(crate::blob::piece_key(&upload, &part.id))
                .await;
        }
    }
    Ok(StatusCode::OK.into_response())
}

/// POST /storage/v1/s3/{bucket}/{key}?uploadId=U
///
/// CompleteMultipartUpload: the pieces the body names, joined up in the
/// order the body names them and written as an ordinary object.
///
/// The order is the body's rather than the numbers' and that is
/// recorded: a completion naming piece two before piece one makes an
/// object with the second piece at the front of it. So is the way a
/// piece is named, which is by its number and its etag together, so
/// that a number with two sends under it is a choice rather than an
/// ambiguity.
///
/// The upload is gone afterwards, whether or not the object is any
/// good, which is why a completion sent twice is `NoSuchUpload` rather
/// than a second object.
async fn assembled(
    app: &App,
    parts: &Parts,
    bucket: &str,
    key: &str,
    query: &BTreeMap<String, String>,
    body: Body,
) -> Result<Response, StorageError> {
    verified(app, parts)?;
    let upload = query.get("uploadId").cloned().unwrap_or_default();
    let raw = axum::body::to_bytes(body, object::UPLOAD_LIMIT)
        .await
        .map_err(|_| StorageError::too_large())?;
    let text = String::from_utf8_lossy(&raw).to_string();
    // Before the upload is looked for, which is where the delete route
    // reads its own body and the same sentence a schema failure earns
    // there.
    let Some(named) = parts_in(&text) else {
        return Err(StorageError::not_valid(
            "Body, CompleteMultipartUpload must be object".to_string(),
        ));
    };
    let blobs = object::blobs(app)?;

    let ctx = context(parts);
    let sess = begin(app, &ctx, false).await?;
    let Some(going) = upload_of(&sess, &upload).await? else {
        return done(sess, Err(StorageError::no_such_upload())).await;
    };
    let kept = held_pieces(&sess, &upload).await?;
    let mut wanted = Vec::new();
    for (number, etag) in &named {
        let under: Vec<&Held> = kept.iter().filter(|held| held.number == *number).collect();
        if under.is_empty() {
            let why = StorageError::missing_part(format!(
                "Part {number} is missing for upload id {upload}"
            ));
            return done(sess, Err(why)).await;
        }
        // A piece is named by its etag, and the etag a piece answered
        // with had no quotes on it. Compared as it arrived: a client
        // that adds them is naming a piece nobody has.
        let Some(held) = under.iter().find(|held| &held.etag == etag) else {
            let why = StorageError::wrong_part(format!("Invalid ETag for part {number}"));
            return done(sess, Err(why)).await;
        };
        wanted.push(held.id.clone());
    }

    let mut bytes = Vec::new();
    for part in &wanted {
        match blobs.get(crate::blob::piece_key(&upload, part)).await {
            Ok(Some(mut some)) => bytes.append(&mut some),
            Ok(None) => {}
            Err(e) => {
                let _ = sess.rollback().await;
                return Err(StorageError::internal(e.to_string()));
            }
        }
    }

    // The upload before the object, in the transaction the object is
    // written in, so that an object that could not be written leaves
    // the upload where it was.
    sess.execute(
        "delete from storage.s3_multipart_uploads where id = $1",
        &[&upload],
    )
    .await
    .map_err(|e| pg_error(&e))?;

    // Through the same code an ordinary put ends in, so that the two of
    // them leave the same row behind for the same bytes, including the
    // empty object in the metadata column that says an S3 put wrote it.
    let joined = object::Upload {
        bytes,
        mime: going.mime,
        cache: going.cache,
    };
    let attached = Some("{}".to_string());
    let written = object::write(app, sess, None, bucket, key, joined, attached, "", true).await?;

    // The pieces are the object now and their own bytes are nobody's.
    // After the commit, for the reason a replaced version's bytes go
    // after one.
    for part in &kept {
        let _ = blobs
            .delete(crate::blob::piece_key(&upload, &part.id))
            .await;
    }

    // Location is the bucket and the key rather than a url, which is
    // recorded and is not what S3 sends.
    Ok(xml(format!(
        "{DECLARATION}<CompleteMultipartUploadResult xmlns=\"{NAMESPACE}\">\
         <Location>{}/{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag>\
         </CompleteMultipartUploadResult>",
        escaped(bucket),
        escaped(key),
        escaped(bucket),
        escaped(key),
        escaped(&written.etag),
    )))
}

/// An upload in progress, as much of it as the five requests need.
struct InPieces {
    bucket: String,
    key: String,
    version: String,
    mime: String,
    cache: String,
}

/// One piece of one, by the row rather than by the number on it.
struct Held {
    id: String,
    number: i32,
    etag: String,
}

/// The upload an id names, or nothing.
///
/// By the id alone. The bucket and the key in the path are not part of
/// the question: an upload asked for under the wrong key is found and
/// then answered about differently by each of the routes that ask, and
/// both of those answers are recorded.
async fn upload_of(sess: &crate::sql::Session, id: &str) -> Result<Option<InPieces>, StorageError> {
    let rows = sess
        .query(
            "select bucket_id, key, version,
                    coalesce(metadata->>'contentType', ''),
                    coalesce(metadata->>'cacheControl', '')
               from storage.s3_multipart_uploads where id = $1",
            &[&id],
        )
        .await
        .map_err(|e| pg_error(&e))?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let mime: String = row.get(3);
    let cache: String = row.get(4);
    Ok(Some(InPieces {
        bucket: row.get(0),
        key: row.get(1),
        version: row.get(2),
        mime: match mime.is_empty() {
            true => object::OCTET_STREAM.to_string(),
            false => mime,
        },
        cache: match cache.is_empty() {
            true => object::NO_CACHE.to_string(),
            false => cache,
        },
    }))
}

/// Every piece an upload is holding, in the order a listing wants them.
async fn held_pieces(sess: &crate::sql::Session, upload: &str) -> Result<Vec<Held>, StorageError> {
    let rows = sess
        .query(
            "select id::text, part_number, etag
               from storage.s3_multipart_uploads_parts
              where upload_id = $1
              order by part_number, created_at",
            &[&upload],
        )
        .await
        .map_err(|e| pg_error(&e))?;
    Ok(rows
        .iter()
        .map(|row| Held {
            id: row.get(0),
            number: row.get(1),
            etag: row.get(2),
        })
        .collect())
}

/// The pieces a completion's body names, in the order it named them, or
/// nothing when there is no `CompleteMultipartUpload` object in it.
///
/// Read by finding the tags, for the reason the delete route reads its
/// own body that way: everything that has to come out of this document
/// is two elements deep and in document order, and a parser would bring
/// a set of decisions about namespaces and entities that nothing here
/// needs.
///
/// A `Part` naming no number or no etag is skipped rather than refused.
/// Nothing recorded says what the reference does with one, and a
/// completion that names nothing at all is the case that is recorded.
fn parts_in(body: &str) -> Option<Vec<(i32, String)>> {
    let at = body.find("<CompleteMultipartUpload")?;
    let rest = &body[at..];
    let end = rest.find("</CompleteMultipartUpload>")?;
    let inside = &rest[rest[..end].find('>')? + 1..end];
    // An element holding no element is a string rather than an object
    // as far as the schema upstream is concerned, and that is the
    // sentence a client hears.
    if !inside.contains('<') {
        return None;
    }
    let mut named = Vec::new();
    for chunk in inside.split("<Part>").skip(1) {
        let Some(number) = inside_of(chunk, "PartNumber") else {
            continue;
        };
        let Ok(number) = number.trim().parse::<i32>() else {
            continue;
        };
        let Some(etag) = inside_of(chunk, "ETag") else {
            continue;
        };
        named.push((number, plain(etag)));
    }
    Some(named)
}

/// What the first `<name>` in some text holds.
fn inside_of<'a>(text: &'a str, name: &str) -> Option<&'a str> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let at = text.find(&open)? + open.len();
    let rest = &text[at..];
    Some(&rest[..rest.find(&close)?])
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
        assert_eq!(signed.region, "us-east-1");
        assert_eq!(signed.headers, vec!["host", "x-amz-date"]);
        assert_eq!(signed.signature, "deadbeef");
    }

    /// A token is not a signature, and neither is nothing. Both are the
    /// same refusal, which is recorded: not signed, rather than signed
    /// in a way that cannot be read.
    #[test]
    fn anything_that_is_not_this_version_is_not_signed_at_all() {
        for raw in ["", "Bearer not-a-signature", "AWS4-HMAC-SHA1 Credential=a"] {
            let why = parse(raw).expect_err("not a signature");
            assert_eq!(why.said(), "Missing signature");
            assert_eq!(why.status(), 403);
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
        assert_eq!(escaped("a&b<c>d"), "a&amp;b&lt;c&gt;d");
        assert_eq!(escaped("photos"), "photos");
    }

    /// The one thing escaping must not touch, because the quotes are
    /// part of the value rather than around it.
    #[test]
    fn an_etag_keeps_its_quotes() {
        assert_eq!(escaped("\"5eb63bbb\""), "\"5eb63bbb\"");
    }

    /// Both spellings of a source are recorded, and a source naming no
    /// key at all is a key nobody has rather than a panic.
    #[test]
    fn a_copy_source_is_read_with_or_without_its_leading_slash() {
        assert_eq!(named("/notes/original.txt"), ("notes", "original.txt"));
        assert_eq!(named("notes/original.txt"), ("notes", "original.txt"));
        assert_eq!(
            named("notes/holiday/beach.txt"),
            ("notes", "holiday/beach.txt")
        );
        assert_eq!(named("/notes"), ("notes", ""));
    }

    /// In the order the body wrote them, which is the order the answer
    /// reports them in, and with the attribute every client puts on the
    /// tag ignored.
    #[test]
    fn a_delete_body_is_read_as_the_keys_it_names() {
        let body = "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                    <Object><Key>zebra.txt</Key></Object>\
                    <Object><Key>apples.txt</Key></Object></Delete>";
        assert_eq!(
            keys_in(body),
            Some(vec!["zebra.txt".into(), "apples.txt".into()])
        );
    }

    /// Quiet mode is read as one more element nobody looks at, because
    /// the reference answers as though it had not been sent.
    #[test]
    fn asking_for_quiet_takes_nothing_out_of_the_answer() {
        let body = "<Delete><Quiet>true</Quiet><Object><Key>apples.txt</Key></Object></Delete>";
        assert_eq!(keys_in(body), Some(vec!["apples.txt".into()]));
    }

    /// An empty element is not an object, and neither is a document
    /// with no delete in it at all.
    #[test]
    fn a_delete_body_with_nothing_in_it_is_not_one() {
        assert_eq!(keys_in("<Delete></Delete>"), None);
        assert_eq!(keys_in("<Delete/>"), None);
        assert_eq!(keys_in("<DeleteObjects><Key>a</Key></DeleteObjects>"), None);
        assert_eq!(keys_in(""), None);
    }

    /// A key can hold the characters a document cannot, and it went out
    /// escaped, so it comes back the same way.
    #[test]
    fn a_key_written_as_escapes_is_read_as_itself() {
        let body = "<Delete><Object><Key>a&amp;b&lt;c</Key></Object></Delete>";
        assert_eq!(keys_in(body), Some(vec!["a&b<c".into()]));
    }

    /// In the order the body wrote them, because that is the order the
    /// object is joined up in. A completion naming the second piece
    /// first makes an object with the second piece at the front of it,
    /// which is recorded.
    #[test]
    fn a_completion_is_read_as_the_pieces_it_names_in_that_order() {
        let body = "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                    <Part><PartNumber>2</PartNumber><ETag>bbb</ETag></Part>\
                    <Part><PartNumber>1</PartNumber><ETag>aaa</ETag></Part>\
                    </CompleteMultipartUpload>";
        assert_eq!(
            parts_in(body),
            Some(vec![(2, "bbb".to_string()), (1, "aaa".to_string())])
        );
    }

    /// The same rule the delete body follows: an element holding no
    /// element is a string rather than an object, and that is what
    /// earns the sentence about a schema.
    #[test]
    fn a_completion_with_nothing_in_it_is_not_one() {
        assert_eq!(
            parts_in("<CompleteMultipartUpload></CompleteMultipartUpload>"),
            None
        );
        assert_eq!(parts_in("<CompleteMultipartUpload/>"), None);
        assert_eq!(parts_in(""), None);
    }

    /// The boundary a copied range is refused on, which was recorded a
    /// byte either side of rather than reasoned about. The end is a
    /// byte the object has, so the last byte of a sixty six byte object
    /// is 65 and asking for 66 is refused.
    #[test]
    fn a_copied_range_ends_on_a_byte_the_object_has() {
        assert_eq!(copied_range("bytes=0-65", 66).unwrap(), Some((0, 65)));
        assert_eq!(copied_range("bytes=65-65", 66).unwrap(), Some((65, 65)));
        assert!(copied_range("bytes=0-66", 66).is_err());
        assert!(copied_range("bytes=66-66", 66).is_err());
        assert!(copied_range("bytes=20-10", 66).is_err());
    }

    /// A header that is not there and a header that is there and empty
    /// are the same request, and both mean the whole object. Recorded,
    /// because an empty one reads like a client that meant something.
    #[test]
    fn no_copied_range_and_an_empty_one_are_the_whole_object() {
        assert_eq!(copied_range("", 66).unwrap(), None);
        assert_eq!(copied_range("   ", 66).unwrap(), None);
    }

    /// Every other spelling a range header takes is refused here, which
    /// is where this parser and the one on a download part company.
    #[test]
    fn a_copied_range_takes_one_spelling_and_no_other() {
        assert!(copied_range("0-9", 66).is_err());
        assert!(copied_range("bytes=-5", 66).is_err());
        assert!(copied_range("bytes=5-", 66).is_err());
        assert!(copied_range("bytes=0-1,3-4", 66).is_err());
        assert!(copied_range("bytes=", 66).is_err());
    }

    /// A piece that names half of itself names nothing. Nothing
    /// recorded says what the reference does with one, and the pieces
    /// that are named are still named.
    #[test]
    fn a_piece_missing_its_number_or_its_etag_is_left_out() {
        let body = "<CompleteMultipartUpload><Part><ETag>bbb</ETag></Part>\
                    <Part><PartNumber>x</PartNumber><ETag>ccc</ETag></Part>\
                    <Part><PartNumber>3</PartNumber></Part>\
                    <Part><PartNumber>1</PartNumber><ETag>aaa</ETag></Part>\
                    </CompleteMultipartUpload>";
        assert_eq!(parts_in(body), Some(vec![(1, "aaa".to_string())]));
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
