//! The Supabase Storage surface. Buckets, for now.
//!
//! Everything here is written against a recording rather than against
//! documentation. storage-api ships as an image rather than a binary,
//! so the reference is the container a local supabase project runs, and
//! the twenty five answers it gave to the bucket questions live in the
//! conformance repository next to the PostgREST and GoTrue ones. Four
//! of those answers are things nobody would have written down from the
//! docs, and each of them is a line of code here.
//!
//! Every failure is HTTP 400. The status a caller is meant to act on is
//! a string inside the body, so a bucket that is not there is 400 on
//! the wire carrying `"statusCode": "404"`. Only a 500 goes out as
//! itself.
//!
//! A bucket in a listing carries `type` and the same bucket fetched on
//! its own does not. Two routes, two column lists, and the column is
//! not really a column: the listing selects the literal `'STANDARD'`.
//!
//! `owner` comes back as the empty string rather than as null, because
//! the response is serialized against a schema that calls it a string
//! and the serializer coerces. The column is a nullable uuid.
//!
//! A key that row level security hides a bucket from is told the bucket
//! is not there, not that it may not look. That falls out of asking the
//! database first and letting the empty result be the answer, which is
//! also the only way to get it right without keeping a permission model
//! of our own alongside the policies.
//!
//! Two more things worth knowing while reading this. The surface sits
//! outside zou's apikey gate, because the reference answers a request
//! carrying no key at all in storage-api's words rather than a
//! gateway's. And it reads its token from the authorization header
//! only, never from the apikey, which is what storage-api's jwt plugin
//! does.
//!
//! The rest of the surface reads this one. Objects are in `object`,
//! resumable uploads in `tus`, and the S3 protocol in `s3`, and all
//! three carry their answers back in the [`StorageError`] below even
//! though none of the three writes it out the way these routes do.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header, request::Parts};
use axum::response::{IntoResponse, Response};

use crate::sql::{RequestContext, Session};
use crate::{App, jwt};

/// How much of a bucket call's body is ever worth reading. A bucket is
/// six fields, so anything approaching this is not one.
const BODY_LIMIT: usize = 64 * 1024;

/// The content type the reference sends on every answer, failures
/// included. The charset is part of it and the conformance differ
/// compares this header, so it is written once here rather than at each
/// of the six routes.
const JSON: &str = "application/json; charset=utf-8";

/// The four fields storage-api puts in a failure, and the status on the
/// wire is not one of them.
///
/// `status` is what goes in the body, as a string. What goes on the
/// wire is 400 unless the body says 500. That is the whole of
/// storage-api's error handler and it is why a client switching on the
/// http status sees one number for everything.
#[derive(Debug)]
pub struct StorageError {
    status: u16,
    error: &'static str,
    message: String,
    pub(crate) code: &'static str,
}

impl StorageError {
    pub(crate) fn no_such_bucket() -> Self {
        StorageError {
            status: 404,
            error: "Bucket not found",
            message: "Bucket not found".to_string(),
            code: "NoSuchBucket",
        }
    }

    pub(crate) fn already_exists() -> Self {
        StorageError {
            status: 409,
            error: "Duplicate",
            message: "The resource already exists".to_string(),
            code: "BucketAlreadyExists",
        }
    }

    /// What a missing object is called, which is not what a missing
    /// bucket is called. The error name is `not_found` in lower case
    /// here and `Bucket not found` in a sentence there, and both are
    /// copied rather than made consistent.
    pub(crate) fn no_such_key() -> Self {
        StorageError {
            status: 404,
            error: "not_found",
            message: "Object not found".to_string(),
            code: "NoSuchKey",
        }
    }

    /// An upload id that names nothing.
    ///
    /// Three situations rather than one, and the recording says they
    /// are all this: an id nobody was ever given, an id whose upload
    /// was dropped, and an id whose upload was put together. Once the
    /// object exists the id that made it is spent, so a client that
    /// retries a completion it never saw the answer to hears that the
    /// upload is gone rather than that the object is there.
    ///
    /// The error name is the code. Nothing on the json routes can earn
    /// this, so there is no recording of what upstream calls it there
    /// and nothing to copy.
    pub(crate) fn no_such_upload() -> Self {
        StorageError {
            status: 404,
            error: "NoSuchUpload",
            message: "Upload not found".to_string(),
            code: "NoSuchUpload",
        }
    }

    /// A piece the client named and the server has nothing under.
    ///
    /// The sentence carries the number and the upload's id, which makes
    /// it the one refusal on this surface that says which upload it is
    /// about.
    pub(crate) fn missing_part(message: String) -> Self {
        StorageError {
            status: 400,
            error: "MissingPart",
            message,
            code: "MissingPart",
        }
    }

    /// A piece the server does have under that number, whose etag is
    /// not the one the client named.
    ///
    /// Which is a different thing from a piece that is not there, and
    /// worth telling apart: the first says the upload lost something
    /// and the second says the client and the server disagree about
    /// what was sent.
    pub(crate) fn wrong_part(message: String) -> Self {
        StorageError {
            status: 400,
            error: "InvalidChecksum",
            message,
            code: "InvalidChecksum",
        }
    }

    /// A range of a source that a copy cannot take.
    ///
    /// Only on a copied piece. A range header on a download clamps to
    /// the end of the object and answers what there is, and this one
    /// refuses: an end past the last byte, a start past it, a suffix
    /// range, two ranges in one header and a range with no unit on it
    /// are all this, and all with the same lower case sentence.
    /// Recorded either side of the boundary, so the rule is that the
    /// end has to be a byte the object has rather than a length it has.
    pub(crate) fn bad_range() -> Self {
        StorageError {
            status: 400,
            error: "InvalidRange",
            message: "invalid range provided".to_string(),
            code: "InvalidRange",
        }
    }

    /// The same sentence a duplicate bucket earns, under a different
    /// code.
    pub(crate) fn key_already_exists() -> Self {
        StorageError {
            status: 409,
            error: "Duplicate",
            message: "The resource already exists".to_string(),
            code: "KeyAlreadyExists",
        }
    }

    /// And the same sentence again, under a third code, which is the
    /// one a move answers when the name it is moving on to is taken. A
    /// copy calls that collision a key and a move calls it a resource.
    /// Nothing distinguishes the two situations except which route the
    /// request came in on, and both are recorded.
    pub(crate) fn resource_already_exists() -> Self {
        StorageError {
            status: 409,
            error: "Duplicate",
            message: "The resource already exists".to_string(),
            code: "ResourceAlreadyExists",
        }
    }

    /// Recorded, for the smaller of the two limits. A bucket's
    /// `file_size_limit` earns this, and so does a bucket asking to be
    /// made with a limit above the one the whole server carries. The
    /// larger limit, the fifty megabytes a request may weigh at all,
    /// still has no case: a fixture that uploaded fifty megabytes would
    /// be a fixture nobody runs twice.
    pub(crate) fn too_large() -> Self {
        StorageError {
            status: 413,
            error: "Payload too large",
            message: "The object exceeded the maximum allowed size".to_string(),
            code: "EntityTooLarge",
        }
    }

    /// A type the bucket's list does not cover, named back to the
    /// caller. The type is the one that was read rather than the one
    /// that was sent, which shows on an upload carrying no content-type
    /// at all: nobody said application/octet-stream and that is the
    /// name in the sentence.
    pub(crate) fn bad_mime(mime: &str) -> Self {
        StorageError {
            status: 415,
            error: "invalid_mime_type",
            message: format!("mime type {mime} is not supported"),
            code: "InvalidMimeType",
        }
    }

    /// Not recorded either. A multipart body with no file part in it is
    /// refused by upstream's schema before its handler runs, the same
    /// way a bucket with no name is.
    pub(crate) fn no_file_in_form() -> Self {
        StorageError {
            status: 400,
            error: "FastifyError",
            message: "Multipart body must contain a file".to_string(),
            code: "InvalidRequest",
        }
    }

    pub(crate) fn not_empty() -> Self {
        StorageError {
            status: 409,
            error: "ResourceNotEmpty",
            message: "The bucket you tried to delete is not empty".to_string(),
            code: "ResourceNotEmpty",
        }
    }

    pub(crate) fn access_denied(message: String) -> Self {
        StorageError {
            status: 403,
            error: "Unauthorized",
            message,
            code: "AccessDenied",
        }
    }

    /// The body did not parse. The error name is fastify's own, which
    /// is the reference implementation showing through, and it is
    /// copied rather than improved on because a client matching on the
    /// string is matching on that one.
    pub(crate) fn bad_json() -> Self {
        StorageError {
            status: 400,
            error: "FastifyError",
            message: "Body is not valid JSON but content-type is set to 'application/json'"
                .to_string(),
            code: "InvalidRequest",
        }
    }

    /// A parameter that arrived and is not usable. The sentence is
    /// built out of the parameter's name, and the error name is the
    /// code because nothing set a friendlier one.
    pub(crate) fn invalid_parameter(name: &str) -> Self {
        StorageError {
            status: 400,
            error: "InvalidParameter",
            message: format!("Invalid Parameter {name}"),
            code: "InvalidParameter",
        }
    }

    /// Not recorded. A cursor that does not read as one is a cursor
    /// from somewhere else, and this is the shape upstream refuses it
    /// in.
    pub(crate) fn invalid_cursor() -> Self {
        StorageError::invalid_parameter("continuation token")
    }

    /// A field the reference's schema requires and the request did not
    /// send. `place` is `body` or `querystring`, which is what fastify
    /// calls the two halves of a request it validates.
    ///
    /// A request missing more than one required field is only told
    /// about the first, in the order the schema lists them, so the
    /// caller passes them in that order.
    pub(crate) fn missing_property(place: &str, name: &str) -> Self {
        StorageError::not_valid(format!("{place} must have required property '{name}'"))
    }

    /// The other half of the same refusal: a field that is there and is
    /// not what the schema said it would be.
    ///
    /// The error name is `Error` and not `FastifyError`, which is the
    /// name a body that did not parse at all earns. Both are recorded.
    /// A parse failure is thrown by fastify and reaches the handler
    /// with its class on it, and a schema failure is a validation
    /// result that has been turned into an error somewhere in between,
    /// so by the time it is serialized there is nothing left saying
    /// where it came from.
    pub(crate) fn not_valid(message: String) -> Self {
        StorageError {
            status: 400,
            error: "Error",
            message,
            code: "InvalidRequest",
        }
    }

    /// A token that does not verify, in the words the library upstream
    /// verifies with uses. What is wrong with it is said out loud
    /// because upstream says it out loud: the message is jose's, passed
    /// through two layers of wrapping and out to the client.
    pub(crate) fn invalid_jwt(message: String) -> Self {
        StorageError {
            status: 400,
            error: "InvalidJWT",
            message,
            code: "InvalidJWT",
        }
    }

    /// A token that verifies and is about something else. The signature
    /// was good, so this is not a forgery, it is a url being spent on
    /// an object or an action it was not signed for.
    pub(crate) fn invalid_signature(message: String) -> Self {
        StorageError {
            status: 400,
            error: "InvalidSignature",
            message,
            code: "InvalidSignature",
        }
    }

    /// What a foreign key violation is called. The row that would not
    /// go in named a bucket that is not there, and upstream's mapping
    /// of postgres's codes says so in the general rather than about
    /// buckets.
    pub(crate) fn related_missing() -> Self {
        StorageError {
            status: 404,
            error: "InvalidRequest",
            message: "The related resource does not exist".to_string(),
            code: "InvalidRequest",
        }
    }

    /// Bytes a transform cannot read. The row is there and so are the
    /// bytes, and they are not an image any decoder here has, which is
    /// the one failure the render routes have that the download routes
    /// cannot: a download hands back whatever it was given.
    ///
    /// The error name is the code again, the way it is for a foreign
    /// key violation above, rather than the `Error` a schema failure
    /// earns. Recorded, from a text file asked for at a width.
    pub(crate) fn not_an_image() -> Self {
        StorageError {
            status: 400,
            error: "InvalidRequest",
            message: "The source image is invalid or unsupported for rendering".to_string(),
            code: "InvalidRequest",
        }
    }

    /// The one refusal only the S3 surface can earn: the request was
    /// signed, the access key id is one this project has, and the
    /// signature is not the one the secret would have produced. The
    /// sentence is Amazon's own, word for word, because the reference
    /// sends Amazon's own and a client matching on it is matching on
    /// that string.
    pub(crate) fn wrong_signature() -> Self {
        StorageError {
            status: 403,
            error: "SignatureDoesNotMatch",
            message: "The request signature we calculated does not match the signature you \
                      provided. Check your key and signing method."
                .to_string(),
            code: "SignatureDoesNotMatch",
        }
    }

    pub(crate) fn internal(message: String) -> Self {
        StorageError {
            status: 500,
            error: "Internal",
            message,
            code: "InternalError",
        }
    }
}

impl StorageError {
    /// What it says, for the one caller that has to say it in another
    /// shape. The resumable routes answer a refusal as a status and a
    /// sentence rather than as a json body, and the sentence is this
    /// one.
    pub(crate) fn said(&self) -> &str {
        &self.message
    }

    /// The status it names, which on these routes is a string in the
    /// body and on the S3 routes is the status on the wire. Two
    /// surfaces, one error, and only one of them tells the truth in the
    /// place http keeps it.
    pub(crate) fn status(&self) -> u16 {
        self.status
    }
}

impl IntoResponse for StorageError {
    fn into_response(self) -> Response {
        let wire = if self.status == 500 {
            StatusCode::INTERNAL_SERVER_ERROR
        } else {
            StatusCode::BAD_REQUEST
        };
        // Written out rather than built from a map, because the map
        // this server has is a sorted one and the field order the
        // reference writes is not sorted. A caller parsing the body
        // cannot tell, and a recording compared byte for byte can, and
        // the second is the whole point of having the recording.
        let body = format!(
            "{{\"statusCode\":{},\"error\":{},\"message\":{},\"code\":{}}}",
            quoted(&self.status.to_string()),
            quoted(self.error),
            quoted(&self.message),
            quoted(self.code),
        );
        (wire, [(header::CONTENT_TYPE, JSON)], body).into_response()
    }
}

/// The same refusal, in the order the resumable routes write it.
///
/// Not a different refusal: the same status, the same words and the
/// same four fields as [`IntoResponse`] above, written statusCode,
/// code, error, message rather than statusCode, error, message, code.
/// Upstream has two serializers for one error object and the resumable
/// door reaches the other one, which nothing but a recording compared
/// byte for byte would ever notice. It lives here rather than in `tus`
/// so that the two orders are one line apart and the day a field is
/// added to one of them the other is looked at.
pub(crate) fn as_tus(why: &StorageError) -> Response {
    let wire = match why.status == 500 {
        true => StatusCode::INTERNAL_SERVER_ERROR,
        false => StatusCode::BAD_REQUEST,
    };
    let body = format!(
        "{{\"statusCode\":{},\"code\":{},\"error\":{},\"message\":{}}}",
        quoted(&why.status.to_string()),
        quoted(why.code),
        quoted(why.error),
        quoted(&why.message),
    );
    (wire, [(header::CONTENT_TYPE, JSON)], body).into_response()
}

/// One json string, escaped the way json escapes.
fn quoted(text: &str) -> String {
    serde_json::Value::from(text).to_string()
}

/// An answer that worked, always json and always with the charset on
/// it.
pub(crate) fn ok(body: String) -> Response {
    (StatusCode::OK, [(header::CONTENT_TYPE, JSON)], body).into_response()
}

/// The one line answer three of the six routes give.
pub(crate) fn message(text: &str) -> Response {
    ok(serde_json::json!({ "message": text }).to_string())
}

/// Why a token was refused, in the words the reference uses.
///
/// storage-api verifies with jose and hands jose's own message straight
/// to the client, so these are jose's strings rather than zou's. Only
/// the first is recorded: a request with no authorization header at all
/// verifies the empty string, and that is what jose calls anything that
/// is not three dot separated segments. The rest are read off jose and
/// are the next thing for the suite to ask about.
pub(crate) fn jose_words(why: &jwt::Reject) -> &'static str {
    match why {
        jwt::Reject::Malformed => "Invalid Compact JWS",
        jwt::Reject::WrongAlgorithm(_) => "\"alg\" (Algorithm) Header Parameter value not allowed",
        jwt::Reject::BadSignature => "signature verification failed",
        jwt::Reject::Expired => "\"exp\" claim timestamp check failed",
        jwt::Reject::TooEarly => "\"nbf\" claim timestamp check failed",
        // jose reads iat as a timestamp to sanity check as well, and
        // this endpoint never asks it to, so the string is the shape
        // the other two have rather than one taken off a run.
        jwt::Reject::IssuedLater => "\"iat\" claim timestamp check failed",
        jwt::Reject::UnknownKey => "no applicable key found in the JSON Web Key Set",
    }
}

/// Who is asking, from the authorization header and nothing else.
///
/// Not the apikey. storage-api reads `request.headers.authorization`,
/// strips a leading Bearer if there is one, and verifies whatever is
/// left, so a request carrying only an apikey is a request carrying no
/// token at all. That is also why this surface sits outside zou's
/// apikey gate: inside it, a request with no key would be answered by
/// the gate in the gateway's words, and the reference answers it in
/// storage-api's.
pub(crate) fn caller(
    app: &App,
    parts: &Parts,
) -> Result<(RequestContext, jwt::Verified), StorageError> {
    let raw = parts
        .headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = match raw.get(..7) {
        Some(prefix) if prefix.eq_ignore_ascii_case("bearer ") => &raw[7..],
        _ => raw,
    };
    let verified = jwt::verify_any(token, &app.cfg.jwt_secret, app.jwks.as_ref())
        .map_err(|why| StorageError::access_denied(jose_words(&why).to_string()))?;

    let mut headers = serde_json::Map::new();
    for (name, value) in &parts.headers {
        if let Ok(v) = value.to_str() {
            headers.insert(name.as_str().to_string(), serde_json::Value::from(v));
        }
    }
    let mut cookies = serde_json::Map::new();
    if let Some(line) = parts
        .headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
    {
        for item in line.split(';') {
            if let Some((name, value)) = item.trim().split_once('=') {
                cookies.insert(name.to_string(), serde_json::Value::from(value));
            }
        }
    }
    let ctx = RequestContext {
        role: verified
            .role
            .clone()
            .unwrap_or_else(|| app.cfg.anon_role.clone()),
        claims: verified.claims.to_string(),
        method: parts.method.as_str().to_string(),
        path: parts.uri.path().to_string(),
        headers: serde_json::Value::Object(headers).to_string(),
        cookies: serde_json::Value::Object(cookies).to_string(),
        // Everything below is schema qualified anyway, so this is a
        // fence rather than a convenience: a policy body that names a
        // bare table resolves inside storage and nowhere else.
        search_path: "\"storage\"".to_string(),
    };
    Ok((ctx, verified))
}

/// A pg failure in the shape storage-api gives it. Only the two codes
/// the bucket surface can really produce are named. Anything else is
/// the database saying something we have not seen, and it goes out as
/// itself rather than dressed up as one of these.
pub(crate) fn pg_error(e: &tokio_postgres::Error) -> StorageError {
    let Some(db) = e.as_db_error() else {
        return StorageError::internal(e.to_string());
    };
    match db.code().code() {
        // storage-api replaces the message rather than passing
        // postgres's through, which drops the `for table "buckets"`
        // postgres puts on the end. Copied because a client matching on
        // the string is matching on the short one.
        "42501" if db.message().contains("row-level security") => {
            StorageError::access_denied("new row violates row-level security policy".to_string())
        }
        "42501" => StorageError::access_denied(db.message().to_string()),
        "23505" => StorageError::already_exists(),
        _ => StorageError::internal(db.message().to_string()),
    }
}

/// The columns a bucket answer is made of, rendered by postgres rather
/// than assembled here.
///
/// Two reasons it is json built in sql. The timestamps have to come out
/// the way a javascript Date prints them, milliseconds and a Z, and
/// to_char is the shortest honest way to say that. And `owner` is a
/// nullable uuid that the reference answers as the empty string,
/// because its response serializer is told the field is a string and
/// coerces null into one; a cast and a coalesce say the same thing
/// without pretending it was a deliberate api decision.
///
/// `type` is a literal rather than the column of that name. The listing
/// asks for it and the single bucket route does not, which is the whole
/// reason this takes a flag.
fn bucket_columns(with_type: bool) -> String {
    let bucket_type = if with_type {
        ", 'STANDARD' as type"
    } else {
        ""
    };
    format!(
        "id, name, coalesce(owner::text, '') as owner, public{bucket_type},
         file_size_limit, allowed_mime_types,
         to_char(created_at at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') as created_at,
         to_char(updated_at at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') as updated_at"
    )
}

/// Open the request's transaction, or say there is no database to open
/// one on.
pub(crate) async fn begin(
    app: &App,
    ctx: &RequestContext,
    read_only: bool,
) -> Result<Session, StorageError> {
    let pool = app
        .pool
        .as_ref()
        .ok_or_else(|| StorageError::internal("zou is running without a database".to_string()))?;
    pool.session(ctx, read_only).await.map_err(|e| pg_error(&e))
}

/// Is the bucket there, as far as this caller is concerned?
///
/// Which is not the same question as whether it exists. A row that row
/// level security hides is a row that is not there, and every route
/// that changes a bucket asks this first, which is why an anon key
/// hears "not found" rather than "not allowed" from update, empty and
/// delete alike.
async fn bucket_exists(sess: &Session, id: &str) -> Result<bool, StorageError> {
    let rows = sess
        .query(
            "select 1 from storage.buckets where id = $1 limit 1",
            &[&id],
        )
        .await
        .map_err(|e| pg_error(&e))?;
    Ok(!rows.is_empty())
}

/// Give the connection back, then answer.
///
/// A refusal still owes the pool its connection: the transaction did
/// nothing worth keeping but it is still open, and a session dropped on
/// the floor forfeits the connection under it. A failed statement is
/// the other case and does drop, the same containment the rest surface
/// gives a query that broke its own transaction.
pub(crate) async fn done<T>(
    sess: Session,
    answer: Result<T, StorageError>,
) -> Result<T, StorageError> {
    sess.commit().await.map_err(|e| pg_error(&e))?;
    answer
}

/// GET /storage/v1/bucket
pub async fn list(State(app): State<Arc<App>>, parts: Parts) -> Result<Response, StorageError> {
    let (ctx, _) = caller(&app, &parts)?;
    let sess = begin(&app, &ctx, true).await?;
    // No order by, deliberately. The reference does not write one
    // either, so both answer in whatever order the scan hands back, and
    // an order added here would be a difference rather than a fix. The
    // day upstream grows a sort parameter this grows one too.
    // Joined by hand rather than through json_agg, which writes a
    // newline and a space between elements. Both spellings parse to the
    // same array and only one of them is the bytes the reference sent.
    let sql = format!(
        "select coalesce('[' || string_agg(to_json(b)::text, ',') || ']', '[]')
         from (select {} from storage.buckets) b",
        bucket_columns(true)
    );
    let rows = sess.query(&sql, &[]).await.map_err(|e| pg_error(&e))?;
    let body = rows
        .first()
        .map(|r| r.get::<_, String>(0))
        .unwrap_or_else(|| "[]".to_string());
    done(sess, Ok(ok(body))).await
}

/// GET /storage/v1/bucket/{id}
pub async fn get(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    parts: Parts,
) -> Result<Response, StorageError> {
    let (ctx, _) = caller(&app, &parts)?;
    let sess = begin(&app, &ctx, true).await?;
    let sql = format!(
        "select to_json(b)::text
         from (select {} from storage.buckets where id = $1 limit 1) b",
        bucket_columns(false)
    );
    let rows = sess.query(&sql, &[&id]).await.map_err(|e| pg_error(&e))?;
    let found = rows.first().map(|r| r.get::<_, String>(0));
    done(sess, found.map(ok).ok_or_else(StorageError::no_such_bucket)).await
}

/// The body of a create or an update, only the fields either of them
/// reads.
///
/// Every field is optional even though a create needs a name, because
/// what a missing name earns is a validation failure rather than a
/// null, and that is a different answer with a different shape.
struct BucketBody {
    raw: serde_json::Value,
    name: Option<String>,
    id: Option<String>,
    public: Option<bool>,
    file_size_limit: Option<i64>,
    allowed_mime_types: Option<Vec<String>>,
}

impl BucketBody {
    /// Was the field written down at all? Which is not whether it has a
    /// value: an update reads a field sent as null as an instruction to
    /// clear the column, and a field left out as an instruction to
    /// leave it alone.
    fn sent(&self, field: &str) -> bool {
        self.raw.get(field).is_some()
    }
}

/// The pattern upstream's schema holds a size limit up against, quoted
/// into the refusal exactly as it is written there, backslash and all.
const SIZE_PATTERN: &str = r"^[0-9]+(?:\.[0-9]+)?(?:[gG][bB]|[mM][bB]|[kK][bB]|[bB])$";

/// A size limit that is not one, in the reference's words.
///
/// The schema is three branches in an `anyOf` and the message is every
/// branch's complaint joined up, so the sentence is long and it is the
/// sentence a client sees. `finite` is the middle branch's own keyword,
/// and whether it appears is the one thing that moves: a number that is
/// finite and not whole passes that branch and fails the other two, and
/// anything else fails all three.
fn not_a_size(finite: bool) -> StorageError {
    let mut said = String::from("body/file_size_limit must be integer");
    if !finite {
        said.push_str(", body/file_size_limit must pass \"finite\" keyword validation");
    }
    said.push_str(&format!(
        ", body/file_size_limit must match pattern \"{SIZE_PATTERN}\", \
         body/file_size_limit must match a schema in anyOf"
    ));
    StorageError::not_valid(said)
}

/// What a `file_size_limit` is, which is a number, or a string with a
/// unit on the end, or a string with no unit that is a number anyway,
/// or a yes.
///
/// storage-js passes whatever it was handed straight through, so a
/// bucket made with `fileSizeLimit: '20mb'` arrives here as words. The
/// schema in front of the reference coerces before it validates, which
/// is where the odder answers come from: a yes is a one, an empty
/// string is a null, and `'1024'` is a thousand and twenty four. All
/// recorded.
fn size_limit(v: &serde_json::Value) -> Result<Option<i64>, StorageError> {
    match v {
        serde_json::Value::Null => Ok(None),
        // Coerced rather than refused, which is what a schema that
        // takes a number does to a boolean. false is a zero and zero is
        // a bucket that takes nothing, so this is a foot gun copied on
        // purpose.
        serde_json::Value::Bool(yes) => Ok(Some(i64::from(*yes))),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(most) => Ok(Some(most)),
            // 20.0 is a whole number where the reference runs, because
            // there is only one kind of number there, so a size written
            // with a nought after the point is the size and not a
            // refusal. A number that is finite and not whole is the one
            // case that gets the shorter sentence.
            None => match n.as_f64() {
                Some(f) if f.is_finite() && f.fract() == 0.0 => Ok(Some(f as i64)),
                Some(f) if f.is_finite() => Err(not_a_size(true)),
                _ => Err(not_a_size(false)),
            },
        },
        serde_json::Value::String(said) => spelled_size(said),
        _ => Err(not_a_size(false)),
    }
}

/// A size limit written down rather than counted out.
fn spelled_size(said: &str) -> Result<Option<i64>, StorageError> {
    // An empty string is the null branch rather than the number one,
    // which is how the coercion in front of the reference reads it, so
    // sending one clears the column instead of being refused.
    if said.is_empty() {
        return Ok(None);
    }
    // The number branch first, because a size sent as `'1024'` is a
    // number that happens to be in quotes and lands there rather than
    // in the pattern, which has no unitless spelling.
    if let Ok(n) = said.trim().parse::<f64>()
        && n.is_finite()
        && n.fract() == 0.0
        && n >= i64::MIN as f64
        && n <= i64::MAX as f64
    {
        return Ok(Some(n as i64));
    }
    let (count, unit) = match said.as_bytes() {
        [.., a, b] if unit_of(*a, *b).is_some() => {
            (&said[..said.len() - 2], unit_of(*a, *b).unwrap())
        }
        [.., b] if b.eq_ignore_ascii_case(&b'b') => (&said[..said.len() - 1], 1),
        _ => return Err(not_a_size(false)),
    };
    // The pattern by hand: digits, then at most one dot with digits
    // after it, and nothing else. No sign, no exponent, no space, which
    // is why a minus and a `20 mb` are both refused.
    let (whole, fraction) = match count.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (count, ""),
    };
    let numbers = |s: &str| !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit());
    if !numbers(whole) || (count.contains('.') && !numbers(fraction)) {
        return Err(not_a_size(false));
    }
    let Ok(count) = count.parse::<f64>() else {
        return Err(not_a_size(false));
    };
    // A thousand and not a thousand and twenty four, so a megabyte here
    // is twenty million and a mebibyte is not a spelling at all. The
    // rounding down only shows on a fraction of a byte, which nobody
    // has asked for and nothing has recorded.
    Ok(Some((count * unit as f64) as i64))
}

/// The three units with a letter in front of the b, or nothing.
fn unit_of(a: u8, b: u8) -> Option<i64> {
    if !b.eq_ignore_ascii_case(&b'b') {
        return None;
    }
    match a.to_ascii_lowercase() {
        b'g' => Some(1_000_000_000),
        b'm' => Some(1_000_000),
        b'k' => Some(1_000),
        _ => None,
    }
}

/// Read the body as json, or say what the reference says about a body
/// that is not.
///
/// Read field by field rather than through a derive, which is how the
/// rest of this server reads json too. It also happens to be the
/// forgiving reading: upstream validates against a schema and refuses a
/// field of the wrong type, and what it says when it does is not
/// recorded, so a wrong type here is treated as a field that was not
/// usable rather than answered with a sentence nobody has checked. The
/// size limit is the exception, because what upstream says about that
/// one is recorded.
async fn body_of(body: Body) -> Result<BucketBody, StorageError> {
    let bytes = axum::body::to_bytes(body, BODY_LIMIT)
        .await
        .map_err(|_| StorageError::bad_json())?;
    let raw: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| StorageError::bad_json())?;
    Ok(BucketBody {
        name: raw.get("name").and_then(|v| v.as_str()).map(str::to_string),
        id: raw.get("id").and_then(|v| v.as_str()).map(str::to_string),
        public: raw.get("public").and_then(|v| v.as_bool()),
        file_size_limit: match raw.get("file_size_limit") {
            Some(v) => size_limit(v)?,
            None => None,
        },
        allowed_mime_types: raw.get("allowed_mime_types").and_then(|v| {
            v.as_array().map(|a| {
                a.iter()
                    .filter_map(|m| m.as_str().map(str::to_string))
                    .collect()
            })
        }),
        raw,
    })
}

/// POST /storage/v1/bucket
pub async fn create(
    State(app): State<Arc<App>>,
    parts: Parts,
    body: Body,
) -> Result<Response, StorageError> {
    let (ctx, verified) = caller(&app, &parts)?;
    let bucket = body_of(body).await?;

    let Some(name) = bucket.name.clone() else {
        // Not recorded here, but the same schema failure is recorded on
        // the move route and on three of the signed url ones, so the
        // shape is copied rather than guessed at.
        return Err(StorageError::missing_property("body", "name"));
    };
    // A create with no id takes the name as one. Recorded, and not
    // something the documentation says.
    let id = bucket.id.clone().unwrap_or_else(|| name.clone());
    if let Some(why) = over_the_ceiling(bucket.file_size_limit) {
        return Err(why);
    }
    let sub = verified
        .claims
        .get("sub")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();

    let sess = begin(&app, &ctx, false).await?;
    // owner is a uuid and owner_id is text, and upstream writes the
    // subject into both while marking the first deprecated. A service
    // token has no subject at all and a third party one may have a
    // subject that is not a uuid, so the cast is guarded rather than
    // attempted. Guarded in sql because postgres is the end holding the
    // column definition, and because zou carries no uuid type of its
    // own to parse it with on this side.
    sess.execute(
        "insert into storage.buckets
             (id, name, owner, owner_id, public, file_size_limit, allowed_mime_types, type)
         values ($1, $2,
                 case when $3::text ~*
                     '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                     then $3::text::uuid end,
                 nullif($3::text, ''),
                 coalesce($4, false), $5, $6, 'STANDARD')",
        &[
            &id,
            &name,
            &sub,
            &bucket.public,
            &bucket.file_size_limit,
            &bucket.allowed_mime_types,
        ],
    )
    .await
    .map_err(|e| pg_error(&e))?;
    done(
        sess,
        Ok(ok(serde_json::json!({ "name": name }).to_string())),
    )
    .await
}

/// PUT /storage/v1/bucket/{id}
pub async fn update(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    parts: Parts,
    body: Body,
) -> Result<Response, StorageError> {
    let (ctx, _) = caller(&app, &parts)?;
    let bucket = body_of(body).await?;

    // Present, not non null. The reference filters on undefined, so a
    // field sent as null clears the column and a field left out leaves
    // it alone, and those are two different statements rather than one
    // statement with a coalesce in it.
    let mut sets: Vec<String> = Vec::new();
    let mut args: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&id];
    if bucket.sent("public") {
        args.push(&bucket.public);
        sets.push(format!("public = ${}", args.len()));
    }
    if bucket.sent("file_size_limit") {
        args.push(&bucket.file_size_limit);
        sets.push(format!("file_size_limit = ${}", args.len()));
    }
    if bucket.sent("allowed_mime_types") {
        args.push(&bucket.allowed_mime_types);
        sets.push(format!("allowed_mime_types = ${}", args.len()));
    }

    // Before the bucket is looked for, which is a guess: the bucket in
    // the recorded case is there, so nothing says whether a bucket that
    // is not there and a limit that is too big answers the one or the
    // other.
    if bucket.sent("file_size_limit")
        && let Some(why) = over_the_ceiling(bucket.file_size_limit)
    {
        return Err(why);
    }

    let sess = begin(&app, &ctx, false).await?;
    // Asked before the update rather than read off its row count,
    // because a body with nothing in it still owes the caller an answer
    // about whether the bucket is there, and reading it off the update
    // would need this query anyway for that case.
    if !bucket_exists(&sess, &id).await? {
        return done(sess, Err(StorageError::no_such_bucket())).await;
    }
    if !sets.is_empty() {
        let sql = format!(
            "update storage.buckets set {} where id = $1",
            sets.join(", ")
        );
        sess.execute(&sql, &args).await.map_err(|e| pg_error(&e))?;
    }
    done(sess, Ok(message("Successfully updated"))).await
}

/// A bucket may not ask to take more than the server will take at all.
///
/// Both routes that write the column check it, because a bucket that
/// could be made small and then widened to a terabyte would have a
/// limit only until somebody sent a second request. Recorded on each of
/// them, and answered with the words an upload over the limit gets
/// rather than with words about a bucket.
fn over_the_ceiling(asked: Option<i64>) -> Option<StorageError> {
    match asked {
        Some(most) if most > crate::object::UPLOAD_LIMIT as i64 => Some(StorageError::too_large()),
        _ => None,
    }
}

/// The setting storage-api's own deletes turn on.
///
/// The storage schema refuses a delete that did not come through the
/// api, from a statement trigger that reads this. Turning it on is not
/// a way around the guard, it is how the guard is meant to be opened:
/// its point is that a hand written delete in a psql session does not
/// silently orphan objects in the store, not that the server cannot
/// delete anything. Local, so it lasts this transaction and no longer.
pub(crate) const ALLOW_DELETE: &str = "set local storage.allow_delete_query = 'true'";

/// POST /storage/v1/bucket/{id}/empty
pub async fn empty(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    parts: Parts,
) -> Result<Response, StorageError> {
    let (ctx, _) = caller(&app, &parts)?;
    let sess = begin(&app, &ctx, false).await?;
    if !bucket_exists(&sess, &id).await? {
        return done(sess, Err(StorageError::no_such_bucket())).await;
    }
    sess.execute(ALLOW_DELETE, &[])
        .await
        .map_err(|e| pg_error(&e))?;
    sess.execute("delete from storage.objects where bucket_id = $1", &[&id])
        .await
        .map_err(|e| pg_error(&e))?;
    // Queued, says the reference, whether or not anything was in it and
    // whether or not there is a queue. The sentence is the contract.
    done(
        sess,
        Ok(message(
            "Empty bucket has been queued. Completion may take up to an hour.",
        )),
    )
    .await
}

/// DELETE /storage/v1/bucket/{id}
pub async fn remove(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    parts: Parts,
) -> Result<Response, StorageError> {
    let (ctx, _) = caller(&app, &parts)?;
    let sess = begin(&app, &ctx, false).await?;
    if !bucket_exists(&sess, &id).await? {
        return done(sess, Err(StorageError::no_such_bucket())).await;
    }
    // Asked rather than left to the foreign key. The reference asks the
    // same question and answers it in its own words, and a caller who
    // got the constraint violation instead would be reading about a
    // column name.
    let occupied = !sess
        .query(
            "select 1 from storage.objects where bucket_id = $1 limit 1",
            &[&id],
        )
        .await
        .map_err(|e| pg_error(&e))?
        .is_empty();
    if occupied {
        return done(sess, Err(StorageError::not_empty())).await;
    }
    sess.execute(ALLOW_DELETE, &[])
        .await
        .map_err(|e| pg_error(&e))?;
    sess.execute("delete from storage.buckets where id = $1", &[&id])
        .await
        .map_err(|e| pg_error(&e))?;
    done(sess, Ok(message("Successfully deleted"))).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn size(json: &str) -> Result<Option<i64>, StorageError> {
        size_limit(&serde_json::from_str(json).expect("the test wrote json"))
    }

    #[test]
    fn a_size_written_in_words_is_counted_in_thousands() {
        assert_eq!(size(r#""20mb""#).unwrap(), Some(20_000_000));
        assert_eq!(size(r#""20MB""#).unwrap(), Some(20_000_000));
        assert_eq!(size(r#""1kb""#).unwrap(), Some(1_000));
        assert_eq!(size(r#""1gb""#).unwrap(), Some(1_000_000_000));
        assert_eq!(size(r#""1000b""#).unwrap(), Some(1_000));
        assert_eq!(size(r#""0.5mb""#).unwrap(), Some(500_000));
    }

    #[test]
    fn a_size_that_is_a_number_in_quotes_is_the_number() {
        assert_eq!(size(r#""1024""#).unwrap(), Some(1024));
        assert_eq!(size("1024").unwrap(), Some(1024));
        assert_eq!(size("1024.0").unwrap(), Some(1024));
    }

    #[test]
    fn an_empty_size_clears_the_column_and_a_yes_is_a_one() {
        assert_eq!(size(r#""""#).unwrap(), None);
        assert_eq!(size("null").unwrap(), None);
        assert_eq!(size("true").unwrap(), Some(1));
        assert_eq!(size("false").unwrap(), Some(0));
        assert_eq!(size(r#""0""#).unwrap(), Some(0));
    }

    #[test]
    fn the_units_are_the_four_the_pattern_names_and_no_others() {
        for said in [
            r#""20mib""#,
            r#""1tb""#,
            r#""20 mb""#,
            r#""20 bananas""#,
            r#""abc""#,
            r#""-1mb""#,
            r#""b""#,
            r#""mb""#,
            r#"".5mb""#,
        ] {
            assert!(size(said).is_err(), "{said} is not a size");
        }
    }

    #[test]
    fn only_a_number_that_is_finite_and_not_whole_gets_the_shorter_sentence() {
        let long = size(r#""abc""#).unwrap_err().message;
        let short = size("1024.5").unwrap_err().message;
        assert!(long.contains("must pass \"finite\" keyword validation"));
        assert!(!short.contains("finite"));
        assert!(short.starts_with("body/file_size_limit must be integer, body/file_size_limit"));
        assert!(long.ends_with("must match a schema in anyOf"));
    }
}
