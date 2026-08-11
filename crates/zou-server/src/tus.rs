//! Resumable uploads, over `/storage/v1/upload/resumable`.
//!
//! The tus protocol, version 1.0.0, and the one place on the object
//! surface where the answer is headers rather than a body. A creation
//! answers 201 with nothing in it and says where the bytes go in
//! `location`; a head answers 200 with nothing in it and says how far
//! the upload got in `upload-offset`; what the endpoint can do is three
//! headers on an options. Sixty of the suite's cases are about this
//! file and most of them compare no body at all.
//!
//! Three things are worth knowing before reading the rest.
//!
//! A refusal from these routes is not shaped like a refusal from the
//! rest of the storage api. Everywhere else the status on the wire is
//! 400 and the real one is a string inside a json body. Here the status
//! is the status and the body is a sentence in text, some of them
//! ending in a newline and some not. That is the seam between the
//! protocol library upstream runs and upstream's own list of refusals
//! showing through, and both spellings are copied rather than tidied
//! up, because a client matching on the string is matching on what it
//! was sent.
//!
//! A request carrying no usable token is refused in the other shape,
//! because the token is read before the protocol is. So the two kinds
//! live in [`Wrong`] and a handler can answer either.
//!
//! An upload is a row in `storage.s3_multipart_uploads` and its bytes
//! are one blob for every request that carried any, listed in
//! `storage.s3_multipart_uploads_parts`. Nothing is joined up until the
//! offset reaches the length that was declared, and at that moment the
//! parts are read in order and written as an ordinary object by the
//! same code an ordinary upload ends in. Which is why an upload that
//! was half sent is not an object and cannot be listed: there is no row
//! in `storage.objects` for it yet, and the suite asks that twice.
//!
//! What the reference does with the metadata is worth writing down,
//! since none of it is in the protocol. `bucketName` and `objectName`
//! say where the object goes, `contentType` is what it is, and any
//! other key is carried along and then dropped rather than kept as the
//! object's user metadata. `cacheControl` is dropped too and every
//! object a resumable upload makes is `no-cache`, which is not what the
//! same request through the ordinary upload door does. Those two are
//! recorded next to each other so that the difference is a decision
//! rather than a guess.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderName, StatusCode, header, request::Parts};
use axum::response::{IntoResponse, Response};
use base64ct::Encoding;
use serde_json::{Value, json};

use crate::App;
use crate::blob::{self, Blobs};
use crate::object::{self, Upload};
use crate::sql::Session;
use crate::storage::{StorageError, begin, caller, pg_error};

/// The version of the protocol this speaks, which is the only one there
/// is.
const VERSION: &str = "1.0.0";

/// What the endpoint says it can do, in the order it says it. The
/// header is one string and it is compared as one.
const EXTENSIONS: &str =
    "creation,creation-with-upload,creation-defer-length,termination,expiration";

/// The largest upload the endpoint will take, in bytes, which is not
/// the limit on an ordinary upload. Upstream answers fifty two thousand
/// million here: the fifty megabytes one request may carry, a thousand
/// times over, and a resumable upload is many requests.
const MAX_SIZE: u64 = 52_428_800_000;

/// The content type every answer with a body on these routes carries.
/// No space after the semicolon and an upper case charset, which is
/// neither of the two spellings the rest of the surface uses.
const TEXT: &str = "text/plain;charset=UTF-8";

/// The content type the protocol fixes for a request carrying bytes, on
/// the creation as well as on the patch.
const OFFSET_OCTET_STREAM: &str = "application/offset+octet-stream";

/// What an upload that says nothing about its type is taken to be.
const OCTET_STREAM: &str = "application/octet-stream";

/// The cache control every object a resumable upload makes gets,
/// whether or not its metadata asked for something else.
const NO_CACHE: &str = "no-cache";

const TUS_RESUMABLE: HeaderName = HeaderName::from_static("tus-resumable");
const TUS_VERSION: HeaderName = HeaderName::from_static("tus-version");
const TUS_EXTENSION: HeaderName = HeaderName::from_static("tus-extension");
const TUS_MAX_SIZE: HeaderName = HeaderName::from_static("tus-max-size");
const UPLOAD_OFFSET: HeaderName = HeaderName::from_static("upload-offset");
const UPLOAD_LENGTH: HeaderName = HeaderName::from_static("upload-length");
const UPLOAD_DEFER_LENGTH: HeaderName = HeaderName::from_static("upload-defer-length");
const UPLOAD_METADATA: HeaderName = HeaderName::from_static("upload-metadata");

/// A refusal in the protocol's shape: a real status and a sentence.
#[derive(Debug)]
pub struct Refusal {
    status: StatusCode,
    /// Ends in a newline or does not, depending on which side of
    /// upstream it comes from. The recording compares the bytes.
    message: String,
}

impl Refusal {
    fn new(status: u16, message: &str) -> Refusal {
        Refusal {
            status: StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_REQUEST),
            message: message.to_string(),
        }
    }
}

/// What can go wrong here, which is two unrelated kinds of thing
/// answered in two unrelated shapes.
pub enum Wrong {
    /// Refused before the protocol was reached, which is only ever the
    /// token. Answered the way the rest of the storage api answers,
    /// since it is the same check, and with no `tus-resumable` on it.
    Gate(StorageError),
    /// Refused by the protocol.
    Tus(Refusal),
}

impl From<StorageError> for Wrong {
    fn from(why: StorageError) -> Wrong {
        Wrong::Gate(why)
    }
}

impl From<Refusal> for Wrong {
    fn from(why: Refusal) -> Wrong {
        Wrong::Tus(why)
    }
}

impl IntoResponse for Wrong {
    fn into_response(self) -> Response {
        match self {
            Wrong::Gate(why) => crate::storage::as_tus(&why),
            Wrong::Tus(why) => (
                why.status,
                [(header::CONTENT_TYPE, TEXT), (TUS_RESUMABLE, VERSION)],
                why.message,
            )
                .into_response(),
        }
    }
}

/// A failure from postgres, in this surface's shape.
///
/// Through [`pg_error`] first, so that row level security is the one
/// short sentence upstream replaces postgres's longer one with. Only
/// that refusal is turned into the protocol's shape; anything else is
/// the database saying something the recording has never seen, and it
/// goes out as itself.
fn refused_by_postgres(e: &tokio_postgres::Error) -> Wrong {
    let why = pg_error(e);
    match why.code {
        "AccessDenied" => Wrong::Tus(Refusal::new(403, why.said())),
        _ => Wrong::Gate(why),
    }
}

/// What one upload knows about itself: the row, and the state kept in
/// the json column beside it.
struct InProgress {
    id: String,
    bucket: String,
    key: String,
    /// How many bytes have arrived, which is `in_progress_size`.
    offset: i64,
    /// What the client said the whole thing would be, or nothing while
    /// it is still deferring saying.
    length: Option<i64>,
    mime: String,
    /// Whether the creation said it meant to replace what is there.
    replace: bool,
    /// The metadata as it arrived, so a head can hand it back.
    metadata: Vec<(String, String)>,
    /// How many requests have carried bytes, which is also the number
    /// the next part gets.
    parts: i32,
}

/// Everything sent to `/storage/v1/upload/resumable`.
///
/// One handler and a match rather than a method router, because a
/// method the protocol does not define is answered by upstream's router
/// in upstream's words, and axum would put an `allow` header on the
/// refusal it made for itself. Recorded: a get here is a 404 naming the
/// route and carrying nothing else.
pub async fn endpoint(State(app): State<Arc<App>>, parts: Parts, body: Body) -> Response {
    match parts.method.as_str() {
        "POST" => create(&app, parts, body).await.into_response(),
        "OPTIONS" => options().await,
        _ => no_such_route(&parts),
    }
}

/// Everything sent to one upload's own url, the same way.
pub async fn upload(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    parts: Parts,
    body: Body,
) -> Response {
    match parts.method.as_str() {
        "HEAD" => head(&app, &id, parts).await.into_response(),
        "PATCH" => patch(&app, &id, parts, body).await.into_response(),
        "DELETE" => terminate(&app, &id, parts).await.into_response(),
        _ => no_such_route(&parts),
    }
}

/// OPTIONS /storage/v1/upload/resumable
///
/// Asked with no token, deliberately. A browser sends this before it
/// sends anything else and it sends it with none of its own headers on
/// it, so a key here would make the endpoint unusable from a page.
/// Nothing is read and nothing is written, so there is nothing to
/// protect.
async fn options() -> Response {
    (
        StatusCode::NO_CONTENT,
        [
            (TUS_RESUMABLE, VERSION.to_string()),
            (TUS_VERSION, VERSION.to_string()),
            (TUS_EXTENSION, EXTENSIONS.to_string()),
            (TUS_MAX_SIZE, MAX_SIZE.to_string()),
        ],
    )
        .into_response()
}

/// POST /storage/v1/upload/resumable
///
/// The order the checks are in is the order the recording puts them in,
/// and some of the cases are there only to pin it down: a creation
/// saying nothing at all is told about the length rather than the
/// metadata, and a terabyte into a bucket that is not there is told
/// about the bucket rather than the terabyte.
async fn create(app: &Arc<App>, parts: Parts, body: Body) -> Result<Response, Wrong> {
    let (ctx, verified) = caller(app, &parts)?;
    speaks_tus(&parts)?;

    let asked = declared_length(&parts)?;
    let metadata = metadata_of(&parts)?;
    let key = value_of(&metadata, "objectName").unwrap_or_default();
    if key.is_empty() {
        return Err(Refusal::new(400, &format!("Invalid key: {key}")).into());
    }
    let bucket = value_of(&metadata, "bucketName").unwrap_or_default();
    let mime = match value_of(&metadata, "contentType") {
        Some(mime) if !mime.trim().is_empty() => mime,
        _ => OCTET_STREAM.to_string(),
    };
    let replace = parts
        .headers
        .get("x-upsert")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|asked| asked.eq_ignore_ascii_case("true"));
    let sub = subject(&verified);

    // The bytes before the transaction, because a creation that carries
    // them is one request and a creation that does not is the same
    // request with nothing in it. A creation that says nothing about
    // its content type carries nothing even when there is a body: the
    // protocol fixes what a request with bytes in it looks like.
    let carried = match content_type(&parts) == OFFSET_OCTET_STREAM {
        true => Some(read_body(body).await?),
        false => None,
    };

    let sess = begin(app, &ctx, false).await?;
    // With the policies out of the way, because a caller who may not
    // write into a bucket still hears that the bucket is there. The
    // refusal about the policies comes from the insert below.
    let there = object::unpoliced(&sess, &ctx.role, async || {
        object::bucket_facts(&sess, &bucket).await
    })
    .await?;
    let Some(limits) = there else {
        return refuse(sess, Refusal::new(404, "Bucket not found")).await;
    };
    if asked.is_some_and(|length| length > MAX_SIZE) {
        return refuse(sess, too_large()).await;
    }
    if let Some(why) = beyond(&limits, &mime, asked) {
        return refuse(sess, why).await;
    }
    // Whether the name is taken is answered now rather than when the
    // upload finishes, which is what saves a client sending a gigabyte
    // to find out.
    if !replace && taken(&sess, &bucket, &key).await? {
        return refuse(sess, Refusal::new(409, "The resource already exists")).await;
    }
    // And whether this caller may write the object at all is answered
    // now for the same reason, by writing it and rolling it back. It
    // has to be asked here, because the rows below are the service's
    // own bookkeeping and no policy is written about them: without
    // this, a resumable upload would be the one door on the storage
    // api that row level security does not stand behind.
    if let Err(why) = may_write(&sess, &bucket, &key, &sub, replace).await {
        return commit(sess, Err(why)).await;
    }

    let version = uuid();
    let id = upload_id(&bucket, &key, &version);
    let state = json!({
        "length": asked,
        "mime": mime,
        "replace": replace,
        "metadata": metadata
            .iter()
            .map(|(name, value)| json!([name, value]))
            .collect::<Vec<Value>>(),
    })
    .to_string();
    object::unpoliced(&sess, &ctx.role, async || {
        sess.query(
            "insert into storage.s3_multipart_uploads
                 (id, upload_signature, bucket_id, key, version, owner_id, metadata)
             values ($1, '', $2, $3, $4, nullif($5::text, ''), $6::text::jsonb)",
            &[&id, &bucket, &key, &version, &sub, &state],
        )
        .await
        .map_err(|e| refused_by_postgres(&e))
    })
    .await?;

    // A creation carrying bytes is a creation and a patch at nothing in
    // the same request, and one carrying all of them finishes here.
    let offset = match carried {
        None => {
            commit(sess, Ok(())).await?;
            None
        }
        Some(bytes) => {
            let upload = InProgress {
                id: id.clone(),
                bucket,
                key,
                offset: 0,
                length: asked.map(|length| length as i64),
                mime,
                replace,
                metadata,
                parts: 0,
            };
            Some(accept(app, sess, &ctx.role, &sub, upload, bytes, None).await?)
        }
    };

    let mut answer = Response::builder()
        .status(StatusCode::CREATED)
        .header(header::CONTENT_TYPE, TEXT)
        .header(TUS_RESUMABLE, VERSION)
        .header(header::LOCATION, url_of(&parts, &id));
    if let Some(offset) = offset {
        answer = answer.header(UPLOAD_OFFSET, offset.to_string());
    }
    Ok(answer.body(Body::empty()).unwrap_or_default())
}

/// HEAD /storage/v1/upload/resumable/{id}
///
/// Where the upload has got to, which is the first thing a client that
/// lost its place asks. An upload that finished still answers, because
/// the row is left behind, and one that was given up on does not.
///
/// Every refusal on this route is a status and nothing else. The
/// sentences below are written the same way they are everywhere else
/// and then dropped on the way out, because a head has no body to say
/// them in.
async fn head(app: &Arc<App>, id: &str, parts: Parts) -> Result<Response, Wrong> {
    let (ctx, _) = caller(app, &parts)?;
    speaks_tus(&parts)?;
    let id = named(id)?;

    let sess = begin(app, &ctx, true).await?;
    let Some(upload) = look_up(&sess, &ctx.role, &id).await? else {
        return refuse(sess, gone()).await;
    };
    commit(sess, Ok(())).await?;

    let answer = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, TEXT)
        .header(TUS_RESUMABLE, VERSION)
        .header(UPLOAD_OFFSET, upload.offset.to_string())
        .header(UPLOAD_METADATA, written_metadata(&upload.metadata));
    let answer = match upload.length {
        Some(length) => answer.header(UPLOAD_LENGTH, length.to_string()),
        None => answer.header(UPLOAD_DEFER_LENGTH, "1"),
    };
    Ok(answer.body(Body::empty()).unwrap_or_default())
}

/// PATCH /storage/v1/upload/resumable/{id}
///
/// The bytes. The content type is read before the id is, which is
/// recorded: a patch to something that is not an id at all, sent as
/// text, is told about the text.
async fn patch(app: &Arc<App>, id: &str, parts: Parts, body: Body) -> Result<Response, Wrong> {
    let (ctx, verified) = caller(app, &parts)?;
    speaks_tus(&parts)?;
    if content_type(&parts) != OFFSET_OCTET_STREAM {
        return Err(Refusal::new(400, "Invalid content-type\n").into());
    }
    let id = named(id)?;
    let sub = subject(&verified);
    let bytes = read_body(body).await?;

    let sess = begin(app, &ctx, false).await?;
    let Some(upload) = look_up(&sess, &ctx.role, &id).await? else {
        return refuse(sess, gone()).await;
    };
    // An upload that reached its length is an object now, and the
    // object is what a second attempt collides with. Asked before the
    // offset is, since a client retrying its last request arrives at
    // exactly the offset the upload is already at.
    if upload.length == Some(upload.offset) {
        return refuse(sess, Refusal::new(409, "The resource already exists")).await;
    }
    if at(&parts) != Some(upload.offset) {
        return refuse(sess, Refusal::new(409, "Upload-Offset conflict\n")).await;
    }
    // A client that deferred saying how long it was says so on the
    // request that finishes it, which is the whole of that extension.
    let later = match upload.length {
        None => declared_length(&parts).unwrap_or(None).map(|n| n as i64),
        Some(_) => None,
    };
    let offset = accept(app, sess, &ctx.role, &sub, upload, bytes, later).await?;

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(TUS_RESUMABLE, VERSION)
        .header(UPLOAD_OFFSET, offset.to_string())
        .body(Body::empty())
        .unwrap_or_default())
}

/// DELETE /storage/v1/upload/resumable/{id}
///
/// Termination, which is one of the extensions the options answer
/// lists, and it is how a client that changed its mind says so rather
/// than leaving a row and its bytes behind for ever.
async fn terminate(app: &Arc<App>, id: &str, parts: Parts) -> Result<Response, Wrong> {
    let (ctx, _) = caller(app, &parts)?;
    speaks_tus(&parts)?;
    let id = named(id)?;

    let sess = begin(app, &ctx, false).await?;
    let Some(upload) = look_up(&sess, &ctx.role, &id).await? else {
        return refuse(sess, gone()).await;
    };
    let blobs = blobs(app)?;
    // The bytes before the row, because a delete that stops halfway
    // through can leave bytes nobody can reach, and doing it the other
    // way round leaves them with nothing left saying they are there.
    for part in 0..upload.parts {
        let _ = blobs.delete(blob::part_key(&upload.id, part)).await;
    }
    object::unpoliced(&sess, &ctx.role, async || {
        sess.query(
            "delete from storage.s3_multipart_uploads where id = $1",
            &[&upload.id],
        )
        .await
        .map_err(|e| refused_by_postgres(&e))?;
        // The parts rows name an upload that is gone, so they go with
        // it.
        sess.query(
            "delete from storage.s3_multipart_uploads_parts where upload_id = $1",
            &[&upload.id],
        )
        .await
        .map_err(|e| refused_by_postgres(&e))?;
        Ok::<(), Wrong>(())
    })
    .await?;
    commit(sess, Ok(())).await?;

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(TUS_RESUMABLE, VERSION)
        .body(Body::empty())
        .unwrap_or_default())
}

/// Any other method on either of these two paths, in the words
/// upstream's own router uses for a url it has no handler for.
///
/// Reachable only by a method the protocol does not define, since the
/// paths themselves exist. The path in the sentence is the one
/// storage-api sees, which is this one without the prefix the gateway
/// routes on.
fn no_such_route(parts: &Parts) -> Response {
    let path = parts.uri.path();
    let path = path.strip_prefix("/storage/v1").unwrap_or(path);
    // Written out rather than serialized, because the field order is
    // upstream's rather than sorted, the same as every other refusal on
    // this surface.
    let body = format!(
        "{{\"message\":{},\"error\":\"Not Found\",\"statusCode\":404}}",
        Value::from(format!("Route {}:{path} not found", parts.method))
    );
    (
        StatusCode::NOT_FOUND,
        [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
        body,
    )
        .into_response()
}

/// Give the connection back, then answer. The same bargain the rest of
/// the storage surface makes, in this file's error type.
async fn commit<T>(sess: Session, answer: Result<T, Wrong>) -> Result<T, Wrong> {
    sess.commit().await.map_err(|e| Wrong::Gate(pg_error(&e)))?;
    answer
}

/// Commit, then refuse. The transaction did nothing worth keeping and
/// the connection is still owed back.
async fn refuse<T>(sess: Session, why: Refusal) -> Result<T, Wrong> {
    commit(sess, Err(Wrong::Tus(why))).await
}

/// The refusals that are the same wherever they are reached from.
fn too_large() -> Refusal {
    Refusal::new(413, "Maximum size exceeded\n")
}

fn gone() -> Refusal {
    Refusal::new(404, "The file for this url was not found\n")
}

/// Does the request say it speaks this protocol? Every route but the
/// options asks first.
fn speaks_tus(parts: &Parts) -> Result<(), Refusal> {
    match parts
        .headers
        .get(TUS_RESUMABLE)
        .and_then(|v| v.to_str().ok())
    {
        None => Err(Refusal::new(412, "Tus-Resumable Required\n")),
        Some(VERSION) => Ok(()),
        // Not the 412 naming the versions that are spoken which the
        // protocol asks for. Upstream answers 400 and says only that
        // the header was wrong, and this is upstream.
        Some(_) => Err(Refusal::new(400, "Invalid tus-resumable\n")),
    }
}

/// How long the client says the whole upload is, or nothing when it
/// says it will say later.
fn declared_length(parts: &Parts) -> Result<Option<u64>, Refusal> {
    if let Some(length) = parts
        .headers
        .get(UPLOAD_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|text| text.trim().parse::<u64>().ok())
    {
        return Ok(Some(length));
    }
    let deferred = parts
        .headers
        .get(UPLOAD_DEFER_LENGTH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|asked| asked.trim() == "1");
    match deferred {
        true => Ok(None),
        // Read before the metadata is, which is recorded: a creation
        // carrying neither is told about this one.
        false => Err(Refusal::new(
            400,
            "Upload-Length or Upload-Defer-Length header required\n",
        )),
    }
}

/// Where the client says this request starts.
fn at(parts: &Parts) -> Option<i64> {
    parts
        .headers
        .get(UPLOAD_OFFSET)
        .and_then(|v| v.to_str().ok())
        .and_then(|text| text.trim().parse::<i64>().ok())
}

fn content_type(parts: &Parts) -> String {
    parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

/// The metadata header, split into what it carries.
///
/// `key value,key value`, where the value is base64 and a key on its
/// own is a flag with no value at all. Kept as a list rather than a map
/// because a head hands it back, and the order it goes out in is the
/// order it came in.
fn metadata_of(parts: &Parts) -> Result<Vec<(String, String)>, Refusal> {
    let header = parts
        .headers
        .get(UPLOAD_METADATA)
        .and_then(|v| v.to_str().ok())
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| Refusal::new(400, "Metadata header is required"))?;
    Ok(header
        .split(',')
        .filter_map(|pair| {
            let pair = pair.trim();
            if pair.is_empty() {
                return None;
            }
            let (name, value) = match pair.split_once(' ') {
                Some((name, value)) => (name, value.trim()),
                None => (pair, ""),
            };
            let decoded = base64ct::Base64::decode_vec(value)
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .unwrap_or_default();
            Some((name.to_string(), decoded))
        })
        .collect())
}

fn value_of(metadata: &[(String, String)], name: &str) -> Option<String> {
    metadata
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.clone())
}

/// The metadata as a head hands it back, which is what arrived with a
/// `cacheControl` on the end that nobody sent.
fn written_metadata(metadata: &[(String, String)]) -> String {
    let mut pairs = metadata.to_vec();
    if !pairs.iter().any(|(name, _)| name == "cacheControl") {
        pairs.push(("cacheControl".to_string(), NO_CACHE.to_string()));
    }
    pairs
        .iter()
        .map(|(name, value)| {
            format!(
                "{name} {}",
                base64ct::Base64::encode_string(value.as_bytes())
            )
        })
        .collect::<Vec<String>>()
        .join(",")
}

/// What the bucket says about an upload this long of this type, or
/// nothing if it takes it.
///
/// The length is the one the client declared rather than any bytes that
/// have arrived, which is the point of declaring it: a bucket that
/// takes twenty bytes refuses an upload that says it is forty before a
/// byte of it is sent.
fn beyond(bucket: &object::Bucket, mime: &str, length: Option<u64>) -> Option<Refusal> {
    if let Some(allowed) = &bucket.mime_types
        && !allowed.is_empty()
        && !allowed
            .iter()
            .any(|pattern| object::mime_matches(pattern, mime))
    {
        return Some(Refusal::new(
            415,
            &format!("mime type {mime} is not supported"),
        ));
    }
    match (bucket.size_limit, length) {
        (Some(most), Some(length)) if most >= 0 && length > most as u64 => Some(too_large()),
        _ => None,
    }
}

/// Is there an object of this name already, as far as this caller is
/// concerned?
/// May this caller write that object, asked by writing it and taking
/// it back?
///
/// Row level security is a rule about rows going in rather than a list
/// this end holds, so the only honest way to ask is to try. The signed
/// upload url route asks the same question the same way; the difference
/// here is the savepoint, because this is one step of a longer
/// transaction rather than a transaction of its own.
///
/// Every row a resumable upload writes before the last byte lands is on
/// the multipart tables, which have row level security on and no policy
/// written about them, so they are written with the policies off, as
/// upstream writes them. That is what makes this check load bearing: it
/// is the only place between the token and the finished object where a
/// policy gets to say no, and without it the whole endpoint would be
/// open to anybody holding any key.
async fn may_write(
    sess: &Session,
    bucket: &str,
    key: &str,
    sub: &str,
    replace: bool,
) -> Result<(), Wrong> {
    let conflict = match replace {
        true => {
            "on conflict (bucket_id, name) do update
                set version = excluded.version"
        }
        false => "",
    };
    let sql = format!(
        "insert into storage.objects
             (bucket_id, name, owner, owner_id, version)
         values ($1, $2,
                 case when $3::text ~*
                     '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
                     then $3::text::uuid end,
                 nullif($3::text, ''), '1')
         {conflict}
         returning id::text"
    );
    sess.query("savepoint before_asking", &[])
        .await
        .map_err(|e| refused_by_postgres(&e))?;
    let rows = sess.query(&sql, &[&bucket, &key, &sub]).await;
    // Rolled back either way, and to a savepoint rather than the whole
    // transaction, because the bucket lookup above happened in it and
    // the upload row below is about to.
    sess.query("rollback to savepoint before_asking", &[])
        .await
        .map_err(|e| refused_by_postgres(&e))?;
    let rows = rows.map_err(|e| match e.as_db_error().map(|db| db.code().code()) {
        // Two callers racing for the same name. The one that lost hears
        // what it would have heard from the check above, which is the
        // sentence for a name that is taken.
        Some("23505") => Wrong::Tus(Refusal::new(409, "The resource already exists")),
        _ => refused_by_postgres(&e),
    })?;
    // An upsert that collided with a row the policies hide writes
    // nothing and says nothing. Upstream signs a url for that caller
    // anyway; here there is no url to sign, and letting the upload
    // start would only move the refusal to the last byte.
    if rows.is_empty() {
        return Err(Wrong::Tus(Refusal::new(
            403,
            "new row violates row-level security policy",
        )));
    }
    Ok(())
}

async fn taken(sess: &Session, bucket: &str, key: &str) -> Result<bool, Wrong> {
    let rows = sess
        .query(
            "select 1 from storage.objects
              where bucket_id = $1 and name = $2 limit 1",
            &[&bucket, &key],
        )
        .await
        .map_err(|e| refused_by_postgres(&e))?;
    Ok(!rows.is_empty())
}

/// The id of an upload: where it is going, and which attempt it is.
///
/// The same three parts upstream uses, base64url with no padding,
/// because a client keeps this string in local storage and hands it
/// back days later. A shape that carries where it was going is one that
/// can still be looked up when everything else has been forgotten. The
/// uuid on the end is what lets the same name be uploaded twice at
/// once.
fn upload_id(bucket: &str, key: &str, version: &str) -> String {
    base64ct::Base64UrlUnpadded::encode_string(format!("{bucket}/{key}/{version}").as_bytes())
}

/// An id that is one, or the refusal for a string that is not.
///
/// The difference between this and an upload nobody made is recorded
/// and it is two statuses: a string that is not an id at all is a 400,
/// and an id naming an upload that is not there is a 404. `nope`
/// decodes to three bytes that are not utf8, which is why the suite
/// hears the first of those.
fn named(id: &str) -> Result<String, Refusal> {
    let decoded = base64ct::Base64UrlUnpadded::decode_vec(id)
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok());
    match decoded {
        Some(text) if text.split('/').count() >= 3 => Ok(id.to_string()),
        _ => Err(Refusal::new(400, "Invalid upload id")),
    }
}

/// Where the bytes go next, as a client reads it.
///
/// Absolute, out of the host the request arrived on, which is what
/// upstream answers and what a client that knows only this url needs.
/// The scheme comes from the proxy when there is one in front, since a
/// hosted project is behind tls and the request that reaches here is
/// not.
fn url_of(parts: &Parts, id: &str) -> String {
    let host = parts
        .headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let scheme = parts
        .headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("http");
    format!("{scheme}://{host}/storage/v1/upload/resumable/{id}")
}

/// The one upload, with the policies off.
///
/// Row level security on this table hides it from every role but the
/// one that bypasses, and no policy is written about it, which is
/// upstream's arrangement too: the resumable routes are the service's
/// own bookkeeping. Whether the caller may write the object is asked
/// where the upload is created, by [`may_write`], since that is the
/// question a policy can answer.
async fn look_up(sess: &Session, role: &str, id: &str) -> Result<Option<InProgress>, Wrong> {
    let rows = object::unpoliced(sess, role, async || {
        sess.query(
            "select u.id, u.bucket_id, u.key, u.in_progress_size,
                    coalesce(u.metadata::text, 'null'),
                    (select count(*) from storage.s3_multipart_uploads_parts p
                      where p.upload_id = u.id)
               from storage.s3_multipart_uploads u where u.id = $1",
            &[&id],
        )
        .await
        .map_err(|e| refused_by_postgres(&e))
    })
    .await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let state: Value = serde_json::from_str(&row.get::<_, String>(4)).unwrap_or(Value::Null);
    let metadata = state["metadata"]
        .as_array()
        .map(|pairs| {
            pairs
                .iter()
                .filter_map(|pair| {
                    Some((
                        pair.get(0)?.as_str()?.to_string(),
                        pair.get(1)?.as_str()?.to_string(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(Some(InProgress {
        id: row.get(0),
        bucket: row.get(1),
        key: row.get(2),
        offset: row.get(3),
        length: state["length"].as_i64(),
        mime: state["mime"].as_str().unwrap_or(OCTET_STREAM).to_string(),
        replace: state["replace"] == Value::Bool(true),
        metadata,
        parts: row.get::<_, i64>(5) as i32,
    }))
}

/// One request's worth of bytes, and the object when they were the last
/// of them.
///
/// Takes the session because finishing hands it on to the ordinary
/// upload path, which commits the object row and its bytes together.
/// Returns where the upload has got to, which is what the answer says.
/// `later` is the length a deferred upload is finally declaring.
async fn accept(
    app: &App,
    sess: Session,
    role: &str,
    sub: &str,
    upload: InProgress,
    bytes: Vec<u8>,
    later: Option<i64>,
) -> Result<i64, Wrong> {
    let length = later.or(upload.length);
    let arriving = upload.offset + bytes.len() as i64;
    // The length was a promise made when the upload was created, and
    // this is the one refusal that cannot be worked out from the
    // headers: the body is what breaks it.
    if length.is_some_and(|length| arriving > length) {
        return refuse(sess, Refusal::new(413, "upload's size exceeded\n")).await;
    }

    let blobs = blobs(app)?;
    let size = bytes.len() as i64;
    if let Err(e) = blobs
        .put(blob::part_key(&upload.id, upload.parts), bytes)
        .await
    {
        let _ = sess.rollback().await;
        return Err(Wrong::Gate(StorageError::internal(e.to_string())));
    }
    // Both of these are the service's bookkeeping, see [`look_up`].
    object::unpoliced(&sess, role, async || {
        sess.query(
            "insert into storage.s3_multipart_uploads_parts
                 (upload_id, size, part_number, bucket_id, key, etag, version, owner_id)
             values ($1, $2, $3, $4, $5, '', $6, nullif($7::text, ''))",
            &[
                &upload.id,
                &size,
                &upload.parts,
                &upload.bucket,
                &upload.key,
                &upload.id,
                &sub.to_string(),
            ],
        )
        .await
        .map_err(|e| refused_by_postgres(&e))?;
        sess.query(
            "update storage.s3_multipart_uploads
                set in_progress_size = $2,
                    metadata = jsonb_set(coalesce(metadata, '{}'::jsonb),
                                         '{length}', $3::text::jsonb)
              where id = $1",
            &[
                &upload.id,
                &arriving,
                &match length {
                    Some(length) => length.to_string(),
                    None => "null".to_string(),
                },
            ],
        )
        .await
        .map_err(|e| refused_by_postgres(&e))?;
        Ok::<(), Wrong>(())
    })
    .await?;

    match length == Some(arriving) {
        true => finish(app, sess, role, sub, &upload, arriving).await?,
        false => commit(sess, Ok(())).await?,
    }
    Ok(arriving)
}

/// The parts, joined up and written as an ordinary object.
///
/// Through the same code an ordinary upload ends in, so that the two of
/// them leave the same row behind for the same bytes. The two things
/// that are not the same are deliberate and both are recorded: the
/// cache control is always `no-cache` here whatever the metadata asked
/// for, and nothing is attached, so the object's own metadata reads
/// back as null where an ordinary upload's reads back as an empty
/// object.
async fn finish(
    app: &App,
    sess: Session,
    role: &str,
    sub: &str,
    upload: &InProgress,
    size: i64,
) -> Result<(), Wrong> {
    let blobs = blobs(app)?;
    let mut bytes = Vec::with_capacity(size.max(0) as usize);
    for part in 0..=upload.parts {
        match blobs.get(blob::part_key(&upload.id, part)).await {
            Ok(Some(mut some)) => bytes.append(&mut some),
            Ok(None) => {}
            Err(e) => {
                let _ = sess.rollback().await;
                return Err(Wrong::Gate(StorageError::internal(e.to_string())));
            }
        }
    }
    object::write(
        app,
        sess,
        Some(role),
        &upload.bucket,
        &upload.key,
        Upload {
            bytes,
            mime: upload.mime.clone(),
            cache: NO_CACHE.to_string(),
        },
        None,
        sub,
        upload.replace,
    )
    .await?;

    // The parts are the object now and their own bytes are nobody's.
    // After the commit rather than before it, because an upload whose
    // object was refused is one a client may still be able to finish.
    // The rows stay, for the same reason a head after a finished upload
    // still answers.
    for part in 0..=upload.parts {
        let _ = blobs.delete(blob::part_key(&upload.id, part)).await;
    }
    Ok(())
}

/// The store, or the sentence a server started without one owes anybody
/// who asks it to keep bytes.
fn blobs(app: &App) -> Result<Blobs, Wrong> {
    app.blobs.clone().ok_or_else(|| {
        Wrong::Gate(StorageError::internal(
            "zou is running without an object store".to_string(),
        ))
    })
}

fn subject(verified: &crate::jwt::Verified) -> String {
    verified
        .claims
        .get("sub")
        .and_then(|claim| claim.as_str())
        .unwrap_or("")
        .to_string()
}

/// The bytes of one request, up to what one request may carry, which is
/// the same ceiling an ordinary upload has. The protocol is many
/// requests by design, and the ceiling on the whole upload is the one
/// the options answer names.
async fn read_body(body: Body) -> Result<Vec<u8>, Wrong> {
    let bytes = axum::body::to_bytes(body, object::UPLOAD_LIMIT)
        .await
        .map_err(|_| Wrong::Tus(too_large()))?;
    Ok(bytes.to_vec())
}

/// A version 4 uuid, for the last part of an upload id.
fn uuid() -> String {
    let mut raw = [0u8; 16];
    getrandom::fill(&mut raw).expect("the os rng never fails");
    raw[6] = (raw[6] & 0x0f) | 0x40;
    raw[8] = (raw[8] & 0x3f) | 0x80;
    let mut out = String::with_capacity(36);
    for (at, byte) in raw.iter().enumerate() {
        if matches!(at, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(header: &str) -> Vec<(String, String)> {
        let parts = axum::http::Request::builder()
            .header(UPLOAD_METADATA, header)
            .body(())
            .unwrap()
            .into_parts()
            .0;
        metadata_of(&parts).unwrap()
    }

    #[test]
    fn an_id_carries_where_the_upload_was_going() {
        let id = upload_id("notes", "a/b.txt", "00000000-0000-0000-0000-000000000000");
        let decoded = base64ct::Base64UrlUnpadded::decode_vec(&id).unwrap();
        assert_eq!(
            String::from_utf8(decoded).unwrap(),
            "notes/a/b.txt/00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(named(&id).unwrap(), id);
    }

    #[test]
    fn a_string_that_is_not_an_id_is_refused_before_it_is_looked_up() {
        // Three bytes that are not utf8, which is what the suite sends.
        assert!(named("nope").is_err());
        // An id of the shape the server hands out naming nothing is not
        // this refusal. It is looked up, and answered as missing.
        assert!(named(&upload_id("notes", "nothing.txt", "x")).is_ok());
    }

    #[test]
    fn metadata_is_pairs_of_a_name_and_some_base64() {
        assert_eq!(
            pairs("bucketName bm90ZXM=,objectName cmVzdW1hYmxlLnR4dA=="),
            vec![
                ("bucketName".to_string(), "notes".to_string()),
                ("objectName".to_string(), "resumable.txt".to_string()),
            ]
        );
    }

    #[test]
    fn a_name_on_its_own_is_a_flag_with_nothing_in_it() {
        assert_eq!(
            pairs("nothing"),
            vec![("nothing".to_string(), String::new())]
        );
    }

    #[test]
    fn a_head_hands_back_a_cache_control_nobody_sent() {
        assert_eq!(
            written_metadata(&pairs("bucketName bm90ZXM=")),
            "bucketName bm90ZXM=,cacheControl bm8tY2FjaGU="
        );
    }

    #[test]
    fn a_cache_control_that_was_sent_is_not_written_twice() {
        assert_eq!(
            written_metadata(&pairs("cacheControl bWF4LWFnZT0zNjAw")),
            "cacheControl bWF4LWFnZT0zNjAw"
        );
    }

    #[test]
    fn a_bucket_reads_the_length_that_was_declared_rather_than_any_bytes() {
        let bucket = object::Bucket {
            public: false,
            size_limit: Some(20),
            mime_types: Some(vec!["text/plain".to_string()]),
        };
        assert!(beyond(&bucket, "text/plain", Some(11)).is_none());
        assert_eq!(
            beyond(&bucket, "text/plain", Some(40)).map(|why| why.status.as_u16()),
            Some(413)
        );
        assert_eq!(
            beyond(&bucket, "application/json", Some(7)).map(|why| why.status.as_u16()),
            Some(415)
        );
        // A deferred upload has declared nothing, so there is nothing to
        // measure until the bytes arrive.
        assert!(beyond(&bucket, "text/plain", None).is_none());
    }
}
