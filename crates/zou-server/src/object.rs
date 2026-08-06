//! Objects: the bytes, and the row that says what they are.
//!
//! Written against a recording, the same way the bucket surface next to
//! it was. Fifty four answers from the storage-api a local supabase
//! project runs live in the conformance repository, twenty nine of them
//! about objects, and everything below that looks arbitrary is one of
//! them. The four worth knowing before reading anything else:
//!
//! A download asked for with no token at all is answered rather than
//! refused, if the bucket is public. An upload with no token is refused
//! in jose's words. So the read routes fall back to a public read when
//! the token is missing or bad, and the write routes do not.
//!
//! Which failure that fallback gives depends on the door. Asked over
//! `/object/public/{bucket}/...`, a bucket that is not public is
//! answered with `NoSuchKey`: the route is about objects, and an object
//! in a bucket nobody may read is an object nobody can name. Asked over
//! `/object/{bucket}/...` with no token, the same bucket is answered
//! with `NoSuchBucket`, because that path looks for a public bucket and
//! does not find one. Both are recorded and they really do differ.
//!
//! Deleting says `Access denied` where reading says `Object not found`,
//! for one object and the same key. That falls out of asking twice: the
//! row is looked for with the caller's policies out of the way, so a
//! delete of something that is not there says so, and then the delete
//! itself runs under those policies, so a delete that removes nothing
//! is a delete that was not allowed. Reading asks once, under the
//! policies, which is why a row row level security hides reads as
//! missing.
//!
//! An upload writes the row first and the bytes after, inside the
//! transaction, and commits last. The other order leaves bytes nothing
//! points at when the insert is refused; this order can leave a row
//! pointing at bytes that were never written only if the process dies
//! between the two, and that row is the one a re-upload replaces.
//!
//! The bytes themselves are in `blob`, under the row's id and version,
//! which is why nothing in a client's name reaches a key and why
//! replacing an object leaves the old bytes readable until the new ones
//! are committed.
//!
//! Listing is two routes rather than one, and neither of them is a
//! query written here. `/object/list` calls `storage.search` and
//! `/object/list-v2` calls `storage.search_v2` or a plain select,
//! because upstream does, and those functions decide what a folder is.
//! Everything this file adds around them is the shape of the answer and
//! the cursor.
//!
//! Moving is the one place this file does less work than upstream
//! rather than the same work. A name is not part of the key the bytes
//! are under, so a move is two columns of one row and the store is
//! never touched. storage-api keys its store by the name, so its move
//! has to copy the bytes and write a new version. Copying does touch
//! the store, because a second row cannot share a key with the first.
//!
//! What is not here: signed urls, resumable uploads, image transforms
//! and the S3 protocol. None of them are asked by the suite yet, and
//! the list on the compatibility issue is where they are tracked.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header, request::Parts};
use axum::response::Response;
use md5::{Digest, Md5};
use serde_json::{Value, json};

use crate::App;
use crate::blob::{self, Blobs};
use crate::sql::Session;
use crate::storage::{ALLOW_DELETE, StorageError, begin, caller, done, message, ok, pg_error};

/// The most bytes an upload may carry.
///
/// storage-api's own global limit on a fresh project, which the CLI
/// sets to fifty megabytes. It is a limit on the request rather than on
/// a bucket: the per bucket `file_size_limit` is a second, smaller
/// number, and enforcing that one needs answers this suite has not
/// asked for yet.
const UPLOAD_LIMIT: usize = 50 * 1024 * 1024;

/// How much of a batch delete's body is worth reading. A list of names,
/// so the room a bucket call gets is enough for a few thousand of them.
const BODY_LIMIT: usize = 1024 * 1024;

/// The cache-control an upload that says nothing about caching gets.
const NO_CACHE: &str = "no-cache";

/// Which door a read came in by, which decides who it is asked as and
/// what a bucket that is not public is told.
#[derive(Clone, Copy, PartialEq)]
enum Door {
    /// `/object/{bucket}/...` and `/object/authenticated/{bucket}/...`,
    /// asked as whoever sent the token, and as a public read when there
    /// is no usable token.
    Authenticated,
    /// `/object/public/{bucket}/...`, asked with no token at all and
    /// only against a bucket that is public.
    Public,
}

/// A row of `storage.objects`, in the shape the answers want it.
///
/// The two json columns are kept parsed because both are read field by
/// field: `metadata` is what storage-api wrote when the bytes went in,
/// and `user_metadata` is whatever the client attached and is never
/// looked into.
struct ObjectRow {
    id: String,
    bucket_id: String,
    name: String,
    version: String,
    created_at: String,
    metadata: Value,
    user_metadata: Value,
}

impl ObjectRow {
    /// A field of the metadata storage-api writes, or null.
    fn meta(&self, field: &str) -> Value {
        self.metadata.get(field).cloned().unwrap_or(Value::Null)
    }

    /// Where the bytes of this version are.
    fn key(&self) -> String {
        blob::key(&self.id, &self.version)
    }

    /// What `/object/info` answers.
    ///
    /// Half of it is the row and half is the metadata unpacked into
    /// fields with different names: `mimetype` is answered as
    /// `content_type`, `cacheControl` as `cache_control`, `eTag` as
    /// `etag`. The renaming is upstream's and so is the choice of which
    /// of the two metadata columns is called `metadata` here: it is the
    /// client's, and a client that attached none is answered with an
    /// empty object rather than a null.
    fn info(&self) -> String {
        json!({
            "id": self.id,
            "name": self.name,
            "bucket_id": self.bucket_id,
            "version": self.version,
            "created_at": self.created_at,
            "last_modified": self.meta("lastModified"),
            "cache_control": self.meta("cacheControl"),
            "content_type": self.meta("mimetype"),
            "etag": self.meta("eTag"),
            "size": self.meta("size"),
            "metadata": match &self.user_metadata {
                Value::Null => json!({}),
                given => given.clone(),
            },
        })
        .to_string()
    }
}

/// The store, or the sentence a server that was started without one
/// owes anybody who asks it for bytes.
fn blobs(app: &App) -> Result<&Blobs, StorageError> {
    app.blobs
        .as_ref()
        .ok_or_else(|| StorageError::internal("zou is running without an object store".to_string()))
}

/// The columns every object answer is built from.
const OBJECT_COLUMNS: &str = "id::text, coalesce(bucket_id, ''), coalesce(name, ''),
     coalesce(version, ''),
     to_char(created_at at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),
     coalesce(metadata::text, 'null'), coalesce(user_metadata::text, 'null')";

fn object_row(row: &tokio_postgres::Row) -> ObjectRow {
    let parsed = |text: String| serde_json::from_str(&text).unwrap_or(Value::Null);
    ObjectRow {
        id: row.get(0),
        bucket_id: row.get(1),
        name: row.get(2),
        version: row.get(3),
        created_at: row.get::<_, Option<String>>(4).unwrap_or_default(),
        metadata: parsed(row.get(5)),
        user_metadata: parsed(row.get(6)),
    }
}

/// The one row, as whoever this session is.
async fn find(sess: &Session, bucket: &str, name: &str) -> Result<Option<ObjectRow>, StorageError> {
    let sql =
        format!("select {OBJECT_COLUMNS} from storage.objects where bucket_id = $1 and name = $2");
    let rows = sess
        .query(&sql, &[&bucket, &name])
        .await
        .map_err(|e| pg_error(&e))?;
    Ok(rows.first().map(object_row))
}

/// Put the caller's policies aside for one question, then put them
/// back.
///
/// storage-api keeps a second connection as the owner for this and asks
/// it whether a bucket exists before letting the first connection fail.
/// One connection does as well if the role goes back on afterwards:
/// `set_config('role', ..., true)` is the same statement the pool uses
/// to put the role on in the first place, and local means it lasts the
/// transaction rather than the connection. The role is restored whether
/// or not the question was answered, because a session that came back
/// with the policies off would be a session the rest of the request
/// trusts wrongly.
async fn unpoliced<T>(
    sess: &Session,
    role: &str,
    work: impl AsyncFnOnce() -> Result<T, StorageError>,
) -> Result<T, StorageError> {
    set_role(sess, "none").await?;
    let answer = work().await;
    set_role(sess, role).await?;
    answer
}

async fn set_role(sess: &Session, role: &str) -> Result<(), StorageError> {
    sess.query("select set_config('role', $1, true)", &[&role])
        .await
        .map_err(|e| pg_error(&e))?;
    Ok(())
}

/// Is there a bucket of this name at all, and is it public?
async fn bucket_public(sess: &Session, id: &str) -> Result<Option<bool>, StorageError> {
    let rows = sess
        .query("select public from storage.buckets where id = $1", &[&id])
        .await
        .map_err(|e| pg_error(&e))?;
    Ok(rows
        .first()
        .map(|row| row.get::<_, Option<bool>>(0) == Some(true)))
}

/// Commit, then fail. The transaction did nothing worth keeping and the
/// connection is still owed back.
async fn refuse<T>(sess: Session, why: StorageError) -> Result<T, StorageError> {
    done(sess, Err(why)).await
}

/// The transaction a read runs in, and who it runs as.
///
/// Everything about the two doors that differs is here. What follows it
/// is one lookup and one answer either way.
async fn read_session(
    app: &App,
    parts: &Parts,
    bucket: &str,
    door: Door,
) -> Result<Session, StorageError> {
    let public_read = async || {
        let pool = app.pool.as_ref().ok_or_else(|| {
            StorageError::internal("zou is running without a database".to_string())
        })?;
        // No role and no claims: a public read is not made on anybody's
        // behalf, and the policies on these tables are written about
        // callers who said who they were.
        pool.admin().await.map_err(|e| pg_error(&e))
    };
    match door {
        Door::Public => {
            let sess = public_read().await?;
            match bucket_public(&sess, bucket).await {
                Err(e) => Err(e),
                Ok(None) => refuse(sess, StorageError::no_such_bucket()).await,
                // Not a refusal about the bucket, deliberately. This
                // route is about an object, and an object in a bucket
                // that is not public is one this caller cannot name.
                Ok(Some(false)) => refuse(sess, StorageError::no_such_key()).await,
                Ok(Some(true)) => Ok(sess),
            }
        }
        Door::Authenticated => match caller(app, parts) {
            Ok((ctx, _)) => begin(app, &ctx, true).await,
            // No usable token, so this is a public read after all. The
            // bucket has to be a public one, and one that is not is
            // answered as one that is not there: this path looks for a
            // public bucket rather than for a bucket.
            Err(_) => {
                let sess = public_read().await?;
                match bucket_public(&sess, bucket).await {
                    Err(e) => Err(e),
                    Ok(Some(true)) => Ok(sess),
                    Ok(_) => refuse(sess, StorageError::no_such_bucket()).await,
                }
            }
        },
    }
}

/// The row a read is about, or the reason there is none.
async fn read_row(
    app: &App,
    parts: &Parts,
    bucket: &str,
    name: &str,
    door: Door,
) -> Result<ObjectRow, StorageError> {
    let sess = read_session(app, parts, bucket, door).await?;
    let found = find(&sess, bucket, name).await;
    match done(sess, found).await? {
        Some(row) => Ok(row),
        None => Err(StorageError::no_such_key()),
    }
}

/// The content type an answer carries, which is not quite the one the
/// upload sent.
///
/// A text type picks up a charset on the way out, spelled the way the
/// reference spells it, capitals and all. Anything else goes back as it
/// came, and a type that already says what its charset is is left
/// alone.
fn served_type(mime: &str) -> String {
    let mime = match mime.trim().is_empty() {
        true => "application/octet-stream",
        false => mime.trim(),
    };
    match mime.starts_with("text/") && !mime.to_ascii_lowercase().contains("charset") {
        true => format!("{mime}; charset=UTF-8"),
        false => mime.to_string(),
    }
}

/// What a range header asks for, as a first and last byte inside an
/// object of `size`.
///
/// `bytes=0-3`, `bytes=4-` and `bytes=-4` are the three spellings a
/// client library sends. Anything else, and anything that starts past
/// the end, is read as no range at all rather than refused: a whole
/// object is a truthful answer to a question about part of one, and
/// what the reference says about an unsatisfiable range is not
/// recorded.
fn wanted_range(header: &str, size: u64) -> Option<(u64, u64)> {
    let spec = header.trim().strip_prefix("bytes=")?;
    let (first, last) = spec.split_once('-')?;
    let (start, end) = match (first.trim(), last.trim()) {
        ("", "") => return None,
        // A suffix range: the last n bytes, however long the object is.
        ("", suffix) => {
            let n: u64 = suffix.parse().ok()?;
            (size.saturating_sub(n), size.checked_sub(1)?)
        }
        (from, "") => (from.parse().ok()?, size.checked_sub(1)?),
        (from, to) => (from.parse().ok()?, to.parse().ok()?),
    };
    if start > end || start >= size {
        return None;
    }
    Some((start, end.min(size - 1)))
}

/// GET or HEAD /storage/v1/object/{bucket}/{name}
pub async fn download(
    State(app): State<Arc<App>>,
    Path((bucket, name)): Path<(String, String)>,
    parts: Parts,
) -> Result<Response, StorageError> {
    send(&app, &parts, &bucket, &name, Door::Authenticated).await
}

/// GET or HEAD /storage/v1/object/authenticated/{bucket}/{name}
///
/// The same route said out loud. Upstream has both and answers them
/// with the same handler, and a client library picks between them by
/// whether it was given a session.
pub async fn download_authenticated(
    State(app): State<Arc<App>>,
    Path((bucket, name)): Path<(String, String)>,
    parts: Parts,
) -> Result<Response, StorageError> {
    send(&app, &parts, &bucket, &name, Door::Authenticated).await
}

/// GET or HEAD /storage/v1/object/public/{bucket}/{name}
pub async fn download_public(
    State(app): State<Arc<App>>,
    Path((bucket, name)): Path<(String, String)>,
    parts: Parts,
) -> Result<Response, StorageError> {
    send(&app, &parts, &bucket, &name, Door::Public).await
}

/// The bytes, whole or in part.
///
/// A HEAD asks the same question and axum drops the body on the way
/// out, so there is one handler for both and the headers a HEAD carries
/// are the ones a GET would have carried.
async fn send(
    app: &App,
    parts: &Parts,
    bucket: &str,
    name: &str,
    door: Door,
) -> Result<Response, StorageError> {
    let blobs = blobs(app)?;
    let row = read_row(app, parts, bucket, name, door).await?;

    let mime = row.meta("mimetype");
    let mime = mime.as_str().unwrap_or_default();
    let cache = row.meta("cacheControl");
    let cache = cache.as_str().unwrap_or(NO_CACHE);
    let etag = row.meta("eTag");

    // The size the row remembers. It is what a range is measured
    // against, and reading the object to find out how long it is would
    // defeat the point of asking for part of it.
    let size = row.meta("size").as_u64();
    let asked = parts
        .headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .zip(size)
        .and_then(|(header, size)| wanted_range(header, size));

    let (status, bytes, content_range) = match asked {
        Some((start, end)) => {
            let bytes = blobs
                .get_range(row.key(), start, end - start + 1)
                .await
                .map_err(|e| StorageError::internal(e.to_string()))?;
            let range = format!("bytes {start}-{end}/{}", size.unwrap_or_default());
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
    // A row with no bytes behind it is an object that is not there. It
    // happens to a row whose upload half failed, and it is what the
    // fixture's own object is.
    let Some(bytes) = bytes else {
        return Err(StorageError::no_such_key());
    };

    let mut answer = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, served_type(mime))
        .header(header::CACHE_CONTROL, cache)
        .header(header::ACCEPT_RANGES, "bytes");
    if let Some(range) = content_range {
        answer = answer.header(header::CONTENT_RANGE, range);
    }
    if let Some(etag) = etag.as_str() {
        answer = answer.header(header::ETAG, etag);
    }
    answer
        .body(Body::from(bytes))
        .map_err(|e| StorageError::internal(e.to_string()))
}

/// GET /storage/v1/object/info/{bucket}/{name}
pub async fn info(
    State(app): State<Arc<App>>,
    Path((bucket, name)): Path<(String, String)>,
    parts: Parts,
) -> Result<Response, StorageError> {
    let row = read_row(&app, &parts, &bucket, &name, Door::Authenticated).await?;
    Ok(ok(row.info()))
}

/// GET /storage/v1/object/info/public/{bucket}/{name}
pub async fn info_public(
    State(app): State<Arc<App>>,
    Path((bucket, name)): Path<(String, String)>,
    parts: Parts,
) -> Result<Response, StorageError> {
    let row = read_row(&app, &parts, &bucket, &name, Door::Public).await?;
    Ok(ok(row.info()))
}

/// A timestamp column, written the way a javascript Date writes one.
///
/// A fragment of sql rather than a constant, because the listing routes
/// need it inside `json_build_object` where a column alias would not
/// reach.
fn iso(column: &str) -> String {
    format!("to_char({column} at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')")
}

/// A jsonb column, written the way javascript writes json.
///
/// postgres prints jsonb with a space after every colon and every
/// comma and the reference prints it with none, so the value goes
/// through the json parser once on its way out, which writes it back
/// compactly. What that costs is a key whose value is null, which
/// `json_strip_nulls` drops. Nothing puts one in this column: it is the
/// metadata the api writes itself out of what the store said about the
/// bytes, never the client's. The day something does, this is the line
/// that has to become a comparison the parser does not do.
fn compact(column: &str) -> String {
    format!("json_strip_nulls({column}::text::json)")
}

/// The body of a request that sends json, as json.
async fn json_body(body: Body) -> Result<Value, StorageError> {
    let bytes = axum::body::to_bytes(body, BODY_LIMIT)
        .await
        .map_err(|_| StorageError::bad_json())?;
    serde_json::from_slice(&bytes).map_err(|_| StorageError::bad_json())
}

/// A string field of a body, or the empty string.
fn text_of(body: &Value, field: &str) -> String {
    body.get(field)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

/// A whole number field of a body, or what the reference falls back to.
///
/// Zero reads as absent, because the reference reads these fields with
/// `||` and javascript calls zero false. It cannot arrive through
/// upstream's own schema, which puts a minimum on both of them, but the
/// fallback is the thing being copied rather than the validation.
fn counted(body: &Value, field: &str, fallback: i32) -> i32 {
    match body.get(field).and_then(Value::as_i64) {
        Some(n) if n != 0 => n as i32,
        _ => fallback,
    }
}

/// One half of the body's `sortBy`, as the client wrote it.
///
/// Kept unvalidated on purpose. What a listing sorts by and what a
/// cursor says it sorted by are the same string, and the cursor carries
/// the one that was asked for rather than the one that was used.
fn sort_field(body: &Value, field: &str) -> Option<String> {
    body.get("sortBy")?.get(field)?.as_str().map(str::to_string)
}

/// The column a listing really sorts on.
///
/// Anything outside `allowed` is read as no column named at all.
/// Upstream refuses it against a schema before its handler runs and
/// what it says when it does is not recorded, so this takes the
/// forgiving reading the bucket surface takes of a field it cannot use.
/// It is also the only reading that is safe: the name reaches a plpgsql
/// function that builds sql out of it.
fn sorted_by(body: &Value, cursor: Option<&Cursor>, allowed: &[&str]) -> Option<String> {
    named(body, cursor, "column").filter(|c| allowed.contains(&c.as_str()))
}

/// asc or desc, and asc for anything else.
fn ascending(body: &Value, cursor: Option<&Cursor>) -> &'static str {
    match named(body, cursor, "order").as_deref() {
        Some("desc") => "desc",
        _ => "asc",
    }
}

/// What the cursor said, or what the body said, in that order.
///
/// A client that is paging already said how it wanted the rows sorted,
/// and the token is where that was written down. Letting the body win
/// would let a second page be sorted differently from the first, which
/// is a page that skips rows.
fn named(body: &Value, cursor: Option<&Cursor>, field: &str) -> Option<String> {
    let carried = match field {
        "column" => cursor.and_then(|c| c.column.clone()),
        _ => cursor.and_then(|c| c.order.clone()),
    };
    carried.or_else(|| sort_field(body, field))
}

/// `%` and `_` mean something to `like`, and a prefix somebody typed is
/// a name rather than a pattern.
fn escape_like(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

/// POST /storage/v1/object/list/{bucket}
///
/// The listing a client library calls. It is `storage.search` with the
/// answer shaped around it, and the two things worth knowing are both
/// upstream's: a prefix that does not end in a slash is given one,
/// because a prefix is a folder, and a folder comes back as a row of
/// its own with every field but the name null.
///
/// A bucket that is not there is answered with an empty list rather
/// than a refusal, and so is a bucket this key may not read. Both are
/// recorded. The function selects from `storage.objects` under the
/// caller's policies and a listing of nothing is a listing.
pub async fn list(
    State(app): State<Arc<App>>,
    Path(bucket): Path<String>,
    parts: Parts,
    body: Body,
) -> Result<Response, StorageError> {
    let (ctx, _) = caller(&app, &parts)?;
    let asked = json_body(body).await?;

    let mut prefix = text_of(&asked, "prefix");
    if !prefix.is_empty() && !prefix.ends_with('/') {
        prefix.push('/');
    }
    let search = text_of(&asked, "search");
    let column = sorted_by(
        &asked,
        None,
        &["name", "updated_at", "created_at", "last_accessed_at"],
    )
    .unwrap_or_else(|| "name".to_string());
    // Escaped only when the sort is not by name. That is upstream's
    // arrangement rather than a rule with a reason behind it: the two
    // spellings reach two halves of storage.search and only one of them
    // reads its argument as a like pattern.
    let (prefix, search) = match column.as_str() {
        "name" => (prefix, search),
        _ => (escape_like(&prefix), escape_like(&search)),
    };
    // How deep the answer is cut. An empty prefix is one level, and
    // every slash is another, which is what makes a folder a row.
    let levels = prefix.split('/').count() as i32;
    let limit = counted(&asked, "limit", 100);
    let offset = counted(&asked, "offset", 0);
    let order = ascending(&asked, None);

    let sess = begin(&app, &ctx, true).await?;
    // One row per object rather than one string for the lot, so the
    // order of the answer is the order the rows came back in rather
    // than the order an aggregate happened to consume them.
    let sql = format!(
        "select to_json(o)::text from (
             select name,
                    id::text as id,
                    {} as updated_at,
                    {} as created_at,
                    {} as last_accessed_at,
                    {} as metadata
             from storage.search($1, $2, $3, $4, $5, $6, $7, $8)) o",
        iso("updated_at"),
        iso("created_at"),
        iso("last_accessed_at"),
        compact("metadata"),
    );
    let found = sess
        .query(
            &sql,
            &[
                &prefix, &bucket, &limit, &levels, &offset, &search, &column, &order,
            ],
        )
        .await
        .map_err(|e| pg_error(&e))?;
    let rows: Vec<String> = found.iter().map(|row| row.get(0)).collect();
    done(sess, Ok(ok(format!("[{}]", rows.join(","))))).await
}

/// Where the last page of a listing stopped and how it was sorted.
#[derive(Default)]
struct Cursor {
    start_after: String,
    order: Option<String>,
    column: Option<String>,
    after: Option<String>,
}

/// Read a continuation token.
///
/// `l` is the name to carry on after, `o` the order, `c` the column and
/// `a` the value that column had on the last row, one to a line, base64
/// over the lot. A token that does not read as that is refused rather
/// than read as far as it goes: a token this server did not write came
/// from somewhere, and answering it with the wrong page is worse than
/// not answering it.
fn read_cursor(token: &str) -> Result<Cursor, StorageError> {
    use base64ct::Encoding;
    let refuse = StorageError::invalid_cursor;
    let decoded = base64ct::Base64::decode_vec(token).map_err(|_| refuse())?;
    let text = String::from_utf8(decoded).map_err(|_| refuse())?;
    // asc unless the token says otherwise. The reference writes that
    // default into the token as it reads it rather than leaving it to
    // the query, which is why a cursor sorts a listing the body did not.
    let mut cursor = Cursor {
        order: Some("asc".to_string()),
        ..Cursor::default()
    };
    for part in text.split('\n') {
        let (key, value) = part.split_once(':').ok_or_else(refuse)?;
        match key {
            "l" => cursor.start_after = value.to_string(),
            "o" => cursor.order = Some(value.to_string()),
            "c" => cursor.column = Some(value.to_string()),
            "a" => cursor.after = Some(value.to_string()),
            _ => return Err(refuse()),
        }
    }
    Ok(cursor)
}

/// Write one.
fn write_cursor(cursor: &Cursor) -> String {
    use base64ct::Encoding;
    let mut parts = vec![format!("l:{}", cursor.start_after)];
    for (key, value) in [
        ("o", &cursor.order),
        ("c", &cursor.column),
        ("a", &cursor.after),
    ] {
        // Only what there is to say. A token with an empty part in it
        // is not the token the reference writes, and the suite compares
        // the string.
        if let Some(value) = value.as_ref().filter(|v| !v.is_empty()) {
            parts.push(format!("{key}:{value}"));
        }
    }
    base64ct::Base64::encode_string(parts.join("\n").as_bytes())
}

/// The columns a cursor can page on besides the name.
///
/// The two the answer carries. `name` is not one of them because the
/// name is the other half of every cursor anyway, and a column the
/// answer does not carry cannot be read off the last row.
const PAGEABLE: &[&str] = &["created_at", "updated_at"];

/// POST /storage/v1/object/list-v2/{bucket}
///
/// The other listing, which pages on a cursor rather than an offset and
/// answers folders and objects in two lists instead of one. It is two
/// queries rather than one: told to fold folders in it calls
/// `storage.search_v2`, and told not to it selects from the table.
///
/// What comes back is one row more than was asked for, which is how the
/// answer knows there is another page without counting the bucket.
pub async fn list_v2(
    State(app): State<Arc<App>>,
    Path(bucket): Path<String>,
    parts: Parts,
    body: Body,
) -> Result<Response, StorageError> {
    let (ctx, _) = caller(&app, &parts)?;
    let asked = json_body(body).await?;

    let token = text_of(&asked, "cursor");
    let cursor = match token.is_empty() {
        true => None,
        false => Some(read_cursor(&token)?),
    };
    let cursor = cursor.as_ref();
    // Kept as it was asked for, because this is what goes back out in
    // the next token. What the query uses is the validated one below.
    let wanted_column = named(&asked, cursor, "column");
    let wanted_order = named(&asked, cursor, "order");
    let order = ascending(&asked, cursor);
    let column = sorted_by(&asked, cursor, &["name", "created_at", "updated_at"])
        .unwrap_or_else(|| "name".to_string());
    let paging_on = wanted_column
        .clone()
        .filter(|c| PAGEABLE.contains(&c.as_str()));
    // Empty means the token said nothing about where to carry on from,
    // which is a token that is not paging.
    let carry_on = cursor
        .map(|c| c.start_after.clone())
        .filter(|name| !name.is_empty());

    let prefix = text_of(&asked, "prefix");
    let folded = asked.get("with_delimiter").and_then(Value::as_bool) == Some(true);
    let limit = match asked.get("limit").and_then(Value::as_i64) {
        Some(n) if n > 0 => n.min(1000),
        _ => 1000,
    } as usize;
    let wanted = limit + 1;

    // The value the next cursor carries, selected as a column because
    // it is read off the last row that came back rather than computed.
    let paged_at = match &paging_on {
        Some(c) => iso(&format!("r.{c}")),
        None => "null::text".to_string(),
    };

    let sess = begin(&app, &ctx, true).await?;
    let found = match folded {
        // The function already cut the names on the slash and turned
        // what is under a folder into one row, so there is no folding
        // left to do here. The reference runs its own fold over these
        // rows too and finds nothing, because `with_delimiter` is a
        // boolean and the only delimiter it can mean is the one the
        // function used.
        true => {
            let levels = match prefix.is_empty() {
                true => 1,
                false => prefix.split('/').count() as i32,
            };
            // The answer's own row is built beside the function's rather
            // than out of it, because three of the function's columns
            // are wanted here and not in the answer: the name before a
            // folder was given its slash, whether it is one, and the
            // value a cursor would carry.
            let sql = format!(
                "select to_json(o)::text, r.name, r.id is null, {paged_at}
                 from storage.search_v2($1, $2, $3, $4, $5, $6, $7, $8) r
                 cross join lateral (
                     select r.key as key,
                            case when r.id is null and right(r.name, 1) <> '/'
                                 then r.name || '/' else r.name end as name,
                            r.id::text as id,
                            {} as updated_at,
                            {} as created_at,
                            {} as last_accessed_at,
                            {} as metadata) o",
                iso("r.updated_at"),
                iso("r.created_at"),
                iso("r.last_accessed_at"),
                compact("r.metadata"),
            );
            let start = carry_on.clone().unwrap_or_default();
            let wanted = wanted as i32;
            let after = cursor.and_then(|c| c.after.clone());
            sess.query(
                &sql,
                &[
                    &prefix, &bucket, &wanted, &levels, &start, &order, &column, &after,
                ],
            )
            .await
        }
        false => {
            // Not the same set of columns as the folded half. Only two
            // of them can be sorted on here, and `name` sorts in C
            // rather than in the database's collation, because the
            // cursor compares names as bytes and a page that stopped
            // where the sort did not would repeat a row or skip one.
            let sort_column = Some(column.clone()).filter(|c| PAGEABLE.contains(&c.as_str()));
            let page = match order {
                "desc" => "<",
                _ => ">",
            };
            let pattern = format!("{}%", escape_like(&prefix));
            let after = cursor.and_then(|c| c.after.clone());
            let wanted = wanted as i64;
            let mut conditions = vec!["r.bucket_id = $1".to_string()];
            let mut args: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&bucket, &wanted];
            if !prefix.is_empty() {
                args.push(&pattern);
                conditions.push(format!("r.name like ${}", args.len()));
            }
            if let Some(start) = &carry_on {
                match (&sort_column, &after) {
                    // Two columns at once, because a page sorted on a
                    // timestamp that repeats has to carry the name as
                    // well to know which of the equal rows it stopped
                    // at. Truncated to milliseconds because that is the
                    // precision the token was written with.
                    (Some(sorted), Some(at)) => {
                        args.push(at);
                        args.push(start);
                        conditions.push(format!(
                            "row(date_trunc('milliseconds', r.{sorted}), r.name collate \"C\")
                             {page}
                             row(coalesce(nullif(${}, '')::timestamptz, 'epoch'::timestamptz), ${})",
                            args.len() - 1,
                            args.len(),
                        ));
                    }
                    _ => {
                        args.push(start);
                        conditions.push(format!("r.name collate \"C\" {page} ${}", args.len()));
                    }
                }
            }
            let by = match &sort_column {
                Some(sorted) => format!("r.{sorted} {order}, "),
                None => String::new(),
            };
            let sql = format!(
                "select to_json(o)::text, r.name, false, {paged_at}
                 from storage.objects r
                 cross join lateral (
                     select r.id::text as id,
                            r.name as name,
                            {} as metadata,
                            {} as updated_at,
                            {} as created_at,
                            {} as last_accessed_at) o
                 where {}
                 order by {by}r.name collate \"C\" {order}
                 limit $2",
                compact("r.metadata"),
                iso("r.updated_at"),
                iso("r.created_at"),
                iso("r.last_accessed_at"),
                conditions.join(" and "),
            );
            sess.query(&sql, &args).await
        }
    };
    let found = found.map_err(|e| pg_error(&e))?;

    let truncated = found.len() > limit;
    let shown = found.len().min(limit);
    let mut folders: Vec<String> = Vec::new();
    let mut objects: Vec<String> = Vec::new();
    for row in &found[..shown] {
        let rendered: String = row.get(0);
        match row.get::<_, bool>(2) {
            true => folders.push(rendered),
            false => objects.push(rendered),
        }
    }

    let mut answer = format!("{{\"hasNext\":{truncated}");
    if truncated {
        let last = &found[shown - 1];
        // The name as the row had it, not as the answer wrote it. A
        // folder is answered with a slash on the end and paged without
        // one, and a cursor built from the answer would ask the next
        // page to start after a name no row has.
        let key: String = last.get(1);
        let next = write_cursor(&Cursor {
            start_after: key.clone(),
            order: wanted_order,
            column: wanted_column,
            after: paging_on.and(last.get(3)),
        });
        answer.push_str(&format!(
            ",\"nextCursor\":{},\"nextCursorKey\":{}",
            Value::from(next),
            Value::from(key),
        ));
    }
    answer.push_str(&format!(
        ",\"folders\":[{}],\"objects\":[{}]}}",
        folders.join(","),
        objects.join(","),
    ));
    done(sess, Ok(ok(answer))).await
}

/// The bytes of an upload and what the request said about them.
struct Upload {
    bytes: Vec<u8>,
    mime: String,
    cache: String,
}

/// Read an upload, whichever of the two ways it was sent.
///
/// A browser form and a raw body carry the same three things in
/// different places, so both end up here as the same three.
async fn read_upload(parts: &Parts, body: Body) -> Result<Upload, StorageError> {
    let content_type = parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = axum::body::to_bytes(body, UPLOAD_LIMIT)
        .await
        .map_err(|_| StorageError::too_large())?
        .to_vec();

    let Some(boundary) = form_boundary(&content_type) else {
        return Ok(Upload {
            bytes,
            mime: content_type,
            cache: parts
                .headers
                .get(header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok())
                .unwrap_or(NO_CACHE)
                .to_string(),
        });
    };
    let form = form_parts(&bytes, &boundary);
    let file = form
        .iter()
        .find(|part| part.filename.is_some())
        .ok_or_else(StorageError::no_file_in_form)?;
    // A form says how long to cache in seconds and a header says it in
    // the words of the header, which is the one place the two ways of
    // uploading really disagree.
    let cache = form
        .iter()
        .find(|part| part.name == "cacheControl")
        .and_then(|part| String::from_utf8(part.body.clone()).ok())
        .map(|seconds| format!("max-age={seconds}"))
        .unwrap_or_else(|| NO_CACHE.to_string());
    Ok(Upload {
        bytes: file.body.clone(),
        mime: file.mime.clone(),
        cache,
    })
}

/// One part of a multipart body.
struct FormPart {
    name: String,
    filename: Option<String>,
    mime: String,
    body: Vec<u8>,
}

/// The boundary of a multipart body, when that is what this is.
fn form_boundary(content_type: &str) -> Option<String> {
    let (kind, rest) = content_type.split_once(';')?;
    if !kind.trim().eq_ignore_ascii_case("multipart/form-data") {
        return None;
    }
    for parameter in rest.split(';') {
        if let Some((name, value)) = parameter.split_once('=')
            && name.trim().eq_ignore_ascii_case("boundary")
        {
            return Some(value.trim().trim_matches('"').to_string());
        }
    }
    None
}

/// Split a multipart body into its parts.
///
/// Written out rather than taken from a crate, because what is needed
/// of multipart here is the shape a file upload has: a delimiter, a
/// couple of headers per part, and bytes. Anything malformed comes back
/// as fewer parts rather than as an error, and an upload with no file
/// part in it is refused by the caller.
fn form_parts(body: &[u8], boundary: &str) -> Vec<FormPart> {
    let delimiter = format!("--{boundary}").into_bytes();
    let mut parts = Vec::new();
    for chunk in split_on(body, &delimiter).into_iter().skip(1) {
        // The end of the body is the delimiter with two more dashes on
        // it, so a chunk that starts that way is where the parts stop.
        if chunk.starts_with(b"--") {
            break;
        }
        let chunk = chunk.strip_prefix(b"\r\n").unwrap_or(chunk);
        let Some(at) = index_of(chunk, b"\r\n\r\n") else {
            continue;
        };
        let (head, rest) = chunk.split_at(at);
        let body = rest[4..].strip_suffix(b"\r\n").unwrap_or(&rest[4..]);

        let mut part = FormPart {
            name: String::new(),
            filename: None,
            mime: String::new(),
            body: body.to_vec(),
        };
        for line in String::from_utf8_lossy(head).lines() {
            let Some((field, value)) = line.split_once(':') else {
                continue;
            };
            let field = field.trim().to_ascii_lowercase();
            if field == "content-type" {
                part.mime = value.trim().to_string();
            }
            if field == "content-disposition" {
                for parameter in value.split(';') {
                    if let Some((key, given)) = parameter.split_once('=') {
                        let given = given.trim().trim_matches('"').to_string();
                        match key.trim() {
                            "name" => part.name = given,
                            "filename" => part.filename = Some(given),
                            _ => {}
                        }
                    }
                }
            }
        }
        parts.push(part);
    }
    parts
}

/// Where `needle` starts in `haystack`, if it is in there.
fn index_of(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Cut on every occurrence of `at`, keeping what is between.
fn split_on<'a>(body: &'a [u8], at: &[u8]) -> Vec<&'a [u8]> {
    let mut pieces = Vec::new();
    let mut rest = body;
    while let Some(cut) = index_of(rest, at) {
        pieces.push(&rest[..cut]);
        rest = &rest[cut + at.len()..];
    }
    pieces.push(rest);
    pieces
}

/// What the upload writes into `metadata`.
///
/// storage-api fills this in from what its backend said when it took
/// the bytes, which is why the field names are the S3 ones rather than
/// the column ones. The info route reads three of them back out, and a
/// listing will read more.
fn metadata(upload: &Upload, when: &str) -> String {
    let mut digest = Md5::new();
    digest.update(&upload.bytes);
    let sum: String = digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let etag = format!("\"{sum}\"");
    json!({
        "eTag": etag,
        "size": upload.bytes.len(),
        "mimetype": upload.mime,
        "cacheControl": upload.cache,
        "lastModified": when,
        "contentLength": upload.bytes.len(),
        "httpStatusCode": 200,
    })
    .to_string()
}

/// What a client attached to the object, from the header it attaches it
/// in.
///
/// Base64 of json, which is how a header carries an object. Nothing is
/// recorded about it: the column is there, the info route answers it,
/// and dropping it on the floor would be answering an empty object to
/// somebody who sent one.
fn user_metadata(parts: &Parts) -> Option<String> {
    use base64ct::Encoding;
    let header = parts.headers.get("x-metadata")?.to_str().ok()?;
    let decoded = base64ct::Base64::decode_vec(header).ok()?;
    let text = String::from_utf8(decoded).ok()?;
    serde_json::from_str::<Value>(&text).ok()?;
    Some(text)
}

/// POST /storage/v1/object/{bucket}/{name}
pub async fn upload(
    State(app): State<Arc<App>>,
    Path((bucket, name)): Path<(String, String)>,
    parts: Parts,
    body: Body,
) -> Result<Response, StorageError> {
    let replace = parts
        .headers
        .get("x-upsert")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|asked| asked.eq_ignore_ascii_case("true"));
    store(&app, &bucket, &name, parts, body, replace).await
}

/// PUT /storage/v1/object/{bucket}/{name}
///
/// The update, which upstream lets stand in for a create: a put where
/// nothing is there yet is recorded as answering the same as an upload.
/// So the two handlers differ only in whether they need to be told to
/// replace.
pub async fn replace(
    State(app): State<Arc<App>>,
    Path((bucket, name)): Path<(String, String)>,
    parts: Parts,
    body: Body,
) -> Result<Response, StorageError> {
    store(&app, &bucket, &name, parts, body, true).await
}

async fn store(
    app: &App,
    bucket: &str,
    name: &str,
    parts: Parts,
    body: Body,
    replace: bool,
) -> Result<Response, StorageError> {
    let (ctx, verified) = caller(app, &parts)?;
    let blobs = blobs(app)?;
    let upload = read_upload(&parts, body).await?;
    let attached = user_metadata(&parts);
    let sub = verified
        .claims
        .get("sub")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();

    let sess = begin(app, &ctx, false).await?;
    // With the policies out of the way, because a caller who may not
    // write into a bucket still hears that the bucket is there. The
    // refusal about the policies comes from the insert, in postgres's
    // own words.
    let there = unpoliced(&sess, &ctx.role, async || {
        bucket_public(&sess, bucket).await
    })
    .await?;
    if there.is_none() {
        return refuse(sess, StorageError::no_such_bucket()).await;
    }
    // What is there now, so that its bytes can go once the new ones are
    // safely in. Under the caller's policies: a row this caller cannot
    // see is a row this caller cannot replace either, and the insert
    // will say so.
    let before = find(&sess, bucket, name).await?;

    let when = now(&sess).await?;
    let written = metadata(&upload, &when);
    let conflict = match replace {
        // Everything the upload decides, and nothing else. owner stays
        // as it was, because a replacement is not a change of hands.
        true => {
            "on conflict (bucket_id, name) do update
                set version = excluded.version,
                    metadata = excluded.metadata,
                    user_metadata = excluded.user_metadata,
                    updated_at = now()"
        }
        false => "",
    };
    let sql = format!(
        "insert into storage.objects
             (bucket_id, name, owner, owner_id, version, metadata, user_metadata)
         values ($1, $2,
                 case when $3::text ~*
                     '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
                     then $3::text::uuid end,
                 nullif($3::text, ''),
                 gen_random_uuid()::text, $4::text::jsonb, $5::text::jsonb)
         {conflict}
         returning id::text, version"
    );
    let rows = sess
        .query(&sql, &[&bucket, &name, &sub, &written, &attached])
        .await
        .map_err(|e| duplicate_is_a_key(&e))?;
    let Some(row) = rows.first() else {
        // An upsert that conflicted with a row the policies hide
        // updates nothing and returns nothing, which is the same
        // refusal an insert would have earned.
        return refuse(
            sess,
            StorageError::access_denied("new row violates row-level security policy".to_string()),
        )
        .await;
    };
    let id: String = row.get(0);
    let version: String = row.get(1);

    // The bytes before the commit. A store that will not take them
    // leaves no row behind saying it did.
    if let Err(e) = blobs.put(blob::key(&id, &version), upload.bytes).await {
        let _ = sess.rollback().await;
        return Err(StorageError::internal(e.to_string()));
    }
    let answer = format!(
        "{{\"Id\":{},\"Key\":{}}}",
        Value::from(id),
        Value::from(format!("{bucket}/{name}"))
    );
    done(sess, Ok(())).await?;

    // The version that was replaced, now that nothing points at it. A
    // reader that started before the commit was reading these bytes and
    // has had them since; one that starts now reads the new ones.
    if let Some(old) = before
        && old.version != version
        && !old.version.is_empty()
    {
        let _ = blobs.delete(old.key()).await;
    }
    Ok(ok(answer))
}

/// Postgres's clock, so that the time in the metadata is the same clock
/// the row's own timestamps are on.
async fn now(sess: &Session) -> Result<String, StorageError> {
    let rows = sess
        .query(
            "select to_char(now() at time zone 'utc',
                            'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')",
            &[],
        )
        .await
        .map_err(|e| pg_error(&e))?;
    Ok(rows.first().map(|row| row.get(0)).unwrap_or_default())
}

/// The same failures the bucket surface reads, except that a duplicate
/// here is a key rather than a bucket.
fn duplicate_is_a_key(e: &tokio_postgres::Error) -> StorageError {
    match e.as_db_error().map(|db| db.code().code()) {
        Some("23505") => StorageError::key_already_exists(),
        _ => pg_error(e),
    }
}

/// Where a move or a copy is going, out of the body it was asked in.
///
/// `destinationBucket` is optional and means the bucket the source is
/// in, which is what makes a rename and a move between buckets the same
/// request.
struct Destination {
    bucket: String,
    source: String,
    into: String,
    target: String,
}

fn destination(asked: &Value) -> Destination {
    let bucket = text_of(asked, "bucketId");
    let into = match text_of(asked, "destinationBucket") {
        named if named.is_empty() => bucket.clone(),
        named => named,
    };
    Destination {
        source: text_of(asked, "sourceKey"),
        target: text_of(asked, "destinationKey"),
        bucket,
        into,
    }
}

/// POST /storage/v1/object/move
///
/// A rename, and the bytes do not go anywhere.
///
/// They are under the row's id and its version, neither of which a name
/// decides, so an object that changes its name or its bucket is a row
/// that changes two columns. storage-api keys its store by the bucket
/// and the name instead, so its move has to copy the bytes across and
/// delete the old ones, and it writes a new version while it does. The
/// answer is a sentence either way. What differs is `version`, which
/// the info route answers and no recorded case reads after a move, and
/// zou's reading of it is the truer one: the bytes did not change.
pub async fn move_object(
    State(app): State<Arc<App>>,
    parts: Parts,
    body: Body,
) -> Result<Response, StorageError> {
    let (ctx, _) = caller(&app, &parts)?;
    let asked = json_body(body).await?;
    let to = destination(&asked);

    let sess = begin(&app, &ctx, false).await?;
    // The bucket first and with the policies aside, the way an upload
    // asks it: a caller who may not touch these rows still hears that
    // the bucket is or is not there.
    let there = unpoliced(&sess, &ctx.role, async || {
        bucket_public(&sess, &to.bucket).await
    })
    .await?;
    if there.is_none() {
        return refuse(sess, StorageError::no_such_bucket()).await;
    }
    // The row under the caller's own policies, so a row they cannot see
    // is a row that is not there. That is the reference's reading and it
    // is not the one a delete takes, which asks twice on purpose.
    if find(&sess, &to.bucket, &to.source).await?.is_none() {
        return refuse(sess, StorageError::no_such_key()).await;
    }
    let moved = sess
        .execute(
            "update storage.objects set bucket_id = $4, name = $3, updated_at = now()
             where bucket_id = $1 and name = $2",
            &[&to.bucket, &to.source, &to.target, &to.into],
        )
        .await
        .map_err(|e| duplicate_is_a_key(&e))?;
    // Seen a moment ago and not moved, so what stopped it was the check
    // on the row it was about to become rather than the row it was.
    if moved == 0 {
        return refuse(
            sess,
            StorageError::access_denied("Access denied".to_string()),
        )
        .await;
    }
    done(sess, Ok(())).await?;
    Ok(message("Successfully moved"))
}

/// POST /storage/v1/object/copy
///
/// A second row and a second set of bytes, which is the whole
/// difference from a move. The bytes are read and written rather than
/// referred to twice: two rows pointing at one key would make a delete
/// of either of them a delete of both, and nothing in the store counts
/// what points at it.
pub async fn copy_object(
    State(app): State<Arc<App>>,
    parts: Parts,
    body: Body,
) -> Result<Response, StorageError> {
    let (ctx, verified) = caller(&app, &parts)?;
    let blobs = blobs(&app)?;
    let asked = json_body(body).await?;
    let to = destination(&asked);
    let replace = parts
        .headers
        .get("x-upsert")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|asked| asked.eq_ignore_ascii_case("true"));
    let sub = verified
        .claims
        .get("sub")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();

    let sess = begin(&app, &ctx, false).await?;
    let there = unpoliced(&sess, &ctx.role, async || {
        bucket_public(&sess, &to.bucket).await
    })
    .await?;
    if there.is_none() {
        return refuse(sess, StorageError::no_such_bucket()).await;
    }
    let Some(from) = find(&sess, &to.bucket, &to.source).await? else {
        return refuse(sess, StorageError::no_such_key()).await;
    };

    // What the copy is described by. `copyMetadata` is true unless the
    // body says otherwise, and when it is false the two fields the body
    // may name are the two that change: everything else the source said
    // about its bytes is still true of the copy, which is the
    // reference's `preserveUnspecifiedFileMetadata`.
    let keep = asked.get("copyMetadata").and_then(Value::as_bool) != Some(false);
    let mut written = from.metadata.clone();
    if !keep && let Some(given) = asked.get("metadata") {
        for field in ["cacheControl", "mimetype"] {
            if let Some(value) = given.get(field)
                && let Some(object) = written.as_object_mut()
            {
                object.insert(field.to_string(), value.clone());
            }
        }
    }
    // The client's own metadata comes across with the copy, or is
    // replaced by the header when the copy was told not to take it.
    let attached = match keep {
        true => match &from.user_metadata {
            Value::Null => None,
            given => Some(given.to_string()),
        },
        false => user_metadata(&parts),
    };

    let conflict = match replace {
        true => {
            "on conflict (bucket_id, name) do update
                set version = excluded.version,
                    metadata = excluded.metadata,
                    user_metadata = excluded.user_metadata,
                    updated_at = now()"
        }
        false => "",
    };
    let sql = format!(
        "insert into storage.objects
             (bucket_id, name, owner, owner_id, version, metadata, user_metadata)
         values ($1, $2,
                 case when $3::text ~*
                     '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
                     then $3::text::uuid end,
                 nullif($3::text, ''),
                 gen_random_uuid()::text, $4::text::jsonb, $5::text::jsonb)
         {conflict}
         returning {OBJECT_COLUMNS}"
    );
    let rows = sess
        .query(
            &sql,
            &[&to.into, &to.target, &sub, &written.to_string(), &attached],
        )
        .await
        .map_err(|e| duplicate_is_a_key(&e))?;
    let Some(made) = rows.first().map(object_row) else {
        return refuse(
            sess,
            StorageError::access_denied("new row violates row-level security policy".to_string()),
        )
        .await;
    };

    // The bytes before the commit, for the reason an upload has: a row
    // pointing at bytes that were never written is the one state a
    // client can see and cannot explain.
    let bytes = blobs
        .get(from.key())
        .await
        .map_err(|e| StorageError::internal(e.to_string()))?;
    if let Some(bytes) = bytes
        && let Err(e) = blobs.put(made.key(), bytes).await
    {
        let _ = sess.rollback().await;
        return Err(StorageError::internal(e.to_string()));
    }
    let answer = json!({
        "Id": made.id,
        "Key": format!("{}/{}", to.into, to.target),
        "name": made.name,
        "bucket_id": made.bucket_id,
        "version": made.version,
        "id": made.id,
        "created_at": made.created_at,
        "metadata": made.metadata,
    })
    .to_string();
    done(sess, Ok(())).await?;
    Ok(ok(answer))
}

/// DELETE /storage/v1/object/{bucket}/{name}
pub async fn remove(
    State(app): State<Arc<App>>,
    Path((bucket, name)): Path<(String, String)>,
    parts: Parts,
) -> Result<Response, StorageError> {
    let (ctx, _) = caller(&app, &parts)?;
    let blobs = blobs(&app)?;
    let sess = begin(&app, &ctx, false).await?;

    // Twice, and the two questions are why a delete can say two
    // different things. Is it there at all, asked with the policies out
    // of the way, so that a delete of nothing says so. Then the delete
    // itself under the policies, so that a delete that removes nothing
    // was a delete somebody was not allowed to make.
    let there = unpoliced(&sess, &ctx.role, async || find(&sess, &bucket, &name).await).await?;
    let Some(row) = there else {
        return refuse(sess, StorageError::no_such_key()).await;
    };
    sess.execute(ALLOW_DELETE, &[])
        .await
        .map_err(|e| pg_error(&e))?;
    let gone = sess
        .execute(
            "delete from storage.objects where bucket_id = $1 and name = $2",
            &[&bucket, &name],
        )
        .await
        .map_err(|e| pg_error(&e))?;
    if gone == 0 {
        return refuse(
            sess,
            StorageError::access_denied("Access denied".to_string()),
        )
        .await;
    }
    done(sess, Ok(())).await?;

    // After the commit, because bytes are not in the transaction. A
    // delete that lands and then fails to reach the store leaves bytes
    // nothing points at, which costs money; the other order loses the
    // bytes of an object whose row is still there, which costs data.
    let _ = blobs.delete(row.key()).await;
    Ok(message("Successfully deleted"))
}

/// DELETE /storage/v1/object/{bucket}
///
/// Several at once, named in the body under `prefixes`, which are whole
/// names rather than prefixes. The answer is the rows that went, so a
/// name that was not there is simply not in it.
pub async fn remove_many(
    State(app): State<Arc<App>>,
    Path(bucket): Path<String>,
    parts: Parts,
    body: Body,
) -> Result<Response, StorageError> {
    let (ctx, _) = caller(&app, &parts)?;
    let blobs = blobs(&app)?;
    let asked = json_body(body).await?;
    let names: Vec<String> = asked
        .get("prefixes")
        .and_then(|v| v.as_array())
        .map(|list| {
            list.iter()
                .filter_map(|name| name.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

    let sess = begin(&app, &ctx, false).await?;
    sess.execute(ALLOW_DELETE, &[])
        .await
        .map_err(|e| pg_error(&e))?;
    // The whole row of everything that went, which is what upstream
    // answers: the two nullable text columns coerced the way its
    // serializer coerces them, and no path_tokens, which is a column it
    // does not answer.
    let sql = "delete from storage.objects where bucket_id = $1 and name = any($2)
               returning id::text, coalesce(bucket_id, ''), coalesce(name, ''),
                   coalesce(owner::text, ''), coalesce(owner_id, ''),
                   coalesce(version, ''),
                   coalesce(metadata::text, 'null'), coalesce(user_metadata::text, 'null'),
                   to_char(created_at at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),
                   to_char(updated_at at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),
                   to_char(last_accessed_at at time zone 'utc',
                           'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')";
    let rows = sess
        .query(sql, &[&bucket, &names])
        .await
        .map_err(|e| pg_error(&e))?;

    let parsed = |text: String| serde_json::from_str(&text).unwrap_or(Value::Null);
    let mut gone = Vec::new();
    let mut keys = Vec::new();
    for row in &rows {
        let id: String = row.get(0);
        let version: String = row.get(5);
        keys.push(blob::key(&id, &version));
        gone.push(json!({
            "id": id,
            "bucket_id": row.get::<_, String>(1),
            "name": row.get::<_, String>(2),
            "owner": row.get::<_, String>(3),
            "owner_id": row.get::<_, String>(4),
            "version": version,
            "metadata": parsed(row.get(6)),
            "user_metadata": parsed(row.get(7)),
            "created_at": row.get::<_, Option<String>>(8),
            "updated_at": row.get::<_, Option<String>>(9),
            "last_accessed_at": row.get::<_, Option<String>>(10),
        }));
    }
    let answer = Value::Array(gone).to_string();
    done(sess, Ok(())).await?;
    for key in keys {
        let _ = blobs.delete(key).await;
    }
    Ok(ok(answer))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The body of the recorded case, byte for byte, including the file
    /// part whose name is the empty string. That is what the browser
    /// client sends and it is the shape the reference accepts.
    const FORM: &str = "--zouconformanceboundary\r\n\
         Content-Disposition: form-data; name=\"cacheControl\"\r\n\r\n\
         3600\r\n\
         --zouconformanceboundary\r\n\
         Content-Disposition: form-data; name=\"\"; filename=\"form.txt\"\r\n\
         Content-Type: text/plain\r\n\r\n\
         sent as a form\r\n\
         --zouconformanceboundary--\r\n";

    #[test]
    fn a_form_is_read_the_way_a_browser_writes_one() {
        let parts = form_parts(FORM.as_bytes(), "zouconformanceboundary");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].name, "cacheControl");
        assert_eq!(parts[0].body, b"3600");
        assert_eq!(parts[1].filename.as_deref(), Some("form.txt"));
        assert_eq!(parts[1].mime, "text/plain");
        assert_eq!(parts[1].body, b"sent as a form");
    }

    /// A file part is found by having a filename rather than by having
    /// a name, because the name of the one a browser sends is empty.
    #[test]
    fn the_file_part_is_the_one_with_a_filename() {
        let parts = form_parts(FORM.as_bytes(), "zouconformanceboundary");
        let file = parts.iter().find(|part| part.filename.is_some()).unwrap();
        assert_eq!(file.name, "");
    }

    #[test]
    fn bytes_in_a_form_are_kept_as_they_were() {
        // A part whose body ends in the delimiter's first bytes, and
        // one with a newline in the middle of it. Neither is text a
        // line based reader would put back together.
        let body = b"--b\r\nContent-Disposition: form-data; name=\"\"; filename=\"f\"\r\n\r\n\
                     one\r\ntwo\r\n--b--\r\n";
        let parts = form_parts(body, "b");
        assert_eq!(parts[0].body, b"one\r\ntwo");
    }

    #[test]
    fn a_boundary_is_found_however_it_is_written() {
        assert_eq!(
            form_boundary("multipart/form-data; boundary=abc"),
            Some("abc".to_string())
        );
        assert_eq!(
            form_boundary("Multipart/Form-Data; charset=utf-8; boundary=\"a b\""),
            Some("a b".to_string())
        );
        assert_eq!(form_boundary("text/plain"), None);
        assert_eq!(form_boundary("multipart/form-data"), None);
    }

    #[test]
    fn a_text_type_picks_up_a_charset_and_nothing_else_does() {
        assert_eq!(served_type("text/plain"), "text/plain; charset=UTF-8");
        assert_eq!(served_type("image/png"), "image/png");
        assert_eq!(
            served_type("text/html; charset=iso-8859-1"),
            "text/html; charset=iso-8859-1"
        );
        assert_eq!(served_type(""), "application/octet-stream");
    }

    #[test]
    fn the_three_spellings_of_a_range() {
        assert_eq!(wanted_range("bytes=0-3", 11), Some((0, 3)));
        assert_eq!(wanted_range("bytes=4-", 11), Some((4, 10)));
        assert_eq!(wanted_range("bytes=-4", 11), Some((7, 10)));
        // Past the end is shortened rather than refused, and a range
        // that starts past the end is no range at all.
        assert_eq!(wanted_range("bytes=8-99", 11), Some((8, 10)));
        assert_eq!(wanted_range("bytes=11-12", 11), None);
        assert_eq!(wanted_range("bytes=3-1", 11), None);
        assert_eq!(wanted_range("lines=1-2", 11), None);
        assert_eq!(wanted_range("bytes=-", 11), None);
    }

    #[test]
    fn the_etag_is_the_md5_of_the_bytes_in_quotes() {
        let upload = Upload {
            bytes: b"hello world".to_vec(),
            mime: "text/plain".to_string(),
            cache: NO_CACHE.to_string(),
        };
        let written: Value =
            serde_json::from_str(&metadata(&upload, "2026-08-06T14:25:39.016Z")).unwrap();
        // The one the reference recorded for the same eleven bytes.
        assert_eq!(written["eTag"], "\"5eb63bbbe01eeed093cb22bb8f5acdc3\"");
        assert_eq!(written["size"], 11);
        assert_eq!(written["mimetype"], "text/plain");
        assert_eq!(written["cacheControl"], "no-cache");
    }

    fn row(metadata: Value, user_metadata: Value) -> ObjectRow {
        ObjectRow {
            id: "2c0d1e4f-5a6b-4c7d-8e9f-0a1b2c3d4e5f".to_string(),
            bucket_id: "notes".to_string(),
            name: "hello.txt".to_string(),
            version: "5877d5bc-0000-4000-8000-000000000000".to_string(),
            created_at: "2026-08-06T14:25:39.016Z".to_string(),
            metadata,
            user_metadata,
        }
    }

    /// The recorded answer, field for field.
    #[test]
    fn what_is_known_about_an_object() {
        let row = row(
            json!({
                "eTag": "\"5eb63bbbe01eeed093cb22bb8f5acdc3\"",
                "size": 11,
                "mimetype": "text/plain",
                "cacheControl": "no-cache",
                "lastModified": "2026-08-06T14:25:39.016Z",
            }),
            Value::Null,
        );
        let answer: Value = serde_json::from_str(&row.info()).unwrap();
        assert_eq!(
            answer,
            json!({
                "bucket_id": "notes",
                "cache_control": "no-cache",
                "content_type": "text/plain",
                "created_at": "2026-08-06T14:25:39.016Z",
                "etag": "\"5eb63bbbe01eeed093cb22bb8f5acdc3\"",
                "id": "2c0d1e4f-5a6b-4c7d-8e9f-0a1b2c3d4e5f",
                "last_modified": "2026-08-06T14:25:39.016Z",
                "metadata": {},
                "name": "hello.txt",
                "size": 11,
                "version": "5877d5bc-0000-4000-8000-000000000000",
            })
        );
    }

    /// What the client attached comes back under `metadata`, which is
    /// the column the client never writes.
    #[test]
    fn the_metadata_in_the_answer_is_the_clients_own() {
        let row = row(json!({"size": 1}), json!({"shot_by": "nobody"}));
        let answer: Value = serde_json::from_str(&row.info()).unwrap();
        assert_eq!(answer["metadata"], json!({"shot_by": "nobody"}));
    }

    /// `read_cursor(..).unwrap()` would ask the failure to say what it
    /// is in debug, and what a storage failure says is a response. The
    /// `ok()` makes it an option, whose unwrap asks for nothing.
    fn read_token(token: &str) -> Cursor {
        read_cursor(token).ok().unwrap()
    }

    /// The token the reference handed out for a page that stopped at
    /// `apples.txt` with nothing else to say, so the string itself is
    /// part of the contract rather than only what it decodes to.
    #[test]
    fn a_cursor_that_only_says_where_it_stopped() {
        let read = read_token("bDphcHBsZXMudHh0");
        assert_eq!(read.start_after, "apples.txt");
        assert_eq!(read.order.as_deref(), Some("asc"));
        assert_eq!(read.column, None);
        assert_eq!(read.after, None);
        // Reading writes the default order into the cursor, so the token
        // for the page after this one says out loud what this one only
        // meant. The reference grows its tokens the same way and the
        // recording is where that was checked.
        assert_eq!(write_cursor(&read), "bDphcHBsZXMudHh0Cm86YXNj");
    }

    #[test]
    fn a_cursor_carries_the_sort_it_was_made_under() {
        let cursor = Cursor {
            start_after: "cat.png".to_string(),
            order: Some("desc".to_string()),
            column: Some("created_at".to_string()),
            after: Some("2026-08-06T14:25:39.016Z".to_string()),
        };
        let read = read_token(&write_cursor(&cursor));
        assert_eq!(read.start_after, "cat.png");
        assert_eq!(read.order.as_deref(), Some("desc"));
        assert_eq!(read.column.as_deref(), Some("created_at"));
        assert_eq!(read.after.as_deref(), Some("2026-08-06T14:25:39.016Z"));
    }

    /// A name with a colon in it is a name, not two parts. Only the
    /// first colon divides, which is what lets `a:` carry a timestamp.
    #[test]
    fn a_colon_in_a_name_stays_in_the_name() {
        let cursor = Cursor {
            start_after: "notes/9:30 am.txt".to_string(),
            ..Cursor::default()
        };
        let read = read_token(&write_cursor(&cursor));
        assert_eq!(read.start_after, "notes/9:30 am.txt");
    }

    #[tokio::test]
    async fn a_token_this_server_did_not_write_is_refused() {
        use axum::response::IntoResponse;
        use base64ct::Encoding;
        let bad = |text: &str| base64ct::Base64::encode_string(text.as_bytes());
        // Not base64, base64 over a part with no colon, and base64 over
        // a part whose letter means nothing here.
        for token in ["!!!!", &bad("apples.txt"), &bad("l:a\nz:b")] {
            let refused = match read_cursor(token) {
                Err(refused) => refused.into_response(),
                Ok(_) => panic!("{token} was read as a cursor"),
            };
            assert_eq!(refused.status(), 400);
            let bytes = axum::body::to_bytes(refused.into_body(), 1 << 16)
                .await
                .unwrap();
            let said: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(said["code"], "InvalidParameter");
            assert_eq!(said["message"], "Invalid Parameter continuation token");
        }
    }

    /// A prefix somebody typed is a name. `like` would read these three
    /// as a pattern, and the escape is the backslash's own too.
    #[test]
    fn the_characters_like_reads_are_spelled_out() {
        assert_eq!(escape_like("100%_off"), "100\\%\\_off");
        assert_eq!(escape_like("back\\slash"), "back\\\\slash");
        assert_eq!(escape_like("holiday/"), "holiday/");
    }

    /// Zero reads as absent because the reference reads these with `||`.
    #[test]
    fn a_count_of_zero_is_no_count_at_all() {
        assert_eq!(counted(&json!({"limit": 25}), "limit", 100), 25);
        assert_eq!(counted(&json!({"limit": 0}), "limit", 100), 100);
        assert_eq!(counted(&json!({}), "limit", 100), 100);
    }

    /// The cursor's sort wins over the body's, or a second page comes
    /// back sorted differently from the first.
    #[test]
    fn what_the_cursor_says_beats_what_the_body_says() {
        let body = json!({"sortBy": {"column": "updated_at", "order": "asc"}});
        let cursor = Cursor {
            order: Some("desc".to_string()),
            column: Some("created_at".to_string()),
            ..Cursor::default()
        };
        assert_eq!(ascending(&body, Some(&cursor)), "desc");
        assert_eq!(
            sorted_by(&body, Some(&cursor), PAGEABLE).as_deref(),
            Some("created_at")
        );
        // And with no cursor the body is all there is.
        assert_eq!(ascending(&body, None), "asc");
        assert_eq!(
            sorted_by(&body, None, PAGEABLE).as_deref(),
            Some("updated_at")
        );
    }

    /// A column outside the list reads as no column named at all, since
    /// the name is spliced into sql further down.
    #[test]
    fn a_column_that_is_not_sortable_is_no_column() {
        let body = json!({"sortBy": {"column": "name; drop table"}});
        assert_eq!(sorted_by(&body, None, PAGEABLE), None);
        assert_eq!(sorted_by(&json!({}), None, PAGEABLE), None);
    }
}
