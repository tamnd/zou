//! The object surface against a live postgres and a real store, for the
//! things the recording cannot ask about.
//!
//! The twenty nine recorded answers in the conformance repository are
//! the compatibility claim and they are not repeated here. All of them
//! are about what comes back over http, and none of them can see the
//! other half of an upload, which is where the bytes went. That half is
//! what is here: that the key they are under is built out of the row and
//! not out of anything a client sent, that a replacement leaves the old
//! bytes alone until it has committed and then takes them away, that a
//! delete takes them with the row, and that an upload refused for a name
//! already taken leaves the bytes of the name it collided with where
//! they were.
//!
//! Move and copy are here for the same reason and for one more: what
//! they do to the store is the whole of the difference between them. A
//! move changes two columns and leaves the bytes alone, because the key
//! is the row's id and its version and a name is in neither. A copy
//! writes a second set, because two rows pointing at one key would make
//! a delete of either a delete of both.
//!
//! Signed urls are here because the recording cannot spend one. The
//! harness compares one request against one answer and has no way to
//! carry a url out of the first into the second, so what it can ask is
//! whether the signing answered and in what shape. Whether the url then
//! works, whether it works for the object it names and for no other,
//! whether a read url is refused on the write route, and whether a
//! token this project did not sign is refused at all, are asked here
//! instead, end to end through the router.
//!
//! Every test works on a bucket of its own with a store of its own, so
//! they can all run at once against one database the way cargo runs
//! them, and so a leftover directory from a failed run cannot be read as
//! a passing one.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test storage_objects

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;
use zou_server::blob::{Blobs, key};
use zou_server::sql::Pool;
use zou_server::{Config, jwt, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

/// A server and the store it writes into, kept together because the
/// tests read the store directly and the directory has to outlive both.
struct Fixture {
    app: axum::Router,
    pool: Pool,
    blobs: Blobs,
    _dir: tempfile::TempDir,
}

fn fixture(dsn: &str) -> Fixture {
    let dir = tempfile::tempdir().expect("a directory to write into");
    let target = dir.path().to_string_lossy().to_string();
    Fixture {
        app: router(Config {
            jwt_secret: SECRET.to_vec(),
            pg: Some(dsn.to_string()),
            objects: Some(target.clone()),
            ..Config::default()
        })
        .expect("router builds"),
        pool: Pool::new(dsn, 4).expect("dsn parses"),
        blobs: Blobs::open(&target, zou_server::blob::LOCAL).expect("the store opens"),
        _dir: dir,
    }
}

fn service() -> String {
    jwt::mint(&jwt::key_claims("service_role"), SECRET)
}

struct Answer {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    bytes: Vec<u8>,
}

impl Answer {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.bytes).unwrap_or(Value::Null)
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.bytes).to_string()
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
}

async fn call(f: &Fixture, method: &str, path: &str, mime: &str, body: &str) -> Answer {
    as_whoever(f, Some(&service()), method, path, mime, body).await
}

/// A read carrying whatever the client says it already has.
async fn asking(f: &Fixture, method: &str, path: &str, said: &[(&str, &str)]) -> Answer {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {}", service()));
    for (name, value) in said {
        req = req.header(*name, *value);
    }
    let res = f
        .app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .expect("router answers");
    let status = res.status();
    let headers = res.headers().clone();
    let bytes = to_bytes(res.into_body(), 1 << 20).await.expect("body");
    Answer {
        status,
        headers,
        bytes: bytes.to_vec(),
    }
}

/// The same request, with a token of the caller's choosing or with none
/// at all. A signed url is spent with none: the token is in the query
/// string and an authorization header would be a second answer to a
/// question nobody asked.
async fn as_whoever(
    f: &Fixture,
    token: Option<&str>,
    method: &str,
    path: &str,
    mime: &str,
    body: &str,
) -> Answer {
    let mut req = Request::builder().method(method).uri(path);
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    if !mime.is_empty() {
        req = req.header("content-type", mime);
    }
    let res = f
        .app
        .clone()
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .expect("router answers");
    let status = res.status();
    let headers = res.headers().clone();
    let bytes = to_bytes(res.into_body(), 1 << 20).await.expect("body");
    Answer {
        status,
        headers,
        bytes: bytes.to_vec(),
    }
}

/// Take the bucket and everything in it away, whether or not any of it
/// was there, then make the bucket again. Through the guard the schema
/// puts in front of a delete rather than around it.
async fn fresh(pool: &Pool, bucket: &str) {
    let sess = pool.unscoped().await.expect("unscoped");
    sess.execute("set storage.allow_delete_query = 'true'", &[])
        .await
        .expect("open the guard");
    sess.execute(
        "delete from storage.objects where bucket_id = $1",
        &[&bucket],
    )
    .await
    .expect("clear objects");
    sess.execute("delete from storage.buckets where id = $1", &[&bucket])
        .await
        .expect("clear the bucket");
    sess.execute(
        "insert into storage.buckets (id, name, public) values ($1, $1, false)",
        &[&bucket],
    )
    .await
    .expect("make the bucket");
    sess.commit().await.expect("finish");
}

/// The id and the version of one row, which together are the only thing
/// the key is built out of.
async fn row(pool: &Pool, bucket: &str, name: &str) -> Option<(String, String)> {
    let sess = pool.unscoped().await.expect("unscoped");
    let rows = sess
        .query(
            "select id::text, coalesce(version, '') from storage.objects
             where bucket_id = $1 and name = $2",
            &[&bucket, &name],
        )
        .await
        .expect("read back");
    let found = rows.first().map(|r| (r.get(0), r.get(1)));
    sess.commit().await.expect("finish");
    found
}

async fn how_many(pool: &Pool, bucket: &str) -> i64 {
    let sess = pool.unscoped().await.expect("unscoped");
    let n: i64 = sess
        .query(
            "select count(*) from storage.objects where bucket_id = $1",
            &[&bucket],
        )
        .await
        .expect("count")[0]
        .get(0);
    sess.commit().await.expect("finish");
    n
}

async fn bytes_at(f: &Fixture, id: &str, version: &str) -> Option<Vec<u8>> {
    f.blobs
        .get(key(id, version))
        .await
        .expect("the store answers")
}

#[tokio::test]
async fn the_key_is_built_out_of_the_row_and_not_out_of_the_name() {
    let Some(dsn) = dsn() else { return };
    let f = fixture(&dsn);
    fresh(&f.pool, "zou-keys").await;

    // A name that is a path, with a segment that would climb out of the
    // store if a name were ever a path here. It is one object with one
    // name, and the bytes are under the id and the version.
    let name = "a/../b/c d.txt";
    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/zou-keys/a/../b/c%20d.txt",
        "text/plain",
        "under the name",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert_eq!(answer.json()["Key"], format!("zou-keys/{name}"));

    let (id, version) = row(&f.pool, "zou-keys", name).await.expect("one row");
    assert_eq!(
        bytes_at(&f, &id, &version).await.as_deref(),
        Some(&b"under the name"[..]),
    );

    // And the round trip, so the read builds the same key the write did.
    let answer = call(
        &f,
        "GET",
        "/storage/v1/object/zou-keys/a/../b/c%20d.txt",
        "",
        "",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK);
    assert_eq!(answer.text(), "under the name");
}

#[tokio::test]
async fn a_replacement_writes_a_new_version_and_takes_the_old_bytes_away() {
    let Some(dsn) = dsn() else { return };
    let f = fixture(&dsn);
    fresh(&f.pool, "zou-replace").await;

    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/zou-replace/note.txt",
        "text/plain",
        "first",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    let (id, first) = row(&f.pool, "zou-replace", "note.txt")
        .await
        .expect("a row");

    let answer = call(
        &f,
        "PUT",
        "/storage/v1/object/zou-replace/note.txt",
        "text/plain",
        "second",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    let (again, second) = row(&f.pool, "zou-replace", "note.txt")
        .await
        .expect("a row");

    // One row, still the same row, and a version it did not have before.
    // The version is what makes a replacement safe to read across: the
    // bytes are written next to the old ones rather than over them.
    assert_eq!(how_many(&f.pool, "zou-replace").await, 1);
    assert_eq!(again, id, "the replacement made a second row");
    assert_ne!(second, first, "the version did not move");

    assert_eq!(
        bytes_at(&f, &id, &second).await.as_deref(),
        Some(&b"second"[..]),
    );
    assert!(
        bytes_at(&f, &id, &first).await.is_none(),
        "the bytes nothing points at any more are still costing money",
    );
    let answer = call(&f, "GET", "/storage/v1/object/zou-replace/note.txt", "", "").await;
    assert_eq!(answer.text(), "second");
}

#[tokio::test]
async fn a_delete_takes_the_bytes_with_the_row() {
    let Some(dsn) = dsn() else { return };
    let f = fixture(&dsn);
    fresh(&f.pool, "zou-delete").await;

    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/zou-delete/gone.txt",
        "text/plain",
        "here for now",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    let (id, version) = row(&f.pool, "zou-delete", "gone.txt").await.expect("a row");
    assert!(bytes_at(&f, &id, &version).await.is_some());

    let answer = call(
        &f,
        "DELETE",
        "/storage/v1/object/zou-delete/gone.txt",
        "",
        "",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert!(row(&f.pool, "zou-delete", "gone.txt").await.is_none());
    assert!(
        bytes_at(&f, &id, &version).await.is_none(),
        "the row went and the bytes stayed",
    );

    // The other delete, which takes a list and is the one a client
    // library calls. Same claim about the store.
    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/zou-delete/one-of-many.txt",
        "text/plain",
        "here for now",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    let (id, version) = row(&f.pool, "zou-delete", "one-of-many.txt")
        .await
        .expect("a row");
    let answer = call(
        &f,
        "DELETE",
        "/storage/v1/object/zou-delete",
        "application/json",
        r#"{"prefixes":["one-of-many.txt","never-existed.txt"]}"#,
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert_eq!(
        answer.json().as_array().map(Vec::len),
        Some(1),
        "a name that was not there was answered as if it had been",
    );
    assert!(bytes_at(&f, &id, &version).await.is_none());
}

#[tokio::test]
async fn an_upload_onto_a_name_already_taken_leaves_the_first_bytes_alone() {
    let Some(dsn) = dsn() else { return };
    let f = fixture(&dsn);
    fresh(&f.pool, "zou-collide").await;

    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/zou-collide/note.txt",
        "text/plain",
        "the one that got there first",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    let (id, version) = row(&f.pool, "zou-collide", "note.txt")
        .await
        .expect("a row");

    // The refusal itself is recorded. What is not is that the refused
    // upload wrote nothing: the row is inside the transaction and the
    // bytes come after it, so a conflict never reaches the store.
    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/zou-collide/note.txt",
        "text/plain",
        "the one that came second",
    )
    .await;
    assert_eq!(answer.status, StatusCode::BAD_REQUEST, "{}", answer.text());
    assert_eq!(answer.json()["error"], "Duplicate");

    assert_eq!(how_many(&f.pool, "zou-collide").await, 1);
    let (still, same) = row(&f.pool, "zou-collide", "note.txt")
        .await
        .expect("a row");
    assert_eq!((still, same), (id.clone(), version.clone()));
    assert_eq!(
        bytes_at(&f, &id, &version).await.as_deref(),
        Some(&b"the one that got there first"[..]),
    );
}

/// Put limits on a bucket [`fresh`] has already made.
async fn limited(pool: &Pool, bucket: &str, most: i64, types: &[&str]) {
    let sess = pool.unscoped().await.expect("unscoped");
    let types: Vec<String> = types.iter().map(|t| t.to_string()).collect();
    sess.execute(
        "update storage.buckets
            set file_size_limit = $2, allowed_mime_types = $3 where id = $1",
        &[&bucket, &most, &types],
    )
    .await
    .expect("set the limits");
    sess.commit().await.expect("finish");
}

#[tokio::test]
async fn an_upload_a_bucket_refuses_leaves_nothing_behind() {
    let Some(dsn) = dsn() else { return };
    let f = fixture(&dsn);
    fresh(&f.pool, "zou-limits").await;
    limited(&f.pool, "zou-limits", 20, &["text/plain"]).await;

    // The two refusals are recorded. What is not is what they leave: an
    // upload is a row and then bytes, and a refusal that happened after
    // the row would leave a row with nothing behind it, which is the
    // one state this surface answers 500 for.
    for (mime, body, code) in [
        ("application/json", "{\"a\":1}", "InvalidMimeType"),
        ("text/plain", "x".repeat(40).as_str(), "EntityTooLarge"),
    ] {
        let answer = call(
            &f,
            "POST",
            "/storage/v1/object/zou-limits/refused.txt",
            mime,
            body,
        )
        .await;
        assert_eq!(answer.status, StatusCode::BAD_REQUEST, "{}", answer.text());
        assert_eq!(answer.json()["code"], code);
        assert_eq!(how_many(&f.pool, "zou-limits").await, 0);
    }

    // And the one that fits goes in, so the refusals above are the
    // limits doing their job rather than the bucket refusing everything.
    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/zou-limits/fits.txt",
        "text/plain",
        "hello world",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    let (id, version) = row(&f.pool, "zou-limits", "fits.txt").await.expect("a row");
    assert_eq!(
        bytes_at(&f, &id, &version).await.as_deref(),
        Some(&b"hello world"[..]),
    );
}

#[tokio::test]
async fn a_move_leaves_the_bytes_where_they_are() {
    let Some(dsn) = dsn() else { return };
    let f = fixture(&dsn);
    fresh(&f.pool, "zou-move").await;

    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/zou-move/here.txt",
        "text/plain",
        "the same bytes either way",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    let (id, version) = row(&f.pool, "zou-move", "here.txt").await.expect("a row");

    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/move",
        "application/json",
        r#"{"bucketId":"zou-move","sourceKey":"here.txt","destinationKey":"there/here.txt"}"#,
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert_eq!(answer.json()["message"], "Successfully moved");

    // One row, the same row, under a new name, and the key its bytes are
    // under has not changed because a name is not part of it. This is
    // where zou and storage-api differ: upstream keys the store by the
    // name, so its move copies the bytes across and writes a new
    // version.
    assert_eq!(how_many(&f.pool, "zou-move").await, 1);
    assert_eq!(row(&f.pool, "zou-move", "here.txt").await, None);
    let (after, still) = row(&f.pool, "zou-move", "there/here.txt")
        .await
        .expect("a row");
    assert_eq!((after, still), (id.clone(), version.clone()));
    assert_eq!(
        bytes_at(&f, &id, &version).await.as_deref(),
        Some(&b"the same bytes either way"[..]),
    );

    // And the object answers at the name it was moved to.
    let answer = call(
        &f,
        "GET",
        "/storage/v1/object/zou-move/there/here.txt",
        "",
        "",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK);
    assert_eq!(answer.text(), "the same bytes either way");
}

#[tokio::test]
async fn a_copy_writes_a_second_set_of_bytes() {
    let Some(dsn) = dsn() else { return };
    let f = fixture(&dsn);
    fresh(&f.pool, "zou-copy").await;

    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/zou-copy/one.txt",
        "text/plain",
        "worth having twice",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    let (id, version) = row(&f.pool, "zou-copy", "one.txt").await.expect("a row");

    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/copy",
        "application/json",
        r#"{"bucketId":"zou-copy","sourceKey":"one.txt","destinationKey":"two.txt"}"#,
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert_eq!(answer.json()["Key"], "zou-copy/two.txt");

    // Two rows and two keys. Sharing one key would make deleting either
    // of them a delete of both, and nothing in the store counts what
    // points at it.
    assert_eq!(how_many(&f.pool, "zou-copy").await, 2);
    let (second, other) = row(&f.pool, "zou-copy", "two.txt").await.expect("a row");
    assert_ne!(second, id, "the copy is the same row");
    assert_ne!(key(&second, &other), key(&id, &version), "one key for two");
    assert_eq!(
        bytes_at(&f, &second, &other).await.as_deref(),
        Some(&b"worth having twice"[..]),
    );
    // The one it was copied from is still whole, which is the difference
    // between a copy and a move.
    assert_eq!(
        bytes_at(&f, &id, &version).await.as_deref(),
        Some(&b"worth having twice"[..]),
    );
}

#[tokio::test]
async fn a_copy_that_is_refused_writes_no_bytes() {
    let Some(dsn) = dsn() else { return };
    let f = fixture(&dsn);
    fresh(&f.pool, "zou-copy-onto").await;

    for name in ["one.txt", "two.txt"] {
        let answer = call(
            &f,
            "POST",
            &format!("/storage/v1/object/zou-copy-onto/{name}"),
            "text/plain",
            name,
        )
        .await;
        assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    }
    let (id, version) = row(&f.pool, "zou-copy-onto", "two.txt")
        .await
        .expect("a row");

    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/copy",
        "application/json",
        r#"{"bucketId":"zou-copy-onto","sourceKey":"one.txt","destinationKey":"two.txt"}"#,
    )
    .await;
    assert_eq!(answer.status, StatusCode::BAD_REQUEST, "{}", answer.text());
    assert_eq!(answer.json()["error"], "Duplicate");

    // The row it collided with still has the bytes it had, because the
    // insert is inside the transaction and the store is only reached
    // after it.
    assert_eq!(how_many(&f.pool, "zou-copy-onto").await, 2);
    let (still, same) = row(&f.pool, "zou-copy-onto", "two.txt")
        .await
        .expect("a row");
    assert_eq!((still, same), (id.clone(), version.clone()));
    assert_eq!(
        bytes_at(&f, &id, &version).await.as_deref(),
        Some(&b"two.txt"[..]),
    );
}

/// Everything a download says about itself, which is more than the
/// bytes and is where a client library gets the filename it saves under.
///
/// The suite cannot see most of this. Its comparison is eight headers
/// long and none of these four are among them, so the only place they
/// can be held to anything is here.
#[tokio::test]
async fn a_download_carries_what_the_renderer_carries() {
    let Some(dsn) = dsn() else { return };
    let f = fixture(&dsn);
    fresh(&f.pool, "zou-headers").await;

    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/zou-headers/report%20card.txt",
        "text/plain",
        "eleven byte",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());

    let answer = call(
        &f,
        "GET",
        "/storage/v1/object/zou-headers/report%20card.txt",
        "",
        "",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert_eq!(answer.header("accept-ranges"), Some("bytes"));
    assert_eq!(
        answer.header("content-type"),
        Some("text/plain; charset=UTF-8")
    );
    assert_eq!(answer.header("x-robots-tag"), Some("none"));
    assert_eq!(answer.header("content-length"), Some("11"));
    // Written by the upload as json and sent as a browser writes it.
    let modified = answer.header("last-modified").expect("no Last-Modified");
    assert!(modified.ends_with(" GMT"), "{modified}");
    // Nobody asked for a download, so there is nothing to say about one.
    assert_eq!(answer.header("content-disposition"), None);

    // Asked for by name, and the name is encoded twice over.
    let answer = call(
        &f,
        "GET",
        "/storage/v1/object/zou-headers/report%20card.txt?download=my%20card.txt",
        "",
        "",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert_eq!(
        answer.header("content-disposition"),
        Some("attachment; filename=my%20card.txt; filename*=UTF-8''my%20card.txt")
    );

    // Asked for without a name, which is the parameter on its own.
    let answer = call(
        &f,
        "GET",
        "/storage/v1/object/zou-headers/report%20card.txt?download",
        "",
        "",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert_eq!(answer.header("content-disposition"), Some("attachment;"));
}

/// A not modified answer carries everything a 200 would have carried
/// except the bytes, which is what upstream's renderer does: it writes
/// the headers and then sends whatever it has, and on a 304 it has
/// nothing.
///
/// The suite asks the status and the content type of this and can ask
/// no more, because the rest of the set is not in the eight headers it
/// compares.
#[tokio::test]
async fn a_not_modified_answer_carries_the_headers_and_no_bytes() {
    let Some(dsn) = dsn() else { return };
    let f = fixture(&dsn);
    fresh(&f.pool, "zou-conditional").await;

    let path = "/storage/v1/object/zou-conditional/etag.txt";
    let answer = call(&f, "POST", path, "text/plain", "hello world").await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());

    let answer = asking(&f, "GET", path, &[]).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    let etag = answer.header("etag").expect("no ETag").to_string();
    let modified = answer
        .header("last-modified")
        .expect("no Last-Modified")
        .to_string();

    let answer = asking(&f, "GET", path, &[("if-none-match", &etag)]).await;
    assert_eq!(answer.status, StatusCode::NOT_MODIFIED, "{}", answer.text());
    assert!(answer.bytes.is_empty(), "{}", answer.text());
    assert_eq!(answer.header("accept-ranges"), Some("bytes"));
    assert_eq!(
        answer.header("content-type"),
        Some("text/plain; charset=UTF-8")
    );
    assert_eq!(answer.header("etag"), Some(etag.as_str()));
    assert_eq!(answer.header("x-robots-tag"), Some("none"));
    assert_eq!(answer.header("last-modified"), Some(modified.as_str()));
    assert_eq!(answer.header("content-length"), Some("0"));

    // A range next to a condition that is already met is a not modified
    // answer, because the condition is read before the range is.
    let answer = asking(
        &f,
        "GET",
        path,
        &[("if-none-match", &etag), ("range", "bytes=0-3")],
    )
    .await;
    assert_eq!(answer.status, StatusCode::NOT_MODIFIED, "{}", answer.text());
    assert_eq!(answer.header("content-range"), None);

    // A HEAD is a different route upstream, answered out of the row by
    // a renderer that never opens the store, so it has no condition to
    // read and answers the whole header set with a 200.
    let answer = asking(&f, "HEAD", path, &[("if-none-match", &etag)]).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert!(answer.bytes.is_empty());
    assert_eq!(answer.header("etag"), Some(etag.as_str()));

    // The moment it was written is not after itself, and a moment
    // before it is a request for the bytes.
    let answer = asking(&f, "GET", path, &[("if-modified-since", &modified)]).await;
    assert_eq!(answer.status, StatusCode::NOT_MODIFIED, "{}", answer.text());
    let answer = asking(
        &f,
        "GET",
        path,
        &[("if-modified-since", "Wed, 01 Jan 2020 00:00:00 GMT")],
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert_eq!(answer.text(), "hello world");
}

/// The url out of a signing answer, with the prefix the client would
/// have put back on it. What comes back is a path relative to
/// /storage/v1, because that is where the client's own url ends.
fn spendable(answer: &Answer, field: &str) -> String {
    let url = answer.json()[field]
        .as_str()
        .unwrap_or_else(|| panic!("no {field} in {}", answer.text()))
        .to_string();
    format!("/storage/v1{url}")
}

#[tokio::test]
async fn a_signed_url_reads_the_one_object_it_names() {
    let Some(dsn) = dsn() else { return };
    let f = fixture(&dsn);
    fresh(&f.pool, "zou-sign").await;

    for name in ["one.txt", "two.txt"] {
        let answer = call(
            &f,
            "POST",
            &format!("/storage/v1/object/zou-sign/{name}"),
            "text/plain",
            name,
        )
        .await;
        assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    }

    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/sign/zou-sign/one.txt",
        "application/json",
        r#"{"expiresIn":600}"#,
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    let url = spendable(&answer, "signedURL");
    assert!(url.starts_with("/storage/v1/object/sign/zou-sign/one.txt?token="));

    // Spent with no token in a header at all, which is the whole point
    // of it: the person holding this url is not a caller this project
    // knows anything about.
    let answer = as_whoever(&f, None, "GET", &url, "", "").await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert_eq!(answer.text(), "one.txt");
    // What a shared cache may keep is the url's own lifetime rather
    // than what the upload said about caching, so this answer carries
    // the one header a plain download does not and drops the one it
    // does.
    assert!(answer.header("expires").is_some(), "no Expires");
    assert_eq!(answer.header("cache-control"), None);

    // The same token against the other object in the same bucket. The
    // name is in the token and the token is signed, so moving it is
    // forging it.
    let elsewhere = url.replace("one.txt", "two.txt");
    let answer = as_whoever(&f, None, "GET", &elsewhere, "", "").await;
    assert_eq!(answer.status, StatusCode::BAD_REQUEST, "{}", answer.text());
    assert_eq!(answer.json()["error"], "InvalidSignature");
}

#[tokio::test]
async fn an_upload_url_writes_the_bytes_as_whoever_signed_it() {
    let Some(dsn) = dsn() else { return };
    let f = fixture(&dsn);
    fresh(&f.pool, "zou-sign-upload").await;

    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/upload/sign/zou-sign-upload/new.txt",
        "",
        "",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    // Nothing was written by the asking. The insert that asked whether
    // the caller could upload was rolled back, so the name is still
    // free and the url is what takes it.
    assert_eq!(how_many(&f.pool, "zou-sign-upload").await, 0);

    let url = spendable(&answer, "url");
    let answer = as_whoever(&f, None, "PUT", &url, "text/plain", "sent by a stranger").await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert_eq!(answer.json()["Key"], "zou-sign-upload/new.txt");

    let (id, version) = row(&f.pool, "zou-sign-upload", "new.txt")
        .await
        .expect("one row");
    assert_eq!(
        bytes_at(&f, &id, &version).await.as_deref(),
        Some(&b"sent by a stranger"[..]),
    );

    // And once only, because the url was signed without upsert. What
    // refuses the second one is the same unique index that refuses a
    // second upload, reached with the policies off.
    let answer = as_whoever(&f, None, "PUT", &url, "text/plain", "again").await;
    assert_eq!(answer.status, StatusCode::BAD_REQUEST, "{}", answer.text());
    assert_eq!(answer.json()["error"], "Duplicate");
    assert_eq!(
        bytes_at(&f, &id, &version).await.as_deref(),
        Some(&b"sent by a stranger"[..]),
    );
}

#[tokio::test]
async fn a_url_signed_for_reading_cannot_be_spent_on_writing() {
    let Some(dsn) = dsn() else { return };
    let f = fixture(&dsn);
    fresh(&f.pool, "zou-scope").await;

    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/zou-scope/one.txt",
        "text/plain",
        "one.txt",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());

    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/sign/zou-scope/one.txt",
        "application/json",
        r#"{"expiresIn":600}"#,
    )
    .await;
    let reading = spendable(&answer, "signedURL");
    let token = reading.split_once("token=").expect("a token").1.to_string();

    // The read url pointed at the write route. Before the scope claim
    // existed the two kinds of token were told apart by whether they
    // carried upsert, and a read token carries none, so this would
    // have been read as an upload url.
    let writing = format!("/storage/v1/object/upload/sign/zou-scope/one.txt?token={token}");
    let answer = as_whoever(&f, None, "PUT", &writing, "text/plain", "not yours").await;
    assert_eq!(answer.status, StatusCode::BAD_REQUEST, "{}", answer.text());
    assert_eq!(answer.json()["message"], "Token is not scoped for upload");

    // And the other way, so neither direction is the loose one.
    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/upload/sign/zou-scope/two.txt",
        "",
        "",
    )
    .await;
    let upload = spendable(&answer, "url");
    let downward = upload.replace("/object/upload/sign/", "/object/sign/");
    let answer = as_whoever(&f, None, "GET", &downward, "", "").await;
    assert_eq!(answer.status, StatusCode::BAD_REQUEST, "{}", answer.text());
    assert_eq!(answer.json()["message"], "Token is not scoped for download");
}

#[tokio::test]
async fn signing_a_list_answers_every_path_that_was_asked() {
    let Some(dsn) = dsn() else { return };
    let f = fixture(&dsn);
    fresh(&f.pool, "zou-sign-many").await;

    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/zou-sign-many/there.txt",
        "text/plain",
        "there.txt",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());

    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/sign/zou-sign-many",
        "application/json",
        r#"{"expiresIn":600,"paths":["missing.txt","there.txt","there.txt"]}"#,
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    let answers = answer.json();
    let answers = answers.as_array().expect("an array");

    // Three answers for three paths, in the order they were asked, and
    // the same path twice is answered twice. The lookup dedupes and the
    // answer does not.
    assert_eq!(answers.len(), 3);
    assert_eq!(answers[0]["signedURL"], Value::Null);
    assert_eq!(
        answers[0]["error"],
        "Either the object does not exist or you do not have access to it"
    );
    for at in [1, 2] {
        assert_eq!(answers[at]["error"], Value::Null);
        assert_eq!(answers[at]["path"], "there.txt");
        let url = format!(
            "/storage/v1{}",
            answers[at]["signedURL"].as_str().expect("a url")
        );
        let got = as_whoever(&f, None, "GET", &url, "", "").await;
        assert_eq!(got.status, StatusCode::OK, "{}", got.text());
        assert_eq!(got.text(), "there.txt");
    }
}

#[tokio::test]
async fn a_token_this_server_did_not_write_reads_nothing() {
    let Some(dsn) = dsn() else { return };
    let f = fixture(&dsn);
    fresh(&f.pool, "zou-forged").await;

    let answer = call(
        &f,
        "POST",
        "/storage/v1/object/zou-forged/one.txt",
        "text/plain",
        "one.txt",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());

    // Everything a real token says, signed with a secret that is not
    // this project's. The claims are right and the signature is not,
    // and the signature is what is checked first.
    let forged = jwt::mint(
        &serde_json::json!({
            "url": "zou-forged/one.txt", "scope": "download",
            "iat": 0, "exp": 9_000_000_000i64,
        }),
        b"not the secret this project signs with at all",
    );
    let answer = as_whoever(
        &f,
        None,
        "GET",
        &format!("/storage/v1/object/sign/zou-forged/one.txt?token={forged}"),
        "",
        "",
    )
    .await;
    assert_eq!(answer.status, StatusCode::BAD_REQUEST, "{}", answer.text());
    assert_eq!(answer.json()["error"], "InvalidJWT");
    assert_eq!(answer.json()["message"], "signature verification failed");

    // And one that expired while nobody was holding it. The claims are
    // this server's own, which is what makes the refusal about the
    // clock rather than about the signature.
    let stale = jwt::mint(
        &serde_json::json!({
            "url": "zou-forged/one.txt", "scope": "download",
            "iat": 1, "exp": 2,
        }),
        SECRET,
    );
    let answer = as_whoever(
        &f,
        None,
        "GET",
        &format!("/storage/v1/object/sign/zou-forged/one.txt?token={stale}"),
        "",
        "",
    )
    .await;
    assert_eq!(answer.status, StatusCode::BAD_REQUEST, "{}", answer.text());
    assert_eq!(answer.json()["error"], "InvalidJWT");
    assert_eq!(
        answer.json()["message"],
        "\"exp\" claim timestamp check failed"
    );
}
