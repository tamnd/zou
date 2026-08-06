//! The bucket surface against a live postgres, for the things the
//! recording cannot ask about.
//!
//! The twenty five recorded answers in the conformance repository are
//! the compatibility claim and they are not repeated here. What is here
//! is the handful of behaviours that are real, that a client can reach,
//! and that the recording never asked in a shape that would pin them:
//! an update clearing a column rather than leaving it alone, a subject
//! landing in two columns of different types, an empty leaving the
//! bucket behind, and an id that is not the name.
//!
//! Every test works on bucket ids of its own and clears only those, so
//! that the four of them can run at once against one database the way
//! cargo runs them. Nothing here reads the whole table, for the same
//! reason.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test storage_buckets

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;
use zou_server::sql::Pool;
use zou_server::{Config, jwt, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

/// Somebody with an account, for the test about ownership. A service
/// key has no subject at all, which is why the recording says nothing
/// about what happens to one.
const WHO: &str = "3f7c1b0a-2d4e-4f6a-8b9c-0d1e2f3a4b5c";

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

fn app(dsn: &str) -> axum::Router {
    router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        ..Config::default()
    })
    .expect("router builds")
}

fn service() -> String {
    jwt::mint(&jwt::key_claims("service_role"), SECRET)
}

/// A token for a person rather than for the project. Both a subject
/// that is a uuid and one that is not, because the two columns the
/// subject is written into disagree about which of those is allowed.
fn user(sub: &str) -> String {
    let mut claims = jwt::key_claims("authenticated");
    claims["sub"] = Value::from(sub);
    jwt::mint(&claims, SECRET)
}

struct Answer {
    status: StatusCode,
    body: Value,
}

async fn call(app: &axum::Router, method: &str, path: &str, token: &str, body: &str) -> Answer {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"));
    if !body.is_empty() {
        req = req.header("content-type", "application/json");
    }
    let res = app
        .clone()
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .expect("router answers");
    let status = res.status();
    let bytes = to_bytes(res.into_body(), 1 << 20).await.expect("body");
    Answer {
        status,
        body: serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    }
}

/// Take the named buckets and their objects away, whether or not they
/// were there. Through the guard the schema puts in front of a delete
/// rather than around it, which is also how the conformance fixture
/// writes its rows.
async fn clear(pool: &Pool, ids: &[&str]) {
    let ids: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
    let sess = pool.unscoped().await.expect("unscoped");
    sess.execute("set storage.allow_delete_query = 'true'", &[])
        .await
        .expect("open the guard");
    sess.execute(
        "delete from storage.objects where bucket_id = any($1)",
        &[&ids],
    )
    .await
    .expect("clear objects");
    sess.execute("delete from storage.buckets where id = any($1)", &[&ids])
        .await
        .expect("clear buckets");
    sess.commit().await.expect("finish");
}

/// One bucket, written straight into the table rather than through the
/// api, for the tests that are about what happens next.
async fn make(pool: &Pool, id: &str) {
    let sess = pool.unscoped().await.expect("unscoped");
    sess.execute(
        "insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
         values ($1, $1, true, 4096, array['text/plain'])",
        &[&id],
    )
    .await
    .expect("make a bucket");
    sess.commit().await.expect("finish");
}

/// One bucket read straight out of the table, so a test can check what
/// was written rather than what was answered. The two owner columns
/// come back joined, because what matters about them is the pair.
async fn row(pool: &Pool, id: &str) -> Option<(bool, Option<i64>, Option<Vec<String>>, String)> {
    let sess = pool.unscoped().await.expect("unscoped");
    let rows = sess
        .query(
            "select public, file_size_limit, allowed_mime_types,
                    coalesce(owner::text, '') || '|' || coalesce(owner_id, '')
             from storage.buckets where id = $1",
            &[&id],
        )
        .await
        .expect("read back");
    let found = rows
        .first()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)));
    sess.commit().await.expect("finish");
    found
}

/// storage-api ships no policies whatsoever, so on a fresh project the
/// only caller who can make a bucket is the one holding the service
/// key, and that caller has no subject to be the owner. A project where
/// people make their own buckets is a project that wrote a policy, and
/// this is that policy: the smallest one that lets the ownership
/// columns ever be filled in. Permissive and only for authenticated, so
/// it cannot change what the other tests here see.
async fn let_people_make_buckets(pool: &Pool, on: bool) {
    let sess = pool.unscoped().await.expect("unscoped");
    sess.execute(
        "drop policy if exists zou_owner_test on storage.buckets",
        &[],
    )
    .await
    .expect("clear the policy");
    if on {
        sess.execute(
            "create policy zou_owner_test on storage.buckets
             for all to authenticated using (true) with check (true)",
            &[],
        )
        .await
        .expect("write the policy");
    }
    sess.commit().await.expect("finish");
}

async fn objects(pool: &Pool, id: &str) -> i64 {
    let sess = pool.unscoped().await.expect("unscoped");
    let n: i64 = sess
        .query(
            "select count(*) from storage.objects where bucket_id = $1",
            &[&id],
        )
        .await
        .expect("count")[0]
        .get(0);
    sess.commit().await.expect("finish");
    n
}

#[tokio::test]
async fn a_field_sent_as_null_clears_the_column_and_one_left_out_does_not() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let app = app(&dsn);
    clear(&pool, &["zou-update"]).await;
    make(&pool, "zou-update").await;

    // Upstream filters the body on undefined rather than on null, so
    // these two bodies are different instructions about the same three
    // columns. A handler that reached for a coalesce would read them as
    // the same one.
    let answer = call(
        &app,
        "PUT",
        "/storage/v1/bucket/zou-update",
        &service(),
        r#"{"public":false}"#,
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);
    let (public, limit, mimes, _) = row(&pool, "zou-update").await.expect("still there");
    assert!(!public, "the field that was sent did not change");
    assert_eq!(limit, Some(4096), "a field that was not sent changed");
    assert_eq!(
        mimes.as_deref(),
        Some(["text/plain".to_string()].as_slice())
    );

    let answer = call(
        &app,
        "PUT",
        "/storage/v1/bucket/zou-update",
        &service(),
        r#"{"file_size_limit":null,"allowed_mime_types":null}"#,
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);
    let (public, limit, mimes, _) = row(&pool, "zou-update").await.expect("still there");
    assert!(!public, "a field that was not sent changed");
    assert_eq!(limit, None, "an explicit null did not clear the column");
    assert_eq!(mimes, None, "an explicit null did not clear the column");
}

#[tokio::test]
async fn the_owner_of_a_new_bucket_is_whoever_asked_for_it() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let app = app(&dsn);
    clear(&pool, &["zou-mine", "zou-theirs", "zou-nobodys"]).await;
    let_people_make_buckets(&pool, true).await;

    // Two columns, and the deprecated one is a uuid. A subject that is
    // not one is not an error, it is a subject the uuid column cannot
    // hold, which is the whole reason upstream added the text column
    // next to it.
    let answer = call(
        &app,
        "POST",
        "/storage/v1/bucket",
        &user(WHO),
        r#"{"name":"zou-mine"}"#,
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);
    let (_, _, _, owner) = row(&pool, "zou-mine").await.expect("made");
    assert_eq!(owner, format!("{WHO}|{WHO}"));

    let answer = call(
        &app,
        "POST",
        "/storage/v1/bucket",
        &user("not-a-uuid"),
        r#"{"name":"zou-theirs"}"#,
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);
    let (_, _, _, owner) = row(&pool, "zou-theirs").await.expect("made");
    assert_eq!(owner, "|not-a-uuid");

    // And the project key, which is nobody. This one the recording does
    // pin, from the other end: every bucket it reads back answers with
    // an owner of "".
    let answer = call(
        &app,
        "POST",
        "/storage/v1/bucket",
        &service(),
        r#"{"name":"zou-nobodys"}"#,
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);
    let (_, _, _, owner) = row(&pool, "zou-nobodys").await.expect("made");
    assert_eq!(owner, "|");

    let_people_make_buckets(&pool, false).await;
}

#[tokio::test]
async fn emptying_a_bucket_takes_the_objects_and_leaves_the_bucket() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let app = app(&dsn);
    clear(&pool, &["zou-empty"]).await;
    make(&pool, "zou-empty").await;

    let sess = pool.unscoped().await.expect("unscoped");
    sess.execute(
        "insert into storage.objects (bucket_id, name) values ($1, 'a.txt'), ($1, 'b.txt')",
        &[&"zou-empty"],
    )
    .await
    .expect("two objects");
    sess.commit().await.expect("finish");
    assert_eq!(objects(&pool, "zou-empty").await, 2);

    let answer = call(
        &app,
        "POST",
        "/storage/v1/bucket/zou-empty/empty",
        &service(),
        "",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);
    assert_eq!(objects(&pool, "zou-empty").await, 0);
    assert!(
        row(&pool, "zou-empty").await.is_some(),
        "the bucket went too"
    );

    // And now it is empty it can be deleted, which is the pair of
    // statements the delete guard exists for.
    let answer = call(
        &app,
        "DELETE",
        "/storage/v1/bucket/zou-empty",
        &service(),
        "",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);
    assert!(row(&pool, "zou-empty").await.is_none());
}

#[tokio::test]
async fn a_bucket_can_be_named_differently_from_its_id() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 4).expect("dsn parses");
    let app = app(&dsn);
    clear(&pool, &["zou-photos"]).await;

    // The id is what every other route addresses and the name is what
    // people read, and a create may send both. What the recording pins
    // is the create that sends only a name, where the id takes the
    // name's value. This is the other half of that line, with the two
    // constraint columns going in and coming back out again on the way.
    let answer = call(
        &app,
        "POST",
        "/storage/v1/bucket",
        &service(),
        r#"{"id":"zou-photos","name":"Holiday photos","public":true,
            "file_size_limit":1048576,"allowed_mime_types":["image/png","image/jpeg"]}"#,
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);
    assert_eq!(answer.body["name"], "Holiday photos");

    let answer = call(&app, "GET", "/storage/v1/bucket/zou-photos", &service(), "").await;
    assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);
    assert_eq!(answer.body["id"], "zou-photos");
    assert_eq!(answer.body["name"], "Holiday photos");
    assert_eq!(answer.body["public"], true);
    assert_eq!(answer.body["file_size_limit"], 1048576);
    assert_eq!(answer.body["allowed_mime_types"][1], "image/jpeg");
}
