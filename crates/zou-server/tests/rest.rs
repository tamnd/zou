//! The REST read path against a live postgres.
//!
//! Gated on ZOU_PG_TEST_DSN like the pool suite. These go through
//! the whole front door: real HTTP requests into the router, the
//! gate, the session pool, the catalog introspection, the planner,
//! and back out as PostgREST shaped bodies and headers.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test rest

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
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

fn anon_key() -> String {
    jwt::mint(&jwt::key_claims("anon"), SECRET)
}

fn app(dsn: &str) -> axum::Router {
    router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        ..Config::default()
    })
    .expect("router builds")
}

fn app_with_schemas(dsn: &str, schemas: &[&str]) -> axum::Router {
    router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        schemas: schemas.iter().map(|s| s.to_string()).collect(),
        ..Config::default()
    })
    .expect("router builds")
}

/// The fixture's tables, and the grant a real project's migrations
/// would carry.
///
/// A table in public arrives readable by nobody who comes through the
/// api, same as upstream, so a suite that reads one through the api has
/// to say so. The grant names each object this seed just created rather
/// than the whole schema, for two reasons: the suites in this crate run
/// as separate binaries against one database, so a blanket grant would
/// hand out tables belonging to a test that is asserting nobody was
/// given them, and two blanket grants running at once rewrite the same
/// catalog rows and one of them dies with "tuple concurrently updated".
async fn seed(dsn: &str, statements: &[&str]) {
    let pool = Pool::new(dsn, 1).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    for stmt in statements {
        sess.execute(stmt, &[]).await.expect(stmt);
        let Some(object) = created(stmt) else {
            continue;
        };
        let grant = format!(
            "grant select, insert, update, delete on {object} \
             to anon, authenticated, service_role"
        );
        sess.execute(&grant, &[]).await.expect(&grant);
    }
    sess.commit().await.expect("park");
}

/// The name a `create table` or `create view` statement just made, or
/// None for a statement that made neither.
fn created(stmt: &str) -> Option<&str> {
    let rest = word(stmt.trim_start(), "create")?;
    let rest = word(rest, "or replace").unwrap_or(rest);
    let rest = word(rest, "materialized").unwrap_or(rest);
    let rest = word(rest, "table").or_else(|| word(rest, "view"))?;
    let rest = word(rest, "if not exists").unwrap_or(rest);
    let name = rest.split(|c: char| c.is_whitespace() || c == '(').next()?;
    match name.is_empty() {
        true => None,
        false => Some(name),
    }
}

/// What is left of `s` after `kw`, whose words may be separated by any
/// run of whitespace, or None if `s` does not start with it.
fn word<'a>(s: &'a str, kw: &str) -> Option<&'a str> {
    let mut rest = s;
    for want in kw.split(' ') {
        let cut = rest.find(char::is_whitespace)?;
        if !rest[..cut].eq_ignore_ascii_case(want) {
            return None;
        }
        rest = rest[cut..].trim_start();
    }
    Some(rest)
}

async fn body_text(res: axum::response::Response) -> String {
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header("apikey", anon_key())
        .body(Body::empty())
        .unwrap()
}

fn req(method: &str, uri: &str, body: &str, prefers: &[&str]) -> Request<Body> {
    let mut b = Request::builder()
        .method(method)
        .uri(uri)
        .header("apikey", anon_key());
    for p in prefers {
        b = b.header("prefer", *p);
    }
    let body = if body.is_empty() {
        Body::empty()
    } else {
        Body::from(body.to_string())
    };
    b.body(body).unwrap()
}

#[tokio::test]
async fn the_read_path_speaks_postgrest() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_rest_books, zou_rest_msgs, zou_rest_authors cascade",
            "create table zou_rest_authors (id int primary key, name text)",
            "create table zou_rest_books (id int primary key, \
             author_id int references zou_rest_authors(id), title text)",
            "create table zou_rest_msgs (id int primary key, \
             sender int references zou_rest_authors(id), \
             recipient int references zou_rest_authors(id))",
            "insert into zou_rest_authors values (1, 'ann'), (2, 'bob')",
            "insert into zou_rest_books values (10, 1, 'a1'), (11, 1, 'a2'), (12, 2, 'b1')",
            "insert into zou_rest_msgs values (100, 1, 2)",
        ],
    )
    .await;
    let app = app(&dsn);

    // A plain ordered read: the body is a json array, the headers
    // are PostgREST's.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_authors?select=name&order=name.asc"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(
        res.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    assert_eq!(res.headers()["content-range"], "0-1/*");
    assert_eq!(body_text(res).await, r#"[{"name":"ann"},{"name":"bob"}]"#);

    // An embedded to many folds to a json array per parent, empty
    // rides as [] and a nested order applies inside the fold.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_authors?select=name,zou_rest_books(title)&order=name&zou_rest_books.order=title.desc",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        body_text(res).await,
        r#"[{"name":"ann","zou_rest_books":[{"title": "a2"}, {"title": "a1"}]},{"name":"bob","zou_rest_books":[{"title": "b1"}]}]"#
    );

    // A filter narrows, limit and offset window, and Content-Range
    // reports the window.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_authors?select=name&name=eq.ann"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"name":"ann"}]"#);

    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_authors?select=name&order=name&limit=1&offset=1",
        ))
        .await
        .unwrap();
    assert_eq!(res.headers()["content-range"], "1-1/*");
    assert_eq!(body_text(res).await, r#"[{"name":"bob"}]"#);

    // A window that starts below the first row starts at the first
    // row, and the rows it would have skipped come off the limit.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_authors?select=name&order=name&offset=-4",
        ))
        .await
        .unwrap();
    assert_eq!(res.headers()["content-range"], "0-1/*");
    assert_eq!(body_text(res).await, r#"[{"name":"ann"},{"name":"bob"}]"#);

    // A window that ends before it starts is a range and not a
    // grammar, so it is refused as one.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_authors?select=name&limit=-1"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert!(res.headers().get("content-range").is_none());
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "PGRST103");
    assert_eq!(body["message"], "Requested range not satisfiable");
    assert_eq!(
        body["details"],
        "Limit should be greater than or equal to zero."
    );

    // The Range header pages when the query string did not.
    let mut req = get("/rest/v1/zou_rest_authors?select=name&order=name");
    req.headers_mut().insert("range", "0-0".parse().unwrap());
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.headers()["content-range"], "0-0/*");
    assert_eq!(body_text(res).await, r#"[{"name":"ann"}]"#);

    // No rows is an empty array and the unsatisfied range shape.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_authors?select=name&name=eq.zed"))
        .await
        .unwrap();
    assert_eq!(res.headers()["content-range"], "*/*");
    assert_eq!(body_text(res).await, "[]");

    // A table the schema does not have is answered from the schema
    // cache, never from postgres, so no statement is written and the
    // message names the schema the caller was profiled into.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_nope"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "PGRST205");
    assert_eq!(
        body["message"],
        "Could not find the table 'public.zou_rest_nope' in the schema cache"
    );
    // Near enough to a table that is there to be worth suggesting.
    assert_eq!(
        body["hint"],
        "Perhaps you meant the table 'public.zou_rest_notes'"
    );

    // Two fks to the same table is the 300 with the hint spellings,
    // and the hint resolves it.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_authors?select=name,zou_rest_msgs(id)",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::MULTIPLE_CHOICES);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "PGRST201");

    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_authors?select=name,zou_rest_msgs!sender(id)&name=eq.ann",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        body_text(res).await,
        r#"[{"name":"ann","zou_rest_msgs":[{"id": 100}]}]"#
    );

    // Grammar failures never reach the database.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_authors?name=zzz.1"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "PGRST100");
}

#[tokio::test]
async fn the_write_path_speaks_postgrest() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_rest_wr_books, zou_rest_wr_authors, zou_rest_wr_nopk cascade",
            "create table zou_rest_wr_authors (id int primary key, name text)",
            "create table zou_rest_wr_books (id int primary key, \
             author_id int references zou_rest_wr_authors(id), \
             title text, price int default 7)",
            "create table zou_rest_wr_nopk (id int, title text)",
            "insert into zou_rest_wr_authors values (1, 'ann'), (2, 'bob')",
        ],
    )
    .await;
    let app = app(&dsn);

    // A bulk insert defaults to return=minimal: 201, empty body, and
    // the column default fills what the payload left out.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_rest_wr_books",
            r#"[{"id": 1, "author_id": 1, "title": "t1"}, {"id": 2, "author_id": 2, "title": "t2"}]"#,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    assert!(res.headers().get("preference-applied").is_none());
    assert_eq!(body_text(res).await, "");

    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_wr_books?select=title,price&order=id",
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"title":"t1","price":7},{"title":"t2","price":7}]"#
    );

    // return=representation answers with the planned select over the
    // mutation CTE, embeds included, and echoes Preference-Applied.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_rest_wr_books?select=title,zou_rest_wr_authors(name)",
            r#"{"id": 3, "author_id": 1, "title": "c1"}"#,
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    assert_eq!(res.headers()["preference-applied"], "return=representation");
    assert_eq!(
        body_text(res).await,
        r#"[{"title":"c1","zou_rest_wr_authors":{"name": "ann"}}]"#
    );

    // return=headers-only points Location at the new row by its pk.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_rest_wr_books",
            r#"{"id": 42, "author_id": 2, "title": "h1"}"#,
            &["return=headers-only"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    // No mount prefix in it: PostgREST serves at the root and the
    // hosted edge takes /rest/v1 off before the request arrives, so
    // the header names the table and nothing else.
    assert_eq!(res.headers()["location"], "/zou_rest_wr_books?id=eq.42");
    assert_eq!(body_text(res).await, "");

    // merge-duplicates without on_conflict finds the pk by itself
    // and overwrites the clashing row in place. Nothing was created,
    // so the answer is 200 and not 201.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_rest_wr_books",
            r#"{"id": 2, "author_id": 2, "title": "t2x"}"#,
            &["resolution=merge-duplicates"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers()["preference-applied"],
        "resolution=merge-duplicates"
    );

    // One row of the batch was new, which is enough to call the whole
    // thing a creation.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_rest_wr_books",
            r#"[{"id": 2, "author_id": 2, "title": "t2x"}, {"id": 9, "author_id": 1, "title": "t9"}]"#,
            &["resolution=merge-duplicates", "return=headers-only"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_wr_books?select=title&id=eq.2"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"title":"t2x"}]"#);

    // ignore-duplicates with an explicit on_conflict drops the clash
    // and keeps the fresh row.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_rest_wr_books?on_conflict=id",
            r#"[{"id": 2, "author_id": 2, "title": "nope"}, {"id": 4, "author_id": 2, "title": "t4"}]"#,
            &["resolution=ignore-duplicates"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_wr_books?select=title&id=in.(2,4)&order=id",
        ))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"title":"t2x"},{"title":"t4"}]"#);

    // An upsert with no pk and no on_conflict has no target to name,
    // so there is nothing to resolve and it is a plain insert. The
    // preference is not refused and not applied either, which is the
    // one place a recognized token is left out of the answer.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_rest_wr_nopk",
            r#"{"id": 1, "title": "x"}"#,
            &["resolution=merge-duplicates"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    assert!(res.headers().get("preference-applied").is_none());

    // PATCH takes the root filters into its WHERE and representation
    // reads the touched rows back, 200 not 201.
    let res = app
        .clone()
        .oneshot(req(
            "PATCH",
            "/rest/v1/zou_rest_wr_books?id=eq.1&select=id,title",
            r#"{"title": "p1"}"#,
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"id":1,"title":"p1"}]"#);

    // A minimal PATCH is the bare 204.
    let res = app
        .clone()
        .oneshot(req(
            "PATCH",
            "/rest/v1/zou_rest_wr_books?id=eq.4",
            r#"{"price": 9}"#,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_wr_books?select=price&id=eq.4"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"price":9}]"#);

    // A PATCH with no keys is PostgREST's no-op answer.
    let res = app
        .clone()
        .oneshot(req(
            "PATCH",
            "/rest/v1/zou_rest_wr_books?id=eq.4",
            "{}",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    // DELETE with representation hands back the removed rows, then a
    // minimal DELETE finishes the cleanup with a 204.
    let res = app
        .clone()
        .oneshot(req(
            "DELETE",
            "/rest/v1/zou_rest_wr_books?id=eq.42&select=id",
            "",
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"id":42}]"#);

    let res = app
        .clone()
        .oneshot(req("DELETE", "/rest/v1/zou_rest_wr_books?id=eq.4", "", &[]))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_wr_books?select=id&id=in.(4,42)"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, "[]");

    // A body that is not json never reaches the database.
    let res = app
        .clone()
        .oneshot(req("POST", "/rest/v1/zou_rest_wr_books", "not json", &[]))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "PGRST102");
}

#[tokio::test]
async fn a_put_replaces_the_row_the_url_names() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_rest_put, zou_rest_put_pair, zou_rest_put_nopk cascade",
            "create table zou_rest_put (name text primary key, rank int)",
            "create table zou_rest_put_pair \
             (first text, last text, salary int, primary key (first, last))",
            "create table zou_rest_put_nopk (a text, b text)",
            "insert into zou_rest_put values ('java', 13)",
            "insert into zou_rest_put_pair values ('frances', 'roe', 60000)",
        ],
    )
    .await;
    let app = app(&dsn);

    // A row the table does not have yet is a creation, and the same
    // url a second time is not.
    let res = app
        .clone()
        .oneshot(req(
            "PUT",
            "/rest/v1/zou_rest_put?name=eq.go",
            r#"[{"name":"go","rank":19}]"#,
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    assert_eq!(body_text(res).await, r#"[{"name":"go","rank":19}]"#);

    let res = app
        .clone()
        .oneshot(req(
            "PUT",
            "/rest/v1/zou_rest_put?name=eq.go",
            r#"[{"name":"go","rank":20}]"#,
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"name":"go","rank":20}]"#);

    // No Prefer at all is the bare 204, and it carries no window,
    // because the url named the row rather than a range of them.
    let res = app
        .clone()
        .oneshot(req(
            "PUT",
            "/rest/v1/zou_rest_put?name=eq.java",
            r#"[{"name": "java", "rank": 1}]"#,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert!(res.headers().get("content-range").is_none());
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_put?select=rank&name=eq.java"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"rank":1}]"#);

    // Every element the url does not name is filtered out of the
    // payload, so a body of two rows puts the one that matches.
    let res = app
        .clone()
        .oneshot(req(
            "PUT",
            "/rest/v1/zou_rest_put?name=eq.java",
            r#"[{"name": "java", "rank": 2}, {"name": "swift", "rank": 12}]"#,
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"name":"java","rank":2}]"#);
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_put?select=name&name=eq.swift"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, "[]");

    // A composite key needs all of its columns, and either half of
    // it alone names a set rather than a row.
    let res = app
        .clone()
        .oneshot(req(
            "PUT",
            "/rest/v1/zou_rest_put_pair?first=eq.frances&last=eq.roe",
            r#"[{"first": "frances", "last": "roe", "salary": 70000}]"#,
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        body_text(res).await,
        r#"[{"first":"frances","last":"roe","salary":70000}]"#
    );

    for uri in [
        "/rest/v1/zou_rest_put_pair?first=eq.frances",
        "/rest/v1/zou_rest_put?rank=eq.19",
        "/rest/v1/zou_rest_put?name=not.eq.go",
        "/rest/v1/zou_rest_put?name=in.(go)",
        "/rest/v1/zou_rest_put?and=(name.eq.go)",
        "/rest/v1/zou_rest_put",
        "/rest/v1/zou_rest_put_nopk?a=eq.one&b=eq.two",
    ] {
        let res = app
            .clone()
            .oneshot(req("PUT", uri, r#"[{"a": "one", "b": "two"}]"#, &[]))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::METHOD_NOT_ALLOWED, "{uri}");
        let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
        assert_eq!(body["code"], "PGRST105", "{uri}");
    }

    // Paging a url that names one row is a contradiction, and it is
    // refused before the key is even looked up.
    for uri in [
        "/rest/v1/zou_rest_put?name=eq.go&limit=1",
        "/rest/v1/zou_rest_put?name=eq.go&offset=1",
    ] {
        let res = app
            .clone()
            .oneshot(req("PUT", uri, r#"[{"name": "go"}]"#, &[]))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST, "{uri}");
        let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
        assert_eq!(body["code"], "PGRST114", "{uri}");
    }

    // A body whose key is not the url's key writes nothing, and the
    // nothing is what the mismatch is read off.
    let res = app
        .clone()
        .oneshot(req(
            "PUT",
            "/rest/v1/zou_rest_put?name=eq.rust",
            r#"[{"name": "perl", "rank": 17}]"#,
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "PGRST115");

    // And the refusal rolled back, so the earlier rows are the only
    // rows and perl never landed.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_put?select=name&order=name"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"name":"go"},{"name":"java"}]"#);
}

#[tokio::test]
async fn a_write_reads_its_body_the_way_postgrest_reads_it() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_rest_body cascade",
            "create table zou_rest_body (id int primary key, name text, rank int default 7)",
            "insert into zou_rest_body values (1, 'java', 13), (2, 'go', 19)",
        ],
    )
    .await;
    let app = app(&dsn);

    // Whatever the parser thought of it, a body that is not json is
    // one sentence.
    for body in ["", "}{ x = 2", "{ name: \"go\" }"] {
        let res = app
            .clone()
            .oneshot(req("PATCH", "/rest/v1/zou_rest_body?id=eq.1", body, &[]))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST, "{body:?}");
        let e: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
        assert_eq!(e["code"], "PGRST102", "{body:?}");
        assert_eq!(e["message"], "Empty or invalid json", "{body:?}");
    }

    // An array of rows that disagree about their keys is refused
    // before it reaches postgres, because one column list unpacks
    // all of them and a row with other keys would lose them.
    for body in [
        r#"[{"id": 8, "name": "perl"}, {"id": 9}]"#,
        r#"[{"id": 8}, {"id": 9, "name": "perl"}]"#,
        "[1, 2]",
    ] {
        let res = app
            .clone()
            .oneshot(req("POST", "/rest/v1/zou_rest_body", body, &[]))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST, "{body:?}");
        let e: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
        assert_eq!(e["message"], "All object keys must match", "{body:?}");
    }
    // Order is not disagreement, and ?columns= says the list
    // outright, which stops the body being read at all.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_rest_body",
            r#"[{"id": 8, "name": "perl"}, {"name": "raku", "id": 9}]"#,
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_rest_body?columns=id",
            r#"[{"id": 10, "name": "perl"}, {"id": 11}]"#,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_body?select=name&id=eq.10"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"name":null}]"#);

    // An update writes one row, so an array is read down to its
    // first element rather than refused, and ?columns= narrows what
    // that element gets to set.
    let res = app
        .clone()
        .oneshot(req(
            "PATCH",
            "/rest/v1/zou_rest_body?id=eq.1",
            r#"[{"rank": 41}, {"rank": 42}]"#,
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-range"], "0-0/*");
    assert_eq!(
        body_text(res).await,
        r#"[{"id":1,"name":"java","rank":41}]"#
    );

    let res = app
        .clone()
        .oneshot(req(
            "PATCH",
            "/rest/v1/zou_rest_body?id=eq.1&columns=rank",
            r#"{"rank": 43, "name": "not this"}"#,
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        body_text(res).await,
        r#"[{"id":1,"name":"java","rank":43}]"#
    );

    // A body with nothing in it to set is not an error and not a
    // no-op either: it is an update that matches no row, whatever
    // the url said, and it says so in the window.
    for body in ["{}", "[]", "[{}]", "42"] {
        let res = app
            .clone()
            .oneshot(req("PATCH", "/rest/v1/zou_rest_body", body, &[]))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT, "{body:?}");
        assert_eq!(res.headers()["content-range"], "*/*", "{body:?}");
    }
    let res = app
        .clone()
        .oneshot(req(
            "PATCH",
            "/rest/v1/zou_rest_body",
            "{}",
            &["return=representation", "count=exact"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-range"], "*/0");
    assert_eq!(body_text(res).await, "[]");

    // And nothing of it landed: the rows are the ones the seed and
    // the inserts above left.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_body?select=id,rank&order=id"))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"id":1,"rank":43},{"id":2,"rank":19},{"id":8,"rank":7},{"id":9,"rank":7},{"id":10,"rank":7},{"id":11,"rank":7}]"#
    );
}

#[tokio::test]
async fn the_rpc_surface_speaks_postgrest() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop function if exists zou_rpc_add(int, int), zou_rpc_evens(int), \
             zou_rpc_shout(text), zou_rpc_touch(), zou_rpc_echo(jsonb), \
             zou_rpc_sum(int[]), zou_rpc_over(int), zou_rpc_over(int, int), \
             zou_rpc_list(int), zou_rpc_pair(int), zou_rpc_one()",
            "drop table if exists zou_rpc_items, zou_rpc_owners cascade",
            "create table zou_rpc_owners (id int primary key, name text)",
            "create table zou_rpc_items (id int primary key, \
             owner_id int references zou_rpc_owners(id), name text)",
            "insert into zou_rpc_owners values (1, 'ann'), (2, 'bob')",
            "insert into zou_rpc_items values (1, 1, 'hammer'), (2, 1, 'saw'), (3, 2, 'drill')",
            "create function zou_rpc_add(a int, b int) returns int \
             language sql immutable as 'select a + b'",
            "create function zou_rpc_evens(top int) returns setof int \
             language sql immutable as \
             'select n from generate_series(0, top) n where n % 2 = 0'",
            "create function zou_rpc_shout(msg text) returns text \
             language sql immutable as 'select upper(msg)'",
            "create function zou_rpc_touch() returns void \
             language sql as 'insert into zou_rpc_items values (99, 1, ''tmp'')'",
            "create function zou_rpc_echo(jsonb) returns jsonb \
             language sql immutable as 'select $1'",
            "create function zou_rpc_sum(variadic nums int[]) returns int \
             language sql immutable as 'select sum(n)::int from unnest(nums) n'",
            "create function zou_rpc_over(a int) returns int \
             language sql immutable as 'select a'",
            "create function zou_rpc_over(a int, b int default 1) returns int \
             language sql immutable as 'select a + b'",
            "create function zou_rpc_list(min_id int default 0) \
             returns setof zou_rpc_items language sql stable as \
             'select * from zou_rpc_items where id >= min_id'",
            "create function zou_rpc_pair(a int) returns table(x int, y int) \
             language sql immutable as 'select a, a * 2'",
            "create function zou_rpc_one() returns zou_rpc_items \
             language sql stable as 'select * from zou_rpc_items where id = 1'",
        ],
    )
    .await;
    let app = app(&dsn);

    // A scalar via GET is the bare json value, arguments cast from
    // the query string with percent decoding applied.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_rpc_add?a=2&b=3"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    // One value is one row, and upstream says so in the range.
    assert_eq!(res.headers()["content-range"], "0-0/*");
    assert_eq!(body_text(res).await, "5");

    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/rpc/zou_rpc_add",
            r#"{"a": 2, "b": 40}"#,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, "42");

    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_rpc_shout?msg=hello+world"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#""HELLO WORLD""#);

    // A set of scalars folds to a json array of bare values.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_rpc_evens?top=4"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, "[0, 2, 4]");

    // A set of table rows takes the whole read grammar: the arg
    // binds, the rest of the query string filters and embeds on the
    // result, and Content-Range reports like a read.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/rpc/zou_rpc_list?min_id=2&id=gt.2&select=name,zou_rpc_owners(name)",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-range"], "0-0/*");
    assert_eq!(
        body_text(res).await,
        r#"[{"name":"drill","zou_rpc_owners":{"name": "bob"}}]"#
    );

    // The same function over POST: body args, query string grammar,
    // and the defaulted argument fills itself.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/rpc/zou_rpc_list?select=id&order=id",
            "{}",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"id":1},{"id":2},{"id":3}]"#);

    // returns table(...) rows come out as objects.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_rpc_pair?a=21"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"x":21,"y":42}]"#);

    // A non set function returning a rowtype is one bare object.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_rpc_one?select=id,name"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"{"id":1,"name":"hammer"}"#);

    // A variadic gathers repeated query params, or a json array.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_rpc_sum?nums=1&nums=2&nums=3"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, "6");

    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/rpc/zou_rpc_sum",
            r#"{"nums": [1, 2, 3]}"#,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, "6");

    // A single unnamed json parameter takes the whole body.
    let res = app
        .clone()
        .oneshot(req("POST", "/rest/v1/rpc/zou_rpc_echo", r#"{"k": 1}"#, &[]))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"{"k": 1}"#);

    // GET runs read only, so a function that writes fails with pg's
    // 25006 at PostgREST's 405; POST runs it for real.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_rpc_touch"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::METHOD_NOT_ALLOWED);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "25006");

    let res = app
        .clone()
        .oneshot(req("POST", "/rest/v1/rpc/zou_rpc_touch", "", &[]))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    // Nothing to send back and a range all the same.
    assert_eq!(res.headers()["content-range"], "0-0/*");
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rpc_items?select=name&id=eq.99"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"name":"tmp"}]"#);

    // Overloads a call shape cannot split are PGRST203 at 300.
    let res = app
        .clone()
        .oneshot(req("POST", "/rest/v1/rpc/zou_rpc_over", r#"{"a": 1}"#, &[]))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::MULTIPLE_CHOICES);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "PGRST203");

    // A missing function is PGRST202 spelling the attempted call.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_rpc_nope?x=1"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "PGRST202");
    assert_eq!(
        body["message"],
        "Could not find the function public.zou_rpc_nope(x) in the schema cache"
    );
}

/// A call reports its window the way a read does: every answer
/// carries a Content-Range, a total arrives only when count= asked
/// for one, and a page smaller than the total is a 206. The Range
/// header is read on a GET and on nothing else, which is upstream
/// reading the method rather than the shape of the request.
/// The two settings a function can leave behind about the response
/// it wants: `response.status` and `response.headers`. They are read
/// back after the call, on the commit, and amend the response the
/// handler already built.
#[tokio::test]
async fn a_function_says_what_it_wants_the_response_to_be() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop function if exists zou_guc_status(), zou_guc_headers(), \
             zou_guc_bad_status(), zou_guc_bad_headers(), zou_guc_quiet()",
            "drop table if exists zou_guc_rows cascade",
            "create table zou_guc_rows (id int primary key)",
            "insert into zou_guc_rows values (1)",
            "create function zou_guc_status() returns setof zou_guc_rows \
             language sql volatile as $$ \
             select set_config('response.status', '205', true); \
             select * from zou_guc_rows $$",
            "create function zou_guc_headers() returns setof zou_guc_rows \
             language sql volatile as $$ \
             select set_config('response.headers', \
               '[{\"Location\": \"/elsewhere\"}, {\"X-Two\": \"a\"}, {\"X-Two\": \"b\"}]', true); \
             select * from zou_guc_rows $$",
            "create function zou_guc_bad_status() returns setof zou_guc_rows \
             language sql volatile as $$ \
             select set_config('response.status', 'unknown', true); \
             select * from zou_guc_rows $$",
            "create function zou_guc_bad_headers() returns setof zou_guc_rows \
             language sql volatile as $$ \
             select set_config('response.headers', '{\"X-One\": \"a\"}', true); \
             select * from zou_guc_rows $$",
            "create function zou_guc_quiet() returns setof zou_guc_rows \
             language sql stable as 'select * from zou_guc_rows'",
        ],
    )
    .await;
    let app = app(&dsn);

    let res = app
        .clone()
        .oneshot(req("POST", "/rest/v1/rpc/zou_guc_status", "{}", &[]))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::RESET_CONTENT);
    // A status of its own costs the response nothing else it had.
    assert_eq!(res.headers()["content-range"], "0-0/*");

    let res = app
        .clone()
        .oneshot(req("POST", "/rest/v1/rpc/zou_guc_headers", "{}", &[]))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["location"], "/elsewhere");
    let two: Vec<&str> = res
        .headers()
        .get_all("x-two")
        .iter()
        .map(|v| v.to_str().unwrap())
        .collect();
    assert_eq!(two, vec!["a", "b"]);

    // Both errors are a 500, and the work the function did still
    // stands, because upstream builds the response after the
    // transaction has landed.
    for (func, code) in [
        ("zou_guc_bad_status", "PGRST112"),
        ("zou_guc_bad_headers", "PGRST111"),
    ] {
        let res = app
            .clone()
            .oneshot(req("POST", &format!("/rest/v1/rpc/{func}"), "{}", &[]))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR, "{func}");
        assert!(res.headers().get("content-range").is_none(), "{func}");
        let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
        assert_eq!(body["code"], code, "{func}");
    }

    // A setting one call made is gone by the next, since it lived in
    // that call's transaction and the connection is scrubbed on its
    // way back to the pool.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_guc_quiet"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(res.headers().get("location").is_none());
}

#[tokio::test]
async fn a_call_answers_with_the_range_a_read_would() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop function if exists zou_range_list(int), zou_range_evens(int), \
             zou_range_one(), zou_range_none()",
            "drop table if exists zou_range_items cascade",
            "create table zou_range_items (id int primary key, name text)",
            "insert into zou_range_items values (1, 'a'), (2, 'b'), (3, 'c')",
            "create function zou_range_list(min_id int default 0) \
             returns setof zou_range_items language sql stable as \
             'select * from zou_range_items where id >= min_id order by id'",
            "create function zou_range_evens(top int) returns setof int \
             language sql immutable as \
             'select n from generate_series(0, top) n where n % 2 = 0'",
            "create function zou_range_one() returns zou_range_items \
             language sql stable as 'select * from zou_range_items where id = 1'",
            "create function zou_range_none() returns setof zou_range_items \
             language sql stable as 'select * from zou_range_items where false'",
        ],
    )
    .await;
    let app = app(&dsn);

    // The whole set, no total asked for.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_range_list?select=id"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-range"], "0-2/*");

    // A page of a known total is a 206, and the total counts the
    // rows the call returned rather than the ones it handed back.
    let res = app
        .clone()
        .oneshot(req(
            "GET",
            "/rest/v1/rpc/zou_range_list?select=id&limit=1&offset=1",
            "",
            &["count=exact"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(res.headers()["content-range"], "1-1/3");
    assert_eq!(body_text(res).await, r#"[{"id":2}]"#);

    // A window that starts past the end is the read path's 416, and
    // it needs the total to know that, so it needs the count to
    // survive a page with no rows in it.
    let res = app
        .clone()
        .oneshot(req(
            "GET",
            "/rest/v1/rpc/zou_range_list?select=id&offset=100",
            "",
            &["count=exact"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(res.headers()["content-range"], "*/3");
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "PGRST103");

    // The Range header pages a GET.
    let ranged = Request::builder()
        .uri("/rest/v1/rpc/zou_range_list?select=id")
        .header("apikey", anon_key())
        .header("range", "1-1")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(ranged).await.unwrap();
    assert_eq!(res.headers()["content-range"], "1-1/*");
    assert_eq!(body_text(res).await, r#"[{"id":2}]"#);

    // The same header on a POST is not a page at all.
    let posted = Request::builder()
        .method("POST")
        .uri("/rest/v1/rpc/zou_range_list?select=id")
        .header("apikey", anon_key())
        .header("range", "1-1")
        .body(Body::from("{}"))
        .unwrap();
    let res = app.clone().oneshot(posted).await.unwrap();
    assert_eq!(res.headers()["content-range"], "0-2/*");

    // A folded set of scalars counts what it folded, and an empty
    // one has no window to report.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_range_evens?top=4"))
        .await
        .unwrap();
    assert_eq!(res.headers()["content-range"], "0-2/*");
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_range_none"))
        .await
        .unwrap();
    assert_eq!(res.headers()["content-range"], "*/*");

    // A function that returns one row is one row, and an exact
    // total of one is what upstream sends for it.
    let res = app
        .clone()
        .oneshot(req(
            "GET",
            "/rest/v1/rpc/zou_range_one?select=id",
            "",
            &["count=exact"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-range"], "0-0/1");
    assert_eq!(body_text(res).await, r#"{"id":1}"#);
}

/// What a call hands back decides how it is asked. A row is
/// expanded in the from clause so the select grammar has columns to
/// work on, and a value is selected as a value, which is the only
/// way to ask for a `record`. A domain is neither: it is whatever
/// it was declared over.
#[tokio::test]
async fn a_call_is_asked_the_way_its_return_type_reads() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop function if exists zou_ret_point(), zou_ret_domain(), \
             zou_ret_record(), zou_ret_records(), zou_ret_rows(int), \
             zou_ret_inout(int), zou_ret_wide()",
            "drop domain if exists zou_point_domain",
            "drop domain if exists zou_rows_domain",
            "drop type if exists zou_point",
            "drop table if exists zou_ret_items cascade",
            "create type zou_point as (x int, y int)",
            "create domain zou_point_domain as zou_point",
            "create table zou_ret_items (id int primary key, name text)",
            "insert into zou_ret_items values (1, 'a'), (2, 'b')",
            "create domain zou_rows_domain as zou_ret_items",
            "create function zou_ret_point() returns zou_point \
             language sql immutable as 'select row(10, 5)::zou_point'",
            "create function zou_ret_domain() returns zou_point_domain \
             language sql immutable as 'select row(10, 5)::zou_point_domain'",
            "create function zou_ret_record() returns record \
             language sql stable as 'select * from zou_ret_items where id = 1'",
            "create function zou_ret_records() returns setof record \
             language sql stable as 'select * from zou_ret_items order by id'",
            "create function zou_ret_rows(min_id int) returns setof zou_rows_domain \
             language sql stable as \
             'select i::zou_rows_domain from zou_ret_items i \
              where i.id >= min_id order by i.id'",
            "create function zou_ret_inout(inout num int) \
             language sql immutable as 'select num + 1'",
        ],
    )
    .await;
    let app = app(&dsn);

    // A composite type nobody can embed on is still a row, and one
    // row is an object rather than a list of one.
    for name in ["zou_ret_point", "zou_ret_domain"] {
        let res = app
            .clone()
            .oneshot(get(&format!("/rest/v1/rpc/{name}")))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "{name}");
        assert_eq!(res.headers()["content-range"], "0-0/*");
        assert_eq!(body_text(res).await, r#"{"x":10,"y":5}"#, "{name}");
    }

    // A record has no columns to expand, which is what postgres
    // means by "a column definition list is required". It travels
    // as a value and writes itself out as an object all the same.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_ret_record"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"{"id":1,"name":"a"}"#);

    // A set of them is as many rows as the set has.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_ret_records"))
        .await
        .unwrap();
    assert_eq!(res.headers()["content-range"], "0-1/*");
    assert_eq!(
        body_text(res).await,
        r#"[{"id":1,"name":"a"}, {"id":2,"name":"b"}]"#
    );

    // A domain over a table's rowtype is that table, so the select
    // grammar applies and the columns are the table's own.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_ret_rows?min_id=2&select=name"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"name":"b"}]"#);

    // One INOUT argument reads as a scalar from the return type
    // alone, and postgres names the output column after it, so the
    // answer is an object.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_ret_inout?num=2"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"{"num":3}"#);
}

#[tokio::test]
async fn a_filter_takes_the_strings_upstream_takes() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_filter_rows cascade",
            "create table zou_filter_rows (\
             id int primary key, arr int[], done bool, and_col text, or_col text)",
            "insert into zou_filter_rows values \
             (1, '{1,2}', true, 'x', 'y'), \
             (2, '{1,2,3}', false, 'x', 'y'), \
             (3, '{9}', null, 'x', 'y')",
        ],
    )
    .await;
    let app = app(&dsn);

    // Spaces go around the brackets and the commas of a tree, and a
    // name gives up the ones on its ends.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_filter_rows?select=id&and=(%20or%20(%20id.eq.1%20,%20id.eq.3%20)%20,%20id.in.(%201,%203%20)%20)",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"id":1},{"id":3}]"#);

    // The commas inside an array literal are the array's, not the
    // tree's, and a negation still reaches the condition behind one.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_filter_rows?select=id&and=(arr.cs.%7B1,2%7D,arr.not.cd.%7B1,2%7D)",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"id":2}]"#);

    // A column named after a keyword is a column, since only the
    // bracket makes the word a group.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_filter_rows?select=id&or=(and_col.eq.z,%20or_col.eq.y)&limit=1",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"id":1}]"#);

    // is takes not_null in any case, and the trileans keep working.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_filter_rows?select=id&done=is.NoT_NuLl"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"id":1},{"id":2}]"#);

    // An in list with nothing in it matches nothing, and negated it
    // matches everything, which is what the empty array does.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_filter_rows?select=id&id=in.(%20%20)"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, "[]");

    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_filter_rows?select=id&id=not.in.()"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"id":1},{"id":2},{"id":3}]"#);

    // A bracket too many is a bracket nobody looks at.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_filter_rows?select=id&and=(id.eq.1,id.neq.2))",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"id":1}]"#);
}

#[tokio::test]
async fn an_rls_write_denial_comes_back_as_401() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_rest_wr_notes cascade",
            "create table zou_rest_wr_notes (id int primary key, body text)",
            "alter table zou_rest_wr_notes enable row level security",
        ],
    )
    .await;
    let app = app(&dsn);

    // RLS is on and no insert policy exists, so the anon write is
    // refused by postgres and surfaces as PostgREST's 401.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_rest_wr_notes",
            r#"{"id": 1, "body": "x"}"#,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "42501");
}

#[tokio::test]
async fn the_count_preferences_speak_postgrest() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_cnt_items, zou_cnt_owners cascade",
            "create table zou_cnt_owners (id int primary key, name text)",
            "create table zou_cnt_items (id int primary key, \
             owner_id int references zou_cnt_owners(id), name text)",
            "insert into zou_cnt_owners values (1, 'ann'), (2, 'bob')",
            "insert into zou_cnt_items values \
             (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c'), (4, 1, 'd'), \
             (5, 2, 'e'), (6, 2, 'f'), (7, 2, 'g')",
            "analyze zou_cnt_items",
        ],
    )
    .await;
    let app = app(&dsn);

    // A paged exact count: the total lands in Content-Range, the
    // partial window is a 206, and the preference echoes back.
    let res = app
        .clone()
        .oneshot(req(
            "GET",
            "/rest/v1/zou_cnt_items?select=id&order=id&limit=2",
            "",
            &["count=exact"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(res.headers()["content-range"], "0-1/7");
    assert_eq!(res.headers()["preference-applied"], "count=exact");
    assert_eq!(body_text(res).await, r#"[{"id":1},{"id":2}]"#);

    // Unpaged with the whole table served is a 200 over the full
    // window.
    let res = app
        .clone()
        .oneshot(req(
            "GET",
            "/rest/v1/zou_cnt_items?select=id",
            "",
            &["count=exact"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-range"], "0-6/7");

    // The count query sees the filters and an inner embed's
    // narrowing, not just the raw table.
    let res = app
        .clone()
        .oneshot(req(
            "GET",
            "/rest/v1/zou_cnt_items?select=id,zou_cnt_owners!inner(name)&zou_cnt_owners.name=eq.ann&id=lte.5&order=id&limit=1",
            "",
            &["count=exact"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(res.headers()["content-range"], "0-0/4");

    // A window past the end is PostgREST's 416 with the PGRST103
    // body, the total still in the header.
    let res = app
        .clone()
        .oneshot(req(
            "GET",
            "/rest/v1/zou_cnt_items?select=id&offset=10&limit=2",
            "",
            &["count=exact"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(res.headers()["content-range"], "*/7");
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "PGRST103");
    assert_eq!(body["message"], "Requested range not satisfiable");
    assert_eq!(
        body["details"],
        "An offset of 10 was requested, but there are only 7 rows."
    );

    // An empty filtered read with a count is 200 over */0.
    let res = app
        .clone()
        .oneshot(req(
            "GET",
            "/rest/v1/zou_cnt_items?select=id&id=gt.100",
            "",
            &["count=exact"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-range"], "*/0");

    // Planned reads the analyzed estimate, estimated agrees with
    // exact on a small table; both still page as 206.
    let res = app
        .clone()
        .oneshot(req(
            "GET",
            "/rest/v1/zou_cnt_items?select=id&order=id&limit=1",
            "",
            &["count=planned"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(res.headers()["content-range"], "0-0/7");
    assert_eq!(res.headers()["preference-applied"], "count=planned");

    let res = app
        .clone()
        .oneshot(req(
            "GET",
            "/rest/v1/zou_cnt_items?select=id&order=id&limit=2",
            "",
            &["count=estimated"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(res.headers()["content-range"], "0-1/7");

    // Writes carry Content-Range too: inserts and deletes collapse
    // the window, an update shows the rows it touched, and the total
    // only lands when count= asked for it.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_cnt_items",
            r#"{"id": 8, "owner_id": 2, "name": "h"}"#,
            &["count=exact"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    assert_eq!(res.headers()["content-range"], "*/1");

    let res = app
        .clone()
        .oneshot(req(
            "PATCH",
            "/rest/v1/zou_cnt_items?owner_id=eq.2",
            r#"{"name": "z"}"#,
            &["count=exact"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(res.headers()["content-range"], "0-3/4");

    let res = app
        .clone()
        .oneshot(req("DELETE", "/rest/v1/zou_cnt_items?id=eq.8", "", &[]))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(res.headers()["content-range"], "*/*");
}

#[tokio::test]
async fn the_response_modes_speak_postgrest() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_media_items cascade",
            "create table zou_media_items (id int primary key, name text, note text)",
            "insert into zou_media_items values \
             (1, 'ann', null), (2, 'bob', 'hi'), (3, 'a,b', null)",
        ],
    )
    .await;
    let app = app(&dsn);

    let accepting = |method: &str, uri: &str, body: &str, accept: &str, prefers: &[&str]| {
        let mut b = Request::builder()
            .method(method)
            .uri(uri)
            .header("apikey", anon_key())
            .header("accept", accept);
        for p in prefers {
            b = b.header("prefer", *p);
        }
        let body = if body.is_empty() {
            Body::empty()
        } else {
            Body::from(body.to_string())
        };
        b.body(body).unwrap()
    };

    // Csv the way PostgREST builds it in SQL: the header from the
    // first row's keys, each line the record text with the parens
    // shaved, nulls empty and commas quoted by postgres itself.
    let res = app
        .clone()
        .oneshot(accepting(
            "GET",
            "/rest/v1/zou_media_items?order=id.asc",
            "",
            "text/csv",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-type"], "text/csv; charset=utf-8");
    assert_eq!(
        body_text(res).await,
        "id,name,note\n1,ann,\n2,bob,hi\n3,\"a,b\","
    );

    // The header follows the select list, and an empty result is a
    // lone newline, upstream's own shape.
    let res = app
        .clone()
        .oneshot(accepting(
            "GET",
            "/rest/v1/zou_media_items?select=name&id=eq.2",
            "",
            "text/csv",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, "name\nbob");

    let res = app
        .clone()
        .oneshot(accepting(
            "GET",
            "/rest/v1/zou_media_items?id=gt.100",
            "",
            "text/csv",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, "\n");

    // A singular read hands back the bare object, and anything but
    // exactly one row is the PGRST116 406.
    let res = app
        .clone()
        .oneshot(accepting(
            "GET",
            "/rest/v1/zou_media_items?id=eq.1",
            "",
            "application/vnd.pgrst.object+json",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers()["content-type"],
        "application/vnd.pgrst.object+json; charset=utf-8"
    );
    assert_eq!(body_text(res).await, r#"{"id":1,"name":"ann","note":null}"#);

    let res = app
        .clone()
        .oneshot(accepting(
            "GET",
            "/rest/v1/zou_media_items?id=lte.2",
            "",
            "application/vnd.pgrst.object+json",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "PGRST116");
    assert_eq!(
        body["message"],
        "Cannot coerce the result to a single JSON object"
    );
    assert_eq!(body["details"], "The result contains 2 rows");

    let res = app
        .clone()
        .oneshot(accepting(
            "GET",
            "/rest/v1/zou_media_items?id=eq.999",
            "",
            "application/vnd.pgrst.object+json",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["details"], "The result contains 0 rows");

    // nulls=stripped drops the null fields; the plain vendored
    // array name folds down to plain json, the stripped one keeps
    // its own content type.
    let res = app
        .clone()
        .oneshot(accepting(
            "GET",
            "/rest/v1/zou_media_items?id=eq.1",
            "",
            "application/vnd.pgrst.object+json;nulls=stripped",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"{"id":1,"name":"ann"}"#);

    let res = app
        .clone()
        .oneshot(accepting(
            "GET",
            "/rest/v1/zou_media_items?order=id.asc",
            "",
            "application/vnd.pgrst.array+json",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(
        res.headers()["content-type"],
        "application/json; charset=utf-8"
    );

    let res = app
        .clone()
        .oneshot(accepting(
            "GET",
            "/rest/v1/zou_media_items?select=id,note&order=id.asc",
            "",
            "application/vnd.pgrst.array+json;nulls=stripped",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(
        res.headers()["content-type"],
        "application/vnd.pgrst.array+json;nulls=stripped; charset=utf-8"
    );
    assert_eq!(
        body_text(res).await,
        r#"[{"id":1},{"id":2,"note":"hi"},{"id":3}]"#
    );

    // An Accept nothing can produce is the PGRST107 406.
    let res = app
        .clone()
        .oneshot(accepting(
            "GET",
            "/rest/v1/zou_media_items",
            "",
            "text/html",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "PGRST107");
    assert_eq!(
        body["message"],
        "None of these media types are available: text/html"
    );

    // Writes negotiate too: a representation can come back as csv
    // or as the bare object.
    let res = app
        .clone()
        .oneshot(accepting(
            "POST",
            "/rest/v1/zou_media_items",
            r#"{"id": 9, "name": "zed"}"#,
            "text/csv",
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    assert_eq!(res.headers()["content-type"], "text/csv; charset=utf-8");
    assert_eq!(body_text(res).await, "id,name,note\n9,zed,");

    let res = app
        .clone()
        .oneshot(accepting(
            "POST",
            "/rest/v1/zou_media_items",
            r#"{"id": 10, "name": "yin"}"#,
            "application/vnd.pgrst.object+json",
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    assert_eq!(
        body_text(res).await,
        r#"{"id":10,"name":"yin","note":null}"#
    );

    // A singular write that lands on the wrong row count is refused
    // and rolled back, PostgREST's condemned transaction.
    let res = app
        .clone()
        .oneshot(accepting(
            "POST",
            "/rest/v1/zou_media_items",
            r#"[{"id": 20, "name": "x"}, {"id": 21, "name": "y"}]"#,
            "application/vnd.pgrst.object+json",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["details"], "The result contains 2 rows");
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_media_items?select=id&id=gte.20"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, "[]", "the refused insert rolled back");

    let res = app
        .clone()
        .oneshot(accepting(
            "PATCH",
            "/rest/v1/zou_media_items?id=lte.2",
            r#"{"note": "clobbered"}"#,
            "application/vnd.pgrst.object+json",
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_media_items?select=note&id=eq.2"))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"note":"hi"}]"#,
        "the refused update rolled back"
    );

    // Csv on an update shows the touched row, and a delete that
    // touches nothing under a singular accept is the 0 rows 406.
    let res = app
        .clone()
        .oneshot(accepting(
            "PATCH",
            "/rest/v1/zou_media_items?id=eq.2",
            r#"{"note": "yo"}"#,
            "text/csv",
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-range"], "0-0/*");
    assert_eq!(body_text(res).await, "id,name,note\n2,bob,yo");

    let res = app
        .clone()
        .oneshot(accepting(
            "DELETE",
            "/rest/v1/zou_media_items?id=eq.999",
            "",
            "application/vnd.pgrst.object+json",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "PGRST116");
    assert_eq!(body["details"], "The result contains 0 rows");
}

#[tokio::test]
async fn the_context_and_rls_hold_through_the_whole_door() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop view if exists zou_rest_ctx",
            "drop table if exists zou_rest_notes cascade",
            "create view zou_rest_ctx as select \
             current_setting('request.method', true) as method, \
             current_setting('request.path', true) as path, \
             current_setting('request.headers', true)::jsonb->>'x-probe' as probe, \
             current_setting('request.cookies', true)::jsonb->>'session' as cookie, \
             current_setting('request.jwt.claims', true)::jsonb->>'sub' as sub, \
             current_user::text as who",
            "create table zou_rest_notes (id int primary key, owner text, body text)",
            "alter table zou_rest_notes enable row level security",
            "create policy zou_rest_notes_own on zou_rest_notes for select \
             using (owner = coalesce(auth.uid()::text, 'nobody'))",
            "insert into zou_rest_notes values \
             (1, '11111111-1111-1111-1111-111111111111', 'mine'), \
             (2, '22222222-2222-2222-2222-222222222222', 'theirs')",
        ],
    )
    .await;
    let app = app(&dsn);

    // All six injected settings are visible to SQL on the exact
    // request that carried them.
    let bearer = jwt::mint(
        &serde_json::json!({
            "role": "authenticated",
            "sub": "11111111-1111-1111-1111-111111111111",
        }),
        SECRET,
    );
    let mut req = get("/rest/v1/zou_rest_ctx");
    req.headers_mut()
        .insert("authorization", format!("Bearer {bearer}").parse().unwrap());
    req.headers_mut()
        .insert("x-probe", "hello".parse().unwrap());
    req.headers_mut()
        .insert("cookie", "session=s1; theme=dark".parse().unwrap());
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let rows: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    let row = &rows[0];
    assert_eq!(row["method"], "GET");
    // request.path is what PostgREST would have seen, the path with
    // the mount prefix off, which is what a function reading the
    // setting to route on is expecting.
    assert_eq!(row["path"], "/zou_rest_ctx");
    assert_eq!(row["probe"], "hello");
    assert_eq!(row["cookie"], "s1");
    assert_eq!(row["sub"], "11111111-1111-1111-1111-111111111111");
    assert_eq!(row["who"], "authenticated");

    // The RLS policy sees auth.uid(): the bearer's user reads their
    // row and nobody else's, anon reads nothing.
    let mut req = get("/rest/v1/zou_rest_notes?select=body");
    req.headers_mut()
        .insert("authorization", format!("Bearer {bearer}").parse().unwrap());
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(body_text(res).await, r#"[{"body":"mine"}]"#);

    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_notes?select=body"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, "[]");
}

#[tokio::test]
async fn the_schema_profiles_speak_postgrest() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop schema if exists zou_alt cascade",
            "drop table if exists zou_profile_rows cascade",
            "create schema zou_alt",
            // The bootstrap only grants public and auth, so an extra
            // exposed schema is the deployment's own grant to make.
            "grant usage on schema zou_alt to anon, authenticated, service_role",
            "create table zou_alt.zou_profile_rows (id int primary key, tag text)",
            "insert into zou_alt.zou_profile_rows values (1, 'alt')",
            "grant all on all tables in schema zou_alt to anon, authenticated, service_role",
            "create function zou_alt.zou_profile_tag() returns text language sql stable \
             as 'select tag from zou_alt.zou_profile_rows where id = 1'",
            // Same table name in both schemas, so only the search_path
            // can explain which rows come back.
            "create table zou_profile_rows (id int primary key, tag text)",
            "insert into zou_profile_rows values (1, 'pub')",
        ],
    )
    .await;
    let two = app_with_schemas(&dsn, &["public", "zou_alt"]);

    let profiled =
        |method: &str, uri: &str, body: &str, hdr: &str, value: &str, prefers: &[&str]| {
            let mut b = Request::builder()
                .method(method)
                .uri(uri)
                .header("apikey", anon_key());
            if !hdr.is_empty() {
                b = b.header(hdr, value);
            }
            for p in prefers {
                b = b.header("prefer", *p);
            }
            let body = if body.is_empty() {
                Body::empty()
            } else {
                Body::from(body.to_string())
            };
            b.body(body).unwrap()
        };

    // Accept-Profile picks the read's schema, and the response echoes
    // it back as Content-Profile.
    let res = two
        .clone()
        .oneshot(profiled(
            "GET",
            "/rest/v1/zou_profile_rows?select=tag",
            "",
            "accept-profile",
            "zou_alt",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-profile"], "zou_alt");
    assert_eq!(body_text(res).await, r#"[{"tag":"alt"}]"#);

    // No header is the first exposed schema, and with more than one
    // exposed the default still counts as negotiated.
    let res = two
        .clone()
        .oneshot(get("/rest/v1/zou_profile_rows?select=tag"))
        .await
        .unwrap();
    assert_eq!(res.headers()["content-profile"], "public");
    assert_eq!(body_text(res).await, r#"[{"tag":"pub"}]"#);

    // A read ignores Content-Profile, that header is the writes'.
    let res = two
        .clone()
        .oneshot(profiled(
            "GET",
            "/rest/v1/zou_profile_rows?select=tag",
            "",
            "content-profile",
            "zou_alt",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"tag":"pub"}]"#);

    // An unexposed schema is the PGRST106 406, hint listing what is.
    let res = two
        .clone()
        .oneshot(profiled(
            "GET",
            "/rest/v1/zou_profile_rows",
            "",
            "accept-profile",
            "secret",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);
    let e: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(e["code"], "PGRST106");
    assert_eq!(e["message"], "Invalid schema: secret");
    assert_eq!(e["details"], serde_json::Value::Null);
    assert_eq!(
        e["hint"],
        "Only the following schemas are exposed: public, zou_alt"
    );

    // The refusal comes before the body is even looked at, upstream's
    // getSchema ordering.
    let res = two
        .clone()
        .oneshot(profiled(
            "POST",
            "/rest/v1/zou_profile_rows",
            "not json at all",
            "content-profile",
            "secret",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);
    let e: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(e["code"], "PGRST106");

    // Content-Profile routes the write, and the representation
    // carries the profile back.
    let res = two
        .clone()
        .oneshot(profiled(
            "POST",
            "/rest/v1/zou_profile_rows",
            r#"{"id": 2, "tag": "alt2"}"#,
            "content-profile",
            "zou_alt",
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    assert_eq!(res.headers()["content-profile"], "zou_alt");
    assert_eq!(body_text(res).await, r#"[{"id":2,"tag":"alt2"}]"#);

    // It landed in zou_alt and nowhere near public.
    let res = two
        .clone()
        .oneshot(profiled(
            "GET",
            "/rest/v1/zou_profile_rows?select=tag&order=id.asc",
            "",
            "accept-profile",
            "zou_alt",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"tag":"alt"},{"tag":"alt2"}]"#);
    let res = two
        .clone()
        .oneshot(get("/rest/v1/zou_profile_rows?select=tag"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"tag":"pub"}]"#);

    // A minimal write answers 204 with no content type, so no
    // Content-Profile rides along either.
    let res = two
        .clone()
        .oneshot(profiled(
            "POST",
            "/rest/v1/zou_profile_rows",
            r#"{"id": 3, "tag": "alt3"}"#,
            "content-profile",
            "zou_alt",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    assert!(res.headers().get("content-profile").is_none());

    // Rpc resolves the function in the profiled schema, GET so the
    // Accept-Profile side is the one that counts.
    let res = two
        .clone()
        .oneshot(profiled(
            "GET",
            "/rest/v1/rpc/zou_profile_tag",
            "",
            "accept-profile",
            "zou_alt",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-profile"], "zou_alt");
    assert_eq!(body_text(res).await, r#""alt""#);

    // The same function is nowhere to be found in public.
    let res = two
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_profile_tag"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    // One exposed schema means nothing was negotiated, so a default
    // deployment's responses look exactly as they did before.
    let one = app(&dsn);
    let res = one
        .clone()
        .oneshot(get("/rest/v1/zou_profile_rows?select=tag"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(res.headers().get("content-profile").is_none());
    assert_eq!(body_text(res).await, r#"[{"tag":"pub"}]"#);

    let res = one
        .clone()
        .oneshot(profiled(
            "GET",
            "/rest/v1/zou_profile_rows",
            "",
            "accept-profile",
            "zou_alt",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);
    let e: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(
        e["hint"], "Only the following schemas are exposed: public",
        "an unexposed schema is unexposed even when it exists"
    );
}

#[tokio::test]
async fn the_openapi_document_speaks_postgrest() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_oa_children, zou_oa_parents cascade",
            "drop view if exists zou_oa_view, zou_oa_agg cascade",
            "drop function if exists zou_oa_add(integer, integer) cascade",
            "drop function if exists zou_oa_touch(text) cascade",
            "drop type if exists zou_oa_mood cascade",
            "create type zou_oa_mood as enum ('sad', 'ok', 'happy')",
            "create table zou_oa_parents (id int primary key, name text not null)",
            "create table zou_oa_children (\
               id serial primary key, \
               parent_id int references zou_oa_parents(id), \
               label varchar(20) default 'none'::character varying, \
               tags text[], \
               weight numeric, \
               mood zou_oa_mood, \
               body jsonb, \
               seen boolean not null default false)",
            "comment on table zou_oa_children is 'children summary\n\nchildren detail'",
            "comment on column zou_oa_children.label is 'the label'",
            "create view zou_oa_view as select id, name from zou_oa_parents",
            "create view zou_oa_agg as select count(*) as n from zou_oa_parents",
            "create function zou_oa_add(a integer, b integer default 1) returns integer \
             language sql immutable as 'select a + b'",
            "comment on function zou_oa_add(integer, integer) is 'adds\n\ntwo numbers'",
            "create function zou_oa_touch(note text) returns void language sql volatile \
             as 'select null::void'",
            // Narrow grants on purpose: two tests rewriting every acl
            // in public at once race each other in the catalog.
            "grant all on zou_oa_parents, zou_oa_children, zou_oa_view, zou_oa_agg \
             to anon, authenticated, service_role",
            "grant usage, select on sequence zou_oa_children_id_seq \
             to anon, authenticated, service_role",
        ],
    )
    .await;
    let app = app(&dsn);

    let res = app.clone().oneshot(get("/rest/v1/")).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers()[axum::http::header::CONTENT_TYPE],
        "application/openapi+json; charset=utf-8"
    );
    assert!(
        res.headers().get("content-profile").is_none(),
        "one exposed schema means nothing was negotiated"
    );
    let doc: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();

    assert_eq!(doc["swagger"], "2.0");
    assert_eq!(
        doc["info"]["title"], "standard public schema",
        "public ships with a comment, and a schema comment names the document"
    );
    assert_eq!(doc["basePath"], "/rest/v1/");
    assert_eq!(
        doc["paths"]["/"]["get"]["summary"],
        "OpenAPI description (this document)"
    );

    // The definition carries the type mapping, the length, the
    // default, the enum, and the required list.
    let def = &doc["definitions"]["zou_oa_children"];
    assert_eq!(def["description"], "children summary\n\nchildren detail");
    assert_eq!(
        def["properties"]["label"],
        serde_json::json!({
            "default": "none",
            "description": "the label",
            "format": "character varying",
            "maxLength": 20,
            "type": "string",
        })
    );
    assert_eq!(
        def["properties"]["tags"],
        serde_json::json!({
            "format": "text[]",
            "type": "array",
            "items": {"type": "string"},
        })
    );
    assert_eq!(
        def["properties"]["weight"],
        serde_json::json!({"format": "numeric", "type": "number"})
    );
    assert_eq!(
        def["properties"]["body"],
        serde_json::json!({"format": "jsonb"}),
        "jsonb takes any shape, so it carries no type"
    );
    assert_eq!(
        def["properties"]["mood"]["enum"],
        serde_json::json!(["sad", "ok", "happy"]),
        "the labels come back in their declared order"
    );
    assert_eq!(
        def["properties"]["seen"],
        serde_json::json!({"default": false, "format": "boolean", "type": "boolean"})
    );
    assert_eq!(
        def["properties"]["id"]["description"],
        "Note:\nThis is a Primary Key.<pk/>"
    );
    assert_eq!(
        def["properties"]["parent_id"]["description"],
        "Note:\nThis is a Foreign Key to `zou_oa_parents.id`.\
         <fk table='zou_oa_parents' column='id'/>"
    );
    assert_eq!(def["required"], serde_json::json!(["id", "seen"]));

    // A table gets the whole write trio, and so does an auto
    // updatable view; a view postgres cannot write through does not.
    let path = &doc["paths"]["/zou_oa_children"];
    assert_eq!(path["get"]["summary"], "children summary");
    assert_eq!(path["get"]["description"], "children detail");
    assert_eq!(path["get"]["tags"], serde_json::json!(["zou_oa_children"]));
    assert_eq!(
        path["get"]["parameters"][0],
        serde_json::json!({"$ref": "#/parameters/rowFilter.zou_oa_children.id"})
    );
    assert_eq!(
        path["post"]["parameters"][0],
        serde_json::json!({"$ref": "#/parameters/body.zou_oa_children"})
    );
    assert!(path["patch"].is_object() && path["delete"].is_object());
    assert!(
        doc["paths"]["/zou_oa_view"]["post"].is_object(),
        "a simple view is auto updatable, so it keeps the write trio"
    );
    assert!(
        doc["paths"]["/zou_oa_agg"]["get"].is_object()
            && doc["paths"]["/zou_oa_agg"]["post"].is_null(),
        "an aggregate view is not updatable, so it is read only"
    );

    // An immutable function answers GET and POST, a volatile one
    // only POST.
    let add = &doc["paths"]["/rpc/zou_oa_add"];
    assert_eq!(add["get"]["tags"], serde_json::json!(["(rpc) zou_oa_add"]));
    assert_eq!(add["get"]["summary"], "adds");
    assert_eq!(
        add["get"]["parameters"],
        serde_json::json!([
            {"name": "a", "required": true, "in": "query", "format": "int32", "type": "integer"},
            {"name": "b", "required": false, "in": "query", "format": "int32", "type": "integer"},
        ])
    );
    assert_eq!(
        add["post"]["parameters"][0]["schema"]["required"],
        serde_json::json!(["a"])
    );
    assert!(
        doc["paths"]["/rpc/zou_oa_touch"]["get"].is_null(),
        "a volatile function cannot be reached over GET"
    );

    // The root negotiates its own producible list, and a table
    // cannot produce openapi at all.
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/rest/v1/")
                .header("apikey", anon_key())
                .header("accept", "text/csv")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);
    let e: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(e["code"], "PGRST107");

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/rest/v1/zou_oa_parents")
                .header("apikey", anon_key())
                .header("accept", "application/openapi+json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/rest/v1/")
                .header("apikey", anon_key())
                .header("accept", "application/openapi+json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // The document is the role's own view of the schema: a table
    // anon cannot touch is not in it.
    seed(
        &dsn,
        &[
            "drop table if exists zou_oa_secret cascade",
            "create table zou_oa_secret (id int primary key)",
            "revoke all on zou_oa_secret from anon, authenticated, service_role",
        ],
    )
    .await;
    let res = app.clone().oneshot(get("/rest/v1/")).await.unwrap();
    let doc: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert!(
        doc["definitions"]["zou_oa_secret"].is_null(),
        "follow privileges is the default, so an unreachable table is unlisted"
    );
}

#[tokio::test]
async fn the_openapi_document_follows_the_profile() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop schema if exists zou_oa_alt cascade",
            "create schema zou_oa_alt",
            "comment on schema zou_oa_alt is 'Alt title\n\nAlt description'",
            "grant usage on schema zou_oa_alt to anon, authenticated, service_role",
            "create table zou_oa_alt.zou_oa_only_here (id int primary key)",
            "grant all on all tables in schema zou_oa_alt to anon, authenticated, service_role",
        ],
    )
    .await;
    let two = app_with_schemas(&dsn, &["public", "zou_oa_alt"]);

    let res = two
        .clone()
        .oneshot(
            Request::builder()
                .uri("/rest/v1/")
                .header("apikey", anon_key())
                .header("accept-profile", "zou_oa_alt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-profile"], "zou_oa_alt");
    let doc: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(
        doc["info"]["title"], "Alt title",
        "the schema comment names the document"
    );
    assert_eq!(doc["info"]["description"], "Alt description");
    assert!(doc["definitions"]["zou_oa_only_here"].is_object());

    let res = two
        .clone()
        .oneshot(
            Request::builder()
                .uri("/rest/v1/")
                .header("apikey", anon_key())
                .header("accept-profile", "nope")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);
    let e: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(e["code"], "PGRST106");
}

#[tokio::test]
async fn the_catalog_follows_the_ddl_epoch() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_ep_children, zou_ep_parents cascade",
            "create table zou_ep_parents (id int primary key, name text)",
            // No foreign key yet, so the embed cannot resolve.
            "create table zou_ep_children (id int primary key, parent_id int)",
            "insert into zou_ep_parents values (1, 'ann')",
            "insert into zou_ep_children values (10, 1)",
            "grant all on zou_ep_parents, zou_ep_children to anon, authenticated, service_role",
        ],
    )
    .await;
    let app = app(&dsn);
    let embed = "/rest/v1/zou_ep_children?select=id,zou_ep_parents(name)";

    let res = app.clone().oneshot(get(embed)).await.unwrap();
    assert_ne!(
        res.status(),
        StatusCode::OK,
        "no foreign key means no embed, and this is what caches the catalog"
    );
    let e: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(e["code"], "PGRST200");

    seed(
        &dsn,
        &["alter table zou_ep_children add constraint zou_ep_fk \
           foreign key (parent_id) references zou_ep_parents(id)"],
    )
    .await;

    // The DDL notification travels over the watch connection, so the
    // epoch moves a moment after the alter commits rather than during
    // it. Poll rather than sleep a fixed amount.
    let mut body = String::new();
    for _ in 0..100 {
        let res = app.clone().oneshot(get(embed)).await.unwrap();
        if res.status() == StatusCode::OK {
            body = body_text(res).await;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(
        body, r#"[{"id":10,"zou_ep_parents":{"name": "ann"}}]"#,
        "the new foreign key reached the cached catalog"
    );
}

#[tokio::test]
async fn the_schema_cache_answers_for_tables_and_columns_nobody_has() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_cache_widgets cascade",
            "drop view if exists zou_cache_widget_view",
            "create table zou_cache_widgets (id int primary key, name text)",
            "insert into zou_cache_widgets values (1, 'one')",
            "create view zou_cache_widget_view as select id from zou_cache_widgets",
        ],
    )
    .await;
    let app = app(&dsn);

    // A table nobody has, on every method that names one, and the
    // suggestion when the name is nearly a table that is there.
    for (method, uri) in [
        ("GET", "/rest/v1/zou_cache_widgex"),
        ("HEAD", "/rest/v1/zou_cache_widgex"),
        ("POST", "/rest/v1/zou_cache_widgex"),
        ("PATCH", "/rest/v1/zou_cache_widgex?id=eq.1"),
        ("PUT", "/rest/v1/zou_cache_widgex?id=eq.1"),
        ("DELETE", "/rest/v1/zou_cache_widgex?id=eq.1"),
        ("OPTIONS", "/rest/v1/zou_cache_widgex"),
    ] {
        let res = app
            .clone()
            .oneshot(req(method, uri, r#"{"id": 2}"#, &[]))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND, "{method} {uri}");
        if method == "HEAD" {
            continue;
        }
        let e: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
        assert_eq!(e["code"], "PGRST205", "{method}");
        assert_eq!(
            e["message"], "Could not find the table 'public.zou_cache_widgex' in the schema cache",
            "{method}"
        );
        assert_eq!(
            e["hint"], "Perhaps you meant the table 'public.zou_cache_widgets'",
            "{method}"
        );
    }

    // Nothing like anything, so nothing suggested.
    let res = app.clone().oneshot(get("/rest/v1/zqx")).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let e: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(e["hint"], serde_json::Value::Null);

    // A view is a relation the cache has, like any other.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_cache_widget_view"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"id":1}]"#);

    // A column a write names and the table does not have, from the
    // body and from ?columns=, and never from postgres.
    for (method, uri, body) in [
        ("POST", "/rest/v1/zou_cache_widgets", r#"{"nope": 1}"#),
        (
            "PATCH",
            "/rest/v1/zou_cache_widgets?id=eq.1",
            r#"{"nope": 1}"#,
        ),
        (
            "PUT",
            "/rest/v1/zou_cache_widgets?id=eq.1",
            r#"{"nope": 1}"#,
        ),
        (
            "POST",
            "/rest/v1/zou_cache_widgets?columns=id,nope",
            r#"{"id": 2}"#,
        ),
        (
            "PATCH",
            "/rest/v1/zou_cache_widgets?id=eq.1&columns=nope",
            r#"{"id": 2}"#,
        ),
    ] {
        let res = app
            .clone()
            .oneshot(req(method, uri, body, &[]))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST, "{method} {uri}");
        let e: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
        assert_eq!(e["code"], "PGRST204", "{method} {uri}");
        assert_eq!(
            e["message"],
            "Could not find the 'nope' column of 'zou_cache_widgets' in the schema cache",
            "{method} {uri}"
        );
    }

    // A PUT reads its columns off the body, so ?columns= narrowing
    // it is not a refusal and not a narrowing either: rank is
    // written even though the url did not name it.
    let res = app
        .clone()
        .oneshot(req(
            "PUT",
            "/rest/v1/zou_cache_widgets?id=eq.1&columns=id",
            r#"{"id": 1, "name": "renamed"}"#,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_cache_widgets?id=eq.1"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"id":1,"name":"renamed"}]"#);

    // What a table will take, which is the same list for every table
    // and only says the table is there.
    let res = app
        .clone()
        .oneshot(req("OPTIONS", "/rest/v1/zou_cache_widgets", "", &[]))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers()["allow"],
        "OPTIONS,GET,HEAD,POST,PUT,PATCH,DELETE"
    );
    assert_eq!(body_text(res).await, "");
}

#[tokio::test]
async fn a_data_representation_is_what_the_client_sees() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_rest_paint cascade",
            "drop domain if exists zou_rest_color cascade",
            "create domain zou_rest_color as int4",
            // Postgres itself will not apply either of these, and
            // says so when they are made: a cast whose source or
            // target is a domain is recorded and ignored. Upstream
            // reads the record and calls the function by name, which
            // is the whole of what a data representation is.
            "create function zou_rest_json(zou_rest_color) returns json as \
             $$ select to_json('#' || lpad(to_hex($1::int), 6, '0')) $$ language sql immutable",
            "create cast (zou_rest_color as json) with function zou_rest_json(zou_rest_color)",
            "create function zou_rest_read(text) returns zou_rest_color as \
             $$ select (('x' || lpad(substring($1 from 2), 8, '0'))::bit(32)::int)::zou_rest_color $$ \
             language sql immutable",
            "create cast (text as zou_rest_color) with function zou_rest_read(text)",
            "create table zou_rest_paint (id int primary key, shade zou_rest_color, note text)",
            "insert into zou_rest_paint values (1, 16711680, 'warm'), (2, 255, 'cool')",
        ],
    )
    .await;
    let app = app(&dsn);

    // Named in the select, the column comes out as what the cast
    // makes of it rather than as what it holds.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_paint?select=id,shade&order=id"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        body_text(res).await,
        r##"[{"id":1,"shade":"#ff0000"},{"id":2,"shade":"#0000ff"}]"##
    );

    // A star has to be spelled out for the call to go anywhere, and
    // every column it spells out keeps its name. The key order is
    // the one jsonb gives every other row here.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_paint?id=eq.1"))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r##"[{"id":1,"shade":"#ff0000","note":"warm"}]"##
    );

    // The url is text, so a filter reads its value through the cast
    // the other way, and #ff0000 finds the row holding 16711680.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_paint?select=note&shade=eq.%23ff0000",
        ))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"note":"warm"}]"#);

    // And a write filtered the same way, whose representation comes
    // back through the cast as well.
    let res = app
        .clone()
        .oneshot(req(
            "PATCH",
            "/rest/v1/zou_rest_paint?shade=eq.%230000ff&select=id,shade",
            r#"{"note": "chilly"}"#,
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r##"[{"id":2,"shade":"#0000ff"}]"##);

    // The request's own cast sits on top of the representation
    // rather than instead of it, so this is the text of the hex
    // string, quotes and all, and not the text of the integer.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_paint?select=shade::text&id=eq.1"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, "[{\"shade\":\"\\\"#ff0000\\\"\"}]");
}

#[tokio::test]
async fn a_data_representation_reads_a_written_value_back_in() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_rest_wpaint cascade",
            "drop domain if exists zou_rest_wcolor cascade",
            "create domain zou_rest_wcolor as int4",
            "create function zou_rest_wjson(zou_rest_wcolor) returns json as \
             $$ select to_json('#' || lpad(to_hex($1::int), 6, '0')) $$ language sql immutable",
            "create cast (zou_rest_wcolor as json) with function zou_rest_wjson(zou_rest_wcolor)",
            // The one a write needs: the body carries json, not the
            // text a url carries, so this is a second function and a
            // second cast even where the two parse the same string.
            "create function zou_rest_wread(json) returns zou_rest_wcolor as \
             $$ select (('x' || lpad(substring(($1 #>> '{}') from 2), 8, '0'))::bit(32)::int)::zou_rest_wcolor $$ \
             language sql immutable",
            "create cast (json as zou_rest_wcolor) with function zou_rest_wread(json)",
            "create table zou_rest_wpaint (id int primary key, shade zou_rest_wcolor, note text)",
        ],
    )
    .await;
    let app = app(&dsn);

    // An insert of one object, and the representation it hands back
    // has been through the cast the other way.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_rest_wpaint?select=id,shade",
            r##"{"id": 1, "shade": "#ff0000", "note": "warm"}"##,
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    assert_eq!(body_text(res).await, r##"[{"id":1,"shade":"#ff0000"}]"##);

    // What actually landed is the integer, which is the point of the
    // whole arrangement.
    let pool = Pool::new(&dsn, 1).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    let rows = sess
        .query("select shade::int from zou_rest_wpaint where id = 1", &[])
        .await
        .expect("read back");
    assert_eq!(rows[0].get::<_, i32>(0), 16711680);
    sess.commit().await.expect("park");

    // An array body takes the same path, one row per element.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_rest_wpaint?select=id,shade&order=id",
            r##"[{"id":2,"shade":"#0000ff"},{"id":3,"shade":"#00ff00"}]"##,
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r##"[{"id":2,"shade":"#0000ff"},{"id":3,"shade":"#00ff00"}]"##
    );

    // And an update, which declares only the column it writes.
    let res = app
        .clone()
        .oneshot(req(
            "PATCH",
            "/rest/v1/zou_rest_wpaint?id=eq.1&select=id,shade",
            r##"{"shade": "#010203"}"##,
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r##"[{"id":1,"shade":"#010203"}]"##);

    // A write that touches nothing represented is left on the plain
    // path, and still writes what it was given.
    let res = app
        .clone()
        .oneshot(req(
            "PATCH",
            "/rest/v1/zou_rest_wpaint?id=eq.1&select=id,note",
            r#"{"note": "chilly"}"#,
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"id":1,"note":"chilly"}]"#);
}

/// `Prefer: missing=default`, the write that wants what the table
/// would have put there rather than the null a body's silence gives.
#[tokio::test]
async fn a_column_left_out_of_a_body_can_take_its_default() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_rest_defaults cascade",
            "create table zou_rest_defaults (\
               id int generated by default as identity primary key, \
               name text, \
               tier text default 'free', \
               seats int default 3)",
        ],
    )
    .await;
    let app = app(&dsn);

    // Without the preference a column the url named and the body did
    // not is null, which is what unpacking the body gives on its own.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_rest_defaults?columns=name,tier,seats&select=name,tier,seats",
            r#"[{"name": "quiet"}]"#,
            &["return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    assert_eq!(
        body_text(res).await,
        r#"[{"name":"quiet","tier":null,"seats":null}]"#
    );

    // With it, each one is what the table says, and the preference
    // is echoed back.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_rest_defaults?columns=name,tier,seats&select=name,tier,seats",
            r#"[{"name": "loud"}]"#,
            &["missing=default", "return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(
        res.headers().get("preference-applied").unwrap(),
        "missing=default, return=representation"
    );
    assert_eq!(
        body_text(res).await,
        r#"[{"name":"loud","tier":"free","seats":3}]"#
    );

    // A key the body does carry still wins, so the defaults go under
    // the body rather than over it.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_rest_defaults?columns=name,tier,seats&select=name,tier,seats",
            r#"[{"name": "mixed", "seats": 9}]"#,
            &["missing=default", "return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"name":"mixed","tier":"free","seats":9}]"#
    );

    // An identity column has no default row in the catalog at all,
    // and the sequence postgres made for it is the default anyway.
    let res = app
        .clone()
        .oneshot(req(
            "POST",
            "/rest/v1/zou_rest_defaults?columns=id,name&select=name",
            r#"[{"name":"surrogate"}]"#,
            &["missing=default", "return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    assert_eq!(body_text(res).await, r#"[{"name":"surrogate"}]"#);

    // An update fills the same way, one object rather than an array.
    let res = app
        .clone()
        .oneshot(req(
            "PATCH",
            "/rest/v1/zou_rest_defaults?name=eq.quiet&columns=tier,seats&select=name,tier,seats",
            r#"{"seats": 1}"#,
            &["missing=default", "return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"name":"quiet","tier":"free","seats":1}]"#
    );

    // A DELETE reads no body, so the preference is neither applied
    // nor claimed, which is upstream's line between the two.
    let res = app
        .clone()
        .oneshot(req(
            "DELETE",
            "/rest/v1/zou_rest_defaults?name=eq.quiet",
            "",
            &["missing=default", "return=representation"],
        ))
        .await
        .unwrap();
    assert_eq!(
        res.headers().get("preference-applied").unwrap(),
        "return=representation"
    );
}

/// The handling preference decides what an unrecognized preference
/// costs, and the timezone preference is checked against the names
/// postgres has before it is set for the length of the transaction.
#[tokio::test]
async fn a_preference_nobody_has_costs_what_handling_says_it_does() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_rest_when cascade",
            "create table zou_rest_when (id int primary key, t timestamptz)",
            "insert into zou_rest_when values (1, '2023-10-18T12:37:59.611Z')",
        ],
    )
    .await;
    let app = app(&dsn);

    // Strict refuses and names what it did not know.
    let res = app
        .clone()
        .oneshot(req(
            "GET",
            "/rest/v1/zou_rest_when",
            "",
            &["handling=strict, anything"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "PGRST122");
    assert_eq!(body["details"], "Invalid preferences: anything");
    assert_eq!(
        body["message"],
        "Invalid preferences given with handling=strict"
    );

    // Lenient carries on and says which of the two it was.
    let res = app
        .clone()
        .oneshot(req(
            "GET",
            "/rest/v1/zou_rest_when",
            "",
            &["handling=lenient, anything"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers().get("preference-applied").unwrap(),
        "handling=lenient"
    );

    // A timezone postgres has renders the row in it.
    let res = app
        .clone()
        .oneshot(req(
            "GET",
            "/rest/v1/zou_rest_when?select=t",
            "",
            &["handling=strict, timezone=America/Los_Angeles"],
        ))
        .await
        .unwrap();
    assert_eq!(
        res.headers().get("preference-applied").unwrap(),
        "handling=strict, timezone=America/Los_Angeles"
    );
    assert_eq!(
        body_text(res).await,
        r#"[{"t":"2023-10-18T05:37:59.611-07:00"}]"#
    );

    // One postgres does not have is postgres's to refuse, and the
    // handling has nothing to say about it. PostgREST 14 checked the
    // name against pg_timezone_names first, so an unknown zone was
    // PGRST122 under strict and quietly dropped otherwise. 16 sets it
    // and lets the server answer, which is the same 22023 either way.
    for line in [
        "handling=strict, timezone=Nowhere/Special",
        "timezone=Nowhere/Special",
    ] {
        let res = app
            .clone()
            .oneshot(req("GET", "/rest/v1/zou_rest_when?select=t", "", &[line]))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST, "{line}");
        let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
        assert_eq!(body["code"], "22023", "{line}");
        assert_eq!(
            body["message"], "invalid value for parameter \"TimeZone\": \"Nowhere/Special\"",
            "{line}"
        );
    }
}

/// max-affected caps what a mutation may touch, and the rows it
/// already wrote go back when it does not hold.
#[tokio::test]
async fn a_write_that_touched_more_rows_than_asked_for_is_taken_back() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_rest_capped cascade",
            "create table zou_rest_capped (id int primary key, name text)",
            "insert into zou_rest_capped \
               select i, 'row ' || i from generate_series(1, 5) i",
        ],
    )
    .await;
    let app = app(&dsn);

    let count = |app: axum::Router| async move {
        let res = app
            .oneshot(get("/rest/v1/zou_rest_capped?select=id"))
            .await
            .unwrap();
        serde_json::from_str::<Vec<serde_json::Value>>(&body_text(res).await)
            .unwrap()
            .len()
    };

    // Over the cap the whole delete is refused, and the rows are
    // still there afterwards.
    let res = app
        .clone()
        .oneshot(req(
            "DELETE",
            "/rest/v1/zou_rest_capped?id=gt.0",
            "",
            &["handling=strict, max-affected=2"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "PGRST124");
    assert_eq!(body["details"], "The query affects 5 rows");
    assert_eq!(
        body["message"],
        "Query result exceeds max-affected preference constraint"
    );
    assert_eq!(count(app.clone()).await, 5);

    // An update is judged the same way and taken back the same way.
    let res = app
        .clone()
        .oneshot(req(
            "PATCH",
            "/rest/v1/zou_rest_capped?id=gt.0",
            r#"{"name": "renamed"}"#,
            &["handling=strict, max-affected=0"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_capped?select=name&id=eq.1"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"name":"row 1"}]"#);

    // Under the cap the write lands and says what it held itself to.
    let res = app
        .clone()
        .oneshot(req(
            "DELETE",
            "/rest/v1/zou_rest_capped?id=lt.3",
            "",
            &["handling=strict, max-affected=2"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        res.headers().get("preference-applied").unwrap(),
        "handling=strict, max-affected=2"
    );
    assert_eq!(count(app.clone()).await, 3);

    // Without strict the cap is not applied at all, so it neither
    // binds nor is claimed.
    let res = app
        .clone()
        .oneshot(req(
            "DELETE",
            "/rest/v1/zou_rest_capped?id=gt.0",
            "",
            &["handling=lenient, max-affected=1"],
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        res.headers().get("preference-applied").unwrap(),
        "handling=lenient"
    );
    assert_eq!(count(app.clone()).await, 0);
}

/// An order term and a filter can both name an embedded resource
/// rather than a column of the table the url names: one sorts the
/// parent by something on the other side of the join, the other asks
/// whether the join found anything.
#[tokio::test]
async fn an_order_and_a_filter_can_reach_into_an_embed() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_rest_task cascade",
            "drop table if exists zou_rest_crew cascade",
            "create table zou_rest_crew (id int primary key, name text)",
            "create table zou_rest_task (\
               id int primary key, \
               title text, \
               crew_id int references zou_rest_crew (id))",
            "insert into zou_rest_crew values (1, 'maps'), (2, 'roads')",
            "insert into zou_rest_task values \
               (1, 'draw', 2), (2, 'name', 1), (3, 'walk', null)",
        ],
    )
    .await;
    let app = app(&dsn);

    // The sort key is a column of the embed, and a task with no crew
    // sorts where a null sorts.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_task?select=title,zou_rest_crew(name)\
             &order=zou_rest_crew(name).asc.nullslast",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        body_text(res).await,
        r#"[{"title":"name","zou_rest_crew":{"name": "maps"}},{"title":"draw","zou_rest_crew":{"name": "roads"}},{"title":"walk","zou_rest_crew":null}]"#
    );

    // The embed does not have to be selected for its columns to be
    // sortable, and an alias is the name the term uses.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_task?select=title,crew:zou_rest_crew()\
             &order=crew(name).desc.nullslast",
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"title":"draw"},{"title":"name"},{"title":"walk"}]"#
    );

    // Sorting a parent by a list is refused rather than guessed at.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_crew?select=name,zou_rest_task(title)\
             &order=zou_rest_task(title)",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        body_text(res).await,
        r#"{"code":"PGRST118","details":"'zou_rest_crew' and 'zou_rest_task' do not form a many-to-one or one-to-one relationship","hint":null,"message":"A related order on 'zou_rest_task' is not possible"}"#
    );

    // A name the select tree does not embed has nowhere to go.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_task?select=title&order=zou_rest_crew(name)",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        body_text(res).await,
        r#"{"code":"PGRST108","details":null,"hint":"Verify that 'zou_rest_crew' is included in the 'select' query parameter.","message":"'zou_rest_crew' is not an embedded resource in this request"}"#
    );

    // The null test is about the whole row of the embed, so it keeps
    // the rows that found one and drops the rest.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_task?select=title,zou_rest_crew()\
             &zou_rest_crew=not.is.null&order=id"))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"title":"draw"},{"title":"name"}]"#
    );

    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_task?select=title,zou_rest_crew()\
             &zou_rest_crew=is.null"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"title":"walk"}]"#);

    // A to many answers the same question about its own filters, and
    // the count that comes back with it is over the same predicate.
    let res = app
        .clone()
        .oneshot(req(
            "GET",
            "/rest/v1/zou_rest_crew?select=name,zou_rest_task()\
             &zou_rest_task.title=eq.draw&zou_rest_task=not.is.null",
            "",
            &["count=exact"],
        ))
        .await
        .unwrap();
    assert_eq!(res.headers().get("content-range").unwrap(), "0-0/1");
    assert_eq!(body_text(res).await, r#"[{"name":"roads"}]"#);

    // Every other operator reads the name as a column, which is what
    // lets a table carry a column and an embed of the same name.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_task?select=title,zou_rest_crew()\
             &zou_rest_crew=eq.1"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(body_text(res).await.contains("42703"));
}

/// A relationship answers to every name it has: the table on the
/// other end, the constraint that makes it, and the foreign key
/// column the constraint sits on. A table that points at itself has
/// two relationships under one name and takes its direction from
/// which of those names the request used.
#[tokio::test]
async fn an_embed_can_be_named_by_the_key_that_makes_it() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_rest_staff cascade",
            "drop table if exists zou_rest_dept cascade",
            "create table zou_rest_dept (id int primary key, name text)",
            "create table zou_rest_staff (\
               id int primary key, \
               name text, \
               dept_id int constraint works_in references zou_rest_dept (id), \
               boss_id int references zou_rest_staff (id))",
            "insert into zou_rest_dept values (1, 'maps'), (2, 'roads')",
            "insert into zou_rest_staff values \
               (1, 'ada', 1, null), (2, 'bob', 2, 1)",
        ],
    )
    .await;
    let app = app(&dsn);

    // The foreign key column names the relationship, and the key it
    // comes back under is the word the request used.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_staff?select=name,dept_id(name)&id=eq.2",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        body_text(res).await,
        r#"[{"name":"bob","dept_id":{"name": "roads"}}]"#
    );

    // So does the constraint, under whatever name it was given.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_staff?select=name,dept:works_in(name)&id=eq.2",
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"name":"bob","dept":{"name": "roads"}}]"#
    );

    // A hint may name either end of the pair it joins on, so the
    // referenced column reaches the same relationship the fk column
    // does.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_staff?select=name,zou_rest_dept!id(name)&id=eq.1",
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"name":"ada","zou_rest_dept":{"name": "maps"}}]"#
    );

    // The table's own name, on a table that points at itself, is the
    // list of rows pointing back.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_staff?select=name,reports:zou_rest_staff(name)&id=eq.1",
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"name":"ada","reports":[{"name": "bob"}]}]"#
    );

    // And the column's name is the one row it points at.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_staff?select=name,boss_id(name)&id=eq.2",
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"name":"bob","boss_id":{"name": "ada"}}]"#
    );

    // A hint on the table name says which column the list comes back
    // through, which is the only spelling that survives a table with
    // more than one reference to itself.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_staff?select=name,reports:zou_rest_staff!boss_id(name)&id=eq.1",
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"name":"ada","reports":[{"name": "bob"}]}]"#
    );

    // A name that is none of those is still nothing, and the details
    // say which schema was looked in and what the hint was. The
    // suggestion is the other name this table's relationships answer
    // to, since the one that was asked for is a relationship the
    // parent has and the hint after ! is what went wrong.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_staff?select=name,zou_rest_dept!nowhere(name)",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        body_text(res).await,
        r#"{"code":"PGRST200","details":"Searched for a foreign key relationship between 'zou_rest_staff' and 'zou_rest_dept' using the hint 'nowhere' in the schema 'public', but no matches were found.","hint":"Perhaps you meant 'zou_rest_staff' instead of 'zou_rest_dept'.","message":"Could not find a relationship between 'zou_rest_staff' and 'zou_rest_dept' in the schema cache"}"#
    );
}

/// Spreading a list has no one row to merge into the parent, so each
/// column of the child arrives as the list of that column over every
/// child row: a key per column, empty rather than missing when the
/// parent has no children, ordered the way the embed asked to be.
#[tokio::test]
async fn a_spread_of_a_list_arrives_one_column_at_a_time() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_rest_player cascade",
            "drop table if exists zou_rest_team cascade",
            "create table zou_rest_team (id int primary key, name text)",
            "create table zou_rest_player (\
               id int primary key, \
               name text, \
               team_id int references zou_rest_team (id), \
               coach_id int references zou_rest_player (id))",
            "insert into zou_rest_team values (1, 'maps'), (2, 'roads'), (3, 'labs')",
            "insert into zou_rest_player values \
               (1, 'ada', 1, null), (3, 'cy', 1, 1), (2, 'bob', 2, 1)",
        ],
    )
    .await;
    let app = app(&dsn);

    // The embed's order belongs inside the aggregate, since the rows
    // it sorts are gone by the time the list exists.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_team?select=name,...zou_rest_player(players:name)\
             &order=name&zou_rest_player.order=name.desc",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        body_text(res).await,
        r#"[{"name":"labs","players":[]},{"name":"maps","players":["cy", "ada"]},{"name":"roads","players":["bob"]}]"#
    );

    // An embed inside the spread is a column of the child like any
    // other, so it comes up as the list of what each child saw.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_team?select=name,...zou_rest_player(id,coach_id(name))\
             &order=name&zou_rest_player.order=id.asc",
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"name":"labs","id":[],"coach_id":[]},{"name":"maps","id":[1, 3],"coach_id":[null, {"name": "ada"}]},{"name":"roads","id":[2],"coach_id":[{"name": "ada"}]}]"#
    );

    // A star spreads every column of the child.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_team?select=team:name,...zou_rest_player(*)&id=eq.2",
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"team":"roads","id":[2],"name":["bob"],"team_id":[2],"coach_id":[1]}]"#
    );

    // The parent without children is still a parent, unless the embed
    // is inner, and it is the one the null test finds.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_team?select=team:name,...zou_rest_player!inner(name)\
             &order=name&zou_rest_player.order=name",
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"team":"maps","name":["ada", "cy"]},{"team":"roads","name":["bob"]}]"#
    );

    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_team?select=name,...zou_rest_player(players:name)\
             &zou_rest_player=is.null",
        ))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"name":"labs","players":[]}]"#);
}

#[tokio::test]
async fn a_view_embeds_on_the_keys_of_the_tables_under_it() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop schema if exists zou_view_hidden cascade",
            "drop table if exists zou_view_tags, zou_view_books, zou_view_authors cascade",
            "create table zou_view_authors (id int primary key, name text)",
            "create table zou_view_books (id int primary key, \
             author_id int references zou_view_authors(id), title text)",
            "create table zou_view_tags (id int primary key, tag text)",
            // A view renaming the key column, which is why the name
            // cannot be what the relationship is found by.
            "create view zou_view_book_list as \
             select id as ident, author_id as written_by, title from zou_view_books",
            "create view zou_view_author_list as select id, name from zou_view_authors",
            // The key column dropped: this one has no relationship at
            // all, and asking for one is an error.
            "create view zou_view_titles as select title from zou_view_books",
            // A view over a view in a schema nobody exposes and anon
            // cannot enter.
            "create schema zou_view_hidden",
            "create view zou_view_hidden.middle as \
             select id, author_id, title from zou_view_books",
            "create view zou_view_outer as \
             select id as bid, author_id as by_who, title from zou_view_hidden.middle",
            // A junction whose table is out of reach, so the many to
            // many exists only through the view.
            "create table zou_view_hidden.book_tags (\
             book_id int references zou_view_books(id), \
             tag_id int references zou_view_tags(id), \
             primary key (book_id, tag_id))",
            "create view zou_view_junction as \
             select book_id, tag_id from zou_view_hidden.book_tags",
            "insert into zou_view_authors values (1, 'ann'), (2, 'bob')",
            "insert into zou_view_books values (10, 1, 'a1'), (11, 2, 'b1')",
            "insert into zou_view_tags values (100, 'old'), (101, 'new')",
            "insert into zou_view_hidden.book_tags values (10, 100), (10, 101)",
        ],
    )
    .await;
    let app = app(&dsn);

    // The view is the child of the key its column carries, under the
    // name it gave that column.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_view_book_list?select=title,zou_view_authors(name)&order=ident",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        body_text(res).await,
        r#"[{"title":"a1","zou_view_authors":{"name": "ann"}},{"title":"b1","zou_view_authors":{"name": "bob"}}]"#
    );

    // And the parent of it, which the table on the other end can
    // embed as readily as it embeds the real one.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_view_books?select=title,zou_view_author_list(name)&order=id",
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"title":"a1","zou_view_author_list":{"name": "ann"}},{"title":"b1","zou_view_author_list":{"name": "bob"}}]"#
    );

    // Two views over the two ends of one key are related to each
    // other, and the way back is a list.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_view_author_list?select=name,zou_view_book_list(title)&order=id",
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"name":"ann","zou_view_book_list":[{"title": "a1"}]},{"name":"bob","zou_view_book_list":[{"title": "b1"}]}]"#
    );

    // A view over a view keeps the key, and the view in between is
    // read for its parse tree without anon being able to reach it.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_view_outer?select=title,zou_view_author_list(name)&order=bid",
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"title":"a1","zou_view_author_list":{"name": "ann"}},{"title":"b1","zou_view_author_list":{"name": "bob"}}]"#
    );

    // Both ends of the junction are the view's, so the many to many
    // through it is found the same way it is found through a table.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_view_books?select=title,zou_view_tags(tag)&id=eq.10",
        ))
        .await
        .unwrap();
    assert_eq!(
        body_text(res).await,
        r#"[{"title":"a1","zou_view_tags":[{"tag": "old"}, {"tag": "new"}]}]"#
    );

    // Naming the key by its column reaches the table it is on. The
    // view holding the same key is not a second answer, or writing
    // a view would make this spelling ambiguous everywhere.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_view_books?select=title,author_id(name)&order=id",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        body_text(res).await,
        r#"[{"title":"a1","author_id":{"name": "ann"}},{"title":"b1","author_id":{"name": "bob"}}]"#
    );

    // A view that did not select the key column does not have the
    // key, and no relationship is invented for it.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_view_titles?select=title,zou_view_authors(name)",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let e: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(e["code"], "PGRST200");
}

#[tokio::test]
async fn a_json_path_reads_whatever_the_column_holds() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_json_rows cascade",
            "drop type if exists zou_json_point cascade",
            "create type zou_json_point as (x int, y int)",
            "create table zou_json_rows (\
             id int primary key, doc jsonb, at zou_json_point, arr int[], odd jsonb)",
            "insert into zou_json_rows values \
             (1, '{\"a\": {\"b\": \"7\"}, \"list\": [10, 20, 30]}', '(1,9)', '{5,6,7}', \
             '{\"a!@\": 1, \"23-x-45\": 2, \"0xy1\": 3}'), \
             (2, '{\"a\": {\"b\": \"8\"}, \"list\": [40, 50, 60]}', '(2,8)', '{8,9,10}', \
             '{\"a!@\": 4, \"23-x-45\": 5, \"0xy1\": 6}')",
        ],
    )
    .await;
    let app = app(&dsn);

    // A composite type and an array are not json, so an arrow into
    // one reads the column as json first.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_json_rows?select=id,at-%3E%3Ex,arr-%3E0&order=id",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        body_text(res).await,
        r#"[{"id":1,"x":"1","arr":5},{"id":2,"x":"2","arr":8}]"#
    );

    // The same path filters and orders, and a negative index counts
    // from the end.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_json_rows?select=id&at-%3E%3Ey=eq.8&order=arr-%3E%3E-1.desc",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"id":2}]"#);

    // A cast over a path takes brackets, since `::` binds tighter
    // than the arrow and would otherwise land on the key.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_json_rows?select=id,n:doc-%3Ea-%3E%3Eb::int&order=id",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"id":1,"n":7},{"id":2,"n":8}]"#);

    // Only the six bytes the grammar needs end a key, a dash joins
    // two pieces of one, and digits are an index only when nothing
    // but an arrow, a cast, a dot or a comma follows them.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_json_rows?select=odd-%3E%3Ea!@,odd-%3E%3E23-x-45,odd-%3E%3E0xy1&id=eq.1",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        body_text(res).await,
        r#"[{"a!@":"1","23-x-45":"2","0xy1":"3"}]"#
    );

    // A jsonb column is read as it is, so an index into a list is
    // still an index.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_json_rows?select=id&doc-%3Elist-%3E%3E1=eq.50",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"id":2}]"#);
}

#[tokio::test]
async fn a_text_search_makes_the_vector_it_needs() {
    let Some(dsn) = dsn() else { return };
    seed(
        &dsn,
        &[
            "drop table if exists zou_fts_rows cascade",
            "drop domain if exists zou_fts_vector cascade",
            "create domain zou_fts_vector as tsvector",
            "create table zou_fts_rows (\
             id int primary key, body text, doc jsonb, vec zou_fts_vector)",
            "insert into zou_fts_rows values \
             (1, 'fat cats ate rats', '{\"a\": \"fat cats ate rats\"}', \
              to_tsvector('fat cats ate rats')), \
             (2, 'ein Spass am Arbeiten', '{\"a\": \"ein Spass am Arbeiten\"}', \
              to_tsvector('ein Spass am Arbeiten'))",
        ],
    )
    .await;
    let app = app(&dsn);

    // Postgres has no `@@` over jsonb at all, so the vector has to be
    // made before the operator sees the column.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_fts_rows?select=id&doc=fts.fat"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"id":1}]"#);

    // The configuration builds the vector as well as the query. The
    // german dictionary stems Arbeiten to arbeit, and the default one
    // does not, so this pair only matches when both halves agree.
    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_fts_rows?select=id&body=plfts(german).Arbeit",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"id":2}]"#);

    // A column that is already a vector faces the operator bare, and
    // a domain over one is still one.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_fts_rows?select=id&vec=fts.rat"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"[{"id":1}]"#);
}
