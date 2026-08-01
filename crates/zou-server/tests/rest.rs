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
        rate: None,
        jwks: None,
    })
    .expect("router builds")
}

async fn seed(dsn: &str, statements: &[&str]) {
    let pool = Pool::new(dsn, 1).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    for stmt in statements {
        sess.execute(stmt, &[]).await.expect(stmt);
    }
    sess.commit().await.expect("park");
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
    assert_eq!(body_text(res).await, r#"[{"name": "ann"},{"name": "bob"}]"#);

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
        r#"[{"name": "ann", "zou_rest_books": [{"title": "a2"}, {"title": "a1"}]},{"name": "bob", "zou_rest_books": [{"title": "b1"}]}]"#
    );

    // A filter narrows, limit and offset window, and Content-Range
    // reports the window.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_authors?select=name&name=eq.ann"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"name": "ann"}]"#);

    let res = app
        .clone()
        .oneshot(get(
            "/rest/v1/zou_rest_authors?select=name&order=name&limit=1&offset=1",
        ))
        .await
        .unwrap();
    assert_eq!(res.headers()["content-range"], "1-1/*");
    assert_eq!(body_text(res).await, r#"[{"name": "bob"}]"#);

    // The Range header pages when the query string did not.
    let mut req = get("/rest/v1/zou_rest_authors?select=name&order=name");
    req.headers_mut().insert("range", "0-0".parse().unwrap());
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.headers()["content-range"], "0-0/*");
    assert_eq!(body_text(res).await, r#"[{"name": "ann"}]"#);

    // No rows is an empty array and the unsatisfied range shape.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_authors?select=name&name=eq.zed"))
        .await
        .unwrap();
    assert_eq!(res.headers()["content-range"], "*/*");
    assert_eq!(body_text(res).await, "[]");

    // A missing table surfaces the pg error with PostgREST's 404.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_nope"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "42P01");
    assert!(body.get("details").is_some() && body.get("hint").is_some());

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
        r#"[{"name": "ann", "zou_rest_msgs": [{"id": 100}]}]"#
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
        r#"[{"price": 7, "title": "t1"},{"price": 7, "title": "t2"}]"#
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
        r#"[{"title": "c1", "zou_rest_wr_authors": {"name": "ann"}}]"#
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
    assert_eq!(
        res.headers()["location"],
        "/rest/v1/zou_rest_wr_books?id=eq.42"
    );
    assert_eq!(body_text(res).await, "");

    // merge-duplicates without on_conflict finds the pk by itself
    // and overwrites the clashing row in place.
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
    assert_eq!(res.status(), StatusCode::CREATED);
    assert_eq!(
        res.headers()["preference-applied"],
        "resolution=merge-duplicates"
    );
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_wr_books?select=title&id=eq.2"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"title": "t2x"}]"#);

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
    assert_eq!(
        body_text(res).await,
        r#"[{"title": "t2x"},{"title": "t4"}]"#
    );

    // An upsert with no pk and no on_conflict has no target to name.
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
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["code"], "PGRST100");

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
    assert_eq!(body_text(res).await, r#"[{"id": 1, "title": "p1"}]"#);

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
    assert_eq!(body_text(res).await, r#"[{"price": 9}]"#);

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
    assert_eq!(body_text(res).await, r#"[{"id": 42}]"#);

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
    assert_eq!(row["path"], "/rest/v1/zou_rest_ctx");
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
    assert_eq!(body_text(res).await, r#"[{"body": "mine"}]"#);

    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rest_notes?select=body"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, "[]");
}
