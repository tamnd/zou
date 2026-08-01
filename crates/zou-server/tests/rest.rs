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
        schemas: vec![],
    })
    .expect("router builds")
}

fn app_with_schemas(dsn: &str, schemas: &[&str]) -> axum::Router {
    router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        rate: None,
        jwks: None,
        schemas: schemas.iter().map(|s| s.to_string()).collect(),
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
        r#"[{"name": "drill", "zou_rpc_owners": {"name": "bob"}}]"#
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
    assert_eq!(body_text(res).await, r#"[{"id": 1},{"id": 2},{"id": 3}]"#);

    // returns table(...) rows come out as objects.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_rpc_pair?a=21"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"x": 21, "y": 42}]"#);

    // A non set function returning a rowtype is one bare object.
    let res = app
        .clone()
        .oneshot(get("/rest/v1/rpc/zou_rpc_one?select=id,name"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"{"id": 1, "name": "hammer"}"#);

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
    let res = app
        .clone()
        .oneshot(get("/rest/v1/zou_rpc_items?select=name&id=eq.99"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"name": "tmp"}]"#);

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
    assert_eq!(body_text(res).await, r#"[{"id": 1},{"id": 2}]"#);

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
    assert_eq!(
        body_text(res).await,
        r#"{"id": 1, "name": "ann", "note": null}"#
    );

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
    assert_eq!(body_text(res).await, r#"{"id": 1, "name": "ann"}"#);

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
        r#"[{"id": 1},{"id": 2, "note": "hi"},{"id": 3}]"#
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
        r#"{"id": 10, "name": "yin", "note": null}"#
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
        r#"[{"note": "hi"}]"#,
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
    assert_eq!(body_text(res).await, r#"[{"tag": "alt"}]"#);

    // No header is the first exposed schema, and with more than one
    // exposed the default still counts as negotiated.
    let res = two
        .clone()
        .oneshot(get("/rest/v1/zou_profile_rows?select=tag"))
        .await
        .unwrap();
    assert_eq!(res.headers()["content-profile"], "public");
    assert_eq!(body_text(res).await, r#"[{"tag": "pub"}]"#);

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
    assert_eq!(body_text(res).await, r#"[{"tag": "pub"}]"#);

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
    assert_eq!(body_text(res).await, r#"[{"id": 2, "tag": "alt2"}]"#);

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
    assert_eq!(body_text(res).await, r#"[{"tag": "alt"},{"tag": "alt2"}]"#);
    let res = two
        .clone()
        .oneshot(get("/rest/v1/zou_profile_rows?select=tag"))
        .await
        .unwrap();
    assert_eq!(body_text(res).await, r#"[{"tag": "pub"}]"#);

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
    assert_eq!(body_text(res).await, r#"[{"tag": "pub"}]"#);

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
            "grant all on all tables in schema public to anon, authenticated, service_role",
            "grant usage, select on all sequences in schema public \
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
