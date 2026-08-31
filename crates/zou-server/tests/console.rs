//! The web admin at `/_zou` against a live postgres.
//!
//! Gated on ZOU_PG_TEST_DSN like the pool and rest suites. The unit
//! tests beside the module can only reach the folding, because neither
//! a `Row` nor a `SimpleQueryRow` can be built outside the driver, so
//! everything about what the console actually answers is here.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test console

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

fn app(dsn: &str) -> axum::Router {
    router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.to_string()),
        ..Config::default()
    })
    .expect("router builds")
}

fn service_key() -> String {
    jwt::mint(&jwt::key_claims("service_role"), SECRET)
}

/// Run `sql` through the console and hand back the whole answer body.
async fn run(app: &axum::Router, sql: &str) -> serde_json::Value {
    let req = Request::builder()
        .method("POST")
        .uri("/_zou/api/sql")
        .header("authorization", format!("Bearer {}", service_key()))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::json!({ "query": sql }).to_string()))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "console refused {sql}");
    let bytes = to_bytes(res.into_body(), 1 << 22).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// The grid of the only statement in `sql`.
async fn grid(app: &axum::Router, sql: &str) -> serde_json::Value {
    let body = run(app, sql).await;
    assert_eq!(body["error"], serde_json::Value::Null, "{body}");
    body["results"][0].clone()
}

#[tokio::test]
async fn a_select_comes_back_as_a_labelled_grid_of_text() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let result = grid(
        &app,
        "select 1 as n, 'x' as s, null::text as nothing, 1.50::numeric as money",
    )
    .await;
    assert_eq!(
        result["columns"],
        serde_json::json!(["n", "s", "nothing", "money"])
    );
    // Every value is the text postgres printed, not a driver's
    // rendering of a type it had to guess at, which is why the numeric
    // keeps its trailing zero.
    assert_eq!(
        result["rows"],
        serde_json::json!([["1", "x", serde_json::Value::Null, "1.50"]])
    );
}

#[tokio::test]
async fn several_statements_get_several_answers_in_order() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let body = run(&app, "select 1 as a; select 2 as b, 3 as c").await;
    assert_eq!(body["error"], serde_json::Value::Null, "{body}");
    let results = body["results"].as_array().expect("an array");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["columns"], serde_json::json!(["a"]));
    assert_eq!(results[1]["columns"], serde_json::json!(["b", "c"]));
    assert!(body["ms"].as_f64().expect("a duration") >= 0.0);
}

#[tokio::test]
async fn a_statement_that_returns_nothing_says_how_many_rows_it_touched() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let _ = run(
        &app,
        "drop table if exists console_touched;
         create table console_touched (n int)",
    )
    .await;
    let result = grid(
        &app,
        "insert into console_touched select generate_series(1, 3)",
    )
    .await;
    assert_eq!(result["columns"], serde_json::json!([]));
    assert_eq!(result["touched"], 3);
    let _ = run(&app, "drop table console_touched").await;
}

#[tokio::test]
async fn a_broken_statement_comes_back_as_something_to_act_on() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    // A refusal from postgres is an answer and not a failure of the
    // request, so the status is 200 and the error is in the body. A
    // console that 500'd on a typo would be a console whose network
    // tab was full of red for the ordinary case.
    let body = run(&app, "select * from no_such_table_here").await;
    assert_eq!(body["error"]["code"], "42P01");
    assert!(
        body["error"]["message"]
            .as_str()
            .expect("a message")
            .contains("no_such_table_here")
    );
}

#[tokio::test]
async fn a_batch_that_leaves_a_transaction_open_is_rolled_back_not_pooled() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let _ = run(
        &app,
        "drop table if exists console_left_open;
         create table console_left_open (n int)",
    )
    .await;
    // Nothing here holds a connection between one press of the button
    // and the next, so an unfinished transaction can only be undone.
    // The alternative is handing the pool a connection with somebody
    // else's uncommitted work on it.
    let _ = run(&app, "begin; insert into console_left_open values (1)").await;
    let after = grid(&app, "select count(*)::text from console_left_open").await;
    assert_eq!(after["rows"], serde_json::json!([["0"]]));
    // And the connection that batch ran on is usable again rather than
    // stuck inside a transaction that never ended.
    let alive = grid(&app, "select 1 as n").await;
    assert_eq!(alive["rows"], serde_json::json!([["1"]]));
    let _ = run(&app, "drop table console_left_open").await;
}

#[tokio::test]
async fn a_batch_that_commits_keeps_what_it_wrote() {
    let Some(dsn) = dsn() else { return };
    let app = app(&dsn);
    let _ = run(
        &app,
        "drop table if exists console_committed;
         create table console_committed (n int)",
    )
    .await;
    let _ = run(
        &app,
        "begin; insert into console_committed values (7); commit",
    )
    .await;
    let after = grid(&app, "select n::text from console_committed").await;
    assert_eq!(after["rows"], serde_json::json!([["7"]]));
    let _ = run(&app, "drop table console_committed").await;
}

#[tokio::test]
async fn the_catalog_lists_a_table_with_its_columns_and_its_key() {
    let Some(dsn) = dsn() else { return };
    let pool = Pool::new(&dsn, 1).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    for stmt in [
        "drop table if exists console_catalog",
        "create table console_catalog (
             id bigint generated always as identity primary key,
             title text not null,
             done boolean default false)",
    ] {
        sess.execute(stmt, &[]).await.expect(stmt);
    }
    sess.commit().await.expect("park");

    let req = Request::builder()
        .uri("/_zou/api/catalog")
        .header("authorization", format!("Bearer {}", service_key()))
        .body(Body::empty())
        .unwrap();
    let res = app(&dsn).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = to_bytes(res.into_body(), 1 << 22).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    let public = body["schemas"]
        .as_array()
        .expect("schemas")
        .iter()
        .find(|s| s["name"] == "public")
        .expect("the public schema");
    let table = public["relations"]
        .as_array()
        .expect("relations")
        .iter()
        .find(|r| r["name"] == "console_catalog")
        .expect("the table just made");
    assert_eq!(table["kind"], "r");
    let columns = table["columns"].as_array().expect("columns");
    // In attribute order, which is the order a person wrote them and
    // the order a create statement would print them back.
    assert_eq!(columns[0]["name"], "id");
    assert_eq!(columns[0]["type"], "bigint");
    assert_eq!(columns[0]["pkey"], true);
    assert_eq!(columns[1]["name"], "title");
    assert_eq!(columns[1]["required"], true);
    assert_eq!(columns[1]["pkey"], false);
    assert_eq!(columns[2]["name"], "done");
    assert_eq!(columns[2]["default"], "false");
    // Nothing has analysed it, and a table nobody has counted is not a
    // table with no rows.
    assert_eq!(table["rows"], serde_json::Value::Null);

    // The catalogs postgres keeps for itself are not somebody's data
    // and are not in the sidebar.
    for schema in body["schemas"].as_array().expect("schemas") {
        assert_ne!(schema["name"], "pg_catalog");
        assert_ne!(schema["name"], "information_schema");
    }

    let sess = pool.unscoped().await.expect("connect");
    sess.execute("drop table console_catalog", &[])
        .await
        .expect("drop");
    sess.commit().await.expect("park");
}

#[tokio::test]
async fn more_sql_than_the_console_will_run_is_refused_rather_than_read() {
    let Some(dsn) = dsn() else { return };
    let big = "-- ".to_string() + &"x".repeat(2 << 20);
    let req = Request::builder()
        .method("POST")
        .uri("/_zou/api/sql")
        .header("authorization", format!("Bearer {}", service_key()))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::json!({ "query": big }).to_string()))
        .unwrap();
    let res = app(&dsn).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::PAYLOAD_TOO_LARGE);
}
