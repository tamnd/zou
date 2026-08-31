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

/// The user list for a search term and a page.
async fn listed(app: &axum::Router, q: &str, page: i64) -> serde_json::Value {
    let req = Request::builder()
        .uri(format!("/_zou/api/users?q={q}&page={page}"))
        .header("authorization", format!("Bearer {}", service_key()))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = to_bytes(res.into_body(), 1 << 22).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// Put a handful of people in `auth.users` and hand back the local part
/// they share, so a search can find them and nothing else.
async fn seed_users(dsn: &str, tag: &str, how_many: usize) {
    let pool = Pool::new(dsn, 1).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    for n in 0..how_many {
        let email = format!("{tag}{n}@example.test");
        sess.execute(
            "insert into auth.users (id, instance_id, aud, role, email,
                                     email_confirmed_at, created_at, updated_at,
                                     is_anonymous, is_sso_user)
             values (gen_random_uuid(), '00000000-0000-0000-0000-000000000000',
                     'authenticated', 'authenticated', $1,
                     case when $2::int = 0 then null else now() end,
                     now() - make_interval(secs => $2::int), now(), false, false)",
            &[&email, &(n as i32)],
        )
        .await
        .expect("insert a user");
    }
    sess.commit().await.expect("park");
}

async fn forget_users(dsn: &str, tag: &str) {
    let pool = Pool::new(dsn, 1).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        "delete from auth.users where email like $1",
        &[&format!("{tag}%@example.test")],
    )
    .await
    .expect("delete");
    sess.commit().await.expect("park");
}

#[tokio::test]
async fn the_user_list_says_who_signed_up_and_whether_they_confirmed() {
    let Some(dsn) = dsn() else { return };
    let tag = "consolelistone";
    forget_users(&dsn, tag).await;
    seed_users(&dsn, tag, 3).await;
    let app = app(&dsn);

    let body = listed(&app, tag, 0).await;
    let users = body["users"].as_array().expect("users");
    assert_eq!(users.len(), 3);
    // Newest first, and the seed made the first one newest, so the one
    // with no confirmation is at the top.
    assert_eq!(users[0]["email"], format!("{tag}0@example.test"));
    assert_eq!(users[0]["confirmed"], false);
    assert_eq!(users[1]["confirmed"], true);
    assert_eq!(users[0]["anonymous"], false);
    assert_eq!(users[0]["banned"], false);
    // Nobody signed in, and never is not the same as a moment nobody
    // recorded, so it comes back as a null and the page prints a word.
    assert_eq!(users[0]["last_sign_in_at"], serde_json::Value::Null);
    assert!(
        users[0]["created_at"]
            .as_str()
            .expect("a timestamp")
            .starts_with("20")
    );
    assert_eq!(users[0]["providers"], serde_json::json!([]));
    assert_eq!(body["more"], false);

    forget_users(&dsn, tag).await;
}

#[tokio::test]
async fn the_search_is_a_term_and_not_a_pattern() {
    let Some(dsn) = dsn() else { return };
    let tag = "consolelisttwo";
    forget_users(&dsn, tag).await;
    seed_users(&dsn, tag, 2).await;
    let app = app(&dsn);

    let body = listed(&app, &format!("{tag}1"), 0).await;
    let users = body["users"].as_array().expect("users");
    assert_eq!(users.len(), 1);
    assert_eq!(users[0]["email"], format!("{tag}1@example.test"));

    // An underscore is a character somebody typed and not a wildcard
    // standing in for the one beside it, so this matches nothing.
    let body = listed(&app, &format!("{}_1", &tag[..tag.len() - 1]), 0).await;
    assert_eq!(body["users"].as_array().expect("users").len(), 0);

    forget_users(&dsn, tag).await;
}

#[tokio::test]
async fn a_list_longer_than_a_page_says_there_is_another_one() {
    let Some(dsn) = dsn() else { return };
    let tag = "consolelistthree";
    forget_users(&dsn, tag).await;
    seed_users(&dsn, tag, 51).await;
    let app = app(&dsn);

    let first = listed(&app, tag, 0).await;
    assert_eq!(first["users"].as_array().expect("users").len(), 50);
    assert_eq!(first["more"], true);
    assert_eq!(first["page"], 0);

    let second = listed(&app, tag, 1).await;
    assert_eq!(second["users"].as_array().expect("users").len(), 1);
    // The last page is the one with no next button on it.
    assert_eq!(second["more"], false);
    assert_eq!(second["page"], 1);
    // And the pages do not overlap, which an off by one in the offset
    // would show up as here and nowhere else.
    let seen = first["users"].as_array().unwrap()[49]["id"].clone();
    assert_ne!(seen, second["users"].as_array().unwrap()[0]["id"]);

    forget_users(&dsn, tag).await;
}

/// Whatever a console GET answers with, as json.
async fn fetched(app: &axum::Router, path: &str) -> serde_json::Value {
    let req = Request::builder()
        .uri(path)
        .header("authorization", format!("Bearer {}", service_key()))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "console refused {path}");
    let bytes = to_bytes(res.into_body(), 1 << 22).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// Let this connection delete storage rows.
///
/// The schema puts a trigger in front of both storage tables that
/// refuses a delete unless the session says it means it, so that a
/// stray statement cannot orphan the files behind the rows. A test
/// clearing up after itself is the case the setting is there for. Set
/// for the session and not for the transaction, because an unscoped
/// checkout has no transaction to set it in and `set local` in one
/// would be a warning and no setting.
async fn allow_delete(sess: &zou_server::sql::Session) {
    sess.execute(
        "select set_config('storage.allow_delete_query', 'true', false)",
        &[],
    )
    .await
    .expect("allow the cleanup");
}

/// A bucket with `how_many` objects in it, named so a search can find
/// them and nothing else.
async fn seed_bucket(dsn: &str, bucket: &str, how_many: usize) {
    let pool = Pool::new(dsn, 1).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    allow_delete(&sess).await;
    sess.execute(
        "delete from storage.objects where bucket_id = $1",
        &[&bucket],
    )
    .await
    .expect("clear the bucket");
    sess.execute(
        "insert into storage.buckets (id, name, public, file_size_limit,
                                      allowed_mime_types)
         values ($1, $1, true, 1048576, array['image/png'])
         on conflict (id) do update set public = excluded.public,
                                        file_size_limit = excluded.file_size_limit,
                                        allowed_mime_types = excluded.allowed_mime_types",
        &[&bucket],
    )
    .await
    .expect("make the bucket");
    for n in 0..how_many {
        // Zero padded, because the list comes back in name order and a
        // test that asserts on it should not be asserting on whether
        // ten sorts before two.
        let name = format!("folder/file-{n:03}.png");
        sess.execute(
            "insert into storage.objects (bucket_id, name, metadata, version)
             values ($1, $2, jsonb_build_object('size', $3::bigint,
                                                'mimetype', 'image/png'),
                     'v1')",
            &[&bucket, &name, &(2048_i64)],
        )
        .await
        .expect("insert an object");
    }
    sess.commit().await.expect("park");
}

async fn forget_bucket(dsn: &str, bucket: &str) {
    let pool = Pool::new(dsn, 1).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    allow_delete(&sess).await;
    sess.execute(
        "delete from storage.objects where bucket_id = $1",
        &[&bucket],
    )
    .await
    .expect("delete the objects");
    sess.execute("delete from storage.buckets where id = $1", &[&bucket])
        .await
        .expect("delete the bucket");
    sess.commit().await.expect("park");
}

#[tokio::test]
async fn the_bucket_list_says_what_an_upload_to_each_one_may_be() {
    let Some(dsn) = dsn() else { return };
    let name = "consolebucketone";
    seed_bucket(&dsn, name, 1).await;
    let app = app(&dsn);

    let body = fetched(&app, "/_zou/api/buckets").await;
    let bucket = body["buckets"]
        .as_array()
        .expect("buckets")
        .iter()
        .find(|b| b["id"] == name)
        .expect("the bucket just made")
        .clone();
    assert_eq!(bucket["name"], name);
    assert_eq!(bucket["public"], true);
    assert_eq!(bucket["type"], "STANDARD");
    assert_eq!(bucket["file_size_limit"], 1048576);
    assert_eq!(
        bucket["allowed_mime_types"],
        serde_json::json!(["image/png"])
    );
    // No count and no total, deliberately: a bucket holds as many rows
    // as somebody uploaded and there is no index that answers how many
    // without reading all of them.
    assert_eq!(bucket["objects"], serde_json::Value::Null);

    forget_bucket(&dsn, name).await;
}

#[tokio::test]
async fn a_bucket_lists_its_files_in_name_order_with_the_size_it_recorded() {
    let Some(dsn) = dsn() else { return };
    let name = "consolebuckettwo";
    seed_bucket(&dsn, name, 3).await;
    let app = app(&dsn);

    let body = fetched(&app, &format!("/_zou/api/objects?bucket={name}&q=&page=0")).await;
    assert_eq!(body["bucket"], name);
    let objects = body["objects"].as_array().expect("objects");
    assert_eq!(objects.len(), 3);
    assert_eq!(objects[0]["name"], "folder/file-000.png");
    assert_eq!(objects[2]["name"], "folder/file-002.png");
    assert_eq!(objects[0]["size"], 2048);
    assert_eq!(objects[0]["mimetype"], "image/png");
    assert_eq!(objects[0]["version"], "v1");
    assert_eq!(body["more"], false);

    // The search is over the name and matches part of it, which is
    // what somebody has when they are looking for one file out of a
    // bucket full of them.
    let one = fetched(
        &app,
        &format!("/_zou/api/objects?bucket={name}&q=file-001&page=0"),
    )
    .await;
    assert_eq!(one["objects"].as_array().expect("objects").len(), 1);

    // Another bucket's files are not this bucket's files.
    let other = fetched(&app, "/_zou/api/objects?bucket=no-such-bucket&q=&page=0").await;
    assert_eq!(other["objects"].as_array().expect("objects").len(), 0);

    forget_bucket(&dsn, name).await;
}

#[tokio::test]
async fn a_size_nobody_wrote_is_a_size_nobody_knows_and_not_a_failed_listing() {
    let Some(dsn) = dsn() else { return };
    let name = "consolebucketthree";
    seed_bucket(&dsn, name, 0).await;
    let pool = Pool::new(&dsn, 1).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    // Metadata is whatever wrote the row, and a row somebody made by
    // hand or an upload that never finished can carry anything at all
    // where the size goes. The listing has to survive that: a cast
    // that raised here would take the whole bucket down with it.
    for (file, metadata) in [
        ("a-missing.png", "{}"),
        ("b-nonsense.png", r#"{"size": "big"}"#),
        ("c-fine.png", r#"{"size": 17}"#),
    ] {
        sess.execute(
            // Through text on the way in, so the driver takes the
            // metadata as the string it is here rather than asking for
            // a json value the test does not otherwise need.
            "insert into storage.objects (bucket_id, name, metadata)
             values ($1, $2, $3::text::jsonb)",
            &[&name, &file, &metadata],
        )
        .await
        .expect("insert an object");
    }
    sess.commit().await.expect("park");

    let body = fetched(
        &app(&dsn),
        &format!("/_zou/api/objects?bucket={name}&q=&page=0"),
    )
    .await;
    let objects = body["objects"].as_array().expect("objects");
    assert_eq!(objects.len(), 3);
    assert_eq!(objects[0]["size"], serde_json::Value::Null);
    assert_eq!(objects[1]["size"], serde_json::Value::Null);
    assert_eq!(objects[2]["size"], 17);

    forget_bucket(&dsn, name).await;
}

#[tokio::test]
async fn a_bucket_deeper_than_a_page_says_there_is_another_one() {
    let Some(dsn) = dsn() else { return };
    let name = "consolebucketfour";
    seed_bucket(&dsn, name, 51).await;
    let app = app(&dsn);

    let first = fetched(&app, &format!("/_zou/api/objects?bucket={name}&q=&page=0")).await;
    assert_eq!(first["objects"].as_array().expect("objects").len(), 50);
    assert_eq!(first["more"], true);
    let second = fetched(&app, &format!("/_zou/api/objects?bucket={name}&q=&page=1")).await;
    assert_eq!(second["objects"].as_array().expect("objects").len(), 1);
    assert_eq!(second["more"], false);
    assert_eq!(second["page"], 1);
    assert_eq!(second["objects"][0]["name"], "folder/file-050.png");

    forget_bucket(&dsn, name).await;
}

/// Write audit entries the way the auth surface writes them, one per
/// action, all naming the same actor.
async fn seed_audit(dsn: &str, who: &str, actions: &[(&str, &str)]) {
    let pool = Pool::new(dsn, 1).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        "delete from auth.audit_log_entries
          where payload->>'actor_username' = $1",
        &[&who],
    )
    .await
    .expect("clear the trail");
    for (n, (action, kind)) in actions.iter().enumerate() {
        sess.execute(
            "insert into auth.audit_log_entries (instance_id, id, payload,
                                                 created_at, ip_address)
             values ('00000000-0000-0000-0000-000000000000', gen_random_uuid(),
                     json_build_object('actor_id', gen_random_uuid()::text,
                                       'actor_username', $1::text,
                                       'actor_via_sso', false,
                                       'action', $2::text,
                                       'log_type', $3::text,
                                       'traits', json_build_object('provider', 'email')),
                     now() - make_interval(secs => $4::int), '10.0.0.7')",
            &[&who, action, kind, &(n as i32)],
        )
        .await
        .expect("insert an entry");
    }
    sess.commit().await.expect("park");
}

async fn forget_audit(dsn: &str, who: &str) {
    let pool = Pool::new(dsn, 1).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    sess.execute(
        "delete from auth.audit_log_entries
          where payload->>'actor_username' = $1",
        &[&who],
    )
    .await
    .expect("delete");
    sess.commit().await.expect("park");
}

#[tokio::test]
async fn the_audit_trail_says_who_did_what_from_where_newest_first() {
    let Some(dsn) = dsn() else { return };
    let who = "consoletrail@example.test";
    seed_audit(
        &dsn,
        who,
        &[
            ("login", "account"),
            ("logout", "account"),
            ("token_refreshed", "token"),
        ],
    )
    .await;
    let app = app(&dsn);

    let body = fetched(
        &app,
        &format!("/_zou/api/audit?q={}&page=0", who.replace('@', "%40")),
    )
    .await;
    let entries = body["entries"].as_array().expect("entries");
    assert_eq!(entries.len(), 3);
    // The seed put each entry a second further back than the one
    // before, so the first written is the newest and comes first.
    assert_eq!(entries[0]["action"], "login");
    assert_eq!(entries[0]["kind"], "account");
    assert_eq!(entries[0]["actor"], who);
    assert_eq!(entries[0]["ip"], "10.0.0.7");
    assert_eq!(entries[2]["action"], "token_refreshed");
    // The traits come back whole, because what a flow thought was worth
    // recording is not something this end can summarise.
    assert!(
        entries[0]["traits"]
            .as_str()
            .expect("traits")
            .contains("email")
    );
    // Nothing has switched the postgres copy off, so an empty page
    // would have meant a quiet project rather than a silent one.
    assert_eq!(body["writing"], true);
    assert_eq!(body["more"], false);

    // The search reaches the action as well as the actor, which is how
    // somebody asks what has been signing in.
    let logins = fetched(&app, "/_zou/api/audit?q=token_refreshed&page=0").await;
    let found = logins["entries"].as_array().expect("entries");
    assert!(!found.is_empty());
    for entry in found {
        assert_eq!(entry["action"], "token_refreshed");
    }

    forget_audit(&dsn, who).await;
}

#[tokio::test]
async fn listing_objects_without_saying_which_bucket_is_a_bad_request() {
    let Some(dsn) = dsn() else { return };
    let req = Request::builder()
        .uri("/_zou/api/objects?q=x&page=0")
        .header("authorization", format!("Bearer {}", service_key()))
        .body(Body::empty())
        .unwrap();
    let res = app(&dsn).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
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
