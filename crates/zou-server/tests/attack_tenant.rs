//! Two projects on one node, and an attacker who holds everything the
//! first one can mint.
//!
//! The other attack suite is about a boundary inside one project: one
//! user's rows against another's, a role nobody granted, a policy read
//! past. This one is about the boundary between two projects, which is
//! a different thing and fails differently. Nothing here is a policy
//! question. Every artifact under test is a bearer credential of some
//! kind, and what has to be true is that the credential is worth
//! nothing one door along.
//!
//! Two projects, each with its own database, its own secret and its own
//! object store, and deliberately the same names inside: the same
//! bucket, the same object, the same table. An attack that only fails
//! because the names differ has not been tested, it has been avoided.
//!
//! The design says all of this is safe by construction. The gateway
//! resolves a ref, reads the entry and builds that project's router
//! with that project's secret in it, and past the front door nothing
//! knows there is more than one project on the node. A storage signing
//! token is checked inside the storage surface against the config's
//! secret, which is that router's, so a token from elsewhere cannot
//! verify. Safe by construction is worth testing precisely because it
//! is the kind of safety that quietly stops holding the day somebody
//! caches a router, shares a pool or hoists a secret, and none of the
//! surfaces underneath would notice.
//!
//! Gated on ZOU_PG_TEST_DSN like the other live suites, skips when
//! unset. The two databases are made here rather than assumed.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test attack_tenant

use std::net::SocketAddr;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;
use zou_server::sql::Pool;
use zou_server::{Config, jwt, router};

const SECRET_A: &[u8] = b"a-projects-own-secret-which-is-at-least-32-characters";
const SECRET_B: &[u8] = b"the-other-projects-secret-also-at-least-32-characters";
const DB_A: &str = "zou_xt_acme";
const DB_B: &str = "zou_xt_globex";

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

/// Point a keyword and value dsn at another database. The last dbname
/// wins in that form, so appending is enough. A url form dsn gets None
/// and the caller skips, rather than a mangled string that would
/// connect somewhere unintended.
fn with_dbname(dsn: &str, db: &str) -> Option<String> {
    match dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        true => None,
        false => Some(format!("{dsn} dbname={db}")),
    }
}

/// A connection outside the pool. create database refuses the extended
/// query protocol the pool's session api uses, and has to run before
/// there is anything to pool anyway.
async fn raw(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect to {dsn}: {e}"));
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// The two databases, made once for the whole binary however many tests
/// ask for them. Everything a single test needs of its own, which is
/// every table and every bucket it touches, is named after that test,
/// so these two are only ever created here and never emptied.
static MADE: tokio::sync::OnceCell<Option<(String, String)>> = tokio::sync::OnceCell::const_new();

async fn databases(dsn: &str) -> Option<(String, String)> {
    MADE.get_or_init(|| async {
        let a = with_dbname(dsn, DB_A)?;
        let b = with_dbname(dsn, DB_B)?;
        let admin = raw(dsn).await;
        for db in [DB_A, DB_B] {
            admin
                .batch_execute(&format!("drop database if exists {db} with (force)"))
                .await
                .expect("drop any leftover");
            admin
                .batch_execute(&format!("create database {db}"))
                .await
                .expect("create the project's database");
        }
        Some((a, b))
    })
    .await
    .clone()
}

/// One project: a router built the way the gateway builds one, with
/// that project's secret and that project's database and store.
struct Project {
    app: axum::Router,
    pool: Pool,
    secret: &'static [u8],
    _dir: tempfile::TempDir,
}

impl Project {
    fn new(dsn: &str, secret: &'static [u8]) -> Project {
        let dir = tempfile::tempdir().expect("a directory to write into");
        Project {
            app: router(Config {
                jwt_secret: secret.to_vec(),
                pg: Some(dsn.to_string()),
                objects: Some(dir.path().to_string_lossy().to_string()),
                mailer_autoconfirm: true,
                ..Config::default()
            })
            .expect("router builds"),
            pool: Pool::new(dsn, 4).expect("dsn parses"),
            secret,
            _dir: dir,
        }
    }

    /// This project's router on a real port, since a websocket needs
    /// one and oneshot does not upgrade.
    async fn serving(&self) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
        let at = listener.local_addr().expect("the port");
        let app = self.app.clone();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        at
    }

    fn anon(&self) -> String {
        jwt::mint(&jwt::key_claims("anon"), self.secret)
    }

    fn service(&self) -> String {
        jwt::mint(&jwt::key_claims("service_role"), self.secret)
    }

    /// A user token the way this project's own auth would mint one.
    fn user(&self, sub: &str) -> String {
        jwt::mint(
            &serde_json::json!({
                "sub": sub,
                "role": "authenticated",
                "email": format!("{sub}@example.com"),
            }),
            self.secret,
        )
    }
}

/// The two dsns, or None when there is no server to make them on.
async fn dsns() -> Option<(String, String)> {
    databases(&dsn()?).await
}

/// Both projects, or None when there is no database to build them on.
async fn projects() -> Option<(Project, Project)> {
    let (a, b) = dsns().await?;
    Some((Project::new(&a, SECRET_A), Project::new(&b, SECRET_B)))
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

/// One request at one project, with whatever credentials the caller
/// wants on it and no others.
async fn ask(
    at: &Project,
    method: &str,
    path: &str,
    said: &[(&str, &str)],
    mime: &str,
    body: &str,
) -> Answer {
    let mut req = Request::builder().method(method).uri(path);
    for (name, value) in said {
        req = req.header(*name, *value);
    }
    if !mime.is_empty() {
        req = req.header("content-type", mime);
    }
    let res = at
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

/// A table of the same name in both projects, each holding one row that
/// says which project it came from, readable by anyone the project let
/// through the door. No policy: a policy would be the other suite's
/// subject, and a table anybody can read is the harder case here since
/// the only thing between a caller and the row is which project they
/// are talking to.
async fn table(p: &Project, name: &str, whose: &str) {
    let sess = p.pool.unscoped().await.expect("unscoped");
    for stmt in [
        format!("drop table if exists public.{name}"),
        format!("create table public.{name} (id int primary key, whose text)"),
        format!("insert into public.{name} values (1, '{whose}')"),
        format!("grant select, insert, update, delete on public.{name} to anon, authenticated"),
    ] {
        sess.execute(&stmt, &[]).await.expect(&stmt);
    }
    sess.commit().await.expect("finish");
}

/// A bucket of the same name in both projects, with one object of the
/// same name in each, holding bytes that say which project wrote them.
async fn bucket(p: &Project, name: &str, object: &str, whose: &str) {
    let sess = p.pool.unscoped().await.expect("unscoped");
    sess.execute("set storage.allow_delete_query = 'true'", &[])
        .await
        .expect("open the guard");
    sess.execute("delete from storage.objects where bucket_id = $1", &[&name])
        .await
        .expect("clear objects");
    sess.execute("delete from storage.buckets where id = $1", &[&name])
        .await
        .expect("clear the bucket");
    sess.execute(
        "insert into storage.buckets (id, name, public) values ($1, $1, false)",
        &[&name],
    )
    .await
    .expect("make the bucket");
    sess.commit().await.expect("finish");

    let answer = ask(
        p,
        "POST",
        &format!("/storage/v1/object/{name}/{object}"),
        &[("authorization", &format!("Bearer {}", p.service()))],
        "text/plain",
        whose,
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
}

/// The url out of a signing answer with the prefix a client would put
/// back on it, since what comes back is relative to /storage/v1.
fn spendable(answer: &Answer, field: &str) -> String {
    let url = answer.json()[field]
        .as_str()
        .unwrap_or_else(|| panic!("no {field} in {}", answer.text()))
        .to_string();
    format!("/storage/v1{url}")
}

/// Nothing of that name is in the project's store. The status line is
/// not the thing asked: a missing object comes back as a 400 whose body
/// carries the 404, which is what the reference server does and what
/// clients read, so the body is where the answer is.
async fn absent(at: &Project, path: &str) {
    let answer = ask(
        at,
        "GET",
        path,
        &[("authorization", &format!("Bearer {}", at.service()))],
        "",
        "",
    )
    .await;
    assert!(
        answer.status.is_client_error() && answer.json()["error"] == "not_found",
        "{path} exists: {} {}",
        answer.status,
        answer.text()
    );
}

/// What every test here really asks. A refusal is necessary and not
/// sufficient: a 500 is a refusal too, and so is a 404 that happens to
/// be right for the wrong reason. What must never appear anywhere in
/// the answer is a word only the other project knows.
fn refused(what: &str, answer: &Answer, secret_word: &str) {
    assert!(
        answer.status.is_client_error(),
        "{what}: answered {} rather than refusing: {}",
        answer.status,
        answer.text()
    );
    assert!(
        !answer.text().contains(secret_word),
        "{what}: the answer carried the other project's data: {}",
        answer.text()
    );
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A socket at this project, opened with whichever key the caller
/// hands it, which is the point when the key is the other project's.
async fn socket(at: SocketAddr, key: &str) -> Result<Socket, String> {
    let url = format!("ws://{at}/realtime/v1/websocket?apikey={key}&vsn=2.0.0");
    match tokio_tungstenite::connect_async(url).await {
        Ok((socket, _)) => Ok(socket),
        Err(e) => Err(e.to_string()),
    }
}

/// A join on a private room as whoever holds `token`, and what came
/// back, which is a status rather than an assertion because half of
/// these are meant to be refused.
async fn joined(socket: &mut Socket, topic: &str, token: &str) -> Value {
    let frame = format!(
        r#"["1","1","{topic}","phx_join",{{"config":{{"private":true}},"access_token":"{token}"}}]"#
    );
    socket
        .send(Message::Text(frame.into()))
        .await
        .expect("the socket takes it");
    match tokio::time::timeout(Duration::from_secs(5), socket.next()).await {
        Ok(Some(Ok(Message::Text(text)))) => serde_json::from_str(&text).expect("json"),
        // A socket that hung up rather than answering has refused, and
        // that reads the same to a client as a reply that said no.
        other => serde_json::json!({ "closed": format!("{other:?}") }),
    }
}

/// Nothing arrives on this socket for long enough to mean it.
async fn quiet(socket: &mut Socket, what: &str) {
    let heard = tokio::time::timeout(Duration::from_millis(1500), socket.next()).await;
    assert!(heard.is_err(), "{what}: {heard:?}");
}

/// This project's server has said listen on this project's database.
/// Filtered by database, because the view is the whole cluster and the
/// other project's server is listening at the same time.
async fn listening(p: &Project) {
    for _ in 0..100 {
        let sess = p.pool.unscoped().await.expect("connect");
        let rows = sess
            .query(
                "select count(*) from pg_stat_activity
                 where query = 'listen zou_realtime' and datname = current_database()",
                &[],
            )
            .await
            .expect("the view reads");
        let waiting: i64 = rows[0].get(0);
        sess.commit().await.expect("done");
        if waiting > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("this project's server never started listening for its database's sends");
}

/// Anybody signed in may be on any room here. A policy this open is
/// deliberate: the question is not whether a policy holds, it is
/// whether being allowed on a room at one project is worth anything at
/// the other, and the loosest policy is the sharpest way to ask.
async fn rooms(p: &Project) {
    let sess = p.pool.unscoped().await.expect("unscoped");
    for stmt in [
        "drop policy if exists xt_rooms on realtime.messages",
        "create policy xt_rooms on realtime.messages for select to authenticated using (true)",
    ] {
        sess.execute(stmt, &[]).await.expect(stmt);
    }
    sess.commit().await.expect("the policy lands");
}

#[tokio::test]
async fn no_key_or_token_the_first_project_mints_is_worth_anything_at_the_second() {
    let Some((a, b)) = projects().await else {
        return;
    };
    table(&a, "xt_keys", "acme").await;
    table(&b, "xt_keys", "globex").await;

    // The anon key, the service role key and a user's access token.
    // Three different roles and three different reasons a server might
    // let one through, and the same secret behind all three.
    for (what, key, token) in [
        ("the anon key", a.anon(), None),
        ("the service role key", a.service(), None),
        ("a user token", a.anon(), Some(a.user("someone"))),
    ] {
        let mut said = vec![("apikey", key.as_str())];
        let bearer;
        if let Some(token) = &token {
            bearer = format!("Bearer {token}");
            said.push(("authorization", &bearer));
        }
        let answer = ask(&b, "GET", "/rest/v1/xt_keys", &said, "", "").await;
        refused(what, &answer, "globex");

        // And the same credential at home, so the test is about whose
        // project it is and not about the credential being broken.
        let answer = ask(&a, "GET", "/rest/v1/xt_keys", &said, "", "").await;
        assert_eq!(
            answer.status,
            StatusCode::OK,
            "{what} did not work at its own project: {}",
            answer.text()
        );
        assert!(answer.text().contains("acme"), "{}", answer.text());
    }
}

#[tokio::test]
async fn a_download_url_the_first_project_signed_is_not_spendable_at_the_second() {
    let Some((a, b)) = projects().await else {
        return;
    };
    bucket(&a, "xt-read", "note.txt", "acme").await;
    bucket(&b, "xt-read", "note.txt", "globex").await;

    let answer = ask(
        &a,
        "POST",
        "/storage/v1/object/sign/xt-read/note.txt",
        &[("authorization", &format!("Bearer {}", a.service()))],
        "application/json",
        r#"{"expiresIn":600}"#,
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    let url = spendable(&answer, "signedURL");

    // The url names a bucket and an object that both projects have, so
    // nothing about the path is wrong at the second one. Only the
    // signature is, and the signature is the whole of the boundary.
    let answer = ask(&b, "GET", &url, &[], "", "").await;
    refused("a url signed elsewhere", &answer, "globex");

    let answer = ask(&a, "GET", &url, &[], "", "").await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert_eq!(answer.text(), "acme");
}

#[tokio::test]
async fn an_upload_url_the_first_project_signed_writes_nothing_at_the_second() {
    let Some((a, b)) = projects().await else {
        return;
    };
    bucket(&a, "xt-write", "note.txt", "acme").await;
    bucket(&b, "xt-write", "note.txt", "globex").await;

    let answer = ask(
        &a,
        "POST",
        "/storage/v1/object/upload/sign/xt-write/planted.txt",
        &[("authorization", &format!("Bearer {}", a.service()))],
        "application/json",
        "{}",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    let url = spendable(&answer, "url");

    let answer = ask(&b, "PUT", &url, &[], "text/plain", "planted by acme").await;
    assert!(
        answer.status.is_client_error(),
        "an upload url signed elsewhere wrote: {} {}",
        answer.status,
        answer.text()
    );

    // A refusal that still left the object behind would be the worse
    // half of this, so the store is asked rather than the answer.
    absent(&b, "/storage/v1/object/xt-write/planted.txt").await;
}

#[tokio::test]
async fn a_resumable_upload_started_at_the_first_project_cannot_be_continued_at_the_second() {
    let Some((a, b)) = projects().await else {
        return;
    };
    bucket(&a, "xt-tus", "note.txt", "acme").await;
    bucket(&b, "xt-tus", "note.txt", "globex").await;

    let meta = |name: &str| {
        use base64ct::Encoding as _;
        format!(
            "bucketName {},objectName {}",
            base64ct::Base64::encode_string(b"xt-tus"),
            base64ct::Base64::encode_string(name.as_bytes()),
        )
    };
    let answer = ask(
        &a,
        "POST",
        "/storage/v1/upload/resumable",
        &[
            ("authorization", &format!("Bearer {}", a.service())),
            ("tus-resumable", "1.0.0"),
            ("upload-length", "6"),
            ("upload-metadata", &meta("resumed.txt")),
        ],
        "",
        "",
    )
    .await;
    assert_eq!(answer.status, StatusCode::CREATED, "{}", answer.text());
    let location = answer.header("location").expect("a location").to_string();
    let at = location
        .find("/upload/resumable/")
        .expect("a resumable path in the location");
    let path = format!("/storage/v1{}", &location[at..]);

    // The upload url is a bearer credential like the signed ones: it
    // names an upload nobody else was told about. What makes it one
    // project's is who it was created by, so continuing it elsewhere
    // has to be as good as making it up.
    let answer = ask(
        &b,
        "PATCH",
        &path,
        &[
            ("authorization", &format!("Bearer {}", a.service())),
            ("tus-resumable", "1.0.0"),
            ("upload-offset", "0"),
        ],
        "application/offset+octet-stream",
        "planted",
    )
    .await;
    assert!(
        answer.status.is_client_error(),
        "an upload started elsewhere was continued: {} {}",
        answer.status,
        answer.text()
    );

    absent(&b, "/storage/v1/object/xt-tus/resumed.txt").await;
}

#[tokio::test]
async fn a_session_at_the_first_project_is_not_a_session_at_the_second() {
    let Some((a, b)) = projects().await else {
        return;
    };

    let signup = ask(
        &a,
        "POST",
        "/auth/v1/signup",
        &[("apikey", &a.anon())],
        "application/json",
        r#"{"email":"crossing@example.com","password":"a-long-enough-password"}"#,
    )
    .await;
    assert_eq!(signup.status, StatusCode::OK, "{}", signup.text());
    let session = signup.json();
    let refresh = session["refresh_token"]
        .as_str()
        .expect("a refresh token")
        .to_string();
    let access = session["access_token"]
        .as_str()
        .expect("an access token")
        .to_string();

    // A refresh token is a random string rather than a signed one, so
    // this is not the signature being checked, it is that the row it
    // names is in the first project's database and nowhere else.
    let answer = ask(
        &b,
        "POST",
        "/auth/v1/token?grant_type=refresh_token",
        &[("apikey", &b.anon())],
        "application/json",
        &serde_json::json!({ "refresh_token": refresh }).to_string(),
    )
    .await;
    assert!(
        answer.status.is_client_error(),
        "a refresh token from elsewhere minted a session: {} {}",
        answer.status,
        answer.text()
    );
    assert_eq!(
        answer.json()["access_token"],
        Value::Null,
        "{}",
        answer.text()
    );

    // The access token is signed, and signed with the other secret, so
    // the surface that reads a session refuses it too.
    let answer = ask(
        &b,
        "GET",
        "/auth/v1/user",
        &[
            ("apikey", &b.anon()),
            ("authorization", &format!("Bearer {access}")),
        ],
        "",
        "",
    )
    .await;
    assert!(
        answer.status.is_client_error(),
        "an access token from elsewhere named a user: {} {}",
        answer.status,
        answer.text()
    );
    assert_eq!(answer.json()["email"], Value::Null, "{}", answer.text());

    // And the account itself did not appear at the second project just
    // because somebody asked about it there.
    let answer = ask(
        &b,
        "POST",
        "/auth/v1/token?grant_type=password",
        &[("apikey", &b.anon())],
        "application/json",
        r#"{"email":"crossing@example.com","password":"a-long-enough-password"}"#,
    )
    .await;
    assert!(
        answer.status.is_client_error(),
        "the account signed in at a project it was never created at: {}",
        answer.text()
    );
}

#[tokio::test]
async fn a_link_the_first_project_mailed_lands_nowhere_at_the_second() {
    let Some((a, b)) = projects().await else {
        return;
    };

    // Both projects have an account at this address, which is the case
    // that matters: the same person really does sign up at two
    // services, and a link one of them mailed must not be a way into
    // the other's account just because the address matches.
    let who = "linked@example.com";
    for at in [&a, &b] {
        let answer = ask(
            at,
            "POST",
            "/auth/v1/signup",
            &[("apikey", &at.anon())],
            "application/json",
            &serde_json::json!({ "email": who, "password": "a-long-enough-password" }).to_string(),
        )
        .await;
        assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    }

    for kind in ["recovery", "magiclink", "invite"] {
        let made = ask(
            &a,
            "POST",
            "/auth/v1/admin/generate_link",
            &[
                ("apikey", &a.service()),
                ("authorization", &format!("Bearer {}", a.service())),
            ],
            "application/json",
            &serde_json::json!({
                "type": kind,
                "email": match kind {
                    // An invite is for somebody who is not there yet,
                    // and the second project has no such account
                    // either, so following it there must not make one.
                    "invite" => "invited@example.com",
                    _ => who,
                },
            })
            .to_string(),
        )
        .await;
        assert_eq!(made.status, StatusCode::OK, "{kind}: {}", made.text());
        let link = made.json()["action_link"]
            .as_str()
            .expect("a link to follow")
            .to_string();
        let (_, query) = link.split_once('?').expect("a link carries its token");

        // A mail client sends no apikey, so this is exactly what
        // clicking it at the wrong project does.
        let answer = ask(&b, "GET", &format!("/auth/v1/verify?{query}"), &[], "", "").await;
        let landed = answer.header("location").unwrap_or_default().to_string();
        assert!(
            !landed.contains("access_token"),
            "{kind}: a link from another project handed out a session: {landed}"
        );
        assert!(
            !answer.text().contains("access_token"),
            "{kind}: a link from another project handed out a session: {}",
            answer.text()
        );
    }

    // And the invite did not quietly create the account at the second
    // project on the way past.
    let answer = ask(
        &b,
        "POST",
        "/auth/v1/recover",
        &[("apikey", &b.anon())],
        "application/json",
        r#"{"email":"invited@example.com"}"#,
    )
    .await;
    let sess = b.pool.unscoped().await.expect("unscoped");
    let rows = sess
        .query(
            "select id from auth.users where email = $1",
            &[&"invited@example.com"],
        )
        .await
        .expect("ask who is there");
    sess.commit().await.expect("finish");
    assert!(
        rows.is_empty(),
        "an invite from another project made an account here: {} {}",
        answer.status,
        answer.text()
    );
}

#[tokio::test]
async fn a_socket_at_the_second_project_takes_nothing_the_first_project_signed() {
    let Some((a, b)) = projects().await else {
        return;
    };
    rooms(&b).await;
    let at_b = b.serving().await;

    // The key on the url is how a socket says which project it is
    // talking to, and it is checked before anything else, so the first
    // of these may be refused at the upgrade rather than at the join.
    match socket(at_b, &a.anon()).await {
        Err(_) => {}
        Ok(mut open) => {
            let reply = joined(&mut open, "realtime:xt-room", &a.user("stranger")).await;
            assert_ne!(
                reply[4]["status"], "ok",
                "another project's anon key opened a room here: {reply}"
            );
        }
    }

    // And with this project's own anon key on the url, which anybody
    // reading a client bundle has, but the other project's user token
    // in the join, which is the credential that decides who you are.
    let mut open = socket(at_b, &b.anon()).await.expect("its own key connects");
    let reply = joined(&mut open, "realtime:xt-room", &a.user("stranger")).await;
    assert_ne!(
        reply[4]["status"], "ok",
        "another project's user token joined a private room here: {reply}"
    );

    // The same join with a token this project signed is fine, so the
    // refusal above is about whose token it was and not about the room.
    let mut mine = socket(at_b, &b.anon()).await.expect("its own key connects");
    let reply = joined(&mut mine, "realtime:xt-room", &b.user("resident")).await;
    assert_eq!(reply[4]["status"], "ok", "{reply}");
}

#[tokio::test]
async fn a_broadcast_at_the_first_project_is_not_heard_at_the_second() {
    let Some((a, b)) = projects().await else {
        return;
    };
    rooms(&a).await;
    rooms(&b).await;
    let (at_a, at_b) = (a.serving().await, b.serving().await);

    // The same room name at both, which is the whole point: two
    // projects using the same obvious name for the same obvious thing
    // is the normal case, not a contrived one.
    let room = "realtime:xt-shared";
    let mut here = socket(at_a, &a.anon())
        .await
        .expect("a socket at the first");
    let mut there = socket(at_b, &b.anon())
        .await
        .expect("a socket at the second");
    let reply = joined(&mut here, room, &a.user("one")).await;
    assert_eq!(reply[4]["status"], "ok", "{reply}");
    let reply = joined(&mut there, room, &b.user("two")).await;
    assert_eq!(reply[4]["status"], "ok", "{reply}");
    listening(&a).await;
    listening(&b).await;

    let sess = a.pool.unscoped().await.expect("unscoped");
    sess.execute(
        "select realtime.send('{\"whose\": \"acme\"}'::jsonb, 'greeting', 'xt-shared')",
        &[],
    )
    .await
    .expect("the send runs");
    sess.commit().await.expect("finish");

    let heard = tokio::time::timeout(Duration::from_secs(10), here.next())
        .await
        .expect("the room it was sent to hears it")
        .expect("the socket is open")
        .expect("a message");
    let frame = match heard {
        Message::Binary(bytes) => String::from_utf8_lossy(&bytes).to_string(),
        other => panic!("{other:?} is not the binary broadcast a 2.0.0 socket takes"),
    };
    assert!(frame.contains("acme"), "{frame}");

    // Same room name, same server, different project. A fanout keyed
    // on the room and not on the project would deliver here, and this
    // is the socket that would have been listening.
    quiet(
        &mut there,
        "a broadcast crossed from one project to another",
    )
    .await;
}

/// The two refs the front door tests use. They share a prefix on
/// purpose: `acme` is a prefix of `acme-prod` as a string, and the only
/// thing that keeps them apart is that a path is matched by segment and
/// a host by label rather than by starts_with.
const REF_A: &str = "acme";
const REF_B: &str = "acme-prod";
const DOMAIN: &str = "zou.example.com";

/// A front door over both projects, which is the deployment the refs
/// are for: one process, one port, two projects, and the ref in the
/// host or in the path deciding which one a request is for.
struct Door {
    app: axum::Router,
    _dirs: Vec<tempfile::TempDir>,
}

struct Both {
    dsns: Vec<(String, String)>,
    dirs: Vec<(String, String)>,
}

impl zou_server::attach::Backend for Both {
    fn up(&self, entry: &zou_store::registry::Tenant) -> Result<Config, String> {
        let find = |list: &Vec<(String, String)>| {
            list.iter()
                .find(|(name, _)| *name == entry.tenant_ref)
                .map(|(_, value)| value.clone())
                .ok_or_else(|| format!("no such project: {}", entry.tenant_ref))
        };
        Ok(Config {
            jwt_secret: entry.jwt_secret.as_bytes().to_vec(),
            pg: Some(find(&self.dsns)?),
            objects: Some(find(&self.dirs)?),
            ..Config::default()
        })
    }

    fn down(&self, _tenant_ref: &str) {}
}

/// Both projects behind one front door, routed by host and by path at
/// once, which is how a node that serves a custom domain and a default
/// url for the same project is configured.
async fn door(a: &str, b: &str) -> Door {
    use std::sync::Arc;
    use zou_server::attach::Attached;
    use zou_server::gateway::gateway;
    use zou_server::tenant::{Registry, Routing};
    use zou_store::registry::{self, Tenant};
    use zou_store::{CasStore, open_store};

    let mut dirs = Vec::new();
    let mut objects = Vec::new();
    for name in [REF_A, REF_B] {
        let dir = tempfile::tempdir().expect("a directory to write into");
        objects.push((name.to_string(), dir.path().to_string_lossy().to_string()));
        dirs.push(dir);
    }
    let registry_dir = tempfile::tempdir().expect("a directory to write into");
    let store: Arc<dyn CasStore> =
        Arc::from(open_store(&registry_dir.path().to_string_lossy()).expect("a store opens"));
    for (name, secret) in [
        (REF_A, std::str::from_utf8(SECRET_A).unwrap()),
        (REF_B, std::str::from_utf8(SECRET_B).unwrap()),
    ] {
        registry::create(store.as_ref(), &Tenant::new(name, secret, 1)).expect("it registers");
    }
    dirs.push(registry_dir);

    let backend = Both {
        dsns: vec![
            (REF_A.to_string(), a.to_string()),
            (REF_B.to_string(), b.to_string()),
        ],
        dirs: objects,
    };
    Door {
        app: gateway(
            Routing {
                domains: vec![DOMAIN.to_string()],
                path_prefix: true,
            },
            Arc::new(Registry::new(store)),
            Arc::new(Attached::new(Arc::new(backend))),
        ),
        _dirs: dirs,
    }
}

/// One request at the front door, with whatever host and whatever path
/// the caller wants, which is the whole subject here.
async fn knock(door: &Door, host: &str, path: &str, key: &str) -> (StatusCode, String) {
    let req = Request::builder()
        .method("GET")
        .uri(path)
        .header("host", host)
        .header("apikey", key)
        .body(Body::empty())
        .unwrap();
    let res = door
        .app
        .clone()
        .oneshot(req)
        .await
        .expect("the front door answers");
    let status = res.status();
    let bytes = to_bytes(res.into_body(), 1 << 20).await.expect("body");
    (status, String::from_utf8_lossy(&bytes).to_string())
}

#[tokio::test]
async fn a_host_naming_one_project_and_a_path_naming_the_other_reads_neither_wrongly() {
    let Some((a, b)) = projects().await else {
        return;
    };
    table(&a, "xt_gate", REF_A).await;
    table(&b, "xt_gate", REF_B).await;
    let (dsn_a, dsn_b) = dsns().await.expect("the databases are there");
    let door = door(&dsn_a, &dsn_b).await;

    let key_a = jwt::mint(&jwt::key_claims("anon"), SECRET_A);
    let key_b = jwt::mint(&jwt::key_claims("anon"), SECRET_B);
    let crossed = format!("/{REF_B}/rest/v1/xt_gate?select=whose");
    let plain = "/rest/v1/xt_gate?select=whose";

    // What each half of the request does on its own, so the answer to
    // the two of them together means something.
    let (status, body) = knock(&door, &format!("{REF_A}.{DOMAIN}"), plain, &key_a).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains(REF_A), "{body}");
    let (status, body) = knock(&door, "node.internal", &crossed, &key_b).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains(REF_B), "{body}");

    // Now both at once, naming different projects. The host wins:
    // Routing::resolve asks the host label first and returns the path
    // untouched, so the request goes to the project the host named with
    // the other project's ref still on the front of the path, where
    // nothing is mounted. That is a 404, and it is a 404 whichever key
    // is on the request, because the route is decided before the key is
    // looked at. Writing it down is the point of the test: a reader
    // should not have to guess which half of a request wins, and a
    // change that made the path win would land here.
    for (whose, key) in [("its own", &key_a), ("the other project's", &key_b)] {
        let (status, body) = knock(&door, &format!("{REF_A}.{DOMAIN}"), &crossed, key).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "the host named one project and the path another, with {whose} key: {status} {body}"
        );
        assert!(
            !body.contains(REF_A) && !body.contains(REF_B),
            "neither project's rows should be in this: {body}"
        );
    }
}

#[tokio::test]
async fn two_refs_that_share_a_prefix_are_two_projects() {
    let Some((a, b)) = projects().await else {
        return;
    };
    table(&a, "xt_prefix", REF_A).await;
    table(&b, "xt_prefix", REF_B).await;
    let (dsn_a, dsn_b) = dsns().await.expect("the databases are there");
    let door = door(&dsn_a, &dsn_b).await;

    // Each ref at its own door with its own key, so the two that follow
    // are about the crossing and not about the setup.
    for (name, secret, whose) in [(REF_A, SECRET_A, REF_A), (REF_B, SECRET_B, REF_B)] {
        for host in [format!("{name}.{DOMAIN}"), "node.internal".to_string()] {
            let path = match host.ends_with(DOMAIN) {
                true => "/rest/v1/xt_prefix?select=whose".to_string(),
                false => format!("/{name}/rest/v1/xt_prefix?select=whose"),
            };
            let (status, body) = knock(
                &door,
                &host,
                &path,
                &jwt::mint(&jwt::key_claims("anon"), secret),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{name} at {host}: {body}");
            assert!(body.contains(whose), "{name} at {host}: {body}");
        }
    }

    // And now the crossings, in both directions and by both routes. A
    // prefix match rather than a segment match would send acme-prod's
    // requests to acme, and a suffix strip that took a label off
    // without checking would do the same for the host.
    for (host, path, key, other) in [
        (
            format!("{REF_A}.{DOMAIN}"),
            "/rest/v1/xt_prefix?select=whose".to_string(),
            SECRET_B,
            REF_B,
        ),
        (
            format!("{REF_B}.{DOMAIN}"),
            "/rest/v1/xt_prefix?select=whose".to_string(),
            SECRET_A,
            REF_A,
        ),
        (
            "node.internal".to_string(),
            format!("/{REF_A}/rest/v1/xt_prefix?select=whose"),
            SECRET_B,
            REF_B,
        ),
        (
            "node.internal".to_string(),
            format!("/{REF_B}/rest/v1/xt_prefix?select=whose"),
            SECRET_A,
            REF_A,
        ),
    ] {
        let (status, body) = knock(
            &door,
            &host,
            &path,
            &jwt::mint(&jwt::key_claims("anon"), key),
        )
        .await;
        assert!(
            status.is_client_error(),
            "a key from {other} was taken at {host}{path}: {status} {body}"
        );
        assert!(!body.contains(other), "{host}{path}: {body}");
    }
}
