//! Database webhooks against a real database and a real socket.
//!
//! A webhook is a trigger that queues a row and a dispatcher that
//! makes the call, and the only interesting questions are on the seam
//! between them: whether the trigger's arguments arrive as the request
//! a receiver would recognise, whether the answer is written back
//! where pg_net writes it, and what happens when the receiver is not
//! there.
//!
//! One test rather than several, because there is one queue in a
//! database and two tests draining it at once would be two tests
//! taking each other's rows. The sections are in order and each one
//! says what it is asking.
//!
//! Gated on ZOU_PG_TEST_DSN, like the other suites that need a
//! database of their own:
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test webhook

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio_postgres::Client;
use zou_server::webhook::{Caller, Retries, Web, drain};
use zou_server::{Config, jwt, router, sql};

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

/// What a receiver does with the requests it is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Plan {
    /// Answer this, every time.
    Answer(u16),
    /// Answer 500 once and 200 after that, which is a receiver that
    /// was restarting.
    FailFirst,
    /// Take the request and never answer, which is what a timeout is
    /// made of.
    Silence,
}

/// A webhook receiver: a socket that answers the way it was told to
/// and keeps what it was sent.
struct Receiver {
    at: SocketAddr,
    got: Arc<Mutex<Vec<String>>>,
    calls: Arc<AtomicUsize>,
}

impl Receiver {
    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.at)
    }

    fn bodies(&self) -> Vec<String> {
        self.got.lock().expect("nobody panicked here").clone()
    }
}

/// A receiver on a port of its own. The thread lives as long as the
/// test does, which is the whole of one run.
fn receiver(plan: Plan) -> Receiver {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let at = listener.local_addr().expect("the port");
    let got = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let keeping = Arc::clone(&got);
    let counting = Arc::clone(&calls);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let seen = counting.fetch_add(1, Ordering::SeqCst);
            let body = read_request(&mut stream);
            keeping.lock().expect("nobody panicked here").push(body);
            let status = match plan {
                Plan::Answer(status) => status,
                Plan::FailFirst if seen == 0 => 500,
                Plan::FailFirst => 200,
                Plan::Silence => {
                    // Held rather than closed: a socket that closes is
                    // a failure to connect and this is a timeout.
                    std::thread::sleep(Duration::from_secs(30));
                    continue;
                }
            };
            let said = "{\"heard\":true}";
            let _ = write!(
                stream,
                "HTTP/1.1 {status} {}\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{said}",
                if status < 400 { "OK" } else { "Error" },
                said.len()
            );
            let _ = stream.flush();
        }
    });
    Receiver { at, got, calls }
}

/// Enough of an http request to get the body out of it.
fn read_request(stream: &mut TcpStream) -> String {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    while let Ok(read) = stream.read(&mut chunk) {
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        let text = String::from_utf8_lossy(&buffer).to_string();
        let Some(head) = text.split("\r\n\r\n").next() else {
            continue;
        };
        if !text.contains("\r\n\r\n") {
            continue;
        }
        let length: usize = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().ok())?
            })
            .unwrap_or(0);
        let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        if body.len() >= length {
            return body;
        }
    }
    String::from_utf8_lossy(&buffer)
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or("")
        .to_string()
}

/// A connection of its own, because these tests run several
/// statements at a time and the pool prepares what it is given.
async fn connected(dsn: &str) -> Client {
    let (client, connection) = dsn
        .parse::<tokio_postgres::Config>()
        .expect("a dsn")
        .connect(tokio_postgres::NoTls)
        .await
        .expect("a connection");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// A table with a webhook on it, the way the dashboard writes one.
async fn hooked(client: &Client, table: &str, url: &str, timeout_ms: u32) {
    client
        .batch_execute(&format!(
            "drop table if exists {table};
             create table {table} (id bigint primary key, note text);
             create trigger {table}_webhook after insert on {table}
                 for each row execute function supabase_functions.http_request(
                     '{url}', 'POST', '{{\"Content-Type\":\"application/json\"}}', '{{}}', '{timeout_ms}'
                 )"
        ))
        .await
        .expect("a table with a webhook on it");
}

/// The one row of `net._http_response` for a request, as json.
async fn answer(client: &Client, id: i64) -> Value {
    let rows = client
        .query(
            "select status_code, content_type, content, timed_out, error_msg
               from net._http_response where id = $1",
            &[&id],
        )
        .await
        .expect("the response table is readable");
    assert_eq!(rows.len(), 1, "one answer for request {id}");
    let status: Option<i32> = rows[0].get(0);
    let content_type: Option<String> = rows[0].get(1);
    let content: Option<String> = rows[0].get(2);
    let timed_out: Option<bool> = rows[0].get(3);
    let error: Option<String> = rows[0].get(4);
    json!({
        "status_code": status,
        "content_type": content_type,
        "content": content,
        "timed_out": timed_out,
        "error_msg": error,
    })
}

/// The request the trigger queued for a row it fired on.
async fn queued(client: &Client, table: &str) -> i64 {
    client
        .query_one(
            "select h.request_id from supabase_functions.hooks h
              where h.hook_table_id = to_regclass($1)::oid
              order by h.id desc limit 1",
            &[&table],
        )
        .await
        .expect("the audit trail has the request in it")
        .get(0)
}

async fn pending(client: &Client, id: i64) -> bool {
    client
        .query_one(
            "select exists (select 1 from net.http_request_queue where id = $1)",
            &[&id],
        )
        .await
        .expect("the queue is readable")
        .get(0)
}

async fn tries(client: &Client, id: i64) -> i32 {
    client
        .query_one(
            "select coalesce((select tries from zou.http_attempt where id = $1), 0)",
            &[&id],
        )
        .await
        .expect("the attempt table is readable")
        .get(0)
}

/// A round of the dispatcher, driven by hand.
async fn round(client: &Client, retries: Retries) {
    let caller: Arc<dyn Caller> = Arc::new(Web::default());
    drain(client, &caller, &retries)
        .await
        .expect("the dispatcher runs");
}

/// A server, so that the bootstrap has run and the schemas are there.
async fn bootstrapped(dsn: &str) {
    let pool = sql::Pool::new(dsn, 2).expect("dsn parses");
    let sess = pool.unscoped().await.expect("connect");
    sess.execute("select 1", &[]).await.expect("a query");
    sess.commit().await.expect("done");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_webhook_is_a_trigger_and_this_is_what_makes_the_call() {
    let Some(dsn) = dsn() else { return };
    bootstrapped(&dsn).await;
    let client = connected(&dsn).await;
    client
        .batch_execute("delete from net.http_request_queue; delete from net._http_response")
        .await
        .expect("an empty queue to start from");

    // What the receiver is sent is what the trigger's arguments say,
    // and the payload is the shape every Supabase webhook example
    // reads: a record, an old_record, the operation and where it
    // happened.
    let heard = receiver(Plan::Answer(200));
    hooked(&client, "hook_orders", &heard.url("/hook"), 2000).await;
    client
        .execute("insert into hook_orders values (1, 'first')", &[])
        .await
        .expect("a row");
    let id = queued(&client, "hook_orders").await;
    assert!(pending(&client, id).await, "the request is queued");
    round(&client, Retries::default()).await;

    let bodies = heard.bodies();
    assert_eq!(bodies.len(), 1, "one call for one row, {bodies:?}");
    let sent: Value = serde_json::from_str(&bodies[0]).expect("the body is json");
    assert_eq!(
        sent,
        json!({
            "old_record": null,
            "record": {"id": 1, "note": "first"},
            "type": "INSERT",
            "table": "hook_orders",
            "schema": "public",
        })
    );
    assert_eq!(
        answer(&client, id).await,
        json!({
            "status_code": 200,
            "content_type": "application/json",
            "content": "{\"heard\":true}",
            "timed_out": false,
            "error_msg": null,
        })
    );
    assert!(
        !pending(&client, id).await,
        "an answered request is out of the queue"
    );

    // A receiver that was restarting is asked again, and the answer
    // written down is the one it gave when it came back. A second of
    // backoff rather than the default two, because the point of the
    // wait is that there is one and this test waits it out.
    let restarting = receiver(Plan::FailFirst);
    hooked(&client, "hook_retry", &restarting.url("/hook"), 2000).await;
    client
        .execute("insert into hook_retry values (1, 'again')", &[])
        .await
        .expect("a row");
    let id = queued(&client, "hook_retry").await;
    let twice = Retries {
        attempts: 2,
        backoff: Duration::from_secs(1),
    };
    round(&client, twice).await;
    assert!(
        pending(&client, id).await,
        "a request that got a 500 is still in the queue"
    );
    assert_eq!(tries(&client, id).await, 1, "one attempt so far");
    assert_eq!(
        restarting.calls.load(Ordering::SeqCst),
        1,
        "the second attempt waits for the backoff"
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    round(&client, twice).await;
    assert_eq!(
        restarting.calls.load(Ordering::SeqCst),
        2,
        "it was asked twice"
    );
    assert_eq!(
        answer(&client, id).await["status_code"],
        json!(200),
        "the answer recorded is the one it gave when it came back"
    );

    // And a receiver that keeps saying no is written down saying no,
    // with the status it said it with, once the tries are gone.
    let refusing = receiver(Plan::Answer(503));
    hooked(&client, "hook_down", &refusing.url("/hook"), 2000).await;
    client
        .execute("insert into hook_down values (1, 'nope')", &[])
        .await
        .expect("a row");
    let id = queued(&client, "hook_down").await;
    round(
        &client,
        Retries {
            attempts: 1,
            backoff: Duration::ZERO,
        },
    )
    .await;
    assert_eq!(answer(&client, id).await["status_code"], json!(503));
    assert!(
        !pending(&client, id).await,
        "a request nobody will retry is done"
    );

    // Nothing listening at all is pg_net's own words, which is what a
    // project reading error_msg has seen before.
    let closed = TcpListener::bind("127.0.0.1:0").expect("a port");
    let nowhere = closed.local_addr().expect("the port");
    drop(closed);
    hooked(
        &client,
        "hook_nowhere",
        &format!("http://{nowhere}/hook"),
        2000,
    )
    .await;
    client
        .execute("insert into hook_nowhere values (1, 'gone')", &[])
        .await
        .expect("a row");
    let id = queued(&client, "hook_nowhere").await;
    round(
        &client,
        Retries {
            attempts: 1,
            backoff: Duration::ZERO,
        },
    )
    .await;
    assert_eq!(
        answer(&client, id).await,
        json!({
            "status_code": null,
            "content_type": null,
            "content": null,
            "timed_out": false,
            "error_msg": "Couldn't connect to server",
        })
    );

    // A receiver that took the request and said nothing is a timeout,
    // and the timeout is the one the trigger asked for.
    let quiet = receiver(Plan::Silence);
    hooked(&client, "hook_quiet", &quiet.url("/hook"), 300).await;
    client
        .execute("insert into hook_quiet values (1, 'silence')", &[])
        .await
        .expect("a row");
    let id = queued(&client, "hook_quiet").await;
    round(
        &client,
        Retries {
            attempts: 1,
            backoff: Duration::ZERO,
        },
    )
    .await;
    let said = answer(&client, id).await;
    assert_eq!(said["timed_out"], json!(true), "{said}");
    assert!(
        said["error_msg"]
            .as_str()
            .is_some_and(|why| why.starts_with("Timeout reached. Total time:")),
        "{said}"
    );

    // Last, because it starts a loop that lives as long as this
    // process does: the dispatcher a request through the front door
    // starts, doing all of the above without being asked to.
    let waiting = receiver(Plan::Answer(200));
    hooked(&client, "hook_alone", &waiting.url("/hook"), 2000).await;
    let app = router(Config {
        jwt_secret: SECRET.to_vec(),
        pg: Some(dsn.clone()),
        webhook: Retries {
            attempts: 1,
            backoff: Duration::ZERO,
        },
        ..Config::default()
    })
    .expect("router builds");
    let knocked = axum::http::Request::builder()
        .uri("/rest/v1/")
        .header("apikey", jwt::mint(&jwt::key_claims("anon"), SECRET))
        .body(axum::body::Body::empty())
        .expect("a request");
    let _ = tower::ServiceExt::oneshot(app, knocked).await;
    client
        .execute("insert into hook_alone values (1, 'nobody asked')", &[])
        .await
        .expect("a row");
    let id = queued(&client, "hook_alone").await;
    for _ in 0..200 {
        if !pending(&client, id).await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        answer(&client, id).await["status_code"],
        json!(200),
        "the dispatcher the first request started made the call by itself"
    );
}
