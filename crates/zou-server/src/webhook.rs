//! Database webhooks: the half of pg_net that is not sql.
//!
//! A webhook on Supabase is a trigger calling
//! `supabase_functions.http_request()`, which queues a row in
//! `net.http_request_queue` and returns. Something else has to make the
//! call, and upstream that something is pg_net's background worker: one
//! process per database, always running, holding a curl multi handle.
//!
//! There is no worker here. The queue announces itself instead, the
//! same way a database send does: `net.wake()` calls `pg_notify` and
//! this listens. Nothing runs while nothing is queued, which is the
//! whole argument for doing it this way rather than porting the worker.
//!
//! Two things are deliberately not upstream's:
//!
//! A request is tried again. Upstream makes one call and writes down
//! whatever happened, so a receiver that was restarting when the row
//! was written never hears about it, and every Supabase project with a
//! webhook in it has this hole. Here an attempt that got no answer, or
//! got one the receiver could not have meant, is made again with a
//! backoff. Which is why a webhook receiver has to be idempotent: a
//! request that timed out may well have been handled, and the only
//! thing this can tell is that no answer arrived.
//!
//! And a queued row is deleted when the request is finished rather than
//! when it is picked up, so `net.http_request_queue` is what has not
//! been delivered yet and a `select` from it during an outage is the
//! backlog. The claim is `for update skip locked` against a lease, so
//! two nodes serving one database take different rows and a node that
//! died mid request has its rows taken by another when the lease runs
//! out.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::{App, sql};
use tokio_postgres::Client;

/// What `net.wake()` notifies, and what this listens on.
pub const CHANNEL: &str = "zou_http_request";

/// How many requests are in flight at once. Upstream's worker does two
/// hundred, on one thread, through curl's multi interface; each of
/// these holds a blocking thread while it waits, so the number is
/// smaller and the reason for it is different.
const BATCH: i64 = 64;

/// The floor under how often the queue is looked at, for the rows that
/// arrived while this was not listening and for the ones waiting on a
/// backoff.
const TICK: Duration = Duration::from_secs(5);

/// How long a request another node claimed is left alone, on top of
/// that request's own timeout. A node that died holding one has its
/// rows taken by somebody else this long after it stopped.
const LEASE: f64 = 60.0;

/// How long an answer is kept in `net._http_response`, which is
/// pg_net's own `pg_net.ttl` default.
const KEEP: &str = "6 hours";

/// How often the answers are swept.
const SWEEP: Duration = Duration::from_secs(3600);

/// How long to wait before dialing again after the listening
/// connection died.
const REDIAL: Duration = Duration::from_secs(5);

/// How much of a receiver's answer is recorded. Upstream has no limit
/// because curl streams into a buffer the worker owns; here a batch of
/// answers is in memory at once, so there is one. A receiver that says
/// more than this gets its request recorded as failed, with a reason
/// saying so, rather than quietly truncated.
const MAX_BODY: u64 = 1 << 20;

/// What a request that did not work out gets: how many times in total
/// it is tried, and the first wait between tries.
///
/// Three attempts, two seconds apart to start with and five times
/// longer each time after, so a receiver that is restarting has two
/// seconds and then ten to come back. One attempt is upstream's
/// behaviour exactly, and is how a project that would rather have a
/// lost webhook than a repeated one says so.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retries {
    pub attempts: u32,
    pub backoff: Duration,
}

impl Default for Retries {
    fn default() -> Retries {
        Retries {
            attempts: 3,
            backoff: Duration::from_secs(2),
        }
    }
}

pub fn retries_from_env() -> Result<Retries, String> {
    retries_configured(&|name| std::env::var(name).unwrap_or_default())
}

pub fn retries_configured(var: &dyn Fn(&str) -> String) -> Result<Retries, String> {
    let fallback = Retries::default();
    let attempts = count(var, "ZOU_WEBHOOK_ATTEMPTS", u64::from(fallback.attempts))?;
    if attempts == 0 {
        return Err("ZOU_WEBHOOK_ATTEMPTS is 0, and a request is made at least once".to_string());
    }
    let backoff = count(
        var,
        "ZOU_WEBHOOK_BACKOFF_SECONDS",
        fallback.backoff.as_secs(),
    )?;
    Ok(Retries {
        attempts: attempts.min(u64::from(u32::MAX)) as u32,
        backoff: Duration::from_secs(backoff),
    })
}

fn count(var: &dyn Fn(&str) -> String, name: &str, fallback: u64) -> Result<u64, String> {
    let text = var(name);
    let text = text.trim();
    if text.is_empty() {
        return Ok(fallback);
    }
    match text.parse::<u64>() {
        Ok(n) => Ok(n),
        Err(_) => Err(format!("{name} is {text:?}, which is not a whole number")),
    }
}

/// A row of `net.http_request_queue` and how many times it has been
/// tried already.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Queued {
    pub id: i64,
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
    pub timeout: Duration,
    pub tries: i32,
}

/// What one attempt came back as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The receiver said something, whatever it was. A 500 is an
    /// answer.
    Answered {
        status: u16,
        headers: Vec<(String, String)>,
        body: String,
    },
    /// The timeout the caller asked for ran out first.
    Timeout { after: Duration },
    /// Nothing came back, in the words curl would have used for it.
    Failed { why: String },
}

/// What makes the call. A trait so that the retry policy can be tested
/// without a receiver, and so that the blocking client stays in one
/// place.
pub trait Caller: Send + Sync {
    fn call(&self, request: &Queued) -> Outcome;
}

/// The wire. One agent per timeout, because ureq's timeout is agent
/// wide and every queued request carries its own, and because a
/// webhook to one receiver is usually the same timeout every time, so
/// the connection pool is kept rather than thrown away per call.
pub struct Web {
    agents: Mutex<HashMap<u64, ureq::Agent>>,
}

impl Default for Web {
    fn default() -> Web {
        Web {
            agents: Mutex::new(HashMap::new()),
        }
    }
}

impl Web {
    fn agent(&self, timeout: Duration) -> ureq::Agent {
        let mut agents = self.agents.lock().unwrap_or_else(|e| e.into_inner());
        agents
            .entry(timeout.as_millis().min(u128::from(u64::MAX)) as u64)
            .or_insert_with(|| {
                ureq::Agent::config_builder()
                    // A receiver's 500 is an answer to record, not a
                    // transport failure, and whether it is worth
                    // another try is decided further down.
                    .http_status_as_error(false)
                    .timeout_global(Some(timeout))
                    .build()
                    .into()
            })
            .clone()
    }
}

impl Caller for Web {
    fn call(&self, request: &Queued) -> Outcome {
        let started = Instant::now();
        let mut built = ureq::http::Request::builder()
            .method(request.method.as_str())
            .uri(&request.url);
        for (name, value) in &request.headers {
            built = built.header(name, value);
        }
        let body = request.body.clone().unwrap_or_default();
        let built = match built.body(&body[..]) {
            Ok(built) => built,
            Err(e) => return Outcome::Failed { why: e.to_string() },
        };
        let answer = match self.agent(request.timeout).run(built) {
            Ok(answer) => answer,
            Err(e) => return failure(e, started.elapsed()),
        };
        let status = answer.status().as_u16();
        let headers = answer
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    String::from_utf8_lossy(value.as_bytes()).to_string(),
                )
            })
            .collect();
        match answer
            .into_body()
            .with_config()
            .limit(MAX_BODY)
            .read_to_vec()
        {
            Ok(body) => Outcome::Answered {
                status,
                headers,
                body: String::from_utf8_lossy(&body).to_string(),
            },
            Err(e) => failure(e, started.elapsed()),
        }
    }
}

/// An attempt that got nothing, in curl's words where curl has one,
/// because those are the words a project has seen in `error_msg` on
/// every Supabase project it has run this on.
fn failure(e: ureq::Error, took: Duration) -> Outcome {
    match e {
        ureq::Error::Timeout(_) => Outcome::Timeout { after: took },
        ureq::Error::HostNotFound => Outcome::Failed {
            why: "Couldn't resolve host name".to_string(),
        },
        ureq::Error::ConnectionFailed => Outcome::Failed {
            why: "Couldn't connect to server".to_string(),
        },
        // Nothing there and something that broke halfway are both io
        // here and are different things to a person reading them, so
        // the kind decides: the ones that mean the call never started
        // get curl's sentence for that, and the rest say what
        // happened.
        ureq::Error::Io(e) => Outcome::Failed {
            why: match e.kind() {
                std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::NetworkUnreachable
                | std::io::ErrorKind::HostUnreachable
                | std::io::ErrorKind::AddrNotAvailable
                | std::io::ErrorKind::NotConnected => "Couldn't connect to server".to_string(),
                _ => e.to_string(),
            },
        },
        other => Outcome::Failed {
            why: other.to_string(),
        },
    }
}

/// Whether an outcome is worth trying again, before counting how many
/// tries are left.
///
/// Nothing that got no answer is the receiver saying no, so all of
/// those are tried again. An answer is only tried again when it is the
/// receiver saying it could not take it now: too many requests, a
/// gateway with nothing behind it, or anything it calls a server
/// error. A 404 or a 401 is a receiver that is up and means it, and
/// repeating that request would just be four of the same row in
/// somebody's log.
pub fn worth_repeating(outcome: &Outcome) -> bool {
    match outcome {
        Outcome::Answered { status, .. } => {
            *status == 408 || *status == 429 || (500..600).contains(status)
        }
        Outcome::Timeout { .. } | Outcome::Failed { .. } => true,
    }
}

/// How long after the `tries`th attempt the next one is made.
pub fn wait(retries: &Retries, tries: i32) -> Duration {
    let steps = u32::try_from(tries.max(1) - 1).unwrap_or(0).min(8);
    retries.backoff * 5u32.saturating_pow(steps)
}

/// What goes in `net._http_response` for a request nobody is going to
/// try again: the status, the type, the headers, the body, whether it
/// timed out, and why it failed. Upstream writes one of these for
/// every request; here it is written once, describing the last
/// attempt, when the retries are done.
pub fn recorded(outcome: &Outcome) -> Answer {
    match outcome {
        Outcome::Answered {
            status,
            headers,
            body,
        } => {
            let kind = headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
                .map(|(_, value)| value.clone());
            let mut object = serde_json::Map::new();
            for (name, value) in headers {
                object.insert(name.clone(), Value::String(value.clone()));
            }
            Answer {
                status: Some(i32::from(*status)),
                content_type: kind,
                headers: Some(Value::Object(object)),
                content: Some(body.clone()),
                timed_out: false,
                error: None,
            }
        }
        Outcome::Timeout { after } => Answer {
            status: None,
            content_type: None,
            headers: None,
            content: None,
            timed_out: true,
            // Upstream's message carries curl's breakdown of where the
            // time went, which is curl's to know and not this
            // server's, so the sentence is the same up to that.
            error: Some(format!(
                "Timeout reached. Total time: {:.6} ms",
                after.as_secs_f64() * 1000.0
            )),
        },
        Outcome::Failed { why } => Answer {
            status: None,
            content_type: None,
            headers: None,
            content: None,
            timed_out: false,
            error: Some(why.clone()),
        },
    }
}

/// One row of `net._http_response`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub status: Option<i32>,
    pub content_type: Option<String>,
    pub headers: Option<Value>,
    pub content: Option<String>,
    pub timed_out: bool,
    pub error: Option<String>,
}

/// Make the calls the database asked for, until the process ends.
///
/// Started on the first request through the gate rather than at boot,
/// because a router can be built outside a runtime and because a queue
/// is only ever filled by a transaction, which is something a request
/// did.
pub fn dispatch(app: Arc<App>) {
    tokio::spawn(async move {
        loop {
            match dispatch_once(&app).await {
                Ok(()) => return,
                Err(e) => log::warn!("webhooks: the queue listener stopped: {e}"),
            }
            tokio::time::sleep(REDIAL).await;
        }
    });
}

/// One connection's worth of dispatching, until it dies. `Ok` means
/// there is nothing here to do and nothing that will change that.
async fn dispatch_once(app: &App) -> Result<(), sql::Error> {
    let Some(pool) = &app.pool else {
        return Ok(());
    };
    let (client, mut notes) = pool.listening(CHANNEL).await?;
    // A database with the real pg_net in it has a worker on this queue
    // already, and two things draining one queue is two calls to
    // somebody's endpoint. Theirs wins: it is the one the project
    // installed.
    let theirs: bool = client
        .query_one(
            "select exists (select 1 from pg_extension where extname = 'pg_net')",
            &[],
        )
        .await?
        .get(0);
    if theirs {
        log::info!("webhooks: pg_net is installed, its worker has the queue");
        return Ok(());
    }
    let caller: Arc<dyn Caller> = Arc::new(Web::default());
    let retries = app.cfg.webhook;
    let mut tick = tokio::time::interval(TICK);
    let mut sweep = tokio::time::interval_at(tokio::time::Instant::now() + SWEEP, SWEEP);
    loop {
        // Whatever woke this up, the answer is the same: look at the
        // queue. A notification per row would be a claim per row, and
        // the claim takes a batch.
        drain(&client, &caller, &retries).await?;
        tokio::select! {
            note = notes.recv() => if note.is_none() { return Ok(()) },
            _ = tick.tick() => {}
            _ = sweep.tick() => {
                if let Err(e) = client
                    .execute(
                        &format!("delete from net._http_response where created < now() - interval '{KEEP}'"),
                        &[],
                    )
                    .await
                {
                    log::debug!("webhooks: the answers were not swept: {e}");
                }
            }
        }
    }
}

/// Claim what is due and make those calls, until there is nothing due.
///
/// Public because the live tests drive it a round at a time rather
/// than starting the loop and waiting: a retry that is asked for
/// explicitly is a test that says what it means and takes no time.
pub async fn drain(
    client: &Client,
    caller: &Arc<dyn Caller>,
    retries: &Retries,
) -> Result<(), sql::Error> {
    loop {
        let due = claim(client).await?;
        if due.is_empty() {
            return Ok(());
        }
        let mut calling = tokio::task::JoinSet::new();
        for request in due {
            let caller = Arc::clone(caller);
            calling.spawn_blocking(move || {
                let outcome = caller.call(&request);
                (request, outcome)
            });
        }
        while let Some(done) = calling.join_next().await {
            let Ok((request, outcome)) = done else {
                // The only thing in that task is the call, and the
                // call catches its own failures, so this is the
                // runtime shutting down.
                continue;
            };
            settle(client, retries, &request, &outcome).await?;
        }
    }
}

/// The rows this node is now the one calling.
///
/// One statement, so that taking a row and saying it was taken cannot
/// come apart. `for update skip locked` is what makes two nodes take
/// different rows in the same instant, and `taken_at` is what makes
/// that hold after the statement commits and the lock is gone.
async fn claim(client: &Client) -> Result<Vec<Queued>, sql::Error> {
    let rows = client
        .query(
            "with due as (
                 select q.id
                   from net.http_request_queue q
                   left join zou.http_attempt a on a.id = q.id
                  where coalesce(a.next_at, '-infinity'::timestamptz) <= now()
                    and (
                        a.taken_at is null
                        or a.taken_at < now() - make_interval(
                            secs => $2::double precision + q.timeout_milliseconds / 1000.0
                        )
                    )
                  order by q.id
                  limit $1
                    for update of q skip locked
             ), taken as (
                 insert into zou.http_attempt (id, tries, next_at, taken_at)
                 select id, 0, now(), now() from due
                 on conflict (id) do update set taken_at = now()
                 returning id
             )
             select q.id, q.method::text, q.url, q.headers, q.body,
                    q.timeout_milliseconds, coalesce(a.tries, 0)
               from taken t
               join net.http_request_queue q on q.id = t.id
               left join zou.http_attempt a on a.id = t.id",
            &[&BATCH, &LEASE],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| {
            let headers: Option<Value> = row.get(3);
            let timeout: i32 = row.get(5);
            Queued {
                id: row.get(0),
                method: row.get(1),
                url: row.get(2),
                headers: sent(headers.as_ref()),
                body: row.get(4),
                timeout: Duration::from_millis(timeout.max(0) as u64),
                tries: row.get(6),
            }
        })
        .collect())
}

/// The headers column as headers. A value that is not a string is the
/// project having put something strange in the jsonb, and it is sent
/// as whatever json says it is rather than dropped.
fn sent(headers: Option<&Value>) -> Vec<(String, String)> {
    let Some(Value::Object(object)) = headers else {
        return Vec::new();
    };
    object
        .iter()
        .map(|(name, value)| {
            let value = match value {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            (name.clone(), value)
        })
        .collect()
}

/// What happens to a request after an attempt: another try later, or
/// an answer written down and the row gone.
async fn settle(
    client: &Client,
    retries: &Retries,
    request: &Queued,
    outcome: &Outcome,
) -> Result<(), sql::Error> {
    let tries = request.tries + 1;
    let again = worth_repeating(outcome) && (tries as u32) < retries.attempts;
    zou_ops::registry()
        .counter(
            "zou_webhook_attempts_total",
            "webhook requests attempted",
            &[("outcome", named(outcome))],
        )
        .inc();
    if again {
        let seconds = wait(retries, tries).as_secs_f64();
        let why = recorded(outcome).error.unwrap_or_else(|| {
            let Outcome::Answered { status, .. } = outcome else {
                return String::new();
            };
            format!("the receiver answered {status}")
        });
        client
            .execute(
                "update zou.http_attempt
                    set tries = $2, next_at = now() + make_interval(secs => $3::double precision),
                        taken_at = null, last_error = $4
                  where id = $1",
                &[&request.id, &tries, &seconds, &why],
            )
            .await?;
        return Ok(());
    }
    let answer = recorded(outcome);
    // The insert is conditional on the queue row still being there, so
    // that a node whose lease ran out while it was still calling does
    // not write a second answer over the one that arrived.
    client
        .execute(
            "with answered as (
                 delete from net.http_request_queue where id = $1 returning id
             ), forgotten as (
                 delete from zou.http_attempt where id = $1 returning id
             )
             insert into net._http_response
                 (id, status_code, content_type, headers, content, timed_out, error_msg)
             select $1, $2, $3, $4, $5, $6, $7 from answered",
            &[
                &request.id,
                &answer.status,
                &answer.content_type,
                &answer.headers,
                &answer.content,
                &answer.timed_out,
                &answer.error,
            ],
        )
        .await?;
    zou_ops::registry()
        .counter(
            "zou_webhook_requests_total",
            "webhook requests finished",
            &[("outcome", named(outcome))],
        )
        .inc();
    Ok(())
}

/// What an outcome is called on a metric. The status is in there
/// because a dashboard's first question about a webhook is whether the
/// receiver is refusing them or missing them.
fn named(outcome: &Outcome) -> &'static str {
    match outcome {
        Outcome::Answered { status, .. } if *status < 400 => "answered",
        Outcome::Answered { status, .. } if *status < 500 => "refused",
        Outcome::Answered { .. } => "errored",
        Outcome::Timeout { .. } => "timeout",
        Outcome::Failed { .. } => "failed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn answered(status: u16) -> Outcome {
        Outcome::Answered {
            status,
            headers: vec![("Content-Type".to_string(), "text/plain".to_string())],
            body: "ok".to_string(),
        }
    }

    #[test]
    fn a_receiver_that_answered_is_not_asked_twice() {
        for status in [200, 201, 204, 301, 400, 401, 404, 422] {
            assert!(
                !worth_repeating(&answered(status)),
                "a {status} was repeated"
            );
        }
    }

    #[test]
    fn a_receiver_that_could_not_take_it_is_asked_again() {
        for status in [408, 429, 500, 502, 503, 504] {
            assert!(worth_repeating(&answered(status)), "a {status} was dropped");
        }
        assert!(worth_repeating(&Outcome::Failed {
            why: "Couldn't connect to server".to_string()
        }));
        assert!(worth_repeating(&Outcome::Timeout {
            after: Duration::from_millis(1000)
        }));
    }

    #[test]
    fn the_wait_between_tries_grows_by_five() {
        let retries = Retries {
            attempts: 4,
            backoff: Duration::from_secs(2),
        };
        assert_eq!(wait(&retries, 1), Duration::from_secs(2));
        assert_eq!(wait(&retries, 2), Duration::from_secs(10));
        assert_eq!(wait(&retries, 3), Duration::from_secs(50));
    }

    #[test]
    fn an_answer_is_recorded_the_way_pg_net_records_one() {
        let answer = recorded(&Outcome::Answered {
            status: 201,
            headers: vec![
                ("Content-Type".to_string(), "application/json".to_string()),
                ("Server".to_string(), "whatever".to_string()),
            ],
            body: "{\"ok\":true}".to_string(),
        });
        assert_eq!(answer.status, Some(201));
        assert_eq!(answer.content_type.as_deref(), Some("application/json"));
        assert_eq!(
            answer.headers,
            Some(json!({"Content-Type": "application/json", "Server": "whatever"}))
        );
        assert_eq!(answer.content.as_deref(), Some("{\"ok\":true}"));
        assert!(!answer.timed_out);
        assert_eq!(answer.error, None);
    }

    #[test]
    fn nothing_came_back_is_recorded_as_the_reason_alone() {
        let answer = recorded(&Outcome::Failed {
            why: "Couldn't connect to server".to_string(),
        });
        assert_eq!(answer.status, None);
        assert_eq!(answer.headers, None);
        assert_eq!(answer.content, None);
        assert!(!answer.timed_out);
        assert_eq!(answer.error.as_deref(), Some("Couldn't connect to server"));

        let answer = recorded(&Outcome::Timeout {
            after: Duration::from_millis(1500),
        });
        assert!(answer.timed_out);
        assert_eq!(
            answer.error.as_deref(),
            Some("Timeout reached. Total time: 1500.000000 ms")
        );
    }

    #[test]
    fn headers_are_sent_as_the_jsonb_spells_them() {
        let headers = json!({"Content-Type": "application/json", "X-Count": 3});
        let mut carried = sent(Some(&headers));
        carried.sort();
        assert_eq!(
            carried,
            vec![
                ("Content-Type".to_string(), "application/json".to_string()),
                ("X-Count".to_string(), "3".to_string()),
            ]
        );
        assert!(sent(None).is_empty());
        assert!(sent(Some(&Value::Null)).is_empty());
    }

    #[test]
    fn retries_come_from_the_environment_or_from_the_defaults() {
        let none = retries_configured(&|_| String::new()).expect("nothing configured is fine");
        assert_eq!(none, Retries::default());

        let set = retries_configured(&|name| match name {
            "ZOU_WEBHOOK_ATTEMPTS" => "5".to_string(),
            "ZOU_WEBHOOK_BACKOFF_SECONDS" => "1".to_string(),
            _ => String::new(),
        })
        .expect("both are numbers");
        assert_eq!(
            set,
            Retries {
                attempts: 5,
                backoff: Duration::from_secs(1)
            }
        );

        let off = retries_configured(&|name| match name {
            "ZOU_WEBHOOK_ATTEMPTS" => "1".to_string(),
            _ => String::new(),
        })
        .expect("one attempt is upstream's behaviour");
        assert_eq!(off.attempts, 1);

        let bad = retries_configured(&|name| match name {
            "ZOU_WEBHOOK_ATTEMPTS" => "0".to_string(),
            _ => String::new(),
        });
        assert!(bad.is_err(), "zero attempts is not a request");

        let worse = retries_configured(&|name| match name {
            "ZOU_WEBHOOK_BACKOFF_SECONDS" => "soon".to_string(),
            _ => String::new(),
        });
        assert!(worse.is_err(), "{worse:?}");
    }

    #[test]
    fn what_a_metric_calls_an_outcome() {
        assert_eq!(named(&answered(200)), "answered");
        assert_eq!(named(&answered(404)), "refused");
        assert_eq!(named(&answered(503)), "errored");
        assert_eq!(
            named(&Outcome::Timeout {
                after: Duration::ZERO
            }),
            "timeout"
        );
        assert_eq!(named(&Outcome::Failed { why: String::new() }), "failed");
    }
}
