//! The `cron` schema against a real database.
//!
//! The unit tests in `cron.rs` are about the schedule parser, which is
//! the half of pg_cron that needs no database. This is the other half:
//! whether the functions a project calls are the ones pg_cron gave it,
//! whether the sql that refuses a bad schedule refuses the same
//! strings the parser does, and whether a round of the ticker runs the
//! command and writes down what happened.
//!
//! One test rather than several, for the same reason the webhook suite
//! is one: there is one job table in a database and two tests firing
//! it at once would be two tests taking each other's rows.
//!
//! Gated on ZOU_PG_TEST_DSN, like the other suites that need a
//! database of their own:
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test cron

use std::time::Duration;

use tokio_postgres::Client;
use zou_server::cron::{Schedule, round, sweep};
use zou_server::sql;

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

/// The interesting half of the list the unit tests carry, which is
/// every string where the two readers of a schedule could disagree:
/// the macros, the interval form, names, steps, bounds, and the junk
/// vixie drops rather than refuses.
const BOTH_READERS: [&str; 30] = [
    "*/5 * * * *",
    "5 4 * * *",
    "@hourly",
    "@midnight",
    "@reboot",
    "@Daily",
    "@every 5 minutes",
    "@hourly extra",
    "30 seconds",
    "60 seconds",
    "1 second",
    "30 SECONDS",
    "0 seconds",
    "0 0 * * MON",
    "0 0 * * SUN-FRI",
    "0 0 * jan-mar *",
    "0 0 * * 7",
    "0 0 * * 8",
    "*/0 * * * *",
    "*/61 * * * *",
    "5/2 * * * *",
    "1-5/2 * * * *",
    "5-1 * * * *",
    "* * * *",
    "* * * * * garbage",
    "0 0 * * MON#2",
    "* * * MON *",
    "JAN * * * *",
    "0 0 L * *",
    "daily",
];

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

/// Whether the sql took a schedule, and the message when it did not.
async fn takes(client: &Client, schedule: &str) -> bool {
    client
        .query_one("select cron._valid_schedule($1)", &[&schedule])
        .await
        .expect("the validator answers")
        .get(0)
}

/// The last run of a job.
async fn last_run(client: &Client, id: i64) -> Option<(String, String, Option<i32>)> {
    let rows = client
        .query(
            "select status, return_message, job_pid from cron.job_run_details
              where jobid = $1 order by runid desc limit 1",
            &[&id],
        )
        .await
        .expect("the run table is readable");
    rows.first().map(|row| (row.get(0), row.get(1), row.get(2)))
}

async fn runs(client: &Client, id: i64) -> i64 {
    client
        .query_one(
            "select count(*) from cron.job_run_details where jobid = $1",
            &[&id],
        )
        .await
        .expect("the run table is readable")
        .get(0)
}

/// Move a job's last firing back, which is what a database that was
/// asleep looks like when it comes back.
async fn slept(client: &Client, id: i64, seconds: i64) {
    client
        .execute(
            "update zou.cron_run set fired_for = now() - make_interval(secs => $2::bigint)
              where jobid = $1",
            &[&id, &seconds],
        )
        .await
        .expect("the ticker's table is writable");
}

async fn error_from(client: &Client, sql: &str) -> String {
    let e = client
        .simple_query(sql)
        .await
        .expect_err("this is meant to fail");
    e.as_db_error().expect("a database error").message().into()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scheduled_job_is_a_row_and_this_is_what_runs_it() {
    let Some(dsn) = dsn() else { return };
    // A pool, which is what puts the schema there: the ticker's own
    // connection is a listener and the bootstrap is what the first
    // session through the pool does.
    let pool = sql::Pool::new(&dsn, 4).expect("dsn parses");
    let session = pool.unscoped().await.expect("connect");
    session.execute("select 1", &[]).await.expect("a query");
    session.commit().await.expect("done");

    let client = connected(&dsn).await;
    client
        .batch_execute(
            "delete from cron.job;
             delete from cron.job_run_details;
             drop table if exists cron_probe;
             create table cron_probe (at timestamptz default now())",
        )
        .await
        .expect("a clean table to schedule against");

    // The four settings pg_cron registers, on a connection made
    // after the bootstrap, which is where a project reads them.
    for (name, value) in [
        ("cron.timezone", "GMT"),
        ("cron.max_running_jobs", "32"),
        ("cron.use_background_workers", "off"),
    ] {
        let said: String = client
            .query_one(&format!("show {name}"), &[])
            .await
            .unwrap_or_else(|e| panic!("{name} is set: {e}"))
            .get(0);
        assert_eq!(said, value, "{name}");
    }

    // The sql refuses what the parser refuses. Two readers of one
    // grammar is a thing that drifts, so this is the test that says
    // it has not: a schedule the server cannot read must not be one
    // the database took, or the job sits in the table doing nothing.
    for schedule in BOTH_READERS {
        assert_eq!(
            takes(&client, schedule).await,
            Schedule::parse(schedule).is_some(),
            "the sql and the ticker disagree about {schedule:?}"
        );
    }

    // A bad one is refused where a project finds out about it, which
    // is the call rather than the run, and with upstream's words.
    assert_eq!(
        error_from(
            &client,
            "select cron.schedule('bad', 'not a schedule', 'select 1')"
        )
        .await,
        "invalid schedule: not a schedule"
    );
    assert_eq!(
        error_from(&client, "select cron.unschedule('nope')").await,
        "could not find valid entry for job 'nope'"
    );
    assert_eq!(
        error_from(&client, "select cron.unschedule(999999)").await,
        "could not find valid entry for job 999999"
    );
    assert_eq!(
        error_from(&client, "select cron.alter_job(999999, active := false)").await,
        "Job 999999 does not exist or you don't own it"
    );
    assert_eq!(
        error_from(
            &client,
            "select cron.schedule_in_database('x', '* * * * *', 'select 1', 'nosuchdb')"
        )
        .await,
        "database \"nosuchdb\" does not exist"
    );

    // A name is a job, and scheduling it again moves the one that is
    // there rather than making a second.
    let id: i64 = client
        .query_one(
            "select cron.schedule('probe', '* * * * *', 'insert into cron_probe default values')",
            &[],
        )
        .await
        .expect("a job")
        .get(0);
    let again: i64 = client
        .query_one(
            "select cron.schedule('probe', '* * * * *', 'insert into cron_probe default values')",
            &[],
        )
        .await
        .expect("the same job")
        .get(0);
    assert_eq!(id, again, "a name is upsert");
    let count: i64 = client
        .query_one("select count(*) from cron.job", &[])
        .await
        .expect("the job table is readable")
        .get(0);
    assert_eq!(count, 1, "one row for two calls with one name");

    // The first round after a job appears does not run it. Writing a
    // job is not an occurrence of it, and a project that schedules a
    // nightly clean up at noon does not want it at noon.
    round(&client, &pool, true).await.expect("a round");
    assert_eq!(runs(&client, id).await, 0, "scheduling is not a firing");

    // A minute goes by, and it runs.
    slept(&client, id, 120).await;
    round(&client, &pool, false).await.expect("a round");
    let (status, said, pid) = last_run(&client, id).await.expect("it ran");
    assert_eq!(status, "succeeded");
    assert_eq!(said, "1 row", "what the command touched");
    assert!(pid.is_some(), "the backend that ran it is written down");
    let inserted: i64 = client
        .query_one("select count(*) from cron_probe", &[])
        .await
        .expect("the probe table is readable")
        .get(0);
    assert_eq!(inserted, 1, "the command really ran");

    // And the round after it does not run it again.
    round(&client, &pool, false).await.expect("a round");
    assert_eq!(runs(&client, id).await, 1, "once for one occurrence");

    // A day asleep is one run and not twenty four, which is the whole
    // of the catch up policy.
    slept(&client, id, 24 * 3600).await;
    round(&client, &pool, true).await.expect("a waking round");
    assert_eq!(runs(&client, id).await, 2, "a day of them is one run");
    round(&client, &pool, false).await.expect("a round");
    assert_eq!(runs(&client, id).await, 2, "and no backlog behind it");

    // What a failure says, down to the two spaces postgres puts after
    // the word.
    let broken: i64 = client
        .query_one(
            "select cron.schedule('broken', '* * * * *', 'select 1/0')",
            &[],
        )
        .await
        .expect("a job that will not work")
        .get(0);
    round(&client, &pool, false).await.expect("a seeding round");
    slept(&client, broken, 120).await;
    round(&client, &pool, false).await.expect("a round");
    let (status, said, _) = last_run(&client, broken).await.expect("it ran");
    assert_eq!(status, "failed");
    assert!(
        said.starts_with("ERROR:  division by zero"),
        "the error is written down as postgres said it: {said:?}"
    );

    // A run left open by a server that stopped is closed by the next
    // one to take the job table, rather than saying `running` for
    // ever.
    client
        .execute(
            "insert into cron.job_run_details (jobid, job_pid, database, username, command, status, start_time)
             values ($1, 1, current_database(), current_user, 'select 1', 'running', now())",
            &[&broken],
        )
        .await
        .expect("a run that was in flight");
    assert_eq!(sweep(&client).await.expect("a sweep"), 1);
    let (status, said, _) = last_run(&client, broken).await.expect("it is written down");
    assert_eq!(status, "failed");
    assert_eq!(said, "ERROR:  the server running this job stopped");
    assert_eq!(sweep(&client).await.expect("a sweep"), 0, "nothing twice");

    // A wake is what @reboot means, and nothing else is.
    let boot: i64 = client
        .query_one(
            "select cron.schedule('boot', '@reboot', 'insert into cron_probe default values')",
            &[],
        )
        .await
        .expect("a boot job")
        .get(0);
    round(&client, &pool, false).await.expect("a round");
    assert_eq!(runs(&client, boot).await, 0, "a tick is not a wake");
    // Backdated by a few seconds because the claim is on the second
    // and this test does a wake and a tick inside one of them.
    slept(&client, boot, 5).await;
    round(&client, &pool, true).await.expect("a waking round");
    assert_eq!(runs(&client, boot).await, 1, "a wake is");
    round(&client, &pool, false).await.expect("a round");
    assert_eq!(
        runs(&client, boot).await,
        1,
        "and the ticks after it are not"
    );

    // An inactive job is a job that does not run, which is what
    // alter_job is mostly called for.
    client
        .execute("select cron.alter_job($1, active := false)", &[&id])
        .await
        .expect("the job is altered");
    slept(&client, id, 120).await;
    round(&client, &pool, false).await.expect("a round");
    assert_eq!(runs(&client, id).await, 2, "inactive means inactive");

    // Writing the table says so, which is what keeps a job scheduled
    // a second before it is due from waiting a whole tick.
    let (listener, mut notes) = pool.listening("zou_cron").await.expect("a listener");
    client
        .execute("select cron.alter_job($1, active := true)", &[&id])
        .await
        .expect("the job is altered back");
    let note = tokio::time::timeout(Duration::from_secs(5), notes.recv())
        .await
        .expect("the trigger notifies");
    assert!(note.is_some(), "the channel carries the change");
    drop(listener);

    // Unscheduling takes the job and leaves the history, which is
    // upstream's answer too: job_run_details is a log and not a
    // foreign key.
    let gone: bool = client
        .query_one("select cron.unschedule('probe')", &[])
        .await
        .expect("it is unscheduled")
        .get(0);
    assert!(gone);
    assert_eq!(runs(&client, id).await, 2, "the history stays");

    client
        .batch_execute(
            "delete from cron.job;
             delete from cron.job_run_details;
             drop table if exists cron_probe",
        )
        .await
        .expect("tidy");
}
