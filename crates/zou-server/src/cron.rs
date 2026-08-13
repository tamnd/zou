//! Scheduled jobs: the half of pg_cron that is not sql.
//!
//! A project writes `select cron.schedule('nightly', '0 3 * * *',
//! 'select clean()')` and expects something to run it at three in the
//! morning. Upstream that something is pg_cron's launcher: one process
//! per cluster, awake every minute, forking a connection per due job.
//!
//! There is no launcher here. The ticker below is started by the first
//! request through the front door, the same as the webhook dispatcher,
//! and only one node runs it at a time because it holds an advisory
//! lock while it does. Every firing is also claimed in the database
//! before the command runs, so a lock that changed hands at the wrong
//! moment costs nothing.
//!
//! Three things are deliberately not upstream's, all of them the
//! consequence of a server that is allowed to not be running:
//!
//! A job that came due while nothing was running is run once when the
//! server wakes, rather than once per occurrence that went by. A job
//! that never ran is not caught up at all, because there is no
//! occurrence to catch up to and a fresh job firing the instant it is
//! written is the surprise nobody wants.
//!
//! `return_message` says how many rows the command touched. Upstream
//! writes the command tag postgres printed, and the tag is not on the
//! wire the pooled protocol hands back, so the honest thing is the
//! number rather than a tag reconstructed from the statement text.
//!
//! And `nodename` and `nodeport` are the database's address rather
//! than a launcher's, because there is no separate process to name.

use std::sync::Arc;
use std::time::Duration;

use crate::{App, sql};
use tokio_postgres::Client;

/// What `cron.job_cache_invalidate()` notifies, and what this listens
/// on. Upstream's trigger of that name drops the launcher's cached job
/// list, which is close enough to the same sentence.
pub const CHANNEL: &str = "zou_cron";

/// The lock one node holds while it is the one firing jobs. A second
/// node asks for it without waiting, finds it taken, and sleeps.
const LOCK: i64 = 730_502;

/// How often the schedule is looked at. A second, because pg_cron 1.6
/// takes schedules in seconds and a minute wide tick would fire a `5
/// seconds` job twelve times at once.
const TICK: Duration = Duration::from_secs(1);

/// How long to wait before asking for the lock again, and before
/// dialing again after the listening connection died.
const REDIAL: Duration = Duration::from_secs(5);

/// How many jobs run at once. A job that takes longer than its own
/// interval is the usual way a schedule turns into a thundering herd,
/// so there is a ceiling and a job that would go over it waits for the
/// next tick.
const AT_ONCE: usize = 8;

/// How much of a failure's message is written to `return_message`. A
/// command that raises with a megabyte of detail should not put a
/// megabyte in a table that is never swept.
const MAX_MESSAGE: usize = 8 * 1024;

/// One row of `cron.job`, as the ticker needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: i64,
    pub name: Option<String>,
    pub schedule: String,
    pub command: String,
    pub username: String,
    pub database: String,
}

/// A schedule, in the three shapes pg_cron takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Schedule {
    /// `@reboot`: once when the thing that runs jobs starts, which
    /// here is when this server wakes.
    Reboot,
    /// pg_cron 1.6's interval form, one to fifty nine seconds.
    Every(u32),
    /// Five fields.
    Fields(Fields),
}

/// The five fields as bit sets, plus which of the two day fields were
/// written as `*`, which is what decides whether they are read
/// together or either or.
///
/// A bit's place is the value it stands for, so the first of the month
/// is bit one and January is bit one, and bit zero of those two is
/// never set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fields {
    minute: u64,
    hour: u32,
    dom: u32,
    month: u16,
    dow: u8,
    dom_star: bool,
    dow_star: bool,
}

const MONTHS: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];
const DAYS: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

impl Schedule {
    /// Read a schedule the way vixie cron reads one, which is what
    /// pg_cron vendors.
    ///
    /// Anything after the fifth field is ignored rather than refused,
    /// which is why `0 0 * * MON#2` is a schedule upstream takes: the
    /// parser stops when it has five fields and never looks at the
    /// rest. The same goes for a word after a macro.
    pub fn parse(text: &str) -> Option<Schedule> {
        let text = text.trim();
        if text.is_empty() {
            return None;
        }
        if let Some(macro_name) = text
            .split_whitespace()
            .next()
            .filter(|w| w.starts_with('@'))
        {
            return match macro_name {
                "@reboot" => Some(Schedule::Reboot),
                "@yearly" | "@annually" => Schedule::parse("0 0 1 1 *"),
                "@monthly" => Schedule::parse("0 0 1 * *"),
                "@weekly" => Schedule::parse("0 0 * * 0"),
                "@daily" | "@midnight" => Schedule::parse("0 0 * * *"),
                "@hourly" => Schedule::parse("0 * * * *"),
                _ => None,
            };
        }
        if let Some(seconds) = interval(text) {
            return Some(Schedule::Every(seconds));
        }
        let fields: Vec<&str> = text.split_whitespace().collect();
        if fields.len() < 5 {
            return None;
        }
        let minute = set(fields[0], 0, 59, None)?;
        let hour = set(fields[1], 0, 23, None)?;
        let dom = set(fields[2], 1, 31, None)?;
        let month = set(fields[3], 1, 12, Some(&MONTHS))?;
        let dow = set(fields[4], 0, 7, Some(&DAYS))?;
        Some(Schedule::Fields(Fields {
            minute,
            hour: hour as u32,
            dom: dom as u32,
            month: month as u16,
            // Seven and zero are both Sunday, and the bit set has one
            // of them.
            dow: ((dow | (dow >> 7)) & 0x7f) as u8,
            dom_star: fields[2].starts_with('*'),
            dow_star: fields[4].starts_with('*'),
        }))
    }

    /// The first second strictly after `unix` that this schedule
    /// names, or nothing when it names none.
    ///
    /// A reversed range like `5-1` is a field with nothing in it,
    /// which vixie takes and nothing ever matches, so the answer is
    /// nothing rather than a search that never ends.
    pub fn next_after(&self, unix: i64) -> Option<i64> {
        match self {
            Schedule::Reboot => None,
            Schedule::Every(seconds) => {
                let step = i64::from(*seconds).max(1);
                Some(unix - unix.rem_euclid(step) + step)
            }
            Schedule::Fields(fields) => fields.next_after(unix),
        }
    }

    /// The last second at or before `unix` that this schedule names.
    ///
    /// This is what makes a wake one run rather than a queue: whatever
    /// went by while nothing was running, the job is fired for the
    /// most recent of them and the rest are dropped.
    pub fn last_at_or_before(&self, unix: i64) -> Option<i64> {
        match self {
            Schedule::Reboot => None,
            Schedule::Every(seconds) => {
                let step = i64::from(*seconds).max(1);
                Some(unix - unix.rem_euclid(step))
            }
            Schedule::Fields(fields) => fields.last_at_or_before(unix),
        }
    }
}

impl Fields {
    fn next_after(&self, unix: i64) -> Option<i64> {
        if self.minute == 0 || self.hour == 0 || self.month == 0 || self.never() {
            return None;
        }
        // Minute resolution, so the search starts at the next whole
        // minute and walks. Four years is past any February 29 a
        // schedule can name and short enough to give up on rather
        // than loop forever.
        let mut minute = unix.div_euclid(60) + 1;
        let stop = minute + 4 * 366 * 24 * 60;
        while minute < stop {
            if self.matches(minute) {
                return Some(minute * 60);
            }
            minute += 1;
        }
        None
    }

    fn last_at_or_before(&self, unix: i64) -> Option<i64> {
        if self.minute == 0 || self.hour == 0 || self.month == 0 || self.never() {
            return None;
        }
        let mut minute = unix.div_euclid(60);
        let stop = minute - 4 * 366 * 24 * 60;
        while minute > stop {
            if self.matches(minute) {
                return Some(minute * 60);
            }
            minute -= 1;
        }
        None
    }

    /// Whether the two day fields between them name no day at all,
    /// which is what a backwards range leaves behind.
    fn never(&self) -> bool {
        match (self.dom_star, self.dow_star) {
            (true, true) => false,
            (true, false) => self.dow == 0,
            (false, true) => self.dom == 0,
            (false, false) => self.dom == 0 && self.dow == 0,
        }
    }

    fn matches(&self, minute_number: i64) -> bool {
        let days = minute_number.div_euclid(24 * 60);
        let in_day = minute_number.rem_euclid(24 * 60);
        let minute = in_day % 60;
        let hour = in_day / 60;
        if self.minute & (1u64 << minute) == 0 {
            return false;
        }
        if self.hour & (1u32 << hour) == 0 {
            return false;
        }
        let (_, month, day) = crate::smtp::civil(days);
        if self.month & (1u16 << month) == 0 {
            return false;
        }
        // 1970-01-01 was a Thursday.
        let weekday = (days + 4).rem_euclid(7) as u32;
        let by_day = self.dom & (1u32 << day) != 0;
        let by_week = self.dow & (1u8 << weekday) != 0;
        // Vixie's rule, and the one thing about cron everybody gets
        // wrong: when both day fields are restricted the job runs on
        // either, and when one of them is a star it is the other that
        // decides.
        match (self.dom_star, self.dow_star) {
            (true, true) => true,
            (true, false) => by_week,
            (false, true) => by_day,
            (false, false) => by_day || by_week,
        }
    }
}

/// pg_cron 1.6's interval form: a whole number of seconds, one to
/// fifty nine, with the unit in either number and any case.
fn interval(text: &str) -> Option<u32> {
    let (count, unit) = text.split_once(char::is_whitespace)?;
    let unit = unit.trim();
    if !unit.eq_ignore_ascii_case("second") && !unit.eq_ignore_ascii_case("seconds") {
        return None;
    }
    let seconds: u32 = count.parse().ok()?;
    (1..=59).contains(&seconds).then_some(seconds)
}

/// One field as a bit set. `lo` and `hi` are the field's range and
/// `names` are the three letter words it also takes.
///
/// A character that cannot be part of a list ends the field, and what
/// follows it is dropped rather than refused, which is vixie's own
/// behaviour: its reader stops at the first character it does not
/// know and the rest of the line becomes the command. That is the
/// whole reason `0 0 * * MON#2` is a schedule pg_cron takes.
fn set(field: &str, lo: u32, hi: u32, names: Option<&[&str]>) -> Option<u64> {
    let field = match field.find(|c: char| !c.is_ascii_alphanumeric() && !",-/*".contains(c)) {
        Some(at) => &field[..at],
        None => field,
    };
    if field.is_empty() {
        return None;
    }
    let mut bits = 0u64;
    for part in field.split(',') {
        if part.is_empty() {
            return None;
        }
        let (body, step) = match part.split_once('/') {
            Some((body, step)) => {
                let step: u32 = step.parse().ok()?;
                if step == 0 {
                    return None;
                }
                (body, step)
            }
            None => (part, 1),
        };
        let (from, to) = if body == "*" {
            (lo, hi)
        } else {
            let mut ends = body.split('-');
            let first = value(ends.next()?, lo, hi, names)?;
            let second = match ends.next() {
                Some(text) => value(text, lo, hi, names)?,
                None => match step > 1 {
                    // `5/2` is refused by vixie, but `5-59/2` is not,
                    // and a bare number with a step never gets here
                    // because the range parse above wants a dash.
                    true => return None,
                    false => first,
                },
            };
            if ends.next().is_some() {
                return None;
            }
            (first, second)
        };
        let mut at = from;
        while at <= to {
            bits |= 1u64 << at;
            at += step;
        }
    }
    Some(bits)
}

/// One end of a range: a number in the field's own range, or a name
/// where the field takes names.
fn value(text: &str, lo: u32, hi: u32, names: Option<&[&str]>) -> Option<u32> {
    let found = match text.parse::<u32>() {
        Ok(n) => n,
        Err(_) => {
            let names = names?;
            let at = names
                .iter()
                .position(|name| name.eq_ignore_ascii_case(text))?;
            at as u32 + lo
        }
    };
    (lo..=hi).contains(&found).then_some(found)
}

/// Run the jobs the database asked for, until the process ends.
///
/// Started on the first request through the gate rather than at boot,
/// for the same reason the webhook dispatcher is: a router can be
/// built outside a runtime, and a scheduled job is a promise to a
/// project that is being served.
pub fn tick(app: Arc<App>) {
    tokio::spawn(async move {
        loop {
            match tick_once(&app).await {
                Ok(Again::Never) => return,
                Ok(Again::Later) => {}
                Err(e) => log::warn!("cron: the ticker stopped: {e}"),
            }
            tokio::time::sleep(REDIAL).await;
        }
    });
}

/// Whether a ticker that came back is worth starting again.
enum Again {
    /// There is nothing here to do and nothing that will change that.
    Never,
    /// Somebody else is doing it, or the connection died.
    Later,
}

/// One connection's worth of ticking, until it dies.
async fn tick_once(app: &Arc<App>) -> Result<Again, sql::Error> {
    let Some(pool) = &app.pool else {
        return Ok(Again::Never);
    };
    let (client, mut notes) = pool.listening(CHANNEL).await?;
    // A database with the real pg_cron in it has a launcher on this
    // table already, and two things running one job is two runs of
    // somebody's nightly clean up. Theirs wins: it is the one the
    // project installed.
    let theirs: bool = client
        .query_one(
            "select exists (select 1 from pg_extension where extname = 'pg_cron')",
            &[],
        )
        .await?
        .get(0);
    if theirs {
        log::info!("cron: pg_cron is installed, its launcher has the jobs");
        return Ok(Again::Never);
    }
    // One node fires. The lock is session held rather than
    // transactional, so it is this connection's for as long as this
    // connection lives, and it goes back to the cluster the moment
    // the process does.
    let mine: bool = client
        .query_one("select pg_try_advisory_lock($1)", &[&LOCK])
        .await?
        .get(0);
    if !mine {
        // Not an error, and not a reason to stop: the node holding it
        // may go away, and this asks again.
        log::debug!("cron: another node is firing the jobs");
        return Ok(Again::Later);
    }
    // A run that was in flight when a process died is still saying
    // `running`, and nothing else will ever say otherwise. Holding
    // the lock is what makes this safe: no other node is firing, and
    // this node has not started anything yet, so every row still
    // running belongs to a life that ended.
    let stale = sweep(&client).await?;
    if stale > 0 {
        log::info!("cron: {stale} runs were left behind by a stopped server");
    }
    // A wake is a chance to catch up, and @reboot means exactly this
    // moment.
    let mut waking = true;
    let mut beat = tokio::time::interval(TICK);
    beat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        if let Err(e) = round(&client, pool, waking).await {
            log::warn!("cron: a round did not finish: {e}");
        }
        waking = false;
        tokio::select! {
            note = notes.recv() => if note.is_none() { return Ok(Again::Later) },
            _ = beat.tick() => {}
        }
    }
}

/// Close the runs a stopped server left open, and say how many.
///
/// Upstream's launcher notices its child is gone and writes the row;
/// here the process that would have written it is the one that died,
/// so this is the next process doing it. Public for the same reason
/// `round` is.
pub async fn sweep(client: &Client) -> Result<u64, sql::Error> {
    client
        .execute(
            "update cron.job_run_details
                set status = 'failed',
                    return_message = 'ERROR:  the server running this job stopped',
                    end_time = now()
              where status = 'running'",
            &[],
        )
        .await
}

/// One look at the schedule: what is due, claimed, and run.
///
/// Public because the live test drives it a round at a time rather
/// than starting the ticker and waiting out a minute.
pub async fn round(client: &Client, pool: &sql::Pool, waking: bool) -> Result<(), sql::Error> {
    let now = seconds_now();
    let jobs = due(client, now, waking).await?;
    let mut running = tokio::task::JoinSet::new();
    for (job, occurrence) in jobs {
        let pool = pool.clone();
        running.spawn(async move { run(&pool, &job, occurrence).await });
    }
    while running.join_next().await.is_some() {}
    Ok(())
}

/// The jobs this node is now the one running, each with the
/// occurrence it is running for.
///
/// The claim is the insert: `fired_for` moves forward only when the
/// occurrence being claimed is later than the one already recorded, so
/// two nodes that both think they hold the lock still fire a job once.
async fn due(client: &Client, now: i64, waking: bool) -> Result<Vec<(Job, i64)>, sql::Error> {
    let rows = client
        .query(
            "select j.jobid, j.jobname, j.schedule, j.command, j.username, j.database,
                    extract(epoch from r.fired_for)::bigint
               from cron.job j
               left join zou.cron_run r on r.jobid = j.jobid
              where j.active
              order by j.jobid",
            &[],
        )
        .await?;
    let mut claimed = Vec::new();
    for row in &rows {
        let job = Job {
            id: row.get(0),
            name: row.get(1),
            schedule: row.get(2),
            command: row.get(3),
            username: row.get(4),
            database: row.get(5),
        };
        let last: Option<i64> = row.get(6);
        let Some(schedule) = Schedule::parse(&job.schedule) else {
            log::warn!(
                "cron: job {} has a schedule this cannot read: {}",
                job.id,
                job.schedule
            );
            continue;
        };
        let Some(last) = last else {
            // Never seen before. The row is written as if the job had
            // just run, which is what keeps a job from firing the
            // instant it is scheduled, and the next occurrence is a
            // real one.
            client
                .execute(
                    "insert into zou.cron_run (jobid, fired_for)
                     values ($1, to_timestamp($2::bigint)) on conflict do nothing",
                    &[&job.id, &now],
                )
                .await?;
            continue;
        };
        let Some(occurrence) = when(&schedule, last, now, waking) else {
            continue;
        };
        let taken: u64 = client
            .execute(
                "insert into zou.cron_run (jobid, fired_for)
                 values ($1, to_timestamp($2::bigint))
                 on conflict (jobid) do update set fired_for = excluded.fired_for
                  where zou.cron_run.fired_for < excluded.fired_for",
                &[&job.id, &occurrence],
            )
            .await?;
        if taken == 1 {
            claimed.push((job, occurrence));
        }
        // A job that is not claimed now is claimed on the next tick,
        // a second from here, so the ceiling delays a run rather than
        // dropping one.
        if claimed.len() == AT_ONCE {
            log::debug!("cron: {AT_ONCE} jobs are due at once, the rest wait a tick");
            break;
        }
    }
    Ok(claimed)
}

/// The occurrence a job is due for right now, or nothing.
///
/// `last` is the occurrence it was last fired for, which for a job
/// that has never run is the moment it was first seen, so writing a
/// job does not fire it.
///
/// A job whose next occurrence went by while nothing was running is
/// fired for the most recent occurrence rather than for the first one
/// missed, so a database that was asleep for a day comes back to one
/// run of its hourly job and not twenty four. That is the whole of the
/// catch up policy, and it is the same policy whether the gap was a
/// scale to zero, a restart, or a node that lost the lock.
pub fn when(schedule: &Schedule, last: i64, now: i64, waking: bool) -> Option<i64> {
    if let Schedule::Reboot = schedule {
        // Once per wake, and only for a job that is not already
        // recorded as having run for this one.
        return waking.then_some(now);
    }
    let next = schedule.next_after(last)?;
    if next > now {
        return None;
    }
    Some(schedule.last_at_or_before(now).unwrap_or(next).max(next))
}

/// Seconds since the epoch, which is the clock every schedule here is
/// read against. pg_cron reads `cron.timezone`, whose default on a
/// Supabase project is GMT, so this is that default and there is no
/// other.
fn seconds_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .unwrap_or(0)
}

/// Run one job and write down what happened to it.
///
/// One connection does all of it, which is why the role is set and
/// reset around the command rather than the command being run on a
/// connection of its own: the two rows in `cron.job_run_details`
/// belong to the ticker and the command belongs to whoever scheduled
/// it.
async fn run(pool: &sql::Pool, job: &Job, occurrence: i64) {
    let started = std::time::Instant::now();
    let session = match pool.unscoped().await {
        Ok(session) => session,
        Err(e) => {
            log::warn!("cron: job {} got no connection: {e}", job.id);
            return;
        }
    };
    let pid = session.backend_pid().await.ok();
    let run_id = match record(&session, job, pid, occurrence).await {
        Ok(run_id) => run_id,
        Err(e) => {
            log::warn!("cron: job {} was not recorded: {e}", job.id);
            let _ = session.commit().await;
            return;
        }
    };
    // The job runs as the role that scheduled it, which is what
    // upstream's per job connection does by connecting as that role.
    // A role this cannot become is the job's own problem and is
    // written down as one.
    let outcome = match session
        .simple(&format!("set role {}", quoted(&job.username)))
        .await
    {
        Err(e) => Outcome::Failed(message(&e)),
        Ok(_) => match session.simple(&job.command).await {
            Ok(touched) => Outcome::Done(touched),
            Err(e) => Outcome::Failed(message(&e)),
        },
    };
    let (status, said) = match &outcome {
        Outcome::Done(touched) => ("succeeded", rows(*touched)),
        Outcome::Failed(why) => ("failed", why.clone()),
    };
    if let Err(e) = session.simple("reset role").await {
        log::warn!("cron: job {} left its connection as its role: {e}", job.id);
    }
    if let Err(e) = finish(&session, run_id, status, &said).await {
        log::warn!(
            "cron: job {} finished but was not written down: {e}",
            job.id
        );
    }
    let _ = session.commit().await;
    zou_ops::registry()
        .counter(
            "zou_cron_runs_total",
            "scheduled jobs run",
            &[("status", status)],
        )
        .inc();
    zou_ops::registry()
        .histogram(
            "zou_cron_run_seconds",
            "how long a scheduled job took",
            &BUCKETS,
            &[],
        )
        .observe(started.elapsed().as_secs_f64());
}

const BUCKETS: [f64; 8] = [0.005, 0.05, 0.25, 1.0, 5.0, 30.0, 120.0, 600.0];

enum Outcome {
    Done(u64),
    Failed(String),
}

/// The row `cron.job_run_details` gets when a job starts.
///
/// Upstream writes one at `connecting` with no times on it and moves
/// it through `sending` and `running`. There is nothing to connect
/// here, so the row starts at `running` with the time it started, and
/// the states that describe a launcher opening a socket are not
/// invented.
async fn record(
    session: &sql::Session,
    job: &Job,
    pid: Option<i32>,
    occurrence: i64,
) -> Result<i64, sql::Error> {
    let rows = session
        .query(
            "insert into cron.job_run_details
                 (jobid, job_pid, database, username, command, status, start_time)
             values ($1, $2, $3, $4, $5, 'running', to_timestamp($6::bigint))
             returning runid",
            &[
                &job.id,
                &pid,
                &job.database,
                &job.username,
                &job.command,
                &occurrence,
            ],
        )
        .await?;
    Ok(rows.first().map_or(0, |row| row.get(0)))
}

async fn finish(
    session: &sql::Session,
    run_id: i64,
    status: &str,
    said: &str,
) -> Result<(), sql::Error> {
    session
        .execute(
            "update cron.job_run_details
                set status = $2, return_message = $3, end_time = now()
              where runid = $1",
            &[&run_id, &status, &said],
        )
        .await?;
    Ok(())
}

/// What upstream says about a command that worked, for the half of it
/// this can say: how many rows the last statement touched. Upstream
/// prints the command tag, which the pooled protocol does not carry.
pub fn rows(touched: u64) -> String {
    match touched {
        1 => "1 row".to_string(),
        other => format!("{other} rows"),
    }
}

/// What upstream says about a command that raised, which is the error
/// as psql would have printed it.
fn message(e: &sql::Error) -> String {
    let said = match e.as_db_error() {
        Some(db) => format!("ERROR:  {}", db.message()),
        None => e.to_string(),
    };
    match said.len() > MAX_MESSAGE {
        true => said.chars().take(MAX_MESSAGE).collect(),
        false => said,
    }
}

/// A role name as an identifier. Every name that gets here came out of
/// `cron.job.username`, which a project can write, so it is quoted
/// rather than pasted.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2024-01-01 00:00:00 UTC, a Monday.
    const MONDAY: i64 = 1_704_067_200;

    fn parsed(text: &str) -> Schedule {
        Schedule::parse(text).unwrap_or_else(|| panic!("{text} should be a schedule"))
    }

    /// Every string a real pg_cron 1.6.4 was asked about, and what it
    /// said. Written down rather than reasoned about, because the
    /// surprises in it are the whole point: a step bigger than the
    /// field is fine, a backwards range is fine, a sixth field is
    /// ignored, and the macros are lower case only.
    const READ_OFF_PG_CRON: [(&str, bool); 60] = [
        ("*/5 * * * *", true),
        ("5 4 * * *", true),
        ("@hourly", true),
        ("@daily", true),
        ("@weekly", true),
        ("@monthly", true),
        ("@yearly", true),
        ("@annually", true),
        ("@midnight", true),
        ("@reboot", true),
        ("@every 5 minutes", false),
        ("30 seconds", true),
        ("59 seconds", true),
        ("60 seconds", false),
        ("1 second", true),
        ("1 seconds", true),
        ("2 second", true),
        ("0 0 * * MON", true),
        ("0 0 * * mon", true),
        ("0 0 * * SUN-FRI", true),
        ("0 0 * JAN *", true),
        ("0 0 * jan-mar *", true),
        ("0 0 * * 0", true),
        ("0 0 * * 7", true),
        ("0 0 * * 8", false),
        ("0 0 1-5 * *", true),
        ("0 0 * * 1,3,5", true),
        ("*/0 * * * *", false),
        ("0-59/2 * * * *", true),
        ("60 * * * *", false),
        ("* 24 * * *", false),
        ("* * 0 * *", false),
        ("* * 32 * *", false),
        ("* * * 0 *", false),
        ("* * * 13 *", false),
        ("* * * * *", true),
        ("* * * *", false),
        ("* * * * * *", true),
        ("", false),
        ("  *  *  *  *  *  ", true),
        ("a * * * *", false),
        ("?  * * * *", false),
        ("5/2 * * * *", false),
        ("*/61 * * * *", true),
        ("1-5/2 * * * *", true),
        ("5-1 * * * *", true),
        ("0 0 29 2 *", true),
        ("@every 1 hour", false),
        ("30 SECONDS", true),
        ("0 seconds", false),
        ("-1 seconds", false),
        ("* * * * * garbage", true),
        ("@hourly extra", true),
        ("0 0 * * MON-FRI", true),
        ("* * * MON *", false),
        ("* * * * JAN", false),
        ("JAN * * * *", false),
        ("0 0 L * *", false),
        ("@Daily", false),
        ("daily", false),
    ];

    #[test]
    fn the_same_schedules_pg_cron_takes() {
        for (text, taken) in READ_OFF_PG_CRON {
            assert_eq!(
                Schedule::parse(text).is_some(),
                taken,
                "pg_cron says {taken} about {text:?}"
            );
        }
    }

    #[test]
    fn every_minute_is_the_next_minute() {
        let next = parsed("* * * * *")
            .next_after(MONDAY)
            .expect("there is one");
        assert_eq!(next, MONDAY + 60);
    }

    #[test]
    fn a_daily_job_is_tomorrow_when_today_has_gone() {
        let at_three = parsed("0 3 * * *");
        assert_eq!(at_three.next_after(MONDAY), Some(MONDAY + 3 * 3600));
        assert_eq!(
            at_three.next_after(MONDAY + 3 * 3600),
            Some(MONDAY + 27 * 3600)
        );
        assert_eq!(parsed("@daily").next_after(MONDAY), Some(MONDAY + 86_400));
        assert_eq!(parsed("@hourly").next_after(MONDAY), Some(MONDAY + 3600));
    }

    #[test]
    fn a_step_walks_the_field_from_its_first_value() {
        let every_five = parsed("*/5 * * * *");
        assert_eq!(every_five.next_after(MONDAY), Some(MONDAY + 300));
        assert_eq!(every_five.next_after(MONDAY + 60), Some(MONDAY + 300));
        // A step bigger than the field leaves one value in it, which
        // is the field's first.
        let hourly = parsed("*/61 * * * *");
        assert_eq!(hourly.next_after(MONDAY + 60), Some(MONDAY + 3600));
    }

    #[test]
    fn a_name_is_the_number_it_stands_for() {
        // 2024-01-01 is a Monday, so the next Friday is the fifth.
        let friday = parsed("0 0 * * FRI").next_after(MONDAY).expect("a friday");
        assert_eq!(friday, MONDAY + 4 * 86_400);
        assert_eq!(
            parsed("0 0 * * 5").next_after(MONDAY),
            Some(MONDAY + 4 * 86_400)
        );
        // Sunday is both zero and seven.
        assert_eq!(
            parsed("0 0 * * 7").next_after(MONDAY),
            parsed("0 0 * * 0").next_after(MONDAY)
        );
        // March, from a January.
        let march = parsed("0 0 1 MAR *").next_after(MONDAY).expect("a march");
        assert_eq!(march, MONDAY + 60 * 86_400);
    }

    #[test]
    fn a_leap_day_is_found_and_a_thirtieth_of_february_is_not() {
        // 2024 is a leap year and the first of January is in it.
        let leap = parsed("0 0 29 2 *").next_after(MONDAY).expect("a leap day");
        assert_eq!(leap, MONDAY + 59 * 86_400);
        assert_eq!(parsed("0 0 30 2 *").next_after(MONDAY), None);
        // A backwards range is a field with nothing in it, which
        // never matches and is not an error.
        assert_eq!(parsed("5-1 * * * *").next_after(MONDAY), None);
    }

    #[test]
    fn both_day_fields_restricted_means_either_one() {
        // The first of the month or a Friday, whichever comes first,
        // which is cron's oldest surprise.
        let either = parsed("0 0 1 * FRI");
        assert_eq!(either.next_after(MONDAY), Some(MONDAY + 4 * 86_400));
        // With one of them a star the other decides on its own, so a
        // first that is not a Friday still counts.
        let first = parsed("0 0 1 * *");
        assert_eq!(first.next_after(MONDAY), Some(MONDAY + 31 * 86_400));
    }

    #[test]
    fn an_interval_lands_on_the_multiple() {
        let five = parsed("5 seconds");
        assert_eq!(five, Schedule::Every(5));
        assert_eq!(five.next_after(MONDAY), Some(MONDAY + 5));
        assert_eq!(five.next_after(MONDAY + 1), Some(MONDAY + 5));
        assert_eq!(five.next_after(MONDAY + 4), Some(MONDAY + 5));
        assert_eq!(parsed("1 second").next_after(MONDAY), Some(MONDAY + 1));
    }

    #[test]
    fn a_job_is_due_for_the_occurrence_that_went_by() {
        let daily = parsed("0 3 * * *");
        // Fired for yesterday's three in the morning, and it is now
        // four, so today's is due.
        let yesterday = MONDAY - 21 * 3600;
        assert_eq!(
            when(&daily, yesterday, MONDAY + 4 * 3600, false),
            Some(MONDAY + 3 * 3600)
        );
        // An hour past midnight and today's has not arrived, so
        // nothing is due.
        assert_eq!(when(&daily, yesterday, MONDAY + 3600, false), None);
    }

    #[test]
    fn a_day_of_missed_hours_is_one_run_and_not_twenty_four() {
        let hourly = parsed("@hourly");
        let asleep_since = MONDAY;
        let now = MONDAY + 24 * 3600 + 30;
        // A day of them went by, and what is due is the last one
        // rather than the first, so the backlog is dropped rather
        // than queued.
        assert_eq!(when(&hourly, asleep_since, now, true), Some(now - 30));
        // Having fired for that, nothing is due until the next hour.
        assert_eq!(when(&hourly, now - 30, now, false), None);
        // The same for the interval form.
        let five = parsed("5 seconds");
        assert_eq!(
            when(&five, MONDAY, MONDAY + 3600 + 2, true),
            Some(MONDAY + 3600)
        );
    }

    #[test]
    fn reboot_is_the_wake_and_nothing_else() {
        let reboot = parsed("@reboot");
        assert_eq!(when(&reboot, MONDAY - 1, MONDAY, true), Some(MONDAY));
        assert_eq!(when(&reboot, MONDAY - 1, MONDAY, false), None);
        assert_eq!(reboot.next_after(MONDAY), None);
        assert_eq!(reboot.last_at_or_before(MONDAY), None);
    }

    #[test]
    fn what_a_run_says_it_touched() {
        assert_eq!(rows(0), "0 rows");
        assert_eq!(rows(1), "1 row");
        assert_eq!(rows(12), "12 rows");
    }

    #[test]
    fn a_role_name_is_quoted_rather_than_pasted() {
        assert_eq!(quoted("postgres"), "\"postgres\"");
        assert_eq!(quoted("odd\"name"), "\"odd\"\"name\"");
    }
}
