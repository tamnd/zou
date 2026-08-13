# Scheduled jobs

A statement runs every night at three, without anything outside the database to run it.

On Supabase that is the pg_cron extension: a `cron` schema, a `cron.job` table, and a launcher process that wakes every minute and forks a connection for each job that is due.
zou has the same schema, the same functions and the same two tables, and the firing is done by the server rather than by a launcher in the database.

## Writing one

```sql
select cron.schedule(
    'nightly-vacuum',
    '0 3 * * *',
    $$ delete from events where at < now() - interval '30 days' $$
);
```

The name is optional, and it is what makes the call an upsert: scheduling `nightly-vacuum` again moves the job that is there and hands back the id it already had, while `cron.schedule('0 3 * * *', '...')` with no name makes a new job every time it is called.

`cron.schedule`, `cron.schedule_in_database`, `cron.unschedule` by id or by name, `cron.alter_job` and `cron.job_cache_invalidate` are all there with upstream's argument names, defaults and refusals.
`cron.schedule_in_database` refuses a database that is not this one, which is the same error upstream raises for a database that is not there, because a deployment is one database.

Schedules are vixie cron, which is five fields, or one of `@reboot`, `@hourly`, `@daily`, `@midnight`, `@weekly`, `@monthly`, `@yearly` and `@annually`, or pg_cron 1.6's interval form of one to fifty nine `seconds`.
A schedule is checked when the job is written rather than when it is due, with upstream's message and upstream's hint, so a typo is an error at the call and not a job that quietly never runs.
The acceptance test for the parser is a list of strings read off a real pg_cron 1.6.4, down to the corners: `*/61 * * * *` is taken and `*/0 * * * *` is not, `1-5/2` is taken and `5/2` is not, `5-1` is taken and matches nothing, day and month names work in their own field and nowhere else, and anything after a field's last usable character is dropped rather than refused, which is why `0 0 * * MON#2` is a schedule.

## What happened to it

Every run writes a row to `cron.job_run_details`, the way it does upstream.

```sql
select jobid, status, return_message, start_time, end_time
  from cron.job_run_details order by runid desc limit 5;
```

`status` is `running` while the command is running and `succeeded` or `failed` after it.
A failure carries the error postgres gave, in the form a psql user would have seen it: `ERROR:  division by zero`.
A run that was in flight when the server stopped is closed by the next server to take the job table, as a failure saying so, rather than saying `running` for ever.

Both tables have row level security on them and upstream's policies, so a role sees its own jobs and its own runs and nobody else's, and the unique constraint is on the name and the owner together rather than on the name.
A job runs as the role that scheduled it.

## Catch up on wake

Upstream's launcher is always running, so a job that is due is run within the minute, and there is nothing to catch up.
A zou deployment can be asleep, so there is.

A job that came due while nothing was running is run once when the server wakes, for the most recent occurrence it missed rather than for each one.
A database that was asleep for a day comes back to one run of its hourly job, not twenty four.
This is the same policy whether the gap was a scale to zero, a restart, or a node that lost the lock, and it is the policy pg_cron itself has for a cluster that was down.

A job that has never run does not run when it is written.
The first time the ticker sees a job it writes down that moment as the job's last firing, so scheduling a nightly clean up at noon does not clean up at noon.

`@reboot` means the wake: it fires once each time the ticker takes the job table, which for a project that never sleeps is once per process and for one that does is once per wake.

## How it runs

There is no launcher, for the same reason there is no pg_net worker: a process per database running while nothing is happening is the thing this server is built not to have.

`cron.job` has upstream's `cron_job_cache_invalidate` trigger on it, and here it is a `pg_notify` rather than a message to a launcher, so a job scheduled a second before it is due is not missed by a minute.
The ticker starts on the first request through the front door, looks at the schedule once a second, and one node holds an advisory lock so a deployment of three serves one database and fires each job once.
Every firing is claimed by writing the occurrence it is for, so a lock that changed hands mid minute costs a duplicate run rather than causing one.
Eight jobs are started at once and the rest wait a tick, which delays a run rather than dropping one.

A database that really has the pg_cron extension installed keeps it: the schema is left exactly as the extension made it, and the ticker stays out of a table that already has a launcher on it.

## Differences

Three that are visible from sql.

* `return_message` says how many rows the command touched, as `1 row` or `12 rows`, where upstream says the command tag postgres printed, as `INSERT 0 1` or `DO`. The tag is not on the wire the pooled protocol hands back.
* `job_pid` is the backend that ran the command, and `nodename` and `nodeport` are the address of the database rather than of a launcher, because there is no separate process to name.
* The catch up above. Upstream's launcher runs a job that is due within the minute and has no notion of a gap, so the question does not come up for it.

`cron.database_name`, `cron.timezone`, `cron.max_running_jobs` and `cron.use_background_workers` are the four settings a project might read.
Schedules are read against GMT, which is what `cron.timezone` is on a Supabase project, and there is no other.
