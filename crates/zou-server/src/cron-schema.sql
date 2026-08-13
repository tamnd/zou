-- The `cron` schema, which on a Supabase project is the pg_cron
-- extension, and here is sql plus a ticker in the server.
--
-- pg_cron is a C extension with a launcher process: a project calls
-- cron.schedule(), a row lands in cron.job, the launcher wakes every
-- minute, forks a connection per due job, and writes what happened to
-- cron.job_run_details. Everything a project writes is written against
-- those three names, so those three names are what this file makes,
-- down to the column defaults, the row level security policies and the
-- wording of every refusal.
--
-- What is different is the half that is not sql. A launcher per cluster
-- is standing machinery, and this server has none while nothing is
-- happening, so the job table announces itself the way the http queue
-- does: cron.job_cache_invalidate() calls pg_notify and cron.rs is
-- listening. Upstream that trigger tells the launcher to reread its
-- cache, which is close enough to the same sentence that the trigger
-- keeps its name.
--
-- Three differences a person can see, all written down in
-- docs/cron.md:
--
--   * A job that came due while nothing was running is run once when
--     the server wakes, rather than once per occurrence that went by,
--     and never at all if it has never run.
--   * `return_message` says how many rows the command touched rather
--     than the command tag postgres would have printed, because the
--     tag is not on the wire the pooled protocol hands back.
--   * `job_pid` is the backend running the command, and `nodename` and
--     `nodeport` are the address of the database rather than of a
--     launcher, because there is no separate process to name.
--
-- Only applied to a database that has no cron.job. A project that
-- really has pg_cron installed keeps it, and the ticker stays out of a
-- table that already has a launcher on it.

create schema if not exists cron;
create schema if not exists zou;

-- Not granted to public, which is upstream's answer too: the functions
-- below carry a public execute bit and the schema is what keeps them
-- out of reach of a role nobody meant to give this to.
grant usage on schema cron to postgres;

create sequence cron.jobid_seq;
create sequence cron.runid_seq;

create table cron.job (
    jobid bigint primary key default nextval('cron.jobid_seq'),
    schedule text not null,
    command text not null,
    nodename text not null default 'localhost',
    nodeport integer not null default inet_server_port(),
    database text not null default current_database(),
    username text not null default current_user,
    active boolean not null default true,
    jobname text,
    constraint jobname_username_uniq unique (jobname, username)
);

create table cron.job_run_details (
    jobid bigint,
    runid bigint primary key default nextval('cron.runid_seq'),
    job_pid integer,
    database text,
    username text,
    command text,
    status text,
    return_message text,
    start_time timestamptz,
    end_time timestamptz
);

-- A role sees its own jobs and nobody else's, which is the whole of
-- pg_cron's security model and the reason the unique constraint is on
-- the pair rather than on the name.
alter table cron.job enable row level security;
alter table cron.job_run_details enable row level security;

create policy cron_job_policy on cron.job using (username = current_user);
create policy cron_job_run_details_policy on cron.job_run_details using (username = current_user);

-- Upstream's functions are C and write the table with no permission
-- check at all, so a role that can reach the schema can schedule a job
-- and the policies above are the only thing keeping it to its own.
-- These are plpgsql, so the same sentence has to be said with grants:
-- everybody may write the table, and row level security decides which
-- rows, which is what upstream means rather than what upstream does.
grant select, insert, update, delete on cron.job to public;
grant select, delete on cron.job_run_details to public;
grant usage on sequence cron.jobid_seq to public;
grant usage on sequence cron.runid_seq to public;
grant all on cron.job to postgres;
grant all on cron.job_run_details to postgres;
grant all on sequence cron.jobid_seq to postgres;
grant all on sequence cron.runid_seq to postgres;

-- Upstream this drops the launcher's cached copy of the job list.
-- Here it is what tells the ticker the schedule changed, so a job
-- scheduled a second before it is due is not missed by a minute.
create or replace function cron.job_cache_invalidate()
returns trigger
language plpgsql
as $$
begin
    perform pg_notify('zou_cron', '');
    return null;
end
$$;

create trigger cron_job_cache_invalidate
    after insert or update or delete or truncate on cron.job
    for each statement execute function cron.job_cache_invalidate();

-- Whether a schedule string is one pg_cron would have taken.
--
-- Upstream this is vixie cron's parser in C, and it runs at
-- cron.schedule() time, so a project finds out about a typo when it
-- writes the job rather than when the job silently never runs. That is
-- worth keeping, which is why the rules are here in plpgsql as well as
-- in the server: the same strings are accepted and refused in both, and
-- the live test asserts it against the list this was read off a real
-- pg_cron with.
--
-- Five fields, or one of the @macros, or the interval form pg_cron 1.6
-- added. Anything after the fifth field is ignored, which is vixie's
-- own behaviour and the reason '0 0 * * MON#2' is accepted.
create or replace function cron._valid_schedule(schedule text)
returns boolean
language plpgsql
immutable
as $$
declare
    fields text[];
    field text;
    part text;
    bounds int[][] := array[array[0, 59], array[0, 23], array[1, 31], array[1, 12], array[0, 7]];
    months text[] := array['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
    days text[] := array['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];
    at integer;
    body text;
    step text;
    ends text[];
    one text;
    value integer;
begin
    if schedule is null then
        return false;
    end if;
    schedule := btrim(schedule);
    if schedule = '' then
        return false;
    end if;

    fields := regexp_split_to_array(schedule, '[[:space:]]+');

    -- A macro is the first word, lower case and nothing else, and a
    -- word after it is ignored the way a sixth field is.
    if left(schedule, 1) = '@' then
        return fields[1] in
            ('@reboot', '@yearly', '@annually', '@monthly', '@weekly', '@daily', '@midnight', '@hourly');
    end if;

    -- The interval form: a whole number of seconds from one to
    -- fifty nine, the unit in either number and any case.
    if schedule ~* '^[0-9]+[[:space:]]+seconds?$' then
        return fields[1]::integer between 1 and 59;
    end if;

    if array_length(fields, 1) < 5 then
        return false;
    end if;

    for at in 1..5 loop
        -- A character that cannot be part of a list ends the field,
        -- and what follows it is dropped rather than refused, which
        -- is what vixie's reader does and the reason
        -- '0 0 * * MON#2' is a schedule.
        field := regexp_replace(fields[at], '[^A-Za-z0-9,/*-].*$', '');
        if field = '' then
            return false;
        end if;
        foreach part in array string_to_array(field, ',') loop
            if part = '' then
                return false;
            end if;
            -- A step is whatever follows the first slash, and the
            -- range is what comes before it.
            if position('/' in part) > 0 then
                body := split_part(part, '/', 1);
                step := split_part(part, '/', 2);
                if step !~ '^[0-9]+$' or step::integer = 0 then
                    return false;
                end if;
            else
                body := part;
                step := null;
            end if;

            if body = '*' then
                continue;
            end if;

            ends := string_to_array(body, '-');
            if array_length(ends, 1) > 2 then
                return false;
            end if;
            -- A step wants a range to walk: '5/2' is refused and
            -- '1-5/2' is not, which is vixie's rule and not an
            -- obvious one.
            if array_length(ends, 1) = 1 and step is not null and step::integer > 1 then
                return false;
            end if;
            foreach one in array ends loop
                if one ~ '^[0-9]+$' then
                    value := one::integer;
                elsif at = 4 then
                    -- Names are one based in the array, and so are the
                    -- months they stand for.
                    value := array_position(months, lower(one));
                elsif at = 5 then
                    value := array_position(days, lower(one)) - 1;
                else
                    return false;
                end if;
                if value is null then
                    return false;
                end if;
                if value < bounds[at][1] or value > bounds[at][2] then
                    return false;
                end if;
            end loop;
        end loop;
    end loop;

    return true;
end
$$;

-- Upstream's message and upstream's hint, because a project that
-- catches one is catching a string.
create or replace function cron._check_schedule(schedule text)
returns void
language plpgsql
immutable
as $$
begin
    if not cron._valid_schedule(schedule) then
        raise exception 'invalid schedule: %', schedule
            using hint = 'Use cron format (e.g. 5 4 * * *), or interval format ''[1-59] seconds''';
    end if;
end
$$;

-- A job with a name, which is upsert by name: scheduling the same name
-- twice moves the existing job rather than making a second one, and
-- hands back the id it already had.
create or replace function cron.schedule(job_name text, schedule text, command text)
returns bigint
language plpgsql
as $$
declare
    id bigint;
begin
    perform cron._check_schedule(schedule);
    insert into cron.job as j (jobname, schedule, command)
    values (job_name, schedule, command)
    on conflict (jobname, username) do update
        set schedule = excluded.schedule,
            command = excluded.command,
            active = true
    returning j.jobid into id;
    return id;
end
$$;

-- A job with no name, which is never an upsert: two calls with the
-- same schedule and the same command are two jobs, both of which run.
create or replace function cron.schedule(schedule text, command text)
returns bigint
language plpgsql
as $$
declare
    id bigint;
begin
    perform cron._check_schedule(schedule);
    insert into cron.job (schedule, command)
    values (schedule, command)
    returning jobid into id;
    return id;
end
$$;

create or replace function cron.unschedule(job_id bigint)
returns boolean
language plpgsql
as $$
declare
    gone integer;
begin
    delete from cron.job where jobid = job_id;
    get diagnostics gone = row_count;
    if gone = 0 then
        raise exception 'could not find valid entry for job %', job_id;
    end if;
    return true;
end
$$;

create or replace function cron.unschedule(job_name text)
returns boolean
language plpgsql
as $$
declare
    gone integer;
begin
    delete from cron.job where jobname = job_name;
    get diagnostics gone = row_count;
    if gone = 0 then
        raise exception 'could not find valid entry for job ''%''', job_name;
    end if;
    return true;
end
$$;

-- Every argument but the id is null for leave it alone, which is why
-- there is no way to clear a job's name through this and there is none
-- upstream either.
create or replace function cron.alter_job(
    job_id bigint,
    schedule text default null,
    command text default null,
    database text default null,
    username text default null,
    active boolean default null
)
returns void
language plpgsql
as $$
declare
    changed integer;
begin
    if schedule is not null then
        perform cron._check_schedule(schedule);
    end if;
    update cron.job j
       set schedule = coalesce(alter_job.schedule, j.schedule),
           command = coalesce(alter_job.command, j.command),
           database = coalesce(alter_job.database, j.database),
           username = coalesce(alter_job.username, j.username),
           active = coalesce(alter_job.active, j.active)
     where j.jobid = job_id;
    get diagnostics changed = row_count;
    if changed = 0 then
        raise exception 'Job % does not exist or you don''t own it', job_id;
    end if;
end
$$;

-- One database per deployment here, so this refuses another one the
-- way upstream refuses a database that is not there. The name is kept
-- because a migration copied off the docs calls it.
create or replace function cron.schedule_in_database(
    job_name text,
    schedule text,
    command text,
    database text,
    username text default null,
    active boolean default true
)
returns bigint
language plpgsql
as $$
declare
    id bigint;
begin
    if database is not null and database <> current_database() then
        raise exception 'database "%" does not exist', database;
    end if;
    perform cron._check_schedule(schedule);
    insert into cron.job as j (jobname, schedule, command, username, active)
    values (
        job_name,
        schedule,
        command,
        coalesce(schedule_in_database.username, current_user),
        schedule_in_database.active
    )
    on conflict (jobname, username) do update
        set schedule = excluded.schedule,
            command = excluded.command,
            active = excluded.active
    returning j.jobid into id;
    return id;
end
$$;

-- The four settings pg_cron puts in pg_settings, with the values a
-- Supabase project has. They are read rather than obeyed: the timezone
-- is GMT because that is what schedules here are read against, one
-- database is the whole of `cron.database_name`, and there are no
-- background workers to turn on. Set on the database rather than in
-- the session so that `show cron.timezone` answers, and skipped
-- without complaint when this connection does not own the database.
do $$
begin
    execute format('alter database %I set cron.database_name = %L', current_database(), current_database());
    execute format('alter database %I set cron.timezone = ''GMT''', current_database());
    execute format('alter database %I set cron.max_running_jobs = ''32''', current_database());
    execute format('alter database %I set cron.use_background_workers = ''off''', current_database());
exception when insufficient_privilege then
    null;
end
$$;

-- What the ticker keeps: the occurrence a job was last fired for. It
-- is what makes two nodes serving one database fire a job once rather
-- than twice, and what a wake reads to decide whether a job was missed
-- while nothing was running.
create table if not exists zou.cron_run (
    jobid bigint primary key,
    fired_for timestamptz not null
);
