-- The `supabase_functions` schema: what a database webhook is made of.
--
-- A webhook on Supabase is a trigger and nothing else. The dashboard
-- writes `create trigger ... execute function
-- supabase_functions.http_request('https://...', 'POST', '{...}',
-- '{}', '1000')` and that is the whole feature, so the compatible
-- surface is one plpgsql function, one audit table, and the argument
-- order they are called with.
--
-- The function below is upstream's, copied from a project's own
-- catalog rather than retyped, which is why it is in upstream's case
-- and upstream's indentation while everything around it is not. It
-- calls net.http_get and net.http_post, so what it actually does is
-- decided by the file next to this one.
--
-- Only applied to a database with no supabase_functions.hooks in it.
-- A database restored from a real project already has all of this and
-- keeps it.

create schema if not exists supabase_functions;

grant usage on schema supabase_functions to postgres, anon, authenticated, service_role;

-- Upstream's migration ledger, and the two rows a project that has
-- never been migrated by hand has in it. It is here because a dump
-- taken from zou and a dump taken from a project should not differ by
-- a table, not because anything reads it.
create table supabase_functions.migrations (
    version text primary key,
    inserted_at timestamptz not null default now()
);

insert into supabase_functions.migrations (version)
values ('initial'), ('20210809183423_update_grants');

create table supabase_functions.hooks (
    id bigserial primary key,
    hook_table_id integer not null,
    hook_name text not null,
    created_at timestamptz not null default now(),
    request_id bigint
);

comment on table supabase_functions.hooks is 'Supabase Functions Hooks: Audit trail for triggered hooks.';

create index supabase_functions_hooks_request_id_idx on supabase_functions.hooks using btree (request_id);
create index supabase_functions_hooks_h_table_id_h_name_idx on supabase_functions.hooks using btree (hook_table_id, hook_name);

create or replace function supabase_functions.http_request() returns trigger
    language plpgsql security definer
    set search_path to 'supabase_functions'
    as $function$
  DECLARE
    request_id bigint;
    payload jsonb;
    url text := TG_ARGV[0]::text;
    method text := TG_ARGV[1]::text;
    headers jsonb DEFAULT '{}'::jsonb;
    params jsonb DEFAULT '{}'::jsonb;
    timeout_ms integer DEFAULT 1000;
  BEGIN
    IF url IS NULL OR url = 'null' THEN
      RAISE EXCEPTION 'url argument is missing';
    END IF;

    IF method IS NULL OR method = 'null' THEN
      RAISE EXCEPTION 'method argument is missing';
    END IF;

    IF TG_ARGV[2] IS NULL OR TG_ARGV[2] = 'null' THEN
      headers = '{"Content-Type": "application/json"}'::jsonb;
    ELSE
      headers = TG_ARGV[2]::jsonb;
    END IF;

    IF TG_ARGV[3] IS NULL OR TG_ARGV[3] = 'null' THEN
      params = '{}'::jsonb;
    ELSE
      params = TG_ARGV[3]::jsonb;
    END IF;

    IF TG_ARGV[4] IS NULL OR TG_ARGV[4] = 'null' THEN
      timeout_ms = 1000;
    ELSE
      timeout_ms = TG_ARGV[4]::integer;
    END IF;

    CASE
      WHEN method = 'GET' THEN
        SELECT http_get INTO request_id FROM net.http_get(
          url,
          params,
          headers,
          timeout_ms
        );
      WHEN method = 'POST' THEN
        payload = jsonb_build_object(
          'old_record', OLD,
          'record', NEW,
          'type', TG_OP,
          'table', TG_TABLE_NAME,
          'schema', TG_TABLE_SCHEMA
        );

        SELECT http_post INTO request_id FROM net.http_post(
          url,
          payload,
          params,
          headers,
          timeout_ms
        );
      ELSE
        RAISE EXCEPTION 'method argument % is invalid', method;
    END CASE;

    INSERT INTO supabase_functions.hooks
      (hook_table_id, hook_name, request_id)
    VALUES
      (TG_RELID, TG_NAME, request_id);

    RETURN NEW;
  END
$function$;

-- The grants upstream's second migration is named after. The audit
-- table is readable and writable by the three api roles there, so it
-- is here, and a project that exposed the schema over rest would get
-- the same answers.
grant all on all tables in schema supabase_functions to postgres, anon, authenticated, service_role;
grant all on all sequences in schema supabase_functions to postgres, anon, authenticated, service_role;
alter default privileges in schema supabase_functions
    grant all on tables to postgres, anon, authenticated, service_role;
alter default privileges in schema supabase_functions
    grant all on sequences to postgres, anon, authenticated, service_role;
