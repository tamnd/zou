-- The `net` schema, which on a Supabase project is the pg_net
-- extension, and here is sql plus a dispatcher in the server.
--
-- pg_net is a C extension with a background worker: a project calls
-- net.http_post(), a row lands in net.http_request_queue, the worker
-- picks it up, curl makes the call, and the answer lands in
-- net._http_response. Everything a project writes is written against
-- those three names, so those three names are what this file makes.
--
-- What is different is the half that is not sql. A background worker
-- per database is standing machinery, and this server has none while
-- nothing is happening, so the row announces itself the same way a
-- database send does: net.wake() calls pg_notify and webhook.rs is
-- listening. The rest of the shape is upstream's, function signatures,
-- defaults, column types and all, taken from a real project rather
-- than from the extension's source.
--
-- Two differences a person can see, both written down in
-- docs/webhooks.md:
--
--   * A queued row is deleted when the request has an answer rather
--     than when a worker picks it up, so net.http_request_queue is
--     what has not been delivered yet. Upstream a row in flight is in
--     neither table.
--   * A request that could not be delivered is tried again, which
--     upstream never does. How often and for what is the dispatcher's
--     business, not this file's.
--
-- Only applied to a database that has no net.http_request_queue. A
-- project that really has pg_net installed keeps it, and the
-- dispatcher stays out of a queue that already has a worker on it.

create schema if not exists net;
create schema if not exists zou;
grant usage on schema net to public;

-- Upstream's domain, check constraint and all, so that an insert
-- straight into the queue is refused here for the reason it is
-- refused there.
do $$
begin
    create domain net.http_method as text
        check (value ilike 'get' or value ilike 'post' or value ilike 'delete');
exception when duplicate_object then
    null;
end
$$;

create type net.request_status as enum ('PENDING', 'SUCCESS', 'ERROR');

create type net.http_response as (
    status_code integer,
    headers jsonb,
    body text
);

create type net.http_response_result as (
    status net.request_status,
    message text,
    response net.http_response
);

-- Unlogged upstream and unlogged here. The queue is work in flight and
-- the responses are read back within seconds of being written, so
-- neither is worth a write ahead log record, and a crash losing them
-- is the same answer either way.
create unlogged table net.http_request_queue (
    id bigserial,
    method net.http_method not null,
    url text not null,
    headers jsonb,
    body bytea,
    timeout_milliseconds integer not null
);

create unlogged table net._http_response (
    id bigint,
    status_code integer,
    content_type text,
    headers jsonb,
    content text,
    timed_out boolean,
    error_msg text,
    created timestamptz not null default now()
);

create index _http_response_created_idx on net._http_response using btree (created);

grant all on net.http_request_queue to public;
grant all on net._http_response to public;
grant all on sequence net.http_request_queue_id_seq to public;

-- Percent encoding, the unreserved set of RFC 3986: letters, digits
-- and the four characters -._~ go through and every other byte of the
-- utf-8 encoding becomes %XX in upper case. Upstream this is C and
-- immutable, and it is immutable here for the same reason, which is
-- that a query building a url out of a column should be able to plan
-- it once.
-- A character at a time, and every byte of the ones that are not
-- unreserved, which is what makes this right for text that is not
-- ascii: é is two bytes and becomes two escapes.
create or replace function net._urlencode_string(string character varying)
returns text
language sql
immutable
strict
as $$
    select coalesce(
        string_agg(
            case
                when part ~ '^[A-Za-z0-9_.~-]$' then part
                else (
                    select string_agg('%' || upper(byte[1]), '')
                    from regexp_matches(
                        encode(convert_to(part, 'UTF8'), 'hex'), '..', 'g'
                    ) as byte
                )
            end,
            ''
            order by ordinality
        ),
        ''
    )
    from regexp_split_to_table(string, '') with ordinality as s(part, ordinality)
$$;

-- The url the queue gets: the one that was asked for with the params
-- hung off it.
--
-- Validated rather than parsed, and the three refusals are the three
-- curl reports through pg_net, because a project that catches one is
-- catching a message. The params go in front of the fragment, which is
-- where a query string belongs and where curl puts them.
create or replace function net._encode_url_with_params_array(url text, params_array text[])
returns text
language plpgsql
immutable
as $$
declare
    front text;
    fragment text := '';
    cut integer;
begin
    if url is null then
        return null;
    end if;
    if url ~ '[[:space:]]' or url ~ '[[:cntrl:]]' then
        raise exception 'invalid URL "%": Malformed input to a URL function', url;
    end if;
    if url !~ '^[A-Za-z][A-Za-z0-9+.-]*://' then
        raise exception 'invalid URL "%": Bad scheme', url;
    end if;
    if regexp_replace(url, '^[A-Za-z][A-Za-z0-9+.-]*://', '') ~ '^([/?#]|$)' then
        raise exception 'invalid URL "%": No host part in the URL', url;
    end if;

    cut := position('#' in url);
    if cut > 0 then
        front := substring(url from 1 for cut - 1);
        fragment := substring(url from cut);
    else
        front := url;
    end if;

    if params_array is null or cardinality(params_array) = 0 then
        return front || fragment;
    end if;
    return front
        || case when position('?' in front) > 0 then '&' else '?' end
        || array_to_string(params_array, '&')
        || fragment;
end
$$;

-- What tells the server there is something in the queue.
--
-- Upstream this pokes the background worker. There is no worker here,
-- so it is a notification, and the notification is transactional,
-- which is the whole reason a webhook fires when its transaction
-- commits and never when it rolls back.
create or replace function net.wake()
returns void
language sql
as $$
    select pg_notify('zou_http_request', '')
$$;

-- Three upstream calls that a worker answers and nothing answers here,
-- kept because a project's health check calls them. There is no worker
-- to be down, to wait for, or to restart.
create or replace function net.check_worker_is_up()
returns void
language sql
as $$
    select null::void
$$;

create or replace function net.wait_until_running()
returns void
language sql
as $$
    select null::void
$$;

create or replace function net.worker_restart()
returns boolean
language sql
as $$
    select true
$$;

create or replace function net.http_get(
    url text,
    params jsonb default '{}'::jsonb,
    headers jsonb default '{}'::jsonb,
    timeout_milliseconds integer default 5000
)
returns bigint
language plpgsql
security definer
set search_path to 'net'
as $$
declare
    request_id bigint;
    params_array text[];
begin
    select coalesce(array_agg(net._urlencode_string(key) || '=' || net._urlencode_string(value)), '{}')
    into params_array
    from jsonb_each_text(params);

    insert into net.http_request_queue(method, url, headers, timeout_milliseconds)
    values (
        'GET',
        net._encode_url_with_params_array(url, params_array),
        headers,
        timeout_milliseconds
    )
    returning id
    into request_id;

    perform net.wake();

    return request_id;
end
$$;

create or replace function net.http_post(
    url text,
    body jsonb default '{}'::jsonb,
    params jsonb default '{}'::jsonb,
    headers jsonb default '{"Content-Type": "application/json"}'::jsonb,
    timeout_milliseconds integer default 5000
)
returns bigint
language plpgsql
security definer
set search_path to 'net'
as $$
declare
    request_id bigint;
    params_array text[];
    content_type text;
begin
    select
        header_value into content_type
    from
        jsonb_each_text(coalesce(headers, '{}'::jsonb)) r(header_name, header_value)
    where
        lower(header_name) = 'content-type'
    limit
        1;

    -- A caller that sent headers and forgot the content type gets it
    -- back, and a caller that sent a different one is refused. Both
    -- are upstream's, and the refusal is why a webhook cannot post
    -- form encoded bodies on either server.
    if content_type is null then
        select headers || '{"Content-Type": "application/json"}'::jsonb into headers;
    end if;

    if content_type <> 'application/json' then
        raise exception 'Content-Type header must be "application/json"';
    end if;

    select
        coalesce(array_agg(net._urlencode_string(key) || '=' || net._urlencode_string(value)), '{}')
    into
        params_array
    from
        jsonb_each_text(params);

    insert into net.http_request_queue(method, url, headers, body, timeout_milliseconds)
    values (
        'POST',
        net._encode_url_with_params_array(url, params_array),
        headers,
        convert_to(body::text, 'UTF8'),
        timeout_milliseconds
    )
    returning id
    into request_id;

    perform net.wake();

    return request_id;
end
$$;

create or replace function net.http_delete(
    url text,
    params jsonb default '{}'::jsonb,
    headers jsonb default '{}'::jsonb,
    timeout_milliseconds integer default 5000,
    body jsonb default null::jsonb
)
returns bigint
language plpgsql
as $$
declare
    request_id bigint;
    params_array text[];
begin
    select coalesce(array_agg(net._urlencode_string(key) || '=' || net._urlencode_string(value)), '{}')
    into params_array
    from jsonb_each_text(params);

    insert into net.http_request_queue(method, url, headers, body, timeout_milliseconds)
    values (
        'DELETE',
        net._encode_url_with_params_array(url, params_array),
        headers,
        convert_to(body::text, 'UTF8'),
        timeout_milliseconds
    )
    returning id
    into request_id;

    perform net.wake();

    return request_id;
end
$$;

-- Waiting for an answer by polling for it, which is what upstream
-- does and the only thing that can be done from inside a statement.
create or replace function net._await_response(request_id bigint)
returns boolean
language plpgsql
as $$
declare
    rec net._http_response;
begin
    while rec is null loop
        select *
        into rec
        from net._http_response
        where id = request_id;

        if rec is null then
            perform pg_sleep(0.05);
        end if;
    end loop;

    return true;
end
$$;

create or replace function net._http_collect_response(request_id bigint, async boolean default true)
returns net.http_response_result
language plpgsql
as $$
declare
    rec net._http_response;
begin
    if not async then
        perform net._await_response(request_id);
    end if;

    select *
    into rec
    from net._http_response
    where id = request_id;

    -- A request still in flight and a request that never existed are
    -- the same answer here, which is upstream's own todo and not
    -- something to fix on one server only.
    if rec is null or rec.error_msg is not null then
        return (
            'ERROR',
            coalesce(rec.error_msg, 'request matching request_id not found'),
            null
        )::net.http_response_result;
    end if;

    return (
        'SUCCESS',
        'ok',
        (
            rec.status_code,
            rec.headers,
            rec.content
        )::net.http_response
    )::net.http_response_result;
end
$$;

-- Deprecated upstream, and upstream's body does not run: it selects
-- into nothing, which postgres refuses at first call. This one hands
-- back what it says it hands back, on the grounds that nobody was
-- depending on the error.
create or replace function net.http_collect_response(request_id bigint, async boolean default true)
returns net.http_response_result
language plpgsql
as $$
begin
    raise notice 'The net.http_collect_response function is deprecated.';
    return net._http_collect_response(request_id, async);
end
$$;

grant execute on function net.http_get(text, jsonb, jsonb, integer) to anon, authenticated, service_role;
grant execute on function net.http_post(text, jsonb, jsonb, jsonb, integer) to anon, authenticated, service_role;

-- What the dispatcher keeps about a request the first attempt did not
-- finish. Nothing is written here for a webhook that was delivered on
-- the first try, which is almost all of them, and a row here without
-- one in the queue is impossible because both are deleted together.
create table if not exists zou.http_attempt (
    id bigint primary key,
    tries integer not null default 0,
    next_at timestamptz not null default now(),
    taken_at timestamptz,
    last_error text
);
