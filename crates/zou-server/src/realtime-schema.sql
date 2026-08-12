-- The realtime schema, cut down to the part a private channel needs.
--
-- Upstream this is a long line of migrations and the tail of it is
-- postgres changes: a subscription table, a wal decoder, a pile of
-- filter helpers. None of that is here yet. What is here is the table
-- a project writes its channel policies against and the function
-- those policies read the topic out of, because that pair is the whole
-- convention: a private channel is allowed or refused by whatever
-- select and insert policies the project put on realtime.messages.
--
-- The column list is upstream's, in upstream's order, so a policy
-- written for Supabase reads the same columns here. The one difference
-- is that this table is not partitioned. Upstream partitions it by day
-- and has a janitor creating tomorrow's partition and dropping last
-- week's, because upstream keeps messages in it. This server does not
-- keep any: the rows it writes are policy probes inside a transaction
-- that is always rolled back, so there is nothing to expire and
-- nothing to sweep.

create schema if not exists realtime;

create table realtime.messages (
    topic text not null,
    extension text not null,
    payload jsonb,
    event text,
    private boolean default false,
    updated_at timestamp without time zone default now() not null,
    inserted_at timestamp without time zone default now() not null,
    id uuid default gen_random_uuid() not null,
    binary_payload bytea,
    constraint messages_pkey primary key (id, inserted_at),
    constraint messages_payload_exclusive
        check (payload is null or binary_payload is null)
);

create index messages_inserted_at_topic_index on realtime.messages
    using btree (inserted_at desc, topic)
    where (extension = 'broadcast' and private is true);

-- Row level security with no policy on it means no policy says yes,
-- which means every private channel is refused until the project
-- writes one. That is the safe direction to be wrong in, and it is
-- upstream's default too.
alter table realtime.messages enable row level security;

-- What a policy compares against to say which room it is about. The
-- server sets realtime.topic for the length of the check, so this is
-- null in any other transaction, which is what makes a policy written
-- against it refuse everything outside a channel check.
create or replace function realtime.topic() returns text
    language sql stable
as $$
select nullif(current_setting('realtime.topic', true), '')::text;
$$;

grant usage on schema realtime to anon, authenticated, service_role;
grant select, insert, update on table realtime.messages
    to anon, authenticated, service_role;
grant execute on function realtime.topic() to anon, authenticated, service_role;
