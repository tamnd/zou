-- Sending to a room from sql, and how what is sent gets out.
--
-- Two halves with a table between them. The first half is upstream's:
-- realtime.send, realtime.send_binary and realtime.broadcast_changes,
-- copied from a Supabase project's own schema, and all three of them
-- do exactly one thing, which is insert a row into realtime.messages.
-- A trigger written against them is written against that shape, down
-- to the argument order and the warning a refused insert turns into,
-- so they are here as they are rather than as they would be if this
-- were being designed today.
--
-- The second half is not upstream's, because it cannot be. Upstream
-- reads the rows back out of a logical replication slot and fans them
-- to the sockets from there. That needs a slot, a publication, a wal
-- decoder and a process holding all three per project, which is a lot
-- of standing machinery for a server whose whole point is to have
-- none while nothing is happening. So the row announces itself: an
-- after insert trigger calls pg_notify, and the server listens.
--
-- Notifications are transactional, which is what makes this safe next
-- to the policy probes. A probe inserts its rows and rolls back, and
-- a notification from a transaction that rolled back is never
-- delivered, so nothing is ever fanned out for a check that was only
-- ever asking a question.
--
-- Applied on every boot rather than only on a fresh database, unlike
-- the schema file next to it. A database that has been served by an
-- older zou already has realtime.messages in it, so the fresh only
-- guard would mean it never sees any of this. Everything here is
-- create or replace and a trigger that is dropped first, so applying
-- it to a database that has it already is a few catalog writes.

-- Upstream's three, and the reason the whole block is guarded: on a
-- database that came from a real Supabase project these already exist
-- and belong to supabase_realtime_admin, and the right answer there is
-- to leave the project's own functions alone rather than to fail the
-- boot over them.
do $do$
begin
    execute $fn$
        create or replace function realtime.send(
            payload jsonb,
            event text,
            topic text,
            private boolean default true
        ) returns void
        language plpgsql as $body$
        declare
            generated_id uuid;
            final_payload jsonb;
        begin
            begin
                generated_id := gen_random_uuid();

                -- The id the client will see inside the payload, so
                -- that a sender and a receiver are talking about the
                -- same message.
                if payload ? 'id' then
                    final_payload := payload;
                else
                    final_payload := jsonb_set(payload, '{id}', to_jsonb(generated_id));
                end if;

                -- What the policies on this table read, for as long as
                -- this insert takes.
                execute format('set local realtime.topic to %L', topic);

                insert into realtime.messages (id, payload, event, topic, private, extension)
                values (generated_id, final_payload, event, topic, private, 'broadcast');
            exception
                -- Upstream swallows everything here, a refusal by the
                -- project's own policies included, and a caller that
                -- may not write to the room gets a warning and a
                -- message nobody hears rather than a failed statement.
                -- A trigger calling this is usually doing something
                -- else that matters more, which is the argument for
                -- it.
                when others then
                    raise warning 'WarnSendingBroadcastMessage: %', sqlerrm;
            end;
        end
        $body$
    $fn$;

    execute $fn$
        create or replace function realtime.send_binary(
            payload bytea,
            event text,
            topic text,
            private boolean default true
        ) returns void
        language plpgsql as $body$
        declare
            generated_id uuid;
        begin
            begin
                generated_id := gen_random_uuid();

                execute format('set local realtime.topic to %L', topic);

                insert into realtime.messages (id, binary_payload, event, topic, private, extension)
                values (generated_id, payload, event, topic, private, 'broadcast');
            exception
                when others then
                    raise warning 'WarnSendingBroadcastMessage: %', sqlerrm;
            end;
        end
        $body$
    $fn$;

    -- What a row level trigger calls, and the reason the arguments are
    -- in this order: a project writes `realtime.broadcast_changes(...,
    -- tg_op, tg_table_name, tg_table_schema, new, old)` and copies it
    -- between tables, so the order is part of the interface.
    execute $fn$
        create or replace function realtime.broadcast_changes(
            topic_name text,
            event_name text,
            operation text,
            table_name text,
            table_schema text,
            new record,
            old record,
            level text default 'ROW'
        ) returns void
        language plpgsql as $body$
        declare
            row_data jsonb := '{}'::jsonb;
        begin
            if level = 'STATEMENT' then
                raise exception 'function can only be triggered for each row, not for each statement';
            end if;
            if operation = 'INSERT' or operation = 'UPDATE' or operation = 'DELETE' then
                row_data := jsonb_build_object(
                    'old_record', old,
                    'record', new,
                    'operation', operation,
                    'table', table_name,
                    'schema', table_schema
                );
                perform realtime.send(row_data, event_name, topic_name);
            else
                raise exception 'Unexpected operation type: %', operation;
            end if;
        exception
            when others then
                raise exception 'Failed to process the row: %', sqlerrm;
        end
        $body$
    $fn$;
exception when insufficient_privilege then
    raise warning 'zou: the realtime send functions belong to somebody else: %', sqlerrm;
end
$do$;

-- And the part that is ours: how a row gets from the table to a
-- socket.
--
-- The whole message rides in the notification when it fits, because
-- the alternative is a round trip per message to read back a row the
-- server has just been told about. A notification is capped under 8000
-- bytes, so anything bigger, and anything binary, sends the id alone
-- and is read back. Which of the two happened is visible to nobody
-- but this file and the listener.
do $do$
begin
    create schema if not exists zou;
    execute $fn$
        create or replace function zou.realtime_sent() returns trigger
        language plpgsql as $body$
        declare
            note text;
        begin
            note := null;
            if new.binary_payload is null then
                note := json_build_object(
                    'id', new.id,
                    'topic', new.topic,
                    'event', new.event,
                    'private', new.private,
                    'payload', new.payload
                )::text;
                if octet_length(note) > 7000 then
                    note := null;
                end if;
            end if;
            perform pg_notify(
                'zou_realtime',
                coalesce(note, json_build_object('id', new.id)::text)
            );
            return null;
        end
        $body$
    $fn$;
    drop trigger if exists zou_realtime_sent on realtime.messages;
    -- Broadcast only, and after the insert. Presence rows in this
    -- table are what a policy probe writes and nothing else, and a
    -- probe is a question rather than a message.
    create trigger zou_realtime_sent after insert on realtime.messages
        for each row when (new.extension = 'broadcast')
        execute function zou.realtime_sent();
exception when insufficient_privilege then
    raise warning 'zou: no trigger on realtime.messages, database sends will not be delivered: %', sqlerrm;
end
$do$;
