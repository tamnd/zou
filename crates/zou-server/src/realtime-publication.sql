-- The publication a project adds its tables to.
--
-- Postgres streams a table's changes to a logical decoder only if the
-- table is in a publication the decoder asked for, which is what makes
-- postgres changes opt in rather than every write in the database
-- going out to sockets. Supabase's name for that publication is
-- supabase_realtime, and every project that has ever turned realtime on
-- for a table has run
--
--   alter publication supabase_realtime add table todos;
--
-- by hand or through a dashboard that does it for them. Reading a
-- differently named publication here would make that line do nothing,
-- so the name is not a default that could have been anything.
--
-- Created empty, which is upstream's shape too: a publication with no
-- tables in it publishes nothing, and a project that wants a table in
-- it says so. `for all tables` would have been the friendlier default
-- and the wrong one, since it would put every write in the database
-- through the decoder whether or not anybody subscribed.
--
-- Applied on every boot rather than once on a fresh database, because
-- a database an older zou already served has the realtime schema in it
-- and would never see this otherwise.
--
-- Creating a publication needs the create privilege on the database,
-- which the owner has and a role somebody handed out narrowly may not.
-- That is a notice rather than a failed boot: the rest of this server
-- works without it, and refusing to start over a feature the project
-- may not use would be the wrong end of the trade.

do $$
begin
    if not exists (select 1 from pg_publication where pubname = 'supabase_realtime') then
        create publication supabase_realtime;
    end if;
exception
    -- Somebody else created it between the check and the create, which
    -- is fine: the point is that it exists.
    when duplicate_object then null;
    when insufficient_privilege then
        raise notice 'no privilege to create publication supabase_realtime, so postgres changes will have nothing to read';
end
$$;
