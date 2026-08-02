-- The schema the rest suite asks about.
--
-- Applied to every target before the cases run, so a recording taken
-- from PostgREST and a run against zou are answering about the same
-- rows. Everything here is fixed: no now(), no serial, no generated
-- ids, and an order by on every case that reads more than one row.
-- A case whose answer is not the same twice cannot be diffed.
--
-- The tables live in their own schema so that a database somebody else
-- is also using does not change what these cases see. public gets one
-- table, which is only there so that switching schemas with a profile
-- header has somewhere to switch to.

do $$
begin
  if not exists (select from pg_roles where rolname = 'anon') then
    create role anon nologin noinherit;
  end if;
  if not exists (select from pg_roles where rolname = 'authenticated') then
    create role authenticated nologin noinherit;
  end if;
  if not exists (select from pg_roles where rolname = 'service_role') then
    create role service_role nologin noinherit bypassrls;
  end if;
  -- Whoever connects has to be able to become them, which is how both
  -- PostgREST and zou apply the role a key carries.
  execute format('grant anon, authenticated, service_role to %I', current_user);
end
$$;

drop schema if exists conformance cascade;
create schema conformance;

create table conformance.projects (
  id int primary key,
  name text not null,
  live boolean not null default true,
  budget numeric
);

create table conformance.tasks (
  id int primary key,
  project_id int not null references conformance.projects (id),
  title text not null,
  done boolean not null default false,
  rank int,
  due date
);

create table conformance.people (
  id int primary key,
  name text not null,
  tags text[] not null default '{}',
  profile jsonb not null default '{}'
);

-- What the writing cases write to. Nothing reads it, so an insert here
-- cannot change what a later select sees.
create table conformance.scratch (
  id int primary key,
  label text not null,
  count int not null default 0
);

create table conformance.secrets (
  id int primary key,
  body text not null
);

insert into conformance.projects (id, name, live, budget) values
  (1, 'orbit', true, 1200.50),
  (2, 'anvil', false, null),
  (3, 'kite', true, 30.00);

insert into conformance.tasks (id, project_id, title, done, rank, due) values
  (1, 1, 'draw the map', true, 2, '2024-01-05'),
  (2, 1, 'name the roads', false, 1, '2024-02-11'),
  (3, 1, 'walk them', false, null, null),
  (4, 2, 'sharpen', true, 3, '2023-12-24'),
  (5, 3, 'fly it', false, 1, '2024-06-30');

insert into conformance.people (id, name, tags, profile) values
  (1, 'ada', '{maps,roads}', '{"city": "leeds", "years": 9}'),
  (2, 'bo', '{}', '{"city": "hue"}'),
  (3, 'cy', '{roads}', '{}');

insert into conformance.scratch (id, label, count) values
  (1, 'one', 1),
  (2, 'two', 2),
  (3, 'three', 3),
  (4, 'four', 4),
  (5, 'five', 5),
  (6, 'six', 6);

insert into conformance.secrets (id, body) values
  (1, 'the map is wrong'),
  (2, 'so is the other one');

alter table conformance.secrets enable row level security;

create policy read_when_signed_in on conformance.secrets
  for select to authenticated using (true);

create function conformance.add(a int, b int) returns int
  language sql immutable as $$ select a + b $$;

create function conformance.greet(name text default 'world') returns text
  language sql immutable as $$ select 'hello ' || name $$;

create function conformance.tasks_of(project int) returns setof conformance.tasks
  language sql stable as $$
    select * from conformance.tasks where project_id = project order by id
  $$;

create function conformance.nothing() returns void
  language plpgsql volatile as $$ begin end $$;

create function conformance.refuse() returns int
  language plpgsql immutable as $$
  begin
    raise exception 'no' using errcode = 'P0001';
  end
  $$;

grant usage on schema conformance to anon, authenticated, service_role;
grant all on all tables in schema conformance to anon, authenticated, service_role;
grant execute on all functions in schema conformance to anon, authenticated, service_role;

-- The one table outside the suite's own schema, so that a request that
-- names a schema has a different answer to a request that does not.
drop table if exists public.conformance_notes cascade;
create table public.conformance_notes (
  id int primary key,
  body text not null
);
insert into public.conformance_notes (id, body) values (1, 'kept in public');
grant usage on schema public to anon, authenticated, service_role;
grant all on public.conformance_notes to anon, authenticated, service_role;

-- PostgREST keeps its own picture of the schema and this file has just
-- replaced the one it was holding. The notification is how upstream
-- says "look again"; every other target ignores it.
notify pgrst, 'reload schema';
