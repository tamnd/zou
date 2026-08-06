-- The structural fingerprint of a schema, one sorted line per fact, so
-- two databases can be compared as text.
--
-- Used two ways: to record what replaying somebody else's migrations
-- produces, and to check what zou's own bootstrap produces. The tests
-- that compare the two are what "field compatible with GoTrue" and
-- "field compatible with storage-api" mean in practice.
--
-- The schema is read from a setting rather than written in, because
-- two copies of this file that had drifted apart would compare two
-- different things and still both pass. Set it first, in the same
-- session:
--
--   psql "$DSN" -qAt -c "set zou.fingerprint_schema = 'auth'" \
--       -f scripts/schema-fingerprint.sql
--
-- Everything here is ordered by name rather than by oid or by attnum,
-- so the output does not depend on the order the objects were created
-- in.

with target as (
    select current_setting('zou.fingerprint_schema') as nspname
),
cols as (
    select format(
        'column %s.%s %s %s%s%s',
        c.relname,
        a.attname,
        format_type(a.atttypid, a.atttypmod),
        case when a.attnotnull then 'not null' else 'null' end,
        coalesce(' default ' || pg_get_expr(d.adbin, d.adrelid), ''),
        case a.attgenerated when 's' then ' stored' else '' end
    ) as line
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    join pg_attribute a on a.attrelid = c.oid
    left join pg_attrdef d on d.adrelid = c.oid and d.adnum = a.attnum
    where n.nspname = (select nspname from target)
      and c.relkind in ('r', 'p', 'v', 'm')
      and a.attnum > 0
      and not a.attisdropped
),
cons as (
    select format(
        'constraint %s.%s %s',
        c.relname,
        con.conname,
        pg_get_constraintdef(con.oid)
    ) as line
    from pg_constraint con
    join pg_class c on c.oid = con.conrelid
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = (select nspname from target)
      -- Not null constraints are named by postgres 18 and the name it
      -- picks depends on which column the constraint was first written
      -- against, so a column added and later renamed keeps a name that
      -- says something else. Two schemas that agree on every column
      -- being not null would disagree here on nothing that matters.
      -- The nullability itself is in the column lines above.
      and con.contype <> 'n'
),
idx as (
    select format('index %s', indexdef) as line
    from pg_indexes
    where schemaname = (select nspname from target)
),
rls as (
    select format('rls %s %s', c.relname,
                  case when c.relrowsecurity then 'on' else 'off' end) as line
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = (select nspname from target) and c.relkind in ('r', 'p')
),
enums as (
    select format('enum %s %s', t.typname,
                  string_agg(e.enumlabel, ',' order by e.enumsortorder)) as line
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    join pg_enum e on e.enumtypid = t.oid
    where n.nspname = (select nspname from target)
    group by t.typname
),
funcs as (
    select format('function %s(%s) returns %s %s',
                  p.proname,
                  pg_get_function_arguments(p.oid),
                  pg_get_function_result(p.oid),
                  p.provolatile) as line
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = (select nspname from target)
),
-- Triggers are a fact about behaviour rather than about shape, which
-- is exactly why they belong here. The storage schema refuses a delete
-- that did not come through the api, and it refuses it from a trigger,
-- so a fingerprint that did not look at triggers would call a schema
-- missing that guard identical to one that has it.
trigs as (
    select format('trigger %s', pg_get_triggerdef(t.oid)) as line
    from pg_trigger t
    join pg_class c on c.oid = t.tgrelid
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = (select nspname from target)
      and not t.tgisinternal
),
comments as (
    select format('comment %s %s', c.relname, obj_description(c.oid, 'pg_class')) as line
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = (select nspname from target)
      and c.relkind in ('r', 'p', 'v', 'm')
      and obj_description(c.oid, 'pg_class') is not null
)
select line from (
    select line from cols
    union all select line from cons
    union all select line from idx
    union all select line from rls
    union all select line from enums
    union all select line from funcs
    union all select line from trigs
    union all select line from comments
) all_lines
order by line;
