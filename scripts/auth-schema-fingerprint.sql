-- The structural fingerprint of the auth schema, one sorted line per
-- fact, so two databases can be compared as text.
--
-- Used two ways: to record what replaying GoTrue's migrations
-- produces, and to check what zou's own bootstrap produces. The test
-- that compares the two is what "field compatible with GoTrue" means
-- in practice.
--
-- Everything here is ordered by name rather than by oid or by
-- attnum, so the output does not depend on the order the objects
-- were created in.

with cols as (
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
    where n.nspname = 'auth'
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
    where n.nspname = 'auth'
),
idx as (
    select format('index %s', indexdef) as line
    from pg_indexes
    where schemaname = 'auth'
),
rls as (
    select format('rls %s %s', c.relname,
                  case when c.relrowsecurity then 'on' else 'off' end) as line
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'auth' and c.relkind in ('r', 'p')
),
enums as (
    select format('enum %s %s', t.typname,
                  string_agg(e.enumlabel, ',' order by e.enumsortorder)) as line
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    join pg_enum e on e.enumtypid = t.oid
    where n.nspname = 'auth'
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
    where n.nspname = 'auth'
),
comments as (
    select format('comment %s %s', c.relname, obj_description(c.oid, 'pg_class')) as line
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'auth'
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
    union all select line from comments
) all_lines
order by line;
