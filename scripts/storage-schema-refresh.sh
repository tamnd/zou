#!/usr/bin/env bash
#
# Regenerate zou's storage schema from storage-api's own migrations.
#
# Same argument as scripts/auth-schema-refresh.sh, and deliberately the
# same shape. supabase/storage ships its schema as sixty one migrations
# that have accumulated since 2021, and several of them drop or replace
# what an earlier one made, so the finished shape is not something to
# read off the files by hand. This replays them against a scratch
# database and takes what falls out.
#
# Two files come out of it:
#
#   crates/zou-server/src/storage-schema.sql
#     the consolidated ddl zou runs when it finds no storage schema
#
#   crates/zou-server/tests/fixtures/storage-api-fingerprint.txt
#     every column, constraint, index, trigger, enum and comment as one
#     sorted line each, which the storage schema test compares zou's
#     own bootstrap against
#
# Needs a running postgres with an empty database to replay into, and
# network access to fetch the pinned tag.
#
#   scripts/storage-schema-refresh.sh "host=127.0.0.1 port=5432 dbname=scratch"
#
# The tag is pinned rather than tracking main, and it is the same tag
# the local supabase stack runs, which is what makes the recording in
# the conformance repository and this schema two views of one thing.

set -euo pipefail

STORAGE_TAG="${STORAGE_TAG:-v1.67.20}"
DSN="${1:-}"
if [ -z "$DSN" ]; then
    echo "usage: $0 <dsn to an empty database>" >&2
    exit 2
fi

root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

echo "fetching supabase/storage $STORAGE_TAG migrations"
gh api "repos/supabase/storage/contents/migrations/tenant?ref=$STORAGE_TAG" \
    --jq '.[] | select(.name|endswith(".sql")) | .name + " " + .download_url' \
    > "$work/list"

mkdir -p "$work/sql"
while read -r name url; do
    curl -sSL "$url" -o "$work/sql/$name"
done < "$work/list"
echo "fetched $(ls "$work/sql" | wc -l | tr -d ' ') migrations"

# Ordered by the number in the file name read as a number, not by the
# file name read as text. There is a 00010 among the 0001 to 0060, and
# sorted as text it lands between 0001 and 0002 rather than after 0009,
# which is four migrations earlier than the runner would have applied
# it. The runner parses the leading digits, so this does too.
order() {
    for f in "$work"/sql/*.sql; do
        printf '%d %s\n' "$(basename "$f" | sed -e 's/[^0-9].*$//' -e 's/^0*//' -e 's/^$/0/')" "$f"
    done | sort -n -k1,1 | cut -d' ' -f2-
}

echo "replaying into $DSN"
# The migrations grant to these and hand ownership to one of them. On a
# real deployment the three api roles come from zou's own bootstrap and
# the other three do not exist at all, which is fine: the dump drops
# every owner and every grant, so nothing about these roles survives
# into the generated file. They only have to exist for the replay.
for role in anon authenticated service_role postgres supabase_storage_admin; do
    psql "$DSN" -q -c "do \$\$ begin
        if not exists (select 1 from pg_roles where rolname = '$role') then
            create role $role;
        end if;
    end \$\$" >/dev/null
done
# search_path matters and is not a detail. storage-api connects as
# supabase_storage_admin, and migration 0002 sets that user's search
# path to storage, so the migrations after it are written against a
# session that resolves a bare table name to a storage one. Migration
# 0047 alters iceberg_namespaces without saying which schema. Replaying
# with the default path gets "relation does not exist" there, which
# looks like a broken migration and is really a missing session.
for f in $(order); do
    psql "$DSN" -q -v ON_ERROR_STOP=1 \
        -c "set search_path = storage" -f "$f" >/dev/null
done

echo "recording the migration rows"
# postgres-migrations, which storage-api runs, takes the id from the
# digits at the front of the file name, the name from what is left
# after the separator, and the hash from the sha1 of the file name
# followed by the contents. The rows recorded in the conformance
# repository's storage recording are what says that is right.
: > "$work/migrations.sql"
# Row zero is not in the tenant directory. postgres-migrations carries
# its own first migration, the one that makes the table the other rows
# go in, and it numbers it 0. So it cannot be derived from anything
# fetched here and is written down instead, from the rows a recording
# run read out of the reference. Every row below it was derived and all
# sixty matched that same recording, which is what says the derivation
# is right and this line is only what it could not reach.
echo "insert into storage.migrations (id, name, hash) values (0, 'create-migrations-table', 'e18db593bcde2aca2a408c4d1100f6abba2195df') on conflict do nothing;" \
    >> "$work/migrations.sql"
for f in $(order); do
    base="$(basename "$f")"
    id="$(printf '%s' "$base" | sed -e 's/[^0-9].*$//' -e 's/^0*//' -e 's/^$/0/')"
    name="$(printf '%s' "$base" | sed -e 's/^[0-9]*[-_]//' -e 's/\.sql$//')"
    hash="$( (printf '%s' "$base"; cat "$f") | shasum -a 1 | cut -d' ' -f1)"
    echo "insert into storage.migrations (id, name, hash) values ($id, '$name', '$hash') on conflict do nothing;" \
        >> "$work/migrations.sql"
done

echo "dumping"
pg_dump "$DSN" -n storage --schema-only --no-owner --no-privileges -f "$work/dump.sql"

out="$root/crates/zou-server/src/storage-schema.sql"
{
    cat <<EOF
-- The canonical storage schema, field compatible with storage-api.
--
-- Generated by scripts/storage-schema-refresh.sh from supabase/storage
-- $STORAGE_TAG by replaying its migrations and dumping the result. Do
-- not edit by hand: rerun the script against a new tag and read the
-- diff.
--
-- Two things here are not verbatim storage-api.
--
-- Its grants and its owners are not here. Upstream the tables belong
-- to supabase_storage_admin and everyone else is granted in, while
-- under zou the connecting role creates them and therefore already
-- owns them, and the three api roles are granted in by zou's own
-- bootstrap next to the same grants it makes for auth and public.
--
-- The migration rows at the end are inserted so that a real
-- storage-api pointed at this database considers itself up to date and
-- does not replay sixty one migrations over a schema that already has
-- them.
--
-- Applied once, on a database that has no storage.objects yet. zou
-- never rewrites a storage schema it did not create; migrating an
-- older one is storage-api's own job and is deliberately out of scope.

-- pg_dump writes the functions in name order, and one of the first of
-- them calls one of the last. Nothing is wrong with that once they all
-- exist, and postgres only objects because it validates a sql function
-- body as it creates it. pg_dump turns that validation off for the
-- length of its own output for exactly this reason. Local, so it lasts
-- the transaction that applies this and not a moment longer.
SET LOCAL check_function_bodies = false;

EOF
    # pg_dump writes a preamble of SET statements, a "-- Name: x; Type:
    # y" banner before every object, and a pair of \restrict directives
    # that only psql understands. None of it survives.
    sed -e '/^SET /d' \
        -e '/^SELECT pg_catalog\./d' \
        -e '/^\\restrict /d' \
        -e '/^\\unrestrict /d' \
        -e '/^-- Dumped/d' \
        -e '/^-- PostgreSQL database dump/d' \
        -e '/^-- Name: /d' \
        -e '/^--$/d' \
        "$work/dump.sql" \
        | sed -e 's/^CREATE SCHEMA storage;/CREATE SCHEMA IF NOT EXISTS storage;/' \
              -e 's/^CREATE TABLE /CREATE TABLE IF NOT EXISTS /' \
              -e 's/^CREATE INDEX /CREATE INDEX IF NOT EXISTS /' \
              -e 's/^CREATE UNIQUE INDEX /CREATE UNIQUE INDEX IF NOT EXISTS /' \
              -e 's/^CREATE FUNCTION /CREATE OR REPLACE FUNCTION /' \
        | cat -s
    cat "$work/migrations.sql"
} > "$out"
echo "wrote $out ($(wc -l < "$out" | tr -d ' ') lines)"

fp="$root/crates/zou-server/tests/fixtures/storage-api-fingerprint.txt"
mkdir -p "$(dirname "$fp")"
psql "$DSN" -qAt \
    -c "set zou.fingerprint_schema = 'storage'" \
    -f "$root/scripts/schema-fingerprint.sql" > "$fp"
echo "wrote $fp ($(wc -l < "$fp" | tr -d ' ') lines)"
