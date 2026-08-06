#!/usr/bin/env bash
# End to end branch smoke on a real pgbench database: run load, fold,
# branch at the head checkpoint, restore parent and child, verify the
# child matches the parent exactly, then write to the child and verify
# the parent never moves.
# Usage: zou-branch-smoke.sh <pg-bin-dir> <zou-bin-dir>
set -euo pipefail

PG_BIN=$1
ZOU_BIN=$2
PORT=${PORT:-5641}
WORK=$(mktemp -d /tmp/zou-branchsmoke.XXXXXX)
SOCK="$WORK"
export ZOU_TARGET="$WORK/store"
# The chain reader serves inherited pages from a run bearing full, so
# the parent must fold one before branching; factor 0 makes the second
# fold a full instead of the fifth.
export ZOU_FOLD_DOWN_FACTOR=0
mkdir -p "$ZOU_TARGET"

q() { "$PG_BIN/psql" -h "$SOCK" -p "$PORT" -d postgres -Atqc "$1"; }
sums() { q "select count(*) || ':' || coalesce(sum(abalance),0) || ':' || coalesce(sum(hashtext(aid::text || abalance::text)),0) from pgbench_accounts"; }

start() { ZOU_TENANT=${2:-local} "$PG_BIN/pg_ctl" -D "$1" -l "$1.log" -w -t 120 -o "-p $PORT -k $SOCK -c listen_addresses=''" start >/dev/null; }
stop() { "$PG_BIN/pg_ctl" -D "$1" stop -m fast >/dev/null; }

PGDATA="$WORK/pgdata"
"$PG_BIN/initdb" -D "$PGDATA" --set io_method=sync --set full_page_writes=off >"$WORK/initdb.log" 2>&1
REDO=$("$PG_BIN/pg_controldata" -D "$PGDATA" | grep "REDO location" | awk '{print $NF}')
"$ZOU_BIN/zou-bootstrap" "$ZOU_TARGET" "$PGDATA" --redo "$REDO"

start "$PGDATA"
"$PG_BIN/pgbench" -h "$SOCK" -p "$PORT" -i -s 1 postgres >"$WORK/pginit.log" 2>&1
"$PG_BIN/pgbench" -h "$SOCK" -p "$PORT" -c 4 -j 2 -t 500 postgres >"$WORK/pgrun.log" 2>&1
q "checkpoint" >/dev/null
for _ in $(seq 1 30); do
    grep -q "folded a" "$PGDATA.log" && break
    sleep 1
done
"$PG_BIN/pgbench" -h "$SOCK" -p "$PORT" -c 4 -j 2 -t 200 postgres >"$WORK/pgrun2.log" 2>&1
q "checkpoint" >/dev/null
for _ in $(seq 1 60); do
    grep -q "folded a full" "$PGDATA.log" && break
    sleep 1
done
grep -q "folded a full" "$PGDATA.log" || { echo "no full fold on the parent"; exit 1; }
grep -c "folded a" "$PGDATA.log" | sed 's/^/folds on the parent: /'
PARENT_SUM=$(sums)
echo "parent state: $PARENT_SUM"
stop "$PGDATA"

"$ZOU_BIN/zou-branch" "$ZOU_TARGET" local copy

COPYDATA="$WORK/pgdata-copy"
"$ZOU_BIN/zou-restore" "$ZOU_TARGET" "$COPYDATA" copy >/dev/null
start "$COPYDATA" copy
COPY_SUM=$(sums)
echo "child state:  $COPY_SUM"
[ "$COPY_SUM" = "$PARENT_SUM" ] || { echo "MISMATCH: child differs from parent at the branch point"; exit 1; }
q "insert into pgbench_accounts values (999999, 1, 42, 'child only')" >/dev/null
q "checkpoint" >/dev/null
stop "$COPYDATA"

MAINDATA="$WORK/pgdata-main"
"$ZOU_BIN/zou-restore" "$ZOU_TARGET" "$MAINDATA" local >/dev/null
start "$MAINDATA"
AFTER_SUM=$(sums)
MARKER=$(q "select count(*) from pgbench_accounts where aid = 999999")
stop "$MAINDATA"
echo "parent after child wrote: $AFTER_SUM, child marker rows visible: $MARKER"
[ "$AFTER_SUM" = "$PARENT_SUM" ] || { echo "MISMATCH: the child's writes leaked into the parent"; exit 1; }
[ "$MARKER" = "0" ] || { echo "MISMATCH: the child's row is visible on the parent"; exit 1; }
echo "OK: branch equals parent at the branch point and diverges cleanly"
rm -rf "$WORK"
