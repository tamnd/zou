#!/bin/sh
# Run pgbench against a zou target and print one line per phase, ready
# to paste into the perf table in docs/perf.md.
#
# Usage: scripts/zou-bench.sh <target> [scale] [seconds]
#
# The target is a directory or an object store URL like s3://bucket/prefix.
# Credentials and the endpoint come from the usual AWS_ACCESS_KEY_ID,
# AWS_SECRET_ACCESS_KEY, ZOU_S3_ENDPOINT, and ZOU_S3_REGION variables.
# PG points at the postgres install, default build/pg/bin.
#
# ZOU_PAGESERVE picks the read path and is left alone when it is already
# set, which is how the two paths get compared on one box with one
# binary. Every run keeps store counters, so a column that came out slow
# can be asked where its reads went and what its commits waited on
# rather than argued about.
set -eu

TARGET=$1
SCALE=${2:-100}
DURATION=${3:-60}
PG=${PG:-build/pg/bin}
BOOTSTRAP=${BOOTSTRAP:-target/release/zou-bootstrap}
PORT=${PORT:-54312}
CLIENTS=${CLIENTS:-8}

RUNDIR=$(mktemp -d /tmp/zou-bench.XXXXXX)
DATADIR=$RUNDIR/data
SOCK=$RUNDIR/sock
LOG=$RUNDIR/server.log
mkdir "$SOCK"

stop() { "$PG"/pg_ctl -D "$DATADIR" stop -m fast >/dev/null 2>&1 || true; }
trap stop EXIT

export ZOU_TARGET=$TARGET
# The object path unless the caller asked for the other one. These
# start postgres themselves, and the page service is a background
# worker under it, so both paths run from here.
ZOU_PAGESERVE=${ZOU_PAGESERVE:-0}
export ZOU_PAGESERVE
STATS=$RUNDIR/store-stats
ZOU_STORE_STATS=${ZOU_STORE_STATS:-$STATS}
export ZOU_STORE_STATS
"$PG"/initdb -D "$DATADIR" --set io_method=sync --set full_page_writes=off >/dev/null
REDO=$("$PG"/pg_controldata -D "$DATADIR" | grep "REDO location" | awk '{print $NF}')
"$BOOTSTRAP" "$TARGET" "$DATADIR" --redo "$REDO" >/dev/null
"$PG"/pg_ctl -D "$DATADIR" -l "$LOG" -o "-p $PORT -k $SOCK" start >/dev/null

echo "target: $TARGET"
echo "rundir: $RUNDIR"
echo "page service: $ZOU_PAGESERVE"

T0=$(date +%s)
"$PG"/pgbench -h "$SOCK" -p "$PORT" -i -s "$SCALE" -q postgres 2>"$RUNDIR/init.log" ||
    { cat "$RUNDIR/init.log"; exit 1; }
T1=$(date +%s)
echo "pgbench -i -s $SCALE: $((T1 - T0)) s"

T0=$(date +%s)
"$PG"/psql -h "$SOCK" -p "$PORT" -d postgres -c checkpoint >/dev/null
T1=$(date +%s)
echo "checkpoint: $((T1 - T0)) s"

bench() {
    name=$1
    shift
    out=$("$PG"/pgbench -h "$SOCK" -p "$PORT" -c "$CLIENTS" -j "$CLIENTS" \
        -T "$DURATION" -P 10 "$@" postgres 2>&1)
    tps=$(printf '%s\n' "$out" | awk '/^tps/ {printf "%.0f", $3}')
    lat=$(printf '%s\n' "$out" | awk '/latency average/ {print $4}')
    echo "$name, $CLIENTS clients, $DURATION s: $tps tps, $lat ms average latency"
}

bench "tpcb-like"
bench "select-only" -S

stop
trap - EXIT
# After the stop, so the counters carry the whole run including whatever
# the shutdown checkpoint cost.
if [ -s "$ZOU_STORE_STATS" ]; then
    echo "counters:"
    ${ZOU_BIN:-target/release}/zou stats "$ZOU_STORE_STATS"
fi
echo "done, server log at $LOG"
