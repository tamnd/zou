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
#
# Each phase prints what it cost the store underneath its own tps, out
# of the counters between its start and its end rather than out of the
# run total. The two read paths buy different things, so a tps on its
# own says which bargain suited the scenario and not what either bargain
# was: the object path pays a put per page and a get per read and buys a
# fast read with it, the layer path pays neither and buys a slow one.
# Both halves belong beside the number.
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
# initdb and the bootstrap are tools, not a server. The page service is
# a background worker inside postgres, so nothing is listening on its
# socket while these run and asking them to read that way is asking for
# a connect failure on the first catalog page. Every other tool in the
# tree pins the object path for the same reason.
ZOU_PAGESERVE=0 "$PG"/initdb -D "$DATADIR" --set io_method=sync --set full_page_writes=off >/dev/null
REDO=$("$PG"/pg_controldata -D "$DATADIR" | grep "REDO location" | awk '{print $NF}')
ZOU_PAGESERVE=0 "$BOOTSTRAP" "$TARGET" "$DATADIR" --redo "$REDO" >/dev/null
"$PG"/pg_ctl -D "$DATADIR" -l "$LOG" -o "-p $PORT -k $SOCK" start >/dev/null

ZOU=${ZOU_BIN:-target/release}/zou
MARK=$RUNDIR/mark

echo "target: $TARGET"
echo "rundir: $RUNDIR"
echo "page service: $ZOU_PAGESERVE"

# A phase costs the counters at its end minus the counters at its start,
# so every phase leaves a copy of the file behind for the next one. The
# copies are kept under the run directory as well, named by the boundary
# they were taken at, so a finished run can be asked anything the three
# lines below leave out without being run again.
BOUNDARY=0
mark() {
    if [ -s "$ZOU_STORE_STATS" ]; then
        cp "$ZOU_STORE_STATS" "$MARK"
        BOUNDARY=$((BOUNDARY + 1))
        cp "$ZOU_STORE_STATS" "$RUNDIR/stats-$BOUNDARY-$1"
    fi
}

# What the phase just printed cost the store. Best effort: a run against
# a build without the counters, or one whose zou is somewhere else, is
# still a run and should not die here over its footnotes.
cost() {
    if [ -s "$ZOU_STORE_STATS" ] && [ -s "$MARK" ] && [ -x "$ZOU" ]; then
        "$ZOU" stats "$ZOU_STORE_STATS" --since "$MARK" --brief 2>/dev/null |
            awk '{print "  " $0}' || true
    fi
    mark "$1"
}

mark start
T0=$(date +%s)
"$PG"/pgbench -h "$SOCK" -p "$PORT" -i -s "$SCALE" -q postgres 2>"$RUNDIR/init.log" ||
    { cat "$RUNDIR/init.log"; exit 1; }
T1=$(date +%s)
echo "pgbench -i -s $SCALE: $((T1 - T0)) s"
cost init

T0=$(date +%s)
"$PG"/psql -h "$SOCK" -p "$PORT" -d postgres -c checkpoint >/dev/null
T1=$(date +%s)
echo "checkpoint: $((T1 - T0)) s"
cost checkpoint

bench() {
    name=$1
    shift
    out=$("$PG"/pgbench -h "$SOCK" -p "$PORT" -c "$CLIENTS" -j "$CLIENTS" \
        -T "$DURATION" -P 10 "$@" postgres 2>&1)
    tps=$(printf '%s\n' "$out" | awk '/^tps/ {printf "%.0f", $3}')
    lat=$(printf '%s\n' "$out" | awk '/latency average/ {print $4}')
    echo "$name, $CLIENTS clients, $DURATION s: $tps tps, $lat ms average latency"
    cost "$name"
}

bench "tpcb-like"
bench "select-only" -S

stop
trap - EXIT
# The shutdown checkpoint is a phase like any other and it is the one a
# run is most likely to forget it paid for, so it gets its own line
# before the totals.
echo "shutdown:"
cost shutdown
# After the stop, so the counters carry the whole run including whatever
# the shutdown checkpoint cost.
if [ -s "$ZOU_STORE_STATS" ]; then
    echo "counters:"
    "$ZOU" stats "$ZOU_STORE_STATS"
fi
echo "done, server log at $LOG"
