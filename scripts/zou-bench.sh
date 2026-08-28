#!/bin/sh
# Run pgbench against a zou target and print one line per phase, ready
# to paste into the perf table in docs/perf.md.
#
# Usage: scripts/zou-bench.sh <target> [scale] [seconds]
#
# The target is a directory or an object store URL like s3://bucket/prefix,
# or the word none for the vanilla leg: the same postgres binary with
# nothing to point the store shim at, so it writes its own files and the
# store is out of it. That leg lives here rather than in a script of its
# own because M1b asks for cpu seconds per thousand transactions within
# 20 percent of vanilla, and a comparison whose two halves were produced
# by two different scripts is partly a comparison of the scripts.
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
#
# Each phase also prints what it cost the machine, memory and cpu,
# sampled once a second by scripts/zou-usage.sh while the run happens.
# Two M1b claims are about that and neither can be answered after the
# fact: the shim's own footprint, and cpu seconds per thousand
# transactions against vanilla.
set -eu

TARGET=$1
SCALE=${2:-100}
DURATION=${3:-60}
PG=${PG:-build/pg/bin}
BOOTSTRAP=${BOOTSTRAP:-target/release/zou-bootstrap}
PORT=${PORT:-54312}
CLIENTS=${CLIENTS:-8}

VANILLA=0
if [ "$TARGET" = none ]; then
    VANILLA=1
fi

RUNDIR=$(mktemp -d /tmp/zou-bench.XXXXXX)
DATADIR=$RUNDIR/data
SOCK=$RUNDIR/sock
LOG=$RUNDIR/server.log
mkdir "$SOCK"

SAMPLER=
stop() {
    "$PG"/pg_ctl -D "$DATADIR" stop -m fast >/dev/null 2>&1 || true
    [ -n "$SAMPLER" ] && kill "$SAMPLER" 2>/dev/null
    SAMPLER=
    return 0
}
trap stop EXIT

STATS=$RUNDIR/store-stats
if [ "$VANILLA" = 1 ]; then
    # Nothing to point the shim at, and nothing left in the environment
    # to point it at either, since a leg that is supposed to be vanilla
    # and inherited a ZOU_TARGET from the shell that launched it is the
    # one measurement nobody would catch by reading the output.
    unset ZOU_TARGET ZOU_PAGESERVE ZOU_STORE_STATS 2>/dev/null || true
    # Kept as a plain variable rather than exported: the counter checks
    # below read it and no file will ever appear at it, which is the
    # right answer for a run with no store under it.
    ZOU_STORE_STATS=$STATS
else
    export ZOU_TARGET=$TARGET
    # The object path unless the caller asked for the other one. These
    # start postgres themselves, and the page service is a background
    # worker under it, so both paths run from here.
    ZOU_PAGESERVE=${ZOU_PAGESERVE:-0}
    export ZOU_PAGESERVE
    ZOU_STORE_STATS=${ZOU_STORE_STATS:-$STATS}
    export ZOU_STORE_STATS
fi
# initdb and the bootstrap are tools, not a server. The page service is
# a background worker inside postgres, so nothing is listening on its
# socket while these run and asking them to read that way is asking for
# a connect failure on the first catalog page. Every other tool in the
# tree pins the object path for the same reason.
ZOU_PAGESERVE=0 "$PG"/initdb -D "$DATADIR" --set io_method=sync --set full_page_writes=off >/dev/null
if [ "$VANILLA" = 0 ]; then
    REDO=$("$PG"/pg_controldata -D "$DATADIR" | grep "REDO location" | awk '{print $NF}')
    ZOU_PAGESERVE=0 "$BOOTSTRAP" "$TARGET" "$DATADIR" --redo "$REDO" >/dev/null
fi
"$PG"/pg_ctl -D "$DATADIR" -l "$LOG" -o "-p $PORT -k $SOCK" start >/dev/null

ZOU=${ZOU_BIN:-target/release}/zou
MARK=$RUNDIR/mark

# What the run costs in memory and cpu, sampled once a second for as
# long as the postmaster is up. Two M1b claims need it, the shim's own
# footprint and cpu seconds per thousand transactions, and neither can
# be answered after the fact. The samples are timestamped and each
# phase cuts its own window out of them, since a peak paid for during
# the load is not a peak the select-only phase paid for.
USAGE=$RUNDIR/usage
USAGE_SH=$(dirname "$0")/zou-usage.sh
POSTMASTER=$(head -1 "$DATADIR/postmaster.pid" 2>/dev/null || true)
if [ -r "$USAGE_SH" ] && [ -n "$POSTMASTER" ]; then
    sh "$USAGE_SH" "$POSTMASTER" "$USAGE" 1 >/dev/null 2>&1 &
    SAMPLER=$!
fi

if [ "$VANILLA" = 1 ]; then
    echo "target: none, vanilla postgres writing its own files"
else
    echo "target: $TARGET"
fi
echo "rundir: $RUNDIR"
if [ "$VANILLA" = 0 ]; then
    echo "page service: $ZOU_PAGESERVE"
fi

# Which box this was, at the top of the run rather than in somebody's
# memory. A tps is a number about a pair, a machine and a store, and
# the pair is the half a result table leaves out: eight shared cores
# against a MinIO on the same disk as the WAL and thirty two quiet ones
# against a bucket are the same column and not the same measurement.
#
# The distance half needs a probe and a probe writes, so it is asked
# for rather than assumed: ZOU_BENCH_PROBE=1 puts a latency and
# bandwidth line beside the specs, which is what a result being
# published wants and not what a run being iterated on wants.
HARDWARE=$(dirname "$0")/zou-hardware.sh
if [ -r "$HARDWARE" ]; then
    case $TARGET in
    *://* | none) WHERE=$RUNDIR ;;
    *) WHERE=$TARGET ;;
    esac
    if [ "${ZOU_BENCH_PROBE:-0}" = 1 ] && [ "$VANILLA" = 0 ]; then
        sh "$HARDWARE" "$WHERE" "$TARGET" | sed 's/^/  /'
    else
        sh "$HARDWARE" "$WHERE" | sed 's/^/  /'
    fi
fi

# A phase costs the counters at its end minus the counters at its start,
# so every phase leaves a copy of the file behind for the next one. The
# copies are kept under the run directory as well, named by the boundary
# they were taken at, so a finished run can be asked anything the three
# lines below leave out without being run again.
BOUNDARY=0
MARK_AT=$(date +%s)
mark() {
    if [ -s "$ZOU_STORE_STATS" ]; then
        cp "$ZOU_STORE_STATS" "$MARK"
        BOUNDARY=$((BOUNDARY + 1))
        cp "$ZOU_STORE_STATS" "$RUNDIR/stats-$BOUNDARY-$1"
    fi
    MARK_AT=$(date +%s)
}

# What the phase just printed cost the store. Best effort: a run against
# a build without the counters, or one whose zou is somewhere else, is
# still a run and should not die here over its footnotes.
cost() {
    if [ -s "$ZOU_STORE_STATS" ] && [ -s "$MARK" ] && [ -x "$ZOU" ]; then
        "$ZOU" stats "$ZOU_STORE_STATS" --since "$MARK" --brief 2>/dev/null |
            awk '{print "  " $0}' || true
    fi
    if [ -s "$USAGE" ]; then
        sh "$USAGE_SH" --report "$USAGE" "$MARK_AT" "$(date +%s)" "${2:-0}" || true
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
    # pgbench's own count rather than tps times duration, since the
    # two differ by however long the last transaction took and the cpu
    # figure divides by this.
    txns=$(printf '%s\n' "$out" |
        awk '/number of transactions actually processed/ {
            split($NF, done, "/"); print done[1]
        }')
    echo "$name, $CLIENTS clients, $DURATION s: $tps tps, $lat ms average latency"
    cost "$name" "${txns:-0}"
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
if [ -s "$USAGE" ]; then
    echo "usage over the whole run:"
    sh "$USAGE_SH" --report "$USAGE" || true
fi
if [ -s "$ZOU_STORE_STATS" ]; then
    echo "counters:"
    "$ZOU" stats "$ZOU_STORE_STATS"
fi
echo "done, server log at $LOG"
