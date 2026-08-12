#!/usr/bin/env bash
# Time a cold attach over a simulated distant object store, and say what
# it spent the time on.
#
# A cold attach is two phases. The restore pulls the skeleton, which is
# a fixed and small number of objects. Crash recovery then replays the
# WAL tail, and every page a record touches is a store round trip taken
# one at a time, so on a store thirty milliseconds away the second phase
# is the whole of the attach. This measures both, and dumps the store op
# counters, because the number that explains the wall clock is gets and
# not bytes.
#
# The scenario is built once into WORK and reused: a pgbench database,
# a load, then kill -9, which leaves the store with a WAL tail past its
# last checkpoint. It is kept as a pristine copy and every run attaches
# from a fresh copy of it, because an attach is a write: recovery puts
# the pages it rebuilt back, and a second attach of the same store then
# finds the page LSN already past the record and replays nothing. Runs
# have to start from the same store or they are not the same run.
#
# Usage: scripts/zou-cold-attach.sh [label]
# Env overrides: PG_BIN, ZOU_BIN, WORK, SIM, SCALE, LOAD_SECS, and the
# ZOU_WARM_* knobs, which pass through, so ZOU_WARM_BLOCKS=0 measures
# the same store with the warm up off.

set -euo pipefail

LABEL=${1:-run}
PG_BIN=${PG_BIN:-build/pg/bin}
ZOU_BIN=${ZOU_BIN:-target/release}
WORK=${WORK:-/tmp/zou-cold-attach}
SIM=${SIM:-s3-standard}
SCALE=${SCALE:-25}
LOAD_SECS=${LOAD_SECS:-20}
# The pool the attaching server gets. Small on purpose: a recovery that
# holds the whole database in shared buffers faults every page once and
# then never reads again, which is not what a real attach looks like.
SHARED_BUFFERS=${SHARED_BUFFERS:-32MB}
PORT=${PORT:-5613}

STORE="$WORK/store"
PRISTINE="$WORK/store-pristine"
say() { echo "[cold-attach] $*"; }
now() { perl -MTime::HiRes=time -e 'printf "%.3f\n", time'; }
took() { perl -e 'printf "%.2f\n", $ARGV[1] - $ARGV[0]' "$1" "$2"; }

mkdir -p "$WORK"

# Phase one, once: a database with a WAL tail nobody checkpointed away.
if [ ! -d "$PRISTINE" ]; then
	say "building the scenario in $WORK, scale $SCALE, ${LOAD_SECS}s of load"
	mkdir -p "$STORE" "$WORK/cache0"
	PGDATA="$WORK/pgdata0"
	# initdb runs through the shim too, which is what puts the pages and
	# the fork sizes of a fresh cluster in the store. Without it the
	# store holds a file capture nothing can read a block out of.
	export ZOU_TARGET="$STORE" ZOU_TENANT=local ZOU_PAGE_CACHE="$WORK/cache0"
	export ZOU_PAGESERVE=0
	"$PG_BIN/initdb" -D "$PGDATA" --set io_method=sync --set full_page_writes=off \
		>"$WORK/initdb.log" 2>&1
	REDO=$("$PG_BIN/pg_controldata" -D "$PGDATA" | grep "REDO location" | awk '{print $NF}')
	"$ZOU_BIN/zou-bootstrap" "$STORE" "$PGDATA" --redo "$REDO"
	"$PG_BIN/pg_ctl" -D "$PGDATA" -l "$WORK/build.log" -w -t 300 \
		-o "-p $PORT -k $WORK -c listen_addresses=''" start
	"$PG_BIN/pgbench" -h "$WORK" -p "$PORT" -i -s "$SCALE" postgres \
		>"$WORK/pgbench-init.log" 2>&1
	# Checkpoint first, so what recovery replays is the load and not the
	# load plus the whole build. A short scattered update load over a
	# database many times the pool is the shape that matters: a small
	# WAL tail touching thousands of distinct pages, every one of them
	# a page recovery has to read before it can change it.
	"$PG_BIN/psql" -h "$WORK" -p "$PORT" -d postgres -Atqc "checkpoint" >/dev/null
	"$PG_BIN/pgbench" -h "$WORK" -p "$PORT" -c 4 -T "$LOAD_SECS" postgres \
		>"$WORK/pgbench-load.log" 2>&1 || true
	sleep 3
	say "kill -9 the postmaster"
	kill -9 "$(head -1 "$PGDATA/postmaster.pid")"
	sleep 2
	cp -R "$STORE" "$PRISTINE"
	say "scenario kept at $PRISTINE, $(du -sh "$PRISTINE" | awk '{print $1}')"
fi

rm -rf "$STORE"
cp -R "$PRISTINE" "$STORE"

PGDATA="$WORK/pgdata-$LABEL"
CACHE="$WORK/cache-$LABEL"
rm -rf "$PGDATA" "$CACHE" "$WORK/stats-$LABEL"
mkdir -p "$CACHE"

export ZOU_TARGET="$STORE"
export ZOU_TENANT=local
# This starts postgres itself, with no page service to read through,
# so the object path is the one being timed here.
export ZOU_PAGESERVE=0
export ZOU_PAGE_CACHE="$CACHE"
export ZOU_STORE_SIM="$SIM"
export ZOU_STORE_STATS="$WORK/stats-$LABEL"

say "attaching as $LABEL under $SIM, shared_buffers $SHARED_BUFFERS, warm blocks ${ZOU_WARM_BLOCKS:-default}"
t0=$(now)
"$ZOU_BIN/zou-restore" "$STORE" "$PGDATA" local
t1=$(now)
"$PG_BIN/pg_ctl" -D "$PGDATA" -l "$WORK/attach-$LABEL.log" -w -t 3600 \
	-o "-p $PORT -k $WORK -c listen_addresses='' -c shared_buffers=$SHARED_BUFFERS" start
t2=$(now)
"$PG_BIN/pg_ctl" -D "$PGDATA" -w -t 120 stop >/dev/null

say "restore $(took "$t0" "$t1")s, recovery to ready $(took "$t1" "$t2")s, attach $(took "$t0" "$t2")s"
"$ZOU_BIN/zou" stats "$ZOU_STORE_STATS" | tee "$WORK/stats-$LABEL.json"
say "log $WORK/attach-$LABEL.log"
