#!/usr/bin/env bash
# Time a cold attach to a first answered query, and say what it spent
# the time on.
#
# A cold attach is three phases. The restore pulls the skeleton, which
# is a fixed and small number of objects. Crash recovery then replays
# the WAL tail, and every page a record touches is a store round trip
# taken one at a time, so on a store thirty milliseconds away that phase
# is most of the attach. The third is the one a client can see: the
# first query it gets an answer to, which faults in the pages that
# recovery did not happen to need. This measures all three, and dumps
# the store op counters, because the number that explains the wall clock
# is gets and not bytes.
#
# The scenario is built once into WORK and reused: a pgbench database,
# a load, then kill -9, which leaves the store with a WAL tail past its
# last checkpoint. It is kept as a pristine copy and every run attaches
# from a fresh copy of it, because an attach is a write: recovery puts
# the pages it rebuilt back, and a second attach of the same store then
# finds the page LSN already past the record and replays nothing. Runs
# have to start from the same store or they are not the same run.
#
# Two ways to run it. With nothing set the store is a local directory
# and ZOU_STORE_SIM adds the latency of a distant one, which is cheap
# and repeatable and is not a measurement of any real store. With
# REMOTE set to an object store prefix, the pristine store is pushed
# under it and the attach reads the real thing over the real network,
# which is what the 500 ms target in docs/perf.md is about.
#
# Usage: scripts/zou-cold-attach.sh [label]
# Env overrides: PG_BIN, ZOU_BIN, WORK, REMOTE, SIM, SCALE, LOAD_SECS,
# and the ZOU_WARM_* knobs, which pass through, so ZOU_WARM_BLOCKS=0
# measures the same store with the warm up off.
#
#   REMOTE=s3://zou-bench/cold ZOU_S3_ENDPOINT=http://127.0.0.1:9100 \
#     AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
#     scripts/zou-cold-attach.sh minio-1

set -euo pipefail

LABEL=${1:-run}
PG_BIN=${PG_BIN:-build/pg/bin}
ZOU_BIN=${ZOU_BIN:-target/release}
WORK=${WORK:-/tmp/zou-cold-attach}
REMOTE=${REMOTE:-}
SIM=${SIM:-s3-standard}
SCALE=${SCALE:-25}
LOAD_SECS=${LOAD_SECS:-20}
# The pool the attaching server gets. Small on purpose: a recovery that
# holds the whole database in shared buffers faults every page once and
# then never reads again, which is not what a real attach looks like.
SHARED_BUFFERS=${SHARED_BUFFERS:-32MB}
PORT=${PORT:-5613}
# The first query. One row out of one page of the biggest table, by the
# primary key, which is the cheapest thing that still has to reach the
# store for a page. A fixed key rather than a random one because every
# run starts with an empty page cache directory anyway, and a fixed key
# is one less thing that differs between two runs being compared.
FIRST_QUERY=${FIRST_QUERY:-select abalance from pgbench_accounts where aid = 1}

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

PGDATA="$WORK/pgdata-$LABEL"
CACHE="$WORK/cache-$LABEL"
rm -rf "$PGDATA" "$CACHE" "$WORK/stats-$LABEL"
mkdir -p "$CACHE"

if [ -n "$REMOTE" ]; then
	# Each run gets its own prefix, because the attach writes into the
	# store it reads and the second attach of a prefix is not a cold
	# one. The upload is not timed and is not part of the number.
	TARGET="$REMOTE/$LABEL"
	say "uploading the scenario to $TARGET"
	"$ZOU_BIN/zou" push "$PRISTINE" "$TARGET" >"$WORK/push-$LABEL.log" 2>&1
	unset ZOU_STORE_SIM
else
	TARGET="$STORE"
	rm -rf "$STORE"
	cp -R "$PRISTINE" "$STORE"
	export ZOU_STORE_SIM="$SIM"
fi

export ZOU_TARGET="$TARGET"
export ZOU_TENANT=local
# This starts postgres itself, with no page service to read through,
# so the object path is the one being timed here.
export ZOU_PAGESERVE=0
export ZOU_PAGE_CACHE="$CACHE"
export ZOU_STORE_STATS="$WORK/stats-$LABEL"

say "attaching as $LABEL from $TARGET${ZOU_STORE_SIM:+ under $ZOU_STORE_SIM}, shared_buffers $SHARED_BUFFERS, warm blocks ${ZOU_WARM_BLOCKS:-default}"
t0=$(now)
"$ZOU_BIN/zou-restore" "$TARGET" "$PGDATA" local
t1=$(now)
# -W on purpose, so pg_ctl does not wait. What it waits for is a
# connection it then asks nothing, which is ready to take a client
# rather than ready to answer one, and the difference between those two
# is a phase of this measurement.
#
# Emptied first, because pg_ctl appends and the wait below is a grep of
# this file. A previous run's ready line sitting in it is a wait that
# ends before the postmaster has read anything.
: >"$WORK/attach-$LABEL.log"
"$PG_BIN/pg_ctl" -D "$PGDATA" -l "$WORK/attach-$LABEL.log" -W \
	-o "-p $PORT -k $WORK -c listen_addresses='' -c shared_buffers=$SHARED_BUFFERS" start
trap 'kill -9 "$(head -1 "$PGDATA/postmaster.pid" 2>/dev/null)" 2>/dev/null || true' EXIT

# Wait on the log rather than on the socket. The obvious loop here is a
# psql retried until it answers, and the first run of this script did
# that: it spun without a sleep, on the theory that a refused connection
# is cheap. It is not. Postgres answers a connection during recovery by
# forking a backend that then says FATAL, so the loop forked a third of
# a million backends on the box that was trying to recover, wrote 33MB
# of log doing it, and measured its own interference. Grepping a file
# costs the recovery nothing.
until grep -q "ready to accept connections" "$WORK/attach-$LABEL.log"; do
	# The pid file is written before the postmaster opens its socket,
	# so its absence this early is the start still happening.
	pid=$(head -1 "$PGDATA/postmaster.pid" 2>/dev/null || true)
	if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
		say "the postmaster is gone, see $WORK/attach-$LABEL.log"
		exit 1
	fi
	sleep 0.05
done
t2=$(now)
# Then the query, once, on a connection made after readiness, which is
# the first thing a client gets to ask. Ready is not answered: the pages
# this reads are still in the store at this point, and on a store thirty
# milliseconds away that gap is the part of the wait a user sees.
answer=$("$PG_BIN/psql" -h "$WORK" -p "$PORT" -d postgres -Atqc "$FIRST_QUERY")
t3=$(now)

# Immediate, and after the numbers are printed. A smart shutdown writes
# a checkpoint, which over a simulated distant store took longer than
# any timeout worth waiting and left a postmaster holding the socket
# against the next run. Nothing here needs the cluster again.
say "restore $(took "$t0" "$t1")s, recovery $(took "$t1" "$t2")s, first query $(took "$t2" "$t3")s, attach $(took "$t0" "$t3")s, answered ${answer:-null}"
"$PG_BIN/pg_ctl" -D "$PGDATA" -m immediate -w -t 120 stop >/dev/null || true
trap - EXIT
"$ZOU_BIN/zou" stats "$ZOU_STORE_STATS" | tee "$WORK/stats-$LABEL.json"
say "log $WORK/attach-$LABEL.log"
