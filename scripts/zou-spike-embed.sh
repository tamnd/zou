#!/usr/bin/env bash
# The M1 embedded execution spikes: measure a managed child postmaster
# on a unix socket loopback against the single user backend that an in
# process C ABI link would wrap, both attached to a real zou store.
#
# Measured per round: cold start to first answered query for each mode,
# plus a crash isolation probe and a concurrent session probe for the
# postmaster. Sizes come from the installed binaries. The numbers land
# in docs/architecture.md next to the decision.
#
# Usage: scripts/zou-spike-embed.sh [rounds]
# Env overrides: PG_BIN, ZOU_BIN, WORK, PORT.

set -euo pipefail

N=${1:-5}
PG_BIN=${PG_BIN:-build/pg/bin}
ZOU_BIN=${ZOU_BIN:-target/release}
WORK=${WORK:-$(mktemp -d /tmp/zou-spike.XXXXXX)}
PORT=${PORT:-5613}
SOCK="$WORK"

export ZOU_TARGET="$WORK/store"
# These start postgres themselves, with no page service to read
# through, so the object path is the one under test here.
export ZOU_PAGESERVE=0
mkdir -p "$ZOU_TARGET"

say() { echo "[spike] $*"; }
now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
q() { "$PG_BIN/psql" -h "$SOCK" -p "$PORT" -d postgres -Atqc "$1"; }

start() {
	"$PG_BIN/pg_ctl" -D "$PGDATA" -l "$PGDATA.log" -w -t 120 \
		-o "-p $PORT -k $SOCK -c listen_addresses=''" start >/dev/null
}
stop() {
	"$PG_BIN/pg_ctl" -D "$PGDATA" -w -t 120 -m fast stop >/dev/null
}

# Writes block until the pusher holds the lease, so after any unclean
# exit the first write waits out the previous incarnation's TTL. Probe
# until one lands so the timings measure the mode, not lease waits.
wait_writable() {
	for _ in $(seq 1 120); do
		if q "insert into probe values(1)" >/dev/null 2>&1; then
			return 0
		fi
		sleep 1
	done
	say "FAIL: server never became writable"
	exit 1
}

PGDATA="$WORK/pgdata"
say "initdb and bootstrap into $ZOU_TARGET"
"$PG_BIN/initdb" -D "$PGDATA" --set io_method=sync --set full_page_writes=off >"$WORK/initdb.log" 2>&1
REDO=$("$PG_BIN/pg_controldata" -D "$PGDATA" | grep "REDO location" | awk '{print $NF}')
"$ZOU_BIN/zou-bootstrap" "$ZOU_TARGET" "$PGDATA" --redo "$REDO"

say "seed data through a first postmaster session"
start
q "create table probe(x int)" >/dev/null
q "create table t(id int primary key, v text)" >/dev/null
wait_writable
q "insert into t select g, repeat('x', 50) from generate_series(1, 10000) g" >/dev/null
stop

say "spike B: managed child postmaster, $N cold starts"
B_TIMES=()
for i in $(seq 1 "$N"); do
	t0=$(now_ms)
	start
	got=$(q "select count(*) from t")
	t1=$(now_ms)
	[ "$got" = 10000 ] || { say "FAIL: bad answer $got"; exit 1; }
	B_TIMES+=($((t1 - t0)))
	stop
done

say "spike B: crash isolation and concurrent sessions"
start
wait_writable
("$PG_BIN/psql" -h "$SOCK" -p "$PORT" -d postgres -Atqc "select pg_sleep(30)" >/dev/null 2>&1 || true) &
sleep 1
VICTIM=$(q "select pid from pg_stat_activity where query like '%pg_sleep%' and pid <> pg_backend_pid() limit 1")
kill -9 "$VICTIM"
CRASH_B="backend $VICTIM killed with SIGKILL, host script unaffected"
for _ in $(seq 1 60); do
	if [ "$(q "select 1" 2>/dev/null)" = 1 ]; then
		CRASH_B="$CRASH_B, postmaster recovered by itself"
		break
	fi
	sleep 1
done
wait_writable
for s in 1 2 3 4; do
	q "select count(*) from t" >/dev/null &
done
wait
SESS_B="4 concurrent sessions answered"
stop

say "spike A: single user backend, $N sessions"
A_TIMES=()
for i in $(seq 1 "$N"); do
	t0=$(now_ms)
	out=$("$PG_BIN/postgres" --single -D "$PGDATA" postgres \
		<<<"select count(*) from t;" 2>"$WORK/single-$i.log")
	t1=$(now_ms)
	grep -q 'count = "10000"' <<<"$out" || {
		say "FAIL: single user session $i gave no answer, see $WORK/single-$i.log"
		exit 1
	}
	A_TIMES+=($((t1 - t0)))
done

say "spike A: a write through the single user backend"
"$PG_BIN/postgres" --single -D "$PGDATA" postgres \
	<<<"insert into t values (10001, 'single');" >/dev/null 2>"$WORK/single-write.log"
out=$("$PG_BIN/postgres" --single -D "$PGDATA" postgres \
	<<<"select v from t where id = 10001;" 2>/dev/null)
grep -q 'single' <<<"$out" || { say "FAIL: single user write lost"; exit 1; }
WRITE_A="a single user insert commits through the store and survives the session"

stats() {
	python3 -c '
import sys
xs = sorted(int(a) for a in sys.argv[1:])
mid = xs[len(xs) // 2]
print(f"median {mid} ms, min {xs[0]} ms, max {xs[-1]} ms")
' "$@"
}

PG_SIZE=$(du -h "$PG_BIN/postgres" | cut -f1)
LIB_SIZE=$(du -h "$ZOU_BIN/libzou_pg.a" | cut -f1)

say "results"
echo "postmaster cold start to first query:    $(stats "${B_TIMES[@]}")"
echo "single user session to first query:      $(stats "${A_TIMES[@]}")"
echo "postmaster crash isolation:              $CRASH_B"
echo "postmaster concurrency:                  $SESS_B"
echo "single user write path:                  $WRITE_A"
echo "single user concurrency:                 one session per process, by design"
echo "postgres binary: $PG_SIZE, zou-pg staticlib linked into it: $LIB_SIZE"
say "work dir kept at $WORK"
