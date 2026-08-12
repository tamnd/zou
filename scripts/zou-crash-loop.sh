#!/usr/bin/env bash
# kill -9 the server under sustained write load, reattach from the store
# alone with zou-restore, and verify that every acked commit survived.
#
# The ledger protocol is the whole point: a client records an id only
# after the server acks the COMMIT, and after every crash and reattach
# each recorded id must be present. Losing one would mean an acked
# commit was not durable on the store.
#
# Usage: scripts/zou-crash-loop.sh [iterations]
# Env overrides: PG_BIN, ZOU_BIN, WORK.

set -euo pipefail

N=${1:-3}
PG_BIN=${PG_BIN:-build/pg/bin}
ZOU_BIN=${ZOU_BIN:-target/release}
WORK=${WORK:-$(mktemp -d /tmp/zou-crash.XXXXXX)}
PORT=${PORT:-5611}
SOCK="$WORK"

export ZOU_TARGET="$WORK/store"
# These start postgres themselves, with no page service to read
# through, so the object path is the one under test here.
export ZOU_PAGESERVE=0
mkdir -p "$ZOU_TARGET"
ACKED="$WORK/acked.txt"
: > "$ACKED"

say() { echo "[crash-loop] $*"; }
q() { "$PG_BIN/psql" -h "$SOCK" -p "$PORT" -d postgres -Atqc "$1"; }

start() {
	"$PG_BIN/pg_ctl" -D "$1" -l "$1.log" -w -t 120 \
		-o "-p $PORT -k $SOCK -c listen_addresses=''" start
}

# Writes block until the pusher holds the lease, which after a kill -9
# means waiting out the previous incarnation's TTL. Probe until a real
# commit gets through so the load phase measures load, not lease waits.
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

PGDATA="$WORK/pgdata0"
say "initdb and bootstrap into $ZOU_TARGET"
"$PG_BIN/initdb" -D "$PGDATA" --set io_method=sync --set full_page_writes=off >"$WORK/initdb.log" 2>&1
REDO=$("$PG_BIN/pg_controldata" -D "$PGDATA" | grep "REDO location" | awk '{print $NF}')
"$ZOU_BIN/zou-bootstrap" "$ZOU_TARGET" "$PGDATA" --redo "$REDO"

start "$PGDATA"
q "create table ledger(id bigint primary key)"
q "create table probe(x int)"
wait_writable
"$PG_BIN/pgbench" -h "$SOCK" -p "$PORT" -i -s 1 postgres >"$WORK/pgbench-init.log" 2>&1

next_id=1
for i in $(seq 1 "$N"); do
	say "iteration $i: pgbench load plus ledger writes against $PGDATA"
	"$PG_BIN/pgbench" -h "$SOCK" -p "$PORT" -c 4 -T 60 postgres \
		>"$WORK/pgbench-$i.log" 2>&1 &
	LOAD=$!
	(
		k=$next_id
		while :; do
			if "$PG_BIN/psql" -h "$SOCK" -p "$PORT" -d postgres -Atqc \
				"insert into ledger values($k)" >/dev/null 2>&1; then
				echo "$k" >>"$ACKED"
			fi
			k=$((k + 1))
		done
	) &
	LEDGER=$!

	sleep $((3 + i % 4))
	PM=$(head -1 "$PGDATA/postmaster.pid")
	say "kill -9 postmaster $PM"
	kill -9 "$PM"
	# Sever the clients too, so orphaned backends see EOF and exit
	# instead of serving a dead cluster.
	kill -9 "$LOAD" "$LEDGER" 2>/dev/null || true
	wait "$LOAD" 2>/dev/null || true
	wait "$LEDGER" 2>/dev/null || true
	sleep 2

	# Ids acked but still in flight at the kill were never recorded, so
	# leave a gap before reusing the counter to dodge duplicate keys.
	last=$(tail -1 "$ACKED" 2>/dev/null || echo 0)
	next_id=$((last + 1000))

	PGDATA="$WORK/pgdata$i"
	say "reattach from the store into $PGDATA"
	"$ZOU_BIN/zou-restore" "$ZOU_TARGET" "$PGDATA"
	"$PG_BIN/pg_controldata" -D "$PGDATA" | grep -q "in production"
	start "$PGDATA"

	q "select id from ledger" | sort >"$WORK/have.txt"
	sort "$ACKED" >"$WORK/want.txt"
	missing=$(comm -23 "$WORK/want.txt" "$WORK/have.txt")
	if [ -n "$missing" ]; then
		say "FAIL: acked commits missing after reattach:"
		echo "$missing" | head
		exit 1
	fi
	say "iteration $i ok: all $(wc -l <"$WORK/want.txt" | tr -d ' ') acked commits present"
	wait_writable
done

"$PG_BIN/pg_ctl" -D "$PGDATA" stop
say "done: $N kill -9 and reattach cycles, no acked commit lost, work dir $WORK"
