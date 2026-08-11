#!/usr/bin/env bash
# SIGSTOP a whole cluster under write load, let a successor take the
# store, and SIGCONT the first one back into a world that moved on.
#
# This is not the crash loop. A killed server is gone and its in flight
# PUTs died with it, which is what zou-crash-loop.sh covers. A frozen one
# keeps every request it had issued, and its lease heartbeat, and lets
# them all go at once after the TTL has passed and a successor has sealed
# the chain and been writing. Lambda between invocations and a suspended
# Fly machine are both exactly this, and neither of them tells the
# process it happened.
#
# What must hold, and what each phase below checks:
#
#   - An ack is never a lie. Every id the first cluster acked before the
#     freeze is present in the successor, and in a third cluster restored
#     from nothing but the objects.
#   - The successor keeps the store. The thawed writer's late landing
#     PUTs do not stop it from writing, and do not stop a fresh reader
#     from walking the chain past them.
#   - The thawed writer acks nothing. It lost the lease while it was
#     stopped, and it has to find that out rather than keep taking work.
#
# Usage: scripts/zou-freeze-thaw.sh [freeze seconds]
# Env overrides: PG_BIN, ZOU_BIN, WORK, PORT.

set -euo pipefail

FREEZE=${1:-20}
PG_BIN=${PG_BIN:-build/pg/bin}
ZOU_BIN=${ZOU_BIN:-target/release}
WORK=${WORK:-$(mktemp -d /tmp/zou-freeze.XXXXXX)}
PORT=${PORT:-5621}
SOCK="$WORK"

export ZOU_TARGET="$WORK/store"
# How long a pusher waits for a lease somebody else holds before deciding
# it has been replaced and stopping the cluster. Well above the 15s TTL,
# so the successor below still starts on the first try, and low enough
# that the thawed cluster stops while this script is still watching.
export ZOU_LEASE_WAIT_SECS=${ZOU_LEASE_WAIT_SECS:-25}
mkdir -p "$ZOU_TARGET"
ACKED="$WORK/acked.txt"
: >"$ACKED"

LOAD=
LEDGER=

say() { echo "[freeze-thaw] $*"; }
# Both timeouts are there for the thawed cluster, which may answer, may
# refuse, or may be a socket nobody is listening on any more. None of
# those may hang the run.
q() {
	PGCONNECT_TIMEOUT=5 "$PG_BIN/psql" -h "$SOCK" -p "$1" -d postgres -Atqc \
		"set statement_timeout='20s'; $2"
}

start() {
	"$PG_BIN/pg_ctl" -D "$1" -l "$1.log" -w -t 120 \
		-o "-p $2 -k $SOCK -c listen_addresses=''" start
}

# Writes block until the pusher holds the lease, which after a freeze
# means waiting out the frozen incarnation's TTL. Probe until a real
# commit gets through so what follows measures the test and not the wait.
wait_writable() {
	for _ in $(seq 1 120); do
		if q "$1" "insert into probe values(1)" >/dev/null 2>&1; then
			return 0
		fi
		sleep 1
	done
	say "FAIL: cluster on port $1 never became writable"
	exit 1
}

# Every process of a cluster, because a suspend stops all of them. The
# postmaster alone would leave the backends and the pusher running, which
# is a different and much less interesting thing.
tree() {
	echo "$1"
	local child
	for child in $(pgrep -P "$1" 2>/dev/null || true); do
		tree "$child"
	done
}

signal_tree() {
	local pid
	for pid in $(tree "$1"); do
		kill -"$2" "$pid" 2>/dev/null || true
	done
}

PGDATA_A="$WORK/pgdata-a"
PGDATA_B="$WORK/pgdata-b"
PGDATA_C="$WORK/pgdata-c"
PORT_B=$((PORT + 1))
PORT_C=$((PORT + 2))

say "initdb and bootstrap into $ZOU_TARGET"
"$PG_BIN/initdb" -D "$PGDATA_A" --set io_method=sync --set full_page_writes=off \
	>"$WORK/initdb.log" 2>&1
REDO=$("$PG_BIN/pg_controldata" -D "$PGDATA_A" | grep "REDO location" | awk '{print $NF}')
"$ZOU_BIN/zou-bootstrap" "$ZOU_TARGET" "$PGDATA_A" --redo "$REDO"

start "$PGDATA_A" "$PORT"
PM_A=$(head -1 "$PGDATA_A/postmaster.pid")
trap 'signal_tree "$PM_A" CONT; kill ${LOAD:-} ${LEDGER:-} 2>/dev/null || true' EXIT

q "$PORT" "create table ledger(id bigint primary key)"
q "$PORT" "create table probe(x int)"
wait_writable "$PORT"

say "load on the first cluster, pid $PM_A, port $PORT"
"$PG_BIN/pgbench" -h "$SOCK" -p "$PORT" -i -s 1 postgres >"$WORK/pgbench-init.log" 2>&1
"$PG_BIN/pgbench" -h "$SOCK" -p "$PORT" -c 4 -T 300 postgres >"$WORK/pgbench.log" 2>&1 &
LOAD=$!
(
	k=1
	while :; do
		if "$PG_BIN/psql" -h "$SOCK" -p "$PORT" -d postgres -Atqc \
			"insert into ledger values($k)" >/dev/null 2>&1; then
			echo "$k" >>"$ACKED"
		fi
		k=$((k + 1))
	done
) &
LEDGER=$!
sleep 5

# The freeze. Everything stops where it stands: backends mid commit, the
# pusher with PUTs issued and unanswered, the heartbeat with a lease it
# will not renew again.
say "SIGSTOP the whole cluster with $(wc -l <"$ACKED" | tr -d ' ') ids acked"
signal_tree "$PM_A" STOP
# The load and ledger clients are ours, and they are now blocked on a
# server that will never answer. Cut them so nothing is in flight from
# this side either.
kill -9 "$LOAD" "$LEDGER" 2>/dev/null || true
wait "$LOAD" 2>/dev/null || true
wait "$LEDGER" 2>/dev/null || true
FROZEN_ACKED=$(wc -l <"$ACKED" | tr -d ' ')

say "waiting $FREEZE seconds, past the 15 the writer lease lives for"
sleep "$FREEZE"

say "the successor restores from the store alone into $PGDATA_B"
"$ZOU_BIN/zou-restore" "$ZOU_TARGET" "$PGDATA_B"
start "$PGDATA_B" "$PORT_B"
wait_writable "$PORT_B"

sort "$ACKED" >"$WORK/want.txt"
q "$PORT_B" "select id from ledger" | sort >"$WORK/have.txt"
missing=$(comm -23 "$WORK/want.txt" "$WORK/have.txt")
if [ -n "$missing" ]; then
	say "FAIL: the successor is missing ids the frozen cluster acked:"
	echo "$missing" | head
	exit 1
fi
say "the successor has all $FROZEN_ACKED acked ids"

# The successor writes into the seqs the frozen pipeline was handed and
# never got to use, which is where the thaw's stragglers are about to
# land.
next=$((FROZEN_ACKED + 1000))
for i in $(seq 0 199); do
	q "$PORT_B" "insert into ledger values($((next + i)))" >/dev/null
	echo "$((next + i))" >>"$ACKED"
done

say "SIGCONT the frozen cluster into a store it no longer owns"
signal_tree "$PM_A" CONT

# Its landing PUTs go out now, wall clock minutes after they were
# issued, into a chain a successor sealed. The write must not be acked,
# and the cluster has to stop rather than sit there hanging commits: a
# backend past its flush holds interrupts, so a commit blocked in the
# write gate cannot even be cancelled, and a postmaster that answers on
# its port is one a load balancer keeps sending work to.
if q "$PORT" "insert into ledger values(999000001)" >/dev/null 2>&1; then
	say "FAIL: the thawed cluster acked a write after it was fenced"
	exit 1
fi
if ! grep -q "has been replaced and is stopping" "$PGDATA_A.log"; then
	say "FAIL: the thawed cluster refused the write but never worked out why"
	exit 1
fi
for _ in $(seq 1 60); do
	kill -0 "$PM_A" 2>/dev/null || break
	sleep 1
done
if kill -0 "$PM_A" 2>/dev/null; then
	say "FAIL: the thawed cluster is still up after it was told it lost the store"
	signal_tree "$PM_A" KILL
	exit 1
fi
say "the thawed cluster refused the write and stopped itself"

# And the successor is unharmed by the litter. This is the half that
# fails when a late landing PUT is treated as a fence rather than as
# what it is.
for i in $(seq 200 399); do
	if ! q "$PORT_B" "insert into ledger values($((next + i)))" >/dev/null 2>&1; then
		say "FAIL: the successor lost the shard to a thawed writer's late put"
		exit 1
	fi
	echo "$((next + i))" >>"$ACKED"
done
say "the successor took 200 more commits across the thaw"

"$PG_BIN/pg_ctl" -D "$PGDATA_B" stop >/dev/null

say "a third cluster restores from nothing but the objects into $PGDATA_C"
"$ZOU_BIN/zou-restore" "$ZOU_TARGET" "$PGDATA_C"
"$PG_BIN/pg_controldata" -D "$PGDATA_C" | grep -q "in production"
start "$PGDATA_C" "$PORT_C"

sort "$ACKED" >"$WORK/want.txt"
q "$PORT_C" "select id from ledger" | sort >"$WORK/have.txt"
missing=$(comm -23 "$WORK/want.txt" "$WORK/have.txt")
if [ -n "$missing" ]; then
	say "FAIL: acked commits missing after the freeze, the thaw, and a restore:"
	echo "$missing" | head
	exit 1
fi
if q "$PORT_C" "select count(*) from ledger where id = 999000001" | grep -q '^1$'; then
	say "FAIL: a write the thawed cluster was told had failed is in the chain"
	exit 1
fi

"$PG_BIN/pg_ctl" -D "$PGDATA_C" stop >/dev/null
say "ok: $(wc -l <"$WORK/want.txt" | tr -d ' ') acked commits survived a ${FREEZE}s freeze, work dir $WORK"
