#!/usr/bin/env bash
# Long soak: pgbench load, random kills, reattach, and lease probes
# against one store, checking invariants after every recovery. This is
# the M1 exit line: 24 h on MinIO, zero violations.
#
# Three failure modes rotate:
#   crash   kill -9 the postmaster, zou dev restarts it in place and
#           recovery replays, the recover-and-continue path
#   death   kill -9 zou dev and every postgres under it, then attach a
#           fresh instance from the store alone, which also exercises
#           taking over the dead node's expired lease
#   steal   while the writer is alive and heartbeating, attach a second
#           instance and try to write through it, any acked write there
#           would be split brain
#
# Invariants checked after every wait for writability:
#   ledger  every id acked to a client is present, the crash loop
#           protocol, an id is recorded only after COMMIT returned
#   tpcb    sum of history deltas equals the account, branch, and
#           teller balance sums, so replay was exact, not just present
#
# Usage: scripts/zou-soak.sh <target>
# Env: SOAK_SECONDS (86400), SOAK_SCALE (10), PG_BIN, ZOU_BIN, WORK,
# PORT, plus whatever the target needs (ZOU_S3_ENDPOINT, AWS keys).

set -euo pipefail

TARGET=${1:?usage: scripts/zou-soak.sh <target>}
SOAK_SECONDS=${SOAK_SECONDS:-86400}
SOAK_SCALE=${SOAK_SCALE:-10}
PG_BIN=${PG_BIN:-build/pg/bin}
ZOU_BIN=${ZOU_BIN:-target/release}
WORK=${WORK:-$(mktemp -d /tmp/zou-soak.XXXXXX)}
PORT=${PORT:-5613}

mkdir -p "$WORK"
ACKED="$WORK/acked.txt"
: >"$ACKED"
violations=0
iter=0

say() { echo "[soak $(date '+%H:%M:%S')] $*"; }
q() { "$PG_BIN/psql" -h 127.0.0.1 -p "$PORT" -d postgres -Atqc "$1"; }

start_dev() {
	RT="$WORK/rt-$iter"
	"$ZOU_BIN/zou" dev "$TARGET" --pg-bin "$PG_BIN" --port "$PORT" \
		--runtime "$RT" >>"$WORK/dev-$iter.log" 2>&1 &
	DEVPID=$!
}

kill_node() {
	kill -9 "$DEVPID" 2>/dev/null || true
	pkill -9 -f "$RT" 2>/dev/null || true
	wait "$DEVPID" 2>/dev/null || true
	sleep 2
}

# Writes block until the lease is held, which after a node death means
# waiting out the previous incarnation's TTL, and a fresh attach then
# replays the whole durable WAL overlay before accepting anything. On
# a slow store that legitimately takes many minutes, so the clock here
# measures stall, not wall time: as long as the server keeps logging
# something we did not cause ourselves, keep waiting, under a one hour
# cap. Our own probes append not-yet-accepting FATALs to the dev log
# every second, so those lines are filtered out of the progress count
# or the log would always look alive.
wait_writable() {
	local devlog="$WORK/dev-$iter.log"
	local stall=0 last=-1 lines
	for _ in $(seq 1 3600); do
		if q "insert into probe values(1)" >/dev/null 2>&1; then
			return 0
		fi
		lines=$(grep -cv "accepting connections\|Consistent recovery state" \
			"$devlog" 2>/dev/null || echo 0)
		if [ "$lines" != "$last" ]; then
			last=$lines
			stall=0
		else
			stall=$((stall + 1))
		fi
		if [ "$stall" -ge 240 ]; then
			break
		fi
		sleep 1
	done
	say "VIOLATION: server never became writable in iteration $iter"
	violations=$((violations + 1))
	return 1
}

# Query with retries, so a connection blip during verification does
# not read as data loss. Only a stable wrong answer counts.
vq() {
	local out
	for _ in 1 2 3 4 5; do
		if out=$(q "$1" 2>/dev/null); then
			echo "$out"
			return 0
		fi
		sleep 2
	done
	echo "query-failed"
}

verify() {
	vq "select id from ledger" | sort >"$WORK/have.txt"
	sort "$ACKED" >"$WORK/want.txt"
	missing=$(comm -23 "$WORK/want.txt" "$WORK/have.txt")
	if [ -n "$missing" ]; then
		say "VIOLATION: acked commits missing after iteration $iter:"
		echo "$missing" | head
		violations=$((violations + 1))
	fi
	balanced=$(vq "select coalesce(sum(delta),0) = (select sum(abalance) from pgbench_accounts)
		and coalesce(sum(delta),0) = (select sum(bbalance) from pgbench_branches)
		and coalesce(sum(delta),0) = (select sum(tbalance) from pgbench_tellers)
		from pgbench_history")
	if [ "$balanced" != "t" ]; then
		say "VIOLATION: tpcb balances disagree with history after iteration $iter ($balanced)"
		violations=$((violations + 1))
	fi
}

load_start() {
	# -n matters: a plain pgbench run truncates pgbench_history before
	# starting, which would wipe the very rows the balance invariant
	# sums. Found the hard way when iteration 2 read as data loss.
	"$PG_BIN/pgbench" -n -h 127.0.0.1 -p "$PORT" -c 4 -T "$SOAK_SECONDS" postgres \
		>>"$WORK/pgbench.log" 2>&1 &
	LOAD=$!
	(
		k=$next_id
		while :; do
			if q "insert into ledger values($k)" >/dev/null 2>&1; then
				echo "$k" >>"$ACKED"
			fi
			k=$((k + 1))
		done
	) &
	LEDGER=$!
}

load_stop() {
	kill -9 "$LOAD" "$LEDGER" 2>/dev/null || true
	wait "$LOAD" 2>/dev/null || true
	wait "$LEDGER" 2>/dev/null || true
	# Ids acked but in flight at the kill were never recorded, leave a
	# gap before reusing the counter to dodge duplicate keys.
	last=$(tail -1 "$ACKED" 2>/dev/null || echo 0)
	next_id=$((last + 1000))
}

say "soak target $TARGET for ${SOAK_SECONDS}s, work dir $WORK"
start_dev
next_id=1
for _ in $(seq 1 240); do
	if q "select 1" >/dev/null 2>&1; then break; fi
	sleep 1
done
q "create table if not exists ledger(id bigint primary key)"
q "create table if not exists probe(x int)"
wait_writable
"$PG_BIN/pgbench" -h 127.0.0.1 -p "$PORT" -i -q -s "$SOAK_SCALE" postgres \
	>"$WORK/pgbench-init.log" 2>&1
say "initialized scale $SOAK_SCALE, entering the kill loop"

while [ "$SECONDS" -lt "$SOAK_SECONDS" ] && [ "$violations" -eq 0 ]; do
	iter=$((iter + 1))
	mode=$((iter % 3))
	load_start
	sleep $((RANDOM % 160 + 20))

	case $mode in
	1)
		PM=$(head -1 "$RT/pgdata/postmaster.pid" 2>/dev/null || echo "")
		say "iteration $iter: kill -9 postmaster ${PM:-unknown}, supervised restart"
		[ -n "$PM" ] && kill -9 "$PM" 2>/dev/null || true
		load_stop
		;;
	2)
		say "iteration $iter: kill -9 the whole node, fresh attach from the store"
		load_stop
		kill_node
		start_dev
		;;
	0)
		say "iteration $iter: lease steal probe against the live writer"
		SRT="$WORK/steal-$iter"
		"$ZOU_BIN/zou" dev "$TARGET" --pg-bin "$PG_BIN" --port $((PORT + 1)) \
			--runtime "$SRT" >>"$WORK/steal-$iter.log" 2>&1 &
		SPID=$!
		stole=""
		for _ in $(seq 1 25); do
			if "$PG_BIN/psql" -h 127.0.0.1 -p $((PORT + 1)) -d postgres -Atqc \
				"insert into probe values(2)" >/dev/null 2>&1; then
				stole=yes
				break
			fi
			sleep 1
		done
		kill -9 "$SPID" 2>/dev/null || true
		pkill -9 -f "$SRT" 2>/dev/null || true
		wait "$SPID" 2>/dev/null || true
		if [ -n "$stole" ]; then
			say "VIOLATION: second instance wrote while the lease holder was alive"
			violations=$((violations + 1))
		fi
		load_stop
		;;
	esac

	if wait_writable; then
		verify
	fi
	say "iteration $iter done, $(wc -l <"$ACKED" | tr -d ' ') acked ids, violations $violations, elapsed ${SECONDS}s"
done

kill_node
if [ "$violations" -eq 0 ]; then
	say "PASS: $iter iterations over ${SECONDS}s, zero invariant violations, work dir $WORK"
else
	say "FAIL: $violations violations in $iter iterations, evidence in $WORK"
	exit 1
fi
