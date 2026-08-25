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

# Two spaces in front of whatever came out of something else, so a dump
# of a log or a lease reads as part of the line that introduced it.
indent() { sed 's/^/  /'; }

# The lease as the store has it, which is the one question a write that
# will not go through is about. `zou info` reads the manifest and takes
# nothing, so it is safe to ask this of a store somebody is writing to.
lease_now() {
	if [ -x "$ZOU_BIN/zou" ]; then
		"$ZOU_BIN/zou" info "$ZOU_TARGET" "${ZOU_TENANT:-local}" 2>&1 |
			grep -E "^(ref|lease)" || true
	else
		echo "no $ZOU_BIN/zou here to read the lease with"
	fi
}

# Writes block until the pusher holds the lease, which after a kill -9
# means waiting out the previous incarnation's TTL. Probe until a real
# commit gets through so the load phase measures load, not lease waits.
#
# It says why it is still waiting rather than only that it gave up. Both
# halves of that are the point of zou #503: the wait ran out twice on CI
# and the only thing in the log was that it had, so nobody could tell
# whether the takeover was never tried or tried and refused. The probe's
# own answer names which, the lease says who is holding it and until
# when, and the postmaster log has whatever `zou_wal_open` said on its
# way past.
wait_writable() {
	# Local because the loop below has a `last` of its own, holding the
	# last acked ledger id, and a probe's error message is not an id.
	local tries=${1:-120}
	local last=
	local n
	for n in $(seq 1 "$tries"); do
		if last=$(q "insert into probe values(1)" 2>&1); then
			return 0
		fi
		# Every ten seconds and not every one: the normal case is a
		# wait of one ttl, and fifteen lines of it would bury the run
		# they are part of.
		if [ $((n % 10)) -eq 0 ]; then
			say "still not writable after ${n}s: $(echo "$last" | tr '\n' ' ')"
			say "  lease: $(lease_now | tr '\n' ' ')"
		fi
		sleep 1
	done
	say "FAIL: server never became writable after ${tries}s"
	say "the probe's last answer:"
	printf '%s\n' "$last" | indent
	say "the lease as the store has it:"
	lease_now | indent
	say "the last 40 lines of $PGDATA.log:"
	tail -40 "$PGDATA.log" 2>&1 | indent
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
