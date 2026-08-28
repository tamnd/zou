#!/bin/sh
# The write leg, A against B, on three stores, alternating.
#
# Usage: scripts/zou-write-ab.sh --before <prefix> --after <prefix> [options]
#
#   --before <prefix>   an install to measure, see below
#   --after  <prefix>   the other one
#   --legs   pg,fs,s3   which stores, default pg,fs
#   --s3     <url>      the object store for the s3 leg, s3://bucket/prefix
#   --zou    <path>     a zou binary, for `zou stats`, default none
#   --runs   <n>        rounds of every leg on both sides, default 3
#   --seconds <n>       one measurement, default 45
#   --clients <n>       connections, default 16
#   --port   <n>        base port, default 54399
#   --work   <dir>      where data directories and logs go, default a mktemp
#
# A prefix is a directory holding `bin/postgres` and the rest of an
# install, with a `zou-bootstrap` next to that bin. Build one per side
# yourself and pass both, because building two vendored postgres trees
# is not something a bench script should be doing behind anybody's
# back, and because which two revisions are being compared is the whole
# question. For each side:
#
#   git checkout <rev>
#   make zou-pg-lib && ninja -C build/pg-build install
#   cargo build --release -p zou-pg --bin zou-bootstrap
#   mkdir -p /tmp/ab-<side> && cp -R build/pg/. /tmp/ab-<side>/
#   cp target/release/zou-bootstrap /tmp/ab-<side>/zou-bootstrap
#
# The lib matters and not only the binary: the store code runs inside
# the postmaster, so a cargo build that did not go through
# `ninja install` measures the old one and reports that the change did
# nothing.
#
# Why this exists rather than a run of each side in turn. The write
# numbers in #476 were taken on a box that was also running its owner's
# crawler, and four runs of the same binary against MinIO came back
# 553, 515, 454 and 264 tps. Any pair of single runs taken minutes
# apart on a box like that says whatever the box was doing at the time.
# Alternating the two sides inside a round, swapping which goes first
# each round, and reporting the spread of three rounds is the least
# that makes a difference readable. It does not make a busy box quiet.
# Run this where nothing else is running, and read the spread before
# reading the middle.
set -eu

BEFORE=
AFTER=
LEGS=pg,fs
S3=
ZOU=
RUNS=3
SECONDS_PER=45
CLIENTS=16
PORT=54399
WORK=

while [ $# -gt 0 ]; do
	case $1 in
	--before) BEFORE=$2; shift 2 ;;
	--after) AFTER=$2; shift 2 ;;
	--legs) LEGS=$2; shift 2 ;;
	--s3) S3=$2; shift 2 ;;
	--zou) ZOU=$2; shift 2 ;;
	--runs) RUNS=$2; shift 2 ;;
	--seconds) SECONDS_PER=$2; shift 2 ;;
	--clients) CLIENTS=$2; shift 2 ;;
	--port) PORT=$2; shift 2 ;;
	--work) WORK=$2; shift 2 ;;
	*) echo "unknown option $1" >&2; exit 2 ;;
	esac
done

[ -n "$BEFORE" ] || { echo "--before is required" >&2; exit 2; }
[ -n "$AFTER" ] || { echo "--after is required" >&2; exit 2; }
for side in "$BEFORE" "$AFTER"; do
	[ -x "$side/bin/postgres" ] || { echo "no $side/bin/postgres" >&2; exit 2; }
	[ -x "$side/zou-bootstrap" ] || { echo "no $side/zou-bootstrap" >&2; exit 2; }
done
case ",$LEGS," in
*,s3,*) [ -n "$S3" ] || { echo "--legs s3 needs --s3" >&2; exit 2; } ;;
esac

WORK=${WORK:-$(mktemp -d /tmp/zou-write-ab.XXXXXX)}
mkdir -p "$WORK"
RESULTS=$WORK/results.tsv
: >"$RESULTS"

say() { echo "$(date +%H:%M:%S) $*"; }

# One transaction is 250 rows into a two column table with no index,
# which is the shape the extension lock shows up in and the one #476
# and #478 quote. Written once and read by every leg, so nothing about
# the workload can differ between them.
cat >"$WORK/insert.sql" <<'SQL'
insert into w (a, b) select g, g::text from generate_series(1, 250) g;
SQL

# One measurement, on a postmaster and a data directory and a store
# prefix of its own, so no run inherits another's heap, wal or objects.
#
#   $1 side label, $2 install prefix, $3 leg, $4 round
measure() {
	side=$1
	prefix=$2
	leg=$3
	round=$4
	tag=$leg-$side-$round
	pgdata=$WORK/pgdata-$tag
	sock=$WORK/sock-$tag
	store=$WORK/store-$tag
	log=$WORK/pg-$tag.log
	stats=$WORK/stats-$tag
	rm -rf "$pgdata" "$sock" "$store" "$stats"
	mkdir -p "$sock"

	unset ZOU_TARGET ZOU_TENANT ZOU_PAGESERVE ZOU_STORE_STATS 2>/dev/null || true
	if [ "$leg" != pg ]; then
		if [ "$leg" = fs ]; then
			target=$store
			mkdir -p "$store"
		else
			# A prefix per measurement, since a store that already
			# holds a run's wal is not the store the last one started
			# from.
			target=$S3/$tag
		fi
		ZOU_TARGET=$target
		ZOU_TENANT=local
		# No page service: the object write path is what is under
		# test, and reading through the service changes what the
		# extension path costs. initdb has nothing to read through
		# either way, it is one process on its own.
		ZOU_PAGESERVE=0
		export ZOU_TARGET ZOU_TENANT ZOU_PAGESERVE
	fi
	# Vanilla is the same binary with nothing to point the shim at, so
	# postgres writes its own files and the store is out of it.

	# The same settings all three ways, which is what makes the legs
	# comparable: the question is where the time goes, not which leg
	# was tuned. 6GB of shared buffers is what `zou dev` gives a 23GB
	# box, which is where the numbers being requoted came from.
	#
	# On a store leg this runs through the storage manager, with the
	# environment above already set, because the genesis capture below
	# does not upload relation pages: it uploads the rest of the data
	# directory and takes the pages being there as given. A plain
	# initdb and then a capture leaves a store with a skeleton and no
	# relations in it, and the postmaster starts and the first backend
	# to want pg_authid gets `zou smgr nblocks failed with -1 on rel
	# 1260`, which is what every fs and s3 leg did until this line
	# moved below the export.
	"$prefix"/bin/initdb -D "$pgdata" \
		--set io_method=sync \
		--set full_page_writes=off \
		--set max_wal_size=1GB \
		--set wal_level=logical \
		--set shared_buffers=6GB \
		--set fsync=on >"$WORK/initdb-$tag.log" 2>&1

	if [ "$leg" != pg ]; then
		redo=$("$prefix"/bin/pg_controldata -D "$pgdata" |
			awk '/REDO location/ {print $NF}')
		"$prefix"/zou-bootstrap "$target" "$pgdata" --redo "$redo" \
			>"$WORK/bootstrap-$tag.log" 2>&1
		# Counted from here, so what the stats say is the measurement
		# and not the cluster being made.
		ZOU_STORE_STATS=$stats
		export ZOU_STORE_STATS
	fi

	"$prefix"/bin/pg_ctl -D "$pgdata" -l "$log" \
		-o "-p $PORT -k $sock -c listen_addresses=''" -w -t 300 start >/dev/null
	# shellcheck disable=SC2064
	trap "'$prefix/bin/pg_ctl' -D '$pgdata' -m immediate stop >/dev/null 2>&1 || true" EXIT

	"$prefix"/bin/psql -h "$sock" -p "$PORT" -d postgres -q \
		-c "create table w (a int, b text)" >/dev/null

	out=$("$prefix"/bin/pgbench -h "$sock" -p "$PORT" -d postgres \
		-c "$CLIENTS" -j "$CLIENTS" -T "$SECONDS_PER" -n \
		-f "$WORK/insert.sql" 2>&1) || { echo "$out" >&2; exit 1; }
	tps=$(printf '%s\n' "$out" | awk '/^tps/ {printf "%.1f", $3}')
	lat=$(printf '%s\n' "$out" | awk '/latency average/ {print $4}')

	# Immediate, because a smart shutdown takes a checkpoint and a
	# checkpoint over a remote store is minutes that belong to nothing
	# under measurement.
	"$prefix"/bin/pg_ctl -D "$pgdata" -m immediate -w -t 300 stop >/dev/null 2>&1 || true
	trap - EXIT

	printf '%s\t%s\t%s\t%s\t%s\n' "$leg" "$side" "$round" "$tps" "$lat" >>"$RESULTS"
	say "$leg $side round $round: $tps tps, $lat ms mean"
	# The counter file outlives the store, because the four byte puts
	# in it are the thing #476 said was stable across every run and
	# the store itself is gigabytes nobody reads again.
	if [ -n "$ZOU" ] && [ -f "$stats" ]; then
		"$ZOU" stats "$stats" >"$stats.json" 2>/dev/null || true
	fi
	rm -rf "$pgdata" "$store"
}

say "work $WORK, $RUNS rounds of $LEGS at $CLIENTS clients for ${SECONDS_PER}s each"
round=1
while [ "$round" -le "$RUNS" ]; do
	for leg in $(echo "$LEGS" | tr ',' ' '); do
		if [ "$leg" = pg ]; then
			# Vanilla is a baseline and not a side, so it is measured
			# once a round rather than twice. Which install it comes
			# out of makes no difference to it.
			measure baseline "$AFTER" pg "$round"
			continue
		fi
		# The order swaps every round, so whichever side goes first is
		# not the same side each time. A store that gets faster as a
		# box warms up would otherwise be a difference between sides.
		if [ $((round % 2)) -eq 1 ]; then
			measure before "$BEFORE" "$leg" "$round"
			measure after "$AFTER" "$leg" "$round"
		else
			measure after "$AFTER" "$leg" "$round"
			measure before "$BEFORE" "$leg" "$round"
		fi
	done
	round=$((round + 1))
done

# min, median and max rather than a mean, because three runs of a leg
# that came back 553, 515 and 264 have no mean worth printing and the
# spread is the finding.
middle() {
	sort -n | awk '{v[NR] = $0} END {print v[int((NR + 1) / 2)]}'
}

echo
printf 'leg\tside\truns\ttps min\ttps med\ttps max\tmean latency med\n'
cut -f1,2 "$RESULTS" | awk '!seen[$0]++' | while IFS='	' read -r leg side; do
	tps=$(awk -F'\t' -v l="$leg" -v s="$side" '$1 == l && $2 == s {print $4}' "$RESULTS")
	lat=$(awk -F'\t' -v l="$leg" -v s="$side" '$1 == l && $2 == s {print $5}' "$RESULTS")
	n=$(printf '%s\n' "$tps" | wc -l | tr -d ' ')
	lo=$(printf '%s\n' "$tps" | sort -n | head -1)
	hi=$(printf '%s\n' "$tps" | sort -n | tail -1)
	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
		"$leg" "$side" "$n" "$lo" \
		"$(printf '%s\n' "$tps" | middle)" "$hi" \
		"$(printf '%s\n' "$lat" | middle)"
done

echo
echo "every run is in $RESULTS, logs and counters next to it in $WORK"
