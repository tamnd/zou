#!/usr/bin/env bash
# What one project on Lambda and S3 costs a month, from measured ops
# rather than from a guess.
#
# The bill has two halves and only one of them is about time. Lambda
# charges for GB seconds and for requests, which is what the function
# spends answering, and S3 charges per op, which is what the database
# underneath does while it answers: a put is a put whether it carried
# eight kilobytes or eight megabytes. So this runs a real project
# through the real Lambda adapter, reads the store's own op counters
# either side of each phase, and turns both into money.
#
# What it measures, per invocation:
#
#   - a cold start, exec to the first answer, which is what a container
#     image function is billed for at the front of an environment
#   - a read of twenty rows through /rest/v1/, warm
#   - a write of one row through /rest/v1/, warm
#   - the store gets and puts each of those cost
#   - the log bytes each of those write, because CloudWatch charges to
#     ingest them and a chatty RUST_LOG is a line on the bill
#
# Then it prices a month of a traffic profile you give it. The rates
# are the published on demand ones and they are in one block below, so
# --rates replaces them with a file when they change or when the region
# is not one of the cheap ones.
#
# Real numbers from a deployment that already exists go in as flags
# instead of being measured: --invocations-per-day, --avg-duration-ms,
# --log-mb-per-day and --store-gb. The aws commands that print them are
# at the bottom of the output, because reading them out of CloudWatch is
# three calls and this script has no business holding your credentials.
#
# Usage: scripts/zou-lambda-cost.sh [flags]
#   --reads-per-day N        default 100000
#   --writes-per-day N       default 10000
#   --cold-starts-per-day N  default 96, one every fifteen minutes
#   --memory-mb N            default 2048, matching the terraform example
#   --samples N              invocations per measured phase, default 20
#   --rates FILE             json replacing the rate block
#   --invocations-per-day N  skip measuring, price these instead
#   --avg-duration-ms N      with the above
#   --log-mb-per-day N       with the above
#   --store-gb N             with the above
# Env overrides: PG_BIN, ZOU_BIN, WORK, REF.

set -euo pipefail

PG_BIN=${PG_BIN:-build/pg/bin}
ZOU_BIN=${ZOU_BIN:-target/release}
WORK=${WORK:-/tmp/zou-lambda-cost}
REF=${REF:-demo}
# Two ports for the two starts below, and nothing listens on either
# during the measured run itself, because a function has no port at all.
HTTP_PORT=${HTTP_PORT:-54871}
PG_PORT=${PG_PORT:-54872}

READS_PER_DAY=100000
WRITES_PER_DAY=10000
COLD_STARTS_PER_DAY=96
MEMORY_MB=2048
SAMPLES=20
RATES=
INVOCATIONS_PER_DAY=
AVG_DURATION_MS=
LOG_MB_PER_DAY=
STORE_GB=

while [ $# -gt 0 ]; do
	case "$1" in
	--reads-per-day) READS_PER_DAY=$2 ;;
	--writes-per-day) WRITES_PER_DAY=$2 ;;
	--cold-starts-per-day) COLD_STARTS_PER_DAY=$2 ;;
	--memory-mb) MEMORY_MB=$2 ;;
	--samples) SAMPLES=$2 ;;
	--rates) RATES=$2 ;;
	--invocations-per-day) INVOCATIONS_PER_DAY=$2 ;;
	--avg-duration-ms) AVG_DURATION_MS=$2 ;;
	--log-mb-per-day) LOG_MB_PER_DAY=$2 ;;
	--store-gb) STORE_GB=$2 ;;
	*)
		echo "unknown flag $1" >&2
		exit 2
		;;
	esac
	shift 2
done

say() { echo "[cost] $*"; }

MEASURED="$WORK/measured.json"

# Measuring needs the patched postgres, because the thing being
# measured is postgres in a function. Without it the pricing half still
# works on numbers handed in, which is the case for a deployment that is
# already running somewhere.
if [ -z "$INVOCATIONS_PER_DAY" ] && [ ! -x "$PG_BIN/postgres" ]; then
	say "no patched postgres at $PG_BIN, so there is nothing to measure"
	say "run 'make pg-build' first, or hand in real numbers from a deployment:"
	say "  scripts/zou-lambda-cost.sh --invocations-per-day 4000 --avg-duration-ms 12 \\"
	say "      --log-mb-per-day 40 --store-gb 2"
	exit 1
fi

if [ -z "$INVOCATIONS_PER_DAY" ]; then
	rm -rf "$WORK"
	mkdir -p "$WORK"
	STORE="$WORK/store"
	STATS="$WORK/store-stats"
	export ZOU_STORE_STATS="$STATS"

	say "making a project in $STORE"
	"$ZOU_BIN/zou" tenant "$STORE" create "$REF" >"$WORK/create.log" 2>&1
	eval "$("$ZOU_BIN/zou" tenant "$STORE" keys "$REF" --env)"
	# The dev loop mints its own secret when nothing pins one, and the
	# keys above are the registry's, which is what the function will
	# check tokens against.
	ZOU_JWT_SECRET=$(awk '/^jwt secret /{print $3}' "$WORK/create.log")
	export ZOU_JWT_SECRET

	# Two starts, because they are two different jobs. The first one
	# makes the database, which is an initdb and a genesis capture and
	# is exactly what the recipe says to do once from a laptop rather
	# than in a function. The second one is the migration, and a
	# migration is superuser SQL on a port: the pg door a served project
	# answers on is the pooler's shape, where the database names the
	# project and the password is an api key, and no api key is a
	# superuser.
	say "making the database, which is the initdb a function should never do"
	"$ZOU_BIN/zou" serve "$STORE" --ref "$REF" --http "$HTTP_PORT" --pg 0 --pool 0 \
		--pg-bin "$PG_BIN" >"$WORK/serve.log" 2>&1 &
	SERVE_PID=$!
	trap 'kill "$SERVE_PID" 2>/dev/null || true' EXIT
	for _ in $(seq 1 900); do
		grep -q "attached in " "$WORK/serve.log" && break
		kill -0 "$SERVE_PID" 2>/dev/null || break
		sleep 0.2
	done
	# INT rather than TERM, and waited for: a stop puts the writer lease
	# back, and whatever starts next would otherwise wait out the rest of
	# it before it could write.
	kill -INT "$SERVE_PID" 2>/dev/null || true
	wait "$SERVE_PID" 2>/dev/null || true

	say "the schema an app has, and the caches a function should not pay for"
	"$ZOU_BIN/zou" dev "$STORE" --ref "$REF" --http "$HTTP_PORT" --port "$PG_PORT" --no-config \
		--pg-bin "$PG_BIN" --runtime "$WORK/dev" >"$WORK/dev.log" 2>&1 &
	SERVE_PID=$!
	for _ in $(seq 1 900); do
		"$PG_BIN/psql" -h 127.0.0.1 -p "$PG_PORT" -U postgres -d postgres -Atqc "select 1" \
			>/dev/null 2>&1 && break
		kill -0 "$SERVE_PID" 2>/dev/null || break
		sleep 0.2
	done
	# Asked before the table is made, because the three api roles are
	# created by the first request that reaches SQL and the grants below
	# are about them. A project that has served one request has them, and
	# every project this recipe describes has served one by now.
	curl -fsS -H "apikey: $SERVICE_ROLE_KEY" -H "authorization: Bearer $SERVICE_ROLE_KEY" \
		"http://127.0.0.1:$HTTP_PORT/rest/v1/" >/dev/null
	"$PG_BIN/psql" -h 127.0.0.1 -p "$PG_PORT" -U postgres -d postgres -v ON_ERROR_STOP=1 -Atq \
		>"$WORK/schema.log" 2>&1 <<-'SQL'
		create table if not exists notes (
		    id bigserial primary key,
		    body text not null,
		    created_at timestamptz not null default now()
		);
		insert into notes (body)
		    select 'seed row ' || g from generate_series(1, 200) g;
		grant all on notes to anon, authenticated, service_role;
		grant all on notes_id_seq to anon, authenticated, service_role;
	SQL
	# The rest surface caches the schema and the first request that needs
	# the auth schema installs it, both once in a project's life. Ask for
	# them here, so the warm numbers below are warm and the cold start
	# below is a cold start of a project that has been used.
	curl -fsS -H "apikey: $SERVICE_ROLE_KEY" -H "authorization: Bearer $SERVICE_ROLE_KEY" \
		"http://127.0.0.1:$HTTP_PORT/rest/v1/notes?select=id&limit=1" >/dev/null
	curl -fsS -H "apikey: $ANON_KEY" \
		"http://127.0.0.1:$HTTP_PORT/auth/v1/health" >/dev/null
	kill -INT "$SERVE_PID" 2>/dev/null || true
	wait "$SERVE_PID" 2>/dev/null || true
	trap - EXIT

	# The measured run. One environment, one health invocation, then
	# reads and then writes, which is the order an app arrives in and
	# the order that keeps each phase's store ops its own.
	ROUND="$WORK/round"
	FN_LOG="$WORK/round/function.log"
	mkdir -p "$ROUND"
	T0=$(python3 -c 'import time; print(time.time())')

	KEY="$SERVICE_ROLE_KEY" ROUND="$ROUND" SAMPLES="$SAMPLES" \
		STATS="$STATS" FN_LOG="$FN_LOG" T0="$T0" python3 - <<'PY' &
import http.server, json, os, shutil, threading, time

round, key = os.environ["ROUND"], os.environ["KEY"]
samples = int(os.environ["SAMPLES"])
stats, fn_log, t0 = os.environ["STATS"], os.environ["FN_LOG"], float(os.environ["T0"])

def event(method, path, body=None):
    e = {
        "version": "2.0",
        "rawPath": path.split("?")[0],
        "rawQueryString": path.split("?")[1] if "?" in path else "",
        "headers": {
            "host": "x.execute-api.eu-west-1.amazonaws.com",
            "apikey": key,
            "authorization": f"Bearer {key}",
        },
        "requestContext": {"http": {"method": method, "path": path.split("?")[0]}},
    }
    if body is not None:
        e["headers"]["content-type"] = "application/json"
        e["headers"]["prefer"] = "return=minimal"
        e["body"] = json.dumps(body)
        e["isBase64Encoded"] = False
    return e

# The phases, in the order they are handed out. cold is one health
# check and it carries the environment's whole start with it, because a
# container image function is billed for its init.
#
# first is the read after it, on its own, because the pages a query
# touches are faulted in from the store the first time it is asked and
# are in memory every time after. Averaging that over the reads that
# follow would spread one environment's warm up across all of them and
# price every read as if it were the first.
plan = [("cold", event("GET", "/auth/v1/health"))]
plan += [("first", event("GET", "/rest/v1/notes?select=id,body&limit=20"))]
plan += [("read", event("GET", "/rest/v1/notes?select=id,body&limit=20"))] * samples
plan += [
    ("write", event("POST", "/rest/v1/notes", {"body": f"written by the cost run {i}"}))
    for i in range(samples)
]

pending = list(enumerate(plan))
handed, results = {}, []
done = threading.Event()

def snapshot(name):
    # A cold copy of the mapped counter file, which is safe while the
    # store is live, and the size of what the function has said so far.
    shutil.copyfile(stats, f"{round}/stats-{name}")
    size = os.path.getsize(fn_log) if os.path.exists(fn_log) else 0
    with open(f"{round}/log-{name}", "w") as f:
        f.write(str(size))

class Runtime(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        if not self.path.endswith("/invocation/next"):
            self.send_error(404)
            return
        if not pending:
            done.wait(300)
            self.send_error(404)
            return
        i, (phase, e) = pending.pop(0)
        handed[str(i)] = time.time()
        body = json.dumps(e).encode()
        self.send_response(200)
        self.send_header("Lambda-Runtime-Aws-Request-Id", str(i))
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        body = self.rfile.read(length)
        i = self.path.split("/")[-2]
        now = time.time()
        answer = json.loads(body) if body else {}
        phase = plan[int(i)][0]
        # A cold start is billed from exec, everything else from when
        # the invocation was handed out.
        started = t0 if phase == "cold" else handed[i]
        results.append({
            "phase": phase,
            "status": answer.get("statusCode"),
            "ms": (now - started) * 1000,
            "out_bytes": len(answer.get("body") or ""),
        })
        # The last invocation of a phase is where that phase's counters
        # get read, so each delta below is one phase and nothing else.
        following = plan[int(i) + 1][0] if int(i) + 1 < len(plan) else None
        if following != phase:
            snapshot(phase)
        self.send_response(202)
        self.send_header("Content-Length", "0")
        self.end_headers()
        if len(results) == len(plan):
            with open(f"{round}/results.json", "w") as f:
                json.dump(results, f)
            done.set()

snapshot("start")
server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Runtime)
with open(f"{round}/port", "w") as f:
    f.write(str(server.server_port))
threading.Thread(target=server.serve_forever, daemon=True).start()
done.wait(600)
PY
	API_PID=$!
	trap 'kill "$API_PID" "${FN_PID:-}" 2>/dev/null || true' EXIT
	for _ in $(seq 1 200); do
		[ -s "$ROUND/port" ] && break
		sleep 0.05
	done
	PORT=$(cat "$ROUND/port")

	say "one environment, 1 cold start then $SAMPLES reads and $SAMPLES writes"
	AWS_LAMBDA_RUNTIME_API="127.0.0.1:$PORT" RUST_LOG=${RUST_LOG:-info} \
		ZOU_TARGET="$STORE" ZOU_REF="$REF" \
		"$ZOU_BIN/zou" lambda --pg-bin "$PG_BIN" --runtime "$ROUND/runtime" \
		>"$ROUND/function.log" 2>&1 &
	FN_PID=$!
	for _ in $(seq 1 1800); do
		[ -f "$ROUND/results.json" ] && break
		kill -0 "$FN_PID" 2>/dev/null || break
		sleep 0.2
	done
	sleep 0.5
	kill "$FN_PID" "$API_PID" 2>/dev/null || true
	wait "$FN_PID" 2>/dev/null || true
	wait "$API_PID" 2>/dev/null || true
	trap - EXIT

	if [ ! -f "$ROUND/results.json" ]; then
		say "the function answered nothing, its log is $ROUND/function.log"
		tail -20 "$ROUND/function.log" || true
		exit 1
	fi

	# The store as it stands, which is what S3 charges rent on.
	for name in start cold first read write; do
		[ -f "$ROUND/stats-$name" ] &&
			"$ZOU_BIN/zou" stats "$ROUND/stats-$name" >"$ROUND/ops-$name.json"
	done
	du -sk "$STORE" | awk '{print $1}' >"$ROUND/store-kb"
	printf '%s\n' "$ROUND" >"$WORK/round-path"
fi

# The pricing. Everything above produced numbers, this turns them into
# a bill, and it is the only part that runs when the numbers were handed
# in rather than measured.
ROUND=${ROUND:-}
READS_PER_DAY="$READS_PER_DAY" WRITES_PER_DAY="$WRITES_PER_DAY" \
	COLD_STARTS_PER_DAY="$COLD_STARTS_PER_DAY" MEMORY_MB="$MEMORY_MB" \
	ROUND="$ROUND" RATES="$RATES" MEASURED="$MEASURED" \
	INVOCATIONS_PER_DAY="$INVOCATIONS_PER_DAY" AVG_DURATION_MS="$AVG_DURATION_MS" \
	LOG_MB_PER_DAY="$LOG_MB_PER_DAY" STORE_GB="$STORE_GB" python3 - <<'PY'
import json, math, os

# The published on demand rates, eu-west-1 and us-east-1, which agree on
# all of these. Written down rather than fetched, because a script that
# needs the network to say what a put costs is a script that cannot run
# on a plane, and --rates takes a json file with any of these keys when
# they move or when the region is a dearer one.
RATES = {
    "lambda_gb_second_arm64": 0.0000133334,
    "lambda_request": 0.20 / 1_000_000,
    "apigw_http_request": 1.00 / 1_000_000,
    "s3_put_per_1000": 0.005,
    "s3_get_per_1000": 0.0004,
    "s3_storage_gb_month": 0.023,
    "logs_ingest_gb": 0.50,
    "logs_storage_gb_month": 0.03,
    "egress_gb": 0.09,
    "egress_free_gb": 100.0,
}
if os.environ.get("RATES"):
    RATES.update(json.load(open(os.environ["RATES"])))

DAYS = 30.4
reads = float(os.environ["READS_PER_DAY"])
writes = float(os.environ["WRITES_PER_DAY"])
colds = float(os.environ["COLD_STARTS_PER_DAY"])
memory_gb = float(os.environ["MEMORY_MB"]) / 1024

def say(line):
    print(f"[cost] {line}")

def money(v):
    return f"${v:,.2f}"

round_dir = os.environ.get("ROUND") or ""

if round_dir:
    results = json.load(open(f"{round_dir}/results.json"))

    def phase(name):
        return [r for r in results if r["phase"] == name]

    def ops(name):
        path = f"{round_dir}/ops-{name}.json"
        if not os.path.exists(path):
            return {}
        snap = json.load(open(path))
        return {op["op"]: (op["count"], op["bytes"]) for op in snap["ops"]}

    # A phase costs what the counters moved by while it ran, divided by
    # how many invocations it was. get_range is a get and is billed as
    # one, put_if_match is a put, and a delete is free everywhere so it
    # is counted and never priced.
    def delta(before, after):
        a, b = ops(before), ops(after)
        gets = sum(b.get(k, (0, 0))[0] - a.get(k, (0, 0))[0] for k in ("get", "get_range"))
        puts = sum(b.get(k, (0, 0))[0] - a.get(k, (0, 0))[0] for k in ("put_if_match", "put"))
        return gets, puts

    def log_bytes(before, after):
        def size(name):
            path = f"{round_dir}/log-{name}"
            return int(open(path).read()) if os.path.exists(path) else 0
        return size(after) - size(before)

    cold = phase("cold")
    first = phase("first")
    read = phase("read")
    write = phase("write")
    bad = [r for r in results if r["status"] not in (200, 201, 204)]
    if bad:
        say(f"{len(bad)} of {len(results)} invocations did not answer 2xx, the numbers below are not a measurement")
        for r in bad[:3]:
            say(f"  {r['phase']} answered {r['status']}")
        raise SystemExit(1)

    def mean(rows, key):
        return sum(r[key] for r in rows) / len(rows) if rows else 0.0

    cold_ms = mean(cold, "ms")
    first_ms = mean(first, "ms")
    read_ms = mean(read, "ms")
    write_ms = mean(write, "ms")
    cold_gets, cold_puts = delta("start", "cold")
    first_gets, first_puts = delta("cold", "first")
    read_gets, read_puts = delta("first", "read")
    write_gets, write_puts = delta("read", "write")
    read_gets, read_puts = read_gets / max(len(read), 1), read_puts / max(len(read), 1)
    write_gets, write_puts = write_gets / max(len(write), 1), write_puts / max(len(write), 1)
    cold_log = log_bytes("start", "cold")
    first_log = log_bytes("cold", "first")
    read_log = log_bytes("first", "read") / max(len(read), 1)
    write_log = log_bytes("read", "write") / max(len(write), 1)
    first_out = mean(first, "out_bytes")
    read_out = mean(read, "out_bytes")
    write_out = mean(write, "out_bytes")
    store_gb = int(open(f"{round_dir}/store-kb").read()) / 1024 / 1024

    say("measured here, one environment, a store on the local disk:")
    say(f"  cold start, exec to the first answer   {cold_ms:8.1f} ms   {cold_gets:5.0f} gets {cold_puts:5.0f} puts")
    say(f"  first read of a fresh environment      {first_ms:8.1f} ms   {first_gets:5.0f} gets {first_puts:5.0f} puts")
    say(f"  read, twenty rows through /rest/v1/    {read_ms:8.1f} ms   {read_gets:5.2f} gets {read_puts:5.2f} puts")
    say(f"  write, one row through /rest/v1/       {write_ms:8.1f} ms   {write_gets:5.2f} gets {write_puts:5.2f} puts")
    say(f"  the project on the store is {store_gb * 1024:.0f} MB after {len(write)} writes")
    say("")
    say("a store on a disk answers faster than S3 does, so the milliseconds")
    say("above are the compute and not the wire. docs/benchmarks.md has the")
    say("same shape with S3 latency simulated, and the op counts are the same")
    say("either way, which is the half of this that decides the bill.")
    say("")
else:
    invocations = float(os.environ["INVOCATIONS_PER_DAY"])
    cold_ms = first_ms = read_ms = write_ms = float(os.environ["AVG_DURATION_MS"])
    colds = 0.0
    reads, writes = invocations, 0.0
    read_gets = read_puts = write_gets = write_puts = 0.0
    first_gets = first_puts = first_log = 0
    cold_gets = cold_puts = cold_log = 0
    read_log = write_log = float(os.environ["LOG_MB_PER_DAY"]) * 1024 * 1024 / max(invocations, 1)
    first_out = read_out = write_out = 0.0
    store_gb = float(os.environ["STORE_GB"])
    say(f"pricing {invocations:,.0f} invocations a day at {cold_ms:.1f} ms each, handed in rather than measured")
    say("")

# Lambda bills a whole millisecond, so a half millisecond answer is a
# millisecond, which matters when the answer is that fast.
def gb_seconds(ms, n):
    return math.ceil(ms) / 1000 * memory_gb * n

# One read of every cold environment is a first read, and the rest are
# warm. A day with more cold starts than reads is a day of environments
# that woke up for a write, so the split never goes negative.
firsts = min(colds, reads)
warm_reads = reads - firsts

invocations_month = (reads + writes + colds) * DAYS
duration = (
    gb_seconds(cold_ms, colds * DAYS)
    + gb_seconds(first_ms, firsts * DAYS)
    + gb_seconds(read_ms, warm_reads * DAYS)
    + gb_seconds(write_ms, writes * DAYS)
) * RATES["lambda_gb_second_arm64"]
requests = invocations_month * RATES["lambda_request"]
gateway = invocations_month * RATES["apigw_http_request"]

gets_month = (
    cold_gets * colds + first_gets * firsts + read_gets * warm_reads + write_gets * writes
) * DAYS
puts_month = (
    cold_puts * colds + first_puts * firsts + read_puts * warm_reads + write_puts * writes
) * DAYS
s3_gets = gets_month / 1000 * RATES["s3_get_per_1000"]
s3_puts = puts_month / 1000 * RATES["s3_put_per_1000"]
s3_storage = store_gb * RATES["s3_storage_gb_month"]

log_gb = (
    cold_log * colds + first_log * firsts + read_log * warm_reads + write_log * writes
) * DAYS / 1024**3
logs = log_gb * RATES["logs_ingest_gb"] + log_gb * RATES["logs_storage_gb_month"]

egress_gb = (first_out * firsts + read_out * warm_reads + write_out * writes) * DAYS / 1024**3
egress = max(0.0, egress_gb - RATES["egress_free_gb"]) * RATES["egress_gb"]

lines = [
    ("lambda duration", duration),
    ("lambda requests", requests),
    ("api gateway", gateway),
    (f"s3 puts ({puts_month:,.0f})", s3_puts),
    (f"s3 gets ({gets_month:,.0f})", s3_gets),
    (f"s3 storage ({store_gb:.2f} GB)", s3_storage),
    (f"cloudwatch logs ({log_gb:.2f} GB)", logs),
    (f"egress ({egress_gb:.2f} GB)", egress),
]
total = sum(v for _, v in lines)

if round_dir:
    say(f"a month at {reads:,.0f} reads, {writes:,.0f} writes and {colds:,.0f} cold starts a day,")
    say(f"arm64 at {os.environ['MEMORY_MB']} MB, {invocations_month:,.0f} invocations:")
for label, value in lines:
    say(f"  {label:<34} {money(value):>10}")
say(f"  {'total':<34} {money(total):>10}")
say("")
say(f"idle, which is the storage line on its own, is {money(s3_storage)} a month.")
say("")
say("the same numbers from a deployment that already exists:")
say("  aws cloudwatch get-metric-statistics --namespace AWS/Lambda --metric-name Invocations \\")
say("      --dimensions Name=FunctionName,Value=zou-demo --statistics Sum \\")
say("      --start-time $(date -u -v-30d +%Y-%m-%dT%H:%M:%SZ) --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) --period 2592000")
say("  aws cloudwatch get-metric-statistics --namespace AWS/Lambda --metric-name Duration ... --statistics Average")
say("  aws s3 ls s3://my-zou-bucket/projects --recursive --summarize | tail -2")
say("then hand them back in with --invocations-per-day, --avg-duration-ms, --log-mb-per-day and --store-gb.")
PY
