#!/usr/bin/env bash
# Run `zou lambda` against a runtime api that is not AWS.
#
# The adapter is a loop around two http calls, so the whole of it can be
# proved on a laptop by being the other end of those calls: a small
# server that hands out invocations and collects the answers, and the
# real binary talking to it with AWS_LAMBDA_RUNTIME_API pointed here.
#
# What this shows is the part a unit test cannot: that a function url
# event reaches a real attached project and comes back as an answer an
# api gateway would accept, and that the project was up before the first
# invocation arrived rather than because of it.
#
# Two environments, because the first attach of a project that has never
# run is an initdb and a genesis capture, and the number worth looking at
# is the second one, which is what every cold start after the first one
# in the world does.
#
# Usage: scripts/zou-lambda-smoke.sh
# Env overrides: PG_BIN, ZOU_BIN, WORK, REF.

set -euo pipefail

PG_BIN=${PG_BIN:-build/pg/bin}
ZOU_BIN=${ZOU_BIN:-target/release}
WORK=${WORK:-/tmp/zou-lambda-smoke}
REF=${REF:-demo}

STORE="$WORK/store"
say() { echo "[lambda] $*"; }

rm -rf "$WORK"
mkdir -p "$WORK"

say "making a project in $STORE"
"$ZOU_BIN/zou" tenant "$STORE" create "$REF" >"$WORK/create.log" 2>&1
SECRET=$(awk '/^jwt secret /{print $3}' "$WORK/create.log")

# The project's anon key, the same HS256 token over the same claims zou
# mints, signed with the secret the registry holds.
KEY=$(SECRET="$SECRET" python3 -c '
import base64, hashlib, hmac, json, os, time
def b64(raw): return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()
iat = int(time.time())
head = b64(b"{\"alg\":\"HS256\",\"typ\":\"JWT\"}")
body = b64(json.dumps({"iss": "zou", "role": "anon", "iat": iat, "exp": iat + 315360000}).encode())
sig = b64(hmac.new(os.environ["SECRET"].encode(), f"{head}.{body}".encode(), hashlib.sha256).digest())
print(f"{head}.{body}.{sig}")
')

# The fake runtime api. It answers /invocation/next with one event at a
# time and writes every answer it is posted into the round's directory,
# which is the whole contract the adapter is written against.
runtime_api() {
	KEY="$KEY" ROUND="$1" EVENTS="$2" python3 - <<'PY' &
import http.server, json, os, threading, time

round, key = os.environ["ROUND"], os.environ["KEY"]
wanted = os.environ["EVENTS"].split()

def url_event(method, path, body=None):
    event = {
        "version": "2.0",
        "rawPath": path,
        "rawQueryString": "",
        "headers": {"host": "x.lambda-url.eu-west-1.on.aws", "apikey": key},
        "requestContext": {"http": {"method": method, "path": path}},
    }
    if body is not None:
        event["headers"]["content-type"] = "application/json"
        event["body"] = json.dumps(body)
        event["isBase64Encoded"] = False
    return event

catalogue = {
    "health": url_event("GET", "/auth/v1/health"),
    "signup": url_event(
        "POST", "/auth/v1/signup",
        {"email": "a@example.com", "password": "a-long-enough-password"},
    ),
    "rest": url_event("GET", "/rest/v1/"),
    # The same write again, because the first one a project ever takes
    # pays for the wal chain the genesis capture left behind and the
    # second one is what a signup costs from then on.
    "again": url_event(
        "POST", "/auth/v1/signup",
        {"email": "b@example.com", "password": "a-long-enough-password"},
    ),
}
events = [(name, catalogue[name]) for name in wanted]
# When each event was handed out, so the round can say what an
# invocation cost from where the caller stands rather than from inside
# the function's own logs.
handed = {}
done = threading.Event()

class Runtime(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        if not self.path.endswith("/invocation/next"):
            self.send_error(404)
            return
        if not events:
            # A real runtime api holds this open until the environment
            # is thawed for the next event, so this one does too, and
            # the round ends by the function being asked to stop.
            done.wait(300)
            self.send_error(404)
            return
        name, event = events.pop(0)
        handed[name] = time.monotonic()
        body = json.dumps(event).encode()
        self.send_response(200)
        self.send_header("Lambda-Runtime-Aws-Request-Id", name)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        body = self.rfile.read(length)
        which = self.path.rstrip("/").split("/")[-1]
        name = self.path.split("/")[-2]
        if name in handed:
            took = (time.monotonic() - handed[name]) * 1000
            with open(f"{round}/took-{name}", "w") as f:
                f.write(f"{took:.1f}")
        with open(f"{round}/{which}-{name}.json", "wb") as f:
            f.write(body)
        self.send_response(202)
        self.send_header("Content-Length", "0")
        self.end_headers()

# Threaded because the last invocation.next is held open, and an
# answer to an earlier one should not be queued behind it.
server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Runtime)
with open(f"{round}/port", "w") as f:
    f.write(str(server.server_port))
threading.Thread(target=server.serve_forever, daemon=True).start()
done.wait(300)
PY
	API_PID=$!
}

# One environment: a runtime api, a function talking to it, and both
# gone by the time this returns.
round() {
	local name="$1" events="$2" last="$3"
	local dir="$WORK/$name"
	mkdir -p "$dir"
	runtime_api "$dir" "$events"
	for _ in $(seq 1 200); do
		[ -s "$dir/port" ] && break
		sleep 0.05
	done
	local port
	port=$(cat "$dir/port")

	# Store and project from the environment and not the command line,
	# because that is how a function is configured and the recipes in
	# docs/serverless.md set exactly these two.
	AWS_LAMBDA_RUNTIME_API="127.0.0.1:$port" RUST_LOG=${RUST_LOG:-info} \
		ZOU_TARGET="$STORE" ZOU_REF="$REF" \
		"$ZOU_BIN/zou" lambda \
		--pg-bin "$PG_BIN" --runtime "$dir/runtime" >"$dir/function.log" 2>&1 &
	FN_PID=$!
	for _ in $(seq 1 900); do
		[ -f "$dir/response-$last.json" ] && break
		kill -0 "$FN_PID" 2>/dev/null || break
		sleep 0.1
	done
	sleep 0.5
	kill "$FN_PID" 2>/dev/null || true
	wait "$FN_PID" 2>/dev/null || true
	kill "$API_PID" 2>/dev/null || true
	wait "$API_PID" 2>/dev/null || true
}

trap 'kill "$API_PID" "$FN_PID" 2>/dev/null || true' EXIT

say "first environment: the project has never run, so this one initdbs it"
round first health health
grep -h 'attached in ' "$WORK/first/function.log" | sed 's/^/[lambda]   /' || true

say "second environment: a cold start of a project that exists"
round second "health signup rest again" again
grep -h 'up in \|attached in ' "$WORK/second/function.log" | sed 's/^/[lambda]   /' || true

fail=0
for name in health signup rest again; do
	file="$WORK/second/response-$name.json"
	if [ ! -f "$file" ]; then
		say "no answer for $name"
		fail=1
		continue
	fi
	status=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["statusCode"])' "$file")
	body=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["body"][:90])' "$file")
	say "$name: $status in $(cat "$WORK/second/took-$name") ms, $body"
	[ "$status" = 200 ] || fail=1
done

if [ -n "$(ls "$WORK"/*/error-* 2>/dev/null)" ]; then
	say "the function reported errors:"
	cat "$WORK"/*/error-*
	fail=1
fi

[ "$fail" = 0 ] && say "ok, four invocations answered through the runtime api"
exit "$fail"
