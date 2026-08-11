#!/usr/bin/env bash
# Time a node from exec to its first answered request, and say what it
# spent the time on.
#
# A serverless deployment is judged on the request that arrives when
# nothing is running, and that request pays for three things in order:
# starting the binary, opening the doors, and attaching the project it
# names. This measures all three from outside the process, and prints
# the node's own breakdown of each, which it logs on the way past.
#
# One perl process starts the node and speaks every request over a
# socket, because a process spawn is ten milliseconds and the budget
# being measured is a hundred: a run that shells out to curl measures
# curl.
#
# The store is a directory here, so what this measures is the process
# and not the network. Set SIM to a latency profile to put a distant
# object store in front of it, the way scripts/zou-cold-attach.sh does.
#
# Usage: scripts/zou-cold-start.sh [runs]
# Env overrides: PG_BIN, ZOU_BIN, WORK, SIM, PORT, REF.

set -euo pipefail

RUNS=${1:-3}
PG_BIN=${PG_BIN:-build/pg/bin}
ZOU_BIN=${ZOU_BIN:-target/release}
WORK=${WORK:-/tmp/zou-cold-start}
SIM=${SIM:-}
# A port nothing else has. Asked for rather than picked, because a
# process already listening on 127.0.0.1 answers ahead of a node bound
# to every address, and the run then measures that process.
PORT=${PORT:-$(perl -MIO::Socket::INET -e 'my $s = IO::Socket::INET->new(LocalAddr => "127.0.0.1", Proto => "tcp", Listen => 1); print $s->sockport')}
REF=${REF:-demo}

STORE="$WORK/store"
say() { echo "[cold-start] $*"; }

# The project's anon key, the same HS256 token over the same claims zou
# mints, signed with the secret the registry holds.
anon_key() {
	SECRET="$1" python3 -c '
import base64, hashlib, hmac, json, os, time
def b64(raw): return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()
iat = int(time.time())
head = b64(b"{\"alg\":\"HS256\",\"typ\":\"JWT\"}")
body = b64(json.dumps({"iss": "zou", "role": "anon", "iat": iat, "exp": iat + 315360000}).encode())
sig = b64(hmac.new(os.environ["SECRET"].encode(), f"{head}.{body}".encode(), hashlib.sha256).digest())
print(f"{head}.{body}.{sig}")
'
}

# One run: start the node, wait for the door, ask for the project twice,
# stop it. Prints four milliseconds: listening, routed, attached, warm.
#
# Listening is a connection being taken, which is the binary being up
# and nothing more. Routed is an answer for a ref nobody registered,
# which is a routing decision and a registry lookup and starts no
# database, so it is the door working rather than a project running.
run_once() {
	PORT="$PORT" KEY="$KEY" REF="$REF" LOG="$1" perl -MIO::Socket::INET -MTime::HiRes=time,sleep -e '
	sub bound {
		my ($port) = @_;
		for (1 .. 10000) {
			my $sock = IO::Socket::INET->new(
				PeerAddr => "127.0.0.1", PeerPort => $port, Proto => "tcp", Timeout => 5);
			if ($sock) { close $sock; return time }
			sleep 0.001;
		}
		die "nothing listening on $port\n";
	}
	sub ask {
		my ($port, $path, $key, $tries) = @_;
		for (1 .. $tries) {
			my $sock = IO::Socket::INET->new(
				PeerAddr => "127.0.0.1", PeerPort => $port, Proto => "tcp", Timeout => 5);
			if ($sock) {
				my $auth = $key ? "apikey: $key\r\n" : "";
				print $sock "GET $path HTTP/1.0\r\nHost: 127.0.0.1\r\n${auth}\r\n";
				my $line = <$sock>;
				close $sock;
				return time if defined $line && $line =~ m{^HTTP/};
			}
			sleep 0.002;
		}
		die "no answer on $path\n";
	}
	my ($port, $key, $ref, $log) = @ENV{qw(PORT KEY REF LOG)};
	my $t0 = time;
	my $pid = fork();
	die "fork: $!" unless defined $pid;
	if (!$pid) {
		open(STDOUT, ">", $log) or die $!;
		open(STDERR, ">&", \*STDOUT) or die $!;
		exec(@ARGV) or die "exec: $!";
	}
	my $tb = bound($port);
	my $t1 = ask($port, "/nobody/auth/v1/health", "", 5000);
	my $t2 = ask($port, "/$ref/auth/v1/health", $key, 1);
	my $t3 = ask($port, "/$ref/auth/v1/health", $key, 1);
	kill "TERM", $pid;
	waitpid($pid, 0);
	printf "%.1f %.1f %.1f %.1f\n", ($tb - $t0) * 1000, ($t1 - $tb) * 1000,
		($t2 - $t1) * 1000, ($t3 - $t2) * 1000;
	' "$ZOU_BIN/zou" serve "$STORE" --http "$PORT" --pg 0 --pool 0 \
		--pg-bin "$PG_BIN" --runtime "$WORK/runtime"
}

mkdir -p "$WORK"

# Phase one, once: a registered project with a database behind it. The
# first attach of a fresh ref runs initdb and captures genesis, which is
# a different thing from attaching a project that already exists, so it
# happens here rather than inside a measured run.
if [ ! -d "$STORE" ]; then
	say "building a project in $WORK, initdb and a genesis capture"
	mkdir -p "$STORE"
	"$ZOU_BIN/zou" tenant "$STORE" create "$REF" >"$WORK/create.log" 2>&1
	awk '/^jwt secret /{print $3}' "$WORK/create.log" >"$WORK/secret"
	KEY=$(anon_key "$(cat "$WORK/secret")")
	run_once "$WORK/build.log" >/dev/null
fi

KEY=$(anon_key "$(cat "$WORK/secret")")
# After the build, so the project is made against a plain directory and
# only the measured runs pay the simulated distance.
[ -n "$SIM" ] && export ZOU_STORE_SIM="$SIM"

# Every run attaches the same project. A run appends to the shared log
# and the next attach replays what the last one wrote, so without this
# the fifth run measures a different database than the first.
if [ ! -d "$WORK/store-pristine" ]; then
	cp -R "$STORE" "$WORK/store-pristine"
fi

for run in $(seq 1 "$RUNS"); do
	rm -rf "$WORK/runtime" "$STORE"
	cp -R "$WORK/store-pristine" "$STORE"
	LOG="$WORK/serve-$run.log"
	read -r LISTEN ROUTED ATTACH WARM < <(run_once "$LOG")
	say "run $run: listening $LISTEN ms, routed $ROUTED ms, attached $ATTACH ms, warm request $WARM ms"
	grep -h 'up in \|attached in ' "$LOG" | sed 's/^.*zou::serve\] *//; s/^/[cold-start]   /' || true
done

say "logs in $WORK"
