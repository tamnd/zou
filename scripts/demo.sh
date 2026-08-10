#!/usr/bin/env bash
# The zou demo in two acts. Act one tours the object layer on a local
# directory and needs nothing but a Rust toolchain. Act two runs the
# real patched Postgres against a store, writes rows, branches the
# database, and restarts from nothing but the store. Act two needs the
# vendored build, `make pg-build`, and is skipped with a pointer when
# it is missing. See docs/quickstart.md for the full walkthrough.
set -eu

cd "$(dirname "$0")/.."

PORT="${ZOU_DEMO_PORT:-5488}"
PG_BIN="${ZOU_PG_BIN:-build/pg/bin}"

say() { printf '\n== %s\n' "$*"; }

say "act one: the object layer on a local directory"
cargo run --quiet -p zou-store --example demo

if [ ! -x "$PG_BIN/postgres" ]; then
    say "act two skipped: no patched postgres at $PG_BIN"
    echo "run 'make pg-build' once (the longest part of the ten minutes),"
    echo "then 'make demo' again for the full tour: postgres on a store,"
    echo "a branch, and a restart from nothing but the objects."
    exit 0
fi

say "act two: postgres with every page and WAL byte on a store"
cargo build --quiet --release -p zou

DEMO_DIR="$(mktemp -d "${TMPDIR:-/tmp}/zou-demo.XXXXXX")"
STORE="$DEMO_DIR/store"
ZOU=target/release/zou
# The cluster superuser is postgres wherever a project is hosted, so
# the connection names it rather than taking the OS user.
PSQL() { "$PG_BIN/psql" -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -X -q "$@"; }

DEV_PID=""
stop_dev() {
    if [ -n "$DEV_PID" ] && kill -0 "$DEV_PID" 2>/dev/null; then
        kill -INT "$DEV_PID"
        wait "$DEV_PID" 2>/dev/null || true
    fi
    DEV_PID=""
}
trap stop_dev EXIT

start_dev() {
    "$ZOU" dev "$STORE" --pg-bin "$PG_BIN" --port "$PORT" --runtime "$1" \
        >"$DEMO_DIR/$(basename "$1").log" 2>&1 &
    DEV_PID=$!
    for _ in $(seq 1 60); do
        if PSQL -c 'select 1' >/dev/null 2>&1; then return 0; fi
        if ! kill -0 "$DEV_PID" 2>/dev/null; then
            echo "zou dev exited early, log follows"
            cat "$DEMO_DIR/$(basename "$1").log"
            exit 1
        fi
        sleep 1
    done
    echo "postgres did not come up on port $PORT"
    exit 1
}

echo "store: $STORE"
start_dev "$DEMO_DIR/run1"

say "writing rows through plain SQL"
PSQL -c "create table guestbook(id serial primary key, note text)"
PSQL -c "insert into guestbook(note) values ('hello from zou'), ('pages live on the store'), ('so does the WAL')"
PSQL -c "table guestbook"

say "stopping postgres, the store keeps everything"
stop_dev

say "what the store holds"
"$ZOU" info "$STORE"

say "branching the database, one small manifest, no data copied"
"$ZOU" branch "$STORE" local time-travel
"$ZOU" info "$STORE" time-travel

say "restarting from nothing but the store"
start_dev "$DEMO_DIR/run2"
PSQL -c "table guestbook"
stop_dev

say "done"
echo "the store at $STORE is a plain directory of objects, look around:"
echo "  find $STORE -type f | head"
echo "the same demo runs on s3://bucket/prefix, a single .zou file for"
echo "the sequential tools, or sqlite://path.db, see docs/quickstart.md."
