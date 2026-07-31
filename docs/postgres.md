# Vendored Postgres

zou ships its own Postgres build.
The source lives in `vendor/postgres` as a shallow git submodule pinned to the `REL_18_4` release commit, and zou's modifications are a patch series in `patches/`, applied on top by `scripts/pg-apply-patches.sh`.

## Why a submodule plus patches, not a fork branch

The patch series keeps the delta against upstream visible and reviewable as a handful of files in this repo.
Moving to a new Postgres minor is a one line submodule bump plus a series re-apply, not a rebase of a private branch that only exists on some contributor's remote.
And a fresh clone of this repo contains everything needed to reproduce the exact server binary, which is the property the fault injection and soak results depend on.

## Building

Prerequisites: meson, ninja, a C compiler, flex, bison, and the usual Postgres libraries (readline, zlib, icu).

- Debian and Ubuntu: `apt-get install meson ninja-build flex bison libreadline-dev zlib1g-dev libicu-dev pkg-config`
- macOS: `brew install meson ninja icu4c pkg-config`

Then:

```sh
make pg-build
```

That runs three steps, each also available on its own:

- `make pg-init` fetches the pinned source, shallow.
- `make pg-patch` resets the tree to the pinned commit and applies the series in filename order. It refuses to run if the submodule has uncommitted edits, so patch authoring work cannot be silently destroyed.
- `make pg-build` does an out of tree meson build into `build/pg-build` and installs into `build/pg`, so the submodule working tree stays clean.

Smoke test the result:

```sh
build/pg/bin/initdb -D /tmp/zou-pgdata
build/pg/bin/pg_ctl -D /tmp/zou-pgdata -l /tmp/zou-pg.log start
build/pg/bin/psql -h /tmp -d postgres -c 'select version()'
build/pg/bin/pg_ctl -D /tmp/zou-pgdata stop
```

## The zou storage manager

Patch `0001-zou-smgr.patch` adds a second entry to the smgr table that routes relation pages to zou-store through the C ABI of the `zou-pg` crate, linked into the backend as a static library.
Set `ZOU_TARGET` to a store root before running `initdb --set io_method=sync` and every non temp relation lives as one object per block under `tenants/local/pg/<spc>/<db>/<rel>/<fork>/`, with a `SIZE` marker per fork and absent blocks reading as zeros.
Without `ZOU_TARGET` the binary behaves exactly like stock Postgres on md.

Two constraints in v0.
Reads arrive through the PG 18 AIO path, which executes on file descriptors, so `zoustartreadv` stages pages into a per process scratch file and points the IO handle at it, which confines the build to `io_method=sync` and zouinit enforces that at startup.
`initdb` switches `CREATE DATABASE` to the `wal_log` strategy when `ZOU_TARGET` is set, because `file_copy` duplicates databases with `copydir()` underneath the storage manager and cannot see pages in the object store.
Patch `0002-zou-wal.patch` adds the durability side.
A background worker, the zou wal pusher, owns the zou-store writer lease and tails `pg_wal`: every flushed byte is appended to the group commit pipeline as a chunk prefixed with its Postgres LSN, and the durable LSN is published into shared memory.
Committing backends block after their local flush until the store holds their commit record, the same contract as a synchronous replication ack, so an acked COMMIT is durable on object storage, not just on the local disk.
The pusher lives in one process because zou-store has a single writer lease, while `XLogWrite` runs in whichever process holds `WALWriteLock`.
The `zou-bootstrap` tool completes the picture for a fresh cluster.
Run once between `initdb` and the first server start, it captures the pristine data directory, pg_control, the SLRUs, the initial WAL segment, every config file and empty directory, as immutable objects under `chk/genesis/` plus an `INDEX`, and records a full checkpoint at the initdb redo location in the manifest.
Relation pages are already in the store through the storage manager, so after bootstrap the store alone holds everything a node needs to attach.

```sh
REDO=$(build/pg/bin/pg_controldata -D /tmp/zou-pgdata | grep "REDO location" | awk '{print $NF}')
target/release/zou-bootstrap /tmp/zou-pg-store /tmp/zou-pgdata --redo "$REDO"
```

The `zou-restore` tool closes the loop: it rebuilds a data directory from the store alone.
It writes the newest full capture back exactly as its INDEX describes it, applies every delta checkpoint after it in manifest order with later files winning, flips the pg_control state from shut down to in production so the server runs crash recovery instead of trusting an old clean shutdown, and overlays every mirrored WAL record into the `pg_wal` segment file it came from.
A pg_control taken from a running server is already in production and passes through untouched.
A plain server start then replays from the last checkpoint's redo through the last durable record, and the node attaches with all committed data and no other local state.

```sh
target/release/zou-restore /tmp/zou-pg-store /tmp/zou-restored
build/pg/bin/pg_controldata -D /tmp/zou-restored | grep "cluster state"
ZOU_TARGET=/tmp/zou-pg-store build/pg/bin/pg_ctl -D /tmp/zou-restored -l /tmp/zou-restored.log start
```

On restart or reattach the pusher resumes right after the store's last record rather than at the local flush pointer, because the previous session can exit before pushing its final bytes, the shutdown checkpoint record at least, which is written after background workers stop.
The manifest tail chains segment lists across writer sessions, each entry named `<epoch>/<start-lsn>.wal`, and a session opening the store first reconciles the tail against a full listing of `wal/` so segments sealed by a crashed session are never lost.

Page writes are gated the same way commits are: `zouwritev` waits until the mirrored stream holds the WAL that produced the page, so a store object can never carry effects of records the stream has never heard of.
Without that gate a kill -9 could leave future pages in the store, and a node attaching from the store could not explain its own data.
`scripts/zou-crash-loop.sh` proves the whole contract: it runs pgbench plus a ledger client that records an id only after the server acks the COMMIT, kills the postmaster with -9 mid load, reattaches from the store alone with `zou-restore`, and asserts every recorded id is present, in a loop.
CI runs three cycles on every PR that touches the server.
One known limit: an in place crash restart replays local WAL the store has not seen yet and can push pages early during recovery, so after a crash a node should reattach with `zou-restore`; the fix, starting the pusher at consistent state, is tracked in the milestone issue.

The mirrored tail would grow without bound, so the pusher folds it at every completed Postgres checkpoint.
Once a checkpoint completes, every page change before its redo location is on the page store, and the WAL before redo is only needed for the state that does not flow through the storage manager.
The fold captures exactly that as a delta checkpoint under `chk/<redo>/`: pg_control, the transaction status SLRUs (pg_xact, pg_multixact, pg_commit_ts), two phase state, the relation maps, and the config files.
It then drops the sealed stream segments that lie entirely below the 16MB pg_wal segment boundary under redo, in the same manifest swap that records the checkpoint, so no failure between the two steps can lose WAL coverage.
The cut sits at the segment boundary rather than at redo itself because the xlog reader validates the first page header of any segment file it opens, so restore must rebuild retained segment files from their start.
The pusher only folds while fully caught up, pushed equal to the local flush, which guarantees the checkpoint record named by the captured pg_control is already durable in the store.
Transaction status captured in the fold can run slightly ahead of the record stream for commits landing in the capture window, which is safe because those commits were never acked.
Dropped segment objects stay in the bucket until the garbage collection job arrives, so a restore may overlay more WAL files than the manifest references, which recovery ignores because replay starts at the restored redo.

```sh
mkdir -p /tmp/zou-pg-store
ZOU_TARGET=/tmp/zou-pg-store build/pg/bin/initdb -D /tmp/zou-pgdata --set io_method=sync
ZOU_TARGET=/tmp/zou-pg-store build/pg/bin/pg_ctl -D /tmp/zou-pgdata -l /tmp/zou-pg.log start
build/pg/bin/psql -h /tmp -d postgres -c 'create table t(id int)'
find /tmp/zou-pg-store/tenants/local/pg -type f | head
```

## Extensions

The build ships the extension set a Supabase compatible stack leans on: pgvector, pg_trgm, pgcrypto, uuid-ossp, and pg_stat_statements.
The contrib four come with the vendored source; `-Duuid=e2fs` in the meson setup enables uuid-ossp, which needs the util-linux uuid headers on Linux (`uuid-dev`) and the system uuid on macOS.
pgvector is a second shallow submodule, `vendor/pgvector`, pinned to a release tag and built out of tree by `make pg-vector` against the installed `pg_config` via pgxs; `make pg-build` runs it as its last step.
All five load and run with `ZOU_TARGET` set, hnsw index builds included, and CI smokes each one on zou storage on every server PR.

## CI

The `postgres-build` workflow builds the vendored source with the full series applied and runs three smoke tests: one on stock md storage, one with `ZOU_TARGET` set that creates a table, restarts the server, reads the rows back from the object store, and checkpoints so the manifest carries a folded delta, and one that restores a second data directory from the store with `zou-restore` and reads the same rows after crash recovery, which exercises the delta chain.
It triggers on any PR touching `vendor/`, `patches/`, the build scripts, the `zou-pg` or `zou-store` crates, or the Makefile, and on manual dispatch.
A patch that breaks the build or changes `select version()` output gets caught in the same PR that introduces it.

## Authoring a patch

See `patches/README.md` for the mechanics.
The short version: get the tree to series state with `make pg-patch`, edit, export with `git -C vendor/postgres diff`, then prove the series applies from scratch.

## Moving the pin

Bumping to a new upstream release is its own PR: update the submodule commit, run `make pg-patch` to prove every patch still applies, run `make pg-build` and the smoke test, and let the CI job confirm it.
Nothing else in the repo may change in that PR, so a pin move can always be bisected cleanly.

## Licensing

Postgres is distributed under the PostgreSQL License, which is BSD style and compatible with this repo's Apache 2.0.
The vendored source keeps its own COPYRIGHT file, and zou's patches to it are contributed under the same PostgreSQL License terms.
