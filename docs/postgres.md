# Vendored Postgres

zou ships its own Postgres build.
The source lives in `vendor/postgres` as a shallow git submodule pinned to the `REL_18_4` release commit, and zou's modifications are a patch series in `patches/`, applied on top by `scripts/pg-apply-patches.sh`.

## Why a submodule plus patches, not a fork branch

The patch series keeps the delta against upstream visible and reviewable as a handful of files in this repo.
Moving to a new Postgres minor is a one line submodule bump plus a series re-apply, not a rebase of a private branch that only exists on some contributor's remote.
And a fresh clone of this repo contains everything needed to reproduce the exact server binary, which is the property the fault injection and soak results depend on.

## Building

Prerequisites: meson, ninja, a C compiler, flex, bison, and the Postgres libraries the build wants (readline, zlib, icu, lz4, zstd, openssl, libxml2, libxslt).

- Debian and Ubuntu: `apt-get install meson ninja-build flex bison libreadline-dev zlib1g-dev libicu-dev liblz4-dev libzstd-dev libssl-dev libxml2-dev libxslt1-dev pkg-config uuid-dev`
- macOS: `brew install meson ninja icu4c pkg-config`

That list is not advice, it is the dependency set a release is built against.
Most of Postgres' optional dependencies default to `auto` in meson, so a machine with one more dev package than another builds a postmaster that needs one more shared library, from the same commit, and the person who finds out is whoever unpacked the tarball on a machine without it.
`make pg-build` turns off the ones zou does not offer by name, gssapi and ldap and pam and the rest, and `scripts/zou-bundle.sh` prints what a bundle still expects from the machine and fails on anything outside that list.

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
Set `ZOU_TARGET` to a store root before running `initdb --set io_method=sync --set full_page_writes=off` and every non temp relation lives as one object per block under `tenants/local/pg/<spc>/<db>/<rel>/<fork>/`, with a `SIZE` marker per fork and absent blocks reading as zeros.
Full page writes are off because a store object put is atomic on every backend, so the torn local write that setting guards against cannot be observed; docs/storage-engine.md carries the whole argument, and setting it at initdb time means restores and branches inherit it through the captured config.
Without `ZOU_TARGET` the binary behaves exactly like stock Postgres on md.
The target is a local directory or an object store URL: `s3://bucket/prefix` speaks the S3 wire API against AWS, MinIO, or R2, and `gs://bucket/prefix` speaks the same client in the GCS dialect, with the prefix scoping every key so stores can share a bucket.
URL targets read `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from the environment, `ZOU_S3_ENDPOINT` to point at a non AWS endpoint like a local MinIO, and `ZOU_S3_REGION` falling back to `AWS_REGION` then us-east-1, and the same forms work for `zou-bootstrap`, `zou-restore`, and `zou-gc`.
`ZOU_STORE_DELAY=get=15,put=25,list=40` sleeps that many milliseconds inside every store call of the named kind, which turns a fast local store into a stand in for a distant one when benchmarking, see docs/perf.md. `ZOU_STORE_SIM=s3-standard` is the richer version: a per provider latency curve with jitter, tails, transfer time, and 503 SlowDown emulation, one profile per provider or a measured calibration file, and the two knobs are mutually exclusive.
The S3 client absorbs throttling and transient server errors, 429 and most 5xx answers, with up to three retries on an exponential backoff starting at `ZOU_S3_RETRY_BASE_MS` milliseconds, 100 by default, and a PUT that dies mid transfer is never blindly retried, the caller decides.

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
It writes the newest full capture back exactly as its INDEX describes it, and the INDEX describes a file's length separately from its object's when the two differ, since a capture drops a file's trailing zeros and a WAL segment is sixteen megabytes however little has been written into it.
It applies every delta checkpoint after it in manifest order with later files winning, flips the pg_control state from shut down to in production so the server runs crash recovery instead of trusting an old clean shutdown, and overlays every mirrored WAL record into the `pg_wal` segment file it came from.
A pg_control taken from a running server is already in production and passes through untouched.
A plain server start then replays from the last checkpoint's redo through the last durable record, and the node attaches with all committed data and no other local state.

```sh
target/release/zou-restore /tmp/zou-pg-store /tmp/zou-restored
build/pg/bin/pg_controldata -D /tmp/zou-restored | grep "cluster state"
ZOU_TARGET=/tmp/zou-pg-store build/pg/bin/pg_ctl -D /tmp/zou-restored -l /tmp/zou-restored.log start
```

The `zou dev` command wraps this whole choreography for daily use.
`zou dev <target>` runs initdb plus the genesis capture when the target holds no checkpoints yet, restores into a throwaway runtime directory otherwise, then supervises the patched postmaster on 127.0.0.1:5432 plus a unix socket in a private directory, restarting it when it dies and shutting it down fast on SIGINT or SIGTERM.
`--pg-bin` or `ZOU_PG_BIN` points it at the patched install, `build/pg/bin` by default, and `--port` and `--runtime` override the rest.

The Rust side logs through the standard log facade to stderr with level, timestamp, and module target, and `RUST_LOG` filters it with the usual directives, info being the default.
That holds inside the server too: the shim installs the same backend on init, its lines land in the server log next to Postgres's own, and `RUST_LOG=zou_pg=debug,zou_store=debug` in the server environment turns up the detail.
The zou binary passes its environment to the postmaster child, so one `RUST_LOG` set in front of `zou dev` reaches both the supervisor and the shim.

On restart or reattach the pusher resumes right after the store's last record rather than at the local flush pointer, because the previous session can exit before pushing its final bytes, the shutdown checkpoint record at least, which is written after background workers stop.
The manifest tail chains segment lists across writer sessions, each entry named `<epoch>/<start-lsn>.wal`, and a session opening the store first reconciles the tail against a full listing of `wal/` so segments sealed by a crashed session are never lost.

Page writes are gated the same way commits are: `zouwritev` waits until the mirrored stream holds the WAL that produced the page, so a store object can never carry effects of records the stream has never heard of.
Without that gate a kill -9 could leave future pages in the store, and a node attaching from the store could not explain its own data.
`scripts/zou-crash-loop.sh` proves the whole contract: it runs pgbench plus a ledger client that records an id only after the server acks the COMMIT, kills the postmaster with -9 mid load, reattaches from the store alone with `zou-restore`, and asserts every recorded id is present, in a loop.
CI runs three cycles on every PR that touches the server.
One known limit: an in place crash restart replays local WAL the store has not seen yet and can push pages early during recovery, so after a crash a node should reattach with `zou-restore`; the fix, starting the pusher at consistent state, is tracked in the milestone issue.

The mirrored tail would grow without bound, so the shim folds it at every completed Postgres checkpoint.
The whole fold lifecycle, capture, publish, and log consolidation, runs on a thread of its own inside the shim, so the pusher loop never spends a store call on a fold's behalf and commit acks never wait one out; the loop only starts a fold and later harvests its verdict with a mutex peek.
Once a checkpoint completes, every page change before its redo location is on the page store, and the WAL before redo is only needed for the state that does not flow through the storage manager.
The fold captures exactly that as a delta checkpoint under `chk/<redo>/`: pg_control, the transaction status SLRUs (pg_xact, pg_multixact, pg_commit_ts), two phase state, the relation maps, and the config files.
It then drops the sealed stream segments that lie entirely below the 16MB pg_wal segment boundary under redo, in the same manifest swap that records the checkpoint, so no failure between the two steps can lose WAL coverage.
The cut sits at the segment boundary rather than at redo itself because the xlog reader validates the first page header of any segment file it opens, so restore must rebuild retained segment files from their start.
The pusher only starts a fold once its pushed position covers the checkpoint's redo, which guarantees the checkpoint record named by the captured pg_control is already durable in the store.
Transaction status captured in the fold can run slightly ahead of the record stream for commits landing in the capture window, which is safe because those commits were never acked.
Dropped segment objects stay in the bucket until the garbage collection job arrives, so a restore may overlay more WAL files than the manifest references, which recovery ignores because replay starts at the restored redo.

Every fold also packs the checkpoint's pages into sorted runs under `chk/<redo>/`, so a reader can serve a point in time without walking the mutable page prefix object by object.
A delta finds its pages by scanning the mirrored WAL between the previous checkpoint and redo for block references, which names every dirtied page because the write gate makes each page's WAL durable in the stream before the page itself can reach the store.
A full skips the scan and lists the whole `pg/` prefix instead, recording every fork size alongside the pages.
The pages land in `<n>.pages` objects of 1024 pages each in sorted block order, and a `PAGES` index records the run size and one line per packed block, so a reader locates any block with a binary search and one range read.
Pages read at fold time can be slightly newer than redo, the same replay idempotence argument Postgres recovery itself rests on, a checkpoint is a consistent starting point rather than a point in time snapshot.
The fold down policy keeps the chain short: once the chain holds four deltas, or once the deltas since the newest full outweigh it five times, the next fold captures a full instead, a whole data directory walk minus the wal segments the mirrored tail already owns and the per instance noise the server recreates.
The count cap is the read amplification bound: a page read walks at most one full, four deltas, and the WAL tail, and a test drives the worst case chain and counts the store operations, one LIST, one segment fetch, and one range read for the miss in every delta, then zero for the warm neighboring block.
One wal segment file is kept in that walk, the segment holding the redo location, because the mirrored stream can begin mid segment, restore only rebuilds segment files from the first mirrored byte onward, and recovery refuses a segment file whose first page header reads as zeros.
Restore then starts at that full and never looks behind it, so the full publish also prunes the superseded refs from the manifest and the old chain becomes garbage for the gc job.
`ZOU_FOLD_DOWN_FACTOR` overrides the factor, which the CI smoke test uses to force a fold down without writing five fulls worth of deltas.

The gc job, `zou-gc <store-root> [window-secs]`, deletes what no manifest references anymore: superseded chains, captures a failed fold left behind, and WAL segments dropped from the tail.
It walks every tenant under the root, pins the checkpoint ids and tail segments each current manifest names, follows branch_of so a branch pins the same names under its parent's prefix, and treats every other chk/ object as garbage, while a tenant whose manifest is missing contributes nothing and loses nothing.
WAL is never judged by reference alone, recovery reconciles the tail from a LIST and a segment absent from every wal_tail can still carry acked frames from a session that crashed before its first publish, so a segment dies only by the fold's own rule: its successor within the epoch starts at or below the cut, the minimum newest checkpoint redo over every manifest that can reach the tenant's WAL rounded down to a segment, which means a branch pinned at an old LSN drags the cut down and keeps the history it replays from alive.
Deletion is two phase with a safety window: one run stamps a garbage key into `gc/CANDIDATES`, and a later run deletes it only when the stamp is older than the window and the key is still garbage in that run's own scan, so a branch created between the runs republishes a reference and the deleting run drops the candidate instead of the object.
The window defaults to a day and must exceed the longest fold upload and the gap between reading a manifest and publishing a branch from it, and one gc job runs at a time, which a lock object under `gc/` enforces: a second run refuses rather than sweeping on top of the first.
`zou gc <target>` in the main CLI is the same sweep with named flags, durations written as `24h` and `7d`, a dry run that names what would go, and a `--gc-every` on `zou serve` for a node that collects on its own timer, see [operations.md](operations.md).

The chain reader is the consumer of those runs: the server serves page reads from checkpoint run objects instead of the mutable `pg/` prefix whenever it can.
At first read it loads the PAGES index of every checkpoint from the newest full onward, and a read walks that chain newest first with a binary search per index, first hit names the run object and offset, a miss falls back to the `pg/` block as before.
Serving an immutable image is only correct if nothing changed the block since that checkpoint, and the freshness argument has three legs.
The write gate guarantees any page change has its WAL durable in the mirrored stream before the page object can change.
Postgres itself serializes buffer eviction against reads of the same block, so the evicting write finishes before a competing read of that block begins.
Given those two, a listing of `wal/` is a sound barrier: the reader scans every stream segment it has not seen, across all epochs including zombie writers, and marks each referenced block dirty, and dirty blocks fall back to `pg/`.
Listing on every read would be the whole read cost, so the barrier is gated on the durable LSN the wal pusher publishes into shared memory: the write gate reads that same value before a page object may mutate, so any page change is preceded by an advance of the published LSN, and a read that sees it unchanged since its last scan skips the LIST outright.
A zero, meaning no pusher has published yet, falls back to listing every time.
Run slabs are cached in two tiers, a per process RAM tier with strict LRU eviction under a byte budget, `ZOU_READ_CACHE_RAM_MB`, default 64, and an optional disk tier shared by all backends when `ZOU_READ_CACHE_DIR` is set, sized by `ZOU_READ_CACHE_DISK_MB`, default 1024.
Cache keys are the checkpoint id, run number, and slab offset, which is content addressing because run objects are immutable, so eviction is purely about space and invalidation does not exist.
Each backend logs its hit rates every 65536 lookups and a summary on exit.
Two cases need more than block references.
A relation truncated or a relfilenode reused before the newest checkpoint leaves stale images in older chain entries with no WAL above the chain to flag them, so delta folds also persist smgr create and truncate events as `r` lines in PAGES and the chain walk stops for a relation at the first index naming it.
Unlogged relations write their main fork without WAL, so no stream barrier can see those pages change; full folds skip any relation that has an init fork and their pages always come from `pg/`.
Attach also enforces a shape rule: every fold drops WAL below its redo, so a checkpoint without runs sitting newer than one with runs would hide its window of changes forever, and the reader refuses to attach to that chain rather than serve stale pages.
On any doubt at runtime, a scan error, a vanished segment, a short run object, the reader poisons itself and every read falls back to `pg/` for the rest of the process, logged once.
`ZOU_CHAIN_READER=0` disables the reader outright.
CI proves the path end to end by deleting a `pg/` block object after a clean shutdown and reading the rows back through the runs, which only the chain reader can serve.

```sh
mkdir -p /tmp/zou-pg-store
ZOU_TARGET=/tmp/zou-pg-store build/pg/bin/initdb -D /tmp/zou-pgdata --set io_method=sync --set full_page_writes=off
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

The `postgres-build` workflow builds the vendored source with the full series applied and runs three smoke tests: one on stock md storage, one with `ZOU_TARGET` set that creates a table, restarts the server, reads the rows back from the object store, checkpoints so the manifest carries a folded delta, forces a fold down so it also carries a runtime full, and then deletes a `pg/` block object and reads the rows back through the checkpoint runs, then runs `zou-gc` twice with a zero window, asserts the superseded chain is swept and the rows still read back, and one that restores a second data directory from the gc'd store with `zou-restore` and reads the same rows after crash recovery, which exercises restore from a runtime full.
A further step runs the whole cycle against a MinIO container with `ZOU_TARGET=s3://zou-pg/smoke`: initdb, bootstrap, insert, checkpoint fold, restart, read back, and a `zou-restore` from the bucket, so the URL target path is proven against a real S3 endpoint on every server PR.
A `zou dev` step boots the supervisor against a fresh store, writes rows, kill -9s the postmaster underneath it and waits for the automatic restart to serve them again, stops it with SIGINT, and reattaches from the store alone with a second run in a new runtime directory, then branches the tenant with `zou branch` and checks both sides with `zou info`.
It triggers on any PR touching `vendor/`, `patches/`, the build scripts, the `zou`, `zou-pg`, or `zou-store` crates, or the Makefile, and on manual dispatch.
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
