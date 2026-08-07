# Architecture

Zou is one binary and one library. Every service that Supabase runs as a separate container lives here as a crate in a single process: the REST layer, auth, realtime, file storage, and Postgres itself.

## The big picture

```
                     +----------------------------------------------+
                     |              zou (binary / library)          |
                     |                                              |
  supabase-js -----> |  +--------+ +--------+ +---------+ +------+  |
  (unchanged)        |  | REST   | | Auth   | |Realtime | |Files |  |  API layer
                     |  |zou-rest| |zou-auth| |zou-rt   | |zou-fs|  |  (HTTP/WS)
                     |  +---+----+ +---+----+ +----+----+ +--+---+  |
  psql / ORMs -----> |      +-----+----+-----------+        |      |
  (pg wire, :5432)   |  +---------v--------------+          |      |
                     |  | Postgres 18 executor    |          |      |  Query layer
                     |  | (vendored, patched)     |          |      |
                     |  +---------+--------------+          |      |
                     |  +---------v--------------------------v---+ |
                     |  | zou-store                               | |  Storage engine
                     |  | WAL buffer, page cache, manifest,       | |
                     |  | lease, compaction, CDC tap              | |
                     |  +---------+-------------------------------+ |
                     +------------+--------------------------------+
                                  v
                     S3 / GCS / R2 / MinIO / local filesystem
```

There is no Kong, no separate GoTrue, no Phoenix cluster, no PostgREST sidecar. The API layer talks to the query layer through in-process SQL sessions, not TCP.

## Core components

### Postgres executor (zou-pg)

We vendor Postgres 18 and carry a small patch series instead of writing a new SQL engine. Postgres compatibility is the whole point, and half-compatible rewrites are a graveyard.

The patches are:

- A custom smgr (storage manager) that routes page reads and writes to zou-store instead of the local filesystem. Neon proved this seam works and their patch set is Apache licensed prior art.
- A WAL hook that hands completed WAL records to zou-store for durability. The local pg_wal directory is only a scratch buffer.
- A single-tenant process model, one postmaster or single-user session per attached logical database.

Extensions compiled in: pgvector, pg_trgm, pgcrypto, uuid-ossp, pg_stat_statements, and pg_cron reimplemented on our own scheduler so it works with scale to zero.

### Storage engine (zou-store)

The heart of the project, full detail in [storage-engine.md](storage-engine.md). It appends WAL to object storage with group commit, folds sealed WAL into page checkpoints in the background, and keeps a small manifest object per database as the root of truth. The manifest is swapped atomically with conditional PUTs and carries the writer lease. A local NVMe and RAM cache holds hot pages. The WAL stream is decoded once and exposed as a change feed for realtime, so there are no logical replication slots anywhere.

### API layer

Four crates speaking the exact Supabase wire formats. zou-rest implements the PostgREST grammar under /rest/v1. zou-auth implements the GoTrue endpoints under /auth/v1 and issues JWTs with the same claim shapes. zou-realtime speaks Phoenix Channels frames under /realtime/v1. zou-files implements the Storage API under /storage/v1 and passes objects through to the same object store.

A thin router in zou-server plays the role Kong plays at Supabase: path prefixes, apikey handling, CORS, rate limits.

## Tenancy model

One logical database per tenant, the Turso model, not one shared cluster.

A tenant is a self-contained prefix like s3://bucket/tenants/acme containing its manifest, WAL, checkpoints, and files. Copying the prefix copies the project. Idle tenants cost only storage because there is no per-tenant process until a request arrives. One server node packs a thousand or more small attached tenants, and the fleet scales horizontally because any node can attach any tenant, with the lease arbitrating writes.

Who a request is for is decided before anything is attached, out of `registry/<ref>.json` and nothing else. A host under a serve domain names the tenant, `acme-prod.zou.example`, which is the way this is meant to be run because a tenant with a hostname has an origin, and an origin is what cookies, CORS, and the rest of the browser's security boundaries are drawn around. The first path segment names it too, `zou.example/acme-prod/rest/v1/todos`, for a deployment without a wildcard certificate or a laptop without DNS, and there the first segment is always the ref with no exceptions carved out, since a server routing by path has no surface of its own at the root. Entries are cached for a minute, misses for five seconds, because a minute of 404 right after `zou tenant create` reads as a bug. A host that matches nothing is answered as nothing rather than routed to whichever tenant happens to be first.

## Consistency and durability

Single writer, many readers. Writes are linearizable per database because exactly one node holds the lease. COMMIT is acknowledged only after its WAL batch is durable on the object store, so RPO is zero by construction. Crash recovery is ordinary Postgres recovery with S3 playing the role of pg_wal: the new lease holder reads the manifest, loads the latest checkpoint, and replays the WAL tail.

A branch is a new manifest pointing at the same immutable checkpoint and WAL objects, so branching is copy on write and completes in under a second. Point in time recovery is a branch materialized at an older LSN.

## Deployment modes

The same crates ship three ways. Embedded links into a host process like SQLite, with the local filesystem as just another object store backend, so the whole engine works offline. Single server mode is one static binary serving many tenants. Serverless mode runs the same binary on Lambda or Fly machines, attaching a tenant on demand in a few hundred milliseconds because attach only needs the manifest plus lazy page faults.

## Embedded execution decision

The M1 gate asked whether embedded zou runs Postgres in process as a single user session linked over the C ABI, or as a managed child postmaster on a unix socket loopback. Both were spiked against a real zou store with the patched build, `scripts/zou-spike-embed.sh` reproduces the run.

Measured on a dev laptop against a local filesystem store: a postmaster cold start answers its first query in about 150 ms, a single user session in about 40 ms. Both modes commit through the store, the single user backend included, its inserts land in the WAL stream and survive the session. A SIGKILLed backend under the postmaster is recovered by the postmaster itself with the host process unaffected, and concurrent sessions just work. The single user backend is one session per process by design, and a crash of an in process session would take the host down with it.

The decision is the managed child postmaster. Crash isolation decides it: Postgres treats a backend crash as a cluster event, reinitializes shared memory, and replays, and a host application that embeds zou must never die because one query hit a segfault in an extension. Multi session support comes for free, which the API layer needs, and the loopback is a unix socket in a private directory, so nothing is exposed. The 150 ms start sits comfortably inside the 500 ms cold attach target and the gap to 40 ms is postmaster initialization, not storage.

A true in process link stays deferred, not rejected. The blockers are concrete: the backend assumes it owns the process, FATAL errors reach exit(), signal handlers are installed globally, and the global state cannot be reinitialized for a second session in one process lifetime, which is why PGlite runs the same single user mode inside a WASM sandbox instead of linking it natively. The single user backend itself earned a place as an internal tool for one shot maintenance against a store, it is the fastest path from nothing to an answered query.
