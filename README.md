# zou

[![CI](https://github.com/tamnd/zou/actions/workflows/ci.yml/badge.svg)](https://github.com/tamnd/zou/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Zou is an open, embeddable, Supabase compatible backend that stores everything directly on object storage.

The name is the Japanese word for elephant (象). It is the Postgres elephant, living on the sea of S3.

## Why

Supabase has a great developer experience, but its unit of deployment is an always-on Postgres VM per project, plus a fleet of sidecar services. That means a hard cost floor for idle projects, a scaling ceiling on connections and replication slots, and a heavy Docker stack for local dev.

Zou takes a different shape:

- Real Postgres as the query executor, vendored and patched, so SQL, RLS, plpgsql, and extensions like pgvector behave exactly as you expect. Not a rewrite that is 95% compatible.
- The storage layer is replaced. WAL and page checkpoints live on S3, GCS, R2, MinIO, or a local directory. Compute nodes are stateless and scale to zero.
- The four Supabase service APIs (REST, Auth, Realtime, Storage) are reimplemented in process, speaking the exact same wire formats. An app using supabase-js points at Zou by changing only the URL and keys.
- One binary, three modes: embedded like SQLite, single server, or serverless.

Because the durable state is a prefix of immutable objects, some things fall out of the design for free: copy-on-write branching in under a second, point-in-time recovery by default, and databases per tenant that cost nothing while idle.

## Status

Early development. The first runnable piece is `zou dev <target>`: it bootstraps or attaches a store at a local directory or an S3 prefix and serves it through a supervised Postgres 18 on 127.0.0.1:5432. It needs the patched Postgres built first, see [docs/postgres.md](docs/postgres.md). `zou branch` takes copy on write branches, at the head, at an LSN, or at a timestamp, and `zou info` inspects a tenant's manifest, checkpoints, and WAL tail. Everything above the storage engine does not exist yet.

The full design lives in the [architecture docs](docs/), and the implementation plan is tracked in the milestone issues:

- [M1: Core engine, Postgres 18 on object storage](https://github.com/tamnd/zou/issues/1)
- [M2: REST and Auth, supabase-js works unchanged](https://github.com/tamnd/zou/issues/2)
- [M3: Storage API, multi tenant server, serverless, embedded bindings](https://github.com/tamnd/zou/issues/3)
- [M4: Realtime and edge functions](https://github.com/tamnd/zou/issues/4)
- [M5: Migration from Supabase, hardening, 1.0](https://github.com/tamnd/zou/issues/5)

Supabase compatibility is measured rather than claimed. `conformance/` asks the real PostgREST and GoTrue binaries and zou the same questions, and compares status, headers, and bodies. There are two REST suites: a hand written one about the surface a Supabase project uses, 82 cases, and one derived from PostgREST's own spec files, 1217 requests. Both pass in full. The auth suite stands at 74 of 77, with three differences that are deliberate and written down. The storage suite is 478 cases against the storage-api image a local Supabase project runs, buckets, objects, image transforms and the S3 protocol, and all 478 pass. supabase-js's own integration tests are run against zou as well, unedited apart from the url they point at, and 33 of the 34 pass with the thirty fourth skipped by upstream itself, as do 133 of storage-js's own. Resumable uploads and presence are asked in a third shape, because an upload and a room are conversations rather than requests: the real tus-js-client drives an upload against zou and supabase-js drives a presence channel and both ways of broadcasting over http, the answers are read back through the same client an application would use, and both files are run against a real supabase project so that the assertions are the reference's behaviour rather than ours. The questions and the recorded answers live in [tamnd/zou-conformance](https://github.com/tamnd/zou-conformance), pinned to a commit here, and every case that differs is listed with the reason it differs, see [docs/conformance.md](docs/conformance.md). The numbers per endpoint and per feature are in [docs/scoreboard.md](docs/scoreboard.md), which CI rewrites on every merge, and what a project will actually notice when it moves is in [docs/compatibility.md](docs/compatibility.md).

## What it will look like

Embedded, for tests and local dev:

```ts
import { createZou } from "zou";

const zou = await createZou({ dir: "./data" });
const supabase = zou.client(); // the supabase-js interface, in process

await supabase.from("todos").select("*").eq("done", false);
```

That runs today.
[`zou-embed`](docs/embedded.md) opens a project inside a host process, answers requests through the same router the server puts on a port, serves it on a port too when something outside wants in, and branches.
`libzou` is the same thing behind `zou.h`, for anything that can load a shared library, the node package is napi over the crate, the python package is PyO3 over it, and the Go package is cgo over `libzou`.
`client()` is a real supabase-js client with a `fetch` that goes to the router in this process, so there is no socket anywhere under that snippet, `zou.client()` in python is a real supabase-py client with an httpx transport doing the same thing, and in Go a project is an `http.RoundTripper`, so `project.Client()` is an ordinary `*http.Client` that never touches a socket.

`createFixture()` is the same thing for a suite that wants a database per test rather than per run.
The machine builds one template, once, and every fixture after it is a copy on write branch of that template, which is tens of milliseconds instead of the half minute initdb through a store costs.
The suites in this repo run on it.

A server, where the only durable state is the bucket:

```bash
zou serve s3://mybucket/tenants --domain api.example.com
```

Or one project on a function, where there is no server to run at all:

```bash
zou lambda s3://mybucket/tenants --ref demo
```

`--ref` serves one project at every url it answers and brings it up before the first request rather than because of it, which is what Lambda, Cloud Run and Fly all want. The recipes for the three are in [docs/serverless.md](docs/serverless.md).

Realtime is on the same url and the same key:

```js
const room = supabase.channel('room')
room.on('broadcast', { event: 'cursor' }, ({ payload }) => draw(payload))
room.on('presence', { event: 'sync' }, () => render(room.presenceState()))
room.subscribe(async (status) => {
  if (status === 'SUBSCRIBED') await room.track({ typing: false })
})
```

The socket, the channels on it, tokens refreshed mid connection, broadcast between the members of a topic and presence on it are served today, against the real realtime-js.
A room can be sent to over http as well, both shapes the client posts, which is how a trigger or a worker talks to one without holding a socket.
Private channels are served too, against the project's own row level security policies on `realtime.messages`, so policies written for Supabase Realtime answer the same way here.
`postgres_changes` is served as well, so a channel can subscribe to a table and be sent the rows that changed in it, filtered by the same operators upstream takes and checked against the policies on that table, see [docs/realtime.md](docs/realtime.md).

Database webhooks are served too, which is to say the trigger a dashboard writes:

```sql
create trigger orders_webhook after insert on public.orders
    for each row execute function supabase_functions.http_request(
        'https://example.com/hooks/orders', 'POST', '{"Content-Type":"application/json"}', '{}', '1000'
    );
```

The `net` and `supabase_functions` schemas are pg_net's interface and upstream's trigger function, but there is no background worker behind them: the queued row announces itself with a notification and the server makes the call, and a request that could not be delivered is tried again, which upstream never does, see [docs/webhooks.md](docs/webhooks.md).

Scheduled jobs are pg_cron's interface on the same idea:

```sql
select cron.schedule('nightly-vacuum', '0 3 * * *', 'delete from events where at < now() - interval ''30 days''');
```

The `cron` schema is upstream's functions and its two tables, and the firing is done by the server rather than by a launcher process, so a deployment that scales to zero comes back to one run of a job it missed rather than to a queue of them, see [docs/cron.md](docs/cron.md).

Branch a database for a preview deploy:

```bash
zou branch s3://mybucket/tenants create prod pr-142
```

That is a manifest write and no data movement, so it is the same tens of milliseconds against a 73 GB database as against a 73 MB one, and the branch is served by pointing any command at it with `--ref pr-142`. Point in time, the refusals, the composite action that does it per pull request, and what a branch costs a store over a year are in [docs/branching.md](docs/branching.md).

## Design at a glance

A tenant is a self-contained prefix on the object store: a manifest, WAL segments, and page checkpoints. The manifest is the root of truth and is swapped atomically with conditional PUTs, which also carries a writer lease with epoch fencing. One writer per database, unlimited stateless readers, no consensus service. Commits are acknowledged only after the WAL batch is durable on the object store.

There is a longer writeup in [docs/architecture.md](docs/architecture.md) and the storage engine details are in [docs/storage-engine.md](docs/storage-engine.md).

## Building

You need a recent stable Rust toolchain.

```bash
cargo build
cargo test
```

The grammars, the on disk formats and the token parsers are fuzzed. Every target keeps a seed corpus next to it and CI runs all of them from those seeds on every change and for longer each night.

```bash
cargo install cargo-fuzz
cargo +nightly fuzz run rest_filter
```

`make demo` tours the object layer in seconds, and after a one time `make pg-build` it continues into the real Postgres on a store, rows, a branch, and a restart from nothing but the objects. The walkthrough is in [docs/quickstart.md](docs/quickstart.md).

A release is the binary and the patched Postgres together, since one without the other cannot start a database, and installing is a directory:

```bash
curl -fsSL https://raw.githubusercontent.com/tamnd/zou/main/install.sh | sh
```

That takes the bundle for the platform, checks it against its sha256, and unpacks it into `~/.zou`, and the zou inside it finds the postmaster next to itself with nothing to configure. The bundle is 56.5 MB on darwin arm64 and 50.6 MB on linux x64 against a budget of 150 MB, and `scripts/zou-bundle.sh` builds one from a checkout, see [docs/packaging.md](docs/packaging.md).

`brew install tamnd/zou/zou` and `npm install -g zou-cli` are the same tree for anyone who would rather get it from a package manager, and `pip install zou zou-postgres` is the same tree again for python. Either way a project that installs the binding next to the postgres package has a database per test with no path to point anywhere.

## Contributing

The project is being built top to bottom against the milestone checklists, one PR per feature. Issues and PRs are welcome, but expect churn while M1 is in progress.

## License

Apache 2.0, see [LICENSE](LICENSE).
