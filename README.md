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

Early development. The first runnable piece is `zou dev <target>`: it bootstraps or attaches a store at a local directory or an S3 prefix and serves it through a supervised Postgres 18 on 127.0.0.1:5432. It needs the patched Postgres built first, see [docs/postgres.md](docs/postgres.md). Everything above the storage engine does not exist yet.

The full design lives in the [architecture docs](docs/), and the implementation plan is tracked in the milestone issues:

- [M1: Core engine, Postgres 18 on object storage](https://github.com/tamnd/zou/issues/1)
- [M2: REST and Auth, supabase-js works unchanged](https://github.com/tamnd/zou/issues/2)
- [M3: Storage API, multi tenant server, serverless, embedded bindings](https://github.com/tamnd/zou/issues/3)
- [M4: Realtime and edge functions](https://github.com/tamnd/zou/issues/4)
- [M5: Migration from Supabase, hardening, 1.0](https://github.com/tamnd/zou/issues/5)

## What it will look like

Embedded, for tests and local dev:

```ts
import { createZou } from "zou";

const zou = await createZou({ dir: "./data" });
const supabase = zou.client(); // the supabase-js interface, in process

await supabase.from("todos").select("*").eq("done", false);
```

A server, where the only durable state is the bucket:

```bash
zou serve --store s3://mybucket/tenants --domain '*.api.example.com'
```

Branch a database for a preview deploy:

```bash
zou branch create prod pr-142
```

## Design at a glance

A tenant is a self-contained prefix on the object store: a manifest, WAL segments, and page checkpoints. The manifest is the root of truth and is swapped atomically with conditional PUTs, which also carries a writer lease with epoch fencing. One writer per database, unlimited stateless readers, no consensus service. Commits are acknowledged only after the WAL batch is durable on the object store.

There is a longer writeup in [docs/architecture.md](docs/architecture.md) and the storage engine details are in [docs/storage-engine.md](docs/storage-engine.md).

## Building

You need a recent stable Rust toolchain.

```bash
cargo build
cargo test
```

## Contributing

The project is being built top to bottom against the milestone checklists, one PR per feature. Issues and PRs are welcome, but expect churn while M1 is in progress.

## License

Apache 2.0, see [LICENSE](LICENSE).
