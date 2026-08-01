# Quickstart

From clone to a Postgres whose every page and WAL byte lives on a store, in about ten minutes, most of them spent compiling Postgres once.

## Prerequisites

A recent stable Rust toolchain for everything, and for the full tour the usual Postgres build tools: meson, ninja, a C compiler, flex, bison, and the readline, zlib, and icu libraries.

- Debian and Ubuntu: `apt-get install meson ninja-build flex bison libreadline-dev zlib1g-dev libicu-dev pkg-config uuid-dev`
- macOS: `brew install meson ninja icu4c pkg-config`

## Act one, the object layer

```bash
git clone https://github.com/tamnd/zou && cd zou
make demo
```

That runs in seconds and needs nothing but Rust. It plays the storage engine on a local directory: the genesis manifest, the writer lease with epoch fencing, group committed WAL, sealed segments, and the manifest tail, printing each object as it lands. The directory is kept so you can look around.

Without the vendored Postgres built yet the demo stops there and tells you so.

## Act two, the real Postgres

```bash
make pg-build   # once, this is the slow part
make demo       # now both acts play
```

`make pg-build` fetches the pinned Postgres submodule, applies the zou patch series, and builds it with the storage manager shim linked in, see docs/postgres.md. With that in place `make demo` continues past act one: it starts `zou dev` on a fresh store, writes rows through plain `psql`, stops the server, prints what the store holds with `zou info`, takes a branch with `zou branch` which costs one small manifest and copies no data, and then restarts Postgres from nothing but the store and reads the rows back.

## Your own targets

`zou dev <target>` accepts more than a directory:

```bash
zou dev /tmp/mydb                      # a directory of objects
zou dev s3://bucket/prefix             # any S3 compatible endpoint, see below
zou dev sqlite:///tmp/mydb.db          # the whole store in one SQLite database
```

S3 style targets read `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from the environment, plus `ZOU_S3_ENDPOINT` for a non AWS endpoint like a local MinIO and `ZOU_S3_REGION` when it matters. `gs://bucket/prefix` speaks the GCS dialect with HMAC interop keys.

A path ending `.zou` is the single file backend. Every sequential tool works over it today, `zou info`, `zou branch`, `zou-bootstrap`, `zou-restore`, while `zou dev` needs the multi process postmaster and waits on the in process engine, see the note in docs/storage-engine.md.

## Mail on a laptop

`zou dev <target> --http 54321` starts the API front door next to the postmaster and logs the anon and service_role keys the way `supabase start` does, so a client is pointed at it by copying two lines.

Signups are confirmed on the spot by default, which is the same thing the Supabase CLI does locally. Set `ZOU_MAILER_AUTOCONFIRM=false` and the dev loop mails its confirmations instead. Nothing carries them anywhere: with no mail server configured, zou keeps what it sends in memory, logs the link, and serves the last hundred messages to the service role. `zou inbox` prints them.

```bash
ZOU_JWT_SECRET=$(openssl rand -hex 32) ZOU_MAILER_AUTOCONFIRM=false zou dev /tmp/mydb --http 54321
zou inbox                              # who it went to, the subject, the link
zou inbox --clear                      # start the next flow with an empty mailbox
```

`zou inbox` mints the service_role key from `ZOU_JWT_SECRET`, the same variable `zou dev` asks to be pinned, or takes `ZOU_SERVICE_KEY` when there is one to hand. It talks to 127.0.0.1 and nowhere else. Recovery, magic link, reauthentication and email change codes all arrive in the same place, so the whole of the email surface can be walked through without a mail catcher container or a second port.

There is no SMTP transport yet, so a project that configures a real mail server has nowhere to send. That is the next piece of milestone 2.

## Where to go next

- docs/architecture.md for the shape of the whole system
- docs/storage-engine.md for the manifest, lease, WAL, checkpoint, and branching design
- docs/postgres.md for the patch series and the storage manager shim
- docs/operations.md for leases, retention, and recovery in operation
- docs/perf.md and docs/benchmarks.md for how the numbers are measured
