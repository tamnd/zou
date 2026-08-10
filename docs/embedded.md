# Embedded

A whole Supabase compatible project inside another process, with no daemon to start, no port to pick, and no docker compose file.
This is the `zou-embed` crate.
The Node, Python, and Go bindings are built over it and are not there yet.

## What it is

`zou dev` and `zou serve` are a store, a Postgres over it, and the front door as one axum router, with a command line on the front.
`zou-embed` is the same three things with a Rust API on the front instead.

```rust
use zou_embed::{Options, Zou};

let zou = Zou::open(Options::dir("./data"))?;
let answer = zou.request(
    "GET",
    "/rest/v1/todos?select=id,title&done=eq.false",
    &[("apikey", zou.keys().anon.as_str())],
    b"",
)?;
assert_eq!(answer.status, 200);
```

`request` is answered in this process.
There is no socket under it, nothing serializes the request line, and no port has to be open, so a test suite that opens a project per test case is not competing for ports or waiting on listeners.
It is the same router `zou serve` puts on a port, so a call that would be refused over HTTP is refused here in the same words, including a call with no `apikey` on it.

Nothing is added on the way in.
The handle does not sign anything for you and does not raise anybody's privileges, which is why `keys()` hands back the same two keys `zou status` prints and you choose which one a call carries.

## Opening

```rust
Options::dir("./data")            // a directory of objects, kept
Options::url("s3://bucket/app")   // a bucket, or sqlite://, or a .zou file
Options::ephemeral()              // a store of the handle's own, gone on close
```

A store with no database in it is initdb'd once and its genesis captured.
A store that has one is restored into a runtime directory and replayed.
Either way the durable state is the store and the runtime directory is a copy, which is why closing removes it without asking.

Postgres is a child process, not a library.
It has to be the patched build, so `Options::pg_bin` or `ZOU_PG_BIN` has to name one, see docs/postgres.md for building it.
That is the cost of `open` and it is seconds rather than milliseconds, most of them on a store nobody has run initdb against yet.

By default every handle mints a fresh random JWT secret, which is right for a test and wrong for anything that has to still recognise its own tokens after a restart.
`Options::jwt_secret` pins it.

## Serving it to somebody else as well

```rust
let port = zou.listen(0)?;  // 0 asks the kernel for one
```

That puts the same router on a port, serving the project this handle is already holding rather than a second copy of it.
A browser, a supabase-js client, or another process reaches `http://127.0.0.1:<port>` and sees exactly what `request` sees.
`zou.dsn()` is the other door, for a host that wants psql or its own Postgres driver on the database directly.

## Branching

```rust
let preview = zou.branch("pr-142")?;
```

A checkpoint is taken first, on purpose: asking for a branch from inside the process that is writing should carry what that process has written.
Then the child manifest is written, no data is copied, and the child is opened and handed back as a second live handle.
The two are separate databases from that moment, so a write to one is not visible in the other.

A branch reads inherited pages out of the captures it names and has no fallback for the ones it cannot.
A database young enough that no fold has packed a full page capture down yet cannot be branched, and `branch` says so and leaves nothing of the child on the store, rather than handing back something only shaped like a database that would fail on its first page read.
`branchable()` asks the same question in advance, for a host that wants to know whether to offer the button.

## Closing

`close` stops Postgres and removes the running copy, and reports whether that went cleanly.
Dropping the handle does the same work and has nowhere to report it, which is the only difference between them.
Shutdown is fast, then immediate, then the signal nothing catches, because a postmaster that outlives its handle would be writing pages nobody is holding.

## Threads

A handle is `Send` and `Sync` and requests may be issued from any thread at once.
What is not allowed is calling `request` from inside an async runtime's own thread: it blocks on this handle's runtime to get the answer, and blocking a runtime thread on another runtime is how a program stops.
A host that is already async should call it from `spawn_blocking`.

## Testing it

The unit tests run offline.
The end to end suite needs the patched Postgres and skips without it:

```bash
ZOU_PG_BIN=$PWD/build/pg/bin cargo test -p zou-embed --test embed
```

It opens real projects, so it is minutes rather than seconds.
