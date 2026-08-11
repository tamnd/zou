# Embedded

A whole Supabase compatible project inside another process, with no daemon to start, no port to pick, and no docker compose file.
This is the `zou-embed` crate, `libzou`, which is the same thing behind a C ABI, and the node, python, and Go packages over it.

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

Unix only for now, since the postmaster is supervised with signals and its socket directory with unix permissions.
On Windows the crate is an empty library rather than a build failure, so a workspace that carries it still builds there.

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

## A database per test

```rust
Options::fixture()                // branched off the machine's template
```

An ephemeral project runs initdb against its new store, and initdb through a store writes about 3800 one page objects: 5.5 s on a github runner, 30 s on a laptop, 150 s on a small vps.
That is the whole cost of a create, and it is what stops a suite from taking a database per test.

A fixture does not run initdb.
One template is built per machine and per Postgres build, and every fixture is a branch of it: a manifest write, no pages copied, and the postmaster start that was always going to be there.
On this laptop that is 36 ms at p50 over 50 creates, of which 1 ms is the branch, 16 ms the restore, 19 ms the postmaster, and a fraction of a millisecond the front door.
A 6 vcpu vps with a slow disk is 1.6 s, which is the same four steps on a machine where all four are twenty times slower.

```rust
#[test]
fn a_signup_lands_in_auth_users() {
    let zou = Zou::open(Options::fixture()).unwrap();
    // Your own schema, over the dsn, the way a host process does it.
    // Then the surface, in this process, with nobody else's rows in it.
}
```

Fixtures share the template store and see nothing of each other, each one is a tenant of its own, and closing one takes that tenant off the store while the captures it read from stay where they are.
A fixture is branchable the moment it is cut, because the template folded a full page capture down before it was published, so a test that wants a branch of its fixture does not have to write to it first.

Two things a real project pays for are not paid here.
The template store carries a `.zou-scratch` marker, so writes to it are not fsynced: everything under it is either the template, which is rebuilt from nothing when it is missing, or a fixture, which is deleted by the handle that made it, and a full fsync is 4 to 5 ms on APFS.
And whether a branch of the template would serve is checked on the first fixture of a process rather than on every one, because a published template never changes again, so the answer cannot change either.
Together they are most of the 12 ms the branch used to cost.

The first fixture on a cold machine builds the template, which is one initdb and one fold, 45 s on this laptop.
It lands in `$ZOU_TEMPLATE_CACHE`, or `$XDG_CACHE_HOME/zou/templates`, or `~/.cache/zou/templates`, and a CI job that keeps that directory between runs pays for it once.
Two processes arriving at a cold machine at the same time do not both build one: the loser waits for the winner under a lock, and a lock nobody released within ten minutes is treated as a build that died.

The template id is a hash of the Postgres build, the crate version, and the DDL `zou_server::sql::bootstrap` would apply.
The last one is in there because a fixture skips the bootstrap on the strength of the template having run it, which is 75 ms of the create, so a template built against an older auth schema has to be a different template rather than a stale one.

`Options::fixture()` names no target, since the store is the template's.
Anything that has to end up in a particular directory is `Options::dir`, which is a real store and pays what a real store costs.

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

## From C, and from anything that speaks C

`libzou` is the same crate behind a C ABI, built as a shared library, a static library, and an rlib.
The header is [`crates/libzou/include/zou.h`](../crates/libzou/include/zou.h) and it is written by hand rather than generated, because it is the contract and a contract nobody read is not one.

```c
#include "zou.h"

zou_options *options = zou_options_new();
zou_options_set(options, "target", "./data");

zou *handle = NULL;
if (zou_open(options, &handle) != ZOU_OK) {
    fprintf(stderr, "%s\n", zou_last_error());
}
zou_options_free(options);

zou_header headers[1] = {{"apikey", zou_anon_key(handle)}};
zou_response *answer = NULL;
zou_request(handle, "GET", "/rest/v1/todos?select=title", headers, 1, NULL, 0, &answer);
printf("%d\n", zou_response_status(answer));
zou_response_free(answer);

zou_close(handle);
```

Four rules cover all of it.
Everything that can fail returns `ZOU_OK` or a negative code and none of it unwinds, so a panic anywhere inside becomes `ZOU_ERR_PANIC` rather than an unwind across a boundary that has no idea what one is; on a nonzero code nothing was written to the out parameter and `zou_last_error()` has a sentence about it, on this thread, until the next call.
Ownership is by name: `zou_options_new`, `zou_open`, `zou_branch`, and `zou_request` hand back something to free, and everything else is borrowed and lives as long as what it came from.
A handle may be used from any thread and from several at once, which is what makes it worth embedding in a server.
Strings are UTF-8 and NUL terminated, and a body is bytes with a length, because a body may be an image.

Options are set by name rather than through a `repr(C)` struct.
A struct is an ABI and every field added to one later is a break, while a function added later is just a function nobody is calling yet.

`ZouError` is `{ kind, message }` in `zou-embed` for this reason: an int and one string is what crosses a C boundary, and the codes are the kinds, one for one.

```bash
ZOU_PG_BIN=$PWD/build/pg/bin crates/libzou/tests/smoke.sh
```

That builds the library, compiles [`crates/libzou/tests/smoke.c`](../crates/libzou/tests/smoke.c) against the header with `-Wall -Wextra -Werror`, and runs it: open, sign somebody up, get refused without a key, take a port, checkpoint, branch or be told why not, close.
It runs in CI on the job that builds the patched Postgres.

## From node

```js
import { createZou } from "zou";

const zou = await createZou({ dir: "./data" });
const supabase = zou.client();

await supabase.from("todos").select("*").eq("done", false);
await zou.close();
```

That is a real supabase-js client and there is no socket under it.
`client()` builds one with a `fetch` that hands the request to the same router in this process, so a `.from()`, a `.rpc()`, an `auth.signUp()`, or a storage upload goes where it would have gone and comes back the way it would have come back.
`client()` takes the anon key unless you pass `zou.serviceRoleKey`, because a test that means to skip row level security should have to say so in the same place it would against a hosted project.

supabase-js is a peer dependency and an optional one.
A project that only wants `zou.fetch` or a port does not install it, and a project that calls `client()` without it gets a sentence rather than a stack.

```js
import { createFixture } from "zou";

test("a signup lands in auth.users", async (t) => {
  const zou = await createFixture();
  t.after(() => zou.close());
  const supabase = zou.client();
  // A database of this test's own, in tens of milliseconds.
});
```

`createFixture` is `Options::fixture()` from javascript, and the section above is the whole story: the machine builds one template and every fixture is a branch of it.
The node suite in this repo runs on fixtures and went from 232 s to 46 s, of which 39 s is the one test that is still about the ordinary initdb path and is meant to be.

```js
const zou = await createZou();                     // ephemeral, gone on close
const zou = await createZou({ url: "s3://bucket/app" });
const port = await zou.listen(0);                  // 0 asks the kernel
const preview = await zou.branch("pr-142");
await zou.checkpoint();
```

`zou.dsn` is the other door, for a host that wants `pg` or `psql` on the database directly, which is how a test suite creates its own schema before serving it.
`zou.anonKey`, `zou.serviceRoleKey`, `zou.target`, and `zou.tenant` are the rest of what `zou status` prints.
`zou.url` is the port once `listen` has been called, and a name that resolves nowhere before that, since before that there is nothing to resolve.

A node project still needs the patched Postgres, and the way it gets one without a `ZOU_PG_BIN` in every script is the command line package next door.

```bash
npm install --save-dev zou zou-cli
```

`zou-cli` downloads the release bundle for the platform it is installed on, which is the `zou` binary and the patched Postgres in one tree, and the binding looks in it: `pgBin`, then `ZOU_PG_BIN`, then `zou-cli`.
So a project that installed both has a database per test and nothing to configure, and a project that has a checkout and a `build/pg` carries on as before.

The binding is [napi](https://napi.rs) over `zou-embed` rather than over the C ABI, since it is Rust either way and a Rust crate calling its own C ABI is a longer road to the same place.
`libzou` is the road for everything that is not Rust.
Everything that takes time is a task on the thread pool rather than work on the thread node runs javascript on, so opening a project and answering a request do not stop the event loop.

```bash
crates/zou-node/build.sh
npm --prefix crates/zou-node install
ZOU_PG_BIN=$PWD/build/pg/bin npm --prefix crates/zou-node test
```

`build.sh` is cargo plus a copy: node loads a cdylib under a name ending in `.node` and that is the whole build.
The tests open real projects and run in CI on the job that builds the patched Postgres.

## From python

```python
from zou import create_fixture

def test_a_signup_lands_in_auth_users():
    with create_fixture() as zou:
        supabase = zou.client()
        supabase.auth.sign_up({"email": "a@example.com", "password": "correct horse battery"})
```

`create_zou` and `create_fixture` are the same two doors node has, and `client()` is a real supabase-py client with an httpx transport that hands the request to the router in this process.
supabase-py takes an `httpx_client` in its options, so nothing is monkeypatched and nothing is subclassed: the client is built the way its own documentation says to build it, with a transport that goes somewhere else.

```python
zou = create_zou(dir="./data")             # a directory of objects, kept
zou = create_zou(url="s3://bucket/app")    # a bucket, or sqlite://, or a .zou file
zou = create_zou()                         # ephemeral, gone on close
port = zou.listen(0)                       # 0 asks the kernel
preview = zou.branch("pr-142")
zou.checkpoint()
```

`zou.dsn` is the other door, for psycopg or psql on the database directly, which is how a suite creates its own schema before serving it.
`zou.anon_key`, `zou.service_role_key`, `zou.target`, and `zou.tenant` are the rest of what `zou status` prints, and the handle is a context manager so `with` closes it.
`zou.request(method, path, headers, body)` is one level down from the client, and `zou.transport()` is the httpx transport on its own for a client somebody else built.

Anything that fails raises `zou.ZouError`, which carries `.code`, one of `ZOU_OPTIONS`, `ZOU_POSTGRES`, `ZOU_STORE`, `ZOU_REQUEST`, or `ZOU_IO`, so a test can branch on the kind rather than on the shape of a sentence.
supabase-py and httpx are optional: `pip install zou[client]` for `client()`, and neither is needed for `request()` or `listen()`.

The binding is [PyO3](https://pyo3.rs) over `zou-embed`, built abi3 so one extension serves every python from 3.9 up, and every call that takes time releases the GIL while it takes it, so a thread that opens a project does not stop the rest of the process.

```bash
crates/zou-python/build.sh
ZOU_PG_BIN=$PWD/build/pg/bin python3 -m unittest discover -s crates/zou-python/test
```

`build.sh` is cargo plus a copy, the same as node's, and maturin builds the wheel.
The tests open real projects, skip the supabase-py one when supabase-py is not installed, and run in CI on the job that builds the patched Postgres.

A project that never built anything installs the pair instead:

```bash
pip install zou zou-postgres
```

`zou` is the extension module and `zou-postgres` is the patched Postgres as a wheel per platform, and the binding looks for a postmaster in `pg_bin`, then `ZOU_PG_BIN`, then that package, so both installed means `create_fixture()` with nothing to configure.
The details of the two wheels and what their platform tags exclude are in [docs/packaging.md](packaging.md).

## From Go

```go
import zou "github.com/tamnd/zou/go"

project, err := zou.Fixture()
defer project.Close()

answer, err := project.Client().Get(project.URL() + "/rest/v1/todos?select=title")
```

The Go binding is cgo over `libzou` rather than a binding of its own, because Go's way into a C library is cgo and there is nothing else to invent.
The seam into Go is `http.RoundTripper`: a `*Zou` is one, so `Client()` is an ordinary `*http.Client` that answers in this process, and any library that takes a client can be pointed at a database of its own without knowing anything about zou.
That is the same move `fetch` is in node and an httpx transport is in python, in the shape Go already has.

`Open(zou.Options{...})` and `Fixture()` are the two doors, then `Request`, `Listen`, `Branch`, `Branchable`, `Checkpoint`, `Close`, and `AnonKey`, `ServiceRoleKey`, `DSN`, `Target`, `Tenant`, `URL`.
A failure is a `*zou.Error` with `Code`, `Kind`, and `Message`, so `errors.As` gets the kind rather than a string to match on.
`Close` twice is fine, and a handle that goes out of scope without it has a finalizer, because a postmaster that outlives the thing holding it is worse than a leak.

One thing cgo makes true that C does not: `zou_last_error` belongs to the thread that made the call, and a goroutine is not a thread.
Every call that can fail is therefore paired with its message inside a single cgo call, through a few lines of C in the preamble, since one cgo call is one thread by construction.

```bash
cargo build -p libzou
ZOU_PG_BIN=$PWD/build/pg/bin go test ./go/...
```

`go/test.sh` does both.
The package names `target/debug` and bakes an rpath to it, so a checkout works with nothing else set, and `CGO_LDFLAGS` names the directory anywhere else.
The tests open real projects and run in CI on the job that builds the patched Postgres.

## Testing it

The unit tests run offline.
The end to end suite needs the patched Postgres and skips without it:

```bash
ZOU_PG_BIN=$PWD/build/pg/bin cargo test -p zou-embed --test embed
```

It opens real projects, so it is minutes rather than seconds.
