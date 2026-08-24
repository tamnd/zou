# Edge functions

A directory with an `index.ts` in it, `Deno.serve(handler)` inside that, and a url of `/functions/v1/<name>`.

```
supabase/
  config.toml
  functions/
    hello/index.ts
    _shared/cors.ts
```

```ts
Deno.serve(async (req) => {
  const { name } = await req.json()
  return new Response(JSON.stringify({ hello: name }), {
    headers: { "content-type": "application/json" },
  })
})
```

```
curl -X POST http://127.0.0.1:54321/functions/v1/hello \
  -H "Authorization: Bearer $ANON_KEY" \
  -H "content-type: application/json" \
  -d '{"name":"world"}'
```

The client library reaches the same place.

```js
const { data } = await supabase.functions.invoke('hello', { body: { name: 'world' } })
```

## The engine is a build time choice

V8 is a static library the size of the rest of the binary several times over, and a zou serving a database and a bucket should not be carrying one it never starts.
So the javascript engine is behind a feature of the crate that owns it, and the way in is to ask for it by name:

```
cargo build --release -p zou --features zou-deno/isolate
```

Spelled that way rather than as a feature of `zou` on purpose: a feature of `zou` is one `cargo test --workspace --all-features` turns on, and somebody working on the database should not be downloading V8 because of a flag that means "everything".

Measured on macos arm64, release, unstripped:

| Build | Size |
| --- | --- |
| `cargo build --release -p zou` | 20,939,040 bytes, 20.0 MiB |
| `cargo build --release -p zou --features zou-deno/isolate` | 74,885,168 bytes, 71.4 MiB |

That is 51.4 MiB of engine, three and a half times the binary, which is why it is a choice and not a default.
The released bundles for linux and macos are built with it, because somebody who downloads one should not have to rebuild it to run a function.
The windows binary is not, since the dev loop that would start a function is unix only there.

A build without the feature still serves the whole surface in front of the runtime.
It reads the project's functions, lists them at boot and answers `/functions/v1/<name>` for each, and then answers the call with the 500 a broken function answers, with the reason in the log.
That is deliberate.
A deployed function answered with a 404 would be indistinguishable from a name nobody wrote, and a caller cannot see through that; a 500 and a log line say what actually happened.

`zou status` and the boot log both name which of the two is running.

## What is served

The listing is one level deep, it is the directory's name that becomes the url, and the entrypoint is `index.ts`.

- `functions/hello/index.ts` is served as `hello`.
- `functions/_shared/` is not served, and neither is anything starting with a dot. That is the convention every example project already uses for the code its functions import.
- `functions/nested/deep/index.ts` is not served. The listing does not walk.
- Unless the config file names it: a `[functions.deep]` with an `entrypoint` of `./functions/nested/deep/index.ts` is served as `deep`, with no `functions/deep` anywhere. That is upstream's behaviour, measured on the Supabase examples project, whose `simple-mcp-server` is exactly that shape. It is the block that adds the name, not the depth of the path.
- `functions/noindex/other.ts` is not served, because the entrypoint is missing.
- `functions/jsfn.js` is not served. A function is a directory.

Every one of those answers `404 Function not found`, as text, which is what upstream answers a name nobody deployed.
A function that exists and one that does not are the same thing to a caller, on purpose.

## Serving them while you write them

`zou dev <target>` serves functions along with everything else, on the project's api port, because a project is a database and a bucket and its functions together.
`zou functions serve` is the same surface on its own, for somebody who is writing a function rather than running a project, which is what upstream's `supabase functions serve` is for.

```
zou functions serve
```

```
function hello at supabase/functions/hello/index.ts
functions run on a v8 isolate per function, kept between calls
serving functions on http://127.0.0.1:54321
  http://127.0.0.1:54321/functions/v1/hello
anon key eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
service_role key eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
sql goes to 127.0.0.1:54322, which is whatever is serving the project
```

The port is the project's own api port when the file names one, and 54321 otherwise, which is where a client library already looks.
The keys are minted from `ZOU_JWT_SECRET` when it is set, and pinning it is what makes them the same keys `zou dev` prints, so a project's `.env` keeps working whichever of the two is running.
The database a function is told about in `SUPABASE_DB_URL` is the project's db port, dialled lazily: a `zou dev` on that port is a database these functions reach through the client library, and no database at all is a function that runs and a `/rest/v1` that says so.

| Flag | What it does |
| --- | --- |
| `--port <n>` | Where this listens, over the project's api port. |
| `--env-file <path>` | The dotenv file the functions' environment is read out of, instead of `functions/.env`. |
| `--import-map <path>` | One map for every function of this run, over the config file and over what is beside each function. |
| `--no-verify-jwt` | Every function of this run is callable without a token. |
| `--inspect [<port>]` | Open the debugger port, which is the config file's `inspector_port` when the flag names none and 8083 when neither does. |
| `--config <path>`, `--no-config` | Which `config.toml` to read, or none at all. |

The first four are upstream's, spelled the same way and with the same precedence: a flag beats the config file, and the config file beats what is beside the function.

The disk is watched while it serves, and three things change under a running server.

- A function directory that appears is served, and one that is deleted goes back to answering 404. The listing is looked at twice a second, which is the debounce upstream waits before it restarts its container.
- A secret that changed is a new runtime, so every kept isolate is thrown away, because an isolate built with the old `Deno.env` must not answer a call made after it changed.
- The config file being written is read again, so a block that switches a function off takes it out of the listing, and a `policy` or an `inspector_port` that moved applies from then on.

Editing a function's own source is not in that list, because it is already handled a layer down: a kept isolate ends itself when a file it was built out of changes, which is the hot reload the policy section covers, and that includes anything the function imports from `_shared`.

The ports are the one thing a reload cannot move.
This process is already listening on one and already telling every function about the other, so changing either in the config file needs the command restarted.

## Deploying them

A laptop serves functions off a directory. A node serving a thousand projects has none of their directories, so a deploy is what turns the first into the second: the files go into the project's own prefix on the store, and a node that brings the project up reads them back out.

```
zou functions deploy --target s3://bucket --ref acme
```

```
deployed hello to acme on s3://bucket
2 files, 2 of them new, 118 bytes uploaded
  /functions/v1/hello
```

Names limit it to some of them, and no name at all is all of them, which is what `supabase functions deploy` with no slug does.
A deploy is a merge, so deploying one function leaves the others where they are.
What is deployed is what would have been served: a function `enabled = false` switches off is not deployed, and a directory with no entrypoint in it is not either.

| Flag | What it does |
| --- | --- |
| `--target <store>` | The store the project lives on, or `ZOU_TARGET`. |
| `--ref <tenant>` | Which project on it, or `ZOU_TENANT`, or the config file's `project_id`, which is the field upstream's `--project-ref` names. |
| `--import-map <path>` | One map for every function of this deploy, the same as on `serve`. |
| `--no-verify-jwt` | Deploy them callable without a token. |
| `--config <path>`, `--no-config` | Which `config.toml` to read, or none at all. |

`zou functions list` prints what is deployed right now, which is the question worth asking before changing it.

What a deploy carries is the function's own directory and every `_`-prefixed directory beside it, which is the shared code convention, plus whatever the config file pointed `entrypoint` and `import_map` at.
For a function the config file named rather than the listing, the function's own directory is the one its entrypoint is in.
Remote imports stay remote: an `npm:`, `jsr:` or `https:` specifier is resolved by the node that runs the function, through the module cache it already has.
A `.env` is never carried, and neither is anything else whose name starts with a dot.
The secrets a deployed function gets come from the project rather than from whatever was on the laptop that ran the deploy.

In the store it looks like this.

```text
tenants/<ref>/functions/DEPLOYED           the names and what each is made of
tenants/<ref>/functions/blobs/<sha256>     the bytes, once each
```

Files are addressed by the hash of their contents, so a redeploy of a project where one file changed writes one object, and a rollback is a manifest naming older hashes.
`DEPLOYED` is swapped with a compare and swap, so two people deploying at once resolve to one of them rather than to half of each.
It is the second mutable object in a tenant's prefix and it is deliberately not part of the database manifest: a deploy happens from a laptop while a postmaster somewhere else holds the writer lease, and the two must not be able to overwrite each other's work.

A node brings a deployment back to a directory under the project's runtime directory and serves it through the same listing reader a laptop uses, so a deployed project and a local one differ in where the files came from and in nothing after that.
Every file is checked against the hash that named it on the way in.
A deployment that cannot be read is logged and the project comes up without it, because the database is why a project is being attached and functions that will not load should not be able to hold it down.

`SUPABASE_URL` inside a deployed function is the project's own url, `https://<ref>.<domain>` on a node that was given a domain, and not the address of the machine it happens to be running on.

A redeploy is picked up by a node that is already serving the project, on the next request, without the project being detached and without its database being touched.

How it knows is a counter on the project's registry entry, `deployed`, which `zou functions deploy` raises with a compare and swap once the new deployment is the one in the store.
A node resolves a request through that entry already, out of a cache with a sixty second life, so a project nobody deployed to costs no store request at all and one atomic load per request.
That is the reason it is a counter on an object already being read rather than a poll of the deployment or a message to every node: at a thousand projects per node, a poll is a thousand store requests a minute for a question whose answer is almost always no.
The cost of the sixty seconds is that a deploy can take up to that long to be seen by a node whose cache is warm, and no longer.

Picking it up is the deployment being materialized again, into a directory of its own beside the one it replaces, and moved into the registry the project's front door is already holding.
The front door is not rebuilt, so no session talking to the database notices anything, and a call already inside an isolate finishes on the deployment it started on.
One generation of files is kept behind the current one for that reason, and the one before that is removed.
The exception is a project that had nothing deployed to it when the node brought it up: there is no functions door in front of it to move a deployment into, so the node lets go of the project instead and the request after that one attaches it with what was deployed.

## Serving what was deployed

The dev loop serves a directory. Given a store and a project it serves what a deploy wrote instead.

```
zou functions serve --target s3://bucket --ref acme
```

```
serving what is deployed to acme on s3://bucket
its files are at /tmp/zou-deployed-acme-54321
function hello at /tmp/zou-deployed-acme-54321/functions/hello/index.ts
serving functions on http://127.0.0.1:54321
```

This is the same read a node does at attach, through the same `materialize`, the same hash check on every file and the same listing reader over what it wrote, which is the point of it: it is how somebody checks a deployment without standing a node up, and it needs no database.
Neither flag is upstream's, because upstream's dev loop only ever serves a directory.
Only the flags switch it and not `ZOU_TARGET` in the environment, because a person with a store exported who runs the dev loop in their project means the project.

What is served is what the store says, so nothing is watched.
Editing a file under that directory changes a copy of a deployment and not the deployment, and the way a deployment changes is another deploy and another serve.
The directory is named for the project and the port, so a second serve of the same deployment on another port gets its own copy, and it is emptied first, because a file left over from a deployment somebody has since replaced is not part of the answer.

A project's secrets come out of its own prefix here, the way they do on a node, and not from a `.env` on this disk.
A project that has secrets and a process with no `ZOU_SECRET_KEY` to open them with serves nothing and says why.
The four variables above them are this process's own, so `SUPABASE_URL` is the port it is serving on.

The conformance suite for functions is run twice in CI for this reason: once against a project directory and once against the same project deployed to a store and served back out of it, with the same 22 questions and the same file.
A deployment that answered differently would be a deployment that is not the project.

## config.toml

The same file the project already has.

```toml
[edge_runtime]
policy = "per_worker"
inspector_port = 8083
deno_version = 2

[functions.hello]
verify_jwt = false

[functions.private]
enabled = false

[functions.other]
entrypoint = "./functions/other/main.ts"
import_map = "./functions/import_map.json"
static_files = ["./functions/other/index.html"]
```

`enabled = false` is not a refusal, it is an absence: the function is not served at all and its url answers the same 404 as a name nobody wrote, which is what `supabase functions serve` does when it prints `Skipped serving Function: hello`.

`verify_jwt` is on unless the block says otherwise.
A function that configures nothing is a function nobody can call without a key, rather than one anybody can.

`entrypoint` moves the file the runtime starts at, and the function is still called by the block's name.
A block with an entrypoint is a function even when there is no directory under `functions/` with that name, and what a deploy carries for one is the directory the entrypoint is in.
`import_map` is one of the places a map is looked for, and the first one, which the import maps section covers.
`static_files` is what the function may read off the disk, which the static files section covers.

`policy` is honoured, and `inspector_port` opens the port a debugger attaches to, which the debugger section covers.

`deno_version` is which Deno the CLI's runtime is, and upstream has two answers: `1` pulls an older image and `2` is what the current one runs.
This server has one runtime and it is the second, so `deno_version = 2` is read and anything else is listed by `zou status` as unread rather than quietly obeyed.
What a function is told from the other side is `Deno.version`, which reads `zou-<version> (compatible with Deno v2.1.4)` with the real v8 beside it, and that is the shape upstream says it in: measured on `supabase/edge-runtime` 1.74.2, `Deno.version.deno` there is `supabase-edge-runtime-1.74.2 (compatible with Deno v2.1.4)`.
Neither is a bare version number, so a function that parses one is already wrong upstream.

Anything under `[functions.<name>]` this server does not know is listed by `zou status` as unread, the same as anywhere else in the file.

## What a function keeps between calls

That is the policy, and it is upstream's two words.

`per_worker` is the default, here and upstream, and it keeps one isolate per function and calls it again.
So whatever the module built at the top of itself is still there on the second call: a database client, a compiled regular expression, a cache.
The cold start is paid once.

`oneshot` throws the isolate away after the call, so every call starts from nothing and nothing a call leaves behind can reach the next one.
It is what the hosted service does and what upstream's own config file calls the fallback.

A kept isolate is not a kept call.
The clocks start again for each call, the request is the one that arrived, and `SB_EXECUTION_ID` is that invocation's own rather than the one the isolate was built for.
What is not reset is memory, which belongs to the isolate for as long as it lives, so a function that leaks reaches the memory limit eventually rather than never.

Three things end a kept isolate, and none of them is a handler that threw.

- A limit reached. A terminated isolate is not somewhere the next call should start from.
- A file it was built out of changing, which is hot reload, and which is what the CLI's own config file says `per_worker` is for. Every file off this disk that went into the isolate counts, so editing something under `_shared` reloads every function that imports it.
- A minute with nobody calling. That number is this project's rather than upstream's, and what it trades is a cold start nobody is waiting on against a quarter of a gigabyte of address space per function nobody is calling.

An isolate is one thread, because a v8 isolate is thread bound.
A call that arrives while that thread is busy gets an isolate of its own rather than queueing behind the call in front of it, and the extra ones go away when the burst is over.

## A debugger

`inspector_port` in `[edge_runtime]` opens a port, and what answers on it is the Chrome DevTools Protocol, which is what Chrome's `chrome://inspect`, VS Code's attach configuration and every other debugger that talks to node or to Deno already speak.

```toml
[edge_runtime]
inspector_port = 8083
```

`GET /json/version` says what is answering, and `GET /json/list` is the isolates that are running, one line each with the websocket url to connect to.
A function appears in that list once it has been called and leaves it when its isolate goes, so the list is what is running rather than what is deployed.
Under `per_worker` an isolate is still there between calls, which is what makes it possible to attach after a call, set a breakpoint, and have the next call stop on it.

The port is bound on `127.0.0.1` and nowhere else.
A session evaluates arbitrary javascript inside an isolate holding the project's secrets, so it is a shell on the process, and a line in a config file should not be able to open one to the network by accident.

A function with a debugger attached runs without the wall clock and cpu limits, because a breakpoint and a time limit are contradictory.
The memory limit is unchanged.

`zou functions serve --inspect` opens the same port without the file saying anything, and `--inspect 9229` names another, which is for a debugger somebody attaches once rather than a port a project always opens.

There is no `--inspect-brk`.
Upstream has one as a flag on `supabase functions serve`, not as a setting in the config file, and holding the first request until somebody attaches is still to be written.

## Verification

With `verify_jwt` on, the call needs an `Authorization: Bearer <token>` this project can verify.
The refusals are upstream's, byte for byte, `sb-error-code` header included, and `access-control-expose-headers: sb-error-code` is on them so a browser can read the code.

| What arrived | Status | `sb-error-code` |
| --- | --- | --- |
| No header | 401 | `UNAUTHORIZED_NO_AUTH_HEADER` |
| Not three segments, or a header that is not json, or no `alg` | 401 | `UNAUTHORIZED_INVALID_JWT_FORMAT` |
| `alg` is `HS256` and the signature does not check out | 401 | `UNAUTHORIZED_LEGACY_JWT` |
| `alg` is `RS256` or `ES256` | 401 | `UNAUTHORIZED_ASYMMETRIC_JWT` |
| Any other `alg` | 401 | `UNAUTHORIZED_UNSUPPORTED_TOKEN_ALGORITHM` |

The body is upstream's three fields, `code`, `message` and `msg`, with the same sentence in the last two.

The code the caller gets is decided by the token's header alone and not by why it failed, which is upstream's behaviour and was measured rather than guessed: a well formed `RS256` token this project has never seen and a garbage one both come back `UNAUTHORIZED_ASYMMETRIC_JWT`.

## CORS

A preflight to `/functions/v1/<name>` is the function's to answer, so the `_shared/cors.ts` pattern every Supabase example is written around works here as written.

```ts
import { corsHeaders } from "../_shared/cors.ts";

Deno.serve((req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }
  return new Response(JSON.stringify({ hello: "world" }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
});
```

An `OPTIONS` is never checked against `verify_jwt`, whatever the function's setting, because a browser sends no `Authorization` on a preflight.
Any other method is checked as usual, and the check is on the method alone, which is what upstream's runtime does: the same call sent as `HEAD` with no token is `UNAUTHORIZED_NO_AUTH_HEADER`.

Whatever CORS headers a function sets are the ones the caller gets, on a preflight and on an answer both.
A function that allows one origin still allows one origin.

A function that says nothing about CORS gets an answer from the server instead, so a project that never wrote a `cors.ts` still works from a browser:

| | |
| --- | --- |
| Status | 204 |
| `Access-Control-Allow-Origin` | the caller's `Origin`, mirrored |
| `Access-Control-Allow-Credentials` | `true` |
| `Access-Control-Allow-Methods` | `GET, POST, PATCH, PUT, DELETE, OPTIONS, HEAD` |
| `Access-Control-Allow-Headers` | whatever the preflight asked for |
| `Access-Control-Max-Age` | `86400` |

That is the same answer every other surface here gives a preflight, and a name nobody deployed gets it too, rather than the 404 the call after it would get.

Upstream is two different things here and this is neither of them exactly, so the differences are worth stating.
On `supabase start` the gateway answers every preflight itself, function or not, with a 200, `Access-Control-Allow-Origin: *`, and `GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS,TRACE,CONNECT`, and a function's own `OPTIONS` handler never runs.
That gateway also replaces the `Access-Control-Allow-Origin` a function set with `*` on ordinary answers, which quietly widens a function that meant to allow one origin.
The hosted platform does the other thing: the runtime hands the `OPTIONS` to the function, adds no header of its own, and a function that handles no CORS is one a browser cannot call, which is why the pattern above is in every example.
Here the function is authoritative like the hosted platform, and the server answers for a function that did not like the local stack, and nothing a function says about an origin is ever widened.

## How a function says what to run

Three ways, all three of them upstream's, because most of the functions people already have were written against the second or the third.

```ts
Deno.serve((req) => new Response("one"))
```

```ts
export default {
  fetch(req: Request) {
    return new Response("two")
  },
}
```

```ts
import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
serve((req) => new Response("three"))
```

The third is the one every example written before `Deno.serve` existed uses, and it is a socket rather than a handler: it calls `Deno.listen`, accepts a connection, hands it to `Deno.serveHttp`, pulls a request off with `nextRequest` and answers it with `respondWith`.
There is no socket under a function here, because the server holds the only one, so `Deno.listen` and `Deno.serveHttp` are that shape with one call going through them.
Which is enough for the real `std/http/server.ts`, over `https://deno.land/std@0.168.0/http/server.ts` or `jsr:@std/http/server`, and that is what is tested rather than a copy of it.

A module that does two of them at once is answered by the one that took the socket, which was measured a pair at a time against the runtime: `Deno.serve` beats both of the others and a listener beats a default export.
So it is one rule and not three.
The default export is what runs when nobody took the socket.

A module that does none of the three is an error naming the three.
Upstream holds the request until the wall clock and kills the worker with `request has been cancelled by supervisor`, which tells a developer a timeout rather than what is wrong.

## What the handler is given

`req.url` is the url the runtime was reached on, `http://127.0.0.1:<port>/functions/v1/<name>` with the path after the name and the query string on it, not the public url a caller typed.
That is upstream's behaviour in the local stack, where a gateway sits in front, and it is kept here so that a function written against one works against the other.
A function that needs the public url should read `x-forwarded-host` and `x-forwarded-proto`, which are what the request that reached the front door said.

The headers a function is told about:

- `x-forwarded-host`, `x-forwarded-port`, `x-forwarded-proto`, `x-forwarded-path` and `x-forwarded-prefix`, describing the request as it arrived.
- `x-real-ip`, the caller's address, which is also `info.remoteAddr.hostname` in the handler.
- Everything the caller sent, `Authorization` included, lowercased, which is what a `Headers` object gives javascript back.

What a proxy in front already said is kept rather than overwritten.

The body has a ceiling of 20 MiB, which is zou's own number and not upstream's.

## Deno.env

Seven variables are the project's and are the same on every call.

| Variable | What it is |
| --- | --- |
| `SUPABASE_URL` | This server, which is what a function's own supabase client should be built with |
| `SUPABASE_ANON_KEY` | The project's anon key |
| `SUPABASE_SERVICE_ROLE_KEY` | The service role key, which bypasses row level security, so a function that uses it is deciding who may do what by itself |
| `SUPABASE_DB_URL` | The database, as a url |
| `SUPABASE_PUBLISHABLE_KEYS` | `{"default": <the anon key>}` |
| `SUPABASE_SECRET_KEYS` | `{"default": <the service role key>}` |
| `SUPABASE_JWKS_URL` | Where this project publishes the public half of its signing keys |

The middle two are the newer names, and they are a map because a project can have several keys with a name each and a library picks one out by name.
`npm:@supabase/server` reads `SUPABASE_PUBLISHABLE_KEY` first and then the `default` entry of the plural, and it builds its client before a handler runs, so a function that uses it and finds neither refuses the request before anybody's code has said anything.

The values are this project's own keys rather than `sb_publishable_` and `sb_secret_` strings.
zou does not issue keys in that format and writing the prefix onto something that is not one would be a claim about a format nothing here implements.
A library treats the value as opaque and sends it back as the apikey header, which is a key this server accepts, so the round trip works either way.

The last one is for a function that wants to know who called it without asking the server.
`npm:@supabase/server`'s `{ auth: "user" }` verifies the caller's access token itself, against a key set it reads from `SUPABASE_JWKS` or fetches from `SUPABASE_JWKS_URL`, and with neither set it refuses every caller before the handler runs.
The url and not the key set inline, because a url survives a key rotation and because the endpoint it names is the one endpoint on the whole surface that needs no apikey, which is what makes it fetchable from inside an isolate.

A project's access tokens are signed with a P-256 key derived from the project's own secret, ES256 with the key named in the header, and the public half is what that url serves.
That is the arrangement a project created on Supabase today has, and it is what makes verifying a caller possible at all: nothing can verify an HS256 token without holding the secret that signed it, so a token signed that way is a token only the server itself can check.
Keys issued under the legacy format keep working, apikeys included, because a token with no key named in it is still verified against the secret.

`SB_EXECUTION_ID` is one per invocation, which is what ties a log line from inside a function to the request that caused it.

On `supabase start` it is one per worker rather than one per call.
The local runtime sets it in the worker's environment when the worker is made, and a worker is kept between calls, so a function called five times a second apart is handed one id five times.
Both are a uuid and both are set on every call, and the difference is only visible to a function that logs it or returns it, which is the case the conformance suite asks about and asserts on both sides.

The environment this server process was started with is not in it, which matters because that environment is holding a database password and a function is somebody else's code.
`Deno.env.set` and `Deno.env.delete` throw: these are a project's settings rather than a shell.

## Secrets

A project's own variables go underneath those, from two places.

`supabase/functions/.env` is the first, and it is read with no flag and no command.

```
STRIPE_KEY=sk_test_51H
GREETING=hello
```

The format is the one the Supabase CLI parses, which is dotenv.
`NAME=value` and `NAME: value` both work, `export NAME=value` works, a `#` starts a comment, and a `#` with a space before it ends an unquoted value.
Single quotes are literal, double quotes unescape `\n` and `\r` and may span lines, and either kind of quote is not part of the value.
`$NAME` and `${NAME}` expand from names set earlier in the same file, never from this server's own environment, and a name nothing set expands to nothing.

`[edge_runtime.secrets]` in `config.toml` is the second, and it is where a value can stay out of the repository.

```toml
[edge_runtime.secrets]
STRIPE_KEY = "env(STRIPE_KEY)"
```

`env(NAME)` is read from the environment `zou dev` was started with, so the name is committed and the secret is not.
A name the environment does not have is left out rather than handed to a function as an empty string.
The `.env` file wins over this block when both name the same variable, which is upstream's order.

A name starting with `SUPABASE_` is refused from either place, with a line in the log saying which one was skipped.
Those belong to the server, and the five above are the five.

The names that arrived are printed at boot, without their values, so a project can see whether its file was found.

## Secrets in a deployment

A deployed project has no `.env` beside it, because nothing whose name starts with a dot is deployed.
Its secrets are an object in its own prefix, and the object is sealed.

```
zou secrets key
zou secrets set STRIPE_KEY=sk_live_51H GREETING=hello --target ./store --ref acme
zou secrets list --target ./store --ref acme
zou secrets unset GREETING --target ./store --ref acme
```

```
NAME                             DIGEST
STRIPE_KEY                       f52fbd32b2b3b86f
```

`--target` and `--ref` come off `ZOU_TARGET` and `ZOU_TENANT` when they are not passed, and the ref falls back to the config file's `project_id`, which is the same precedence `zou functions deploy` has.
`set` also takes `--env-file <path>`, read with the same dotenv parser the dev loop reads `supabase/functions/.env` with, so the file a project has been running against locally is a file it can deploy.
No verb prints a value, because a command that prints a secret is one somebody eventually runs in a shared terminal.
The digest is the first eight bytes of the sha256 of the value, as hex, which is enough to check that what is set is what was meant without being enough to be worth stealing.

### The key

`ZOU_SECRET_KEY` is thirty two bytes as base64 or hex, or `ZOU_SECRET_KEY_FILE` names a file holding the same thing, and `zou secrets key` prints a new one.
The file wins where both are set, because a file is what a secret manager mounts and a variable is what a person exports.
There is deliberately no `--secret-key` flag: an argument is in `ps` output and in the shell history of whoever ran it.

The root key encrypts nothing.
Every project gets its own, derived as `HMAC-SHA256(root, "zou/functions/secrets/1/<ref>")`, which is one HKDF expand step with the label written out.
The same label is the associated data, so a ciphertext lifted out of one project's prefix and dropped into another's does not open.
The cipher is ChaCha20-Poly1305, chosen over AES-GCM because it is constant time in software on every machine a node might run on, and a fleet should not have a security property that depends on whether the box has AES-NI.

### What is in the object

```text
tenants/<ref>/functions/SECRETS
```

A nonce, a ciphertext and the time of the last change.
The names and the values are sealed together rather than one value at a time, because names leak in a per value scheme and `STRIPE_SECRET_KEY` tells somebody what a project is worth breaking into.
The time is in the clear on purpose: an operator asking how old a project's environment is should not need the key.

The key is not in the store, which is the point.
A database on object storage is a database whose bytes are somebody else's problem to keep, so a bucket is a thing that can be copied without the copy being noticed, and secrets written in the clear next to the data they unlock are what would make that copy worth having.

### What a node does with them

A node reads and opens them when it attaches the project, and hands them to the functions underneath the five the server sets, the same order the dev loop stacks them in.
The names are logged and the values are not.

A project with no secrets needs no key on the node at all, which is most projects.
A project that has them on a node that has none is not served: every name answers the 404 upstream answers for a name nobody deployed, and the log line says why.
Serving them anyway would be a function running without the environment it was written against, which is a function calling somebody else's api with an empty token.

Like a deployment, the secrets are read at attach, so a change is picked up the next time that happens.

## Typescript

Real typescript, through the same swc transpiler Deno itself uses, so what runs here is what would run there.
Interfaces, enums, generics, decorators and `.tsx` all arrive as javascript before v8 sees them.

`Deno.version.typescript` says `5.3.3`, and what that number means is the highest release whose syntax is tested here, one test per release in `crates/zou-deno/tests/typescript.rs`.
`satisfies` and `accessor` fields from 4.9, `const` type parameters and decorators from 5.0, `using` from 5.2, and import attributes from 5.3.
The number moves when a test for a later release's syntax is written, and not before.

Two of those are not type stripping.
A decorator is a call that has to happen and an `accessor` field is a getter, a setter and a private field that v8 does not make on its own, so both are a transform, and the decorators are the TC39 proposal rather than typescript's older `experimentalDecorators`, which is Deno's default and what upstream's runtime was measured doing.

A function can import the files beside it.

```ts
import { corsHeaders } from "../_shared/cors.ts"
import settings from "./settings.json" with { type: "json" }
```

## Packages

`npm:` and `jsr:` work, and so does an ordinary `https:` url.

```ts
import { z } from "npm:zod@3.23.8"
import { encodeHex } from "jsr:@std/encoding@1/hex"
```

They are not resolved the way Deno resolves them.
There is no node module resolution here, no `package.json` walk and no CJS.
Both specifiers are rewritten to a url on a registry that serves packages as modules, `esm.sh` by default, and from there a package is an ordinary graph of `https:` imports.
What runs is the registry's build of the package rather than the tarball npm would have unpacked, which is worth knowing before reporting a difference in behaviour to a package's author.

- Pin the version. `npm:zod@3.23.8` is a version, `npm:zod` is whatever the registry thinks latest is on the day the cache is cold.
- A package that reaches for a node built in runs if the built in is one of the ones below, and is refused by the name of the one it wanted if it is not.
- The loader asks the registry as `zou-edge-runtime`, and that decides which build comes back. esm.sh serves a Deno agent the build it makes for Deno, which is the build a package author tested on a Deno runtime, and serves anything else the build it makes for a browser, with the platform bits stubbed out. The browser build is the one that links here, and the reason is measured rather than argued: the Supabase examples corpus, run on the same machine on the same afternoon against both, ran thirty two of forty functions asking as this and twenty five asking as Deno. The seven it costs are packages whose Deno build imports `node:child_process`, `node:diagnostics_channel` or `node:module`, and four of the seven want to start a process, which is not something a function here is ever going to do. What asking as itself costs is the other direction: a package whose browser build needs something a browser has and this does not, such as a `.wasm` esbuild will not bundle, is a 500 from the registry rather than a module.
- The build asked for is the unminified one, `?dev` on esm.sh, because a minified class is a class with a one letter name and a library that reports its own names reports letters. What that is worth was counted on the same corpus, one binary and two runs: nine of the forty functions produce an error whose text carries a minified name under the registry's default build and the author's name under this one, `new he` becoming `new Resend`, `new $e` becoming `new Bot`, `i._setAuthenticator` becoming `_Stripe._setAuthenticator`, and `custom-jwt-validation` answering with `JOSENotSupported: Unsupported "alg" value for a JSON Web Key Set` where before it answered `I:`, which is the shape upstream answers with. Nothing else moved: the same forty statuses on both runs, one row of forty different in its body, and that row is the one that got its name back. What it costs is a bigger cache and a slower cold start, measured on the same corpus at six megabytes on thirty six and a boot that is under twenty milliseconds a function slower once the cache is warm. `ZOU_MODULE_BUILD` names the query to ask with, and an empty value asks for nothing, which is what a mirror that is not esm.sh and has its own idea of what a query means wants.
- A build the registry could not make is asked for again as the one it can. esm.sh answers 500 for `@vercel/og` asked as a browser, because that build hands esbuild a `.wasm` and esbuild has no loader for one, so a 5xx from the registry is asked again as Deno and what comes back is the `denonext` build. The modules it re exports name the build in their own path, so the rest of that package's graph arrives without asking twice, and a package the registry can build for a browser never takes this path at all.
- `@supabase/supabase-js` runs. `createClient` builds its auth, storage, functions and realtime clients, and the realtime one is a `WebSocket`, which is why that had to exist before this line could say so.
- `http:` is refused. A module arrives and is executed, so it arrives over https.
- `data:` is not supported yet.
- The slash after the scheme is allowed. `npm:/drizzle-orm@0.29.1/pg-core` is the same specifier as the one without it, which matters because that is the spelling a registry's own build of a package imports itself with rather than one anybody types.
- A declaration file is a module with nothing in it. `import 'jsr:@supabase/functions-js/edge-runtime.d.ts'` is how a project tells its editor what `Deno.serve` is, and there is no runtime code in a `.d.ts` to run and nothing fetched for one.

### Node built ins

`node:` is a specifier here, and what it resolves to is javascript carried in the binary rather than anything on the network.
A function may import one itself, and so may a package the registry served, which is the other reason they exist: the browser build of a package still reaches for `node:buffer` and `node:process` here and there.

```
assert  buffer  child_process  cluster  crypto  diagnostics_channel
events  fs  fs/promises  module  os  path  process  querystring
stream  stream/promises  stream/web  string_decoder  timers
timers/promises  url  util  util/types  worker_threads
```

`path/posix` and `path/win32` are the same module as `path`, which is posix, because there is one file system here and it has one separator.

What each one is is the part of it a package reaches for and not node's whole surface, and the shape of the difference is worth reading before depending on one:

- `buffer` is a real `Buffer`, which is a `Uint8Array` with node's methods on it: the seven encodings, the fixed width readers and writers including the variable width `readUIntBE` pair, `concat`, `compare`, `copy`, `fill`, `indexOf` and the byte swaps.
- `crypto` is `createHash` and `createHmac` over the same digests `crypto.subtle` has, plus `randomBytes`, `randomInt`, `randomUUID` and `timingSafeEqual`, and `webcrypto` is the global one. There are no ciphers behind it, no key derivation and no md5, and each of those is refused by the name that was asked for.
- `fs` and `fs/promises` read and will not write. A read that is not there fails the way node fails it, with `ENOENT` on an error that has `code`, `errno`, `syscall` and `path` on it, because that is what a library branches on.
- `stream` is `Readable`, `Writable`, `Duplex`, `Transform` and `PassThrough` on top of the `events` emitter, with `pipe`, `pipeline`, `finished`, the async iterator and the bridges to and from a web stream. None of node's internals are under it, so a package that reaches past the public methods into `_readableState` finds nothing.
- `process` is a global as well as a module, the way it is in upstream's runtime, so a package that sets something on one sees it through the other. `env` is the function's environment and is read only, `stdout` and `stderr` go to the log, `nextTick` is a microtask, and `chdir` and `exit` throw rather than pretending.
- `os` answers about the machine the function is on the way a container does, `util.promisify` reads the custom symbol, `assert` throws an `AssertionError`, and `timers` and `timers/promises` are the globals under their node names.
- `diagnostics_channel` is a real one: named channels, subscribers, `publish`, `hasSubscribers` and the tracing channel wrapper. Nothing here subscribes to anything, so a library instrumenting itself through it publishes into channels nobody is on, which is what it is for.
- `module` is `createRequire`, `builtinModules` and `isBuiltin`. There is no CJS here, so the require it hands back serves the built ins and names anything else as a module it cannot find, which is the answer a package feature detecting its way onto node is asking for.
- `child_process`, `worker_threads` and `cluster` import and then refuse. Every call that would need a process, a thread or a fork throws with a sentence saying a function has none, and the parts that can be true are: `isMainThread` is true, `isPrimary` is true, and `worker_threads` hands back the platform's own `MessageChannel`.

Those last three are worth a word, because a module that exists and throws looks like the worse of the two options and is not.
A package that imports one at the top and calls into it in a branch nobody takes runs perfectly well against a stub and does not load at all against an import that is refused, and that shape is the difference between the two registry builds on seven of the forty functions in the examples corpus.
A built in nobody has written is still refused when the module is resolved, by name, so `import "node:dgram"` says there is no `dgram` here rather than failing at the first call into it.

### A package's own files

A package sometimes ships a file that is not a module and reads it itself, which is what a wasm library does with its `.wasm`.

```ts
const bytes = await Deno.readFile(
  new URL("magick.wasm", import.meta.resolve("npm:@imagemagick/magick-wasm@^0")),
)
```

Upstream unpacks a tarball into a directory, so that line is a file beside a file.
Here a package is a url, so it is a url beside a url, and two things make it work.

`import.meta.resolve` of a package answers with the module the registry served rather than with the specifier that was asked for.
A version range is a name for a package and not a place, and `new URL('magick.wasm', ...)` resolves against a place: esm.sh answers `@imagemagick/magick-wasm@^0` with a module that says in `x-esm-path` which build of which version it is, and that is the url this answers with.
A registry that says nothing answers with wherever the fetch landed, which is the redirect it followed if it followed one.

`Deno.readFile` of an `http:` or `https:` url reads it through the same cache the modules are fetched into, so the file is fetched once and a second cold start has it.
This is not a new thing for a function to be able to reach: a function has `fetch`, and this is the same reach through a cache that is already paid for.
The synchronous spellings serve what has been fetched already and will not start a download while a handler is waiting on one.
While the module is still being loaded they will, because a package reading its own wasm with `readFileSync` at the top of itself is the ordinary shape and there is nothing else for the isolate to be doing: the module load either side of that read is a blocking fetch on the same thread.

A package that goes the other way and asks for a path rather than a url gets the url back.

```ts
import { fileURLToPath } from "node:url"
readFileSync(fileURLToPath(new URL("./resvg.wasm", import.meta.url)))
```

That is what `@vercel/og` does, and on node it is a path because the package is a directory.
Here it is a url, so `fileURLToPath` of an `http:` or `https:` url answers with the url rather than throwing, and the read that follows takes it.
A url that is neither a file nor http is still `ERR_INVALID_URL_SCHEME`, the way node spells it.

Everything fetched is kept on disk, keyed by url, so only the first cold start pays for it.

- `ZOU_MODULE_CACHE` is where, and defaults to `$XDG_CACHE_HOME/zou/modules` or `~/.cache/zou/modules`.
- `ZOU_MODULE_CACHE_ONLY=1` means this server does not fetch. A module that is not in the cache is refused by name rather than reached for, which is what a deployment that warmed its cache somewhere else wants.
- `ZOU_MODULE_REGISTRY` points `npm:` and `jsr:` at a mirror instead of esm.sh.
- `ZOU_MODULE_BUILD` is the query a package is asked for with, `dev` by default, and empty for whatever the registry serves without being asked. A cache is keyed by the url, so a cache warmed one way and read the other way fetches again rather than serving the other build.
- `ZOU_MODULE_AGENT` is who the registry is asked as, and is nobody by default. esm.sh reads the user agent and serves a different build for it, a browser one that stubs the platform out and a Deno one that imports `node:`, and which of the two runs more of somebody's code is a thing to measure rather than to assume. `ZOU_MODULE_AGENT=deno` is the runtime's own agent without writing the string out, and any other value is sent as it stands. Setting it also turns the 5xx fallback off, since the fallback is the ask this replaces.

## Import maps

A bare name in an import is whatever the function's map says it is.

```json
{
  "imports": {
    "zod": "npm:zod@3.23.8",
    "std/": "jsr:@std/",
    "@/": "./lib/"
  }
}
```

```ts
import { z } from "zod"
import { encodeHex } from "std/encoding/hex"
import { greet } from "@/greet.ts"
```

The map is found where the CLI looks for it, in this order, and the first one that exists is the one used.

- `import_map` under `[functions.<name>]`, relative to the project directory.
- `functions/<name>/deno.json`, then `functions/<name>/deno.jsonc`.
- `functions/<name>/import_map.json`, which is deprecated and logs a line saying `deno.json` replaces it.
- `functions/import_map.json`, the project's shared map, which is deprecated the same way and logs a line saying a `deno.json` beside the function replaces it.

A function with no map at all runs, it just has no bare names.

What is in the map is the import maps specification, not all of it.

- The longest matching key wins, so `@/deep/one.ts` takes `@/deep/` over `@/`.
- A key that ends in `/` is a prefix and the rest of the specifier is appended, and a key that does not is an exact match.
- `scopes` are consulted before `imports`, innermost first, so a package that needs its own version of something can have it.
- An entry where only one of the key and the address ends in `/` is dropped with a log line, which is what the specification says to do with it.
- A relative address is resolved against the directory the map is in, not the file doing the importing.
- `npm:`, `jsr:` and `https:` addresses mean on the other side of the map exactly what they mean in an import, so everything the packages section says still applies.

The file is read as JSONC, comments and trailing commas and all, because `deno.json` is a file people comment.
A map that is only `{"importMap": "./other.json"}` is a reference and the other file is read instead, one step and no further.

The map is one of the files the function is built out of, so editing it under `per_worker` reloads the function the same as editing a module.
A map that is not valid json is the call's error, by the name of the file, rather than something the server refuses at boot.

## Static files

A function may read the files its own `static_files` covers, and nothing else on the disk.

```toml
[functions.hello]
static_files = ["./functions/hello/*.html"]
```

```ts
Deno.serve(async () => {
  const page = await Deno.readTextFile("./page.html")
  return new Response(page, { headers: { "content-type": "text/html" } })
})
```

`Deno.readFile`, `Deno.readTextFile` and the two `Sync` spellings of them are here, and they are the whole of the file system a function has.
There is no writing, no listing a directory, no `Deno.open` and no stat.
The same four calls read an `https:` url, which is a package reading a file of its own and is written down in the packages section rather than here: the patterns below are about the disk.

A relative name starts at the directory the entrypoint is in, which is upstream's `servicePath`, so `./page.html` in `functions/hello/index.ts` is `functions/hello/page.html`.
An absolute name and a `file:` url both work and are matched the same way.
A name is tidied before it is matched, so `..` cannot walk out of what the patterns cover and back in through another door.

The patterns are the ones upstream's deploy path globs with, character for character.

- `*` matches inside one path segment and `**` matches across them.
- `?` is one character that is not a separator, and `[abc]` is a class that `!` negates.
- Everything else is a literal, so the `.` in `*.html` is a dot.
- The slash after a `**` is a slash the path has to have. `dist/**/*.css` covers `dist/app/main.css` and does not cover `dist/one.css`, which is what the regular expression upstream builds does. A project that wants both writes both patterns.

Two errors, and they are Deno's own, so a function can tell them apart.

- `Deno.errors.NotFound` for a file the patterns cover that is not there.
- `Deno.errors.PermissionDenied` for a name nothing covers, with a message naming the function and saying `static_files`.

A function that configures no `static_files` reads nothing, which is the same rule with an empty list rather than a special case.
That is deliberate and it is upstream's shape too: the process running these functions holds a database superuser connection and a JWT secret, and a function is somebody else's code.

Static files are data rather than code, so editing one is not a reload.
The next call reads the new bytes in the same kept isolate, which is what a page being edited during development should do.

## fetch

A function can call out.

```ts
Deno.serve(async () => {
  const res = await fetch("https://api.example.com/rates", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ pair: "eur/usd" }),
  })
  return Response.json({ ok: res.ok, rate: (await res.json()).rate })
})
```

It is the same HTTP client the server calls a database webhook with, behind an op, rather than a second stack linked in beside the first.
What that means in practice, written down because a difference from Deno is a difference somebody's function will meet:

- The answer is collected before the handler sees it, so a large download is held in memory and the 20 MiB ceiling on a call's body applies to what a function may read back too. `res.body` is a stream over what arrived rather than a stream that is still arriving.
- A call has 30 seconds. Deno waits forever, which is fine for a program somebody is watching and not for a request holding an isolate.
- `res.statusText` is the canonical phrase for the code rather than the one the server wrote, because the client does not keep the reason phrase.
- `res.url` and `res.redirected` are where the answer came from, so a redirect that was followed can be seen. Redirects are followed.
- The `user-agent`, when the function does not set one, is the string `navigator.userAgent` says: `Deno/2.1.4 (variant; zou/<version>)`. That is what upstream sends, measured by having a function on a real `supabase start` fetch an echo, where it read back `Deno/2.1.4 (variant; SupabaseEdgeRuntime/1.74.2)`, which is that runtime's own navigator string. It is not only politeness: a registry serves a different build of a package depending on this header, so a function fetching a module at runtime gets the same one it would have got upstream.
- `http:` and `https:` and nothing else. `file:`, `data:` and a string that is not a url each throw a `TypeError` saying which.
- A request that could not be made at all throws `TypeError: error sending request for url (<url>): <why>`, which is the sentence Deno throws. A 404 or a 500 is an answer and not a throw.
- A signal ends the connection as well as the waiting, including one the call was handed out of the pool rather than opened. See below.

### Giving up on a call

`fetch` takes `init.signal`, and a `Request` built with one carries it, so the ordinary way of bounding a call works.

```ts
Deno.serve(async () => {
  try {
    const res = await fetch("https://api.example.com/rates", { signal: AbortSignal.timeout(2_000) })
    return Response.json(await res.json())
  } catch (e) {
    if (e.name === "TimeoutError") return new Response("the rates service is slow", { status: 504 })
    throw e
  }
})
```

The reasons are the ones a library branches on, and they are the strings a real `supabase start` was measured throwing: a caller that gave up gets `AbortError: The signal has been aborted`, a clock that ran out gets `TimeoutError: Signal timed out.`, and a signal aborted with a reason of its own throws that reason unchanged.
A signal that was already aborted is a call that is never made.

An abort ends the connection and not only the waiting.
The socket is shut down under the thread that was reading it, so the read comes back instead of waiting, the thread is not held for the rest of a call nobody wants, and the server on the other end reads the end of the stream rather than writing an answer into one.
That includes a call made on a connection it was handed rather than one it opened: connections are kept between calls, so the second call to a host is usually on the first call's socket, and it is the socket the call is using that comes down.
The one case that is still the waiting alone is a call tunnelled through a CONNECT proxy, which is a socket the client opens for itself and nothing here configures.

What a function may reach is not restricted.
It can call a metadata endpoint on the machine it is running on, the same as upstream's runtime can and the same as `pg_net` can from inside the database.
A function is the project's own code, and the way to keep it off the local network is a network the server is not on.

### Wasm from a response

`WebAssembly.instantiateStreaming` and `WebAssembly.compileStreaming` take a `Response`, or a promise of one, which is why a wasm package is usually loaded by handing one of them a `fetch`.

```ts
const { instance } = await WebAssembly.instantiateStreaming(fetch(url))
```

Both work here and neither streams.
The response is read, and the bytes go to `WebAssembly.instantiate` or `WebAssembly.compile`, so the module is compiled after it has arrived rather than while it is arriving.
That costs a copy of the module and is invisible to the caller, which is the trade: what a package wants from these is for the call to work.

The checks the spec puts in front of them are here.
A response whose `content-type` is not `application/wasm` is a `TypeError` naming what it was, which is the ordinary failure when a url that used to serve a module now serves an error page, and something that is not a response at all is a `TypeError` naming the call.

## URL

`URL` and `URLSearchParams`, which is how a handler routes on a path and reads a query.

```ts
Deno.serve((req) => {
  const url = new URL(req.url)
  const who = url.searchParams.get("who") ?? "world"
  return Response.json({ path: url.pathname, hello: who })
})
```

The parsing is the same Rust crate the rest of this server parses urls with, which is the same one Deno's `URL` is built on, so a url that comes apart one way there comes apart that way here.
A `Request`'s url is parsed rather than kept as the string it was written as, so `new Request("https://example.com")` has a `url` of `https://example.com/` and `new Request("/one")` throws, both the same as Deno.

One difference: a percent sequence in a query that is not valid utf-8 is left as it was written rather than becoming a replacement character.

## Blob, File and FormData

A blob is bytes with a media type on it, a file is a blob with a name, and a form is what a browser posts.

```ts
Deno.serve(async (req) => {
  const form = await req.formData()
  const file = form.get("file") as File
  return Response.json({ name: file.name, type: file.type, size: file.size })
})
```

Both form encodings are read: `multipart/form-data` and `application/x-www-form-urlencoded`.
A form given to `new Request` or `new Response` as a body is written out as multipart with a boundary, and a `URLSearchParams` given as a body is written out urlencoded, each with the content type that says so.

- The multipart boundary is not random. It is a name with a number on it, and the number is stepped until the boundary appears nowhere in the form, which is a stronger guarantee than a random string and is what a runtime with no randomness yet can offer.
- `blob.stream()` is a stream over the bytes the blob already holds, because a blob is bytes and there is nothing left to wait for.
- A part with no `name` on its content disposition is dropped rather than being an error, so one malformed part does not lose the ones around it.

## Copying a value

`structuredClone` is the deep copy a library reaches for when it does not want its caller's object to change under it, and it is not a spread and not a trip through JSON.

```ts
const settings = structuredClone(given)
settings.headers.set = new Set(["x"])
```

A cycle stays a cycle, a value that appears twice arrives as one object twice, and a `Map`, a `Set`, a `Date`, a `RegExp`, an `ArrayBuffer`, a typed array, a `DataView` and a `BigInt` all arrive as themselves.
A key whose value is `undefined` is still a key, which is the first thing JSON loses.

The copy is v8's own serializer, which is what upstream's is too, so what it refuses and the sentence it refuses with are the same on both servers: a function, a symbol, a `WeakMap` and a proxy all throw a `DataCloneError` reading `()=>1 could not be cloned.` with the value's own inspection in front.

Two things a copy loses, both of them measured on a real `supabase start` rather than decided here.

- A platform object arrives as an empty object. A `Blob`, a `File`, a `Headers`, a `URL`, a `Response` and a stream all keep what they hold where the serializer cannot see it, so the copy is `{}` rather than a copy or a refusal. A library cloning options with a `Blob` in them loses it on both servers.
- A buffer named for transfer is copied and left where it was. `structuredClone(value, { transfer: [buf] })` reads the list and checks it and then does not act on it, so `buf` is still readable afterwards, where a browser and a newer Deno both leave it detached. The checking is not idle: an `ArrayBuffer` and a `MessagePort` are the transferable things on either server, and a stream, a typed array or anything that is not an object in that list is refused by name.
A port is the one of the two that is really moved, and there is a section on it below.

A class instance is a plain object afterwards, a getter becomes the value it returned, an error keeps its name and message and loses what was hung on it, and a hole in an array stays a hole.
All of that is v8 rather than a choice, and all of it is the same on both servers.

## A channel with two ports

`MessageChannel` gives you two ports, and what is posted into one arrives at the other as a `message` event.

```ts
const channel = new MessageChannel()
channel.port1.onmessage = (event) => console.log("heard", event.data)
channel.port2.postMessage({ n: 1 })
```

There is one isolate here and no worker to be on the far side, so both ports are in the same call.
The point of them is not the thread they cross but the queue they are, which is what a library uses when it wants a reader on one side and a writer on the other, and several of them make a channel on the way to something else whether or not they ever send anything across it.

What arrives is a copy taken when it was posted, through the same serializer `structuredClone` uses, so the sender changing the object afterwards changes nothing that arrives and a value that cannot be copied throws a `DataCloneError` at the `postMessage` rather than later.

The rest of it, all measured against the reference runtime.

- Setting `onmessage` starts the port, and a handler set long after the messages were posted still sees them. `addEventListener("message", ...)` on its own does not start anything, and needs a `start()`. Setting `onmessage` to null starts nothing and does not stop a port that was already started.
- A message is delivered ahead of a timer that was set before it, and a message a handler posts is delivered ahead of a timer that same handler sets. Two ports answering each other still let the timers through, one round of messages per turn of the loop, because the round after this one is booked before any handler runs.
- `close()` throws away what was waiting and takes the other end with it. A message posted before the close and not yet delivered is gone, and posting into a closed port is quiet rather than an error.
- A port named in a transfer list is really transferred: it arrives as a fresh port holding the same end, still holding whatever was posted to it before it was sent, and the port it came from reaches nobody afterwards without saying so. That works both through `structuredClone(port, { transfer: [port] })` and through `postMessage(data, [port])`, where the arrived port is in `event.ports`.
- A port that is not in the transfer list is refused rather than arriving as an empty object the way a `Blob` does, with v8's own words, `DataCloneError: Unsupported object type`. A port in its own transfer list is `DataCloneError: Can not transfer self`.

One difference from the reference, which is a difference in when rather than in what.
A message posted from a call is delivered on the microtask queue, so a microtask queued after the `postMessage` runs after the message here and before it upstream.
Everything else about the order is the same, including the two cases above that libraries actually race.

## crypto

Random bytes, a uuid, the four hashes, HMAC, AES and ECDSA over P-256.

```ts
Deno.serve(async (req) => {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(Deno.env.get("WEBHOOK_SECRET")!),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["verify"],
  )
  const sent = Uint8Array.from(atob(req.headers.get("x-signature") ?? ""), (c) => c.charCodeAt(0))
  const ok = await crypto.subtle.verify("HMAC", key, sent, await req.bytes())
  return new Response(ok ? "signed" : "not signed", { status: ok ? 200 : 401 })
})
```

`crypto.getRandomValues` and `crypto.randomUUID` are the operating system's randomness, and the digest and the HMAC are the same Rust crates this server signs its own tokens with.
`crypto.subtle.digest` has `SHA-1`, `SHA-256`, `SHA-384` and `SHA-512`, and `sign` and `verify` are HMAC over those four and ECDSA on P-256 with SHA-256, which is the algorithm an access token is signed under.
`encrypt` and `decrypt` are AES in CBC and in GCM, at all three key lengths, which is what a session cookie or a sealed payload is written with, and `generateKey` and `exportKey` are there for the AES half of that.
A failed decryption is an `OperationError` saying `Decryption failed` and saying nothing else, because which part of the ciphertext was wrong is not something the party holding the wrong key should be told.
Two deliberate narrowings inside AES-GCM: the iv is twelve bytes and the tag is the full 128 bits, and an iv or a tag of another length is refused by name rather than served.
`importKey` reads `raw` and `jwk`, and no other format.
A `jwk` is how a key arrives when it came off a published key set rather than out of a variable: `oct` is bytes for HMAC or AES, and `EC` on P-256 is a key for ECDSA, public when it carries only `x` and `y` and private when it carries `d`.
A private jwk's point is derived from its scalar rather than read off the jwk, so coordinates that disagree with the scalar cannot import as a key that verifies nothing.

The verification is done in Rust, so a wrong signature takes as long to reject as a right one takes to accept, which is not true of a comparison written in javascript.

What is not there is refused by name rather than being undefined: `deriveBits`, `deriveKey`, `wrapKey` and `unwrapKey`, the asymmetric algorithms other than ECDSA on P-256, the curves other than P-256, and the der key formats, which want a parser this has no reason to carry.
One difference from the spec: asking `getRandomValues` for more than 65536 bytes throws a `TypeError` with the spec's message rather than a `QuotaExceededError`, because there is no `DOMException` here yet.

## Timers

`setTimeout`, `setInterval`, `clearTimeout`, `clearInterval` and `queueMicrotask`.

```ts
Deno.serve(async (req) => {
  const slow = await Promise.race([
    fetch("https://example.com/slow").then((res) => res.text()),
    new Promise((resolve) => setTimeout(() => resolve("gave up"), 500)),
  ])
  return new Response(slow)
})
```

The waiting is the host's clock and not a loop, so a handler that awaits a timer costs nothing while it waits.
Clearing a timer cancels the wait rather than marking it unwanted, so `setTimeout(f, 3600_000)` that is cleared a millisecond later is not an hour of anything being held.

- A timer only fires while the call is running. The isolate ends when the handler's answer does, plus whatever `EdgeRuntime.waitUntil` is still waiting for, so a timer that has not come due by then never comes due unless something registered is still holding the call open.
- A module may leave a timer running. It is evaluated by waiting on its own evaluation rather than on an idle event loop, which is the difference between importing `@supabase/supabase-js` and hanging on it, because `createClient` starts a token refresh interval on the way through.
- A callback that throws is logged and the call carries on. It cannot be caught, because whatever set the timer returned before it fired, and ending the process the way Deno does would lose an answer that is already written.
- A delay is the spec's signed 32 bit integer: past `2 ** 31 - 1` it wraps, and anything at or below zero fires as soon as the event loop can get to it. `setTimeout(f, Infinity)` is `setTimeout(f, 0)`, which is what browsers do.
- One difference: `setTimeout("code()")` throws a `TypeError`. Deno evaluates a string there, which is `eval` with a longer name.

## EdgeRuntime.waitUntil

Work that outlives the response.

```ts
Deno.serve(async (req) => {
  const body = await req.json()
  EdgeRuntime.waitUntil(
    fetch("https://example.com/audit", { method: "POST", body: JSON.stringify(body) }),
  )
  return new Response("accepted", { status: 202 })
})
```

The caller is answered as soon as the handler returns a `Response`, and what was registered here keeps running afterwards.
Work registered from inside work counts too, so a promise chain that ends in another `waitUntil` is waited for as well.

- A rejection is logged and nothing else. Whatever registered the work has returned by then and the answer is already on its way, so there is nobody left to tell.
- Thirty seconds is the budget. Work still running after that is dropped and the drop is logged, because the isolate holding it is memory and the thread running it is a real thread, and a promise nobody resolves is a thing a function can write by accident. The call's own wall clock is still running underneath it, so the background work gets the shorter of the two.
- An ordinary shutdown waits for it. The work runs on the blocking thread the call already had, so the process does not exit out from under it.
- On Lambda it is best effort. The response goes back to the runtime api first, and an environment that is frozen straight afterwards stops the work where it stands until the next invocation thaws it, or loses it if the environment is destroyed. That is the platform and not the runtime.

## Streams

`ReadableStream`, with a source a function wrote, a reader, `for await`, `tee` and `cancel`.

```ts
Deno.serve(() => {
  const encoder = new TextEncoder()
  let at = 0
  return new Response(
    new ReadableStream({
      pull(controller) {
        at += 1
        if (at > 3) return controller.close()
        controller.enqueue(encoder.encode(`part ${at}\n`))
      },
    }),
  )
})
```

Every body is a stream, whichever way it was made: `req.body` on the request a handler is given, `res.body` on an answer that came back from `fetch`, `.body` on a `Response` built out of a string, and `blob.stream()`.
A body that is not there is `null` rather than an empty stream, so `if (req.body)` means what it says.
Reading the stream is reading the body, so `bodyUsed` is true afterwards and `text()` says `Body already consumed.` rather than answering with nothing.
`clone()` on a request or a response whose body is a stream tees it, which is what makes reading the same body twice work.

- A response whose body is a stream is sent as it is made. The head goes out when the handler returns and every chunk goes out as it is enqueued, with no length counted and so `Transfer-Encoding: chunked` on the wire, which is what a caller reading tokens out of a model needs. A response built any other way is sent whole, the way it always was.
- Eight chunks may be waiting before the function is made to wait too, so a handler generating faster than the caller reads is slowed down rather than allowed to hold the whole body in memory.
- A body that throws after the head has gone out ends where it got to. There is no status code left to change by then, and a chunked body that stops early is what an http client is shown.
- The queueing strategy is a count of chunks and not a size in bytes, so `highWaterMark` is how many chunks are read ahead. `size` is ignored.
- A body stream may only give out bytes. A stream of strings is fine until somebody asks for the body, which is where the `TypeError` is.
- There is no byte stream and no BYOB reader. `new ReadableStream({ type: "bytes" })` and `getReader({ mode: "byob" })` are refused by name.

The writable half is here too: `WritableStream` with a sink a function wrote, its writer, `TransformStream`, and `pipeTo` and `pipeThrough` on a readable.

```ts
Deno.serve((req) => {
  const decoder = new TextDecoder()
  const encoder = new TextEncoder()
  const upper = new TransformStream({
    transform(chunk, controller) {
      controller.enqueue(encoder.encode(decoder.decode(chunk, { stream: true }).toUpperCase()))
    },
  })
  return new Response(req.body.pipeThrough(upper))
})
```

- A sink is `start`, `write`, `close` and `abort`, and a `write` that returns a promise is waited for, so backpressure is the sink taking its time rather than a queue size.
- The queueing strategy is a count of chunks here as well. A writer's `desiredSize` counts down as chunks go in and `ready` resolves when the sink has taken them.
- A sink that throws is the error every later `write`, `close` and `ready` gets, and the writer's `closed` rejects with it. Aborting is the same with the reason the caller gave.
- A pipe ends the way its source ended. A source that closed closes the destination, a source that errored aborts it, and `preventClose`, `preventAbort` and `preventCancel` each stop one half of that.
- `TextDecoderStream` is the one transform that is not here yet, which is why the example above decodes with a `TextDecoder` of its own.
- A `TextDecoder` decodes utf-8, `utf-16le` and `utf-16be`, under the labels the encoding standard gives those three. The legacy single byte pages are not here. utf-16 is because a wasm module compiled by emscripten reads its own heap with it, and a lone byte at the end of a utf-16 buffer decodes to the replacement character rather than throwing.
- `fatal` is not read. A `TextDecoder` here replaces what it cannot decode whether or not it was asked to throw.

## WebSocket

The client half, for a function that talks to something over a socket rather than over a request.

```ts
Deno.serve(async (req) => {
  const ws = new WebSocket("wss://example.com/socket", ["graphql-ws"])
  const said = await new Promise((answer) => {
    ws.onopen = () => ws.send("hello")
    ws.onmessage = (event) => {
      ws.close(1000, "that is all")
      answer(event.data)
    }
    ws.onerror = (event) => answer(`it did not open: ${event.message}`)
  })
  return new Response(said)
})
```

`open`, `message`, `error` and `close`, as `onopen` and the rest or through `addEventListener`, with `Event`, `MessageEvent`, `CloseEvent` and `ErrorEvent` as their arguments.
`readyState` is the four constants, on the class and on an instance both.

- A socket lives as long as the call does. The isolate ends with the answer, so a function that wants to hear back has to be waiting on the socket when it answers, or to have said so with `EdgeRuntime.waitUntil`. A socket left open when the call ends is closed with it.
- `ws:` and `wss:`, and `http:` and `https:` rewritten into them, which is the spec's own rewrite and means one url in one environment variable is enough. Any other scheme, and a url with a fragment on it, throws before anything is opened.
- Binary arrives as a `Blob` or as an `ArrayBuffer`, whichever `binaryType` says, and `send` takes a string, a `Blob`, an `ArrayBuffer` or a view of one.
- `close(code)` takes 1000 or 3000 to 4999, the codes an application may send. Everything else throws, including the ones only the protocol itself may use.
- A handshake that fails is an `error` event and then a `close` with code 1006, the code that means the connection went away without one being agreed. `send` before the socket is open throws, and after it has closed does nothing.
- 20 MiB is the largest message, which is the ceiling a call's body has for the same reason. The handshake has 30 seconds.
- Pings are answered underneath and are not something a handler is told about. Compression is not negotiated, so `extensions` is empty.
- One difference from the spec: what the constructor throws is a `TypeError` rather than a `SyntaxError`, because there is no `DOMException` here yet.

## Sockets

A tcp connection, which is what a database driver is written against.

```ts
import * as postgres from "https://deno.land/x/postgres@v0.17.0/mod.ts"

const pool = new postgres.Pool(Deno.env.get("SUPABASE_DB_URL"), 3, true)

Deno.serve(async () => {
  const conn = await pool.connect()
  try {
    const { rows } = await conn.queryObject`select now()`
    return Response.json(rows)
  } finally {
    conn.release()
  }
})
```

`Deno.connect` opens one, `Deno.connectTls` opens one with the handshake already done, and `Deno.startTls` puts TLS on a connection that is already open, which is what postgres and every STARTTLS protocol needs: they ask in the clear whether the server speaks it before they speak it.
What comes back has `read`, `write`, `close`, `closeWrite`, `localAddr`, `remoteAddr` and the `readable` and `writable` streams.
`std/io`'s `BufReader` and `BufWriter` are built on `read` and `write` alone, and so is every driver that uses them.

- tcp only. A unix socket is a file on the machine the function is running on rather than somewhere on the network, so `transport: "unix"` is refused by name. It is the same line `Deno.readFile` draws.
- Where a function may connect to is not restricted, the same as `fetch`, and for the same reason: a function is the project's own code, and stopping it opening a socket to a host it may already call over http would be drawing a line that means nothing. The way to keep it off the local network is a network the server is not on.
- An isolate may hold 256 sockets at once. A function that leaks one per call is stopped by a number rather than by the box running out of descriptors, and what it is told says so.
- A connection lives as long as the isolate does, so under `per_worker` a pool survives between calls and under `oneshot` it does not. A socket left open when the isolate goes is closed with it.
- 30 seconds to connect, and another 30 for a handshake. Deno waits forever, which is fine for a program somebody is watching and not for a request holding an isolate.
- A certificate is checked against the Mozilla roots, plus whatever `caCerts` names, which is an array of PEM. Something in there that is not a certificate is refused where it was handed in rather than at a handshake that would have said something about the server instead.
- `read` fills the buffer it was handed and answers how many bytes went in, or `null` at the end of the stream, and it reads at most 64 KiB at a time however large the buffer is. Upstream reads into that buffer directly and this copies once more, which is what a runtime with no detached buffers costs.
- What fails is a `Deno.errors` class with the name Deno gives it, so a driver's retry loop takes the same branch: `ConnectionRefused` for nobody listening, `BadResource` for a connection that has been closed, `TimedOut`, `BrokenPipe`, `ConnectionReset` and the rest.
- Nagle is off on every connection, which is upstream's default too, so `setNoDelay` asks for what is already true and `setKeepAlive` is the operating system's.

## What a function can reach, and what it cannot

Present: `Request`, `Response`, `Headers`, `fetch`, `URL`, `URLSearchParams`, `Blob`, `File`, `FormData`, `crypto`, `setTimeout`, `setInterval`, `clearTimeout`, `clearInterval`, `queueMicrotask`, `EdgeRuntime.waitUntil`, `WebSocket`, `EventTarget`, `Event`, `CustomEvent`, `MessageEvent`, `MessageChannel`, `MessagePort`, `CloseEvent`, `ErrorEvent`, `AbortController`, `AbortSignal`, `DOMException`, `ReadableStream`, `WritableStream`, `TransformStream`, `TextEncoder`, `TextDecoder`, `atob`, `btoa`, `structuredClone`, `console`, `performance`, `navigator`, `Deno.serve`, `Deno.listen`, `Deno.serveHttp`, `Deno.connect`, `Deno.connectTls`, `Deno.startTls`, `Deno.env`, `Deno.readFile`, `Deno.readTextFile`, their two `Sync` spellings, `Deno.errors`, `Deno.build`, `Deno.version` and `Deno.permissions`.

`AbortSignal` is the whole of it: the three statics, `AbortSignal.abort`, `AbortSignal.timeout` and `AbortSignal.any`, as well as what a controller makes.
`fetch` takes one and a `Request` carries one, and the section on giving up on a call says what that does and where it stops.
A timer does not take one, because nothing takes a signal on a timer.

`EventTarget` is the real one and not the three methods a socket needs, so a library that extends it to emit its own events works: `{ once }` and `{ signal }` in the options, `stopImmediatePropagation`, and `dispatchEvent` answering false when a cancelable event was prevented.
There is no tree here, so there is no capture and no bubbling and nothing for `stopPropagation` to stop: one object dispatches to its own listeners.

The global is one of them.
`addEventListener`, `removeEventListener` and `dispatchEvent` are there without a receiver in front of them, `globalThis instanceof EventTarget` is true, and `self` and `window` are both the global itself.
That is the shape upstream was measured having, and it is not decoration: a library calls the bare `addEventListener` while a module is still being evaluated often enough that a runtime without one is a `ReferenceError` before the function has a handler.
What is not there is anything dispatching to it. Nothing here fires `error` or `unhandledrejection`, so a library that reports crashes by listening on the global reports nothing, and a handler that throws is the log line further down instead.

`performance` is `now()` and `timeOrigin`, which is a monotonic clock counting from the moment the isolate started, in milliseconds with a fraction. `mark` and `measure` are not here.

`Deno.permissions` is all six methods, `query`, `querySync`, `request`, `requestSync`, `revoke` and `revokeSync`, and a `PermissionStatus` that is an `EventTarget` with a `state` on it.
What it is for is a library deciding whether to reach for something it can do without, which is what `@sentry/deno` does while its sdk is being set up.
`env`, `net`, `read` and `hrtime` are granted, and `write`, `run`, `ffi` and `sys` are denied because none of the four is here at all.
Upstream answers granted to all eight, and a worker there can no more start a process than one here can, so this is a deliberate difference: a library told granted and then handed a `TypeError` is worse off than one told no.
Nothing here can be revoked, because a function's permissions are the runtime's rather than the function's, so `revoke` answers what `query` answers rather than pretending to take away something it is not enforcing.

`navigator` is `userAgent`, `hardwareConcurrency`, `language` and `languages`, and nothing else, which is what upstream has. The user agent reads `Deno/2.1.4 (variant; zou/<version>)`, the same sentence upstream builds with its own name in the brackets, and the core count is 1 whatever the host has, because a function gets one thread.

Not present yet, and named rather than silently missing:

- The rest of `crypto.subtle`: key derivation, the asymmetric algorithms, and every key format other than `raw`.
- Byte streams. There is no `new ReadableStream({ type: "bytes" })`, no BYOB reader and no `TextDecoderStream`.
- Streaming the other way. A response body is sent as it is made, and a body coming back from `fetch` is still collected before the handler sees it, so a function that wants to read somebody else's answer a chunk at a time cannot yet. That also moves when the promise settles: upstream hands the response back when the headers arrive and this hands it back when the body is in, so a handler racing a slow answer against a clock sees the clock win here and the headers win there.
- The rest of the file system. Reading a file the function's own `static_files` covers is all of it: there is no write, no directory listing, no `Deno.open` and no stat.
- The rest of the node built ins. Nineteen of them are here and the section on packages says what each one covers, and one that is not, `node:child_process` among them, is refused by name when it is resolved.

The rest of that list, and where each line stands, is [issue #369](https://github.com/tamnd/zou/issues/369).

## A function that throws

500, `Internal Server Error`, as text.
The error goes to the log and never to the caller, which is upstream's behaviour and the right one: a stack trace is for the operator.

## Embedded

A function does not have to be javascript.
Something linking zou into its own application usually already has the code it wants to run and no wish for a second language to run it in, so a handler can be Rust:

```rust
let hosted = zou_functions::Hosted::new().at("hello", |call| {
    Ok(zou_functions::Answer::new("text/plain", call.body.clone()))
});
let cfg = zou_server::Config {
    functions: Some(std::sync::Arc::new(zou_functions::Registry::hosted(hosted))),
    ..Default::default()
};
```

Everything in front of the runtime is the same: the same url, the same verification, the same 404 and the same 500.
An isolate and a closure are the same kind of thing to the server in front of them, which is why the engine can be a feature at all.
