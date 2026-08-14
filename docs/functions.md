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
- `functions/noindex/other.ts` is not served, because the entrypoint is missing.
- `functions/jsfn.js` is not served. A function is a directory.

Every one of those answers `404 Function not found`, as text, which is what upstream answers a name nobody deployed.
A function that exists and one that does not are the same thing to a caller, on purpose.

## config.toml

The same file the project already has.

```toml
[edge_runtime]
policy = "per_worker"
inspector_port = 8083

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

`policy` and `inspector_port` are read and carried.
The policy in force today is `oneshot`, one isolate per call, whatever the file says, and the line here will change when the pool arrives rather than the file having to.

Anything under `[functions.<name>]` this server does not know is listed by `zou status` as unread, the same as anywhere else in the file.

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

Four variables are the project's and are the same on every call.

| Variable | What it is |
| --- | --- |
| `SUPABASE_URL` | This server, which is what a function's own supabase client should be built with |
| `SUPABASE_ANON_KEY` | The project's anon key |
| `SUPABASE_SERVICE_ROLE_KEY` | The service role key, which bypasses row level security, so a function that uses it is deciding who may do what by itself |
| `SUPABASE_DB_URL` | The database, as a url |

`SB_EXECUTION_ID` is the fifth and is one per invocation, which is what ties a log line from inside a function to the request that caused it.

`Deno.env` is that map and nothing else.
The environment this server process was started with is not in it, which matters because that environment is holding a database password and a function is somebody else's code.
`Deno.env.set` and `Deno.env.delete` throw: these are a project's settings rather than a shell.

## Typescript

Real typescript, through the same swc transpiler Deno itself uses, so what runs here is what would run there.
Interfaces, enums, generics, decorators and `.tsx` all arrive as javascript before v8 sees them.

A function can import the files beside it.

```ts
import { corsHeaders } from "../_shared/cors.ts"
import settings from "./settings.json" with { type: "json" }
```

`npm:`, `jsr:`, `https:`, `node:` and `data:` specifiers are refused by name, with a message saying which one and that it is not supported yet, rather than failing to be found.

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

- The answer is collected before the handler sees it, so `res.body` is null and a large download is held in memory. The 20 MiB ceiling on a call's body applies to what a function may read back too.
- A call has 30 seconds. Deno waits forever, which is fine for a program somebody is watching and not for a request holding an isolate.
- `res.statusText` is the canonical phrase for the code rather than the one the server wrote, because the client does not keep the reason phrase.
- `res.url` and `res.redirected` are where the answer came from, so a redirect that was followed can be seen. Redirects are followed.
- The `user-agent`, when the function does not set one, is `zou-edge-runtime`.
- `http:` and `https:` and nothing else. `file:`, `data:` and a string that is not a url each throw a `TypeError` saying which.
- A request that could not be made at all throws `TypeError: error sending request for url (<url>): <why>`, which is the sentence Deno throws. A 404 or a 500 is an answer and not a throw.

What a function may reach is not restricted.
It can call a metadata endpoint on the machine it is running on, the same as upstream's runtime can and the same as `pg_net` can from inside the database.
A function is the project's own code, and the way to keep it off the local network is a network the server is not on.

## What a function can reach, and what it cannot

Present: `Request`, `Response`, `Headers`, `fetch`, `TextEncoder`, `TextDecoder`, `atob`, `btoa`, `console`, `Deno.serve`, `Deno.env`, `Deno.build` and `Deno.version`.

Not present yet, and named rather than silently missing:

- `crypto`, and `crypto.subtle` with it.
- Streams. `new ReadableStream()` throws with its own name in the message, and a response body is collected before it is sent rather than arriving in chunks.
- `URL` and `URLSearchParams`.
- Timers, so a handler that sleeps will not.
- `EdgeRuntime.waitUntil`, so work that outlives the response has nowhere to go.

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
