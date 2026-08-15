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

`policy` is honoured and `inspector_port` is read and carried until there is an inspector to attach to it.

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

The environment this server process was started with is not in it, which matters because that environment is holding a database password and a function is somebody else's code.
`Deno.env.set` and `Deno.env.delete` throw: these are a project's settings rather than a shell.

## Secrets

A project's own variables go underneath those five, from two places.

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

## Typescript

Real typescript, through the same swc transpiler Deno itself uses, so what runs here is what would run there.
Interfaces, enums, generics, decorators and `.tsx` all arrive as javascript before v8 sees them.

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
There is no node module resolution here, no `package.json` walk, no CJS and no node built ins.
Both specifiers are rewritten to a url on a registry that serves packages as modules, `esm.sh` by default, and from there a package is an ordinary graph of `https:` imports.
What runs is the registry's build of the package rather than the tarball npm would have unpacked, which is worth knowing before reporting a difference in behaviour to a package's author.

- Pin the version. `npm:zod@3.23.8` is a version, `npm:zod` is whatever the registry thinks latest is on the day the cache is cold.
- A package that reaches for a node built in will not run. `node:` is refused by name and the registry's own shims cover the common ones and not all of them.
- `@supabase/supabase-js` runs. `createClient` builds its auth, storage, functions and realtime clients, and the realtime one is a `WebSocket`, which is why that had to exist before this line could say so.
- `http:` is refused. A module arrives and is executed, so it arrives over https.
- `data:` is not supported yet.

Everything fetched is kept on disk, keyed by url, so only the first cold start pays for it.

- `ZOU_MODULE_CACHE` is where, and defaults to `$XDG_CACHE_HOME/zou/modules` or `~/.cache/zou/modules`.
- `ZOU_MODULE_CACHE_ONLY=1` means this server does not fetch. A module that is not in the cache is refused by name rather than reached for, which is what a deployment that warmed its cache somewhere else wants.
- `ZOU_MODULE_REGISTRY` points `npm:` and `jsr:` at a mirror instead of esm.sh.

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
- The `user-agent`, when the function does not set one, is `zou-edge-runtime`.
- `http:` and `https:` and nothing else. `file:`, `data:` and a string that is not a url each throw a `TypeError` saying which.
- A request that could not be made at all throws `TypeError: error sending request for url (<url>): <why>`, which is the sentence Deno throws. A 404 or a 500 is an answer and not a throw.

What a function may reach is not restricted.
It can call a metadata endpoint on the machine it is running on, the same as upstream's runtime can and the same as `pg_net` can from inside the database.
A function is the project's own code, and the way to keep it off the local network is a network the server is not on.

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

## crypto

Random bytes, a uuid, the four hashes and HMAC.

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
`crypto.subtle.digest` has `SHA-1`, `SHA-256`, `SHA-384` and `SHA-512`, and `sign`, `verify` and `importKey` are HMAC over those four with a raw key.

The verification is done in Rust, so a wrong signature takes as long to reject as a right one takes to accept, which is not true of a comparison written in javascript.

What is not there is refused by name rather than being undefined: `encrypt`, `decrypt`, `deriveBits`, `deriveKey`, `exportKey`, `generateKey`, `wrapKey` and `unwrapKey`, and every key format other than `raw`.
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
- There is no `WritableStream` and no `TransformStream`, so there is no `pipeTo` and no `pipeThrough`. They are absent rather than throwing, because there is nothing to construct.
- There is no byte stream and no BYOB reader. `new ReadableStream({ type: "bytes" })` and `getReader({ mode: "byob" })` are refused by name.

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

## What a function can reach, and what it cannot

Present: `Request`, `Response`, `Headers`, `fetch`, `URL`, `URLSearchParams`, `Blob`, `File`, `FormData`, `crypto`, `setTimeout`, `setInterval`, `clearTimeout`, `clearInterval`, `queueMicrotask`, `EdgeRuntime.waitUntil`, `WebSocket`, `Event`, `MessageEvent`, `CloseEvent`, `ErrorEvent`, `ReadableStream`, `TextEncoder`, `TextDecoder`, `atob`, `btoa`, `console`, `Deno.serve`, `Deno.env`, `Deno.build` and `Deno.version`.

Not present yet, and named rather than silently missing:

- The rest of `crypto.subtle`: encryption, key derivation, key generation and the asymmetric algorithms.
- The writable half of streams. There is no `WritableStream` and no `TransformStream`, and so no `pipeTo` and no `pipeThrough`, and no byte stream or BYOB reader.
- Streaming the other way. A response body is sent as it is made, and a body coming back from `fetch` is still collected before the handler sees it, so a function that wants to read somebody else's answer a chunk at a time cannot yet.
- Node built ins. `node:fs` and the rest are refused by name.

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
