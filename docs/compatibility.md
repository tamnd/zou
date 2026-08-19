# Supabase compatibility

What a project can move to zou today, what it will notice if it does, and what is still missing. The per endpoint numbers live in [docs/scoreboard.md](scoreboard.md), which CI rewrites on every merge; this page is the part a number cannot say.

Nothing here is an assertion. Every claim on this page is a case in a suite, and every expected answer in those suites is a recording of what the real PostgREST or GoTrue binary replied when it was asked the same question against the same fixtures. When zou and upstream disagree, the case fails, and nobody had to decide which of them was right for the disagreement to show up. [docs/conformance.md](conformance.md) describes how that works.

## What it is compatible with

One file answers "compatible with what", [versions.json](https://github.com/tamnd/zou-conformance/blob/main/versions.json) in the conformance repository. Everything except the javascript clients comes from the image list the Supabase CLI builds its local stack from, which is what `supabase start` gives people. The clients have no image, so each is the latest of its line on npm.

| piece | version |
| --- | --- |
| PostgREST | 14.15 |
| GoTrue | 2.194.0 |
| storage-api | 1.67.20 |
| edge-runtime | 1.74.2, which is Deno 2.1.4 |
| postgres-meta | 0.96.6 |
| supabase-js | 2.111.0 |
| storage-js | 2.111.0, the same release |
| tus-js-client | 4.3.1 |
| Postgres | 17.6.1.156 upstream, REL_18_4 in zou |

realtime-js has no line of its own because it has no release of its own: it lives in the supabase-js repository and goes out with it, so the client the realtime suites drive is that same 2.111.0.

The Postgres line is the one place zou is deliberately ahead. A suite that depends on the difference is a suite testing Postgres rather than the api in front of it.

## Where it stands

| surface | passing | of | | notes |
| --- | ---: | ---: | ---: | --- |
| rest, the hand written suite | 82 | 82 | 100% | the surface a Supabase project actually uses |
| postgrest, derived from upstream's spec files | 1217 | 1217 | 100% | upstream's own test corpus turned into questions |
| auth | 74 | 77 | 96% | three known differences, below |
| storage | 478 | 478 | 100% | buckets, objects, image transforms, resumable uploads and the S3 protocol |
| supabase-js | 33 | 33 | 100% | upstream's integration file, url changed, no assertion touched |
| storage-js | 133 | 133 | 100% | upstream's own tests for the storage client, likewise |
| js-tus | 12 | 12 | 100% | tus-js-client against both servers in the same run |
| js-realtime | 18 | 18 | 100% | realtime-js against both servers, sockets, broadcast and presence |
| js-realtime-private | 12 | 12 | 100% | private channels against the project's own policies |
| js-realtime-changes | 11 | 11 | 100% | `postgres_changes`, with a recorded frame golden |
| js-functions | 29 | 29 | 100% | a function's answers, asked of edge-runtime in the same run |

A known difference still counts as a failure in every number above. It is excused from the exit code and from nothing else, so the score cannot be improved by writing an explanation down.

The last five are asked of both servers in one CI run rather than compared with a recording, because a recording of a socket or of a function is a recording of the thing that answered it.
Whatever `supabase start` says is the expected answer for zou, in the same job, on the same fixtures.

Next to those eleven there are two more things that have no number, because they either work or they do not: two of Supabase's own example apps, unedited, driven through a browser on every push. The only file either of them gains is the `.env` its own `.env.example` asks for, holding the url and the anon key the Supabase CLI prints.

`examples/todo-list/sveltejs-todo-list` covers signing up, signing in, a row level security policy holding between two accounts, and a Github login that goes all the way through the code exchange and comes back as a session. Details in [demo/README.md](https://github.com/tamnd/zou-conformance/blob/main/demo/README.md).

`examples/slack-clone/nextjs-slack-clone` covers the other half, which is what a person sees rather than what a socket sent, and every test in it has two browsers in it. A message arriving on somebody else's screen with the author on it, a message taken back disappearing from it, a channel appearing in the other sidebar and being a room somebody can walk into, the anon key out of the javascript bundle hearing nothing it may not read, and an admin taking back a message that is not theirs through a claim the project's own postgres function mints. Details in [demo-realtime/README.md](https://github.com/tamnd/zou-conformance/blob/main/demo-realtime/README.md).

## What an auth or storage project will notice

Three answers differ on purpose. They are checked in as `known.json` in the conformance repository, which means a case that starts matching upstream fails the run too, and the day one of these stops being true is a day CI complains.

**`GET /auth/v1/health` reports zou's own version.** The name and the description are GoTrue's, so a client checking what it is talking to still reads GoTrue, but the version string is zou's. Claiming to be a GoTrue release it is not would make this page unnecessary and every bug report harder.

**`GET /auth/v1/settings` answers `saml_private_key_next_configured: false`.** Upstream answers `true` with SAML off and no key of any kind configured, which reads as an inverted flag. Nothing in the clients reads it while `saml_enabled` is false.

**`GET /auth/v1/admin/users` fills in the identity list.** Upstream answers `identities: null` on the listing, because its ORM does not load the association on that query, and fills it in when a single account is fetched. A client reading identities off a listed account gets them from zou and has to fetch each account again from upstream, so nothing can be branching on a null that the single account fetch never returns.

One more difference is not an answer at all, and so is not in `known.json`. **A move does not rewrite the bytes.** storage-api keys its store by the object's name, so moving an object copies the bytes to a new key under a new version and deletes the old ones. zou keys the bytes by the row's id and version, and a name is in neither, so a move is two columns of one row and the store is never opened. Both answer `Successfully moved` and both leave the object readable at its new name, but after a zou move the object's `version` is the string it already had, where upstream's is a new one. Copying touches the store either way, because a second row cannot share a key with the first.

## What a realtime project will notice

The socket is realtime-js's, the frames are compared with Supabase Realtime's in the same CI run, and a private channel is decided by the project's own policies on `realtime.messages`, so the parts a project writes code against are the same. Five things behind that are deliberately not, and the whole of each is argued in [realtime.md](realtime.md).

**A push a write policy refused is answered.** Upstream drops it in silence, so a send that nobody was allowed to make looks exactly like a send that nobody happened to hear. Here the push gets an error. The http endpoints keep upstream's answers, 403 on the single message shape and 202 on the batch shape with the refused messages dropped, because clients are written against those.

**`realtime.send()` reaches a socket without a replication slot.** Upstream reads `realtime.messages` out of a logical slot, which is a slot, a publication and a decoder held open per project whether or not anybody is listening. Here an after insert trigger calls `pg_notify` and the server listens on one connection, with the message riding inside the notification when it fits and read back by id when it does not. Notifications are transactional, so a message from a transaction that rolled back is never delivered, which is what makes it safe next to the policy probes that roll back on purpose.

**`realtime.messages` is one table rather than a partition a day.** Upstream partitions by day with a janitor keeping tomorrow's partition ready and drops the old ones. The only rows that outlive their statement here are the ones `realtime.send()` wrote, and they are deleted after three days, which is upstream's own retention with one statement instead of a moving part.

**A `postgres_changes` join is answered once the tap is open.** Upstream answers the join first and sends `Subscribed to PostgreSQL` when the changes are flowing, which for the first subscriber can be seconds later. The slot here is temporary, so it sees what was written after it existed, and a client told `SUBSCRIBED` while one was still being taken would lose whatever it wrote next. The wait is bounded at five seconds, inside realtime-js's own join timeout, and a database that cannot be tapped answers the join anyway and tells the subscriber there is a gap. Upstream's slot is permanent and always being read, which is the same trade in the other direction: it has nothing to wait for and it retains write ahead log for a browser tab that may never come back.

**A refusal that is not about policies does not answer 403.** A server with no database behind it says a private channel cannot be decided and says why, rather than sending somebody to read policies that were never consulted.

Two of upstream's limits are not built: presence events a second, and how often one client may track. The rest of a project's budget is upstream's five numbers with upstream's defaults, in [realtime.md](realtime.md), and on a fleet each node refuses against its own share of the first two, which is [#444](https://github.com/tamnd/zou/issues/444). The open list is [#303](https://github.com/tamnd/zou/issues/303).

## What a functions project will notice

A function is `supabase/functions/<name>/index.ts` calling `Deno.serve(handler)`, the url is `/functions/v1/<name>`, and the refusals, the limits and the environment are edge-runtime 1.74.2's, measured from inside a function on a real `supabase start` rather than read off a container. What differs is a runtime that is not Deno wearing Deno's api, so the differences are things a function reaches for rather than answers it gets, and [functions.md](functions.md) names each one where it belongs.

**CORS is the function's, like the hosted platform and not like the local stack.** On `supabase start` the gateway answers every preflight itself and replaces the `Access-Control-Allow-Origin` a function set with `*`, which quietly widens a function that meant to allow one origin. Here the function is authoritative, the server answers a preflight for a function that did not, and nothing a function says about an origin is ever widened.

**`SB_EXECUTION_ID` is one per invocation.** The local runtime sets it when a worker is made and keeps the worker between calls, so a function called five times is handed the same id five times. Both are a uuid, the suite asks both sides, and it is visible only to a function that logs it.

**`Deno.permissions` answers denied to the four that are not here.** Upstream answers granted to all eight, and a worker there can no more start a process than one here can, so a library told granted and then handed a `TypeError` is worse off than one told no. `env`, `net`, `read` and `hrtime` are granted, `write`, `run`, `ffi` and `sys` are not.

**The keys in `Deno.env` are this project's own.** `SUPABASE_PUBLISHABLE_KEYS` and `SUPABASE_SECRET_KEYS` are the maps a library reads a `default` out of, and the values are the project's anon and service role keys rather than `sb_publishable_` and `sb_secret_` strings, because zou does not issue keys in that format and writing the prefix onto something that is not one would be a claim about a format nothing here implements. A library treats the value as opaque and sends it back as the apikey header, which this server accepts.

**Three spec answers are a `TypeError` where a browser would throw a `DOMException`**, because there is no `DOMException` in the runtime yet: `getRandomValues` over 65536 bytes, and the `WebSocket` constructor on a url it will not take.

What a function cannot reach yet is named rather than left to be discovered: the rest of `crypto.subtle` past the four digests, HMAC, AES and ECDSA on P-256, byte streams and BYOB readers, a `fetch` response handed back when the headers arrive rather than when the body is in, `MessageChannel` and `MessagePort` ([#434](https://github.com/tamnd/zou/issues/434)), the file system past reading what `static_files` covers and what a package keeps beside itself on the registry, a unix socket, the legacy text encodings, and the node built ins, which are refused by name. The body ceiling is 20 MiB, which is zou's number and not upstream's. The open list is [#369](https://github.com/tamnd/zou/issues/369), and the gate it is measured by is Supabase's own examples repository running unmodified.

## What is not served yet

`/storage/v1` is served as far as buckets, objects, image transforms, resumable uploads and the S3 protocol go. Making, reading, updating, emptying and deleting a bucket, uploading, downloading, describing, listing, moving, copying and deleting an object, signing a url for a download or an upload, resizing and re-encoding an image on the way out, the whole of the TUS protocol a resumable upload speaks, and the whole of the S3 protocol this endpoint speaks including an upload sent in pieces, are built and measured against the reference. The transforms are the one thing here that is not compared byte for byte: a case names the dimensions, the format and the header saying what was asked for, and gives up on the digest, because two jpeg encoders at one quality do not agree on a single byte. What is left under `/storage/v1` is analytics buckets.

`/realtime/v1` is served as far as the socket, broadcast, presence and postgres changes go, which is described in [realtime.md](realtime.md). A client connects, joins channels, refreshes its token on a live connection, broadcasts to the other members of a topic and tracks itself on one, and a socket joining a topic is told who is already there. A room can also be sent to over http, both the batch shape `channel.send()` falls back to and the single message shape `channel.httpSend()` posts, which is how a trigger or a worker talks to one. A private channel is served against the project's own row level security policies on `realtime.messages`, read at the join and written on the first send, so a project's existing policies decide the same way here as they do upstream. A channel can subscribe to `postgres_changes`, on a table or a whole schema, with the same filter operators upstream takes, and the join reply carries the client's own subscription entries back with the ids the server gave them, which is what realtime-js matches its callbacks on. The rows are read out of a logical replication slot the first subscriber opens and the last one closes, and every row is checked against the policies on its table for the person who asked, so a subscriber is sent what a select would have shown it and nothing else. The rest of the prefix is a route in the router with nothing behind it.

Everything in that state answers 501 with a body saying so and naming the milestone, rather than 404, so a client gets told the surface exists and is not finished instead of being told the url is wrong. Storage is M3 and Realtime is M4.

The S3 protocol half of that is asked with a key pair rather than with an anon or service_role key, so it answers only where there is one to check a signature against. `zou dev` answers to the fixed pair a local Supabase project answers to, a served project answers to its own out of its registry entry, and the library and the node package take one as an option. A project with no pair still has the endpoint, and it says of every signed request that the access key is not one this project has, which is the honest answer rather than a door with no lock.

The bytes of an object go to the same place the pages do: a directory on a laptop, a prefix on an object store, opened by the same code the engine opens its own target with, and under the same tenant prefix as the database whose rows describe them, at `tenants/<ref>/files/`. One prefix per tenant is what lets a bucket hold more than one project without either of them being able to name the other's bytes. A server that names no tenant serves `local`, which is what a single database deployment is. A server built without a store at all answers the bucket surface and refuses the routes that carry bytes, because writing files somewhere nobody asked for would be worse than saying so.

This is also why the supabase-js run skips 17 of its cases, and why the storage-js run skips three of its five. They are skipped rather than deleted so the count keeps saying how much of the file is not being asked, and the other two of the five are skips upstream carries itself.

## The database underneath

Two servers can agree on every answer and still be sitting on different databases, and no suite can see it, because a suite asks a server. A Supabase project's database comes with things no server of theirs made and a project's own migrations lean on. The record workflow dumps that list next to every recording, so what follows is the reference's answer rather than a memory of it, and [#214](https://github.com/tamnd/zou/issues/214) is where the difference was worked off.

**What zou has.** An `extensions` schema holding pgcrypto and uuid-ossp, `extensions` on the database's search path, and usage on it for the three api roles, so a migration copied off the Supabase docs that calls `gen_salt`, `crypt`, `digest` or `uuid_generate_v4` with nothing in front of it resolves the way it does upstream. `statement_timeout` is three seconds for `anon` and eight for `authenticated`, the same numbers a project gets. Those two are worth a note of their own: Postgres reads a role's settings at connection time for the role that connected, and zou reaches every api role with `set role` on a connection that logged in as somebody else, so the pool reads `pg_db_role_setting` and applies them per transaction the way PostgREST does. An `alter role` a project runs itself works the same way, within ten seconds of running it. The two it will not take from a role are `role` and `search_path`, since the schema a request runs against is negotiated per request from `Accept-Profile`.

**What zou also has.** The `net` schema and the `supabase_functions` schema, which is what a database webhook is made of, described in [webhooks.md](webhooks.md). The functions have pg_net's signatures, defaults and error messages, and `supabase_functions.http_request()` is upstream's own trigger function, so the trigger a dashboard writes fires here and the audit row lands in the table it lands in there. What is behind them is not the extension: there is no background worker, the queued row announces itself with a notification, and the server makes the call. A database that really has pg_net installed keeps it and is left alone. The `cron` schema is there on the same terms, described in [cron.md](cron.md): pg_cron's functions, its two tables with their defaults and their row level security policies, and its refusals word for word, with a ticker in the server in place of the launcher process and a catch up rule for the wake that a launcher which never sleeps has no need of. A database that really has pg_cron installed keeps that too.

**What zou does not have.** supabase_vault and the `vault` schema, pg_graphql and the `graphql` and `graphql_public` schemas, pg_stat_statements, and the `realtime`, `_realtime` and `pgbouncer` schemas. Each of those is a piece of the platform rather than a setting, and none of them is M3. The `supabase_*` roles are absent for a different reason: they exist upstream because auth, storage, realtime and the pooler are separate processes connecting as separate roles, and zou is one process on one pool. A schema dump taken from a hosted project names them in its grants, so restoring one into zou means creating them first or dumping without owners and privileges.

## What the suites do not ask yet

The honest reading of a 96% is that it is 96% of the questions somebody thought to write down. These are the areas where the number above is a lower bound on the work and an upper bound on the confidence, tracked on [#170](https://github.com/tamnd/zou/issues/170):

- The MFA flow past the factor listing: enroll to challenge to verify, and the `aal2` claim that comes out of it.
- PKCE, the `code` grant and the flow state behind it.
- Phone and SMS signup, otp, verify and update.
- External OAuth providers past the refusal `/authorize` gives when none are configured: the redirect, the callback, identity linking and unlinking.
- SAML and SSO.
- Anonymous sign in and the `is_anonymous` claim.
- Session rows: what `scope=global` and `scope=others` leave behind, and what a refresh does to `refreshed_at`.
- Mail templates and the links in them. Autoconfirm is on in the recorded configuration, so nothing in the suite ever reads a mail.
- The shape of a 429, its `retry-after` and its body. The limits themselves are a configured number and a clock rather than a compatibility surface, but the refusal is one.
- Auth hooks and the `before-user-created` extension point.

One known gap in the token check is [#173](https://github.com/tamnd/zou/issues/173): zou validates `exp` and does not check `nbf` or `aud`. Both libraries upstream verifies with check `nbf` by default, so a token that is not valid until an hour from now is accepted by zou and refused by GoTrue. The attack suite pins that behaviour so the day it changes is a test edit rather than a surprise.

## Row level security

The parts a policy relies on are in place and tested from the outside rather than from the inside. `auth.uid()`, `auth.jwt()`, `auth.role()` and `auth.email()` carry Supabase's own definitions, `anon`, `authenticated` and `service_role` are created at tenant bootstrap with Supabase's grants, and `service_role` bypasses RLS.

All seven request settings, including `role`, `request.jwt.claims`, `request.headers` and `request.cookies`, are bound as parameters rather than interpolated, so a claim or a header or a cookie with SQL in it is a value and not a statement. `role` is the one setting Postgres reads back as an identifier, and a bogus role fails the session before any statement runs.

`crates/zou-server/tests/attack.rs` asks the questions a leak actually comes from: a token minted with the wrong secret, a claim carrying SQL, an embed hanging off a visible parent, a count over rows the caller cannot see, the `auth` schema asked for over REST with a service token, and an expired token. Two of its tests pin behaviour that is not a defence, because they are how a project ends up handing out a table it thought a policy covered: a plain view reads its table with the view owner's rights, and a `SECURITY DEFINER` function does the same. `with (security_invoker = true)` is the fix for the first and the second is deliberate. Both are Postgres's rules rather than zou's, and a suite that only asserted the good cases would not notice the day one of them changed.

`crates/zou-server/tests/attack_realtime.rs` asks the same kind of question over a socket, where a subscription that is allowed goes on being answered every time anybody writes a row. A subscription worded three ways to reach a table the policies hide, a table and a column the subscriber may not select, a filter carrying SQL in its value and in its column name, a token refresh that does not verify, and a service key signed with the wrong secret. The one it caught: a delete on a table with `replica identity full` published the whole deleted row, because the cut to the identifying columns was made on what the change said about itself rather than on the primary key, and postgres marks every column of such a table as part of the identity.

## Speed

Measured, on an idle box, against PostgREST 14.16 over a stock Postgres 18.4 answering the same scenario with the same tenant and the same tokens. Full entry in [tamnd/zou-bench](https://github.com/tamnd/zou-bench/blob/main/docs/results/2026-08-06.md).

| | tps | p50 ms | p95 | p99 | p999 |
| --- | ---: | ---: | ---: | ---: | ---: |
| PostgREST on Postgres 18.4 | 10002 | 0.652 | 1.931 | 2.313 | 2.771 |
| zou | 7690 | 0.811 | 2.403 | 2.634 | 2.917 |

zou is about 0.2 ms behind at the median and 0.77x the throughput. Per request shape its p99 is the lower of the two on four of the seven shapes, including all three plain reads. One shape is genuinely behind, the rpc at 2.43 ms against 0.997, which is [#178](https://github.com/tamnd/zou/issues/178).

## Checking it yourself

The suites are data, so they run against anything that speaks the api:

```
cargo run -p zou-conformance -- check --suite rest \
  --url http://127.0.0.1:54321 \
  --dsn "host=127.0.0.1 port=54322 dbname=postgres user=postgres" \
  --suites /path/to/zou-conformance/suites
```

Point it at `supabase start`, at a hosted project, or at `zou dev`, and it asks the same questions of each. A target that does not serve under `/rest/v1`, a bare PostgREST for instance, takes `--strip-prefix /rest/v1`, and `diff` asks two targets and compares them with each other rather than with a recording.

CI does the first of those on every push: `supabase start` at the pinned CLI version, the rest suite asked of it and of zou, and the two answers compared with each other rather than with a file. It is a stronger question than the PostgREST binary the job next to it downloads, because a stack has a gateway in front of it and serves `/rest/v1` because something put it there. The hosted target is the one CI cannot have, since it is somebody's account and somebody's key.

That is the whole design: the recordings belong to upstream, they live in their own repository pinned to a commit here, and bumping the pin is a diff somebody reads.

## How this page stays true

The tables come from the same run `docs/scoreboard.md` does. The three known differences are the contents of `known.json`, not a memory of them. The gap lists are the open boxes on [#125](https://github.com/tamnd/zou/issues/125), [#170](https://github.com/tamnd/zou/issues/170), [#190](https://github.com/tamnd/zou/issues/190), [#303](https://github.com/tamnd/zou/issues/303) and [#369](https://github.com/tamnd/zou/issues/369), one per surface, which are ticked by the pull request that earns them.

If something on this page is out of date, the fix is upstream of the page: prune the known list, tick the box, bump the pin.
