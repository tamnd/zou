# Supabase compatibility

What a project can move to zou today, what it will notice if it does, and what is still missing. The per endpoint numbers live in [docs/scoreboard.md](scoreboard.md), which CI rewrites on every merge; this page is the part a number cannot say.

Nothing here is an assertion. Every claim on this page is a case in a suite, and every expected answer in those suites is a recording of what the real PostgREST or GoTrue binary replied when it was asked the same question against the same fixtures. When zou and upstream disagree, the case fails, and nobody had to decide which of them was right for the disagreement to show up. [docs/conformance.md](conformance.md) describes how that works.

## What it is compatible with

One file answers "compatible with what", [versions.json](https://github.com/tamnd/zou-conformance/blob/main/versions.json) in the conformance repository. Everything except supabase-js comes from the image list the Supabase CLI builds its local stack from, which is what `supabase start` gives people.

| piece | version |
| --- | --- |
| PostgREST | 14.15 |
| GoTrue | 2.194.0 |
| postgres-meta | 0.96.6 |
| supabase-js | 2.111.0 |
| Postgres | 17.6.1.156 upstream, REL_18_4 in zou |

The Postgres line is the one place zou is deliberately ahead. A suite that depends on the difference is a suite testing Postgres rather than the api in front of it.

## Where it stands

| surface | passing | of | | notes |
| --- | ---: | ---: | ---: | --- |
| rest, the hand written suite | 82 | 82 | 100% | the surface a Supabase project actually uses |
| postgrest, derived from upstream's spec files | 1217 | 1217 | 100% | upstream's own test corpus turned into questions |
| auth | 74 | 77 | 96% | three known differences, below |
| supabase-js | 16 | 16 | 100% | upstream's integration file, url changed, no assertion touched |

A known difference still counts as a failure in every number above. It is excused from the exit code and from nothing else, so the score cannot be improved by writing an explanation down.

Next to those four there is a fifth thing that has no number, because it either works or it does not: one of Supabase's own example apps, unedited, driven through a browser on every push. Signing up, signing in, a row level security policy holding between two accounts, and a Github login that goes all the way through the code exchange and comes back as a session. The app is `examples/todo-list/sveltejs-todo-list` and the only file it gains is the `.env` its own `.env.example` asks for, holding the url and the anon key the Supabase CLI prints. Details in [demo/README.md](https://github.com/tamnd/zou-conformance/blob/main/demo/README.md).

## What a project will notice

Three answers differ on purpose. They are checked in as `known.json` in the conformance repository, which means a case that starts matching upstream fails the run too, and the day one of these stops being true is a day CI complains.

**`GET /auth/v1/health` reports zou's own version.** The name and the description are GoTrue's, so a client checking what it is talking to still reads GoTrue, but the version string is zou's. Claiming to be a GoTrue release it is not would make this page unnecessary and every bug report harder.

**`GET /auth/v1/settings` answers `saml_private_key_next_configured: false`.** Upstream answers `true` with SAML off and no key of any kind configured, which reads as an inverted flag. Nothing in the clients reads it while `saml_enabled` is false.

**`GET /auth/v1/admin/users` fills in the identity list.** Upstream answers `identities: null` on the listing, because its ORM does not load the association on that query, and fills it in when a single account is fetched. A client reading identities off a listed account gets them from zou and has to fetch each account again from upstream, so nothing can be branching on a null that the single account fetch never returns.

One more difference is not an answer at all, and so is not in `known.json`. **A move does not rewrite the bytes.** storage-api keys its store by the object's name, so moving an object copies the bytes to a new key under a new version and deletes the old ones. zou keys the bytes by the row's id and version, and a name is in neither, so a move is two columns of one row and the store is never opened. Both answer `Successfully moved` and both leave the object readable at its new name, but after a zou move the object's `version` is the string it already had, where upstream's is a new one. Copying touches the store either way, because a second row cannot share a key with the first.

## What is not served yet

`/storage/v1` is served as far as buckets and objects go. Making, reading, updating, emptying and deleting a bucket, uploading, downloading, describing, listing, moving, copying and deleting an object, and signing a url for a download or an upload, are built and measured against the reference. What is left under `/storage/v1` is resumable uploads, image transforms and the S3 protocol, and all of `/realtime/v1`. Those are routes in the router with nothing behind them, and they answer 501 with a body saying so and naming the milestone, rather than 404, so a client gets told the surface exists and is not finished instead of being told the url is wrong. Storage is M3 and Realtime is M4.

The bytes of an object go to the same place the pages do: a directory on a laptop, a prefix on an object store, opened by the same code the engine opens its own target with. A server built without one answers the bucket surface and refuses the routes that carry bytes, because writing files somewhere nobody asked for would be worse than saying so.

This is also why the supabase-js run skips 18 of its cases. They are skipped rather than deleted so the count keeps saying how much of the file is not being asked.

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

The tables come from the same run `docs/scoreboard.md` does. The three known differences are the contents of `known.json`, not a memory of them. The gap lists are the open boxes on [#125](https://github.com/tamnd/zou/issues/125) and [#170](https://github.com/tamnd/zou/issues/170), which are ticked by the pull request that earns them.

If something on this page is out of date, the fix is upstream of the page: prune the known list, tick the box, bump the pin.
