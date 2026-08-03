# Conformance

zou claims to be Supabase compatible, and a claim like that is only worth what it is measured against.
So the suites this harness runs are not assertions about what zou should do.
They are questions, and the answers they are compared with were recorded from the real PostgREST and GoTrue binaries at the versions pinned in `versions.json`.
A case that fails is a case where zou and upstream answered the same question differently, and nobody had to decide which of them was right for the difference to show up.

## Where the suites are

`conformance/` here is the harness and nothing else, about three thousand lines of Rust.
The suites themselves are in [tamnd/zou-conformance](https://github.com/tamnd/zou-conformance): the questions, the recordings of what upstream answered, and the fixtures those questions are asked against.

They are there rather than here because they are not zou's code.
The derived suite is upstream's fixtures and upstream's questions, a megabyte and a half of somebody else's SQL and JSON, and it changes when PostgREST changes rather than when zou does.
This repository pins a commit of it in `conformance/suites.json`, so a recording cannot move under a CI run without the bump arriving in a pull request somebody read.

Point the harness at a checkout with `--suites <dir>`, or with `ZOU_CONFORMANCE_SUITES` in the environment, which is what CI sets after cloning the pin.

## The versions

`versions.json`, in that repository, is the whole answer to "compatible with what?".
Everything except supabase-js comes from the image list the Supabase CLI builds its local stack from, which is what `supabase start` gives people, so the reference is the stack a user actually runs and not a version chosen here.
Bumping a version means re-recording the suites it covers, and the re-recording is the point: a diff in a recording is upstream changing its mind, and it should be read rather than merged.

Today that is PostgREST 14.15, GoTrue 2.194.0, postgres-meta 0.96.6, supabase-js 2.111.0, and the Supabase CLI 2.111.0.
The one line where zou is deliberately ahead is Postgres: the Supabase local stack is on 17 and zou vendors 18, and a suite that depends on the difference is a suite that is testing Postgres rather than the API in front of it.

## Six modes

```
record      ask a reference and write down what it said
check       ask a target and compare it with what was written down
diff        ask two targets and compare them with each other
derive      read a PostgREST checkout and write a suite out of it
serve       start zou on a port and wait, for a suite asked from somewhere else
scoreboard  turn the json those runs wrote into the published markdown
```

`check` is what CI runs on every push, because it needs no reference on the machine, and because it fails on the day the two drift apart rather than on the day somebody remembers to look.
`diff` is what you run when you have both up and you do not trust the recording, and CI runs it too, against a PostgREST it downloads at the pinned version, so a release that answers differently cannot sit in the recording being believed.
`record` is how a suite is created or refreshed, and it refuses to write a recording with a hole in it, since the hole is what every later run would be compared against.
`derive` is how the second suite got written, and it is described below.
`serve` asks nothing at all, and is described below too.
`scoreboard` asks nothing either: it reads the json those runs wrote and renders [the scoreboard](scoreboard.md), which is described at the end.

## What counts as the same answer

Status and headers are compared exactly.
There is no normalizing of ids or timestamps anywhere in the harness, and there is not meant to be: a case whose answer moves is a case that should be pinned down in `setup.sql` instead.

Headers are compared on a fixed list: `allow`, `content-profile`, `content-range`, `content-type`, `location`, `preference-applied`, `retry-after`, `www-authenticate`.
Everything else is transport or bookkeeping, `date` and `server` are never equal, the request id is meant to differ, and `content-length` is the body again in a form that says nothing the body did not.
`content-location` is left out for a duller reason: it echoes the path the request came in on, and a bare PostgREST is asked on a path with `/rest/v1` taken off it, so it would differ on every case without anything having differed.

Bodies get three outcomes rather than two.

- **Same.** Byte for byte.
- **Written differently.** The same JSON, different bytes, which is usually whitespace or key order. Both are usable by a client, but they are not the same thing to somebody whose test suite compares strings, so this is a pass that says so rather than a pass that hides it.
- **Different.** Anything else.

Numbers are compared twice, once parsed and once as they were written.
A parsed body cannot tell 1200.50 and 1200.5 apart, because a double cannot, and a client reading a price can, so the literals are scanned out of the raw text and compared as a sorted multiset.
Sorted, because key order is already allowed to differ and this must not be the one thing that turns that back into a failure.

## Known differences

`suites/<name>/known.json` names the cases where zou is known to answer differently, and why, with the issue tracking the fix.
A known case still counts as a failure in the score.
It is excused from the exit code and nothing else, because a scoreboard that quietly forgives its own failures is a scoreboard that goes up when you write the excuse.

A known case that starts passing also fails the run.
That is deliberate: the list is meant to shrink, and the only way it shrinks is if the day it becomes wrong is a day CI complains.

`check --write-known` writes the run's differences to that file instead of failing over them.
It exists for a suite that arrives with hundreds of them at once, where the alternative is either not running the suite in CI or hand writing hundreds of lines that say what the report already said.
The entries it writes carry the difference itself as the reason, `status 400 not 200` and the like, so the file reads as the list of things zou does not do yet.
It is a thing to run once when a suite lands and then never again, and the diff it produces is the review.

## Running it

You need a Postgres and, for `diff` or `record`, a PostgREST.
zou itself is linked into the harness, so it is started on a free port and torn down with the process, and there is nothing to leave behind when a case fails halfway.

```
git clone https://github.com/tamnd/zou-conformance /tmp/zou-conformance
cargo run -p zou-conformance -- check --suite rest \
  --suites /tmp/zou-conformance/suites \
  --zou-dsn postgresql://postgres@127.0.0.1:5432/postgres
```

The database has to be on UTC, and the harness reads the timezone before it asks anything rather than scoring what it finds.
A timestamptz is rendered in the session's timezone, so the same binary answers `+00:00` on one machine and `+07:00` on another, and a recording cannot survive that.
`alter database <name> set timezone to 'UTC'` fixes it, and whatever holds a pool open on that database has to be started again afterwards.

For a diff, run PostgREST against the same database with the same JWT secret, so the same minted keys work on both ends and the requests really are the same requests rather than requests that happen to mean the same thing:

```
db-uri = "postgresql://postgres@127.0.0.1:5432/postgres"
db-schemas = "conformance,public"
db-anon-role = "anon"
jwt-secret = "super-secret-jwt-token-with-at-least-32-characters-long"
server-port = 3999
```

```
cargo run -p zou-conformance -- diff --suite rest \
  --suites /tmp/zou-conformance/suites \
  --zou-dsn postgresql://postgres@127.0.0.1:5432/postgres \
  --reference-url http://127.0.0.1:3999 \
  --reference-dsn postgresql://postgres@127.0.0.1:5432/postgres \
  --reference-strip-prefix /rest/v1
```

`db-schemas` puts `conformance` first because the first schema is the one a request that names none gets, and the suites keep their tables there and use `public` only to have somewhere else to point at.
`--reference-strip-prefix` is there because cases are written with the paths a Supabase project answers on, `/rest/v1/todos`, and a bare PostgREST serves that table at `/todos`, so the prefix comes off on the way out rather than being written twice in every case.

Both targets are asked one after the other rather than side by side, and each gets `setup.sql` applied immediately before its own pass.
They usually read the same Postgres, and the writing cases leave rows behind, so a shared setup would mean the second target meeting a database the first one had already changed.

## A suite

```
suites/rest/
  setup.sql       the schema and the fixed rows, ending in a schema cache reload
  cases.json      the questions
  recorded.json   what the reference answered, per case
  known.json      the cases zou answers differently, and why
```

A case is a name, a feature, a method, a path, which key to send, optional headers and body, and an optional note.
The name is what the report and the known list use, so it has to be unique and it should read like a sentence: "a column nobody has, in the order" is a better name than `test_bad_order_column`.
The feature is only for grouping the score.

Adding a case means adding it to `cases.json` and re-running `record`, and the diff in `recorded.json` is the review: it is upstream's answer, written down, and if it is surprising then the case found something.
That is a pull request in tamnd/zou-conformance, and then a bump of the pinned commit here, which is the same two steps that pruning a known list takes.

A suite may also carry a `reset.sql`, which the `postgrest` suite does.
It is applied before every case that writes, so that a case is asked against the rows it was written against rather than against what the twenty cases before it left behind.
Upstream gets that for free by rolling every transaction back; here the answers are recorded, so the rows go back the hard way, and identically for both targets.

## The suite derived from upstream

The `rest` suite is hand written, 82 cases about the surface a Supabase project actually uses.
The `postgrest` suite is not written at all.
`derive` reads a PostgREST checkout at the pinned version, walks its spec files, and turns every request in them into a case: 1217 of them, out of the 22 spec modules the default test app in `Main.hs` runs.

```
git clone --branch v14.15 https://github.com/PostgREST/postgrest /tmp/postgrest-src
cargo run -p zou-conformance -- derive --from /tmp/postgrest-src --suite postgrest \
  --suites /tmp/zou-conformance/suites
```

The line drawn around what to take is upstream's own: exactly the specs that run against the default configuration, because a spec that needs a server flag is testing that flag rather than the REST surface.
What comes across is the request, never the expectation.
Upstream's `shouldRespondWith` is ignored on purpose, since the whole design here is that the answer comes from asking the binary rather than from reading somebody's assertion about it.

`setup.sql` and `reset.sql` are upstream's fixtures with four differences, each of them noted in the file.
The psql variables and includes are gone, because it is applied over a connection rather than by psql.
The rows that arrived over `copy ... from stdin` are inserts, for the same reason, whitespace intact.
PostGIS is gone, because it is not in the Postgres CI runs and the specs that need it are not in this suite, and it goes a whole statement at a time by reference rather than by name, so the tables, the functions over them, and the media type handlers over those all go with the extension.
And two statements are swept up: upstream creates two schemas whose names are made of the characters a URL has opinions about and never drops them, and upstream rewrites a function's OID in `pg_proc` on purpose to build the collision from PostgREST issue 4052, which leaves a `pg_depend` row naming an OID no function has.
Neither matters upstream, where the fixtures load into a database made a moment earlier.
Here the same file is applied once per target, so it has to be able to run twice.

Two of the requests in those files are not understood by the deriver, both in `InsertSpec`, and it says so rather than quietly dropping them.

Sixteen more are left out on purpose, and the reason is worth writing down because it is the one kind of case this design cannot handle.
They ask for `Prefer: count=planned` or `count=estimated`, and the answer to those is the planner's row estimate in `Content-Range`.
That estimate is a property of the physical table at the moment of the query rather than of the request: it moves with the page count, so a table that a few writing cases have churned answers differently from the same table an autovacuum has just been over.
Four of them flipped between two runs of the same commit in CI, 400 to 960 and 1200 to 1600 on a four row table.
Upstream can ask them because upstream runs against a database made seconds earlier in a fixed order; here the answer is recorded on one machine and compared on another, days apart.
They are not excused and not known, they are not asked, because a recording of a guess is not something to compare and a ratchet that goes red at random teaches everybody to ignore it.

The reference for this suite is configured the way upstream configures its own:

```
db-schemas = "test"
db-anon-role = "postgrest_test_anonymous"
db-extra-search-path = ""
```

## The suite asked from somewhere else

Every suite above is asked by this harness, over an HTTP client written here.
That is the right way to compare two servers and the wrong way to answer a different question: whether the client a person actually installs works against zou.
supabase-js does its own URL building, its own token refresh, its own retries and its own error shapes, and a harness that reimplements them is testing the reimplementation.

So there is a suite that is not asked from here.
It is supabase-js's own `test/integration.test.ts`, in `js/` in the conformance repository, run against zou with the URL and the keys made pluggable and nothing else touched.
The assertions are upstream's, which is the one case where copying an assertion proves something: upstream wrote them about upstream's own client, against the stack `supabase start` brings up.

`serve` is what gives it somewhere to point.

```
cargo run -p zou-conformance -- serve \
  --zou-dsn postgresql://postgres@127.0.0.1:5432/zoujs \
  --setup /tmp/zou-conformance/js/setup.sql &

cd /tmp/zou-conformance/js && npm ci && npm test
```

It starts zou on 54321, the port the Supabase CLI serves a local project on, prints the three keys minted from the secret so a shell script does not have to know how, and then stays up until it is killed.

`--setup` is applied after the server is up rather than before, and the ordering is the whole reason the flag exists.
zou installs the auth schema on the first connection it takes out of its pool, the fixture has a foreign key into `auth.users`, and the health endpoint answers without taking a connection.
So `serve` makes zou answer a request that has to reach Postgres before it says it is ready, and the `url` line it prints last is a readiness check CI can grep for.

16 of the 34 tests run and zou passes all 16: the client constructing, the PostgREST block, the RLS block, the Authentication block, and the timeout configuration block.
The other 18 are Realtime and Storage, which zou does not serve on this URL yet.
They are skipped behind an environment flag rather than deleted, so the day the feature lands the test runs exactly as upstream wrote it.

## Where zou stands

The `postgrest` suite is 1217 cases against PostgREST 14.15, and zou passes 607 of them, 49%, with 610 known differences.
That number is the honest one, and it is meant to be uncomfortable.
The suite asks everything upstream asks itself, including the parts of PostgREST nobody using Supabase has ever typed, so 49% against it and 86% against the hand written suite are both true and they measure different things.
The gap is broken down by feature and by shape in [tamnd/zou#118](https://github.com/tamnd/zou/issues/118), and it is a small number of missing features rather than 610 separate bugs: spread embeds through a to-many relationship, the parts of the query parser that answer `PGRST100`, relationship resolution that answers `PGRST200`, `OPTIONS`, `explain`, the media type handlers, `Range` on a table, and `preference-applied`.

supabase-js 2.111.0 runs 16 of its integration tests against zou and all 16 pass.

The REST suite is 82 cases against PostgREST 14.15, and zou passes 71 of them, 86%, with 11 known differences.
Every difference left is a message, a code, or a header, not a wrong answer to a question about data: error messages that name an internal alias instead of the table, raw SQLSTATEs where upstream has `PGRST205` or `PGRST204`, four wordings of a hint or a detail, an upsert that answers 201 where upstream answers 200 on a merge that updated, and `OPTIONS` on a table.
They are tracked in [tamnd/zou#116](https://github.com/tamnd/zou/issues/116).

The 49 cases that pass "written differently" are all the same two things: zou puts a space after each colon where PostgREST puts a newline between rows, and a `select=*` comes back with the columns in a different order.
Neither is a difference in what was said, which is why the harness has a third verdict for them, and they are left as they are until something turns out to depend on them.

## The scoreboard

Those paragraphs are prose, and prose goes stale.
[docs/scoreboard.md](scoreboard.md) is the same numbers generated out of the run, and CI rewrites it on every merge to main, out of the json the two conformance jobs uploaded rather than out of a run of its own.
Rendering it from a fresh run would publish a number nobody had failed a build over.

```
cargo run -p zou-conformance -- scoreboard \
  --report /tmp/conformance.json \
  --js /tmp/js.json \
  --pin "$(jq -r .ref conformance/suites.json)" \
  --out docs/scoreboard.md
```

Two cuts through the same run, and the second one is the useful one.
A feature says what somebody was testing, which is how the suite is organised, and it is upstream's vocabulary: `upsert` is at 52% and that is not a piece of work.
An endpoint says what the server had to implement, with the fixture's table and function names taken out, and `PATCH /rest/v1/{table}` at 28% is.

There is no date and no run number in the file, so a merge that moved no number leaves no diff and makes no commit.
The commit it does make carries `[skip ci]`, so the workflow does not start again to measure what it has just measured.
