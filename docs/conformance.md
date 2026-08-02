# Conformance

zou claims to be Supabase compatible, and a claim like that is only worth what it is measured against.
So the suites in `conformance/` are not assertions about what zou should do.
They are questions, and the answers they are compared with were recorded from the real PostgREST and GoTrue binaries at the versions pinned in `conformance/versions.json`.
A case that fails is a case where zou and upstream answered the same question differently, and nobody had to decide which of them was right for the difference to show up.

## The versions

`conformance/versions.json` is the whole answer to "compatible with what?".
Everything except supabase-js comes from the image list the Supabase CLI builds its local stack from, which is what `supabase start` gives people, so the reference is the stack a user actually runs and not a version chosen here.
Bumping a version means re-recording the suites it covers, and the re-recording is the point: a diff in `conformance/suites` is upstream changing its mind, and it should be read rather than merged.

Today that is PostgREST 14.15, GoTrue 2.194.0, postgres-meta 0.96.6, supabase-js 2.111.0, and the Supabase CLI 2.111.0.
The one line where zou is deliberately ahead is Postgres: the Supabase local stack is on 17 and zou vendors 18, and a suite that depends on the difference is a suite that is testing Postgres rather than the API in front of it.

## Three modes

```
record  ask a reference and write down what it said
check   ask a target and compare it with what was written down
diff    ask two targets and compare them with each other
```

`check` is what CI runs on every push, because it needs no reference on the machine, and because it fails on the day the two drift apart rather than on the day somebody remembers to look.
`diff` is what you run when you have both up and you do not trust the recording, and CI runs it too, against a PostgREST it downloads at the pinned version, so a release that answers differently cannot sit in the recording being believed.
`record` is how a suite is created or refreshed, and it refuses to write a recording with a hole in it, since the hole is what every later run would be compared against.

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

`conformance/suites/<name>/known.json` names the cases where zou is known to answer differently, and why, with the issue tracking the fix.
A known case still counts as a failure in the score.
It is excused from the exit code and nothing else, because a scoreboard that quietly forgives its own failures is a scoreboard that goes up when you write the excuse.

A known case that starts passing also fails the run.
That is deliberate: the list is meant to shrink, and the only way it shrinks is if the day it becomes wrong is a day CI complains.

## Running it

You need a Postgres and, for `diff` or `record`, a PostgREST.
zou itself is linked into the harness, so it is started on a free port and torn down with the process, and there is nothing to leave behind when a case fails halfway.

```
cargo run -p zou-conformance -- check --suite rest \
  --zou-dsn postgresql://postgres@127.0.0.1:5432/postgres
```

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
conformance/suites/rest/
  setup.sql       the schema and the fixed rows, ending in a schema cache reload
  cases.json      the questions
  recorded.json   what the reference answered, per case
  known.json      the cases zou answers differently, and why
```

A case is a name, a feature, a method, a path, which key to send, optional headers and body, and an optional note.
The name is what the report and the known list use, so it has to be unique and it should read like a sentence: "a column nobody has, in the order" is a better name than `test_bad_order_column`.
The feature is only for grouping the score.

Adding a case means adding it to `cases.json` and re-running `record`, and the diff in `recorded.json` is the review: it is upstream's answer, written down, and if it is surprising then the case found something.

## Where zou stands

The REST suite is 82 cases against PostgREST 14.15, and zou passes 71 of them, 86%, with 11 known differences.
Every difference left is a message, a code, or a header, not a wrong answer to a question about data: error messages that name an internal alias instead of the table, raw SQLSTATEs where upstream has `PGRST205` or `PGRST204`, four wordings of a hint or a detail, an upsert that answers 201 where upstream answers 200 on a merge that updated, and `OPTIONS` on a table.
They are tracked in [tamnd/zou#116](https://github.com/tamnd/zou/issues/116).

The 49 cases that pass "written differently" are all the same two things: zou puts a space after each colon where PostgREST puts a newline between rows, and a `select=*` comes back with the columns in a different order.
Neither is a difference in what was said, which is why the harness has a third verdict for them, and they are left as they are until something turns out to depend on them.
