# Conformance

zou claims to be Supabase compatible, and a claim like that is only worth what it is measured against.
So the suites this harness runs are not assertions about what zou should do.
They are questions, and the answers they are compared with were recorded from the real PostgREST and GoTrue binaries and the real storage-api image, at the versions pinned in `versions.json`.
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

Today that is PostgREST 16.1, GoTrue 2.195.0, storage-api 1.69.11, postgres-meta 0.98.0, supabase-js 2.112.3, and the Supabase CLI 2.115.0.
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
[docs/compatibility.md](compatibility.md) is the prose next to those numbers: which differences are deliberate, which surfaces are not served yet, and what the suites do not ask.

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

## The values that cannot be the same twice

A suite over a table keeps a recording honest by pinning its rows down in `setup.sql`, and the same question really does have the same answer tomorrow.
A suite over an auth server has no such option.
A sign in answers with a token that was signed a moment ago, a session row that was made a moment ago, and an expiry that is now plus an hour, and the choice is to name those values or to stop comparing the answer.

So a case names them, as json pointers in `volatile`, and they are replaced before the answer reaches the recording or the comparison.
What replaces a value is not a blank, it is the name of the shape it had: `<uuid>`, `<jwt>`, `<number>`, `<timestamp>`.
A uuid that comes back as a number is still a difference, and so is a token with two segments where the reference sent three.
What is given up is the value itself, which is the only thing that could not have been kept.

A pointer is written with slashes and takes `*` for every element of an array or every value of an object, which is what `identities` and `amr` need.
A pointer that matches nothing is left alone rather than reported, because the answer to a request that failed does not carry the keys the answer to one that worked does.
Both sides are redacted by the same case, so this cannot make one target look better than another: a path that is volatile is volatile for the reference too.

## The same answer twice

A recording is only worth comparing against if the same question gets the same answer tomorrow, and one thing in postgres makes that untrue on its own: the planner.

The order of the rows inside an embed is not something PostgREST or zou promises.
It falls out of the plan.
With no statistics postgres hashes one side of the join and with statistics it hashes the other, and the rows come back in a different order without anything having changed about the question or the data.
A run is twelve hundred cases and several minutes long, autovacuum wakes up every minute, and the writing cases put the rows back three hundred and sixty seven times, so on any long enough run autovacuum analyzes the fixtures somewhere in the middle and every case after that point is answered off a different plan than every case before it.
Where that line falls depends on how fast the machine is, which is how a recording made on a laptop stops reproducing in CI, and how a change to something else entirely moves a handful of cases across it.

So the harness turns autovacuum off on every table in the database, once, immediately after `setup.sql` has created them and before anything is asked.
Nothing in a conformance run needs a good plan.
It needs the same plan on the first case as on the last, and the same plan in CI as on a laptop.

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
db-aggregates-enabled = true
```

The last line is the one setting here that is not PostgREST's default, and both suites need it now that the rest suite has aggregate cases of its own.
The argument for it is the same in both places and it is written out under the derived suite below.

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

A case may say `chained`, which means it is asked against what the case before it left rather than against the fixture.
Almost nothing needs it, and the things that do could not be asked without it: a fixture writes rows, and there is no way to write bytes into the object store behind a reference from SQL, so the only way to ask what a copy of an object does is to upload one and then copy it.
The cost is that a chain is order dependent in a way the rest of a suite is not, so a chain should be short, should sit together in the file, and should start with a case that resets like any other.

## The suite derived from upstream

The `rest` suite is hand written, and it is about the surface a Supabase project actually uses.
The `postgrest` suite is not written at all.
`derive` reads a PostgREST checkout at the pinned version, walks its spec files, and turns every request in them into a case, out of the 22 spec modules the default test app in `Main.hs` runs.

```
git clone --branch v16.1 https://github.com/PostgREST/postgrest /tmp/postgrest-src
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
db-aggregates-enabled = true
```

The last line is the one that is not upstream's test default, and it is there because zou has no matching switch.
PostgREST has kept aggregates behind `db-aggregates-enabled` since 12 and defaults it off; zou accepts `count()`, `sum()`, `avg()`, `max()` and `min()` as part of its select surface with no way to configure them away.
So a reference with the flag off would be refusing requests zou cannot refuse, and the comparison would be about the flag rather than about the surface.
That difference is real and it is [#555](https://github.com/tamnd/zou/issues/555), which is also why the four cases in upstream's `AggregateFunctionsSpec` that ask what the refusal looks like are not in the suite: there is nothing here for them to ask.

## The suite compared with GoTrue

The `auth` suite is over the endpoints a sign in flow uses: signup, the three token grants, the user endpoints, verify, recover, magiclink, otp, resend, logout, reauthenticate, the MFA listing, the admin endpoints including the audit trail and the factor endpoints, settings, health, jwks and authorize.
The reference is GoTrue 2.195.0 configured the way `supabase start` configures it for a project that has changed nothing, with the mail rate limits raised out of the way.
A rate limit is a configured number and a clock rather than a compatibility surface, and at the default of thirty an hour which case got the 429 would depend on how long the run before it took.

```
GOTRUE_MAILER_AUTOCONFIRM=true
GOTRUE_DISABLE_SIGNUP=false
GOTRUE_JWT_SECRET=super-secret-jwt-token-with-at-least-32-characters-long
GOTRUE_RATE_LIMIT_EMAIL_SENT=100000
```

```
cargo run -p zou-conformance -- diff --suite auth \
  --suites /tmp/zou-conformance/suites \
  --zou-dsn postgresql://postgres@127.0.0.1:5432/postgres \
  --reference-url http://127.0.0.1:54331 \
  --reference-dsn postgresql://postgres@127.0.0.1:5432/gotrue_ref \
  --reference-strip-prefix /auth/v1
```

The reference keeps its rows in a database of its own, which is the one place this suite differs from the others.
GoTrue brings its own migrations and runs them at boot, zou installs the auth schema on its first connection, and a run that let both land in the same database would be measuring whichever of the two wrote the schema last.

Nothing in the suite signs in and then uses the answer, because a case is one request.
The token for the seeded person is minted by the harness from the same secret both servers are configured with, and a case asks for it with `"key": "user"`.
A case about a missing or malformed token still names a key, so that the apikey goes out and the answer is about the token rather than about the gateway, and the case's own `authorization` header replaces the key's rather than being sent next to it.
That distinction has to be made here because zou is the gateway as well as the server, and a bare GoTrue has no apikey gate at all.

## The suite recorded from an image

The `storage` suite comes in three parts, the bucket endpoints, the object ones and the S3 protocol, each asked with a service key, an anon key, and in a good few cases with no key at all.
The reference is storage-api at the version in `versions.json`, and it is the one reference that cannot be downloaded and run on a flag line.
storage-api ships as an image and nothing else, so the recording comes from `supabase start` rather than from a binary, which is what the record workflow in this repository brings up.

```
cargo run -p zou-conformance -- check --suite storage \
  --suites /tmp/zou-conformance/suites \
  --zou-dsn postgresql://postgres@127.0.0.1:5432/postgres
```

That is why this suite has no diff step in CI while the other three do.
What the diff steps buy is the guarantee that a recording cannot sit in the file being believed after upstream changes its mind, and here that is paid for at the other end: bumping the pinned version means running the record workflow again and reading the diff it produces in `recorded.json`.

The fixture is rows and nothing else.
storage-api makes the storage schema with its own migrations, zou makes it on the first connection it takes out of the pool, and a `setup.sql` that made a third one would be measuring itself rather than either of them.
It does have to open one door to write those rows: the storage schema refuses a delete that did not come through the API, from a statement trigger reading a setting, and the fixture sets the same setting storage-api itself sets rather than dropping the trigger and putting it back.

The object cases are the reason a case can say `chained`.
An upload has to be read back, and a fixture cannot put the bytes there for it: a fixture writes rows and an object is bytes in a store no SQL statement can reach.
So the first case uploads, and the ones under it that write say they are asked against what the case before them left rather than against the fixture.

The image transforms are asked about here too, under `/storage/v1/render/image`, and they are the one group whose answers are not compared byte for byte: a case names the dimensions, the format and the header saying what was asked for, and gives up on the digest, because two jpeg encoders at one quality do not agree on a single byte.
What the object cases do not ask about yet is analytics buckets.
The S3 cases are the one group here that carries no token at all.
A client of that surface signs the request itself, so the harness computes the signature the way a client does, from an algorithm written out in `sigv4.rs` rather than borrowed from zou: a harness that signed with the server's own code would agree with the server about anything both of them got wrong.
Four of the keys a case can name are signatures rather than tokens, three of them correct in every way but one, which is how a case asks what a wrong secret, an unknown access key id or the wrong region is answered with.
The limits a bucket carries are asked, by cases that make their own bucket and chain off it rather than by a change to the fixture: a bucket in the fixture is a bucket every listing case has to count, so adding one would mean recording every case above it again.

## The suite compared with the stack rather than the binary

Everything above compares zou with a binary the job downloaded and configured on a flag line.
That is the same code a local Supabase project runs and it is not the same thing.
A project gets a gateway in front, the image list the CLI pins, and the api under `/rest/v1` because something put it there rather than because a suite asked for it.

So the `rest` suite is also diffed against `supabase start` itself, at the CLI version pinned in `versions.json` next to the images it brings.
The generated config is used with one line changed, the schema list, because PostgREST only serves the schemas it was told about and the suite keeps its tables in a schema of its own.

```
supabase init
supabase start

cargo run -p zou-conformance -- diff --suite rest \
  --zou-dsn postgresql://postgres@127.0.0.1:5432/postgres \
  --reference-url http://127.0.0.1:54321 \
  --reference-name "supabase start" \
  --reference-dsn postgresql://postgres:postgres@127.0.0.1:54322/postgres
```

The fixture goes into `supabase/migrations` rather than being applied afterwards.
PostgREST will not report itself healthy while a schema it was told to serve does not exist, and `supabase start` does not finish until every container is healthy, so a fixture applied after the stack is up never gets a stack that is up.
The harness applies it again over the reference dsn before it asks anything, the same as it does for every target, and the file drops what it creates first.

A hosted project is the third target the harness takes and the one CI cannot have, because it is somebody's account and somebody's key.
The command is the same with the url and the keys of that project, and it is worth saying out loud that `setup.sql` drops and creates schemas, so it wants a project made for the purpose rather than one with anything in it.

## The suites asked from somewhere else

Every suite above is asked by this harness, over an HTTP client written here.
That is the right way to compare two servers and the wrong way to answer a different question: whether the client a person actually installs works against zou.
supabase-js does its own URL building, its own token refresh, its own retries and its own error shapes, and a harness that reimplements them is testing the reimplementation.

So there are two suites that are not asked from here.
The first is supabase-js's own `test/integration.test.ts`, in `js/` in the conformance repository, run against zou with the URL and the keys made pluggable and nothing else touched.
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

Every block in the file runs and zou passes all of it: the client constructing, the PostgREST block, the RLS block, the Authentication block, the Storage block, the timeout configuration block and both Realtime blocks.
The one test that does not run is skipped by upstream itself, a binary broadcast over the older protocol version.
The Storage block is the one that says something the recorded storage suite cannot, because it goes through storage-js with the anon key and a policy on `storage.objects` decides rather than a service role going around it.
The Realtime blocks ran behind an environment flag while zou served no Realtime, which is what the Storage block did before storage was served, and both are upstream's own lines again.
They are the blocks that cost the most to pass: both protocol versions, every channel private and so decided by policies on `realtime.messages`, a socket that has to stay up while channels come and go and hang up when the last one leaves, and a broadcast posted over http arriving on a socket.

The second is storage-js's own tests, in `js-storage/`, and it is the same arrangement pointed at the client that owns the surface M3 is about.
Four files and 6 of upstream's snapshots, with everything in them passing against zou except what upstream skips itself.
storage-js is not released on its own: it lives at `packages/core/storage-js` in the supabase-js repository and ships with it, which is why `versions.json` pins it at the same 2.112.3.

Two things about it are worth writing down.
It runs under jest where the supabase-js suite runs under vitest, because the snapshot file is jest's and a snapshot is an expectation like any other here, so running it under something that writes the file in a different shape would be editing upstream's assertions rather than checking them.
And the tests are run out of the published package rather than out of a checkout: `@supabase/storage-js` ships its `src/` next to its `dist/`, so `import { StorageClient } from '../src/index'` is left alone and jest maps `../src` onto the package's own sources, which keeps the diff against upstream readable as a diff.

Both skips are image transforms and both are upstream's own, one for webp negotiation and one for `format: 'origin'`.
Three more were skipped here behind `ZOU_IMAGE_TRANSFORMS` while `/storage/v1/render/image` was the part of the storage api zou did not serve, and they run now.
One of them is what found `x-transformations`, the header a render carries saying what it was asked to do, which no recording had compared because the harness compares a fixed list of headers and that one was not on it.
That flag is the whole difference from upstream, and no assertion is edited.

Its fixture is upstream's seed rather than one written here, which is how it found something no suite could: the passwords in it are hashed with `crypt` and `gen_salt` unqualified, and until [tamnd/zou#214](https://github.com/tamnd/zou/issues/214) there was no `extensions` schema on the search path for those to resolve in.

## The suites that are ours, and how they are kept honest

There is a third shape, in `js-tus/`, and it exists because resumable uploads cannot be asked about one request at a time.

The recorded storage suite asks more about `/storage/v1/upload/resumable` than about any other route, and every one of those cases is one request with one answer.
That is the half of the protocol a recording can hold.
The other half is a conversation: a client picks its own chunk boundaries, asks where it got to after an interruption, believes the number it is told, sends the offset it believes in, and gives up if the server contradicts it.
A second process finishes what the first one started with nothing but a url.

So `js-tus/` drives the real tus-js-client, at the version pinned in `versions.json`, the way Supabase's own documentation writes a resumable upload, and reads every result back through supabase-js: the claim being made is that an object uploaded this way is an ordinary object, so the client that would have called `upload()` downloads it and lists it.
They cover an upload in one piece, an upload in three chunks, an upload interrupted and picked up by a second client, a terminated upload, and the four refusals.

The questions and the assertions there are both ours, which nothing else in this repository or the conformance one can say.
What keeps that honest is that the same file, unedited, is run against a real `supabase start` in the same CI job that diffs the rest suite against it.
A failure on that leg is this repository having written down something storage-api does not do, and it should be read as the suite being wrong rather than the server.

It earned its keep on the day it was written.
The documented flow uses a signed in user's access token rather than the service key, every recorded tus case had used the service key, and zou refused: the tables a resumable upload keeps its bookkeeping in have row level security on and no policy written about them, so the first insert an ordinary user made was refused before a byte moved.
The bookkeeping is written with the policies off now, as upstream writes it, and whether the caller may write the object at all is asked once at creation by writing the row and rolling it back, which is the same question the signed upload url route asks the same way.

The `supabase start` leg earned its keep on the same day, by refusing an assertion.
Every upload in that suite sends a `cacheControl` in its metadata, and the reference answers two different things about the same object: a listing reads `max-age=3600` off the row and the info route reads `no-cache` off the stored file.
zou used to keep one value and say `no-cache` in both places, so it matched the recording of the info route and not the listing.
It keeps both now: the row records what the metadata asked for, which is what a listing reads, and the info route and a download answer `no-cache` for an object a resumable upload made, which zou knows by the null `user_metadata` that path leaves behind and an ordinary upload never does.
The suite can ask about cache control again.

`js-realtime/` is the same shape for the same reason, about presence and about sending to a room over http.

supabase-js's own suite has broadcast in it and no presence at all, so there is nothing to copy, and presence is the other feature here that cannot be asked about one message at a time.
The server sends a joining socket the whole state once and a diff for every change after that, and what an application reads is the fold of them, which happens in the client.
A server whose diffs the fold cannot apply is a server whose own socket tests pass and whose users watch a room slowly stop matching reality.

So the suite drives the real client at the pinned version the way the Supabase documentation writes a presence channel, a key in the channel config and `track` after SUBSCRIBED, and reads the state back with `presenceState()`.
It asks about a tracker seeing itself, a late joiner being told who is already there, a join and a leave reaching everybody else, a client that disconnects without saying anything, one key in two tabs, a client with no key of its own, and presence and broadcast sharing a channel.
It needs no database and no fixture, because a channel touches no rows, so CI serves it from the front door example rather than from the harness.
Like the tus suite, it also runs against a real `supabase start`, and that leg is the only thing that makes it a statement about Supabase rather than about zou.

It corrected itself on the day it was written, which is the useful kind of finding.
Half the tests were written without a `presence` listener on the channel, read an empty state and failed.
That is the client doing exactly what it documents: it sets `config.presence.enabled` for any channel that has a presence binding and for nobody else, and the flag decides whether that client is *sent* presence rather than whether it can be *seen*.
The tests bind a listener where they read state now, which is what an application does, and one test is left over for the other half of the rule: a channel with no binding tracks, and the watcher sees it.

The broadcast file next to it is there for a smaller reason: supabase-js's own suite sends over a socket and never over http, and a client that cannot push has two different urls to post to.
8 more tests: the batch endpoint by hand, several messages in one post, a message with no event refused and not delivered, `send()` on a channel with no socket falling back, `httpSend()` putting the names in the url, a payload of bytes, a message reaching one room and not the one next to it, and a post with no key at all.
Two of them need a server new enough to have the single message url, and against one that is not they warn and skip rather than pass, so a run says which questions it did not get to ask.
The question about a payload of bytes is deliberately loose, since the two servers have not been shown to agree on what an octet-stream body is wrapped in, and tightening it is worth a run against a newer reference.

`js-realtime-private/` is a directory of its own for one reason: it needs a database and the two files above need none.

A private channel is the one part of realtime whose answer is not in the server.
Whether a room may be read, and whether it may be written to, comes out of row level security policies on `realtime.messages` that the project wrote about its own tables, with the room name in `realtime.topic()` and the person in `auth.uid()`.
So the suite ships a project rather than a configuration: a membership table, a row per person per room, a flag for whether that membership may send, and two policies that read it, applied to both targets unedited through `serve --setup` on one leg and `psql` on the other.
It asks about a join the policies allow, one no policy names, one asked with nothing but the project key behind it, a room named after the person and the same room asked for by somebody else, a broadcast between two members, a send to a room that may only be read, both http shapes, a batch with one allowed room and one refused one, a public channel of the same name hearing none of it, and two about `realtime.send()`.
The public channel one is the question the rest of them rest on: a public channel is joined by name with nothing read, so a private room and a public room of one name have to be two rooms or the policies are decoration.
The `realtime.send()` pair is the other way into a room, which a project reaches from a trigger on its own table: the suite has no connection to the database, so `setup.sql` puts an ordinary security invoker function in front of it and the questions call that as the person, which leaves the policies deciding as they do everywhere else here.

One question is deliberately not asked, and it is the only place these two servers are known to differ.
A push a write policy refused is dropped in silence by Supabase Realtime and answered with an error on the push by zou, and both look identical to a listener, so the suite asks what they agree on, that nothing arrives.
The difference is written down in [realtime.md](realtime.md) instead, because a suite that asserts a divergence stops being a record of what upstream does.

`js-realtime-changes/` is a fourth realtime directory, and it needs a database for a different reason than the third: the thing under test is a row nobody sent.

`setup.sql` there is five tables and one realtime line, the `alter publication` a project runs when it turns a table on.
Four of the tables are in the publication and one is not, which is the only way to ask whether a table nobody opted in is really left alone; one of the four publishes its old rows, which is the single setting that changes what an update and a delete carry; one has row level security on it with a policy about who owns a row; and one is written to by nothing but the frame test.
Every write in the suite goes through `/rest/v1` as the service key, because a suite holding a connection to the database could set a row up in a way no application could.
It asks about an insert, an update, a delete, the same update and delete on a table with `replica identity full`, a subscription for one event, a filter, a table nobody published, a policy withholding somebody else's row, a delete on that table telling everybody the key and nothing else, and two subscriptions on one channel.

The last one is a different shape of question, and the reason this suite exists rather than being three more tests in the file above.
Everything else reads what the client handed the application, which is the fold of the frames rather than the frames: the client takes the ids out of the join's answer, routes changes on them, renames `record` to `new` and hands over an object, and a server can get all of that right while sending something upstream never sends.
So the last test records the frames off the socket before the client has decoded them and compares them with what Supabase Realtime sent for the same three writes, which is `frames.json` in that directory, taken from a real `supabase start`.
Three things in a frame are true of a run rather than of a server and are replaced rather than dropped: a ref, the subscription ids, and the commit timestamp.
The ids become the position they held in the join's answer, which keeps the only thing about them that matters, that a change came back naming the subscription the join made.

It caught four things, from each direction it can be run.

Against zou, eight of the eleven failed on their first run, and they are the kind only a real client finds.
zou answered a join once its subscriptions were registered, and it takes its replication slot when the first subscriber arrives, so a client that subscribed and then wrote was told `SUBSCRIBED` while the slot was still being taken and the row it wrote next was written before there was anything to see it.
They passed with a second and a half of sleep in front of every write, which is how it was found.
The join reply waits for the tap now, and the socket test in `crates/zou-server/tests/changes.rs` that used to wait for a slot before writing asserts there is one instead.

Against the reference, the recording had a frame in it that zou did not send: a `system` event reading `Subscribed to PostgreSQL`, after the join reply and before any change.
supabase-js does nothing with it, which is exactly why nothing above the frames could have noticed, and an application binding `system` is handed it.
The reference also disagreed about an ordinary update: on a table with the default replica identity zou sent `old_record: {}` and Supabase Realtime sent the key, taken from the new row, which is the difference between a client being told which row this was and being told nothing.
Both are fixed in the server, and the eleven now pass against both.

The reference had the same shape of race in it as zou did, which is why every test in the file writes after the `system` frame rather than after `SUBSCRIBED`.
Upstream answers the join first and says `Subscribed to PostgreSQL` when the changes are actually flowing, and for the first subscriber to a project after Supabase Realtime starts that is about three and a half seconds later, so a suite that wrote on the join reply lost its first change on a stack that had just come up and never on one that had been running.
zou holds the join until the tap is open and then says both, so waiting for the second one costs it nothing.
And it says one more thing zou did not: a subscription to a table nobody added to the publication comes back on that frame with `status: error` and a message naming every parameter of the binding, where zou said `ok` and then had nothing to send, which is the same silence as a table that never changes.
Forgetting the `alter publication` is the most common way a subscription that reads correctly hears nothing, so it is worth saying out loud, and it is [#347](https://github.com/tamnd/zou/issues/347).
The wording is asserted whole now, on both servers, because it is what somebody reads in their console.

And the fixture itself was wrong in a way only the reference could say.
It granted the reads and left the writes ungranted, on the reasoning that a service key has `bypassrls` and needs no grant, and every write in it failed against `supabase start` with a 403: `bypassrls` is about policies and grants are grants.
zou took the writes because its bootstrap granted the three api roles everything in `public`, which upstream's default privileges do not, and that was [#344](https://github.com/tamnd/zou/issues/344) rather than something to hide in a fixture.
The bootstrap grants what upstream grants now, so a fixture that means to write says so on both legs.

`js-functions/` is the only directory here that ships code somebody deploys rather than requests somebody sends.

An edge function is not a request, it is a project: a directory per function, an `index.ts` in each, `_shared` beside them, and a `config.toml` that says which of them may be called without a token.
So the suite is that project, and both legs put it on disk unedited, one under `zou functions serve` and the other under a `supabase start` with edge-runtime in it.
There is nothing upstream to copy, the way there was nothing for presence: supabase-js's own suite has one line about functions in it and edge-runtime's tests are about the runtime rather than about the surface a project sees.
Three files, and the reference leg is what makes them a statement about Supabase rather than about zou.

`entry.test.ts` is the three ways a module says what to run, all three deployed and all three called, because `Deno.serve` is the documented one and the other two are what most functions in the wild are written with.
It is also where a function the config file names rather than the listing is asked for, with its entrypoint two directories down and no directory of its own, which is how the examples project deploys its mcp server.
`invoke.test.ts` is the invocation: supabase-js's own `functions.invoke`, the refusals, the two error pages, and an answer arriving before the work `waitUntil` was handed has finished.
The refusals are three rather than one because the server tells them apart before it verifies anything, and no header, a string that is not a token, and a token signed with something else are three codes and three messages.
`runtime.test.ts` is what the handler is given: the environment variables, the web surface, giving up on a call, copying a value, a body that arrives in pieces as it is written, the headers describing the request that reached the front door, and CORS.

Three questions have two right answers and all three are asserted rather than skipped, because a suite that skips a divergence stops noticing the day one of the two changes.
A preflight is answered by kong on the local stack before the function is reached, and zou hands it to the function the way the hosted runtime does, so the `_shared/cors.ts` that is in every Supabase example is what decides.
`SB_EXECUTION_ID` is a uuid on both, and the local stack mints it when the worker is made rather than when a call arrives, so five calls a second apart come back with one id where zou hands each call its own.
`SUPABASE_JWKS` is set on the local stack, which signs a project's tokens with a key pair and publishes the public half, and unset on zou, which signs with one shared secret and has nothing asymmetric to publish.

There was a fourth difference that a suite cannot see, and it is closed.
Giving up on a call is two questions here, the three statics and what a signal handed to a `Request` reaches, and both servers answer both the same way down to the message on the rejection.
What differed was the connection: upstream tore the socket down and zou ended the waiting while the request ran to its own end.
The only witness is the far end, so it was measured with a slow server that reports what it saw, which came back with a broken pipe from upstream and with an answer nobody read from zou.
Both tear it down now, and the far end is where it is asserted too, in `crates/zou-deno/tests/hangup.rs`.

The zou leg is run twice, and that is a claim the other suites cannot make.
The first run serves the project directory, the way a person does while writing a function.
The second deploys that same directory to a store with `zou functions deploy`, serves what was written there with `zou functions serve --target`, and asks the same questions of it, on a process that has no project directory at all.
A deploy carries a subset of a project on purpose, it drops every dotfile and it flattens what the config file said into a manifest, so whether what comes back is still the project is exactly the kind of thing that is easy to assume and cheap to ask.
Both legs are `needs` of the scoreboard job, so a deployment that answered differently stops the page from being regenerated.

It caught one thing on the day it was written, and it is the kind only an end to end suite finds.
A handler that threw answered 500 after exactly sixty seconds.
The answer travels back on a channel whose sending end lives in the isolate, the error path returned without taking it, and under the default policy the isolate is kept, so the caller waited for the minute it takes a kept isolate to go idle.
Every unit test around it passed, because none of them kept the isolate afterwards.

## The corpus, which is not a suite

M4 names Supabase's own examples repository as a gate, and that is a different kind of measurement from everything above.
A suite is a list of questions with a recording beside it and a case either matches or it does not.
The examples project is forty two functions somebody else wrote, most of which want a secret nobody has and half of which fetch what they import off the network at boot.
There is no pass in it.

So it is written down as a measurement rather than run as a suite, in `examples/` in the conformance repository, and nothing in CI touches it.
`corpus.sh` lays the project out from a checkout of `supabase/supabase` at the commit in `versions.json`, `probe.sh` asks one server every function in it the same question, and `measured.json` is what both servers said.
The project is not vendored: it moves, and pinning the commit is the part that matters.
`corpus.sh` was checked against the directory the numbers were taken from and there is no diff, which is the whole claim a corpus number rests on.

The question is a POST with an empty json body and the project's anon key, which is the smallest question that reaches a handler.
A function ran when its own code or its own library produced the outcome: a refusal from the gateway, a config error out of a library it imports, a throw from the handler.
It did not run when the module never finished loading or when it reached for a runtime api that is not there.
Which of the two happened is read off the server log rather than off the status, because from the outside both are a 500.

40 names were asked, the reference on 2026-08-17 and zou again on 2026-08-19.
zou ran 32, `supabase start` ran 34, they agree on 29, and 24 of the 40 answer the same status with the same bytes on both.
The 40 is the union of the two lists a server serves: 39 of the 42 directories have an `index.ts`, and `config.toml` adds `simple-mcp-server`, which has no directory at all.

One of the 40 is asked of one server only, and it is worth a sentence because the two refuse it differently.
`wasm-modules` imports a file a wasm build writes and nobody built, so neither server can load it.
zou serves the other thirty nine and answers 500 for that one, and the CLI reads the whole project before it starts anything and will not bring the stack up at all while the function is in the directory, so the reference copy has it taken out.

The five most useful rows are the failures that are byte for byte identical on both servers, out of Resend, the OpenAI sdk, ElevenLabs, Stripe and grammy.
Those are third party npm packages resolved, loaded and run far enough to make a decision about their own configuration, and the two runtimes reached the same sentence.

The identical count was 16 until the day zou set `SUPABASE_PUBLISHABLE_KEYS` and `SUPABASE_SECRET_KEYS`.
Most of this corpus goes through `npm:@supabase/server`, which builds a client out of those before a handler runs and refuses the request when it finds neither, and four functions went straight from that refusal to the reference's own answer.
A fifth, `custom-jwt-validation`, got past the client and landed on `AbortSignal.timeout`, which zou did not have, so the environment had been hiding a runtime gap rather than there being nothing there.

The signal arrived and that function moved again, to `structuredClone`, and the copy arrived and it moved a third time, to where upstream stops: both servers now refuse the same jwks out of the same library for the same reason.
The two bodies still differ by one word, the name of the error class, which is `JOSENotSupported` upstream and `I` here because esm.sh serves jose minified and the class names its errors after itself, and that is [#435](https://github.com/tamnd/zou/issues/435) rather than a runtime difference.
Nothing else in the corpus moved with any of it and the identical count was still 20 at that point, which is the honest shape of a gap found this way: each one is one function's next line, and the value is in the finding rather than in the number.

Four names have moved since, one runtime gap each, and the identical count is 24.
`oak-server` boots, because a module whose last line is `await app.listen({ port: 8000 })` is parked on an op now rather than waited out and answered 500.
`connect-supabase` builds the cookie store it could not build, because `crypto.subtle` has AES.
`image-manipulation` reads fourteen megabytes of wasm out of a package, because `import.meta.resolve` answers with the module the registry served and `Deno.readFile` takes the url beside it.
`postgres-on-the-edge` opens a socket to the engine, authenticates, runs a select and is told by postgres that the table is not there, because `Deno.connect` is there, and that sentence is the one upstream is told.

The three zou runs and upstream does not are all one shape: upstream builds the module graph ahead of time and refuses a graph it cannot complete, and zou fetches a module when something asks for it, so a type only file nobody imports at runtime is never fetched.
The five the other way are none of them the runtime any more: three are esm.sh answering 500, one is a browser build missing an export, and one is the mcp sdk failing inside the registry's build of itself.

One finding out of the corpus is not in the file and cost the most to get.
esm.sh serves different code for different `User-Agent` headers: asking as Deno gets a build that expects node built ins, and asking as a browser gets one that does not.
zou asks as a browser deliberately, and the corpus is the reason, because on the build measured at the time asking as Deno took it from 28 running to 21.

## The package a project installs, asked the same questions

Everything above runs a zou this harness linked in, against a Postgres somebody else brought up, usually in a container.
That is one build of the code.
What a node project installs is another: `npm install zou`, an addon that starts a patched postmaster over a directory, and the whole api answering inside the node process with no socket under it and no container anywhere.

So the suites are asked of that one too, by `conformance/embedded.mjs`:

```
node conformance/embedded.mjs storage --suites /tmp/zou-conformance/suites
```

It opens a project through the package, hands the harness the url and the dsn it got back, and closes it again on the way out.
The suite decides the configuration rather than the script: the schemas a suite names, the role its anon key carries, and the S3 pair every target in a run has to be asked with all come out of `cases.json`, because a suite derived from upstream's fixtures keeps its tables where upstream keeps them and grants to a role of its own.

The `rest`, `auth` and `storage` suites run this way in the postgres build workflow, which is where the patched postgres the addon needs is built.
The `postgrest` suite is left out by name rather than by accident: it re-applies its rows before every case that writes, which against a postmaster the test process started is minutes of fixture for the same claim the other three make.

What this catches is anything that is true of the harness's zou and not of the shipped one.
Both are the same crates, so it is the configuration that differs, and configuration is exactly where a library that cannot be told something quietly does without it.
It found one immediately: the S3 protocol surface takes a key pair, the harness had always set one, and nothing else could.
`zou dev`, `zou serve` and the embedded library all served an endpoint that answered every signature with a key nobody could have.
All four can be told one now, each in the shape that suits it: the library and the package take a pair as an option, `zou dev` answers to the fixed pair a local Supabase project answers to, and a served project's pair is its own, out of the registry entry the JWT secret already came from.

## The app

A suite passing says every answer matched a recording. An app working says the answers were enough to build something on, and the second does not follow from the first.

So one of Supabase's own example apps is run against zou with nothing changed in it, in a real browser: `examples/todo-list/sveltejs-todo-list`, in `demo/` in the conformance repository.
The diff against upstream is one file upstream does not ship, the `.env` its own `.env.example` asks for, and the url and key in it are the ones the Supabase CLI prints for a local project.
No test hook, no id added to a button, no module replaced.

Four things get driven, each of them something a person does: signing up and adding a todo that is still there after a reload, two accounts that cannot see each other's rows, the anon key on its own reading nothing, and signing in with Github twice landing on the same account rather than on a second one that looks like it.

The Github login is the reason `ZOU_EXTERNAL_GITHUB_URL` exists.
There is no account to sign in as on github.com and no consent anybody can give in CI, so a stub stands where github does, reached the way a GitHub Enterprise install is reached.
Everything on zou's side of that is the real path: the code exchange, the two profile calls that find the address github keeps out of the profile document, the identity row, the session in the url fragment, and supabase-js parsing it out.

## The other app

That one asks whether an application can be built on the answers. There is a second half to it, and a suite cannot ask it at all: whether an application can be built on the frames.

A realtime suite compares one frame with a recording, one at a time. Every frame in it can match while the thing a person is looking at never changes, because what is on the screen is the application's own state and the frame is only what arrived. The claim is somebody typing and somebody else seeing it, so the only way to ask it the way it is felt is with two browsers open at once.

So a second app, run the same way with nothing changed in it: `examples/slack-clone/nextjs-slack-clone`, in `demo-realtime/` in the conformance repository. It subscribes to `postgres_changes` on three tables through unmodified supabase-js, all three are `replica identity full`, and every one of them has row level security on it.

Five things get driven, each of them two people in a room. A message one person sends arriving on the other person's screen, with the author on it, on the page that was already open. A message taken back disappearing from the other screen too. A channel one person makes appearing in the other's sidebar, and being a room rather than a name: the second person walks into it and sends the first message in it. A third browser with no session, holding the anon key out of the javascript bundle and subscribed to the same three tables, hearing nothing of what the other two are passing, asked of the socket and of the api both. And an admin taking back a message that is not theirs.

"Without a reload" is asserted rather than implied. Every page carries a mark on its window that a reload would wipe, nothing in the app reads it, and it is checked at the moment the change shows up.

The last of the five is the reason `ZOU_HOOK_CUSTOM_ACCESS_TOKEN_ENABLED` appears in CI. The project decides who is an admin in a trigger of its own, writes it into a table of its own, and hands it to the token in a postgres function of its own, named in its `supabase/config.toml`. zou calls that function while minting, the policy that allows the delete reads the claim back with `auth.jwt() ->> 'user_role'`, and the button only renders for somebody the policy would allow.

Running that app found two things a suite had not. Its first migration ends with a grant to `supabase_auth_admin`, which every Supabase database has and zou's bootstrap did not create, so the schema did not apply at all. And the harness built its configuration without reading the hook settings, so the claim was never minted and the admin had no button.

## The third app

Both of those read their code off a disk. A hosted project does not: what runs there was deployed, which is a manifest, blobs addressed by their hashes, and a materialize step on the way back out. Nothing that asks a running server can tell the two apart, so the only way to ask is to deploy first and then drive the thing that answers.

So a third app, `examples/edge-functions`, in `demo-functions/` in the conformance repository. Its functions are deployed to a store and served out of it, and the app invokes them from a browser on another origin. That makes it the longest chain in the product: the client library, a gateway, an isolate built out of the store, a package fetched off a registry, that package calling this same server's rest api with the browser's own access token, and postgres deciding what the person holding it may see.

Six things get driven. The app being upstream's own, listing upstream's four options in upstream's order. A greeting that came out of a deployed function's own code. The claims the function verified being the person at the browser, address and all. Two accounts asking the same function the same question and getting one row each, and not the same row. An anon key not being a person, so the function refuses instead of answering. And the invoke being cross origin, so the browser preflights it and the function answers the preflight rather than the gateway.

Running this one found the two things that made it possible at all. A project published an empty key set and signed its access tokens with a secret and no key id, which is two refusals before `@supabase/server` attempts a verification, so every function that asked for a user answered 401. And zou's own runtime read `raw` keys and nothing else, so `jose` asking for a jwk threw before it reached the signature, and there was no ECDSA to use a P-256 key with even once it imported. Both are in [tamnd/zou#486](https://github.com/tamnd/zou/issues/486).

One step here is not the browser's: accounts are made through the admin api, because a node serving projects out of a registry has no mail settings of its own yet and a sign up in the browser ends at a confirmation nobody can send, which is [tamnd/zou#488](https://github.com/tamnd/zou/issues/488).

## Where zou stands

Not here.
Every count, per suite and per endpoint and per feature, is in [docs/scoreboard.md](scoreboard.md), which is generated out of the run the last merge failed or passed on and committed by CI when a number moves.
This section used to hold a paragraph per suite with the numbers typed into the sentences, and by the time anybody noticed, three of them were wrong: the `rest` suite had grown by nine cases, the `auth` suite by three, and the supabase-js line was reporting half of what that suite runs.
None of it was caught by anything, because there is nothing to catch it.
A count in a sentence is a count somebody has to remember to change, and the whole point of the scoreboard is that nobody has to.

What belongs here is what the scoreboard cannot say, which is why a number is what it is.

The `auth` suite is the only one of the recorded four that does not pass in full, and the difference is deliberate in every case.
zou answers `/health` with its own version rather than claiming to be a GoTrue release it is not, it answers `saml_private_key_next_configured` false where upstream answers true with SAML off, it publishes a signing key where upstream publishes an empty set because a project on zou signs its access tokens with a key derived from its own secret, and it fills the identity list in on the admin listing where upstream answers null because its ORM does not load the association on that query.
Each of the four is written out in `known.json` with the reason next to it, and each is still counted as a failure on the scoreboard, so the number there cannot be improved by writing something down.

The `postgrest` suite asks everything upstream asks itself, including the parts of PostgREST nobody using Supabase has ever typed, and what it took to get there is written down in [tamnd/zou#125](https://github.com/tamnd/zou/issues/125).

The five suites this project wrote have lines of their own on the scoreboard, `js-tus`, `js-realtime`, `js-realtime-private`, `js-realtime-changes` and `js-functions`, and they are labelled with what they were compared with rather than being folded in with the recordings.
The number on each is the zou leg, and what makes it a compatibility number rather than a self assessment is the other leg: the same file runs against a real `supabase start` in the diff job of the same run, so an assertion the reference does not pass is the suite being wrong.

The cases that pass "written differently" are all the same three things: zou puts a space after each colon where PostgREST puts a newline between rows, a `select=*` comes back with the columns in a different order, and two auth answers carry their keys in a different order than Go wrote them.
None of them is a difference in what was said, which is why the harness has a third verdict for them, and they are left as they are until something turns out to depend on them.

## The scoreboard

[docs/scoreboard.md](scoreboard.md) is every number out of the run, and CI rewrites it on every merge to main, out of the json the conformance jobs uploaded rather than out of a run of its own.
Rendering it from a fresh run would publish a number nobody had failed a build over.

```
cargo run -p zou-conformance -- scoreboard \
  --report /tmp/reports/conformance.json \
  --report /tmp/reports/auth.json \
  --report /tmp/reports/storage.json \
  --js supabase-js=/tmp/reports/js.json \
  --js storage-js=/tmp/reports/js-storage.json \
  --ours js-tus=/tmp/reports/js-tus.json \
  --ours js-realtime=/tmp/reports/js-realtime.json \
  --ours js-realtime-private=/tmp/reports/js-realtime-private.json \
  --ours js-realtime-changes=/tmp/reports/js-realtime-changes.json \
  --ours js-functions=/tmp/reports/js-functions.json \
  --pin "$(jq -r .ref conformance/suites.json)" \
  --out docs/scoreboard.md \
  --compat docs/compatibility.md
```

Two cuts through the same run, and the second one is the useful one.
A feature says what somebody was testing, which is how the suite is organised, and it is upstream's vocabulary: "upsert is at 52%" names a chapter of somebody else's test file rather than a piece of work.
An endpoint says what the server had to implement, with the fixture's table and function names taken out, and "PATCH /rest/v1/{table} is at 28%" is a piece of work.

The last flag is the same numbers going somewhere else.
`docs/compatibility.md` is a hand written page with one table in it, and `--compat` rewrites the numeric cells of that table in place between a pair of markers and leaves every word around them alone, so the one page a reader is most likely to open first cannot be carrying a count nobody has measured since they typed it.
It refuses in both directions, on a suite the page has no row for and on a row no suite in the run answers to, because filling in what matches and skipping the rest is exactly how a row outlives the suite it was counting.

There is no date and no run number in the file, so a merge that moved no number leaves no diff and makes no commit.
The commit it does make carries `[skip ci]`, so the workflow does not start again to measure what it has just measured.
