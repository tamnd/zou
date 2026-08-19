# Migrating a Supabase project

`zou import supabase` reads a hosted project, says what moving it here would cost, and then moves it.

The order is deliberate. A migration tool that finds out halfway through is worse than no migration tool, because the half is now somebody's problem. So the survey is a step of its own and it always goes first, whether or not a copy follows: connect to the project, count what is there, and write a report naming every extension, schema, role and object that was found and what happens to each of them here. Nothing is silently dropped, which in practice means the report has a section for the things that do not come over, and a section for the things the survey did not look at, and neither one is ever empty by accident.

## Running it

```
zou import supabase --project-ref abcdefghijklmnopqrst --dry-run
```

That reads and reports and copies nothing. The project ref is the one the dashboard prints, and it becomes `db.<ref>.supabase.co:5432` with the database password. The password comes from `--db-password` or from `SUPABASE_DB_PASSWORD`, and it is percent encoded on the way into the url, so a generated password with `@` or `/` or `#` in it works without anybody having to escape it by hand.

Reading the report, then moving it:

```
zou import supabase --project-ref abcdefghijklmnopqrst --to "postgresql://postgres@127.0.0.1:5432/postgres"
```

The other way in is the connection string itself, which is what a project not on the hosted platform will have:

```
zou import supabase --db-url "postgresql://postgres:pw@host:5432/postgres?sslmode=require" --dry-run
```

| flag | what it does |
| --- | --- |
| `--project-ref <ref>` | Build the hosted url from a project ref. |
| `--db-url <url>` | Use a connection string as given. Not with `--project-ref`. |
| `--db-password <pw>` | The database password for a project ref, or `SUPABASE_DB_PASSWORD`. |
| `--to <url>` | The database to copy into. Without it the run is a survey. |
| `--dry-run` | Survey only, said out loud so that a run with no target is not a typo. |
| `--report <path>` | Where the report goes, `import-report.md` by default. |
| `--store <target>` | Where this server keeps its objects, which is where the storage object bytes go. Needs `--to`. |
| `--tenant <ref>` | Which tenant of that store, `local` by default. |
| `--service-key <key>` | The service role key, or `SUPABASE_SERVICE_ROLE_KEY`. It is what reads a private bucket. |
| `--storage-url <url>` | The storage api to fetch objects from. Worked out from a project ref, required with `--db-url`. |
| `--jobs <n>` | How many objects are fetched at once, 8 by default. |
| `--manifest <path>` | Where the digests go, `import-objects.sha256` by default. |

Everything the survey runs is a catalog read or a `count(*)`. The source is not written to at any point, by the survey or by the copy, and no lease is taken on anything. A probe that fails is written down and the survey carries on, because a project with one table the connecting role cannot see is still a project worth reporting on.

### TLS

A hosted project only answers over TLS, so `sslmode=require` is what the generated url carries. `require` in libpq means an encrypted socket to whoever answered and no statement about who that was, and that is what it means here too, which matters because a hosted project's certificate is signed by its provider's own authority rather than by anything in the public roots.

`sslmode=verify-full` or `sslmode=verify-ca` in a `--db-url` asks for the other behaviour, the public roots and the hostname, and the command honours it. Those two spellings are taken out of the url before it is parsed, because the client library only parses the first three modes, and they are turned back into the check they name. So the spelling somebody already knows works and there is no second knob to learn.

## What the report says

Seven sections, in the order somebody moving a project cares about them.

**What comes over.** Schemas, tables, views, sequences, rows and bytes, one row per schema and a total. The row counts are the planner's estimate out of `reltuples` and are labelled as an estimate everywhere they appear, because a `count(*)` over every table in a hosted project is a bill somebody pays for a number that is stale by the time it is read. Then policies, tables with row level security on, triggers and routines. Routines an extension brought with it are not counted, only the ones the project wrote, otherwise pgcrypto and uuid-ossp alone answer with forty seven.

**Extensions.** Three lists. The ones built here, so `create extension` does what it did there, which is the contrib set the vendored Postgres builds plus pgvector. The ones zou answers for by other means, which today is pg_net and pg_cron: the schema and the tables are there and the behaviour is the server's rather than a background worker's, see [docs/webhooks.md](webhooks.md) and [docs/cron.md](cron.md). And the ones with no answer here, each with a sentence saying what that costs. An extension nobody has classified lands in the third list rather than being left out, so a project using something unusual sees it named.

**Auth.** Users, of which how many have a password and how many a confirmed email and how many a phone, identities broken down by provider, mfa factors, sso and saml providers, refresh tokens. Every count is guarded by a lookup first, so a project on an older auth schema reports what it has instead of failing on a table that was added later. The section also says the two things somebody needs to know before the cutover: passwords are bcrypt on both sides, so a user who had one signs in here with the same one and nothing is reset, and refresh tokens are deliberately not brought over, so everybody signs in again once and no token minted by the old project is accepted by this one.

**Storage.** Buckets, how many of them public, objects, uploads in flight, and the summed size out of the recorded metadata.

**Roles and ownership.** A hosted project has a role per platform process, because auth, storage, realtime and the pooler are separate programs there. Here they are one process on one pool, so those roles do not exist, and a dump that names them in its grants has to be restored without owners and privileges or with those roles created first. The report tells the platform's roles apart from the ones the project made itself and lists them separately, because only the second list has to be recreated. `anon`, `authenticated`, `service_role` and `postgres` are here already and are not listed as work.

**The rest of it.** Publications and how many tables each covers, foreign servers, event triggers.

**What this did not look at.** Column level grants and default privileges, large objects, replication slots and subscriptions, configuration set with `alter database` or `alter role`, and the contents of any table. Plus anything a probe failed on, with the error. This section exists for the same reason `zou db diff` prints one: a section that came back empty from a tool that never looked reads exactly like good news.

## What the copy does

`--to` copies, in eight steps, in this order:

1. **Extensions.** `create extension if not exists` for each one the project has that is built here. The ones zou answers for by other means are not created, because there is nothing to create, and the ones with no answer here were already named in the report.
2. **Schemas.** Every schema the project owns, which is what the survey found minus the platform's own and minus this server's.
3. **Sequence definitions.** The sequences a column defaults to, created before the tables that call `nextval` on them. Identity sequences are skipped here because the identity column makes its own.
4. **Definitions.** Tables, constraints, indexes, views, functions, triggers, policies and comments, read out of the source catalog and diffed against this one.
5. **Data.** One `copy` out of the source straight into a `copy` into the target, table by table, with `session_replication_role = replica` so foreign keys and user triggers do not care what order the tables arrive in. Generated columns and partition leaves are left out, because the one is computed here and the other arrives through its parent.
6. **Sequences.** `setval` to where the source had got to, and `alter sequence ... owned by` now that the tables exist.
7. **Platform rows.** The `auth` and `storage` rows, into the tables this server already made at startup.
8. **Grants.** Usage and privileges for `anon`, `authenticated` and `service_role` on each schema that arrived, plus default privileges so the next table made in one of them is reachable too.

The definitions step goes through the same code `zou db diff` does, given an empty catalog to diff from, so it can only ever produce creates. If a `drop` were ever to come out of it the copy stops instead of running it.

### Sessions, and the other things left where they are

Step 7 does not copy every `auth` and `storage` table it finds. Fourteen of them are left behind on purpose, and each one gets a `not copied:` line in the run's output saying which and why, because a table the report counted and the copy skipped is exactly the kind of thing somebody finds out about months later.

The first kind is a session. `auth.refresh_tokens` and `auth.sessions` do not come, and neither does `auth.mfa_amr_claims`, which records how a session was proved. This is the policy the report states in its auth section: no token minted by the old project is accepted by this one, so everybody signs in again once after the cutover and that is the whole of it for a signed in user. Passwords are bcrypt on both sides and they do come, so signing in again means typing the same password, not resetting it.

The second kind is something in flight. A sign in part way through a redirect (`auth.flow_state`, `auth.saml_relay_states`, `auth.oauth_client_states`), a factor being proved right now (`auth.mfa_challenges`, `auth.webauthn_challenges`), a confirmation or recovery link already sent (`auth.one_time_tokens`), a multipart upload with its parts still on the old project (`storage.s3_multipart_uploads` and `storage.s3_multipart_uploads_parts`). All of these were going to expire in minutes anyway, and the other half of each one is on a server nobody is going to talk to again. A recovery link sent before the cutover points at the old project and has to be asked for again.

The third kind is the platform's own bookkeeping about the project rather than anything in it: `auth.schema_migrations` and `storage.migrations`, which are GoTrue's and the storage service's migration histories and not this server's, and `auth.instances`, which is the platform's row about the project.

Everything else in those two schemas comes over, which is the part that matters: users, identities, mfa factors, sso and saml providers, buckets and the object rows.

### Resume

Each step is one transaction, and the row that records the step is written inside that transaction. So a step either happened and is written down, or did not happen and left nothing behind, and there is no third state to detect. Running the same command again skips what the ledger already names and prints what it skipped:

```
copied 0 rows in 0 steps, 8 steps were already done
```

The ledger is `zou.import_progress`, one row per step with its row count and the time. Deleting a row from it makes that step run again, which is the supported way to redo one.

### What it will not do

It will not import into a database that already has the project's tables in it. A target with anything of its own in a non platform schema is refused by name, because merging two projects is a decision somebody makes deliberately and not something a migration tool does on their behalf.

The `auth` and `storage` tables are the exception, because this server makes them at startup and so they are always already there. Those two are copied column by column on the intersection of what the source has and what this server has, and every column and table left out of that intersection is named in the output rather than dropped quietly. A platform table that already holds rows here is refused rather than appended to.

## The storage object bytes

A storage object is two things in two places: a row in `storage.objects` saying which bucket it is in and what it is called, and the bytes, which on a hosted project live behind the storage api and here live in the object store. Step 7 brings the rows. `--store` brings the bytes:

```
zou import supabase --project-ref abcdefghijklmnopqrst \
  --to "postgresql://postgres@127.0.0.1:5432/postgres" \
  --store /var/lib/zou --service-key "$SUPABASE_SERVICE_ROLE_KEY"
```

Each object is fetched from `/storage/v1/object/authenticated/<bucket>/<name>` with the service role key, which is what reads a private bucket, and written to `tenants/<ref>/files/objects/<id>/<version>`, which is the key this server reads it back from. The `id` and the `version` both come off the row, so nothing about the key is invented here and writing the same object twice writes the same bytes to the same place.

That is also why this step's ledger, `zou.import_objects`, is an optimisation rather than a correctness requirement, unlike the one the eight steps use. Losing it costs a second download and nothing else, which is why it is written a chunk at a time: a run killed halfway repeats at most a chunk.

A row whose bytes are gone on the far side answers 404. Rather than stopping, which would leave every later object unfetched over one deleted file, it is named and the run carries on. A wrong key answers 401 and does stop, because every remaining object would answer the same way. An object whose bytes are not the size the row recorded is copied anyway, the bytes being the thing that is real, and the disagreement is printed.

The manifest at `import-objects.sha256` is a sha256 and a size per object in the shape `sha256sum` prints. It is written from the ledger rather than from the run, so a resumed run still writes one covering every object rather than only the ones it happened to fetch.

Nothing here is a server side copy. The source is one provider's storage and the target is another's, so every byte goes through the machine running the command. `--jobs` is how many at a time.

## What a user does the morning after

They sign in, with the password they already had. Nothing is reset and no link is sent.

Passwords are bcrypt on both sides and the hash comes over as it was written, so the same password verifies against the same hash. The user's uuid comes over too, which is what makes an RLS policy written against `auth.uid()` keep matching the rows it matched before, and their identities come over, so an account that signs in with GitHub still signs in with GitHub and a client reading `app_metadata.providers` to decide which buttons to draw sees what it saw. A confirmed address stays confirmed. A second factor enrolled there is still enrolled here: the TOTP secret is carried, so the code the authenticator app is already showing is the code this server expects, and the account goes to `aal2` on it without anybody re enrolling a phone.

What changes for them is the one sign in. Their old token is refused, in GoTrue's words, which is the message every Supabase client already handles by sending them to the sign in page.

`crates/zou-server/tests/auth_imported.rs` is that paragraph as tests. It seeds the rows in the shape the platform's `auth` schema has them, a Go bcrypt hash and two identities and a verified factor and deliberately no session, and then puts the auth api in front of them.

## Going the other way

```
zou export --db-url postgresql://postgres:pass@127.0.0.1:5432/postgres --to ./out
```

That writes four files and nothing else. `schema.sql` is the ddl, `data.sql` is the rows, `platform.sql` is the `auth` and `storage` rows, and `export-report.md` says what is in them and what is not. Restoring is three commands in that order:

```
psql -v ON_ERROR_STOP=1 -d <target> -f schema.sql
psql -v ON_ERROR_STOP=1 -d <target> -f data.sql
psql -v ON_ERROR_STOP=1 -d <target> -f platform.sql
```

The target can be a stock Postgres, a hosted Supabase project, or another zou. Nothing in the three files is a zou format: the ddl is sql and the rows are Postgres's own copy text format, which is what `pg_dump` writes and what every Postgres since the nineties reads. If this program disappeared tomorrow the files would still restore.

The third command comes last and is the one to skip when the target has no auth in it. `auth` and `storage` are made on the far side by whatever runs them, the platform on hosted Supabase and this server at startup on another zou, which is why their rows are in a file of their own rather than mixed in with the project's.

Two things make the restore work without being a superuser on the target. The tables in `data.sql` are in dependency order, worked out from the foreign keys, so a key never points at a table whose rows have not landed yet. And each table's user triggers are turned off around its own rows, which a table's owner may do and a superuser is not needed for, because those triggers already ran on the database the rows came out of.

What does not come out is in the report, and that section is never empty. The object bytes are not in these files, the rows in `storage.objects` that name them are, and the bytes are in the object store where any client can read them. Roles and their passwords belong to the cluster rather than to the database. Sessions and refresh tokens are left behind for the same reason the import leaves them, so a user signs in once on the other side. And the two extensions this server answers for rather than installs, `pg_net` and `pg_cron`, are named rather than written as a `create extension` that would fail, because a project using them needs the real ones wherever the files land.

Foreign keys that point in a circle are the one case ordering cannot solve. Those tables are still written, last, and the report names them, because their keys are checked as their rows land and somebody should hear that before they run it rather than after.

## Related

- [docs/compatibility.md](compatibility.md) for what a project will notice once it has moved, which is the prose the extension lists here are drawn from
- [docs/quickstart.md](quickstart.md) for pointing an existing client at zou
- [docs/operations.md](operations.md) for `zou doctor` and the rest of running it
