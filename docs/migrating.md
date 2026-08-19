# Migrating a Supabase project

`zou import supabase` reads a hosted project and says what moving it here would cost. Today it reads and reports. It does not copy yet, and the command refuses to run without `--dry-run` rather than pretending otherwise.

The order is deliberate. A migration tool that finds out halfway through is worse than no migration tool, because the half is now somebody's problem. So the survey is a step of its own and it goes first: connect to the project, count what is there, and write a report naming every extension, schema, role and object that was found and what happens to each of them here. Nothing is silently dropped, which in practice means the report has a section for the things that do not come over, and a section for the things the survey did not look at, and neither one is ever empty by accident.

## Running it

```
zou import supabase --project-ref abcdefghijklmnopqrst --dry-run
```

The project ref is the one the dashboard prints, and it becomes `db.<ref>.supabase.co:5432` with the database password. The password comes from `--db-password` or from `SUPABASE_DB_PASSWORD`, and it is percent encoded on the way into the url, so a generated password with `@` or `/` or `#` in it works without anybody having to escape it by hand.

The other way in is the connection string itself, which is what a project not on the hosted platform will have:

```
zou import supabase --db-url "postgresql://postgres:pw@host:5432/postgres?sslmode=require" --dry-run
```

| flag | what it does |
| --- | --- |
| `--project-ref <ref>` | Build the hosted url from a project ref. |
| `--db-url <url>` | Use a connection string as given. Not with `--project-ref`. |
| `--db-password <pw>` | The database password for a project ref, or `SUPABASE_DB_PASSWORD`. |
| `--dry-run` | Read and report. Required, because reading is all there is so far. |
| `--report <path>` | Where the report goes, `import-report.md` by default. |

Everything the survey runs is a catalog read or a `count(*)`. The source is not written to and no lease is taken on anything. A probe that fails is written down and the survey carries on, because a project with one table the connecting role cannot see is still a project worth reporting on.

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

## What is not built yet

The copy. Schema and data, ownership fixups, auth users verified after the move, storage objects copied in parallel with resume, and `zou export` for going the other way, are all on [issue #5](https://github.com/tamnd/zou/issues/5). Until they land the command says so in the error rather than doing part of the job.

## Related

- [docs/compatibility.md](compatibility.md) for what a project will notice once it has moved, which is the prose the extension lists here are drawn from
- [docs/quickstart.md](quickstart.md) for pointing an existing client at zou
- [docs/operations.md](operations.md) for `zou doctor` and the rest of running it
