# Trademarks

Zou is not a Supabase product. This project is not affiliated with, sponsored by or endorsed by Supabase Inc, and nothing here should be read as saying that it is.

Supabase is a trademark of Supabase Inc. It appears in this repository for one reason, to say what zou is compatible with, and a compatibility claim cannot be made without naming the thing it is a claim about. Calling zou a Supabase compatible backend is the same kind of sentence as calling a part a replacement for a particular model of car: it says what the thing works with, and it does not say who made it.

The claim is measured rather than asserted. [docs/scoreboard.md](docs/scoreboard.md) is regenerated on every merge to main out of the run that merge passed or failed on, and it says exactly how much of each surface answers what upstream answers. Where zou does something different on purpose, [docs/compatibility.md](docs/compatibility.md) says which difference and why.

## Where the line is

The mark is used as an adjective and never as a name for this project or anything published from it. Zou is a Supabase compatible backend. Zou is not "a Supabase", not "an open source Supabase", not "a self hosted Supabase", and not an official or unofficial anything of theirs.

Nothing published from here is named after somebody else's mark. The crates are `zou` and `zou-*`, the command line is published to npm as `zou-cli`, and the Postgres wheel is `zou-postgres`.

No logo, wordmark or other design belonging to anybody else is committed here. This repository tracks no image files at all.

The word does appear in paths and package names that belong to other people, because those are their names: a project keeps its functions in `supabase/functions`, the clients tested against are `@supabase/supabase-js` and `@supabase/storage-js`, and the local stack this project is compared with is brought up with `supabase start`. Referring to a thing by its name is the use being made everywhere in this repository.

## The other names

PostgreSQL, Postgres, PostgREST, GoTrue, Deno, Amazon S3, AWS, MinIO, Docker, Homebrew and every other product named in this repository are the property of their respective owners, and they are named here on the same footing: to say what zou runs, what it is compared with, what it speaks to, or where it can be installed from.

## What holds this

`conformance/tests/trademarks.rs` reads the tracked files on every run of the test suite and fails on a sentence claiming affiliation or endorsement, and on a package published from here whose name carries somebody else's mark. Everything above was true when it was written, and the test is there because a page saying so is not the same as a repository that stays that way.

## If you own one of these

If you own a mark named in this repository and something here reads wrong to you, open an issue at [tamnd/zou](https://github.com/tamnd/zou/issues) and say what you would like changed. It will be changed.
