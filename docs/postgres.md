# Vendored Postgres

zou ships its own Postgres build.
The source lives in `vendor/postgres` as a shallow git submodule pinned to the `REL_18_4` release commit, and zou's modifications are a patch series in `patches/`, applied on top by `scripts/pg-apply-patches.sh`.

## Why a submodule plus patches, not a fork branch

The patch series keeps the delta against upstream visible and reviewable as a handful of files in this repo.
Moving to a new Postgres minor is a one line submodule bump plus a series re-apply, not a rebase of a private branch that only exists on some contributor's remote.
And a fresh clone of this repo contains everything needed to reproduce the exact server binary, which is the property the fault injection and soak results depend on.

## Building

Prerequisites: meson, ninja, a C compiler, flex, bison, and the usual Postgres libraries (readline, zlib, icu).

- Debian and Ubuntu: `apt-get install meson ninja-build flex bison libreadline-dev zlib1g-dev libicu-dev pkg-config`
- macOS: `brew install meson ninja icu4c pkg-config`

Then:

```sh
make pg-build
```

That runs three steps, each also available on its own:

- `make pg-init` fetches the pinned source, shallow.
- `make pg-patch` resets the tree to the pinned commit and applies the series in filename order. It refuses to run if the submodule has uncommitted edits, so patch authoring work cannot be silently destroyed.
- `make pg-build` does an out of tree meson build into `build/pg-build` and installs into `build/pg`, so the submodule working tree stays clean.

Smoke test the result:

```sh
build/pg/bin/initdb -D /tmp/zou-pgdata
build/pg/bin/pg_ctl -D /tmp/zou-pgdata -l /tmp/zou-pg.log start
build/pg/bin/psql -h /tmp -d postgres -c 'select version()'
build/pg/bin/pg_ctl -D /tmp/zou-pgdata stop
```

## CI

The `postgres-build` workflow builds the vendored source with the full series applied and runs the initdb plus psql smoke test.
It triggers on any PR touching `vendor/`, `patches/`, the build scripts, or the Makefile, and on manual dispatch.
While the series is empty this is the reproducible stock baseline the milestone asks for, so a patch that breaks the build or changes `select version()` output gets caught in the same PR that introduces it.

## Authoring a patch

See `patches/README.md` for the mechanics.
The short version: get the tree to series state with `make pg-patch`, edit, export with `git -C vendor/postgres diff`, then prove the series applies from scratch.

## Moving the pin

Bumping to a new upstream release is its own PR: update the submodule commit, run `make pg-patch` to prove every patch still applies, run `make pg-build` and the smoke test, and let the CI job confirm it.
Nothing else in the repo may change in that PR, so a pin move can always be bisected cleanly.

## Licensing

Postgres is distributed under the PostgreSQL License, which is BSD style and compatible with this repo's Apache 2.0.
The vendored source keeps its own COPYRIGHT file, and zou's patches to it are contributed under the same PostgreSQL License terms.
