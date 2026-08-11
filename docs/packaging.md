# Packaging

A zou release is not one binary.
Postgres is a child process, and it has to be the patched build, so a download that is only `zou` is a download that cannot start a database.
`scripts/zou-bundle.sh` puts the two together the way an installer would and prints what the result weighs.

```bash
make pg-build
scripts/zou-bundle.sh --tar
```

## What is in it

```
zou-darwin-arm64/
  bin/zou
  pg/bin/{postgres,initdb,psql,pg_dump,pg_restore}
  pg/lib/postgresql/*.dylib
  pg/share/postgresql/{postgres.bki,timezone,extension,...}
```

Zou starts two of the thirty odd programs a postgres install ships: `initdb` once, and the postmaster after that.
`psql` comes along because a person with a database wants a prompt for it, and `pg_dump` and `pg_restore` because getting data out is not optional.
The rest is tooling for a postgres that is administered by hand, which this is not.

The loadable modules come whole, `vector` among them, minus the three language handlers whose interpreters are not in the bundle: `plpython3` without the python it was linked against is a file that can only fail to load.
`share` comes whole as well, because `initdb` reads `postgres.bki` and the timezone database out of it and `create extension` reads the sql, and the only thing dropped there is the documentation.

Postgres finds its share directory and its modules relative to the postmaster, so the bundle keeps the layout the install had rather than one of its own.
That matters more than it sounds: a debian meson build puts the modules under `lib/x86_64-linux-gnu/postgresql` and a mac build under `lib/postgresql`, and the script reads `pg_config` rather than assuming either.
A bundle moved anywhere still runs, which is the whole point of shipping one.

## What it weighs

The budget is 150 MB for the pair, and the script exits non zero over it, so CI notices the day something doubles.

| | darwin arm64 | linux x64 |
| zou | 17.3 MB | 18.1 MB |
| postgres | 16.7 MB | 19.1 MB |
| the other pg programs | 1.5 MB | 1.8 MB |
| loadable modules | 7.8 MB | 6.3 MB |
| share, bki and timezones | 5.3 MB | 5.3 MB |
| the bundle | 48.6 MB | 50.6 MB |
| the tarball | 18.0 MB | 19.4 MB |

Measured on an apple silicon laptop and on a 6 vCPU vps, both against the vendored Postgres 18.4 with the patch series applied.
The rust binary is stripped by the release profile; the postgres binaries are stripped by the script, which is a third of the postmaster.
A third of the whole bundle is timezones, catalog templates, and extension sql, none of which compresses badly, which is why the tarball is roughly a third of the tree.

CI builds a bundle on every postgres build, starts the postmaster out of it, and creates the vector extension, because a bundle that is small and does not run is not a smaller bundle.
