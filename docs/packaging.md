# Packaging

## Installing it

```bash
curl -fsSL https://raw.githubusercontent.com/tamnd/zou/main/install.sh | sh
```

That works out the platform, downloads the bundle for it, checks it against the sha256 published next to it, and unpacks it into `~/.zou/versions/<tag>` with `~/.zou/bin/zou` linked at the current one.
Versions sit side by side, so a second install is a link change rather than a directory that is half one version and half another, and uninstalling is `rm -rf ~/.zou`.
`ZOU_VERSION` picks a tag other than the latest and `ZOU_HOME` picks somewhere other than `~/.zou`.
Nothing runs as root and nothing is written outside that directory.

There is no `--pg-bin` to pass afterwards.
A zou that was installed this way finds the postmaster next to itself, because the install layout puts one there, and the order it asks in is the flag, then `ZOU_PG_BIN`, then the bundle it shipped in, then `build/pg/bin` for somebody working in a checkout.

## What a release is

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

| | darwin arm64, laptop | linux x64, vps | linux x64, github runner |
| zou | 17.3 MB | 18.1 MB | 20.0 MB |
| postgres | 16.7 MB | 19.1 MB | 19.2 MB |
| the other pg programs | 1.5 MB | 1.8 MB | 1.8 MB |
| loadable modules | 7.8 MB | 6.3 MB | 6.3 MB |
| share, bki and timezones | 5.3 MB | 5.3 MB | 5.3 MB |
| the bundle | 48.6 MB | 50.6 MB | 52.6 MB |
| the tarball | 18.0 MB | 19.4 MB | 20.2 MB |

Measured on an apple silicon laptop, on a 6 vCPU vps, and on the runner that builds every postgres build, all three against the vendored Postgres 18.4 with the patch series applied.
The two linux columns are the same commit and the same rustc, and the rust binary is two megabytes apart between them anyway, which is what a bundle size is: a number about a machine rather than about a program.
The rust binary is stripped by the release profile; the postgres binaries are stripped by the script, which is a third of the postmaster.
A third of the whole bundle is timezones, catalog templates, and extension sql, none of which compresses badly, which is why the tarball is roughly a third of the tree.

CI builds a bundle on every postgres build, starts the postmaster out of it, and creates the vector extension, because a bundle that is small and does not run is not a smaller bundle.

## What a tag builds

The release workflow builds a bundle per unix platform, linux and darwin on both architectures, and each leg builds the patched Postgres from the vendored tree, builds `zou`, assembles, starts a database out of the result, and uploads the tarball with its sha256.
That is roughly fifteen minutes a platform, which is why it runs on tags and not on pushes.
Windows gets the binary on its own, the way every target did before, because there is no patched Postgres and no embedded story there yet.

## From npm

```bash
npm install -g zou-cli
zou dev ./data
```

`zou` on npm is the embedded binding, so the command line is `zou-cli`, and the two are meant to be installed together as often as separately.

npm cannot carry the bundle as package content: four platforms of a fifty megabyte tree in one tarball is not a package anybody should download to get one of them, and npm has no story for shipping one of four.
So the package is a `postinstall` that downloads the same tarball `install.sh` does, checks the same sha256, and unpacks it into the package under `vendor/`.
`bin/zou.js` is a shim onto the binary in there, and it is a shim rather than a wrapper: signals, exit codes, and the terminal belong to the child, because `zou dev` is a process a person leaves running and interrupts with ctrl-c.

The version follows the package rather than the release feed: `zou-cli@0.2.0` downloads from the `v0.2.0` release, because a version number that means whatever was newest at install time is not a version number.
The tag job publishes the package after it has uploaded the bundles, never before, and it publishes nothing at all when there is no `NPM_TOKEN`, which leaves the tarballs on the release either way.
`ZOU_VERSION` takes a different tag, `ZOU_SKIP_DOWNLOAD=1` installs the package without the bundle for an image that will mount one in later, and `npm rebuild zou-cli` is the fix when the bundle is missing.

The other half of this is the binding: a node project that installs `zou` and `zou-cli` together gets a patched Postgres without a `ZOU_PG_BIN` anywhere, because the binding looks for `pgBin`, then the environment, then `zou-cli` next door.
That is the answer to the question the embedded packages have had open since they landed, which is where a project that never built anything is supposed to find a postmaster.

CI proves the postinstall against a bundle it just built, served over localhost with a checksum beside it, since there is no published release to test against yet and a postinstall nobody has run is a postinstall that does not work.

Still to come in this section: PyPI, brew, and a docker image, all of which want this same tree.
