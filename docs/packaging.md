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

## Checking what you downloaded

A sha256 published on the same page as the file it describes answers one question, which is whether the download finished.
Anybody who could replace the tarball could replace the number next to it, so it is not an answer to where the file came from.
Two other things on the release are, and they answer different questions.

Where it came from:

```bash
gh attestation verify zou-linux-x64.tar.gz --repo tamnd/zou
```

That checks a statement signed through sigstore at the moment the release was built, saying which workflow in this repository, at which commit, produced exactly those bytes.
There is no zou signing key: the workflow trades its GitHub identity for a short lived certificate, so there is no private key anywhere to keep safe or to lose, and nothing to rotate after a laptop is stolen.
Every asset a tag produces is attested, the bundles and the wheels and the windows zips alike.
`npm i -g zou-cli` carries the same provenance on the registry side, which npm shows on the package page and checks with `npm audit signatures`.

What is inside it:

```bash
jq -r '.components[] | "\(.name) \(.version)"' zou-linux-x64.cdx.json
```

`zou-<platform>.cdx.json` next to each bundle is a CycloneDX 1.5 document listing everything that went into it: the rust crates cargo resolved for that target with the features a release is built with, marked as normal or build time, and the two vendored C projects, the patched Postgres and pgvector, each with the version it calls itself and the commit this repository pins.
That last part is why the document is not only a `cargo` dependency list.
A bundle is mostly Postgres by weight, and an SBOM that names four hundred rust crates and no postmaster would read as complete while being silent about the largest thing in the artifact.

`scripts/zou-sbom.sh` writes it and is the same script the release runs, so it can be read and run against a checkout rather than being something that only happens inside a workflow.
It refuses to write a document that is missing either vendored tree, that resolved too few crates to be a real graph, or that carries a path from the machine it was built on.
The serial number is a hash of the contents rather than a fresh uuid each time, so the same tree produces the same document twice and a diff between two releases is only what actually changed.

Advisories are the other half of this and they run on every push, not at a tag.
`cargo-deny` checks `advisories licenses sources` against the RustSec database, which is the same database and the same `rustsec` crate `cargo audit` reads, and additionally refuses yanked crates, licences outside the allow list, and unknown registries and git sources.
There is no separate `cargo audit` step because it would be a second copy of one check with a second ignore list to keep in step.
The four ignores in `deny.toml` each name the crate, the feature that reaches it, and why it cannot be upgraded.

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

## Why it runs somewhere else

A postgres that has just been built is linked against the tree it was built in, by absolute path: `/src/build/pg/lib/x86_64-linux-gnu` is written into `initdb` and stays there when the file is copied.
Copy that into an image and the postmaster comes up as `error while loading shared libraries: libpq.so.5`, on a machine where libpq is sitting right next to it.
So the last thing the script does is rewrite those paths into ones relative to the file holding them, `$ORIGIN/../lib/x86_64-linux-gnu` on linux with `patchelf` and `@loader_path/../lib` on mac with `install_name_tool`, and a mach-o that has been edited is re-signed because an arm64 mac will not run a file whose signature no longer matches it.

A mac build also links homebrew, openssl and lz4 and zstd, and homebrew is not in the bundle and is not even at the same path on an intel mac as on an arm one, so those libraries are copied in and rewritten with everything else.
Anything under `/usr/lib` or `/System` is the operating system and is left where it is.
Linux is the other way round: openssl and icu and zlib and readline and the compression libraries come from the distribution, so the bundle expects them.
Which ones it expects is not a thing to be remembered, so the script reads it off the binaries and prints it, and fails on anything outside the set the platform is expected to provide:

```
  from the machine, and nothing else:
    libc.so.6
    libicui18n.so.72
    libreadline.so.8
    libssl.so.3
    ...
```

That check exists because a dependency nobody chose is still a dependency.
Most of Postgres' optional libraries default to `auto` in meson, so a builder with `libkrb5-dev` on it produces a postmaster that needs `libgssapi_krb5` and a builder without it produces one that does not, from the same commit, and the first person to hear about the difference is whoever ran the container.
That is exactly how it went: the image failed with `initdb: error while loading shared libraries: libgssapi_krb5.so.2` and the tarball would have failed the same way on a machine without kerberos.
So `make pg-build` names the ones zou does not offer, gssapi and ldap and pam and bonjour and systemd and the rest, and turns them off, and the docker image installs the ones that are left and nothing else.

CI proves this rather than asserting it: before starting a database out of a fresh bundle it moves `build/pg` out of the way, so what runs is the bundle standing on its own, the way it will arrive on a machine that never built anything.
Assembling a linux bundle needs `patchelf` on the machine doing the assembling, and the script says so and stops rather than writing a tarball that only works where it was made.

## What it weighs

The budget is 150 MB for the pair, and the script exits non zero over it, so CI notices the day something doubles.

| | darwin arm64, laptop | linux x64, vps | linux x64, github runner |
| zou | 17.3 MB | 18.1 MB | 20.0 MB |
| postgres | 16.8 MB | 19.1 MB | 19.2 MB |
| the other pg programs | 1.6 MB | 1.8 MB | 1.8 MB |
| modules and the libs they use | 15.5 MB | 6.3 MB | 6.3 MB |
| share, bki and timezones | 5.3 MB | 5.3 MB | 5.3 MB |
| the bundle | 56.5 MB | 50.6 MB | 52.6 MB |
| the tarball | 21.0 MB | 19.4 MB | 20.2 MB |

Measured on an apple silicon laptop, on a 6 vCPU vps, and on the runner that builds every postgres build, all three against the vendored Postgres 18.4 with the patch series applied.
The two linux columns are the same commit and the same rustc, and the rust binary is two megabytes apart between them anyway, which is what a bundle size is: a number about a machine rather than about a program.
The mac column carries seven megabytes the linux ones do not, which is openssl and lz4 and zstd: a mac gets those from homebrew and a linux gets them from the distribution, and only one of those two is a thing a download can rely on.
The rust binary is stripped by the release profile; the postgres binaries are stripped by the script, which is a third of the postmaster.
A third of the whole bundle is timezones, catalog templates, and extension sql, none of which compresses badly, which is why the tarball is roughly a third of the tree.

CI builds a bundle on every postgres build, starts the postmaster out of it, and creates the vector extension, because a bundle that is small and does not run is not a smaller bundle.

## What a tag builds

The release workflow builds a bundle per unix platform, linux and darwin on both architectures, and each leg builds the patched Postgres from the vendored tree, builds `zou`, assembles, starts a database out of the result, and uploads the tarball with its sha256 and its SBOM.
The SBOM is written per platform rather than once at the end, because the crate graph is resolved for that target with that build's features and the Postgres in it is the one that leg just compiled.
Everything then lands in one job that attests the lot before uploading, so the provenance covers the windows zips too even though they were uploaded by the job that built them.
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

## From PyPI

```bash
pip install zou zou-postgres
```

Two wheels, because they are two different things that happen to be needed together.

`zou` is the [embedded binding](embedded.md), a PyO3 extension module and the python next to it, built by maturin.
It is abi3, so one wheel per platform covers 3.9 and everything after it, and it contains no postgres at all, which is why it builds in a manylinux container and installs anywhere.

`zou-postgres` is the patched Postgres, the same tree the tarball ships, as a wheel per platform.
That is what `zou-cli` is for node, and the binding asks for `pg_bin`, then `ZOU_PG_BIN`, then this package, so a project that installed both has a database per test with nothing to configure and nothing to point anywhere.

```python
import zou_postgres

zou_postgres.pg_bin()   # .../site-packages/zou_postgres/pg/bin
```

It is a wheel of binaries rather than of python, so the tag has to be honest about what they need.
On linux that is the glibc of the machine that built them, since the bundle takes openssl and icu from the distribution: saying `manylinux_2_28` on something built against 2.39 is a claim pip has no way to check and a user finds out about at import time.
On mac it is the macos of the machine that built them, because the bundle carries homebrew's openssl and lz4 and zstd and a bottle is built for the release it was poured on.
Older machines than the ones the release builds on are not served by a wheel, and have the tarball or a checkout.

`packaging/pypi/zou-postgres/build.sh` turns a bundle into that wheel, and a tag builds both wheels for all four unix platforms, uploads them to the release, and pushes them to PyPI when there is a `PYPI_TOKEN`.
One account token rather than a project one, since it publishes two projects.

CI installs both wheels into a venv with no `ZOU_PG_BIN` and no checkout on the path, and runs the python suite against them, because a wheel that nobody has installed is a wheel that does not work.

## From homebrew

```bash
brew install tamnd/zou/zou
```

The formula installs the bundle whole into `libexec` and links `bin/zou` at the one inside it, rather than putting the binary in `bin` and the postgres somewhere else.
That is not tidiness, it is how the binary finds its postmaster: it looks beside itself, follows the link first, and a `zou` in `bin` with a `pg` two directories away is a `zou` that cannot start a database.

Mac only, and on purpose.
The mac bundle carries the libraries it needs that are not the operating system, so it stands on its own wherever it is unpacked, while the linux one takes icu and readline and openssl from the distribution, which is a thing apt and dnf know about and homebrew on linux does not.
A formula that installs and then cannot run is worse than no formula, so linux has `install.sh` or the tarball and the formula says `depends_on :macos`.

The formula is generated rather than kept in the tree, by `packaging/brew/formula.sh`, because half of it is a pair of sha256 sums that do not exist until the bundles do, and a checked in formula carrying the last release's sums is a formula that installs the wrong thing.
A tag writes it out of the checksums the release job just uploaded and pushes it to the tap, `tamnd/homebrew-zou`, when there is a `HOMEBREW_TAP_TOKEN`, which is a second repository and so out of reach of the token a workflow gets for free.
Without the token it prints the formula into the job log and the release is otherwise unaffected.
CI runs the generator on every change and hands the result to `ruby -c`, since a formula is only found to be broken at a tag, and a tag does not come round again.

## In a container

```bash
docker build -t zou .
docker run --rm -v zou-data:/data zou tenant /data create demo
docker run --rm -p 54321:54321 -p 5432:5432 -v zou-data:/data zou
```

Two stages: one with meson and bison and a rust toolchain that builds the patched Postgres and `zou`, and one with the shared libraries the postmaster links against and nothing else.
What crosses between them is the bundle, the same one `scripts/zou-bundle.sh` writes for a release, so the image and the tarball are the same tree rather than two definitions of what ships.

The command is `zou serve` rather than `zou dev`, because `serve` binds `0.0.0.0` and `dev` deliberately does not, and a dev loop that could only be reached from inside the container would be a strange thing to ship.
A registry with nothing in it serves nothing, which is why the tenant is made first; the first request after that attaches it, which runs `initdb` and takes a moment, and every start after that is a restore.
Postgres refuses to run as root and is right to, so the image has a user of its own and `/data` belongs to them.

CI builds the image on every change to the Dockerfile, makes a tenant, starts it, and asks the front door a question it should refuse, because a 401 from the api is the api being up with a database behind it.
A tag pushes it to `ghcr.io/tamnd/zou`.
