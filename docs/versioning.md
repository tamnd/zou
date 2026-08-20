# Versioning

A backend that keeps its state in an object store does not have one version. It has a release, a set of durable formats that outlive every process that wrote them, a wire that two different releases speak to each other for as long as a rollout takes, and a compatibility claim that is measured rather than declared. Each of those breaks differently, so each has its own rule, and this page is the four of them in one place.

## The release

The tag is the version. `git tag v1.2.3` is the only place a version number is authored, and every artifact built from that tag carries it: the platform bundles on the release page, the windows zips, `zou --version` and the version the ops endpoint and `/auth/v1/health` report, `zou-cli` on npm, the `zou` and `zou-postgres` wheels on PyPI, and the homebrew formula.

Nothing in the tree is authored. The number committed in the manifests is a placeholder, the same one in every manifest, and `scripts/zou-version.sh` stamps the tag over it in each release job that builds or publishes something. The manifests it stamps are listed in `packaging/versioned-manifests.txt` rather than in the workflow, because the workflow used to carry a rewrite of its own in three places and miss two, which would have published `zou-cli` at 1.0.0 around a binary that answered `zou --version` with `zou 0.0.1`.

That is held rather than remembered. `conformance/tests/versions.rs` refuses a manifest in the tree that carries a version nothing stamps, refuses a listed manifest the script would silently match nothing in, refuses two different placeholder numbers, and refuses a release job that writes a version itself instead of calling the script.

A run of the release workflow that was dispatched rather than tagged keeps the placeholder and publishes nothing. It exists to prove the build, and a build that says 0.0.1 is a build nobody can mistake for a release.

## What semver covers

The number is semver, and what it is a promise about is the surface somebody else's code touches.

- The HTTP surfaces. `/rest/v1`, `/auth/v1`, `/storage/v1`, `/realtime/v1` and `/functions/v1`, which are not this project's designs and are covered by the section below on compatibility rather than by anything decided here.
- The command line. Flag names, what a command reads out of a project directory, what it prints on success, and the exit codes. What a command prints on failure is a message, and a message is not an interface.
- The embedding surfaces. The C ABI in `libzou` and the node, python and Go packages over it, which are how zou runs inside somebody else's process rather than on a port.
- The configuration a project writes. `config.toml`, the environment variables, and the shape of a deployed function's manifest.

What it does not cover:

- The rust crates. They are not published to crates.io, they carry a version because cargo requires one, and `zou-store`, `zou-log`, `zou-pg`, `zou-rest`, `zou-realtime`, `zou-server` and the rest are the inside of one program that happens to be built out of several. Depending on them from a git revision is fine and pinning that revision is the whole of the contract.
- The conformance harness and the suites, which are a measuring instrument. They change when what they measure changes.
- Metrics names, log lines and anything under a debug or development flag.
- The layout of bytes in the object store, which has its own rule below and is not the release number's business.

## The durable formats

This is the part a release number cannot express, because a store outlives every binary that ever wrote to it and a fleet halfway through a restart is running two of them at once.

Every durable and spoken format in the tree is in the census at the top of `crates/zou-log/tests/upgrade.rs`, with four things recorded per format: where the constant lives, how far the format reaches, the highest version this binary reads, and the lowest version a plain object of it is written at. The census is checked against the source on every test run, so a constant that moves fails there until somebody says what the new floor is. That is deliberate: bumping a format is a decision about every binary already running, and it is made in that file rather than in a diff nobody connected to a rollout.

Three rules hold across all of them.

**A writer emits the lowest format that carries what it wrote.** A tenant that never split keeps writing the manifest format that predates sharding, so a node from before sharding reads it, and the newer format is only ever written by a tenant that used the feature it was added for. Nothing about this is free: the moment that tenant splits is the moment the old binary stops being able to read it, and that is the price of the feature rather than the price of the release.

**A reader refuses a version it does not know.** It does not guess, skip the field it does not recognise, or read the parts it understands. A store misread once is a store that stays wrong, and a refusal is loud on the machine that is behind rather than silent on the data.

**How far a format reaches is what decides how expensive a bump is.** An object in the shared store is read by every node in the fleet including the ones still on the previous release, and it outlives all of them. A wire format is spoken between two live processes that are on different releases for exactly as long as the rollout takes. A file one machine writes and another machine's zou may be handed is somebody's problem but not the fleet's. A file one process rewrites when it does not fit is nobody's. Those four are named in the census and a bump is argued in those terms.

What a plain object of each format serializes to today is checked in under `crates/zou-log/testdata/upgrade/`, so the first change that would break a reader older than it shows up as a diff somebody has to look at rather than as a fleet that will not route.

## Compatibility is not the release number

zou's compatibility with Supabase is a measurement, not a version. [docs/scoreboard.md](scoreboard.md) is regenerated on every merge to main out of the run that merge passed or failed on, and it says per suite, per endpoint and per feature how much of each surface answers what upstream answers.

A change that lowers a number on that page is breaking whatever the release number does, and it does not get to be a patch release because the code change was small. A deliberate difference is written into that suite's `known.json` with its reason and is still counted as a failure on the scoreboard, so the number cannot be improved by writing something down. The differences that exist today, and why each one is deliberate, are in [docs/compatibility.md](compatibility.md).

The other direction has a version of its own. What zou is compatible with is pinned in `versions.json` in the conformance repository, at the versions the Supabase CLI's own local stack runs, and bumping one of those means re-recording the suites it covers and reading the diff, because a diff in a recording is upstream changing its mind.

## Before 1.0

Everything above is the shape the project releases in. It has not released yet.

While the version is 0.x, the durable format rules are already in force, because a store written today is a store that has to be readable tomorrow and the census has governed that since before there was anything to release. The semver promise on the surfaces is not: a 0.x is allowed to change a flag or a binding signature, and the changelog is where that is said.

Tagging 1.0.0 is what turns the second half on, and the conditions for it are the exit criteria on the M5 milestone rather than a date.

## Deprecation

A format's reader stays for at least one minor release after the last writer of it is gone, and longer for anything with store reach, since the objects are still there whether or not anything writes new ones. Removing a reader is the same kind of decision as adding a format and is argued in the same file.

A command line flag or a binding entry point that is going away keeps working for one minor release with a warning, and is removed in the next. A surface that upstream removed is not this project's to keep: it goes when the pinned version it belonged to goes, and the suite that asked about it goes with it.
