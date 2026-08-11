# Branching

A branch is a second database that starts out as the first one and then goes its own way, and making one moves no data.

A tenant in a store is a prefix holding a manifest and the objects that manifest names.
A branch is a new prefix holding a manifest that names the parent's objects, so creating one is a read of the parent's manifest and a write of the child's, and it finishes in the time of two round trips whatever the database weighs.
On a laptop that is 18 ms against a 73 MB database, and it would be 18 ms against a 73 GB one.

Writes on a branch land in the branch's own prefix and the parent never sees them.
The parent's objects are read through, never written to, so a branch cannot damage what it came from.

## One, from the command line

```bash
zou branch ./store create local pr-142
# branched local into pr-142 at 0x1C06898, 1 checkpoints inherited

zou dev ./store --ref pr-142 --port 5482 --http 5483
```

`--ref` is how a command names a tenant other than the default, and a branch is an ordinary tenant from that point on, so `zou dev --ref` and `zou serve --ref` serve one the same way they serve anything else.
`zou info <target> <ref>` takes the ref as its second word and prints that branch's manifest, checkpoints and WAL tail.

What that gets you, with the parent still running on its own ports:

```
branch: select count(*) from notes   ->  1000
branch: insert one row               ->  1001
parent: select count(*) from notes   ->  1000
```

And what it cost on the store, after that write:

| prefix | objects | bytes |
| --- | --- | --- |
| the parent | 3879 | 73 MB |
| the branch, written to | 7 | 28 KB |
| a branch nothing has written to | 1 | 4 KB |

## Listing and deleting

```bash
zou branch ./store list
# pr-142	from local at 0x1C06898, 1 checkpoints
# 1 branches

zou branch ./store delete pr-142
# deleted branch pr-142, 21 objects
```

`list` reads every manifest on the store and prints the ones that name a parent, so it is the truth rather than a registry that could disagree with it.
`list <parent>` narrows it to one ref's children.

Delete refuses three things, and each refusal is about somebody's data:

```
zou branch ./store delete local
# local is not a branch, its objects are its own, remove tenants/local by hand to delete it

zou branch ./store delete pr-142
# pr-142 still has branches of its own: pr-142-child
```

The third is a live lease: something is attached to that database right now, so it is not deleted out from under a running postmaster.
Stop the server, or wait out the lease, and try again.
`zou tenant delete` is the gentler command that only forgets the ref in the registry and leaves the objects alone.

Deleting a branch removes its prefix, which is what it wrote and the manifest that named it.
The objects it inherited belong to the parent and stay.

## A point in time

Every manifest change lands a history snapshot under the tenant's `manifests/`, and a branch can be cut from one of those instead of from the head.

```bash
zou branch ./store create local before-the-migration --at '0/1C06898'   # a checkpoint lsn
zou branch ./store create local last-tuesday --from-time 1786000000     # a unix second
zou branch ./store create local last-tuesday --at 1786000000            # the same thing
```

An `--at` value shaped like an lsn, `X/Y` or `0x` hex, has to name a checkpoint lsn exactly, because branch points are checkpoint lsns in this release.
Plain digits are a unix second, and `--from-time` is that spelled out for people who would rather say what they mean than rely on a shape.

Two things to know before relying on it.

A snapshot is written at most once a second, so a second in which the database changed twice keeps the last state of that second and a point in time inside a busy second lands on the state at its edge.
Asking for "now" is therefore not the same as asking for the head, and the head is what you get by passing no flag at all.

And how far back it reaches is the GC retention window, a week by default:

```
zou branch ./store create local last-year --from-time 1700000000
# no history snapshot at or before unix 1700000000, the retention window has passed it
```

That window is `zou gc --retention`, see [operations.md](operations.md), and it is the same promise from both ends: a snapshot younger than it keeps everything it references alive, which is what makes a branch from it possible.

## The one thing that can refuse a branch

A child reads the pages it inherited out of the parent's captures, and a capture that has not been folded into page runs cannot serve them.

```
zou branch ./store create local pr-142
# local cannot be branched yet, there is no run bearing full capture to serve inherited
# pages from, fold one in the source first. Nothing of pr-142 was left on the store
```

A fold packs a full capture down after a few checkpoints of writes, so a project that has been running for a while has one and a store somebody made this morning may not.
The check runs before the call returns and takes the half made child back off the store, which is the difference between an error now and a database that fails on its first query an hour from now.
`ZOU_FOLD_DOWN_FACTOR=0` in the server's environment brings the fold forward, the second checkpoint packs a full instead of the fifth, which is how the tests get a branchable database out of one that has only just started.

The same rule applies to a point in time: the state you land on has to have a full capture under it, so the first minutes of a database's life are not a place a branch can be taken from.

## A database per pull request

The composite action in `actions/branch` is the two calls above driven off the pull request event.

```yaml
- uses: tamnd/zou/actions/branch@main
  with:
    target: s3://mybucket/tenants
```

It names the branch `pr-<number>` unless `ref:` says otherwise, `source:` is what it branches from and defaults to `local`, and `zou:` is the binary if it is not on `PATH`.
Every push runs it again and a branch that is already there is left alone, because the database from the first run is the one with the test data in it.
Give the workflow the `closed` type as well and the same step deletes the branch, or set `delete: create` and `delete: delete` to say which half runs where.

## From the library

The embedded API has the same two operations, and hands back an open database rather than a name.

```rust
let zou = Zou::open(Options::dir("./data"))?;
let pr = zou.branch("pr-142")?;   // checkpoints the parent first, then cuts and opens
assert!(zou.branchable()?);       // whether a branch would serve, without making one
```

```js
const zou = await createZou({ dir: './data' })
const pr = await zou.branch('pr-142')
```

`branch` checkpoints the parent before it cuts, so what was committed a moment ago is in the child, and it refuses for exactly the same reason the command line does.
`branchable()` asks the question without making anything, which is what a test that wants to know whether a fixture is ready should call.

Fixtures are this mechanism used once more: every fixture database is a branch of the machine's template, see [embedded.md](embedded.md).

## Moving a store, and what a branch costs over time

`zou push ./store s3://bucket/prefix` and `zou pull s3://bucket/prefix ./store` copy a whole store or one ref of it between a directory and an object store, so a branch made on a laptop can be pushed for somebody else to serve.

A branch pins the parent objects its manifest names, which is the point of it and also the cost of it: a store with a year of pull request branches on it keeps a year of the parent's history alive.
`zou gc` collects what no live manifest and no retained snapshot references, so deleting the branches is what lets a sweep free anything.
The retention window and the sweep are both in [operations.md](operations.md).

## What is not here

Branch points are checkpoint lsns, so an arbitrary lsn in the middle of a segment is not one and is refused rather than rounded.

There is no merge.
A branch is a database, and getting changes back into the parent is a migration or a dump, the same as it would be between any two databases.

A branch of a branch works and is ordinary, but `delete` will not take a parent out from under its children, so a chain is unwound from the end.
