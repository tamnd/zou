# Storage engine

zou-store makes object storage the only durable medium while keeping commit and page read latency acceptable. The design borrows from Neon (page server model), SlateDB (LSM on S3), and Litestream (WAL shipping).

## Object layout

One prefix per logical database:

```
s3://<bucket>/tenants/<ref>/
  MANIFEST                      current manifest, swapped with CAS
  manifests/<epoch>.json        manifest history, enables PITR and branching
  wal/<epoch>/<start-lsn>.wal   sealed WAL segments, immutable
  chk/<chk-id>/index            page location index for one checkpoint
  chk/<chk-id>/<n>.pages        sorted page images, 64 to 256 MB objects
  files/<bucket>/<key>          Storage API user files
```

Everything under wal/ and chk/ is immutable once written. Only MANIFEST is mutated, and only through conditional PUT (If-Match on S3 and R2, preconditions on GCS, atomic rename plus lock on the local backend). That gives atomicity without any external coordination service.

## Manifest

A small JSON document, the root of truth for one database. It names the Postgres version (18), the current checkpoint set, the WAL tail, the lease, and the branch provenance:

```json
{
  "format": 1,
  "ref": "acme-prod",
  "epoch": 42,
  "lease": { "holder": "node-7f3a", "expires_unix": 1767100000, "fence": 1042 },
  "pg": { "version": 18, "timeline": 3 },
  "checkpoints": [
    { "id": "chk-000121", "lsn": "0/8A211000", "kind": "full" },
    { "id": "chk-000122", "lsn": "0/8B000000", "kind": "delta" }
  ],
  "wal_tail": { "epoch_dir": 42, "from_lsn": "0/8B000000" },
  "branch_of": { "ref": "acme-prod", "at_lsn": "0/7000000" }
}
```

## Writer lease

A node becomes the writer by reading MANIFEST, then writing itself as the lease holder with the epoch incremented, conditional on the ETag it read. CAS success means it holds the lease. It renews with a heartbeat at a third of the TTL and releases on graceful detach.

The epoch is the fencing mechanism. Every WAL segment is written under its epoch directory and every frame carries the epoch and fence token, so a zombie writer that lost its lease can only write into a dead epoch that the new manifest never references. Acknowledged commits are never lost because acks wait for durability, and split brain is impossible because two holders would need two successful CAS swaps of the same ETag.

## Write path

WAL records flow into a ring buffer and are flushed as a group commit batch at 2 ms or 512 KB, whichever comes first. A batch is framed with a checksum, compressed with lz4, and PUT to the object store. COMMIT returns only after the PUT succeeds. If the object store stalls, commits stall. We never fake an ack.

There are three latency tiers. Pure S3 gives roughly 30 to 100 ms commits with zero extra infrastructure. S3 Express One Zone brings that to single digits. An optional write buffer pair of tiny NVMe nodes acks in 1 to 3 ms and uploads within seconds, an explicit opt-in trade.

## Read path

```
smgr_read(rel, block)
  RAM cache, then NVMe cache
  else: checkpoint index lookup, S3 range read of the surrounding page run
  apply newer WAL tail redo for that page if any
```

The cache is content addressed by checkpoint object and offset, so branches and replicas share entries. Compaction keeps read amplification bounded: a page read touches at most one full checkpoint, four deltas, and the WAL tail.

## Checkpoints and compaction

A background checkpointer replays sealed WAL into delta checkpoints, sorted page runs packed into large objects plus an index. When deltas grow past a multiple of the full checkpoint size they fold down into a new full checkpoint, which also lets old WAL age out per the retention window. Garbage collection deletes objects unreferenced by any retained manifest, and branch manifests pin whatever they reference.

## Branching and PITR

A branch is a new prefix containing only a manifest that points into the parent's immutable objects. It costs one small object and completes in well under a second. Writes to the branch diverge into its own epoch directories. Point in time recovery picks the manifest at or before the target time and replays parent WAL to the target LSN, materialized as a branch. Retention defaults to 7 days.

## Failure behavior

These are the invariants the test suite enforces, not aspirations:

- Writer crash mid batch: unacked commits are lost, acked commits are not, the next holder replays sealed WAL and the database is consistent.
- Zombie writer after lease expiry: epoch fencing makes its uploads unreachable.
- Object store errors: bounded retries with backoff, commits stall rather than lie.
- Manifest CAS race: the loser rereads and routes to the winner.
- Partial checkpoint upload: a checkpoint is referenced by the manifest only after every object and the index are verified, orphans are collected later.
