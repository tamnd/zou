# Storage engine

zou-store makes object storage the only durable medium while keeping commit and page read latency acceptable. The design borrows from Neon (page server model), SlateDB (LSM on S3), and Litestream (WAL shipping).

## Object layout

One prefix per logical database:

```
s3://<bucket>/tenants/<ref>/
  MANIFEST                      current manifest, swapped with CAS
  manifests/<epoch>-<unix>.json manifest history, enables PITR and branching
  wal/<epoch>/<start-lsn>.wal   sealed WAL segments, immutable
  chk/<chk-id>/index            page location index for one checkpoint
  chk/<chk-id>/<n>.pages        sorted page images, 64 to 256 MB objects
  files/<bucket>/<key>          Storage API user files
```

Everything under wal/ and chk/ is immutable once written. Only MANIFEST is mutated, and only through conditional PUT (If-Match on S3 and R2, preconditions on GCS, atomic rename plus lock on the local backend). That gives atomicity without any external coordination service.

The same layout lives on any backend that passes the CAS contract. A plain path is a directory tree of one file per object, and a path ending in `.zou` is the single file backend: append only frames holding keys, fixed width metadata, and lz4 compressed payloads column wise, a crc guarded scan that truncates torn tails at open, versions as never reused sequence numbers, and compaction that rewrites live entries and swaps the file in atomically. One process owns a `.zou` file at a time through an OS level lock, which fits every sequential tool today, initdb over the shim, zou-bootstrap, zou info, and zou branch, while attaching the multi process postmaster to one waits on the in process engine. A bootstrapped store that spreads over a few thousand files in a directory lands in one file about a quarter the size.

Builds with the `sqlite` feature add one more single file backend: `sqlite://<path>`, or a bare path ending `.db`, `.sqlite`, or `.sqlite3`, keeps the whole store in one SQLite database, WAL mode with synchronous FULL so an acked put survives power loss, conditional PUT as an UPDATE guarded by the expected version, per key integer versions, and the same lz4 rule as the .zou backend. Where .zou bets on a purpose built format, this one bets on the most deployed storage engine there is, and it brings tooling for free, `sqlite3 store.db` inspects a live store and `.backup` takes a consistent hot copy. `ZOU_SQLITE_SYNC=normal` relaxes durability for benchmark runs only.

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

A branch is a new prefix containing only manifests that point into the parent's immutable objects. It costs a handful of small objects and completes in well under a second whatever the database size. `zou branch <target> <src> <dst>` takes it at the parent's last published state, and `--at` pins it: a value shaped like an LSN, `X/Y` or `0x` hex, must name a checkpoint lsn since branch points sit on the fold grid, while plain digits are a unix second that materializes the newest history snapshot at or before it. The lower level `zou-branch <store-root> <src> <dst> [--at <lsn>|--ts <unix>]` tool exposes the same machinery with the two modes spelled out, and `zou info <target> [ref]` prints a tenant's manifest, checkpoint chain, reconciled WAL tail, and history snapshot count without taking a lease or writing anything.

The child manifest carries two things. Inherited checkpoint refs are tagged with their owner, so reads and restores fetch those objects from the owner's prefix no matter how many hops down the branch chain sits, and `branch_of` records provenance. No WAL crosses a branch point: the tenant's WAL lives in the shared log keyed by tenant id, the fold that made the branch point checkpoint already covers everything before it, and the child starts a stream of its own. Writes to the branch diverge into its own epoch directories under its own prefix, the parent's objects are never touched.

The page shard store branches the same way. Each of the parent's SHARD manifests is copied under the child, every inherited entry tagged with its owner and, when a delta spans the branch point, cut at it, so the reader fetches inherited layers from the owner's shard prefix and never serves a record past the cut. The copy captures the parent's layer list at branch time on purpose: the parent keeps compacting, and its own manifest will stop naming layers the child still needs. Shard layer gc, when it lands with compaction, pins through these owner tags the same way checkpoint gc does today. A branched shard writes manifest format 2, which a binary that predates inheritance refuses whole instead of quietly fetching from the wrong prefix. The shard copies land as plain puts before the tenant manifest CAS, which stays the single commit point, so a branch that crashes midway leaves only orphans a retry overwrites.

History snapshots make PITR work: every manifest state change writes a copy under `manifests/`, at most one per second. Garbage collection pins everything a snapshot younger than the retention window references, a week by default, so a branch materialized inside the window always finds its objects. Snapshots past retention are collected through the same two phase candidate window as everything else, and a parked branch pins its inherited objects for as long as it lives.

Restoring a branch to a data directory works through `zou-restore <store-root> <pgdata> <ref>`, and `--at <unix>` restores the newest history snapshot at or before that second instead of the live head, stopping on the fold grid. One detail makes such a streamless attach bootable at all: pg_control names the lsn of the checkpoint record itself and recovery starts by reading it, but a delta capture carries no pg_wal and a branch child has no shared log stream of its own to overlay. So every fold stores a WALTAIL object next to its capture, the stream bytes from the redo page boundary through the end of that record, and the restore lays the newest checkpoint's tail into pg_wal before overlaying whatever live frames exist. Replaying those few records is a no op, the fold already read the pages they touch, but without them the server panics on a checkpoint record that exists nowhere. A live server serves a branch through the chain reader: inherited page runs answer from their owner's prefix, and truncate events in a newer delta mask only the blocks past their cutoff so surviving rows of an inherited table keep serving. The first fold on a branch that captures a full merges the inherited chain into the child's own run objects, after which the manifest names nothing of the parent and gc may unpin it.

## Torn pages and full_page_writes

zou clusters run with `full_page_writes = off`. The flag is set at initdb time so it lands in the generated postgresql.conf, and since genesis capture and restore carry the config files, every restart, restore, and branch inherits it. This section is the argument for why that is safe here when it is not on ordinary local storage.

Stock Postgres needs full page writes because an 8 KB page write to a local disk can tear: the kernel and the device only promise atomicity in smaller units, so a crash mid write leaves a page that is half old and half new bytes. WAL redo cannot repair such a page, because most records are deltas that only make sense against a sound base. Upstream's fix is to write the entire page into WAL on its first modification after each checkpoint, so recovery restores a known good image before applying deltas. The price is WAL volume and the commit latency spike right after every checkpoint.

In zou that failure mode does not exist, because no durable medium is ever written in place. The embedded storage manager keeps every relation page as one object in the store, and a put is atomic on every backend: a plain or conditional PUT on S3 and GCS, a temp file plus rename on a directory store, a crc guarded frame in the `.zou` single file backend whose torn tail is truncated at open, and a transaction in the sqlite backend. A reader sees the old bytes or the new bytes, never a mix. In the layered engine pages are never written at all, they are reconstructed from immutable layer objects, each block range guarded by the crc in its own index row, plus WAL redo. Compute local state is disposable and recovery never trusts it: a crashed node reattaches from the store, and a torn file in the local NVMe cache fails its decode and turns into a miss and a refetch, not an error. The WAL stream itself is framed with checksums and a commit is only acked after its put succeeds, so the record a page depends on is durable before the page can ever change.

Turning the setting off does not make images vanish from the WAL. Records whose redo is the image itself remain, log_newpage during index builds and bulk loads, and on a checksummed cluster the hint bit images, and redo applies them like any other record. What goes away is the first touch image per page per checkpoint cycle. WAL volume stops spiking after checkpoints, and the record chains the page service replays are born as deltas on top of the block's own init record. The chain cutting that bounds reconstruction work then comes from image layers built by consolidation and compaction, not from Postgres re imaging its working set every checkpoint.

The integration tests run their clusters with the setting off, so every corpus the redo pool and the page service are proven on has this shape, and one of them asserts no heap block in the corpus ever received an image. The crash loop drill, kill -9 mid load then reattach from the store alone, runs without full page protection and still finds every acked commit.

## Failure behavior

These are the invariants the test suite enforces, not aspirations:

- Writer crash mid batch: unacked commits are lost, acked commits are not, the next holder replays sealed WAL and the database is consistent.
- Zombie writer after lease expiry: epoch fencing makes its uploads unreachable.
- Object store errors: bounded retries with backoff, commits stall rather than lie.
- Manifest CAS race: the loser rereads and routes to the winner.
- Partial checkpoint upload: a checkpoint is referenced by the manifest only after every object and the index are verified, orphans are collected later.
