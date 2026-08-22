# Storage engine

zou-store makes object storage the only durable medium while keeping commit and page read latency acceptable. The design borrows from Neon (page server model), SlateDB (LSM on S3), and Litestream (WAL shipping).

## Object layout

One prefix per logical database:

```
s3://<bucket>/registry/<ref>.json  one entry per registered tenant
s3://<bucket>/tenants/<ref>/
  MANIFEST                      current manifest, swapped with CAS
  manifests/<epoch>-<unix>.json manifest history, enables PITR and branching
  log/cellwal/<shard>/<seq>     the WAL chain, one landing segment per seq
  log/cellwal/<shard>/manifest  the chain head, swapped with CAS
  chk/<chk-id>/index            page location index for one checkpoint
  chk/<chk-id>/<n>.pages        sorted page images, 64 to 256 MB objects
  files/objects/<id>/<version>  Storage API object bytes
  files/renders/…               image transforms of those, cached
  files/uploads/<id>/<part>     pieces of an upload that has not finished
```

The `files/` prefix is the Storage API's, and what goes under it is not the engine's business: the row for an object is in the database, in `storage.objects`, and the bytes are keyed by the id and version that row carries, so nothing a client ever sends reaches a key. What the layout owns is that they are under the tenant. One prefix per tenant is what makes a shared bucket safe, and it makes removing a tenant a prefix delete rather than a query. gc does not walk it: gc collects unpinned checkpoints and expired manifest history, and a file is removed by the request that removed its row.

A branch reads through. Its rows arrive with the database it was branched from, so they name ids the child's own prefix has never held, and a read that misses at home tries the parent and then the parent's parent. Writes never do: an upload into a branch lands in the branch, and deleting an object in a branch deletes the row and leaves inherited bytes alone, because the parent is a live database somebody else is using. That leaves bytes a branch has stopped referencing and cannot remove, which is the cost branching already pays for pages.

The log is under the tenant, which is a statement about writers rather than about storage. A chain has exactly one writer and fences every other one off it: a newcomer seals the chain, and the incumbent's next landing PUT loses to that seal and it steps down. The writer today is the postmaster that has the project attached, and a node serving a fleet runs one of those per project, so a chain two projects shared would be a chain they take from each other forever. A cell wide log that many projects land in wants a sequencer of its own for the same reason, and that is a different writer, not a different tenant; until it exists a project's log is the project's.

Everything under log/ and chk/ is immutable once written. Only MANIFEST is mutated, and only through conditional PUT (If-Match on S3 and R2, preconditions on GCS, atomic rename plus lock on the local backend). That gives atomicity without any external coordination service.

The same layout lives on any backend that passes the CAS contract. A plain path is a directory tree of one file per object, and a path ending in `.zou` is the single file backend: append only frames holding keys, fixed width metadata, and lz4 compressed payloads column wise, a crc guarded scan that truncates torn tails at open, versions as never reused sequence numbers, and compaction that rewrites live entries and swaps the file in atomically. One process owns a `.zou` file at a time through an OS level lock, which fits every sequential tool, initdb over the shim, zou-bootstrap, zou info, zou branch, and `zou check`, which restores a store and reads every table of every database in it through the single user backend and is the whole engine over one file with one process holding it, exactly the shape the lock wants. What one lock still rules out is the multi process postmaster, where every backend opens the store for itself, so `zou dev` and `zou serve` refuse a `.zou` target by name before they open anything and say to copy it into a directory first, which `zou push <file>.zou <dir>` does. A bootstrapped store that spreads over a few thousand files in a directory lands in one file about a quarter the size.

Builds with the `sqlite` feature add one more single file backend: `sqlite://<path>`, or a bare path ending `.db`, `.sqlite`, or `.sqlite3`, keeps the whole store in one SQLite database, WAL mode with synchronous FULL so an acked put survives power loss, conditional PUT as an UPDATE guarded by the expected version, per key integer versions, and the same lz4 rule as the .zou backend. Where .zou bets on a purpose built format, this one bets on the most deployed storage engine there is, and it brings tooling for free, `sqlite3 store.db` inspects a live store and `.backup` takes a consistent hot copy. `ZOU_SQLITE_SYNC=normal` relaxes durability for benchmark runs only.

## Tenant registry

A server that serves more than one database has to answer, on the request path, which tenant a request is for and whether the caller may have it. That answer is `registry/<ref>.json`, one small object per tenant holding the ref, the second it was registered, the project's JWT secret, and any extra hostnames that route to it.

It holds one thing that is not about routing: a count of how many times the project has been deployed to under `/functions/v1`. It is there because it is the one object every node serving the project reads anyway, so a node can find out that there is a new deployment to pick up without asking the store a question of its own. A counter rather than a time or a digest, because two nodes comparing it need no clock and no bytes, and zero is both a project nobody has deployed to and an entry written before the field existed.

A custom hostname is a second small object, `hosts/<host>.json`, holding the ref that claimed it. Same shape and same reasons: resolving one is a point GET on the request path, and claiming one is a conditional write, so the first project to ask for `api.example.com` is the one that has it and a second is told rather than quietly taking it over. Unregistering a tenant frees the hostnames it held, since a name claimed forever by a tenant that is not there is a name nobody can reclaim.

One object per tenant rather than one index object for all of them, for two reasons that both bite at scale. The lookup is the hot operation, and against a per tenant key it is a point GET the store can cache, while against an index it is a read of every tenant to find one. And creating a tenant against a per tenant key is a conditional PUT that fails if the ref is taken, while against an index it is a read modify write two creates can lose.

The secret lives here because the front door has to verify an apikey before it attaches anything. Verifying against the tenant's own database would mean acquiring its lease, hydrating it, and starting a session for a request that turns out to be unauthenticated, which is a lever anyone on the internet can pull. Reading one small object and checking a signature is not.

The S3 pair a project's storage endpoint is asked with lives here too, for the same reason one pair per server would be wrong: a fleet answering one pair for everything on it would let whoever holds it sign for a project they were never given anything for. An entry without one is a project whose S3 endpoint says of every signed request that the access key is not one this project has, which is what an older entry written before the field existed reads as.

What a project asks of the auth service lives here as well, for a third version of the same reason: a node builds a project's config before it has attached the project, so a setting kept in the project's own database is a setting the node cannot read in time, and an environment variable on the node can only say one thing for every project on it. Two settings are carried, whether a sign up has to prove its address and where the links in that mail point, and an entry that says nothing means a sign up confirms itself. That default is deliberate rather than strict: a node with no mail server that asked for a confirmation would answer the sign up with a 200 and send nothing, which is the failure the field exists to end. A project that does ask for a confirmation on a node with no mail server is refused at the sign up instead, because an error somebody can act on beats a link nobody receives.

`zou tenant <target> create <ref> [--secret <s>]` registers one and prints the generated secret and the generated S3 pair, `list` prints the refs and whether each has a database yet, `info <ref>` prints the entry, and `delete <ref>` removes the entry. `keys <ref>` mints the anon and service_role keys from the secret the entry holds, which is how a client is configured for a project nobody has a port on, and `--env` prints them in the form a shell evals, together with the S3 pair under the names the Supabase CLI gives it. `s3 <ref>` gives a pair to a project registered before pairs existed, and `--rotate` replaces one, which takes effect the next time the project is attached because a server holding it in its config goes on holding it. `auth <ref>` prints the project's auth settings and `auth <ref> [--confirm-email on|off] [--site-url <url>]` changes the ones it names and leaves the rest, with `--site-url ""` clearing it back to the address the project is reached at. `host add <ref> <host>` points a project's own domain at it and `host remove` gives it back. Registering is not creating a database and deleting is not destroying one: the data under `tenants/<ref>/` arrives when something bootstraps or branches into it, and stays when the entry goes, because a router's index should not be able to destroy a project as a side effect of forgetting it. None of them takes a lease or writes into a tenant prefix, so they all run against a live store.

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

A branch is a new prefix containing only manifests that point into the parent's immutable objects. It costs a handful of small objects and completes in well under a second whatever the database size. `zou branch <target> create <src> <dst>` takes it at the parent's last published state, and `--at` pins it: a value shaped like an LSN, `X/Y` or `0x` hex, must name a checkpoint lsn since branch points sit on the fold grid, while plain digits are a unix second that materializes the newest history snapshot at or before it, which `--from-time <unix>` also spells out on its own. `zou branch <target> list [parent]` prints the branches on a store and who they came from, and `zou branch <target> delete <ref>` removes one along with everything under its prefix, refusing a tenant that is not a branch, one that has branches of its own, and one whose lease is still live. The lower level `zou-branch <store-root> <src> <dst> [--at <lsn>|--ts <unix>]` tool exposes the same machinery with the two modes spelled out, and `zou info <target> [ref]` prints a tenant's manifest, checkpoint chain, reconciled WAL tail, and history snapshot count without taking a lease or writing anything.

The child manifest carries two things. Inherited checkpoint refs are tagged with their owner, so reads and restores fetch those objects from the owner's prefix no matter how many hops down the branch chain sits, and `branch_of` records provenance. No WAL crosses a branch point: the tenant's WAL lives in a log of its own keyed by tenant id, the fold that made the branch point checkpoint already covers everything before it, and the child starts a stream of its own. Writes to the branch diverge into its own epoch directories under its own prefix, the parent's objects are never touched.

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

## Format numbers

Every durable object carries a format number: the tenant manifest, the page shard manifest, the registry entry and the host alias, the shard map, the chain shard manifest, the round index, and, as two little endian bytes after the magic, the landing segment, the sealed segment and the layer file.

A reader refuses a number higher than the one its binary knows, by name, saying what it found and that the binary is behind. It refuses before it verifies anything else, so an operator reading the log sees a version to upgrade past rather than a file that looks corrupt. `crates/zou-log/tests/formats.rs` holds all ten to that, and a format added later belongs in it.

Which way a change goes is the part worth getting right, because a fleet is never all one version at once. During a rolling restart the new binary writes and the old one reads, and the old one is the one at risk.

Bump the number when an older binary reading the object would get the wrong answer rather than an incomplete one. Adding a field an old reader ignores at its peril is a bump: none of these structs deny unknown fields, so an old binary reading, modifying and CAS writing an object drops every field it does not know, and the write succeeds. The format number is the only thing between that and losing what a newer node wrote. Changing what an existing field means is a bump. Removing a field an old reader requires is a bump.

Leave the number alone when an old reader that ignores the addition is still correct, and only then. A purely additive diagnostic field that nothing routes or fences on is the case that qualifies, and it still has to survive the read modify write question: if an old node can round trip the object and drop the field, the field has to be one nobody misses.

A bump costs a restart ordering: readers have to be upgraded before writers, or the readers refuse until they are. That is the intended cost. The alternative is a node that keeps serving from a map it half understood.
