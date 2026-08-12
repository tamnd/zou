# Benchmarks

This page is the write up: what was measured, on what, and what it means.
Which milestone claims those measurements have actually earned is one generated page in the harness repo, [tamnd/zou-bench `docs/dashboard.md`](https://github.com/tamnd/zou-bench/blob/main/docs/dashboard.md), where every row carries the line it has to beat, the run it was read from, and the milestone box it ticks.
A claim nothing has measured yet is on that page saying so rather than left off it.

Recorded baselines for the commit latency harness.
Reproduce with `cargo run --release -p zou-store --features s3,sqlite --example commit_latency -- [target]`.
Without a target the harness runs on a fresh temp directory, with one it runs on whatever the string names, a directory, `sqlite://`, a `.zou` file, or `s3://`, under a nonce prefix it deletes afterwards.
`ZOU_BENCH_SAMPLES` and `ZOU_BENCH_WARMUP` shrink the run for slow remote endpoints.

## Commit latency

The harness measures append-to-ack for 2000 sequential 128 B commits after 200 warmup commits, one producer, defaults everywhere (2 ms flush interval, 512 KB flush bytes).
A sequential single producer pays the full flush interval on every commit since no other producer fills the batch, so these numbers are the worst case for the batching design and real concurrent workloads amortize better.

Express and Buffered are interface stubs in M1 and behave like PureS3 by design, so the table records the pure_s3 tier per backend.
Every run confirmed the stubs measure within noise of pure_s3.

2026-08-01, MacBook (Apple Silicon, macOS 15, APFS), rustc 1.97.1, release profile, MinIO in podman on localhost:9000:

| backend | p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- |
| local fs (APFS) | 7.8 ms | 8.8 ms | 9.8 ms | 14.1 ms |
| sqlite:// (WAL, synchronous FULL) | 3.1 ms | 3.4 ms | 3.5 ms | 4.6 ms |
| .zou single file | 7.0 ms | 7.7 ms | 8.3 ms | 13.0 ms |
| s3:// MinIO on localhost | 4.1 ms | 5.0 ms | 5.7 ms | 10.5 ms |

Reading the table: the 2 ms flush interval is a floor for every backend, the rest is the durability call.
On APFS a full fsync costs 4 to 5 ms, which is why the local directory and the .zou file, both fsync per flush, sit near 7 to 8 ms.
SQLite in WAL mode with synchronous FULL syncs only the WAL file and lands near 3 ms.
MinIO on localhost acks a PUT in about 2 ms of wire and server time, so it beats the local fsync path, a reminder that these numbers measure the durability primitive, not the network to a real region.

## Per tier targets, gap analysis

Targets from the M1 exit checklist: pure s3 under 25 ms p50, express under 10 ms p50, buffered under 2 ms p50, all against real backends.

- pure_s3: the full pipeline against an S3 compatible endpoint measures 4.1 ms p50 with localhost wire time, leaving over 20 ms of headroom under the target for real S3 round trip time. The target is credible but not yet met on evidence: it needs a run from a box in an AWS region against real S3, planned on the benchmark machines, no numbers are assumed here.
- express: same shape, needs S3 Express One Zone from an in region box. The stub confirms the pipeline adds about 2 ms over the durability call, so the under 10 ms p50 target hinges almost entirely on the One Zone PUT time.
- buffered: the write buffer service does not exist yet, there is nothing to measure. Under 2 ms p50 requires acking before object storage, which is exactly what the buffered tier is for, so this row stays open until that service lands.

## pgbench scale 100, four systems on one box

server3 (8 cores, 24 GB, Ubuntu, local NVMe), 2026-08-01, pgbench scale 100, 8 clients, 60 s runs, all driven by tamnd/zou-bench with the dated json kept in its result book.
Vanilla is Postgres 18.4 with stock settings, Neon is its docker compose stack self hosted on the same box, zou is the vendored 18.4 with the v1 smgr (vectored io plus the write through local page cache), once with the store on a local directory and once on MinIO on localhost.

| system | tpcb tps | tpcb avg lat | select tps | select avg lat | init |
| --- | --- | --- | --- | --- | --- |
| postgres 18, local NVMe | 506.4 | 15.8 ms | 4259.3 | 1.9 ms | 50.2 s |
| neon, self hosted compose | 109.9 | 72.8 ms | 562.4 | 14.2 ms | 208.7 s |
| zou v1, localfs store | 356.1 | 22.5 ms | 12773.7 | 0.63 ms | 1330.7 s |
| zou v1, minio store | 0.87 * | 9204 ms * | 37.8 * | 211.4 ms * | 4455.7 s |

Reading the table: zou select-only on localfs is 3x vanilla and 22.7x Neon on identical hardware, because the page cache serves every read locally while the store only sees the write stream.
tpcb on localfs is 3.2x Neon and 0.7x vanilla, the remaining gap to vanilla is commit ack latency.

The starred MinIO row is the rerun after the concurrent fold change from perf spec 006, and it came back worse than the run it replaced, which is exactly why it is published.
The fold no longer blocks the pusher loop, but it now runs during the benchmark window and its GET traffic saturates the same localhost MinIO the foreground needs: the 60 s select-only run alone saw 2.2 GB of checkpoint range reads and 1.2 GB of wal reads from background fold and replay, plus 7019 page reads that should have been cache hits.
The 30 s buckets show the shape: tpcb did 53 transactions in its first half then 6 in its second at 67 s p50, and select-only was fully blocked for its first 30 s (1 transaction), ran the middle window at 76.5 tps, then stalled again at the tail.
Moving the fold off the commit path moved the stall, it did not remove it, because in the v1 store the fold has to read the entire wal and checkpoint history back through the object store to produce pages.
That cost is a property of the v1 layout, so this row stays as the honest v1 number and the fix is the storage v2 redesign rather than another pusher patch.

## A thousand tenants on one node

gamingpc (i9-13900K, 32 GB, Ubuntu 26.04 under WSL2, store on a local directory), 2026-08-10, `zoubench fleet` with the node pinned to cpus 0-7 so the eight cores are the deployment and the traffic generator is not sharing them.
A thousand registered tenants, a hundred attached at once, 16MB shared buffers each, 16 clients over the REST door, a 300 s measured window capped at 200 rps and a 300 s churn window drawing from all thousand.
Full write up and the raw shape in tamnd/zou-bench `docs/results/2026-08-11.md`.

| phase | requests | errors | rps | p50 | p99 | max |
| --- | --- | --- | --- | --- | --- | --- |
| steady, working set attached | 59999 | 0 | 200 | 0.715 ms | 1.298 ms | 4.251 ms |
| steady, 30 s warmup | 507 | 64 | 1.69 | 1.389 ms | 10786 ms | 11603 ms |
| churn, all thousand drawn from | 695 | 2 | 2.32 | 7617 ms | 18240 ms | 21897 ms |

Reading the table: once a project is up, a node holding a thousand of them serves reads in under a millisecond and a half at the tail, and everything else on this page is attach.
The two steady rows are the same node and the same hundred tenants measured with different warmups.
Thirty seconds is not long enough to attach a hundred tenants at 16 clients, so that row reports the attach storm, with the node itself confirming all 100 attaches happened inside the measured window at a 6.2 s mean.
Ten minutes of warmup is, and then the node answers every request the scenario asks for with none dropped.

Churn is the honest cost of attach.
Drawing uniformly from a thousand with room for a hundred, nearly every request pays an attach plus the eviction that makes room for it, and the node turned over 1758 attaches in 300 s at a 7.3 s mean while holding the gauge at 100.
These tenants are idle at eviction, so their attach is a restore and not a recovery, and the warm up added since this run has nothing to fetch on that path.
The sizing rule is still to keep the attached ceiling above the working set, because an attach is round trips whatever fills them.

Memory holds flat.
Peak RSS across the whole process tree is 15.7 GB over up to 1028 processes, median 13.6 GB, and across thirty minutes covering warmup, steady, idle and a churn window that attached seventeen hundred times the slope points slightly down, so about 140 MB per attached tenant and no drift.
Provisioning is almost all initdb and the genesis capture, with the registry write itself at 2.7 ms p50.
The 36.2 tenants a minute this run reports is not the rate of building a fleet and this page previously read as though it were: the run made 63 tenants and skipped the 937 that earlier passes had already built, so it times the tail of a job that was nearly done.
The from empty rate is 5.14 tenants a minute, measured on the eight hundred tenant fleet in the next section.
The store holds 45.6 GB for the thousand, 45.6 MB a tenant.

## Eight hundred mostly idle tenants, and what they cost asleep

gamingpc (i9-13900K, 32 GB, Ubuntu 26.04 under WSL2, store on a local directory), 2026-08-11, `zoubench fleet` with the node pinned to cpus 0-7, scenario `fleet-800-idle`.
Eight hundred tenants built from empty, a hundred attached at once, 16MB shared buffers each, 16 clients, then a 300 s window at 200 rps, a 300 s churn window drawing from all eight hundred, and ninety minutes of hold in which nobody sends a request at all.
The store is a local directory, so the dollars below are the S3 standard and R2 price cards of 2026-08-01 applied to the operations the run counted, not a bill anybody paid.
Full write up in tamnd/zou-bench `docs/results/2026-08-12.md`.

Building the fleet took 9346.5 s for 800 tenants, 5.14 a minute, with the registry write itself at 10.5 ms p50 and everything else initdb, the genesis capture and the first attach, eight at a time on eight cores.
That is the honest from empty number and it is three orders of magnitude under the M1b line of a hundred a second.
Serving them is unchanged from the thousand tenant node: 59999 requests at 200 rps with no errors, 0.692 ms p50 and 1.236 ms p99, and churn over all eight hundred at 1852.7 ms p50 and 11941.1 ms p99 with 4962 attaches at a 2.263 s mean.

The hold window is what the run was for, and it splits in two.

| a project nobody is using | puts an hour | gets an hour | deletes an hour |
| --- | --- | --- | --- |
| dormant | 0 | 0 | 0 |
| attached | 742.9 | 762.9 | 55.6 |

A dormant project is free rather than cheap.
It does nothing at all to the store, so it costs the bytes it left behind and not one request, which is scale to zero working the way the design says it should.
An attached project with no traffic puts an object every 4.85 seconds and reads one every 4.72, and that timer is the WAL lease heartbeat: the lease TTL is 15 s and the heartbeat renews at a third of it, a manifest read plus a conditional write each time, which is 720 gets and 720 puts an hour before anything else happens.

That rate is the whole bill.
Priced for a month the fleet is 294.18 usd on S3 standard, of which 0.76 is the 33 GB of data and 293.42 is 54.2 million puts, 55.7 million gets and 4.1 million deletes; on R2 it is 264.57.
Per project that is 0.37 a month against the 1.10 Neon models for the same scenario, and as a fleet it misses the M1b line of 90 by more than three times, entirely because an eighth of the fleet stays attached and the attached tenth is the only thing writing.
A cheaper price card does not fix it.
Either an idle attached project stops renewing and takes the restore if somebody comes back, or the renewal interval grows with idleness instead of sitting at a third of a fifteen second TTL forever, and that is [tamnd/zou#294](https://github.com/tamnd/zou/issues/294).

Memory across the whole run peaks at 17.5 GB, which is the provisioning phase and its eight concurrent initdbs, and sits at 8.6 GB median with a hundred attached over up to 1016 processes, slope negative across nearly four hours.
The store holds 35.66 GB for the eight hundred in 3.54 million objects, 44.58 MB a tenant.

## A hundred thousand registered tenants

server3 (8 vCPU, 24 GB, Ubuntu), 2026-08-11, MinIO on the same box on a single drive, `cargo test --release -p zou-store --test registry_scale` and `-p zou-server --test registry_cache_scale`.
A hundred thousand registered tenants, no databases behind them, which is what a registered tenant is: an entry pointing at a prefix.
Registered at 32 jobs, then read back at each decade on the way up.

| fleet | lookup p50 | p90 | p99 | max |
| --- | --- | --- | --- | --- |
| 1,000 | 3.120 ms | 8.992 ms | 30.405 ms | 71.807 ms |
| 10,000 | 2.596 ms | 5.651 ms | 14.167 ms | 23.951 ms |
| 100,000 | 2.138 ms | 4.940 ms | 16.055 ms | 25.456 ms |

A hundred times the fleet and the lookup does not move, which is the whole point of one object per tenant: routing a request is a point GET of one key and never a search.
A ref nobody registered answers in 1.372 ms p50, so a request naming a project that does not exist costs the same round trip as one that does, and a custom hostname costs 2.545 ms p50 for its two reads.
Registering ran at 458 to 524 a second at 32 jobs, 61.7 ms p50 per conditional PUT, and `registry list`, which is the one operation that does read every entry, took 5.7 s for the hundred thousand.
The entries themselves are 145 bytes each, so the fleet is 14.5 MB of logical objects, 145 MB at a million; MinIO's own single drive layout writes 785 MB for them, which is per object overhead on 4 KiB blocks rather than anything zou wrote.

The node half is the bounded cache in front of that store.

| what | p50 | p90 | p99 | max |
| --- | --- | --- | --- | --- |
| cold, one round trip each | 4.175 ms | 9.847 ms | 25.239 ms | 61.928 ms |
| warm, a working set of 500 | 601 ns | 641 ns | 761 ns | 302.886 µs |
| a ref nobody registered | 2.292 ms | 5.076 ms | 19.878 ms | 38.038 ms |

Resident memory went from 4.1 MiB to 7.2 MiB over 22,500 distinct tenants asked for, of which the cache can hold 4,096, and stayed there while it evicted.
That is the claim being measured: what a node spends on the registry is the bound and not the fleet, and a tenant nobody has asked for costs a node nothing at all.
Warm lookups are 601 ns because they never leave the process, which is what puts the apikey check in front of attach instead of behind a round trip.

What this does not measure is a node serving a hundred thousand, because attaching is what serving costs and that is the thousand tenant run above.
It also runs against MinIO on the same box, so the round trips are a loopback and a real S3 will be slower in the same shape.

## Attaching a tenant that crashed

Apple silicon laptop, 2026-08-11, `scripts/zou-cold-attach.sh`, a local store with `ZOU_STORE_SIM=s3-standard` in front of it so a get costs about 33 ms.
pgbench scale 25, a checkpoint, twenty seconds of load at four clients, then the postmaster killed with a nine.
Every attach starts from a fresh copy of the same store, because an attach is a write and a second one on the same store replays nothing.

| pool | warm up | restore | recovery to ready | attach | redo | pages redo read from the store |
| --- | --- | --- | --- | --- | --- | --- |
| 32MB | off | 3.92 s | 1099.60 s | 1103.52 s | 939.76 s | 10,586 |
| 32MB | on | 25.63 s | 391.82 s | 417.45 s | 246.70 s | 14 |
| 256MB | off | 3.67 s | 1518.77 s | 1522.44 s | 939.50 s | 10,586 |
| 256MB | on | 25.41 s | 589.08 s | 614.48 s | 5.09 s | 14 |

Restore is under four seconds because it does not download the database, and everything after it is crash recovery, which reads one page per record it replays and takes each of those round trips alone.
That is why redo is the same 939 s at either pool size when the warm up is off: it is not a memory problem, it is ten and a half thousand requests with never more than one in flight.
The warm up reads the WAL tail the restore just wrote, takes the pages redo is about to touch out of it, and fetches them in parallel, 10,584 pages and 17 fork sizes in about twenty one seconds, after which redo reads local files and does 14 store reads for the rest of its run.

What is left is the write half.
The last row is still ten minutes to ready and 583 s of it is the end of recovery checkpoint putting 10,743 dirtied pages back one at a time, which is why the smaller pool finishes sooner: it evicts during redo instead, where the same puts are partly batched, and its checkpoint writes 2,698 buffers in 145 s.
That path belongs to the storage redesign and not to attach.

## Cold start, from exec to the first row

Apple silicon laptop, 2026-08-11, `scripts/zou-cold-start.sh`, five runs, each starting from a pristine copy of the same store because an attach appends to the shared log and the next one replays what the last one wrote.
The project is what `zou tenant create` plus one attach leaves behind, an initdb and a genesis capture and nothing else, which is the small end on purpose: this measures the fixed cost every cold request pays, not a database's size.
One perl process starts the node and speaks every request over a socket, since a process spawn is ten milliseconds and the budget being measured is a hundred.

Binary init, which is the milestone's under 100 ms line:

| from | to | took |
| --- | --- | --- |
| exec | a connection taken on the http port | 5.7 to 6.2 ms |
| `main` | four doors listening | 0.3 ms |

The node's own breakdown is `up in 0.3 ms, arguments 0.1 ms, store 0.1 ms, doors 0.1 ms`, so nearly all of the six milliseconds is the exec and the dynamic linker, before there is a program to ask.
Nothing is opened at start that a request has not asked for, which is why the store lap is a tenth of a millisecond against a store that is thirty three milliseconds away: it is a handle and not a read.

Attaching that project with `ZOU_STORE_SIM=s3-standard` in front of the store, so a get costs about 33 ms:

| run | attached | restore | of which skeleton | of which wal catch up | warm | spawn | recovery |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 441.7 ms | 408.0 ms | 284.7 ms | 88.9 ms | 18.2 ms | 0.8 ms | 14.4 ms |
| 2 | 533.4 ms | 477.6 ms | 323.3 ms | 119.2 ms | 13.1 ms | 1.2 ms | 41.4 ms |
| 3 | 467.5 ms | 435.8 ms | 313.7 ms | 98.7 ms | 9.1 ms | 1.1 ms | 21.3 ms |
| 4 | 479.2 ms | 443.1 ms | 333.7 ms | 95.9 ms | 11.1 ms | 0.9 ms | 24.0 ms |
| 5 | 480.1 ms | 451.2 ms | 271.6 ms | 134.8 ms | 10.0 ms | 0.8 ms | 17.9 ms |

The two halves of the restore are the interesting split, and they are why they are timed apart.
The skeleton is a fixed set of objects a project's size does not change, twenty files here, fetched in parallel, so it is one round trip plus however long the bytes take.
The catch up is the shared log read from the newest checkpoint's redo page to the end of the stream, so it grows with how much was written since the last fold and not with the database.
The restore column is larger than the two of them added up because the same manifest is read twice, once to decide whether the tenant is fresh and once inside the restore, which at 33 ms a get is a round trip nobody needs.

Before and after the capture stopped storing trailing zeros: attach was 985.5, 1019.5, 1125.7, 1282.7 and 1285.9 ms on the same script and the same laptop, against 441.7 to 533.4 ms above.
A full capture used to PUT the whole 16 MB WAL segment holding redo, of which a fresh cluster has written 12,446,782 bytes, and the restore paid to read all sixteen back.
The INDEX now carries the file's real length beside the object's, the restore recreates the file at its length with a `set_len`, and the store went from 42M to 38M with the genesis WAL object at 12,446,782 bytes.
That the saving is four megabytes and the attach halves is the same fact from two sides: a cold attach at this size is bytes on the wire, not round trips.

Against a plain directory, where the bytes are free, the same five runs attach in 75.1, 81.5, 87.7, 157.2 and 173.2 ms.
The spread is the laptop and not the code, and the trimming is worth nothing here, which is the point of measuring both.

## Pending

- The same registry walk at a million entries and against real S3, which is the NFR-20 number rather than its smoke.
- A fleet rerun with a crashed tenant in the mix, since the churn above only ever attaches a cleanly stopped one and never pays for recovery.
- The write half of a cold attach, since with the pages warmed the end of recovery checkpoint puts them back one at a time.
- The manifest read twice on the attach path, once for the fresh check and once inside the restore.
- Capturing only the head of the WAL segment holding redo, since recovery validates page zero and then reads from redo forward.
- Real S3 and S3 Express One Zone runs from an in region box, plus GCS and R2.
- Concurrent producer runs to show batching amortization.
- MinIO rerun on the storage v2 layout once it exists, the v1 rerun above showed the fold cost is structural.
- pgbench scale 1000 and the TPCC shape from the M1b checklist.
