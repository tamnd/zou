# Benchmarks

This page is the write up: what was measured, on what, and what it means.
Which milestone claims those measurements have actually earned is one generated page in the harness repo, [tamnd/zou-bench `docs/dashboard.md`](https://github.com/tamnd/zou-bench/blob/main/docs/dashboard.md), where every row carries the line it has to beat, the run it was read from, and the milestone box it ticks.
A claim nothing has measured yet is on that page saying so rather than left off it.
Which box each row came off, dated, with its disk and a measured latency and bandwidth probe to its store, is in [docs/hardware.md](hardware.md).

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

## pgbench scale 10, the two read paths against vanilla and Neon

gamingpc (i9-13900K, 32 cores, 32 GB, Ubuntu under WSL2, stores on a local directory), 2026-08-28, pgbench scale 10, 4 clients, 300 s a phase, one box and one pgbench binary for every row.
Neon is its own docker compose stack, which is a pageserver, three safekeepers, a storage broker and a MinIO, with a compute node on Postgres 16.9 because that is what the compose builds.
Every other row is the vendored Postgres 18.4 with `io_method=sync` and `full_page_writes=off` and otherwise stock, which means 128MB of shared buffers everywhere.
The per run numbers and the full phase counters are in tamnd/zou-bench `docs/results/2026-08-28.md`.

| system | tpcb tps | tpcb avg lat | select tps | select avg lat | init |
| --- | --- | --- | --- | --- | --- |
| postgres 18, local disk | 720 | 5.552 ms | 71221 | 0.056 ms | 1 s |
| zou, object path, localfs store | 331 to 343 | 11.652 to 12.099 ms | 58497 to 60259 | 0.066 to 0.068 ms | 74 to 76 s |
| zou, layer path, localfs store | 366 to 374 | 10.686 to 10.921 ms | 12789 to 16609 | 0.241 to 0.313 ms | 4 to 6 s |
| zou, layer path before #695 | 81 to 83 | 48.8 ms | 14118 to 14431 | 0.28 ms | 5 s |
| neon, self hosted compose | 246 | 16.268 ms | 5855 | 0.683 ms | 4 s |

The two zou rows are five runs on the object path and six on the layer path, taken alternately on two evenings, which is why they are ranges, and the only thing that changed between the two layer path rows is #695.
One number in there is not a range, it is an outlier: the six layer select-only runs are 12789, 13094, 13101, 13111, 13303 and 16609, so five of them agree to three digits and the sixth does not, and nothing we changed between them explains it.
Read the low end of that column until it reproduces.
Against the outside systems: on the write mix the layer path is 1.5x Neon and about half of vanilla, on the read only mix it is 2.2x Neon and about a fifth of vanilla, and the object path is the other way round, 1.3x Neon on writes and 0.83x vanilla on reads.

The two zou rows are not a fast path and a slow one, they are two prices, and the ratio between their tps columns means nothing without what each of them paid for it.
So here is what they paid, from the store counters of one pair of runs, both legs on the same build, differenced at each phase boundary rather than rolled up over the run.
`scripts/zou-bench.sh` prints these under each phase now, and leaves a copy of the counter file at every boundary so a finished run can be asked the rest.

| phase | object path | layer path |
| --- | --- | --- |
| pgbench -i -s 10 | 76 s, 46227 page ops of 359.0 MiB, 338 wal ops of 14.3 MiB | 6 s, 31823 page ops of 23.5 MiB, 192 wal ops of 28.5 MiB, 302 shard ops of 264.7 MiB |
| checkpoint | 12 s, 2145 page ops of 16.0 MiB | 0 s, 76 page ops of 33.1 KiB |
| tpcb-like | 335 tps, 68131 page ops of 529.8 MiB, 227436 wal ops of 321.9 MiB, reads 30729 pages at 32 us p50 | 366 tps, 62968 page ops of 46.0 MiB, 110776 wal ops of 127.6 MiB, 6330 shard ops of 1.0 GiB, reads 43567 pages at 2048 us p50 |
| select-only | 58497 tps, 3238099 page ops of 24.7 GiB, 818689 wal ops of 963.6 MiB, reads 3192386 pages at 16 us p50 | 16609 tps, 511063 page ops of 4.2 MiB, 80522 wal ops of 121.7 MiB, reads 908181 pages at 1024 us p50 |
| shutdown | 16520 page ops of 128.7 MiB | 3 shard ops of 206.1 KiB, no page ops |

A page op on the object path is a page: 24.7 GiB over 3238099 ops in the read only phase is 8192 bytes each, to the byte.
A page op on the layer path is not: 4.2 MiB over 511063 ops is under nine bytes each, which is the relation length record filed under the same prefix, and the pages themselves stay inside the shard layer objects that the page service reads on the other side of the socket.
So the read only phase moved 25 GB through the store on one path and 4 MB on the other, and the path that moved 25 GB is the one that finished 3.5x the transactions.
The write phase is the same trade with the sign flipped: the object path writes 530 MiB of pages, the layer path writes 46 MiB of length records and pushes the change into wal instead, and there it is the layer path that ends up ahead.
Load is where it is loudest, 76 s against 6 s and 359 MiB of pages against none, and shutdown is the object path paying its last 128.7 MiB of pages while the layer path pays nothing because it never had a dirty page to flush.

Which one wins depends on the shape of the tenant rather than on the paths, so the two tps columns are not a ranking.
A tenant that loads a lot, writes steadily, and reads a working set that fits in shared buffers wants the layer path: it loads in seconds instead of minutes, it is ahead on the write mix, and it only pays for a read when the buffer is missing.
A tenant that writes rarely and reads widely wants the object path: a miss costs a 16 us store get there against a millisecond through the page service, and 3.2 million misses in five minutes is the whole difference between the two read only numbers.
The scale 10 read only phase is that second shape at its most extreme, since the database is 160 MB and a miss is cheap for everybody, and it is the shape this table is worst at describing.

The service tier p50 of 1024 us is what #671 and #697 are about, and it is a wait rather than work: #695 took the parked read poll from 100 ms to a millisecond, and what is left is a read position that is one lsn for the whole cluster instead of one per block.

### One read position per block

gamingpc, 2026-08-29, the same scale 10 and 4 clients and 300 s a phase, both legs on one binary back to back with `ZOU_LWLSN_ENTRIES` the only difference, so this is #697 against the code immediately before it rather than against another evening.

| lwlsn table | tpcb tps | select tps | reads that waited for ingest | tpcb read p50 | select read p50 | service p99 |
| --- | --- | --- | --- | --- | --- | --- |
| on | 362 | 18992 | 2 of 1080956 | 512 us | 512 us | 8192 us |
| off | 361 | 16129 | 30044 of 926211 | 2048 us | 512 us | 8192 us |

The waits are gone, 30044 of them down to 2, which is what the change is for.
The write mix does not move, because a writer waits for its own commit either way and 8 ms of that is the store PUT, and what it gets instead is the read half of the mix at a quarter of the latency.
The read only phase gains 18% and lands above every layer path run before it, the 16609 outlier included.
The service tier p99 does not move at all, and that is the honest headline: what is left is not a wait.
Serving a read costs 128 us at p50 and 512 at p95 on this box, the select phase asks for 3462 of them a second, and one thread owns ingest and every read, so the tail is that thread's queue.
The read position was one of two things in the way and this is the other one, which is what #671 stays open for.

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

## A hundred thousand realtime sockets on one node

server3 (8 cores, 23 GB, store on a local MinIO) as the node under test, server2 (6 cores, 12 GB) on the other side of the internet as the generator, 2026-08-17, `zoubench sockets`.
A hundred thousand websockets, each joined to a channel filtered `shard=eq.N`, so every socket on a shard is owed every row written to it.
The generator has to be the other box, because a hundred thousand descriptors and a goroutine each on the node under test makes the node's cpu partly the benchmark.

Three runs, and the shards are what tell them apart.
`sockets-10k` is ten thousand sockets over five hundred shards, `sockets-100k` is a hundred thousand over a thousand, so a hundred sockets are owed every row, and `sockets-100k-wide` is the same hundred thousand over ten thousand shards, so nine are.
Same socket count, same rows a second, an order of magnitude apart in deliveries: raising the shard count moves the work from fan out to change processing, which is how the two costs are told apart.

Neither box was quiet, and this is not a lab.
server3 runs the owner's crawler at between one and two of its eight cores and a MinIO at a third of one, load average two to four before the run.
server2 runs a browser and other work at about one and a half cores.
Every number below has that in it, and the two the node measures about itself are the ones to read.

| | sockets-10k | sockets-100k | sockets-100k-wide |
| --- | --- | --- | --- |
| shards, sockets on each | 500, 20 | 1000, 100 | 10000, 9 |
| writers, rows a transaction | 4, 25 | 8, 25 | 16, 250 |
| sockets held, of asked | 10000 of 10000 | 100000 of 100000 | 99979 of 100000 |
| refused | 0 | 0 | 21 |
| lost mid run | 0 | 6 | 0 |
| ramp | 42 s | 493 s | 690 s |
| node RSS at full ramp | 254 MB | 2.28 GB | 2.37 GB |
| rows committed in the window | 33750 in 120 s, 281 a second | 18825 in 300 s, 62.8 a second | 136613 in 300 s, 455 a second |
| deliveries owed | 675000 | 1882500 | 1366046 |
| deliveries missing | 0 | 110 | 4672 |
| shards with no delivery | 0 of 500 | 0 of 1000 | 0 of 10000 |
| node change fan out, mean | 11.8 ms | 47.1 ms | 29.4 ms |
| node commit to socket, mean | 73.2 ms | 126.7 ms | 106.6 ms |
| client delivery p50 | 394 ms | 2530 ms | 1790 ms |
| client delivery p99 | 2920 ms | 286000 ms | 40800 ms |
| client commit p50 / p99 | 186 ms / 2100 ms | 653 ms / 53566 ms | 727 ms / 15632 ms |
| connect p50 / p99 | 39 ms / 478 ms | 77 ms / 1614 ms | 74 ms / 2838 ms |
| join p50 / p99 | 13 ms / 238 ms | 27 ms / 962 ms | 27 ms / 1636 ms |

A hundred thousand sockets on one node is held rather than argued about: on `sockets-100k` every one asked for was accepted, none refused, and the node's own gauges agree at 99994 sockets and 99994 subscribers.
Six sockets died mid run, all four distinct failures a read timeout, which at a hundred thousand sockets over the public internet for eleven minutes is the internet.
Of 1,882,500 deliveries owed, 1,882,390 arrived, so 110 did not, one delivery in seventeen thousand, and every one of the thousand shards was served.
That accounting is per shard and exact, rows in that shard times sockets on it, so a run that dropped frames says so rather than publishing a percentile over the ones that survived.

The wide run's 21 refusals and 4672 missing are both the harness and both worth naming rather than rounding off.
The refusals are read timeouts on the handshake, spread over all six ports, from a generator whose ramp took eleven and a half minutes against a node at a third of its cpu.
The missing are the drain: it is fifteen seconds and that run's delivery p99 is 40.8 s, so frames still in flight when the window closed are counted as never arriving, which is the accounting being pessimistic on purpose rather than a node dropping rows.
A drain longer than the tail it is draining is the fix, and until then 4672 of 1,366,046 is an upper bound on loss and not a measurement of it.

Memory is 23.4 KB a socket, and that number is the whole reason this run fits on the box.
Before the read buffer on a socket was sized for a realtime client, it was 128 KB eagerly allocated and zeroed before every read, which is 145 KB a socket, and a hundred thousand of those wants 14.5 GB on a 24 GB box that is also running a database, a MinIO and somebody's crawler.
The 10k run measured that change directly, 1.45 GB down to 254 MB at the same socket count, with `__memset_avx2_unaligned_erms` going from 14.66 percent of the node's profile to 2.30.

The node's own cost of a change is the number this page is actually publishing.
Reading one change out of the stream and writing it to all hundred sockets that are owed it takes 47.1 ms at the mean, and the distance from the database's own commit timestamp to the frame leaving for the socket is 126.7 ms.
Both are measured by the node, on 1,882,394 changes, and both sit in the result file beside the client's view.

The client's tail is not the fan out, it is the writes, and three readings say so together.
`sockets-100k` asks for a thousand rows a second and got 62.8, because eight writers times twenty five rows over a 3.57 s commit mean is 62.8.
The delivery clock starts before the insert is sent, on purpose, so a row that waited 550 s to commit is a 563 s delivery, and a commit p99 of 53.6 s is most of a delivery p99 of 286 s.
The gap that is left, a client mean of 22.8 s against a commit mean of 3.57 s plus the node's 127 ms, is the generator's own read side queueing on six shared cores with a hundred thousand goroutines to schedule, which is why the node's histograms and not the client's mean are the node's number.

`sockets-100k-wide` is the same claim from the other direction.
Nothing about the realtime tier changed between the two runs: the shards went up, the transactions went from twenty five rows to two hundred and fifty, and the writers from eight to sixteen, which a project's forty connections leave room for.
The rate went from 62.8 rows a second to 455, the commit mean from 3.57 s to 1.80 s, the client's delivery p99 from 286 s to 40.8 s, and the node's own cost of a change fell too, 47.1 ms to 29.4 ms, because it is delivering nine sockets a row instead of a hundred.
Two runs at the same socket count, seven times the write rate, and the node under a third of its cpu in both.

So the socket half of the M4 line is measured and the rate half is not: the best of these is 455 rows a second of a thousand, and what is in the way is the engine's commit path rather than anything in the realtime tier.
More writers is not the answer either, since a project's postmaster takes forty connections in total, and at twenty five rows a transaction all forty of them would only reach 280 rows a second.

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
