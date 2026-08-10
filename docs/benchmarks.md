# Benchmarks

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

Churn is the honest cost of eager attach.
Drawing uniformly from a thousand with room for a hundred, nearly every request pays an attach plus the eviction that makes room for it, and the node turned over 1758 attaches in 300 s at a 7.3 s mean while holding the gauge at 100.
Attach hydrates before serving the first row rather than faulting pages in on demand, and lazy hydrate belongs to the storage v2 redesign, so until that lands the sizing rule is to keep the attached ceiling above the working set.

Memory holds flat.
Peak RSS across the whole process tree is 15.7 GB over up to 1028 processes, median 13.6 GB, and across thirty minutes covering warmup, steady, idle and a churn window that attached seventeen hundred times the slope points slightly down, so about 140 MB per attached tenant and no drift.
Provisioning costs 36.2 tenants a minute at 8 parallel jobs, almost all of it initdb and the genesis capture, with the registry write itself at 2.7 ms p50.
The store holds 45.6 GB for the thousand, 45.6 MB a tenant.

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

## Pending

- The same registry walk at a million entries and against real S3, which is the NFR-20 number rather than its smoke.
- Lazy hydrate, then a fleet rerun, since the churn tail above is entirely eager attach.
- Real S3 and S3 Express One Zone runs from an in region box, plus GCS and R2.
- Concurrent producer runs to show batching amortization.
- MinIO rerun on the storage v2 layout once it exists, the v1 rerun above showed the fold cost is structural.
- pgbench scale 1000 and the TPCC shape from the M1b checklist.
