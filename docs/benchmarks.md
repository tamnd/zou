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
| zou v1, minio store | 6.5 * | 1238 ms * | 311.7 | 25.7 ms | 3485.6 s |

Reading the table: zou select-only on localfs is 3x vanilla and 22.7x Neon on identical hardware, because the page cache serves every read locally while the store only sees the write stream.
tpcb on localfs is 3.2x Neon and 0.7x vanilla, the remaining gap to vanilla is commit ack latency.

The starred MinIO tpcb row is published deliberately.
Steady state ran 90 to 110 tps at 67 ms p50 and 199 ms p99, then the wal pusher began a full checkpoint fold, and in the v1 pusher folding and segment pushing share one loop, so the final 8 transactions waited 790 s for commit acks and were released the second the fold completed, dragging the 60 s run average to 6.5 tps.
The MinIO select-only row shows a second problem: 25.7 ms per point read means the store is on the read path where the localfs leg answers from the local cache at 0.63 ms.
Both issues are tracked for perf spec 006 and the MinIO group reruns after the fixes, replacing this row.

## Pending

- Real S3 and S3 Express One Zone runs from an in region box, plus GCS and R2.
- Concurrent producer runs to show batching amortization.
- MinIO rerun after the concurrent fold and page cache fixes from perf spec 006.
- pgbench scale 1000 and the TPCC shape from the M1b checklist.
