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

## Pending

- Real S3 and S3 Express One Zone runs from an in region box, plus GCS and R2.
- Concurrent producer runs to show batching amortization.
- pgbench scale 100 through the full Postgres path, tracked on the M1 checklist.
