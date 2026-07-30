# Benchmarks

Recorded baselines for the commit latency harness.
Reproduce with `cargo run --release --example commit_latency`.

## Commit latency

The harness measures append-to-ack for 2000 sequential 128 B commits after 200 warmup commits, one producer, defaults everywhere (2 ms flush interval, 512 KB flush bytes).
A sequential single producer pays the full flush interval on every commit since no other producer fills the batch, so these numbers are the worst case for the batching design and real concurrent workloads amortize better.

All tiers currently run against the local filesystem backend, so the numbers measure the pipeline itself: batching wait, framing, lz4, sha256 versioning, fsync, atomic rename.
Express and Buffered are interface stubs in M1 and behave like PureS3 by design, which the near identical numbers confirm.

2026-07-30, MacBook (Apple Silicon, macOS 15, APFS), rustc 1.97.1, release profile:

| tier | p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- |
| pure_s3 | 6.8 ms | 7.6 ms | 8.2 ms | 13.4 ms |
| express | 6.8 ms | 7.6 ms | 8.7 ms | 12.9 ms |
| buffered | 6.8 ms | 7.6 ms | 8.2 ms | 12.8 ms |

Roughly 4 to 5 ms of the p50 is fsync on APFS, the 2 ms flush interval accounts for most of the rest.

## Pending

- Real S3, S3 Express One Zone, MinIO, GCS, and R2 baselines when those backends land in this milestone.
- Concurrent producer runs to show batching amortization.
- pgbench numbers once the Postgres integration exists.

Targets from the M1 exit checklist: pure s3 under 25 ms p50, express under 10 ms p50, buffered under 2 ms p50, all to be measured against real backends.
