# Performance

First numbers for the storage engine under pgbench, published so every later change has a baseline to beat.
Run any leg yourself with `scripts/zou-bench.sh <target> [scale] [seconds]`, which does a fresh initdb, bootstraps the target, loads pgbench at the given scale, forces a checkpoint, and runs a tpcb-like and a select-only workload.

## Method

- Postgres 18.4 with the zou patches, stock initdb settings plus `io_method=sync`, the same configuration the CI smoke uses, so the table measures zou and not tuning.
- pgbench scale 100, about 1.3 GB of data across 10 million accounts rows.
- 8 clients, 8 threads, 60 seconds per workload.
- Every commit is durable on the object store before pgbench sees it acknowledged, there is no local WAL fallback to hide behind.
- The simulated S3 leg is MinIO plus `ZOU_STORE_DELAY=get=15,put=25,list=40`, which sleeps that many milliseconds inside every store call, matching typical S3 Standard service times. It stands in for real S3 until a real bucket run replaces it, and its numbers are labeled simulated.
- The simulated leg loads its data with the delay off and restarts the server with the delay on for the timed runs, because the load phase serializes hundreds of thousands of extends and would spend hours measuring nothing but the injected sleep. Its init cell is marked accordingly, and its timed runs start from a cold cache where the other columns run warm from the load.

## Hardware

- Apple M4, 10 cores, 24 GB RAM, local NVMe, macOS.
- MinIO runs in a podman container on the same machine, so its numbers include the real HTTP client and SigV4 signing but no network distance, which is exactly what the delay leg adds back.
- Real S3: pending credentials, the column fills in as soon as a real bucket is available.

## Results

| phase | local fs | MinIO | MinIO + delay (simulated S3) | real S3 |
|---|---|---|---|---|
| pgbench -i -s 100 | 2899 s | 2377 s | loads undelayed | pending |
| tpcb-like tps | 189 | 16 | pending | pending |
| tpcb-like latency avg | 42.3 ms | 503.8 ms | pending | pending |
| select-only tps | 167 | 172 | pending | pending |
| select-only latency avg | 48.0 ms | 46.4 ms | pending | pending |

## Reading the numbers

- The load phase is extend heavy: every new block is a foreground round trip to the store, which is why init time grows with store latency and why bulk load batching is the obvious next optimization.
- tpcb-like commits gate on group commit to the store, so their latency floor is one store round trip amortized across the group, and every cache miss during a write workload pays a freshness barrier LIST of the wal tail.
- select-only reads come from the RAM cache and checkpoint range reads, and with no writes advancing the durable LSN the barrier is skipped, so it mostly measures the read path.
- select-only lands below tpcb-like in the local fs column because it runs after the write leg, which leaves thousands of unfolded wal objects behind (7,341 in this run), and every cache miss searches that tail for newer page versions. Tighter folding cadence is the fix and is tracked in the M1b milestone.
- The MinIO column splits the story: reads hold parity with local fs because the cache and the tail search dominate either way, but tpcb-like drops to 16 tps because every commit is an HTTP PUT and every barrier is a paged LIST over a tail of thousands of objects. Folding cadence and a start-after LIST are the two levers, both tracked in M1b.
- Benchmarking this table found and fixed a real bug: the local fs backend listed by walking the whole store and filtering afterward, which cost every barrier a 200k file walk and held tpcb-like at 2 tps. Walking only the prefix subtree brought the accounts update from 610 ms to 17 ms.
