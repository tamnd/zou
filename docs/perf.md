# Performance

First numbers for the storage engine under pgbench, published so every later change has a baseline to beat.
Run any leg yourself with `scripts/zou-bench.sh <target> [scale] [seconds]`, which does a fresh initdb, bootstraps the target, loads pgbench at the given scale, forces a checkpoint, and runs a tpcb-like and a select-only workload.
Each phase prints what it cost the store underneath its own tps, out of the counters between that phase's start and its end, because a tps on its own says which bargain suited the scenario and not what the bargain was.
The counter file is copied at every phase boundary into the run directory, so `zou stats <file> --since <earlier>` can ask a finished run anything those lines left out.
Each phase also prints what it cost the machine, memory and cpu, sampled once a second by `scripts/zou-usage.sh` while the run happens, because a peak that happened during the load is not a peak the select-only phase paid for and neither figure can be recovered once the run is over.
What is inside the boundary those two figures draw, and what a Neon number has to be split into before it can sit beside them, is [docs/resource-accounting.md](resource-accounting.md).
Each phase prints its latency tail too, p50 through p999 and the max, out of pgbench's per transaction log rather than out of its summary, because the summary offers a mean and a mean is the one statistic an engine on object storage can pass while being unusable.
A run on server2 makes the point: select-only averaged 0.492 ms with a median of 0.118 ms, a p99 of 6.950 ms and a worst transaction of 530.706 ms, so the average describes no transaction that happened.

## Method

- Postgres 18.4 with the zou patches, stock initdb settings plus `io_method=sync` and `full_page_writes=off`, the same configuration the CI smoke uses, so the table measures zou and not tuning.
- pgbench scale 100, about 1.3 GB of data across 10 million accounts rows.
- 8 clients, 8 threads, 60 seconds per workload.
- Every commit is durable on the object store before pgbench sees it acknowledged, there is no local WAL fallback to hide behind.
- The simulated S3 leg is MinIO plus `ZOU_STORE_DELAY=get=15,put=25,list=40`, which sleeps that many milliseconds inside every store call, matching typical S3 Standard service times. It stands in for real S3 until a real bucket run replaces it, and its numbers are labeled simulated.
- `ZOU_STORE_SIM=s3-standard` is the successor to the fixed delay: per provider profiles (s3-standard, s3-express, r2, gcs, b2, wasabi) sample each call from a p50/p95/p99/max curve, charge transfer time on the bytes moved, and emulate 503 SlowDown rounds with the real backend's backoff schedule. A calibration file measured by the zou-bench probe replaces the built in numbers, and everything produced under it stays labeled simulated.
- The simulated leg loads its data with the delay off and restarts the server with the delay on for the timed runs, because the load phase serializes hundreds of thousands of extends and would spend hours measuring nothing but the injected sleep. Its init cell is marked accordingly, and its timed runs start from a cold cache where the other columns run warm from the load.

## Hardware

Every box these numbers come off, dated, with its disk and a measured latency and bandwidth probe to its store, is in [docs/hardware.md](hardware.md).
`scripts/zou-bench.sh` prints the row at the top of every run, so a result and its machine travel together whether or not anybody remembers to look there.

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

## Cold attach to first query

The other number a storage engine on object storage lives or dies by, and the one issue #36 asks for: how long from nothing but a store to a query with an answer in it.
`scripts/zou-cold-attach.sh <label>` measures it, and the scenario it measures is deliberately the bad case.
It builds a pgbench database at scale 25, checkpoints, runs 20 seconds of four client load, and kills the postmaster with a 9, which leaves the store holding a WAL tail past its last checkpoint the way a crashed node does.
That store is kept pristine and every leg attaches from a fresh copy of it, because an attach is a write and the second attach of a store is not a cold one.

Measured on gamingpc, a 13th Gen Core i9-13900K, 32 threads, 31 GB of RAM, WSL2 Ubuntu on NVMe, with a 1.6 GB store and shared_buffers held down to 32 MB so recovery faults pages rather than holding the database in the pool.
The MinIO leg is a real MinIO on the same box reached over HTTP with SigV4, so it carries a real object store client and no network distance.

| phase | local fs | MinIO | s3-standard profile (simulated) | real S3 |
|---|---|---|---|---|
| zou-restore | 0.16 s | 0.78 s | 28.27 s | pending |
| crash recovery to ready | 26.32 s | 188.96 s | 552.43 s | pending |
| first answered query | 0.04 s | 0.10 s | 4.34 s | pending |
| attach, total | 26.52 s | 189.83 s | 585.04 s | pending |

The target is 500 ms and the closest leg misses it by 53 times, so what follows is the gap analysis the issue asks for rather than a victory.

The store op counters say where it all goes, and it is not the restore.
Recovery of this tail does about 16,100 page gets and 20,600 page puts, one at a time, and both counts scale with the length of the WAL tail rather than with the size of the database.
On local fs a get has a p50 of 8 microseconds and the whole thing still takes 26 seconds, which is the shape of the problem: it is not the per operation latency, it is that there are 37,000 operations in a line.
MinIO multiplies that by a real client, a p50 of 1 ms per get and 32 ms per put, and lands at 190 seconds.
The s3-standard profile at a p50 of 32 ms per get pushes it to 585.

Three things follow, and they are all engine work rather than tuning.
Recovery reads a page, changes it, and writes it back, which is why the puts outnumber the gets, and nothing about a crash recovery requires those pages to be durable individually before the cluster is up.
The reads are issued one at a time by redo, which knows every block it is about to touch and could ask for all of them at once.
And the tail itself is the multiplier, so folding more often is worth more here than anywhere else, since it is what decides how many records recovery has to replay at all.
The buffered WAL tier in the storage redesign is aimed at the same term.

Two legs of this are still owed: real S3 waits on credentials, and a run from a box at a real network distance from the bucket is what turns the simulated column into a measured one.

## The write leg, one build against another

The third measurement here is not a table of absolutes but a comparison: a change to the write path, before and after, on the same box.
`scripts/zou-write-ab.sh --before <prefix> --after <prefix>` is that harness, and it is a separate script from `zou-bench.sh` because the thing that makes an A against B trustworthy is not the workload, it is the schedule.

The workload is one insert of 250 rows a transaction into a two column table with no index, sixteen connections, forty five seconds, which is the shape the relation extension path shows up in.
Three legs, all on the same postgres binary and the same settings: vanilla with nothing to point the store shim at, zou over a filesystem store, and zou over an object store.
Each prefix is a full install with a `zou-bootstrap` next to it, built by whoever is running the comparison, because the store code runs inside the postmaster and a `cargo build` that never went through `ninja install` measures the old one and reports that the change did nothing.

What the script does that a pair of runs does not is alternate.
Both sides of a leg are measured inside the same round, the order they go in swaps every round, and three rounds are reported as min, median and max rather than as a mean.
That is a direct answer to how the numbers in issue #476 went wrong: they were taken on a box that was also running its owner's crawler, and four runs of the same binary against MinIO came back 553, 515, 454 and 264 tps, so a single before against a single after was reading the box rather than the change.
The spread is the finding and is printed first for that reason.

None of this makes a busy box quiet, and the harness cannot tell that it is on one.
The numbers this is meant to produce are still owed, and they are owed on a machine with nothing else running.

## Reading the numbers

- The load phase is extend heavy: every new block is a foreground round trip to the store, which is why init time grows with store latency and why bulk load batching is the obvious next optimization.
- tpcb-like commits gate on group commit to the store, so their latency floor is one store round trip amortized across the group, and every cache miss during a write workload pays a freshness barrier LIST of the wal tail.
- select-only reads come from the RAM cache and checkpoint range reads, and with no writes advancing the durable LSN the barrier is skipped, so it mostly measures the read path.
- select-only lands below tpcb-like in the local fs column because it runs after the write leg, which leaves thousands of unfolded wal objects behind (7,341 in this run), and every cache miss searches that tail for newer page versions. Tighter folding cadence is the fix and is tracked in the M1b milestone.
- The MinIO column splits the story: reads hold parity with local fs because the cache and the tail search dominate either way, but tpcb-like drops to 16 tps because every commit is an HTTP PUT and every barrier is a paged LIST over a tail of thousands of objects. Folding cadence and a start-after LIST are the two levers, both tracked in M1b.
- Benchmarking this table found and fixed a real bug: the local fs backend listed by walking the whole store and filtering afterward, which cost every barrier a 200k file walk and held tpcb-like at 2 tps. Walking only the prefix subtree brought the accounts update from 610 ms to 17 ms.
