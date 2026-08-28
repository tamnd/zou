# Hardware

Every number in [docs/perf.md](perf.md) and [docs/benchmarks.md](benchmarks.md) came off one of the machines below.
A tps is a number about a pair, a box and a store, and the pair is the half a result table leaves out: eight shared cores against a MinIO on the same disk as the WAL and thirty two quiet ones against a bucket are the same column and not the same measurement.
So the pair gets written down, dated, because a box is not the same box six months later and a result read a year from now should say which box it was.

Rows are produced by `scripts/zou-hardware.sh [path] [store-target]` and `scripts/zou-bench.sh` stamps one at the top of every run, so a result and its machine travel together whether or not anybody remembers to look here.
The path decides which disk gets reported, since a store on a spare spindle and a store on the root disk are two different machines as far as a result is concerned.

## The boxes

Measured 2026-08-28.
Load is the one minute average at the moment of measurement and is here because these are shared machines, not a lab: server1 is a kubernetes worker running its owner's workload, server2 and server3 run their owner's jobs alongside zou, and gamingpc runs postgres instances, an inference engine, and docker containers of its own.
A run on a box at load 25 is a run at load 25 and the table says so rather than letting the number pass for a quiet one.

| box | host | cpu | cores / threads | memory | store disk | free | os |
| --- | --- | --- | --- | --- | --- | --- | --- |
| mac | USERnoMacBook-Air | Apple M4 | 10 / 10 | 24 GB | /dev/disk3s1 apfs, 460 GB | 15 GB | macOS 15.8, Darwin 24.6.0 |
| server1 | doge-01 | AMD EPYC (with IBPB) | 4 / 4 | 6 GB | /dev/sda3 ext4, 391 GB | 213 GB | Ubuntu 24.04.4, Linux 6.8.0-101 |
| server2 | vmi3112167 | AMD EPYC (with IBPB) | 6 / 6 | 12 GB | /dev/sda1 ext4, 193 GB | 32 GB | Ubuntu 24.04.4, Linux 6.8.0-136 |
| server3 | vmi3391933 | AMD EPYC (with IBPB) | 8 / 8 | 23 GB | /dev/sda1 ext4, 387 GB | 237 GB | Ubuntu 24.04.4, Linux 6.8.0-106 |
| gamingpc | GamingPC | Intel i9-13900K | 16 / 32 | 31 GB | /dev/sdd ext4, 1007 GB | 386 GB | Ubuntu 26.04 on WSL2, Linux 6.18.33.2 |

Loads at the time of the reading: server1 25.45, server2 15.51, server3 15.32, gamingpc 8.10.

Notes the table does not hold:

- server1, server2 and server3 are virtual machines. Their disks report no model at all and answer 0 for rotational whatever is underneath them, which is what a virtio disk says and not a fact about the hardware, so nothing here claims to know their media.
- server1 has 6 GB of memory with most of it spoken for, no passwordless sudo, and a standing load in the twenties. It is a box to read numbers off, not to build on.
- server2 and server3 mount with `discard`, server1 without it.
- gamingpc is WSL2, so its ext4 sits inside a virtual disk file on the Windows host and its kernel is Microsoft's. Its i9 has 8 performance and 8 efficiency cores and zou work is pinned to `taskset -c 8-23` so it stays off the cores the owner's jobs use. Builds run under `nice -n 15` for the same reason.
- The mac is the development machine. Full workspace builds and every published benchmark run on the servers, not here.

## Distance to the store

The other half of the pair is how far the store is, which is two numbers and not one.
A page read and a manifest swap are each one small object, so what they pay is round trip time.
A layer fetch and a checkpoint upload are bytes, so what they pay is bandwidth.
`zou probe <target>` measures both through the same `CasStore` client the engine uses, which means the signing, the http client, the retries and the connection reuse are all in the number rather than around it.

```
$ zou probe /home/zoubench/ab671-pool
target: /home/zoubench/ab671-pool
latency, 8.0 KiB x 30: put p50 4.8 ms p95 5.4 ms, get p50 9 us p95 18 us, list p50 25 us p95 41 us, delete p50 75 us p95 273 us
bandwidth, 8.0 MiB x 3: put 329.3 MiB/s p50 25.1 ms, get 1.6 GiB/s p50 5.1 ms
```

`--rounds`, `--size` and `--large` change the shape of the probe and `--json` prints it for a harness.
It writes under `probe/<pid>/` and deletes what it wrote, in a cleanup pass that runs before any error is returned, so a probe that fails halfway still leaves nothing behind.

Measured 2026-08-29, 8 KiB objects over 30 rounds and 8 MiB objects over 3, each box against a directory store on its own store disk, which is the target the pgbench legs in docs/perf.md use.
There is no real S3 or S3 Express row yet because there are no credentials for one, and the moment there are, the same command fills it in.

| box | put p50 | put p95 | get p50 | list p50 | delete p50 | put bandwidth | get bandwidth | load at probe |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| mac | 3.9 ms | 8.0 ms | 26 us | 78 us | 142 us | 770.7 MiB/s | 1.8 GiB/s | quiet |
| server1 | 11.7 ms | 51.1 ms | 39 us | 114 us | 163 us | 280.3 MiB/s | 580.7 MiB/s | 25 |
| server2 | 1.9 ms | 14.8 ms | 30 us | 119 us | 201 us | 115.6 MiB/s | 209.0 MiB/s | 27 |
| server3 | 7.5 ms | 48.8 ms | 51 us | 165 us | 227 us | 42.9 MiB/s | 93.8 MiB/s | 33 |
| gamingpc | 4.8 ms | 5.4 ms | 9 us | 25 us | 75 us | 329.3 MiB/s | 1.6 GiB/s | 8 |

Reading the table, since these are not the numbers a network probe would give:

- A put costs milliseconds and a get costs tens of microseconds on every box, which is the fsync and nothing else. The put is the durable one, the get comes back out of page cache. That two order of magnitude split is the shape a commit path has to live with, and it is why the commit latency work in docs/benchmarks.md is all about how many puts a commit costs rather than how fast one is.
- The p95 column is the load column in disguise. The three servers were probed in the twenties and thirties and every one of them has a put p95 four to eight times its p50, gamingpc at load 8 has a p95 within 15% of its p50. Nothing about the disks explains that and everything about the neighbours does. It is also why the load in this table is the load at probe time rather than the one in the specs table above, which was read at a different moment.
- server3's bandwidth is a tenth of gamingpc's on paper. It was measured at load 33 with the box's owner running an ingest job on the same spindle, and a rerun with the disk quieter is what would say what the hardware can do. The row is published as measured, with the load beside it, rather than waited on.
- The mac and gamingpc get bandwidth over a gigabyte a second because an 8 MiB object just written comes back from page cache. That is a real number for a store that fits in memory and a fiction for one that does not, which is the other reason a directory store is not a stand in for a bucket.

None of this is the distance to a real object store, which is the number the probe exists for. These rows are the floor: whatever a bucket costs, it costs at least this much of local work on top.

## Regenerating a row

```
scripts/zou-hardware.sh /path/where/the/store/lives
```

With a probe, which needs a release `zou` on that box:

```
cargo build --release -p zou --bin zou
scripts/zou-hardware.sh /path/where/the/store/lives s3://bucket/prefix
```

And inside a benchmark run, where it is on by default without the probe because a probe writes:

```
ZOU_BENCH_PROBE=1 scripts/zou-bench.sh /path/to/store 10 300
```

`ZOU_BIN` points the script at a `zou` that is not in `target/release`.
