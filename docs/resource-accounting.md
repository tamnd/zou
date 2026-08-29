# Resource accounting

A memory number for a database is not a number until it says what was inside the boundary.
Postgres alone, postgres plus a page service, postgres plus a page service plus the kernel cache holding the whole store, and a self hosted stack of six containers are four different answers to the same question, and the difference between them is larger than the difference any of our patches make.
So this page fixes the boundary first and the measurements come after.

It is the method behind the resource lines in [M1b](https://github.com/tamnd/zou/issues/31), which claim the shim's overhead is under 256 MB, that total zou memory is under a tenth of the Neon stack's share, and that cpu per transaction is within 20 percent of vanilla.

## What is measured, and with what

`scripts/zou-usage.sh` samples once a second while a run happens and `scripts/zou-bench.sh` starts it automatically, so every phase of every run prints what it cost.
Neither figure can be recovered afterwards, which is why they are sampled rather than read off at the end.

Memory is Pss, the proportional set size, and not the sum of RSS.
Postgres is a process per connection over one shared memory segment, so summing RSS across eight backends counts `shared_buffers` eight times and answers a multiple of the truth.
Pss divides each shared page by the number of processes mapping it, so the total is what the tree actually occupies.
Pss is Linux only, so runs on other systems report the sum of RSS and say that is what they are reporting, which is one more reason every published run happens on the servers in [docs/hardware.md](hardware.md).

Cpu is the live tree's user and system time plus the postmaster's counters for the children it has already reaped.
Both halves are needed and they do not overlap: a backend is either alive in the tree or reaped into the child counters, and it moves from one to the other at the moment it exits.
Per transaction figures divide by pgbench's own count of transactions rather than by tps times duration, since the two differ by however long the last transaction took.

## The zou boundary

Everything in the postgres process tree, the postmaster included.
The page service is a background worker started by the postmaster, not a separate daemon, so it is inside the boundary without anything special being done about it, which is the point of putting it there.

Excluded, and why:

- The kernel page cache. It is the kernel's memory, it is reclaimed under pressure, and a directory store that fits in RAM is being served out of it. Counting it would charge zou for the size of the box.
- The store's bytes on disk. That is footprint, tracked separately in the disk capture, and adding it to a memory figure would compare a working set with an archive.
- pgbench itself. It runs on the same box in every leg, so it is a constant that belongs to none of them.

The shim's own overhead, which is the line under 256 MB, is the zou tree minus the vanilla tree on the same box, at the same scale, with the same settings, in the same script.
It is a subtraction and not a reading, because postgres would use most of that memory whether or not the store existed.

## The vanilla leg

The same postgres binary with nothing to point the store shim at, so it writes its own files.
`scripts/zou-bench.sh none <scale> <seconds>` runs it, with the same initdb settings, the same phases and the same sampler as any other leg.
The environment is unset rather than left alone, because a leg that is supposed to be vanilla and inherited a `ZOU_TARGET` from the shell that launched it is the one wrong measurement nobody would catch by reading the output.

## The Neon boundary

Neon is not one process, so it does not have one number.
The self hosted compose stack is a compute node, a pageserver, three safekeepers, a storage broker and a MinIO, and only the compute node is per tenant.
That splits the accounting in two:

- Per tenant: the compute node, measured the same way as the zou tree, Pss over its process tree.
- Shared: the pageserver, the safekeepers, the broker and the object store, measured the same way and then divided by however many tenants the stack is carrying.

A single tenant benchmark charges the whole shared half to one tenant, which is the worst case for Neon and has to be labeled as one.
The interesting number is the shared cost per tenant at a tenant count worth serving, which is what the idle tenant soak scenario exists to produce, and it is the shape of the comparison M1b's tenth of Neon line is asking for.
Container overhead is counted where a leg runs in containers, because that is what running it that way costs.

Managed Neon has nothing to point `ps` at.
Its shape comes from public documentation, compute per endpoint, a sharded pageserver fleet, three safekeepers per timeline, a shared proxy and a shared control plane, and any per tenant share derived from it is an estimate.
Every figure produced that way is published with the word estimated on it, the same way the store latency simulation labels its runs simulated.
A measured self hosted stack and an estimated managed one are not the same evidence and this page will not let a table pretend otherwise.

## Reading a result

One phase of a real run, zou on server3 with the page service on, scale 1:

```
select-only, 4 clients, 25 s: 7206 tps, 0.554 ms average latency
  pss peak 132.0 MiB, end 131.9 MiB, cpu 23.4 s user 5.8 s sys over 18 samples, 0.162 s cpu per 1000 transactions
```

The peak and the end are both there because they answer different questions.
A peak that happened in the first second of a phase is a spike to explain, and an end that is well above the peak of the phase before it is a slope, which is what the long soak runs are watching for.
The sample count is there so a phase too short to be sampled properly is visible as such rather than passing for a quiet one.

Windows are cut out of one run's samples rather than the sampler being restarted around each phase, so the load phase's peak stays with the load phase.
The baseline for a window's cpu is the last sample before it, so the second between two phases belongs to one of them instead of to neither.
