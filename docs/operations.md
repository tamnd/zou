# Operations

Operational contracts for running zou writers.

## Metrics, health, and logs

`zou dev --ops 9187` opens a second listener on loopback with `/healthz` and `/metrics` on it, and nothing else.
It is a second listener rather than two more routes on the api port because the api port has no routes of its own: under path routing the first segment of every url is a tenant ref, so a `/metrics` there would take a name a project could have had, and a scrape is the operational state of the node, which is not something to hand to whoever holds an anon key for one of the projects on it.
Point a scraper at the ops port and firewall it the way you would any admin port.

`/healthz` answers 200 and reads nothing.
A liveness check that touches a store or a database turns one slow dependency into a rolling restart of everything that depends on it.
There is no readiness endpoint, because readiness here would have to name a tenant: nothing is attached until a request asks for one, so a node with nothing attached is idle rather than unready.

`/metrics` is the Prometheus text format.
The process counts requests as `zou_http_requests_total{surface,status}` and `zou_http_request_seconds{surface}`, where surface is rest, auth, storage, realtime, functions or other, and never the tenant, because a label per project on a node running a thousand of them is a series count no scrape wants to carry.
The multi tenant path adds `zou_tenants_attached`, `zou_tenant_attaches_total{outcome}` and `zou_tenant_attach_seconds`, which is the cold attach NFR-12 puts a number on, and `zou_registry_lookups_total{result}`, which is whether the registry cache ttls are doing anything.
`zou_build_info{version}` is always 1 and is there to join on.

A subscription is not a request, so the surface counters say nothing about how live one is, and `zou_realtime_changes_total` counts the database changes that reached a socket with two histograms next to it.
`zou_realtime_commit_to_socket_seconds` is what an application feels, counted from the commit timestamp postgres wrote, which means it carries whatever a database on another machine disagrees with this one's clock about, and a clock behind the database's is counted as a zero rather than as a negative.
`zou_realtime_change_seconds` is this server's own share of the same interval, from the tap reading the change out of the slot to the frame going out, on one clock, and it is the one that says whether what moved was here.

`zou_realtime_stage_seconds{stage}` splits that share into the five parts a change passes through, so a number that moved says what to look at.
The tap is the round trip to postgres for the next batch, the decode is turning those messages into changes, the selection is asking who wanted each change and what each of them may see of it, the sending is the reader handing each finished payload to a queue, and the socket is one task waiting its turn and writing the frame.
They grow with different things: the selection with the subscribers on a table and the policies on it, the sending with the sockets owed a row, the socket with how many tasks the node is running, and the tap and the decode with none of those.
The first four are one observation apiece per batch, and the tap is only counted on a poll that came back with something, since an idle reader asks every hundred milliseconds and is told there is nothing.
The socket stage is counted per delivery instead, because it is the only one that happens once per socket rather than once for all of them, and away from the holder it carries the link crossing as well, since everything after the holder decided a row is what the stage is asking about.
The four batch stages add up to a cycle of the reader, and which of them is the largest share is the reading worth taking: the tap says the database is the limit, the selection says the policies are, the sending says the queues are.
Running the tap alongside the other two was tried and measured, since none of them wait on each other, and it made the cycle a third shorter and what a client waited half again longer, so the reader still does them one after the other.

These three do not share the bucket edges the request path uses, since a request that took five minutes has been given up on and a change on a node holding a hundred thousand sockets has not.
They run from a tenth of a millisecond to five minutes, five edges to the decade between a millisecond and ten seconds, so a quantile off the fan out is at worst two thirds high rather than double, and a p99 in the hundreds of milliseconds is a number rather than the nearest round edge.

`zou_realtime_sockets` and `zou_realtime_subscribers` are what a socket tier is sized on: how many sockets this node is holding, and how many of them asked for database changes.
Two numbers rather than one because they cost different things: a socket is a connection, a task and whatever the client has not read yet, and a subscriber is that plus a place in the change reader and a policy check per row that matches it.
On a fleet they do not add up to one machine's, and that is the honest shape rather than a rounding: a socket served away from the node holding its project is a socket there and a subscriber on the holder, because the row it may see is decided where the database is.
`zou_realtime_socket_tiers` is the third of them and is counted per project rather than per socket: how many projects this node is serving sockets for and does not write, which is one hub and one link each and the same again on the holder.
It goes down as well as up, so a node with a lot of projects moving through it reads as what it is holding now rather than as everything it has ever seen.

Store numbers come from the counter file `ZOU_STORE_STATS` names rather than from counters of this process, and a scrape folds that file in as it reads it.
That is deliberate: the file is shared memory, so the ops a postgres backend made in another process are in it, and counting in process would count the ones this process can see and miss the rest.
Set `ZOU_STORE_STATS=/run/zou/stats` and the scrape gains `zou_store_ops_total{op,class}`, `zou_store_bytes_total{op,class}`, `zou_store_errors_total{op}`, `zou_store_conflicts_total`, `zou_store_op_seconds{op}`, the read tier counters and `zou_commit_step_seconds{step}`.
Counts and bytes stay separate because that is how S3 bills.
The latency buckets are the file's own powers of two, folded in at each bucket's upper bound, so the buckets are exact and `_sum` is a ceiling.

The commit steps are the one metric here that is a decomposition rather than a count, and they exist because a commit that got slower is otherwise a number with nowhere to go.
`durable` is what a committing backend waited from the moment its WAL was handed to the pipeline, and the other six say where that went: `push` is the pusher's own loop between two appends, so it is WAL that was flushed locally and had not been picked up yet, `stage` is the append call with its encoding, `window` is how long the batch it joined stayed open, `dispatch` is from that window closing to its store call starting, which is the inflight bound when the bound is what binds, `put` is the store, and `ack` is what a window waited on the windows in front of it after its own put came back, because the chain acks in order and a segment behind a hole is not durable.
Six of them are sampled per batch and `push`, `stage` and `durable` per chunk of WAL, so the counts differ on purpose and only the latencies are comparable.
`push` is the one that has to be read with the others in hand, because a pusher with nothing to push is asleep and the sleep is in the number: on a project writing well under what the pipeline would take, `push` is hundreds of milliseconds and says only that the writes were far apart.
It is a cost when the pipeline is busy and the rest of the steps say so, and it is the clock when it is not.

Logs go to stderr, `RUST_LOG` filters them, and `ZOU_LOG_FORMAT=json` writes them as one json object per line with `ts`, `level`, `target`, `msg` and the source location.
An environment variable rather than a flag, because the thing that wants json is a container runtime, which sets environment and does not get to rewrite the command line of what it runs.
Output meant for scripts stays on stdout either way.

## Traces

`ZOU_OTLP_ENDPOINT=http://localhost:4318` turns traces on and is the whole switch.
With nothing set there is no exporter thread, no queue and no span built, so a deployment that does not collect traces pays a null pointer load per request.
The endpoint is a base url and `/v1/traces` is appended, the body is OTLP json over http, and any collector that speaks OTLP http reads it.

Context is W3C trace context, the `traceparent` header.
A request that arrives with one gets a span that is a child of the caller's, so a trace that started in a browser or at a gateway is one trace and not two, and a request that arrives without one starts a trace here.
A `traceparent` that cannot be parsed is treated as no header at all rather than as a bad request, since a broken tracing header is not worth costing a caller their answer.
A caller that sets the sampled flag to zero is believed and nothing is recorded for that request.

Each request is one server span named by method and surface, `GET /rest`, with `url.path`, `http.request.method`, `zou.surface` and `http.response.status_code` on it, and a 5xx marks the span as an error.
The query string is never exported, because `?apikey=<jwt>` is a spelling this server accepts and a span carrying it would mail credentials to a collector.
A cold attach is a child span named `attach`, carrying `zou.tenant` and the failure when it failed, which is the span that explains a slow first request to a project that was not up.

Spans leave on a thread of their own through a bounded queue, batched every five seconds or every 512 spans.
A collector that has stopped reading loses spans rather than slowing the server down, and the ones lost are counted as `zou_trace_spans_dropped_total`, so the gap is visible on the scrape rather than silent.
With json logs on, every line written while serving a request carries `trace_id` and `span_id`, which is what makes a slow trace open its log lines and a suspicious line open its trace.

## Writer lease

Exactly one node writes to a tenant at a time.
The lease lives in the manifest and every take, renewal, and release is a manifest CAS, so the object store is the only coordination dependency.
The default TTL is 15 seconds (`DEFAULT_TTL_SECS`).
The heartbeat renews at a third of the TTL with plus or minus 20 percent jitter, so 5 seconds nominal at the default, and drops to a quarter of that cadence while retrying transient store errors.
A clean shutdown calls `Heartbeat::detach`, which clears the lease from the manifest so the next writer attaches immediately instead of waiting out the TTL.
A renewal is one conditional PUT and no read: the holder already has the manifest it last swapped, so it swaps that one again and only reads when the condition fails, which is also how it learns whether the thing that changed the manifest was its own checkpoint publish or somebody taking the lease.

An attached project that is not being written backs off.
The TTL is a promise about how long a dead node's work stays unavailable, and a database that has taken no writes has no work to be unavailable, so after three quiet renewals the heartbeat writes a 300 second lease and renews it at a third of that instead.
That takes an idle attached project from 720 store requests an hour to 12, and the first WAL append puts the 15 second lease back before it returns, waking the heartbeat out of the long sleep rather than letting it finish, so a project taking writes has the failover time measured below and nothing else does anything different.
The number is chosen against the attach manager's idle budget rather than against a price list: a project this quiet is detached outright a quarter of an hour in, which releases the lease and takes the cost to zero, and the backoff only has to carry it that far.
The window it opens is worth naming.
A node that dies holding a backed off lease makes the next writer for that project wait out what is left of the 300 seconds instead of 15, and reads are unaffected because reads do not need the lease.
`ZOU_LEASE_IDLE_SECS=15` turns the backoff off for a deployment that would rather pay the requests, and any value at or below the 15 second TTL does the same.

## Clock skew bound

Lease expiry compares wall clocks on different machines: the holder writes `expires_unix` from its clock, a challenger compares against its own.
Correctness never depends on this comparison.
Every acquisition increments the epoch, WAL lands under epoch directories, frames carry the fence token, and readers reject stale epochs, so even a steal triggered by a wildly wrong clock leaves the old writer's post-steal work unreferenced and no acked commit is lost.
What clocks affect is availability: a challenger whose clock runs fast by S seconds sees the lease expire S seconds early and may steal from a healthy writer, forcing a failover.

The bound for stable operation is that renewals must land before any challenger sees the lease as expired, which with renewal at TTL/3 leaves roughly TTL times 2/3 minus S of slack for retries.
Keep the worst case clock skew across nodes under TTL/3, 5 seconds at the default TTL.
NTP or chrony disciplined hosts sit under 100 ms, which leaves the entire slack budget for object store hiccups.
If your environment cannot bound skew, raise the TTL rather than living with spurious steals, the cost is a longer failover wait after an unclean crash.

The other direction costs more and is quieter.
A holder whose clock runs fast by S seconds writes an expiry S seconds further out than it should, and every other node reads it.
Nothing fails, nothing is lost, and the tenant simply stops failing over for S seconds, which for a host that came up before NTP disciplined it can be an hour.
A node that reads a lease running out further ahead than its own clock plus the TTL can reach says so by name:

```
lease held by node-7 until unix 1798000000, 3595s further out than a 15s lease can reach from this clock, so one of the two clocks is wrong or node-7 runs a longer lease ttl than this node
```

A lease records the TTL it was written for as well as when it runs out, so the one case that is not worth reporting no longer is: a holder that has deliberately backed off on an idle project says so in the lease and reads as held, and what is left in this message is a genuine disagreement between the two numbers.
Both halves of that sentence are worth checking.
The usual cause is a clock, and `zou doctor` on each node reports what it can prove about its own.
The other cause is real: a node configured with a longer TTL than this one writes expiries this one cannot account for, and a rolling change to the TTL looks exactly like skew from the node that has not been restarted yet.
Neither is a safety problem and neither self heals, so the fix is the clock or the config, not a wait.
Once the holder is confirmed dead, `ZOU_LEASE_STEAL=1` on the node taking over is the deliberate failover, the same escape hatch as any other stuck lease, and it is safe here for the same reason it is safe there: the epoch bump fences the old holder whatever its clock says.

## Checking a store

`zou doctor <target>` runs the operations the engine depends on against a scratch prefix and reports what the backend actually did.
It is the thing to run before pointing a database at a bucket you have not used before, because the two ways a store can be wrong are both invisible under an ordinary smoke test.
A backend that takes every conditional write passes reads and writes all day and loses a manifest the first time two nodes race, so the check that matters is not that a compare and swap succeeds but that a stale one is refused.
A backend that answers a range request with the whole object is correct by the letter of the protocol and turns every page read into a full object fetch, so the range check asks for 256 bytes from the middle and compares them.

```
zou doctor s3://acme-zou/prod
zou doctor s3://acme-zou/prod --tenant acme --samples 50 -o json
```

The checks are the prefix listing, a create, a read back with a byte compare, the range read, a swap against the current version, a swap against a stale version that must be refused, a second create that must be refused, the written key appearing in its own listing, a latency probe, the clock, and a cleanup that deletes everything and lists the prefix again to confirm it went.
Nothing is written outside `doctor/<random>/`, no lease is taken, and no tenant prefix is touched, so it is safe against a store with a live database in it.
Any failed check exits non-zero.

The clock check can only see one direction and says so.
A manifest carries the second its holder wrote it, so a manifest dated in the future means this node's clock is behind the writer's by at least that much, which is the case that shortens a lease this node takes.
A manifest dated in the past is either old or written by a clock this node is ahead of, and nothing in the store separates them, so the check reports the skew it can prove and stays quiet about the rest.
For the bound that matters and what to do when you cannot hold it, see the section above.

## Checking a database

`zou doctor` asks whether a store could hold a database.
`zou check <target> [ref]` asks the other question, which is whether the database already in one still reads.

```
zou check s3://acme-zou/prod
zou check s3://acme-zou/prod yesterday
```

It restores the ref into a temporary directory that goes away on the way out, then reads every ordinary table of every database that allows connections with `select count(*)`, one table per session so a refusal names the table it came from.
Index only scans and bitmap scans are off, because a count answered out of an index would say nothing about the heap it was counting.
There is no server in it: the SQL runs through the single user backend, which reaches a first answer in about a third of the time a postmaster takes and does not have to be waited for or shut down.
A table that could not be read is printed with what postgres said and the command exits non-zero.

```
checking /srv/store at ref local
restored 44 files and replayed 5 wal records
  postgres.public.empty 0 rows
  postgres.public.t refused: ERROR:  invalid page in block 5 of relation "base/5/16384"
zou: 1 of 2 tables could not be read out of /srv/store
```

Attaching to read the database is still an attach, so the backend takes the writer lease through the ordinary protocol and a tenant a server is serving right now refuses this with the lease error.
Run it against a branch or a ref nothing is serving, or stop the server first.

A target ending `.zou` works, which a postmaster over the same file does not, because the single file backend admits one process at a time and this is a chain of one process at a time: the restore, and then the backend.

One failure it cannot see, and the reason to keep the row counts rather than the ok line: a page that reads back as zeros is a page postgres accepts as empty, so a relation that lost one scans clean and comes up short by the rows that page held.
Comparing the counts of a check against the counts of the one before it catches that, and issue #546 is about closing it off in the read path where it belongs.

## Losing the lease

The heartbeat flips `lost()` when the manifest shows another holder or when the store refused renewals past the local expiry.
The group commit pipeline independently discovers loss on its next manifest publish and stops acking, see the ack ordering notes in commit.rs.
Treat `lost()` as a stop sign: keep running and every upload is wasted work in a dead epoch.
Recovery is a fresh `acquire`, which takes a new epoch and a new fence.

## Failover

A node that dies stops renewing, and that is the whole signal.
The lease runs out on its own, and the next node asked for the tenant finds an expired lease, takes it with the CAS that bumps the epoch, and attaches.
`lease::takeover` is the wait: it sleeps until the expiry the manifest names, with a tenth of a second of per node jitter so a rack of standbys does not fire one CAS storm, and it gives up after a caller supplied limit because a holder that is still renewing is alive and the caller deserves to be told who to ask rather than kept waiting.
It is not `steal`.
A node that cannot be reached from here may be perfectly alive and serving from somewhere else, and a partition is not a death, so the TTL is the one thing both sides agree on.

Recovery time is what is left of the TTL at the moment of death, plus one CAS, plus the attach.
Death lands between renewals, which at the TTL/3 cadence leaves between two thirds of a TTL and a full one, so with the default 15 second TTL expect 10 to 15 seconds before another node is the writer, and the attach on top of that.
Measured on server2, 6 vCPU, against the default 15 second TTL over 10 killed holders: min 12.1 s, p50 13.1 s, max 13.3 s against a local store, and min 12.2 s, p50 13.2 s, max 13.7 s against MinIO over the network.
The object store adds tens to hundreds of milliseconds to a wait measured in seconds, which is the point: recovery is the TTL and nothing else is hiding in it.
`cargo test -p zou-store --test failover -- --nocapture` reruns it, with `ZOU_FAILOVER_TTL` and `ZOU_FAILOVER_ROUNDS` to change the shape and the S3 test variables to point it at a real store.

A clean shutdown costs none of that.
`Heartbeat::detach` clears the lease, so the next node in finds nothing to wait for, measured at 9 to 15 ms.
Roll restarts through detach and a fleet of a thousand tenants does not spend a TTL per tenant.

RPO stays zero across all of it.
The holder that comes back finds its next renewal answered with `Lost`, its uploads sit in an epoch the live manifest never references, and no acked commit is lost.
Shorten the TTL to shorten the RTO, and read the clock skew bound above before you do, since the two are the same budget.

## Stopping a node

A stop is a Postgres fast shutdown and its order matters: backends first, then background workers, the wal pusher among them, and only then the shutdown checkpoint.
The pusher leaves in a state that owes nothing, it pushes until insert, flush, pushed and published all agree and asks for a checkpoint to fold before it goes, so the checkpoint that runs after it is writing pages whose WAL the store already holds.
The checkpointer itself stores no pages while the page service is on, since the eager puts are elided and pages come out of the stream, so it never waits for the pusher that is no longer there.
That is what keeps a stop from hanging, and before zou #468 it did hang: the checkpointer waited inside the shutdown checkpoint for a durable LSN only the departed pusher could publish, `pg_ctl stop` gave up, and the cluster state stayed "shutting down".

With the page service off the checkpointer does store its own pages and does owe the barrier, so a stop in that mode depends on the pusher having drained.
If it has not, a waiter gives up ten seconds after the pusher's exit with `zou wal has no pusher to make X/X durable`, the postmaster exits, and the next start replays its WAL tail.
That is a refusal rather than a failure to try: the store never received the WAL for those pages, and storing them anyway would leave the store holding a page that carries the effects of records it has never seen.
The same message from a committing backend is a PANIC, which is the answer Postgres gives to any WAL write it cannot make durable, and the restart that follows brings up a fresh pusher.

## Upgrading

A rolling restart means two releases are reading the same bucket for as long as it takes, and the old binary is the one at risk, because it meets objects written by something that knows more than it does.
What keeps that safe is that a writer emits the lowest format that carries what it wrote, so an upgraded node writing an ordinary tenant writes exactly what the nodes still on the old release already read.
Nothing about a deploy changes a format on its own.

What does change one is using a feature that needs it, and that is where a tenant stops being readable by the older binary.
Splitting a tenant into shards writes a manifest at format 3, and creating a branch writes that branch's shard manifests at format 2.
Both are the price of the feature rather than the price of the release, so a fleet mid rollout should finish the rollout before splitting or branching.

A node that meets a format it does not know refuses the object by name and says the binary is behind, and it never half reads one.
That matters because every one of these objects has fields that default: a newer object parses into something plausible and wrong, an empty roster or a manifest with no layers, and the format number is the only thing standing between that and a node serving it.
The message reads like `manifest format 4 is newer than this binary supports (3), upgrade zou`, and it is the same shape for every one of them.

The realtime link between a node and the tenant's holder carries its own version, and the two ends say it rather than guess.
Ends on different versions do not open a link, so broadcast for a tenant held by the other end does not cross until both are upgraded, and the node keeps retrying while that is true.

Two smaller things move under an upgrade.
The `ZOU_STORE_STATS` counter file is per layout, so a release that changes the layout starts the counters at zero rather than reading the old ones as if they meant the same thing, and a dashboard reading it sees a reset.
A single file `.zou` store says which way a mismatch goes: newer than the binary means upgrade the binary, older than it means the file has to be exported by the zou that wrote it, because no upgrade of this one will open it.

Going back a release is only safe while no tenant used a feature the older binary predates, which is the same rule read backwards.
The one case with its own message is a store that still carries a v1 per tenant WAL tail: fold it down with the previous binary first, since this one refuses it rather than guessing what the tail meant.

The rules are not left to reviewers.
`crates/zou-log/tests/upgrade.rs` holds a census of every durable and spoken format in the tree, checked against the source constant by constant and value by value, together with the frozen bytes of what a plain object of each writes.
A format that moves, or one that is added and not censused, fails there until somebody says what a binary already running does with it.

## Write forwarding

Any node in a fleet answers for any tenant, but only the lease holder writes one, so a node that is asked for a tenant it does not hold sends the request to the node that does.
Holder discovery is the manifest: the lease already records the node, and it now records that node's address as well, so finding the writer is a GET of an object every node reads anyway and there is no membership service to run or to be wrong.
The answer is cached for one second, which is under the renewal cadence and keeps a manifest read off the front of every request.
A tenant nobody holds is served here, because the attach that follows is what takes the lease, and if two nodes race for it the loser fails its attach rather than writing anything.

The forwarded request is the request that came in, with its method, path, query, headers, and body, `Host` included so the peer resolves the same tenant this node did.
Hop-by-hop headers are dropped in both directions, the node adds `x-zou-forwarded-by` with its own id, and a request that already carries that header is answered 508 rather than forwarded again, since one hop is a fleet working and two is a loop.
The body is buffered to forward it, so it is bounded at 64 MiB and a larger one is answered 413 with the advice to talk to the writer directly.
A peer that cannot be reached is a 502 whose body does not name the address, and a writer whose lease carries no address at all is a 503 that says so.

Stale reads are off by default.
On, a GET, HEAD, or OPTIONS for a tenant another node holds is answered from this node's own copy instead of being forwarded, and the answer carries `x-zou-stale-seconds` counted from the writer's last publish, which is the freshest state any other node could have.
That header is on stale answers and on no others, so its absence is the statement that the answer came from the writer.
A caller sends `x-zou-max-staleness` to override the node's choice for one request: a value the local copy is within is served here, anything else is forwarded, and `0` is how a client reads back what it just wrote.
POST is never treated as safe, even though `POST /rest/v1/rpc/<fn>` is often a read, because whether it is depends on what the function does.
A read forwarded is slower; a write served locally is data that never happened.

`zou_forwarded_requests_total{outcome="sent"|"failed"}` counts the hops, `zou_stale_reads_total` counts the answers served locally, and `zou_stale_read_seconds` is how far behind they were.
Each forwarded request opens a client span whose `traceparent` goes across, so a trace covers both nodes and the hop is a span in it.

## Serving a fleet

`zou serve <target>` is the node.
`zou dev` serves the one database in a store, which is what a laptop wants; this serves whatever is in `registry/`, which on a real deployment is a few hundred or a few thousand projects that are mostly asleep.
How many may be registered is a different question from how many may be attached, and the registry has been walked at a hundred thousand: the lookup is a point read that does not move between a thousand entries and a hundred thousand, and a node's own memory follows the cache bound rather than the fleet, see [benchmarks.md](benchmarks.md).

    zou serve s3://bucket/fleet --domain zou.example --ops 9187

Four listeners come up on one runtime: the http front door on 54321, the postgres port on 5432, the pooler on 6543, and the scrape wherever `--ops` says, off unless it is asked for.
One runtime rather than one each, because four thread pools on an eight core node compete for eight cores and the density this is built for is a number of tenants and not a number of servers.
They share the registry and the attach manager, so a project brought up by whichever door was asked first is the project the others find already running.
Any door except http is turned off by giving it port 0.

`--http 54321,54322,54323` puts the same api on more than one port, one accept loop per port and one router behind all of them, so a request cannot tell which port it arrived on and nothing about it should.
A node published at several ports is one reason to want it.
A load generator is the other: what one client address has to any one destination port is its ephemeral range, 28231 ports on a stock kernel and not the 65535 the arithmetic wants, and the ceiling does not arrive as a refusal but as a kernel scanning most of that range per connect under a lock.
So a run holding a great many sockets against one node spreads them over several ports on this side, which is how the 100k socket number in [benchmarks.md](benchmarks.md) was taken: six ports, and the generator's range widened, after three ports made the generator spend 70 percent of itself choosing source ports while the node sat at a fifth of its cpu.
The first port in the list is the one the node calls its own and builds urls from.

Routing is `--domain` and the path prefix, and at least one of them has to be on, which is checked at the command line rather than at the first request.
`--domain zou.example` makes `acme-prod.zou.example` a project, and it is also where a tenant's own external url comes from, so the links in its confirmation mail point at the project instead of at the node.
The path prefix is on by default and `--no-path-prefix` turns it off, for a deployment that has a wildcard certificate and does not want a second way in.

`--ref demo` is the other shape: one project, at every url the node answers, with the routing taken out rather than configured off.
Nothing is resolved per request, `--domain` has nothing left to name and is refused alongside it, and the project is attached before the http door starts accepting, so the first request waits in the accept queue for an attach instead of being the reason for it.
`ZOU_REF` sets it and `ZOU_TARGET` sets the store, for a platform that configures a container with variables rather than a command line.
That is the shape a function or a container per project wants, and the Lambda, Cloud Run and Fly recipes are in [serverless.md](serverless.md).

`--advertise http://10.0.0.4:54321` is what makes a node one of several rather than the only one.
Without it a node believes every project on the store is its own, which is exactly true when there is one node and is why it is the default.
With it the node publishes that address into every lease it takes, reads the lease before it attaches anything, and forwards a request for a project somebody else is writing to whoever is writing it instead of starting a second postmaster on the same data.
A socket for that project is not forwarded, because an upgrade is not a request and an answer: it is served here on this node's own hub with one link to the writer behind it, and nothing is attached or leased for it.
`--node iad-3` is what the node is called, which is the name it takes leases under and the name its peers see.
It defaults to the hostname, which is right for a box and wrong for a container that gets a new one every deploy, so anything scheduled should say it.
`ZOU_NODE_ID` and `ZOU_NODE_ENDPOINT` set the same two for a platform that configures with variables.
`--ref` and `--advertise` are refused together: one project on this node is the other answer to the same question, and the door `--ref` builds has no forwarding in it.
Reading a project this node does not hold, rather than forwarding it, waits on the lazy hydrate work ([#39](https://github.com/tamnd/zou/issues/39)), so there is no switch for it yet.

Nothing is running until a request names a project.
The first one for a cold tenant restores its runtime directory out of that tenant's own prefix and starts a postmaster on loopback with a private socket directory, and both are thrown away when it is let go of.
`--max-attached` is how many are up at once and `--idle-secs` is how long an untouched one stays up, both defaulting to what the attach manager uses, and the sweep that enforces the second runs on a timer at a quarter of it, because a node that has gone quiet is exactly the node with no requests to notice on.
Neither budget takes a project somebody is in the middle of using: a request holds its project until the answer is written and a postgres session holds its project until the client goes away, and a held project is passed over by both.
On a node whose working set is up against its ceiling that means the ceiling is briefly exceeded, which is the trade worth making, since the other answer is stopping a database under work the node has already accepted and the client reads that as `57P01` or `57P03` in the middle of a request.
The room comes back on the next attach, or on the next sweep if no attach arrives.
`--shared-buffers` is per tenant and defaults to 16MB, small on purpose: the ceiling multiplies it, and the store backed page cache is the tier that is supposed to be doing the work.
A node running a few large projects rather than a thousand small ones should raise it.

`--max-connections` is the same trade on the same axis and defaults to 40 per tenant.
Forty is one bank of the pooler at 20, the http door's own pool at 10, three postgres keeps back for a superuser, and a little room.
It is also an arithmetic ceiling on write rate: forty concurrent commits at twenty five rows a transaction is a thousand rows a second and nothing left over, so a node doing serious write volume for a few projects should raise it and a node packed with a thousand quiet ones should not.
Under the three numbers added up it is refused at the command line, since a project that cannot open the connections its own pooler wants is a project that stops rather than one that is slow.
A project that runs out answers `53300` with the postmaster's own `sorry, too many clients already` and then a sentence saying the ceiling is this flag, because the postmaster's message names no setting and the setting is not the operator's.

A postmaster that dies on its own detaches its tenant, so the next request attaches again instead of being routed at a database that is not there.
One that was asked to stop does not, because something already did.
Runtime directories are `<ref>-<n>` and never bare `<ref>`, so a detach followed immediately by an attach does not put a new postmaster in the directory the old one is still shutting down in.

One project has one postmaster at a time, and the node enforces it rather than assuming it.
Detaching does not wait for a shutdown, because the attach manager is holding its own lock while it evicts and a request that displaced somebody else's project has no business waiting on that project's shutdown checkpoint.
The next attach of that same project is what waits, which on a node that is churning is a wait that is already over by the time it is asked for.
Skipping it is not an option: two postmasters put the same tenant's pages into the same prefix, and a database restored out of that has an index and a heap that disagree, which shows up as `create role anon` failing on the unique index that says the role it could not find is already there.
A postmaster that has not gone within five seconds is asked for an immediate shutdown, and one that has not gone five seconds after that fails the attach instead of being started alongside.

SIGINT or SIGTERM stops every attached tenant with a fast shutdown and removes the tree; no data waits on that, since an acked write is durable on the store by definition.

Attach does not download the database.
A restore writes back a skeleton, the control file and the configuration and the WAL tail, and every relation page stays a store object until something reads it, so the size of the database is not what a cold attach costs.
What it costs is crash recovery, and recovery is a single process that reads one page per record it replays, so against a store thirty milliseconds away the bill is round trips.
That is why a restore is followed by a warm up: the WAL tail names the pages redo is about to change, so they are fetched in parallel into the tenant's page cache before the postmaster is started, and redo then reads them off local disk.
`scripts/zou-cold-attach.sh` is the measurement, on a simulated S3 with a pgbench database and a killed postmaster.
Redo went from 939.76 s to 246.70 s and the attach from 1103.52 s to 417.45 s at the 32MB pool a packed node gives a tenant, and with a pool that holds what recovery touches redo is 5.09 s against 939.50 s, because the 10,586 pages redo read one at a time became 14.
`ZOU_WARM_BLOCKS` caps how many pages it will fetch, 65536 by default, and zero turns it off.

An attach that is replaying is given as long as it keeps moving.
A postmaster has sixty seconds to say it is accepting connections, and one that is still in redo says where it has got to every ten seconds, so every report with a newer LSN than the last one starts those sixty seconds again, up to ten minutes for the attach as a whole.
Without that a project with an hour of WAL behind it is a project that cannot be attached at all rather than one that is slow to attach, because the postmaster killed at sixty seconds wrote nothing anybody would start from and its replacement begins at the same redo point with the same sixty seconds.
A redo that reports the same LSN twice is stuck rather than slow and is killed on the spot, and so is one that is still going at the ten minutes, since leaving it be would let the retry start a second postmaster on a project that already has one.

An attach that was given up on part way leaves its runtime directory behind, and the next attach of that project carries on from it.
Crash recovery takes no restartpoints, so redo does begin where it began before; what the next attempt does not do again is restore the skeleton, replay the WAL ahead of it, or read a single page the last attempt already pulled out of the store into the tenant's page cache.
On a remote store that is nearly all of the wall clock, and it is the difference between attempts that add up and seven identical attempts that all die in the same place.
The directory is only reused while the project's manifest is byte for byte the one it was set aside with, because the page cache is keyed by block and holds no LSN, so anything that moved the store, a fold, a checkpoint, a shard split, or another node taking the lease, throws it away and starts over.
A node holds at most eight of them at once and only for projects whose redo was moving when it was given up on; over that the newest is dropped rather than an older one, since an older one belongs to a project somebody is retrying.
The lines to look for are `keeping <dir> so the next attach carries on from it` and `carrying on from the attach that was given up on`.

What is left of a cold attach is the writes: recovery dirties every page it changed and the end of recovery checkpoint puts them back one at a time, which is where the rest of those ten minutes goes and is the storage engine's problem rather than this command's.

### Where a cold start went

A node says what it spent starting up, because a total tells nobody which step to go and fix.
Two lines carry it, both at info level, so a node with no collector pointed at it still has the answer in its log.

    up in 0.3 ms, arguments 0.1 ms, store 0.1 ms, doors 0.1 ms
    acme-prod: attached in 479.2 ms, wait 0.2 ms, restore 443.1 ms, warm 11.1 ms, spawn 0.9 ms, recovery 24.0 ms

The first is the binary, measured from the top of `main` to the four doors listening, and its laps add up to its total.
It is small because nothing is opened at start that a request has not asked for, the store lap included, which is a handle and not a read.
What it cannot see is the exec and the dynamic linker that ran before `main`, so a cold start timed from outside is always a few milliseconds larger, and on this laptop that difference is most of it, six milliseconds against a third of one.

The second is one project being brought up, printed once per attach with the ref in front of it.
`wait` is the previous postmaster for that same ref shutting down, normally nothing and occasionally the whole line on a node that is churning.
`restore` is the runtime directory being written back out of the tenant's prefix, `warm` is fetching the pages redo is about to read, `spawn` is the fork, and `recovery` is from the postmaster starting to the first connection it takes.
`initdb` appears instead of `restore` and `warm` the first time a ref is asked for, since there is nothing in the store to restore yet.

`RUST_LOG=debug` splits the restore in two:

    acme-prod: restored 20 files in 333.7 ms, replayed 1 wal records in 95.9 ms

The two halves fail differently, which is why they are counted apart.
The first is the skeleton, a fixed set of objects a project's size does not change, fetched in parallel, so a large number there is the store being far away or the objects being fat.
The second is the shared log read from the newest checkpoint's redo location to the end of the stream, so a large number there is a project that has written a lot since its last fold, and the fold cadence is the knob.
`scripts/zou-cold-start.sh` is the measurement, an exec to first answered request against a store on a directory or behind `SIM`, with the node's own breakdown printed under each run, see [benchmarks.md](benchmarks.md).

`zou serve` needs the patched postgres, `--pg-bin` or `ZOU_PG_BIN` pointing at the install, the same one `zou dev` uses.

### Large downloads out of the way

A node that keeps objects on S3 and serves them itself pays for every byte twice, once out of the bucket and once out of its own network, and holds a request open for as long as the download takes.
On a large file that is long enough to matter to everything else the node is doing, and none of it is work: the bytes arrive from the bucket and leave unchanged.
`--passthrough <size>` answers a download of an object at least that big with a 302 at a presigned url to the same object instead, so the bucket serves it and the node steps out of the egress path.

    zou serve s3://bucket/fleet --domain zou.example --passthrough 8MB

The size takes a unit, so `8MB`, `8M` and `8388608` are the same number, and it is off unless it is given.
It applies to a whole object and nothing else.
A HEAD is answered here as it always was, since it carries no bytes to save, and so is a range request, because the request that follows a redirect is not the one that asked and a client that wanted part of a file must not be handed all of it.
A store that cannot name its objects with a url, which is every store but S3, answers the way it always did whatever the flag says.

The url is signed for fifteen minutes, carries the content type and the download name the request asked for so the answer says what a download from the node would have said, and a url signed by `createSignedUrl` caps it further: a passthrough never outlives the url that asked for it.

What it gives up is worth knowing before turning it on.
The url outlives the request that made it, so an object deleted a second later is still readable until it expires, and the caller that follows it is talking to the bucket rather than to a permission check.
It is also visible: the answer is a 302 where upstream sends 200, which every client library follows and every recorded comparison notices, so the conformance suites run with it off.

## Where the sockets live

The default topology is inline, and a deployment that never thinks about this gets it.
A node holds the sockets of the projects it holds, in the same process that writes them, and there is no socket tier to install, no second binary and no mode flag.
A socket that arrives at a node which does not hold its project is served on that node's own hub with one link per project back to the holder, so what a node is being is decided per socket by who holds the lease rather than by how it was started.
`zou serve` does not build a fleet's forwarding yet ([#444](https://github.com/tamnd/zou/issues/444)), so today the command only ever serves inline and the link is exercised by tests and by an embedder that builds its own front door.

Inline is the default because the measurement says a socket is cheap and a link is not free.
One node held a hundred thousand sockets, every one of them subscribed to a table, at 23.4 KB of resident memory each and under a third of eight cores, while it was also the holder writing the project, on a box that was also running a MinIO and the owner's crawler, see [benchmarks.md](benchmarks.md).
So there is no socket count between there and here at which a node has to be split, and a node that is short of memory or cpu below that number is short of it for the databases it is attaching rather than for the sockets.

What a link costs, against that, is four things an inline node does not pay.

A link that drops is resumed rather than gapped, inside a window.
The holder keeps the last 1024 frames it sent down each link plus everything that link's sockets held, presence and subscriptions included, for 30 seconds; a node names its link and says which frame number it got to; and a link back inside both is handed what it missed, in order, with no socket closed and nothing rejoined.
Past either the resume is refused and the node is told so, and then it is a gap again, which closes every socket on that node.
Both numbers are in `crates/zou-server/src/fanout.rs` as `KEPT` and `GRACE` with the reasoning next to them, and both refusals name their numbers in the log.
What a resume does not restore is a subscription that was never announced: subscriptions live on the holder and are kept with the rest, but a node whose link could not be resumed re-announces its sockets and its topics and lets its sockets hear the gap and resubscribe, because a subscription asked for again comes back under new ids while the client is still holding the ones its join reply carried.
Catching up further back than the ring is waiting on the buffered WAL tier ([#39](https://github.com/tamnd/zou/issues/39)), because the change stream itself retains nothing.

The project's budget is the project's on every node.
Each node says its four numbers up its link once a second, sockets connected and joins, messages and presence events a second; the holder adds every node's up and answers with the project less that node's own share, and every refusal on every node is then that node's live numbers plus what it was last told.
So sockets at once, joins a second, messages a second and presence events a second are all one number across the fleet, and the three headers the http broadcast endpoints report are the project's whichever node answered.
The cadence is `TALLY` in `crates/zou-server/src/fanout.rs`, and a second of lag on meters averaged over a minute is not a number anybody can tell apart from the true one.
A node that cannot reach the holder goes back to refusing against its own numbers alone, because a node refusing every socket for as long as a partition lasts is a partition made worse rather than survived.
A message that crosses is counted once for the crossing and once for each socket it reached, which is one more than the same message costs inline.

A lease that moves while sockets are on it is noticed by the tier that was built from it, which rereads the lease every `MOVED` in `crates/zou-server/src/gateway.rs` and gaps its sockets when the answer is a different node.
Five seconds against a busy lease of fifteen that is renewed well inside that, so a handover is published inside one lease and noticed inside one of these, and the clients reconnect and land on the tier the new holder is behind.
The cost is one cached lease lookup per project this node holds sockets for, which is the lookup the front door already makes for every request for that project.

The same loop is what lets a tier go: nobody on it and nothing sent to it for `EMPTY`, fifteen minutes, and it is dropped along with the hub and the link behind it, which is one less linked node on the holder as well.
Fifteen is the number an idle attach gets and is set beside it for the same reason, a client that comes back weighed against holding the thing until it does, not because a tier costs what an attach costs.
An embedder that knows its own clients better can say otherwise with `fleet_keeping`.
Watch `zou_realtime_socket_tiers` for whether the sweep is keeping up on a node with a lot of projects passing through it.

A broadcast between two sockets on the same away node goes to the holder and comes back, because one ordering is the whole reason there is one link.

So the threshold for v1 is a rule rather than a socket count, since a socket count is not what the numbers found.
Run inline until one project's sockets would crowd the node that writes it, and what says they are crowding it is the node's own ops port: resident memory against the box, descriptors against the limit, and the tenant page cache being evicted for work that is not the database's.
A tenant with a thousand sockets and a busy table wants a bigger holder and not a socket tier.

Descriptors are the one limit to set before a socket count gets large.
A socket is a descriptor and the soft limit a shell hands a program is 1024 nearly everywhere, so a node that inherits it stops accepting at the thousandth socket and says `too many open files` for the rest of the run, whatever the realtime budget it was started with says it may hold.
The node raises its own soft limit to the hard one at startup and logs the number it got, so what an operator sets is the hard limit, `LimitNOFILE` under systemd, and the node takes it from there.

When a dedicated tier does become worth it, what makes one is where the load balancer sends the sockets and not how the node was started.
A node that is sent nothing but `/realtime/v1/websocket` for projects it does not hold attaches nothing, starts no postmaster and takes no lease, because everything a socket needs from the database crosses the link and comes back as an answer.
Such a node wants the same store and the same jwt secret as the rest of the fleet, since the link is authorized by the project's own secret and a socket's token is verified at both ends.
That is a fleet's routing decision, so it arrives with the forwarding in [#444](https://github.com/tamnd/zou/issues/444) rather than as a flag of its own.

## The page service

Reads that miss the local caches go to a page service running as a background worker inside the node, over a unix socket in the data directory, and it answers them out of the page layers it builds by replaying the tenant's durable WAL.
It is on by default, and the reason it is not a knob anyone should have to find is what happens without it.
The other path writes one 8 KB object per page write and reads pages back as objects, which is the thing the storage layer exists to stop doing.

Measured on the same box with the same binaries running the same scenario, the variable being the only difference:

| | on | off |
| --- | --- | --- |
| crash recovery | 4.3 s | 112.2 s |
| pusher drill | 3.3 s | 215.2 s |
| death drill | 82.9 s | 253.6 s |
| init | 54.9 s | 149.5 s |
| peak rss | 0.9 GB | 1.8 GB |

The run that started that investigation never finished at all: a backend sat twenty five minutes in the end of recovery checkpoint writing about 291 objects a second and the kernel took it before it was done.

`ZOU_PAGESERVE=0` turns it off, and `zou dev --page-service off` does the same for a dev node without touching the environment.
Off is worth keeping reachable, it is how the two paths get compared, and a comparison is how the numbers above exist.
The spellings are `1 0 true false on off yes no` in any case, and anything else is refused at startup rather than read as off, because a value nobody can parse is a mistake and answering it with the slow path is how a month of runs measured the wrong path.
initdb runs with it off in every command that runs one, since bootstrap is a standalone process with no service to talk to, and the redo workers never see it because they run with no store attached at all.

Off is only reachable on a store that has never had it on, and a node asked to do otherwise refuses to start rather than lets you find out later.
The service elides the eager put per page write, so the objects under `pg/` hold whatever they held when the first such session opened while the checkpoints keep advancing on the layers above them, and the manifest records that point as `pages_elided_from`.
Reading those objects again puts recovery at a redo location far past the pages it is applying records to: a heap record lands on a page whose line pointers it does not describe and the postmaster dies with `PANIC: invalid lp`, and a store that was shut down cleanly instead comes up serving a catalog from before that point, where a table created after it does not exist.
So a postmaster with the service off over a store carrying the mark stops before recovery with `zou store has no page objects past X/X to read`, naming that point and the newest capture taken since, and `zou dev --page-service off` says the same thing before it restores anything.
Comparing the two paths therefore needs a store that has only ever run with the service off, which is one initdb away and is how every number above was taken.

The service polls the store for the tenant's stream every 100 ms while anything is arriving, and a poll is a shard manifest and a round index whether or not anything was written.
A project nobody is connected to therefore used to read the store about 21 times a second forever, 1883 gets in ninety seconds of an idle node, which on S3 is a bill and a rate limit for a node that is doing nothing.
The gap now doubles towards two seconds once the stream stops moving, so the same ninety seconds cost 243, and a frame arriving or a reader waiting on an lsn puts it straight back to 100 ms.
What that spends is up to two seconds on the first read after a quiet spell, and only on a read that has to wait for the stream at all.

The service holds two caches over the layers it reads and both of them are load bearing.
Footers are cached whole, because a footer is a bloom filter and an index row per block and runs to megabytes on a large layer, so a reader that dropped them would refetch those megabytes on the next read of the same layer.
Blocks are cached under a byte budget, 64 MB by default and `ZOU_BLOCK_CACHE_MB` to change it, because a block is 256 KB of entries and a page read wants one entry out of it: without the cache a sequential scan fetched the same block once per page in it, which measured 1.7 range GETs and 111 KB of store traffic for every 8 KB page served and read a table at about a megabyte a second.
That budget is memory inside the postmaster and it is per shard reader, so a node that is short of memory should lower it before it lowers anything the database itself is using, and a node reading scattered pages out of a working set that nearly fits is the one case worth raising it for.

Branching asks a different question with the service on.
A child served this way reads the pages it inherited out of the layers, never out of the parent's `pg/` prefix, because those are the parent's live base images and a truncate deletes them, so what it needs is an image layer at or below the branch point on every page shard.
Compaction cuts one on its own once a shard has 128 MB of delta debt, which a project that has been running for a while has and a store somebody made this morning has not, and `zou compact <target> <ref> --horizon` cuts one on demand for a store that has not got there yet.
Without one, `zou branch create` refuses with `cannot be branched yet` and names the shard.
The embedded library runs its postmasters with the service off, so templates and fixtures keep answering the capture question instead: a template is a fresh initdb and a few thousand rows and would never earn a fold, and issue #579 is letting a branch ask for one rather than wait.

## Retention and collecting

A store only grows on its own.
A checkpoint fold supersedes the chain under it, a fold that failed leaves captures nothing names, a deleted branch leaves whatever only it referenced, and every state change leaves a manifest snapshot behind so point in time recovery has somewhere to land.
`zou gc <target>` is the sweep that walks the store, pins everything a live manifest or a retained snapshot references, and collects the rest.

    zou gc s3://bucket/fleet --retention 7d --window 24h

Two numbers are the whole policy and they are promises to different people.
`--retention` is how far back point in time recovery reaches, a week by default: a manifest snapshot younger than it keeps everything it references alive, so it is what a customer asking to be restored to last Tuesday is relying on, and it is the same window `zou branch --from-time` can reach into, see [branching.md](branching.md).
`--window` is how long a key that looks like garbage waits before it is deleted, a day by default: it is a promise to whoever is mid publish, and it has to be longer than the longest fold upload and the longest gap between reading a manifest and publishing a branch from it.

The two numbers are not independent: `--retention` has to be longer than `--window`, and both commands refuse a policy where it is not.
The reason is that a candidate stamp only means the key was garbage at that moment, and what makes it mean the key has been garbage ever since is the snapshot record, which is kept for exactly the retention.
So a stamp older than the retention is thrown away and the key waits a fresh window, and if the retention were the shorter of the two no stamp would ever live long enough to come of age.
The defaults, a day against a week, are the shape this wants.

Deleting anything takes two runs whatever the numbers say.
The first run stamps a key as a candidate, a later run deletes it only if it was still garbage on that run's own scan, so a branch published between the two takes its objects back off the list instead of losing them.
That is also why a shorter window is not a faster sweep: `--window 0` still takes two runs.

Three things are yours to hold up rather than the sweep's, and all three are the same shape: something that will reference an object has to finish referencing it inside the window.
A fold has to publish the manifest naming its capture within `--window` of uploading it.
Branch creation has to write the child's manifest within `--window` of reading the parent's, which is the gap `zou branch` closes and the reason a hand rolled equivalent that reads, thinks and writes hours later is not safe.
And a reader fetching a checkpoint has to finish inside the window, because nothing in the store records that a fetch is in progress, so the window is the only grace a superseded object gets.
The default day is chosen to be far longer than any of the three; if you cut it, cut it to something still far longer than your slowest fold.

Missing a sweep entirely is safe, and missing them for longer than the retention is where it stops being free.
A key stamped before it was ever published used to keep that stamp through its whole life and be deleted the moment it was superseded, with none of the window it was owed, and that needed a gap between sweeps longer than the retention to happen at all.
It is fixed, and the cost of the fix is that after such a gap the first sweep restamps rather than deletes, so a store that has gone a fortnight without one takes two sweeps to start freeing bytes rather than one.

`--dry-run` names every object that would go and writes nothing at all.
It is worth reading the second half of that: a dry run does not stamp candidates either, so it is a question and not a first run, and the two runs a deletion takes are still ahead of you.

    zou gc s3://bucket/fleet --dry-run

One sweep runs at a time across the whole deployment, and a lock object in the store enforces it rather than an operator remembering to.
A second run says who holds it and until when and exits non zero, since a cron entry that never runs and never says so is a slow way to fill a bucket.
The lock is held for an hour by default, `--lock-ttl` moves it, and it is released at the end of the run, so a node that died mid sweep costs the next one a TTL rather than a person.
`--force` takes it anyway, for the case where the holder is known to be gone and nobody wants to wait out the rest.
A dry run takes no lock at all, because it writes nothing anyone else's sweep could disagree with.

A node can do this itself instead:

    zou serve s3://bucket/fleet --domain zou.example --gc-every 6h --retention 7d

`--gc-every` is off by default, and `--gc-window` and `--gc-retention` are the same two numbers under it, refused at the command line if there is no `--gc-every` for them to run under, because a retention window on a node that never sweeps is a policy nothing applies.
Every node in a fleet can be given the same flag: the lock is what makes that safe, whichever gets there first sweeps and the rest go back to sleep, so no node has to be the special one with the cron entry.
The first sweep is one interval in rather than at boot, so a node being restarted in a loop does not walk the whole store every time.

What it never collects is worth saying.
WAL is not this job's problem, a tenant's log is trimmed by its own rules, and a tenant prefix with no readable manifest is left entirely alone rather than treated as an orphan.
That last one has a bill attached: deleting a project by removing its `MANIFEST` and stopping there leaves its captures and its snapshots in the bucket for good, because the sweep will not look at a prefix it cannot read a manifest for.
Delete the prefix, not the manifest.
The rule is what makes branch creation safe, since a branch has no manifest until its final write and would otherwise be racing the sweep for its own bytes.
The cost is a full listing of everything under `tenants/`, so this is a daily job on a large fleet and not an hourly one.

### Page history

The sweep above collects what nothing references, and until recently nothing under a shard ever stopped being referenced.
Every fold cut a new image and left the old ones where they were, because an old image is the base a read below the new one needs, and every record the tenant ever wrote stayed in some delta.
So the disk a shard needed was the whole write volume of its life: at pgbench scale 500 with eight clients that is hundreds of gigabytes a day, and none of it was garbage the sweep could take.

The page service now buys itself a horizon on a schedule.
The pass merges every image below the horizon into one image sitting at it, which is the only way old images can go, since they are sparse and each one is the only copy of the base for whatever has not been written since it was cut.
After that merge every layer below the horizon is unreachable for a read at or above it, and comes off the shard manifest; the bytes themselves go on the next `zou gc` that sees them unreferenced, with the usual two runs and the usual window.
Reads below the horizon are refused rather than answered from half a chain.

Where the horizon is is not a setting.
It is the oldest lsn anything still names, which means the oldest checkpoint in the live manifest and the oldest in any history snapshot inside the retention window, so a branch, a restore and a point in time recovery all pin exactly what they are relying on.
`ZOU_PAGE_RETENTION_SECS` is the window that reckoning uses, a week by default, the same default `zou gc --retention` carries.
Set it to zero and the merge never runs and the shard keeps everything, which is what it did before and is an escape hatch rather than a tuning.

The pass runs eight times per window, so a shard holds the window plus at most an eighth of it, and never more often than once a minute whatever the window is.
That floor is there because this is the expensive shape: it reads every layer below the horizon once to learn which keys they hold, then materializes those keys, so its cost is the history it is about to retire plus the working set it is about to image.
It logs what it did at info, including the keys it could build no page for and the layers it therefore left alone, which is the one case where a merge frees less than the numbers suggest it should.

## The postgres port

A node serving many projects listens once on 5432 and proxies each connection to the project's own postgres, because a thousand databases on a node have no thousand ports to be exposed on.
The project is named in the startup packet, in either of the two places a client can put it.
`dbname=acme-prod` is what a person types at psql, and `user=<role>.acme-prod` is the convention Supabase's pooler already taught every driver that cannot set a database name freely, so both work and the user suffix wins when a client spells it twice.
The part of the user that is not the ref is the role the session runs as, which is how anon, authenticated and service_role reach SQL as themselves and RLS means the same thing here as it does over http.

The password is the project key.
It is the same JWT that goes in an `apikey` header, signed with the project's secret, and its `role` claim has to be the role the connection asked for, so a key for anon cannot open a service_role session.
This server checks it and the tenant's own postgres never sees it: the connection this node then opens carries the dsn's credential, which belongs to the node that started the postmaster and is not something a client should hold.
The check happens before the attach, so an unauthenticated stranger cannot make a node start a database.

    psql "postgresql://service_role.acme-prod:$SERVICE_ROLE_KEY@zou.example:5432/postgres"

`postgres` is the fourth role and the one a project's own migrations run as.
It is the cluster superuser every database is initialised with, so it owns the schemas and can create in them, which is what separates it from service_role: service_role sees every row because it bypasses RLS, and it still cannot create a table, exactly as on Supabase.
A key for it is minted from the project's secret the same way the other three are, and it is the project owner's credential rather than anything an application should carry.

    psql "postgresql://postgres.acme-prod:$POSTGRES_KEY@zou.example:5432/postgres" -f migration.sql

A database initialised by a build before this one took its superuser from the account that ran the node, and this build asks for `postgres`, so such a store answers `role "postgres" does not exist` and wants recreating.

TLS is a certificate and its key, given at startup, and one pair covers both postgres ports because it is the same credential crossing the same network.

    zou serve --pg 5432 --pool 6543 --pg-tls-cert /etc/zou/tls.crt --pg-tls-key /etc/zou/tls.key

The file the certificate goes in is the leaf first and then whatever intermediates a client might not already have, which is the same file `ssl_cert_file` takes, and the key is PEM beside it.
A key that does not belong to the certificate is a sentence at startup rather than a surprise at the first connection.
With a certificate the ports take `sslmode=require` and above and nothing else: a client that sends a startup packet in the clear is told `this port requires TLS` and hung up on, since the packet after it would carry the project key.
There are no client certificates, because the key is the credential and a second one would only be a second thing to rotate.
Without a certificate an `SSLRequest` is declined rather than ignored, which is what makes a client decide instead of guess, and the port belongs on a private network or behind a terminator because the key crosses in the clear.
What the connection behind the door answers is trust, cleartext, md5 or SCRAM-SHA-256: the first three are what a postmaster this node started asks for, and the fourth is what a postgres somebody else initialised asks for instead.
There is no channel binding, so the gs2 header says this client does not do it rather than that the server cannot, and a deployment whose tenant database is across a network it does not own wants a terminator in front of that database until there is.
Replication connections are refused too, and a startup packet over 10000 bytes or a login that takes longer than 30 seconds is dropped.

Cancellation works the way it does against postgres directly: the key the client is handed is the key its own backend generated, this node only remembers which database that pair is on so a cancel arriving on a fresh connection has somewhere to go.
A pair this node has never seen is dropped in silence, because guessing one must not cancel a stranger's query.

### The transaction pooler on 6543

The same door in transaction mode listens on 6543, and it is the port a serverless function should use.
Routing, the key and the role are identical to 5432, so the only thing that changes is the connection string's port number.
What changes underneath is what a connection owns: on 5432 a client holds a backend from login to hangup, and on 6543 it borrows one at the first message of a transaction and hands it back at the ReadyForQuery that ends it, so a hundred idle clients cost a hundred sockets on this node and no backends at all on the database.
The ceiling is twenty backends per project and role, and a client that arrives when they are all out waits for one rather than being refused, because a queue is what a pooler is for.

What the extra hop costs is 0.4 ms, measured on server3 as 1.19 ms for `select 1` through 6543 against 0.81 ms for the same statement through 5432 on the same node and the same database, so the choice is not about latency.
What settles it is the other end: a project's postmaster takes forty connections, and the forty first is refused by postgres itself with `sorry, too many clients already` before the client has said anything.
Writes through both doors on one project on that node, twenty five rows an insert and thirty seconds a point, are 306 a second on 5432 at sixteen clients against 350 on 6543, 373 against 327 at thirty two, and nothing at all on 5432 at sixty four against 221 on 6543.
Neither door raises the write ceiling, because a commit is one round trip to the store and concurrent commits already merge into one push, so what the pooler is worth is that a client count the database cannot hold is a queue rather than an error.

There is no backend at login, so the greeting is this server's: the parameter set is the one the project's database announced to the first backend opened for it, replayed to every client after, and the cancel key is this node's own.
A backend is cleaned with `DISCARD ALL` on the way back rather than the next client paying for what the last one left behind, and one that does not come back clean is closed instead of parked.
A cancel is translated instead of forwarded, to the backend the session is on at that moment and with that backend's own key, and a cancel for a session that is between transactions does nothing at all, because the backend it used last belongs to somebody else now.

Session state does not survive a transaction here, which is the rule every transaction pooler has.
`SET`, `LISTEN`, `WITH HOLD` cursors and advisory locks taken outside a transaction all belong on 5432.
Named prepared statements are the one case refused out loud, at the Parse that names them, with a message saying to turn statement caching off in the driver or use 5432, because the alternative is a failure one transaction later about a statement that does not exist.
A transaction left open with nothing said by either side for 60 seconds is closed with `25P03`, since a pooler whose backends are all pinned by clients that went to lunch is an outage for everyone else on the project.

`zou_pg_sessions` is how many are open right now, which is the number a pooler is sized against.
`zou_pg_logins_total{outcome="ok"|"refused"|"error"}` separates a client that was told why in its own protocol, a wrong key or an unknown project, from everything that never got that far.
`zou_pg_bytes_total{direction}` is what the sessions moved.
`zou_pg_backends` against `zou_pg_sessions` is the whole claim the pooler makes, many sessions and few backends, and the two being equal means the pooling is not buying anything.
`zou_pg_transactions_total` counts what ran through it, and `zou_pg_checkout_seconds` is how long clients waited for a backend, which is the number that says the ceiling is too low and is kept apart from query time so it cannot be confused with it.

## Moving a store

`zou push <dir> <target> [ref]` copies a local store, or one tenant of it, out to a remote prefix, and `zou pull <target> <dir> [ref]` copies the other way.
Both are the same walk with the ends named, so the direction is in the verb rather than in an argument order somebody has to remember, and `--jobs` sets how many objects are in flight at once, sixteen by default.

The interesting part is what it does not copy.
Checkpoint objects, manifest history snapshots, and WAL segments never change once written, so a key that already exists on the far side is the same bytes and is skipped without being read.
That makes the second run cheap and makes an interrupted first run resumable: it picks up where the objects run out.
Everything else is copied every time, the live manifest above all, because that is the object whose whole job is to change, and a copy that skipped it would hand over a database pointing at a state it no longer has.

A tenant with a live lease is copied with a warning rather than refused.
The walk is not a snapshot, so a manifest written while it ran can name a checkpoint the walk had already passed, and the fix is to copy a detached tenant or to copy twice.
Nothing here takes a lease of its own, and nothing is deleted on either side, so a push into a prefix that already has a database updates it rather than replacing it.
