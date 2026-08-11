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

Store numbers come from the counter file `ZOU_STORE_STATS` names rather than from counters of this process, and a scrape folds that file in as it reads it.
That is deliberate: the file is shared memory, so the ops a postgres backend made in another process are in it, and counting in process would count the ones this process can see and miss the rest.
Set `ZOU_STORE_STATS=/run/zou/stats` and the scrape gains `zou_store_ops_total{op,class}`, `zou_store_bytes_total{op,class}`, `zou_store_errors_total{op}`, `zou_store_conflicts_total`, `zou_store_op_seconds{op}` and the read tier counters.
Counts and bytes stay separate because that is how S3 bills.
The latency buckets are the file's own powers of two, folded in at each bucket's upper bound, so the buckets are exact and `_sum` is a ceiling.

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

## Clock skew bound

Lease expiry compares wall clocks on different machines: the holder writes `expires_unix` from its clock, a challenger compares against its own.
Correctness never depends on this comparison.
Every acquisition increments the epoch, WAL lands under epoch directories, frames carry the fence token, and readers reject stale epochs, so even a steal triggered by a wildly wrong clock leaves the old writer's post-steal work unreferenced and no acked commit is lost.
What clocks affect is availability: a challenger whose clock runs fast by S seconds sees the lease expire S seconds early and may steal from a healthy writer, forcing a failover.

The bound for stable operation is that renewals must land before any challenger sees the lease as expired, which with renewal at TTL/3 leaves roughly TTL times 2/3 minus S of slack for retries.
Keep the worst case clock skew across nodes under TTL/3, 5 seconds at the default TTL.
NTP or chrony disciplined hosts sit under 100 ms, which leaves the entire slack budget for object store hiccups.
If your environment cannot bound skew, raise the TTL rather than living with spurious steals, the cost is a longer failover wait after an unclean crash.

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

Routing is `--domain` and the path prefix, and at least one of them has to be on, which is checked at the command line rather than at the first request.
`--domain zou.example` makes `acme-prod.zou.example` a project, and it is also where a tenant's own external url comes from, so the links in its confirmation mail point at the project instead of at the node.
The path prefix is on by default and `--no-path-prefix` turns it off, for a deployment that has a wildcard certificate and does not want a second way in.

`--ref demo` is the other shape: one project, at every url the node answers, with the routing taken out rather than configured off.
Nothing is resolved per request, `--domain` has nothing left to name and is refused alongside it, and the project is attached before the http door starts accepting, so the first request waits in the accept queue for an attach instead of being the reason for it.
`ZOU_REF` sets it and `ZOU_TARGET` sets the store, for a platform that configures a container with variables rather than a command line.
That is the shape a function or a container per project wants, and the Lambda, Cloud Run and Fly recipes are in [serverless.md](serverless.md).

Nothing is running until a request names a project.
The first one for a cold tenant restores its runtime directory out of that tenant's own prefix and starts a postmaster on loopback with a private socket directory, and both are thrown away when it is let go of.
`--max-attached` is how many are up at once and `--idle-secs` is how long an untouched one stays up, both defaulting to what the attach manager uses, and the sweep that enforces the second runs on a timer at a quarter of it, because a node that has gone quiet is exactly the node with no requests to notice on.
`--shared-buffers` is per tenant and defaults to 16MB, small on purpose: the ceiling multiplies it, and the store backed page cache is the tier that is supposed to be doing the work.
A node running a few large projects rather than a thousand small ones should raise it.

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

## Retention and collecting

A store only grows on its own.
A checkpoint fold supersedes the chain under it, a fold that failed leaves captures nothing names, a deleted branch leaves whatever only it referenced, and every state change leaves a manifest snapshot behind so point in time recovery has somewhere to land.
`zou gc <target>` is the sweep that walks the store, pins everything a live manifest or a retained snapshot references, and collects the rest.

    zou gc s3://bucket/fleet --retention 7d --window 24h

Two numbers are the whole policy and they are promises to different people.
`--retention` is how far back point in time recovery reaches, a week by default: a manifest snapshot younger than it keeps everything it references alive, so it is what a customer asking to be restored to last Tuesday is relying on, and it is the same window `zou branch --from-time` can reach into, see [branching.md](branching.md).
`--window` is how long a key that looks like garbage waits before it is deleted, a day by default: it is a promise to whoever is mid publish, and it has to be longer than the longest fold upload and the longest gap between reading a manifest and publishing a branch from it.

Deleting anything takes two runs whatever the numbers say.
The first run stamps a key as a candidate, a later run deletes it only if it was still garbage on that run's own scan, so a branch published between the two takes its objects back off the list instead of losing them.
That is also why a shorter window is not a faster sweep: `--window 0` still takes two runs.

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
The cost is a full listing of everything under `tenants/`, so this is a daily job on a large fleet and not an hourly one.

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

There is no TLS on this port yet, and an `SSLRequest` is declined rather than ignored, which is what makes a client decide instead of guess.
Until there is, put the port on a private network or behind a terminator, because the key crosses in the clear.
A database that asks this node for SCRAM is refused with a sentence saying so, since trust, cleartext and md5 are what a postmaster this node started asks for.
Replication connections are refused too, and a startup packet over 10000 bytes or a login that takes longer than 30 seconds is dropped.

Cancellation works the way it does against postgres directly: the key the client is handed is the key its own backend generated, this node only remembers which database that pair is on so a cancel arriving on a fresh connection has somewhere to go.
A pair this node has never seen is dropped in silence, because guessing one must not cancel a stranger's query.

### The transaction pooler on 6543

The same door in transaction mode listens on 6543, and it is the port a serverless function should use.
Routing, the key and the role are identical to 5432, so the only thing that changes is the connection string's port number.
What changes underneath is what a connection owns: on 5432 a client holds a backend from login to hangup, and on 6543 it borrows one at the first message of a transaction and hands it back at the ReadyForQuery that ends it, so a hundred idle clients cost a hundred sockets on this node and no backends at all on the database.
The ceiling is twenty backends per project and role, and a client that arrives when they are all out waits for one rather than being refused, because a queue is what a pooler is for.

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
