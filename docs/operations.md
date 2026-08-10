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
