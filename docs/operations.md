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
