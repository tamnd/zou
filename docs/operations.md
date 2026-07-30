# Operations

Operational contracts for running zou writers.

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
