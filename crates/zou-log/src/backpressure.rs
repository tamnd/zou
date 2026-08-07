//! The lag bounds (spec 08 section 4).
//!
//! Unbounded lag anywhere becomes unbounded read waits or unbounded
//! recovery, so every background race the design depends on gets a
//! hard bound and an enforcement point:
//!
//! - Ingest behind durable: 1 GB or 10 s per tenant. Past it the
//!   sequencer throttles that tenant's appends at admission, and only
//!   that tenant's. A lagging page service never slows a neighbor.
//! - Consolidation behind the chain: 2 GB per WAL shard. The runner
//!   bumps the worst shard to the front, and past the bound the cell
//!   throttles every append, because landing bytes nobody folds are
//!   takeover time and expensive storage growing without limit. That
//!   one is an alarm, not a steady state.
//! - Compaction debt is bounded by read amplification, enforced in the
//!   debt scheduler ([`READ_AMP_BOUND`] in zou-pg's compact module):
//!   an over bound shard jumps the queue. GetPage never waits on
//!   compaction, reads just pay the amp until the pass lands.
//!
//! This type is the gauge board the roles report into and the
//! sequencer reads at admission. Reports replace, so a recovered
//! tenant clears itself with its next report and a throttle lifts the
//! moment the lag is back under the bound. Commits are delayed, never
//! lost: a throttled append stages nothing and the compute retries.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

/// Ingest may trail the durable end by this many bytes per tenant.
pub const INGEST_BEHIND_BYTES: u64 = 1 << 30;

/// Ingest may trail the durable end by this many seconds per tenant.
pub const INGEST_BEHIND_SECS: u64 = 10;

/// The landing chain may hold this many unconsolidated bytes per WAL
/// shard before the cell level throttle engages.
pub const CONSOLIDATION_BEHIND_BYTES: u64 = 2 << 30;

/// The bounds, tunable per cell. The defaults are the spec's numbers.
#[derive(Debug, Clone, Copy)]
pub struct LagBounds {
    pub ingest_bytes: u64,
    pub ingest_secs: u64,
    pub consolidation_bytes: u64,
}

impl Default for LagBounds {
    fn default() -> Self {
        LagBounds {
            ingest_bytes: INGEST_BEHIND_BYTES,
            ingest_secs: INGEST_BEHIND_SECS,
            consolidation_bytes: CONSOLIDATION_BEHIND_BYTES,
        }
    }
}

/// One tenant's ingest lag as its page service driver measures it:
/// bytes between the durable end and the applied watermark, and how
/// long the applied watermark has been stuck behind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestLag {
    pub bytes: u64,
    pub secs: u64,
}

/// Why an append was refused admission.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Throttle {
    #[error("ingest is {lag} bytes behind durable, the bound is {bound}")]
    IngestBytes { lag: u64, bound: u64 },
    #[error("ingest is {lag} s behind durable, the bound is {bound} s")]
    IngestSecs { lag: u64, bound: u64 },
    #[error(
        "consolidation on wal shard {shard} is {backlog} bytes behind the chain, the bound is {bound}"
    )]
    Consolidation {
        shard: u32,
        backlog: u64,
        bound: u64,
    },
}

#[derive(Debug, Default)]
struct Gauges {
    /// Worst shard lag per tenant, kept only while nonzero.
    ingest: HashMap<u128, IngestLag>,
    /// Unconsolidated landing bytes per WAL shard, kept only while
    /// nonzero.
    consolidation: BTreeMap<u32, u64>,
}

/// The gauge board. Share one per cell process: the ingest drivers and
/// the consolidation runner report, the sequencer asks at admission.
#[derive(Debug, Default)]
pub struct Backpressure {
    bounds: LagBounds,
    gauges: Mutex<Gauges>,
}

impl Backpressure {
    pub fn new(bounds: LagBounds) -> Self {
        Backpressure {
            bounds,
            gauges: Mutex::default(),
        }
    }

    pub fn bounds(&self) -> LagBounds {
        self.bounds
    }

    /// Report a tenant's ingest lag, the worst across its page shards.
    /// The report replaces the last one, so reporting zero is how a
    /// caught up tenant lifts its own throttle.
    pub fn report_ingest(&self, tenant: u128, lag: IngestLag) {
        let mut g = self.gauges.lock().unwrap();
        if lag == IngestLag::default() {
            g.ingest.remove(&tenant);
        } else {
            g.ingest.insert(tenant, lag);
        }
    }

    /// Report a WAL shard's unconsolidated landing bytes, from
    /// [`landing_backlog`](crate::consolidate::landing_backlog) or the
    /// runner's own count. Replaces the last report.
    pub fn report_consolidation(&self, shard: u32, backlog: u64) {
        let mut g = self.gauges.lock().unwrap();
        if backlog == 0 {
            g.consolidation.remove(&shard);
        } else {
            g.consolidation.insert(shard, backlog);
        }
    }

    /// The admission decision for one tenant's append. A tenant over
    /// its own ingest bound is refused alone; a WAL shard over the
    /// consolidation bound refuses everyone, that is the cell alarm.
    pub fn admit(&self, tenant: u128) -> Result<(), Throttle> {
        let g = self.gauges.lock().unwrap();
        if let Some(lag) = g.ingest.get(&tenant) {
            if lag.bytes > self.bounds.ingest_bytes {
                return Err(Throttle::IngestBytes {
                    lag: lag.bytes,
                    bound: self.bounds.ingest_bytes,
                });
            }
            if lag.secs > self.bounds.ingest_secs {
                return Err(Throttle::IngestSecs {
                    lag: lag.secs,
                    bound: self.bounds.ingest_secs,
                });
            }
        }
        if let Some((shard, backlog)) = g
            .consolidation
            .iter()
            .map(|(&s, &b)| (s, b))
            .max_by_key(|&(_, b)| b)
            .filter(|&(_, b)| b > self.bounds.consolidation_bytes)
        {
            return Err(Throttle::Consolidation {
                shard,
                backlog,
                bound: self.bounds.consolidation_bytes,
            });
        }
        Ok(())
    }

    /// The cell alarm: the worst WAL shard past the consolidation
    /// bound, if any. This is a page-the-operator state, the throttle
    /// only buys time.
    pub fn alarmed(&self) -> Option<(u32, u64)> {
        self.gauges
            .lock()
            .unwrap()
            .consolidation
            .iter()
            .map(|(&s, &b)| (s, b))
            .filter(|&(_, b)| b > self.bounds.consolidation_bytes)
            .max_by_key(|&(_, b)| b)
    }

    /// The priority bump: the WAL shard with the deepest backlog,
    /// bound or not. A consolidation runner sweeping many shards folds
    /// this one first.
    pub fn worst_consolidation(&self) -> Option<(u32, u64)> {
        self.gauges
            .lock()
            .unwrap()
            .consolidation
            .iter()
            .map(|(&s, &b)| (s, b))
            .max_by_key(|&(_, b)| b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tight() -> Backpressure {
        Backpressure::new(LagBounds {
            ingest_bytes: 1000,
            ingest_secs: 10,
            consolidation_bytes: 5000,
        })
    }

    #[test]
    fn a_lagging_tenant_is_refused_alone_and_recovers_by_reporting() {
        let bp = tight();
        assert_eq!(bp.admit(1), Ok(()), "no reports, no throttle");

        bp.report_ingest(
            1,
            IngestLag {
                bytes: 1500,
                secs: 0,
            },
        );
        assert_eq!(
            bp.admit(1),
            Err(Throttle::IngestBytes {
                lag: 1500,
                bound: 1000
            })
        );
        assert_eq!(bp.admit(2), Ok(()), "the neighbor is untouched");

        bp.report_ingest(
            1,
            IngestLag {
                bytes: 900,
                secs: 12,
            },
        );
        assert_eq!(
            bp.admit(1),
            Err(Throttle::IngestSecs { lag: 12, bound: 10 }),
            "either half of the bound throttles"
        );

        bp.report_ingest(
            1,
            IngestLag {
                bytes: 900,
                secs: 3,
            },
        );
        assert_eq!(bp.admit(1), Ok(()), "under both bounds, admitted");
        bp.report_ingest(1, IngestLag::default());
        assert_eq!(bp.admit(1), Ok(()));
    }

    #[test]
    fn a_consolidation_alarm_throttles_the_whole_cell() {
        let bp = tight();
        bp.report_consolidation(3, 4000);
        assert_eq!(bp.alarmed(), None, "under the bound is not an alarm");
        assert_eq!(bp.worst_consolidation(), Some((3, 4000)));
        assert_eq!(bp.admit(1), Ok(()));

        bp.report_consolidation(7, 6000);
        assert_eq!(bp.alarmed(), Some((7, 6000)));
        let err = bp.admit(1).unwrap_err();
        assert_eq!(
            err,
            Throttle::Consolidation {
                shard: 7,
                backlog: 6000,
                bound: 5000
            }
        );
        assert_eq!(
            bp.admit(2).unwrap_err(),
            err,
            "every tenant is refused, this is the cell alarm"
        );

        bp.report_consolidation(7, 0);
        assert_eq!(bp.alarmed(), None);
        assert_eq!(bp.admit(1), Ok(()));
        assert_eq!(bp.worst_consolidation(), Some((3, 4000)));
    }
}
