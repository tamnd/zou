//! The page service driver: the thread beside the pusher that turns
//! the sequencer's tee stream into the tenant's layer store.
//!
//! The ingest, layer, and read modules have been complete for a while;
//! this is the wiring that gives them a live role. The pusher's
//! sequencer publishes every durable window into a [`Tee`], this
//! thread consumes the tenant's stream through a [`ShardIngest`],
//! and the memtable drains into delta layers under the shard prefix,
//! advancing `disk_consistent_lsn` in the shard manifest. From there
//! the GetPage path can serve any ingested block, which is what lets
//! the smgr stop writing one store object per page later.
//!
//! Join and rejoin follow the tee contract: subscribe first, then
//! [`catch_up`] from the applied watermark, then live events, and the
//! watermark drops the overlap. A cut subscription surfaces as
//! [`IngestError::Lagged`] and runs the same catch up.
//!
//! The anchor rule: an existing shard manifest anchors ingest at its
//! `disk_consistent_lsn`. Without one this store has never ingested,
//! and the driver anchors at the start of the first frame it sees, so
//! layers cover the stream from this session forward. State older
//! than the anchor stays the v1 engine's problem, its pg/ images act
//! as the base a reconstruction starts from.
//!
//! A gap the sealed WAL cannot bridge would leave a hole in the
//! record chains, and a hole poisons every later delta of the blocks
//! it touches. The driver refuses to ingest past one: it reports the
//! tenant caught up, logs the error, and stops. The v1 path keeps
//! serving, the next session anchors fresh and reports the same
//! error if the hole is still there.
//!
//! The lag gauge is the other half of the spec 08 ingest bound: every
//! second the driver reports how far applied trails the durable end
//! into the cell's [`Backpressure`], which is what lets the sequencer
//! throttle a tenant whose ingest cannot keep up, and only that
//! tenant. A zero report on shutdown lifts the throttle rather than
//! pinning a stale lag on the board forever.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use zou_log::{Backpressure, DEFAULT_TEE_BUFFER, IngestLag, Tee, TeeFilter, WalMedia, catch_up};
use zou_store::CasStore;
use zou_store::layout::TenantLayout;
use zou_store::lsn::Lsn;
use zou_store::shardmanifest::PageShardManifest;

use crate::WAL_SHARD;
use crate::ingest::{IngestConfig, IngestError, ShardIngest};

/// How often the loop wakes to check flush deadlines and report the
/// lag gauge when no frames arrive.
const TICK: Duration = Duration::from_millis(100);

/// Gauge reports are rate limited to one per second; the board is a
/// mutex the sequencer's admission path also takes.
const REPORT_EVERY: Duration = Duration::from_secs(1);

/// The single page shard of a self hosted store, the pair of
/// [`WAL_SHARD`]. A cell with sharded tenants runs one driver per
/// page shard later.
const PAGE_SHARD: u32 = 0;

/// The ingest thresholds, with env overrides for bench work.
/// ZOU_INGEST_FLUSH_MB caps memtable memory, ZOU_INGEST_BEHIND_MB
/// bounds how much sealed WAL a rejoin replays, and
/// ZOU_INGEST_AGE_SECS bounds how stale a layer view of a trickling
/// tenant goes. Unset or unparsable falls back to the library
/// defaults.
fn ingest_config(tenant: u128) -> IngestConfig {
    let mut cfg = IngestConfig::new(tenant, PAGE_SHARD, 1);
    if let Some(mb) = env_u64("ZOU_INGEST_FLUSH_MB") {
        cfg.flush_bytes = (mb << 20) as usize;
    }
    if let Some(mb) = env_u64("ZOU_INGEST_BEHIND_MB") {
        cfg.flush_wal_behind = mb << 20;
    }
    if let Some(secs) = env_u64("ZOU_INGEST_AGE_SECS") {
        cfg.flush_age = Duration::from_secs(secs);
    }
    cfg
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

pub(crate) struct PageSvc {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl PageSvc {
    /// Signal the driver, wait for its final flush, and report how it
    /// ended. Idempotent, and safe to call with the sequencer already
    /// closed: the driver only reads its subscription and the store.
    pub(crate) fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for PageSvc {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Start the driver thread. `durable` is the pusher's durability
/// watermark, the end lsn the lag and the flush deadlines measure
/// against. The subscription is taken inside the thread; anything
/// published before it lands is on the chain and the catch up reads
/// it.
pub(crate) fn spawn(
    store: Arc<dyn CasStore>,
    layout: TenantLayout,
    tenant: u128,
    tee: Arc<Tee>,
    media: Arc<WalMedia>,
    gate: Arc<Backpressure>,
    durable: Arc<AtomicU64>,
) -> PageSvc {
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let handle = std::thread::Builder::new()
        .name("zou-pagesvc".into())
        .spawn(move || {
            let mut driver = Driver {
                store,
                layout,
                tenant,
                tee,
                media,
                gate: &gate,
                durable,
                stop: thread_stop,
                ingest: None,
                last_advance: Instant::now(),
                last_report: Instant::now() - REPORT_EVERY,
            };
            if let Err(e) = driver.run() {
                log::error!("zou pagesvc: ingest stopped: {e}");
            }
            // Leaving a nonzero lag on the board would throttle the
            // tenant with nobody left to work it off.
            gate.report_ingest(tenant, IngestLag::default());
        })
        .expect("spawn zou-pagesvc");
    PageSvc {
        stop,
        handle: Some(handle),
    }
}

struct Driver<'a> {
    store: Arc<dyn CasStore>,
    layout: TenantLayout,
    tenant: u128,
    tee: Arc<Tee>,
    media: Arc<WalMedia>,
    gate: &'a Backpressure,
    durable: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    /// `None` until the anchor rule picks a start lsn.
    ingest: Option<ShardIngest>,
    /// When `applied` last moved, the seconds half of the lag gauge.
    last_advance: Instant,
    last_report: Instant,
}

impl Driver<'_> {
    fn run(&mut self) -> Result<(), IngestError> {
        let filter = TeeFilter::Tenant(self.tenant);
        let sub = self.tee.subscribe(filter, DEFAULT_TEE_BUFFER);
        let anchor = match PageShardManifest::load(&*self.store, &self.layout.shard_manifest(0)) {
            Ok(Some((manifest, _))) => Some(manifest.disk_consistent_lsn.0),
            Ok(None) => None,
            Err(e) => return Err(IngestError::Publish(e)),
        };
        if let Some(at) = anchor {
            self.ingest = Some(ShardIngest::new(ingest_config(self.tenant), at));
            self.catch_up(&filter)?;
        }
        loop {
            let mut idle = true;
            while let Some(event) = sub.try_recv() {
                idle = false;
                self.consume(&event, &filter)?;
            }
            self.flush_and_report()?;
            if self.stop.load(Ordering::Acquire) {
                // The sequencer is closed by now, nothing else will
                // publish: drain what raced the flag and finish.
                while let Some(event) = sub.try_recv() {
                    self.consume(&event, &filter)?;
                }
                if let Some(ingest) = &mut self.ingest
                    && let Some(entry) = ingest.flush(&*self.store, &self.layout)?
                {
                    log::info!(
                        "zou pagesvc: final flush, layer {} of {} bytes",
                        entry.name,
                        entry.size
                    );
                }
                return Ok(());
            }
            if idle {
                std::thread::sleep(TICK);
            }
        }
    }

    /// One tee event into the ingest, anchoring first when this store
    /// has never ingested: the anchor is the start of the first frame
    /// seen, nothing older exists to ingest.
    fn consume(
        &mut self,
        event: &zou_log::TeeEvent,
        filter: &TeeFilter,
    ) -> Result<(), IngestError> {
        let ingest = match &mut self.ingest {
            Some(ingest) => ingest,
            None => match first_start(event, self.tenant) {
                Some(start) => {
                    log::info!("zou pagesvc: anchoring a fresh shard at {start:#x}");
                    self.ingest
                        .insert(ShardIngest::new(ingest_config(self.tenant), start))
                }
                None => return Ok(()),
            },
        };
        let before = ingest.applied();
        match ingest.apply_event(event) {
            Ok(()) => {
                if ingest.applied() > before {
                    self.last_advance = Instant::now();
                }
                Ok(())
            }
            Err(IngestError::Lagged { next_seq }) => {
                log::info!("zou pagesvc: cut at seq {next_seq}, catching up");
                self.catch_up(filter)
            }
            Err(e) => Err(e),
        }
    }

    /// Replay the chain from the applied watermark through the same
    /// door live frames use. The tee contract makes the overlap with
    /// the subscription exact.
    fn catch_up(&mut self, filter: &TeeFilter) -> Result<(), IngestError> {
        let ingest = self.ingest.as_mut().expect("anchored before catch up");
        let frames = catch_up(&self.media, WAL_SHARD, filter, Lsn(ingest.applied()))
            .map_err(|e| IngestError::Wal(format!("catch up: {e}")))?;
        let before = ingest.applied();
        ingest.apply_frames(&frames)?;
        if ingest.applied() > before {
            self.last_advance = Instant::now();
        }
        Ok(())
    }

    fn flush_and_report(&mut self) -> Result<(), IngestError> {
        let Some(ingest) = &mut self.ingest else {
            return Ok(());
        };
        let durable = self.durable.load(Ordering::Acquire);
        if let Some(reason) = ingest.flush_due(durable)
            && let Some(entry) = ingest.flush(&*self.store, &self.layout)?
        {
            log::info!(
                "zou pagesvc: flush on {reason:?}, layer {} of {} bytes, applied {:#x}",
                entry.name,
                entry.size,
                ingest.applied()
            );
        }
        if self.last_report.elapsed() >= REPORT_EVERY {
            let bytes = ingest.lag(durable);
            let lag = IngestLag {
                bytes,
                secs: if bytes == 0 {
                    0
                } else {
                    self.last_advance.elapsed().as_secs()
                },
            };
            self.gate.report_ingest(self.tenant, lag);
            self.last_report = Instant::now();
        }
        Ok(())
    }
}

/// The start lsn of the event's first frame of this tenant, the
/// anchor for a shard that has never ingested.
fn first_start(event: &zou_log::TeeEvent, tenant: u128) -> Option<u64> {
    match event {
        zou_log::TeeEvent::Window { frames, .. } => frames
            .iter()
            .find(|f| f.tenant == tenant)
            .map(|f| f.start_lsn.0),
        zou_log::TeeEvent::Lagged { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::walscan::BlockRef;
    use crate::walscan::testwal::Builder;
    use zou_store::frame::Frame2;
    use zou_store::mem::MemStore;

    const WAL_BASE: u64 = 16 << 20;
    const TENANT: u128 = 7;

    fn frames_over(start: u64, raw: &[u8]) -> Vec<Frame2> {
        let mut frames = Vec::new();
        let mut at = start;
        for chunk in raw.chunks(1024) {
            frames.push(Frame2 {
                tenant: TENANT,
                writer_epoch: 1,
                start_lsn: Lsn(at),
                end_lsn: Lsn(at + chunk.len() as u64),
                contains_commit: false,
                first_of_epoch: false,
                hints: Vec::new(),
                payload: chunk.to_vec(),
            });
            at += chunk.len() as u64;
        }
        frames
    }

    fn test_wal() -> (u64, Vec<u8>, u64) {
        let mut b = Builder::new(WAL_BASE);
        for blk in 0..40u32 {
            let r = BlockRef {
                spc: 1663,
                db: 5,
                rel: 1000,
                fork: 0,
                blk,
            };
            b.record(&[(r, false)], &[blk as u8; 64]);
        }
        let end = b.pos();
        let (base, bytes) = b.stream();
        (base, bytes.to_vec(), end)
    }

    /// Publishing into a subscription that does not exist yet would
    /// vanish, the test waits for the driver's subscribe first.
    fn wait_subscribed(tee: &Tee) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while tee.subscriber_count() == 0 {
            assert!(Instant::now() < deadline, "driver never subscribed");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// The driver end to end over a real tee: subscribe, publish, see
    /// a layer in the store and `disk_consistent_lsn` at the stream
    /// end, and the gauge back at zero after the stop.
    #[test]
    fn driver_turns_published_frames_into_a_layer() {
        let store: Arc<dyn CasStore> = Arc::new(MemStore::default());
        let layout = TenantLayout::new("t");
        let tee = Arc::new(Tee::new());
        let media = Arc::new(WalMedia::single(Arc::clone(&store)));
        let gate = Arc::new(Backpressure::default());
        let durable = Arc::new(AtomicU64::new(0));

        let (start, raw, end) = test_wal();
        let mut svc = spawn(
            Arc::clone(&store),
            layout.clone(),
            TENANT,
            Arc::clone(&tee),
            Arc::clone(&media),
            Arc::clone(&gate),
            Arc::clone(&durable),
        );
        wait_subscribed(&tee);
        tee.publish(1, &frames_over(start, &raw));
        durable.store(end, Ordering::Release);
        svc.stop();

        let (manifest, _) = PageShardManifest::load(&*store, &layout.shard_manifest(0))
            .expect("manifest loads")
            .expect("the flush published a manifest");
        assert_eq!(
            manifest.disk_consistent_lsn.0, end,
            "disk consistent lsn sits at the stream end"
        );
        let map = manifest.layer_map().expect("map builds");
        assert_eq!(map.layers().len(), 1, "one delta layer");
        assert!(
            gate.admit(TENANT).is_ok(),
            "no stale lag pins the throttle after the stop"
        );
    }

    /// A driver over a store with no manifest and no frames anchors
    /// nothing, flushes nothing, and stops clean.
    #[test]
    fn driver_idles_clean_on_a_silent_stream() {
        let store: Arc<dyn CasStore> = Arc::new(MemStore::default());
        let layout = TenantLayout::new("t");
        let tee = Arc::new(Tee::new());
        let media = Arc::new(WalMedia::single(Arc::clone(&store)));
        let gate = Arc::new(Backpressure::default());
        let tee_probe = Arc::clone(&tee);
        let mut svc = spawn(
            Arc::clone(&store),
            layout.clone(),
            TENANT,
            tee,
            media,
            Arc::clone(&gate),
            Arc::new(AtomicU64::new(0)),
        );
        wait_subscribed(&tee_probe);
        svc.stop();
        assert!(
            PageShardManifest::load(&*store, &layout.shard_manifest(0))
                .expect("store answers")
                .is_none(),
            "nothing to ingest publishes nothing"
        );
    }
}
