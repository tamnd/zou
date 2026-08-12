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
//! second the driver reports how far the received stream end trails
//! the durable end into the cell's [`Backpressure`], which is what
//! lets the sequencer throttle a tenant whose ingest cannot keep up,
//! and only that tenant. A zero report on shutdown lifts the throttle
//! rather than pinning a stale lag on the board forever.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use zou_log::{
    Backpressure, DEFAULT_TEE_BUFFER, IngestLag, Tee, TeeFilter, WalMedia, catch_up_with,
};
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
pub(crate) fn ingest_config(tenant: u128) -> IngestConfig {
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
    spawn_with_budget(
        store,
        layout,
        tenant,
        tee,
        media,
        gate,
        durable,
        DEFAULT_TEE_BUFFER,
    )
}

/// [`spawn`] with the tee budget explicit, so a test can force a cut
/// without publishing gigabytes.
#[allow(clippy::too_many_arguments)]
fn spawn_with_budget(
    store: Arc<dyn CasStore>,
    layout: TenantLayout,
    tenant: u128,
    tee: Arc<Tee>,
    media: Arc<WalMedia>,
    gate: Arc<Backpressure>,
    durable: Arc<AtomicU64>,
    tee_budget: usize,
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
                tee_budget,
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
    tee_budget: usize,
    /// `None` until the anchor rule picks a start lsn.
    ingest: Option<ShardIngest>,
    /// When the received stream end last moved, the seconds half of
    /// the lag gauge. Tracks `seen`, not `applied`: a stream parked
    /// inside a partial record is caught up, not stuck.
    last_advance: Instant,
    last_report: Instant,
}

impl Driver<'_> {
    fn run(&mut self) -> Result<(), IngestError> {
        let filter = TeeFilter::Tenant(self.tenant);
        let mut sub = self.tee.subscribe(filter, self.tee_budget);
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
                if let Some(next_seq) = self.consume(&event)? {
                    // The cut removed this subscription from the tee,
                    // so resubscribe before replaying: the other order
                    // leaves a window landed between the replay's end
                    // and the first delivery, with no later event to
                    // reveal it. The overlap is harmless, apply skips
                    // bytes below the stream end.
                    log::info!("zou pagesvc: cut at seq {next_seq}, catching up");
                    sub = self.tee.subscribe(filter, self.tee_budget);
                    if self.ingest.is_some() {
                        self.catch_up(&filter)?;
                    }
                }
            }
            self.flush_and_report()?;
            if self.stop.load(Ordering::Acquire) {
                // The sequencer is closed by now, nothing else will
                // publish: drain what raced the flag and finish.
                while let Some(event) = sub.try_recv() {
                    if self.consume(&event)?.is_some() && self.ingest.is_some() {
                        self.catch_up(&filter)?;
                    }
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
                // Nothing in the channel and nothing coming: if the
                // stream end still trails durable, no future event will
                // say so, because whatever was missed was missed. Close
                // the gap ourselves. A stream that merely ends inside a
                // record reads as caught up and sleeps.
                let durable = self.durable.load(Ordering::Acquire);
                if self
                    .ingest
                    .as_ref()
                    .is_some_and(|ingest| ingest.lag(durable) > 0)
                {
                    self.catch_up(&filter)?;
                }
                std::thread::sleep(TICK);
            }
        }
    }

    /// One tee event into the ingest, anchoring first when this store
    /// has never ingested: the anchor is the start of the first frame
    /// seen, nothing older exists to ingest.
    ///
    /// A cut comes back as `Some(next_seq)` instead of being handled
    /// here, because recovering from one means resubscribing and only
    /// the run loop holds the subscription. That also keeps a cut that
    /// races the anchor from vanishing, which it used to: with no
    /// ingest yet there were no frames to anchor on and the event fell
    /// through as consumed.
    fn consume(&mut self, event: &zou_log::TeeEvent) -> Result<Option<u64>, IngestError> {
        if let zou_log::TeeEvent::Lagged { next_seq } = event {
            return Ok(Some(*next_seq));
        }
        let ingest = match &mut self.ingest {
            Some(ingest) => ingest,
            None => match first_start(event, self.tenant) {
                Some(start) => {
                    log::info!("zou pagesvc: anchoring a fresh shard at {start:#x}");
                    self.ingest
                        .insert(ShardIngest::new(ingest_config(self.tenant), start))
                }
                None => return Ok(None),
            },
        };
        let before = ingest.seen();
        ingest.apply_event(event)?;
        if ingest.seen() > before {
            self.last_advance = Instant::now();
        }
        Ok(None)
    }

    /// Replay the chain from the applied watermark through the same
    /// door live frames use. The tee contract makes the overlap with
    /// the subscription exact.
    /// The replay is streamed and flushes as it goes. A driver that
    /// rejoins after a cut, or starts against a long chain, has the
    /// whole backlog to apply, and collecting it first holds it twice
    /// over: once as frames and once as a memtable nothing checks
    /// until the last one is in. The serving half died that way at
    /// scale 1000. Flushing inside the loop keeps the memtable at its
    /// threshold whatever the backlog is.
    fn catch_up(&mut self, filter: &TeeFilter) -> Result<(), IngestError> {
        let applied = self
            .ingest
            .as_ref()
            .expect("anchored before catch up")
            .applied();
        let before = self
            .ingest
            .as_ref()
            .expect("anchored before catch up")
            .seen();
        let store = &self.store;
        let layout = &self.layout;
        let durable = &self.durable;
        let ingest = self.ingest.as_mut().expect("anchored before catch up");
        catch_up_with::<IngestError, _>(&self.media, WAL_SHARD, filter, Lsn(applied), |frame| {
            ingest.apply_frames(std::slice::from_ref(&frame))?;
            let durable = durable.load(Ordering::Acquire);
            if ingest.flush_due(durable).is_some()
                && let Some(entry) = ingest.flush(&**store, layout)?
            {
                log::info!(
                    "zou pagesvc: flush mid replay, layer {} of {} bytes, applied {:#x}",
                    entry.name,
                    entry.size,
                    ingest.applied()
                );
            }
            Ok(())
        })?;
        if ingest.seen() > before {
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

    /// Zero bounds so any nonzero lag report refuses admission: the
    /// gate doubles as the probe for whether the driver thinks it is
    /// caught up.
    fn zero_bounds() -> Arc<Backpressure> {
        Arc::new(Backpressure::new(zou_log::LagBounds {
            ingest_bytes: 0,
            ingest_secs: 0,
            consolidation_bytes: u64::MAX,
        }))
    }

    /// Wait past at least one report interval and then for the gate to
    /// admit, so a pre-report `Ok` cannot pass for caught up.
    fn wait_caught_up(gate: &Backpressure, why: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let settle = Instant::now() + Duration::from_secs(2);
        while Instant::now() < settle || gate.admit(TENANT).is_err() {
            assert!(Instant::now() < deadline, "{why}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// The wedge class from the scale 1000 run: a window lands on the
    /// chain but its publish never reaches the driver, and nothing
    /// later arrives to reveal it. The idle loop has to notice the
    /// stream end trailing durable and close the gap from the chain by
    /// itself.
    #[test]
    fn an_unpublished_window_is_healed_from_the_chain() {
        let store: Arc<dyn CasStore> = Arc::new(MemStore::default());
        let layout = TenantLayout::new("t");
        let tee = Arc::new(Tee::new());
        let media = Arc::new(WalMedia::single(Arc::clone(&store)));
        let gate = zero_bounds();
        let durable = Arc::new(AtomicU64::new(0));

        let (start, raw, end) = test_wal();
        // The whole stream lands on the chain with no tee attached.
        let t = zou_log::take_over(&media, WAL_SHARD, "test").expect("take over");
        let sink = Arc::new(zou_log::MediaSink::new(
            Arc::clone(&media),
            WAL_SHARD,
            t.sealed_seq,
        ));
        let seq = zou_log::Sequencer::resume(
            WAL_SHARD,
            sink,
            zou_log::SequencerConfig::default(),
            t.next_seq,
            t.prev_digest,
        );
        seq.append(frames_over(start, &raw))
            .expect("admitted")
            .wait()
            .expect("durable");
        seq.close().expect("sequencer close");

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
        // Only the first two frames are published; the rest is the
        // missed window.
        tee.publish(1, &frames_over(start, &raw[..2048]));
        durable.store(end, Ordering::Release);

        wait_caught_up(&gate, "the idle loop never closed the gap from the chain");
        svc.stop();
        let (manifest, _) = PageShardManifest::load(&*store, &layout.shard_manifest(0))
            .expect("manifest loads")
            .expect("the flush published a manifest");
        assert_eq!(
            manifest.disk_consistent_lsn.0, end,
            "the heal reached the durable end"
        );
    }

    /// A subscription cut mid stream: the driver must resubscribe and
    /// replay the chain, then keep hearing live windows on the new
    /// subscription. It used to catch up once and stay deaf, and the
    /// tail here is published but never landed, so only the live path
    /// can deliver it.
    #[test]
    fn a_cut_subscription_rejoins_and_stays_live() {
        let store: Arc<dyn CasStore> = Arc::new(MemStore::default());
        let layout = TenantLayout::new("t");
        let tee = Arc::new(Tee::new());
        let media = Arc::new(WalMedia::single(Arc::clone(&store)));
        let gate = zero_bounds();
        let durable = Arc::new(AtomicU64::new(0));

        // Split the stream at record boundaries so every piece parses
        // on its own no matter which publishes the driver hears.
        let mut b = Builder::new(WAL_BASE);
        let mut first = 0u64;
        let mut second = 0u64;
        for blk in 0..40u32 {
            let r = BlockRef {
                spc: 1663,
                db: 5,
                rel: 1000,
                fork: 0,
                blk,
            };
            b.record(&[(r, false)], &[blk as u8; 64]);
            if blk == 7 {
                first = b.pos();
            }
            if blk == 29 {
                second = b.pos();
            }
        }
        let end = b.pos();
        let (start, bytes) = b.stream();
        let raw = bytes.to_vec();
        let w1 = &raw[..(first - start) as usize];
        let w2 = &raw[(first - start) as usize..(second - start) as usize];
        let w3 = &raw[(second - start) as usize..];
        assert!(
            w2.len() > w1.len(),
            "the cut window must blow the budget alone"
        );

        let t = zou_log::take_over(&media, WAL_SHARD, "test").expect("take over");
        let sink = Arc::new(zou_log::MediaSink::new(
            Arc::clone(&media),
            WAL_SHARD,
            t.sealed_seq,
        ));
        let config = zou_log::SequencerConfig {
            tee: Some(Arc::clone(&tee)),
            ..Default::default()
        };
        let seq = zou_log::Sequencer::resume(WAL_SHARD, sink, config, t.next_seq, t.prev_digest);

        let mut svc = spawn_with_budget(
            Arc::clone(&store),
            layout.clone(),
            TENANT,
            Arc::clone(&tee),
            Arc::clone(&media),
            Arc::clone(&gate),
            Arc::clone(&durable),
            w2.len() - 1,
        );
        wait_subscribed(&tee);
        seq.append(frames_over(start, w1))
            .expect("admitted")
            .wait()
            .expect("durable");
        // Let the driver drain the first window so the budget math
        // below is about the second window alone.
        std::thread::sleep(Duration::from_millis(300));
        // Bigger than the whole budget: this window cuts the
        // subscription and lands on the chain for the replay.
        seq.append(frames_over(first, w2))
            .expect("admitted")
            .wait()
            .expect("durable");
        seq.close().expect("sequencer close");
        durable.store(end, Ordering::Release);

        // Republish the tail until the new subscription hears it, the
        // watermark drops the overlap. A driver that never resubscribes
        // never closes the lag: the tail is not on the chain.
        let deadline = Instant::now() + Duration::from_secs(10);
        let settle = Instant::now() + Duration::from_secs(2);
        loop {
            tee.publish(1000, &frames_over(second, w3));
            std::thread::sleep(Duration::from_millis(50));
            if Instant::now() >= settle && gate.admit(TENANT).is_ok() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the driver never rejoined the live stream after the cut"
            );
        }
        svc.stop();
        let (manifest, _) = PageShardManifest::load(&*store, &layout.shard_manifest(0))
            .expect("manifest loads")
            .expect("the flush published a manifest");
        assert_eq!(
            manifest.disk_consistent_lsn.0, end,
            "live windows on the new subscription reach the layers"
        );
    }

    /// A write pause that ends inside a record parks `applied` a few
    /// bytes short of durable for as long as the pause lasts. Measured
    /// from the received end that is not lag; the old gauge read it as
    /// a stuck ingest, throttled the tenant, and the throttle refused
    /// the very appends that would have completed the record.
    #[test]
    fn a_pause_inside_a_record_does_not_throttle() {
        let store: Arc<dyn CasStore> = Arc::new(MemStore::default());
        let layout = TenantLayout::new("t");
        let tee = Arc::new(Tee::new());
        let media = Arc::new(WalMedia::single(Arc::clone(&store)));
        // Any report with nonzero seconds refuses admission, so one
        // stuck report during the pause fails the test.
        let gate = Arc::new(Backpressure::new(zou_log::LagBounds {
            ingest_bytes: 1 << 30,
            ingest_secs: 0,
            consolidation_bytes: u64::MAX,
        }));
        let durable = Arc::new(AtomicU64::new(0));

        let (start, raw, _end) = test_wal();
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
        let cut = raw.len() - 10;
        tee.publish(1, &frames_over(start, &raw[..cut]));
        durable.store(start + cut as u64, Ordering::Release);

        // Long enough for two report intervals with the stream parked.
        std::thread::sleep(Duration::from_millis(2500));
        assert!(
            gate.admit(TENANT).is_ok(),
            "a stream parked inside a record is caught up, not stuck"
        );
        svc.stop();
        assert!(
            gate.admit(TENANT).is_ok(),
            "the stop leaves the board clean"
        );
    }
}
