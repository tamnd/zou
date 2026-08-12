//! Ingest: the tee stream becomes the layer store (spec 04 sec 3).
//!
//! One `ShardIngest` owns one page shard of one tenant. It consumes
//! the shard's tee subscription, reassembles the frames into the
//! contiguous WAL byte stream, walks complete records, and indexes
//! each record into the memtable under every page key the record's
//! block refs put on this shard. When a flush threshold trips, the
//! memtable drains into one delta layer object and a manifest CAS
//! publishes it, advancing `disk_consistent_lsn`.
//!
//! The flush thresholds bound three different costs. Bytes bounds
//! memory: a memtable past [`FLUSH_BYTES`] flushes no matter what.
//! WAL behind bounds restart replay: if the oldest unflushed record
//! sits more than [`FLUSH_WAL_BEHIND`] behind the durable end, a
//! restart would re read that much sealed WAL, so flush now. Age
//! bounds recency, but only above [`SMALL_TENANT_FLOOR`]: a tenant
//! trickling a few records an hour never earns a layer object, its
//! tail just lives in the sealed WAL and a restart replays it from
//! there for free. That floor is what makes an idle tenant cost
//! nothing but its manifest.
//!
//! Rejoin protocol, same as every tee consumer: subscribe first, then
//! [`zou_log::catch_up`] from `disk_consistent_lsn`, feed those frames
//! here, then feed the live subscription. The `applied` watermark
//! makes the overlap exact: a frame whose bytes are all at or below it
//! is the same data coming around again and is skipped.
//!
//! Frames today carry no block ref hints, so the tee delivers every
//! frame of the tenant and the byte stream a subscriber sees is
//! contiguous. When producers start hinting, a hint filtered gap means
//! the parser cannot walk record lengths across the hole; that will
//! need record aligned frames or a sealed WAL range read, and until
//! then a gap is a loud [`IngestError::Gap`].
//!
//! Relation size tracking (the relsize keys the read path already
//! understands) rides with the GetPage box: sizes answer smgr nblocks
//! calls, which is the page service's contract, not the ingest loop's.

use std::time::{Duration, Instant};

use zou_log::{ConsolidateError, TeeEvent};
use zou_store::cas::{CasError, CasStore};
use zou_store::frame::{BlockRef as StripeRef, Frame2};
use zou_store::layer::{LayerBuildError, LayerKey, build_delta};
use zou_store::layermap::LayerDesc;
use zou_store::layout::TenantLayout;
use zou_store::lsn::Lsn;
use zou_store::memtable::Memtable;
use zou_store::shardmanifest::{self, LayerEntry, PageShardError};

use crate::walscan::{self, WalWindow};

/// Flush when the memtable holds this many record bytes.
pub const FLUSH_BYTES: usize = 64 << 20;

/// Flush when the oldest unflushed record is this far behind the
/// durable end of the WAL, whatever the memtable weighs.
pub const FLUSH_WAL_BEHIND: u64 = 512 << 20;

/// Flush a memtable older than this, provided it clears the small
/// tenant floor.
pub const FLUSH_AGE: Duration = Duration::from_secs(3600);

/// Below this many bytes the age trigger stays quiet: the tail lives
/// in the sealed WAL and a restart replays it from there. This is the
/// small tenant rule, a trickle never earns a layer object.
pub const SMALL_TENANT_FLOOR: usize = 8 << 20;

/// Block target handed to [`build_delta`], the layer codec's unit of
/// fetch and checksum.
pub const DELTA_BLOCK_TARGET: usize = 256 << 10;

#[derive(Debug, Clone)]
pub struct IngestConfig {
    pub tenant: u128,
    pub shard: u32,
    pub shard_count: u32,
    pub flush_bytes: usize,
    pub flush_wal_behind: u64,
    pub flush_age: Duration,
    pub small_floor: usize,
    pub block_target: usize,
}

impl IngestConfig {
    pub fn new(tenant: u128, shard: u32, shard_count: u32) -> Self {
        IngestConfig {
            tenant,
            shard,
            shard_count,
            flush_bytes: FLUSH_BYTES,
            flush_wal_behind: FLUSH_WAL_BEHIND,
            flush_age: FLUSH_AGE,
            small_floor: SMALL_TENANT_FLOOR,
            block_target: DELTA_BLOCK_TARGET,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error(
        "wal gap: buffered through {have:#x} but the next frame starts at {frame_start:#x}, catch up from the sealed wal"
    )]
    Gap { have: u64, frame_start: u64 },
    #[error("the tee cut this subscription at seq {next_seq}, catch up and resubscribe")]
    Lagged { next_seq: u64 },
    #[error("wal parse: {0}")]
    Wal(String),
    #[error("wal catch up: {0}")]
    CatchUp(#[from] ConsolidateError),
    #[error("building delta layer: {0}")]
    Build(#[from] LayerBuildError),
    #[error(transparent)]
    Store(#[from] CasError),
    #[error(transparent)]
    Publish(#[from] PageShardError),
}

/// Why a flush is due. Reported so the caller can count them: a fleet
/// where WalBehind dominates has its thresholds wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    Bytes,
    WalBehind,
    Age,
}

pub struct ShardIngest {
    cfg: IngestConfig,
    mem: Memtable,
    /// End of the last fully parsed record, the resume point. Frames
    /// entirely at or below it are replays and skipped.
    applied: u64,
    /// Contiguous stream bytes over `[buf_base, buf_base + buf.len())`,
    /// the unparsed tail: at most one partial record plus whatever the
    /// last window did not cover.
    buf_base: u64,
    buf: Vec<u8>,
    /// When the oldest record in the memtable arrived, the age clock.
    first_insert: Option<Instant>,
}

impl ShardIngest {
    /// Start ingesting at `start`, which must be a record boundary:
    /// the shard manifest's `disk_consistent_lsn`, or the stream start
    /// for a fresh tenant.
    pub fn new(cfg: IngestConfig, start: u64) -> Self {
        ShardIngest {
            cfg,
            mem: Memtable::new(),
            applied: start,
            buf_base: start,
            buf: Vec::new(),
            first_insert: None,
        }
    }

    /// The resume point: every record ending at or below this lsn has
    /// been parsed and indexed. This is what catch up replays from.
    pub fn applied(&self) -> u64 {
        self.applied
    }

    /// How far ingest trails the durable end of the tenant's WAL, the
    /// bytes half of the spec 08 section 4 ingest bound. The driver
    /// reports the worst shard's lag into the cell's
    /// [`Backpressure`](zou_log::Backpressure) so the sequencer can
    /// throttle this tenant, and only this tenant, past 1 GB.
    ///
    /// Measured from [`Self::seen`], not [`Self::applied`]: a flush
    /// position usually ends inside a record, so `applied` parks a few
    /// bytes short of durable until unrelated WAL completes it. Those
    /// bytes are received, not lagging, and counting them would report
    /// a stuck watermark on every write pause until the ten second
    /// bound throttled the tenant, with nothing left to lift it.
    pub fn lag(&self, durable_end: u64) -> u64 {
        durable_end.saturating_sub(self.seen())
    }

    pub fn memtable(&self) -> &Memtable {
        &self.mem
    }

    /// End of the contiguous stream received so far. Everything below
    /// this is either indexed or the partial record the stream ends
    /// inside, so the reconstructed page state at [`Self::applied`]
    /// equals the page state at any lsn up to here. Readers wait on
    /// this, not on `applied`: a flush position usually lands on a WAL
    /// page boundary with a record spilling over it, and `applied`
    /// stays a few bytes short of it until unrelated new WAL arrives.
    pub fn seen(&self) -> u64 {
        self.buf_base + self.buf.len() as u64
    }

    /// One tee event. `Lagged` surfaces as an error telling the driver
    /// to catch up from the sealed WAL and resubscribe; the watermark
    /// makes the replayed overlap harmless.
    pub fn apply_event(&mut self, event: &TeeEvent) -> Result<(), IngestError> {
        match event {
            TeeEvent::Window { frames, .. } => self.apply_frames(frames),
            TeeEvent::Lagged { next_seq } => Err(IngestError::Lagged {
                next_seq: *next_seq,
            }),
        }
    }

    /// Append the frames' bytes to the stream buffer and index every
    /// newly completed record. Frames from live delivery and from
    /// catch up go through the same door, in stream order.
    pub fn apply_frames(&mut self, frames: &[Frame2]) -> Result<(), IngestError> {
        for frame in frames {
            if frame.tenant != self.cfg.tenant {
                continue;
            }
            let start = frame.start_lsn.0;
            let frame_end = start + frame.payload.len() as u64;
            let end = self.buf_base + self.buf.len() as u64;
            if frame_end <= end {
                continue;
            }
            if start > end {
                return Err(IngestError::Gap {
                    have: end,
                    frame_start: start,
                });
            }
            self.buf
                .extend_from_slice(&frame.payload[(end - start) as usize..]);
        }
        self.parse_available()
    }

    /// Walk complete records in the buffer, index them, and drop the
    /// consumed prefix. A record the buffer ends inside stays for the
    /// next frame to complete.
    fn parse_available(&mut self) -> Result<(), IngestError> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let window = WalWindow::from_raw(self.buf_base, std::mem::take(&mut self.buf));
        let out = walscan::read_records(&window, self.applied, None).map_err(IngestError::Wal)?;
        for record in &out.records {
            self.index_record(record)?;
        }
        self.applied = out.resume;
        let mut buf = window.buf;
        let consumed = (out.resume - self.buf_base).min(buf.len() as u64) as usize;
        buf.drain(..consumed);
        self.buf = buf;
        self.buf_base = out.resume;
        Ok(())
    }

    /// Index one record under every page key its block refs put on
    /// this shard. A record touching two of our blocks lands under
    /// both keys: reconstruction looks records up by key, and a few
    /// duplicated bytes in a delta layer beat a second lookup path.
    fn index_record(&mut self, record: &walscan::WalRecord) -> Result<(), IngestError> {
        let refs = record.block_refs().map_err(IngestError::Wal)?;
        for r in &refs {
            let stripe = StripeRef {
                relfilenode: r.rel,
                fork: r.fork as u8,
                block: r.blk,
            };
            if stripe.page_shard(self.cfg.shard_count) != self.cfg.shard {
                continue;
            }
            let key = LayerKey::page(r.spc, r.db, r.rel, r.fork as u8, r.blk);
            self.mem.insert(key, Lsn(record.lsn), record.bytes.clone());
            self.first_insert.get_or_insert_with(Instant::now);
        }
        Ok(())
    }

    /// Whether a flush is due, with the age measured by the wall
    /// clock. `durable_end` is the durable end of the tenant's WAL.
    pub fn flush_due(&self, durable_end: u64) -> Option<FlushReason> {
        let age = self
            .first_insert
            .map(|t| t.elapsed())
            .unwrap_or(Duration::ZERO);
        self.flush_reason(durable_end, age)
    }

    /// The flush decision with an explicit age, the testable core of
    /// [`Self::flush_due`].
    pub fn flush_reason(&self, durable_end: u64, age: Duration) -> Option<FlushReason> {
        let (oldest, _) = self.mem.lsn_range()?;
        if self.mem.bytes() >= self.cfg.flush_bytes {
            return Some(FlushReason::Bytes);
        }
        if durable_end.saturating_sub(oldest.0) > self.cfg.flush_wal_behind {
            return Some(FlushReason::WalBehind);
        }
        if age >= self.cfg.flush_age && self.mem.bytes() >= self.cfg.small_floor {
            return Some(FlushReason::Age);
        }
        None
    }

    /// Drain the memtable into one delta layer, publish it in the
    /// shard manifest, and advance `disk_consistent_lsn` to the resume
    /// point. `None` when there is nothing to flush.
    ///
    /// Crash safe at every step: the layer object is immutable and
    /// named by its coverage, so a retry after a crash rebuilds the
    /// identical object from the replayed WAL and adopts the existing
    /// copy, and the manifest merge is idempotent.
    pub fn flush(
        &mut self,
        store: &dyn CasStore,
        layout: &TenantLayout,
    ) -> Result<Option<LayerEntry>, IngestError> {
        if self.mem.is_empty() {
            return Ok(None);
        }
        let entries = self.mem.drain_sorted();
        self.first_insert = None;
        let (bytes, footer) = build_delta(&entries, self.cfg.block_target)?;
        let desc = LayerDesc::from_footer(&footer, bytes.len() as u64);
        let shard = self.cfg.shard as u16;
        let key = format!("{}{}", layout.shard_prefix(shard), desc.name());
        match store.put_if_absent(&key, &bytes) {
            Ok(_) => {}
            // The same WAL built the same bytes under the same name on
            // an earlier attempt, adopt them.
            Err(CasError::AlreadyExists { .. }) => {}
            Err(e) => return Err(e.into()),
        }
        let entry = LayerEntry {
            name: desc.name(),
            size: bytes.len() as u64,
            owner: None,
            upto: None,
        };
        shardmanifest::publish_layer(
            store,
            &layout.shard_manifest(shard),
            shard,
            &entry,
            Lsn(self.applied),
        )?;
        Ok(Some(entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::walscan::BlockRef;
    use crate::walscan::testwal::Builder;
    use zou_store::mem::MemStore;
    use zou_store::pageread::LayerReader;
    use zou_store::shardmanifest::PageShardManifest;

    const WAL_BASE: u64 = 16 << 20;
    const TENANT: u128 = 7;

    /// The smallest relfilenode whose stripe is `shard` of two.
    fn rel_on(shard: u32) -> u32 {
        (1u32..)
            .find(|&rel| {
                StripeRef {
                    relfilenode: rel,
                    fork: 0,
                    block: 0,
                }
                .page_shard(2)
                    == shard
            })
            .unwrap()
    }

    fn blk(rel: u32, blk: u32) -> BlockRef {
        BlockRef {
            spc: 1663,
            db: 5,
            rel,
            fork: 0,
            blk,
        }
    }

    fn frame(start: u64, payload: &[u8]) -> Frame2 {
        Frame2 {
            tenant: TENANT,
            writer_epoch: 1,
            start_lsn: Lsn(start),
            end_lsn: Lsn(start + payload.len() as u64),
            contains_commit: false,
            first_of_epoch: false,
            hints: Vec::new(),
            payload: payload.to_vec(),
        }
    }

    fn cfg(shard: u32) -> IngestConfig {
        IngestConfig::new(TENANT, shard, 2)
    }

    #[test]
    fn records_index_under_their_keys_and_foreign_stripes_stay_out() {
        let (mine, theirs) = (rel_on(0), rel_on(1));
        let mut wal = Builder::new(WAL_BASE);
        wal.record(&[(blk(mine, 3), false), (blk(theirs, 3), false)], b"ab");
        wal.record(&[(blk(mine, 3), false)], b"cd");
        wal.record(&[(blk(theirs, 9), false)], b"ef");
        let (base, bytes) = wal.stream();

        let mut ingest = ShardIngest::new(cfg(0), WAL_BASE);
        ingest.apply_frames(&[frame(base, bytes)]).unwrap();

        assert_eq!(ingest.applied(), wal.pos());
        let key = LayerKey::page(1663, 5, mine, 0, 3);
        let records: Vec<_> = ingest
            .memtable()
            .records_for(&key, Lsn(0), Lsn(u64::MAX))
            .collect();
        assert_eq!(records.len(), 2, "both records touching our block land");
        assert_eq!(
            ingest.memtable().len(),
            2,
            "the foreign stripe's records stay out"
        );
    }

    #[test]
    fn frames_split_mid_record_replay_exactly_once_and_gaps_are_loud() {
        let mine = rel_on(0);
        let mut wal = Builder::new(WAL_BASE);
        wal.record(&[(blk(mine, 1), false)], &[0x11; 100]);
        wal.record(&[(blk(mine, 2), false)], &[0x22; 100]);
        let (base, bytes) = wal.stream();
        let frames: Vec<Frame2> = bytes
            .chunks(7)
            .enumerate()
            .map(|(i, c)| frame(base + 7 * i as u64, c))
            .collect();

        let mut ingest = ShardIngest::new(cfg(0), WAL_BASE);
        for f in &frames {
            ingest.apply_frames(std::slice::from_ref(f)).unwrap();
        }
        assert_eq!(ingest.memtable().len(), 2);
        assert_eq!(ingest.applied(), wal.pos());

        // A full catch up replay overlaps everything already applied.
        ingest.apply_frames(&frames).unwrap();
        assert_eq!(ingest.memtable().len(), 2, "the watermark drops replays");

        // A frame past the buffered end means missing bytes.
        let err = ingest
            .apply_frames(&[frame(wal.pos() + 64, &[0; 8])])
            .unwrap_err();
        assert!(matches!(err, IngestError::Gap { .. }), "{err}");

        // Frames of other tenants are ignored wholesale.
        let mut foreign = frame(wal.pos(), &[0; 8]);
        foreign.tenant = TENANT + 1;
        ingest.apply_frames(&[foreign]).unwrap();
        assert_eq!(ingest.applied(), wal.pos());
    }

    /// A stream cut inside a record leaves `applied` at the last
    /// complete record but `seen` at the cut: the page service serves
    /// against `seen`, or an idle stream ending on a WAL page boundary
    /// would park every read until unrelated new WAL arrives.
    #[test]
    fn seen_tracks_the_stream_end_past_a_partial_record() {
        let mine = rel_on(0);
        let mut wal = Builder::new(WAL_BASE);
        wal.record(&[(blk(mine, 1), false)], &[0x11; 100]);
        wal.record(&[(blk(mine, 2), false)], &[0x22; 100]);
        wal.record(&[(blk(mine, 3), false)], &[0x33; 100]);
        let (base, bytes) = wal.stream();
        let cut = bytes.len() - 10;

        let mut ingest = ShardIngest::new(cfg(0), WAL_BASE);
        ingest.apply_frames(&[frame(base, &bytes[..cut])]).unwrap();
        assert_eq!(ingest.memtable().len(), 2, "the third record is partial");
        assert_eq!(ingest.seen(), base + cut as u64);
        assert!(ingest.applied() < ingest.seen());

        ingest
            .apply_frames(&[frame(base + cut as u64, &bytes[cut..])])
            .unwrap();
        assert_eq!(ingest.applied(), wal.pos());
        assert_eq!(ingest.seen(), wal.pos());
        assert_eq!(ingest.memtable().len(), 3);
    }

    #[test]
    fn flush_reasons_follow_the_thresholds_and_spare_small_tenants() {
        let mut c = cfg(0);
        c.flush_bytes = 1000;
        c.flush_wal_behind = 10_000;
        c.small_floor = 300;
        let ingest = ShardIngest::new(c, WAL_BASE);

        assert_eq!(
            ingest.flush_reason(u64::MAX, Duration::from_secs(86_400)),
            None,
            "an empty memtable never flushes"
        );

        let mine = rel_on(0);
        let mut wal = Builder::new(WAL_BASE);
        wal.record(&[(blk(mine, 1), false)], &[0; 100]);
        let (base, bytes) = wal.stream();
        let mut ingest2 = ShardIngest::new(cfg(0), WAL_BASE);
        ingest2.apply_frames(&[frame(base, bytes)]).unwrap();
        let mem_bytes = ingest2.memtable().bytes();

        let mut c = cfg(0);
        c.flush_wal_behind = 10_000;
        c.small_floor = mem_bytes + 1;
        let mut small = ShardIngest::new(c.clone(), WAL_BASE);
        small.apply_frames(&[frame(base, bytes)]).unwrap();
        assert_eq!(
            small.flush_reason(WAL_BASE + 5_000, FLUSH_AGE * 10),
            None,
            "below the floor, age never triggers: the sealed wal is the tail"
        );
        c.small_floor = mem_bytes;
        let mut aged = ShardIngest::new(c.clone(), WAL_BASE);
        aged.apply_frames(&[frame(base, bytes)]).unwrap();
        assert_eq!(
            aged.flush_reason(WAL_BASE + 5_000, FLUSH_AGE),
            Some(FlushReason::Age)
        );
        assert_eq!(
            aged.flush_reason(WAL_BASE + 20_000, Duration::ZERO),
            Some(FlushReason::WalBehind),
            "the oldest unflushed record fell too far behind the durable end"
        );
        c.flush_bytes = mem_bytes;
        let mut fat = ShardIngest::new(c, WAL_BASE);
        fat.apply_frames(&[frame(base, bytes)]).unwrap();
        assert_eq!(
            fat.flush_reason(WAL_BASE, Duration::ZERO),
            Some(FlushReason::Bytes)
        );
    }

    #[test]
    fn a_flush_publishes_a_layer_the_read_path_serves() {
        let mine = rel_on(0);
        let mut wal = Builder::new(WAL_BASE);
        for i in 0..4u8 {
            wal.record(&[(blk(mine, 3), false)], &[i; 50]);
        }
        wal.record(&[(blk(mine, 8), false)], &[9; 50]);
        let (base, bytes) = wal.stream();
        // The expected lsns come from a plain record walk: a record's
        // lsn starts past any page header, not at the write position.
        let walked =
            walscan::read_records(&WalWindow::from_raw(base, bytes.to_vec()), WAL_BASE, None)
                .unwrap();
        let lsns: Vec<u64> = walked.records[..4].iter().map(|r| r.lsn).collect();

        let store = MemStore::default();
        let layout = TenantLayout::new("t");
        let mut ingest = ShardIngest::new(cfg(0), WAL_BASE);
        ingest.apply_frames(&[frame(base, bytes)]).unwrap();
        let entry = ingest.flush(&store, &layout).unwrap().unwrap();
        assert!(ingest.memtable().is_empty());
        assert_eq!(ingest.flush(&store, &layout).unwrap(), None);

        let (manifest, _) = PageShardManifest::load(&store, &layout.shard_manifest(0))
            .unwrap()
            .unwrap();
        assert_eq!(manifest.layers, vec![entry]);
        assert_eq!(manifest.disk_consistent_lsn, Lsn(ingest.applied()));

        let map = manifest.layer_map().unwrap();
        let reader = LayerReader::new(&store, layout.shard_prefix(0));
        let key = LayerKey::page(1663, 5, mine, 0, 3);
        let got = reader
            .reconstruct(&map, &Memtable::new(), &key, Lsn(u64::MAX))
            .unwrap();
        assert_eq!(
            got.records.iter().map(|(l, _)| l.0).collect::<Vec<_>>(),
            lsns,
            "every record for the key comes back in lsn order"
        );
        assert!(got.base.is_none(), "no image layer yet");
        assert_eq!(got.layers_touched, 1);
    }

    #[test]
    fn a_crash_retry_reflushes_identically_and_duplicates_nothing() {
        let mine = rel_on(0);
        let mut wal = Builder::new(WAL_BASE);
        wal.record(&[(blk(mine, 1), false)], &[7; 64]);
        wal.record(&[(blk(mine, 2), false)], &[8; 64]);
        let (base, bytes) = wal.stream();

        let store = MemStore::default();
        let layout = TenantLayout::new("t");
        let mut first = ShardIngest::new(cfg(0), WAL_BASE);
        first.apply_frames(&[frame(base, bytes)]).unwrap();
        let a = first.flush(&store, &layout).unwrap().unwrap();

        // The process dies before recording anything locally and comes
        // back, replaying the same sealed wal from the same boundary.
        let mut retry = ShardIngest::new(cfg(0), WAL_BASE);
        retry.apply_frames(&[frame(base, bytes)]).unwrap();
        let b = retry.flush(&store, &layout).unwrap().unwrap();
        assert_eq!(a, b, "the replay rebuilds the identical layer");

        let (manifest, _) = PageShardManifest::load(&store, &layout.shard_manifest(0))
            .unwrap()
            .unwrap();
        assert_eq!(manifest.layers.len(), 1);
    }

    #[test]
    fn lag_measures_bytes_behind_the_durable_end_without_underflow() {
        let ingest = ShardIngest::new(cfg(0), WAL_BASE);
        assert_eq!(ingest.lag(WAL_BASE + 500), 500);
        assert_eq!(ingest.lag(WAL_BASE), 0);
        assert_eq!(
            ingest.lag(0),
            0,
            "a durable end behind the resume point reads as caught up"
        );
    }

    /// The wedge the scale 1000 init found: a flush position that ends
    /// inside a record leaves `applied` a few bytes short of durable
    /// forever if writes pause there. Those bytes are received, not
    /// lagging, so the gauge measures from `seen` and a pause reads as
    /// caught up instead of ten seconds later throttling the tenant
    /// with nothing left to lift it.
    #[test]
    fn a_partial_record_at_the_stream_end_is_not_lag() {
        let mine = rel_on(0);
        let mut wal = Builder::new(WAL_BASE);
        wal.record(&[(blk(mine, 1), false)], &[0x11; 100]);
        wal.record(&[(blk(mine, 2), false)], &[0x22; 100]);
        let (base, bytes) = wal.stream();
        let cut = bytes.len() - 10;

        let mut ingest = ShardIngest::new(cfg(0), WAL_BASE);
        ingest.apply_frames(&[frame(base, &bytes[..cut])]).unwrap();
        let durable = base + cut as u64;
        assert!(ingest.applied() < durable, "the second record is partial");
        assert_eq!(ingest.lag(durable), 0, "every durable byte is received");
        assert_eq!(
            ingest.lag(durable + 64),
            64,
            "bytes past the received end still count"
        );
    }

    #[test]
    fn a_lag_cut_surfaces_as_the_catch_up_signal() {
        let mut ingest = ShardIngest::new(cfg(0), WAL_BASE);
        let err = ingest
            .apply_event(&TeeEvent::Lagged { next_seq: 42 })
            .unwrap_err();
        assert!(matches!(err, IngestError::Lagged { next_seq: 42 }), "{err}");
    }
}
