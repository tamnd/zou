//! Scan mirrored WAL for the blocks its records touch.
//!
//! The fold needs the set of pages dirtied between two checkpoints to
//! pack them into page runs, and the write gate guarantees every page
//! mutation in the store has its WAL in the mirrored stream first, so
//! the stream is a complete dirty page log. This module reassembles the
//! stream bytes over an LSN window and walks the records, reading only
//! the record headers: the fixed XLogRecord, then the block reference
//! headers up to the main data header, exactly the layout xlogrecord.h
//! documents for Postgres 18.
//!
//! Record payloads are never decoded and record crcs are not checked,
//! the transport frames already carry crc32c over every chunk. Page
//! headers interrupt the record stream every 8KB and a long header
//! opens every 16MB segment, the cursor skips them and validates the
//! page magic as it goes.

use zou_store::Frame2;

use crate::restore::WAL_SEGMENT_SIZE;

const XLOG_BLCKSZ: u64 = 8192;
/// XLOG_PAGE_MAGIC for the vendored Postgres 18 WAL format.
pub(crate) const XLOG_PAGE_MAGIC: u16 = 0xD118;
const SHORT_PHD: u64 = 24;
const LONG_PHD: u64 = 40;
const RECORD_HEADER: u64 = 24;
const MAXALIGN: u64 = 8;

const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
const XLR_BLOCK_ID_DATA_LONG: u8 = 254;
const XLR_BLOCK_ID_ORIGIN: u8 = 253;
const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;
const XLR_MAX_BLOCK_ID: u8 = 32;
const BKPBLOCK_HAS_IMAGE: u8 = 0x10;
const BKPBLOCK_SAME_REL: u8 = 0x80;
const BKPBLOCK_FORK_MASK: u8 = 0x0F;
const BKPIMAGE_HAS_HOLE: u8 = 0x01;
const BKPIMAGE_COMPRESSED: u8 = 0x04 | 0x08 | 0x10;

/// Storage rmgr, whose create and truncate records name a relation in
/// main data instead of a block reference.
const RM_SMGR_ID: u8 = 2;
const XLOG_SMGR_CREATE: u8 = 0x10;
const XLOG_SMGR_TRUNCATE: u8 = 0x20;
/// The truncate record's flag for the main fork, SMGR_TRUNCATE_HEAP.
const SMGR_TRUNCATE_HEAP: u32 = 0x0001;

/// A block a WAL record references. Orders by relation then block,
/// which is the page run order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockRef {
    pub spc: u32,
    pub db: u32,
    pub rel: u32,
    pub fork: u32,
    pub blk: u32,
}

/// A relation an smgr create record names. After a file recreation,
/// checkpoint copies of any block in the relation may be stale even
/// though no block reference ever said so.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RelTag {
    pub spc: u32,
    pub db: u32,
    pub rel: u32,
}

/// What a scan produced: the blocks the records touch, the relations
/// smgr create records recreated, the truncate events with their main
/// fork cutoff, and where the next scan should resume, the end of the
/// last complete record. A truncate is softer than a recreation: main
/// fork blocks below the cutoff keep their bytes and older checkpoint
/// copies of them stay valid, only blocks at or past it and the vm and
/// fsm forks go stale. A cutoff of `u32::MAX` means the record did not
/// truncate the main fork at all, only vm or fsm. Duplicate truncates
/// of one relation collapse to the smallest cutoff, the only blocks
/// that survived every event in the window.
pub struct ScanOut {
    pub refs: Vec<BlockRef>,
    pub rels: Vec<RelTag>,
    pub truncs: Vec<(RelTag, u32)>,
    pub resume: u64,
}

/// Mirrored stream bytes reassembled over `[base, base + buf.len())` in
/// Postgres LSN space. `covered_from` is where coverage actually starts,
/// the stream may begin after the requested base when older bytes were
/// never pushed, the genesis case.
pub struct WalWindow {
    pub(crate) base: u64,
    pub(crate) buf: Vec<u8>,
    pub covered_from: u64,
}

impl WalWindow {
    /// Wrap raw stream bytes starting at `base`, for callers that
    /// already hold a contiguous run, like a test reading pg_wal
    /// segments straight off disk.
    pub fn from_raw(base: u64, buf: Vec<u8>) -> WalWindow {
        WalWindow {
            base,
            buf,
            covered_from: base,
        }
    }
}

/// Lay the frames of one tenant's shared log stream that intersect
/// `[from, to)` into one contiguous window. A frame's payload is a raw
/// chunk of the Postgres stream and its lsn range says where the bytes
/// sit, so retried frames overlap harmlessly: later copies write the
/// same bytes.
pub fn assemble_window_frames(frames: &[Frame2], from: u64, to: u64) -> WalWindow {
    let mut buf = vec![0u8; (to.saturating_sub(from)) as usize];
    let mut covered_from = to;
    for frame in frames {
        let lsn = frame.start_lsn.0;
        let wal = &frame.payload;
        let (start, end) = (lsn.max(from), (lsn + wal.len() as u64).min(to));
        if start >= end {
            continue;
        }
        covered_from = covered_from.min(start);
        let src = (start - lsn) as usize..(end - lsn) as usize;
        let dst = (start - from) as usize..(end - from) as usize;
        buf[dst].copy_from_slice(&wal[src]);
    }
    WalWindow {
        base: from,
        buf,
        covered_from,
    }
}

/// Why a scan step could not proceed. Truncation means the window ends
/// inside the current record, which a tolerant caller treats as a clean
/// stop at the record's start. Corruption always propagates.
enum ScanErr {
    Truncated,
    Corrupt(String),
}

/// Byte cursor over a window in absolute LSN space. Skips the page
/// header at every 8KB boundary, the long form at segment starts, and
/// validates the page magic on the way through.
struct Cursor<'a> {
    window: &'a WalWindow,
    pos: u64,
}

impl<'a> Cursor<'a> {
    fn end(&self) -> u64 {
        self.window.base + self.window.buf.len() as u64
    }

    fn at(&self, pos: u64, len: usize) -> Result<&'a [u8], ScanErr> {
        let off = (pos - self.window.base) as usize;
        self.window
            .buf
            .get(off..off + len)
            .ok_or(ScanErr::Truncated)
    }

    /// Skip the page header if the cursor sits on a page boundary.
    fn skip_page_header(&mut self) -> Result<(), ScanErr> {
        if !self.pos.is_multiple_of(XLOG_BLCKSZ) {
            return Ok(());
        }
        let header = self.at(self.pos, 4)?;
        let magic = u16::from_le_bytes(header[..2].try_into().expect("checked length"));
        if magic != XLOG_PAGE_MAGIC {
            return Err(ScanErr::Corrupt(format!(
                "bad wal page magic {magic:#06X} at {:#X}",
                self.pos
            )));
        }
        let info = u16::from_le_bytes(header[2..4].try_into().expect("checked length"));
        if info & 0x0008 != 0 {
            // XLP_FIRST_IS_OVERWRITE_CONTRECORD only appears when local
            // WAL diverged from the stream, which reattach via restore
            // prevents. Refusing beats misparsing.
            return Err(ScanErr::Corrupt(format!(
                "overwrite contrecord at {:#X}",
                self.pos
            )));
        }
        self.pos += if self.pos.is_multiple_of(WAL_SEGMENT_SIZE) {
            LONG_PHD
        } else {
            SHORT_PHD
        };
        Ok(())
    }

    /// Read `len` record bytes into `out`, hopping page headers.
    fn read(&mut self, len: u64, out: &mut Vec<u8>) -> Result<(), ScanErr> {
        let mut remaining = len;
        while remaining > 0 {
            self.skip_page_header()?;
            let run = (XLOG_BLCKSZ - self.pos % XLOG_BLCKSZ).min(remaining);
            out.extend_from_slice(self.at(self.pos, run as usize)?);
            self.pos += run;
            remaining -= run;
        }
        Ok(())
    }

    /// Advance past `len` record bytes without copying.
    fn skip(&mut self, len: u64) -> Result<(), ScanErr> {
        let mut remaining = len;
        while remaining > 0 {
            self.skip_page_header()?;
            let run = (XLOG_BLCKSZ - self.pos % XLOG_BLCKSZ).min(remaining);
            if self.pos + run > self.end() {
                return Err(ScanErr::Truncated);
            }
            self.pos += run;
            remaining -= run;
        }
        Ok(())
    }
}

/// Parse the block references out of one record's header bytes,
/// following DecodeXLogRecord: items run from after the fixed header
/// until the main data header, which is always last. Smgr create and
/// truncate records carry their relation in main data with no block
/// references at all, so those surface as relation events, a hard one
/// in `rels` for a file recreation and a truncate event with its main
/// fork cutoff in `truncs`, which a reader needs to know where to stop
/// trusting checkpoint copies of the relation.
fn record_block_refs(
    header: &[u8],
    tot_len: u64,
    info: u8,
    rmid: u8,
    out: &mut Vec<BlockRef>,
    rels: &mut Vec<RelTag>,
    truncs: &mut Vec<(RelTag, u32)>,
) -> Result<bool, ScanErr> {
    let mut p = 0usize;
    let mut remaining = tot_len - RECORD_HEADER;
    let mut datatotal = 0u64;
    let mut saw_image = false;
    let mut rel: Option<(u32, u32, u32)> = None;
    let take = |p: &mut usize, n: usize| -> Result<&[u8], ScanErr> {
        let s = header
            .get(*p..*p + n)
            .ok_or_else(|| ScanErr::Corrupt("record header items overrun".to_string()))?;
        *p += n;
        Ok(s)
    };
    while remaining > datatotal {
        let id = take(&mut p, 1)?[0];
        remaining = remaining.saturating_sub(1);
        match id {
            // Main data is always the last item, nothing to collect.
            XLR_BLOCK_ID_DATA_SHORT => {
                take(&mut p, 1)?;
                break;
            }
            XLR_BLOCK_ID_DATA_LONG => {
                take(&mut p, 4)?;
                break;
            }
            XLR_BLOCK_ID_ORIGIN => {
                take(&mut p, 2)?;
                remaining = remaining.saturating_sub(2);
            }
            XLR_BLOCK_ID_TOPLEVEL_XID => {
                take(&mut p, 4)?;
                remaining = remaining.saturating_sub(4);
            }
            id if id <= XLR_MAX_BLOCK_ID => {
                let fork_flags = take(&mut p, 1)?[0];
                let data_len =
                    u16::from_le_bytes(take(&mut p, 2)?.try_into().expect("checked length"));
                remaining = remaining.saturating_sub(3);
                datatotal += data_len as u64;
                if fork_flags & BKPBLOCK_HAS_IMAGE != 0 {
                    saw_image = true;
                    let img = take(&mut p, 5)?;
                    let length =
                        u16::from_le_bytes(img[..2].try_into().expect("checked length")) as u64;
                    let bimg_info = img[4];
                    remaining = remaining.saturating_sub(5);
                    datatotal += length;
                    if bimg_info & BKPIMAGE_HAS_HOLE != 0 && bimg_info & BKPIMAGE_COMPRESSED != 0 {
                        take(&mut p, 2)?;
                        remaining = remaining.saturating_sub(2);
                    }
                }
                if fork_flags & BKPBLOCK_SAME_REL == 0 {
                    let loc = take(&mut p, 12)?;
                    rel = Some((
                        u32::from_le_bytes(loc[..4].try_into().expect("checked length")),
                        u32::from_le_bytes(loc[4..8].try_into().expect("checked length")),
                        u32::from_le_bytes(loc[8..12].try_into().expect("checked length")),
                    ));
                    remaining = remaining.saturating_sub(12);
                }
                let (spc, db, relnum) = rel.ok_or_else(|| {
                    ScanErr::Corrupt("same rel flag with no prior relation".to_string())
                })?;
                let blk = u32::from_le_bytes(take(&mut p, 4)?.try_into().expect("checked length"));
                remaining = remaining.saturating_sub(4);
                out.push(BlockRef {
                    spc,
                    db,
                    rel: relnum,
                    fork: (fork_flags & BKPBLOCK_FORK_MASK) as u32,
                    blk,
                });
            }
            _ => {
                return Err(ScanErr::Corrupt(format!(
                    "unknown block id {id} in record header"
                )));
            }
        }
    }
    if rmid == RM_SMGR_ID {
        // Main data sits after the block data region. Smgr records have
        // no block references so datatotal is zero in practice, the add
        // keeps the offset honest anyway.
        let main = p + datatotal as usize;
        let field = |at: usize| -> Result<u32, ScanErr> {
            header
                .get(at..at + 4)
                .map(|s| u32::from_le_bytes(s.try_into().expect("checked length")))
                .ok_or_else(|| ScanErr::Corrupt("smgr record too short".to_string()))
        };
        match info & 0xF0 {
            XLOG_SMGR_CREATE => rels.push(RelTag {
                spc: field(main)?,
                db: field(main + 4)?,
                rel: field(main + 8)?,
            }),
            // xl_smgr_truncate: the surviving block count, the locator,
            // then which forks the truncate hit. Without the heap flag
            // the main fork was not touched, only vm or fsm, and the
            // MAX cutoff says every main block survived.
            XLOG_SMGR_TRUNCATE => {
                let nblocks = field(main)?;
                let tag = RelTag {
                    spc: field(main + 4)?,
                    db: field(main + 8)?,
                    rel: field(main + 12)?,
                };
                let flags = field(main + 16)?;
                let cut = if flags & SMGR_TRUNCATE_HEAP != 0 {
                    nblocks
                } else {
                    u32::MAX
                };
                truncs.push((tag, cut));
            }
            _ => {}
        }
    }
    Ok(saw_image)
}

/// The scan loop shared by the strict and tolerant entry points. With
/// `end` set, running out of window before it is an error. Without it,
/// the scan consumes complete records until the window ends inside one
/// and reports where it stopped, which is how a reader tails a stream
/// whose last frame can end mid record.
fn scan(window: &WalWindow, start: u64, end: Option<u64>) -> Result<ScanOut, String> {
    let mut cursor = Cursor { window, pos: start };
    let limit = end.unwrap_or_else(|| cursor.end());
    let mut out = ScanOut {
        refs: Vec::new(),
        rels: Vec::new(),
        truncs: Vec::new(),
        resume: start,
    };
    let mut header = Vec::new();
    while cursor.pos < limit {
        let rec_start = cursor.pos;
        let step = (|cursor: &mut Cursor| -> Result<(), ScanErr> {
            // Zero bytes where a record should begin are xlog switch
            // padding, the rest of the segment is unused and the stream
            // resumes behind the next 16MB boundary's long header.
            if cursor.at(rec_start, 4)? == [0, 0, 0, 0] {
                cursor.pos = rec_start / WAL_SEGMENT_SIZE * WAL_SEGMENT_SIZE + WAL_SEGMENT_SIZE;
                return Ok(());
            }
            cursor.skip_page_header()?;
            if cursor.pos >= limit {
                return Ok(());
            }
            header.clear();
            cursor.read(RECORD_HEADER, &mut header)?;
            let tot_len = u64::from(u32::from_le_bytes(
                header[..4].try_into().expect("checked length"),
            ));
            if tot_len < RECORD_HEADER {
                return Err(ScanErr::Corrupt(format!(
                    "bad record length {tot_len} at {rec_start:#X}"
                )));
            }
            let info = header[16];
            let rmid = header[17];
            // Only the header items matter, capped well above the
            // biggest possible header region, 33 block references at 46
            // bytes each.
            let body = tot_len - RECORD_HEADER;
            let head = body.min(4096);
            header.clear();
            cursor.read(head, &mut header)?;
            record_block_refs(
                &header,
                tot_len,
                info,
                rmid,
                &mut out.refs,
                &mut out.rels,
                &mut out.truncs,
            )?;
            cursor.skip(body - head)?;
            cursor.pos = (cursor.pos + MAXALIGN - 1) & !(MAXALIGN - 1);
            Ok(())
        })(&mut cursor);
        match step {
            Ok(()) => out.resume = cursor.pos,
            Err(ScanErr::Truncated) if end.is_none() => break,
            Err(ScanErr::Truncated) => {
                return Err(format!("wal window ends inside record at {rec_start:#X}"));
            }
            Err(ScanErr::Corrupt(msg)) => return Err(msg),
        }
    }
    out.refs.sort();
    out.refs.dedup();
    out.rels.sort();
    out.rels.dedup();
    // Ascending sort puts the smallest cutoff first per relation, and
    // only blocks below every cutoff in the window survived them all.
    out.truncs.sort();
    out.truncs.dedup_by(|b, a| b.0 == a.0);
    Ok(out)
}

/// Walk the records in `[start, end)` and collect every block they
/// reference, sorted and deduplicated. `start` and `end` must both be
/// record boundaries, which checkpoint redo locations are.
pub fn scan_block_refs(window: &WalWindow, start: u64, end: u64) -> Result<Vec<BlockRef>, String> {
    scan_range(window, start, end).map(|out| out.refs)
}

/// Like [`scan_block_refs`] but with the relation events too, which the
/// fold persists so the read path knows where a truncate or a file
/// recreation invalidated older checkpoint copies of a relation.
pub fn scan_range(window: &WalWindow, start: u64, end: u64) -> Result<ScanOut, String> {
    scan(window, start, Some(end))
}

/// Walk complete records from `start` to wherever the window stops
/// covering them, and report the resume point along with everything the
/// records touch. A record the window ends inside is left for the next
/// call, which is safe for dirty tracking: the write gate holds page
/// writes until their record is fully durable, so a partially mirrored
/// record cannot have pages in the store yet.
pub fn scan_available(window: &WalWindow, start: u64) -> Result<ScanOut, String> {
    scan(window, start, None)
}

/// The aligned end of a record `tot_len` bytes long starting at `lsn`,
/// the value [`read_records`] reports as `end_lsn`. Page headers the
/// record crosses count, and the end is maxaligned because that is
/// where the next record starts. Redo stamps this lsn into pages, and
/// layers store records keyed by start lsn only, so the page service
/// recomputes it when it builds a redo batch. It can, because the WAL
/// geometry is fixed.
pub fn record_end(lsn: u64, tot_len: u64) -> u64 {
    let mut pos = lsn;
    let mut remaining = tot_len;
    while remaining > 0 {
        if pos.is_multiple_of(XLOG_BLCKSZ) {
            pos += if pos.is_multiple_of(WAL_SEGMENT_SIZE) {
                LONG_PHD
            } else {
                SHORT_PHD
            };
        }
        let run = (XLOG_BLCKSZ - pos % XLOG_BLCKSZ).min(remaining);
        pos += run;
        remaining -= run;
    }
    (pos + MAXALIGN - 1) & !(MAXALIGN - 1)
}

/// One complete record pulled out of a window: the raw bytes with page
/// headers stripped, which is exactly the contiguous form xlogreader
/// hands a redo routine and what the redo pool ships to its workers.
pub struct WalRecord {
    /// Where the record starts, past any page header.
    pub lsn: u64,
    /// The aligned end, which is the next record's start.
    pub end_lsn: u64,
    pub rmid: u8,
    pub info: u8,
    pub xid: u32,
    /// The full record, fixed header included, `xl_tot_len` long.
    pub bytes: Vec<u8>,
}

impl WalRecord {
    fn parse_refs(&self) -> Result<(Vec<BlockRef>, bool), String> {
        let mut refs = Vec::new();
        let mut rels = Vec::new();
        let mut truncs = Vec::new();
        let image = record_block_refs(
            &self.bytes[RECORD_HEADER as usize..],
            self.bytes.len() as u64,
            self.info,
            self.rmid,
            &mut refs,
            &mut rels,
            &mut truncs,
        )
        .map_err(|err| match err {
            ScanErr::Corrupt(msg) => msg,
            ScanErr::Truncated => "record header items overrun".to_string(),
        })?;
        Ok((refs, image))
    }

    /// The blocks this record references, parsed from its header items.
    pub fn block_refs(&self) -> Result<Vec<BlockRef>, String> {
        self.parse_refs().map(|(refs, _)| refs)
    }

    /// Whether any block reference in this record carries a full page
    /// image. With `full_page_writes` off nothing gets one as torn
    /// page protection anymore; the images left are the records whose
    /// redo is the image itself, log_newpage from index builds and
    /// bulk loads plus the hint bit images a checksummed cluster
    /// forces, all under the xlog rmgr.
    pub fn carries_image(&self) -> Result<bool, String> {
        self.parse_refs().map(|(_, image)| image)
    }
}

/// What [`read_records`] produced: the records and where the next call
/// should resume, the end of the last complete record.
pub struct RecordsOut {
    pub records: Vec<WalRecord>,
    pub resume: u64,
}

/// Walk complete records from `start` and return them whole, payloads
/// included. With `end` set, running out of window before it is an
/// error, without it the walk stops cleanly at the first record the
/// window ends inside, like [`scan_available`]. This is the redo pool's
/// feed: the record bytes go to a worker verbatim.
pub fn read_records(
    window: &WalWindow,
    start: u64,
    end: Option<u64>,
) -> Result<RecordsOut, String> {
    let mut cursor = Cursor { window, pos: start };
    let limit = end.unwrap_or_else(|| cursor.end());
    let mut out = RecordsOut {
        records: Vec::new(),
        resume: start,
    };
    while cursor.pos < limit {
        let rec_start = cursor.pos;
        let step = (|cursor: &mut Cursor| -> Result<Option<WalRecord>, ScanErr> {
            // Zero bytes where a record should begin are xlog switch
            // padding, see `scan`.
            if cursor.at(rec_start, 4)? == [0, 0, 0, 0] {
                cursor.pos = rec_start / WAL_SEGMENT_SIZE * WAL_SEGMENT_SIZE + WAL_SEGMENT_SIZE;
                return Ok(None);
            }
            cursor.skip_page_header()?;
            if cursor.pos >= limit {
                return Ok(None);
            }
            let lsn = cursor.pos;
            let mut bytes = Vec::new();
            cursor.read(RECORD_HEADER, &mut bytes)?;
            let tot_len = u64::from(u32::from_le_bytes(
                bytes[..4].try_into().expect("checked length"),
            ));
            if tot_len < RECORD_HEADER {
                return Err(ScanErr::Corrupt(format!(
                    "bad record length {tot_len} at {lsn:#X}"
                )));
            }
            let xid = u32::from_le_bytes(bytes[4..8].try_into().expect("checked length"));
            let info = bytes[16];
            let rmid = bytes[17];
            cursor.read(tot_len - RECORD_HEADER, &mut bytes)?;
            cursor.pos = (cursor.pos + MAXALIGN - 1) & !(MAXALIGN - 1);
            Ok(Some(WalRecord {
                lsn,
                end_lsn: cursor.pos,
                rmid,
                info,
                xid,
                bytes,
            }))
        })(&mut cursor);
        match step {
            Ok(record) => {
                out.resume = cursor.pos;
                if let Some(record) = record {
                    out.records.push(record);
                }
            }
            Err(ScanErr::Truncated) if end.is_none() => break,
            Err(ScanErr::Truncated) => {
                return Err(format!("wal window ends inside record at {rec_start:#X}"));
            }
            Err(ScanErr::Corrupt(msg)) => return Err(msg),
        }
    }
    Ok(out)
}

/// Synthetic WAL for tests, shared with the fold tests: real page
/// headers, real record header layout, garbage payloads.
#[cfg(test)]
pub(crate) mod testwal {
    use super::*;

    /// Append one record with the given block refs to a synthetic WAL
    /// stream laid out with real page headers and alignment.
    pub(crate) struct Builder {
        base: u64,
        bytes: Vec<u8>,
    }

    impl Builder {
        pub(crate) fn new(base: u64) -> Self {
            Self {
                base,
                bytes: Vec::new(),
            }
        }

        pub(crate) fn pos(&self) -> u64 {
            self.base + self.bytes.len() as u64
        }

        /// Write raw record bytes, inserting page headers at boundaries.
        fn write(&mut self, data: &[u8]) {
            for &b in data {
                if self.pos().is_multiple_of(XLOG_BLCKSZ) {
                    let long = self.pos().is_multiple_of(WAL_SEGMENT_SIZE);
                    let mut header = Vec::new();
                    header.extend_from_slice(&XLOG_PAGE_MAGIC.to_le_bytes());
                    header.extend_from_slice(&0u16.to_le_bytes());
                    header.extend_from_slice(&1u32.to_le_bytes());
                    header.extend_from_slice(&self.pos().to_le_bytes());
                    header.extend_from_slice(&0u32.to_le_bytes());
                    header.resize(if long { LONG_PHD } else { SHORT_PHD } as usize, 0);
                    self.bytes.extend_from_slice(&header);
                }
                self.bytes.push(b);
            }
        }

        pub(crate) fn record(&mut self, refs: &[(BlockRef, bool)], main_data: &[u8]) {
            self.record_with(refs, main_data, 0, 0);
        }

        pub(crate) fn record_with(
            &mut self,
            refs: &[(BlockRef, bool)],
            main_data: &[u8],
            info: u8,
            rmid: u8,
        ) {
            let mut items = Vec::new();
            let mut datatotal = 0u64;
            for (i, (r, same_rel)) in refs.iter().enumerate() {
                items.push(i as u8);
                let flags = (r.fork as u8) | if *same_rel { BKPBLOCK_SAME_REL } else { 0 };
                items.push(flags);
                items.extend_from_slice(&7u16.to_le_bytes());
                datatotal += 7;
                if !same_rel {
                    items.extend_from_slice(&r.spc.to_le_bytes());
                    items.extend_from_slice(&r.db.to_le_bytes());
                    items.extend_from_slice(&r.rel.to_le_bytes());
                }
                items.extend_from_slice(&r.blk.to_le_bytes());
            }
            items.push(XLR_BLOCK_ID_DATA_SHORT);
            items.push(main_data.len() as u8);
            let tot_len = RECORD_HEADER + items.len() as u64 + datatotal + main_data.len() as u64;
            let mut record = Vec::new();
            record.extend_from_slice(&(tot_len as u32).to_le_bytes());
            record.extend_from_slice(&7u32.to_le_bytes());
            record.extend_from_slice(&0u64.to_le_bytes());
            record.push(info);
            record.push(rmid);
            record.extend_from_slice(&[0, 0]);
            record.extend_from_slice(&0u32.to_le_bytes());
            record.extend_from_slice(&items);
            record.resize(tot_len as usize - main_data.len(), 0x5A);
            record.extend_from_slice(main_data);
            self.write(&record);
            while !self.pos().is_multiple_of(MAXALIGN) {
                self.bytes.push(0);
            }
        }

        /// One record whose single block reference carries a full page
        /// image, the shape log_newpage writes: no hole, no
        /// compression, the image bytes in the data region.
        pub(crate) fn image_record(&mut self, r: BlockRef, image: &[u8], rmid: u8) {
            let mut items = Vec::new();
            items.push(0u8);
            items.push((r.fork as u8) | BKPBLOCK_HAS_IMAGE);
            items.extend_from_slice(&0u16.to_le_bytes());
            items.extend_from_slice(&(image.len() as u16).to_le_bytes());
            items.extend_from_slice(&0u16.to_le_bytes());
            items.push(0);
            items.extend_from_slice(&r.spc.to_le_bytes());
            items.extend_from_slice(&r.db.to_le_bytes());
            items.extend_from_slice(&r.rel.to_le_bytes());
            items.extend_from_slice(&r.blk.to_le_bytes());
            items.push(XLR_BLOCK_ID_DATA_SHORT);
            items.push(0);
            let tot_len = RECORD_HEADER + items.len() as u64 + image.len() as u64;
            let mut record = Vec::new();
            record.extend_from_slice(&(tot_len as u32).to_le_bytes());
            record.extend_from_slice(&7u32.to_le_bytes());
            record.extend_from_slice(&0u64.to_le_bytes());
            record.push(0);
            record.push(rmid);
            record.extend_from_slice(&[0, 0]);
            record.extend_from_slice(&0u32.to_le_bytes());
            record.extend_from_slice(&items);
            record.extend_from_slice(image);
            self.write(&record);
            while !self.pos().is_multiple_of(MAXALIGN) {
                self.bytes.push(0);
            }
        }

        /// The raw stream bytes and their base LSN, one pushable chunk.
        pub(crate) fn stream(&self) -> (u64, &[u8]) {
            (self.base, &self.bytes)
        }

        pub(crate) fn window(self) -> WalWindow {
            WalWindow {
                base: self.base,
                covered_from: self.base,
                buf: self.bytes,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testwal::Builder;
    use super::*;

    fn blk(rel: u32, blk: u32) -> BlockRef {
        BlockRef {
            spc: 1663,
            db: 5,
            rel,
            fork: 0,
            blk,
        }
    }

    #[test]
    fn the_scan_collects_sorted_deduplicated_block_refs() {
        // Start on a fresh segment so the long header path runs too.
        let mut b = Builder::new(WAL_SEGMENT_SIZE);
        b.record(&[(blk(16384, 3), false)], b"one");
        b.record(&[(blk(16384, 1), false), (blk(16384, 2), true)], b"two");
        b.record(&[(blk(999, 0), false)], b"three");
        b.record(&[(blk(16384, 3), false)], b"again");
        let end = b.pos();
        let window = b.window();

        let refs = scan_block_refs(&window, WAL_SEGMENT_SIZE, end).unwrap();
        assert_eq!(
            refs,
            vec![blk(999, 0), blk(16384, 1), blk(16384, 2), blk(16384, 3)]
        );
    }

    #[test]
    fn a_record_reports_whether_a_block_carries_an_image() {
        let mut b = Builder::new(WAL_SEGMENT_SIZE);
        b.record(&[(blk(16384, 0), false)], b"plain");
        b.image_record(blk(16384, 1), &[0xAB; 128], 0);
        let end = b.pos();
        let window = b.window();
        let out = read_records(&window, WAL_SEGMENT_SIZE, None).unwrap();
        assert_eq!(out.records.len(), 2);
        assert!(!out.records[0].carries_image().unwrap());
        assert!(out.records[1].carries_image().unwrap());
        // The image payload does not confuse the block walk: both the
        // record's own parse and the range scan still name the block.
        assert_eq!(out.records[1].block_refs().unwrap(), vec![blk(16384, 1)]);
        let refs = scan_block_refs(&window, WAL_SEGMENT_SIZE, end).unwrap();
        assert_eq!(refs, vec![blk(16384, 0), blk(16384, 1)]);
    }

    #[test]
    fn the_scan_follows_records_across_page_boundaries() {
        let mut b = Builder::new(WAL_SEGMENT_SIZE);
        // Fat main data pushes later records across several 8KB pages.
        let filler = vec![0x42u8; 6000];
        for i in 0..6 {
            b.record(&[(blk(20000 + i, i), false)], &filler);
        }
        let end = b.pos();
        assert!(end - WAL_SEGMENT_SIZE > 4 * XLOG_BLCKSZ, "spans pages");
        let window = b.window();

        let refs = scan_block_refs(&window, WAL_SEGMENT_SIZE, end).unwrap();
        assert_eq!(refs.len(), 6);
        assert_eq!(refs[0], blk(20000, 0));
        assert_eq!(refs[5], blk(20005, 5));
    }

    #[test]
    fn record_end_recomputes_what_read_records_reported() {
        let mut b = Builder::new(WAL_SEGMENT_SIZE);
        // Sizes all over the place so records land inside pages, end
        // exactly on boundaries, and cross one or several pages.
        for i in 0..40u32 {
            let filler = vec![0x77u8; (i * 397 % 6100) as usize];
            b.record(&[(blk(30000 + i, i), false)], &filler);
        }
        let end = b.pos();
        let window = b.window();
        let out = read_records(&window, WAL_SEGMENT_SIZE, Some(end)).unwrap();
        assert_eq!(out.records.len(), 40);
        assert!(
            out.records
                .iter()
                .any(|r| r.lsn / XLOG_BLCKSZ != (r.end_lsn - 1) / XLOG_BLCKSZ),
            "some record crosses a page"
        );
        for r in &out.records {
            assert_eq!(
                record_end(r.lsn, r.bytes.len() as u64),
                r.end_lsn,
                "record at {:#x}",
                r.lsn
            );
        }
    }

    #[test]
    fn the_tolerant_scan_stops_before_an_incomplete_record_and_resumes() {
        let mut b = Builder::new(WAL_SEGMENT_SIZE);
        b.record(&[(blk(16384, 7), false)], b"whole");
        let boundary = b.pos();
        b.record(&[(blk(16384, 8), false)], b"cut off");
        let full_end = b.pos();
        let mut window = b.window();
        // Chop the second record in half: only the first one is complete.
        window
            .buf
            .truncate((boundary - WAL_SEGMENT_SIZE + 10) as usize);

        let out = scan_available(&window, WAL_SEGMENT_SIZE).unwrap();
        assert_eq!(out.refs, vec![blk(16384, 7)]);
        assert_eq!(out.resume, boundary);

        // With the rest of the bytes present, resuming picks up the
        // record that was cut off, and nothing is double counted.
        let mut b2 = Builder::new(WAL_SEGMENT_SIZE);
        b2.record(&[(blk(16384, 7), false)], b"whole");
        b2.record(&[(blk(16384, 8), false)], b"cut off");
        let out2 = scan_available(&b2.window(), out.resume).unwrap();
        assert_eq!(out2.refs, vec![blk(16384, 8)]);
        assert_eq!(out2.resume, full_end);
    }

    #[test]
    fn smgr_create_and_truncate_surface_as_relation_events() {
        let mut b = Builder::new(WAL_SEGMENT_SIZE);
        // xl_smgr_create: locator then fork number.
        let mut create = Vec::new();
        create.extend_from_slice(&1663u32.to_le_bytes());
        create.extend_from_slice(&5u32.to_le_bytes());
        create.extend_from_slice(&24000u32.to_le_bytes());
        create.extend_from_slice(&0u32.to_le_bytes());
        b.record_with(&[], &create, 0x10, 2);
        // xl_smgr_truncate: block count first, then the locator.
        let mut trunc = Vec::new();
        trunc.extend_from_slice(&3u32.to_le_bytes());
        trunc.extend_from_slice(&1663u32.to_le_bytes());
        trunc.extend_from_slice(&5u32.to_le_bytes());
        trunc.extend_from_slice(&16384u32.to_le_bytes());
        trunc.extend_from_slice(&7u32.to_le_bytes());
        b.record_with(&[], &trunc, 0x20, 2);
        b.record(&[(blk(999, 1), false)], b"plain");

        let out = scan_available(&b.window(), WAL_SEGMENT_SIZE).unwrap();
        assert_eq!(out.refs, vec![blk(999, 1)]);
        assert_eq!(
            out.rels,
            vec![RelTag {
                spc: 1663,
                db: 5,
                rel: 24000
            }],
            "only the create is a hard relation event"
        );
        assert_eq!(
            out.truncs,
            vec![(
                RelTag {
                    spc: 1663,
                    db: 5,
                    rel: 16384
                },
                3
            )],
            "the truncate carries its main fork cutoff"
        );
    }

    #[test]
    fn truncate_cutoffs_collapse_to_the_minimum_and_honor_the_heap_flag() {
        let mut b = Builder::new(WAL_SEGMENT_SIZE);
        let trunc = |nblocks: u32, rel: u32, flags: u32| {
            let mut d = Vec::new();
            d.extend_from_slice(&nblocks.to_le_bytes());
            d.extend_from_slice(&1663u32.to_le_bytes());
            d.extend_from_slice(&5u32.to_le_bytes());
            d.extend_from_slice(&rel.to_le_bytes());
            d.extend_from_slice(&flags.to_le_bytes());
            d
        };
        b.record_with(&[], &trunc(9, 16384, 7), 0x20, 2);
        b.record_with(&[], &trunc(4, 16384, 7), 0x20, 2);
        // A vm only truncate leaves the main fork alone.
        b.record_with(&[], &trunc(0, 20000, 2), 0x20, 2);

        let out = scan_available(&b.window(), WAL_SEGMENT_SIZE).unwrap();
        let tag = |rel| RelTag {
            spc: 1663,
            db: 5,
            rel,
        };
        assert_eq!(out.truncs, vec![(tag(16384), 4), (tag(20000), u32::MAX)]);
        assert!(out.rels.is_empty());
    }

    #[test]
    fn the_scan_stops_at_zero_padding_and_rejects_bad_magic() {
        let mut b = Builder::new(WAL_SEGMENT_SIZE);
        b.record(&[(blk(1, 1), false)], b"x");
        let end = b.pos();
        let mut window = b.window();
        window.buf.resize(window.buf.len() + 64, 0);

        let refs = scan_block_refs(&window, WAL_SEGMENT_SIZE, end + 64).unwrap();
        assert_eq!(refs, vec![blk(1, 1)]);

        window.buf[0] = 0xFF;
        let err = scan_block_refs(&window, WAL_SEGMENT_SIZE, end).unwrap_err();
        assert!(err.contains("magic"));
    }
}
