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

use zou_store::layout::TenantLayout;
use zou_store::{CasStore, SegmentReader};

use crate::restore::WAL_SEGMENT_SIZE;

const XLOG_BLCKSZ: u64 = 8192;
/// XLOG_PAGE_MAGIC for the vendored Postgres 18 WAL format.
const XLOG_PAGE_MAGIC: u16 = 0xD118;
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

/// A block a WAL record references. Orders by relation then block,
/// which is the page run order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockRef {
    pub spc: u32,
    pub db: u32,
    pub rel: u32,
    pub fork: u32,
    pub blk: u32,
}

/// Mirrored stream bytes reassembled over `[base, base + buf.len())` in
/// Postgres LSN space. `covered_from` is where coverage actually starts,
/// the stream may begin after the requested base when older bytes were
/// never pushed, the genesis case.
pub struct WalWindow {
    base: u64,
    buf: Vec<u8>,
    pub covered_from: u64,
}

/// Read every chunk the named stream segments hold and lay the ones
/// intersecting `[from, to)` into one contiguous window.
pub fn assemble_window(
    store: &dyn CasStore,
    layout: &TenantLayout,
    segments: &[String],
    from: u64,
    to: u64,
) -> Result<WalWindow, String> {
    let mut buf = vec![0u8; (to.saturating_sub(from)) as usize];
    let mut covered_from = to;
    for name in segments {
        let epoch = zou_store::commit::segment_epoch(name)
            .ok_or_else(|| format!("bad segment name {name:?}"))?;
        let Some((bytes, _)) = store
            .get(&layout.wal_segment_path(name))
            .map_err(|e| format!("store: {e}"))?
        else {
            continue;
        };
        for frame in SegmentReader::new(&bytes, epoch) {
            let frame = frame.map_err(|e| format!("segment {name}: {e}"))?;
            let records = zou_store::commit::split_records(&frame.payload)
                .ok_or_else(|| format!("bad batch in {name}"))?;
            for record in records {
                if record.len() < 8 {
                    return Err(format!("short record in {name}"));
                }
                let lsn = u64::from_le_bytes(record[..8].try_into().expect("checked length"));
                let wal = &record[8..];
                let (start, end) = (lsn.max(from), (lsn + wal.len() as u64).min(to));
                if start >= end {
                    continue;
                }
                covered_from = covered_from.min(start);
                let src = (start - lsn) as usize..(end - lsn) as usize;
                let dst = (start - from) as usize..(end - from) as usize;
                buf[dst].copy_from_slice(&wal[src]);
            }
        }
    }
    Ok(WalWindow {
        base: from,
        buf,
        covered_from,
    })
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

    fn at(&self, pos: u64, len: usize) -> Result<&'a [u8], String> {
        let off = (pos - self.window.base) as usize;
        self.window
            .buf
            .get(off..off + len)
            .ok_or_else(|| format!("wal window ends inside data at {pos:#X}"))
    }

    /// Skip the page header if the cursor sits on a page boundary.
    fn skip_page_header(&mut self) -> Result<(), String> {
        if !self.pos.is_multiple_of(XLOG_BLCKSZ) {
            return Ok(());
        }
        let header = self.at(self.pos, 4)?;
        let magic = u16::from_le_bytes(header[..2].try_into().expect("checked length"));
        if magic != XLOG_PAGE_MAGIC {
            return Err(format!(
                "bad wal page magic {magic:#06X} at {:#X}",
                self.pos
            ));
        }
        let info = u16::from_le_bytes(header[2..4].try_into().expect("checked length"));
        if info & 0x0008 != 0 {
            // XLP_FIRST_IS_OVERWRITE_CONTRECORD only appears when local
            // WAL diverged from the stream, which reattach via restore
            // prevents. Refusing beats misparsing.
            return Err(format!("overwrite contrecord at {:#X}", self.pos));
        }
        self.pos += if self.pos.is_multiple_of(WAL_SEGMENT_SIZE) {
            LONG_PHD
        } else {
            SHORT_PHD
        };
        Ok(())
    }

    /// Read `len` record bytes into `out`, hopping page headers.
    fn read(&mut self, len: u64, out: &mut Vec<u8>) -> Result<(), String> {
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
    fn skip(&mut self, len: u64) -> Result<(), String> {
        let mut remaining = len;
        while remaining > 0 {
            self.skip_page_header()?;
            let run = (XLOG_BLCKSZ - self.pos % XLOG_BLCKSZ).min(remaining);
            if self.pos + run > self.end() {
                return Err(format!("wal window ends inside record at {:#X}", self.pos));
            }
            self.pos += run;
            remaining -= run;
        }
        Ok(())
    }
}

/// Parse the block references out of one record's header bytes,
/// following DecodeXLogRecord: items run from after the fixed header
/// until the main data header, which is always last.
fn record_block_refs(header: &[u8], tot_len: u64, out: &mut Vec<BlockRef>) -> Result<(), String> {
    let mut p = 0usize;
    let mut remaining = tot_len - RECORD_HEADER;
    let mut datatotal = 0u64;
    let mut rel: Option<(u32, u32, u32)> = None;
    let take = |p: &mut usize, n: usize| -> Result<&[u8], String> {
        let s = header
            .get(*p..*p + n)
            .ok_or_else(|| "record header truncated".to_string())?;
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
                let (spc, db, relnum) =
                    rel.ok_or_else(|| "same rel flag with no prior relation".to_string())?;
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
            _ => return Err(format!("unknown block id {id} in record header")),
        }
    }
    Ok(())
}

/// Walk the records in `[start, end)` and collect every block they
/// reference, sorted and deduplicated. `start` and `end` must both be
/// record boundaries, which checkpoint redo locations are.
pub fn scan_block_refs(window: &WalWindow, start: u64, end: u64) -> Result<Vec<BlockRef>, String> {
    let mut cursor = Cursor { window, pos: start };
    let mut refs = Vec::new();
    let mut header = Vec::new();
    while cursor.pos < end {
        let rec_start = cursor.pos;
        // Zero bytes where a record should begin are xlog switch
        // padding, the rest of the segment is unused and the stream
        // resumes behind the next 16MB boundary's long header.
        if cursor.at(rec_start, 4)? == [0, 0, 0, 0] {
            cursor.pos = rec_start / WAL_SEGMENT_SIZE * WAL_SEGMENT_SIZE + WAL_SEGMENT_SIZE;
            continue;
        }
        cursor.skip_page_header()?;
        if cursor.pos >= end {
            break;
        }
        header.clear();
        cursor.read(RECORD_HEADER, &mut header)?;
        let tot_len = u64::from(u32::from_le_bytes(
            header[..4].try_into().expect("checked length"),
        ));
        if tot_len < RECORD_HEADER {
            return Err(format!("bad record length {tot_len} at {rec_start:#X}"));
        }
        // Only the header items matter, capped well above the biggest
        // possible header region, 33 block references at 46 bytes each.
        let body = tot_len - RECORD_HEADER;
        let head = body.min(4096);
        header.clear();
        cursor.read(head, &mut header)?;
        record_block_refs(&header, tot_len, &mut refs)?;
        cursor.skip(body - head)?;
        cursor.pos = (cursor.pos + MAXALIGN - 1) & !(MAXALIGN - 1);
    }
    refs.sort();
    refs.dedup();
    Ok(refs)
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
            record.push(0);
            record.push(0);
            record.extend_from_slice(&[0, 0]);
            record.extend_from_slice(&0u32.to_le_bytes());
            record.extend_from_slice(&items);
            record.resize(tot_len as usize, 0x5A);
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
