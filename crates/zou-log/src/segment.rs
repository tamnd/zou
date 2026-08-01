//! Landing segment codec (spec 03 section 3).
//!
//! A landing segment is one batch window of frames from every tenant on
//! a shard, interleaved, plus a footer that summarizes the batch per
//! tenant: lsn range, frame count, and the byte runs the tenant's
//! frames occupy. The footer is what makes shared segments cheap to
//! consume later: consolidation plans per tenant range reads from
//! footers alone and never touches payloads it does not need.
//!
//! Integrity is layered. Every frame carries its own crc32c, and the
//! footer crc covers the header bytes and the footer body, so a flipped
//! bit anywhere in a segment fails decode: frame region flips fail the
//! frame crc, everything else fails the footer crc or the magic checks.
//! The decoder never panics and never allocates on a lying count.

use std::collections::BTreeMap;

use zou_store::{Frame2, Frame2DecodeError, Frame2Stream, Lsn};

pub const SEGMENT_MAGIC: [u8; 4] = *b"ZSEG";
pub const FOOTER_MAGIC: [u8; 4] = *b"ZSGF";
pub const SEGMENT_VERSION: u16 = 1;

/// magic, version, kind, shard, seq, prev tenants digest.
const HEADER_LEN: usize = 4 + 2 + 2 + 4 + 8 + 8;
/// footer body length, crc, footer magic.
const TAIL_LEN: usize = 4 + 4 + 4;
/// tenant, min lsn, max lsn, frame count, run count.
const SUMMARY_FIXED_LEN: usize = 16 + 8 + 8 + 4 + 4;
const RUN_LEN: usize = 8 + 8;

/// What a chain position holds. Landing segments carry frames, a seal
/// is an empty segment a successor PUTs to fence the old sequencer out
/// of the chain (spec 03 section 5). The kind lives in the header so a
/// reader walking the chain can skip seals without parsing footers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentKind {
    Landing,
    Seal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    pub kind: SegmentKind,
    pub shard: u32,
    pub seq: u64,
    /// Digest of the previous segment's per tenant tail lsns, the link
    /// the chain rule uses to disambiguate a half landed zombie PUT
    /// (spec 03 section 5). Zero for the first segment of a chain.
    pub prev_digest: u64,
}

/// One contiguous byte range of a tenant's frames inside the segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantRun {
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantSummary {
    pub tenant: u128,
    pub min_lsn: Lsn,
    pub max_lsn: Lsn,
    pub frames: u32,
    pub runs: Vec<TenantRun>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Footer {
    pub frame_count: u32,
    /// Sorted by tenant ref.
    pub tenants: Vec<TenantSummary>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SegmentDecodeError {
    #[error("truncated segment: have {have} bytes, need {need}")]
    Truncated { have: usize, need: usize },
    #[error("not a landing segment")]
    BadMagic,
    #[error("landing segment version {found} is newer than this zou, upgrade")]
    UnsupportedVersion { found: u16 },
    #[error("segment kind {found} is newer than this zou, upgrade")]
    UnknownKind { found: u16 },
    #[error("segment crc mismatch, the object is corrupt")]
    Corrupt,
    #[error("frame {index} is bad: {source}")]
    Frame {
        index: usize,
        #[source]
        source: Frame2DecodeError,
    },
}

/// Digest over per tenant tail lsns, carried in the next segment's
/// header so recovery can tell a fully landed predecessor from a half
/// landed zombie. Input order is the footer's, sorted by tenant.
pub fn tenants_digest(tenants: &[TenantSummary]) -> u64 {
    let mut bytes = Vec::with_capacity(tenants.len() * 24);
    for t in tenants {
        bytes.extend_from_slice(&t.tenant.to_le_bytes());
        bytes.extend_from_slice(&t.max_lsn.0.to_le_bytes());
    }
    (tenants.len() as u64) << 32 | crc32c::crc32c(&bytes) as u64
}

struct SummaryAcc {
    min_lsn: Lsn,
    max_lsn: Lsn,
    frames: u32,
    runs: Vec<TenantRun>,
}

/// Accumulates pre encoded frames into one segment. The sequencer
/// encodes frames on the append path, so the builder only copies bytes
/// and keeps the per tenant accounting.
pub struct SegmentBuilder {
    buf: Vec<u8>,
    tenants: BTreeMap<u128, SummaryAcc>,
    frame_count: u32,
}

impl SegmentBuilder {
    pub fn new(header: SegmentHeader) -> Self {
        let mut buf = Vec::with_capacity(HEADER_LEN);
        buf.extend_from_slice(&SEGMENT_MAGIC);
        buf.extend_from_slice(&SEGMENT_VERSION.to_le_bytes());
        let kind: u16 = match header.kind {
            SegmentKind::Landing => 0,
            SegmentKind::Seal => 1,
        };
        buf.extend_from_slice(&kind.to_le_bytes());
        buf.extend_from_slice(&header.shard.to_le_bytes());
        buf.extend_from_slice(&header.seq.to_le_bytes());
        buf.extend_from_slice(&header.prev_digest.to_le_bytes());
        debug_assert_eq!(buf.len(), HEADER_LEN);
        Self {
            buf,
            tenants: BTreeMap::new(),
            frame_count: 0,
        }
    }

    /// Append one frame that is already in wire form. The caller passes
    /// the frame's identity alongside because decoding it back out of
    /// `wire` here would be wasted work on the flush path.
    pub fn push_encoded(&mut self, tenant: u128, start_lsn: Lsn, end_lsn: Lsn, wire: &[u8]) {
        let offset = self.buf.len() as u64;
        self.buf.extend_from_slice(wire);
        self.frame_count += 1;
        let acc = self.tenants.entry(tenant).or_insert(SummaryAcc {
            min_lsn: start_lsn,
            max_lsn: end_lsn,
            frames: 0,
            runs: Vec::new(),
        });
        acc.min_lsn = acc.min_lsn.min(start_lsn);
        acc.max_lsn = acc.max_lsn.max(end_lsn);
        acc.frames += 1;
        match acc.runs.last_mut() {
            Some(run) if run.offset + run.len == offset => run.len += wire.len() as u64,
            _ => acc.runs.push(TenantRun {
                offset,
                len: wire.len() as u64,
            }),
        }
    }

    pub fn frame_count(&self) -> u32 {
        self.frame_count
    }

    /// Close the segment: write the footer and return the object bytes
    /// with the footer summaries, which the sequencer digests into the
    /// next header.
    pub fn finish(mut self) -> (Vec<u8>, Vec<TenantSummary>) {
        let footer_start = self.buf.len();
        let tenants: Vec<TenantSummary> = self
            .tenants
            .into_iter()
            .map(|(tenant, acc)| TenantSummary {
                tenant,
                min_lsn: acc.min_lsn,
                max_lsn: acc.max_lsn,
                frames: acc.frames,
                runs: acc.runs,
            })
            .collect();
        self.buf
            .extend_from_slice(&(tenants.len() as u32).to_le_bytes());
        for t in &tenants {
            self.buf.extend_from_slice(&t.tenant.to_le_bytes());
            self.buf.extend_from_slice(&t.min_lsn.0.to_le_bytes());
            self.buf.extend_from_slice(&t.max_lsn.0.to_le_bytes());
            self.buf.extend_from_slice(&t.frames.to_le_bytes());
            self.buf
                .extend_from_slice(&(t.runs.len() as u32).to_le_bytes());
            for run in &t.runs {
                self.buf.extend_from_slice(&run.offset.to_le_bytes());
                self.buf.extend_from_slice(&run.len.to_le_bytes());
            }
        }
        self.buf.extend_from_slice(&self.frame_count.to_le_bytes());
        let footer_len = (self.buf.len() - footer_start) as u32;
        self.buf.extend_from_slice(&footer_len.to_le_bytes());
        let mut crc = crc32c::crc32c(&self.buf[..HEADER_LEN]);
        crc = crc32c::crc32c_append(
            crc,
            &self.buf[footer_start..footer_start + footer_len as usize],
        );
        self.buf.extend_from_slice(&crc.to_le_bytes());
        self.buf.extend_from_slice(&FOOTER_MAGIC);
        (self.buf, tenants)
    }
}

fn parse_header(buf: &[u8]) -> Result<SegmentHeader, SegmentDecodeError> {
    if buf.len() < HEADER_LEN + TAIL_LEN {
        return Err(SegmentDecodeError::Truncated {
            have: buf.len(),
            need: HEADER_LEN + TAIL_LEN,
        });
    }
    if buf[0..4] != SEGMENT_MAGIC {
        return Err(SegmentDecodeError::BadMagic);
    }
    let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
    if version != SEGMENT_VERSION {
        return Err(SegmentDecodeError::UnsupportedVersion { found: version });
    }
    let kind = match u16::from_le_bytes(buf[6..8].try_into().unwrap()) {
        0 => SegmentKind::Landing,
        1 => SegmentKind::Seal,
        found => return Err(SegmentDecodeError::UnknownKind { found }),
    };
    Ok(SegmentHeader {
        kind,
        shard: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
        seq: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
        prev_digest: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
    })
}

/// Parse header and footer, locate and verify them, but leave the frame
/// region untouched. Returns the frame region bounds so callers can
/// range read exactly the runs they want.
fn parse_shell(buf: &[u8]) -> Result<(SegmentHeader, Footer, usize), SegmentDecodeError> {
    let header = parse_header(buf)?;
    if buf[buf.len() - 4..] != FOOTER_MAGIC {
        return Err(SegmentDecodeError::BadMagic);
    }
    let tail_at = buf.len() - TAIL_LEN;
    let footer_len = u32::from_le_bytes(buf[tail_at..tail_at + 4].try_into().unwrap()) as usize;
    let stored_crc = u32::from_le_bytes(buf[tail_at + 4..tail_at + 8].try_into().unwrap());
    if footer_len > tail_at - HEADER_LEN {
        return Err(SegmentDecodeError::Corrupt);
    }
    let footer_start = tail_at - footer_len;
    let mut crc = crc32c::crc32c(&buf[..HEADER_LEN]);
    crc = crc32c::crc32c_append(crc, &buf[footer_start..tail_at]);
    if crc != stored_crc {
        return Err(SegmentDecodeError::Corrupt);
    }

    let body = &buf[footer_start..tail_at];
    let mut at = 0usize;
    let take = |at: &mut usize, n: usize| -> Result<&[u8], SegmentDecodeError> {
        let end = at.checked_add(n).ok_or(SegmentDecodeError::Corrupt)?;
        let slice = body.get(*at..end).ok_or(SegmentDecodeError::Corrupt)?;
        *at = end;
        Ok(slice)
    };
    let tenant_count = u32::from_le_bytes(take(&mut at, 4)?.try_into().unwrap()) as usize;
    // Every summary needs its fixed part in the body, so a lying count
    // is caught before this reserves anything.
    if tenant_count > body.len() / SUMMARY_FIXED_LEN {
        return Err(SegmentDecodeError::Corrupt);
    }
    let mut tenants = Vec::with_capacity(tenant_count);
    for _ in 0..tenant_count {
        let fixed = take(&mut at, SUMMARY_FIXED_LEN)?;
        let run_count = u32::from_le_bytes(fixed[36..40].try_into().unwrap()) as usize;
        if run_count > (body.len() - at) / RUN_LEN {
            return Err(SegmentDecodeError::Corrupt);
        }
        let mut runs = Vec::with_capacity(run_count);
        for _ in 0..run_count {
            let r = take(&mut at, RUN_LEN)?;
            runs.push(TenantRun {
                offset: u64::from_le_bytes(r[0..8].try_into().unwrap()),
                len: u64::from_le_bytes(r[8..16].try_into().unwrap()),
            });
        }
        tenants.push(TenantSummary {
            tenant: u128::from_le_bytes(fixed[0..16].try_into().unwrap()),
            min_lsn: Lsn(u64::from_le_bytes(fixed[16..24].try_into().unwrap())),
            max_lsn: Lsn(u64::from_le_bytes(fixed[24..32].try_into().unwrap())),
            frames: u32::from_le_bytes(fixed[32..36].try_into().unwrap()),
            runs,
        });
    }
    let frame_count = u32::from_le_bytes(take(&mut at, 4)?.try_into().unwrap());
    if at != body.len() {
        return Err(SegmentDecodeError::Corrupt);
    }
    Ok((
        header,
        Footer {
            frame_count,
            tenants,
        },
        footer_start,
    ))
}

/// Header and footer only, for planning. Consolidation reads these to
/// decide which byte runs to fetch and never pays for payloads.
pub fn read_footer(buf: &[u8]) -> Result<(SegmentHeader, Footer), SegmentDecodeError> {
    let (header, footer, _) = parse_shell(buf)?;
    Ok((header, footer))
}

/// Full decode: header, every frame in segment order, footer.
pub fn decode_segment(
    buf: &[u8],
) -> Result<(SegmentHeader, Vec<Frame2>, Footer), SegmentDecodeError> {
    let (header, footer, footer_start) = parse_shell(buf)?;
    let mut frames = Vec::new();
    for item in Frame2Stream::new(&buf[HEADER_LEN..footer_start]) {
        match item {
            Ok(frame) => frames.push(frame),
            Err(source) => {
                return Err(SegmentDecodeError::Frame {
                    index: frames.len(),
                    source,
                });
            }
        }
    }
    if frames.len() != footer.frame_count as usize {
        return Err(SegmentDecodeError::Corrupt);
    }
    Ok((header, frames, footer))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(tenant: u128, lsn: u64, body: &[u8]) -> Frame2 {
        Frame2 {
            tenant,
            writer_epoch: 1,
            start_lsn: Lsn(lsn),
            end_lsn: Lsn(lsn + body.len() as u64),
            contains_commit: true,
            first_of_epoch: false,
            hints: Vec::new(),
            payload: body.to_vec(),
        }
    }

    fn build(seq: u64, prev_digest: u64, frames: &[Frame2]) -> (Vec<u8>, Vec<TenantSummary>) {
        let mut b = SegmentBuilder::new(SegmentHeader {
            kind: SegmentKind::Landing,
            shard: 3,
            seq,
            prev_digest,
        });
        for f in frames {
            b.push_encoded(f.tenant, f.start_lsn, f.end_lsn, &f.encode());
        }
        b.finish()
    }

    #[test]
    fn a_segment_round_trips_with_honest_summaries() {
        let frames = vec![
            frame(1, 100, b"one hundred"),
            frame(2, 500, b"five hundred"),
            frame(1, 200, b"two hundred"),
            frame(1, 300, b"three hundred"),
        ];
        let (wire, summaries) = build(9, 0xfeed, &frames);
        let (header, decoded, footer) = decode_segment(&wire).unwrap();
        assert_eq!(header.shard, 3);
        assert_eq!(header.seq, 9);
        assert_eq!(header.prev_digest, 0xfeed);
        assert_eq!(decoded, frames);
        assert_eq!(footer.frame_count, 4);
        assert_eq!(footer.tenants, summaries);

        let t1 = &footer.tenants[0];
        assert_eq!(t1.tenant, 1);
        assert_eq!(t1.min_lsn, Lsn(100));
        assert_eq!(t1.max_lsn, Lsn(300 + 13));
        assert_eq!(t1.frames, 3);
        // Tenant 1's frames sit in two runs, split by tenant 2's frame.
        assert_eq!(t1.runs.len(), 2);
        let t2 = &footer.tenants[1];
        assert_eq!(t2.runs.len(), 1);
    }

    #[test]
    fn footer_runs_slice_exactly_a_tenants_frames_back_out() {
        let frames = vec![
            frame(7, 10, b"aaaa"),
            frame(8, 20, b"bbbb"),
            frame(7, 30, b"cccc"),
        ];
        let (wire, _) = build(1, 0, &frames);
        let (_, footer) = read_footer(&wire).unwrap();
        let t7 = footer.tenants.iter().find(|t| t.tenant == 7).unwrap();
        let mut got = Vec::new();
        for run in &t7.runs {
            let bytes = &wire[run.offset as usize..(run.offset + run.len) as usize];
            for item in Frame2Stream::new(bytes) {
                got.push(item.unwrap());
            }
        }
        assert_eq!(got, vec![frames[0].clone(), frames[2].clone()]);
    }

    #[test]
    fn an_empty_segment_is_legal_and_round_trips() {
        // The sequencer never PUTs an empty window, but the codec does
        // not enforce policy.
        let (wire, summaries) = build(1, 0, &[]);
        let (_, frames, footer) = decode_segment(&wire).unwrap();
        assert!(frames.is_empty());
        assert!(footer.tenants.is_empty());
        assert!(summaries.is_empty());
    }

    #[test]
    fn every_single_byte_corruption_fails_decode_and_never_panics() {
        let frames = vec![frame(1, 100, b"payload"), frame(2, 50, b"data")];
        let (wire, _) = build(4, 77, &frames);
        for i in 0..wire.len() {
            let mut bad = wire.clone();
            bad[i] ^= 0xFF;
            if decode_segment(&bad).is_ok() {
                panic!("byte {i} corruption went unnoticed");
            }
        }
    }

    #[test]
    fn truncation_at_every_length_errors_and_never_panics() {
        let (wire, _) = build(4, 0, &[frame(1, 100, b"payload")]);
        for len in 0..wire.len() {
            assert!(decode_segment(&wire[..len]).is_err(), "cut at {len}");
            assert!(read_footer(&wire[..len]).is_err(), "cut at {len}");
        }
    }

    #[test]
    fn lying_footer_lengths_and_counts_cannot_allocate_or_panic() {
        let (wire, _) = build(4, 0, &[frame(1, 100, b"payload")]);
        let tail_at = wire.len() - TAIL_LEN;
        for lie in [0u32, 1, u32::MAX, wire.len() as u32] {
            let mut bad = wire.clone();
            bad[tail_at..tail_at + 4].copy_from_slice(&lie.to_le_bytes());
            let _ = decode_segment(&bad);
        }
        // A huge tenant count with a matching crc must still be refused.
        let footer_len =
            u32::from_le_bytes(wire[tail_at..tail_at + 4].try_into().unwrap()) as usize;
        let footer_start = tail_at - footer_len;
        let mut bad = wire.clone();
        bad[footer_start..footer_start + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let mut crc = crc32c::crc32c(&bad[..HEADER_LEN]);
        crc = crc32c::crc32c_append(crc, &bad[footer_start..tail_at]);
        bad[tail_at + 4..tail_at + 8].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(decode_segment(&bad), Err(SegmentDecodeError::Corrupt));
    }

    #[test]
    fn random_garbage_never_panics() {
        let mut state = 11u64;
        for _ in 0..200 {
            let junk: Vec<u8> = (0..2048)
                .map(|_| {
                    state = state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    (state >> 33) as u8
                })
                .collect();
            let _ = decode_segment(&junk);
            let _ = read_footer(&junk);
        }
    }

    #[test]
    fn the_digest_tracks_tenant_tails_and_nothing_else() {
        let (_, a) = build(1, 0, &[frame(1, 100, b"x"), frame(2, 50, b"y")]);
        let (_, b) = build(2, 0, &[frame(1, 100, b"x"), frame(2, 50, b"y")]);
        assert_eq!(tenants_digest(&a), tenants_digest(&b));
        let (_, c) = build(3, 0, &[frame(1, 100, b"x"), frame(2, 51, b"y")]);
        assert_ne!(tenants_digest(&a), tenants_digest(&c));
        assert_ne!(tenants_digest(&a), tenants_digest(&[]));
    }
}
