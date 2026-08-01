//! Sealed segment codec (spec 03 section 7).
//!
//! A sealed segment is one consolidation round of landing segments
//! folded into a single large object on cheap storage, internally
//! sorted by (tenant, lsn) so every tenant's WAL is one contiguous
//! region. The footer indexes each tenant's region as a list of
//! chunks, each a byte range with its lsn range, so per tenant access
//! is a couple of range GETs planned from the round index alone. A
//! bloom over tenant refs answers "is this tenant in here at all"
//! without walking the index.
//!
//! Frames keep their landing wire form, which means payloads stay lz4
//! compressed per frame. The spec asks for zstd at rest; per frame lz4
//! is the deliberate deviation, because the footer's byte ranges must
//! land on frame boundaries to be range readable, so compression has
//! to stay inside frames, and keeping one wire format end to end means
//! a chunk read from a sealed segment decodes with the same
//! Frame2Stream as a landing run. Container level zstd would break
//! range reads for a size win the per frame compression already took.
//!
//! Integrity is layered exactly like the landing codec: every frame
//! carries its own crc32c, the footer crc covers the header and footer
//! body, and the decoder never panics and never allocates on a lying
//! count.

use zou_store::{Frame2, Frame2DecodeError, Frame2Stream, Lsn};

pub const SEALED_MAGIC: [u8; 4] = *b"ZSLD";
pub const SEALED_FOOTER_MAGIC: [u8; 4] = *b"ZSLF";
pub const SEALED_VERSION: u16 = 1;

/// Chunk split target. A tenant's region is cut into chunks of at most
/// this many bytes (a single oversized frame gets its own chunk), so a
/// reader after a narrow lsn range never fetches more than one chunk
/// of slack on each end.
pub const SEALED_CHUNK_TARGET: u64 = 1 << 20;

/// magic, version, reserved, shard, first seq, last seq.
const HEADER_LEN: usize = 4 + 2 + 2 + 4 + 8 + 8;
/// footer body length, crc, footer magic.
const TAIL_LEN: usize = 4 + 4 + 4;
/// tenant, frame count, chunk count.
const TENANT_FIXED_LEN: usize = 16 + 4 + 4;
/// min lsn, max lsn, offset, len.
const CHUNK_LEN: usize = 8 + 8 + 8 + 8;

/// Object key for one sealed segment, named by the landing range it
/// folded so a rerun of the same round collides harmlessly.
pub fn sealed_key(shard: u32, first_seq: u64, last_seq: u64) -> String {
    format!("cellwal-sealed/{shard:04x}/{first_seq:016x}-{last_seq:016x}.seg")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedHeader {
    pub shard: u32,
    /// The landing chain range this segment folded, inclusive.
    pub first_seq: u64,
    pub last_seq: u64,
}

/// One byte range of a tenant's region with the lsn range it covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedChunk {
    pub min_lsn: Lsn,
    pub max_lsn: Lsn,
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedTenant {
    pub tenant: u128,
    pub frames: u32,
    /// In lsn order, contiguous: each chunk starts where the previous
    /// one ended.
    pub chunks: Vec<SealedChunk>,
}

/// A bloom filter over the tenant refs in a sealed segment, sized at
/// build time to roughly ten bits per tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantBloom {
    bits: Vec<u8>,
}

const BLOOM_HASHES: u32 = 7;

impl TenantBloom {
    fn sized_for(tenants: usize) -> Self {
        let bytes = (tenants * 10).div_ceil(8).next_power_of_two().max(64);
        Self {
            bits: vec![0; bytes],
        }
    }

    fn positions(&self, tenant: u128) -> impl Iterator<Item = usize> {
        let bytes = tenant.to_le_bytes();
        let h1 = crc32c::crc32c(&bytes) as u64;
        let h2 = crc32c::crc32c_append(0x5EED, &bytes) as u64 | 1;
        let bits = (self.bits.len() * 8) as u64;
        (0..BLOOM_HASHES as u64).map(move |i| (h1.wrapping_add(i.wrapping_mul(h2)) % bits) as usize)
    }

    fn insert(&mut self, tenant: u128) {
        for pos in self.positions(tenant).collect::<Vec<_>>() {
            self.bits[pos / 8] |= 1 << (pos % 8);
        }
    }

    /// False positives are possible, false negatives are not.
    pub fn may_contain(&self, tenant: u128) -> bool {
        self.positions(tenant)
            .collect::<Vec<_>>()
            .into_iter()
            .all(|pos| self.bits[pos / 8] & (1 << (pos % 8)) != 0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedFooter {
    pub frame_count: u32,
    pub bloom: TenantBloom,
    /// Sorted by tenant ref, regions in file order.
    pub tenants: Vec<SealedTenant>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SealedDecodeError {
    #[error("truncated sealed segment: have {have} bytes, need {need}")]
    Truncated { have: usize, need: usize },
    #[error("not a sealed segment")]
    BadMagic,
    #[error("sealed segment version {found} is newer than this zou, upgrade")]
    UnsupportedVersion { found: u16 },
    #[error("sealed segment crc mismatch, the object is corrupt")]
    Corrupt,
    #[error("frame {index} is bad: {source}")]
    Frame {
        index: usize,
        #[source]
        source: Frame2DecodeError,
    },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SealedBuildError {
    #[error("frames are not sorted by tenant and lsn")]
    Unsorted,
}

/// Encode one sealed segment from frames already sorted by
/// (tenant, start lsn). Returns the object bytes and the footer, which
/// the consolidator copies into the round index so readers plan range
/// GETs without touching this object's footer at all.
pub fn build_sealed(
    header: SealedHeader,
    frames: &[Frame2],
    chunk_target: u64,
) -> Result<(Vec<u8>, SealedFooter), SealedBuildError> {
    if frames
        .windows(2)
        .any(|w| (w[1].tenant, w[1].start_lsn) < (w[0].tenant, w[0].start_lsn))
    {
        return Err(SealedBuildError::Unsorted);
    }

    let mut buf = Vec::with_capacity(HEADER_LEN);
    buf.extend_from_slice(&SEALED_MAGIC);
    buf.extend_from_slice(&SEALED_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes());
    buf.extend_from_slice(&header.shard.to_le_bytes());
    buf.extend_from_slice(&header.first_seq.to_le_bytes());
    buf.extend_from_slice(&header.last_seq.to_le_bytes());
    debug_assert_eq!(buf.len(), HEADER_LEN);

    let mut tenants: Vec<SealedTenant> = Vec::new();
    for frame in frames {
        let wire = frame.encode();
        let offset = buf.len() as u64;
        let len = wire.len() as u64;
        buf.extend_from_slice(&wire);

        if tenants.last().map(|t| t.tenant) != Some(frame.tenant) {
            tenants.push(SealedTenant {
                tenant: frame.tenant,
                frames: 0,
                chunks: Vec::new(),
            });
        }
        let t = tenants.last_mut().expect("just pushed");
        t.frames += 1;
        match t.chunks.last_mut() {
            Some(c) if c.len + len <= chunk_target => {
                c.max_lsn = c.max_lsn.max(frame.end_lsn);
                c.len += len;
            }
            _ => t.chunks.push(SealedChunk {
                min_lsn: frame.start_lsn,
                max_lsn: frame.end_lsn,
                offset,
                len,
            }),
        }
    }

    let mut bloom = TenantBloom::sized_for(tenants.len());
    for t in &tenants {
        bloom.insert(t.tenant);
    }

    let footer_start = buf.len();
    buf.extend_from_slice(&(bloom.bits.len() as u32).to_le_bytes());
    buf.extend_from_slice(&bloom.bits);
    buf.extend_from_slice(&(tenants.len() as u32).to_le_bytes());
    for t in &tenants {
        buf.extend_from_slice(&t.tenant.to_le_bytes());
        buf.extend_from_slice(&t.frames.to_le_bytes());
        buf.extend_from_slice(&(t.chunks.len() as u32).to_le_bytes());
        for c in &t.chunks {
            buf.extend_from_slice(&c.min_lsn.0.to_le_bytes());
            buf.extend_from_slice(&c.max_lsn.0.to_le_bytes());
            buf.extend_from_slice(&c.offset.to_le_bytes());
            buf.extend_from_slice(&c.len.to_le_bytes());
        }
    }
    buf.extend_from_slice(&(frames.len() as u32).to_le_bytes());
    let footer_len = (buf.len() - footer_start) as u32;
    buf.extend_from_slice(&footer_len.to_le_bytes());
    let mut crc = crc32c::crc32c(&buf[..HEADER_LEN]);
    crc = crc32c::crc32c_append(crc, &buf[footer_start..footer_start + footer_len as usize]);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&SEALED_FOOTER_MAGIC);

    let footer = SealedFooter {
        frame_count: frames.len() as u32,
        bloom,
        tenants,
    };
    Ok((buf, footer))
}

fn parse_shell(buf: &[u8]) -> Result<(SealedHeader, SealedFooter, usize), SealedDecodeError> {
    if buf.len() < HEADER_LEN + TAIL_LEN {
        return Err(SealedDecodeError::Truncated {
            have: buf.len(),
            need: HEADER_LEN + TAIL_LEN,
        });
    }
    if buf[0..4] != SEALED_MAGIC {
        return Err(SealedDecodeError::BadMagic);
    }
    let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
    if version != SEALED_VERSION {
        return Err(SealedDecodeError::UnsupportedVersion { found: version });
    }
    let header = SealedHeader {
        shard: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
        first_seq: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
        last_seq: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
    };
    if buf[buf.len() - 4..] != SEALED_FOOTER_MAGIC {
        return Err(SealedDecodeError::BadMagic);
    }
    let tail_at = buf.len() - TAIL_LEN;
    let footer_len = u32::from_le_bytes(buf[tail_at..tail_at + 4].try_into().unwrap()) as usize;
    let stored_crc = u32::from_le_bytes(buf[tail_at + 4..tail_at + 8].try_into().unwrap());
    if footer_len > tail_at - HEADER_LEN {
        return Err(SealedDecodeError::Corrupt);
    }
    let footer_start = tail_at - footer_len;
    let mut crc = crc32c::crc32c(&buf[..HEADER_LEN]);
    crc = crc32c::crc32c_append(crc, &buf[footer_start..tail_at]);
    if crc != stored_crc {
        return Err(SealedDecodeError::Corrupt);
    }

    let body = &buf[footer_start..tail_at];
    let mut at = 0usize;
    let take = |at: &mut usize, n: usize| -> Result<&[u8], SealedDecodeError> {
        let end = at.checked_add(n).ok_or(SealedDecodeError::Corrupt)?;
        let slice = body.get(*at..end).ok_or(SealedDecodeError::Corrupt)?;
        *at = end;
        Ok(slice)
    };
    let bloom_len = u32::from_le_bytes(take(&mut at, 4)?.try_into().unwrap()) as usize;
    if bloom_len > body.len() - at || !bloom_len.is_power_of_two() || bloom_len < 64 {
        return Err(SealedDecodeError::Corrupt);
    }
    let bloom = TenantBloom {
        bits: take(&mut at, bloom_len)?.to_vec(),
    };
    let tenant_count = u32::from_le_bytes(take(&mut at, 4)?.try_into().unwrap()) as usize;
    // Every tenant needs its fixed part in the body, so a lying count
    // is caught before this reserves anything.
    if tenant_count > (body.len() - at) / TENANT_FIXED_LEN {
        return Err(SealedDecodeError::Corrupt);
    }
    let mut tenants = Vec::with_capacity(tenant_count);
    for _ in 0..tenant_count {
        let fixed = take(&mut at, TENANT_FIXED_LEN)?;
        let chunk_count = u32::from_le_bytes(fixed[20..24].try_into().unwrap()) as usize;
        if chunk_count > (body.len() - at) / CHUNK_LEN {
            return Err(SealedDecodeError::Corrupt);
        }
        let mut chunks = Vec::with_capacity(chunk_count);
        for _ in 0..chunk_count {
            let c = take(&mut at, CHUNK_LEN)?;
            let chunk = SealedChunk {
                min_lsn: Lsn(u64::from_le_bytes(c[0..8].try_into().unwrap())),
                max_lsn: Lsn(u64::from_le_bytes(c[8..16].try_into().unwrap())),
                offset: u64::from_le_bytes(c[16..24].try_into().unwrap()),
                len: u64::from_le_bytes(c[24..32].try_into().unwrap()),
            };
            // A chunk must land inside the frame region, a reader
            // range reads on its word.
            let end = chunk.offset.checked_add(chunk.len);
            if chunk.offset < HEADER_LEN as u64 || end.is_none_or(|e| e > footer_start as u64) {
                return Err(SealedDecodeError::Corrupt);
            }
            chunks.push(chunk);
        }
        tenants.push(SealedTenant {
            tenant: u128::from_le_bytes(fixed[0..16].try_into().unwrap()),
            frames: u32::from_le_bytes(fixed[16..20].try_into().unwrap()),
            chunks,
        });
    }
    let frame_count = u32::from_le_bytes(take(&mut at, 4)?.try_into().unwrap());
    if at != body.len() {
        return Err(SealedDecodeError::Corrupt);
    }
    Ok((
        header,
        SealedFooter {
            frame_count,
            bloom,
            tenants,
        },
        footer_start,
    ))
}

/// Header and footer only, for planning and inspection.
pub fn read_sealed_footer(buf: &[u8]) -> Result<(SealedHeader, SealedFooter), SealedDecodeError> {
    let (header, footer, _) = parse_shell(buf)?;
    Ok((header, footer))
}

/// Full decode: header, every frame in file order, footer.
pub fn decode_sealed(
    buf: &[u8],
) -> Result<(SealedHeader, Vec<Frame2>, SealedFooter), SealedDecodeError> {
    let (header, footer, footer_start) = parse_shell(buf)?;
    let mut frames = Vec::new();
    for item in Frame2Stream::new(&buf[HEADER_LEN..footer_start]) {
        match item {
            Ok(frame) => frames.push(frame),
            Err(source) => {
                return Err(SealedDecodeError::Frame {
                    index: frames.len(),
                    source,
                });
            }
        }
    }
    if frames.len() != footer.frame_count as usize {
        return Err(SealedDecodeError::Corrupt);
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

    fn header() -> SealedHeader {
        SealedHeader {
            shard: 3,
            first_seq: 10,
            last_seq: 25,
        }
    }

    #[test]
    fn a_sealed_segment_round_trips_sorted() {
        let frames = vec![
            frame(1, 100, b"one"),
            frame(1, 103, b"two"),
            frame(2, 50, b"three"),
        ];
        let (wire, footer) = build_sealed(header(), &frames, SEALED_CHUNK_TARGET).unwrap();
        let (h, decoded, f) = decode_sealed(&wire).unwrap();
        assert_eq!(h, header());
        assert_eq!(decoded, frames);
        assert_eq!(f, footer);
        assert_eq!(f.frame_count, 3);
        assert_eq!(f.tenants.len(), 2);
        assert_eq!(f.tenants[0].tenant, 1);
        assert_eq!(f.tenants[0].frames, 2);
        assert_eq!(f.tenants[0].chunks.len(), 1);
        assert_eq!(f.tenants[0].chunks[0].min_lsn, Lsn(100));
        assert_eq!(f.tenants[0].chunks[0].max_lsn, Lsn(106));
    }

    #[test]
    fn unsorted_frames_are_refused() {
        let frames = vec![frame(2, 50, b"x"), frame(1, 100, b"y")];
        assert_eq!(
            build_sealed(header(), &frames, SEALED_CHUNK_TARGET).unwrap_err(),
            SealedBuildError::Unsorted
        );
        let frames = vec![frame(1, 200, b"x"), frame(1, 100, b"y")];
        assert_eq!(
            build_sealed(header(), &frames, SEALED_CHUNK_TARGET).unwrap_err(),
            SealedBuildError::Unsorted
        );
    }

    #[test]
    fn chunks_split_at_the_target_and_slice_frames_back_out() {
        let frames: Vec<Frame2> = (0..20)
            .map(|i| frame(7, 1000 + i * 64, &[i as u8; 64]))
            .collect();
        // A target barely above one frame's wire size forces a chunk
        // per frame or two.
        let one = frames[0].encode().len() as u64;
        let (wire, footer) = build_sealed(header(), &frames, one * 2).unwrap();
        let t = &footer.tenants[0];
        assert!(t.chunks.len() >= 10, "got {} chunks", t.chunks.len());

        // Chunks are contiguous, cover the tenant's lsn span, and each
        // one decodes standalone from its byte range.
        let mut got = Vec::new();
        let mut expect_at = t.chunks[0].offset;
        for c in &t.chunks {
            assert_eq!(c.offset, expect_at);
            expect_at += c.len;
            for item in Frame2Stream::new(&wire[c.offset as usize..(c.offset + c.len) as usize]) {
                let f = item.unwrap();
                assert!(f.start_lsn >= c.min_lsn && f.end_lsn <= c.max_lsn);
                got.push(f);
            }
        }
        assert_eq!(got, frames);
    }

    #[test]
    fn the_bloom_has_no_false_negatives() {
        let frames: Vec<Frame2> = (0..300)
            .map(|i| frame(i as u128 * 7919 + 1, 100, b"x"))
            .collect();
        let mut sorted = frames.clone();
        sorted.sort_by_key(|f| (f.tenant, f.start_lsn));
        let (_, footer) = build_sealed(header(), &sorted, SEALED_CHUNK_TARGET).unwrap();
        for t in &footer.tenants {
            assert!(footer.bloom.may_contain(t.tenant));
        }
        // Not a correctness property, but the filter must actually
        // filter: most absent tenants come back negative.
        let misses = (1_000_000u128..1_000_200)
            .filter(|t| !footer.bloom.may_contain(*t))
            .count();
        assert!(misses > 150, "only {misses} of 200 absent tenants missed");
    }

    #[test]
    fn an_empty_sealed_segment_is_legal() {
        let (wire, _) = build_sealed(header(), &[], SEALED_CHUNK_TARGET).unwrap();
        let (_, frames, footer) = decode_sealed(&wire).unwrap();
        assert!(frames.is_empty());
        assert!(footer.tenants.is_empty());
    }

    #[test]
    fn every_single_byte_corruption_fails_decode_and_never_panics() {
        let frames = vec![frame(1, 100, b"payload"), frame(2, 50, b"data")];
        let (wire, _) = build_sealed(header(), &frames, SEALED_CHUNK_TARGET).unwrap();
        for i in 0..wire.len() {
            let mut bad = wire.clone();
            bad[i] ^= 0xFF;
            if decode_sealed(&bad).is_ok() {
                panic!("byte {i} corruption went unnoticed");
            }
        }
    }

    #[test]
    fn truncation_at_every_length_errors_and_never_panics() {
        let (wire, _) =
            build_sealed(header(), &[frame(1, 100, b"payload")], SEALED_CHUNK_TARGET).unwrap();
        for len in 0..wire.len() {
            assert!(decode_sealed(&wire[..len]).is_err(), "cut at {len}");
            assert!(read_sealed_footer(&wire[..len]).is_err(), "cut at {len}");
        }
    }

    #[test]
    fn random_garbage_never_panics() {
        let mut state = 23u64;
        for _ in 0..200 {
            let junk: Vec<u8> = (0..4096)
                .map(|_| {
                    state = state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    (state >> 33) as u8
                })
                .collect();
            let _ = decode_sealed(&junk);
            let _ = read_sealed_footer(&junk);
        }
    }
}
