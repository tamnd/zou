//! Frame format v2, the unit of the shared multi tenant WAL
//! (spec 03 section 2).
//!
//! Everything that enters a shard is a frame. Frames from many tenants
//! interleave freely inside a landing segment, so unlike the v1 frame
//! every frame carries its tenant ref, and the writer epoch is per
//! tenant rather than per store. Block ref hints ride
//! along uncompressed so the sequencer can tee filter per page shard
//! without decrypting or parsing payloads: the compute already knows
//! the block refs and pays nothing to repeat them.
//!
//! The decoder consumes bytes fetched from the network and must never
//! panic, never allocate on a lying length, and never accept a frame
//! whose crc does not match. Same paranoia budget as v1, plus the hint
//! table's own bounds.
//!
//! Wire layout, little endian:
//!
//! ```text
//! u32  len          total frame length, including this field
//! u32  crc32c       over everything after this field
//! u128 tenant_ref   tenant/branch id
//! u32  writer_epoch fencing token of the producing compute
//! u64  start_lsn
//! u64  end_lsn
//! u8   flags        contains_commit, first_of_epoch, hints, raw body
//! u32  payload_len  uncompressed payload length
//! [u16 hint_count, hint_count * (u32 rel, u8 fork, u32 block)]  if hinted
//! bytes body        payload, lz4 unless the raw flag is set
//! ```

use crate::lsn::Lsn;

pub const FRAME2_HEADER_LEN: usize = 49;
const CRC_END: usize = 8;

/// Hard cap on one frame's uncompressed payload. Pushers seal batches
/// far below this, the cap only exists to stop a hostile length field.
pub const MAX_PAYLOAD_LEN: u32 = 64 * 1024 * 1024;

/// Hard cap on block ref hints per frame. A full 4 MB batch of 8 KB
/// pages references at most 512 blocks, so 4096 leaves headroom
/// without letting a lying count allocate anything that matters.
pub const MAX_HINTS: u16 = 4096;

const FLAG_CONTAINS_COMMIT: u8 = 1;
const FLAG_FIRST_OF_EPOCH: u8 = 2;
const FLAG_HINTS: u8 = 4;
const FLAG_UNCOMPRESSED: u8 = 8;
const FLAG_KNOWN: u8 = FLAG_CONTAINS_COMMIT | FLAG_FIRST_OF_EPOCH | FLAG_HINTS | FLAG_UNCOMPRESSED;

const HINT_LEN: usize = 9;

/// One (relfilenode, fork, block) reference, the tee filtering key. The
/// sequencer routes a frame to a page shard when any hint maps there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRef {
    pub relfilenode: u32,
    pub fork: u8,
    pub block: u32,
}

impl BlockRef {
    /// The page shard stripe this block lands on, spec 04 section 1:
    /// hash over (relfilenode, block >> 14), so 16384 consecutive
    /// blocks of a relation, 128 MB, stay on one shard and sequential
    /// scans do not fan out. The fork is deliberately not hashed, all
    /// forks of a stripe live together. Format frozen: this function
    /// decides data placement, changing it reshuffles every tenant.
    pub fn page_shard(&self, shard_count: u32) -> u32 {
        let mut bytes = [0u8; 8];
        bytes[..4].copy_from_slice(&self.relfilenode.to_le_bytes());
        bytes[4..].copy_from_slice(&(self.block >> 14).to_le_bytes());
        crc32c::crc32c(&bytes) % shard_count.max(1)
    }
}

/// One decoded v2 frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame2 {
    pub tenant: u128,
    pub writer_epoch: u32,
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,
    pub contains_commit: bool,
    pub first_of_epoch: bool,
    /// Empty means the producer sent no hints and the tee falls back to
    /// broadcasting the frame to every shard of the tenant.
    pub hints: Vec<BlockRef>,
    pub payload: Vec<u8>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Frame2DecodeError {
    #[error("truncated frame: have {have} bytes, need {need}")]
    Truncated { have: usize, need: usize },
    #[error("frame length field {len} is impossible")]
    BadLength { len: u32 },
    #[error("unknown flag bits {bits:#04x}, this frame is from a newer zou, upgrade")]
    UnknownFlags { bits: u8 },
    #[error("frame payload of {len} bytes exceeds the {MAX_PAYLOAD_LEN} byte cap")]
    PayloadTooLarge { len: u32 },
    #[error("hint count {count} exceeds the {MAX_HINTS} cap")]
    TooManyHints { count: u16 },
    #[error("crc mismatch, frame is corrupt")]
    Corrupt,
    #[error("lz4 body does not decompress to the declared length")]
    BadCompression,
}

impl Frame2 {
    /// Encode into the wire form. The payload is stored lz4 compressed
    /// unless that fails to shrink it, in which case it goes raw and
    /// flagged, so incompressible bodies never pay to grow. Hints stay
    /// uncompressed always: the sequencer reads them on the hot path
    /// without touching the payload.
    pub fn encode(&self) -> Vec<u8> {
        assert!(
            self.payload.len() <= MAX_PAYLOAD_LEN as usize,
            "payload exceeds the frame cap"
        );
        assert!(
            self.hints.len() <= MAX_HINTS as usize,
            "hint count exceeds the frame cap"
        );
        let compressed = lz4_flex::compress(&self.payload);
        let (raw, body): (bool, &[u8]) = if compressed.len() < self.payload.len() {
            (false, &compressed)
        } else {
            (true, &self.payload)
        };

        let mut flags = 0u8;
        if self.contains_commit {
            flags |= FLAG_CONTAINS_COMMIT;
        }
        if self.first_of_epoch {
            flags |= FLAG_FIRST_OF_EPOCH;
        }
        if !self.hints.is_empty() {
            flags |= FLAG_HINTS;
        }
        if raw {
            flags |= FLAG_UNCOMPRESSED;
        }

        let hints_len = if self.hints.is_empty() {
            0
        } else {
            2 + self.hints.len() * HINT_LEN
        };
        let total = FRAME2_HEADER_LEN + hints_len + body.len();
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&(total as u32).to_le_bytes());
        out.extend_from_slice(&[0u8; 4]);
        out.extend_from_slice(&self.tenant.to_le_bytes());
        out.extend_from_slice(&self.writer_epoch.to_le_bytes());
        out.extend_from_slice(&self.start_lsn.0.to_le_bytes());
        out.extend_from_slice(&self.end_lsn.0.to_le_bytes());
        out.push(flags);
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        debug_assert_eq!(out.len(), FRAME2_HEADER_LEN);
        if !self.hints.is_empty() {
            out.extend_from_slice(&(self.hints.len() as u16).to_le_bytes());
            for h in &self.hints {
                out.extend_from_slice(&h.relfilenode.to_le_bytes());
                out.push(h.fork);
                out.extend_from_slice(&h.block.to_le_bytes());
            }
        }
        out.extend_from_slice(body);
        debug_assert_eq!(out.len(), total);

        let crc = crc32c::crc32c(&out[CRC_END..]);
        out[4..8].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// Decode one frame from the front of `buf`, returning it and the
    /// bytes consumed so a segment decodes by walking forward.
    pub fn decode(buf: &[u8]) -> Result<(Frame2, usize), Frame2DecodeError> {
        if buf.len() < FRAME2_HEADER_LEN {
            return Err(Frame2DecodeError::Truncated {
                have: buf.len(),
                need: FRAME2_HEADER_LEN,
            });
        }
        let len = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        // The length field bounds every later read, so it gets checked
        // before anything else is trusted. The upper bound is generous,
        // header plus max hints plus a worst case lz4 expansion of the
        // payload cap, anything above it is a lie.
        let max_len = FRAME2_HEADER_LEN
            + 2
            + MAX_HINTS as usize * HINT_LEN
            + lz4_flex::block::get_maximum_output_size(MAX_PAYLOAD_LEN as usize);
        if (len as usize) < FRAME2_HEADER_LEN || len as usize > max_len {
            return Err(Frame2DecodeError::BadLength { len });
        }
        let total = len as usize;
        if buf.len() < total {
            return Err(Frame2DecodeError::Truncated {
                have: buf.len(),
                need: total,
            });
        }

        let stored_crc = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if crc32c::crc32c(&buf[CRC_END..total]) != stored_crc {
            return Err(Frame2DecodeError::Corrupt);
        }

        let tenant = u128::from_le_bytes(buf[8..24].try_into().unwrap());
        let writer_epoch = u32::from_le_bytes(buf[24..28].try_into().unwrap());
        let start_lsn = Lsn(u64::from_le_bytes(buf[28..36].try_into().unwrap()));
        let end_lsn = Lsn(u64::from_le_bytes(buf[36..44].try_into().unwrap()));
        let flags = buf[44];
        if flags & !FLAG_KNOWN != 0 {
            return Err(Frame2DecodeError::UnknownFlags {
                bits: flags & !FLAG_KNOWN,
            });
        }
        let payload_len = u32::from_le_bytes(buf[45..49].try_into().unwrap());
        if payload_len > MAX_PAYLOAD_LEN {
            return Err(Frame2DecodeError::PayloadTooLarge { len: payload_len });
        }

        let mut at = FRAME2_HEADER_LEN;
        let mut hints = Vec::new();
        if flags & FLAG_HINTS != 0 {
            if total < at + 2 {
                return Err(Frame2DecodeError::Corrupt);
            }
            let count = u16::from_le_bytes(buf[at..at + 2].try_into().unwrap());
            if count > MAX_HINTS {
                return Err(Frame2DecodeError::TooManyHints { count });
            }
            at += 2;
            if total < at + count as usize * HINT_LEN {
                return Err(Frame2DecodeError::Corrupt);
            }
            hints.reserve_exact(count as usize);
            for _ in 0..count {
                hints.push(BlockRef {
                    relfilenode: u32::from_le_bytes(buf[at..at + 4].try_into().unwrap()),
                    fork: buf[at + 4],
                    block: u32::from_le_bytes(buf[at + 5..at + 9].try_into().unwrap()),
                });
                at += HINT_LEN;
            }
        }

        let body = &buf[at..total];
        let payload = if flags & FLAG_UNCOMPRESSED != 0 {
            if body.len() != payload_len as usize {
                return Err(Frame2DecodeError::BadCompression);
            }
            body.to_vec()
        } else {
            match lz4_flex::decompress(body, payload_len as usize) {
                Ok(p) if p.len() == payload_len as usize => p,
                _ => return Err(Frame2DecodeError::BadCompression),
            }
        };

        Ok((
            Frame2 {
                tenant,
                writer_epoch,
                start_lsn,
                end_lsn,
                contains_commit: flags & FLAG_CONTAINS_COMMIT != 0,
                first_of_epoch: flags & FLAG_FIRST_OF_EPOCH != 0,
                hints,
                payload,
            },
            total,
        ))
    }
}

/// Walks the interleaved frames of one landing segment region. No
/// epoch filtering here: frames from many tenants share a segment and
/// epoch admission already happened at the sequencer, so a consumer
/// filters by tenant itself.
pub struct Frame2Stream<'a> {
    buf: &'a [u8],
}

impl<'a> Frame2Stream<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }
}

impl Iterator for Frame2Stream<'_> {
    type Item = Result<Frame2, Frame2DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            return None;
        }
        match Frame2::decode(self.buf) {
            Ok((frame, consumed)) => {
                self.buf = &self.buf[consumed..];
                Some(Ok(frame))
            }
            Err(e) => {
                // An error ends the walk; a torn tail decodes as
                // Truncated and the caller treats it as end of segment.
                self.buf = &[];
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic pseudo-random bytes, no rand dependency.
    fn lcg_bytes(seed: u64, len: usize) -> Vec<u8> {
        let mut state = seed;
        (0..len)
            .map(|_| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (state >> 33) as u8
            })
            .collect()
    }

    fn sample(payload: Vec<u8>, hints: Vec<BlockRef>) -> Frame2 {
        Frame2 {
            tenant: 0x00c0_ffee_0000_0000_0000_0000_0000_1234,
            writer_epoch: 7,
            start_lsn: Lsn(0x8B00_0000),
            end_lsn: Lsn(0x8B00_4000),
            contains_commit: true,
            first_of_epoch: false,
            hints,
            payload,
        }
    }

    fn some_hints(n: usize) -> Vec<BlockRef> {
        (0..n)
            .map(|i| BlockRef {
                relfilenode: 16384 + i as u32,
                fork: (i % 3) as u8,
                block: i as u32 * 131,
            })
            .collect()
    }

    #[test]
    fn round_trips_across_payloads_hints_and_flags() {
        for payload in [vec![], vec![7u8; 100_000], lcg_bytes(1, 100_000)] {
            for hints in [vec![], some_hints(1), some_hints(512)] {
                for (commit, first) in [(false, false), (true, false), (false, true)] {
                    let mut frame = sample(payload.clone(), hints.clone());
                    frame.contains_commit = commit;
                    frame.first_of_epoch = first;
                    let wire = frame.encode();
                    let (decoded, consumed) = Frame2::decode(&wire).unwrap();
                    assert_eq!(decoded, frame);
                    assert_eq!(consumed, wire.len());
                }
            }
        }
    }

    #[test]
    fn hints_are_readable_without_touching_the_payload() {
        // The tee filter parses hints in place, so they must sit at a
        // fixed offset before the body, uncompressed.
        let frame = sample(lcg_bytes(4, 10_000), some_hints(3));
        let wire = frame.encode();
        let count = u16::from_le_bytes(wire[49..51].try_into().unwrap());
        assert_eq!(count, 3);
        let rel = u32::from_le_bytes(wire[51..55].try_into().unwrap());
        assert_eq!(rel, 16384);
    }

    #[test]
    fn an_interleaved_multi_tenant_stream_reads_back_in_order() {
        let frames: Vec<Frame2> = (0..6)
            .map(|i| {
                let mut f = sample(lcg_bytes(i, 2000), some_hints(i as usize % 4));
                f.tenant = u128::from(i % 3);
                f.start_lsn = Lsn(i * 0x1000);
                f.end_lsn = Lsn((i + 1) * 0x1000);
                f
            })
            .collect();
        let segment: Vec<u8> = frames.iter().flat_map(Frame2::encode).collect();
        let read: Vec<Frame2> = Frame2Stream::new(&segment).map(Result::unwrap).collect();
        assert_eq!(read, frames);
    }

    #[test]
    fn a_torn_tail_reads_as_truncated_and_ends_the_walk() {
        let mut segment = sample(b"first".to_vec(), vec![]).encode();
        let second = sample(b"second".to_vec(), some_hints(2)).encode();
        segment.extend_from_slice(&second[..second.len() / 2]);
        let mut stream = Frame2Stream::new(&segment);
        assert!(stream.next().unwrap().is_ok());
        assert!(matches!(
            stream.next().unwrap().unwrap_err(),
            Frame2DecodeError::Truncated { .. }
        ));
        assert!(stream.next().is_none());
    }

    #[test]
    fn every_single_byte_corruption_is_caught_and_never_panics() {
        let wire = sample(lcg_bytes(2, 500), some_hints(5)).encode();
        for i in 0..wire.len() {
            let mut bad = wire.clone();
            bad[i] ^= 0xFF;
            if let Ok((frame, _)) = Frame2::decode(&bad) {
                panic!("byte {i} corruption went unnoticed: {frame:?}");
            }
        }
    }

    #[test]
    fn truncation_at_every_length_errors_and_never_panics() {
        let wire = sample(lcg_bytes(3, 300), some_hints(7)).encode();
        for len in 0..wire.len() {
            assert!(Frame2::decode(&wire[..len]).is_err());
        }
    }

    #[test]
    fn random_garbage_never_panics() {
        for seed in 0..200 {
            let junk = lcg_bytes(seed, 4096);
            let _ = Frame2::decode(&junk);
        }
    }

    #[test]
    fn lying_length_fields_cannot_force_huge_allocations() {
        let mut wire = sample(b"tiny".to_vec(), some_hints(1)).encode();
        wire[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            Frame2::decode(&wire),
            Err(Frame2DecodeError::BadLength { .. })
        ));

        let mut wire = sample(b"tiny".to_vec(), vec![]).encode();
        wire[45..49].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            Frame2::decode(&wire),
            Err(Frame2DecodeError::PayloadTooLarge { .. } | Frame2DecodeError::Corrupt)
        ));

        // A hint count above the cap must be refused even when the crc
        // is recomputed to match, so a hostile producer gains nothing.
        let frame = sample(b"x".to_vec(), some_hints(1));
        let mut wire = frame.encode();
        wire[49..51].copy_from_slice(&(MAX_HINTS + 1).to_le_bytes());
        let crc = crc32c::crc32c(&wire[8..]);
        wire[4..8].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            Frame2::decode(&wire),
            Err(Frame2DecodeError::TooManyHints { .. })
        ));
    }

    #[test]
    fn unknown_flag_bits_are_refused_with_an_upgrade_hint() {
        let mut wire = sample(b"x".to_vec(), vec![]).encode();
        wire[44] |= 0x80;
        let crc = crc32c::crc32c(&wire[8..]);
        wire[4..8].copy_from_slice(&crc.to_le_bytes());
        let err = Frame2::decode(&wire).unwrap_err();
        assert_eq!(err, Frame2DecodeError::UnknownFlags { bits: 0x80 });
        assert!(err.to_string().contains("upgrade"));
    }

    #[test]
    fn compressible_payloads_actually_shrink_and_hints_survive() {
        let frame = sample(vec![7u8; 100_000], some_hints(100));
        let wire = frame.encode();
        assert!(wire.len() < 3_000);
        let (decoded, _) = Frame2::decode(&wire).unwrap();
        assert_eq!(decoded.hints.len(), 100);
    }
}

#[cfg(test)]
mod page_shard_tests {
    use super::*;

    fn r(relfilenode: u32, block: u32) -> BlockRef {
        BlockRef {
            relfilenode,
            fork: 0,
            block,
        }
    }

    /// Placement is format frozen: these exact values decide where a
    /// tenant's pages live. If this test breaks, you changed data
    /// placement for every existing tenant, not just a hash function.
    #[test]
    fn placement_values_are_pinned() {
        assert_eq!(r(1663, 0).page_shard(64), 10);
        assert_eq!(r(1663, 16384).page_shard(64), 50);
        assert_eq!(r(16400, 123_456_789).page_shard(4), 3);
    }

    #[test]
    fn a_stripe_is_16384_consecutive_blocks_and_forks_stay_together() {
        let base = r(5000, 3 * 16384);
        for block in 3 * 16384..3 * 16384 + 16384 {
            assert_eq!(r(5000, block).page_shard(16), base.page_shard(16));
        }
        for fork in 0..4u8 {
            let with_fork = BlockRef { fork, ..base };
            assert_eq!(with_fork.page_shard(16), base.page_shard(16));
        }
        assert_ne!(
            r(5000, 4 * 16384).page_shard(16),
            u32::MAX,
            "next stripe still maps somewhere"
        );
    }

    #[test]
    fn every_shard_of_a_small_count_gets_traffic() {
        let count = 8;
        let mut hit = vec![false; count as usize];
        for i in 0..256u32 {
            let s = r(1000 + i, i * 20_000).page_shard(count);
            assert!(s < count);
            hit[s as usize] = true;
        }
        assert!(hit.iter().all(|h| *h), "a dead shard means a skewed hash");
    }
}
