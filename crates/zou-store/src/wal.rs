//! WAL frame encoding.
//!
//! WAL batches travel to the object store as frames: a fixed header with
//! magic, version, epoch, fence, LSN range, and a crc32c, followed by an
//! lz4 compressed body. The epoch and fence in every frame are what make
//! zombie writers harmless on the read side: a reader replaying a segment
//! rejects frames from any epoch other than the one the manifest names.
//!
//! The decoder is the paranoid half. It runs against bytes fetched from
//! the network, so it must never panic, never over-allocate on a lying
//! length field, and never accept a frame whose checksum does not match.

use crate::lsn::Lsn;

pub const WAL_MAGIC: [u8; 4] = *b"ZWAL";
pub const WAL_VERSION: u16 = 1;

/// Hard cap on the uncompressed body of one frame. Group commit seals
/// batches far below this; the cap exists so a corrupt or hostile length
/// field cannot make the decoder allocate gigabytes.
pub const MAX_BODY_LEN: u32 = 64 * 1024 * 1024;

const FLAG_UNCOMPRESSED: u16 = 1;
const HEADER_LEN: usize = 52;
const CRC_OFFSET: usize = HEADER_LEN - 4;

/// One decoded WAL frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub epoch: u64,
    pub fence: u64,
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,
    pub payload: Vec<u8>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WalDecodeError {
    #[error("truncated frame: have {have} bytes, need {need}")]
    Truncated { have: usize, need: usize },
    #[error("bad magic, not a zou WAL frame")]
    BadMagic,
    #[error(
        "frame version {found} is newer than this binary supports ({WAL_VERSION}), upgrade zou"
    )]
    UnsupportedVersion { found: u16 },
    #[error("frame body of {len} bytes exceeds the {MAX_BODY_LEN} byte cap")]
    BodyTooLarge { len: u32 },
    #[error("crc mismatch, frame is corrupt")]
    Corrupt,
    #[error("lz4 body does not decompress to the declared length")]
    BadCompression,
    #[error("frame is from epoch {found}, expected epoch {expected}")]
    StaleEpoch { found: u64, expected: u64 },
}

impl Frame {
    /// Encode into the wire form: header, crc, lz4 body. If compression
    /// does not shrink the payload the body is stored raw and flagged, so
    /// incompressible pages never pay to grow.
    pub fn encode(&self) -> Vec<u8> {
        let compressed = lz4_flex::compress(&self.payload);
        let (flags, body): (u16, &[u8]) = if compressed.len() < self.payload.len() {
            (0, &compressed)
        } else {
            (FLAG_UNCOMPRESSED, &self.payload)
        };

        let mut out = Vec::with_capacity(HEADER_LEN + body.len());
        out.extend_from_slice(&WAL_MAGIC);
        out.extend_from_slice(&WAL_VERSION.to_le_bytes());
        out.extend_from_slice(&flags.to_le_bytes());
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&self.fence.to_le_bytes());
        out.extend_from_slice(&self.start_lsn.0.to_le_bytes());
        out.extend_from_slice(&self.end_lsn.0.to_le_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&(body.len() as u32).to_le_bytes());
        debug_assert_eq!(out.len(), CRC_OFFSET);
        out.extend_from_slice(&[0u8; 4]);
        out.extend_from_slice(body);

        let crc = crc_over(&out);
        out[CRC_OFFSET..HEADER_LEN].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// Decode one frame from the front of `buf`. Returns the frame and the
    /// number of bytes consumed, so segments decode by walking forward.
    pub fn decode(buf: &[u8]) -> Result<(Frame, usize), WalDecodeError> {
        if buf.len() < HEADER_LEN {
            return Err(WalDecodeError::Truncated {
                have: buf.len(),
                need: HEADER_LEN,
            });
        }
        if buf[0..4] != WAL_MAGIC {
            return Err(WalDecodeError::BadMagic);
        }
        let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        if version > WAL_VERSION {
            return Err(WalDecodeError::UnsupportedVersion { found: version });
        }
        let flags = u16::from_le_bytes(buf[6..8].try_into().unwrap());
        let epoch = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let fence = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let start_lsn = Lsn(u64::from_le_bytes(buf[24..32].try_into().unwrap()));
        let end_lsn = Lsn(u64::from_le_bytes(buf[32..40].try_into().unwrap()));
        let payload_len = u32::from_le_bytes(buf[40..44].try_into().unwrap());
        let body_len = u32::from_le_bytes(buf[44..48].try_into().unwrap());
        if payload_len > MAX_BODY_LEN {
            return Err(WalDecodeError::BodyTooLarge { len: payload_len });
        }
        if body_len > MAX_BODY_LEN {
            return Err(WalDecodeError::BodyTooLarge { len: body_len });
        }

        let total = HEADER_LEN + body_len as usize;
        if buf.len() < total {
            return Err(WalDecodeError::Truncated {
                have: buf.len(),
                need: total,
            });
        }

        let stored_crc = u32::from_le_bytes(buf[CRC_OFFSET..HEADER_LEN].try_into().unwrap());
        let mut check = buf[..total].to_vec();
        check[CRC_OFFSET..HEADER_LEN].fill(0);
        if crc_over(&check) != stored_crc {
            return Err(WalDecodeError::Corrupt);
        }

        let body = &buf[HEADER_LEN..total];
        let payload = if flags & FLAG_UNCOMPRESSED != 0 {
            if body.len() != payload_len as usize {
                return Err(WalDecodeError::BadCompression);
            }
            body.to_vec()
        } else {
            match lz4_flex::decompress(body, payload_len as usize) {
                Ok(p) if p.len() == payload_len as usize => p,
                _ => return Err(WalDecodeError::BadCompression),
            }
        };

        Ok((
            Frame {
                epoch,
                fence,
                start_lsn,
                end_lsn,
                payload,
            },
            total,
        ))
    }
}

/// crc32c over header (with the crc field zeroed) and body.
fn crc_over(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

/// Walks the frames of one sealed segment, enforcing that every frame
/// belongs to the expected epoch. Stale frames from a fenced-out writer
/// fail loudly instead of replaying.
pub struct SegmentReader<'a> {
    buf: &'a [u8],
    expected_epoch: u64,
}

impl<'a> SegmentReader<'a> {
    pub fn new(buf: &'a [u8], expected_epoch: u64) -> Self {
        Self {
            buf,
            expected_epoch,
        }
    }
}

impl Iterator for SegmentReader<'_> {
    type Item = Result<Frame, WalDecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            return None;
        }
        match Frame::decode(self.buf) {
            Ok((frame, consumed)) => {
                self.buf = &self.buf[consumed..];
                if frame.epoch != self.expected_epoch {
                    return Some(Err(WalDecodeError::StaleEpoch {
                        found: frame.epoch,
                        expected: self.expected_epoch,
                    }));
                }
                Some(Ok(frame))
            }
            Err(e) => {
                // An error ends the walk; a torn tail after a crash decodes
                // as Truncated and the caller treats it as end of segment.
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

    fn sample(payload: Vec<u8>) -> Frame {
        Frame {
            epoch: 42,
            fence: 1042,
            start_lsn: Lsn(0x8B00_0000),
            end_lsn: Lsn(0x8B00_4000),
            payload,
        }
    }

    #[test]
    fn round_trips_compressible_incompressible_and_empty_payloads() {
        for payload in [vec![], vec![7u8; 100_000], lcg_bytes(1, 100_000)] {
            let frame = sample(payload);
            let wire = frame.encode();
            let (decoded, consumed) = Frame::decode(&wire).unwrap();
            assert_eq!(decoded, frame);
            assert_eq!(consumed, wire.len());
        }
    }

    #[test]
    fn compressible_payloads_actually_shrink() {
        let frame = sample(vec![7u8; 100_000]);
        assert!(frame.encode().len() < 2_000);
    }

    #[test]
    fn a_segment_of_frames_reads_back_in_order() {
        let frames: Vec<Frame> = (0..5)
            .map(|i| Frame {
                epoch: 42,
                fence: 1042,
                start_lsn: Lsn(i * 0x1000),
                end_lsn: Lsn((i + 1) * 0x1000),
                payload: lcg_bytes(i, 3000),
            })
            .collect();
        let segment: Vec<u8> = frames.iter().flat_map(Frame::encode).collect();
        let read: Vec<Frame> = SegmentReader::new(&segment, 42)
            .map(Result::unwrap)
            .collect();
        assert_eq!(read, frames);
    }

    #[test]
    fn stale_epochs_are_rejected_by_the_reader() {
        let segment = sample(b"data".to_vec()).encode();
        let err = SegmentReader::new(&segment, 43)
            .next()
            .unwrap()
            .unwrap_err();
        assert_eq!(
            err,
            WalDecodeError::StaleEpoch {
                found: 42,
                expected: 43
            }
        );
    }

    #[test]
    fn a_torn_tail_reads_as_truncated_and_ends_the_walk() {
        let mut segment: Vec<u8> = sample(b"first".to_vec()).encode();
        let second = sample(b"second".to_vec()).encode();
        segment.extend_from_slice(&second[..second.len() / 2]);

        let mut reader = SegmentReader::new(&segment, 42);
        assert!(reader.next().unwrap().is_ok());
        assert!(matches!(
            reader.next().unwrap().unwrap_err(),
            WalDecodeError::Truncated { .. }
        ));
        assert!(reader.next().is_none());
    }

    #[test]
    fn every_single_byte_corruption_is_caught_and_never_panics() {
        let wire = sample(lcg_bytes(2, 500)).encode();
        for i in 0..wire.len() {
            let mut bad = wire.clone();
            bad[i] ^= 0xFF;
            // Any outcome but a panic or a silently different frame is fine.
            if let Ok((frame, _)) = Frame::decode(&bad) {
                panic!("byte {i} corruption went unnoticed: {frame:?}");
            }
        }
    }

    #[test]
    fn truncation_at_every_length_errors_and_never_panics() {
        let wire = sample(lcg_bytes(3, 300)).encode();
        for len in 0..wire.len() {
            assert!(Frame::decode(&wire[..len]).is_err());
        }
    }

    #[test]
    fn random_garbage_never_panics() {
        for seed in 0..200 {
            let junk = lcg_bytes(seed, 4096);
            let _ = Frame::decode(&junk);
            // Garbage prefixed with a valid magic and version exercises the
            // deeper header paths.
            let mut magical = junk;
            magical[0..4].copy_from_slice(&WAL_MAGIC);
            magical[4..6].copy_from_slice(&WAL_VERSION.to_le_bytes());
            let _ = Frame::decode(&magical);
        }
    }

    #[test]
    fn a_lying_length_field_cannot_force_a_huge_allocation() {
        let mut wire = sample(b"tiny".to_vec()).encode();
        wire[40..44].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            Frame::decode(&wire),
            Err(WalDecodeError::BodyTooLarge { .. } | WalDecodeError::Corrupt)
        ));
    }

    #[test]
    fn newer_frame_versions_are_refused_with_an_upgrade_hint() {
        let mut wire = sample(b"x".to_vec()).encode();
        wire[4..6].copy_from_slice(&(WAL_VERSION + 1).to_le_bytes());
        let err = Frame::decode(&wire).unwrap_err();
        assert_eq!(
            err,
            WalDecodeError::UnsupportedVersion {
                found: WAL_VERSION + 1
            }
        );
        assert!(err.to_string().contains("upgrade zou"));
    }
}
