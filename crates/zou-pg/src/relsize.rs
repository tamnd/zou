//! Relation sizes in the layer keyspace: what a relsize record says
//! and how a read folds a chain of them into one block count.
//!
//! `smgr nblocks` has to be answerable from the layers, not from the
//! parent's `pg/` prefix, because a branch reads its inherited state
//! out of the layers and nothing else. The size cannot be inferred
//! from the highest block a relation has records for, because a
//! truncate makes that a lie: the blocks above the cut keep their
//! history and the relation is shorter than they suggest. So the size
//! is a fact of its own, keyed by [`LayerKey::relsize`] and written
//! where the WAL says it changed.
//!
//! Two kinds of record, which is all the WAL distinguishes:
//!
//! - `Grow(n)`: something referenced block `n - 1`, so the fork is at
//!   least `n` long. Growth is a floor, not an assignment, because one
//!   record can touch a block below the current end without shortening
//!   anything.
//! - `Set(n)`: a truncate said the fork is exactly `n` long. This one
//!   assigns, and it is the only thing that ever makes a size smaller.
//!
//! Folding is therefore a left fold over the chain in lsn order,
//! starting from the base: `Grow` takes the max, `Set` overwrites. An
//! image of a relsize key, when compaction learns to cut one, is a
//! single `Set` at the image lsn, which is why the base decodes with
//! the same reader.

use zou_store::layer::LayerKey;
use zou_store::lsn::Lsn;

/// Bytes of one encoded record: a tag and a block count.
pub const REC_LEN: usize = 5;

/// One relation fork, the thing a size is a size of. A page key is
/// this plus a block number, which is why it travels as one value
/// rather than four loose fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ForkRef {
    pub spc: u32,
    pub db: u32,
    pub rel: u32,
    pub fork: u8,
}

impl ForkRef {
    pub fn key(&self) -> LayerKey {
        LayerKey::relsize(self.spc, self.db, self.rel, self.fork)
    }
}

const OP_GROW: u8 = 0;
const OP_SET: u8 = 1;

/// One thing the WAL said about the length of a relation fork.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeRec {
    /// The fork is at least this many blocks long.
    Grow(u32),
    /// The fork is exactly this many blocks long.
    Set(u32),
}

impl SizeRec {
    pub fn encode(&self) -> [u8; REC_LEN] {
        let (op, n) = match self {
            SizeRec::Grow(n) => (OP_GROW, *n),
            SizeRec::Set(n) => (OP_SET, *n),
        };
        let mut out = [0u8; REC_LEN];
        out[0] = op;
        out[1..5].copy_from_slice(&n.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let head: [u8; REC_LEN] = bytes
            .get(..REC_LEN)
            .ok_or_else(|| format!("relsize record of {} bytes, want {REC_LEN}", bytes.len()))?
            .try_into()
            .expect("checked length");
        let n = u32::from_le_bytes(head[1..5].try_into().expect("four bytes"));
        match head[0] {
            OP_GROW => Ok(SizeRec::Grow(n)),
            OP_SET => Ok(SizeRec::Set(n)),
            op => Err(format!("unknown relsize op {op}")),
        }
    }
}

/// The block count a base plus a record chain adds up to, the chain
/// ascending by lsn as [`zou_store::pageread::LayerReader::reconstruct`]
/// returns it. A fork with no base and no records is zero blocks long,
/// which is the same answer the object path gives for a relation
/// nothing has extended.
pub fn fold(base: Option<&[u8]>, records: &[(Lsn, Vec<u8>)]) -> Result<u32, String> {
    let mut size = match base {
        Some(bytes) => match SizeRec::decode(bytes)? {
            SizeRec::Set(n) => n,
            // An image is the whole answer as of its lsn, so a floor
            // is not one. Refusing says the writer was wrong rather
            // than quietly serving a length nothing vouches for.
            SizeRec::Grow(n) => return Err(format!("relsize image is a floor of {n}, not a size")),
        },
        None => 0,
    };
    for (_, rec) in records {
        match SizeRec::decode(rec)? {
            SizeRec::Grow(n) => size = size.max(n),
            SizeRec::Set(n) => size = n,
        }
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chain(recs: &[SizeRec]) -> Vec<(Lsn, Vec<u8>)> {
        recs.iter()
            .enumerate()
            .map(|(i, r)| (Lsn(i as u64 + 1), r.encode().to_vec()))
            .collect()
    }

    #[test]
    fn round_trips() {
        for r in [
            SizeRec::Grow(0),
            SizeRec::Grow(9),
            SizeRec::Set(0),
            SizeRec::Set(u32::MAX),
        ] {
            assert_eq!(SizeRec::decode(&r.encode()).unwrap(), r);
        }
    }

    #[test]
    fn a_short_record_is_refused() {
        assert!(SizeRec::decode(&[0u8; 3]).is_err());
        assert!(SizeRec::decode(&[7, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn nothing_written_is_no_blocks() {
        assert_eq!(fold(None, &[]).unwrap(), 0);
    }

    #[test]
    fn growth_takes_the_high_water_mark() {
        // The second record touches a block below the end, which is
        // the ordinary case for an update, and must not shorten it.
        let recs = chain(&[SizeRec::Grow(4), SizeRec::Grow(2), SizeRec::Grow(7)]);
        assert_eq!(fold(None, &recs).unwrap(), 7);
    }

    #[test]
    fn a_truncate_shortens_and_growth_resumes() {
        let recs = chain(&[SizeRec::Grow(7), SizeRec::Set(3), SizeRec::Grow(5)]);
        assert_eq!(fold(None, &recs).unwrap(), 5);
        let recs = chain(&[SizeRec::Grow(7), SizeRec::Set(3)]);
        assert_eq!(fold(None, &recs).unwrap(), 3);
    }

    #[test]
    fn a_drop_to_nothing_reads_as_nothing() {
        let recs = chain(&[SizeRec::Grow(7), SizeRec::Set(0)]);
        assert_eq!(fold(None, &recs).unwrap(), 0);
    }

    #[test]
    fn the_base_is_where_the_fold_starts() {
        let base = SizeRec::Set(12).encode();
        assert_eq!(fold(Some(&base), &[]).unwrap(), 12);
        let recs = chain(&[SizeRec::Grow(4)]);
        assert_eq!(fold(Some(&base), &recs).unwrap(), 12);
        let recs = chain(&[SizeRec::Set(4)]);
        assert_eq!(fold(Some(&base), &recs).unwrap(), 4);
    }

    #[test]
    fn a_floor_is_not_an_image() {
        let base = SizeRec::Grow(12).encode();
        assert!(fold(Some(&base), &[]).is_err());
    }
}
