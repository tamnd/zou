//! What a fork's SIZE marker says, and the part of it that makes a
//! lost page tell on itself.
//!
//! The marker used to be four bytes, the block count and nothing else,
//! and that is what made zou #546 possible: a block below the count
//! with no object anywhere reads as zeros, postgres takes an all zero
//! page for a new and empty one, and a table that lost a page comes up
//! short with nobody saying a word.
//!
//! Zeros are the right answer for a block that was extended and never
//! written, which is most of what an absent block object means, so the
//! read side cannot refuse on absence alone. It needs to know which
//! blocks have definitely been written, and that is what `dense` is:
//! every block below it has had a page reach the store at some point.
//! A block under `dense` that no tier can produce is not a hole, it is
//! gone.
//!
//! `dense` only ever moves forward, and only after the page for the
//! block below it is durable. That is the whole safety argument. A
//! stale low `dense` costs detection and never costs correctness, so
//! every place that cannot work out a better value leaves it where it
//! was, and a marker still in the old four byte form reads as zero,
//! which is the behaviour zou had before this existed.
//!
//! `filled` is the small amount of memory that lets `dense` keep up
//! with out of order writes. Postgres extends a relation in runs of at
//! most 64 blocks and the backends that take them write them back in
//! whatever order the buffer pool evicts them, so waiting for block
//! `dense` itself to be written would stall on the first block of a run
//! that happened to be used last. The bits cover `[dense, dense + 64)`,
//! a write inside that window sets one, and `dense` walks forward over
//! whatever prefix of set bits it finds. A run written back in any
//! order at all is caught up by the time its last page lands.

/// The block count of a fork and what is known to have been written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ForkSize {
    /// How long the fork is, the number the marker has always held.
    pub nblocks: u32,
    /// Every block below this has had a page in the store. Zero means
    /// nothing is claimed, which is what an old marker decodes to.
    pub dense: u32,
    /// Blocks written inside `[dense, dense + 64)`, bit 0 being `dense`
    /// itself. Bit 0 is always clear in a value at rest, because a set
    /// bit 0 is a `dense` that has not walked yet.
    pub filled: u64,
}

/// How far past `dense` the bits reach.
pub const WINDOW: u32 = 64;

/// The old marker, four bytes of block count. Still written by nothing,
/// still read by everything, because a store that predates this has
/// them and a fork nobody has touched since keeps them.
const OLD_LEN: usize = 4;

/// The current marker: count, `dense`, and the window.
const LEN: usize = 16;

impl ForkSize {
    /// A fork of `nblocks` with nothing claimed about its pages.
    pub fn plain(nblocks: u32) -> Self {
        Self {
            nblocks,
            dense: 0,
            filled: 0,
        }
    }

    /// Read a marker. An unrecognised length is an error rather than a
    /// guess, since the alternative is answering a length made of some
    /// other object's bytes.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        match bytes.len() {
            OLD_LEN => Some(Self::plain(u32::from_le_bytes(bytes.try_into().ok()?))),
            LEN => {
                let nblocks = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
                let dense = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
                let filled = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
                let mut it = Self {
                    nblocks,
                    dense,
                    filled,
                };
                // A marker written by an older or a confused writer can
                // claim more than the fork is long. Clamping here keeps
                // the one rule the read side leans on, that a block
                // below dense is a block that exists.
                it.dense = it.dense.min(it.nblocks);
                it.walk();
                Some(it)
            }
            _ => None,
        }
    }

    /// Write a marker in the current form.
    pub fn encode(&self) -> [u8; LEN] {
        let mut out = [0u8; LEN];
        out[0..4].copy_from_slice(&self.nblocks.to_le_bytes());
        out[4..8].copy_from_slice(&self.dense.to_le_bytes());
        out[8..16].copy_from_slice(&self.filled.to_le_bytes());
        out
    }

    /// Does the fork definitely hold a page for this block?
    ///
    /// Only `dense` answers yes. The window is deliberately not
    /// consulted: a set bit there says a page was written, but the
    /// blocks under it have not been accounted for, and answering yes
    /// for one of those would be the false accusation this whole thing
    /// is arranged to avoid.
    pub fn written(&self, blk: u32) -> bool {
        blk < self.dense
    }

    /// Note that a page for `blk` is in the store.
    pub fn fill(&mut self, blk: u32) {
        if blk < self.dense {
            return;
        }
        let off = blk - self.dense;
        if off >= WINDOW {
            // Too far ahead to remember. The block is written and this
            // forgets it, which costs detection on that block until
            // something walks dense past it, and costs nothing else.
            return;
        }
        self.filled |= 1u64 << off;
        self.walk();
    }

    /// Note a run of pages, which is what a writev lands.
    pub fn fill_run(&mut self, blk: u32, count: u32) {
        for b in blk..blk.saturating_add(count) {
            self.fill(b);
        }
    }

    /// Move `dense` over the prefix of the window that is set.
    fn walk(&mut self) {
        let steps = self.filled.trailing_ones();
        if steps == 0 {
            return;
        }
        self.dense = self.dense.saturating_add(steps).min(self.nblocks);
        self.filled = if steps >= WINDOW {
            0
        } else {
            self.filled >> steps
        };
    }

    /// Grow the fork. Extension says nothing about pages, so only the
    /// count moves.
    pub fn grow(&mut self, nblocks: u32) {
        if nblocks > self.nblocks {
            self.nblocks = nblocks;
        }
    }

    /// Cut the fork to `nblocks`. Everything the truncate removed is
    /// unwritten again, including the claim that it was ever written.
    pub fn truncate(&mut self, nblocks: u32) {
        self.nblocks = nblocks;
        if self.dense > nblocks {
            self.dense = nblocks;
            self.filled = 0;
            return;
        }
        let room = nblocks - self.dense;
        if room < WINDOW {
            self.filled &= (1u64 << room) - 1;
        }
    }

    /// Take the better of two views of the same fork.
    ///
    /// Both halves move one way only, so more is newer: the longer
    /// count and the further `dense` are each the more recent thing
    /// somebody knew. The windows are unioned after they are lined up
    /// on the further `dense`.
    pub fn merge(&self, other: &Self) -> Self {
        let (ahead, behind) = if self.dense >= other.dense {
            (self, other)
        } else {
            (other, self)
        };
        let shift = ahead.dense - behind.dense;
        let lifted = if shift >= WINDOW {
            0
        } else {
            behind.filled >> shift
        };
        let mut out = Self {
            nblocks: self.nblocks.max(other.nblocks),
            dense: ahead.dense.min(self.nblocks.max(other.nblocks)),
            filled: ahead.filled | lifted,
        };
        out.walk();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_old_four_byte_marker_claims_nothing() {
        let old = ForkSize::decode(&7u32.to_le_bytes()).expect("four bytes is a marker");
        assert_eq!(old.nblocks, 7);
        assert_eq!(old.dense, 0);
        assert!(
            !old.written(0),
            "a store written before any of this says nothing about its pages"
        );
    }

    #[test]
    fn a_marker_survives_a_round_trip() {
        let mut it = ForkSize::plain(9);
        it.fill_run(0, 4);
        let back = ForkSize::decode(&it.encode()).expect("what encode wrote, decode reads");
        assert_eq!(back, it);
        assert_eq!(back.dense, 4);
    }

    #[test]
    fn a_length_that_is_not_a_marker_is_refused() {
        assert!(ForkSize::decode(&[]).is_none());
        assert!(ForkSize::decode(&[1, 2, 3]).is_none());
        assert!(ForkSize::decode(&[0u8; 12]).is_none());
    }

    #[test]
    fn pages_written_in_order_carry_dense_with_them() {
        let mut it = ForkSize::plain(5);
        for b in 0..5 {
            it.fill(b);
            assert_eq!(it.dense, b + 1);
        }
        assert_eq!(it.filled, 0, "nothing is left waiting once dense caught up");
    }

    /// The case the window exists for: a run of blocks extended
    /// together and written back by whoever got to them first.
    #[test]
    fn a_run_written_back_out_of_order_still_catches_up() {
        let mut it = ForkSize::plain(8);
        for b in [5u32, 3, 7, 1, 6, 0, 4, 2] {
            it.fill(b);
        }
        assert_eq!(it.dense, 8);
        assert!(it.written(7));
    }

    #[test]
    fn a_block_written_far_ahead_of_dense_is_forgotten_rather_than_claimed() {
        let mut it = ForkSize::plain(1000);
        it.fill(500);
        assert_eq!(it.dense, 0);
        assert!(!it.written(500), "and the claim is not made anyway");
    }

    #[test]
    fn a_hole_holds_dense_where_it_is() {
        let mut it = ForkSize::plain(4);
        it.fill(1);
        it.fill(2);
        it.fill(3);
        assert_eq!(it.dense, 0, "block 0 was never written");
        assert!(!it.written(0));
        it.fill(0);
        assert_eq!(it.dense, 4, "and the whole run lands the moment it is");
    }

    #[test]
    fn a_truncate_takes_back_what_it_removed() {
        let mut it = ForkSize::plain(10);
        it.fill_run(0, 10);
        assert_eq!(it.dense, 10);
        it.truncate(4);
        assert_eq!(it.nblocks, 4);
        assert_eq!(it.dense, 4);
        assert!(!it.written(4), "the blocks it cut are not claimed any more");
    }

    #[test]
    fn a_truncate_inside_the_window_drops_the_bits_past_the_cut() {
        let mut it = ForkSize::plain(20);
        it.fill(3);
        it.fill(9);
        it.truncate(5);
        assert_eq!(it.filled, 1 << 3, "block 9 is not in the fork any more");
        it.fill_run(0, 3);
        assert_eq!(it.dense, 4);
    }

    #[test]
    fn dense_is_never_past_the_end_of_the_fork() {
        let mut it = ForkSize::plain(2);
        it.fill_run(0, 8);
        assert_eq!(it.dense, 2);
    }

    #[test]
    fn a_marker_claiming_more_than_the_fork_is_long_is_cut_down_to_it() {
        let mut bytes = [0u8; 16];
        bytes[0..4].copy_from_slice(&3u32.to_le_bytes());
        bytes[4..8].copy_from_slice(&99u32.to_le_bytes());
        let it = ForkSize::decode(&bytes).expect("a marker");
        assert_eq!(it.dense, 3);
    }

    #[test]
    fn two_views_of_a_fork_keep_the_further_of_each() {
        let mut a = ForkSize::plain(10);
        a.fill_run(0, 4);
        a.fill(6);
        let mut b = ForkSize::plain(12);
        b.fill_run(0, 5);
        let both = a.merge(&b);
        assert_eq!(both.nblocks, 12);
        assert_eq!(both.dense, 5);
        assert_eq!(both.filled, 1 << 1, "block 6 is still remembered");
        assert_eq!(both, b.merge(&a), "and it does not matter which way round");
    }
}
