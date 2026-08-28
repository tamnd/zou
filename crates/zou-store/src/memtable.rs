//! The memtable: the mutable top of the layer store (spec 04 sec 3).
//!
//! Ingest parses WAL frames into records addressed to keys and puts
//! them here; reads take the records above the layered history; flush
//! drains everything into one delta layer and publishes it. A btree
//! over (key, lsn) gives all three the order they need for free.
//!
//! Re-ingest after a restart or reassignment replays the same WAL, so
//! an insert at an occupied (key, lsn) is the same record coming
//! around again: last write wins and the byte budget stays honest.

use std::collections::BTreeMap;
use std::ops::Bound;

use crate::layer::{DeltaEntry, LayerKey};
use crate::lsn::Lsn;

#[derive(Default)]
pub struct Memtable {
    entries: BTreeMap<(LayerKey, Lsn), Vec<u8>>,
    bytes: usize,
    /// Inclusive lsn bounds, carried along rather than derived. The
    /// btree is ordered by key first, so the only way to read the
    /// bounds back out of it is to walk every entry, and the flush
    /// check asks for them once per record applied. On a service
    /// catching up that walk is the catch up: a memtable heading for
    /// its flush threshold holds hundreds of thousands of entries and
    /// the apply goes quadratic, from under a millisecond a frame to
    /// nine of them (zou #336).
    lsns: Option<(Lsn, Lsn)>,
}

impl Memtable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: LayerKey, lsn: Lsn, record: Vec<u8>) {
        self.bytes += record.len();
        self.lsns = Some(match self.lsns {
            Some((lo, hi)) => (lo.min(lsn), hi.max(lsn)),
            None => (lsn, lsn),
        });
        if let Some(old) = self.entries.insert((key, lsn), record) {
            self.bytes -= old.len();
        }
    }

    /// Records for one key with lsn in `(floor, upto]`, ascending.
    /// The floor is the read plan's image lsn, already folded in.
    pub fn records_for(
        &self,
        key: &LayerKey,
        floor: Lsn,
        upto: Lsn,
    ) -> impl Iterator<Item = (Lsn, &[u8])> {
        self.entries
            .range((
                Bound::Excluded((*key, floor)),
                Bound::Included((*key, upto)),
            ))
            .map(|(&(_, lsn), record)| (lsn, record.as_slice()))
    }

    /// The records for a few keys, as a table of their own.
    ///
    /// One read needs the records for the handful of keys it names and
    /// nothing else, so this is a page's worth of bytes rather than a
    /// copy of the table. The page service takes it on the thread that
    /// owns the memtable and hands it to whichever thread does the
    /// read, which is what lets a read run beside ingest instead of
    /// behind it. What comes back is a fact about the table at the
    /// moment it was taken and stays true however far ingest moves on
    /// afterwards, since a record is written once at one lsn.
    ///
    /// Everything up to `upto`, with no floor, because the floor a read
    /// applies is the image lsn its plan settles on and the plan is not
    /// made yet. Reading past the floor costs the records between the
    /// image and the position asked for, which is the same handful.
    pub fn subset(&self, keys: &[LayerKey], upto: Lsn) -> Memtable {
        let mut out = Memtable::new();
        for key in keys {
            for (lsn, record) in self.records_for(key, Lsn(0), upto) {
                out.insert(*key, lsn, record.to_vec());
            }
        }
        out
    }

    /// Every entry in flush order, leaving the table alone. A read
    /// wants [`Self::records_for`]; this is for a caller that needs to
    /// see the whole table without emptying it, like a test asserting
    /// what an ingest indexed and under which kinds of key.
    pub fn iter(&self) -> impl Iterator<Item = (LayerKey, Lsn, &[u8])> {
        self.entries
            .iter()
            .map(|(&(key, lsn), record)| (key, lsn, record.as_slice()))
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Record payload bytes held, the flush threshold input.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Inclusive lsn bounds over everything held, None when empty.
    pub fn lsn_range(&self) -> Option<(Lsn, Lsn)> {
        self.lsns
    }

    /// Everything, in the (key, lsn) order [`crate::layer::build_delta`]
    /// wants, leaving the memtable empty. Flush encodes this into one
    /// delta layer and publishes it in the shard manifest.
    pub fn drain_sorted(&mut self) -> Vec<DeltaEntry> {
        self.bytes = 0;
        self.lsns = None;
        std::mem::take(&mut self.entries)
            .into_iter()
            .map(|((key, lsn), record)| DeltaEntry { key, lsn, record })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(block: u32) -> LayerKey {
        LayerKey::page(1663, 5, 16384, 0, block)
    }

    #[test]
    fn records_come_back_in_lsn_order_within_the_window() {
        let mut mem = Memtable::new();
        for lsn in [50u64, 10, 30, 20, 40] {
            mem.insert(k(1), Lsn(lsn), vec![lsn as u8]);
        }
        mem.insert(k(2), Lsn(25), vec![0xFF]);
        let got: Vec<(Lsn, &[u8])> = mem.records_for(&k(1), Lsn(10), Lsn(40)).collect();
        assert_eq!(
            got,
            vec![
                (Lsn(20), &[20u8][..]),
                (Lsn(30), &[30u8][..]),
                (Lsn(40), &[40u8][..]),
            ],
            "the floor is excluded, the ceiling included, other keys invisible"
        );
        assert_eq!(mem.records_for(&k(3), Lsn(0), Lsn(100)).count(), 0);
    }

    #[test]
    fn a_subset_is_the_keys_asked_for_and_reads_like_the_table_did() {
        let mut mem = Memtable::new();
        for block in [1u32, 2, 3] {
            for lsn in [10u64, 20, 30] {
                mem.insert(k(block), Lsn(lsn), vec![block as u8, lsn as u8]);
            }
        }
        let taken = mem.subset(&[k(1), k(3)], Lsn(20));
        assert_eq!(taken.len(), 4, "two keys, two lsns each under the ceiling");
        for block in [1u32, 3] {
            assert_eq!(
                taken
                    .records_for(&k(block), Lsn(0), Lsn(100))
                    .collect::<Vec<_>>(),
                mem.records_for(&k(block), Lsn(0), Lsn(20))
                    .collect::<Vec<_>>(),
                "block {block} reads out of the subset the way it read out of the table"
            );
        }
        assert_eq!(taken.records_for(&k(2), Lsn(0), Lsn(100)).count(), 0);
        // Which is the whole point: the reader holds this while ingest
        // goes on filling and draining the table it came from.
        mem.insert(k(1), Lsn(40), vec![9]);
        mem.drain_sorted();
        assert_eq!(taken.records_for(&k(1), Lsn(0), Lsn(100)).count(), 2);
    }

    #[test]
    fn reingest_overwrites_instead_of_duplicating() {
        let mut mem = Memtable::new();
        mem.insert(k(1), Lsn(10), vec![0; 100]);
        mem.insert(k(1), Lsn(10), vec![0; 100]);
        assert_eq!(mem.len(), 1);
        assert_eq!(mem.bytes(), 100);
        mem.insert(k(1), Lsn(4), vec![0; 10]);
        mem.insert(k(1), Lsn(10), vec![0; 10]);
        assert_eq!(
            mem.lsn_range(),
            Some((Lsn(4), Lsn(10))),
            "the carried bounds widen with what lands, replacement and all"
        );
    }

    #[test]
    fn drain_is_sorted_build_delta_input_and_resets() {
        let mut mem = Memtable::new();
        for (block, lsn) in [(2u32, 5u64), (1, 9), (1, 3), (2, 1)] {
            mem.insert(k(block), Lsn(lsn), vec![block as u8, lsn as u8]);
        }
        assert_eq!(mem.lsn_range(), Some((Lsn(1), Lsn(9))));
        let drained = mem.drain_sorted();
        let order: Vec<(LayerKey, Lsn)> = drained.iter().map(|e| (e.key, e.lsn)).collect();
        assert_eq!(
            order,
            vec![
                (k(1), Lsn(3)),
                (k(1), Lsn(9)),
                (k(2), Lsn(1)),
                (k(2), Lsn(5)),
            ]
        );
        crate::layer::build_delta(&drained, 1024).unwrap();
        assert!(mem.is_empty());
        assert_eq!(mem.bytes(), 0);
        assert_eq!(mem.lsn_range(), None);
    }
}
