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
}

impl Memtable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: LayerKey, lsn: Lsn, record: Vec<u8>) {
        self.bytes += record.len();
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
        let mut lsns = self.entries.keys().map(|&(_, lsn)| lsn);
        let first = lsns.next()?;
        let (min, max) = lsns.fold((first, first), |(lo, hi), l| (lo.min(l), hi.max(l)));
        Some((min, max))
    }

    /// Everything, in the (key, lsn) order [`crate::layer::build_delta`]
    /// wants, leaving the memtable empty. Flush encodes this into one
    /// delta layer and publishes it in the shard manifest.
    pub fn drain_sorted(&mut self) -> Vec<DeltaEntry> {
        self.bytes = 0;
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
    fn reingest_overwrites_instead_of_duplicating() {
        let mut mem = Memtable::new();
        mem.insert(k(1), Lsn(10), vec![0; 100]);
        mem.insert(k(1), Lsn(10), vec![0; 100]);
        assert_eq!(mem.len(), 1);
        assert_eq!(mem.bytes(), 100);
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
