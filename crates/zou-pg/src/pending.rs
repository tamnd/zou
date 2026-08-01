//! Per process write buffer behind the smgr FFI.
//!
//! Postgres marks bulk relation fills with skipFsync, the caller owns
//! durability and settles it later through smgrregistersync or
//! smgrimmedsync. md leans on the OS page cache for exactly this, so
//! the shim gets the same license: skipFsync pages accumulate here and
//! drain to the store in one parallel burst instead of one durable put
//! per page. The pages all belong to relfilenodes no other backend can
//! see yet (bulk_write targets brand new relfilenodes), so keeping
//! them process local is safe, and a crash before the drain only ever
//! loses pages that a missing commit record already makes unreachable.
//!
//! The buffer caps at ZOU_SMGR_BUFFER_MB (default 8, 0 turns buffering
//! off) and drains early when full. SIZE markers are settled once per
//! fork per drain rather than once per extend.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use zou_store::CasStore;
use zou_store::layout::TenantLayout;

use crate::ZOU_PAGE_SIZE;

/// (spc, db, rel, fork), the key Postgres addresses forks by.
pub type ForkId = (u32, u32, u32, u32);

/// A buffered block and its bytes, as drains hand them out.
pub type PendingPage = (u32, Box<[u8]>);

/// How many store operations run at once during a drain, a truncate,
/// or an unlink. ZOU_SMGR_PARALLEL overrides, minimum 1.
pub fn parallelism() -> usize {
    std::env::var("ZOU_SMGR_PARALLEL")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(16)
        .max(1)
}

fn buffer_cap_bytes() -> usize {
    let mb = std::env::var("ZOU_SMGR_BUFFER_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8);
    mb << 20
}

struct ForkPending {
    pages: BTreeMap<u32, Box<[u8]>>,
    /// The fork size these extends grew toward, published on drain.
    size: Option<u32>,
}

/// Buffered skipFsync pages for this process, keyed by fork.
pub struct Pending {
    forks: BTreeMap<ForkId, ForkPending>,
    bytes: usize,
    cap: usize,
}

impl Default for Pending {
    fn default() -> Self {
        Self {
            forks: BTreeMap::new(),
            bytes: 0,
            cap: buffer_cap_bytes(),
        }
    }
}

impl Pending {
    /// Buffering is off entirely when the cap is zero.
    pub fn enabled(&self) -> bool {
        self.cap > 0
    }

    /// Stash a page. `grow_to` carries the fork size an extend implies.
    /// Returns true when the buffer is over its cap and the caller must
    /// drain.
    pub fn push(&mut self, fork: ForkId, blk: u32, page: &[u8], grow_to: Option<u32>) -> bool {
        let entry = self.forks.entry(fork).or_insert_with(|| ForkPending {
            pages: BTreeMap::new(),
            size: None,
        });
        if entry.pages.insert(blk, page.into()).is_none() {
            self.bytes += ZOU_PAGE_SIZE;
        }
        if let Some(n) = grow_to {
            entry.size = Some(entry.size.map_or(n, |s| s.max(n)));
        }
        self.bytes >= self.cap
    }

    /// A buffered page, if this process wrote one for the block.
    pub fn page(&self, fork: ForkId, blk: u32) -> Option<&[u8]> {
        self.forks.get(&fork)?.pages.get(&blk).map(|p| &p[..])
    }

    /// Refresh the buffered copy of a block if one exists. An eager
    /// write that lands in the store directly must not leave an older
    /// buffered page shadowing reads.
    pub fn refresh(&mut self, fork: ForkId, blk: u32, page: &[u8]) {
        if let Some(fp) = self.forks.get_mut(&fork)
            && let Some(slot) = fp.pages.get_mut(&blk)
        {
            slot.copy_from_slice(page);
        }
    }

    /// The fork size buffered extends imply, if any are waiting.
    pub fn size(&self, fork: ForkId) -> Option<u32> {
        self.forks.get(&fork)?.size
    }

    pub fn is_empty(&self) -> bool {
        self.forks.is_empty()
    }

    /// Forget everything buffered for the fork. Unlink calls this, the
    /// pages belong to a relfilenode that no longer exists.
    pub fn forget(&mut self, fork: ForkId) {
        if let Some(fp) = self.forks.remove(&fork) {
            self.bytes -= fp.pages.len() * ZOU_PAGE_SIZE;
        }
    }

    /// Drop buffered pages at or past the new end and clamp the
    /// buffered size, mirroring what truncate does to the store.
    pub fn truncate(&mut self, fork: ForkId, nblocks: u32) {
        if let Some(fp) = self.forks.get_mut(&fork) {
            let cut = fp.pages.split_off(&nblocks);
            self.bytes -= cut.len() * ZOU_PAGE_SIZE;
            fp.size = fp.size.map(|s| s.min(nblocks));
            if fp.pages.is_empty() && fp.size.is_none() {
                self.forks.remove(&fork);
            }
        }
    }

    /// Take one fork's pending state out of the buffer, `None` when the
    /// fork has nothing waiting.
    pub fn take_fork(&mut self, fork: ForkId) -> Option<(Vec<PendingPage>, Option<u32>)> {
        let fp = self.forks.remove(&fork)?;
        self.bytes -= fp.pages.len() * ZOU_PAGE_SIZE;
        Some((fp.pages.into_iter().collect(), fp.size))
    }

    /// Take everything, fork by fork.
    pub fn take_all(&mut self) -> Vec<(ForkId, Vec<PendingPage>, Option<u32>)> {
        let forks = std::mem::take(&mut self.forks);
        self.bytes = 0;
        forks
            .into_iter()
            .map(|(id, fp)| (id, fp.pages.into_iter().collect(), fp.size))
            .collect()
    }
}

/// Run `op` over every item from a shared work list, `parallelism()`
/// threads wide, and report whether every call succeeded. Order does
/// not matter to any caller, puts and deletes commute across distinct
/// keys.
pub fn for_each_parallel<T: Sync>(items: &[T], op: impl Fn(&T) -> bool + Sync) -> bool {
    if items.is_empty() {
        return true;
    }
    let workers = parallelism().min(items.len());
    if workers == 1 {
        return items.iter().all(&op);
    }
    let next = AtomicUsize::new(0);
    let ok = std::sync::atomic::AtomicBool::new(true);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= items.len() || !ok.load(Ordering::Relaxed) {
                        break;
                    }
                    if !op(&items[i]) {
                        ok.store(false, Ordering::Relaxed);
                    }
                }
            });
        }
    });
    ok.load(Ordering::Relaxed)
}

/// Flush one fork's drained pages and settle its SIZE marker. The
/// pages go first, in parallel, and SIZE only ever grows here, the
/// same rule the eager extend path follows.
pub(crate) fn flush_fork(
    store: &Arc<dyn CasStore>,
    layout: &TenantLayout,
    fork: ForkId,
    pages: &[PendingPage],
    size: Option<u32>,
) -> Result<(), ()> {
    let (spc, db, rel, fk) = fork;
    let ok = for_each_parallel(pages, |(blk, page)| {
        store
            .put(&layout.pg_block(spc, db, rel, fk, *blk), page)
            .is_ok()
    });
    if !ok {
        return Err(());
    }
    if let Some(new_size) = size {
        let key = layout.pg_size(spc, db, rel, fk);
        let current = match store.get(&key) {
            Ok(Some((data, _))) => {
                let bytes: [u8; 4] = data.as_slice().try_into().map_err(|_| ())?;
                u32::from_le_bytes(bytes)
            }
            Ok(None) => 0,
            Err(_) => return Err(()),
        };
        if new_size > current {
            store.put(&key, &new_size.to_le_bytes()).map_err(|_| ())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(fill: u8) -> Vec<u8> {
        vec![fill; ZOU_PAGE_SIZE]
    }

    #[test]
    fn pages_accumulate_and_read_back_until_taken() {
        let mut p = Pending::default();
        let fork = (1, 2, 3, 0);
        assert!(!p.push(fork, 0, &page(1), Some(1)));
        assert!(!p.push(fork, 1, &page(2), Some(2)));
        assert_eq!(p.page(fork, 0).unwrap()[0], 1);
        assert_eq!(p.size(fork), Some(2));
        assert!(p.page(fork, 9).is_none());
        let (pages, size) = p.take_fork(fork).unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(size, Some(2));
        assert!(p.is_empty());
    }

    #[test]
    fn the_cap_asks_for_a_drain_and_rewrites_do_not_double_count() {
        unsafe { std::env::set_var("ZOU_SMGR_BUFFER_MB", "1") };
        let mut p = Pending::default();
        unsafe { std::env::remove_var("ZOU_SMGR_BUFFER_MB") };
        let fork = (1, 2, 3, 0);
        let pages_to_cap = (1 << 20) / ZOU_PAGE_SIZE;
        for blk in 0..pages_to_cap as u32 - 1 {
            assert!(!p.push(fork, blk, &page(1), None), "block {blk}");
            assert!(!p.push(fork, blk, &page(2), None), "rewrite {blk}");
        }
        assert!(p.push(fork, pages_to_cap as u32 - 1, &page(1), None));
    }

    #[test]
    fn truncate_drops_the_tail_and_clamps_the_size() {
        let mut p = Pending::default();
        let fork = (1, 2, 3, 0);
        for blk in 0..4 {
            p.push(fork, blk, &page(blk as u8), Some(blk + 1));
        }
        p.truncate(fork, 2);
        assert!(p.page(fork, 2).is_none());
        assert!(p.page(fork, 3).is_none());
        assert_eq!(p.page(fork, 1).unwrap()[0], 1);
        assert_eq!(p.size(fork), Some(2));
        p.forget(fork);
        assert!(p.is_empty());
    }

    #[test]
    fn parallel_for_each_covers_every_item_and_reports_failure() {
        let hits = AtomicUsize::new(0);
        let items: Vec<u32> = (0..100).collect();
        assert!(for_each_parallel(&items, |_| {
            hits.fetch_add(1, Ordering::Relaxed);
            true
        }));
        assert_eq!(hits.load(Ordering::Relaxed), 100);
        assert!(!for_each_parallel(&items, |i| *i != 50));
    }
}
