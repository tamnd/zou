//! Fault the pages recovery is about to ask for, before it asks.
//!
//! A cold attach restores a skeleton, no relation file in it, and then
//! crash recovery replays the WAL tail. Every record it replays wants
//! the page it modifies, and every one of those pages is a store round
//! trip taken one at a time, because redo is a single process reading
//! one block per record. On a local disk that is invisible. Against an
//! object store at thirty milliseconds a get it is the whole of the
//! attach: 10,586 serial gets is fifteen minutes of redo on a
//! database whose working set fits in a laptop's page cache.
//!
//! The set of pages redo will want is knowable before it starts. It is
//! written down in the WAL the restore just laid into pg_wal: walk the
//! records from the checkpoint redo location to the end of the stream
//! and every block reference is a page recovery will touch. Minus the
//! ones it will not read, a record carrying a full page image or a
//! will init flag hands redo a page without asking storage for it.
//! What is left is the fault list, and this module fetches it in
//! parallel into the local page cache the postmaster is about to be
//! pointed at. Recovery then reads them from a file on local disk.
//!
//! Two things make this safe that would not be safe later. The first
//! is that nothing else is running: this happens between the restore
//! and the postmaster spawn, so no backend can be writing a page while
//! it is fetched, and the coherence argument the page cache rests on,
//! that two processes never do IO on one block at once, is not even
//! tested.
//!
//! The second is what the checkpoint means. A cached page answers
//! before the chain does, so warming out of pg/ would be wrong if pg/
//! could hold a page older than the redo location, and it cannot: a
//! checkpoint is the promise that every page modified before it is
//! durable, and durable here is the pg/ object. So a warmed page is at
//! worst the page as it stood at the redo location, and every record
//! after it is in the WAL recovery is about to replay. Redo compares
//! page LSNs, so a page that turns out to be newer than the record
//! costs nothing either. Both are what the read path would have
//! handed redo anyway, one round trip later.
//!
//! It is best effort throughout. A miss, a store error, or a cap hit
//! costs a round trip later and nothing else, so nothing here fails an
//! attach.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use zou_store::layout::TenantLayout;
use zou_store::{CasStore, open_store};

use crate::pagecache::PageCache;
use crate::pending::{ForkId, for_each_parallel};
use crate::restore::{WAL_SEGMENT_SIZE, control_redo, wal_file_name};
use crate::walscan::{self, WalWindow};
use crate::{ZOU_PAGE_SIZE, restore::RestoreStats};

/// How many pages a warm up may fetch, over ZOU_WARM_BLOCKS. The
/// default is half a gigabyte of pages, which is more than a recovery
/// between two folds ever touches and small enough that a pathological
/// tail cannot turn the attach into a download. Zero turns warming
/// off.
fn block_cap() -> usize {
    std::env::var("ZOU_WARM_BLOCKS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(65536)
}

/// How much WAL the fault scan reads off disk, over ZOU_WARM_WAL_MB.
/// The scan is local and cheap, this only bounds the memory it holds.
fn wal_cap() -> u64 {
    std::env::var("ZOU_WARM_WAL_MB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(256)
        << 20
}

/// What a warm up did. `wanted` is the fault list the WAL named,
/// `fetched` the pages that made it into the cache, and `capped` says
/// the list was longer than the cap and the tail of it was dropped,
/// which is the one outcome worth a word in the log.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct WarmStats {
    pub wanted: usize,
    pub fetched: usize,
    pub bytes: u64,
    pub forks: usize,
    pub capped: bool,
}

/// Read the WAL the restore laid down over `[redo, wal_end)` into one
/// window. Segments are read whole because that is how they sit on
/// disk; a missing one ends the window rather than failing, the scan
/// takes whatever is contiguous.
fn wal_window(pg_wal: &Path, tli: u32, redo: u64, wal_end: u64) -> Result<WalWindow, String> {
    let base = redo - redo % WAL_SEGMENT_SIZE;
    let mut buf = Vec::new();
    let mut seg = base;
    while seg < wal_end && (seg - base) < wal_cap() {
        let path = pg_wal.join(wal_file_name(tli, seg));
        match std::fs::read(&path) {
            Ok(bytes) => buf.extend_from_slice(&bytes),
            Err(_) => break,
        }
        seg += WAL_SEGMENT_SIZE;
    }
    // Past the stream end is zero fill from the restore, and the scan
    // reads zeros as an xlog switch and walks the rest of the segment
    // for nothing. Cut the window where the bytes stop meaning
    // something.
    let end = (base + buf.len() as u64).min(wal_end);
    buf.truncate((end - base) as usize);
    Ok(WalWindow::from_raw(base, buf))
}

/// Prefetch what recovery will fault in. `pgdata` is a data directory
/// a restore just wrote, `cache` the page cache directory the
/// postmaster will be started with, and `stats` what the restore
/// reported, for the timeline and the stream end.
///
/// Errors are the caller's to log and ignore: every one of them means
/// a page arrives the slow way instead of the fast way.
pub fn warm(
    target: &str,
    tenant: &str,
    pgdata: &Path,
    cache: &Path,
    stats: &RestoreStats,
) -> Result<WarmStats, String> {
    let cap = block_cap();
    if cap == 0 {
        return Ok(WarmStats::default());
    }
    // With the page service on, pg/ is not where a read comes from:
    // eager puts are elided and the service owns everything past the
    // frozen images, so warming out of pg/ would fill the cache with
    // pages older than the ones recovery must see. Warming the service
    // is its own design and its own patch.
    if std::env::var("ZOU_PAGESERVE").is_ok_and(|v| !v.is_empty()) {
        return Ok(WarmStats::default());
    }

    let control = std::fs::read(pgdata.join("global/pg_control"))
        .map_err(|e| format!("read pg_control: {e}"))?;
    let redo = control_redo(&control)?;
    if redo == 0 || redo >= stats.wal_end {
        return Ok(WarmStats::default());
    }

    let window = wal_window(&pgdata.join("pg_wal"), stats.timeline, redo, stats.wal_end)?;
    let faults = walscan::scan_faults(&window, redo)?;
    let mut forks: Vec<ForkId> = faults
        .iter()
        .map(|(r, _)| (r.spc, r.db, r.rel, r.fork))
        .collect();
    forks.sort_unstable();
    forks.dedup();
    let mut wanted: Vec<crate::walscan::BlockRef> = faults
        .into_iter()
        .filter(|(_, init)| !*init)
        .map(|(r, _)| r)
        .collect();
    let capped = wanted.len() > cap;
    wanted.truncate(cap);
    if wanted.is_empty() {
        return Ok(WarmStats {
            capped,
            ..WarmStats::default()
        });
    }

    let store: Arc<dyn CasStore> = Arc::from(open_store(target)?);
    let layout = TenantLayout::new(tenant);
    let cache = PageCache::at(cache);
    let fetched = AtomicUsize::new(0);
    let bytes = AtomicU64::new(0);

    // Fork sizes first, and in the same parallel pass: recovery asks
    // for nblocks on the first record of every relation it touches,
    // which is another serial round trip each, and the same cache
    // answers it.
    for_each_parallel(&forks, |&(spc, db, rel, fork)| {
        if let Ok(Some((data, _))) = store.get(&layout.pg_size(spc, db, rel, fork))
            && let Ok(n) = <[u8; 4]>::try_from(data.as_slice())
        {
            cache.save_size((spc, db, rel, fork), u32::from_le_bytes(n));
        }
        true
    });
    for_each_parallel(&wanted, |r| {
        match store.get(&layout.pg_block(r.spc, r.db, r.rel, r.fork, r.blk)) {
            Ok(Some((data, _))) if data.len() == ZOU_PAGE_SIZE => {
                cache.save((r.spc, r.db, r.rel, r.fork), r.blk, &data);
                fetched.fetch_add(1, Ordering::Relaxed);
                bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
            }
            // An absent block reads as zeros and a short one is a
            // store this has no business second guessing. Both are
            // left for the read path to deal with.
            _ => {}
        }
        true
    });

    Ok(WarmStats {
        wanted: wanted.len(),
        fetched: fetched.load(Ordering::Relaxed),
        bytes: bytes.load(Ordering::Relaxed),
        forks: forks.len(),
        capped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::walscan::BlockRef;
    use crate::walscan::testwal::Builder;
    use zou_store::LocalFsStore;

    fn blk(rel: u32, blk: u32) -> BlockRef {
        BlockRef {
            spc: 1663,
            db: 5,
            rel,
            fork: 0,
            blk,
        }
    }

    /// pg_control shaped the way [`control_redo`] reads it, with the
    /// redo location the caller wants and a crc that validates.
    fn synthetic_control(redo: u64) -> Vec<u8> {
        let mut control = vec![0u8; 8192];
        control[0..8].copy_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());
        control[16..20].copy_from_slice(&6u32.to_le_bytes());
        for (i, b) in control[20..300].iter_mut().enumerate() {
            *b = (i * 7 + 13) as u8;
        }
        control[40..48].copy_from_slice(&redo.to_le_bytes());
        let crc = crc32c::crc32c(&control[..300]);
        control[300..304].copy_from_slice(&crc.to_le_bytes());
        control
    }

    /// A restored data directory: a pg_control naming the redo, and
    /// one WAL segment holding the records past it.
    fn lay_down(pgdata: &Path, redo: u64, wal: &[u8]) {
        std::fs::create_dir_all(pgdata.join("global")).expect("mkdir global");
        std::fs::create_dir_all(pgdata.join("pg_wal")).expect("mkdir pg_wal");
        std::fs::write(pgdata.join("global/pg_control"), synthetic_control(redo))
            .expect("write pg_control");
        std::fs::write(
            pgdata.join("pg_wal").join(wal_file_name(1, redo)),
            [wal, &vec![0u8; WAL_SEGMENT_SIZE as usize - wal.len()]].concat(),
        )
        .expect("write segment");
    }

    fn page(fill: u8) -> Vec<u8> {
        vec![fill; ZOU_PAGE_SIZE]
    }

    #[test]
    fn the_fault_list_is_what_redo_reads_and_not_what_it_writes() {
        let mut b = Builder::new(WAL_SEGMENT_SIZE);
        b.record(&[(blk(16384, 3), false)], b"read this one");
        b.image_record(blk(16384, 9), &page(0xAB), 10);
        // A second mention of a block an image already restored is not
        // a read either, redo has the page by then.
        b.record(&[(blk(16384, 9), false)], b"again");
        b.record(&[(blk(16384, 3), false)], b"and again");
        let window = b.window();

        let faults = walscan::scan_faults(&window, WAL_SEGMENT_SIZE).expect("scan");
        assert_eq!(
            faults,
            vec![(blk(16384, 3), false), (blk(16384, 9), true)],
            "first mention wins, in the order redo reaches them"
        );
    }

    #[test]
    fn warming_lands_the_pages_recovery_would_have_faulted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("store");
        let pgdata = dir.path().join("pgdata");
        let cache = dir.path().join("pagecache");

        let mut b = Builder::new(WAL_SEGMENT_SIZE);
        b.record(&[(blk(16384, 3), false)], b"one");
        b.record(&[(blk(16384, 7), false)], b"two");
        b.image_record(blk(16384, 9), &page(0xAB), 10);
        let end = b.pos();
        let (_, bytes) = b.stream();
        lay_down(&pgdata, WAL_SEGMENT_SIZE, bytes);

        let store = LocalFsStore::new(target.clone());
        let layout = TenantLayout::new("t");
        store
            .put(&layout.pg_block(1663, 5, 16384, 0, 3), &page(3))
            .expect("put 3");
        store
            .put(&layout.pg_block(1663, 5, 16384, 0, 7), &page(7))
            .expect("put 7");
        store
            .put(&layout.pg_block(1663, 5, 16384, 0, 9), &page(9))
            .expect("put 9");
        store
            .put(&layout.pg_size(1663, 5, 16384, 0), &12u32.to_le_bytes())
            .expect("put size");

        let stats = RestoreStats {
            wal_end: end,
            timeline: 1,
            ..RestoreStats::default()
        };
        let warmed =
            warm(target.to_str().expect("utf8"), "t", &pgdata, &cache, &stats).expect("warm");
        assert_eq!(warmed.wanted, 2);
        assert_eq!(warmed.fetched, 2);
        assert!(!warmed.capped);

        let cached = PageCache::at(&cache);
        assert_eq!(cached.load((1663, 5, 16384, 0), 3), Some(page(3)));
        assert_eq!(cached.load((1663, 5, 16384, 0), 7), Some(page(7)));
        assert_eq!(
            cached.load((1663, 5, 16384, 0), 9),
            None,
            "the image record's block is never read, so it is never fetched"
        );
        assert_eq!(cached.load_size((1663, 5, 16384, 0)), Some(12));
    }

    #[test]
    fn the_cap_bounds_what_a_long_tail_can_cost() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("store");
        let pgdata = dir.path().join("pgdata");
        let cache = dir.path().join("pagecache");

        let mut b = Builder::new(WAL_SEGMENT_SIZE);
        for i in 0..8 {
            b.record(&[(blk(16384, i), false)], b"x");
        }
        let end = b.pos();
        let (_, bytes) = b.stream();
        lay_down(&pgdata, WAL_SEGMENT_SIZE, bytes);

        let store = LocalFsStore::new(target.clone());
        let layout = TenantLayout::new("t");
        for i in 0..8 {
            store
                .put(&layout.pg_block(1663, 5, 16384, 0, i), &page(i as u8))
                .expect("put");
        }

        let stats = RestoreStats {
            wal_end: end,
            timeline: 1,
            ..RestoreStats::default()
        };
        unsafe { std::env::set_var("ZOU_WARM_BLOCKS", "3") };
        let warmed =
            warm(target.to_str().expect("utf8"), "t", &pgdata, &cache, &stats).expect("warm");
        unsafe { std::env::remove_var("ZOU_WARM_BLOCKS") };

        assert_eq!(warmed.wanted, 3);
        assert_eq!(warmed.fetched, 3);
        assert!(warmed.capped, "the tail was dropped and says so");
        let cached = PageCache::at(&cache);
        assert!(cached.load((1663, 5, 16384, 0), 2).is_some());
        assert!(cached.load((1663, 5, 16384, 0), 3).is_none());
    }
}
