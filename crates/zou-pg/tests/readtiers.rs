//! The read tier counters, wired end to end: smgr reads through the C
//! ABI land in the counter file under the tier that served them, and
//! the snapshot the bench scrapes says what happened.
//!
//! This lives in its own integration binary because the shim and the
//! stats file handle are process globals, and the env vars steering
//! both must be set before anything touches them.

use std::ffi::CString;

use zou_pg::{
    ZOU_OK, ZOU_PAGE_SIZE, zou_pg_init, zou_smgr_create, zou_smgr_extend, zou_smgr_read,
    zou_smgr_readv, zou_smgr_zeroextend,
};
use zou_store::Snapshot;

#[test]
fn smgr_reads_land_in_their_tiers() {
    let store_dir = tempfile::tempdir().unwrap();
    let cache_dir = tempfile::tempdir().unwrap();
    let stats = store_dir.path().join("stats");
    // Safety: nothing else runs in this test process yet.
    //
    // This is the object path on purpose, so it says so: the default is
    // the page service, and there is no service here to answer a read.
    unsafe {
        std::env::set_var("ZOU_STORE_STATS", &stats);
        std::env::set_var("ZOU_PAGE_CACHE", cache_dir.path());
        std::env::set_var("ZOU_PAGESERVE", "0");
    }
    let target = CString::new(store_dir.path().join("store").to_str().unwrap()).unwrap();
    assert_eq!(unsafe { zou_pg_init(target.as_ptr()) }, ZOU_OK);

    let (spc, db, rel, fork) = (1663u32, 5u32, 16384u32, 0u32);
    assert_eq!(zou_smgr_create(spc, db, rel, fork), ZOU_OK);
    for blk in 0u32..2 {
        let page = [blk as u8 + 1; ZOU_PAGE_SIZE];
        assert_eq!(
            unsafe { zou_smgr_extend(spc, db, rel, fork, blk, page.as_ptr(), 0) },
            ZOU_OK
        );
    }
    // Blocks 2 and 3 exist by size only, no bytes anywhere local, so
    // reading them must pay the store round trip that answers absent.
    assert_eq!(zou_smgr_zeroextend(spc, db, rel, fork, 2, 2), ZOU_OK);

    // Written blocks serve from the local page cache: cache tier.
    let mut buf = [0u8; ZOU_PAGE_SIZE];
    for blk in 0u32..2 {
        assert_eq!(
            unsafe { zou_smgr_read(spc, db, rel, fork, blk, buf.as_mut_ptr(), 0) },
            ZOU_OK
        );
        assert!(buf.iter().all(|b| *b == blk as u8 + 1));
    }
    // A zero extended block: store tier.
    assert_eq!(
        unsafe { zou_smgr_read(spc, db, rel, fork, 3, buf.as_mut_ptr(), 0) },
        ZOU_OK
    );
    assert!(buf.iter().all(|b| *b == 0));

    // A vectored read across both kinds: pages split by serving tier,
    // the one call lands under store because the misses went out.
    let mut bufs = [[0u8; ZOU_PAGE_SIZE]; 4];
    let ptrs: Vec<*mut u8> = bufs.iter_mut().map(|b| b.as_mut_ptr()).collect();
    assert_eq!(
        unsafe { zou_smgr_readv(spc, db, rel, fork, 0, ptrs.as_ptr(), 4, 0) },
        ZOU_OK
    );
    assert!(bufs[0].iter().all(|b| *b == 1));
    assert!(bufs[2].iter().all(|b| *b == 0));

    let snap = Snapshot::read(&stats).unwrap();
    let tier = |name: &str| snap.reads.iter().find(|t| t.tier == name).unwrap();
    assert_eq!(tier("cache").calls, 2);
    assert_eq!(tier("cache").pages, 4);
    assert_eq!(tier("store").calls, 2);
    assert_eq!(tier("store").pages, 3);
    assert_eq!(tier("local").calls, 0);
    assert!(tier("cache").p50_us > 0);
    assert!(tier("store").p50_us >= tier("cache").p50_us);
}
