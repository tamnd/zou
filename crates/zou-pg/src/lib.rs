//! C ABI shim between Postgres and zou-store.
//!
//! The zou storage manager patch (patches/0001) routes relation page I/O
//! to these functions. Pages live as objects under the tenant's pg/
//! prefix: one object per block plus a SIZE marker per fork whose
//! presence means the fork exists and whose content is the block count.
//! A block object missing below the size reads as zeros, which mirrors
//! the file hole semantics of the stock md storage manager.
//!
//! This v0 page store is mutable derived data. The durable truth becomes
//! WAL plus checkpoints as the milestone progresses, at which point reads
//! route through checkpoint objects instead. The FFI surface stays.
//!
//! Every function returns 0 for success or a negative ZOU_ERR code, and
//! never unwinds into C. Postgres turns nonzero into ereport(ERROR).

use std::ffi::{CStr, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::OnceLock;

use zou_store::layout::TenantLayout;
use zou_store::{CasStore, LocalFsStore};

/// Postgres BLCKSZ. The patch checks this against its own BLCKSZ at init.
pub const ZOU_PAGE_SIZE: usize = 8192;

pub const ZOU_OK: i32 = 0;
pub const ZOU_ERR_STORE: i32 = -1;
pub const ZOU_ERR_NOT_INITIALIZED: i32 = -2;
pub const ZOU_ERR_PANIC: i32 = -3;
pub const ZOU_ERR_BAD_ARGUMENT: i32 = -4;

struct Shim {
    store: LocalFsStore,
    layout: TenantLayout,
}

static SHIM: OnceLock<Shim> = OnceLock::new();

fn wrap(f: impl FnOnce() -> i32) -> i32 {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or(ZOU_ERR_PANIC)
}

fn with_shim(f: impl FnOnce(&'static Shim) -> i32) -> i32 {
    wrap(|| match SHIM.get() {
        Some(shim) => f(shim),
        None => ZOU_ERR_NOT_INITIALIZED,
    })
}

fn read_size(shim: &Shim, spc: u32, db: u32, rel: u32, fork: u32) -> Result<Option<u32>, ()> {
    let key = shim.layout.pg_size(spc, db, rel, fork);
    match shim.store.get(&key) {
        Ok(Some((data, _))) => {
            let bytes: [u8; 4] = data.as_slice().try_into().map_err(|_| ())?;
            Ok(Some(u32::from_le_bytes(bytes)))
        }
        Ok(None) => Ok(None),
        Err(_) => Err(()),
    }
}

fn write_size(shim: &Shim, spc: u32, db: u32, rel: u32, fork: u32, nblocks: u32) -> Result<(), ()> {
    let key = shim.layout.pg_size(spc, db, rel, fork);
    shim.store
        .put(&key, &nblocks.to_le_bytes())
        .map(|_| ())
        .map_err(|_| ())
}

/// Block index of a pg/ object key, parsed from the 8 hex digit tail.
/// SIZE and anything unparseable return None.
fn block_index(key: &str) -> Option<u32> {
    let tail = key.rsplit('/').next()?;
    if tail.len() != 8 {
        return None;
    }
    u32::from_str_radix(tail, 16).ok()
}

/// Open the store for this process. Idempotent, every Postgres process
/// (postmaster, backends, checkpointer) calls it on startup. The target
/// is a local directory in v0, object store URLs arrive with the CLI.
///
/// # Safety
/// `target` must be a valid NUL terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_pg_init(target: *const c_char) -> i32 {
    wrap(|| {
        if target.is_null() {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let Ok(target) = unsafe { CStr::from_ptr(target) }.to_str() else {
            return ZOU_ERR_BAD_ARGUMENT;
        };
        if target.contains("://") {
            // Object store URLs need config (region, creds) that the CLI
            // will own. Refuse loudly instead of guessing.
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let _ = SHIM.set(Shim {
            store: LocalFsStore::new(target),
            layout: TenantLayout::new("local"),
        });
        ZOU_OK
    })
}

/// Create a fork: write SIZE=0 unless it already exists.
#[unsafe(no_mangle)]
pub extern "C" fn zou_smgr_create(spc: u32, db: u32, rel: u32, fork: u32) -> i32 {
    with_shim(|shim| match read_size(shim, spc, db, rel, fork) {
        Ok(Some(_)) => ZOU_OK,
        Ok(None) => match write_size(shim, spc, db, rel, fork, 0) {
            Ok(()) => ZOU_OK,
            Err(()) => ZOU_ERR_STORE,
        },
        Err(()) => ZOU_ERR_STORE,
    })
}

/// Does the fork exist? Writes 1 or 0 into `out`.
///
/// # Safety
/// `out` must be a valid pointer to an i32.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_smgr_exists(
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    out: *mut i32,
) -> i32 {
    with_shim(|shim| {
        if out.is_null() {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        match read_size(shim, spc, db, rel, fork) {
            Ok(size) => {
                unsafe { *out = size.is_some() as i32 };
                ZOU_OK
            }
            Err(()) => ZOU_ERR_STORE,
        }
    })
}

/// Block count of a fork, into `out`. Missing fork is an error, Postgres
/// only asks for forks it believes exist.
///
/// # Safety
/// `out` must be a valid pointer to a u32.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_smgr_nblocks(
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    out: *mut u32,
) -> i32 {
    with_shim(|shim| {
        if out.is_null() {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        match read_size(shim, spc, db, rel, fork) {
            Ok(Some(n)) => {
                unsafe { *out = n };
                ZOU_OK
            }
            Ok(None) => ZOU_ERR_STORE,
            Err(()) => ZOU_ERR_STORE,
        }
    })
}

/// Read one page into `buf`. An absent block object reads as zeros,
/// matching md's file hole semantics for blocks extended but not yet
/// written.
///
/// # Safety
/// `buf` must be a valid pointer to ZOU_PAGE_SIZE writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_smgr_read(
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    blk: u32,
    buf: *mut u8,
) -> i32 {
    with_shim(|shim| {
        if buf.is_null() {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let out = unsafe { std::slice::from_raw_parts_mut(buf, ZOU_PAGE_SIZE) };
        match shim
            .store
            .get(&shim.layout.pg_block(spc, db, rel, fork, blk))
        {
            Ok(Some((data, _))) if data.len() == ZOU_PAGE_SIZE => {
                out.copy_from_slice(&data);
                ZOU_OK
            }
            Ok(Some(_)) => ZOU_ERR_STORE,
            Ok(None) => {
                out.fill(0);
                ZOU_OK
            }
            Err(_) => ZOU_ERR_STORE,
        }
    })
}

/// Write one page. The block must already be within the fork size,
/// Postgres extends before it writes.
///
/// # Safety
/// `buf` must be a valid pointer to ZOU_PAGE_SIZE readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_smgr_write(
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    blk: u32,
    buf: *const u8,
) -> i32 {
    with_shim(|shim| {
        if buf.is_null() {
            return ZOU_ERR_BAD_ARGUMENT;
        }
        let data = unsafe { std::slice::from_raw_parts(buf, ZOU_PAGE_SIZE) };
        match shim
            .store
            .put(&shim.layout.pg_block(spc, db, rel, fork, blk), data)
        {
            Ok(_) => ZOU_OK,
            Err(_) => ZOU_ERR_STORE,
        }
    })
}

/// Write a page at `blk` and grow the fork size to cover it. Postgres
/// serializes extension per relation, so the read-modify-write on SIZE
/// has a single writer.
///
/// # Safety
/// `buf` must be a valid pointer to ZOU_PAGE_SIZE readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_smgr_extend(
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    blk: u32,
    buf: *const u8,
) -> i32 {
    let rc = unsafe { zou_smgr_write(spc, db, rel, fork, blk, buf) };
    if rc != ZOU_OK {
        return rc;
    }
    with_shim(|shim| match read_size(shim, spc, db, rel, fork) {
        Ok(size) => {
            let current = size.unwrap_or(0);
            if blk + 1 > current && write_size(shim, spc, db, rel, fork, blk + 1).is_err() {
                return ZOU_ERR_STORE;
            }
            ZOU_OK
        }
        Err(()) => ZOU_ERR_STORE,
    })
}

/// Grow the fork by `count` zero pages starting at `blk`. Zero pages are
/// represented as absent block objects, reads fill zeros.
#[unsafe(no_mangle)]
pub extern "C" fn zou_smgr_zeroextend(
    spc: u32,
    db: u32,
    rel: u32,
    fork: u32,
    blk: u32,
    count: u32,
) -> i32 {
    with_shim(|shim| {
        let Some(new_size) = blk.checked_add(count) else {
            return ZOU_ERR_BAD_ARGUMENT;
        };
        match write_size(shim, spc, db, rel, fork, new_size) {
            Ok(()) => ZOU_OK,
            Err(()) => ZOU_ERR_STORE,
        }
    })
}

/// Shrink the fork to `nblocks`, deleting block objects past the new end
/// so a later re-extend cannot resurrect stale pages.
#[unsafe(no_mangle)]
pub extern "C" fn zou_smgr_truncate(spc: u32, db: u32, rel: u32, fork: u32, nblocks: u32) -> i32 {
    with_shim(|shim| {
        if write_size(shim, spc, db, rel, fork, nblocks).is_err() {
            return ZOU_ERR_STORE;
        }
        let prefix = format!("{}/", shim.layout.pg_fork_prefix(spc, db, rel, fork));
        let Ok(keys) = shim.store.list(&prefix) else {
            return ZOU_ERR_STORE;
        };
        for key in keys {
            if let Some(idx) = block_index(&key)
                && idx >= nblocks
                && shim.store.delete(&key).is_err()
            {
                return ZOU_ERR_STORE;
            }
        }
        ZOU_OK
    })
}

/// Remove a fork entirely: SIZE marker and every block object.
#[unsafe(no_mangle)]
pub extern "C" fn zou_smgr_unlink(spc: u32, db: u32, rel: u32, fork: u32) -> i32 {
    with_shim(|shim| {
        // SIZE goes first so the fork stops existing even if a block
        // delete fails midway, leaving only unreachable garbage.
        if shim
            .store
            .delete(&shim.layout.pg_size(spc, db, rel, fork))
            .is_err()
        {
            return ZOU_ERR_STORE;
        }
        let prefix = format!("{}/", shim.layout.pg_fork_prefix(spc, db, rel, fork));
        let Ok(keys) = shim.store.list(&prefix) else {
            return ZOU_ERR_STORE;
        };
        for key in keys {
            if shim.store.delete(&key).is_err() {
                return ZOU_ERR_STORE;
            }
        }
        ZOU_OK
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    /// One test drives the whole C ABI lifecycle, because the shim is a
    /// process global and tests share the process.
    #[test]
    fn the_c_abi_lifecycle_works_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let target = CString::new(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(unsafe { zou_pg_init(target.as_ptr()) }, ZOU_OK);
        // Idempotent: a second init from the same process is fine.
        assert_eq!(unsafe { zou_pg_init(target.as_ptr()) }, ZOU_OK);

        let (spc, db, rel, fork) = (1663, 5, 16384, 0);
        let mut flag = -1i32;
        assert_eq!(
            unsafe { zou_smgr_exists(spc, db, rel, fork, &mut flag) },
            ZOU_OK
        );
        assert_eq!(flag, 0);

        assert_eq!(zou_smgr_create(spc, db, rel, fork), ZOU_OK);
        assert_eq!(
            unsafe { zou_smgr_exists(spc, db, rel, fork, &mut flag) },
            ZOU_OK
        );
        assert_eq!(flag, 1);

        // Extend three pages with distinct contents.
        for blk in 0u32..3 {
            let page = [blk as u8 + 1; ZOU_PAGE_SIZE];
            assert_eq!(
                unsafe { zou_smgr_extend(spc, db, rel, fork, blk, page.as_ptr()) },
                ZOU_OK
            );
        }
        let mut n = 0u32;
        assert_eq!(
            unsafe { zou_smgr_nblocks(spc, db, rel, fork, &mut n) },
            ZOU_OK
        );
        assert_eq!(n, 3);

        // Overwrite block 1 through the write path and read everything back.
        let page = [0xAB; ZOU_PAGE_SIZE];
        assert_eq!(
            unsafe { zou_smgr_write(spc, db, rel, fork, 1, page.as_ptr()) },
            ZOU_OK
        );
        let mut buf = [0u8; ZOU_PAGE_SIZE];
        for (blk, expect) in [(0u32, 1u8), (1, 0xAB), (2, 3)] {
            assert_eq!(
                unsafe { zou_smgr_read(spc, db, rel, fork, blk, buf.as_mut_ptr()) },
                ZOU_OK
            );
            assert!(buf.iter().all(|b| *b == expect), "block {blk}");
        }

        // Zero extension: size grows, the new blocks read as zeros.
        assert_eq!(zou_smgr_zeroextend(spc, db, rel, fork, 3, 2), ZOU_OK);
        assert_eq!(
            unsafe { zou_smgr_nblocks(spc, db, rel, fork, &mut n) },
            ZOU_OK
        );
        assert_eq!(n, 5);
        assert_eq!(
            unsafe { zou_smgr_read(spc, db, rel, fork, 4, buf.as_mut_ptr()) },
            ZOU_OK
        );
        assert!(buf.iter().all(|b| *b == 0));

        // Truncate drops the tail blocks for real.
        assert_eq!(zou_smgr_truncate(spc, db, rel, fork, 1), ZOU_OK);
        assert_eq!(
            unsafe { zou_smgr_nblocks(spc, db, rel, fork, &mut n) },
            ZOU_OK
        );
        assert_eq!(n, 1);
        assert_eq!(
            unsafe { zou_smgr_read(spc, db, rel, fork, 0, buf.as_mut_ptr()) },
            ZOU_OK
        );
        assert!(buf.iter().all(|b| *b == 1));

        // Unlink removes the fork and its objects.
        assert_eq!(zou_smgr_unlink(spc, db, rel, fork), ZOU_OK);
        assert_eq!(
            unsafe { zou_smgr_exists(spc, db, rel, fork, &mut flag) },
            ZOU_OK
        );
        assert_eq!(flag, 0);
    }
}
