//! Write-through local page cache for the live pg/ prefix.
//!
//! Every page the shim reads from or writes to the store also lands
//! here, and reads try here first, so a warm working set stops paying
//! store round trips at all. The cache is shared by every backend of
//! one postgres instance through plain files under ZOU_PAGE_CACHE.
//!
//! Coherence rests on two facts. Smgr reads only happen when a page is
//! not in shared_buffers, and any newer version of the page went
//! through this same write-through path before its buffer was reused,
//! so the cache can never answer with something older than the store.
//! Cross process safety rests on the bufmgr contract md already leans
//! on: two processes never do IO on the same block at the same time,
//! so an 8K pwrite is never read half done by anyone.
//!
//! Crash safety needs nothing from the cache because zou dev starts
//! every instance with an empty directory. Nothing here is fsynced,
//! lost cache writes are just misses, and the store keeps being the
//! durable truth.
//!
//! Layout: one sparse data file per fork holding pages at their block
//! offsets, plus a presence map of one byte per block, because a hole
//! of zeros is also a legitimate page image and absence must be
//! distinguishable. The presence byte flips to 1 only after the page
//! bytes are in place, so a reader who sees the byte sees the page.

use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use crate::ZOU_PAGE_SIZE;
use crate::pending::ForkId;

/// Positional reads and writes without touching the shared cursor,
/// pread and pwrite on unix, seek_read and seek_write on windows.
/// Sharing one File between threads stays safe either way because no
/// call here depends on the file position.
#[cfg(unix)]
fn read_exact_at(f: &File, buf: &mut [u8], off: u64) -> std::io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(f, buf, off)
}

#[cfg(unix)]
fn write_all_at(f: &File, buf: &[u8], off: u64) -> std::io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(f, buf, off)
}

#[cfg(windows)]
fn read_exact_at(f: &File, mut buf: &mut [u8], mut off: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match f.seek_read(buf, off) {
            Ok(0) => return Err(ErrorKind::UnexpectedEof.into()),
            Ok(n) => {
                buf = &mut buf[n..];
                off += n as u64;
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(windows)]
fn write_all_at(f: &File, mut buf: &[u8], mut off: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match f.seek_write(buf, off) {
            Ok(0) => return Err(ErrorKind::WriteZero.into()),
            Ok(n) => {
                buf = &buf[n..];
                off += n as u64;
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

pub struct PageCache {
    dir: PathBuf,
}

fn fork_stem(fork: ForkId) -> String {
    let (spc, db, rel, fk) = fork;
    format!("{spc}-{db}-{rel}-{fk}")
}

impl PageCache {
    /// A cache rooted at ZOU_PAGE_CACHE, `None` when the variable is
    /// unset or empty. The directory is created on first use.
    pub fn from_env() -> Option<PageCache> {
        let dir = std::env::var("ZOU_PAGE_CACHE").ok()?;
        if dir.is_empty() {
            return None;
        }
        Some(PageCache { dir: dir.into() })
    }

    /// A cache at a directory the caller names, for a process that is
    /// not the postgres the variable was set for: the warm up runs
    /// before the postmaster exists and fills the cache it will read.
    pub fn at(dir: &Path) -> PageCache {
        PageCache { dir: dir.into() }
    }

    fn data_path(&self, fork: ForkId) -> PathBuf {
        self.dir.join(format!("{}.pages", fork_stem(fork)))
    }

    fn map_path(&self, fork: ForkId) -> PathBuf {
        self.dir.join(format!("{}.map", fork_stem(fork)))
    }

    fn size_path(&self, fork: ForkId) -> PathBuf {
        self.dir.join(format!("{}.size", fork_stem(fork)))
    }

    fn open_rw(&self, path: &Path) -> Option<File> {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
        {
            Ok(f) => Some(f),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                std::fs::create_dir_all(&self.dir).ok()?;
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(path)
                    .ok()
            }
            Err(_) => None,
        }
    }

    /// The cached page, `None` on a miss. Every error is a miss, the
    /// cache is best effort and the store answers instead.
    pub fn load(&self, fork: ForkId, blk: u32) -> Option<Vec<u8>> {
        let map = File::open(self.map_path(fork)).ok()?;
        let mut flag = [0u8];
        read_exact_at(&map, &mut flag, blk as u64).ok()?;
        if flag[0] != 1 {
            return None;
        }
        let data = File::open(self.data_path(fork)).ok()?;
        let mut page = vec![0u8; ZOU_PAGE_SIZE];
        read_exact_at(&data, &mut page, blk as u64 * ZOU_PAGE_SIZE as u64).ok()?;
        Some(page)
    }

    /// Land a page in the cache. Failures are swallowed, a page that
    /// never makes it here just reads from the store next time.
    pub fn save(&self, fork: ForkId, blk: u32, page: &[u8]) {
        debug_assert_eq!(page.len(), ZOU_PAGE_SIZE);
        let Some(data) = self.open_rw(&self.data_path(fork)) else {
            return;
        };
        if write_all_at(&data, page, blk as u64 * ZOU_PAGE_SIZE as u64).is_err() {
            return;
        }
        if let Some(map) = self.open_rw(&self.map_path(fork)) {
            let _ = write_all_at(&map, &[1], blk as u64);
        }
    }

    /// The cached fork size, `None` on a miss. Sizes join the cache
    /// because the planner asks for nblocks on every planning cycle
    /// and a 4 byte SIZE round trip to a remote store per query is
    /// the whole latency budget. Coherence adds one fact to the page
    /// argument: Postgres serializes relation extension, so the fork
    /// has a single size writer at a time, and a reader racing an
    /// extend sees the old size exactly as a vanilla reader whose
    /// lseek lands before the extend completes.
    pub fn load_size(&self, fork: ForkId) -> Option<u32> {
        let f = File::open(self.size_path(fork)).ok()?;
        let mut n = [0u8; 4];
        read_exact_at(&f, &mut n, 0).ok()?;
        Some(u32::from_le_bytes(n))
    }

    /// Land a fork size, called only after the store accepted it, so
    /// the local answer is never newer than the store. Absence is
    /// never cached, an absent SIZE means the fork does not exist and
    /// that answer has to keep coming from the store.
    pub fn save_size(&self, fork: ForkId, nblocks: u32) {
        if let Some(f) = self.open_rw(&self.size_path(fork)) {
            let _ = write_all_at(&f, &nblocks.to_le_bytes(), 0);
        }
    }

    /// Drop every cached page at or past the new fork end. Shrinking
    /// the presence map is what forgets them, the data file follows so
    /// the space comes back.
    pub fn truncate(&self, fork: ForkId, nblocks: u32) {
        if let Ok(map) = OpenOptions::new().write(true).open(self.map_path(fork)) {
            let _ = map.set_len(nblocks as u64);
        }
        if let Ok(data) = OpenOptions::new().write(true).open(self.data_path(fork)) {
            let _ = data.set_len(nblocks as u64 * ZOU_PAGE_SIZE as u64);
        }
    }

    /// Remove a fork's cache files entirely.
    pub fn forget(&self, fork: ForkId) {
        let _ = std::fs::remove_file(self.map_path(fork));
        let _ = std::fs::remove_file(self.data_path(fork));
        let _ = std::fs::remove_file(self.size_path(fork));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FORK: ForkId = (1, 2, 3, 0);

    fn page(fill: u8) -> Vec<u8> {
        vec![fill; ZOU_PAGE_SIZE]
    }

    #[test]
    fn save_load_roundtrip_and_misses() {
        let dir = tempfile::tempdir().unwrap();
        let cache = PageCache::at(dir.path());
        assert!(cache.load(FORK, 0).is_none());
        cache.save(FORK, 5, &page(0xAB));
        assert_eq!(cache.load(FORK, 5).unwrap()[0], 0xAB);
        // Blocks below a saved one are holes in both files, absent.
        assert!(cache.load(FORK, 4).is_none());
        assert!(cache.load(FORK, 6).is_none());
        cache.save(FORK, 5, &page(0xCD));
        assert_eq!(cache.load(FORK, 5).unwrap()[0], 0xCD);
    }

    #[test]
    fn truncate_drops_the_tail_only() {
        let dir = tempfile::tempdir().unwrap();
        let cache = PageCache::at(dir.path());
        for blk in 0..4 {
            cache.save(FORK, blk, &page(blk as u8 + 1));
        }
        cache.truncate(FORK, 2);
        assert_eq!(cache.load(FORK, 1).unwrap()[0], 2);
        assert!(cache.load(FORK, 2).is_none());
        assert!(cache.load(FORK, 3).is_none());
        // A later save past the cut works again.
        cache.save(FORK, 3, &page(9));
        assert_eq!(cache.load(FORK, 3).unwrap()[0], 9);
        assert!(cache.load(FORK, 2).is_none());
    }

    #[test]
    fn forget_removes_the_fork_and_others_survive() {
        let dir = tempfile::tempdir().unwrap();
        let cache = PageCache::at(dir.path());
        let other: ForkId = (1, 2, 4, 0);
        cache.save(FORK, 0, &page(1));
        cache.save(other, 0, &page(2));
        cache.forget(FORK);
        assert!(cache.load(FORK, 0).is_none());
        assert_eq!(cache.load(other, 0).unwrap()[0], 2);
    }

    #[test]
    fn sizes_roundtrip_and_forget_drops_them() {
        let dir = tempfile::tempdir().unwrap();
        let cache = PageCache::at(dir.path());
        assert!(cache.load_size(FORK).is_none());
        cache.save_size(FORK, 6);
        assert_eq!(cache.load_size(FORK), Some(6));
        cache.save_size(FORK, 3);
        assert_eq!(cache.load_size(FORK), Some(3));
        // Zero is a real size, a created empty fork, not a miss.
        cache.save_size(FORK, 0);
        assert_eq!(cache.load_size(FORK), Some(0));
        let other: ForkId = (1, 2, 4, 0);
        cache.save_size(other, 9);
        cache.forget(FORK);
        assert!(cache.load_size(FORK).is_none());
        assert_eq!(cache.load_size(other), Some(9));
    }

    #[test]
    fn a_second_handle_on_the_same_dir_sees_the_pages() {
        let dir = tempfile::tempdir().unwrap();
        let one = PageCache::at(dir.path());
        one.save(FORK, 7, &page(0x77));
        let two = PageCache::at(dir.path());
        assert_eq!(two.load(FORK, 7).unwrap()[0], 0x77);
    }
}
