//! Single file `.zou` backend, spec 06-single-file-store.md.
//!
//! One file is the whole store: a 16 byte header followed by append
//! only frames, each frame a columnar block of keys, fixed width entry
//! metadata, and payloads. The open scan rebuilds the index from the
//! keys and meta sections and seeks past every data section, so open
//! cost follows metadata volume, not payload volume. A torn tail fails
//! its crc or its bounds check and gets truncated away, which only
//! ever discards writes that were never acked, every acked put ends
//! with one fdatasync.
//!
//! Versions are entry sequence numbers, monotonic and never reused.
//! Entries carry their own sequence because compaction folds all live
//! entries into one frame and their versions must survive unchanged.
//! One process owns the file at a time via an exclusive lock on a
//! sidecar `.lock` file, a sidecar because compaction renames the data
//! file and Windows will not rename a locked file.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use crate::cas::{CasError, CasStore, Version};
use crate::stats::{Packed, note_packed};

const FILE_MAGIC: &[u8; 4] = b"ZOUF";
const FRAME_MAGIC: &[u8; 4] = b"FRAM";
const FORMAT: u32 = 1;
const HEADER_LEN: u64 = 16;
const FRAME_HEAD_LEN: usize = 40;
const META_RECORD_LEN: usize = 32;

const OP_PUT: u8 = 0;
const OP_TOMBSTONE: u8 = 1;
const ENC_RAW: u8 = 0;
const ENC_LZ4: u8 = 1;

/// Values below this many bytes are never worth a compression attempt.
const COMPRESS_MIN: usize = 512;
/// Compaction at open kicks in past this much dead payload, so tiny
/// stores never pay a rewrite.
const AUTO_COMPACT_MIN_DEAD: u64 = 4 << 20;

#[derive(Clone)]
struct Entry {
    seq: u64,
    /// Absolute file offset of the stored payload.
    offset: u64,
    stored_len: u32,
    raw_len: u32,
    crc: u32,
    lz4: bool,
}

struct Inner {
    file: File,
    index: BTreeMap<String, Entry>,
    /// Highest sequence number ever issued, the next write gets seq + 1.
    seq: u64,
    /// Append position, which is also the file length after recovery.
    end: u64,
    /// Payload bytes still referenced by the index.
    live: u64,
    /// Payload bytes shadowed by overwrites and tombstones.
    dead: u64,
}

pub struct ZouFileStore {
    path: PathBuf,
    inner: RwLock<Inner>,
    /// Exclusive while the store lives, see the module doc.
    _lock: File,
}

fn io_err(key: &str, e: std::io::Error) -> CasError {
    CasError::Io {
        key: key.to_string(),
        source: e,
    }
}

fn corrupt(path: &Path, why: &str) -> String {
    format!("{}: {why}", path.display())
}

fn read_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0;
        while done < buf.len() {
            let n = file.seek_read(&mut buf[done..], offset + done as u64)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "short read",
                ));
            }
            done += n;
        }
        Ok(())
    }
}

/// One pending entry while a frame is being encoded.
struct FrameEntry<'a> {
    key: &'a str,
    op: u8,
    enc: u8,
    stored: Vec<u8>,
    raw_len: u32,
    seq: u64,
}

/// Compress when it saves at least an eighth, otherwise store raw.
fn encode_value(data: &[u8]) -> (u8, Vec<u8>) {
    if data.len() >= COMPRESS_MIN {
        let packed = lz4_flex::compress(data);
        if packed.len() <= data.len() - data.len() / 8 {
            note_packed(Packed::File, data.len(), packed.len());
            return (ENC_LZ4, packed);
        }
    }
    note_packed(Packed::File, data.len(), data.len());
    (ENC_RAW, data.to_vec())
}

/// Serialize one frame and report each entry's absolute payload offset,
/// in entry order, given the frame will land at `frame_start`.
fn encode_frame(
    frame_seq: u64,
    entries: &[FrameEntry<'_>],
    frame_start: u64,
) -> (Vec<u8>, Vec<u64>) {
    let mut keys_raw = Vec::new();
    for e in entries {
        keys_raw.extend_from_slice(&(e.key.len() as u16).to_le_bytes());
        keys_raw.extend_from_slice(e.key.as_bytes());
    }
    let keys_comp = lz4_flex::compress(&keys_raw);

    let mut meta = Vec::with_capacity(entries.len() * META_RECORD_LEN);
    let mut data = Vec::new();
    let mut offsets = Vec::with_capacity(entries.len());
    for e in entries {
        let data_off = data.len() as u64;
        meta.push(e.op);
        meta.push(e.enc);
        meta.extend_from_slice(&[0u8; 2]);
        meta.extend_from_slice(&(e.stored.len() as u32).to_le_bytes());
        meta.extend_from_slice(&e.raw_len.to_le_bytes());
        meta.extend_from_slice(&crc32c::crc32c(&e.stored).to_le_bytes());
        meta.extend_from_slice(&e.seq.to_le_bytes());
        meta.extend_from_slice(&data_off.to_le_bytes());
        data.extend_from_slice(&e.stored);
        offsets.push(data_off);
    }

    let mut head = Vec::with_capacity(FRAME_HEAD_LEN);
    head.extend_from_slice(FRAME_MAGIC);
    head.extend_from_slice(&frame_seq.to_le_bytes());
    head.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    head.extend_from_slice(&(keys_comp.len() as u32).to_le_bytes());
    head.extend_from_slice(&(keys_raw.len() as u32).to_le_bytes());
    head.extend_from_slice(&(meta.len() as u32).to_le_bytes());
    head.extend_from_slice(&(data.len() as u64).to_le_bytes());
    let mut crc = crc32c::crc32c(&head[4..]);
    crc = crc32c::crc32c_append(crc, &keys_comp);
    crc = crc32c::crc32c_append(crc, &meta);
    head.extend_from_slice(&crc.to_le_bytes());

    let data_base = frame_start + (FRAME_HEAD_LEN + keys_comp.len() + meta.len()) as u64;
    let abs: Vec<u64> = offsets.iter().map(|o| data_base + o).collect();

    let mut frame = head;
    frame.extend_from_slice(&keys_comp);
    frame.extend_from_slice(&meta);
    frame.extend_from_slice(&data);
    (frame, abs)
}

fn u32_at(buf: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(buf[at..at + 4].try_into().unwrap())
}

fn u64_at(buf: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(buf[at..at + 8].try_into().unwrap())
}

impl ZouFileStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|e| corrupt(&path, &format!("create dir: {e}")))?;
        }

        let mut lock_name = path.as_os_str().to_os_string();
        lock_name.push(".lock");
        let lock = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_name)
            .map_err(|e| corrupt(&path, &format!("lock file: {e}")))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(corrupt(&path, "another process has this store open"));
            }
            Err(std::fs::TryLockError::Error(e)) => {
                return Err(corrupt(&path, &format!("lock: {e}")));
            }
        }

        // A crash between removing the old file and renaming the
        // compacted one leaves only the .new, adopt it.
        let mut new_name = path.as_os_str().to_os_string();
        new_name.push(".new");
        let new_path = PathBuf::from(&new_name);
        if !path.exists() && new_path.exists() {
            fs::rename(&new_path, &path).map_err(|e| corrupt(&path, &format!("adopt: {e}")))?;
        } else if new_path.exists() {
            // Compaction never finished, the original is authoritative.
            let _ = fs::remove_file(&new_path);
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| corrupt(&path, &format!("open: {e}")))?;
        let len = file
            .metadata()
            .map_err(|e| corrupt(&path, &format!("stat: {e}")))?
            .len();
        if len == 0 {
            let mut header = Vec::with_capacity(HEADER_LEN as usize);
            header.extend_from_slice(FILE_MAGIC);
            header.extend_from_slice(&FORMAT.to_le_bytes());
            header.extend_from_slice(&[0u8; 8]);
            file.write_all(&header)
                .and_then(|_| file.sync_data())
                .map_err(|e| corrupt(&path, &format!("init: {e}")))?;
        } else {
            let mut header = [0u8; HEADER_LEN as usize];
            read_at(&file, &mut header, 0).map_err(|e| corrupt(&path, &format!("header: {e}")))?;
            if &header[..4] != FILE_MAGIC {
                return Err(corrupt(&path, "not a .zou store, bad magic"));
            }
            // Which side of this build the file is on decides what the
            // person holding it should do, so the two are not one
            // message. Newer means the binary is behind and upgrading
            // it opens the file. Older means the file predates a change
            // this build cannot read, and no upgrade of this binary
            // will help: the file has to be exported by the build that
            // wrote it. Saying "newer" for both would send half of them
            // the wrong way.
            let format = u32_at(&header, 4);
            if format > FORMAT {
                return Err(corrupt(
                    &path,
                    &format!(
                        "format {format} is newer than this binary supports ({FORMAT}), upgrade zou"
                    ),
                ));
            }
            if format < FORMAT {
                return Err(corrupt(
                    &path,
                    &format!(
                        "format {format} is older than this binary reads ({FORMAT}), export it with the zou that wrote it"
                    ),
                ));
            }
        }

        let inner = Self::scan(&mut file, &path)?;
        let store = Self {
            path,
            inner: RwLock::new(inner),
            _lock: lock,
        };
        {
            let needs = {
                let inner = store.inner.read().unwrap();
                inner.dead > inner.live && inner.dead > AUTO_COMPACT_MIN_DEAD
            };
            if needs {
                store.compact().map_err(|e| format!("auto compact: {e}"))?;
            }
        }
        Ok(store)
    }

    /// Rebuild the index by walking frames, truncating at the first
    /// torn or corrupt one. Payload bytes are never read here.
    fn scan(file: &mut File, path: &Path) -> Result<Inner, String> {
        let file_len = file
            .metadata()
            .map_err(|e| corrupt(path, &format!("stat: {e}")))?
            .len();
        let mut index: BTreeMap<String, Entry> = BTreeMap::new();
        let mut seq = 0u64;
        let mut live = 0u64;
        let mut dead = 0u64;
        let mut pos = HEADER_LEN.min(file_len);
        loop {
            if pos + FRAME_HEAD_LEN as u64 > file_len {
                break;
            }
            let mut head = [0u8; FRAME_HEAD_LEN];
            read_at(file, &mut head, pos).map_err(|e| corrupt(path, &format!("scan: {e}")))?;
            if &head[..4] != FRAME_MAGIC {
                break;
            }
            let frame_seq = u64_at(&head, 4);
            let count = u32_at(&head, 12) as usize;
            let keys_comp_len = u32_at(&head, 16) as usize;
            let keys_raw_len = u32_at(&head, 20) as usize;
            let meta_len = u32_at(&head, 24) as usize;
            let data_len = u64_at(&head, 28);
            let want_crc = u32_at(&head, 36);
            if meta_len != count * META_RECORD_LEN {
                break;
            }
            let sections = (keys_comp_len + meta_len) as u64;
            if pos + FRAME_HEAD_LEN as u64 + sections + data_len > file_len {
                break;
            }
            let mut keys_meta = vec![0u8; keys_comp_len + meta_len];
            read_at(file, &mut keys_meta, pos + FRAME_HEAD_LEN as u64)
                .map_err(|e| corrupt(path, &format!("scan: {e}")))?;
            let mut crc = crc32c::crc32c(&head[4..36]);
            crc = crc32c::crc32c_append(crc, &keys_meta);
            if crc != want_crc {
                break;
            }
            let keys_raw = match lz4_flex::decompress(&keys_meta[..keys_comp_len], keys_raw_len) {
                Ok(k) => k,
                Err(_) => break,
            };
            let meta = &keys_meta[keys_comp_len..];
            let data_base = pos + FRAME_HEAD_LEN as u64 + sections;

            let mut keys = Vec::with_capacity(count);
            let mut at = 0usize;
            let mut ok = true;
            for _ in 0..count {
                if at + 2 > keys_raw.len() {
                    ok = false;
                    break;
                }
                let klen = u16::from_le_bytes(keys_raw[at..at + 2].try_into().unwrap()) as usize;
                at += 2;
                if at + klen > keys_raw.len() {
                    ok = false;
                    break;
                }
                match std::str::from_utf8(&keys_raw[at..at + klen]) {
                    Ok(k) => keys.push(k.to_string()),
                    Err(_) => {
                        ok = false;
                        break;
                    }
                }
                at += klen;
            }
            if !ok || at != keys_raw.len() {
                break;
            }

            for (i, key) in keys.into_iter().enumerate() {
                let rec = &meta[i * META_RECORD_LEN..(i + 1) * META_RECORD_LEN];
                let op = rec[0];
                let enc = rec[1];
                let stored_len = u32_at(rec, 4);
                let raw_len = u32_at(rec, 8);
                let crc = u32_at(rec, 12);
                let entry_seq = u64_at(rec, 16);
                let data_off = u64_at(rec, 24);
                if let Some(old) = index.remove(&key) {
                    live -= old.stored_len as u64;
                    dead += old.stored_len as u64;
                }
                if op == OP_PUT {
                    live += stored_len as u64;
                    index.insert(
                        key,
                        Entry {
                            seq: entry_seq,
                            offset: data_base + data_off,
                            stored_len,
                            raw_len,
                            crc,
                            lz4: enc == ENC_LZ4,
                        },
                    );
                }
            }
            seq = seq.max(frame_seq);
            pos += FRAME_HEAD_LEN as u64 + sections + data_len;
        }
        if pos < file_len {
            file.set_len(pos)
                .and_then(|_| file.sync_data())
                .map_err(|e| corrupt(path, &format!("truncate torn tail: {e}")))?;
        }
        Ok(Inner {
            file: file
                .try_clone()
                .map_err(|e| corrupt(path, &format!("clone fd: {e}")))?,
            index,
            seq,
            end: pos,
            live,
            dead,
        })
    }

    /// Append one single entry frame and index it. Callers hold the
    /// write lock and have already decided the operation is legal.
    fn append(
        &self,
        inner: &mut Inner,
        key: &str,
        op: u8,
        data: Option<&[u8]>,
    ) -> Result<u64, CasError> {
        let seq = inner.seq + 1;
        let (enc, stored, raw_len) = match data {
            Some(d) => {
                let (enc, stored) = encode_value(d);
                (enc, stored, d.len() as u32)
            }
            None => (ENC_RAW, Vec::new(), 0),
        };
        let stored_len = stored.len() as u32;
        let crc = crc32c::crc32c(&stored);
        let lz4 = enc == ENC_LZ4;
        let entries = [FrameEntry {
            key,
            op,
            enc,
            stored,
            raw_len,
            seq,
        }];
        let (frame, offsets) = encode_frame(seq, &entries, inner.end);
        inner
            .file
            .seek(SeekFrom::Start(inner.end))
            .and_then(|_| inner.file.write_all(&frame))
            .and_then(|_| inner.file.sync_data())
            .map_err(|e| io_err(key, e))?;
        inner.end += frame.len() as u64;
        inner.seq = seq;
        if let Some(old) = inner.index.remove(key) {
            inner.live -= old.stored_len as u64;
            inner.dead += old.stored_len as u64;
        }
        if op == OP_PUT {
            inner.live += stored_len as u64;
            inner.index.insert(
                key.to_string(),
                Entry {
                    seq,
                    offset: offsets[0],
                    stored_len,
                    raw_len,
                    crc,
                    lz4,
                },
            );
        }
        Ok(seq)
    }

    fn read_entry(&self, inner: &Inner, key: &str, e: &Entry) -> Result<Vec<u8>, CasError> {
        let mut stored = vec![0u8; e.stored_len as usize];
        read_at(&inner.file, &mut stored, e.offset).map_err(|e| io_err(key, e))?;
        if crc32c::crc32c(&stored) != e.crc {
            return Err(io_err(
                key,
                std::io::Error::other("payload crc mismatch, store is damaged"),
            ));
        }
        if e.lz4 {
            lz4_flex::decompress(&stored, e.raw_len as usize)
                .map_err(|err| io_err(key, std::io::Error::other(format!("lz4: {err}"))))
        } else {
            Ok(stored)
        }
    }

    /// Rewrite live entries into a fresh file and swap it in. The write
    /// lock is held throughout, readers never see the swap.
    pub fn compact(&self) -> Result<(), CasError> {
        let path_str = self.path.display().to_string();
        let mut inner = self.inner.write().unwrap();
        let mut new_name = self.path.as_os_str().to_os_string();
        new_name.push(".new");
        let new_path = PathBuf::from(&new_name);

        let mut entries = Vec::with_capacity(inner.index.len());
        let mut payloads = Vec::with_capacity(inner.index.len());
        for (key, e) in &inner.index {
            let mut stored = vec![0u8; e.stored_len as usize];
            read_at(&inner.file, &mut stored, e.offset).map_err(|err| io_err(&path_str, err))?;
            payloads.push((key.clone(), e.clone(), stored));
        }
        for (key, e, stored) in &payloads {
            entries.push(FrameEntry {
                key,
                op: OP_PUT,
                enc: if e.lz4 { ENC_LZ4 } else { ENC_RAW },
                stored: stored.clone(),
                raw_len: e.raw_len,
                seq: e.seq,
            });
        }

        let mut out = Vec::with_capacity(HEADER_LEN as usize);
        out.extend_from_slice(FILE_MAGIC);
        out.extend_from_slice(&FORMAT.to_le_bytes());
        out.extend_from_slice(&[0u8; 8]);
        let (frame, offsets) = if entries.is_empty() {
            (Vec::new(), Vec::new())
        } else {
            encode_frame(inner.seq, &entries, HEADER_LEN)
        };
        out.extend_from_slice(&frame);

        let mut new_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&new_path)
            .map_err(|e| io_err(&path_str, e))?;
        new_file
            .write_all(&out)
            .and_then(|_| new_file.sync_data())
            .map_err(|e| io_err(&path_str, e))?;

        // Unix renames over the target atomically. Windows refuses, so
        // remove first, and open() adopts a lone .new after a crash in
        // the gap.
        #[cfg(not(unix))]
        fs::remove_file(&self.path).map_err(|e| io_err(&path_str, e))?;
        fs::rename(&new_path, &self.path).map_err(|e| io_err(&path_str, e))?;

        let live = inner.index.values().map(|e| e.stored_len as u64).sum();
        for (i, (key, _, _)) in payloads.iter().enumerate() {
            if let Some(entry) = inner.index.get_mut(key) {
                entry.offset = offsets[i];
            }
        }
        inner.end = out.len() as u64;
        inner.live = live;
        inner.dead = 0;
        inner.file = new_file;
        Ok(())
    }
}

impl CasStore for ZouFileStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        let inner = self.inner.read().unwrap();
        match inner.index.get(key) {
            Some(e) => {
                let data = self.read_entry(&inner, key, e)?;
                Ok(Some((data, Version::from_backend(e.seq.to_string()))))
            }
            None => Ok(None),
        }
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        let mut inner = self.inner.write().unwrap();
        let current = inner.index.get(key).map(|e| e.seq.to_string());
        let matches = match (expected, &current) {
            (None, None) => true,
            (Some(v), Some(cur)) => v.as_str() == cur,
            _ => false,
        };
        if !matches {
            return Err(CasError::Conflict {
                key: key.to_string(),
            });
        }
        let seq = self.append(&mut inner, key, OP_PUT, Some(data))?;
        Ok(Version::from_backend(seq.to_string()))
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        let mut inner = self.inner.write().unwrap();
        let seq = self.append(&mut inner, key, OP_PUT, Some(data))?;
        Ok(Version::from_backend(seq.to_string()))
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        let mut inner = self.inner.write().unwrap();
        if inner.index.contains_key(key) {
            self.append(&mut inner, key, OP_TOMBSTONE, None)?;
        }
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        let inner = self.inner.read().unwrap();
        Ok(inner
            .index
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store(name: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        (dir, path)
    }

    #[test]
    fn data_and_versions_survive_reopen() {
        let (_dir, path) = temp_store("t.zou");
        let v1;
        {
            let store = ZouFileStore::open(&path).unwrap();
            v1 = store.put_if_match("a/k", b"hello", None).unwrap();
            store.put("a/mut", b"first").unwrap();
            store.put("a/mut", b"second").unwrap();
            store.put("gone", b"bye").unwrap();
            store.delete("gone").unwrap();
        }
        let store = ZouFileStore::open(&path).unwrap();
        let (data, version) = store.get("a/k").unwrap().unwrap();
        assert_eq!(data, b"hello");
        assert_eq!(version, v1);
        assert_eq!(store.get("a/mut").unwrap().unwrap().0, b"second");
        assert!(store.get("gone").unwrap().is_none());
        assert_eq!(store.list("a/").unwrap(), vec!["a/k", "a/mut"]);
    }

    #[test]
    fn a_torn_tail_is_truncated_and_acked_writes_survive() {
        let (_dir, path) = temp_store("t.zou");
        {
            let store = ZouFileStore::open(&path).unwrap();
            store.put("keep", b"acked").unwrap();
        }
        // A crash mid append leaves a partial frame behind.
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(FRAME_MAGIC).unwrap();
        f.write_all(&[7u8; 21]).unwrap();
        drop(f);
        let store = ZouFileStore::open(&path).unwrap();
        assert_eq!(store.get("keep").unwrap().unwrap().0, b"acked");
        // And the store keeps working after the truncate.
        store.put("more", b"after recovery").unwrap();
        assert_eq!(store.get("more").unwrap().unwrap().0, b"after recovery");
    }

    #[test]
    fn compaction_drops_dead_bytes_and_keeps_versions() {
        let (_dir, path) = temp_store("t.zou");
        let store = ZouFileStore::open(&path).unwrap();
        let big = vec![42u8; 100_000];
        for _ in 0..10 {
            store.put("churn", &big).unwrap();
        }
        let (_, version) = store.get("churn").unwrap().unwrap();
        store.put("small", b"stays").unwrap();
        let before = fs::metadata(&path).unwrap().len();
        store.compact().unwrap();
        let after = fs::metadata(&path).unwrap().len();
        assert!(after < before, "{after} should shrink from {before}");
        let (data, v2) = store.get("churn").unwrap().unwrap();
        assert_eq!(data, big);
        assert_eq!(v2, version, "compaction must not change versions");
        assert_eq!(store.get("small").unwrap().unwrap().0, b"stays");
        // CAS against the preserved version still works.
        store.put_if_match("churn", b"swapped", Some(&v2)).unwrap();
        drop(store);
        let store = ZouFileStore::open(&path).unwrap();
        assert_eq!(store.get("churn").unwrap().unwrap().0, b"swapped");
    }

    #[test]
    fn large_compressible_values_shrink_on_disk() {
        let (_dir, path) = temp_store("t.zou");
        let store = ZouFileStore::open(&path).unwrap();
        let value = b"zou ".repeat(64 * 1024);
        store.put("big", &value).unwrap();
        let on_disk = fs::metadata(&path).unwrap().len();
        assert!(on_disk < value.len() as u64 / 2);
        assert_eq!(store.get("big").unwrap().unwrap().0, value);
        assert_eq!(
            store.get_range("big", 4, 4).unwrap().unwrap(),
            b"zou ".to_vec()
        );
    }

    #[test]
    fn a_second_opener_is_refused() {
        let (_dir, path) = temp_store("t.zou");
        let _store = ZouFileStore::open(&path).unwrap();
        let err = ZouFileStore::open(&path).map(|_| ()).unwrap_err();
        assert!(err.contains("another process"), "{err}");
    }

    #[test]
    fn a_foreign_file_is_refused() {
        let (_dir, path) = temp_store("t.zou");
        fs::write(&path, b"PK\x03\x04 definitely not a zou store").unwrap();
        let err = ZouFileStore::open(&path).map(|_| ()).unwrap_err();
        assert!(err.contains("bad magic"), "{err}");
    }

    /// A file is the one thing here that outlives the binary that
    /// wrote it and travels: it gets copied to a laptop, attached to a
    /// bug report, opened by whatever zou is on the machine. So both
    /// directions of a format mismatch are things a person will meet,
    /// and each has a different answer.
    #[test]
    fn a_file_from_another_format_says_which_way_it_is_wrong() {
        let (_dir, path) = temp_store("t.zou");
        let stamp = |format: u32| {
            drop(ZouFileStore::open(&path));
            let mut header = fs::read(&path).unwrap();
            header[4..8].copy_from_slice(&format.to_le_bytes());
            fs::write(&path, &header).unwrap();
            ZouFileStore::open(&path).map(|_| ()).unwrap_err()
        };

        let ahead = stamp(FORMAT + 1);
        assert!(
            ahead.contains("newer than") && ahead.contains("upgrade zou"),
            "{ahead}"
        );

        let behind = stamp(FORMAT - 1);
        assert!(
            behind.contains("older than") && behind.contains("export"),
            "{behind}"
        );
        assert!(
            !behind.contains("newer"),
            "an upgrade will not open it, so do not ask for one: {behind}"
        );
    }
}
