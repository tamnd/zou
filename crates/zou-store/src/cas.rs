//! Compare-and-swap object store abstraction.
//!
//! This is the only coordination primitive zou needs. S3 gives it to us as
//! conditional PUT with If-Match, GCS as generation preconditions, and the
//! local filesystem backend emulates it with a per-key lock plus atomic
//! rename. Versions are opaque: the local backend uses content hashes,
//! remote backends use whatever token their API returns, and callers may
//! only ever compare them for equality.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

/// Opaque object version, the ETag equivalent. Compare with `==` only.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Version(String);

impl Version {
    fn of(data: &[u8]) -> Self {
        let mut h = Sha256::new();
        h.update(data);
        let digest = h.finalize();
        let mut hex = String::with_capacity(digest.len() * 2);
        for b in digest {
            hex.push_str(&format!("{b:02x}"));
        }
        Version(hex)
    }

    /// Wrap a backend supplied version token, an ETag or a generation
    /// number, verbatim.
    #[cfg_attr(not(feature = "s3"), allow(dead_code))]
    pub(crate) fn from_backend(token: impl Into<String>) -> Self {
        Version(token.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CasError {
    /// The precondition failed: someone else swapped the object first.
    /// The caller should re-read and decide again, never blind-retry.
    #[error("version conflict on {key}")]
    Conflict { key: String },
    #[error("object {key} already exists and is immutable")]
    AlreadyExists { key: String },
    /// The guard refused a versioned overwrite of a write-once key. Unlike
    /// Conflict this is not retryable: no version makes it legal.
    #[error("refusing to overwrite immutable object {key}")]
    ImmutableOverwrite { key: String },
    #[error("io error on {key}: {source}")]
    Io {
        key: String,
        #[source]
        source: std::io::Error,
    },
}

pub trait CasStore: Send + Sync {
    /// Read an object and its current version. `None` if it does not exist.
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError>;

    /// Swap an object, conditional on its current version.
    ///
    /// `expected: None` means "create, fail if it already exists". This is
    /// the primitive under the manifest swap and the writer lease.
    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError>;

    /// Write an immutable object, failing if the key exists. This is the
    /// fencing primitive of the v2 design: exactly one writer wins the
    /// creation race for a landing segment or a chain link, everyone else
    /// gets [`CasError::AlreadyExists`] and reads the winner's bytes. On
    /// S3 dialects it maps to a PUT with If-None-Match, everywhere else
    /// to the create arm of [`CasStore::put_if_match`].
    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        match self.put_if_match(key, data, None) {
            Err(CasError::Conflict { key }) => Err(CasError::AlreadyExists { key }),
            other => other,
        }
    }

    /// Overwrite an object unconditionally. This is for mutable derived
    /// data like relation page objects, never for the manifest or anything
    /// under wal/ or chk/, which the guard refuses. The default emulates
    /// it with a CAS retry loop, backends with a native unconditional
    /// write override it.
    fn put(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        loop {
            let current = self.get(key)?.map(|(_, v)| v);
            match self.put_if_match(key, data, current.as_ref()) {
                Err(CasError::Conflict { .. }) => continue,
                other => return other,
            }
        }
    }

    /// Read `len` bytes of an object starting at `offset`, clamped to
    /// the object's end, so a range past it comes back short or empty.
    /// `None` if the object does not exist. Backends with native range
    /// requests override this, the default fetches the whole object.
    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
        match self.get(key)? {
            Some((data, _)) => {
                let start = (offset as usize).min(data.len());
                let end = (offset.saturating_add(len) as usize).min(data.len());
                Ok(Some(data[start..end].to_vec()))
            }
            None => Ok(None),
        }
    }

    /// Delete an object. Deleting a missing key succeeds, so retries and
    /// concurrent deleters are harmless. When history must be protected
    /// that is the GC safety window's job, not the store's.
    fn delete(&self, key: &str) -> Result<(), CasError>;

    /// List keys under a prefix, sorted.
    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError>;
}

/// Local filesystem backend.
///
/// This is a real backend, not a test double: embedded mode runs on it, and
/// a tenant prefix synced to disk is byte-identical to the S3 one. CAS is
/// emulated with a per-key lock directory (mkdir is atomic on every
/// platform we care about) around read, compare, tmp-write, rename.
pub struct LocalFsStore {
    root: PathBuf,
}

impl LocalFsStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path_for(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    fn io(key: &str, source: std::io::Error) -> CasError {
        CasError::Io {
            key: key.to_string(),
            source,
        }
    }
}

/// Held while mutating one key. Dropping releases the lock.
struct KeyLock {
    dir: PathBuf,
}

impl KeyLock {
    fn acquire(path: &Path, key: &str) -> Result<Self, CasError> {
        let dir = path.with_extension("lock");
        if let Some(parent) = dir.parent() {
            fs::create_dir_all(parent).map_err(|e| LocalFsStore::io(key, e))?;
        }
        let stale = stale_lock_age();
        let deadline = Instant::now() + stale + Duration::from_secs(10);
        loop {
            match fs::create_dir(&dir) {
                Ok(()) => return Ok(Self { dir }),
                Err(e) if lock_busy(&e) => {
                    // A crash between mkdir and Drop leaves the lock dir
                    // behind forever, and without this check the key would
                    // be wedged for good. A live holder keeps a lock for
                    // the duration of one small file write, so a lock dir
                    // whose mtime is minutes old belongs to a dead process
                    // and gets broken. The mtime of a dir is set at mkdir
                    // and a crashed owner never touches it again.
                    if lock_is_stale(&dir, stale) {
                        break_stale_lock(&dir, stale);
                        continue;
                    }
                    if Instant::now() > deadline {
                        return Err(LocalFsStore::io(
                            key,
                            std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "gave up waiting for key lock",
                            ),
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(e) => return Err(LocalFsStore::io(key, e)),
            }
        }
    }
}

/// How old a lock dir must be before a waiter breaks it. The default is
/// far above any legitimate hold time, and the env override exists so
/// the crash fuzz can exercise the recovery path without waiting a
/// minute per case. Read on the contended path only, which is already
/// sleeping.
fn stale_lock_age() -> Duration {
    let ms = std::env::var("ZOU_LOCALFS_LOCK_STALE_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60_000);
    Duration::from_millis(ms)
}

fn lock_is_stale(dir: &Path, stale: Duration) -> bool {
    if let Ok(meta) = fs::metadata(dir)
        && let Ok(modified) = meta.modified()
        && let Ok(age) = modified.elapsed()
    {
        return age > stale;
    }
    false
}

/// Remove a stale lock so exactly one waiter does the breaking. A naive
/// stat then remove races: between one waiter's staleness check and its
/// remove, another waiter can break the stale lock, win the mkdir, and
/// have its fresh lock deleted out from under it, which lets two writers
/// into the critical section. The breaker lock serializes candidates and
/// the second staleness check inside it sees any lock that was rebuilt
/// fresh in the meantime and leaves it alone.
fn break_stale_lock(dir: &Path, stale: Duration) {
    let breaker = dir.with_extension("lock-break");
    match fs::create_dir(&breaker) {
        Ok(()) => {
            if lock_is_stale(dir, stale) {
                let _ = fs::remove_dir_all(dir);
            }
            let _ = fs::remove_dir(&breaker);
        }
        Err(_) => {
            // Another waiter is breaking right now, or a breaker crashed
            // and left this behind. A live breaker holds it for two stat
            // calls and a remove, so an old one is a crash leftover and
            // gets removed the plain way; the recheck above bounds what
            // the residual race here can do.
            if lock_is_stale(&breaker, stale) {
                let _ = fs::remove_dir_all(&breaker);
            }
        }
    }
}

/// A tmp path no other writer of the same key can collide with, so even
/// a lock protocol failure that lets two writers in cannot make one
/// rename the other's half written bytes or fail on a vanished tmp. The
/// name keeps the .tmp suffix so list keeps hiding it.
fn unique_tmp(path: &Path) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    path.with_extension(format!("{}-{n}.tmp", std::process::id()))
}

/// Whether a failed lock mkdir means "held by someone else, wait". Windows
/// reports a lock dir that its releasing thread has marked delete-pending
/// as access denied rather than already-exists, so both count as busy there.
fn lock_busy(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::AlreadyExists
        || (cfg!(windows) && e.kind() == std::io::ErrorKind::PermissionDenied)
}

impl Drop for KeyLock {
    fn drop(&mut self) {
        let _ = fs::remove_dir(&self.dir);
    }
}

impl CasStore for LocalFsStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        let path = self.path_for(key);
        // Windows cannot rename over a file another handle has open, so
        // put_if_match briefly removes the destination there. Readers take
        // the same key lock so they never observe that gap as a missing
        // object. On unix the rename is atomic and no lock is needed.
        #[cfg(windows)]
        let _lock = KeyLock::acquire(&path, key)?;
        match fs::read(&path) {
            Ok(data) => {
                let version = Version::of(&data);
                Ok(Some((data, version)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Self::io(key, e)),
        }
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
        use std::io::{Read, Seek, SeekFrom};
        let path = self.path_for(key);
        #[cfg(windows)]
        let _lock = KeyLock::acquire(&path, key)?;
        let mut file = match fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Self::io(key, e)),
        };
        let size = file.metadata().map_err(|e| Self::io(key, e))?.len();
        let start = offset.min(size);
        let end = offset.saturating_add(len).min(size);
        file.seek(SeekFrom::Start(start))
            .map_err(|e| Self::io(key, e))?;
        let mut data = vec![0u8; (end - start) as usize];
        file.read_exact(&mut data).map_err(|e| Self::io(key, e))?;
        Ok(Some(data))
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        let path = self.path_for(key);
        let _lock = KeyLock::acquire(&path, key)?;

        let current = match fs::read(&path) {
            Ok(existing) => Some(Version::of(&existing)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(Self::io(key, e)),
        };
        if current.as_ref() != expected {
            return Err(CasError::Conflict {
                key: key.to_string(),
            });
        }

        let tmp = unique_tmp(&path);
        let mut f = fs::File::create(&tmp).map_err(|e| Self::io(key, e))?;
        f.write_all(data).map_err(|e| Self::io(key, e))?;
        f.sync_all().map_err(|e| Self::io(key, e))?;
        drop(f);
        // Windows refuses to rename over an existing file when any handle
        // is open on it, so remove the destination first. Safe because both
        // writers and windows readers hold the key lock here.
        #[cfg(windows)]
        if path.exists() {
            fs::remove_file(&path).map_err(|e| Self::io(key, e))?;
        }
        fs::rename(&tmp, &path).map_err(|e| Self::io(key, e))?;
        Ok(Version::of(data))
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        let path = self.path_for(key);
        let _lock = KeyLock::acquire(&path, key)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| Self::io(key, e))?;
        }
        let tmp = unique_tmp(&path);
        let mut f = fs::File::create(&tmp).map_err(|e| Self::io(key, e))?;
        f.write_all(data).map_err(|e| Self::io(key, e))?;
        f.sync_all().map_err(|e| Self::io(key, e))?;
        drop(f);
        #[cfg(windows)]
        if path.exists() {
            fs::remove_file(&path).map_err(|e| Self::io(key, e))?;
        }
        fs::rename(&tmp, &path).map_err(|e| Self::io(key, e))?;
        Ok(Version::of(data))
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        let path = self.path_for(key);
        let _lock = KeyLock::acquire(&path, key)?;
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Self::io(key, e)),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        // Walk only the subtree the prefix can reach. The root can hold
        // hundreds of thousands of page objects, and a LIST of the wal
        // tail must not pay a full walk over all of them, S3 does not.
        // The prefix may end mid name, so descend to its directory part
        // and let the string filter below handle the rest.
        let dir = prefix.rsplit_once('/').map_or("", |(d, _)| d);
        let mut out = Vec::new();
        let mut stack = vec![self.root.join(dir)];
        while let Some(dir) = stack.pop() {
            let entries = match fs::read_dir(&dir) {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(Self::io(prefix, e)),
            };
            for entry in entries {
                let entry = entry.map_err(|e| Self::io(prefix, e))?;
                let path = entry.path();
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.ends_with(".lock")
                    || name.ends_with(".lock-break")
                    || name.ends_with(".tmp")
                {
                    continue;
                }
                if path.is_dir() {
                    stack.push(path);
                } else if let Ok(rel) = path.strip_prefix(&self.root) {
                    let key = rel.to_string_lossy().replace('\\', "/");
                    if key.starts_with(prefix) {
                        out.push(key);
                    }
                }
            }
        }
        out.sort();
        Ok(out)
    }
}
