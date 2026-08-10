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

    /// A url that reads this object from the backend directly, good for
    /// `ttl`, or `None` from a backend where no such url exists.
    ///
    /// This is the one thing in the trait that is not about correctness.
    /// Nothing zou does needs it: every read here works by reading. It
    /// is here so a server that would otherwise copy a large object
    /// through itself can hand the caller a url and step out of the
    /// egress path, which is a deployment's choice rather than the
    /// store's, so the store only answers whether it is possible.
    ///
    /// `response` are `response-*` parameters the url carries, so the
    /// answer the backend gives has the content type and the
    /// disposition the caller asked for rather than whatever the bytes
    /// were uploaded with. They are part of what is signed, so a url
    /// cannot be edited into one that says something else.
    ///
    /// `None` is the honest answer almost everywhere: a directory on a
    /// laptop is not reachable by url, and neither is a sqlite file.
    fn presigned_get(
        &self,
        key: &str,
        ttl: Duration,
        response: &[(&str, &str)],
    ) -> Result<Option<String>, CasError> {
        let _ = (key, ttl, response);
        Ok(None)
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
                Ok(()) => {
                    // Stamp the holder pid so a waiter can tell a crashed
                    // owner from a slow one. A failed stamp only costs the
                    // early break, the age rule still applies.
                    let _ = fs::File::create(dir.join(format!("pid-{}", std::process::id())));
                    return Ok(Self { dir });
                }
                Err(e) if lock_busy(&e) => {
                    // A crash between mkdir and Drop leaves the lock dir
                    // behind forever, and without this check the key would
                    // be wedged for good. A live holder keeps a lock for
                    // the duration of one small file write, so a lock dir
                    // whose mtime is minutes old belongs to a dead process
                    // and gets broken. The mtime of a dir is set at mkdir
                    // and a crashed owner never touches it again. The pid
                    // stamp shortcuts the wait: local means same host, so
                    // a lock whose every stamped owner is gone is a crash
                    // leftover no matter how fresh, and every second spent
                    // honoring it is a second of commit stall in a kill
                    // drill.
                    if lock_expired(&dir, stale) {
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

/// A lock is expired when it is old enough, or when every process that
/// stamped it is gone. The second arm never fires for a lock whose
/// stamp did not land or could not be read: absence of evidence keeps
/// the conservative age rule.
fn lock_expired(dir: &Path, stale: Duration) -> bool {
    lock_is_stale(dir, stale) || lock_owner_dead(dir)
}

/// Whether the lock dir carries pid stamps and every stamped pid is
/// dead. Localfs means one host, so a dead pid is proof the holder
/// crashed mid-write. A recycled pid reads as alive and falls back to
/// the age rule, which only ever errs toward waiting.
fn lock_owner_dead(dir: &Path) -> bool {
    let Ok(entries) = fs::read_dir(dir) else {
        return false;
    };
    let mut stamped = false;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(pid) = name
            .to_str()
            .and_then(|n| n.strip_prefix("pid-"))
            .and_then(|n| n.parse::<u32>().ok())
        else {
            continue;
        };
        stamped = true;
        if pid_alive(pid) {
            return false;
        }
    }
    stamped
}

#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    // Signal zero probes without sending. EPERM still means the pid
    // exists, it just belongs to someone we cannot signal.
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// No cheap liveness probe here, the age rule alone decides.
#[cfg(not(unix))]
fn pid_alive(_pid: u32) -> bool {
    true
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
            if lock_expired(dir, stale) {
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
        // Only our own stamp comes out. If a breaker handed this lock
        // to a new holder while we were still alive, the remove_dir
        // fails on their stamp and their lock survives us.
        let _ = fs::remove_file(self.dir.join(format!("pid-{}", std::process::id())));
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A pid that no live process holds: a child that already exited
    /// and was reaped. Recycling between the wait and the assertion
    /// would need the kernel to lap its whole pid space in
    /// microseconds.
    #[cfg(unix)]
    fn dead_pid() -> u32 {
        let mut child = std::process::Command::new("true")
            .spawn()
            .expect("spawn true");
        let pid = child.id();
        child.wait().expect("wait true");
        pid
    }

    #[cfg(unix)]
    #[test]
    fn dead_owner_lock_breaks_without_the_age_wait() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let lock = dir.path().join("wedged.lock");
        fs::create_dir(&lock).unwrap();
        fs::File::create(lock.join(format!("pid-{}", dead_pid()))).unwrap();
        let started = Instant::now();
        store.put_if_absent("wedged", b"through").unwrap();
        // The age rule alone would sit on this lock for a minute.
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(store.get("wedged").unwrap().unwrap().0, b"through");
    }

    #[cfg(unix)]
    #[test]
    fn live_owner_keeps_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("held.lock");
        fs::create_dir(&lock).unwrap();
        fs::File::create(lock.join(format!("pid-{}", std::process::id()))).unwrap();
        assert!(!lock_owner_dead(&lock));
        assert!(!lock_expired(&lock, stale_lock_age()));
    }

    #[test]
    fn unstamped_lock_falls_back_to_the_age_rule() {
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("bare.lock");
        fs::create_dir(&lock).unwrap();
        assert!(!lock_owner_dead(&lock));
        assert!(!lock_expired(&lock, Duration::from_secs(60)));
        assert!(lock_expired(&lock, Duration::ZERO));
    }

    #[test]
    fn a_released_lock_leaves_nothing_behind() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        store.put_if_absent("clean", b"x").unwrap();
        assert!(!dir.path().join("clean.lock").exists());
    }
}
