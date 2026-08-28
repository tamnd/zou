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
    #[error(
        "object {key} already exists and is immutable, the bytes under that key are final, so a writer meaning to replace them has picked a key an earlier write already used, `zou inspect <target> {key}` prints what is under it"
    )]
    AlreadyExists { key: String },
    /// The guard refused a versioned overwrite of a write-once key. Unlike
    /// Conflict this is not retryable: no version makes it legal.
    #[error(
        "refusing to overwrite immutable object {key}, no version makes this legal so retrying cannot help, this is a bug in zou rather than anything to do about the store, please report it with the key"
    )]
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
///
/// Create if absent does not use the lock at all, because the lock is
/// breakable and this one operation must hold against a writer that was
/// stopped mid put and comes back minutes later. It publishes by hard
/// link instead, which fails against whatever is at the key when it
/// runs. Whether that is available is asked of the filesystem once per
/// store rather than once per call, since two creators that pick
/// different ways of saying no do not say it to each other, see
/// `LocalFsStore::links_work`.
pub struct LocalFsStore {
    root: PathBuf,
    /// Whether a write waits for the disk before it says it landed.
    ///
    /// True everywhere except a store that carries the scratch marker,
    /// see [`SCRATCH_MARKER`].
    durable: bool,
    /// Whether this filesystem makes hard links, asked once and then
    /// remembered, see [`LocalFsStore::links_work`].
    links: std::sync::OnceLock<bool>,
}

/// A file at the root of a store that says its contents are disposable.
///
/// A durable write is a write plus a wait for the platter, which on a
/// mac is F_FULLFSYNC and is the single most expensive thing in a small
/// put: about 14 ms of a 47 ms fixture database create. That is the
/// right price for a database somebody will come back to, and the wrong
/// price for one that exists for the length of a test run and is deleted
/// by the handle that made it. A store with this file in it skips the
/// wait, so a crash can lose or tear its most recent writes.
///
/// It is a file rather than an environment variable because durability
/// is a property of the store, not of the process: one test process can
/// hold a fixture and a real project at the same time, and only the
/// fixture is throwaway. Nothing writes this on its own except the
/// embedded template cache, which writes it after the template is built
/// and fsynced, so what the marker covers is the fixtures cut from it
/// rather than the template they read.
pub const SCRATCH_MARKER: &str = ".zou-scratch";

impl LocalFsStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let durable = !root.join(SCRATCH_MARKER).exists();
        Self {
            root,
            durable,
            links: std::sync::OnceLock::new(),
        }
    }

    /// Whether a create on this store is decided by a hard link or by
    /// the key lock, asked of the filesystem once and then held.
    ///
    /// It is one answer per store and not one per call, and that is the
    /// whole point of it. A link and a lock are two ways of saying no to
    /// a second creator, and they do not say it to each other: a caller
    /// that links while another caller, refused a link a moment ago, is
    /// inside the lock writing the same key, gets two winners out of a
    /// race that has exactly one. Deciding per call is what made that
    /// reachable, since the decision was a transient error from one
    /// link, and a transient error is by definition something the next
    /// caller does not see.
    ///
    /// So the question asked here is about the filesystem rather than
    /// about the call: exFAT on a usb stick and some network mounts make
    /// no links, and everything else does. The probe is two files in the
    /// store root, named so that two processes probing at once cannot
    /// collide.
    fn links_work(&self) -> bool {
        *self.links.get_or_init(|| {
            if fs::create_dir_all(&self.root).is_err() {
                return false;
            }
            let from = self.root.join(format!(".zou-links-{}.tmp", stamp()));
            let to = from.with_extension("link.tmp");
            let made = fs::File::create(&from).is_ok() && fs::hard_link(&from, &to).is_ok();
            let _ = fs::remove_file(&from);
            let _ = fs::remove_file(&to);
            if !made {
                log::warn!(
                    "localfs: {} makes no hard links, so creates are decided by the key lock instead, which a stalled writer can break",
                    self.root.display()
                );
            }
            made
        })
    }

    /// Say that the store at `root` is scratch. Stores opened on it after
    /// this stop fsyncing, see [`SCRATCH_MARKER`].
    pub fn mark_scratch(root: &Path) -> std::io::Result<()> {
        fs::create_dir_all(root)?;
        fs::write(
            root.join(SCRATCH_MARKER),
            b"This store is disposable, so writes to it are not fsynced.\n",
        )
    }

    /// Whether this store waits for the disk. Public for the tests that
    /// check the marker is read where it is written.
    pub fn is_durable(&self) -> bool {
        self.durable
    }

    fn sync(&self, key: &str, f: &fs::File) -> Result<(), CasError> {
        if self.durable {
            f.sync_all().map_err(|e| Self::io(key, e))?;
        }
        Ok(())
    }

    fn path_for(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    /// Write `data` into a tmp file and publish it at `path` only if
    /// nothing is there, atomically and without holding anything.
    ///
    /// A rename cannot do this. It replaces whatever it lands on, so an
    /// absence checked before the write is a promise about the past, and
    /// the key lock is what usually turns that into a promise about now.
    /// The lock is breakable by design, since a crashed holder must not
    /// wedge a key forever, and a writer that was frozen rather than
    /// killed is a live process holding a lock it will not touch again
    /// until it thaws. Wait out the stale age, break the lock, write,
    /// and the thaw's rename lands on top of the winner and quietly
    /// takes the key from it. On the WAL chain that is a successor's
    /// seal being replaced by the landing segment it fenced, which
    /// unlinks every segment the successor wrote after it.
    ///
    /// A hard link is the operation that says no. It fails with EEXIST
    /// against whatever is at the destination when it runs, so a
    /// creation stopped for a minute mid flight loses to the writer that
    /// got there first, which is exactly what the object store this
    /// stands in for does with If-None-Match.
    fn publish(&self, key: &str, path: &Path, data: &[u8]) -> Result<Version, CasError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| Self::io(key, e))?;
        }
        let tmp = unique_tmp(path);
        let mut f = fs::File::create(&tmp).map_err(|e| Self::io(key, e))?;
        f.write_all(data).map_err(|e| Self::io(key, e))?;
        self.sync(key, &f)?;
        drop(f);
        let linked = fs::hard_link(&tmp, path);
        let _ = fs::remove_file(&tmp);
        match linked {
            Ok(()) => Ok(Version::of(data)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(CasError::AlreadyExists {
                    key: key.to_string(),
                })
            }
            Err(e) => Err(Self::io(key, e)),
        }
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
    /// The name of this acquisition's stamp file, which is what makes
    /// [`Self::still_mine`] an answer about this lock rather than about
    /// this process. See [`stamp`].
    stamp: String,
}

impl KeyLock {
    fn acquire(path: &Path, key: &str) -> Result<Self, CasError> {
        let dir = path.with_extension("lock");
        if let Some(parent) = dir.parent() {
            fs::create_dir_all(parent).map_err(|e| LocalFsStore::io(key, e))?;
        }
        let stale = stale_lock_age();
        let started = Instant::now();
        let deadline = started + stale + Duration::from_secs(10);
        let mut polls = 0u64;
        let mut breaks = 0u64;
        loop {
            match fs::create_dir(&dir) {
                Ok(()) => {
                    // Stamp the holder pid so a waiter can tell a crashed
                    // owner from a slow one. A failed stamp only costs the
                    // early break, the age rule still applies.
                    let stamp = stamp();
                    let _ = fs::File::create(dir.join(&stamp));
                    return Ok(Self { dir, stamp });
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
                        breaks += 1;
                        break_stale_lock(&dir, stale);
                        // Both outcomes go round through the sleep
                        // below. A break that worked leaves nothing to
                        // wait for and losing 2 ms on it costs nothing,
                        // and a break that failed is a lock this waiter
                        // cannot remove, which retried without a sleep
                        // is a hot spin that never reaches the deadline
                        // check and so never gives up either.
                    }
                    if Instant::now() > deadline {
                        return Err(LocalFsStore::io(
                            key,
                            std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                format!(
                                    "gave up waiting for key lock after {:?}, {}",
                                    started.elapsed(),
                                    lock_report(&dir, polls, breaks)
                                ),
                            ),
                        ));
                    }
                    polls += 1;
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(e) => return Err(LocalFsStore::io(key, e)),
            }
        }
    }

    /// Is this still the lock we took? A breaker removes the whole dir,
    /// stamp and all, so a holder that was frozen past the stale age
    /// comes back to a lock that is somebody else's or gone, and knows
    /// another writer has had the key in the meantime.
    fn still_mine(&self) -> bool {
        self.dir.join(&self.stamp).exists()
    }
}

/// A name for one acquisition of one lock.
///
/// The pid is in it because a waiter deciding whether to break a lock
/// asks whether its holder is still alive, and localfs means one host,
/// so a pid is the whole of that question. The counter is in it because
/// a pid is not enough to say *which* holder: two threads of one process
/// stamp the same name, and then a holder whose lock was broken and
/// taken by a sibling thread looks at the thief's stamp, reads its own
/// pid, and concludes it still holds a lock somebody else is inside.
///
/// That is how two writers got into the critical section on windows,
/// where everything is slow enough for a live holder to pass the stale
/// age. It is not a windows bug, it is a stamp that answered the wrong
/// question, and every platform could hit it under a long enough stall.
fn stamp() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("pid-{}-{n}", std::process::id())
}

/// How old a lock dir must be before a waiter breaks it. The default is
/// far above any legitimate hold time, and the env override exists so
/// the crash fuzz can exercise the recovery path without waiting a
/// minute per case. Read on the contended path only, which is already
/// sleeping.
fn stale_lock_age() -> Duration {
    let ms = crate::setting::number_or(
        "ZOU_LOCALFS_LOCK_STALE_MS",
        "a number of milliseconds",
        60_000u64,
    );
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
        // `pid-<pid>-<n>`, and `pid-<pid>` from a store last written by
        // a build before the counter existed. Either way what is being
        // asked is whose process it was.
        let Some(pid) = name
            .to_str()
            .and_then(|n| n.strip_prefix("pid-"))
            .map(|n| n.split_once('-').map_or(n, |(pid, _)| pid))
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

/// Whether the lock dir carries a stamp that is not this one, which is
/// how a holder tells "my own release has not landed yet" from "a
/// breaker handed this lock on and it is somebody else's now".
fn lock_stamped_by_another(dir: &Path, mine: &str) -> bool {
    let Ok(entries) = fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|entry| {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        name.starts_with("pid-") && name != mine
    })
}

/// What a waiter could see about the lock it gave up on. A timeout that
/// says nothing but timeout leaves the next one as hard to read as the
/// last: a lock a live writer is inside, a lock standing with nobody
/// inside it, and a name windows will not stat because a delete on it
/// is still pending are three different bugs and they all print the
/// same otherwise.
fn lock_report(dir: &Path, polls: u64, breaks: u64) -> String {
    let age = match fs::metadata(dir) {
        Ok(meta) => match meta.modified().ok().and_then(|m| m.elapsed().ok()) {
            Some(age) => format!("last touched {} ms ago", age.as_millis()),
            None => "with no mtime this platform would give up".to_string(),
        },
        Err(e) => format!("that does not stat ({e})"),
    };
    let holders = match fs::read_dir(dir) {
        Ok(entries) => {
            let names: Vec<String> = entries
                .flatten()
                .take(4)
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect();
            if names.is_empty() {
                "and carries no stamp".to_string()
            } else {
                format!("and carries {}", names.join(", "))
            }
        }
        Err(e) => format!("and does not list ({e})"),
    };
    format!("{polls} polls and {breaks} break attempts against a lock dir {age} {holders}")
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
        // fails on their stamp and their lock survives us. That holds
        // for a thief in this process too, which is what the counter in
        // the stamp name buys: before it, this line removed the thief's
        // stamp and the remove_dir below then deleted a lock somebody
        // else was inside.
        //
        // The removal is retried for a moment because a delete is not
        // final on windows until the last handle to the name closes,
        // and a lock dir whose stamp is in that state is not empty yet,
        // so the one shot version could return leaving the lock
        // standing with nobody inside it. That is worse than a slow
        // release: the key is then unwritable until some waiter sits
        // out the whole stale age on it and breaks it. The window here
        // is a hundredth of that age, and a stamp that is not ours ends
        // it early because that lock is somebody else's.
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let _ = fs::remove_file(self.dir.join(&self.stamp));
            if fs::remove_dir(&self.dir).is_ok() {
                return;
            }
            if lock_stamped_by_another(&self.dir, &self.stamp) || !self.dir.exists() {
                return;
            }
            if Instant::now() >= deadline {
                return;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
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

    /// Create a key, or refuse because something is already there.
    ///
    /// Which mechanism says no is a property of the filesystem and is
    /// settled before the first create, see `Self::links_work`. Asking
    /// per call is what let two creators of the same key take different
    /// mechanisms and both be told yes, because a link that failed for a
    /// transient reason is a reason only the caller that hit it can see.
    /// On a filesystem that makes no links there is nothing to fence a
    /// frozen writer with, so the lock is the best available answer and
    /// the warning at the probe is where that is said.
    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        if !self.links_work() {
            return match self.put_if_match(key, data, None) {
                Err(CasError::Conflict { key }) => Err(CasError::AlreadyExists { key }),
                other => other,
            };
        }
        let path = self.path_for(key);
        self.publish(key, &path, data)
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        let path = self.path_for(key);
        let lock = KeyLock::acquire(&path, key)?;

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
        self.sync(key, &f)?;
        drop(f);
        // Whoever broke this lock has been through the key since the
        // read above, so the compare it passed says nothing any more and
        // the rename below would take the key from them. A holder frozen
        // past the stale age is the way that happens.
        if !lock.still_mine() {
            return Err(CasError::Conflict {
                key: key.to_string(),
            });
        }
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
        self.sync(key, &f)?;
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
                    || name == SCRATCH_MARKER
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
        store.put_if_match("wedged", b"through", None).unwrap();
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

    /// A holder whose lock was broken and taken has to know, and it has
    /// to know when the thief is a thread of its own process.
    ///
    /// The stamp used to be the pid alone, which is the right question
    /// for "is the holder still alive" and the wrong one for "is this
    /// still my lock". Two threads stamped the same name, so the victim
    /// read the thief's stamp, recognised its own pid, and carried on
    /// into a critical section somebody else was already in. On windows
    /// that is reachable without freezing anything, because a live
    /// holder there can take longer than the stale age over one small
    /// write, and it is how a creator race elected two winners.
    #[test]
    fn a_lock_stolen_by_a_sibling_thread_is_noticed_by_the_holder() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("contended");
        let lock = path.with_extension("lock");

        let held = KeyLock::acquire(&path, "contended").unwrap();
        assert!(held.still_mine());

        // What a waiter does to a lock it has decided is stale, done
        // here with a zero age so nothing has to sleep for it.
        std::thread::sleep(Duration::from_millis(2));
        break_stale_lock(&lock, Duration::ZERO);
        let thief = KeyLock::acquire(&path, "contended").unwrap();

        assert!(thief.still_mine(), "the thief holds the lock it took");
        assert!(
            !held.still_mine(),
            "the holder did not notice its lock had been taken, so both are in the critical section"
        );

        // And the victim going away leaves the thief holding it.
        drop(held);
        assert!(
            thief.still_mine(),
            "the victim's release took the thief's lock with it"
        );
    }

    /// A release has to end with the lock gone even when the first
    /// remove loses a race. On windows the thing it loses to is the
    /// stamp, whose delete is pending until the last handle to the name
    /// closes, so the dir is briefly not empty and remove_dir says so.
    /// A release that gave up there would leave a lock nobody holds and
    /// nobody can create, and every later writer of that key would sit
    /// out the stale age before breaking it.
    #[test]
    fn a_release_that_cannot_remove_the_dir_yet_keeps_trying() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("slow");
        let lock = path.with_extension("lock");
        let held = KeyLock::acquire(&path, "slow").unwrap();

        // Stands in for the pending delete: something in the dir that
        // makes remove_dir fail and then goes away on its own.
        let blocker = lock.join("not-a-stamp");
        fs::File::create(&blocker).unwrap();
        let clearer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            fs::remove_file(&blocker).unwrap();
        });

        drop(held);
        clearer.join().unwrap();
        assert!(!lock.exists(), "the release left the lock standing");
    }

    /// And it stops as soon as the lock is somebody else's, because a
    /// stamp that is not ours means a breaker handed the key on while
    /// this holder was still alive.
    #[test]
    fn a_release_leaves_a_lock_another_holder_stamped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("taken");
        let lock = path.with_extension("lock");
        let held = KeyLock::acquire(&path, "taken").unwrap();
        fs::File::create(lock.join("pid-1-999")).unwrap();

        let started = Instant::now();
        drop(held);
        assert!(lock.exists(), "the release took a lock it no longer held");
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "the release waited on a lock that was not its own"
        );
        fs::remove_dir_all(&lock).unwrap();
    }

    /// What the give up says. The point of the message is that the next
    /// windows flake arrives with the state that produced it, so the
    /// three ways a wait can end are told apart from the log alone.
    #[test]
    fn the_give_up_message_carries_what_the_waiter_saw() {
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("wedged.lock");
        fs::create_dir(&lock).unwrap();
        fs::File::create(lock.join("pid-1-7")).unwrap();
        let report = lock_report(&lock, 35, 2);
        assert!(report.contains("35 polls"), "{report}");
        assert!(report.contains("2 break attempts"), "{report}");
        assert!(report.contains("last touched"), "{report}");
        assert!(report.contains("pid-1-7"), "{report}");

        // And a lock dir that is not there at all, which is the shape
        // windows reports for a name whose delete is still pending.
        fs::remove_dir_all(&lock).unwrap();
        let report = lock_report(&lock, 1, 0);
        assert!(report.contains("does not stat"), "{report}");
    }

    #[test]
    fn a_released_lock_leaves_nothing_behind() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        store.put_if_match("clean", b"x", None).unwrap();
        assert!(!dir.path().join("clean.lock").exists());
    }

    /// The freeze case. A writer stopped mid create holds the key lock
    /// and will not touch it again until it thaws, so the lock has to be
    /// breakable or the key is wedged, and the creation that goes
    /// through while it is broken has to survive the thaw.
    #[test]
    fn a_creation_beats_a_frozen_writer_that_still_holds_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let frozen = KeyLock::acquire(&dir.path().join("seq"), "seq").expect("hold the lock");

        // The successor creates the object without waiting on a lock it
        // could only take by breaking, so the frozen writer is still
        // holding the lock it froze with afterwards. That is the
        // assertion rather than how long the create took, because the
        // difference between the two paths is whether a lock was broken
        // and not how slow the machine is.
        store.put_if_absent("seq", b"the seal").expect("created");
        assert!(
            frozen.still_mine(),
            "the create went through the lock and broke a live holder's"
        );

        // And the thaw: the same PUT the frozen writer had in flight,
        // arriving after. It loses, and takes nothing with it.
        match store.put_if_absent("seq", b"a fenced landing") {
            Err(CasError::AlreadyExists { key }) => assert_eq!(key, "seq"),
            other => panic!("a late creation took the key: {other:?}"),
        }
        assert_eq!(store.get("seq").unwrap().unwrap().0, b"the seal");
        drop(frozen);
    }

    /// Which way a create says no is settled once, because two creators
    /// on different mechanisms do not exclude each other.
    #[test]
    fn the_link_probe_is_asked_once_and_leaves_nothing_behind() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        assert!(
            store.links_work(),
            "a temporary directory makes hard links on every platform this builds for"
        );
        store.put_if_absent("k", b"v").expect("created");

        // The probe writes two files at the root, and list skips
        // anything ending .tmp, so ask the directory instead.
        let left: Vec<String> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(".zou-links-"))
            .collect();
        assert!(left.is_empty(), "the probe left {left:?} in the store");

        // Asked once: the answer outlives the directory it was asked
        // about, which a second probe would have to recreate to run.
        fs::remove_dir_all(dir.path()).unwrap();
        assert!(store.links_work());
        assert!(!dir.path().exists(), "the probe ran a second time");
    }

    /// The marker is a property of the directory, so a store opened on a
    /// marked one is scratch and a store opened on the directory next to
    /// it is not, in the same process.
    #[test]
    fn the_scratch_marker_turns_the_fsync_off_for_that_store_only() {
        let dir = tempfile::tempdir().unwrap();
        let scratch = dir.path().join("throwaway");
        let kept = dir.path().join("kept");
        LocalFsStore::mark_scratch(&scratch).expect("mark");
        fs::create_dir_all(&kept).expect("create");

        let scratch = LocalFsStore::new(&scratch);
        let kept = LocalFsStore::new(&kept);
        assert!(!scratch.is_durable());
        assert!(kept.is_durable());

        // Writes still work and still read back, they just do not wait
        // for the platter.
        scratch.put("a/b", b"one").unwrap();
        scratch.put_if_match("c", b"two", None).unwrap();
        scratch.put_if_absent("d", b"three").unwrap();
        assert_eq!(scratch.get("a/b").unwrap().unwrap().0, b"one");
        assert_eq!(scratch.get("c").unwrap().unwrap().0, b"two");
        assert_eq!(scratch.get("d").unwrap().unwrap().0, b"three");
        // And the marker is not an object, so nothing walking the store
        // has to know about it.
        assert_eq!(scratch.list("").unwrap(), vec!["a/b", "c", "d"]);
    }

    /// The same freeze against a conditional write, where the compare
    /// the caller passed was read before the lock was broken and says
    /// nothing about the key any more.
    #[test]
    fn a_conditional_write_whose_lock_was_broken_does_not_publish() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest");
        let mine = KeyLock::acquire(&path, "manifest").expect("hold the lock");
        assert!(mine.still_mine());
        break_stale_lock(&path.with_extension("lock"), Duration::ZERO);
        assert!(!mine.still_mine());
    }
}
