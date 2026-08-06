//! Pool of sandboxed postgres redo workers.
//!
//! Each worker is a `postgres --zou-redo <scratchdir>` process, the
//! vendored walredo mode from patch 0005. It holds pages in memory,
//! applies WAL records through the real rmgr redo routines, and speaks
//! a tiny length-prefixed protocol on stdin and stdout. The pool feeds
//! it batches: a reset, the base images the batch needs, the record
//! chain in LSN order, then the page reads. One write, then the
//! responses, so a batch costs a single round trip.
//!
//! Workers are disposable by design. A malformed message or a redo
//! failure makes the worker die rather than limp along, and a wedged
//! worker gets killed by the janitor thread when its batch deadline
//! passes, which the blocked reader then sees as EOF. Either way the
//! caller gets an error and the pool spawns a fresh worker for the
//! next batch. Workers are also recycled after a fixed number of
//! batches, because postgres tracks references to unmaterialized
//! blocks in a table that only grows over a worker's life.
//!
//! Every worker gets its own scratch directory. The directory holds no
//! data, but postgres derives its sysv shared memory keys from it, so
//! sharing one across live workers would make their startups collide.
//!
//! Pages coming back carry whatever checksum the WAL images had, which
//! after a few record applies is stale. A page service serving a
//! checksum-enabled tenant has to recompute checksums before handing
//! pages to postgres.

use std::collections::HashMap;
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::walscan::BlockRef;

const BLCKSZ: usize = 8192;

/// How a [`RedoPool`] runs its workers.
pub struct RedoPoolConfig {
    /// The patched postgres binary, the one `make pg-build` installs.
    pub postgres: PathBuf,
    /// Parent for the per-worker scratch directories.
    pub scratch_root: PathBuf,
    /// Most workers alive at once. Batches beyond that wait their turn.
    pub workers: usize,
    /// A batch taking longer than this gets its worker killed.
    pub batch_timeout: Duration,
    /// Batches a worker serves before the pool replaces it, which
    /// bounds the growth of postgres's invalid page tracking.
    pub batches_per_worker: u64,
    /// Whether the cluster whose WAL the workers replay has data
    /// checksums on. This must match: redo routines branch on it, for
    /// example the visibility record only advances the heap page LSN
    /// under checksums, so a mismatch silently yields pages whose LSNs
    /// diverge from the primary's. With checksums on, pushed base
    /// images must carry valid checksums, because reads verify them.
    pub data_checksums: bool,
}

/// One batch: base images to push, records to apply, pages to read
/// back. Records are (lsn, end lsn, bytes) triples in LSN order, the
/// bytes the full record with header, exactly the form
/// [`crate::walscan::read_records`] returns. The end lsn matters: redo
/// stamps it into pages as the page LSN, and it differs from lsn plus
/// aligned length whenever the record crossed a WAL page boundary.
pub struct RedoRequest<'a> {
    pub pages: &'a [(BlockRef, &'a [u8])],
    pub records: &'a [(u64, u64, &'a [u8])],
    pub gets: &'a [BlockRef],
}

struct Worker {
    id: usize,
    batches: u64,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    child: Arc<Mutex<Child>>,
    deadline: Arc<AtomicU64>,
}

/// A live worker as the janitor sees it: something to kill when its
/// deadline, in milliseconds since the pool's epoch with zero meaning
/// idle, has passed.
struct Slot {
    child: Arc<Mutex<Child>>,
    deadline: Arc<AtomicU64>,
}

struct PoolState {
    idle: Vec<Worker>,
    spawned: usize,
    free_ids: Vec<usize>,
}

struct PoolShared {
    config: RedoPoolConfig,
    epoch: Instant,
    state: Mutex<PoolState>,
    available: Condvar,
    slots: Mutex<Vec<Slot>>,
    stop: AtomicBool,
}

/// The pool. Cheap to share behind an `Arc`, `apply` takes `&self` and
/// callers on different threads get different workers.
pub struct RedoPool {
    shared: Arc<PoolShared>,
    janitor: Option<JoinHandle<()>>,
}

impl RedoPool {
    pub fn new(config: RedoPoolConfig) -> RedoPool {
        let workers = config.workers.max(1);
        let shared = Arc::new(PoolShared {
            config,
            epoch: Instant::now(),
            state: Mutex::new(PoolState {
                idle: Vec::new(),
                spawned: 0,
                free_ids: (0..workers).rev().collect(),
            }),
            available: Condvar::new(),
            slots: Mutex::new(Vec::new()),
            stop: AtomicBool::new(false),
        });
        let janitor = {
            let shared = Arc::clone(&shared);
            std::thread::spawn(move || {
                while !shared.stop.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(200));
                    let now = shared.epoch.elapsed().as_millis() as u64;
                    let slots = shared.slots.lock().expect("no panics under lock");
                    for slot in slots.iter() {
                        let deadline = slot.deadline.load(Ordering::Acquire);
                        if deadline != 0 && now > deadline {
                            let _ = slot.child.lock().expect("no panics under lock").kill();
                        }
                    }
                }
            })
        };
        RedoPool {
            shared,
            janitor: Some(janitor),
        }
    }

    /// Run one batch and return the requested pages in `gets` order,
    /// each `BLCKSZ` bytes. Blocks the pushed images and records do
    /// not explain come back as all zeros. On error the worker involved
    /// is gone and the pool recovers by itself, so retrying is safe,
    /// but a batch that failed on a bad record will fail again. The
    /// responses are the only failure feedback there is, so a batch
    /// with no `gets` reports success without knowing whether the
    /// worker survived it. Real batches always read pages back.
    pub fn apply(&self, req: &RedoRequest) -> Result<Vec<Vec<u8>>, String> {
        for (blk, page) in req.pages {
            if page.len() != BLCKSZ {
                return Err(format!("base image for {blk:?} is {} bytes", page.len()));
            }
        }
        let mut batch = Vec::new();
        frame(&mut batch, b'R', 0);
        for (blk, page) in req.pages {
            frame(&mut batch, b'P', 17 + BLCKSZ);
            target(&mut batch, blk)?;
            batch.extend_from_slice(page);
        }
        for (lsn, end_lsn, record) in req.records {
            frame(&mut batch, b'A', 16 + record.len());
            batch.extend_from_slice(&lsn.to_le_bytes());
            batch.extend_from_slice(&end_lsn.to_le_bytes());
            batch.extend_from_slice(record);
        }
        for blk in req.gets {
            frame(&mut batch, b'G', 17);
            target(&mut batch, blk)?;
        }

        let mut worker = self.checkout()?;
        let deadline_ms = self.shared.epoch.elapsed() + self.shared.config.batch_timeout;
        worker
            .deadline
            .store(deadline_ms.as_millis().max(1) as u64, Ordering::Release);
        let run = (|| -> std::io::Result<Vec<Vec<u8>>> {
            worker.stdin.write_all(&batch)?;
            worker.stdin.flush()?;
            let mut out = Vec::with_capacity(req.gets.len());
            for _ in req.gets {
                let mut head = [0u8; 5];
                worker.stdout.read_exact(&mut head)?;
                if head[0] != b'O'
                    || u32::from_le_bytes([head[1], head[2], head[3], head[4]]) != BLCKSZ as u32
                {
                    return Err(std::io::Error::other("bad response frame"));
                }
                let mut page = vec![0u8; BLCKSZ];
                worker.stdout.read_exact(&mut page)?;
                out.push(page);
            }
            Ok(out)
        })();
        worker.deadline.store(0, Ordering::Release);
        match run {
            Ok(pages) => {
                worker.batches += 1;
                if worker.batches >= self.shared.config.batches_per_worker {
                    self.retire(worker);
                } else {
                    let mut state = self.shared.state.lock().expect("no panics under lock");
                    state.idle.push(worker);
                    drop(state);
                    self.shared.available.notify_one();
                }
                Ok(pages)
            }
            Err(err) => {
                let timed_out = self.shared.epoch.elapsed() >= deadline_ms;
                self.retire(worker);
                if timed_out {
                    Err(format!("redo batch timed out: {err}"))
                } else {
                    Err(format!("redo worker failed: {err}"))
                }
            }
        }
    }

    /// Take an idle worker, spawn one if there is room, or wait.
    fn checkout(&self) -> Result<Worker, String> {
        let mut state = self.shared.state.lock().expect("no panics under lock");
        loop {
            if let Some(worker) = state.idle.pop() {
                return Ok(worker);
            }
            if state.spawned < self.shared.config.workers.max(1) {
                let id = state.free_ids.pop().expect("ids track spawn slots");
                state.spawned += 1;
                drop(state);
                return self.spawn(id).inspect_err(|_| {
                    let mut state = self.shared.state.lock().expect("no panics under lock");
                    state.spawned -= 1;
                    state.free_ids.push(id);
                    drop(state);
                    self.shared.available.notify_one();
                });
            }
            state = self
                .shared
                .available
                .wait(state)
                .expect("no panics under lock");
        }
    }

    fn spawn(&self, id: usize) -> Result<Worker, String> {
        let scratch = self.shared.config.scratch_root.join(format!("w{id}"));
        std::fs::create_dir_all(&scratch)
            .map_err(|err| format!("create scratch dir {scratch:?}: {err}"))?;
        let mut child = Command::new(&self.shared.config.postgres)
            .arg("--zou-redo")
            .arg(&scratch)
            .arg(if self.shared.config.data_checksums {
                "1"
            } else {
                "0"
            })
            .env_remove("ZOU_TARGET")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|err| format!("spawn redo worker: {err}"))?;
        let stdin = child.stdin.take().expect("piped");
        let stdout = BufReader::new(child.stdout.take().expect("piped"));
        let child = Arc::new(Mutex::new(child));
        let deadline = Arc::new(AtomicU64::new(0));
        let mut slots = self.shared.slots.lock().expect("no panics under lock");
        slots.push(Slot {
            child: Arc::clone(&child),
            deadline: Arc::clone(&deadline),
        });
        drop(slots);
        Ok(Worker {
            id,
            batches: 0,
            stdin,
            stdout,
            child,
            deadline,
        })
    }

    /// Kill and reap a worker and free its slot for a fresh spawn.
    fn retire(&self, worker: Worker) {
        let mut slots = self.shared.slots.lock().expect("no panics under lock");
        slots.retain(|slot| !Arc::ptr_eq(&slot.child, &worker.child));
        drop(slots);
        // Closing stdin first lets a healthy worker exit on its own,
        // the kill only matters for a wedged one.
        drop(worker.stdin);
        let mut child = worker.child.lock().expect("no panics under lock");
        let _ = child.kill();
        let _ = child.wait();
        drop(child);
        let mut state = self.shared.state.lock().expect("no panics under lock");
        state.spawned -= 1;
        state.free_ids.push(worker.id);
        drop(state);
        self.shared.available.notify_one();
    }
}

impl Drop for RedoPool {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        if let Some(janitor) = self.janitor.take() {
            let _ = janitor.join();
        }
        let mut state = self.shared.state.lock().expect("no panics under lock");
        state.idle.clear();
        drop(state);
        let mut slots = self.shared.slots.lock().expect("no panics under lock");
        for slot in slots.drain(..) {
            let mut child = slot.child.lock().expect("no panics under lock");
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn frame(buf: &mut Vec<u8>, tag: u8, len: usize) {
    buf.push(tag);
    buf.extend_from_slice(&(len as u32).to_le_bytes());
}

fn target(buf: &mut Vec<u8>, blk: &BlockRef) -> Result<(), String> {
    if blk.fork > u8::MAX as u32 {
        return Err(format!("fork {} out of range", blk.fork));
    }
    buf.extend_from_slice(&blk.spc.to_le_bytes());
    buf.extend_from_slice(&blk.db.to_le_bytes());
    buf.extend_from_slice(&blk.rel.to_le_bytes());
    buf.push(blk.fork as u8);
    buf.extend_from_slice(&blk.blk.to_le_bytes());
    Ok(())
}

/// The postgres data page checksum, a port of pg_checksum_page from
/// storage/checksum_impl.h: 32 parallel FNV-1a style sums over the page
/// with the checksum field read as zero, folded, mixed with the block
/// number and squeezed to 16 bits. The page service needs it because
/// pages materialized by redo carry stale checksums, redo never
/// recomputes them, only real disk writes do.
pub fn page_checksum(page: &[u8], blkno: u32) -> u16 {
    assert_eq!(page.len(), BLCKSZ, "one full page");
    const FNV_PRIME: u32 = 16777619;
    const BASE: [u32; 32] = [
        0x5B1F36E9, 0xB8525960, 0x02AB50AA, 0x1DE66D2A, 0x79FF467A, 0x9BB9F8A3, 0x217E7CD2,
        0x83E13D2C, 0xF8D4474F, 0xE39EB970, 0x42C6AE16, 0x993216FA, 0x7B093B5D, 0x98DAFF3C,
        0xF718902A, 0x0B1C9CDB, 0xE58F764B, 0x187636BC, 0x5D7B3BB1, 0xE73DE7DE, 0x92BEC979,
        0xCCA6C0B2, 0x304A0979, 0x85AA43D4, 0x783125BB, 0x6CA8EAA2, 0xE407EAC6, 0x4B5CFC3E,
        0x9FBF8C76, 0x15CA20BE, 0xF2CA9FD3, 0x959BD756,
    ];
    let comp = |sum: u32, value: u32| {
        let tmp = sum ^ value;
        tmp.wrapping_mul(FNV_PRIME) ^ (tmp >> 17)
    };
    let mut sums = BASE;
    for (i, chunk) in page.chunks_exact(4).enumerate() {
        let mut word = u32::from_le_bytes(chunk.try_into().expect("checked length"));
        if i == 2 {
            // pd_checksum occupies bytes 8..10, computed as zero.
            word &= 0xFFFF0000;
        }
        sums[i % 32] = comp(sums[i % 32], word);
    }
    for sum in &mut sums {
        *sum = comp(comp(*sum, 0), 0);
    }
    let folded = sums.iter().fold(0u32, |acc, sum| acc ^ sum);
    (((folded ^ blkno) % 65535) + 1) as u16
}

/// Group a record chain by the blocks each record touches, the shape a
/// page service wants when it turns one WAL window into per-page apply
/// chains. Records whose parse fails are reported, not dropped, so the
/// caller decides whether a corrupt record poisons the whole window.
pub fn chains_by_block(
    records: &[crate::walscan::WalRecord],
) -> Result<HashMap<BlockRef, Vec<&crate::walscan::WalRecord>>, String> {
    let mut chains: HashMap<BlockRef, Vec<&crate::walscan::WalRecord>> = HashMap::new();
    for record in records {
        for blk in record.block_refs()? {
            chains.entry(blk).or_default().push(record);
        }
    }
    Ok(chains)
}
