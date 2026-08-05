//! Protocol-only commit latency bench for the wal pipeline, no
//! Postgres. Eight committer threads flush fake WAL and wait on the
//! published watermark, one pusher thread drives the C ABI the same
//! way ZouWalPusherMain does. Mode "serial" emulates the old blocking
//! pusher, mode "pipeline" the staged one.
//!
//!   cargo run --release -p zou-pg --example pipe_bench -- pipeline 500
//!
//! The second arg is committer think time in microseconds.

use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use zou_pg::{zou_wal_append, zou_wal_durable, zou_wal_open};

const CLIENTS: usize = 8;
const RUN_SECS: u64 = 10;
const COMMIT_BYTES: u64 = 350;

struct Gate {
    lock: Mutex<u64>,
    cv: Condvar,
}

impl Gate {
    fn publish(&self, v: u64) {
        *self.lock.lock().unwrap() = v;
        self.cv.notify_all();
    }
    fn wait_for(&self, v: u64) {
        let mut cur = self.lock.lock().unwrap();
        while *cur < v {
            cur = self.cv.wait(cur).unwrap();
        }
    }
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or("pipeline".into());
    let think_us: u64 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);
    let serial = mode == "serial";

    let dir = tempfile::tempdir().unwrap();
    let target = CString::new(dir.path().to_str().unwrap()).unwrap();
    let mut resume = 0u64;
    let rc = unsafe { zou_wal_open(target.as_ptr(), 0x0100_0000, &mut resume) };
    assert_eq!(rc, 0, "open failed: {rc}");

    let flush = Arc::new(AtomicU64::new(0x0100_0000));
    let latch = Arc::new((Mutex::new(false), Condvar::new()));
    let gate = Arc::new(Gate {
        lock: Mutex::new(0),
        cv: Condvar::new(),
    });
    let stop = Arc::new(AtomicBool::new(false));

    // The pusher, one thread, the same loop shape as ZouWalPusherMain.
    let pusher = {
        let flush = Arc::clone(&flush);
        let latch = Arc::clone(&latch);
        let gate = Arc::clone(&gate);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut pushed = 0x0100_0000u64;
            let mut published = 0u64;
            let buf = vec![7u8; 1 << 20];
            loop {
                let f = flush.load(Ordering::Acquire);
                if stop.load(Ordering::Relaxed) && published >= f {
                    break;
                }
                if f > pushed {
                    let n = (f - pushed).min(1 << 20) as usize;
                    let mut d = 0u64;
                    let rc = unsafe { zou_wal_append(buf.as_ptr(), n, pushed, &mut d) };
                    assert_eq!(rc, 0);
                    pushed += n as u64;
                    if serial {
                        // The old pusher: nothing else happens until
                        // this chunk is durable.
                        loop {
                            let mut d = 0u64;
                            assert_eq!(unsafe { zou_wal_durable(&mut d) }, 0);
                            if d >= pushed {
                                break;
                            }
                            std::thread::sleep(Duration::from_micros(200));
                        }
                    }
                } else {
                    // WaitLatch with 1ms in flight, 5ms idle.
                    let (lock, cv) = &*latch;
                    let timeout = if published < pushed {
                        Duration::from_millis(1)
                    } else {
                        Duration::from_millis(5)
                    };
                    let set = lock.lock().unwrap();
                    let (mut set, _) = cv.wait_timeout(set, timeout).unwrap();
                    *set = false;
                }
                let mut d = 0u64;
                assert_eq!(unsafe { zou_wal_durable(&mut d) }, 0);
                if d > published {
                    published = d;
                    gate.publish(d);
                }
            }
        })
    };

    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..CLIENTS {
        let flush = Arc::clone(&flush);
        let latch = Arc::clone(&latch);
        let gate = Arc::clone(&gate);
        let stop = Arc::clone(&stop);
        handles.push(std::thread::spawn(move || {
            let mut lat_us = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_micros(think_us));
                let t0 = Instant::now();
                let lsn = flush.fetch_add(COMMIT_BYTES, Ordering::AcqRel) + COMMIT_BYTES;
                {
                    let (lock, cv) = &*latch;
                    *lock.lock().unwrap() = true;
                    cv.notify_one();
                }
                gate.wait_for(lsn);
                lat_us.push(t0.elapsed().as_micros() as u64);
            }
            lat_us
        }));
    }

    std::thread::sleep(Duration::from_secs(RUN_SECS));
    stop.store(true, Ordering::Relaxed);
    // The pusher drains everything flushed before it exits, then the
    // sentinel unsticks any committer still on the gate.
    let mut all: Vec<u64> = Vec::new();
    pusher.join().unwrap();
    gate.publish(u64::MAX);
    for h in handles {
        all.extend(h.join().unwrap());
    }
    all.sort_unstable();
    let n = all.len();
    let pick = |q: f64| all[((n as f64 * q) as usize).min(n - 1)] as f64 / 1000.0;
    println!(
        "mode {mode} think_us {think_us}: {} commits in {:.1}s, tps {:.0}, p50 {:.2} ms, p90 {:.2} ms, p99 {:.2} ms",
        n,
        start.elapsed().as_secs_f64(),
        n as f64 / start.elapsed().as_secs_f64(),
        pick(0.50),
        pick(0.90),
        pick(0.99),
    );
}
