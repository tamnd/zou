//! `zou dev <target>`: attach a store and serve it through a supervised
//! postmaster.
//!
//! The store is the only durable state. A fresh target gets initdb plus
//! a genesis capture, an existing one is restored into a throwaway
//! runtime directory. Then the patched postmaster runs as a child on
//! 127.0.0.1 plus a unix socket in a private directory, gets restarted
//! if it dies, and is shut down fast on SIGINT or SIGTERM. The spike
//! behind this choreography is scripts/zou-spike-embed.sh and the
//! decision it fed is in docs/architecture.md.

use std::fs;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use zou_pg::{bootstrap, restore};
use zou_store::layout::TenantLayout;
use zou_store::{CasStore, Manifest, open_store};

/// How many times in a row the postmaster may die before its first
/// accepted connection until we stop retrying. A crash after it was up
/// resets the count, that is the recover-and-continue path.
const MAX_FAILED_STARTS: u32 = 3;

/// Set by the signal handler, drained by the supervision loop.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_sig: libc::c_int) {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

pub struct Args {
    pub target: String,
    pub pg_bin: PathBuf,
    pub port: u16,
    pub runtime: PathBuf,
}

use crate::DEV_USAGE as USAGE;

pub fn parse(argv: &[String]) -> Result<Args, String> {
    let mut target = None;
    let mut pg_bin = None;
    let mut port = 5432u16;
    let mut runtime = None;
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--pg-bin" => pg_bin = Some(PathBuf::from(need(&mut it, "--pg-bin")?)),
            "--port" => {
                let raw = need(&mut it, "--port")?;
                port = raw.parse().map_err(|_| format!("bad port {raw:?}"))?;
            }
            "--runtime" => runtime = Some(PathBuf::from(need(&mut it, "--runtime")?)),
            other if target.is_none() && !other.starts_with('-') => {
                target = Some(other.to_string());
            }
            other => return Err(format!("unexpected argument {other:?}\n{USAGE}")),
        }
    }
    let target = target.ok_or(USAGE)?;
    let pg_bin = pg_bin
        .or_else(|| std::env::var_os("ZOU_PG_BIN").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("build/pg/bin"));
    let runtime = runtime
        .unwrap_or_else(|| std::env::temp_dir().join(format!("zou-dev-{}", std::process::id())));
    Ok(Args {
        target,
        pg_bin,
        port,
        runtime,
    })
}

fn need<'a>(it: &mut std::slice::Iter<'a, String>, flag: &str) -> Result<&'a String, String> {
    it.next().ok_or_else(|| format!("{flag} needs a value"))
}

/// shared_buffers for this machine: a quarter of physical RAM, the
/// stock initdb 128M starves any working set bigger than toy scale and
/// turns every eviction into a store round trip. Passed on the
/// postgres command line rather than baked into postgresql.conf at
/// initdb time, so a store initialized on a laptop still sizes to the
/// server that later attaches it. ZOU_SHARED_BUFFERS overrides, any
/// value postgres accepts.
fn shared_buffers() -> String {
    if let Ok(v) = std::env::var("ZOU_SHARED_BUFFERS")
        && !v.is_empty()
    {
        return v;
    }
    let bytes = unsafe {
        let pages = libc::sysconf(libc::_SC_PHYS_PAGES);
        let size = libc::sysconf(libc::_SC_PAGE_SIZE);
        if pages > 0 && size > 0 {
            pages as u64 * size as u64
        } else {
            0
        }
    };
    let mb = ((bytes / 4) >> 20).max(128);
    format!("{mb}MB")
}

pub fn run(args: &Args) -> Result<(), String> {
    let postgres = args.pg_bin.join("postgres");
    if !postgres.is_file() {
        return Err(format!(
            "{} not found, point --pg-bin or ZOU_PG_BIN at a patched install",
            postgres.display()
        ));
    }

    fs::create_dir_all(&args.runtime)
        .map_err(|e| format!("create {}: {e}", args.runtime.display()))?;
    let pgdata = args.runtime.join("pgdata");
    // The write-through page cache starts empty on every boot. It only
    // ever mirrors what this instance wrote to or read from the store,
    // and wiping it is what keeps a cache from a previous life from
    // answering for a store some other node has advanced since.
    let pagecache = args.runtime.join("pagecache");
    let _ = fs::remove_dir_all(&pagecache);
    fs::create_dir_all(&pagecache).map_err(|e| format!("create {}: {e}", pagecache.display()))?;
    let sock = args.runtime.join("sock");
    fs::create_dir_all(&sock).map_err(|e| format!("create {}: {e}", sock.display()))?;
    fs::set_permissions(&sock, fs::Permissions::from_mode(0o700))
        .map_err(|e| format!("chmod {}: {e}", sock.display()))?;
    log::info!("runtime directory {}", args.runtime.display());

    let store: Arc<dyn CasStore> = Arc::from(open_store(&args.target)?);
    let layout = TenantLayout::new("local");
    let fresh = match store
        .get(&layout.manifest())
        .map_err(|e| format!("store: {e}"))?
    {
        None => true,
        Some((data, _)) => Manifest::from_json(&data)
            .map_err(|e| format!("manifest: {e}"))?
            .checkpoints
            .is_empty(),
    };
    if fresh {
        log::info!("{} is empty, running initdb", args.target);
        let out = Command::new(args.pg_bin.join("initdb"))
            .arg("-D")
            .arg(&pgdata)
            .args(["--set", "io_method=sync"])
            .env("ZOU_TARGET", &args.target)
            .env("ZOU_PAGE_CACHE", &pagecache)
            .output()
            .map_err(|e| format!("initdb: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "initdb failed:\n{}",
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        let control = fs::read(pgdata.join("global/pg_control"))
            .map_err(|e| format!("read pg_control: {e}"))?;
        let redo = restore::control_redo(&control)?;
        let stats = bootstrap::capture_genesis(&*store, &layout, &pgdata, redo)?;
        log::info!(
            "captured genesis, {} files, {} bytes, redo {redo:#X}",
            stats.files,
            stats.bytes
        );
    } else {
        let stats = restore::restore(&args.target, "local", &pgdata)?;
        log::info!(
            "restored {} files and replayed {} wal records from {}",
            stats.files,
            stats.wal_records,
            args.target
        );
    }

    unsafe {
        let handler = on_signal as extern "C" fn(libc::c_int) as usize;
        libc::signal(libc::SIGINT, handler);
        libc::signal(libc::SIGTERM, handler);
    }

    let mut failed_starts = 0u32;
    loop {
        let ready = Arc::new(AtomicBool::new(false));
        let mut child = Command::new(&postgres)
            .arg("-D")
            .arg(&pgdata)
            .args(["-p", &args.port.to_string()])
            .arg("-k")
            .arg(&sock)
            .args(["-c", "listen_addresses=127.0.0.1"])
            .args(["-c", &format!("shared_buffers={}", shared_buffers())])
            .env("ZOU_TARGET", &args.target)
            .env("ZOU_PAGE_CACHE", &pagecache)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn {}: {e}", postgres.display()))?;

        // Postgres logs to stderr with the collector off. Echo every
        // line and flip the ready flag on the postmaster's own signal,
        // which doubles as the connection banner.
        let echo = {
            let stderr = child.stderr.take().ok_or("no stderr pipe")?;
            let ready = Arc::clone(&ready);
            let port = args.port;
            let sock = sock.clone();
            std::thread::spawn(move || {
                for line in BufReader::new(stderr).lines() {
                    let Ok(line) = line else { break };
                    eprintln!("{line}");
                    if line.contains("ready to accept connections")
                        && !ready.swap(true, Ordering::SeqCst)
                    {
                        log::info!(
                            "postgres ready on 127.0.0.1:{port} and socket {}",
                            sock.display()
                        );
                        log::info!("try psql -h 127.0.0.1 -p {port} -d postgres");
                    }
                }
            })
        };

        // SIGINT is forwarded even when the terminal already delivered
        // it to the whole process group, a repeat during fast shutdown
        // is harmless and a plain kill on our pid alone needs it.
        let mut forwarded = false;
        let status = loop {
            if SHUTDOWN.load(Ordering::SeqCst) && !forwarded {
                unsafe {
                    libc::kill(child.id() as libc::pid_t, libc::SIGINT);
                }
                forwarded = true;
            }
            match child.try_wait().map_err(|e| format!("wait: {e}"))? {
                Some(status) => break status,
                None => std::thread::sleep(Duration::from_millis(100)),
            }
        };
        let _ = echo.join();

        if SHUTDOWN.load(Ordering::SeqCst) {
            log::info!("postmaster stopped, store is at {}", args.target);
            return Ok(());
        }
        if status.success() {
            log::info!("postmaster exited cleanly on its own");
            return Ok(());
        }
        if ready.load(Ordering::SeqCst) {
            failed_starts = 0;
        } else {
            failed_starts += 1;
            if failed_starts >= MAX_FAILED_STARTS {
                return Err(format!(
                    "postmaster failed to start {failed_starts} times in a row, giving up"
                ));
            }
        }
        log::warn!("postmaster died ({status}), restarting");
        std::thread::sleep(Duration::from_secs(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_takes_the_target_and_defaults_the_rest() {
        let args = parse(&argv(&["./data"])).unwrap();
        assert_eq!(args.target, "./data");
        assert_eq!(args.port, 5432);
        assert_eq!(args.pg_bin, PathBuf::from("build/pg/bin"));
    }

    #[test]
    fn parse_honors_every_flag() {
        let args = parse(&argv(&[
            "s3://bucket/x",
            "--pg-bin",
            "/opt/pg/bin",
            "--port",
            "5614",
            "--runtime",
            "/tmp/run",
        ]))
        .unwrap();
        assert_eq!(args.target, "s3://bucket/x");
        assert_eq!(args.pg_bin, PathBuf::from("/opt/pg/bin"));
        assert_eq!(args.port, 5614);
        assert_eq!(args.runtime, PathBuf::from("/tmp/run"));
    }

    #[test]
    fn parse_rejects_noise() {
        assert!(parse(&argv(&[])).is_err());
        assert!(parse(&argv(&["./data", "--port"])).is_err());
        assert!(parse(&argv(&["./data", "--port", "hot"])).is_err());
        assert!(parse(&argv(&["./data", "extra"])).is_err());
        assert!(parse(&argv(&["--bogus", "./data"])).is_err());
    }
}
