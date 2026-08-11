//! `zou serve <target>`: one node, every project on a store.
//!
//! `zou dev` serves the one database in a store. This serves whatever
//! is in the registry, which on a real deployment is a few hundred or a
//! few thousand projects that are mostly asleep. Nothing is up until a
//! request names it, and what is up is let go of again when nobody has
//! asked for it in a while, because a project nobody is using should
//! cost storage and nothing else.
//!
//! What this command owns is the machinery the attach manager takes as
//! a [`zou_server::attach::Backend`]: a postmaster per attached tenant,
//! its own runtime directory restored from that tenant's own prefix,
//! its own port on loopback, and its own socket directory. The policy,
//! which is the ceiling and the idle budget and which tenant to let go
//! of first, is not here. Neither is the routing. This starts databases
//! and stops them, and that is the whole of it.
//!
//! The doors are in zou-server and share one runtime, so a project
//! brought up by whichever door was asked first is the project the
//! others find already running.

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak, mpsc};
use std::time::Duration;

use crate::dev::SUPERUSER;
use zou_pg::gc::{self, Sweep};
use zou_pg::{bootstrap, install, restore, warm};
use zou_server::Config;
use zou_server::attach::{Attached, Backend};
use zou_server::fleet::Doors;
use zou_server::object::Passthrough;
use zou_server::tenant::{Registry, Routing};
use zou_store::layout::TenantLayout;
use zou_store::registry::Tenant;
use zou_store::{CasStore, Manifest, open_store};

pub const USAGE: &str = "usage: zou serve <target> [--ref <ref>] [--http <n>] [--pg <n>] [--pool <n>] [--ops <n>] [--domain <suffix>] [--no-path-prefix] [--pg-bin <dir>] [--runtime <dir>] [--max-attached <n>] [--idle-secs <n>] [--shared-buffers <size>] [--passthrough <size>] [--gc-every <duration>] [--gc-window <duration>] [--gc-retention <duration>]";

pub const LAMBDA_USAGE: &str = "usage: zou lambda <target> [--ref <ref>] [--pg-bin <dir>] [--runtime <dir>] [--shared-buffers <size>] [--passthrough <size>]";

/// How long a tenant's postmaster has to say it is accepting
/// connections. A cold attach is meant to be under half a second and
/// this is the point at which something is wrong rather than slow, so
/// it is generous and still bounded: a request waiting on an attach is
/// a request somebody is watching.
const START_TIMEOUT: Duration = Duration::from_secs(60);

/// shared_buffers for one tenant on a packed node.
///
/// `zou dev` gives its single database a quarter of the machine, which
/// is right when there is one. Here the ceiling on attached tenants
/// multiplies whatever this is, so it is small on purpose and the store
/// backed page cache is the tier that matters. A node running a few
/// large projects rather than a thousand small ones should raise it.
const SHARED_BUFFERS: &str = "16MB";

/// How long a passthrough url works for, when one is handed out.
///
/// A quarter of an hour is long enough to start a multi gigabyte
/// download on a bad line and short enough that a url which leaked into
/// a proxy log stops being a way in by the time anyone reads it.
const PASSTHROUGH_TTL: Duration = Duration::from_secs(900);

/// Connections one tenant's postmaster will take. The pooler's own
/// ceiling is twenty per project and role, so this is that plus room
/// for the http door's pool and a person with psql open.
const MAX_CONNECTIONS: u32 = 40;

/// Set by the signal handler, drained by the loop that waits.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Runtime directories are `<ref>-<n>`, never just `<ref>`, so that a
/// tenant which is detached and immediately attached again does not
/// share a directory with the postmaster that is still shutting down.
static ATTACHES: AtomicU64 = AtomicU64::new(0);

extern "C" fn on_signal(_sig: libc::c_int) {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

#[derive(Debug)]
pub struct Args {
    pub target: String,
    pub pg_bin: PathBuf,
    pub runtime: PathBuf,
    pub http: u16,
    pub pg: u16,
    pub pool: u16,
    pub ops: u16,
    pub domains: Vec<String>,
    pub path_prefix: bool,
    pub max_attached: usize,
    pub idle: Duration,
    pub shared_buffers: String,
    /// The smallest object this node hands out a store url for instead
    /// of serving. None, the default, and it serves every byte itself.
    pub passthrough: Option<u64>,
    /// How often this node sweeps the store, and under what policy.
    /// None, the default, and collecting is somebody else's cron entry
    /// running `zou gc`.
    pub gc: Option<Gc>,
    /// The one project this node serves. None is the registry, which is
    /// what a fleet node does. Set, and nothing is resolved out of a
    /// request at all, the project comes up before the doors open, and
    /// what is served is the url a hosted project serves with no prefix
    /// in front of it.
    pub only: Option<String>,
    /// Whether the door is the lambda runtime api instead of a port,
    /// which is `zou lambda` and nothing else.
    pub lambda: bool,
}

/// A node that collects on a timer.
#[derive(Debug, Clone, Copy)]
pub struct Gc {
    pub every: Duration,
    pub policy: gc::Policy,
}

pub fn parse(argv: &[String]) -> Result<Args, String> {
    parse_as(argv, false)
}

/// The same arguments a node takes, minus the ones a function has no
/// use for. A function has no listener, so a port on the command line
/// is a misunderstanding worth saying out loud rather than accepting
/// and ignoring.
pub fn parse_lambda(argv: &[String]) -> Result<Args, String> {
    parse_as(argv, true)
}

fn parse_as(argv: &[String], lambda: bool) -> Result<Args, String> {
    let usage = if lambda { LAMBDA_USAGE } else { USAGE };
    let mut target = None;
    let mut pg_bin = None;
    let mut runtime = None;
    let mut http = 54321u16;
    let mut pg = 5432u16;
    let mut pool = 6543u16;
    let mut ops = 0u16;
    let mut domains = Vec::new();
    let mut path_prefix = true;
    let mut max_attached = zou_server::attach::MAX_ATTACHED;
    let mut idle = zou_server::attach::IDLE;
    let mut shared_buffers = SHARED_BUFFERS.to_string();
    let mut passthrough = None;
    let mut gc_every = None;
    let mut policy = gc::Policy::default();
    let mut only = std::env::var("ZOU_REF").ok().filter(|r| !r.is_empty());
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        if lambda
            && matches!(
                arg.as_str(),
                "--http"
                    | "--pg"
                    | "--pool"
                    | "--ops"
                    | "--domain"
                    | "--no-path-prefix"
                    | "--max-attached"
                    | "--idle-secs"
                    | "--gc-every"
            )
        {
            return Err(format!(
                "{arg} is for a node with ports and a schedule, and a function has neither\n{usage}"
            ));
        }
        match arg.as_str() {
            "--ref" => only = Some(need(&mut it, "--ref")?.clone()),
            "--pg-bin" => pg_bin = Some(PathBuf::from(need(&mut it, "--pg-bin")?)),
            "--runtime" => runtime = Some(PathBuf::from(need(&mut it, "--runtime")?)),
            "--http" => http = port(&mut it, "--http")?,
            "--pg" => pg = port(&mut it, "--pg")?,
            "--pool" => pool = port(&mut it, "--pool")?,
            "--ops" => ops = port(&mut it, "--ops")?,
            "--domain" => domains.push(need(&mut it, "--domain")?.clone()),
            "--no-path-prefix" => path_prefix = false,
            "--max-attached" => {
                let raw = need(&mut it, "--max-attached")?;
                max_attached = raw
                    .parse()
                    .map_err(|_| format!("bad tenant ceiling {raw:?}"))?;
            }
            "--idle-secs" => {
                let raw = need(&mut it, "--idle-secs")?;
                let secs: u64 = raw.parse().map_err(|_| format!("bad idle {raw:?}"))?;
                idle = Duration::from_secs(secs);
            }
            "--shared-buffers" => shared_buffers = need(&mut it, "--shared-buffers")?.clone(),
            "--passthrough" => passthrough = Some(size(need(&mut it, "--passthrough")?)?),
            "--gc-every" => {
                gc_every = Some(Duration::from_secs(crate::gc::secs(need(
                    &mut it,
                    "--gc-every",
                )?)?));
            }
            "--gc-window" => policy.window_secs = crate::gc::secs(need(&mut it, "--gc-window")?)?,
            "--gc-retention" => {
                policy.retention_secs = crate::gc::secs(need(&mut it, "--gc-retention")?)?;
            }
            other if target.is_none() && !other.starts_with('-') => {
                target = Some(other.to_string());
            }
            other => return Err(format!("unexpected argument {other:?}\n{usage}")),
        }
    }
    // Env as well as argv, because the three places a function or a
    // container is configured from are environment variables, and an
    // image whose command has to be rewritten to point at a different
    // bucket is an image per bucket.
    let target = target
        .or_else(|| std::env::var("ZOU_TARGET").ok().filter(|t| !t.is_empty()))
        .ok_or(format!(
            "no store to serve: pass a target or set ZOU_TARGET\n{usage}"
        ))?;
    if let Some(one) = &only {
        zou_store::registry::check_ref(one).map_err(|e| format!("--ref {one:?}: {e}"))?;
    }
    // A function answers whatever url it is given at whatever hostname
    // it was deployed under, so there is nothing for a request to name
    // and the project has to be named here.
    if lambda && only.is_none() {
        return Err(format!(
            "a function serves one project: pass --ref or set ZOU_REF\n{usage}"
        ));
    }
    if !lambda {
        if http == 0 {
            return Err("the http door is what this serves, --http 0 turns it off".to_string());
        }
        // A node with neither way of naming a tenant resolves every
        // request to nothing, which is a server that answers 404
        // forever, and the time to say so is now rather than at the
        // first request. Unless it serves one project, where there is
        // nothing to resolve and that is the point.
        if only.is_none() && domains.is_empty() && !path_prefix {
            return Err("nothing would route: pass --domain or drop --no-path-prefix".to_string());
        }
        // Both would be a node that says a request is for one project
        // by its hostname and serves a different one, so the two ways
        // of deciding are not allowed to disagree.
        if only.is_some() && !domains.is_empty() {
            return Err(
                "--ref serves one project, so --domain has nothing left to name".to_string(),
            );
        }
    }
    // A retention window set on a node that never collects is a policy
    // nobody is applying, and finding that out from a full bucket is
    // worse than finding it out here.
    let gc = match gc_every {
        Some(every) if every.is_zero() => {
            return Err("--gc-every 0 would sweep without pause, leave it off instead".to_string());
        }
        Some(every) => Some(Gc { every, policy }),
        None if policy != gc::Policy::default() => {
            return Err("--gc-window and --gc-retention need --gc-every to run under".to_string());
        }
        None => None,
    };
    let pg_bin = install::pg_bin(pg_bin);
    let runtime = runtime
        .unwrap_or_else(|| std::env::temp_dir().join(format!("zou-serve-{}", std::process::id())));
    Ok(Args {
        target,
        pg_bin,
        runtime,
        http,
        pg,
        pool,
        ops,
        domains,
        path_prefix,
        max_attached,
        idle,
        shared_buffers,
        passthrough,
        gc,
        only,
        lambda,
    })
}

/// A number of bytes, written the way a person writes one: plain, or
/// with a K, M or G on it, powers of two because that is what every
/// other size in a postgres deployment means.
fn size(raw: &str) -> Result<u64, String> {
    // The B is optional, so 8M and 8MB are the same number and 8B is
    // eight bytes.
    let digits = raw.trim().trim_end_matches(['b', 'B']);
    let (digits, scale) = match digits.chars().last() {
        Some('k' | 'K') => (&digits[..digits.len() - 1], 1024u64),
        Some('m' | 'M') => (&digits[..digits.len() - 1], 1024 * 1024),
        Some('g' | 'G') => (&digits[..digits.len() - 1], 1024 * 1024 * 1024),
        _ => (digits, 1),
    };
    digits
        .trim()
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(scale))
        .ok_or_else(|| format!("bad size {raw:?}, write bytes or something like 8MB"))
}

fn need<'a>(it: &mut std::slice::Iter<'a, String>, flag: &str) -> Result<&'a String, String> {
    it.next().ok_or_else(|| format!("{flag} needs a value"))
}

/// A port, where zero is the way to say this door is not wanted.
fn port(it: &mut std::slice::Iter<'_, String>, flag: &str) -> Result<u16, String> {
    let raw = need(it, flag)?;
    raw.parse()
        .map_err(|_| format!("bad {flag} port {raw:?}, use 0 to turn it off"))
}

/// One tenant's postmaster, as much of it as anything outside the
/// thread that owns the child needs to know.
struct Live {
    pid: u32,
    dir: PathBuf,
}

/// A postmaster that has been asked to leave and has not gone yet.
///
/// Detaching does not wait for one: it is called by the attach manager
/// while that holds its own lock, so a request that evicted somebody
/// else's project would be waiting on a shutdown checkpoint it has
/// nothing to do with.
///
/// What must not happen is a second postmaster for the same project
/// starting while the first is still writing. Both would be putting
/// that tenant's pages into the same store prefix, and a database
/// restored out of the result has an index and a heap that disagree:
/// `create role anon` says the role is not there and then fails on the
/// unique index that says it is. So the wait is not skipped, it is
/// moved to the one place that needs it, which is the next attach of
/// that same project.
struct Dying {
    pid: u32,
    gone: Mutex<bool>,
    woken: Condvar,
}

/// How long a postmaster gets to leave before it is asked less politely.
///
/// A fast shutdown of a small project is tens of milliseconds, so this
/// is not a number anything normally reaches. Reaching it means a
/// backend is not responding to a fast shutdown, and immediate shutdown
/// is what postgres has for that: the next attach replays the wal,
/// which is the recovery path a crash takes anyway.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// What the thread supervising a postmaster shares with everything
/// else: which children are meant to be running, which are on their way
/// out, and the attach manager to tell when one of them is not.
struct State {
    live: Mutex<HashMap<String, Live>>,
    dying: Mutex<HashMap<String, Arc<Dying>>>,
    /// Weak because the attach manager owns the backend, so a strong
    /// one here would be a cycle that never frees. Empty until the
    /// server is built, which cannot happen before the backend exists.
    attached: Mutex<Weak<Attached>>,
}

impl State {
    /// Note that a postmaster has been asked to leave, so the next
    /// attach of this project knows there is something to wait for.
    fn leaving(&self, tenant_ref: &str, pid: u32) {
        self.dying.lock().expect("the dying map").insert(
            tenant_ref.to_string(),
            Arc::new(Dying {
                pid,
                gone: Mutex::new(false),
                woken: Condvar::new(),
            }),
        );
    }

    /// A postmaster has been reaped. Wake whoever is waiting to start
    /// its replacement.
    fn left(&self, tenant_ref: &str, pid: u32) {
        let dying = {
            let mut map = self.dying.lock().expect("the dying map");
            match map.get(tenant_ref) {
                Some(entry) if entry.pid == pid => map.remove(tenant_ref),
                _ => None,
            }
        };
        let Some(dying) = dying else { return };
        *dying.gone.lock().expect("a dying postmaster") = true;
        dying.woken.notify_all();
    }

    /// Wait for the last postmaster of this project to be gone.
    ///
    /// The error is deliberate rather than a timeout that carries on:
    /// an attach that could not be sure the previous postmaster is dead
    /// has to fail, because starting a second one is how a tenant's
    /// pages get written by two processes at once.
    fn wait_for_the_last_one(&self, tenant_ref: &str) -> Result<(), String> {
        let Some(dying) = self
            .dying
            .lock()
            .expect("the dying map")
            .get(tenant_ref)
            .cloned()
        else {
            return Ok(());
        };
        let mut gone = dying.gone.lock().expect("a dying postmaster");
        for asked_harder in [false, true] {
            if *gone {
                return Ok(());
            }
            let (next, waited) = dying
                .woken
                .wait_timeout(gone, SHUTDOWN_GRACE)
                .expect("a dying postmaster");
            gone = next;
            if *gone {
                return Ok(());
            }
            if waited.timed_out() && !asked_harder {
                log::warn!(
                    "{tenant_ref}: postmaster {} has not gone, asking for an immediate shutdown",
                    dying.pid
                );
                quit(dying.pid);
            }
        }
        if *gone {
            return Ok(());
        }
        Err(format!(
            "the previous postmaster for {tenant_ref} is still running, so this one is not started"
        ))
    }

    /// A postmaster left. If the map still names this exact process,
    /// nothing asked it to, so the tenant is let go of and the next
    /// request for it attaches again rather than being routed at a
    /// database that is not there.
    fn died(&self, tenant_ref: &str, pid: u32, why: String) {
        let ours = {
            let mut live = self.live.lock().expect("the live map");
            match live.get(tenant_ref) {
                Some(entry) if entry.pid == pid => live.remove(tenant_ref).is_some(),
                _ => false,
            }
        };
        if !ours {
            return;
        }
        log::warn!("{tenant_ref}: postmaster {why}, detaching");
        let Some(attached) = self.attached.lock().expect("the attach manager").upgrade() else {
            return;
        };
        let tenant_ref = tenant_ref.to_string();
        std::thread::spawn(move || {
            // detach is async and this thread is not, so it gets a
            // runtime of its own for the one call. It happens when a
            // database dies, which is rare enough to be worth nothing.
            let rt = match tokio::runtime::Builder::new_current_thread().build() {
                Ok(rt) => rt,
                Err(e) => return log::error!("detach {tenant_ref}: {e}"),
            };
            rt.block_on(attached.detach(&tenant_ref));
        });
    }
}

/// A postmaster per attached tenant.
struct Postmasters {
    target: String,
    pg_bin: PathBuf,
    runtime: PathBuf,
    shared_buffers: String,
    /// The serve domain a tenant's own url is built from, so that the
    /// links in its confirmation mail point at the project and not at
    /// this node. None and nothing is set, which is the honest answer
    /// for a node reached by path prefix, where the url depends on how
    /// the caller got here.
    domain: Option<String>,
    /// What every tenant this node brings up is told about handing out
    /// store urls instead of bytes. None and every one of them serves
    /// its own.
    passthrough: Option<Passthrough>,
    store: Arc<dyn CasStore>,
    state: Arc<State>,
}

impl Postmasters {
    fn watch(&self, attached: Weak<Attached>) {
        *self.state.attached.lock().expect("the attach manager") = attached;
    }

    /// Whether this tenant's prefix holds a database yet. A registered
    /// ref with nothing under it is a project somebody created and has
    /// not used, and the first request for it is what makes it real.
    fn fresh(&self, tenant_ref: &str) -> Result<bool, String> {
        let layout = TenantLayout::new(tenant_ref);
        match self
            .store
            .get(&layout.manifest())
            .map_err(|e| format!("store: {e}"))?
        {
            None => Ok(true),
            Some((data, _)) => Ok(Manifest::from_json(&data)
                .map_err(|e| format!("manifest: {e}"))?
                .checkpoints
                .is_empty()),
        }
    }

    fn bootstrap(
        &self,
        tenant_ref: &str,
        pgdata: &std::path::Path,
        pagecache: &std::path::Path,
    ) -> Result<(), String> {
        log::info!("{tenant_ref} has no database yet, running initdb");
        let out = Command::new(self.pg_bin.join("initdb"))
            .arg("-D")
            .arg(pgdata)
            // Named rather than left to the OS user, because the
            // cluster superuser is the role a project's own migrations
            // run as, and a Supabase project's is postgres wherever it
            // is hosted. Taking the OS user would make the owner of a
            // database depend on which account started the node.
            .args(["-U", SUPERUSER])
            .args(["--set", "io_method=sync"])
            .args(["--set", "full_page_writes=off"])
            .env("ZOU_TARGET", &self.target)
            .env("ZOU_TENANT", tenant_ref)
            .env("ZOU_PAGE_CACHE", pagecache)
            .env_remove("ZOU_PAGESERVE")
            .output()
            .map_err(|e| format!("initdb: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "initdb for {tenant_ref} failed:\n{}",
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        let control =
            fs::read(pgdata.join("global/pg_control")).map_err(|e| format!("pg_control: {e}"))?;
        let redo = restore::control_redo(&control)?;
        let layout = TenantLayout::new(tenant_ref);
        let stats = bootstrap::capture_genesis(&*self.store, &layout, pgdata, redo)?;
        log::info!(
            "{tenant_ref}: captured genesis, {} files, {} bytes",
            stats.files,
            stats.bytes
        );
        Ok(())
    }
}

impl Backend for Postmasters {
    fn up(&self, entry: &Tenant) -> Result<Config, String> {
        let tenant_ref = entry.tenant_ref.clone();
        let mut boot = crate::boot::Boot::now();
        // Before anything is read out of the store for this project,
        // and long before anything is started that would write to it.
        self.state.wait_for_the_last_one(&tenant_ref)?;
        let dir = self.runtime.join(format!(
            "{tenant_ref}-{}",
            ATTACHES.fetch_add(1, Ordering::Relaxed)
        ));
        let pgdata = dir.join("pgdata");
        let pagecache = dir.join("pagecache");
        let sock = dir.join("sock");
        fs::create_dir_all(&pagecache)
            .map_err(|e| format!("create {}: {e}", pagecache.display()))?;
        fs::create_dir_all(&sock).map_err(|e| format!("create {}: {e}", sock.display()))?;
        fs::set_permissions(&sock, fs::Permissions::from_mode(0o700))
            .map_err(|e| format!("chmod {}: {e}", sock.display()))?;

        boot.lap("wait");
        if self.fresh(&tenant_ref)? {
            self.bootstrap(&tenant_ref, &pgdata, &pagecache)?;
            boot.lap("initdb");
        } else {
            let stats = restore::restore(&self.target, &tenant_ref, &pgdata)?;
            log::debug!(
                "{tenant_ref}: restored {} files in {}, replayed {} wal records in {}",
                stats.files,
                crate::boot::ms(stats.skeleton_took),
                stats.wal_records,
                crate::boot::ms(stats.wal_took)
            );
            boot.lap("restore");
            warm_pages(&self.target, &tenant_ref, &pgdata, &pagecache, &stats);
            boot.lap("warm");
        }

        let port = free_port()?;
        let mut child = Command::new(self.pg_bin.join("postgres"))
            .arg("-D")
            .arg(&pgdata)
            .args(["-p", &port.to_string()])
            .arg("-k")
            .arg(&sock)
            .args(["-c", "listen_addresses=127.0.0.1"])
            .args(["-c", &format!("shared_buffers={}", self.shared_buffers)])
            .args(["-c", &format!("max_connections={MAX_CONNECTIONS}")])
            .env("ZOU_TARGET", &self.target)
            .env("ZOU_TENANT", &tenant_ref)
            .env("ZOU_PAGE_CACHE", &pagecache)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn postgres for {tenant_ref}: {e}"))?;
        let pid = child.id();
        let stderr = child.stderr.take().ok_or("no stderr pipe")?;
        boot.lap("spawn");

        // One thread per attached tenant, which is the cost of knowing
        // when one dies. It echoes the postmaster's log with the ref in
        // front of every line, because a thousand postmasters writing
        // to one stderr is otherwise unreadable, and it reaps the child
        // at the end, because a zombie per detach on a node that
        // detaches all day is a process table that fills up.
        let (ready, told) = mpsc::sync_channel::<Result<(), String>>(1);
        let state = Arc::clone(&self.state);
        let watched = tenant_ref.clone();
        let cleanup = dir.clone();
        std::thread::spawn(move || {
            let mut said = false;
            for line in BufReader::new(stderr).lines() {
                let Ok(line) = line else { break };
                log::info!("{watched}: {line}");
                if line.contains("ready to accept connections") && !said {
                    said = true;
                    let _ = ready.try_send(Ok(()));
                }
            }
            let status = child.wait();
            if !said {
                let _ = ready.try_send(Err(match &status {
                    Ok(status) => {
                        format!("postgres for {watched} exited ({status}) before it was ready")
                    }
                    Err(e) => format!("postgres for {watched}: {e}"),
                }));
            }
            let why = match status {
                Ok(status) => format!("exited ({status})"),
                Err(e) => format!("could not be waited for: {e}"),
            };
            state.died(&watched, pid, why);
            let _ = fs::remove_dir_all(&cleanup);
            // Last, because the next attach of this project starts as
            // soon as this says so, and it should not start while this
            // one's directory is still on disk.
            state.left(&watched, pid);
        });

        match told.recv_timeout(START_TIMEOUT) {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                // Noted as leaving before it is asked to, because it is
                // still running and the retry this failure causes must
                // not be the second postmaster on this project. Both
                // orders are wrong the other way round: a child that
                // dies between the signal and the note leaves a project
                // waiting for a process that is already gone.
                self.state.leaving(&tenant_ref, pid);
                stop(pid);
                return Err(format!(
                    "postgres for {tenant_ref} was not ready within {}s",
                    START_TIMEOUT.as_secs()
                ));
            }
        }
        boot.lap("recovery");
        self.state
            .live
            .lock()
            .expect("the live map")
            .insert(tenant_ref.clone(), Live { pid, dir });
        log::info!(
            "{tenant_ref}: attached in {}, {}",
            crate::boot::ms(boot.total()),
            boot.laps()
        );

        // Local connections are trust, and nothing but this node can
        // reach the port either way: it is on loopback in a 0700 socket
        // directory and the client of it is in this process.
        Ok(Config {
            jwt_secret: entry.jwt_secret.as_bytes().to_vec(),
            pg: Some(format!(
                "host=127.0.0.1 port={port} user={SUPERUSER} dbname=postgres"
            )),
            // Objects go where the pages go: the same store, the same
            // tenant prefix, under files/.
            objects: Some(self.target.clone()),
            passthrough: self.passthrough,
            tenant: Some(tenant_ref.clone()),
            external_url: self
                .domain
                .as_ref()
                .map(|domain| format!("https://{tenant_ref}.{domain}")),
            // The S3 pair is this tenant's own, out of the same entry
            // the secret came from, and a tenant without one has no S3
            // endpoint rather than a shared one. Nothing here reads an
            // environment variable for it on purpose: a pair in the
            // node's environment would be a pair every project on the
            // node answers to, which is a key that opens every door.
            s3: entry
                .s3()
                .map(|(access, secret)| zou_server::s3::Credentials::new(access, secret)),
            ..Config::default()
        })
    }

    fn down(&self, tenant_ref: &str) {
        // Nothing is waited for here. This is called from the attach
        // manager while it holds its own lock, and a postmaster's fast
        // shutdown is not something a request path should be inside
        // of: the thread that owns the child reaps it and removes its
        // directory. The next attach of this same project is what waits
        // for that, and it is the only thing that has to.
        let Some(live) = self
            .state
            .live
            .lock()
            .expect("the live map")
            .remove(tenant_ref)
        else {
            return;
        };
        log::info!("{tenant_ref}: detaching");
        self.state.leaving(tenant_ref, live.pid);
        stop(live.pid);
    }
}

/// Pull the pages recovery is about to fault into the page cache the
/// postmaster will be started with, between the restore and the spawn.
/// Nothing here is load bearing: a warm up that fails costs the round
/// trips it would have saved, so it logs and returns.
pub(crate) fn warm_pages(
    target: &str,
    tenant_ref: &str,
    pgdata: &std::path::Path,
    pagecache: &std::path::Path,
    stats: &restore::RestoreStats,
) {
    match warm::warm(target, tenant_ref, pgdata, pagecache, stats) {
        Ok(w) if w.wanted == 0 => {}
        Ok(w) => log::debug!(
            "{tenant_ref}: warmed {} of {} pages and {} fork sizes, {} bytes{}",
            w.fetched,
            w.wanted,
            w.forks,
            w.bytes,
            if w.capped {
                ", fault list truncated at ZOU_WARM_BLOCKS"
            } else {
                ""
            }
        ),
        Err(e) => log::debug!("{tenant_ref}: warm up skipped, {e}"),
    }
}

/// SIGINT, which is postgres' fast shutdown: roll back what is open,
/// do not wait for clients to leave.
fn stop(pid: u32) {
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGINT);
    }
}

/// SIGQUIT, which is postgres' immediate shutdown: every backend is
/// killed without a shutdown checkpoint and the next start replays the
/// wal. Asked for only when a fast shutdown did not happen, because the
/// alternative is holding a project hostage to one stuck backend.
fn quit(pid: u32) {
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGQUIT);
    }
}

/// A port nothing is listening on, by asking the kernel for one and
/// giving it straight back. Something else could take it in between,
/// and then the postmaster fails to bind and the attach fails and the
/// next request tries again with a different one, which is a better
/// trade than a range of ports this command has to be told about.
fn free_port() -> Result<u16, String> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|e| format!("looking for a free port: {e}"))?;
    listener
        .local_addr()
        .map(|addr| addr.port())
        .map_err(|e| format!("looking for a free port: {e}"))
}

fn bind(port: u16, what: &str) -> Result<Option<std::net::TcpListener>, String> {
    if port == 0 {
        return Ok(None);
    }
    std::net::TcpListener::bind(("0.0.0.0", port))
        .map(Some)
        .map_err(|e| format!("bind {what} on 0.0.0.0:{port}: {e}"))
}

pub fn run(args: &Args) -> Result<(), String> {
    // Running from the top of main, so what it ends up saying is what
    // a request arriving at a cold node waits for before it is even
    // routed, see boot.rs.
    let mut boot = crate::boot::Boot::from_entry();
    let postgres = args.pg_bin.join("postgres");
    if !postgres.is_file() {
        return Err(format!(
            "{} not found, point --pg-bin or ZOU_PG_BIN at a patched install",
            postgres.display()
        ));
    }
    fs::create_dir_all(&args.runtime)
        .map_err(|e| format!("create {}: {e}", args.runtime.display()))?;
    // Store op counters for the whole process tree, the same as the dev
    // loop, so `zou stats` says what a node's traffic cost in store
    // operations. set_var is safe here, no thread exists yet.
    if std::env::var_os("ZOU_STORE_STATS").is_none_or(|v| v.is_empty()) {
        let stats = args.runtime.join("store-stats");
        let _ = fs::remove_file(&stats);
        unsafe { std::env::set_var("ZOU_STORE_STATS", &stats) };
    }

    boot.lap("arguments");
    let store: Arc<dyn CasStore> = Arc::from(open_store(&args.target)?);
    boot.lap("store");
    let registry = Arc::new(Registry::new(Arc::clone(&store)));
    let backend = Arc::new(Postmasters {
        target: args.target.clone(),
        pg_bin: args.pg_bin.clone(),
        runtime: args.runtime.clone(),
        shared_buffers: args.shared_buffers.clone(),
        domain: args.domains.first().cloned(),
        passthrough: args.passthrough.map(|min_bytes| Passthrough {
            min_bytes,
            ttl: PASSTHROUGH_TTL,
        }),
        store,
        state: Arc::new(State {
            dying: Mutex::new(HashMap::new()),
            live: Mutex::new(HashMap::new()),
            attached: Mutex::new(Weak::new()),
        }),
    });
    let attached = Arc::new(
        Attached::new(backend.clone() as Arc<dyn Backend>)
            .with_budget(args.max_attached, args.idle),
    );
    backend.watch(Arc::downgrade(&attached));

    if args.lambda {
        // No listener to bind, and a freeze is not something this
        // process is told about: an environment that is done answering
        // is stopped where it stands and thawed for the next event.
        // Nothing is lost by that, because an acked write is durable on
        // the store before it is acked.
        //
        // The end of an environment is worth catching, though. A
        // postmaster killed outright leaves its writer lease held until
        // it expires, and the next cold start of the project waits the
        // rest of that out wherever in the world it happens. Lambda
        // sends SIGTERM before it destroys an environment when there is
        // an extension registered, Cloud Run and Fly always send one,
        // and this is what makes it mean something.
        unsafe {
            let handler = on_signal as extern "C" fn(libc::c_int) as usize;
            libc::signal(libc::SIGINT, handler);
            libc::signal(libc::SIGTERM, handler);
        }
        let stopping = Arc::clone(&backend);
        let tree = args.runtime.clone();
        std::thread::spawn(move || {
            while !SHUTDOWN.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(20));
            }
            stop_all(&stopping, &tree);
            // The loop this leaves behind is parked on the runtime
            // api's long poll for an invocation that is not coming, so
            // the way out is the process ending and not a return.
            std::process::exit(0);
        });
        boot.lap("runtime api");
        log::info!("up in {}, {}", crate::boot::ms(boot.total()), boot.laps());
        let only = args.only.clone().ok_or("a function serves one project")?;
        log::info!("serving {only} from {} as a function", args.target);
        let api = zou_server::lambda::Lambda::from_env()?;
        let served = zou_server::lambda::serve_blocking(&api, &only, registry, attached);
        // The loop only returns when the runtime api itself stopped
        // making sense, which is a bad way to end and still no reason to
        // leave a lease behind for the next environment to wait out.
        stop_all(&backend, &args.runtime);
        return served;
    }

    let http = bind(args.http, "http")?.ok_or("the http door needs a port")?;
    let pg = bind(args.pg, "the postgres port")?;
    let pool = bind(args.pool, "the pooler")?;
    let ops = bind(args.ops, "ops")?;
    // Listening is the moment a request stops being refused, so it is
    // the moment the node counts as up whatever else it does after.
    boot.lap("doors");
    log::info!("up in {}, {}", crate::boot::ms(boot.total()), boot.laps());
    log::info!("serving {} from {}", args.target, args.runtime.display());
    if let Some(only) = &args.only {
        log::info!("one project, {only}, at every url this node answers");
    }
    for domain in &args.domains {
        log::info!("http://<ref>.{domain}:{} names a project", args.http);
    }
    if args.path_prefix {
        log::info!("http://<host>:{}/<ref>/ names one too", args.http);
    }
    if args.pg > 0 {
        log::info!(
            "postgres on {}, transaction pooler on {}",
            args.pg,
            args.pool
        );
    }
    if args.ops > 0 {
        log::info!("metrics on http://0.0.0.0:{}/metrics", args.ops);
    }
    log::info!(
        "up to {} tenants attached at once, let go of after {}s idle",
        args.max_attached,
        args.idle.as_secs()
    );
    if let Some(gc) = args.gc {
        log::info!(
            "collecting every {}, {} of retention, {} of safety window",
            crate::gc::span(gc.every.as_secs()),
            crate::gc::span(gc.policy.retention_secs),
            crate::gc::span(gc.policy.window_secs)
        );
        let store = Arc::clone(&backend.store);
        std::thread::spawn(move || collect(&*store, gc));
    }

    unsafe {
        let handler = on_signal as extern "C" fn(libc::c_int) as usize;
        libc::signal(libc::SIGINT, handler);
        libc::signal(libc::SIGTERM, handler);
    }

    let doors = Doors {
        only: args.only.clone(),
        routing: Routing {
            domains: args.domains.clone(),
            path_prefix: args.path_prefix,
        },
        registry,
        attached: Arc::clone(&attached),
        http,
        pg,
        pool,
        ops,
        // A quarter of the idle budget, so a tenant is let go of within
        // a quarter of it of going quiet rather than up to twice as
        // long as asked for.
        sweep: (args.idle / 4).max(Duration::from_secs(1)),
    };
    std::thread::spawn(move || {
        if let Err(e) = doors.serve_blocking(env!("CARGO_PKG_VERSION")) {
            log::error!("{e}");
            SHUTDOWN.store(true, Ordering::SeqCst);
        }
    });

    while !SHUTDOWN.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(100));
    }
    stop_all(&backend, &args.runtime);
    log::info!("stopped, the store is at {}", args.target);
    Ok(())
}

/// Stop every attached postmaster and take the runtime tree with them.
///
/// Worth doing on the way out even though no data depends on it. An
/// acked write is durable on the store before it is acked, so nothing is
/// lost by a postmaster that is killed outright, but the writer lease it
/// was holding is the store's and stays held until it expires. Stopping
/// puts the lease back, and the project's next start, on this node or
/// any other, is a restore rather than a restore behind a timeout.
fn stop_all(backend: &Postmasters, runtime: &Path) {
    let live: Vec<Live> = backend
        .state
        .live
        .lock()
        .expect("the live map")
        .drain()
        .map(|(_, live)| live)
        .collect();
    log::info!("stopping {} attached tenants", live.len());
    for one in &live {
        stop(one.pid);
    }
    // Waiting for postmasters to let go of their runtime directories
    // before the tree they are in goes away, not for data to be safe.
    for _ in 0..100 {
        if live.iter().all(|one| !one.dir.exists()) {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = fs::remove_dir_all(runtime);
}

/// Sweep the store on a timer until the node is asked to stop.
///
/// The first sweep is one interval in rather than at start, because a
/// node that is being restarted in a loop should not walk the whole
/// store on every boot, and nothing about collecting is urgent.
///
/// Every node in a fleet can be told to do this. The lock in the store
/// is what makes that safe: whichever node gets there first sweeps, the
/// rest find it busy and go back to sleep, and no node has to be the
/// special one that owns the cron entry.
fn collect(store: &dyn CasStore, gc: Gc) {
    let holder = crate::gc::holder();
    let mut due = gc.every;
    while !SHUTDOWN.load(Ordering::SeqCst) {
        let tick = Duration::from_millis(200);
        std::thread::sleep(tick);
        due = due.saturating_sub(tick);
        if !due.is_zero() {
            continue;
        }
        due = gc.every;
        let now = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(since) => since.as_secs(),
            Err(_) => continue,
        };
        match gc::sweep(store, &holder, now, gc.policy) {
            Ok(Sweep::Ran(stats)) => log::info!(
                "gc: {} tenants, {} objects deleted, {} waiting out the window",
                stats.tenants,
                stats.deleted,
                stats.candidates
            ),
            Ok(Sweep::Busy { holder, until_unix }) => {
                log::debug!("gc: {holder} is sweeping until unix {until_unix}, leaving it to them");
            }
            Err(e) => log::warn!("gc: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_takes_the_target_and_defaults_the_doors() {
        let args = parse(&argv(&["s3://bucket/fleet"])).unwrap();
        assert_eq!(args.target, "s3://bucket/fleet");
        assert_eq!(args.http, 54321);
        assert_eq!(args.pg, 5432);
        assert_eq!(args.pool, 6543);
        assert_eq!(args.ops, 0, "nothing is scraped unless a port is asked for");
        assert!(
            args.path_prefix,
            "a node with no dns still has to be reachable"
        );
        assert!(args.domains.is_empty());
        assert_eq!(args.max_attached, zou_server::attach::MAX_ATTACHED);
        assert_eq!(
            args.passthrough, None,
            "a node serves its own bytes unless it was told not to"
        );
        assert!(
            args.gc.is_none(),
            "a node does not collect unless it was asked to"
        );
    }

    #[test]
    fn parse_honors_every_flag() {
        let args = parse(&argv(&[
            "./fleet",
            "--http",
            "8000",
            "--pg",
            "15432",
            "--pool",
            "16543",
            "--ops",
            "9187",
            "--domain",
            "zou.example",
            "--domain",
            "zou.dev",
            "--no-path-prefix",
            "--pg-bin",
            "/opt/pg/bin",
            "--runtime",
            "/tmp/run",
            "--max-attached",
            "64",
            "--idle-secs",
            "30",
            "--shared-buffers",
            "64MB",
            "--passthrough",
            "8MB",
            "--gc-every",
            "6h",
            "--gc-window",
            "2d",
            "--gc-retention",
            "30d",
        ]))
        .unwrap();
        assert_eq!(args.target, "./fleet");
        assert_eq!(args.http, 8000);
        assert_eq!(args.pg, 15432);
        assert_eq!(args.pool, 16543);
        assert_eq!(args.ops, 9187);
        assert_eq!(args.domains, vec!["zou.example", "zou.dev"]);
        assert!(!args.path_prefix);
        assert_eq!(args.pg_bin, PathBuf::from("/opt/pg/bin"));
        assert_eq!(args.runtime, PathBuf::from("/tmp/run"));
        assert_eq!(args.max_attached, 64);
        assert_eq!(args.idle, Duration::from_secs(30));
        assert_eq!(args.shared_buffers, "64MB");
        assert_eq!(args.passthrough, Some(8 * 1024 * 1024));
        let gc = args.gc.expect("a node that was told to collect");
        assert_eq!(gc.every, Duration::from_secs(6 * 60 * 60));
        assert_eq!(gc.policy.window_secs, 2 * 24 * 60 * 60);
        assert_eq!(gc.policy.retention_secs, 30 * 24 * 60 * 60);
        assert!(!gc.policy.dry_run, "a node that sweeps for real");
    }

    /// Retention written on a node that never sweeps is a promise
    /// nothing keeps, and a bucket that fills up is a slow way to find
    /// that out.
    #[test]
    fn a_retention_policy_with_nothing_to_apply_it_is_refused() {
        assert!(parse(&argv(&["./fleet", "--gc-retention", "30d"])).is_err());
        assert!(parse(&argv(&["./fleet", "--gc-window", "2d"])).is_err());
        assert!(parse(&argv(&["./fleet", "--gc-every", "0"])).is_err());
        assert!(parse(&argv(&["./fleet", "--gc-every", "soon"])).is_err());
        assert!(parse(&argv(&["./fleet", "--gc-every"])).is_err());
        let args = parse(&argv(&["./fleet", "--gc-every", "6h"])).unwrap();
        let gc = args.gc.expect("asked for");
        assert_eq!(
            (gc.policy.window_secs, gc.policy.retention_secs),
            (24 * 60 * 60, 7 * 24 * 60 * 60),
            "a day of window and a week of retention, the same as the command"
        );
    }

    /// Sizes the way people write them, and a refusal for the rest,
    /// because a node that read `8 megs` as eight bytes would redirect
    /// every download it has.
    #[test]
    fn sizes_are_read_with_or_without_their_units() {
        assert_eq!(size("1024"), Ok(1024));
        assert_eq!(size("8B"), Ok(8));
        assert_eq!(size(" 8k "), Ok(8 * 1024));
        assert_eq!(size("8KB"), Ok(8 * 1024));
        assert_eq!(size("8mb"), Ok(8 * 1024 * 1024));
        assert_eq!(size("2G"), Ok(2 * 1024 * 1024 * 1024));
        for bad in ["", "8 megs", "-1", "1.5MB", "MB", "18446744073709551615G"] {
            assert!(size(bad).is_err(), "{bad} should not parse");
        }
    }

    #[test]
    fn a_door_is_turned_off_with_zero() {
        let args = parse(&argv(&["./fleet", "--pg", "0", "--pool", "0"])).unwrap();
        assert_eq!((args.pg, args.pool), (0, 0));
        assert!(bind(0, "nothing").unwrap().is_none());
    }

    #[test]
    fn a_node_that_could_not_route_is_refused_at_the_command_line() {
        let stop = parse(&argv(&["./fleet", "--no-path-prefix"])).unwrap_err();
        assert!(stop.contains("--domain"), "{stop}");
        // With a domain it is a server again.
        assert!(
            parse(&argv(&[
                "./fleet",
                "--no-path-prefix",
                "--domain",
                "zou.example"
            ]))
            .is_ok()
        );
    }

    /// One project on a node is the deployment a container platform
    /// gives you: one url, one database, no registry to route through.
    #[test]
    fn one_project_needs_nothing_to_route_by() {
        let args = parse(&argv(&[
            "./fleet",
            "--ref",
            "acme-prod",
            "--no-path-prefix",
        ]))
        .unwrap();
        assert_eq!(args.only.as_deref(), Some("acme-prod"));
        assert!(!args.lambda);
        // And the path prefix left on is not a contradiction, it is a
        // flag that has nothing to do: nothing is parsed either way.
        let args = parse(&argv(&["./fleet", "--ref", "acme-prod"])).unwrap();
        assert_eq!(args.only.as_deref(), Some("acme-prod"));
    }

    #[test]
    fn one_project_and_a_domain_would_disagree() {
        let stop = parse(&argv(&[
            "./fleet",
            "--ref",
            "acme-prod",
            "--domain",
            "zou.example",
        ]))
        .unwrap_err();
        assert!(stop.contains("--domain"), "{stop}");
    }

    #[test]
    fn a_ref_that_is_not_a_ref_is_refused_here() {
        let stop = parse(&argv(&["./fleet", "--ref", "Acme Prod"])).unwrap_err();
        assert!(stop.contains("Acme Prod"), "{stop}");
    }

    #[test]
    fn a_function_serves_one_project_and_has_no_ports() {
        let args = parse_lambda(&argv(&["s3://bucket/fleet", "--ref", "acme-prod"])).unwrap();
        assert!(args.lambda);
        assert_eq!(args.only.as_deref(), Some("acme-prod"));
        let stop = parse_lambda(&argv(&["s3://bucket/fleet"])).unwrap_err();
        assert!(stop.contains("--ref"), "{stop}");
        for flag in [
            "--http",
            "--pg",
            "--pool",
            "--ops",
            "--domain",
            "--max-attached",
            "--idle-secs",
            "--gc-every",
        ] {
            let stop = parse_lambda(&argv(&[
                "s3://bucket/fleet",
                "--ref",
                "acme-prod",
                flag,
                "1",
            ]))
            .unwrap_err();
            assert!(stop.contains(flag), "{flag}: {stop}");
        }
    }

    #[test]
    fn parse_rejects_noise() {
        assert!(parse(&argv(&[])).is_err());
        assert!(parse(&argv(&["./fleet", "--http"])).is_err());
        assert!(parse(&argv(&["./fleet", "--http", "0"])).is_err());
        assert!(parse(&argv(&["./fleet", "--pg", "eleven"])).is_err());
        assert!(parse(&argv(&["./fleet", "--idle-secs", "soon"])).is_err());
        assert!(parse(&argv(&["./fleet", "--max-attached", "lots"])).is_err());
        assert!(parse(&argv(&["./fleet", "--passthrough", "big"])).is_err());
        assert!(parse(&argv(&["./fleet", "extra"])).is_err());
        assert!(parse(&argv(&["--bogus", "./fleet"])).is_err());
    }

    /// A dead postmaster whose tenant is still in the map is a tenant
    /// nothing would ever fix, so the supervisor lets go of it. One
    /// that was asked to stop is not, because something already did.
    #[test]
    fn only_an_unexpected_death_detaches() {
        let state = State {
            live: Mutex::new(HashMap::new()),
            dying: Mutex::new(HashMap::new()),
            attached: Mutex::new(Weak::new()),
        };
        state.live.lock().unwrap().insert(
            "acme-prod".to_string(),
            Live {
                pid: 4242,
                dir: PathBuf::from("/tmp/none"),
            },
        );
        // A pid the map does not name is a postmaster something already
        // replaced, so the entry stays.
        state.died("acme-prod", 1, "exited".to_string());
        assert!(state.live.lock().unwrap().contains_key("acme-prod"));

        state.died("acme-prod", 4242, "exited".to_string());
        assert!(
            !state.live.lock().unwrap().contains_key("acme-prod"),
            "a postmaster that died on its own is not still attached"
        );
        // And a tenant nothing knows about is not an error.
        state.died("gone", 4242, "exited".to_string());
    }

    fn empty_state() -> Arc<State> {
        Arc::new(State {
            live: Mutex::new(HashMap::new()),
            dying: Mutex::new(HashMap::new()),
            attached: Mutex::new(Weak::new()),
        })
    }

    /// The rule the store depends on: one postmaster per project at a
    /// time. Two of them write the same tenant's pages into the same
    /// prefix, and what is restored out of that has an index and a heap
    /// that disagree.
    #[test]
    fn an_attach_waits_for_the_postmaster_it_is_replacing() {
        let state = empty_state();
        state.leaving("acme-prod", 4242);

        let waiter = Arc::clone(&state);
        let handle = std::thread::spawn(move || waiter.wait_for_the_last_one("acme-prod"));
        // Long enough to be sure the waiter is inside the wait rather
        // than having missed the notify, short enough not to matter.
        std::thread::sleep(Duration::from_millis(50));
        assert!(!handle.is_finished(), "the attach did not wait");

        state.left("acme-prod", 4242);
        handle.join().expect("the waiter").expect("it was let go");
    }

    /// Nothing is waited for when nothing is on its way out, which is
    /// every attach on a node that is not churning.
    #[test]
    fn an_attach_of_a_project_nothing_is_leaving_waits_for_nothing() {
        let state = empty_state();
        state
            .wait_for_the_last_one("acme-prod")
            .expect("no wait at all");

        // A different project leaving is not this project's business.
        state.leaving("other", 1);
        state
            .wait_for_the_last_one("acme-prod")
            .expect("no wait at all");
    }

    /// A postmaster that was replaced before it was reaped does not
    /// clear the one that replaced it, the same rule `died` follows.
    #[test]
    fn only_the_postmaster_that_was_asked_to_leave_clears_the_way() {
        let state = empty_state();
        state.leaving("acme-prod", 4242);
        state.left("acme-prod", 1);
        assert!(
            state.dying.lock().unwrap().contains_key("acme-prod"),
            "another pid's exit cleared the wait"
        );
        state.left("acme-prod", 4242);
        assert!(state.dying.lock().unwrap().is_empty());
    }
}
