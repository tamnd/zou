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
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak, mpsc};
use std::time::Duration;

use crate::dev::SUPERUSER;
use zou_pg::{bootstrap, restore};
use zou_server::Config;
use zou_server::attach::{Attached, Backend};
use zou_server::fleet::Doors;
use zou_server::tenant::{Registry, Routing};
use zou_store::layout::TenantLayout;
use zou_store::registry::Tenant;
use zou_store::{CasStore, Manifest, open_store};

pub const USAGE: &str = "usage: zou serve <target> [--http <n>] [--pg <n>] [--pool <n>] [--ops <n>] [--domain <suffix>] [--no-path-prefix] [--pg-bin <dir>] [--runtime <dir>] [--max-attached <n>] [--idle-secs <n>] [--shared-buffers <size>]";

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
}

pub fn parse(argv: &[String]) -> Result<Args, String> {
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
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
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
            other if target.is_none() && !other.starts_with('-') => {
                target = Some(other.to_string());
            }
            other => return Err(format!("unexpected argument {other:?}\n{USAGE}")),
        }
    }
    let target = target.ok_or(USAGE)?;
    if http == 0 {
        return Err("the http door is what this serves, --http 0 turns it off".to_string());
    }
    // A node with neither way of naming a tenant resolves every request
    // to nothing, which is a server that answers 404 forever, and the
    // time to say so is now rather than at the first request.
    if domains.is_empty() && !path_prefix {
        return Err("nothing would route: pass --domain or drop --no-path-prefix".to_string());
    }
    let pg_bin = pg_bin
        .or_else(|| std::env::var_os("ZOU_PG_BIN").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("build/pg/bin"));
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
    })
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

/// What the thread supervising a postmaster shares with everything
/// else: which children are meant to be running, and the attach manager
/// to tell when one of them is not.
struct State {
    live: Mutex<HashMap<String, Live>>,
    /// Weak because the attach manager owns the backend, so a strong
    /// one here would be a cycle that never frees. Empty until the
    /// server is built, which cannot happen before the backend exists.
    attached: Mutex<Weak<Attached>>,
}

impl State {
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

        if self.fresh(&tenant_ref)? {
            self.bootstrap(&tenant_ref, &pgdata, &pagecache)?;
        } else {
            let stats = restore::restore(&self.target, &tenant_ref, &pgdata)?;
            log::debug!(
                "{tenant_ref}: restored {} files, replayed {} wal records",
                stats.files,
                stats.wal_records
            );
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
        });

        match told.recv_timeout(START_TIMEOUT) {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                stop(pid);
                return Err(format!(
                    "postgres for {tenant_ref} was not ready within {}s",
                    START_TIMEOUT.as_secs()
                ));
            }
        }
        self.state
            .live
            .lock()
            .expect("the live map")
            .insert(tenant_ref.clone(), Live { pid, dir });

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
            tenant: Some(tenant_ref.clone()),
            external_url: self
                .domain
                .as_ref()
                .map(|domain| format!("https://{tenant_ref}.{domain}")),
            ..Config::default()
        })
    }

    fn down(&self, tenant_ref: &str) {
        // Nothing is waited for here. This is called from the attach
        // manager while it holds its own lock, and a postmaster's fast
        // shutdown is not something a request path should be inside
        // of: the thread that owns the child reaps it and removes its
        // directory.
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
        stop(live.pid);
    }
}

/// SIGINT, which is postgres' fast shutdown: roll back what is open,
/// do not wait for clients to leave.
fn stop(pid: u32) {
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGINT);
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

    let store: Arc<dyn CasStore> = Arc::from(open_store(&args.target)?);
    let registry = Arc::new(Registry::new(Arc::clone(&store)));
    let backend = Arc::new(Postmasters {
        target: args.target.clone(),
        pg_bin: args.pg_bin.clone(),
        runtime: args.runtime.clone(),
        shared_buffers: args.shared_buffers.clone(),
        domain: args.domains.first().cloned(),
        store,
        state: Arc::new(State {
            live: Mutex::new(HashMap::new()),
            attached: Mutex::new(Weak::new()),
        }),
    });
    let attached = Arc::new(
        Attached::new(backend.clone() as Arc<dyn Backend>)
            .with_budget(args.max_attached, args.idle),
    );
    backend.watch(Arc::downgrade(&attached));

    let http = bind(args.http, "http")?.ok_or("the http door needs a port")?;
    let pg = bind(args.pg, "the postgres port")?;
    let pool = bind(args.pool, "the pooler")?;
    let ops = bind(args.ops, "ops")?;
    log::info!("serving {} from {}", args.target, args.runtime.display());
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

    unsafe {
        let handler = on_signal as extern "C" fn(libc::c_int) as usize;
        libc::signal(libc::SIGINT, handler);
        libc::signal(libc::SIGTERM, handler);
    }

    let doors = Doors {
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
    // Every acked write is durable on the store by definition, so this
    // is not waiting for data to be safe. It is waiting for postmasters
    // to let go of their runtime directories before the tree they are
    // in goes away.
    for _ in 0..100 {
        if live.iter().all(|one| !one.dir.exists()) {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = fs::remove_dir_all(&args.runtime);
    log::info!("stopped, the store is at {}", args.target);
    Ok(())
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

    #[test]
    fn parse_rejects_noise() {
        assert!(parse(&argv(&[])).is_err());
        assert!(parse(&argv(&["./fleet", "--http"])).is_err());
        assert!(parse(&argv(&["./fleet", "--http", "0"])).is_err());
        assert!(parse(&argv(&["./fleet", "--pg", "eleven"])).is_err());
        assert!(parse(&argv(&["./fleet", "--idle-secs", "soon"])).is_err());
        assert!(parse(&argv(&["./fleet", "--max-attached", "lots"])).is_err());
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
}
