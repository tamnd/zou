//! Zou inside somebody else's process.
//!
//! `zou dev` and `zou serve` are this library with a command line on
//! them: a store, a postmaster over it, and the front door as one axum
//! router. What a host process wants is the same thing without the
//! sockets, so this hands back a handle that answers a request by
//! calling the router directly. [`Zou`] is that handle.
//!
//! Three things are worth knowing before building on it.
//!
//! A request is answered in this process. There is no socket, no
//! serialization of the request line, and no port to be firewalled; the
//! router is called and its response is read back. `listen` puts the
//! same router on a port as well, for the case where another process,
//! or a browser, wants in on the project this one is holding.
//!
//! Postgres is a child process, not a library. It has to be the patched
//! build, `pg_bin` or `ZOU_PG_BIN` naming it, and it is started when the
//! handle opens and stopped when the handle closes. That is the cost of
//! the first `open` and it is seconds, not milliseconds, on a store
//! nobody has run initdb against yet.
//!
//! A handle is `Send` and `Sync` and requests may be issued from any
//! thread at once. What is not allowed is calling `request` from inside
//! an async runtime's own thread, because it blocks on this handle's
//! runtime to get the answer.
//!
//! Unix only for now, since the postmaster is supervised with signals
//! and the socket directory with unix permissions. On Windows this
//! crate is an empty library rather than a build failure, so a
//! workspace that carries it still builds there.

#![cfg(unix)]

use std::fs;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt;
use zou_pg::{bootstrap, restore};
use zou_store::layout::TenantLayout;
use zou_store::{Manifest, open_store};

mod template;

/// The role initdb makes and everything else here connects as, which is
/// the one `zou dev` and `zou serve` use so a store made by one is a
/// store the others can open.
const SUPERUSER: &str = "postgres";

/// The tenant an unnamed database lives under, the same name the dev
/// loop gives it.
pub const LOCAL: &str = "local";

/// How long the postmaster has to say it is accepting connections.
const START_TIMEOUT: Duration = Duration::from_secs(60);

/// How long it gets to leave before it is asked less politely.
const STOP_GRACE: Duration = Duration::from_secs(5);

/// How long a branch waits for the capture of its parent's checkpoint.
/// It is a tenth of a second against a local store and the wait is
/// there for the case where it is not, so this is long enough to be a
/// real failure rather than a slow bucket having a bad minute.
const FOLD_WAIT: Duration = Duration::from_secs(30);

/// Runtime directories are numbered, because a process may hold several
/// handles at once and a branch opens a second one while the first is
/// still up.
static OPENED: AtomicU64 = AtomicU64::new(0);

/// What went wrong, in the shape a C caller can carry: a code and a
/// sentence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// The options do not describe anything openable.
    Options,
    /// Postgres would not start, would not stop, or would not answer.
    Postgres,
    /// The store said no.
    Store,
    /// The request could not be built or its answer could not be read.
    Request,
    /// The filesystem or the network under all of it.
    Io,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct Error {
    pub kind: Kind,
    pub message: String,
}

impl Error {
    fn new(kind: Kind, message: impl Into<String>) -> Error {
        Error {
            kind,
            message: message.into(),
        }
    }
}

fn io(what: impl std::fmt::Display) -> impl FnOnce(std::io::Error) -> Error {
    move |e| Error::new(Kind::Io, format!("{what}: {e}"))
}

/// A postgres failure, said in full.
///
/// tokio_postgres prints "db error" and keeps what actually happened
/// one link down the source chain, which is no use to a caller holding
/// one string and no way to walk it.
fn pg(what: impl std::fmt::Display) -> impl FnOnce(tokio_postgres::Error) -> Error {
    move |e| {
        let mut said = format!("{what}: {e}");
        let mut cause: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(&e);
        while let Some(next) = cause {
            said.push_str(&format!(": {next}"));
            cause = next.source();
        }
        Error::new(Kind::Postgres, said)
    }
}

/// What to open and how.
#[derive(Debug, Clone)]
pub struct Options {
    /// The store: a directory, an `s3://` url, a `sqlite://` file, or a
    /// `.zou` file. Empty means ephemeral, a directory of its own that
    /// goes away with the handle.
    pub target: String,
    /// The database inside it. `local` unless a branch is being opened.
    pub tenant: String,
    /// The patched postgres install. `ZOU_PG_BIN` when this is empty,
    /// and `build/pg/bin` when that is unset too.
    pub pg_bin: PathBuf,
    /// Where the running copy lives while the handle is open. A
    /// directory of this handle's own under the temporary directory
    /// when it is None, removed on close either way.
    pub runtime: Option<PathBuf>,
    /// The secret every token is signed with. A fresh random one when
    /// this is None, which is right for a test and wrong for anything
    /// that has to still recognise its own tokens after a restart.
    pub jwt_secret: Option<String>,
    /// The schemas the rest surface serves, first one the default.
    /// Empty for the server's own list.
    pub schemas: Vec<String>,
    /// The role a request that carries no key of its own runs as.
    /// Empty for `anon`, which is what a Supabase project calls it.
    pub anon_role: String,
    /// shared_buffers for the child postmaster.
    pub shared_buffers: Option<String>,
    /// Cut this database out of the machine's template instead of
    /// running initdb for it, which is what makes a create milliseconds
    /// rather than seconds. It lives on the shared template store under
    /// a tenant of its own and goes away with the handle, so it names
    /// no target and keeps nothing.
    pub fixture: bool,
    /// The pair the S3 protocol surface is asked with. None and that
    /// surface tells every request the key it named is not one this
    /// project has, which is the honest answer for a project that was
    /// never given a pair.
    pub s3: Option<S3Keys>,
}

/// The key pair an S3 client signs with, and where the project says it
/// is.
///
/// Nothing to do with the credentials a store on a bucket reads out of
/// the environment. Those are how zou reaches somebody else's S3, and
/// this is how somebody else's S3 client reaches zou.
#[derive(Debug, Clone)]
pub struct S3Keys {
    pub access: String,
    pub secret: String,
    /// What a bucket answers when asked where it is, and half of what a
    /// signature is checked against. `us-east-1` when it is empty,
    /// where every client that was never told assumes it is.
    pub region: String,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            target: String::new(),
            tenant: LOCAL.to_string(),
            pg_bin: PathBuf::new(),
            runtime: None,
            jwt_secret: None,
            schemas: Vec::new(),
            anon_role: String::new(),
            shared_buffers: None,
            fixture: false,
            s3: None,
        }
    }
}

impl Options {
    /// A store that exists only while the handle does. Everything it
    /// writes lands in a directory that is removed on close, which is
    /// what a test suite wants and what nothing else does.
    pub fn ephemeral() -> Options {
        Options::default()
    }

    /// The same thing, cut out of the machine's template.
    ///
    /// An ephemeral handle runs initdb against its new store, which is
    /// seconds: 5 on a fast runner, 30 on a laptop, and it is the whole
    /// cost of opening one. A fixture branches a database that already
    /// exists instead, so what is left is the postmaster start.
    ///
    /// The template is built once per machine and per postgres build,
    /// cached under `ZOU_TEMPLATE_CACHE` or `~/.cache/zou/templates`,
    /// and the first caller on a cold machine pays for it. Fixtures are
    /// tenants on it, they see nothing of each other, and closing one
    /// takes it off the store.
    pub fn fixture() -> Options {
        Options {
            fixture: true,
            ..Options::default()
        }
    }

    /// A store on disk, kept.
    pub fn dir(path: impl AsRef<Path>) -> Options {
        Options {
            target: path.as_ref().display().to_string(),
            ..Options::default()
        }
    }

    /// A store named by a url, which is how a bucket is named.
    pub fn url(target: impl Into<String>) -> Options {
        Options {
            target: target.into(),
            ..Options::default()
        }
    }

    /// The database inside the store, for opening a branch directly.
    pub fn tenant(mut self, tenant: impl Into<String>) -> Options {
        self.tenant = tenant.into();
        self
    }

    /// Keys that survive a restart, because the host pinned the secret.
    pub fn jwt_secret(mut self, secret: impl Into<String>) -> Options {
        self.jwt_secret = Some(secret.into());
        self
    }

    /// A pair for the S3 protocol surface, which answers nothing
    /// without one.
    pub fn s3(mut self, access: impl Into<String>, secret: impl Into<String>) -> Options {
        self.s3 = Some(S3Keys {
            access: access.into(),
            secret: secret.into(),
            region: String::new(),
        });
        self
    }
}

/// The two keys a client needs, the same pair `zou status` prints.
#[derive(Debug, Clone)]
pub struct Keys {
    pub anon: String,
    pub service_role: String,
}

/// What the router answered.
#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    /// The first value of a header, since almost every caller wants one
    /// and matching case is not their job.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// The child postmaster, and what it takes to be sure it is gone.
struct Postmaster {
    pid: u32,
    child: Mutex<Option<Child>>,
}

impl Postmaster {
    /// Fast shutdown, then immediate shutdown, then the signal nothing
    /// catches. A postmaster that will not leave must not outlive the
    /// handle: its pages and the store's would be written by a process
    /// nobody is holding.
    fn stop(&self) -> Result<(), Error> {
        let Some(mut child) = self.child.lock().expect("the postmaster").take() else {
            return Ok(());
        };
        for (signal, wait) in [
            (libc::SIGINT, STOP_GRACE),
            (libc::SIGQUIT, STOP_GRACE),
            (libc::SIGKILL, STOP_GRACE),
        ] {
            unsafe { libc::kill(self.pid as libc::pid_t, signal) };
            let deadline = std::time::Instant::now() + wait;
            while std::time::Instant::now() < deadline {
                match child.try_wait() {
                    Ok(Some(_)) => return Ok(()),
                    Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                    Err(e) => return Err(Error::new(Kind::Postgres, format!("wait: {e}"))),
                }
            }
        }
        Err(Error::new(
            Kind::Postgres,
            format!("postmaster {} would not stop", self.pid),
        ))
    }
}

/// One open project: a store, the postgres over it, and the front door.
///
/// ```no_run
/// use zou_embed::{Options, Zou};
///
/// let zou = Zou::open(Options::ephemeral()).unwrap();
/// let key = zou.keys().anon.clone();
/// let answer = zou
///     .request("GET", "/rest/v1/todos", &[("apikey", &key)], b"")
///     .unwrap();
/// assert_eq!(answer.status, 200);
/// ```
pub struct Zou {
    router: Router,
    rt: tokio::runtime::Runtime,
    pg: Postmaster,
    dsn: String,
    target: String,
    tenant: String,
    pg_bin: PathBuf,
    secret: String,
    keys: Keys,
    /// Kept so a branch of this database answers the S3 surface the
    /// same way its parent does.
    s3: Option<S3Keys>,
    /// Removed on close, always: this is the running copy and the store
    /// is where the data actually is.
    runtime: PathBuf,
    /// Removed on close only when this handle made it, which is the
    /// ephemeral case.
    owned_store: Option<PathBuf>,
    /// Taken off the store on close, which is the fixture case: the
    /// tenant is this handle's and the captures under it are the
    /// template's.
    owned_tenant: bool,
    closed: Mutex<bool>,
}

/// What a child gets from the parent it was cut out of, which nothing
/// in the options can express because a branch and a durable project
/// name the same two things: a store and a tenant on it.
#[derive(Clone, Copy, Default)]
struct Inherited {
    /// Taken off the store on close, because the parent was.
    owned_tenant: bool,
    /// The roles and the auth and storage schemas are in there already,
    /// because they were in the database this was copied from.
    contracted: bool,
}

/// What a database is going to be, worked out before anything has been
/// made that would have to be unmade.
struct Cut {
    target: String,
    tenant: String,
    owned_store: Option<PathBuf>,
    owned_tenant: bool,
    /// Whether the roles and the auth and storage schemas are already
    /// in there, which is true of a fixture because the template ran
    /// the bootstrap before it was published.
    contracted: bool,
}

impl Zou {
    /// Open a project, starting postgres over the store and building
    /// the front door in front of it.
    ///
    /// A store with no database in it is initdb'd and its genesis
    /// captured, which is the slow path and happens once. A store that
    /// has one is restored into the runtime directory and replayed,
    /// which is what every later open does.
    pub fn open(options: Options) -> Result<Zou, Error> {
        Zou::open_inheriting(options, Inherited::default())
    }

    /// The same, for a caller that knows things about the database it is
    /// opening which the options have no way to say.
    ///
    /// [`branch`](Zou::branch) is that caller. A branch names a store
    /// and a tenant on it, which is exactly what a durable project
    /// looks like, so nothing in the options tells the two apart and
    /// what the child inherits from its parent has to be handed over
    /// separately.
    fn open_inheriting(options: Options, inherited: Inherited) -> Result<Zou, Error> {
        let pg_bin = pg_bin(&options);
        let postgres = pg_bin.join("postgres");
        if !postgres.is_file() {
            return Err(Error::new(
                Kind::Options,
                format!(
                    "{} not found, point Options::pg_bin or ZOU_PG_BIN at a patched install",
                    postgres.display()
                ),
            ));
        }

        let nth = OPENED.fetch_add(1, Ordering::Relaxed);
        let runtime = match &options.runtime {
            Some(dir) => dir.clone(),
            None => std::env::temp_dir().join(format!("zou-embed-{}-{nth}", std::process::id())),
        };
        fs::create_dir_all(&runtime).map_err(io(format!("create {}", runtime.display())))?;

        let at = std::time::Instant::now();
        let mut cut = Zou::cut(&options, &pg_bin, &runtime)?;
        cut.owned_tenant |= inherited.owned_tenant;
        cut.contracted |= inherited.contracted;
        log::debug!("open: the database exists after {:?}", at.elapsed());
        // From here there is a tenant, and maybe a store, that a
        // failure has to take back off rather than leave for the next
        // run to trip over.
        match Zou::raise(&options, &pg_bin, &runtime, &cut) {
            Ok(zou) => Ok(zou),
            Err(e) => {
                if cut.owned_tenant {
                    let _ = template::drop_tenant(&cut.target, &cut.tenant);
                }
                let _ = fs::remove_dir_all(&runtime);
                Err(e)
            }
        }
    }

    /// Where this database is going to live, made if it is not there.
    ///
    /// Three cases. A named target is somebody else's store and nothing
    /// here owns it. An empty one is ephemeral: a store of this
    /// handle's own, under the runtime directory so one cleanup removes
    /// both. A fixture is a branch of the machine's template, which is
    /// the only one of the three that costs nothing.
    fn cut(options: &Options, pg_bin: &Path, runtime: &Path) -> Result<Cut, Error> {
        if !options.fixture {
            let (target, owned_store) = if options.target.is_empty() {
                let store = runtime.join("store");
                fs::create_dir_all(&store).map_err(io(format!("create {}", store.display())))?;
                (store.display().to_string(), Some(store))
            } else {
                (options.target.clone(), None)
            };
            return Ok(Cut {
                target,
                tenant: options.tenant.clone(),
                owned_store,
                owned_tenant: false,
                contracted: false,
            });
        }

        if !options.target.is_empty() {
            return Err(Error::new(
                Kind::Options,
                "a fixture is cut out of the machine's template, so it cannot name a store of \
                 its own, drop the target or drop the fixture flag",
            ));
        }
        let target = template::ensure(pg_bin)?.display().to_string();
        // Fixtures share the template store, so the default name would
        // be the same name for every one of them.
        let tenant = if options.tenant == LOCAL {
            format!("fixture-{}", random_hex(8)?)
        } else {
            options.tenant.clone()
        };
        template::cut(&target, &tenant)?;
        Ok(Cut {
            target,
            tenant,
            owned_store: None,
            owned_tenant: true,
            contracted: true,
        })
    }

    /// Everything from a database that exists to a handle over it.
    fn raise(options: &Options, pg_bin: &Path, runtime: &Path, cut: &Cut) -> Result<Zou, Error> {
        let postgres = pg_bin.join("postgres");
        let target = cut.target.clone();
        let tenant = cut.tenant.clone();
        let pgdata = runtime.join("pgdata");
        let pagecache = runtime.join("pagecache");
        let sock = runtime.join("sock");
        for dir in [&pagecache, &sock] {
            fs::create_dir_all(dir).map_err(io(format!("create {}", dir.display())))?;
        }
        fs::set_permissions(&sock, fs::Permissions::from_mode(0o700))
            .map_err(io(format!("chmod {}", sock.display())))?;

        let at = std::time::Instant::now();
        if fresh(&target, &tenant)? {
            initdb(pg_bin, &pgdata, &target, &tenant, &pagecache)?;
            log::debug!("open: initdb in {:?}", at.elapsed());
        } else {
            restore::restore(&target, &tenant, &pgdata).map_err(|e| Error::new(Kind::Store, e))?;
            log::debug!("open: restore in {:?}", at.elapsed());
        }

        let at = std::time::Instant::now();
        let port = free_port()?;
        let pg = start(
            &postgres,
            &pgdata,
            &sock,
            port,
            &target,
            &tenant,
            &pagecache,
            options.shared_buffers.as_deref(),
        )?;
        let dsn = format!("host=127.0.0.1 port={port} user={SUPERUSER} dbname=postgres");
        log::debug!("open: the postmaster is up after {:?}", at.elapsed());

        // From here on a failure has a postmaster to take down with it,
        // since nothing is holding one yet.
        let at = std::time::Instant::now();
        let rest = Zou::assemble(options, &target, &tenant, &dsn, cut.contracted);
        log::debug!("open: the front door is up after {:?}", at.elapsed());
        let (router, rt, secret, keys) = match rest {
            Ok(built) => built,
            Err(e) => {
                let _ = pg.stop();
                return Err(e);
            }
        };

        Ok(Zou {
            router,
            rt,
            pg,
            dsn,
            target,
            tenant,
            pg_bin: pg_bin.to_path_buf(),
            secret,
            keys,
            s3: options.s3.clone(),
            runtime: runtime.to_path_buf(),
            owned_store: cut.owned_store.clone(),
            owned_tenant: cut.owned_tenant,
            closed: Mutex::new(false),
        })
    }

    /// Everything between a live postmaster and a usable handle: the
    /// runtime, the tenant contract, the keys, and the front door.
    #[allow(clippy::type_complexity)]
    fn assemble(
        options: &Options,
        target: &str,
        tenant: &str,
        dsn: &str,
        contracted: bool,
    ) -> Result<(Router, tokio::runtime::Runtime, String, Keys), Error> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(io("tokio runtime"))?;

        // The roles, the auth schema and its functions, put in place
        // before the handle is handed back rather than on the first
        // request that happens to need them. A host process sets its
        // own schema up over `dsn` as its first act, and `grant select
        // to anon` has to work then.
        //
        // Unless the database was cut from a template, which ran this
        // before it was published and whose id covers the ddl, so the
        // answer cannot go stale without the template going with it.
        // Asking anyway costs 75 ms against the 45 the rest of a create
        // takes, which is the difference between a fixture per test and
        // a fixture per suite.
        if !contracted {
            let at = std::time::Instant::now();
            let contract = dsn.to_string();
            rt.block_on(async move {
                let (client, connection) =
                    tokio_postgres::connect(&contract, tokio_postgres::NoTls)
                        .await
                        .map_err(pg("connect"))?;
                let pump = tokio::spawn(connection);
                let out = zou_server::sql::bootstrap(&client)
                    .await
                    .map_err(pg("the tenant contract"));
                drop(client);
                let _ = pump.await;
                out
            })?;
            log::debug!("open: the tenant contract in {:?}", at.elapsed());
        }

        let secret = match &options.jwt_secret {
            Some(secret) if !secret.is_empty() => secret.clone(),
            _ => random_secret()?,
        };
        let keys = Keys {
            anon: zou_server::jwt::mint(&zou_server::jwt::key_claims("anon"), secret.as_bytes()),
            service_role: zou_server::jwt::mint(
                &zou_server::jwt::key_claims("service_role"),
                secret.as_bytes(),
            ),
        };
        let cfg = zou_server::Config {
            jwt_secret: secret.as_bytes().to_vec(),
            pg: Some(dsn.to_string()),
            schemas: if options.schemas.is_empty() {
                zou_server::Config::default().schemas
            } else {
                options.schemas.clone()
            },
            anon_role: match options.anon_role.is_empty() {
                true => zou_server::Config::default().anon_role,
                false => options.anon_role.clone(),
            },
            // Objects go where the pages go, the same store under the
            // same tenant prefix.
            objects: Some(target.to_string()),
            tenant: Some(tenant.to_string()),
            // Nothing carries mail out of a host process unless the
            // host wired one up, so a signup that had to wait for a
            // link would wait forever.
            mailer_autoconfirm: true,
            s3: options.s3.as_ref().map(|keys| zou_server::s3::Credentials {
                access: keys.access.clone(),
                secret: keys.secret.clone(),
                region: match keys.region.is_empty() {
                    true => zou_server::s3::REGION.to_string(),
                    false => keys.region.clone(),
                },
            }),
            ..Default::default()
        };
        let router = zou_server::router(cfg).map_err(|e| Error::new(Kind::Options, e))?;
        Ok((router, rt, secret, keys))
    }

    /// Answer one request, in this process.
    ///
    /// The path is what a url would carry after the host, query string
    /// and all, and the headers are what a client would send, `apikey`
    /// among them: nothing is added here, so a call that would be
    /// refused over http is refused the same way.
    pub fn request(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Result<Response, Error> {
        let mut req = Request::builder().method(method).uri(path);
        for (name, value) in headers {
            req = req.header(*name, *value);
        }
        let req = req
            .body(Body::from(body.to_vec()))
            .map_err(|e| Error::new(Kind::Request, format!("request: {e}")))?;
        let router = self.router.clone();
        self.rt.block_on(async move {
            let answer = router
                .oneshot(req)
                .await
                .map_err(|e| Error::new(Kind::Request, format!("dispatch: {e}")))?;
            let status = answer.status().as_u16();
            let headers = answer
                .headers()
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_string(),
                        String::from_utf8_lossy(value.as_bytes()).into_owned(),
                    )
                })
                .collect();
            let body = axum::body::to_bytes(answer.into_body(), usize::MAX)
                .await
                .map_err(|e| Error::new(Kind::Request, format!("reading the answer: {e}")))?;
            Ok(Response {
                status,
                headers,
                body: body.to_vec(),
            })
        })
    }

    /// Put the same front door on a port as well, and say which one.
    ///
    /// Port 0 asks the kernel for one, which is what a test wants. This
    /// is the door another process uses, supabase-js pointed at
    /// `http://127.0.0.1:<port>` included, and it serves the project
    /// this handle is already holding rather than a second copy of it.
    pub fn listen(&self, port: u16) -> Result<u16, Error> {
        let listener = std::net::TcpListener::bind(("127.0.0.1", port))
            .map_err(io(format!("bind 127.0.0.1:{port}")))?;
        let port = listener.local_addr().map_err(io("local address"))?.port();
        listener.set_nonblocking(true).map_err(io("nonblocking"))?;
        let router = self.router.clone();
        self.rt.spawn(async move {
            let listener = match tokio::net::TcpListener::from_std(listener) {
                Ok(listener) => listener,
                Err(e) => {
                    log::error!("listener: {e}");
                    return;
                }
            };
            let service = router.into_make_service_with_connect_info::<std::net::SocketAddr>();
            if let Err(e) = axum::serve(listener, service).await {
                log::error!("serve: {e}");
            }
        });
        Ok(port)
    }

    /// A copy on write branch of this database, open and ready.
    ///
    /// The child references the parent's objects and copies none of
    /// them, so this costs a checkpoint, the fold that checkpoint
    /// starts, and two manifest round trips, rather than the size of
    /// the database. The checkpoint is taken here on purpose: a branch
    /// is made from what the store has, and the point of asking for one
    /// from inside the process that is writing is that it should carry
    /// what that process has written.
    ///
    /// Waiting for the fold is the other half of that, and it is not
    /// optional. A branch point is a capture, the capture is packed on
    /// a background thread perhaps a tenth of a second after the
    /// checkpoint hands it the data, and a child cut in that gap is a
    /// database that opens, serves, and is missing everything its
    /// parent wrote since the last fold, with nothing anywhere saying
    /// so. A second branch of a parent that has not been written to
    /// since waits for nothing, because the capture it needs is already
    /// there.
    ///
    /// A database young enough that no fold has packed a full capture
    /// down yet cannot be branched, and this says so rather than
    /// handing back a child that would fail on its first page read.
    /// That is the same refusal `zou branch create` gives, out of the
    /// same function, and it leaves nothing of the child behind.
    pub fn branch(&self, name: &str) -> Result<Zou, Error> {
        self.settle()?;
        let store = open_store(&self.target).map_err(|e| Error::new(Kind::Store, e))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::new(Kind::Store, "the clock is before 1970"))?
            .as_secs();
        let manifest = zou_store::branch(&*store, &self.tenant, name, None, now)
            .map_err(|e| Error::new(Kind::Store, e.to_string()))?;
        zou_pg::branching::refuse_unservable(&*store, &self.tenant, name, &manifest)
            .map_err(|e| Error::new(Kind::Store, e))?;
        Zou::open_inheriting(
            Options {
                target: self.target.clone(),
                tenant: name.to_string(),
                pg_bin: self.pg_bin.clone(),
                runtime: None,
                jwt_secret: Some(self.secret.clone()),
                schemas: Vec::new(),
                anon_role: String::new(),
                shared_buffers: None,
                // The child names the store the parent is on, which is
                // the template store when the parent is a fixture, so
                // there is nothing left for the template path to do.
                fixture: false,
                s3: self.s3.clone(),
            },
            Inherited {
                // A branch of a project somebody keeps is a project
                // somebody keeps, and a branch of a throwaway is a
                // throwaway. The second is a suite giving every test a
                // branch of one seeded database, and without this the
                // branches pile up on the machine's template store, one
                // per test, forever.
                owned_tenant: self.owned_tenant,
                // The child is a copy of a database this process is
                // serving, so the roles and the auth and storage
                // schemas came with it. Asking again is 75 ms of ddl
                // that finds everything already there.
                contracted: true,
            },
        )
    }

    /// Whether a branch of this database would serve, which is the same
    /// question [`branch`](Zou::branch) asks after it has cut one.
    ///
    /// It is false for a database young enough that no fold has packed
    /// a full page capture down yet, and it stays true afterwards, so a
    /// host process that wants to offer branching can ask before it
    /// offers rather than fail when somebody takes it up.
    pub fn branchable(&self) -> Result<bool, Error> {
        let store = open_store(&self.target).map_err(|e| Error::new(Kind::Store, e))?;
        let layout = TenantLayout::new(&self.tenant);
        let Some((data, _)) = store
            .get(&layout.manifest())
            .map_err(|e| Error::new(Kind::Store, e.to_string()))?
        else {
            return Ok(false);
        };
        let manifest =
            Manifest::from_json(&data).map_err(|e| Error::new(Kind::Store, e.to_string()))?;
        zou_pg::reader::why_unservable(&*store, &layout, &manifest)
            .map(|why| why.is_none())
            .map_err(|e| Error::new(Kind::Store, e))
    }

    /// Push everything committed so far into the store, so a branch or
    /// a copy of the store made now carries it.
    pub fn checkpoint(&self) -> Result<(), Error> {
        self.checkpoint_redo().map(|_| ())
    }

    /// The same checkpoint, and where in the wal it starts replaying
    /// from, which is the position a capture of it is named after.
    fn checkpoint_redo(&self) -> Result<u64, Error> {
        let dsn = self.dsn.clone();
        self.rt.block_on(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .map_err(pg("connect"))?;
            let pump = tokio::spawn(connection);
            let out = async {
                client
                    .simple_query("checkpoint")
                    .await
                    .map_err(pg("checkpoint"))?;
                // As a plain integer, because that is what the manifest
                // counts in and the two have to be compared.
                let rows = client
                    .simple_query(
                        "select pg_wal_lsn_diff(redo_lsn, '0/0')::bigint from pg_control_checkpoint()",
                    )
                    .await
                    .map_err(pg("the checkpoint position"))?;
                let redo = rows
                    .iter()
                    .find_map(|row| match row {
                        tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
                        _ => None,
                    })
                    .and_then(|value| value.parse::<u64>().ok())
                    .ok_or_else(|| {
                        Error::new(Kind::Postgres, "pg_control_checkpoint said nothing")
                    })?;
                Ok(redo)
            }
            .await;
            drop(client);
            let _ = pump.await;
            out
        })
    }

    /// Checkpoint, and wait until the capture of it is on the store.
    ///
    /// The checkpoint hands the data over and returns. What names it is
    /// a capture, folded on a background thread and published a moment
    /// later, and a branch point is a capture, so everything between
    /// the two is a window where a branch would be cut behind the data.
    /// A parent that has not been written to since its last fold is
    /// already past this and pays nothing.
    fn settle(&self) -> Result<(), Error> {
        let redo = self.checkpoint_redo()?;
        let store = open_store(&self.target).map_err(|e| Error::new(Kind::Store, e))?;
        let layout = TenantLayout::new(&self.tenant);
        let at = std::time::Instant::now();
        loop {
            let Some((data, _)) = store
                .get(&layout.manifest())
                .map_err(|e| Error::new(Kind::Store, e.to_string()))?
            else {
                return Err(Error::new(
                    Kind::Store,
                    format!("{} is not on the store", self.tenant),
                ));
            };
            let manifest =
                Manifest::from_json(&data).map_err(|e| Error::new(Kind::Store, e.to_string()))?;
            if manifest.checkpoints.iter().any(|c| c.lsn.0 >= redo) {
                log::debug!(
                    "branch: the capture of {redo} landed after {:?}",
                    at.elapsed()
                );
                return Ok(());
            }
            if at.elapsed() > FOLD_WAIT {
                return Err(Error::new(
                    Kind::Store,
                    format!(
                        "the checkpoint at {redo} was not captured within {} seconds, so a branch \
                         cut now would not carry what this database has written",
                        FOLD_WAIT.as_secs()
                    ),
                ));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// The keys a client of this handle signs its calls with.
    pub fn keys(&self) -> &Keys {
        &self.keys
    }

    /// What a postgres client would dial to reach the database
    /// directly, for the host that wants psql or its own driver on it.
    pub fn dsn(&self) -> &str {
        &self.dsn
    }

    /// The store this project lives on, which for an ephemeral handle
    /// is the directory that goes away with it.
    pub fn target(&self) -> &str {
        &self.target
    }

    /// The database inside that store.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Stop postgres and remove the running copy, reporting whether it
    /// went cleanly. Dropping the handle does the same thing and has
    /// nowhere to report it, which is the only difference.
    pub fn close(self) -> Result<(), Error> {
        self.shutdown()
    }

    /// The same thing through a shared handle, for a binding that hands
    /// out an `Arc` and cannot consume what it gave away. Calling it
    /// twice is fine and the second call does nothing, which is what
    /// makes it safe for a garbage collected language to call it from
    /// both an explicit close and whatever collects the object later.
    pub fn shutdown(&self) -> Result<(), Error> {
        let mut closed = self.closed.lock().expect("the close flag");
        if *closed {
            return Ok(());
        }
        *closed = true;
        let stopped = self.pg.stop();
        // The runtime directory is a copy. Every acked write is on the
        // store by definition, so this is not throwing data away, it is
        // throwing away the copy postgres was running out of.
        let _ = fs::remove_dir_all(&self.runtime);
        if let Some(store) = &self.owned_store {
            let _ = fs::remove_dir_all(store);
        }
        // A fixture is a tenant on a store somebody else is also using,
        // so what goes is the tenant. The captures it read out of are
        // the template's and stay where they are.
        if self.owned_tenant
            && let Err(e) = template::drop_tenant(&self.target, &self.tenant)
        {
            log::warn!("dropping the fixture {}: {e}", self.tenant);
        }
        stopped
    }
}

impl Drop for Zou {
    fn drop(&mut self) {
        if let Err(e) = self.shutdown() {
            log::warn!("closing an embedded zou: {e}");
        }
    }
}

/// Where the patched postgres is: what the caller said, else the
/// environment, else the install this binary shipped in, else where a
/// checkout builds into.
fn pg_bin(options: &Options) -> PathBuf {
    zou_pg::install::pg_bin(Some(options.pg_bin.clone()))
}

/// Whether the store has no database at this ref yet, which is the one
/// case that runs initdb.
fn fresh(target: &str, tenant: &str) -> Result<bool, Error> {
    let store = open_store(target).map_err(|e| Error::new(Kind::Store, e))?;
    let layout = TenantLayout::new(tenant);
    match store
        .get(&layout.manifest())
        .map_err(|e| Error::new(Kind::Store, e.to_string()))?
    {
        None => Ok(true),
        Some((data, _)) => Ok(Manifest::from_json(&data)
            .map_err(|e| Error::new(Kind::Store, e.to_string()))?
            .checkpoints
            .is_empty()),
    }
}

/// Make the database and capture it, the once.
fn initdb(
    pg_bin: &Path,
    pgdata: &Path,
    target: &str,
    tenant: &str,
    pagecache: &Path,
) -> Result<(), Error> {
    let out = Command::new(pg_bin.join("initdb"))
        .arg("-D")
        .arg(pgdata)
        .args(["-U", SUPERUSER])
        .args(["--set", "io_method=sync"])
        // Pages are store objects and a put is atomic on every backend,
        // so a torn write cannot be observed and the protection against
        // one is pure cost.
        .args(["--set", "full_page_writes=off"])
        .env("ZOU_TARGET", target)
        .env("ZOU_TENANT", tenant)
        .env("ZOU_PAGE_CACHE", pagecache)
        // initdb is standalone, there is no page service yet, and
        // unset means on.
        .env("ZOU_PAGESERVE", "0")
        .output()
        .map_err(|e| Error::new(Kind::Postgres, format!("initdb: {e}")))?;
    if !out.status.success() {
        return Err(Error::new(
            Kind::Postgres,
            format!("initdb failed:\n{}", String::from_utf8_lossy(&out.stderr)),
        ));
    }
    let control = fs::read(pgdata.join("global/pg_control")).map_err(io("read pg_control"))?;
    let redo = restore::control_redo(&control).map_err(|e| Error::new(Kind::Store, e))?;
    let store = open_store(target).map_err(|e| Error::new(Kind::Store, e))?;
    bootstrap::capture_genesis(&*store, &TenantLayout::new(tenant), pgdata, redo)
        .map_err(|e| Error::new(Kind::Store, e))?;
    Ok(())
}

/// Start the postmaster and wait for it to say it is up.
///
/// It is on loopback and in a 0700 socket directory, because the only
/// client it is meant to have is in this process.
#[allow(clippy::too_many_arguments)]
fn start(
    postgres: &Path,
    pgdata: &Path,
    sock: &Path,
    port: u16,
    target: &str,
    tenant: &str,
    pagecache: &Path,
    shared_buffers: Option<&str>,
) -> Result<Postmaster, Error> {
    let mut command = Command::new(postgres);
    command
        .arg("-D")
        .arg(pgdata)
        .args(["-p", &port.to_string()])
        .arg("-k")
        .arg(sock)
        .args(["-c", "listen_addresses=127.0.0.1"])
        // postgres changes reads the logical decoder, and a postmaster
        // started below this level wrote nothing for it to read.
        // Raising it is a restart, so it is on from the first boot
        // rather than from whenever somebody first subscribes. What it
        // costs is write ahead log volume on updates and deletes, which
        // is the standing price of a database that can say what
        // changed, and is what every Supabase project runs at.
        .args(["-c", "wal_level=logical"])
        .env("ZOU_TARGET", target)
        .env("ZOU_TENANT", tenant)
        .env("ZOU_PAGE_CACHE", pagecache)
        // Templates, fixtures and branches all read the page runs a
        // fold packed down, and with the page service on the fold
        // publishes an indexless checkpoint instead, so a branch of a
        // project made this way would be refused. The embedded library
        // is the branching product surface, so it stays on the object
        // path until a branch can be taken off the page layers.
        .env("ZOU_PAGESERVE", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if let Some(size) = shared_buffers {
        command.args(["-c", &format!("shared_buffers={size}")]);
    }
    let mut child = command
        .spawn()
        .map_err(|e| Error::new(Kind::Postgres, format!("spawn {}: {e}", postgres.display())))?;
    let pid = child.id();
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::new(Kind::Postgres, "no stderr pipe"))?;

    // Postgres logs to stderr with the collector off, and the line that
    // says it is accepting connections is the readiness signal. The
    // thread outlives that: a host process wants the log, and a
    // postmaster that dies later says why on the same pipe.
    let (ready, told) = mpsc::sync_channel::<Result<(), String>>(1);
    std::thread::spawn(move || {
        let mut said = false;
        for line in BufReader::new(stderr).lines() {
            let Ok(line) = line else { break };
            log::debug!("postgres: {line}");
            if line.contains("ready to accept connections") && !said {
                said = true;
                let _ = ready.try_send(Ok(()));
            }
        }
        if !said {
            let _ = ready.try_send(Err("postgres exited before it was ready".to_string()));
        }
    });
    match told.recv_timeout(START_TIMEOUT) {
        Ok(Ok(())) => Ok(Postmaster {
            pid,
            child: Mutex::new(Some(child)),
        }),
        Ok(Err(e)) => {
            let _ = child.wait();
            Err(Error::new(Kind::Postgres, e))
        }
        Err(_) => {
            unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
            let _ = child.wait();
            Err(Error::new(
                Kind::Postgres,
                format!("postgres was not ready within {}s", START_TIMEOUT.as_secs()),
            ))
        }
    }
}

/// A port nothing is listening on, by asking the kernel for one.
fn free_port() -> Result<u16, Error> {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").map_err(io("looking for a free port"))?;
    Ok(listener
        .local_addr()
        .map_err(io("looking for a free port"))?
        .port())
}

fn random_secret() -> Result<String, Error> {
    random_hex(32)
}

/// `bytes` random bytes, written out as hex.
fn random_hex(bytes: usize) -> Result<String, Error> {
    let mut raw = vec![0u8; bytes];
    getrandom::fill(&mut raw).map_err(|e| Error::new(Kind::Io, format!("random bytes: {e}")))?;
    Ok(raw.iter().map(|b| format!("{b:02x}")).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_ephemeral_handle_names_no_store_until_it_is_opened() {
        let options = Options::ephemeral();
        assert!(options.target.is_empty(), "the handle makes one");
        assert_eq!(options.tenant, LOCAL);
        assert!(options.jwt_secret.is_none(), "a fresh secret every time");
    }

    #[test]
    fn a_store_is_named_by_a_path_or_by_a_url() {
        assert_eq!(Options::dir("/srv/data").target, "/srv/data");
        assert_eq!(
            Options::url("s3://bucket/fleet").target,
            "s3://bucket/fleet"
        );
        assert_eq!(
            Options::url("s3://bucket/fleet").tenant("pr-42").tenant,
            "pr-42"
        );
    }

    #[test]
    fn the_install_is_the_caller_then_the_environment_then_the_build() {
        let named = Options {
            pg_bin: PathBuf::from("/opt/pg/bin"),
            ..Options::default()
        };
        assert_eq!(pg_bin(&named), PathBuf::from("/opt/pg/bin"));
        // Unset in a test process is the interesting case, since a
        // developer with ZOU_PG_BIN exported is the other one.
        if std::env::var_os("ZOU_PG_BIN").is_none() {
            assert_eq!(pg_bin(&Options::default()), PathBuf::from("build/pg/bin"));
        }
    }

    #[test]
    fn a_handle_can_be_held_by_more_than_one_thread() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Zou>();
        assert_send_sync::<Error>();
    }

    #[test]
    fn headers_are_read_back_without_matching_case() {
        let answer = Response {
            status: 200,
            headers: vec![("Content-Type".into(), "application/json".into())],
            body: Vec::new(),
        };
        assert_eq!(answer.header("content-type"), Some("application/json"));
        assert_eq!(answer.header("CONTENT-TYPE"), Some("application/json"));
        assert_eq!(answer.header("x-nothing"), None);
    }

    #[test]
    fn a_missing_postgres_is_refused_by_name_rather_than_hung_on() {
        let dir = std::env::temp_dir().join(format!("zou-embed-test-{}", std::process::id()));
        let opened = Zou::open(Options {
            pg_bin: PathBuf::from("/nowhere/at/all/bin"),
            runtime: Some(dir.clone()),
            ..Options::ephemeral()
        });
        let Err(e) = opened else {
            panic!("there is no postgres there");
        };
        assert_eq!(e.kind, Kind::Options);
        assert!(e.message.contains("not found"), "{}", e.message);
        let _ = fs::remove_dir_all(&dir);
    }
}
