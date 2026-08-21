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

use crate::config::{self, Project};
use crate::serve;
use zou_pg::{install, restore};
use zou_store::layout::TenantLayout;
use zou_store::{CasStore, Manifest, open_store};

/// The cluster superuser a database is initialised with, here and in
/// `zou serve`.
///
/// It is the role that owns a project's schemas, so it is the role a
/// project's own migrations run as, and `postgres.<ref>` over the
/// postgres port is then the connection string a Supabase project
/// already has. anon, authenticated and service_role reach SQL through
/// the same port and are what the api uses, but none of them owns
/// anything, which is why none of them can create a table.
///
/// Named rather than taken from the environment, because the owner of a
/// database should not depend on which account started the process, and
/// a store initdb'd by one command has to be openable by the other.
pub const SUPERUSER: &str = "postgres";

/// The postgres port with nothing else to go on. Supabase projects
/// name their own, usually 54322, and the file is read for it.
const DEFAULT_PORT: u16 = 5432;

/// How many times in a row the postmaster may die before its first
/// accepted connection until we stop retrying. A crash after it was up
/// resets the count, that is the recover-and-continue path.
const MAX_FAILED_STARTS: u32 = 3;

/// Set by the signal handler, drained by the supervision loop.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_sig: libc::c_int) {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

/// The tenant a dev loop serves when nothing names one. A single
/// database in a store of its own is what a laptop has, and this is the
/// ref every other command defaults to as well.
pub const LOCAL: &str = "local";

pub struct Args {
    pub target: String,
    /// Which tenant in the store to serve. `local` unless `--ref` names
    /// a branch, which is how a per pull request database is opened.
    pub tenant: String,
    pub pg_bin: PathBuf,
    /// None until the project file and the defaults have had their
    /// say, which happens in `run` rather than here, so parsing stays a
    /// pure function of the command line.
    pub port: Option<u16>,
    pub http: Option<u16>,
    pub ops: Option<u16>,
    pub runtime: PathBuf,
    /// A config.toml named on the command line. With nothing named the
    /// working directory is searched, and `--no-config` searches
    /// nowhere.
    pub config: Option<PathBuf>,
    pub no_config: bool,
    /// `--page-service on|off`, None to leave the environment alone
    /// and take the default, which is on.
    pub page_service: Option<bool>,
}

use crate::DEV_USAGE as USAGE;

pub fn parse(argv: &[String]) -> Result<Args, String> {
    let mut target = None;
    let mut tenant = None;
    let mut pg_bin = None;
    let mut port = None;
    let mut http = None;
    let mut ops = None;
    let mut runtime = None;
    let mut config = None;
    let mut no_config = false;
    let mut page_service = None;
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--ref" => tenant = Some(need(&mut it, "--ref")?.to_string()),
            "--pg-bin" => pg_bin = Some(PathBuf::from(need(&mut it, "--pg-bin")?)),
            "--port" => {
                let raw = need(&mut it, "--port")?;
                port = Some(raw.parse().map_err(|_| {
                    format!("bad port {raw:?}, write a port number from 1 to 65535")
                })?);
            }
            "--http" => {
                let raw = need(&mut it, "--http")?;
                http = Some(raw.parse().map_err(|_| {
                    format!("bad http port {raw:?}, write a port number from 1 to 65535")
                })?);
            }
            "--ops" => {
                let raw = need(&mut it, "--ops")?;
                ops = Some(raw.parse().map_err(|_| {
                    format!("bad ops port {raw:?}, write a port number from 1 to 65535")
                })?);
            }
            "--runtime" => runtime = Some(PathBuf::from(need(&mut it, "--runtime")?)),
            "--config" => config = Some(PathBuf::from(need(&mut it, "--config")?)),
            "--no-config" => no_config = true,
            "--page-service" => {
                let raw = need(&mut it, "--page-service")?;
                page_service = Some(match raw.trim().to_ascii_lowercase().as_str() {
                    "1" | "true" | "on" | "yes" => true,
                    "0" | "false" | "off" | "no" => false,
                    _ => return Err(format!("bad --page-service {raw:?}, want on or off")),
                });
            }
            other if target.is_none() && !other.starts_with('-') => {
                target = Some(other.to_string());
            }
            other => return Err(format!("unexpected argument {other:?}\n{USAGE}")),
        }
    }
    let target = target.ok_or(USAGE)?;
    let pg_bin = install::pg_bin(pg_bin);
    let runtime = runtime
        .unwrap_or_else(|| std::env::temp_dir().join(format!("zou-dev-{}", std::process::id())));
    Ok(Args {
        target,
        tenant: tenant.unwrap_or_else(|| LOCAL.to_string()),
        pg_bin,
        port,
        http,
        ops,
        runtime,
        config,
        no_config,
        page_service,
    })
}

/// The project file this dev loop belongs to, which is the same
/// question `zou functions serve` asks and is answered in one place.
fn project(args: &Args) -> Result<Option<Project>, String> {
    config::project(args.config.as_deref(), args.no_config)
}

/// The two ports this command listens on, once the flags, the project
/// file and the defaults have all had their say. Nothing is served
/// over http unless a flag or a project asks for it.
fn ports(args: &Args, project: Option<&Project>) -> (u16, Option<u16>) {
    let db = args
        .port
        .or_else(|| project.and_then(|p| p.db))
        .unwrap_or(DEFAULT_PORT);
    let http = args.http.or_else(|| project.and_then(|p| p.api));
    (db, http)
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

/// Start the HTTP front door on 127.0.0.1:port in its own thread. The
/// secret comes from ZOU_JWT_SECRET when the caller pins one, so the
/// keys stay stable across restarts, otherwise a fresh secret is
/// generated and logged together with the keys it signs, which is
/// enough for a dev loop. The keys are printed the way supabase start
/// prints its own, copy them into the client and go. The SQL pool
/// dials the postmaster this process supervises, lazily, so the order
/// the two come up in does not matter.
fn start_http(
    port: u16,
    pg_port: u16,
    target: String,
    tenant: String,
    project: Option<&Project>,
) -> Result<(), String> {
    let (secret, anon, service) = crate::functions::keys()?;
    let listener = std::net::TcpListener::bind(("127.0.0.1", port))
        .map_err(|e| format!("bind http on 127.0.0.1:{port}: {e}"))?;
    log::info!("http api on http://127.0.0.1:{port}");
    log::info!("anon key {anon}");
    log::info!("service_role key {service}");
    // The S3 endpoint is asked with a pair rather than with either of
    // those keys, so it is printed here next to them, the way a local
    // project prints all three together. A project that switched the
    // endpoint off in its own file gets no pair, and then it answers
    // every signature with the key not being one this project has.
    let s3 = project
        .is_none_or(|p| p.s3)
        .then(crate::config::local_s3)
        .inspect(|s3| {
            log::info!("s3 access key {}", s3.access);
            log::info!("s3 secret key {}", s3.secret);
            log::info!("s3 region {}", s3.region);
        });
    if s3.is_none() {
        log::info!("the s3 endpoint is off, storage.s3_protocol.enabled is false");
    }
    let autoconfirm = zou_store::setting::flag("ZOU_MAILER_AUTOCONFIRM").unwrap_or(true);
    // Which roles a token may ask to run as. A project that made its
    // own role and wrote policies for it says so here, comma
    // separated, and everything else keeps the three a Supabase
    // project has. Naming a set replaces the default rather than
    // adding to it, and the anonymous role is in either way.
    let exposed_roles: Vec<String> = std::env::var("ZOU_EXPOSED_ROLES")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|role| !role.is_empty())
        .map(str::to_string)
        .collect();
    if !exposed_roles.is_empty() {
        log::info!("the exposed roles are {}", exposed_roles.join(", "));
    }
    // A configured mail server carries the mail, and then there is
    // nothing left in memory for `zou inbox` to print, which is why it
    // says which of the two is happening.
    let sender: Option<std::sync::Arc<dyn zou_server::mail::Sender>> =
        match zou_server::smtp::from_env()? {
            Some(smtp) => {
                log::info!("mail goes to {}:{}", smtp.host, smtp.port);
                Some(std::sync::Arc::new(smtp))
            }
            None => None,
        };
    if !autoconfirm && sender.is_none() {
        log::info!("signups need a confirmation link, read it with zou inbox --http {port}");
    }
    // The subjects, the templates, where each link points and how often
    // one may be asked for. A project that set none of them gets
    // GoTrue's own defaults, and a project that set them for GoTrue
    // gets what it wrote, because the names are upstream's with the
    // prefix swapped. A template setting names a file or a url, which
    // is read here, once, rather than on the way to somebody's inbox.
    let mail = zou_server::mail::settings_from_env()?;
    if !mail.bodies.is_empty() {
        log::info!(
            "the {} mail templates are this project's own",
            mail.bodies.len()
        );
    }
    // Phone is off by default, the same as GoTrue, because a project
    // that has not said it wants phone sign in should refuse it by name
    // rather than half serve it.
    let phone_enabled = zou_store::setting::flag("ZOU_EXTERNAL_PHONE_ENABLED").unwrap_or(false);
    let texter: Option<std::sync::Arc<dyn zou_server::sms::Sender>> =
        match zou_server::sms::from_env()? {
            Some(texter) => {
                log::info!("texts go through {}", texter.describe());
                Some(texter)
            }
            None => None,
        };
    if phone_enabled && texter.is_none() {
        log::info!("phone codes stay in this process, read them with zou inbox --http {port}");
    }
    // The template a code arrives in, how many digits it has, how long
    // it lasts and how often one may be asked for, all of them
    // GOTRUE_SMS_ with the prefix swapped.
    let sms = zou_server::sms::settings_from_env()?;
    let oauth = zou_server::oauth::from_env()?;
    if !oauth.is_empty() {
        log::info!("social sign in with {}", oauth.names().join(", "));
    }
    let manual_linking =
        zou_store::setting::flag("ZOU_SECURITY_MANUAL_LINKING_ENABLED").unwrap_or(false);
    let anonymous_users =
        zou_store::setting::flag("ZOU_EXTERNAL_ANONYMOUS_USERS_ENABLED").unwrap_or(false);
    // The default on this one is the other way round from the flags
    // above: on and off are GoTrue's, and GoTrue serves addresses
    // unless it is told not to.
    let email_enabled = zou_store::setting::flag("ZOU_EXTERNAL_EMAIL_ENABLED").unwrap_or(true);
    let disable_signup = zou_store::setting::flag("ZOU_DISABLE_SIGNUP").unwrap_or(false);
    let hook = zou_server::hook::from_env()?;
    // Worth a line of its own: a hook that rewrites claims is the one
    // piece of a project's own code running in the middle of every sign
    // in, and the first place to look when a token carries something
    // nothing in this server puts there.
    if hook.custom_access_token.live() {
        log::info!(
            "every access token goes through {}",
            hook.custom_access_token.uri
        );
    } else if !hook.custom_access_token.uri.is_empty() {
        log::info!(
            "{} is wired up and switched off, set ZOU_HOOK_CUSTOM_ACCESS_TOKEN_ENABLED=true",
            hook.custom_access_token.uri
        );
    }
    let audit = zou_server::audit::from_env()?;
    if let Some(keep) = audit.retention {
        log::info!(
            "audit entries older than {} are deleted once an hour",
            crate::gc::span(keep.as_secs())
        );
    }
    if audit.disable_postgres {
        log::info!("the audit trail goes to the log stream only, no rows are written");
    }
    let limit = zou_server::limit::from_env()?;
    // The endpoint budgets are all configured and none of them do
    // anything until this server can tell one caller from another, so
    // the loop says which of the two it is rather than leaving an
    // operator to find out by not being limited.
    if limit.sb_forwarded_for {
        log::info!("callers are counted by the sb-forwarded-for address");
    } else if !limit.header.is_empty() {
        log::info!("callers are counted by the {} header", limit.header);
    } else if limit.peer {
        log::info!("callers are counted by the socket they arrive on");
    } else {
        log::info!(
            "per endpoint rate limits are off, set ZOU_RATE_LIMIT_HEADER or ZOU_RATE_LIMIT_PEER"
        );
    }
    let realtime = zou_server::realtime::limits_from_env()?;
    let webhook = zou_server::webhook::retries_from_env()?;
    let mfa = zou_server::mfa::from_env()?;
    if !mfa.totp_enroll || !mfa.totp_verify {
        log::info!("authenticator factors are off, /auth/v1/factors refuses by name");
    }
    if !email_enabled {
        log::info!("email sign in is off, nothing here answers an address");
    }
    if disable_signup {
        log::info!("signups are off, invitations are the way in");
    }
    // Local connections are trust, the stock dev loop layout.
    let dsn = format!("host=127.0.0.1 port={pg_port} user={SUPERUSER} dbname=postgres");
    // And the one a request logs in as, which is the split hosted
    // Supabase has between its api and the rest of a project. The
    // superuser dsn above still owns the schemas and still reads auth
    // and storage past their policies; what a token can steer runs
    // here instead, as a role granted the three api roles and nothing
    // more, so a `role` claim naming the superuser is refused by
    // postgres rather than by us. See #92.
    let request = format!("host=127.0.0.1 port={pg_port} user=authenticator dbname=postgres");
    // The functions a project keeps beside its config file, and the
    // four variables every one of them sees. What a function is told
    // about the database is the url shape a client library expects
    // rather than the keyword dsn this process dials with.
    let functions = match project {
        Some(p) => crate::functions::registry(
            &p.dir(),
            &p.functions,
            crate::functions::env(
                port,
                &anon,
                &service,
                &format!("postgresql://{SUPERUSER}@127.0.0.1:{pg_port}/postgres"),
            ),
        )?,
        None => None,
    };
    // What the project's own config.toml says, when it has one. The
    // schemas matter most: PostgREST serves what it was told to serve
    // and the first of them is what a request that names no schema
    // gets, so a project keeping its tables outside public is broken
    // until this arrives.
    let schemas = project.map(|p| p.schemas.clone()).unwrap_or_default();
    let site_url = project.and_then(|p| p.site_url.clone());
    std::thread::spawn(move || {
        let cfg = zou_server::Config {
            // The same key a served project signs with, derived from
            // the same secret, so a function that verifies its caller
            // behaves the same in the dev loop as it does deployed.
            // Pin ZOU_JWT_SECRET and the key is pinned with it, which
            // is what keeps a session alive across a restart.
            jwt_keys: Some(zou_server::jwt::derived_keys(secret.as_bytes())),
            jwt_secret: secret.into_bytes(),
            pg: Some(dsn),
            pg_request: Some(request),
            // A project that named its schemas gets them in its own
            // order, and one that did not gets the server's default.
            schemas: if schemas.is_empty() {
                zou_server::Config::default().schemas
            } else {
                schemas
            },
            // What a `role` claim is allowed to name, from
            // ZOU_EXPOSED_ROLES and otherwise the three a Supabase
            // project has. The dev loop connects to postgres as the
            // superuser, so without this a token signed with the
            // project secret and carrying "role": "postgres" would be
            // a superuser session. See #92.
            exposed_roles,
            // Where a confirmation link sends a person, which is the
            // project's own front end and not this server.
            site_url,
            // The dev loop knows where it answers, so its access tokens
            // say so rather than naming GoTrue's default port, which
            // nothing here listens on.
            external_url: Some(format!("http://127.0.0.1:{port}")),
            // A signup is confirmed on the spot, which is what the
            // Supabase CLI does locally too. Set
            // ZOU_MAILER_AUTOCONFIRM=false to make the dev loop mail
            // its confirmations instead: nothing carries them
            // anywhere, they are kept in memory, and `zou inbox`
            // prints the link.
            mailer_autoconfirm: autoconfirm,
            // Set ZOU_SMTP_HOST and the mail goes out for real. With
            // nothing set this is None and the messages stay in the
            // process, which is what a laptop wants.
            sender,
            // What those messages say, and how often one is sent.
            mail,
            // Set ZOU_EXTERNAL_GOOGLE_CLIENT_ID and its secret, or the
            // same pair for github, and /authorize starts offering
            // them. With nothing set this is empty and every provider
            // is refused by name.
            oauth,
            // Object bytes go where the pages go: the same store, the
            // same tenant prefix, under files/. On a laptop that is a
            // directory next to the data and on a deployment it is the
            // same bucket, which is the whole point of storing both on
            // the same thing. The tenant is the ref the dev loop
            // restored, so a branch serves the files its parent had and
            // writes its own under its own prefix.
            objects: Some(target),
            tenant: Some(tenant),
            // The S3 protocol surface, asked with the pair logged
            // above. A dev loop with no pair would answer every signed
            // request that the key is not one this project has, which
            // is a working endpoint that says no to the client a
            // project already has configured.
            s3,
            // Off by default, the same as GoTrue. Set
            // ZOU_SECURITY_MANUAL_LINKING_ENABLED=true and a signed in
            // person can attach a second provider to the account they
            // already have, and detach one again.
            manual_linking,
            // Off by default too. Set
            // ZOU_EXTERNAL_ANONYMOUS_USERS_ENABLED=true and a signup
            // with no address at all gets an account and a session,
            // which the client turns into a real account later by
            // setting an address on it.
            anonymous_users,
            // On unless ZOU_EXTERNAL_EMAIL_ENABLED=false, which is what
            // a project that signs everyone in by number or by social
            // provider wants, and off unless ZOU_DISABLE_SIGNUP=true,
            // which closes the door to everyone the project has not
            // already invited. Both are readable back from /settings.
            email_enabled,
            disable_signup,
            // Off unless ZOU_EXTERNAL_PHONE_ENABLED=true, and then set
            // ZOU_SMS_PROVIDER to twilio, messagebird, vonage or
            // textlocal with its credentials to send for real. With no
            // provider the codes stay in the process and `zou inbox`
            // prints them, which is how a phone sign in screen gets
            // written on a laptop with no Twilio account.
            phone_enabled,
            texter,
            sms,
            // An authenticator app is on by default, the same as
            // GoTrue. ZOU_MFA_TOTP_VERIFY_ENABLED=false turns MFA off
            // without deleting anybody's factors, and the two
            // ZOU_MFA_MAX_ envs move the ceilings.
            mfa,
            // Nothing unless ZOU_HOOK_CUSTOM_ACCESS_TOKEN_URI names a
            // function, and then that function decides what every
            // access token carries. It runs inside the sign in's own
            // transaction, so what it writes commits with the sign in
            // and a refusal takes the sign in down with it.
            hook,
            // GoTrue's own budgets, which limit nobody until this
            // server is told how to tell callers apart, and which limit
            // the mail and the text messages of the whole project
            // whether or not it is.
            limit,
            // Every row and forever, unless
            // ZOU_AUDIT_LOG_RETENTION names how long a project wants
            // its trail kept, or ZOU_AUDIT_LOG_DISABLE_POSTGRES says it
            // is keeping the log stream instead.
            audit,
            // What the sockets are allowed, realtime's own numbers: two
            // hundred at once, a hundred joins a second, a hundred
            // channels each, a hundred messages a second, three
            // megabytes a message. The five ZOU_REALTIME_MAX_ envs move
            // them, and a zero turns one off, which is what a laptop
            // running a load test of its own wants.
            realtime,
            // How hard a database webhook is tried before its answer
            // is written down: three times, two seconds apart and then
            // ten. ZOU_WEBHOOK_ATTEMPTS=1 is pg_net's own behaviour,
            // which is to try once and record whatever happened.
            webhook,
            // What is deployed under /functions/v1, which is a
            // directory listing beside the project's config file. A
            // project without one carries None and every name under
            // the prefix is the 404 upstream answers.
            functions,
            // Everything else is GoTrue's default, including the
            // unlimited edge rate the dev loop wants.
            ..Default::default()
        };
        if let Err(e) = zou_server::serve_blocking(listener, cfg) {
            log::error!("http server: {e}");
        }
    });
    Ok(())
}

/// The scrape and the health check, on a port of their own and on
/// loopback like everything else this command binds. Separate from the
/// api port because what is on it is for whoever runs the process and
/// not for whoever is using it.
fn start_ops(port: u16) -> Result<(), String> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", port))
        .map_err(|e| format!("bind ops on 127.0.0.1:{port}: {e}"))?;
    log::info!("metrics on http://127.0.0.1:{port}/metrics");
    std::thread::spawn(move || {
        if let Err(e) = zou_server::ops::serve_blocking(listener, env!("CARGO_PKG_VERSION")) {
            log::error!("ops server: {e}");
        }
    });
    Ok(())
}

pub fn run(args: &Args) -> Result<(), String> {
    serve::a_postmaster_cannot_run_over(&args.target, "zou dev")?;
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
    // Store op counters for the whole process tree. Setting the
    // variable in our own environment before the store opens covers
    // this process and everything it spawns, initdb and postgres
    // backends included, and they all bump the same mapped file. Fresh
    // every boot so a run's counters start at zero, and an explicit
    // ZOU_STORE_STATS from the caller wins. set_var is safe here, no
    // thread exists yet.
    if std::env::var_os("ZOU_STORE_STATS").is_none_or(|v| v.is_empty()) {
        let stats = args.runtime.join("store-stats");
        let _ = fs::remove_file(&stats);
        unsafe { std::env::set_var("ZOU_STORE_STATS", &stats) };
    }
    log::info!(
        "store op counters at {}, dump with zou stats",
        std::env::var("ZOU_STORE_STATS").unwrap_or_default()
    );
    let sock = args.runtime.join("sock");
    fs::create_dir_all(&sock).map_err(|e| format!("create {}: {e}", sock.display()))?;
    fs::set_permissions(&sock, fs::Permissions::from_mode(0o700))
        .map_err(|e| format!("chmod {}: {e}", sock.display()))?;
    log::info!("runtime directory {}", args.runtime.display());
    // The same reason `zou serve` does it: a socket is a descriptor and
    // the limit a shell hands a program is a thousand of them.
    log::debug!("up to {} open files, a socket each", serve::descriptors());

    let project = project(args)?;
    let (port, http) = ports(args, project.as_ref());

    let store: Arc<dyn CasStore> = Arc::from(open_store(&args.target)?);
    let layout = TenantLayout::new(&args.tenant);
    let manifest = match store
        .get(&layout.manifest())
        .map_err(|e| format!("store: {e}"))?
    {
        None => None,
        Some((data, _)) => Some(Manifest::from_json(&data).map_err(|e| format!("manifest: {e}"))?),
    };
    let fresh = manifest.as_ref().is_none_or(|m| m.checkpoints.is_empty());
    // `--page-service off` reads pages as objects under pg/, and a
    // store a page service session has run on stopped writing those
    // (zou #462). The postmaster refuses this too, this is only so that
    // the answer arrives before a restore rather than three failed
    // starts later.
    if args.page_service == Some(false)
        && let Some((elided, captured)) = manifest.as_ref().and_then(Manifest::pages_left_behind)
    {
        return Err(format!(
            "{} ran with the page service on from {elided} and has captured through {captured}, \
             so it has no page objects past {elided} to read. Leave the page service on here, \
             and compare the two paths on a store that has only ever run with it off.",
            args.target
        ));
    }
    // An empty `local` is a store nobody has used yet and initdb is
    // what it is waiting for. An empty anything else is a ref that was
    // named and does not exist, which is a typo far more often than it
    // is a request for a second database, and quietly making an empty
    // one under the misspelled name would hide the branch that was
    // meant.
    if fresh && args.tenant != LOCAL {
        return Err(format!(
            "{} has no database at ref {}, take a branch first: \
             zou branch {} create {LOCAL} {}",
            args.target, args.tenant, args.target, args.tenant
        ));
    }
    if args.tenant != LOCAL {
        log::info!("serving ref {}", args.tenant);
    }
    if fresh {
        log::info!("{} is empty, running initdb", args.target);
        let stats = crate::genesis::make(
            &*store,
            &args.target,
            &args.tenant,
            &args.pg_bin,
            &pgdata,
            &pagecache,
        )?;
        log::info!(
            "captured genesis, {} files, {} bytes",
            stats.files,
            stats.bytes
        );
    } else {
        let stats = restore::restore(&args.target, &args.tenant, &pgdata)?;
        log::info!(
            "restored {} files and replayed {} wal records from {}",
            stats.files,
            stats.wal_records,
            args.target
        );
        // Only on the restore path. A postgres that crashes and is
        // restarted by the loop below has its cache already, and its
        // recovery reads come out of it.
        crate::serve::warm_pages(&args.target, &args.tenant, &pgdata, &pagecache, &stats);
    }

    unsafe {
        let handler = on_signal as extern "C" fn(libc::c_int) as usize;
        libc::signal(libc::SIGINT, handler);
        libc::signal(libc::SIGTERM, handler);
    }

    if let Some(http_port) = http {
        start_http(
            http_port,
            port,
            args.target.clone(),
            args.tenant.clone(),
            project.as_ref(),
        )?;
    }

    if let Some(ops_port) = args.ops {
        start_ops(ops_port)?;
    }

    let mut failed_starts = 0u32;
    loop {
        let ready = Arc::new(AtomicBool::new(false));
        let mut child = Command::new(&postgres)
            .arg("-D")
            .arg(&pgdata)
            .args(["-p", &port.to_string()])
            .arg("-k")
            .arg(&sock)
            .args(["-c", "listen_addresses=127.0.0.1"])
            // What postgres changes reads. See the same line in
            // zou-embed for why it is on from the first boot.
            .args(["-c", "wal_level=logical"])
            // A node holds one slot while anybody is subscribed, so
            // the stock ten is ten nodes on one database. See SLOTS
            // for why the same number is on every boot.
            .args(["-c", &format!("max_replication_slots={}", zou_pg::SLOTS)])
            .args(["-c", &format!("max_wal_senders={}", zou_pg::SLOTS)])
            .args(["-c", &format!("shared_buffers={}", shared_buffers())])
            .env("ZOU_TARGET", &args.target)
            .env("ZOU_TENANT", &args.tenant)
            .env("ZOU_PAGE_CACHE", &pagecache)
            .envs(
                args.page_service
                    .map(|on| ("ZOU_PAGESERVE", if on { "1" } else { "0" })),
            )
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
                        log::info!("try psql -h 127.0.0.1 -p {port} -U postgres -d postgres");
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
        assert_eq!(args.port, None);
        assert_eq!(args.http, None);
        assert_eq!(
            args.ops, None,
            "nothing is scraped unless a port is asked for"
        );
        assert_eq!(args.pg_bin, PathBuf::from("build/pg/bin"));
        assert_eq!(args.tenant, LOCAL, "a store with one database in it");
    }

    #[test]
    fn parse_takes_the_ref_a_branch_was_created_under() {
        let args = parse(&argv(&["s3://bucket/x", "--ref", "pr-142"])).unwrap();
        assert_eq!(args.tenant, "pr-142");
    }

    #[test]
    fn parse_honors_every_flag() {
        let args = parse(&argv(&[
            "s3://bucket/x",
            "--pg-bin",
            "/opt/pg/bin",
            "--port",
            "5614",
            "--http",
            "54321",
            "--ops",
            "9187",
            "--runtime",
            "/tmp/run",
            "--ref",
            "pr-9",
            "--page-service",
            "off",
        ]))
        .unwrap();
        assert_eq!(args.target, "s3://bucket/x");
        assert_eq!(args.pg_bin, PathBuf::from("/opt/pg/bin"));
        assert_eq!(args.port, Some(5614));
        assert_eq!(args.http, Some(54321));
        assert_eq!(args.ops, Some(9187));
        assert_eq!(args.runtime, PathBuf::from("/tmp/run"));
        assert_eq!(args.tenant, "pr-9");
        assert_eq!(args.page_service, Some(false));
    }

    /// The flag is how the two read paths get compared without
    /// exporting anything, and saying nothing leaves the environment
    /// alone so the default, which is on, applies.
    #[test]
    fn parse_reads_the_page_service_flag_both_ways() {
        assert_eq!(parse(&argv(&["./data"])).unwrap().page_service, None);
        for on in ["on", "1", "true", "YES"] {
            let args = parse(&argv(&["./data", "--page-service", on])).unwrap();
            assert_eq!(args.page_service, Some(true), "{on:?}");
        }
        for off in ["off", "0", "false", "No"] {
            let args = parse(&argv(&["./data", "--page-service", off])).unwrap();
            assert_eq!(args.page_service, Some(false), "{off:?}");
        }
        assert!(parse(&argv(&["./data", "--page-service", "maybe"])).is_err());
        assert!(parse(&argv(&["./data", "--page-service"])).is_err());
    }

    #[test]
    fn parse_rejects_noise() {
        assert!(parse(&argv(&[])).is_err());
        assert!(parse(&argv(&["./data", "--port"])).is_err());
        assert!(parse(&argv(&["./data", "--port", "hot"])).is_err());
        assert!(parse(&argv(&["./data", "--http", "cold"])).is_err());
        assert!(parse(&argv(&["./data", "--ops", "warm"])).is_err());
        assert!(parse(&argv(&["./data", "extra"])).is_err());
        assert!(parse(&argv(&["--bogus", "./data"])).is_err());
        assert!(parse(&argv(&["./data", "--config"])).is_err());
    }

    fn project_with(api: Option<u16>, db: Option<u16>) -> Project {
        Project {
            api,
            db,
            ..Default::default()
        }
    }

    #[test]
    fn the_flags_beat_the_file_and_the_file_beats_the_default() {
        let plain = parse(&argv(&["./data"])).unwrap();
        assert_eq!(ports(&plain, None), (5432, None), "a dev loop on its own");
        assert_eq!(
            ports(&plain, Some(&project_with(Some(54321), Some(54322)))),
            (54322, Some(54321)),
            "and a project gets the ports its own tooling already uses"
        );
        let flagged = parse(&argv(&["./data", "--port", "5614", "--http", "8000"])).unwrap();
        assert_eq!(
            ports(&flagged, Some(&project_with(Some(54321), Some(54322)))),
            (5614, Some(8000)),
            "what the command line says wins"
        );
        assert_eq!(
            ports(&plain, Some(&project_with(None, None))),
            (5432, None),
            "a project with the api switched off and no db port says nothing"
        );
    }

    #[test]
    fn a_config_is_looked_for_unless_it_is_named_or_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("supabase")).unwrap();
        std::fs::write(
            dir.path().join("supabase/config.toml"),
            "[api]\nport = 55555\n[db]\nport = 55556\n",
        )
        .unwrap();
        let named = parse(&argv(&[
            "./data",
            "--config",
            dir.path().join("supabase/config.toml").to_str().unwrap(),
        ]))
        .unwrap();
        let read = project(&named)
            .unwrap()
            .expect("the file it was pointed at");
        assert_eq!(ports(&named, Some(&read)), (55556, Some(55555)));
        let refused = parse(&argv(&["./data", "--no-config"])).unwrap();
        assert!(
            project(&refused).unwrap().is_none(),
            "and nothing is looked for when the flag says not to"
        );
        let missing = parse(&argv(&["./data", "--config", "/nowhere/config.toml"])).unwrap();
        assert!(
            project(&missing).is_err(),
            "a file that was named and is not there is an error"
        );
    }
}
