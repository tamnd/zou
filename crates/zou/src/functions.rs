//! The edge functions a project has on disk, and the thing that runs
//! them.
//!
//! Two questions, and they are separate on purpose. Which functions are
//! served is a directory listing and a config file, and no javascript
//! engine is needed to answer it. What runs them is a build time
//! decision that `zou-deno` owns: V8 is fifty megabytes of static
//! library, so `zou` is built without one unless somebody asked for
//! `zou-deno/isolate`, and a binary that has no engine says so at boot
//! rather than at the first call.
//!
//! The environment is the other half. `Deno.env` inside a function is
//! what this module hands the runtime and never the environment this
//! process was started with, which matters because the process is
//! holding a database password and the function is somebody else's
//! code.
//!
//! `zou functions serve` is here too, which is the dev loop for a
//! person writing a function rather than for a whole project: the same
//! `/functions/v1` surface, on its own port, watching the disk. Given a
//! store and a project it serves what was deployed there instead, which
//! is the same bytes a node runs and is how a deploy is checked without
//! standing a node up.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use zou_functions::{Function, Layout, Registry};

/// What every function of this project sees in `Deno.env`, the project
/// wide variables upstream sets.
///
/// One more, `SB_EXECUTION_ID`, is one per invocation and is added by
/// the runtime out of the call rather than living here.
pub fn env(port: u16, anon: &str, service: &str, db: &str) -> Vec<(String, String)> {
    env_at(&format!("http://127.0.0.1:{port}"), anon, service, db)
}

/// The same, for a server that knows its own url rather than a port on
/// loopback: a node serving a project on a domain of its own has to
/// tell that project's functions where the project is, because a
/// function calling `createClient(Deno.env.get("SUPABASE_URL"))` is
/// calling the api it was deployed next to and not the machine it
/// happens to be running on.
///
/// The two maps at the end are the newer names, and they are here
/// because a function that reads them is most of the Supabase examples
/// project now: `npm:@supabase/server` builds a client before the
/// handler runs, and with no key to build it with it refuses before
/// anybody's code has said anything. It reads `SUPABASE_PUBLISHABLE_KEY`
/// first and falls back to a `default` entry in the plural, which is
/// the shape `supabase start` sets, so that is the shape set here.
///
/// The values are this project's anon and service_role keys rather than
/// `sb_publishable_` and `sb_secret_` strings, because zou does not
/// issue those and inventing the prefix would be a claim about a format
/// nothing here implements. A library treats the value as opaque and
/// sends it back as the apikey, which is a key this server accepts.
///
/// The last one is where the project publishes the public half of its
/// signing keys. A function that asks who called it verifies the access
/// token itself rather than asking the server, and the same library
/// reads this variable to find out what to verify against. The url and
/// not the key set inline, because a url survives a key rotation and
/// hands out no key material at all: the endpoint it names is the one
/// endpoint of the whole surface that needs no apikey, for exactly this
/// reason.
pub fn env_at(url: &str, anon: &str, service: &str, db: &str) -> Vec<(String, String)> {
    vec![
        ("SUPABASE_URL".to_string(), url.to_string()),
        ("SUPABASE_ANON_KEY".to_string(), anon.to_string()),
        ("SUPABASE_SERVICE_ROLE_KEY".to_string(), service.to_string()),
        ("SUPABASE_DB_URL".to_string(), db.to_string()),
        ("SUPABASE_PUBLISHABLE_KEYS".to_string(), default_key(anon)),
        ("SUPABASE_SECRET_KEYS".to_string(), default_key(service)),
        (
            "SUPABASE_JWKS_URL".to_string(),
            format!("{url}/auth/v1/.well-known/jwks.json"),
        ),
    ]
}

/// One key, in the map of named keys a project can have. Written by
/// hand rather than through serde because it is one string in one
/// object and the escaping a key needs is none: these are base64url
/// with dots in, or `sb_`-prefixed, and neither has a quote in it.
fn default_key(key: &str) -> String {
    format!("{{\"default\":\"{key}\"}}")
}

/// What this build runs functions with, in the words the log and
/// `zou status` both print.
///
/// Asking a runtime rather than writing the sentence twice, because the
/// two answers must not be able to drift: the whole point of the line
/// is telling an operator which binary they are running.
pub fn engine_describe(policy: zou_functions::Policy) -> String {
    zou_deno::engine(Vec::new(), policy, None).describe()
}

/// The functions under `dir`, ready to be served, or None when the
/// project has none.
///
/// None and an empty registry are the same thing to a caller, so the
/// simpler of the two goes to the server: a project with no functions
/// directory is not carrying a runtime around.
pub fn registry(
    dir: &Path,
    layout: &zou_functions::Layout,
    env: Vec<(String, String)>,
) -> Result<Option<Arc<Registry>>, String> {
    let found = zou_functions::read(dir, layout)?;
    if found.is_empty() {
        return Ok(None);
    }
    for function in &found {
        // The line `supabase functions serve` prints, which is how a
        // project checks that the name it is about to call is one this
        // server agreed to serve.
        log::info!(
            "function {} at {}",
            function.name,
            function.entrypoint.display()
        );
    }
    // A project's own secrets go in first and the four above go in over
    // them, which is the order the CLI hands them to docker. Nothing can
    // actually collide there, because the four all start with
    // `SUPABASE_` and that prefix is the one thing a project's `.env` is
    // not allowed to set, but the stack is written this way round so
    // that reading it does not depend on knowing that.
    let mut all = zou_functions::secrets(dir, layout)?;
    if !all.is_empty() {
        // The names and not the values: this is a boot log, and the
        // question an operator has is whether the `.env` beside the
        // functions arrived at all.
        let names: Vec<&str> = all.iter().map(|(name, _)| name.as_str()).collect();
        log::info!("function secrets: {}", names.join(", "));
    }
    all.extend(env);
    let registry = Registry::new(
        found,
        zou_deno::engine(all, layout.policy, layout.inspector_port),
    );
    log::info!("functions run on {}", registry.describe());
    if !zou_deno::available() {
        log::warn!(
            "this build has no javascript engine, every function above answers 500 until it is rebuilt with --features zou-deno/isolate"
        );
    }
    Ok(Some(Arc::new(registry)))
}

/// The secret a dev loop signs with, and the two keys it mints out of
/// it.
///
/// `ZOU_JWT_SECRET` when the caller pinned one, so the keys stay the
/// same across restarts and across the two commands that serve
/// functions, and a fresh one otherwise, logged next to the keys it
/// signs, which is enough for a laptop.
pub fn keys() -> Result<(String, String, String), String> {
    let secret = match std::env::var("ZOU_JWT_SECRET") {
        Ok(s) if !s.is_empty() => s,
        _ => {
            let mut raw = [0u8; 32];
            getrandom::fill(&mut raw).map_err(|e| format!("random secret: {e}"))?;
            let hex: String = raw.iter().map(|b| format!("{b:02x}")).collect();
            log::info!("generated a jwt secret, pin ZOU_JWT_SECRET={hex} to keep keys stable");
            hex
        }
    };
    let anon = zou_server::jwt::mint(&zou_server::jwt::key_claims("anon"), secret.as_bytes());
    let service = zou_server::jwt::mint(
        &zou_server::jwt::key_claims("service_role"),
        secret.as_bytes(),
    );
    Ok((secret, anon, service))
}

pub const USAGE: &str = "usage: zou functions <serve [--port <n>] [--env-file <path>] [--import-map <path>] [--no-verify-jwt] [--inspect [<port>]] [--target <store> --ref <tenant>] | deploy [<name>...] [--target <store>] [--ref <tenant>] [--import-map <path>] [--no-verify-jwt] | list [--target <store>] [--ref <tenant>]> [--config <config.toml> | --no-config]";

/// Where upstream's local api answers, which is where a project's
/// client library already looks for `/functions/v1`.
const DEFAULT_PORT: u16 = 54321;

/// The database a function is told about when the project's file does
/// not name a port, which is the one `supabase start` opens.
const DEFAULT_DB_PORT: u16 = 54322;

/// Where a debugger attaches when `--inspect` asked for one and neither
/// the flag nor the config file named a port. Upstream's
/// `dockerRuntimeInspectorPort`.
const DEFAULT_INSPECTOR_PORT: u16 = 8083;

/// How often the disk is looked at.
///
/// Upstream watches with fsnotify and debounces for half a second
/// before it restarts its container, so half a second is the latency a
/// person writing a function already expects, and asking the directory
/// for its listing that often costs nothing next to what is being
/// waited for.
const POLL: Duration = Duration::from_millis(500);

pub struct Serve {
    /// The port the functions surface answers on. Upstream serves them
    /// through the api port, so this defaults to the project's own api
    /// port when it has a file, and to upstream's 54321 when it does
    /// not.
    pub port: Option<u16>,
    /// Upstream's `--env-file`: the dotenv file the functions'
    /// environment is read out of, instead of the one beside them.
    pub env_file: Option<PathBuf>,
    /// Upstream's `--import-map`, which beats every function's own.
    pub import_map: Option<PathBuf>,
    /// Upstream's `--no-verify-jwt`, which is every function of this
    /// run and not one of them.
    pub no_verify_jwt: bool,
    /// Upstream's `--inspect`, and this loop's port to go with it,
    /// because upstream's port is a container's and this one's is the
    /// config file's.
    pub inspect: bool,
    pub inspect_port: Option<u16>,
    /// The store to serve a deployment out of, and which project on it.
    /// Neither is upstream's, because upstream's dev loop only ever
    /// serves a directory. Naming either of them here serves what `zou
    /// functions deploy` wrote rather than what is on this disk, which
    /// is the bytes a node runs.
    ///
    /// Only the flags switch it, and not `ZOU_TARGET` in the
    /// environment: a person with a store exported who runs the dev
    /// loop in their project means the project.
    pub target: Option<String>,
    pub tenant: Option<String>,
    pub config: Option<PathBuf>,
    pub no_config: bool,
}

pub fn parse(argv: &[String]) -> Result<Serve, String> {
    let mut args = Serve {
        port: None,
        env_file: None,
        import_map: None,
        no_verify_jwt: false,
        inspect: false,
        inspect_port: None,
        target: None,
        tenant: None,
        config: None,
        no_config: false,
    };
    let mut it = argv.iter().peekable();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--port" => {
                let raw = it.next().ok_or("--port needs a value")?;
                args.port = Some(raw.parse().map_err(|_| {
                    format!("bad port {raw:?}, write a port number from 1 to 65535")
                })?);
            }
            "--env-file" => {
                let raw = it.next().ok_or("--env-file needs a value")?;
                args.env_file = Some(PathBuf::from(raw));
            }
            "--import-map" => {
                let raw = it.next().ok_or("--import-map needs a value")?;
                args.import_map = Some(PathBuf::from(raw));
            }
            "--no-verify-jwt" => args.no_verify_jwt = true,
            "--inspect" => {
                args.inspect = true;
                // The port is optional, the way it is optional on
                // node's own `--inspect`, so what follows is only taken
                // as one if it looks like nothing else.
                if let Some(next) = it.peek()
                    && let Ok(port) = next.parse::<u16>()
                {
                    args.inspect_port = Some(port);
                    it.next();
                }
            }
            "--target" => args.target = Some(it.next().ok_or("--target needs a value")?.clone()),
            "--ref" => args.tenant = Some(it.next().ok_or("--ref needs a value")?.clone()),
            "--config" => {
                let raw = it.next().ok_or("--config needs a value")?;
                args.config = Some(PathBuf::from(raw));
            }
            "--no-config" => args.no_config = true,
            other => return Err(format!("unexpected argument {other:?}\n{USAGE}")),
        }
    }
    Ok(args)
}

pub fn run(argv: &[String]) -> Result<(), String> {
    match argv.first().map(String::as_str) {
        Some("serve") => serve(&parse(&argv[1..])?),
        Some("deploy") => deploy(&parse_deploy(&argv[1..])?),
        Some("list") => list(&parse_deploy(&argv[1..])?),
        Some(other) => Err(format!("unknown functions command {other:?}\n{USAGE}")),
        None => Err(USAGE.to_string()),
    }
}

/// `zou functions deploy`, and `zou functions list` which is the same
/// arguments without the names.
pub struct Deploy {
    /// Which functions, and none of them meaning all of them, which is
    /// what `supabase functions deploy` with no slug does.
    pub names: Vec<String>,
    /// The store the project lives on, or `ZOU_TARGET`.
    pub target: Option<String>,
    /// Which project on it, or `ZOU_TENANT`, or the config file's
    /// `project_id`, which is the same field upstream's
    /// `--project-ref` fills in.
    pub tenant: Option<String>,
    /// The same two flags `serve` has, for the same reason: they are
    /// this run's and not one function's.
    pub import_map: Option<PathBuf>,
    pub no_verify_jwt: bool,
    pub config: Option<PathBuf>,
    pub no_config: bool,
}

pub fn parse_deploy(argv: &[String]) -> Result<Deploy, String> {
    let mut args = Deploy {
        names: Vec::new(),
        target: None,
        tenant: None,
        import_map: None,
        no_verify_jwt: false,
        config: None,
        no_config: false,
    };
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--target" => args.target = Some(it.next().ok_or("--target needs a value")?.clone()),
            "--ref" => args.tenant = Some(it.next().ok_or("--ref needs a value")?.clone()),
            "--import-map" => {
                let raw = it.next().ok_or("--import-map needs a value")?;
                args.import_map = Some(PathBuf::from(raw));
            }
            "--no-verify-jwt" => args.no_verify_jwt = true,
            "--config" => {
                let raw = it.next().ok_or("--config needs a value")?;
                args.config = Some(PathBuf::from(raw));
            }
            "--no-config" => args.no_config = true,
            other if other.starts_with('-') => {
                return Err(format!("unexpected argument {other:?}\n{USAGE}"));
            }
            name => args.names.push(name.to_string()),
        }
    }
    Ok(args)
}

/// The project and the prefix a deploy is between: where the functions
/// are on this disk, what the config file says about them, which store
/// they go to and which project on it.
#[derive(Debug)]
struct Bound {
    dir: PathBuf,
    layout: Layout,
    target: String,
    tenant: String,
}

/// Which store, and which project on it, for the commands that talk to
/// one: `zou functions deploy`, `zou functions list` and `zou secrets`.
///
/// One function rather than one per command, because the precedence is
/// the thing a person learns once and then expects everywhere.
/// `--target` or `ZOU_TARGET` for the store, and for the project
/// `--ref`, then `ZOU_TENANT`, then the config file's `project_id`,
/// which is the field upstream's `--project-ref` fills in. The file
/// last, because it is the one of the three that a project shares with
/// everybody it hands the directory to, and a command naming a ref out
/// loud should beat it.
pub fn place(
    target: Option<&str>,
    tenant: Option<&str>,
    project: Option<&crate::config::Project>,
) -> Result<(String, String), String> {
    let target = target
        .map(str::to_string)
        .or_else(|| std::env::var("ZOU_TARGET").ok().filter(|t| !t.is_empty()))
        .ok_or("no store: pass --target or set ZOU_TARGET")?;
    let tenant = tenant
        .map(str::to_string)
        .or_else(|| std::env::var("ZOU_TENANT").ok().filter(|t| !t.is_empty()))
        .or_else(|| project.and_then(|p| p.id.clone()))
        .ok_or("no project: pass --ref, set ZOU_TENANT, or give the config file a project_id")?;
    zou_store::registry::check_ref(&tenant).map_err(|e| format!("--ref {tenant:?}: {e}"))?;
    Ok((target, tenant))
}

/// Work out all four, saying which one is missing rather than failing
/// on the first thing that needs it.
fn bind(args: &Deploy) -> Result<Bound, String> {
    let project = crate::config::project(args.config.as_deref(), args.no_config)?;
    let dir = match &project {
        Some(project) => project.dir(),
        None => std::env::current_dir().map_err(|e| format!("cwd: {e}"))?,
    };
    let (target, tenant) = place(
        args.target.as_deref(),
        args.tenant.as_deref(),
        project.as_ref(),
    )?;
    let mut layout = project.map(|p| p.functions.clone()).unwrap_or_default();
    // Upstream's precedence, and the same two flags `serve` applies:
    // what is on the command line is every function of this run.
    for name in zou_functions::read(&dir, &layout)?.iter().map(|f| &f.name) {
        let mut settings = layout.settings(name);
        if args.no_verify_jwt {
            settings.verify_jwt = false;
        }
        if let Some(map) = &args.import_map {
            settings.import_map = Some(map.display().to_string());
        }
        layout.settings.insert(name.clone(), settings);
    }
    Ok(Bound {
        dir,
        layout,
        target,
        tenant,
    })
}

fn deploy(args: &Deploy) -> Result<(), String> {
    let bound = bind(args)?;
    let store = zou_store::open_store(&bound.target)?;
    let published = crate::bundle::publish(
        store.as_ref(),
        &bound.tenant,
        &bound.dir,
        &bound.layout,
        &args.names,
    )?;
    println!(
        "deployed {} to {} on {}",
        published.names.join(", "),
        bound.tenant,
        bound.target
    );
    println!(
        "{} files, {} of them new, {} bytes uploaded",
        published.files, published.written, published.bytes
    );
    // The name a caller uses, because that is the question a deploy
    // leaves somebody with, and it is the project's url rather than
    // this machine's.
    for name in &published.names {
        println!("  /functions/v1/{name}");
    }
    Ok(())
}

/// What is deployed to this project right now, which is the other half
/// of a deploy: a person wants to know what a node would run before
/// they change it.
fn list(args: &Deploy) -> Result<(), String> {
    let bound = bind(args)?;
    let store = zou_store::open_store(&bound.target)?;
    let Some(deployment) = crate::bundle::fetch(store.as_ref(), &bound.tenant)? else {
        println!("nothing is deployed to {}", bound.tenant);
        return Ok(());
    };
    println!(
        "{} {} deployed to {}",
        deployment.functions.len(),
        if deployment.functions.len() == 1 {
            "function"
        } else {
            "functions"
        },
        bound.tenant
    );
    for function in &deployment.functions {
        println!(
            "  {} at {}, {} files{}",
            function.name,
            function.entrypoint,
            function.files.len(),
            if function.verify_jwt {
                ""
            } else {
                ", no jwt verification"
            }
        );
    }
    Ok(())
}

/// The listing and the environment as the disk has them right now,
/// which is the pair the watch loop compares against itself.
struct Disk {
    functions: Vec<Function>,
    env: Vec<(String, String)>,
}

/// Read both, with the command line having the last word over the
/// config file, which is upstream's precedence: flag, then config, then
/// what is beside the function.
fn disk(
    dir: &Path,
    layout: &Layout,
    args: &Serve,
    project: &[(String, String)],
) -> Result<Disk, String> {
    let mut functions = zou_functions::read(dir, layout)?;
    for function in &mut functions {
        if args.no_verify_jwt {
            function.verify_jwt = false;
        }
        if let Some(map) = &args.import_map {
            function.import_map = Some(map.clone());
        }
    }
    let mut env = zou_functions::secrets_from(&env_file(dir, args), layout)?;
    env.extend(project.iter().cloned());
    Ok(Disk { functions, env })
}

/// The dotenv file this run reads, which is the one named on the
/// command line or the one upstream falls back to beside the functions.
fn env_file(dir: &Path, args: &Serve) -> PathBuf {
    match &args.env_file {
        Some(path) => path.clone(),
        None => zou_functions::env_file(dir),
    }
}

/// What the config file says about functions, with the flags applied to
/// it. `--inspect` is the only one that belongs here rather than on
/// each function: a debugger is a port the runtime opens once.
fn layout(args: &Serve, project: Option<&crate::config::Project>) -> Layout {
    let mut layout = project.map(|p| p.functions.clone()).unwrap_or_default();
    if args.inspect {
        layout.inspector_port = Some(
            args.inspect_port
                .or(layout.inspector_port)
                .unwrap_or(DEFAULT_INSPECTOR_PORT),
        );
    }
    layout
}

fn serve(args: &Serve) -> Result<(), String> {
    let project = crate::config::project(args.config.as_deref(), args.no_config)?;
    // Where the `functions` directory is, which is beside the config
    // file when there is one and beside the caller when there is not.
    let dir = match &project {
        Some(project) => project.dir(),
        None => std::env::current_dir().map_err(|e| format!("cwd: {e}"))?,
    };
    let layout = layout(args, project.as_ref());
    let port = args
        .port
        .or_else(|| project.as_ref().and_then(|p| p.api))
        .unwrap_or(DEFAULT_PORT);
    let db_port = project
        .as_ref()
        .and_then(|p| p.db)
        .unwrap_or(DEFAULT_DB_PORT);
    if let Some(named) = &args.env_file
        && !named.is_file()
    {
        return Err(format!("{} is not a file", named.display()));
    }
    let (secret, anon, service) = keys()?;
    let own = env(
        port,
        &anon,
        &service,
        &format!(
            "postgresql://{}@127.0.0.1:{db_port}/postgres",
            crate::dev::SUPERUSER
        ),
    );
    // Where the functions come from: this disk, or a store, and the
    // second is the same read a node does at attach.
    let attached = match args.target.is_some() || args.tenant.is_some() {
        true => Some(attach(args, project.as_ref(), port, &own)?),
        false => None,
    };
    let (dir, layout, disk) = match attached {
        Some(attached) => (attached.dir, attached.layout, attached.disk),
        None => {
            let disk = disk(&dir, &layout, args, &own)?;
            (dir, layout, disk)
        }
    };
    let deployed = args.target.is_some() || args.tenant.is_some();
    announce(&disk.functions);
    let registry = Arc::new(Registry::new(
        disk.functions.clone(),
        zou_deno::engine(disk.env.clone(), layout.policy, layout.inspector_port),
    ));
    log::info!("functions run on {}", registry.describe());
    if !zou_deno::available() {
        log::warn!(
            "this build has no javascript engine, every function above answers 500 until it is rebuilt with --features zou-deno/isolate"
        );
    }
    let listener = std::net::TcpListener::bind(("127.0.0.1", port))
        .map_err(|e| format!("bind http on 127.0.0.1:{port}: {e}"))?;
    log::info!("serving functions on http://127.0.0.1:{port}");
    for url in zou_server::functions::served(&registry) {
        log::info!("  http://127.0.0.1:{port}{url}");
    }
    log::info!("anon key {anon}");
    log::info!("service_role key {service}");
    // The rest of the api is dialled lazily, so a `zou dev` on the
    // project's own db port is a database these functions can reach
    // through the client library, and no database at all is a function
    // that runs and a `/rest/v1` that says so.
    log::info!("sql goes to 127.0.0.1:{db_port}, which is whatever is serving the project");
    let cfg = zou_server::Config {
        // The keys the dev loop next door signs with, which this
        // process arrives at from the same secret: a gateway that
        // could not verify the access tokens the api it belongs to
        // issues would refuse every signed in caller at the door.
        jwt_keys: Some(zou_server::jwt::derived_keys(secret.as_bytes())),
        jwt_secret: secret.into_bytes(),
        pg: Some(format!(
            "host=127.0.0.1 port={db_port} user={} dbname=postgres",
            crate::dev::SUPERUSER
        )),
        external_url: Some(format!("http://127.0.0.1:{port}")),
        functions: Some(Arc::clone(&registry)),
        ..Default::default()
    };
    let (failed, told) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        if let Err(e) = zou_server::serve_blocking(listener, cfg) {
            let _ = failed.send(e);
        }
    });
    // A deployment is not watched. The files under it are a copy of
    // what a store holds, so editing one changes nothing anybody
    // deployed, and the way a deployment changes is another deploy and
    // another serve. The dev loop watches a project, which is the
    // directory somebody is actually typing in.
    match deployed {
        true => park(told),
        false => watch(
            args,
            &registry,
            Watched {
                dir,
                config: project.map(|p| p.path),
                layout,
                own,
                last: disk,
            },
            told,
        ),
    }
}

/// Serve until the http server stops or somebody interrupts, which is
/// what a serve with nothing to watch does.
fn park(told: std::sync::mpsc::Receiver<String>) -> Result<(), String> {
    match told.recv() {
        Ok(e) => Err(format!("http server: {e}")),
        Err(_) => Err("the http server stopped".to_string()),
    }
}

/// A deployment, read out of a store and written down as a project
/// directory this process can serve out of.
struct Attached {
    dir: PathBuf,
    layout: Layout,
    disk: Disk,
}

/// Read what is deployed to a project, the way a node reads it at
/// attach: the same `materialize`, the same listing reader over what it
/// wrote, and the project's own secrets out of its own prefix.
///
/// The files land in a directory named for the project and the port, so
/// a second serve of the same deployment on another port has its own
/// copy and a repeat of this one reuses the name rather than leaving a
/// pile of them behind. Removed first, because what a store says now is
/// the whole answer and a file left over from a deployment somebody has
/// since replaced is not part of it.
fn attach(
    args: &Serve,
    project: Option<&crate::config::Project>,
    port: u16,
    own: &[(String, String)],
) -> Result<Attached, String> {
    let (target, tenant) = place(args.target.as_deref(), args.tenant.as_deref(), project)?;
    let store = zou_store::open_store(&target)?;
    let into = std::env::temp_dir().join(format!("zou-deployed-{tenant}-{port}"));
    if into.exists() {
        std::fs::remove_dir_all(&into).map_err(|e| format!("remove {}: {e}", into.display()))?;
    }
    let Some((dir, layout)) = crate::bundle::materialize(store.as_ref(), &tenant, &into)? else {
        return Err(format!("nothing is deployed to {tenant} on {target}"));
    };
    log::info!("serving what is deployed to {tenant} on {target}");
    log::info!("its files are at {}", dir.display());
    let mut functions = zou_functions::read(&dir, &layout)?;
    for function in &mut functions {
        // The two flags a serve applies to every function of the run.
        // `--no-verify-jwt` on a deployment is a local decision about a
        // deployed project rather than a change to it, which is worth
        // having: it is how somebody calls a deployed function by hand
        // without minting a token first.
        if args.no_verify_jwt {
            function.verify_jwt = false;
        }
        if let Some(map) = &args.import_map {
            function.import_map = Some(map.clone());
        }
    }
    // The project's own environment first and the four above over it,
    // which is the order a node stacks them in. A deployment's secrets
    // come out of the store and never off this disk: the `.env` beside
    // a project is the one file a deploy does not carry.
    let mut env = secrets(store.as_ref(), &tenant)?;
    env.extend(own.iter().cloned());
    Ok(Attached {
        dir,
        layout,
        disk: Disk { functions, env },
    })
}

/// What a deployed project's functions are told, out of the sealed
/// object in its own prefix. The same read the node does, and the same
/// refusal: a project that has secrets and a process with no key to
/// open them serves nothing, because a function running without the
/// environment it was written against is a function calling somebody
/// else's api with an empty token.
fn secrets(store: &dyn zou_store::CasStore, tenant: &str) -> Result<Vec<(String, String)>, String> {
    if !crate::secrets::present(store, tenant)? {
        return Ok(Vec::new());
    }
    let key = crate::secrets::Key::from_env()?.ok_or(
        "this project has function secrets and this process has no key to open them with, set ZOU_SECRET_KEY",
    )?;
    let all = crate::secrets::read(store, tenant, &key)?;
    let names: Vec<&str> = all.keys().map(String::as_str).collect();
    log::info!("function secrets: {}", names.join(", "));
    Ok(all.into_iter().collect())
}

/// What the loop below is looking at: where the functions are, which
/// file says what about them, and what it made of both last time round.
struct Watched {
    dir: PathBuf,
    config: Option<PathBuf>,
    layout: Layout,
    own: Vec<(String, String)>,
    last: Disk,
}

/// The disk, in a loop, for as long as this command runs.
///
/// What is watched is the listing and the environment, and not the
/// files a function is made of. Those are the isolate's own business:
/// a kept isolate records every file off this disk that went into it
/// and ends itself when one of them changes, which is hot reload and is
/// where a source edit is noticed. What cannot be noticed there is a
/// function that did not exist when the isolate was built, or a secret
/// that changed under all of them, and that is what is here.
fn watch(
    args: &Serve,
    registry: &Registry,
    mut watched: Watched,
    told: std::sync::mpsc::Receiver<String>,
) -> Result<(), String> {
    let mut written = stamp(watched.config.as_deref());
    loop {
        match told.try_recv() {
            Ok(e) => return Err(format!("http server: {e}")),
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                return Err("the http server stopped".to_string());
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
        }
        std::thread::sleep(POLL);
        let now = stamp(watched.config.as_deref());
        if now != written {
            written = now;
            watched.reread(args);
        }
        if let Err(e) = watched.tick(args, registry) {
            // A directory that cannot be read is worth hearing about
            // and is not worth stopping for: it is usually a file being
            // written, and the next turn round this loop reads it
            // whole.
            log::warn!("{e}");
        }
    }
}

impl Watched {
    /// The config file changed, so what it says about functions is read
    /// again. Everything below is read through it: a block that
    /// switches a function off takes it out of the listing, and a
    /// secret in it is part of the environment.
    ///
    /// The ports are not read again. This process is already listening
    /// on one and already telling every function about the other, and
    /// moving either of those under a running server is a restart
    /// rather than a reload.
    fn reread(&mut self, args: &Serve) {
        let Some(path) = &self.config else { return };
        match crate::config::Project::read(path) {
            Ok(project) => {
                log::info!("{} changed", path.display());
                self.layout = layout(args, Some(&project));
            }
            // Half a file, most likely, and the stamp has moved on, so
            // the next write of it is read.
            Err(e) => log::warn!("{e}"),
        }
    }

    /// One look at the disk, and whatever of it the registry has not
    /// been told yet.
    fn tick(&mut self, args: &Serve, registry: &Registry) -> Result<(), String> {
        let found = disk(&self.dir, &self.layout, args, &self.own)?;
        if found.env != self.last.env {
            log::info!("the functions environment changed, every kept isolate is thrown away");
            registry.run_on(zou_deno::engine(
                found.env.clone(),
                self.layout.policy,
                self.layout.inspector_port,
            ));
        }
        if found.functions != self.last.functions {
            announce(&found.functions);
            registry.reload(found.functions.clone());
        }
        self.last = found;
        Ok(())
    }
}

/// The mtime and the size of a file, which is as much as a poll needs
/// to know that it is worth reading again.
fn stamp(path: Option<&Path>) -> Option<(SystemTime, u64)> {
    let meta = std::fs::metadata(path?).ok()?;
    Some((meta.modified().ok()?, meta.len()))
}

/// The line `supabase functions serve` prints per function, which is
/// how a project checks that the name it is about to call is one this
/// server agreed to serve.
fn announce(functions: &[Function]) {
    if functions.is_empty() {
        log::warn!("no functions to serve");
    }
    for function in functions {
        log::info!(
            "function {} at {}",
            function.name,
            function.entrypoint.display()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project(names: &[&str]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        for name in names {
            let at = dir.path().join("functions").join(name);
            std::fs::create_dir_all(&at).expect("mkdir");
            std::fs::write(
                at.join("index.ts"),
                "Deno.serve(() => new Response(\"hi\"))",
            )
            .expect("write");
        }
        dir
    }

    fn call() -> zou_functions::Call {
        zou_functions::Call {
            method: "GET".to_string(),
            url: "http://127.0.0.1:54321/functions/v1/hello".to_string(),
            headers: Vec::new(),
            body: Vec::new(),
            execution_id: "one".to_string(),
        }
    }

    #[test]
    fn a_project_with_no_functions_carries_no_runtime() {
        let dir = tempfile::tempdir().expect("tempdir");
        let none =
            registry(dir.path(), &zou_functions::Layout::default(), Vec::new()).expect("read");
        assert!(none.is_none(), "nothing to serve is nothing to carry");
    }

    #[test]
    fn the_functions_on_disk_are_the_ones_served() {
        let dir = project(&["hello", "open"]);
        let served = registry(dir.path(), &zou_functions::Layout::default(), Vec::new())
            .expect("read")
            .expect("a registry");
        assert_eq!(served.names(), ["hello", "open"]);
    }

    #[test]
    fn the_variables_a_server_owns_are_the_projects_own() {
        let env = env(54321, "an-anon-key", "a-service-key", "postgres://x/y");
        assert_eq!(
            env,
            vec![
                (
                    "SUPABASE_URL".to_string(),
                    "http://127.0.0.1:54321".to_string()
                ),
                ("SUPABASE_ANON_KEY".to_string(), "an-anon-key".to_string()),
                (
                    "SUPABASE_SERVICE_ROLE_KEY".to_string(),
                    "a-service-key".to_string()
                ),
                ("SUPABASE_DB_URL".to_string(), "postgres://x/y".to_string()),
                (
                    "SUPABASE_PUBLISHABLE_KEYS".to_string(),
                    "{\"default\":\"an-anon-key\"}".to_string()
                ),
                (
                    "SUPABASE_SECRET_KEYS".to_string(),
                    "{\"default\":\"a-service-key\"}".to_string()
                ),
                (
                    "SUPABASE_JWKS_URL".to_string(),
                    "http://127.0.0.1:54321/auth/v1/.well-known/jwks.json".to_string()
                ),
            ]
        );
        assert!(
            !env.iter().any(|(name, _)| name == "SB_EXECUTION_ID"),
            "the per invocation one is the call's and not the project's"
        );
    }

    /// The map is what `npm:@supabase/server` reads, so what matters
    /// about it is that a json parse of it has a `default` in it.
    #[test]
    fn the_newer_names_are_a_map_with_a_default_in_it() {
        let env = env(54321, "an-anon-key", "a-service-key", "postgres://x/y");
        for (name, key) in [
            ("SUPABASE_PUBLISHABLE_KEYS", "an-anon-key"),
            ("SUPABASE_SECRET_KEYS", "a-service-key"),
        ] {
            let raw = env
                .iter()
                .find(|(seen, _)| seen == name)
                .map(|(_, value)| value.as_str())
                .unwrap_or_else(|| panic!("{name} is set"));
            let keys: std::collections::BTreeMap<String, String> =
                serde_json::from_str(raw).unwrap_or_else(|e| panic!("{name} is json: {e}"));
            assert_eq!(keys.get("default").map(String::as_str), Some(key));
        }
    }

    /// A deployed function is told where its own project publishes its
    /// keys, not where the node it happens to be running on does, for
    /// the same reason `SUPABASE_URL` is the project's: a function
    /// verifying a caller against another project's key set verifies
    /// nothing.
    #[test]
    fn the_jwks_url_is_the_projects_own() {
        let env = env_at(
            "https://demo.zou.example",
            "an-anon-key",
            "a-service-key",
            "postgres://x/y",
        );
        let url = env
            .iter()
            .find(|(name, _)| name == "SUPABASE_JWKS_URL")
            .map(|(_, value)| value.as_str())
            .expect("SUPABASE_JWKS_URL is set");
        assert_eq!(
            url,
            "https://demo.zou.example/auth/v1/.well-known/jwks.json"
        );
    }

    #[test]
    fn a_project_reaches_its_own_secrets_and_nothing_of_the_hosts() {
        let dir = project(&["hello"]);
        std::fs::write(
            dir.path().join("functions").join("hello").join("index.ts"),
            "Deno.serve(() => new Response(JSON.stringify(Deno.env.toObject())))",
        )
        .expect("write");
        std::fs::write(
            zou_functions::env_file(dir.path()),
            "GREETING=from the file\nSHARED=the file wins\n",
        )
        .expect("write");
        let mut layout = zou_functions::Layout::default();
        layout
            .secrets
            .insert("SHARED".to_string(), "the block loses".to_string());
        layout
            .secrets
            .insert("ONLY_IN_THE_BLOCK".to_string(), "here".to_string());
        let served = registry(
            dir.path(),
            &layout,
            env(54321, "an-anon-key", "a-service-key", "postgres://x/y"),
        )
        .expect("read")
        .expect("a registry");
        let hello = served.lookup("hello").expect("served");
        let Ok(answer) = served.invoke(&hello, call()) else {
            return; // A build with no engine has nothing to ask.
        };
        let seen: std::collections::BTreeMap<String, String> =
            serde_json::from_slice(answer.bytes()).expect("an object");
        assert_eq!(
            seen.get("GREETING").map(String::as_str),
            Some("from the file")
        );
        assert_eq!(
            seen.get("SHARED").map(String::as_str),
            Some("the file wins")
        );
        assert_eq!(
            seen.get("ONLY_IN_THE_BLOCK").map(String::as_str),
            Some("here")
        );
        assert_eq!(
            seen.get("SUPABASE_ANON_KEY").map(String::as_str),
            Some("an-anon-key"),
            "the four the server owns are still there underneath"
        );
        assert!(
            !seen.contains_key("PATH") && !seen.contains_key("HOME"),
            "the process this function is running inside is not its environment: {seen:?}"
        );
    }

    /// A deploy and then a serve of what it wrote, which is the round
    /// trip a node does and the one thing about a deployment a person
    /// can check without standing a node up.
    #[test]
    fn a_serve_pointed_at_a_store_serves_what_was_deployed() {
        let dir = project(&["hello", "world"]);
        let store = tempfile::tempdir().expect("tempdir");
        let target = store.path().display().to_string();
        let opened = zou_store::open_store(&target).expect("store");
        let mut layout = Layout::default();
        layout.settings.insert(
            "world".to_string(),
            zou_functions::Settings {
                verify_jwt: false,
                ..zou_functions::Settings::default()
            },
        );
        crate::bundle::publish(opened.as_ref(), "acme", dir.path(), &layout, &[]).expect("deploy");

        let args = parse(&argv(&[
            "--target",
            &target,
            "--ref",
            "acme",
            "--no-config",
        ]))
        .expect("a serve out of a store");
        let own = env(54321, "an-anon-key", "a-service-key", "postgres://x/y");
        let attached = attach(&args, None, 54321, &own).expect("attach");
        let names: Vec<&str> = attached
            .disk
            .functions
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(names, ["hello", "world"]);
        assert!(attached.disk.functions[0].verify_jwt);
        assert!(
            !attached.disk.functions[1].verify_jwt,
            "what the project said about a function is deployed with it"
        );
        assert!(
            attached.disk.functions[0]
                .entrypoint
                .starts_with(&attached.dir),
            "the files a deployment is served out of are the ones it wrote"
        );
        assert!(
            attached
                .disk
                .env
                .iter()
                .any(|(name, value)| name == "SUPABASE_ANON_KEY" && value == "an-anon-key"),
            "and the four a server owns are on top of what the project deployed"
        );
    }

    #[test]
    fn a_serve_of_a_project_nobody_deployed_to_says_so() {
        let store = tempfile::tempdir().expect("tempdir");
        let target = store.path().display().to_string();
        let args = parse(&argv(&[
            "--target",
            &target,
            "--ref",
            "acme",
            "--no-config",
        ]))
        .expect("a serve out of a store");
        let Err(e) = attach(&args, None, 54321, &[]) else {
            panic!("a serve of a project with no deployment has nothing to serve");
        };
        assert!(e.contains("nothing is deployed to acme"), "{e}");
    }

    /// The engine is a build time choice, so one of these two runs and
    /// the other is compiled out, and both are worth asserting because
    /// the wrong one silently is the failure this is guarding against.
    #[test]
    fn what_answers_a_call_is_what_this_build_has() {
        let dir = project(&["hello"]);
        let served = registry(dir.path(), &zou_functions::Layout::default(), Vec::new())
            .expect("read")
            .expect("a registry");
        let hello = served.lookup("hello").expect("served");
        let ran = served.invoke(&hello, call());
        if zou_deno::available() {
            let answer = ran.expect("an isolate ran it");
            assert_eq!(answer.status, 200);
            assert_eq!(answer.bytes(), b"hi");
        } else {
            let complaint = ran.expect_err("no engine to run it");
            assert!(complaint.why().contains("zou-deno/isolate"), "{complaint}");
        }
    }

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_takes_nothing_and_defaults_everything() {
        let args = parse(&argv(&[])).expect("no flags is a whole command");
        assert_eq!(args.port, None, "the project's own api port, or 54321");
        assert!(args.env_file.is_none() && args.import_map.is_none());
        assert!(!args.no_verify_jwt, "upstream verifies unless told not to");
        assert!(!args.inspect && args.inspect_port.is_none());
        assert!(
            args.target.is_none() && args.tenant.is_none(),
            "a serve nobody pointed at a store is a serve of this directory"
        );
    }

    #[test]
    fn parse_honors_every_flag() {
        let args = parse(&argv(&[
            "--port",
            "8000",
            "--env-file",
            ".env.local",
            "--import-map",
            "map.json",
            "--no-verify-jwt",
            "--inspect",
            "9229",
            "--target",
            "s3://bucket",
            "--ref",
            "acme",
            "--config",
            "supabase/config.toml",
        ]))
        .expect("all of them together");
        assert_eq!(args.port, Some(8000));
        assert_eq!(args.env_file, Some(PathBuf::from(".env.local")));
        assert_eq!(args.import_map, Some(PathBuf::from("map.json")));
        assert!(args.no_verify_jwt);
        assert!(args.inspect);
        assert_eq!(args.inspect_port, Some(9229));
        assert_eq!(args.target.as_deref(), Some("s3://bucket"));
        assert_eq!(args.tenant.as_deref(), Some("acme"));
        assert_eq!(args.config, Some(PathBuf::from("supabase/config.toml")));
    }

    /// The port after `--inspect` is optional, so the next word is only
    /// taken as one when it could not be anything else.
    #[test]
    fn parse_reads_an_inspector_port_only_when_there_is_one() {
        let bare = parse(&argv(&["--inspect"])).expect("a debugger on the usual port");
        assert!(bare.inspect && bare.inspect_port.is_none());
        let after = parse(&argv(&["--inspect", "--no-verify-jwt"])).expect("a flag is not a port");
        assert!(after.inspect && after.inspect_port.is_none() && after.no_verify_jwt);
    }

    #[test]
    fn parse_rejects_noise() {
        assert!(parse(&argv(&["--port"])).is_err());
        assert!(parse(&argv(&["--port", "loud"])).is_err());
        assert!(parse(&argv(&["--env-file"])).is_err());
        assert!(parse(&argv(&["--import-map"])).is_err());
        assert!(parse(&argv(&["serve"])).is_err(), "the verb is run's");
        assert!(run(&argv(&["publish"])).is_err(), "and it is one of three");
        assert!(run(&argv(&[])).is_err());
    }

    #[test]
    fn deploy_takes_names_and_flags_apart() {
        let args = parse_deploy(&argv(&[
            "hello",
            "--target",
            "/tmp/store",
            "world",
            "--ref",
            "acme",
            "--no-verify-jwt",
            "--import-map",
            "map.json",
        ]))
        .expect("parsed");
        assert_eq!(args.names, ["hello", "world"], "a name is anything else");
        assert_eq!(args.target.as_deref(), Some("/tmp/store"));
        assert_eq!(args.tenant.as_deref(), Some("acme"));
        assert!(args.no_verify_jwt);
        assert_eq!(args.import_map, Some(PathBuf::from("map.json")));
        assert!(
            parse_deploy(&argv(&["--nope"])).is_err(),
            "a flag nobody knows is not a function name"
        );
        assert!(parse_deploy(&argv(&["--ref"])).is_err());
    }

    /// A deploy needs a store and a project, and says which of the two
    /// it has not got rather than failing at the first thing that
    /// needed one.
    #[test]
    fn a_deploy_with_nowhere_to_go_says_which_half_is_missing() {
        let args = parse_deploy(&argv(&[
            "--no-config",
            "--ref",
            "acme",
            "--target",
            "/tmp/nowhere",
        ]))
        .expect("parsed");
        assert!(
            bind(&args).is_ok(),
            "both named on the command line is enough"
        );
        let no_target = parse_deploy(&argv(&["--no-config", "--ref", "acme"])).expect("parsed");
        if std::env::var("ZOU_TARGET").is_err() {
            let e = bind(&no_target).expect_err("nowhere to deploy to");
            assert!(e.contains("--target"), "{e}");
        }
    }

    /// A debugger is a port the runtime opens, so it is the one flag
    /// that lands on the layout rather than on each function.
    #[test]
    fn the_inspector_is_off_until_a_flag_or_a_file_asks_for_one() {
        let off = layout(&parse(&argv(&[])).unwrap(), None);
        assert_eq!(off.inspector_port, None);
        let flagged = layout(&parse(&argv(&["--inspect"])).unwrap(), None);
        assert_eq!(flagged.inspector_port, Some(DEFAULT_INSPECTOR_PORT));
        let named = layout(&parse(&argv(&["--inspect", "9229"])).unwrap(), None);
        assert_eq!(named.inspector_port, Some(9229));
        let filed = crate::config::Project {
            functions: Layout {
                inspector_port: Some(8123),
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(
            layout(&parse(&argv(&["--inspect"])).unwrap(), Some(&filed)).inspector_port,
            Some(8123),
            "the file's port is the one the project's editor is set up for"
        );
        assert_eq!(
            layout(&parse(&argv(&[])).unwrap(), Some(&filed)).inspector_port,
            Some(8123),
            "and a file that asked for one does not need the flag"
        );
    }

    /// Upstream's precedence, which is flag, then config, then whatever
    /// is beside the function.
    #[test]
    fn the_flags_are_every_function_of_this_run() {
        let dir = project(&["hello", "open"]);
        std::fs::write(
            dir.path().join("functions").join("hello").join("deno.json"),
            "{}",
        )
        .expect("write");
        let plain = disk(
            dir.path(),
            &Layout::default(),
            &parse(&argv(&[])).unwrap(),
            &[],
        )
        .expect("read");
        assert!(plain.functions.iter().all(|f| f.verify_jwt));
        assert_eq!(
            plain.functions[0].import_map,
            Some(dir.path().join("functions").join("hello").join("deno.json")),
            "the one beside the function, which nothing overrode"
        );
        let flagged = parse(&argv(&["--no-verify-jwt", "--import-map", "map.json"])).unwrap();
        let told = disk(dir.path(), &Layout::default(), &flagged, &[]).expect("read");
        assert!(
            told.functions.iter().all(|f| !f.verify_jwt),
            "every function of this run and not one of them"
        );
        assert!(
            told.functions
                .iter()
                .all(|f| f.import_map == Some(PathBuf::from("map.json"))),
            "and the flag beats the deno.json beside the function"
        );
    }

    /// What the watch loop is for: a function that did not exist when
    /// the server started is served without it being restarted, and one
    /// that was deleted stops being.
    #[test]
    fn a_function_written_while_it_is_serving_is_served() {
        let dir = project(&["hello"]);
        let args = parse(&argv(&[])).unwrap();
        let registry = Registry::new(
            Vec::new(),
            zou_deno::engine(Vec::new(), <_>::default(), None),
        );
        let mut watched = watching(dir.path());
        watched.tick(&args, &registry).expect("a first look");
        assert_eq!(registry.names(), ["hello"]);
        let open = dir.path().join("functions").join("open");
        std::fs::create_dir_all(&open).expect("mkdir");
        std::fs::write(
            open.join("index.ts"),
            "Deno.serve(() => new Response(\"o\"))",
        )
        .expect("write");
        watched.tick(&args, &registry).expect("a second look");
        assert_eq!(registry.names(), ["hello", "open"]);
        std::fs::remove_dir_all(dir.path().join("functions").join("hello")).expect("rm");
        watched.tick(&args, &registry).expect("a third look");
        assert_eq!(registry.names(), ["open"]);
        assert!(registry.lookup("hello").is_none(), "and the url is a 404");
    }

    /// The other half of a reload, which costs more: a secret that
    /// changed is a runtime nothing can be kept in.
    #[test]
    fn a_secret_that_changed_is_a_new_runtime() {
        let dir = project(&["hello"]);
        let args = parse(&argv(&[])).unwrap();
        let registry = Registry::hosted(zou_functions::Hosted::new().at("hello", |_| {
            Ok(zou_functions::Answer::new("text/plain", b"hi".to_vec()))
        }));
        let hosted = registry.describe();
        let mut watched = watching(dir.path());
        watched.tick(&args, &registry).expect("a first look");
        assert_eq!(
            registry.describe(),
            hosted,
            "nothing changed, nothing moved"
        );
        std::fs::write(
            zou_functions::env_file(dir.path()),
            "GREETING=from the file\n",
        )
        .expect("write");
        watched.tick(&args, &registry).expect("a second look");
        assert_eq!(
            registry.describe(),
            engine_describe(<_>::default()),
            "the environment moved, so what runs them is a new one"
        );
    }

    /// A config file is read again when it changes, and a block that
    /// switches a function off takes it out of the listing rather than
    /// making it refuse, which is upstream's behaviour.
    #[test]
    fn a_config_that_switches_a_function_off_takes_it_out_of_the_listing() {
        let dir = project(&["hello", "open"]);
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "[api]\nport = 54321\n").expect("write");
        let args = parse(&argv(&[])).unwrap();
        let registry = Registry::new(
            Vec::new(),
            zou_deno::engine(Vec::new(), <_>::default(), None),
        );
        let mut watched = watching(dir.path());
        watched.config = Some(path.clone());
        watched.tick(&args, &registry).expect("a first look");
        assert_eq!(registry.names(), ["hello", "open"]);
        std::fs::write(
            &path,
            "[api]\nport = 54321\n[functions.hello]\nenabled = false\n",
        )
        .expect("write");
        watched.reread(&args);
        watched.tick(&args, &registry).expect("a second look");
        assert_eq!(registry.names(), ["open"]);
    }

    /// A watcher over a directory, with nothing seen yet, which is what
    /// the first look after boot has.
    fn watching(dir: &Path) -> Watched {
        Watched {
            dir: dir.to_path_buf(),
            config: None,
            layout: Layout::default(),
            own: Vec::new(),
            last: Disk {
                functions: Vec::new(),
                env: Vec::new(),
            },
        }
    }
}
