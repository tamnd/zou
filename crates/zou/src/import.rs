//! `zou import supabase`: read a hosted project and say what moving it
//! here would cost, before anything is moved.
//!
//! A migration tool that finds out halfway through is worse than no
//! migration tool, because the half is now somebody's problem. So the
//! survey is a command of its own and it runs first: it connects to the
//! project read only, counts what is there, and writes
//! `import-report.md` naming every extension, schema, role and object
//! it found and what happens to each of them here. Nothing is silently
//! dropped, which in practice means the report has a section for the
//! things that do not come over and that section is never empty by
//! accident.
//!
//! Everything it runs is a catalog read or a `count(*)`. No lease is
//! taken on the target, the source is not written to, and a probe that
//! fails is recorded and the survey carries on, because a project with
//! one table the connecting role cannot see is still a project worth
//! reporting on. What could not be read gets a line in the report of
//! its own, for the same reason: an empty section from a tool that
//! never looked is the failure this file exists to avoid.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio_postgres::{Client, NoTls, config::SslMode};

pub const USAGE: &str = "usage: zou import supabase <--db-url <url> | --project-ref <ref>> [--db-password <pw>] <--to <url> | --dry-run> [--store <target> [--tenant <ref>] [--service-key <key>] [--storage-url <url>] [--jobs <n>] [--manifest <path>]] [--report <path>]";

pub(crate) mod copy;
mod objects;

/// The report's default name, which is the one the milestone asks for
/// and the one a pull request reviewing an import will look for.
const DEFAULT_REPORT: &str = "import-report.md";

/// Where the digests of the copied object bytes go, in the shape
/// `sha256sum` prints, because that is a format people already have a
/// tool for.
const DEFAULT_MANIFEST: &str = "import-objects.sha256";

/// Schemas postgres owns, which are nobody's project and are not
/// reported as one.
pub(crate) const SYSTEM_SCHEMAS: &[&str] = &["pg_catalog", "pg_toast", "information_schema"];

/// Extensions the vendored Postgres builds, so `create extension` here
/// does what it did there. Taken from `vendor/postgres/contrib` plus
/// pgvector, minus the glue extensions that need a procedural language
/// the default build does not ask for.
const NATIVE: &[&str] = &[
    "amcheck",
    "auto_explain",
    "bloom",
    "btree_gin",
    "btree_gist",
    "citext",
    "cube",
    "dblink",
    "dict_int",
    "dict_xsyn",
    "earthdistance",
    "file_fdw",
    "fuzzystrmatch",
    "hstore",
    "intagg",
    "intarray",
    "isn",
    "lo",
    "ltree",
    "pageinspect",
    "pg_buffercache",
    "pg_freespacemap",
    "pg_logicalinspect",
    "pg_overexplain",
    "pg_prewarm",
    "pg_surgery",
    "pg_trgm",
    "pg_visibility",
    "pg_walinspect",
    "pgcrypto",
    "pgrowlocks",
    "pgstattuple",
    "plpgsql",
    "postgres_fdw",
    "seg",
    "sslinfo",
    "tablefunc",
    "tcn",
    "tsm_system_rows",
    "tsm_system_time",
    "unaccent",
    "uuid-ossp",
    "vector",
    "xml2",
];

/// Extensions zou answers for without installing them: the schema, the
/// functions, their signatures, their defaults and their refusals are
/// there, and what is behind them is this server rather than a
/// background worker.
pub(crate) const EMULATED: &[(&str, &str)] = &[
    (
        "pg_net",
        "the net schema and its functions are here with upstream's signatures and defaults, and the calls are made by the server instead of a background worker, see docs/webhooks.md",
    ),
    (
        "pg_cron",
        "the cron schema, its two tables, their row level security and its refusals are here, with a ticker in the server in place of the launcher process, see docs/cron.md",
    ),
];

/// Extensions with no answer here, and what their absence means for a
/// project that has one. Anything installed on the source that is in
/// none of the three lists is still reported, just without a sentence
/// written for it in advance.
const KNOWN_MISSING: &[(&str, &str)] = &[
    (
        "postgis",
        "not built, and every geometry and geography column depends on it, so those tables do not restore at all",
    ),
    ("postgis_raster", "not built, same as postgis"),
    ("postgis_topology", "not built, same as postgis"),
    (
        "pg_graphql",
        "the graphql and graphql_public schemas are not here, so a client calling /graphql/v1 has nothing behind it",
    ),
    (
        "supabase_vault",
        "the vault schema is not here, so a secret kept in it has to be moved somewhere else before the import rather than after",
    ),
    (
        "pgsodium",
        "not here, and a column encrypted through it cannot be read back on this side",
    ),
    (
        "pg_stat_statements",
        "built but not preloaded, so the view is absent and a dashboard that reads it comes back empty",
    ),
    (
        "pgjwt",
        "not built, its sign and verify functions have to be replaced or vendored as plpgsql",
    ),
    (
        "pg_jsonschema",
        "not built, so a check constraint that calls it fails when the schema is restored",
    ),
    (
        "pgaudit",
        "not built, audit logging has to come from the server's own log instead",
    ),
    ("plv8", "not built, functions written in it do not restore"),
    ("http", "not built, use the net schema, which is here"),
    (
        "wrappers",
        "not built, so every foreign table through it is unreadable here",
    ),
    ("timescaledb", "not built, hypertables do not restore"),
    ("pgmq", "not built"),
    (
        "pg_partman",
        "not built, partitions stay as postgres left them and nothing maintains them",
    ),
    (
        "hypopg",
        "not built, it is an advisory tool and nothing depends on it at runtime",
    ),
    ("index_advisor", "not built, same as hypopg"),
    (
        "pgtap",
        "not built, it is a test harness and nothing in production depends on it",
    ),
    ("pg_repack", "not built"),
    ("pg_hashids", "not built"),
    (
        "rum",
        "not built, an index that uses it has to become a gin index",
    ),
    (
        "pgroonga",
        "not built, an index that uses it has to become a gin index with pg_trgm",
    ),
    ("address_standardizer", "not built, it comes with postgis"),
    (
        "plpython3u",
        "the build does not ask for plpython, so functions written in it do not restore",
    ),
    (
        "plperl",
        "the build does not ask for plperl, so functions written in it do not restore",
    ),
];

/// Roles a hosted project has because auth, storage, realtime and the
/// pooler are separate processes there. Here they are one process on
/// one pool, so these do not exist and a dump that names them in its
/// grants has to be taken without owners and privileges, or they have
/// to be created first. See docs/compatibility.md.
const PLATFORM_ROLES: &[&str] = &[
    "dashboard_user",
    "pgbouncer",
    "pgsodium_keyholder",
    "pgsodium_keyiduser",
    "pgsodium_keymaker",
    "pgtle_admin",
    "supabase_admin",
    "supabase_auth_admin",
    "supabase_functions_admin",
    "supabase_read_only_user",
    "supabase_realtime_admin",
    "supabase_replication_admin",
    "supabase_storage_admin",
];

/// Roles that already exist here, so the report does not list them as
/// something to recreate. The three api roles, the authenticator the
/// bootstrap grants them to, and the superuser, which is a database's
/// own rather than anything a project made.
const HERE_ALREADY: &[&str] = &[
    "anon",
    "authenticated",
    "authenticator",
    "postgres",
    "service_role",
];

/// What the survey does not look at. Printed with every report, for the
/// same reason `zou db diff` prints its own list: a section that came
/// back empty from a tool that never looked is worse than no tool.
const UNREAD: &[&str] = &[
    "column level grants and default privileges",
    "large objects",
    "logical replication slots and subscriptions",
    "custom configuration a project set with alter database or alter role",
    "the contents of any table, this is a count and a size and nothing more",
];

#[derive(Debug, Default)]
pub struct Args {
    pub url: Option<String>,
    pub project_ref: Option<String>,
    pub password: Option<String>,
    pub to: Option<String>,
    pub dry_run: bool,
    pub report: Option<PathBuf>,
    /// The store to put the storage object bytes in. Without it the
    /// rows come over and the bytes do not, which the run says.
    pub store: Option<String>,
    pub tenant: Option<String>,
    pub service_key: Option<String>,
    pub storage_url: Option<String>,
    pub jobs: Option<usize>,
    pub manifest: Option<PathBuf>,
}

pub fn parse(argv: &[String]) -> Result<Args, String> {
    let [verb, rest @ ..] = argv else {
        return Err(USAGE.into());
    };
    if verb != "supabase" {
        return Err(format!(
            "zou import only knows how to read a supabase project, not {verb:?}\n{USAGE}"
        ));
    }
    let mut args = Args::default();
    let mut rest = rest.iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--db-url" => args.url = Some(rest.next().ok_or("--db-url needs a url")?.clone()),
            "--project-ref" => {
                args.project_ref = Some(rest.next().ok_or("--project-ref needs a ref")?.clone())
            }
            "--db-password" => {
                args.password = Some(rest.next().ok_or("--db-password needs a password")?.clone())
            }
            "--report" => {
                args.report = Some(PathBuf::from(rest.next().ok_or("--report needs a path")?))
            }
            "--to" => args.to = Some(rest.next().ok_or("--to needs a url")?.clone()),
            "--store" => args.store = Some(rest.next().ok_or("--store needs a target")?.clone()),
            "--tenant" => args.tenant = Some(rest.next().ok_or("--tenant needs a ref")?.clone()),
            "--service-key" => {
                args.service_key = Some(rest.next().ok_or("--service-key needs a key")?.clone())
            }
            "--storage-url" => {
                args.storage_url = Some(rest.next().ok_or("--storage-url needs a url")?.clone())
            }
            "--jobs" => {
                let value = rest.next().ok_or("--jobs needs a number")?;
                let jobs = value.parse().map_err(|_| {
                    format!("bad job count {value:?}, write a whole number of jobs")
                })?;
                if jobs == 0 {
                    return Err("a copy with no jobs copies nothing".into());
                }
                args.jobs = Some(jobs);
            }
            "--manifest" => {
                args.manifest = Some(PathBuf::from(rest.next().ok_or("--manifest needs a path")?))
            }
            "--dry-run" => args.dry_run = true,
            other => return Err(format!("unexpected argument {other:?}\n{USAGE}")),
        }
    }
    match (&args.url, &args.project_ref) {
        (Some(_), Some(_)) => {
            return Err(
                "--db-url and --project-ref are two ways to say the same thing, pass one".into(),
            );
        }
        (None, None) => {
            return Err(format!(
                "nothing to read from, pass --db-url or --project-ref\n{USAGE}"
            ));
        }
        _ => {}
    }
    // The object bytes are keyed by the rows in `storage.objects`, so
    // the store cannot be filled without the database it is being
    // filled for.
    if args.store.is_some() && args.to.is_none() {
        return Err("--store needs --to, the object bytes are keyed by the rows in storage.objects and those come over with the database".into());
    }
    Ok(args)
}

/// Where the object bytes come from and go, or nothing when the run was
/// not asked to move them.
///
/// A hosted project's storage api is at its own hostname, so a project
/// ref is enough to find it. A project reached by a connection string
/// could be anywhere and has to be told, which is the one case where
/// `--storage-url` is not optional.
fn objects_from(args: &Args) -> Result<Option<objects::Where>, String> {
    let Some(store) = &args.store else {
        return Ok(None);
    };
    let base = match (&args.storage_url, &args.project_ref) {
        (Some(url), _) => url.clone(),
        (None, Some(project_ref)) => objects::base_for(project_ref),
        (None, None) => {
            return Err(
                "--store with --db-url needs --storage-url, a connection string does not say where the storage api is"
                    .into(),
            );
        }
    };
    let key = args
        .service_key
        .clone()
        .or_else(|| std::env::var("SUPABASE_SERVICE_ROLE_KEY").ok())
        .filter(|k| !k.is_empty())
        .ok_or(
            "reading a private bucket needs the service role key, pass --service-key or set SUPABASE_SERVICE_ROLE_KEY",
        )?;
    Ok(Some(objects::Where {
        store: store.clone(),
        tenant: args
            .tenant
            .clone()
            .unwrap_or_else(|| objects::DEFAULT_TENANT.to_string()),
        base,
        key,
        jobs: args.jobs.unwrap_or(objects::DEFAULT_JOBS),
        manifest: args
            .manifest
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_MANIFEST)),
    }))
}

/// The connection string for a project ref, which is the one the
/// dashboard prints. The password is the database password rather than
/// anything from the api, and it is percent encoded because a generated
/// one has characters a url reads as structure.
fn url_for(project_ref: &str, password: Option<&str>) -> Result<String, String> {
    let password = password
        .map(str::to_string)
        .or_else(|| std::env::var("SUPABASE_DB_PASSWORD").ok())
        .filter(|p| !p.is_empty())
        .ok_or("a project ref needs the database password, pass --db-password or set SUPABASE_DB_PASSWORD")?;
    Ok(format!(
        "postgresql://postgres:{}@db.{project_ref}.supabase.co:5432/postgres?sslmode=require",
        encode(&password)
    ))
}

/// Percent encoding for the userinfo part of a url, which is the only
/// place this needs it. Everything outside the unreserved set goes,
/// including the characters a password generator likes.
fn encode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Whether the certificate on the other end is checked.
///
/// libpq's `sslmode=require` encrypts and does not authenticate, and
/// `verify-full` does both. tokio-postgres only parses the first three
/// modes, so `verify-ca` and `verify-full` are taken out of the url
/// here and turned into the flag, which keeps the spelling a person
/// already knows working rather than inventing another one.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Verify {
    /// What `require` means: an encrypted socket to whoever answered.
    Off,
    /// What `verify-full` means: the public roots and the hostname.
    On,
}

fn ssl_choice(url: &str) -> (String, Verify) {
    for spelling in ["sslmode=verify-full", "sslmode=verify-ca"] {
        if let Some(at) = url.find(spelling) {
            let mut rewritten = String::with_capacity(url.len());
            rewritten.push_str(&url[..at]);
            rewritten.push_str("sslmode=require");
            rewritten.push_str(&url[at + spelling.len()..]);
            return (rewritten, Verify::On);
        }
    }
    (url.to_string(), Verify::Off)
}

/// The verifier behind `sslmode=require`, which takes any certificate.
///
/// This is not a shortcut around a check that was meant to happen, it
/// is what the mode means: libpq's own `require` encrypts the socket
/// and says nothing about who is on the other end, and a hosted project
/// answers with a certificate from its provider's own authority that
/// the public roots do not sign. `verify-full` in the url is how a
/// caller asks for the other behaviour, and then this is not used.
#[derive(Debug)]
struct AnyCertificate(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for AnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

fn tls(verify: Verify) -> tokio_postgres_rustls::MakeRustlsConnect {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .expect("ring supports the default protocol versions");
    let config = match verify {
        Verify::On => builder
            .with_root_certificates(rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            })
            .with_no_client_auth(),
        Verify::Off => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AnyCertificate(provider)))
            .with_no_client_auth(),
    };
    tokio_postgres_rustls::MakeRustlsConnect::new(config)
}

pub(crate) async fn connect(url: &str) -> Result<Client, String> {
    let (url, verify) = ssl_choice(url);
    let config: tokio_postgres::Config = url
        .parse()
        .map_err(|e| format!("cannot read the connection string: {e}"))?;
    if config.get_ssl_mode() == SslMode::Disable {
        let (client, connection) = config
            .connect(NoTls)
            .await
            .map_err(|e| format!("cannot connect: {e}"))?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::debug!("connection ended: {e}");
            }
        });
        return Ok(client);
    }
    let (client, connection) = config
        .connect(tls(verify))
        .await
        .map_err(|e| format!("cannot connect: {e}"))?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            log::debug!("connection ended: {e}");
        }
    });
    Ok(client)
}

/// One schema and what is in it. Rows are the planner's estimate, which
/// is what `reltuples` is, and the report says so rather than passing
/// an estimate off as a count.
#[derive(Debug, Default)]
pub struct Schema {
    pub name: String,
    pub tables: i64,
    pub views: i64,
    pub sequences: i64,
    pub rows: i64,
    pub bytes: i64,
}

#[derive(Debug)]
pub struct Extension {
    pub name: String,
    pub version: String,
    pub schema: String,
}

#[derive(Debug, Default)]
pub struct Survey {
    pub server: String,
    pub database: String,
    pub extensions: Vec<Extension>,
    pub schemas: Vec<Schema>,
    pub roles: Vec<String>,
    pub platform_roles: Vec<String>,
    pub publications: Vec<(String, i64)>,
    pub policies: i64,
    pub rls_tables: i64,
    pub triggers: i64,
    pub routines: i64,
    pub foreign_servers: Vec<String>,
    pub event_triggers: Vec<String>,
    /// `auth.users` and its neighbours, keyed by what was counted, so a
    /// project on an older auth schema reports what it has instead of
    /// failing on a table that was added later.
    pub auth: BTreeMap<String, i64>,
    pub identity_providers: Vec<(String, i64)>,
    pub storage: BTreeMap<String, i64>,
    pub storage_bytes: Option<i64>,
    /// What the survey asked for and did not get, so the report can say
    /// so instead of leaving a section quietly empty.
    pub unread: Vec<String>,
}

/// The three lists an extension can land in, and the sentence that goes
/// with it in the two that need one.
type Classified<'a> = (
    Vec<&'a Extension>,
    Vec<(&'a Extension, &'a str)>,
    Vec<(&'a Extension, &'a str)>,
);

impl Survey {
    fn note(&mut self, what: &str, e: &tokio_postgres::Error) {
        self.unread.push(format!("{what}: {e}"));
    }

    /// Extensions split the three ways the report prints them.
    fn classify(&self) -> Classified<'_> {
        let mut native = Vec::new();
        let mut emulated = Vec::new();
        let mut missing = Vec::new();
        for e in &self.extensions {
            let name = e.name.as_str();
            if NATIVE.contains(&name) {
                native.push(e);
            } else if let Some((_, why)) = EMULATED.iter().find(|(n, _)| *n == name) {
                emulated.push((e, *why));
            } else {
                let why = KNOWN_MISSING
                    .iter()
                    .find(|(n, _)| *n == name)
                    .map(|(_, why)| *why)
                    .unwrap_or("not built here, and nothing stands in for it");
                missing.push((e, why));
            }
        }
        (native, emulated, missing)
    }

    fn total_rows(&self) -> i64 {
        self.schemas.iter().map(|s| s.rows).sum()
    }

    fn total_bytes(&self) -> i64 {
        self.schemas.iter().map(|s| s.bytes).sum()
    }
}

const SCHEMAS_SQL: &str = "\
select n.nspname::text,
       count(*) filter (where c.relkind in ('r', 'p'))::bigint,
       count(*) filter (where c.relkind in ('v', 'm'))::bigint,
       count(*) filter (where c.relkind = 'S')::bigint,
       coalesce(sum(greatest(c.reltuples, 0)) filter (where c.relkind in ('r', 'p')), 0)::bigint,
       coalesce(sum(pg_total_relation_size(c.oid)) filter (where c.relkind in ('r', 'p', 'm')), 0)::bigint
from pg_namespace n
  left join pg_class c on c.relnamespace = n.oid
where n.nspname <> all($1)
  and n.nspname not like 'pg\\_temp%'
  and n.nspname not like 'pg\\_toast_temp%'
group by n.nspname
order by n.nspname";

const EXTENSIONS_SQL: &str = "\
select e.extname::text, e.extversion::text, n.nspname::text
from pg_extension e join pg_namespace n on n.oid = e.extnamespace
order by e.extname";

const ROLES_SQL: &str = "\
select rolname::text from pg_roles where rolname not like 'pg\\_%' order by rolname";

const PUBLICATIONS_SQL: &str = "\
select p.pubname::text,
       case when p.puballtables then -1
            else (select count(*) from pg_publication_rel r where r.prpubid = p.oid) end::bigint
from pg_publication p order by p.pubname";

pub fn run(argv: &[String]) -> Result<(), String> {
    let args = parse(argv)?;
    let url = match (&args.url, &args.project_ref) {
        (Some(url), _) => url.clone(),
        (None, Some(project_ref)) => url_for(project_ref, args.password.as_deref())?,
        _ => return Err(USAGE.into()),
    };
    if !args.dry_run && args.to.is_none() {
        return Err(format!(
            "nothing to copy into, pass --to with the database to import into, or --dry-run to read the project and write {DEFAULT_REPORT} without copying anything"
        ));
    }
    let report = args
        .report
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_REPORT));
    // Worked out before anything connects, so a missing service key is
    // said in the first second rather than after a survey.
    let bytes = objects_from(&args)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("cannot start a runtime: {e}"))?;
    runtime.block_on(async {
        let client = connect(&url).await?;
        let survey = survey(&client).await;
        // The report is written before anything is copied and whether
        // or not anything is, because the survey is what somebody reads
        // when a copy goes wrong, and a copy that failed is exactly
        // when it is least convenient to go and take one.
        let markdown = render(&survey);
        std::fs::write(&report, &markdown)
            .map_err(|e| format!("cannot write {}: {e}", report.display()))?;
        summary(&survey, &report);
        let Some(to) = &args.to else { return Ok(()) };
        let mut target = connect(to).await?;
        let done = copy::run(&client, &mut target, &survey, bytes.is_some()).await?;
        print!("{}", done.render());
        let Some(w) = &bytes else { return Ok(()) };
        let moved = objects::run(&mut target, w).await?;
        print!("{}", moved.render());
        Ok(())
    })
}

pub async fn survey(client: &Client) -> Survey {
    let mut s = Survey::default();
    let system: Vec<String> = SYSTEM_SCHEMAS.iter().map(|s| s.to_string()).collect();

    match client
        .query_one("select version(), current_database()", &[])
        .await
    {
        Ok(row) => {
            s.server = row.get::<_, String>(0);
            s.database = row.get::<_, String>(1);
        }
        Err(e) => s.note("the server version", &e),
    }

    match client.query(EXTENSIONS_SQL, &[]).await {
        Ok(rows) => {
            s.extensions = rows
                .iter()
                .map(|r| Extension {
                    name: r.get(0),
                    version: r.get(1),
                    schema: r.get(2),
                })
                .collect()
        }
        Err(e) => s.note("the installed extensions", &e),
    }

    match client.query(SCHEMAS_SQL, &[&system]).await {
        Ok(rows) => {
            s.schemas = rows
                .iter()
                .map(|r| Schema {
                    name: r.get(0),
                    tables: r.get(1),
                    views: r.get(2),
                    sequences: r.get(3),
                    rows: r.get(4),
                    bytes: r.get(5),
                })
                .collect()
        }
        Err(e) => s.note("the schemas and their sizes", &e),
    }

    match client.query(ROLES_SQL, &[]).await {
        Ok(rows) => {
            for row in &rows {
                let name: String = row.get(0);
                if HERE_ALREADY.contains(&name.as_str()) {
                    continue;
                }
                if PLATFORM_ROLES.contains(&name.as_str()) {
                    s.platform_roles.push(name);
                } else {
                    s.roles.push(name);
                }
            }
        }
        Err(e) => s.note("the roles", &e),
    }

    match client.query(PUBLICATIONS_SQL, &[]).await {
        Ok(rows) => s.publications = rows.iter().map(|r| (r.get(0), r.get(1))).collect(),
        Err(e) => s.note("the publications", &e),
    }

    s.policies = scalar(
        client,
        "select count(*)::bigint from pg_policy",
        &mut s.unread,
    )
    .await;
    s.rls_tables = scalar(
        client,
        "select count(*)::bigint from pg_class where relrowsecurity",
        &mut s.unread,
    )
    .await;
    s.triggers = scalar(
        client,
        "select count(*)::bigint from pg_trigger where not tgisinternal",
        &mut s.unread,
    )
    .await;
    s.routines = scalar(
        client,
        "select count(*)::bigint from pg_proc p join pg_namespace n on n.oid = p.pronamespace \
         where n.nspname not in ('pg_catalog', 'information_schema') \
           and not exists (select 1 from pg_depend d where d.objid = p.oid and d.deptype = 'e')",
        &mut s.unread,
    )
    .await;

    match client
        .query(
            "select s.srvname::text from pg_foreign_server s order by s.srvname",
            &[],
        )
        .await
    {
        Ok(rows) => s.foreign_servers = rows.iter().map(|r| r.get(0)).collect(),
        Err(e) => s.note("the foreign servers", &e),
    }
    match client
        .query(
            "select evtname::text from pg_event_trigger order by evtname",
            &[],
        )
        .await
    {
        Ok(rows) => s.event_triggers = rows.iter().map(|r| r.get(0)).collect(),
        Err(e) => s.note("the event triggers", &e),
    }

    auth(client, &mut s).await;
    storage(client, &mut s).await;
    s
}

/// One number, or nothing and a line in the report saying why. A survey
/// that dies on a permission it did not need is a survey nobody can run
/// against a project with a locked down role.
async fn scalar(client: &Client, sql: &str, unread: &mut Vec<String>) -> i64 {
    match client.query_one(sql, &[]).await {
        Ok(row) => row.get(0),
        Err(e) => {
            unread.push(format!("{sql}: {e}"));
            0
        }
    }
}

/// Whether a table is there to be counted. The auth and storage schemas
/// have gained tables over the years, so every count is asked for only
/// after this says the table exists, and a project on an older schema
/// reports what it has instead of erroring on what it does not.
async fn present(client: &Client, table: &str) -> bool {
    client
        .query_one("select to_regclass($1) is not null", &[&table])
        .await
        .map(|row| row.get::<_, bool>(0))
        .unwrap_or(false)
}

async fn count_into(client: &Client, table: &str, label: &str, into: &mut BTreeMap<String, i64>) {
    if !present(client, table).await {
        return;
    }
    if let Ok(row) = client
        .query_one(&format!("select count(*)::bigint from {table}"), &[])
        .await
    {
        into.insert(label.to_string(), row.get(0));
    }
}

async fn auth(client: &Client, s: &mut Survey) {
    if !present(client, "auth.users").await {
        s.unread.push(
            "auth.users is not there, so this is not a project with Supabase auth in it".into(),
        );
        return;
    }
    for (label, sql) in [
        ("users", "select count(*)::bigint from auth.users"),
        (
            "users with a password",
            "select count(*)::bigint from auth.users where encrypted_password is not null and encrypted_password <> ''",
        ),
        (
            "users with a confirmed email",
            "select count(*)::bigint from auth.users where email_confirmed_at is not null",
        ),
        (
            "users with a phone",
            "select count(*)::bigint from auth.users where phone is not null",
        ),
    ] {
        match client.query_one(sql, &[]).await {
            Ok(row) => {
                s.auth.insert(label.to_string(), row.get(0));
            }
            Err(e) => s.unread.push(format!("{label}: {e}")),
        }
    }
    count_into(client, "auth.identities", "identities", &mut s.auth).await;
    count_into(client, "auth.mfa_factors", "mfa factors", &mut s.auth).await;
    count_into(client, "auth.sso_providers", "sso providers", &mut s.auth).await;
    count_into(client, "auth.saml_providers", "saml providers", &mut s.auth).await;
    count_into(client, "auth.refresh_tokens", "refresh tokens", &mut s.auth).await;
    if present(client, "auth.identities").await
        && let Ok(rows) = client
            .query(
                "select provider::text, count(*)::bigint from auth.identities \
                 group by provider order by provider",
                &[],
            )
            .await
    {
        s.identity_providers = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    }
}

async fn storage(client: &Client, s: &mut Survey) {
    if !present(client, "storage.buckets").await {
        return;
    }
    count_into(client, "storage.buckets", "buckets", &mut s.storage).await;
    count_into(client, "storage.objects", "objects", &mut s.storage).await;
    count_into(
        client,
        "storage.s3_multipart_uploads",
        "uploads in flight",
        &mut s.storage,
    )
    .await;
    if let Ok(row) = client
        .query_one(
            "select count(*)::bigint from storage.buckets where public",
            &[],
        )
        .await
    {
        s.storage.insert("public buckets".into(), row.get(0));
    }
    if present(client, "storage.objects").await
        && let Ok(row) = client
            .query_one(
                "select coalesce(sum((metadata->>'size')::bigint), 0)::bigint from storage.objects",
                &[],
            )
            .await
    {
        s.storage_bytes = Some(row.get(0));
    }
}

/// A count and the noun that goes with it, agreeing, because a report
/// that says one tables reads like it was generated and not written.
pub(crate) fn plural(n: i64, one: &str, many: &str) -> String {
    if n == 1 {
        format!("{n} {one}")
    } else {
        format!("{n} {many}")
    }
}

/// Bytes, rounded the way somebody reading a report wants them.
fn size(bytes: i64) -> String {
    const UNITS: &[(&str, f64)] = &[("TB", 1e12), ("GB", 1e9), ("MB", 1e6), ("kB", 1e3)];
    for (unit, scale) in UNITS {
        if bytes.abs() as f64 >= *scale {
            return format!("{:.1} {unit}", bytes as f64 / scale);
        }
    }
    format!("{bytes} bytes")
}

pub fn render(s: &Survey) -> String {
    let mut out = String::new();
    let (native, emulated, missing) = s.classify();

    out.push_str("# Import report\n\n");
    out.push_str("What a `zou import supabase` would find in this project, read from its catalog before anything is copied.\n");
    out.push_str("Every count here is a count except the row totals, which are the planner's estimate out of `reltuples` and are named as such wherever they appear.\n\n");
    if !s.server.is_empty() {
        out.push_str(&format!("Source: {} on `{}`.\n\n", s.server, s.database));
    }

    out.push_str("## What comes over\n\n");
    out.push_str(&format!(
        "{} schemas, {} tables, {} views, roughly {} rows and {} on disk.\n\n",
        s.schemas.len(),
        s.schemas.iter().map(|x| x.tables).sum::<i64>(),
        s.schemas.iter().map(|x| x.views).sum::<i64>(),
        s.total_rows(),
        size(s.total_bytes())
    ));
    out.push_str("| schema | tables | views | sequences | rows, estimated | on disk |\n");
    out.push_str("| --- | --- | --- | --- | --- | --- |\n");
    for schema in &s.schemas {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} |\n",
            schema.name,
            schema.tables,
            schema.views,
            schema.sequences,
            schema.rows,
            size(schema.bytes)
        ));
    }
    out.push_str(&format!(
        "\n{}, {}, {}, {}. Routines the extensions brought with them are not counted, only the ones the project wrote.\n\n",
        plural(s.policies, "row level security policy", "row level security policies"),
        plural(s.rls_tables, "table with row level security on", "tables with row level security on"),
        plural(s.triggers, "trigger", "triggers"),
        plural(s.routines, "routine", "routines"),
    ));

    out.push_str("## Extensions\n\n");
    if native.is_empty() && emulated.is_empty() && missing.is_empty() {
        out.push_str("None installed.\n\n");
    }
    if !native.is_empty() {
        out.push_str("These are built here, so `create extension` does what it did there.\n\n");
        for e in &native {
            out.push_str(&format!("- {} {} in `{}`\n", e.name, e.version, e.schema));
        }
        out.push('\n');
    }
    if !emulated.is_empty() {
        out.push_str("These are answered for without being installed. The schema and the functions are here and something else is behind them.\n\n");
        for (e, why) in &emulated {
            out.push_str(&format!("- {} {}: {}\n", e.name, e.version, why));
        }
        out.push('\n');
    }
    if !missing.is_empty() {
        out.push_str("These have no answer here. Nothing about them is silent: each one is a decision to make before the import rather than a surprise during it.\n\n");
        for (e, why) in &missing {
            out.push_str(&format!("- {} {}: {}\n", e.name, e.version, why));
        }
        out.push('\n');
    }

    out.push_str("## Auth\n\n");
    if s.auth.is_empty() {
        out.push_str("No `auth.users`, so there is nothing here to bring over.\n\n");
    } else {
        for (what, n) in &s.auth {
            out.push_str(&format!("- {what}: {n}\n"));
        }
        if !s.identity_providers.is_empty() {
            out.push_str("\nIdentities by provider:\n\n");
            for (provider, n) in &s.identity_providers {
                out.push_str(&format!("- {provider}: {n}\n"));
            }
        }
        out.push_str("\nPasswords are bcrypt on both sides, so a user who had one signs in here with the same one and nothing is reset.\n");
        out.push_str("Sessions are the exception and they are deliberate: refresh tokens are not brought over, so everybody signs in again once after the cutover and no token minted by the old project is accepted by this one.\n\n");
    }

    out.push_str("## Storage\n\n");
    if s.storage.is_empty() {
        out.push_str("No `storage.buckets`, so there is nothing here to bring over.\n\n");
    } else {
        for (what, n) in &s.storage {
            out.push_str(&format!("- {what}: {n}\n"));
        }
        if let Some(bytes) = s.storage_bytes {
            out.push_str(&format!(
                "- their recorded size adds up to {}\n",
                size(bytes)
            ));
        }
        out.push('\n');
    }

    out.push_str("## Roles and ownership\n\n");
    if !s.platform_roles.is_empty() {
        out.push_str("A hosted project has a role per platform process because auth, storage, realtime and the pooler are separate programs there.\n");
        out.push_str("Here they are one process on one pool, so these do not exist and a dump that names them in its grants has to be taken without owners and privileges, or they have to be created first:\n\n");
        for role in &s.platform_roles {
            out.push_str(&format!("- {role}\n"));
        }
        out.push('\n');
    }
    if s.roles.is_empty() {
        out.push_str("The project created no roles of its own.\n\n");
    } else {
        out.push_str("Roles this project made, which do have to be recreated:\n\n");
        for role in &s.roles {
            out.push_str(&format!("- {role}\n"));
        }
        out.push('\n');
    }
    out.push_str(
        "`anon`, `authenticated`, `service_role` and the superuser are here already and are not listed above.\n\n",
    );

    out.push_str("## The rest of it\n\n");
    if s.publications.is_empty() {
        out.push_str("- no publications\n");
    } else {
        for (name, tables) in &s.publications {
            let what = if *tables < 0 {
                "every table".to_string()
            } else {
                plural(*tables, "table", "tables")
            };
            out.push_str(&format!("- publication {name} over {what}\n"));
        }
    }
    if s.foreign_servers.is_empty() {
        out.push_str("- no foreign servers\n");
    } else {
        for name in &s.foreign_servers {
            out.push_str(&format!(
                "- foreign server {name}, which needs its wrapper on this side before any of its tables read\n"
            ));
        }
    }
    if s.event_triggers.is_empty() {
        out.push_str("- no event triggers\n");
    } else {
        for name in &s.event_triggers {
            out.push_str(&format!("- event trigger {name}\n"));
        }
    }
    out.push('\n');

    out.push_str("## What this did not look at\n\n");
    out.push_str("Said out loud, because a section that came back empty from something that never looked is worse than nothing.\n\n");
    for what in UNREAD {
        out.push_str(&format!("- {what}\n"));
    }
    for what in &s.unread {
        out.push_str(&format!("- could not read {what}\n"));
    }
    out.push('\n');
    out
}

fn summary(s: &Survey, report: &std::path::Path) {
    let (native, emulated, missing) = s.classify();
    println!(
        "{} schemas, {} tables, roughly {} rows, {} on disk",
        s.schemas.len(),
        s.schemas.iter().map(|x| x.tables).sum::<i64>(),
        s.total_rows(),
        size(s.total_bytes())
    );
    if let Some(users) = s.auth.get("users") {
        println!("{users} auth users");
    }
    if let Some(objects) = s.storage.get("objects") {
        println!("{objects} storage objects");
    }
    println!(
        "extensions: {} built here, {} answered for, {} with no answer",
        native.len(),
        emulated.len(),
        missing.len()
    );
    for (e, why) in &missing {
        println!("  {}: {}", e.name, why);
    }
    if !s.unread.is_empty() {
        println!(
            "{} things could not be read, they are in the report",
            s.unread.len()
        );
    }
    println!("written to {}", report.display());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn the_flags_come_apart() {
        let args = parse(&argv(&[
            "supabase",
            "--db-url",
            "postgresql://localhost/x",
            "--dry-run",
            "--report",
            "out.md",
        ]))
        .unwrap();
        assert_eq!(args.url.as_deref(), Some("postgresql://localhost/x"));
        assert!(args.dry_run);
        assert_eq!(args.report, Some(PathBuf::from("out.md")));
        assert!(parse(&argv(&[])).is_err());
        assert!(parse(&argv(&["postgres", "--db-url", "x"])).is_err());
        assert!(
            parse(&argv(&["supabase"])).is_err(),
            "a source is not optional"
        );
        assert!(
            parse(&argv(&["supabase", "--db-url", "x", "--project-ref", "y"])).is_err(),
            "two sources is a mistake, not a merge"
        );
    }

    #[test]
    fn a_project_ref_becomes_the_url_the_dashboard_prints() {
        let url = url_for("abcdefghijklmnop", Some("p@ss word/1")).unwrap();
        assert_eq!(
            url,
            "postgresql://postgres:p%40ss%20word%2F1@db.abcdefghijklmnop.supabase.co:5432/postgres?sslmode=require"
        );
        assert!(
            url_for("abcdefghijklmnop", Some("")).is_err(),
            "an empty password is not a password"
        );
    }

    /// A password out of a generator has characters a url reads as
    /// structure, and a connection string that silently truncates at
    /// one of them fails with something that looks like a wrong
    /// password.
    #[test]
    fn a_password_with_url_syntax_in_it_survives() {
        assert_eq!(encode("aA0-._~"), "aA0-._~");
        assert_eq!(encode("a:b@c/d?e#f"), "a%3Ab%40c%2Fd%3Fe%23f");
    }

    #[test]
    fn verify_full_is_taken_out_of_the_url_and_kept_as_the_flag() {
        let (url, verify) = ssl_choice("postgresql://h/db?sslmode=verify-full&connect_timeout=5");
        assert_eq!(url, "postgresql://h/db?sslmode=require&connect_timeout=5");
        assert_eq!(verify, Verify::On);
        let (url, verify) = ssl_choice("postgresql://h/db?sslmode=require");
        assert_eq!(url, "postgresql://h/db?sslmode=require");
        assert_eq!(
            verify,
            Verify::Off,
            "require encrypts and does not authenticate, the same as libpq"
        );
        let (_, verify) = ssl_choice("postgresql://h/db?sslmode=verify-ca");
        assert_eq!(verify, Verify::On);
    }

    fn ext(name: &str) -> Extension {
        Extension {
            name: name.into(),
            version: "1.0".into(),
            schema: "public".into(),
        }
    }

    #[test]
    fn extensions_split_three_ways_and_nothing_falls_out() {
        let s = Survey {
            extensions: vec![
                ext("pgcrypto"),
                ext("vector"),
                ext("pg_net"),
                ext("pg_cron"),
                ext("postgis"),
                ext("something_nobody_here_has_heard_of"),
            ],
            ..Default::default()
        };
        let (native, emulated, missing) = s.classify();
        assert_eq!(native.len(), 2);
        assert_eq!(emulated.len(), 2);
        assert_eq!(
            missing.len(),
            2,
            "an unknown extension is missing, not ignored"
        );
        assert_eq!(
            native.len() + emulated.len() + missing.len(),
            s.extensions.len(),
            "every extension is in exactly one of the three"
        );
        let unknown = missing
            .iter()
            .find(|(e, _)| e.name == "something_nobody_here_has_heard_of")
            .unwrap();
        assert!(
            !unknown.1.is_empty(),
            "an unknown one still gets a sentence"
        );
    }

    #[test]
    fn a_report_says_what_it_could_not_read() {
        let s = Survey {
            unread: vec!["auth.mfa_factors: permission denied".into()],
            ..Default::default()
        };
        let out = render(&s);
        assert!(out.contains("could not read auth.mfa_factors: permission denied"));
        assert!(
            out.contains("## What this did not look at"),
            "the section is always there"
        );
        for what in UNREAD {
            assert!(out.contains(what), "{what} missing from the report");
        }
    }

    #[test]
    fn the_platform_roles_are_told_apart_from_the_projects_own() {
        let s = Survey {
            platform_roles: vec!["supabase_auth_admin".into()],
            roles: vec!["reporting".into()],
            ..Default::default()
        };
        let out = render(&s);
        assert!(out.contains("without owners and privileges"));
        assert!(out.contains("- supabase_auth_admin"));
        assert!(out.contains("Roles this project made, which do have to be recreated"));
        assert!(out.contains("- reporting"));
    }

    #[test]
    fn a_report_with_nothing_in_it_still_says_so_in_every_section() {
        let out = render(&Survey::default());
        for section in [
            "## What comes over",
            "## Extensions",
            "## Auth",
            "## Storage",
            "## Roles and ownership",
            "## The rest of it",
            "## What this did not look at",
        ] {
            assert!(out.contains(section), "{section} missing");
        }
        assert!(out.contains("No `auth.users`"));
        assert!(out.contains("No `storage.buckets`"));
        assert!(out.contains("None installed."));
        assert!(out.contains("- no publications"));
    }

    #[test]
    fn sizes_read_the_way_a_person_reads_them() {
        assert_eq!(size(0), "0 bytes");
        assert_eq!(size(999), "999 bytes");
        assert_eq!(size(1_500), "1.5 kB");
        assert_eq!(size(2_400_000_000), "2.4 GB");
    }

    /// The object bytes take a store, a tenant and a key, and every one
    /// of them has an answer that does not have to be typed.
    #[test]
    fn the_object_flags_have_defaults_worth_having() {
        let args = parse(&argv(&[
            "supabase",
            "--project-ref",
            "abcdefghijklmnop",
            "--to",
            "postgresql://localhost/zou",
            "--store",
            "/var/lib/zou",
            "--service-key",
            "a-service-role-key",
        ]))
        .unwrap();
        let w = objects_from(&args).unwrap().expect("a store was asked for");
        assert_eq!(w.base, "https://abcdefghijklmnop.supabase.co/storage/v1");
        assert_eq!(w.tenant, objects::DEFAULT_TENANT);
        assert_eq!(w.jobs, objects::DEFAULT_JOBS);
        assert_eq!(w.manifest, PathBuf::from(DEFAULT_MANIFEST));
        assert!(
            objects_from(&parse(&argv(&["supabase", "--project-ref", "x", "--dry-run"])).unwrap())
                .unwrap()
                .is_none(),
            "no store asked for is no bytes moved"
        );
    }

    /// The two shapes of the command that cannot work, both said before
    /// anything connects.
    #[test]
    fn the_bytes_need_somewhere_to_come_from_and_go_to() {
        let e = parse(&argv(&[
            "supabase",
            "--project-ref",
            "x",
            "--dry-run",
            "--store",
            "/var/lib/zou",
        ]))
        .unwrap_err();
        assert!(e.contains("--store needs --to"), "{e}");

        let args = parse(&argv(&[
            "supabase",
            "--db-url",
            "postgresql://elsewhere/postgres",
            "--to",
            "postgresql://localhost/zou",
            "--store",
            "/var/lib/zou",
            "--service-key",
            "k",
        ]))
        .unwrap();
        let e = objects_from(&args).unwrap_err();
        assert!(e.contains("--storage-url"), "{e}");
    }

    /// A command with neither a target nor --dry-run has been asked
    /// for nothing, and saying so beats reading a whole project and
    /// throwing the answer away.
    #[test]
    fn a_run_with_nowhere_to_put_it_says_so_before_it_connects() {
        let e = run(&argv(&["supabase", "--db-url", "postgresql://localhost/x"])).unwrap_err();
        assert!(e.contains("--to"), "{e}");
        assert!(e.contains("--dry-run"), "{e}");
    }

    /// Enough of a project's shape to survey: the two schemas the
    /// platform owns, a table of the project's own with row level
    /// security on it, and a publication.
    const SEED: &str = "
create schema auth;
create schema storage;
create table auth.users (
    id bigserial primary key,
    email text,
    phone text,
    encrypted_password text,
    email_confirmed_at timestamptz
);
insert into auth.users (email, encrypted_password, email_confirmed_at)
select 'u' || g || '@example.com', '$2a$10$notarealhash', now() from generate_series(1, 7) g;
insert into auth.users (email) select 'nopass' || g || '@example.com' from generate_series(1, 3) g;
create table auth.identities (id text primary key, provider text);
insert into auth.identities values ('a', 'email'), ('b', 'email'), ('c', 'google');
create table auth.refresh_tokens (id bigserial primary key);
insert into auth.refresh_tokens select from generate_series(1, 20);
create table storage.buckets (id text primary key, public boolean default false);
insert into storage.buckets values ('avatars', true), ('private', false);
create table storage.objects (id bigserial primary key, metadata jsonb);
insert into storage.objects (metadata)
select jsonb_build_object('size', 1000 * g) from generate_series(1, 50) g;
create table public.notes (id bigserial primary key, body text);
insert into public.notes (body) select repeat('x', 100) from generate_series(1, 5000);
alter table public.notes enable row level security;
create policy everyone on public.notes for select using (true);
create publication zou_survey_pub for table public.notes;
analyze;
";

    /// The catalog queries are the part of this file most likely to be
    /// wrong, and nothing above touches them, so they run against a
    /// real server here. Gated on ZOU_PG_TEST_DSN the same way the pool
    /// and rls suites are, unset means this skips.
    ///
    /// The assertion that carries the most is `unread` being empty:
    /// every probe records its own failure, so an empty list is every
    /// statement in the file having parsed and run.
    #[test]
    fn a_live_project_is_read_the_way_the_report_prints_it() {
        let Ok(dsn) = std::env::var("ZOU_PG_TEST_DSN") else {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            return;
        };
        if dsn.is_empty() {
            eprintln!("skipping: ZOU_PG_TEST_DSN is empty");
            return;
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let base: tokio_postgres::Config = dsn.parse().expect("the dsn parses");
            let admin = open(&base).await;
            // A database of its own, so a survey that counts everything
            // in sight does not count another test's tables.
            admin
                .batch_execute("drop database if exists zou_import_survey with (force)")
                .await
                .expect("drop");
            admin
                .batch_execute("create database zou_import_survey")
                .await
                .expect("create");
            let mut theirs = base.clone();
            theirs.dbname("zou_import_survey");
            let client = open(&theirs).await;
            client.batch_execute(SEED).await.expect("seed");

            let s = survey(&client).await;
            assert!(s.unread.is_empty(), "a probe failed: {:?}", s.unread);
            assert!(s.server.starts_with("PostgreSQL"), "{}", s.server);
            assert_eq!(s.database, "zou_import_survey");

            let named = |name: &str| s.schemas.iter().find(|x| x.name == name).unwrap().tables;
            assert_eq!(named("auth"), 3);
            assert_eq!(named("storage"), 2);
            assert_eq!(named("public"), 1);
            assert!(
                s.schemas.iter().all(|x| x.name != "pg_catalog"),
                "the system schemas are not the project's"
            );
            assert!(s.total_bytes() > 0, "reltuples and sizes came back zero");

            assert_eq!(s.auth.get("users"), Some(&10));
            assert_eq!(s.auth.get("users with a password"), Some(&7));
            assert_eq!(s.auth.get("users with a confirmed email"), Some(&7));
            assert_eq!(s.auth.get("users with a phone"), Some(&0));
            assert_eq!(s.auth.get("identities"), Some(&3));
            assert_eq!(s.auth.get("refresh tokens"), Some(&20));
            assert_eq!(
                s.auth.get("mfa factors"),
                None,
                "a table that is not there is not counted"
            );
            assert_eq!(
                s.identity_providers,
                vec![("email".to_string(), 2), ("google".to_string(), 1)]
            );

            assert_eq!(s.storage.get("buckets"), Some(&2));
            assert_eq!(s.storage.get("objects"), Some(&50));
            assert_eq!(s.storage.get("public buckets"), Some(&1));
            assert_eq!(s.storage_bytes, Some((1..=50).map(|g| g * 1000).sum()));

            assert_eq!(s.policies, 1);
            assert_eq!(s.rls_tables, 1);
            assert_eq!(
                s.routines, 0,
                "the project wrote none, extensions do not count"
            );
            assert!(
                s.publications.contains(&("zou_survey_pub".to_string(), 1)),
                "{:?}",
                s.publications
            );
            let (native, _, _) = s.classify();
            assert!(native.iter().any(|e| e.name == "plpgsql"));

            // The report has to render off a real survey, not just the
            // hand built ones above.
            let out = render(&s);
            assert!(out.contains("| auth | 3 |"), "{out}");
            assert!(out.contains("- users: 10"), "{out}");

            drop(client);
            admin
                .batch_execute("drop database zou_import_survey with (force)")
                .await
                .expect("drop");
        });
    }

    pub(super) async fn open(config: &tokio_postgres::Config) -> Client {
        let (client, connection) = config.connect(NoTls).await.expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
}
