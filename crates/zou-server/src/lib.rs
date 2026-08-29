//! The HTTP front door for zou's Supabase compatible surface.
//!
//! One router carries the four Supabase path prefixes: /rest/v1,
//! /auth/v1, /storage/v1, and /realtime/v1. Every request under them
//! passes the same gate Supabase's edge applies: an apikey header or
//! url parameter that must be a JWT signed with the project secret,
//! then an optional Authorization bearer token whose claims become the
//! request identity. The gate deposits an [`AuthContext`] in request
//! extensions, which is what the REST and auth handlers will turn into
//! SET LOCAL role and request.jwt.claims when they land. Endpoints
//! that do not exist yet answer 501 with a plain message instead of
//! pretending, and the error bodies the gate does send match the
//! shapes supabase-js already tolerates from the hosted edge.
//!
//! The server runs on its own tokio runtime behind [`serve_blocking`],
//! so the sync callers in zou dev just park a thread on it.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, delete, get, post, put};
use axum::{Router, middleware};
use zou_rest::catalog::Catalog;

pub mod admin;
pub mod attach;
pub mod audit;
pub mod auth;
pub mod binding;
pub mod blob;
pub mod cdc;
pub mod cron;
pub mod edge;
pub mod fanout;
pub mod fleet;
pub mod forward;
pub mod functions;
pub mod gateway;
pub mod gojson;
pub mod hook;
pub mod jwt;
pub mod lambda;
pub mod limit;
pub mod mail;
pub mod mfa;
pub mod oauth;
pub mod object;
pub mod openapi;
pub mod ops;
pub mod password;
pub mod payload;
pub mod pgoutput;
pub mod policy;
pub mod reader;
pub mod realtime;
pub mod render;
pub mod rest;
pub mod s3;
pub mod scram;
pub mod sms;
pub mod smtp;
pub mod source;
pub mod sql;
pub mod storage;
pub mod tenant;
pub mod totp;
pub mod tus;
pub mod visible;
pub mod webhook;
pub mod wire;

/// What the front door needs to know: the secret every key and token
/// must verify against, and where postgres lives when there is one to
/// talk to. `pg` is a dsn like "host=127.0.0.1 port=5432 user=x
/// dbname=postgres", None runs the router without a pool, which is
/// what the pure routing tests use. `rate` of None means unlimited,
/// which is what zou dev wants, the real per endpoint budgets arrive
/// with the auth surface. `jwks` is the project's published public
/// keys as JWKS JSON for projects on asymmetric signing keys, None
/// keeps bearer verification HS256 only.
pub struct Config {
    pub jwt_secret: Vec<u8>,
    pub pg: Option<String>,
    /// The dsn a request session logs in as, if it is not the one
    /// above. Upstream this is a separate process with a separate
    /// connection string: PostgREST connects as `authenticator` and
    /// nothing it serves can reach past the three api roles, whatever
    /// a token claims. Pointing this at `authenticator` buys the same
    /// fence here, since the refusal then comes from postgres rather
    /// than from [`Config::exposed_roles`]. None means both identities
    /// are `pg`, which is what a deployment that has not thought about
    /// it gets, and it is the behaviour zou had before the field
    /// existed. Ignored when `pg` is None.
    pub pg_request: Option<String>,
    pub rate: Option<edge::Rate>,
    pub jwks: Option<String>,
    /// The schemas the REST surface exposes, PostgREST's db-schemas.
    /// The first is the default when no profile header picks one;
    /// empty means just public, the fresh Supabase project shape.
    pub schemas: Vec<String>,
    /// Whether the REST surface answers a select that aggregates,
    /// PostgREST's db-aggregates-enabled. True here, which is what the
    /// hosted platform does and what zou did before the switch existed.
    ///
    /// Upstream defaults it off and the Supabase CLI never turns it
    /// on, so `supabase start` refuses `select=total.sum()` with
    /// PGRST123 while a hosted project answers it. That leaves no
    /// single behaviour that matches both, and this takes the hosted
    /// one: it is the surface a project ships against, an application
    /// that works here works in production, and the reason upstream
    /// turned it off, an unbounded aggregate reading a whole table, is
    /// a reason to be able to turn it off rather than to be off. See
    /// #555.
    pub aggregates: bool,
    /// The role a request with no role of its own runs as, PostgREST's
    /// db-anon-role. Supabase names it `anon` and so does this by
    /// default. It is not only the fallback: it is also how a refusal
    /// tells the two audiences apart, since permission denied to a
    /// caller who never said who they were is a 401 with a challenge
    /// on it, and the same refusal to a caller who did is a 403 with
    /// nothing to try next.
    pub anon_role: String,
    /// The roles a request is allowed to run as, which is the set the
    /// `role` claim of a token may name. Empty means the three a
    /// Supabase project has, and [`Config::anon_role`] is always in
    /// the set whatever it is called.
    ///
    /// Hosted Supabase does not need this list because its api
    /// connects as `authenticator`, a role granted exactly those
    /// three, so a claim naming anything else fails at the database.
    /// [`Config::pg_request`] is how a zou deployment gets that same
    /// fence, and `zou dev` sets it. This list is the check in front
    /// of it: it refuses the claim before it reaches set_config, so a
    /// deployment that left both identities on one dsn, which is
    /// usually the superuser in a dev loop, does not turn a token
    /// carrying `"role": "postgres"` into a superuser session with
    /// server file access in it. See #92.
    pub exposed_roles: Vec<String>,
    /// Where this server answers from the outside, GoTrue's
    /// API_EXTERNAL_URL. It is the `iss` claim of every access token,
    /// with /auth/v1 appended, which is the one place a client can
    /// check that a token came from the project it thinks it did.
    /// None takes GoTrue's own default rather than inventing one.
    pub external_url: Option<String>,
    /// The project's own signing keys, GoTrue's GOTRUE_JWT_KEYS: a json
    /// array of private JWKs with exactly one of them carrying sign in
    /// its key_ops. Set, and access tokens are signed with that key and
    /// its public half is served from the jwks endpoint. Unset, and
    /// they are signed with jwt_secret on the legacy HS256 format,
    /// which is what a fresh Supabase project still does.
    pub jwt_keys: Option<String>,
    /// GoTrue's GOTRUE_MAILER_AUTOCONFIRM. True and a signup is
    /// confirmed on the spot and answers with a session, which is what
    /// the Supabase CLI sets locally because there is no inbox to read.
    /// False and the signup answers with a user and waits for the
    /// address to be proved, which is the hosted default.
    pub mailer_autoconfirm: bool,
    /// GoTrue's GOTRUE_SITE_URL, where a confirmation link lands once
    /// it has been followed. It is also the only redirect target that
    /// is trusted by default: a redirect_to on the link is honoured
    /// when it shares this url's scheme and host, and dropped
    /// otherwise, because a link in an email that will bounce anywhere
    /// is a phishing tool.
    pub site_url: Option<String>,
    /// GoTrue's GOTRUE_MAILER_SECURE_EMAIL_CHANGE_ENABLED, on by
    /// default there and here. A change of address is confirmed twice,
    /// once from the old address and once from the new, so someone who
    /// walks up to an unlocked session cannot quietly move the account
    /// somewhere the owner cannot reach.
    pub secure_email_change: bool,
    /// GoTrue's GOTRUE_SECURITY_MANUAL_LINKING_ENABLED, off by default
    /// there and here. On, and a signed in person can start a second
    /// social sign in that attaches its identity to the account they
    /// already have, and can detach one again. Off, and both endpoints
    /// are not there at all.
    pub manual_linking: bool,
    /// GoTrue's GOTRUE_SECURITY_UPDATE_PASSWORD_REQUIRE_REAUTHENTICATION,
    /// off by default there and here. On, and setting a new password
    /// needs a code mailed to the address first unless the session was
    /// started in the last day.
    pub reauthentication_required: bool,
    /// GoTrue's GOTRUE_EXTERNAL_ANONYMOUS_USERS_ENABLED, off by default
    /// there and here. On, and a signup carrying neither an address nor
    /// a number gets an account with no identity and a session, which
    /// a client later turns into a real account by setting an address
    /// on it. Off, and that signup is refused.
    pub anonymous_users: bool,
    /// Templates, link paths, and how often one account may be mailed.
    /// Everything here has a GoTrue default and takes it.
    pub mail: mail::Settings,
    /// Who carries the mail. None is the dev inbox, which is what an
    /// unconfigured project gets. This is not an environment variable
    /// and never will be: it is here because something embedding this
    /// server already has a way to send mail and should be able to
    /// hand it over rather than configure a second one, and because a
    /// test needs to be able to watch a send fail.
    pub sender: Option<Arc<dyn mail::Sender>>,
    /// What no sender means. True, the default, and it means the dev
    /// inbox, which is right for `zou dev` and for a test. False and it
    /// means no mail server at all, so anything that would have been
    /// sent is an error where it was asked for.
    ///
    /// A node serving other people's projects sets this false. Its
    /// projects are reached by people who cannot read its log, so an
    /// inbox in its memory is a message delivered nowhere and reported
    /// as delivered, and a sign up that answers 200 and never arrives
    /// is worse than one that says it could not be done.
    pub dev_inbox: bool,
    /// GoTrue's GOTRUE_EXTERNAL_EMAIL_ENABLED, on by default there and
    /// here. Off, and every way in that names an address is refused,
    /// which is what a project that signs everyone in by number or by
    /// social provider wants: an address left half served is an account
    /// nobody can confirm.
    pub email_enabled: bool,
    /// GoTrue's GOTRUE_DISABLE_SIGNUP, off by default there and here.
    /// On, and nobody new gets an account: signup and anonymous sign in
    /// are refused, and a social sign in by someone the project has
    /// never seen is refused at the callback. Invitations are the way
    /// in, which is why they are not covered by it.
    pub disable_signup: bool,
    /// GoTrue's GOTRUE_EXTERNAL_PHONE_ENABLED, off by default there and
    /// here. Off, and every phone endpoint refuses in upstream's words,
    /// which is what a project with no numbers to text wants.
    pub phone_enabled: bool,
    /// The template, the code length, and how often one account may be
    /// texted. GoTrue's defaults, which are not the mail ones.
    pub sms: sms::Settings,
    /// Who carries the text messages. None is the dev sink, here for
    /// the same reasons `sender` is.
    pub texter: Option<Arc<dyn sms::Sender>>,
    /// The second factor: which kinds may be enrolled, how many an
    /// account may hold, and how long a challenge is good for. The
    /// defaults are GoTrue's, which have TOTP on and everything else
    /// off.
    pub mfa: mfa::Settings,
    /// Where a project puts its own code in the middle of a flow,
    /// GoTrue's GOTRUE_HOOK_*. Nothing hooked is the usual project,
    /// and the one hook built so far is the custom access token one.
    pub hook: hook::Settings,
    /// How often one caller may ask for a token, a code or a factor,
    /// and how much mail this server will post an hour. GoTrue's
    /// GOTRUE_RATE_LIMIT_*, defaults and all, and like upstream none of
    /// the endpoint limits do anything until the server is told how to
    /// tell one caller from another.
    pub limit: limit::Settings,
    /// What happens to the audit trail: whether it is written to this
    /// project's database at all, and how long a row is kept. The
    /// defaults write everything and keep it forever, which is
    /// upstream's behaviour and the only safe thing to do to somebody
    /// else's trail without being asked.
    pub audit: audit::Settings,
    /// The external identity providers, GoTrue's GOTRUE_EXTERNAL_*.
    /// Empty is a project with no social login, which is what
    /// /authorize then says about every provider it is asked for.
    pub oauth: oauth::Providers,
    /// What calls the providers. None is a real HTTP client, and this
    /// is here for the same reason `sender` is: a test that cannot
    /// answer as Google can only assert what the code passes to
    /// itself.
    pub http: Option<Arc<dyn oauth::Http>>,
    /// Where the bytes of a stored object go: a directory, or an
    /// `s3://bucket/prefix` url. The same targets the engine takes,
    /// because on a real deployment it is the same bucket with a
    /// different prefix in it. None and the storage surface answers
    /// buckets and refuses everything that needs bytes, which is what
    /// a server started for the auth or rest surfaces alone should do
    /// rather than quietly writing files somewhere.
    pub objects: Option<String>,
    /// Which tenant's prefix on that store the bytes go under. None is
    /// `local`, the ref the engine uses for a store with one database
    /// on it, so a single tenant deployment names nothing and the
    /// files land beside the pages of the database they belong to.
    pub tenant: Option<String>,
    /// The key pair the S3 surface is asked with. None and every signed
    /// request is told the access key it named is not one this project
    /// has, which is the honest thing to say about a project that has
    /// no pair at all.
    pub s3: Option<s3::Credentials>,
    /// When a download is answered with a url to the store instead of
    /// the bytes. None, the default, and every byte of every object is
    /// copied through this process, which is what upstream does and
    /// what the conformance suites measure.
    pub passthrough: Option<object::Passthrough>,
    /// What the realtime tier is allowed: sockets, joins a second,
    /// channels per socket, messages a second, and how big one may be.
    /// Realtime's `TENANT_MAX_*`, whose defaults are the ones a hosted
    /// project is made with. A zero on any of them is that one off,
    /// which is what a server running its own project usually wants.
    pub realtime: zou_realtime::Limits,
    /// The node that holds this project, when this one only holds its
    /// sockets. An http base url like http://10.0.0.4:8000, and the
    /// link to it is a websocket on that same port.
    ///
    /// None, and this node holds everything a socket needs, which is
    /// every single node deployment and every node serving a tenant it
    /// has the lease on. Set, and there is no database here and no
    /// change reader: the topics are carried locally so one message
    /// from the holder reaches every socket here, and the two questions
    /// only a database can answer go up the link.
    pub holder: Option<String>,
    /// How hard a database webhook is tried: how many attempts in
    /// total and how long the first wait between them is. One attempt
    /// is pg_net's own behaviour.
    pub webhook: webhook::Retries,
    /// What is deployed under /functions/v1, and what runs it. None is
    /// a project with no functions, which answers every name the same
    /// 404 upstream answers a name nobody deployed, rather than
    /// pretending the surface is missing: a server that serves no
    /// functions and a server that was never asked to are the same
    /// thing to a caller.
    pub functions: Option<Arc<zou_functions::Registry>>,
}

impl Default for Config {
    /// What a project that configures nothing gets, which is GoTrue's
    /// own defaults wherever it has one.
    fn default() -> Config {
        Config {
            jwt_secret: Vec::new(),
            pg: None,
            pg_request: None,
            rate: None,
            jwks: None,
            schemas: Vec::new(),
            aggregates: true,
            anon_role: "anon".to_string(),
            exposed_roles: Vec::new(),
            external_url: None,
            jwt_keys: None,
            mailer_autoconfirm: false,
            site_url: None,
            secure_email_change: true,
            manual_linking: false,
            reauthentication_required: false,
            anonymous_users: false,
            mail: mail::Settings::default(),
            sender: None,
            dev_inbox: true,
            email_enabled: true,
            disable_signup: false,
            phone_enabled: false,
            sms: sms::Settings::default(),
            texter: None,
            mfa: mfa::Settings::default(),
            hook: hook::Settings::none(),
            limit: limit::Settings::default(),
            audit: audit::Settings::default(),
            oauth: oauth::Providers::default(),
            http: None,
            objects: None,
            tenant: None,
            s3: None,
            passthrough: None,
            realtime: zou_realtime::Limits::default(),
            holder: None,
            webhook: webhook::Retries::default(),
            functions: None,
        }
    }
}

/// The roles a Supabase project exposes to its api, which is what an
/// `authenticator` there has been granted and so the whole of what a
/// `role` claim can usefully name.
pub const PROJECT_ROLES: [&str; 3] = ["anon", "authenticated", "service_role"];

impl Config {
    /// Whether a request may run as `role`, asked before the name
    /// reaches `set_config('role', ...)`.
    ///
    /// The anonymous role is always in, because it is not a claim: it
    /// is what a request that named no role falls back to, and an
    /// operator who renamed it has said so already.
    #[must_use]
    pub fn exposes(&self, role: &str) -> bool {
        role == self.anon_role
            || match self.exposed_roles.is_empty() {
                true => PROJECT_ROLES.contains(&role),
                false => self.exposed_roles.iter().any(|known| known == role),
            }
    }

    /// The same set written out, for the hint on a refusal. A caller
    /// that guessed wrong is told what there was to guess, the way an
    /// unexposed schema is.
    #[must_use]
    pub fn exposed(&self) -> Vec<&str> {
        let mut roles: Vec<&str> = match self.exposed_roles.is_empty() {
            true => PROJECT_ROLES.to_vec(),
            false => self.exposed_roles.iter().map(String::as_str).collect(),
        };
        if !roles.contains(&self.anon_role.as_str()) {
            roles.push(&self.anon_role);
        }
        roles
    }
}

/// The tables a publication carries, schema then table, as the catalog
/// spells them. Shared because every join reads the same one.
pub type Published = Arc<Vec<(String, String)>>;

/// Everything the handlers share: the config and, when postgres is
/// reachable, the session pool. The pool dials lazily, so building
/// this never blocks on the database.
pub struct App {
    pub cfg: Config,
    pub pool: Option<sql::Pool>,
    pub limiter: Option<edge::RateLimit>,
    /// The per endpoint budgets and the two send budgets, built once
    /// from the config because a bucket that was rebuilt per request
    /// would be a bucket that is always full.
    pub limits: limit::Limits,
    /// Everything a bearer token may have been signed by: the jwks an
    /// operator configured, and the public half of this project's own
    /// signing keys.
    pub jwks: Option<jwt::Jwks>,
    /// The private keys this server signs with, when it has any.
    pub keys: Option<jwt::KeySet>,
    /// Where the email flows post their codes. With nothing configured
    /// that is the dev inbox, which keeps them in memory and logs the
    /// link rather than dropping them the way an unconfigured GoTrue
    /// does.
    pub mailer: Arc<dyn mail::Sender>,
    /// Where the phone flows post their codes. With nothing configured
    /// that is the dev sink, which is the whole reason a phone sign in
    /// can be written on a laptop that has no Twilio account.
    pub texter: Arc<dyn sms::Sender>,
    /// What the external providers are called with.
    pub web: Arc<dyn oauth::Http>,
    /// The fk catalog per exposed schema, each tagged with the epoch
    /// it was introspected under. A request reuses it while the epoch
    /// holds and reintrospects when the DDL watch moves it.
    pub catalog: tokio::sync::RwLock<HashMap<String, (u64, Arc<Catalog>)>>,
    /// Bumped by the DDL watch whenever the schema may have changed.
    pub epoch: Arc<AtomicU64>,
    /// What the realtime publication carries, schema and table, tagged
    /// with the epoch it was read under. A join naming a table has to
    /// know, and asking the catalog once per join is a hundred thousand
    /// reads of one answer on a node holding a hundred thousand
    /// sockets, which is what makes joining slower the more of them
    /// there are. `alter publication` is DDL, so the same watch that
    /// moves the rest of the catalog on moves this.
    pub published: tokio::sync::RwLock<Option<(u64, Published)>>,
    /// The watch starts on the first request that needs a catalog,
    /// because a router can be built outside a runtime.
    pub watching: tokio::sync::OnceCell<()>,
    /// The listener that carries what `realtime.send()` writes to the
    /// sockets, started on the first socket for the same reason: a
    /// message sent while nothing is connected is heard by nobody
    /// whether this is running or not.
    pub sending: tokio::sync::OnceCell<()>,
    /// Where object bytes live, when this server was told.
    pub blobs: Option<blob::Blobs>,
    /// The realtime topics this server is carrying, and the sockets on
    /// each. One per server rather than one per connection, which is
    /// what makes two browser tabs on the same channel hear each
    /// other.
    pub hub: zou_realtime::Hub,
    /// Where a socket's topics, changes and answers come from: this
    /// node, or the node that holds this tenant over one link per
    /// tenant. Everything the socket loop does goes through it, so the
    /// two are the same code with a different answer to one question.
    pub source: source::Source,
    /// What the sockets have spent of the realtime budget: how many
    /// are connected, how fast they are joining, how many messages a
    /// second are moving. One per server, like the hub, because the
    /// budget is the project's and not a connection's.
    pub quota: Arc<realtime::Quota>,
    /// What the links that dropped left behind, kept for a grace in
    /// case they dial again. A node's link going is almost always a
    /// node's link coming back, and closing every socket behind it in
    /// the meantime is a reconnect storm this end asked for.
    pub links: fanout::Dropped,
    /// Who is subscribed to postgres changes and what they asked for,
    /// one per server for the same reason the hub is: the one thing
    /// reading the database has to be able to find every subscription
    /// on it.
    pub changes: Arc<reader::Changes>,
    /// The loop that does the reading, started by the first socket that
    /// subscribes to anything. Not at boot, because a replication slot
    /// nobody is reading from is a database whose write ahead log
    /// cannot be freed.
    pub reading: tokio::sync::OnceCell<()>,
    /// The loop that makes the calls a database webhook queued,
    /// started by the first request through the gate. A queue only
    /// fills because a transaction committed, and a transaction only
    /// happens because somebody arrived.
    pub dispatching: tokio::sync::OnceCell<()>,
    /// The loop that runs the jobs a project scheduled, started by the
    /// same first request. Only one node ends up firing them, which is
    /// an advisory lock rather than a decision made here.
    pub ticking: tokio::sync::OnceCell<()>,
    /// The loop that deletes audit entries older than the project's
    /// retention, started by the same first request and doing nothing
    /// at all unless a retention was set. One node deletes, on the same
    /// terms the ticker fires on.
    pub pruning: tokio::sync::OnceCell<()>,
}

impl App {
    /// The `iss` claim of the access tokens this server signs.
    /// GoTrue's default API_EXTERNAL_URL is http://localhost:9999, so
    /// an unconfigured zou issues what an unconfigured GoTrue does.
    pub fn issuer(&self) -> String {
        let base = self
            .cfg
            .external_url
            .as_deref()
            .unwrap_or("http://localhost:9999");
        format!("{}/auth/v1", base.trim_end_matches('/'))
    }

    /// Where a followed confirmation link lands. GoTrue requires
    /// SITE_URL to be set and the Supabase CLI puts localhost:3000
    /// there, which is the framework dev server every quickstart runs.
    pub fn site_url(&self) -> String {
        self.cfg
            .site_url
            .clone()
            .unwrap_or_else(|| "http://localhost:3000".to_string())
    }

    /// What signs an access token: the project's signing key when one
    /// is configured, the project secret otherwise.
    pub fn signer(&self) -> jwt::Signer<'_> {
        match &self.keys {
            Some(keys) => jwt::Signer::Keys(keys),
            None => jwt::Signer::Secret(&self.cfg.jwt_secret),
        }
    }
}

/// PostgREST's default db-pool size, a sane dev loop default here too.
///
/// Public because it is one of the three numbers that have to fit
/// inside a tenant postmaster's `max_connections`, and the command line
/// that sets that one has to be able to add them up.
pub const POOL_SIZE: usize = 10;

fn app_state(mut cfg: Config) -> Result<Arc<App>, String> {
    if cfg.schemas.is_empty() {
        cfg.schemas.push("public".to_string());
    }
    let pool = match (&cfg.pg, &cfg.pg_request) {
        (Some(dsn), Some(request)) => Some(
            sql::Pool::with_request(dsn, request, POOL_SIZE).map_err(|e| format!("pg dsn: {e}"))?,
        ),
        (Some(dsn), None) => {
            Some(sql::Pool::new(dsn, POOL_SIZE).map_err(|e| format!("pg dsn: {e}"))?)
        }
        (None, _) => None,
    };
    if let Some(pool) = &pool {
        pool.write_audit_rows(!cfg.audit.disable_postgres);
    }
    let limiter = cfg.rate.map(edge::RateLimit::new);
    let limits = limit::Limits::new(cfg.limit.clone());
    let configured = match &cfg.jwks {
        Some(json) => Some(jwt::Jwks::parse(json).map_err(|e| format!("jwks: {e}"))?),
        None => None,
    };
    let keys = match &cfg.jwt_keys {
        Some(json) => Some(jwt::KeySet::parse(json).map_err(|e| format!("jwt keys: {e}"))?),
        None => None,
    };
    // A rotation is two keys at once, so the set this verifies against
    // is the union rather than whichever was configured last.
    let jwks = match (configured, keys.as_ref().map(jwt::KeySet::verifiers)) {
        (Some(a), Some(b)) => Some(a.and(b)),
        (a, b) => a.or(b),
    };
    let mailer: Arc<dyn mail::Sender> = match cfg.sender.take() {
        Some(sender) => sender,
        None if cfg.dev_inbox => Arc::new(mail::Inbox::default()),
        None => Arc::new(mail::Nowhere),
    };
    let texter: Arc<dyn sms::Sender> = match cfg.texter.take() {
        Some(texter) => texter,
        None => Arc::new(sms::Sink::default()),
    };
    let web: Arc<dyn oauth::Http> = match cfg.http.take() {
        Some(http) => http,
        None => Arc::new(oauth::Web::default()),
    };
    let tenant = cfg.tenant.as_deref().unwrap_or(blob::LOCAL);
    let blobs = match &cfg.objects {
        Some(target) => {
            Some(blob::Blobs::open(target, tenant).map_err(|e| format!("objects: {e}"))?)
        }
        None => None,
    };
    let quota = Arc::new(realtime::Quota::new(cfg.realtime));
    let source = match &cfg.holder {
        Some(holder) => source::Source::Away(Arc::new(fanout::Away::new(holder, &cfg.jwt_secret))),
        None => source::Source::Held,
    };
    Ok(Arc::new(App {
        cfg,
        pool,
        limiter,
        limits,
        jwks,
        keys,
        mailer,
        texter,
        web,
        catalog: tokio::sync::RwLock::new(HashMap::new()),
        epoch: Arc::new(AtomicU64::new(0)),
        published: tokio::sync::RwLock::new(None),
        watching: tokio::sync::OnceCell::new(),
        sending: tokio::sync::OnceCell::new(),
        blobs,
        hub: zou_realtime::Hub::new(),
        source,
        quota,
        links: fanout::Dropped::default(),
        changes: Arc::new(reader::Changes::new()),
        reading: tokio::sync::OnceCell::new(),
        dispatching: tokio::sync::OnceCell::new(),
        ticking: tokio::sync::OnceCell::new(),
        pruning: tokio::sync::OnceCell::new(),
    }))
}

/// The verified identity of a request, deposited in request extensions
/// by the gate. role is the bearer token's role claim when a bearer
/// was sent, otherwise the apikey's, otherwise anon, the same
/// precedence PostgREST applies behind Supabase.
#[derive(Clone)]
pub struct AuthContext {
    pub role: String,
    pub claims: Arc<serde_json::Value>,
}

pub(crate) fn json_body(status: StatusCode, body: serde_json::Value) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

/// An answer with nothing in it, which is what a logout is: there is
/// no body to send and no content type to claim for it.
pub(crate) fn no_content() -> Response {
    (StatusCode::NO_CONTENT, ()).into_response()
}

/// The apikey from the header or, failing that, the url query string.
fn apikey_of(req: &Request<Body>) -> Option<String> {
    if let Some(v) = req.headers().get("apikey") {
        return v.to_str().ok().map(str::to_string);
    }
    let query = req.uri().query()?;
    query.split('&').find_map(|pair| {
        pair.strip_prefix("apikey=")
            .map(|v| v.split('#').next().unwrap_or(v).to_string())
    })
}

/// The gate every Supabase prefix sits behind. The error bodies for a
/// missing or invalid apikey are the ones the hosted edge sends, so a
/// misconfigured client sees the exact message it would see against
/// Supabase.
async fn gate(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let Some(apikey) = apikey_of(&req) else {
        return json_body(
            StatusCode::UNAUTHORIZED,
            serde_json::json!({
                "message": "No API key found in request",
                "hint": "No `apikey` request header or url param was found.",
            }),
        );
    };
    // Which server a caller thinks it is talking to decides how its
    // clock is read, because the two references disagree by half a
    // minute and the difference is a phone with a slow clock rather
    // than an attack. jwt::Clock says what each one does.
    let clock = match req.uri().path().starts_with("/rest/v1/") {
        true => jwt::Clock::Postgrest,
        false => jwt::Clock::Exact,
    };
    let key = match jwt::verify_by(&apikey, &app.cfg.jwt_secret, clock) {
        Ok(v) => v,
        Err(_) => {
            return json_body(
                StatusCode::UNAUTHORIZED,
                serde_json::json!({"message": "Invalid API key"}),
            );
        }
    };

    let bearer = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_string);
    let identity = match bearer {
        Some(token) => {
            match jwt::verify_any_by(&token, &app.cfg.jwt_secret, app.jwks.as_ref(), clock) {
                Ok(v) => v,
                // The auth surface is told about a bad token in GoTrue's
                // words and everything else in the edge's, because that is
                // who a client thinks it is talking to. Upstream's gateway
                // never looks at the bearer token at all, so the refusal a
                // client sees for /auth/v1 is the one GoTrue itself wrote.
                Err(why) if req.uri().path().starts_with("/auth/v1/") => {
                    return auth::error_body(StatusCode::FORBIDDEN, "bad_jwt", &why.gotrue());
                }
                // And the same reasoning one door along. Upstream's
                // gateway does not read the bearer at all, so what a
                // client sees on /rest/v1 is PostgREST's own refusal,
                // which is a code and a challenge rather than a line.
                Err(why) if req.uri().path().starts_with("/rest/v1/") => {
                    return rest::token_refused(&why, &token);
                }
                Err(why) => {
                    return json_body(
                        StatusCode::UNAUTHORIZED,
                        serde_json::json!({"message": why.as_str()}),
                    );
                }
            }
        }
        None => key,
    };

    // The claim is carried as it was written, not checked here. What
    // may be entered as a database role is a narrower question than
    // what a token may say, and the two surfaces that ask it are the
    // ones that put the name into a session: an auth request from a
    // user whose role column is something a project made up is still
    // that user's request, and GoTrue answers it.
    req.extensions_mut().insert(AuthContext {
        role: identity.role.unwrap_or_else(|| app.cfg.anon_role.clone()),
        claims: Arc::new(identity.claims),
    });
    // A router can be built outside a runtime, so what makes the calls
    // a webhook queued starts on the first request rather than at
    // boot. Nothing queues one without a request having arrived.
    app.dispatching
        .get_or_init(|| async { webhook::dispatch(Arc::clone(&app)) })
        .await;
    app.ticking
        .get_or_init(|| async { cron::tick(Arc::clone(&app)) })
        .await;
    app.pruning
        .get_or_init(|| async { audit::prune(Arc::clone(&app)) })
        .await;
    next.run(req).await
}

/// The rate limit, sitting outside the gate so a hammering client is
/// refused before its keys are even verified. Keyed on the apikey
/// when one was sent, otherwise everything unkeyed shares a bucket,
/// good enough for the skeleton until per endpoint budgets land with
/// the auth surface. The 429 body is the message the hosted edge
/// sends.
async fn rate_limit(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    if let Some(limiter) = &app.limiter {
        let key = apikey_of(&req).unwrap_or_else(|| "unkeyed".to_string());
        if !limiter.allow(&key) {
            let mut res = json_body(
                StatusCode::TOO_MANY_REQUESTS,
                serde_json::json!({"message": "API rate limit exceeded"}),
            );
            if let Ok(v) = header::HeaderValue::from_str(&limiter.retry_after().to_string()) {
                res.headers_mut().insert(header::RETRY_AFTER, v);
            }
            return res;
        }
    }
    next.run(req).await
}

/// GoTrue's health shape, served locally: the auth surface is going to
/// be a reimplementation, and health is its first endpoint.
async fn auth_health() -> Response {
    json_body(
        StatusCode::OK,
        serde_json::json!({
            "version": concat!("zou-", env!("CARGO_PKG_VERSION")),
            "name": "GoTrue",
            "description": "GoTrue is a user registration and authentication API",
        }),
    )
}

/// Whether this request may read the mail, and whether there is any to
/// read. Both answers are the same 404, because a route that exists
/// only for the right caller should not be discoverable by the wrong
/// one.
///
/// The service role is the bar, and it is the right bar: a caller
/// holding that key can already mint a session for anybody, so being
/// able to read a recovery code tells it nothing it could not have
/// helped itself to. The anon key is public, so it is not a bar at
/// all.
fn readable_inbox<'a>(app: &'a App, ctx: &AuthContext) -> Option<&'a mail::Inbox> {
    match ctx.role.as_str() {
        "service_role" => app.mailer.inbox(),
        _ => None,
    }
}

/// The same question for text messages, and the same answer. The two
/// media are asked separately because a project may well have a mail
/// server and no SMS provider, and the codes that are still being kept
/// in the process are still the ones a person on a laptop needs.
fn readable_sink<'a>(app: &'a App, ctx: &AuthContext) -> Option<&'a sms::Sink> {
    match ctx.role.as_str() {
        "service_role" => app.texter.sink(),
        _ => None,
    }
}

/// GET /dev/inbox, everything the dev inbox is holding, oldest first.
///
/// This is the local loop's mailbox. Nobody is carrying these
/// anywhere, so reading them here is the only way to follow a
/// confirmation link or read a texted code on a laptop, and `zou inbox`
/// is this endpoint with a terminal in front of it. It exists only
/// while there is no transport configured, because with one it would be
/// a way to read somebody else's codes.
async fn dev_inbox(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
) -> Response {
    let (inbox, sink) = (readable_inbox(&app, &ctx), readable_sink(&app, &ctx));
    if inbox.is_none() && sink.is_none() {
        return kong_no_route();
    }
    let messages: Vec<serde_json::Value> = match inbox {
        Some(inbox) => inbox.kept().iter().map(mail::Mail::as_json).collect(),
        None => Vec::new(),
    };
    let texts: Vec<serde_json::Value> = match sink {
        Some(sink) => sink.kept().iter().map(sms::Text::as_json).collect(),
        None => Vec::new(),
    };
    json_body(
        StatusCode::OK,
        serde_json::json!({"messages": messages, "texts": texts}),
    )
}

/// DELETE /dev/inbox, throw the kept messages away, which is what a
/// test or a person starting a fresh flow wants.
async fn dev_inbox_clear(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
) -> Response {
    let (inbox, sink) = (readable_inbox(&app, &ctx), readable_sink(&app, &ctx));
    if inbox.is_none() && sink.is_none() {
        return kong_no_route();
    }
    if let Some(inbox) = inbox {
        inbox.clear();
    }
    if let Some(sink) = sink {
        sink.clear();
    }
    json_body(
        StatusCode::OK,
        serde_json::json!({"messages": [], "texts": []}),
    )
}

/// GET /auth/v1/.well-known/jwks.json, the public half of the
/// project's signing keys.
///
/// No apikey. This is the one endpoint on the whole surface that has
/// to be reachable by anything holding a token: a backend in another
/// language verifying an access token fetches this with a plain http
/// client and no Supabase credentials at all, and hosted Supabase
/// serves it the same way. A project on the legacy HS256 secret
/// publishes nothing, and an empty key set is the correct answer
/// rather than an error, because the secret is not something to hand
/// out.
async fn well_known_jwks(axum::extract::State(app): axum::extract::State<Arc<App>>) -> Response {
    let keys = match &app.keys {
        Some(keys) => keys.published(),
        None => serde_json::json!({"keys": []}),
    };
    let mut res = json_body(StatusCode::OK, keys);
    res.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("public, max-age=600"),
    );
    res
}

/// The checklist a surface's missing pieces are boxes on, one per
/// surface. Naming the milestone was the old answer and it stopped
/// being a useful one: storage and realtime are served, and what still
/// reaches a stub under either of them is a box rather than a
/// milestone.
pub(crate) mod tracked {
    pub const REST: u32 = 125;
    pub const AUTH: u32 = 170;
    pub const STORAGE: u32 = 190;
    pub const REALTIME: u32 = 303;
}

/// An honest placeholder for a route that exists in the router but not
/// yet in the code.
///
/// 501 rather than 404 because a 404 reads as a wrong url and sends
/// somebody looking for a typo they did not make. The issue is in the
/// body for the same reason the rest of this tree's errors carry a next
/// step: knowing that a gap is written down somewhere is not knowing
/// where, and the difference between the two is whether the reader can
/// go and see if their case is a box that is nearly ticked or one
/// nobody has started.
pub(crate) fn not_yet(surface: &str, tracked: u32) -> Response {
    json_body(
        StatusCode::NOT_IMPLEMENTED,
        serde_json::json!({
            "message": format!(
                "{surface} is not implemented yet, it is tracked on https://github.com/tamnd/zou/issues/{tracked}"
            ),
        }),
    )
}

async fn auth_stub() -> Response {
    not_yet("this auth endpoint", tracked::AUTH)
}
async fn storage_stub() -> Response {
    not_yet("the storage surface", tracked::STORAGE)
}
async fn realtime_stub() -> Response {
    not_yet("the realtime surface", tracked::REALTIME)
}

/// Anything outside the four prefixes, in the words the hosted edge
/// uses for an unmatched route.
///
/// Except a url that was aiming at the rest surface, which hears from
/// the rest surface instead. The test is the string rather than the
/// segments on purpose: a client that dropped a slash is talking to
/// this api and should be told so in the shape it can read, not handed
/// the gateway's line about routes. The bare prefix with nothing after
/// it is not aiming at anything, and keeps the gateway's words.
async fn no_route(uri: axum::http::Uri) -> Response {
    match uri.path().strip_prefix(REST_PREFIX) {
        Some(rest) if !rest.is_empty() => rest::invalid_path(),
        _ => kong_no_route(),
    }
}

/// The gateway's words for a url it has no route for, which a surface
/// that is switched off answers with too, so that off is
/// indistinguishable from never built.
fn kong_no_route() -> Response {
    json_body(
        StatusCode::NOT_FOUND,
        serde_json::json!({"message": "no Route matched with those values"}),
    )
}

/// Where the rest surface lives, without its trailing slash.
pub(crate) const REST_PREFIX: &str = "/rest/v1";

/// The whole front door as one axum router. Layer order matters:
/// request id outermost so even a 404 or a 429 carries one, CORS next
/// so preflights never reach the gate and every response with an
/// Origin gets its mirror, then the rate limit, then the apikey gate.
pub fn router(cfg: Config) -> Result<Router, String> {
    routed(cfg).map(|(router, _)| router)
}

/// The same, with the state it was built on handed back.
///
/// For a caller that has to reach into the server it just built rather
/// than only serve it. There is one: the fan out socket tier, which
/// holds a router for a project another node writes and has to be able
/// to tell the sockets on it that the project has moved.
pub fn routed(cfg: Config) -> Result<(Router, Arc<App>), String> {
    let app = app_state(cfg)?;
    let gated = Router::new()
        .route("/auth/v1/health", get(auth_health))
        // What a sign in screen reads before it draws itself. It sits
        // inside the gate because the hosted edge puts it there, not
        // because there is a secret in it.
        .route("/auth/v1/settings", get(auth::settings))
        .route("/auth/v1/token", post(auth::token))
        .route("/auth/v1/signup", post(auth::signup))
        .route("/auth/v1/recover", post(auth::recover))
        .route("/auth/v1/magiclink", post(auth::magiclink))
        .route("/auth/v1/otp", post(auth::otp))
        .route(
            "/auth/v1/reauthenticate",
            get(auth::reauthenticate).post(auth::reauthenticate_posted),
        )
        .route("/auth/v1/resend", post(auth::resend))
        // Throwing a session away is a fetch carrying the token being
        // thrown away, so it lives inside the gate with the rest.
        .route("/auth/v1/logout", post(auth::logout))
        .route("/auth/v1/user", put(auth::user_update).get(auth::user_get))
        // Manual identity linking. Both of these are fetches carrying a
        // bearer token rather than navigations, which is why they are
        // inside the gate while /authorize is not, and both answer 404
        // until a project turns linking on.
        .route(
            "/auth/v1/user/identities/authorize",
            get(auth::link_identity),
        )
        .route(
            "/auth/v1/user/identities/{identity_id}",
            delete(auth::unlink_identity),
        )
        // The second factor. All four carry a bearer token and all four
        // read or move the session it names, so they belong inside the
        // gate rather than beside /verify.
        .route("/auth/v1/factors", post(mfa::enroll))
        .route("/auth/v1/factors/{factor_id}", delete(mfa::unenroll))
        .route(
            "/auth/v1/factors/{factor_id}/challenge",
            post(mfa::challenge),
        )
        .route("/auth/v1/factors/{factor_id}/verify", post(mfa::verify))
        // The admin box. Everything under it is the service role
        // acting on somebody else's account, and every one of these
        // refuses anything that is not holding an admin role, so the
        // apikey gate in front is the outer of two doors rather than
        // the only one.
        .route(
            "/auth/v1/admin/users",
            get(admin::users).post(admin::user_create),
        )
        .route(
            "/auth/v1/admin/users/{user_id}",
            get(admin::user_get)
                .put(admin::user_update)
                .delete(admin::user_delete),
        )
        .route(
            "/auth/v1/admin/users/{user_id}/factors",
            get(admin::user_factors),
        )
        .route(
            "/auth/v1/admin/users/{user_id}/factors/{factor_id}",
            put(admin::factor_update).delete(admin::factor_delete),
        )
        .route("/auth/v1/admin/audit", get(admin::audit_log))
        .route("/auth/v1/admin/generate_link", post(admin::generate_link))
        // Upstream keeps the invitation outside the admin box and behind
        // the same role, so this is where it is.
        .route("/auth/v1/invite", post(admin::invite))
        .route("/auth/v1/{*rest}", any(auth_stub))
        .route("/rest/v1/", any(rest::root))
        .route("/rest/v1/rpc/{func}", any(rest::rpc))
        .route("/rest/v1/{table}", any(rest::table))
        // Three spellings each, because a stubbed surface is stubbed all
        // the way to its root. A wildcard segment matches neither the
        // empty path nor the bare prefix, so without these two extra
        // routes the shortest url under a surface that does not exist
        // yet would be answered as a url that does not exist at all,
        // which is a different and more confusing thing to hear.
        .route("/storage/v1", any(storage_stub))
        .route("/storage/v1/", any(storage_stub))
        .route("/storage/v1/{*rest}", any(storage_stub))
        // The socket and the two ways of sending to a topic without
        // one. Everything else under the prefix is still the honest
        // 501.
        .route("/realtime/v1/websocket", any(realtime::websocket))
        // The link between a node with the sockets and the node with
        // the tenant. Under the same gate as everything else, because
        // what opens it is the project's own service key.
        .route("/realtime/v1/link", any(fanout::link))
        .route("/realtime/v1/api/broadcast", post(realtime::broadcast))
        .route(
            "/realtime/v1/api/broadcast/{topic}/events/{event}",
            post(realtime::broadcast_one),
        )
        .route("/realtime/v1", any(realtime_stub))
        .route("/realtime/v1/", any(realtime_stub))
        .route("/realtime/v1/{*rest}", any(realtime_stub))
        // The local loop's mailbox. It answers only while nothing is
        // carrying the mail anywhere and only to the service role,
        // and 404s otherwise, so a project that configures a
        // transport loses it and a project that does not has not
        // opened its codes to the internet.
        .route("/dev/inbox", get(dev_inbox).delete(dev_inbox_clear))
        .layer(middleware::from_fn_with_state(Arc::clone(&app), gate))
        .layer(middleware::from_fn_with_state(Arc::clone(&app), rate_limit))
        .with_state(Arc::clone(&app));
    // Outside the gate, deliberately. A verifier fetching the public
    // keys has no apikey and no reason to have one, and neither does
    // the link in a confirmation email: it is clicked in a mail client
    // that knows nothing about the project, which is why the hosted
    // gateway leaves /auth/v1/verify open too.
    let open = Router::new()
        .route("/auth/v1/.well-known/jwks.json", get(well_known_jwks))
        .route("/auth/v1/verify", get(auth::verify_get).post(auth::verify))
        // Both halves of a social sign in are navigations rather than
        // fetches: the first is a link or a form post the app hands to
        // the browser, the second is the provider redirecting back.
        // Neither carries an apikey and neither can be made to, which
        // is why the hosted gateway leaves them open too.
        .route("/auth/v1/authorize", get(auth::authorize))
        .route(
            "/auth/v1/callback",
            get(auth::callback).post(auth::callback_form),
        )
        // The bucket surface, and outside the gate for a different
        // reason than the rest of this router. storage-api sits behind
        // the same hosted gateway everything else does, but it answers
        // a request carrying no key in its own words rather than the
        // gateway's, and it reads its token from the authorization
        // header alone. Behind the gate a missing key would be refused
        // one layer too early and in the wrong sentence, so the gate is
        // in front of the surfaces that agree with it and these six
        // check their own caller. The paths below the buckets still
        // land on the stub in the gated router, because a wildcard
        // segment loses to a literal one.
        .route(
            "/storage/v1/bucket",
            get(storage::list).post(storage::create),
        )
        .route(
            "/storage/v1/bucket/{id}",
            get(storage::get)
                .put(storage::update)
                .delete(storage::remove),
        )
        .route("/storage/v1/bucket/{id}/empty", post(storage::empty))
        // The object surface, on the same side of the gate as the
        // buckets and for the same reason. Six paths for what is really
        // three questions, because upstream lets a client say out loud
        // which audience it is asking as, and a route that says
        // `public` is asked with no token at all.
        //
        // A literal segment beats a placeholder, so `public`, `info`
        // and `authenticated` reach their own handlers while every
        // other first segment is read as a bucket id. That is upstream's
        // arrangement too, and it is why a bucket called `public` is a
        // bucket nobody can name here.
        .route(
            "/storage/v1/object/{bucket}",
            axum::routing::delete(object::remove_many),
        )
        // The two listings, which are both posts because what they ask
        // for is a body rather than a path. They are literal segments
        // ahead of the bucket placeholder for the same reason `public`
        // is, so a bucket called `list` is another bucket nobody can
        // name here.
        .route("/storage/v1/object/list/{bucket}", post(object::list))
        .route("/storage/v1/object/list-v2/{bucket}", post(object::list_v2))
        // Move and copy name both ends in the body, so there is no
        // bucket in either path and nothing to collide with a bucket
        // id below them.
        .route("/storage/v1/object/move", post(object::move_object))
        .route("/storage/v1/object/copy", post(object::copy_object))
        // Signing, and spending what was signed. `sign` with a bucket
        // and a name asks for one url and `sign` with only a bucket
        // asks for a list of them, which is two routes rather than one
        // because the names are in the body in the second and in the
        // path in the first.
        //
        // The get and the put are the other end: they carry the token
        // in the query string and no authorization header at all, which
        // is why they are the only two paths here that are not read
        // through `caller`.
        .route("/storage/v1/object/sign/{bucket}", post(object::sign_many))
        .route(
            "/storage/v1/object/sign/{bucket}/{*name}",
            get(object::signed_download).post(object::sign),
        )
        .route(
            "/storage/v1/object/upload/sign/{bucket}/{*name}",
            post(object::sign_upload).put(object::upload_signed),
        )
        .route(
            "/storage/v1/object/{bucket}/{*name}",
            get(object::download)
                .post(object::upload)
                .put(object::replace)
                .delete(object::remove),
        )
        .route(
            "/storage/v1/object/authenticated/{bucket}/{*name}",
            get(object::download_authenticated),
        )
        .route(
            "/storage/v1/object/public/{bucket}/{*name}",
            get(object::download_public),
        )
        // The transforms, which are three doors onto the object routes
        // above rather than a surface of their own. They are their own
        // paths and not a query parameter on a download because that is
        // how the client library builds them, and the `sign` one is
        // where a url signed with a transform in it lands.
        .route(
            "/storage/v1/render/image/authenticated/{bucket}/{*name}",
            get(render::authenticated),
        )
        .route(
            "/storage/v1/render/image/public/{bucket}/{*name}",
            get(render::public),
        )
        .route(
            "/storage/v1/render/image/sign/{bucket}/{*name}",
            get(render::signed),
        )
        .route(
            "/storage/v1/object/info/{bucket}/{*name}",
            get(object::info),
        )
        .route(
            "/storage/v1/object/info/public/{bucket}/{*name}",
            get(object::info_public),
        )
        // Resumable uploads, which are their own protocol rather than
        // another shape of the object routes above: the answers are
        // headers, the refusals carry real statuses, and the bucket and
        // the name are metadata rather than path. Both paths carry a
        // fallback, because a method the protocol does not define is
        // answered by upstream's router rather than by axum's, and the
        // two say different things.
        //
        // The options is the one route on this surface asked with no
        // token at all. A browser sends it before it sends anything
        // else and sends none of its own headers on it.
        .route("/storage/v1/upload/resumable", any(tus::endpoint))
        .route("/storage/v1/upload/resumable/{id}", any(tus::upload))
        // The S3 protocol, on this side of the gate for a third reason
        // again: it is not asked with a key at all. The authorization
        // header carries a signature over the request, so a gate that
        // wanted an apikey in front of it would refuse every client
        // that speaks this protocol correctly.
        //
        // Both spellings of the endpoint, because a client that lists
        // buckets sends the trailing slash and a client that was handed
        // the endpoint url does not, and the two are signed as the two
        // different paths they are.
        // The functions surface, outside the gate for the same reason
        // the buckets are: the gateway in front of upstream's runtime
        // has no key check on this route, so a call carrying no key at
        // all reaches the runtime and is refused in the runtime's own
        // words. Both spellings of a call are one handler, since the
        // name and the path after it are read off the url either way,
        // and `/functions/v1/` with no name at all is upstream's text
        // 404 rather than the gateway's json one.
        .route("/functions/v1/", any(functions::unnamed))
        .route("/functions/v1/{name}", any(functions::call))
        .route("/functions/v1/{name}/{*rest}", any(functions::call))
        .route("/storage/v1/s3", get(s3::list_buckets))
        .route("/storage/v1/s3/", get(s3::list_buckets))
        .route("/storage/v1/s3/{bucket}", any(s3::bucket))
        .route("/storage/v1/s3/{bucket}/{*key}", any(s3::object))
        .with_state(Arc::clone(&app));
    Ok((
        Router::new()
            .merge(open)
            .merge(gated)
            .fallback(no_route)
            // The per endpoint budgets, over both routers because /verify
            // is outside the gate and is limited too. Inside the envelope
            // so a client on the newer api version hears about a 429 in
            // the shape it asked for.
            .layer(middleware::from_fn_with_state(
                Arc::clone(&app),
                limit::guard,
            ))
            // Inside the request id, because the id is what a failure of
            // this server's own carries back, and outside everything that
            // answers, because every auth refusal leaves through it.
            .layer(middleware::from_fn(auth::envelope))
            .layer(middleware::from_fn(edge::cors))
            .layer(middleware::from_fn(edge::request_id))
            // Outermost, so what is counted is what a caller waited for,
            // including the refusals: a graph of a rate limit or a 404 is
            // drawn from the same series as a graph of the answers.
            .layer(middleware::from_fn(ops::measure)),
        app,
    ))
}

/// Serve `router(cfg)` on `listener` forever. Builds a private tokio
/// runtime, so a plain thread can own the whole server: the listener
/// is bound by the caller while it can still report errors cheaply,
/// this end only converts and serves.
pub fn serve_blocking(listener: std::net::TcpListener, cfg: Config) -> Result<(), String> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;
    rt.block_on(async move {
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("nonblocking: {e}"))?;
        let listener =
            tokio::net::TcpListener::from_std(listener).map_err(|e| format!("listener: {e}"))?;
        // With the peer address kept, because it is the last thing a
        // rate limit can be keyed on when nothing is forwarding, and
        // because it is what the audit trail and the auth hooks are
        // told the request came from when there is no header to read.
        let service = router(cfg)?.into_make_service_with_connect_info::<std::net::SocketAddr>();
        axum::serve(listener, service)
            .await
            .map_err(|e| format!("serve: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use tower::ServiceExt;

    const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

    fn app() -> Router {
        router(Config {
            jwt_secret: SECRET.to_vec(),
            ..Config::default()
        })
        .unwrap()
    }

    fn anon_key() -> String {
        jwt::mint(&jwt::key_claims("anon"), SECRET)
    }

    /// One signing key, the shape a project has after Supabase
    /// creates its first asymmetric key.
    fn keys_json() -> String {
        use base64ct::{Base64UrlUnpadded, Encoding};
        use p256::ecdsa::SigningKey;
        let sk = SigningKey::from_slice(&[12u8; 32]).unwrap();
        let point = sk.verifying_key().to_sec1_point(false);
        serde_json::json!([{
            "kty": "EC",
            "crv": "P-256",
            "kid": "in-use",
            "alg": "ES256",
            "key_ops": ["sign", "verify"],
            "d": Base64UrlUnpadded::encode_string(&sk.to_bytes()),
            "x": Base64UrlUnpadded::encode_string(point.x().unwrap()),
            "y": Base64UrlUnpadded::encode_string(point.y().unwrap()),
        }])
        .to_string()
    }

    fn config_with_keys() -> Config {
        Config {
            jwt_secret: SECRET.to_vec(),
            jwt_keys: Some(keys_json()),
            ..Config::default()
        }
    }

    fn app_with_keys() -> Router {
        router(config_with_keys()).unwrap()
    }

    async fn body_json(res: Response) -> serde_json::Value {
        let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn get_req(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn no_apikey_gets_the_edge_error_shape() {
        let res = app().oneshot(get_req("/rest/v1/todos")).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        let body = body_json(res).await;
        assert_eq!(body["message"], "No API key found in request");
        assert_eq!(
            body["hint"],
            "No `apikey` request header or url param was found."
        );
    }

    #[tokio::test]
    async fn the_jwks_endpoint_needs_no_apikey_and_publishes_no_secrets() {
        // No apikey header on purpose. Whoever verifies a token has
        // the token, not the project's credentials.
        let res = app_with_keys()
            .oneshot(get_req("/auth/v1/.well-known/jwks.json"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()["cache-control"], "public, max-age=600");
        let body = body_json(res).await;
        let keys = body["keys"].as_array().unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0]["kid"], "in-use");
        assert_eq!(keys[0]["kty"], "EC");
        assert_eq!(keys[0]["crv"], "P-256");
        assert_eq!(keys[0]["alg"], "ES256");
        assert_eq!(keys[0]["use"], "sig");
        assert_eq!(keys[0]["key_ops"], serde_json::json!(["verify"]));
        assert!(keys[0].get("d").is_none(), "the private scalar stays here");
    }

    #[tokio::test]
    async fn a_project_on_the_legacy_secret_publishes_an_empty_set() {
        // Not a 404 and not an error. The endpoint exists, the project
        // just has nothing asymmetric to hand out, and its secret is
        // not something to publish.
        let res = app()
            .oneshot(get_req("/auth/v1/.well-known/jwks.json"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_json(res).await, serde_json::json!({"keys": []}));
    }

    #[tokio::test]
    async fn what_the_key_set_signs_the_published_set_verifies() {
        let state = app_state(config_with_keys()).unwrap();
        let token = state
            .signer()
            .sign(&serde_json::json!({"role": "authenticated", "sub": "u1"}));

        // What an outside service does: fetch the endpoint, verify with
        // nothing else. The secret handed in is deliberately wrong, so
        // only the published key can be what accepted this.
        let published = body_json(
            app_with_keys()
                .oneshot(get_req("/auth/v1/.well-known/jwks.json"))
                .await
                .unwrap(),
        )
        .await;
        let jwks = jwt::Jwks::parse(&published.to_string()).unwrap();
        let verified = jwt::verify_any(&token, b"the wrong secret", Some(&jwks)).unwrap();
        assert_eq!(verified.role.as_deref(), Some("authenticated"));

        // And the gate in front of the project agrees.
        let echo = Router::new()
            .route(
                "/echo",
                get(|axum::Extension(ctx): axum::Extension<AuthContext>| async move { ctx.role }),
            )
            .layer(middleware::from_fn_with_state(Arc::clone(&state), gate))
            .with_state(state);
        let req = Request::builder()
            .uri("/echo")
            .header("apikey", anon_key())
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let res = echo.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        assert_eq!(&bytes[..], b"authenticated");
    }

    #[tokio::test]
    async fn the_legacy_secret_still_verifies_while_the_keys_are_in_place() {
        // The migration state: keys configured, old tokens still out
        // there. Both have to pass or the rotation logs everyone out.
        let state = app_state(config_with_keys()).unwrap();
        let legacy = jwt::mint(
            &serde_json::json!({"role": "authenticated", "sub": "u1"}),
            SECRET,
        );
        let echo = Router::new()
            .route(
                "/echo",
                get(|axum::Extension(ctx): axum::Extension<AuthContext>| async move { ctx.role }),
            )
            .layer(middleware::from_fn_with_state(Arc::clone(&state), gate))
            .with_state(state);
        let req = Request::builder()
            .uri("/echo")
            .header("apikey", anon_key())
            .header("authorization", format!("Bearer {legacy}"))
            .body(Body::empty())
            .unwrap();
        let res = echo.oneshot(req).await.unwrap();
        let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        assert_eq!(&bytes[..], b"authenticated");
    }

    #[tokio::test]
    async fn a_garbage_apikey_is_invalid() {
        let req = Request::builder()
            .uri("/rest/v1/todos")
            .header("apikey", "not-a-jwt")
            .body(Body::empty())
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(body_json(res).await["message"], "Invalid API key");
    }

    #[tokio::test]
    async fn the_apikey_works_from_the_query_string_too() {
        let res = app()
            .oneshot(get_req(&format!("/auth/v1/health?apikey={}", anon_key())))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_json(res).await["name"], "GoTrue");
    }

    #[tokio::test]
    async fn health_answers_behind_a_valid_key() {
        let req = Request::builder()
            .uri("/auth/v1/health")
            .header("apikey", anon_key())
            .body(Body::empty())
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn a_rest_read_without_a_database_is_the_503_shape() {
        let req = Request::builder()
            .uri("/rest/v1/todos?select=*")
            .header("apikey", anon_key())
            .body(Body::empty())
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = body_json(res).await;
        assert_eq!(body["code"], "PGRST000");
        assert_eq!(
            body["message"],
            "Database connection error. Retrying the connection."
        );
    }

    #[tokio::test]
    async fn a_write_without_a_body_is_pgrst102() {
        let req = Request::builder()
            .method("POST")
            .uri("/rest/v1/todos")
            .header("apikey", anon_key())
            .body(Body::empty())
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(res).await["code"], "PGRST102");
    }

    #[tokio::test]
    async fn a_write_without_a_database_is_the_503_shape() {
        let req = Request::builder()
            .method("POST")
            .uri("/rest/v1/todos")
            .header("apikey", anon_key())
            .body(Body::from(r#"{"id":1}"#))
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body_json(res).await["code"], "PGRST000");
    }

    #[tokio::test]
    async fn an_unknown_rest_method_is_still_the_honest_501() {
        let req = Request::builder()
            .method("TRACE")
            .uri("/rest/v1/todos")
            .header("apikey", anon_key())
            .body(Body::empty())
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn rpc_without_a_database_is_the_503_shape() {
        let req = Request::builder()
            .uri("/rest/v1/rpc/do_thing")
            .header("apikey", anon_key())
            .body(Body::empty())
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body_json(res).await["code"], "PGRST000");
    }

    #[tokio::test]
    async fn a_delete_on_rpc_is_pgrst101() {
        let req = Request::builder()
            .method("DELETE")
            .uri("/rest/v1/rpc/do_thing")
            .header("apikey", anon_key())
            .body(Body::empty())
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::METHOD_NOT_ALLOWED);
        let body = body_json(res).await;
        assert_eq!(body["code"], "PGRST101");
        assert_eq!(body["message"], "Cannot use the DELETE method on RPC");
    }

    #[tokio::test]
    async fn an_expired_bearer_is_rejected_even_with_a_good_key() {
        let expired = jwt::mint(
            &serde_json::json!({"role": "authenticated", "exp": 1}),
            SECRET,
        );
        let req = Request::builder()
            .uri("/rest/v1/todos")
            .header("apikey", anon_key())
            .header("authorization", format!("Bearer {expired}"))
            .body(Body::empty())
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(body_json(res).await["message"], "JWT expired");
    }

    #[tokio::test]
    async fn outside_the_prefixes_is_the_edge_404() {
        let res = app().oneshot(get_req("/nope")).await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body = body_json(res).await;
        assert_eq!(body["message"], "no Route matched with those values");
    }

    /// A router with the real gate in front of a handler that echoes
    /// the deposited role, so precedence is observed, not inferred.
    fn echo_app() -> Router {
        echo_app_with_jwks(None)
    }

    fn echo_app_with_jwks(jwks: Option<String>) -> Router {
        let app = app_state(Config {
            jwt_secret: SECRET.to_vec(),
            jwks,
            ..Config::default()
        })
        .unwrap();
        Router::new()
            .route(
                "/echo",
                get(|axum::Extension(ctx): axum::Extension<AuthContext>| async move { ctx.role }),
            )
            .layer(middleware::from_fn_with_state(Arc::clone(&app), gate))
            .with_state(app)
    }

    #[tokio::test]
    async fn without_a_bearer_the_key_role_is_the_identity() {
        let service_key = jwt::mint(&jwt::key_claims("service_role"), SECRET);
        let req = Request::builder()
            .uri("/echo")
            .header("apikey", service_key)
            .body(Body::empty())
            .unwrap();
        let res = echo_app().oneshot(req).await.unwrap();
        let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        assert_eq!(&bytes[..], b"service_role");
    }

    #[tokio::test]
    async fn a_preflight_needs_no_apikey() {
        let req = Request::builder()
            .method("OPTIONS")
            .uri("/rest/v1/todos")
            .header("origin", "http://localhost:3000")
            .header("access-control-request-method", "POST")
            .header("access-control-request-headers", "authorization, apikey")
            .body(Body::empty())
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        let h = res.headers();
        assert_eq!(
            h["access-control-allow-origin"], "http://localhost:3000",
            "the origin is mirrored, not *"
        );
        assert_eq!(h["access-control-allow-credentials"], "true");
        assert_eq!(
            h["access-control-allow-methods"],
            "GET, POST, PATCH, PUT, DELETE, OPTIONS, HEAD"
        );
        assert_eq!(h["access-control-allow-headers"], "authorization, apikey");
        assert_eq!(h["access-control-max-age"], "86400");
    }

    #[tokio::test]
    async fn responses_with_an_origin_get_the_mirror_and_exposed_headers() {
        let req = Request::builder()
            .uri("/auth/v1/health")
            .header("apikey", anon_key())
            .header("origin", "https://app.example.com")
            .body(Body::empty())
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let h = res.headers();
        assert_eq!(h["access-control-allow-origin"], "https://app.example.com");
        assert_eq!(h["access-control-allow-credentials"], "true");
        assert!(
            h["access-control-expose-headers"]
                .to_str()
                .unwrap()
                .contains("Content-Range"),
            "supabase-js reads Content-Range for counts"
        );
    }

    #[tokio::test]
    async fn no_origin_means_no_cors_headers() {
        let req = Request::builder()
            .uri("/auth/v1/health")
            .header("apikey", anon_key())
            .body(Body::empty())
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert!(!res.headers().contains_key("access-control-allow-origin"));
    }

    #[tokio::test]
    async fn every_response_carries_a_request_id_even_a_404() {
        let res = app().oneshot(get_req("/nope")).await.unwrap();
        let id = res.headers()["x-request-id"].to_str().unwrap().to_string();
        assert_eq!(id.len(), 36, "a minted id is a uuid: {id}");
    }

    #[tokio::test]
    async fn a_client_supplied_request_id_is_echoed() {
        let req = Request::builder()
            .uri("/auth/v1/health")
            .header("apikey", anon_key())
            .header("x-request-id", "trace-me-7")
            .body(Body::empty())
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert_eq!(res.headers()["x-request-id"], "trace-me-7");
    }

    #[tokio::test]
    async fn past_the_budget_is_the_edge_429() {
        let app = router(Config {
            jwt_secret: SECRET.to_vec(),
            rate: Some(edge::Rate {
                burst: 2,
                per_second: 0.5,
            }),
            ..Config::default()
        })
        .unwrap();
        let key = anon_key();
        for _ in 0..2 {
            let req = Request::builder()
                .uri("/auth/v1/health")
                .header("apikey", &key)
                .body(Body::empty())
                .unwrap();
            let res = app.clone().oneshot(req).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
        }
        let req = Request::builder()
            .uri("/auth/v1/health")
            .header("apikey", &key)
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(res.headers()[header::RETRY_AFTER], "2");
        assert!(
            res.headers().contains_key("x-request-id"),
            "even a 429 is traceable"
        );
        let body = body_json(res).await;
        assert_eq!(body["message"], "API rate limit exceeded");
    }

    #[tokio::test]
    async fn the_gate_prefers_the_bearer_identity() {
        let bearer = jwt::mint(
            &serde_json::json!({"role": "authenticated", "sub": "user-1"}),
            SECRET,
        );
        let req = Request::builder()
            .uri("/echo")
            .header("apikey", anon_key())
            .header("authorization", format!("Bearer {bearer}"))
            .body(Body::empty())
            .unwrap();
        let res = echo_app().oneshot(req).await.unwrap();
        let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        assert_eq!(&bytes[..], b"authenticated");
    }

    #[tokio::test]
    async fn an_es256_bearer_passes_the_gate_through_the_jwks() {
        use base64ct::{Base64UrlUnpadded, Encoding};
        use p256::ecdsa::signature::Signer as _;
        use p256::ecdsa::{Signature, SigningKey};

        let sk = SigningKey::from_slice(&[9u8; 32]).unwrap();
        let point = sk.verifying_key().to_sec1_point(false);
        let jwks = serde_json::json!({
            "keys": [{
                "kty": "EC",
                "crv": "P-256",
                "kid": "k1",
                "x": Base64UrlUnpadded::encode_string(point.x().unwrap()),
                "y": Base64UrlUnpadded::encode_string(point.y().unwrap()),
            }]
        });

        let header = Base64UrlUnpadded::encode_string(
            serde_json::json!({"alg": "ES256", "typ": "JWT", "kid": "k1"})
                .to_string()
                .as_bytes(),
        );
        let payload = Base64UrlUnpadded::encode_string(
            serde_json::json!({"role": "authenticated", "sub": "user-2"})
                .to_string()
                .as_bytes(),
        );
        let signed = format!("{header}.{payload}");
        let sig: Signature = sk.sign(signed.as_bytes());
        let bearer = format!(
            "{signed}.{}",
            Base64UrlUnpadded::encode_string(&sig.to_bytes())
        );

        // The apikey stays the HS256 anon key, only the bearer rides
        // the new signing keys, which is what a migrated project does.
        let req = Request::builder()
            .uri("/echo")
            .header("apikey", anon_key())
            .header("authorization", format!("Bearer {bearer}"))
            .body(Body::empty())
            .unwrap();
        let res = echo_app_with_jwks(Some(jwks.to_string()))
            .oneshot(req)
            .await
            .unwrap();
        let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        assert_eq!(&bytes[..], b"authenticated");

        // Without the JWKS configured the same token is refused.
        let req = Request::builder()
            .uri("/echo")
            .header("apikey", anon_key())
            .header("authorization", format!("Bearer {bearer}"))
            .body(Body::empty())
            .unwrap();
        let res = echo_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_configured_transport_takes_the_dev_inbox_away_with_it() {
        // Nothing is kept in the process once something is carrying
        // the messages, so the mailbox is not there to be read. This is
        // the deployment case: the route exists in the router either
        // way, and the only thing standing between a stranger and a
        // recovery code is that there is nothing to hand out.
        let service = jwt::mint(&jwt::key_claims("service_role"), SECRET);
        let ask = |app: Router, key: String| async move {
            let req = Request::builder()
                .uri("/dev/inbox")
                .header("apikey", key)
                .body(Body::empty())
                .unwrap();
            app.oneshot(req).await.unwrap().status()
        };

        let local = router(Config {
            jwt_secret: SECRET.to_vec(),
            ..Config::default()
        })
        .unwrap();
        assert_eq!(ask(local, service.clone()).await, StatusCode::OK);

        // A mail server and no SMS provider still keeps the texted
        // codes in the process, and those are still worth reading.
        let mailing = router(Config {
            jwt_secret: SECRET.to_vec(),
            sender: Some(Arc::new(smtp::Smtp::new("mail.zou.test", 587))),
            ..Config::default()
        })
        .unwrap();
        assert_eq!(ask(mailing, service.clone()).await, StatusCode::OK);

        let sending = router(Config {
            jwt_secret: SECRET.to_vec(),
            sender: Some(Arc::new(smtp::Smtp::new("mail.zou.test", 587))),
            texter: Some(Arc::new(sms::Twilio::new("AC1", "secret", "MG9"))),
            ..Config::default()
        })
        .unwrap();
        assert_eq!(ask(sending, service).await, StatusCode::NOT_FOUND);
    }
}
