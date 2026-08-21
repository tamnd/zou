//! The in process SQL session pool behind the REST and auth surfaces.
//!
//! Every request that reaches SQL runs inside one transaction on a
//! pooled connection, with the request identity injected the way
//! PostgREST does it behind Supabase: set_config with is_local, so
//! role, request.jwt.claims, request.method, request.path,
//! request.headers, and request.cookies all exist exactly for the
//! span of that transaction and RLS policies can read them.
//!
//! Leaks are the whole game for a pool like this, and two rules keep
//! it honest. A session that was not explicitly committed or rolled
//! back never returns its connection, the connection is dropped and
//! the next checkout dials a fresh one, so a handler that bailed out
//! mid transaction cannot poison a neighbor. And a session that did
//! finish cleanly still runs reset role and reset all on the way
//! back, so even a query that smuggled in a session level set_config
//! hands the next request a connection with defaults.
//!
//! Connections are lazy. Building a pool never dials, so the server
//! can come up before postgres does and the first request pays the
//! connect. That first request also pays for a second connection,
//! dialled as the dsn's own role and thrown away again, which applies
//! the tenant contract in the BOOTSTRAP batch: the anon, authenticated, and
//! service_role roles with an authenticator granted the three of
//! them, the auth schema with Supabase's uid, role,
//! email, and jwt functions verbatim, and the open public schema
//! grants that make row level security the actual guard, exactly the
//! stance a Supabase project ships with. Then the two schemas whose
//! definitions belong to somebody else, auth and storage, each applied
//! only to a database that does not have one already.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_postgres::{AsyncMessage, Client, NoTls};

/// One error type end to end: dsn parse, connect, and query failures
/// are all tokio_postgres errors already.
pub type Error = tokio_postgres::Error;

/// What a request injects into its transaction. Claims, headers, and
/// cookies are JSON text because that is what current_setting hands
/// to SQL and what auth.jwt() style helpers cast from.
pub struct RequestContext {
    pub role: String,
    pub claims: String,
    pub method: String,
    pub path: String,
    pub headers: String,
    pub cookies: String,
    /// The transaction's search_path, already quoted as an ident
    /// list, which is how PostgREST scopes a request to its
    /// negotiated schema.
    pub search_path: String,
}

impl RequestContext {
    /// A context with just an identity, the shape tests and internal
    /// callers want. The JSON fields default to empty objects so
    /// current_setting never hands a cast a malformed value.
    pub fn bare(role: &str, claims: &str) -> Self {
        RequestContext {
            role: role.to_string(),
            claims: claims.to_string(),
            method: String::new(),
            path: String::new(),
            headers: "{}".to_string(),
            cookies: "{}".to_string(),
            search_path: "\"public\"".to_string(),
        }
    }
}

struct Inner {
    pg: tokio_postgres::Config,
    /// Who a request session logs in as, which is the same as `pg`
    /// unless a deployment named a second dsn for it.
    request: tokio_postgres::Config,
    /// Whether those two are different, which is the only thing that
    /// decides whether idle connections are one list or two.
    split: bool,
    permits: Arc<Semaphore>,
    idle: Mutex<Vec<Client>>,
    idle_request: Mutex<Vec<Client>>,
    bootstrapped: tokio::sync::OnceCell<()>,
    settings: tokio::sync::RwLock<Option<(std::time::Instant, RoleSettings)>>,
    audit_rows: std::sync::atomic::AtomicBool,
}

/// Which of the pool's two logins a connection is wanted on.
///
/// Upstream this is two servers rather than two idle lists: PostgREST
/// connects as `authenticator` and GoTrue as `supabase_auth_admin`,
/// and neither can do the other's work. In one process it is one pool
/// with two identities, and the split is what keeps a request from
/// being able to do the work only the owner should.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Identity {
    /// The dsn zou was given. It owns the schemas, writes the
    /// bootstrap ddl, and reads auth and storage past their policies,
    /// which is why nothing a request can steer runs on it.
    Owner,
    /// What a request logs in as before its `set role`. Upstream calls
    /// it `authenticator`: granted the api roles and nothing else, so
    /// a `role` claim naming anything further is refused by postgres
    /// whatever the server in front of it believed.
    Request,
}

/// What `alter role x set y to z` wrote, per role, already filtered
/// down to what is worth applying.
type RoleSettings = Arc<std::collections::HashMap<String, Vec<(String, String)>>>;

/// The names zou sets itself, which are never taken from a role.
///
/// `search_path` is the interesting one: a request's schema is
/// negotiated per request from the Accept-Profile header, so a role
/// level search_path would quietly win over the profile the caller
/// asked for. Dropping both here makes the two sets disjoint, which
/// is also why the order they are applied in does not matter.
const OURS: [&str; 2] = ["role", "search_path"];

/// The pool itself, cheap to clone and share.
#[derive(Clone)]
pub struct Pool(Arc<Inner>);

/// The tenant contract, applied once per pool on a connection of its
/// own, dialled as the owner because this is the batch that creates
/// the role the request identity logs in as.
/// The whole batch runs as one implicit transaction under an advisory
/// lock, so concurrent bootstrappers serialize instead of racing the
/// if not exists checks, and it is idempotent throughout.
///
/// The auth.* function bodies are Supabase's own definitions verbatim,
/// including the legacy request.jwt.claim.<name> fallbacks, because
/// user policies copied from Supabase docs must behave identically.
/// The public schema grants mirror Supabase's stance: the three API
/// roles get everything and row level security is the actual guard,
/// which is also why enabling RLS is the app's job, not ours.
const BOOTSTRAP: &str = "
select pg_advisory_xact_lock(730501);

do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'anon') then
        create role anon nologin;
    end if;
    if not exists (select 1 from pg_roles where rolname = 'authenticated') then
        create role authenticated nologin;
    end if;
    if not exists (select 1 from pg_roles where rolname = 'service_role') then
        create role service_role nologin bypassrls;
    -- Roles are the cluster's, not the database's, so this one can
    -- already be there and be wrong: a dump restored with its roles
    -- created first, or a database somebody prepared by hand, both of
    -- which are ordinary ways to arrive here from hosted Supabase.
    -- Without bypassrls the key that is supposed to see everything
    -- quietly sees only what a policy lets it, which is a compatibility
    -- break that answers 200 with fewer rows and nothing in a log. So
    -- the attribute is repaired rather than assumed, and only when it
    -- is missing, which keeps the statement out of the way of a
    -- deployment that has no business issuing it.
    elsif not (select rolbypassrls from pg_roles where rolname = 'service_role') then
        alter role service_role bypassrls;
    end if;
    -- Not an api role, and nothing here connects as it. Upstream it is
    -- the role GoTrue logs in as, and a project's own migrations name
    -- it: the custom access token hook that supabase documents ends
    -- with a grant of the function and the tables it reads to
    -- supabase_auth_admin. The auth server is in process here, so the
    -- role owns nothing and is granted nothing, it exists so that a
    -- migration written for a Supabase database applies to this one.
    if not exists (select 1 from pg_roles where rolname = 'supabase_auth_admin') then
        create role supabase_auth_admin nologin;
    end if;
end
$$;

-- The role a Supabase api connects as: it owns nothing, inherits
-- nothing, and has been granted exactly the three above, so a session
-- opened as it can become those and nothing else however the role
-- claim of a token is written. That is the fence issue #92 is about,
-- and this is the database half of it. The other half is the exposed
-- set in Config, which refuses a claim before it reaches set_config,
-- and the two are worth having together: one of them is a check in a
-- server and the other is what the database will do about a server
-- that got its check wrong.
--
-- It can log in, which upstream's can too and which a role has to be
-- able to do to be connected as. That is not a way in that was not
-- there already: it carries no password, so a cluster asking for one
-- refuses it, and a cluster asking for nothing was already handing
-- out its superuser to whoever could reach the port. Repaired when it
-- is missing for the same reason the bypassrls above is, since a role
-- created by an older zou or restored from a dump is an ordinary way
-- to arrive here.
--
-- Granted one at a time and only when the grant is missing, because a
-- grant that is already there still writes a row and prints a notice
-- on every boot.
do $$
declare
    api text;
begin
    if not exists (select 1 from pg_roles where rolname = 'authenticator') then
        create role authenticator login noinherit;
    elsif not (select rolcanlogin from pg_roles where rolname = 'authenticator') then
        alter role authenticator login;
    end if;
    foreach api in array array['anon', 'authenticated', 'service_role'] loop
        if not exists (
            select 1 from pg_auth_members m
              join pg_roles granted on granted.oid = m.roleid
              join pg_roles member on member.oid = m.member
             where member.rolname = 'authenticator' and granted.rolname = api
        ) then
            execute format('grant %I to authenticator', api);
        end if;
    end loop;
exception when insufficient_privilege then
    raise warning 'zou: no authenticator role: %', sqlerrm;
end
$$;

-- The statement timeouts a Supabase project has, three seconds for
-- anon and eight for authenticated, so a query nobody is waiting for
-- any more stops rather than holding a connection. service_role has
-- none there and gets none here.
--
-- These rows do nothing on their own: postgres reads role settings at
-- connection time for the role that connected, and every role here is
-- reached with set role instead. Pool::settings is the half that makes
-- them real, and an alter role by hand works the same way.
-- Asked before it is set, because an alter role that changes nothing
-- still rewrites the row, and two of those at once is a tuple
-- concurrently updated. The advisory lock above serializes the
-- bootstraps, so what is left to avoid is anybody else's alter role,
-- and the cheapest way is to not write when there is nothing to write.
do $$
begin
    if not exists (
        select 1 from pg_db_role_setting s join pg_roles r on r.oid = s.setrole
        where r.rolname = 'anon' and s.setdatabase = 0
          and s.setconfig @> array['statement_timeout=3s']
    ) then
        alter role anon set statement_timeout to '3s';
    end if;
    if not exists (
        select 1 from pg_db_role_setting s join pg_roles r on r.oid = s.setrole
        where r.rolname = 'authenticated' and s.setdatabase = 0
          and s.setconfig @> array['statement_timeout=8s']
    ) then
        alter role authenticated set statement_timeout to '8s';
    end if;
exception when insufficient_privilege then
    raise warning 'zou: no statement timeouts: %', sqlerrm;
end
$$;

create schema if not exists auth;
grant usage on schema auth to anon, authenticated, service_role;

create or replace function auth.uid() returns uuid as $$
  select
  coalesce(
    nullif(current_setting('request.jwt.claim.sub', true), ''),
    (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'sub')
  )::uuid
$$ language sql stable;

create or replace function auth.role() returns text as $$
  select
  coalesce(
    nullif(current_setting('request.jwt.claim.role', true), ''),
    (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'role')
  )::text
$$ language sql stable;

create or replace function auth.email() returns text as $$
  select
  coalesce(
    nullif(current_setting('request.jwt.claim.email', true), ''),
    (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'email')
  )::text
$$ language sql stable;

create or replace function auth.jwt() returns jsonb as $$
  select
    coalesce(
        nullif(current_setting('request.jwt.claim', true), ''),
        nullif(current_setting('request.jwt.claims', true), '')
    )::jsonb
$$ language sql stable;

grant execute on all functions in schema auth to anon, authenticated, service_role;

do $$
begin
    -- Only the first bootstrap sweeps existing objects. The sweep
    -- rewrites the acl of every table in public, so re running it on
    -- every restart would both race concurrent DDL (tuple concurrently
    -- updated) and silently undo revokes the operator made on purpose.
    -- The marker is the default acl entry this block creates, not
    -- has_schema_privilege, which is always true via PUBLIC.
    if not exists (
        select 1
        from pg_default_acl d
        join pg_namespace n on n.oid = d.defaclnamespace
        cross join aclexplode(d.defaclacl) a
        where n.nspname = 'public'
          and d.defaclobjtype = 'r'
          and a.grantee = 'anon'::regrole
    ) then
        grant usage on schema public to anon, authenticated, service_role;
        grant all on all tables in schema public to anon, authenticated, service_role;
        grant all on all sequences in schema public to anon, authenticated, service_role;
        grant all on all functions in schema public to anon, authenticated, service_role;
        alter default privileges in schema public
            grant all on tables to anon, authenticated, service_role;
        alter default privileges in schema public
            grant all on sequences to anon, authenticated, service_role;
        alter default privileges in schema public
            grant all on functions to anon, authenticated, service_role;
    end if;
end
$$;

-- The half of a Supabase database that nobody's server made.
--
-- A project's own migrations call gen_salt, crypt, digest and
-- uuid_generate_v4 without qualifying them, because the database they
-- were written against has an extensions schema with pgcrypto and
-- uuid-ossp in it and extensions on the search path. Recorded off
-- supabase start rather than remembered, and the recording is kept by
-- the record workflow now, see tamnd/zou#214.
--
-- Best effort on purpose. A deployment pointed at a managed postgres
-- may have neither the privilege to create an extension nor the files
-- to create it from, and the rest of this batch is worth applying
-- either way. What is lost is a project's own sql, not zou's: nothing
-- here calls into either extension.
do $$
begin
    create schema if not exists extensions;
    grant usage on schema extensions to anon, authenticated, service_role;
    create extension if not exists pgcrypto with schema extensions;
    create extension if not exists \"uuid-ossp\" with schema extensions;
    execute format(
        'alter database %I set search_path to %s',
        current_database(),
        '\"$user\", public, extensions'
    );
exception when insufficient_privilege or undefined_file or duplicate_object then
    raise warning 'zou: no extensions schema: %', sqlerrm;
end
$$;

-- The DDL watch behind catalog invalidation. Event triggers are
-- superuser only, so a deployment that connects as a lesser role
-- keeps working and falls back to the timed refresh instead.
do $$
begin
    create schema if not exists zou;
    execute $fn$
        create or replace function zou.notify_catalog() returns event_trigger
        language plpgsql as $body$
        begin
            perform pg_notify('zou_catalog', '');
        end
        $body$
    $fn$;
    if not exists (select 1 from pg_event_trigger where evtname = 'zou_catalog_watch') then
        create event trigger zou_catalog_watch on ddl_command_end
            execute function zou.notify_catalog();
    end if;
    if not exists (select 1 from pg_event_trigger where evtname = 'zou_catalog_drop') then
        create event trigger zou_catalog_drop on sql_drop
            execute function zou.notify_catalog();
    end if;
exception when insufficient_privilege then
    null;
end
$$;
";

/// The canonical auth schema, the shape GoTrue's own migrations leave
/// behind. Generated rather than transcribed, see
/// scripts/auth-schema-refresh.sh for how and why.
const AUTH_SCHEMA: &str = include_str!("auth-schema.sql");

/// The canonical storage schema, the shape storage-api's own
/// migrations leave behind. Generated the same way and for the same
/// reasons, see scripts/storage-schema-refresh.sh.
const STORAGE_SCHEMA: &str = include_str!("storage-schema.sql");

/// The realtime schema, or the part of it a private channel is
/// checked against. Written here rather than generated, because
/// upstream's migrations are mostly postgres changes and this is the
/// handful of objects that are not, see the file for what is left out.
const REALTIME_SCHEMA: &str = include_str!("realtime-schema.sql");

/// Sending to a room from sql: upstream's three functions, and the
/// trigger that gets what they write out to the sockets.
///
/// Applied every boot rather than once on a fresh database, because a
/// database an older zou has already served has realtime.messages in
/// it and would never see this otherwise. See the file for the rest.
const REALTIME_SEND: &str = include_str!("realtime-send.sql");

/// The `net` schema, which on a Supabase project is pg_net: the queue
/// a database webhook writes into, and the functions that write it.
/// Only applied to a database that has neither, so a real pg_net keeps
/// its own, see the file.
const NET_SCHEMA: &str = include_str!("net-schema.sql");

/// The `supabase_functions` schema: the trigger function a database
/// webhook is, and the audit table it writes.
const FUNCTIONS_SCHEMA: &str = include_str!("functions-schema.sql");

/// The `cron` schema, which on a Supabase project is pg_cron: the job
/// table, the run log, and the functions that write them. Only applied
/// to a database that has none of it, so a real pg_cron keeps its own,
/// see the file.
const CRON_SCHEMA: &str = include_str!("cron-schema.sql");

/// The publication postgres changes reads, which a project adds its
/// tables to. Applied every boot for the same reason as the send
/// functions, and a notice rather than a failure when the role cannot
/// create one, see the file.
const REALTIME_PUBLICATION: &str = include_str!("realtime-publication.sql");

/// The grants the storage schema arrives without.
///
/// Upstream these are inside the migrations, granting to roles the
/// migrations also create, and the tables end up owned by
/// supabase_storage_admin with everyone else granted in. Under zou the
/// connecting role creates the tables and already owns them, so the
/// only part left to do is let the three api roles at them, which is
/// the same stance the bootstrap takes for public and for auth: the
/// roles get everything and row level security is the actual guard.
/// Row level security is on for every table here, so this grants
/// nothing away that a policy has not been asked about.
const STORAGE_GRANTS: &str = "
grant usage on schema storage to anon, authenticated, service_role;
grant all on all tables in schema storage to anon, authenticated, service_role;
grant all on all sequences in schema storage to anon, authenticated, service_role;
grant all on all functions in schema storage to anon, authenticated, service_role;
alter default privileges in schema storage
    grant all on tables to anon, authenticated, service_role;
alter default privileges in schema storage
    grant all on sequences to anon, authenticated, service_role;
alter default privileges in schema storage
    grant all on functions to anon, authenticated, service_role;
";

/// Create a schema somebody else owns the definition of when the
/// database has none, and leave it completely alone when it has one.
///
/// `marker` is a table rather than the schema, because zou's own
/// bootstrap creates the auth schema for the auth.uid() helpers before
/// this ever runs, and because a schema with nothing in it is not
/// evidence of anything. An existing marker means somebody else's
/// schema, most likely a real GoTrue's or a real storage-api's,
/// possibly several versions behind this one. Half applying today's
/// ddl over that would leave a shape that is neither version, so the
/// only safe move is to do nothing. Migrating an older schema forward
/// is that server's own job.
///
/// Its own transaction, so that two servers starting against a fresh
/// database do not both decide it is empty and both start creating.
///
/// The same advisory lock the bootstrap takes, not one of its own. The
/// two overlap: the four auth helper functions are defined by both, and
/// under two locks one connection can be replacing auth.uid inside the
/// bootstrap while another replaces it here, which postgres reports as
/// a deadlock on pg_proc or as tuple concurrently updated. One lock over
/// everything that writes these schemas is the whole fix.
/// Everything a database needs before this server can answer from it:
/// the three api roles, the auth helper functions and the public
/// grants, then the auth and storage schemas if nobody has put them
/// there yet. The pool does this once per process on the connection it
/// opens first.
///
/// Public because it is also what a fresh database looks like, and
/// `zou db diff` builds one to compare against. Idempotent, so calling
/// it against a database that has been served from before is a few
/// cheap catalog reads and nothing else.
pub async fn bootstrap(client: &Client) -> Result<(), tokio_postgres::Error> {
    client.batch_execute(BOOTSTRAP).await?;
    ensure_foreign_schema(client, "auth.users", &[AUTH_SCHEMA]).await?;
    ensure_foreign_schema(client, "storage.objects", &[STORAGE_SCHEMA, STORAGE_GRANTS]).await?;
    ensure_foreign_schema(client, "realtime.messages", &[REALTIME_SCHEMA]).await?;
    ensure_foreign_schema(client, "net.http_request_queue", &[NET_SCHEMA]).await?;
    ensure_foreign_schema(client, "supabase_functions.hooks", &[FUNCTIONS_SCHEMA]).await?;
    ensure_foreign_schema(client, "cron.job", &[CRON_SCHEMA]).await?;
    ensure_schema(client, &[REALTIME_SEND, REALTIME_PUBLICATION]).await
}

/// Apply ddl that has to run on every boot rather than only on a
/// fresh database, under the same lock as everything else here.
///
/// `create or replace function` twice at once on one function is
/// `tuple concurrently updated`, and two servers starting together
/// against one database is the ordinary case rather than a strange
/// one, so the lock is the whole reason this is not a bare
/// `batch_execute`.
async fn ensure_schema(client: &Client, ddl: &[&str]) -> Result<(), tokio_postgres::Error> {
    client
        .batch_execute("begin; select pg_advisory_xact_lock(730501)")
        .await?;
    let mut applied = Ok(());
    for batch in ddl {
        applied = client.batch_execute(batch).await;
        if applied.is_err() {
            break;
        }
    }
    match applied {
        Ok(()) => client.batch_execute("commit").await,
        Err(e) => {
            let _ = client.batch_execute("rollback").await;
            Err(e)
        }
    }
}

/// The ddl [`bootstrap`] would apply, as one number.
///
/// A database that has been bootstrapped once does not need it again,
/// and something that caches such a database has to know when the ddl
/// under it has moved on. The version string will not do: the auth
/// schema changes between releases more often than the version does.
/// This is what changed, said cheaply enough to ask on every open.
pub fn contract_version() -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for part in [
        BOOTSTRAP,
        AUTH_SCHEMA,
        STORAGE_SCHEMA,
        STORAGE_GRANTS,
        REALTIME_SCHEMA,
        REALTIME_SEND,
        REALTIME_PUBLICATION,
        NET_SCHEMA,
        FUNCTIONS_SCHEMA,
        CRON_SCHEMA,
    ] {
        for byte in part.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x1000_0000_01b3);
        }
    }
    hash
}

async fn ensure_foreign_schema(
    client: &Client,
    marker: &str,
    ddl: &[&str],
) -> Result<(), tokio_postgres::Error> {
    client
        .batch_execute("begin; select pg_advisory_xact_lock(730501)")
        .await?;
    let fresh: bool = client
        .query_one("select to_regclass($1) is null", &[&marker])
        .await?
        .get(0);
    let mut applied = Ok(());
    if fresh {
        for batch in ddl {
            applied = client.batch_execute(batch).await;
            if applied.is_err() {
                break;
            }
        }
    }
    match applied {
        Ok(()) => client.batch_execute("commit").await,
        Err(e) => {
            // The rollback is best effort: the connection may be the
            // reason the batch failed in the first place.
            let _ = client.batch_execute("rollback").await;
            Err(e)
        }
    }
}

/// How long a watch waits before dialing again after its connection
/// died.
const RETRY: std::time::Duration = std::time::Duration::from_secs(1);

/// How often a database with no event trigger installed is assumed to
/// have changed. Nothing will notify there, so this is the only thing
/// keeping the catalog from going stale forever.
const REFRESH: std::time::Duration = std::time::Duration::from_secs(10);

/// How long a read of the role settings is trusted for. Nothing
/// notifies on an `alter role`, so this is the whole invalidation
/// story, and it is the delay between running one and seeing it.
const SETTLED: std::time::Duration = std::time::Duration::from_secs(10);

impl Pool {
    /// Parse the dsn now, dial nothing. `max` caps the number of live
    /// connections, checkouts past it wait their turn.
    ///
    /// One login for everything, which is what a deployment that named
    /// one dsn asked for and what every embedded caller wants.
    pub fn new(dsn: &str, max: usize) -> Result<Pool, Error> {
        let pg: tokio_postgres::Config = dsn.parse()?;
        Ok(Pool::build(pg.clone(), pg, false, max))
    }

    /// The same pool with request sessions logging in as somebody
    /// else, which is the fence hosted Supabase has and #552 is about.
    ///
    /// `dsn` still owns the database and still writes the bootstrap,
    /// the auth tables and the storage rows. `request` is only ever
    /// the connection a REST or storage request runs its transaction
    /// on, and the point of it is what that role has not been granted:
    /// a `set role` to anything outside the api roles fails in the
    /// database, so a hole in the server's own check is still not a
    /// superuser session.
    ///
    /// Two dsns that spell the same thing are one login again, since
    /// then there is no boundary to keep and two idle lists would only
    /// halve how often a connection is reused.
    pub fn with_request(dsn: &str, request: &str, max: usize) -> Result<Pool, Error> {
        let pg: tokio_postgres::Config = dsn.parse()?;
        let theirs: tokio_postgres::Config = request.parse()?;
        Ok(Pool::build(pg, theirs, dsn != request, max))
    }

    fn build(
        pg: tokio_postgres::Config,
        request: tokio_postgres::Config,
        split: bool,
        max: usize,
    ) -> Pool {
        Pool(Arc::new(Inner {
            pg,
            request,
            split,
            permits: Arc::new(Semaphore::new(max)),
            idle: Mutex::new(Vec::new()),
            idle_request: Mutex::new(Vec::new()),
            bootstrapped: tokio::sync::OnceCell::new(),
            settings: tokio::sync::RwLock::new(None),
            audit_rows: std::sync::atomic::AtomicBool::new(true),
        }))
    }

    /// Whether the audit trail is written to this database as well as to
    /// the log stream, which is the project's `disable_postgres` setting
    /// inverted.
    ///
    /// It lives on the pool rather than being handed to every call
    /// because the trail is written from forty places, none of which
    /// hold anything but the session they are already writing on, and
    /// threading one bool down forty call chains would put it in front
    /// of everybody reading any of them forever. The pool is the
    /// project's, the same as the setting, and it is the one thing every
    /// audit write already has.
    pub fn write_audit_rows(&self, on: bool) {
        self.0
            .audit_rows
            .store(on, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn audit_rows(&self) -> bool {
        self.0.audit_rows.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The idle list that identity's connections come back to. One
    /// list when both identities are the same login, because then a
    /// connection an admin call parked is a connection a request may
    /// have, which is what the pool did before it had two.
    fn idle(&self, who: Identity) -> &Mutex<Vec<Client>> {
        match self.0.split && who == Identity::Request {
            true => &self.0.idle_request,
            false => &self.0.idle,
        }
    }

    fn config(&self, who: Identity) -> &tokio_postgres::Config {
        match who {
            Identity::Owner => &self.0.pg,
            Identity::Request => &self.0.request,
        }
    }

    async fn checkout(&self, who: Identity) -> Result<(OwnedSemaphorePermit, Client), Error> {
        let permit = Arc::clone(&self.0.permits)
            .acquire_owned()
            .await
            .expect("pool semaphore is never closed");
        loop {
            let reused = self.idle(who).lock().await.pop();
            match reused {
                Some(client) if !client.is_closed() => return Ok((permit, client)),
                Some(_) => continue,
                None => break,
            }
        }
        // Before the dial rather than on it, because the request
        // identity logs in as a role the bootstrap is what creates. On
        // a database nobody has served from yet, a first request that
        // dialled as it would be told there is no such role.
        self.bootstrapped().await?;
        let (client, connection) = self.config(who).connect(NoTls).await?;
        tokio::spawn(async move {
            // The pool notices a dead connection through is_closed at
            // the next checkout, an error here has nowhere better to go.
            let _ = connection.await;
        });
        Ok((permit, client))
    }

    /// The tenant contract, applied once per process, on a connection
    /// of its own on the owner identity.
    ///
    /// Its own connection because the first checkout of a process is
    /// usually a request, and a request is exactly what must not be
    /// the thing creating roles and schemas. It costs one dial, once,
    /// and that connection is closed as soon as the batch is done.
    async fn bootstrapped(&self) -> Result<(), Error> {
        self.0
            .bootstrapped
            .get_or_try_init(|| async {
                let (client, connection) = self.0.pg.connect(NoTls).await?;
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                bootstrap(&client).await
            })
            .await?;
        Ok(())
    }

    /// The DDL watch that drives catalog invalidation. A connection of
    /// its own, outside the pool's permits so a busy server cannot
    /// starve it, listening for the event trigger's notification and
    /// bumping `epoch` on every one. A bump is a hint that the schema
    /// may have changed, never a promise that it did, so the cost of a
    /// spurious one is a single reintrospection.
    ///
    /// Two things get the same treatment as a real notification. A lost
    /// connection, because whatever happened while it was down was
    /// missed, and a database with no event trigger installed, because
    /// then nothing will ever notify and the cache would go stale
    /// forever; that deployment gets the timed refresh instead, which is the
    /// honest fallback rather than a silent one.
    pub fn watch(&self, epoch: Arc<AtomicU64>) {
        let pool = self.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = pool.watch_once(&epoch).await {
                    log::warn!("catalog watch: {e}");
                }
                // Whatever was missed while the connection was down is
                // missed, so assume the worst and reintrospect.
                epoch.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(RETRY).await;
            }
        });
    }

    /// One connection's worth of watching, until it dies.
    async fn watch_once(&self, epoch: &AtomicU64) -> Result<(), Error> {
        let (client, mut conn) = self.0.pg.connect(NoTls).await?;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        // Polling for messages is also what drives the queries below,
        // so the pump owns the connection and the client speaks
        // through it.
        tokio::spawn(async move {
            while let Some(Ok(msg)) =
                std::future::poll_fn(|cx| Pin::new(&mut conn).poll_message(cx)).await
            {
                if matches!(msg, AsyncMessage::Notification(_)) && tx.send(()).is_err() {
                    return;
                }
            }
        });
        client.batch_execute("listen zou_catalog").await?;
        // Nothing cached before this point was under watch, so it
        // cannot be trusted: one bump on arrival closes the startup
        // race for the price of one reintrospection.
        epoch.fetch_add(1, Ordering::Relaxed);
        let watched: bool = client
            .query_one(
                "select exists (select 1 from pg_event_trigger \
                 where evtname = 'zou_catalog_watch')",
                &[],
            )
            .await?
            .get(0);
        loop {
            let alive = if watched {
                rx.recv().await.is_some()
            } else {
                tokio::select! {
                    m = rx.recv() => m.is_some(),
                    _ = tokio::time::sleep(REFRESH) => true,
                }
            };
            if !alive {
                return Ok(());
            }
            epoch.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A connection of its own that is listening to `channel`, which
    /// is a literal from this crate rather than anything a request can
    /// name, and what arrives on it.
    ///
    /// Outside the pool's permits, like the catalog watch, because a
    /// connection held for the life of a task is not a connection a
    /// request should ever be waiting behind. The client comes back
    /// with the receiver rather than instead of it: the caller reads
    /// rows on the same connection it is hearing about them through,
    /// which is one dialled connection and not two.
    ///
    /// The receiver closes when the connection dies, which is the
    /// caller's cue to dial again. Anything that happened while it was
    /// down was missed, and a broadcast is a live thing rather than a
    /// log, so there is nothing to catch up on.
    pub async fn listening(
        &self,
        channel: &str,
    ) -> Result<(Client, tokio::sync::mpsc::UnboundedReceiver<String>), Error> {
        let (client, mut conn) = self.0.pg.connect(NoTls).await?;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        // Polling for messages is also what drives the queries the
        // caller runs, so the pump owns the connection and the client
        // speaks through it.
        tokio::spawn(async move {
            while let Some(Ok(msg)) =
                std::future::poll_fn(|cx| Pin::new(&mut conn).poll_message(cx)).await
            {
                if let AsyncMessage::Notification(note) = msg
                    && tx.send(note.payload().to_string()).is_err()
                {
                    return;
                }
            }
        });
        // listen takes a name rather than a parameter, and every name
        // this is called with is a literal in this crate.
        client.batch_execute(&format!("listen {channel}")).await?;
        Ok((client, rx))
    }

    /// What every role in this database had `alter role` run against
    /// it, cached for `SETTLED` because nothing announces a change to
    /// it.
    ///
    /// Postgres reads these rows once, at connection time, for the role
    /// that connected. A role arrived at through `set role` never gets
    /// them, and every role zou serves is arrived at that way, so
    /// `alter role anon set statement_timeout to '3s'` would sit in the
    /// catalog meaning nothing. PostgREST 10 and later reads them
    /// itself and applies them per transaction, which is the only
    /// reason those timeouts are real on Supabase, and this is that.
    ///
    /// Rows for the database and rows for every database can both exist
    /// for one role; the query hands back the general one first and the
    /// specific one last, and the last one of a name wins, which is the
    /// order postgres itself resolves them in.
    ///
    /// An `alter role` fires no event trigger, so the catalog watch
    /// cannot see one and there is nothing to invalidate against. A
    /// short time is the honest answer rather than a cache that would
    /// be right forever or wrong forever.
    async fn settings(&self, client: &Client) -> Result<RoleSettings, Error> {
        if let Some((read, known)) = self.0.settings.read().await.as_ref()
            && read.elapsed() < SETTLED
        {
            return Ok(Arc::clone(known));
        }
        let rows = client
            .query(
                "select r.rolname, s.setconfig \
                 from pg_db_role_setting s \
                 join pg_roles r on r.oid = s.setrole \
                 where s.setconfig is not null \
                   and s.setdatabase in \
                       (0, (select oid from pg_database where datname = current_database())) \
                 order by (s.setdatabase <> 0)",
                &[],
            )
            .await?;
        let mut by_role: std::collections::HashMap<String, Vec<(String, String)>> =
            std::collections::HashMap::new();
        for row in &rows {
            let held = by_role.entry(row.get::<_, String>(0)).or_default();
            for one in row.get::<_, Vec<String>>(1) {
                let Some((name, value)) = one.split_once('=') else {
                    continue;
                };
                if OURS.iter().any(|ours| ours.eq_ignore_ascii_case(name)) {
                    continue;
                }
                held.retain(|(had, _)| had != name);
                held.push((name.to_string(), value.to_string()));
            }
        }
        let fresh: RoleSettings = Arc::new(by_role);
        *self.0.settings.write().await = Some((std::time::Instant::now(), Arc::clone(&fresh)));
        Ok(fresh)
    }

    /// A transaction with the request context injected, the unit every
    /// REST and auth request runs in. Commit or roll back explicitly,
    /// a dropped session forfeits its connection.
    pub async fn session(&self, ctx: &RequestContext, read_only: bool) -> Result<Session, Error> {
        let (permit, client) = self.checkout(Identity::Request).await?;
        // Read before the begin, so a database that will not answer
        // this fails the request rather than poisoning a transaction.
        let known = self.settings(&client).await?;
        let theirs: &[(String, String)] = known.get(&ctx.role).map_or(&[], Vec::as_slice);
        let mut sql = String::from(
            "select set_config('role', $1, true),
                    set_config('request.jwt.claims', $2, true),
                    set_config('request.method', $3, true),
                    set_config('request.path', $4, true),
                    set_config('request.headers', $5, true),
                    set_config('request.cookies', $6, true),
                    set_config('search_path', $7, true)",
        );
        let mut args: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![
            &ctx.role,
            &ctx.claims,
            &ctx.method,
            &ctx.path,
            &ctx.headers,
            &ctx.cookies,
            &ctx.search_path,
        ];
        for (name, value) in theirs {
            // Both sides parameterised: a setting name is an
            // identifier out of a catalog somebody else can write to.
            sql.push_str(&format!(
                ", set_config(${}, ${}, true)",
                args.len() + 1,
                args.len() + 2
            ));
            args.push(name);
            args.push(value);
        }
        let begin = if read_only {
            "begin read only"
        } else {
            "begin"
        };
        let injected = async {
            client.batch_execute(begin).await?;
            client.query_one(&sql, &args).await?;
            Ok::<(), Error>(())
        };
        match injected.await {
            // The client is dropped on failure, never pooled half set up.
            Err(e) => Err(e),
            Ok(()) => Ok(Session {
                pool: self.clone(),
                client: Some(client),
                who: Identity::Request,
                _permit: permit,
                in_txn: true,
            }),
        }
    }

    /// A transaction with no injected identity, which is what the auth
    /// surface runs in. It writes auth.users, auth.sessions and
    /// auth.refresh_tokens as the connecting role, which owns them and
    /// therefore passes their rls, and it needs a transaction because
    /// rotating a refresh token is several statements that have to
    /// land together or not at all.
    pub async fn admin(&self) -> Result<Session, Error> {
        let (permit, client) = self.checkout(Identity::Owner).await?;
        match client.batch_execute("begin").await {
            // The client is dropped on failure, never pooled mid begin.
            Err(e) => Err(e),
            Ok(()) => Ok(Session {
                pool: self.clone(),
                client: Some(client),
                who: Identity::Owner,
                _permit: permit,
                in_txn: true,
            }),
        }
    }

    /// A connection with no transaction and no injected identity, for
    /// bootstrap work and tests. finish() returns it to the pool.
    pub async fn unscoped(&self) -> Result<Session, Error> {
        let (permit, client) = self.checkout(Identity::Owner).await?;
        Ok(Session {
            pool: self.clone(),
            client: Some(client),
            who: Identity::Owner,
            _permit: permit,
            in_txn: false,
        })
    }

    async fn park(&self, client: Client, who: Identity) {
        // Scrub before reuse: reset role covers a session level role
        // switch, reset all covers everything else. If the scrub
        // fails the connection is dropped instead of pooled.
        if client.batch_execute("reset role; reset all").await.is_ok() {
            self.idle(who).lock().await.push(client);
        }
    }
}

/// A checked out connection. While `in_txn`, queries run inside the
/// transaction that carries the request context.
pub struct Session {
    pool: Pool,
    client: Option<Client>,
    /// Which login this connection was dialled on, which is the list
    /// it goes back to.
    who: Identity,
    _permit: OwnedSemaphorePermit,
    in_txn: bool,
}

impl Session {
    /// The pool this connection came from, which is how a write reaches
    /// the project settings that are not carried on the request.
    pub(crate) fn pool(&self) -> &Pool {
        &self.pool
    }

    fn client(&self) -> &Client {
        self.client
            .as_ref()
            .expect("session used after commit or rollback")
    }

    pub async fn query(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Vec<tokio_postgres::Row>, Error> {
        self.client().query(sql, params).await
    }

    pub async fn execute(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<u64, Error> {
        self.client().execute(sql, params).await
    }

    /// Run whatever a project wrote, as one simple query, and say how
    /// many rows the last statement in it touched.
    ///
    /// This is for a scheduled job's command, which is text a person
    /// typed into `cron.schedule` and may be several statements. The
    /// simple protocol is the only one that takes several, and the
    /// count is all it hands back: postgres prints a command tag on
    /// the wire, and this driver keeps the number out of it and drops
    /// the word.
    pub async fn simple(&self, sql: &str) -> Result<u64, Error> {
        let messages = self.client().simple_query(sql).await?;
        let mut touched = 0;
        for message in messages {
            if let tokio_postgres::SimpleQueryMessage::CommandComplete(rows) = message {
                touched = rows;
            }
        }
        Ok(touched)
    }

    async fn end(mut self, stmt: &str) -> Result<(), Error> {
        let client = self.client.take().expect("session ended twice");
        if self.in_txn {
            client.batch_execute(stmt).await?;
        }
        self.pool.park(client, self.who).await;
        Ok(())
    }

    /// Commit and hand the connection back to the pool.
    pub async fn commit(self) -> Result<(), Error> {
        self.end("commit").await
    }

    /// Read one row out of the transaction and commit, in the round
    /// trip the commit was going to cost anyway. Both statements go
    /// out as one simple query batch, which is why the columns come
    /// back as text and why `sql` may not carry parameters.
    ///
    /// This is for the settings a request leaves behind: they live
    /// only as long as the transaction, so they have to be read
    /// before the commit, and a statement of their own would be a
    /// second round trip on every request that ever reads them.
    pub async fn commit_reading(mut self, sql: &str) -> Result<Vec<Option<String>>, Error> {
        let client = self.client.take().expect("session ended twice");
        if !self.in_txn {
            self.pool.park(client, self.who).await;
            return Ok(Vec::new());
        }
        let read = client.simple_query(&format!("{sql}; commit")).await;
        let messages = match read {
            // The connection is dropped rather than pooled, the same
            // containment a failed commit gets.
            Err(e) => return Err(e),
            Ok(m) => m,
        };
        let mut row = Vec::new();
        for message in messages {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = message {
                row = (0..r.len()).map(|i| r.get(i).map(str::to_string)).collect();
                break;
            }
        }
        self.pool.park(client, self.who).await;
        Ok(row)
    }

    /// Roll back and hand the connection back to the pool.
    pub async fn rollback(self) -> Result<(), Error> {
        self.end("rollback").await
    }

    /// The backend pid, which is how the leak tests observe whether a
    /// connection was reused or replaced.
    pub async fn backend_pid(&self) -> Result<i32, Error> {
        let row = self
            .client()
            .query_one("select pg_backend_pid()", &[])
            .await?;
        Ok(row.get(0))
    }
}

// No Drop impl on purpose: dropping a Session drops the Client, which
// closes the connection and ends its spawned task. An unfinished
// transaction dies with it, which is exactly the leak containment the
// module doc promises.
