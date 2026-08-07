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
//! connect. The first successful connection applies the tenant
//! contract in the BOOTSTRAP batch: the anon, authenticated, and
//! service_role roles, the auth schema with Supabase's uid, role,
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
    permits: Arc<Semaphore>,
    idle: Mutex<Vec<Client>>,
    bootstrapped: tokio::sync::OnceCell<()>,
}

/// The pool itself, cheap to clone and share.
#[derive(Clone)]
pub struct Pool(Arc<Inner>);

/// The tenant contract, applied once per pool on the first connection.
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
    end if;
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

impl Pool {
    /// Parse the dsn now, dial nothing. `max` caps the number of live
    /// connections, checkouts past it wait their turn.
    pub fn new(dsn: &str, max: usize) -> Result<Pool, Error> {
        let pg: tokio_postgres::Config = dsn.parse()?;
        Ok(Pool(Arc::new(Inner {
            pg,
            permits: Arc::new(Semaphore::new(max)),
            idle: Mutex::new(Vec::new()),
            bootstrapped: tokio::sync::OnceCell::new(),
        })))
    }

    async fn checkout(&self) -> Result<(OwnedSemaphorePermit, Client), Error> {
        let permit = Arc::clone(&self.0.permits)
            .acquire_owned()
            .await
            .expect("pool semaphore is never closed");
        loop {
            let reused = self.0.idle.lock().await.pop();
            match reused {
                Some(client) if !client.is_closed() => return Ok((permit, client)),
                Some(_) => continue,
                None => break,
            }
        }
        let (client, connection) = self.0.pg.connect(NoTls).await?;
        tokio::spawn(async move {
            // The pool notices a dead connection through is_closed at
            // the next checkout, an error here has nowhere better to go.
            let _ = connection.await;
        });
        self.0
            .bootstrapped
            .get_or_try_init(|| async {
                client.batch_execute(BOOTSTRAP).await?;
                ensure_foreign_schema(&client, "auth.users", &[AUTH_SCHEMA]).await?;
                ensure_foreign_schema(
                    &client,
                    "storage.objects",
                    &[STORAGE_SCHEMA, STORAGE_GRANTS],
                )
                .await
            })
            .await?;
        Ok((permit, client))
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

    /// A transaction with the request context injected, the unit every
    /// REST and auth request runs in. Commit or roll back explicitly,
    /// a dropped session forfeits its connection.
    pub async fn session(&self, ctx: &RequestContext, read_only: bool) -> Result<Session, Error> {
        let (permit, client) = self.checkout().await?;
        let begin = if read_only {
            "begin read only"
        } else {
            "begin"
        };
        let injected = async {
            client.batch_execute(begin).await?;
            client
                .query_one(
                    "select set_config('role', $1, true),
                            set_config('request.jwt.claims', $2, true),
                            set_config('request.method', $3, true),
                            set_config('request.path', $4, true),
                            set_config('request.headers', $5, true),
                            set_config('request.cookies', $6, true),
                            set_config('search_path', $7, true)",
                    &[
                        &ctx.role,
                        &ctx.claims,
                        &ctx.method,
                        &ctx.path,
                        &ctx.headers,
                        &ctx.cookies,
                        &ctx.search_path,
                    ],
                )
                .await?;
            Ok::<(), Error>(())
        };
        match injected.await {
            // The client is dropped on failure, never pooled half set up.
            Err(e) => Err(e),
            Ok(()) => Ok(Session {
                pool: self.clone(),
                client: Some(client),
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
        let (permit, client) = self.checkout().await?;
        match client.batch_execute("begin").await {
            // The client is dropped on failure, never pooled mid begin.
            Err(e) => Err(e),
            Ok(()) => Ok(Session {
                pool: self.clone(),
                client: Some(client),
                _permit: permit,
                in_txn: true,
            }),
        }
    }

    /// A connection with no transaction and no injected identity, for
    /// bootstrap work and tests. finish() returns it to the pool.
    pub async fn unscoped(&self) -> Result<Session, Error> {
        let (permit, client) = self.checkout().await?;
        Ok(Session {
            pool: self.clone(),
            client: Some(client),
            _permit: permit,
            in_txn: false,
        })
    }

    async fn park(&self, client: Client) {
        // Scrub before reuse: reset role covers a session level role
        // switch, reset all covers everything else. If the scrub
        // fails the connection is dropped instead of pooled.
        if client.batch_execute("reset role; reset all").await.is_ok() {
            self.0.idle.lock().await.push(client);
        }
    }
}

/// A checked out connection. While `in_txn`, queries run inside the
/// transaction that carries the request context.
pub struct Session {
    pool: Pool,
    client: Option<Client>,
    _permit: OwnedSemaphorePermit,
    in_txn: bool,
}

impl Session {
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

    async fn end(mut self, stmt: &str) -> Result<(), Error> {
        let client = self.client.take().expect("session ended twice");
        if self.in_txn {
            client.batch_execute(stmt).await?;
        }
        self.pool.park(client).await;
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
            self.pool.park(client).await;
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
        self.pool.park(client).await;
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
