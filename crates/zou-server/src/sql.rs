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
//! stance a Supabase project ships with.

use std::sync::Arc;

use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_postgres::{Client, NoTls};

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
";

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
            .get_or_try_init(|| client.batch_execute(BOOTSTRAP))
            .await?;
        Ok((permit, client))
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
