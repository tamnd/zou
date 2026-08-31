//! The web admin at `/_zou`.
//!
//! One page and seven endpoints, served by the same binary that serves
//! the project. There is no build step, no bundler and nothing fetched
//! from a CDN: the page is a single file compiled into the binary, so
//! an operator who can reach the server can reach the console, on a box
//! with no internet and no node on it.
//!
//! What it is for is the thing a person does when a project misbehaves
//! at three in the morning: look at the tables, run a query, read the
//! answer. It is not Studio and is not trying to be. Studio is a
//! product with a build pipeline behind it and the parking lot in the
//! milestone spec says so out loud.
//!
//! Two rules shape the whole surface.
//!
//! The page carries no data. It is markup, style and script, the same
//! bytes for every project, and it is served to anyone who asks. Every
//! byte of a project that reaches it comes back through the api
//! endpoints below, and every one of them refuses anything that is not
//! a service role token. So the console is exactly as secret as the
//! service key is, which is the same fence the admin auth surface has
//! and the same one Studio has.
//!
//! The console holds no session. Each run of the editor is its own
//! checkout from the pool and postgres never sees two of them as
//! related, which is why a batch that opens a transaction and does not
//! close it is rolled back rather than committed on the way out. A
//! console that pooled an open transaction would hand the next request
//! somebody else's uncommitted work, and there is no way to keep the
//! transaction open for the person who typed it: nothing here is
//! attached to a connection between one press of the button and the
//! next.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::{App, json_body};

/// The page. Compiled in rather than read from disk, because the whole
/// point of a single binary is that there is nothing beside it.
const PAGE: &str = include_str!("console.html");

/// Every relation the console lists, with its columns, the primary key
/// membership of each and the planner's row estimate.
///
/// The estimate rather than a count, deliberately. A listing that
/// counted every row of every table would read the whole database to
/// draw a sidebar, and on a project of any size it would be the
/// slowest thing in the console. `reltuples` is what the planner
/// itself trusts and it is -1 on a relation that has never been
/// analysed, which the page prints as nothing rather than as a number.
const CATALOG: &str = "\
select n.nspname::text as schema,
       c.relname::text as name,
       c.relkind::text as kind,
       c.reltuples::float8 as rows,
       a.attname::text as column,
       format_type(a.atttypid, a.atttypmod) as type,
       a.attnotnull as required,
       pg_get_expr(d.adbin, d.adrelid) as default,
       coalesce(k.pkey, false) as pkey
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  join pg_attribute a on a.attrelid = c.oid and a.attnum > 0
                     and not a.attisdropped
  left join pg_attrdef d on d.adrelid = c.oid and d.adnum = a.attnum
  left join lateral (
       select true as pkey
         from pg_index i
        where i.indrelid = c.oid and i.indisprimary
          and a.attnum = any(i.indkey)
       ) k on true
 where c.relkind in ('r', 'v', 'm', 'p', 'f')
   and n.nspname not in ('pg_catalog', 'information_schema')
   and n.nspname not like 'pg_toast%'
   and n.nspname not like 'pg_temp%'
   and has_table_privilege(c.oid, 'select')
 order by n.nspname, c.relname, a.attnum";

/// One page of any of the lists below.
///
/// Fifty is what fits on a screen without scrolling past what a person
/// came to look at, and every list is ordered so that the rows worth
/// looking at are almost always on the first page.
const LIST_PAGE: i64 = 50;

/// A page of `auth.users` with the providers each of them signed in
/// with.
///
/// Every timestamp is cast to text in the query rather than decoded
/// here, for the reason the sql editor returns text: what a console
/// should print is what postgres would have written, not a driver's
/// rendering of a type and a timezone it had to pick. Truncated to the
/// second first, because the microseconds of a signup are ten
/// characters of noise and the columns they push off the side of the
/// screen are ones somebody came to read.
///
/// The search is one term against the email, the phone and the id,
/// because those are the three things somebody has in front of them
/// when they come here, usually pasted out of a support ticket.
const USERS: &str = "\
select u.id::text as id,
       u.email::text as email,
       u.phone::text as phone,
       date_trunc('second', u.created_at)::text as created_at,
       date_trunc('second', u.last_sign_in_at)::text as last_sign_in_at,
       (u.email_confirmed_at is not null
        or u.phone_confirmed_at is not null) as confirmed,
       u.is_anonymous as anonymous,
       (u.banned_until is not null and u.banned_until > now()) as banned,
       array(select i.provider::text
               from auth.identities i
              where i.user_id = u.id
              order by i.provider) as providers
  from auth.users u
 where $1::text = ''
    or u.email ilike $2::text
    or u.phone ilike $2::text
    or u.id::text ilike $2::text
 order by u.created_at desc nulls last, u.id
 limit $3::bigint offset $4::bigint";

/// Every bucket, with the settings that decide what an upload to it is
/// allowed to be.
///
/// No object count and no total size, on purpose and for the reason the
/// sidebar prints an estimate rather than a count: a bucket holds as
/// many rows as somebody has uploaded, there is no index that answers
/// how many or how large without reading all of them, and a listing
/// that did it would spend a project's storage table on a column of
/// numbers nobody asked for. What a person opening this wants first is
/// which buckets exist and which of them are public, and both of those
/// are one short row each.
///
/// Unpaged, because buckets come in the dozens. A project with more of
/// them than fit on a screen is one nobody has yet.
const BUCKETS: &str = "\
select b.id::text as id,
       b.name::text as name,
       b.public as public,
       b.type::text as type,
       b.file_size_limit as file_size_limit,
       b.allowed_mime_types as allowed_mime_types,
       date_trunc('second', b.created_at)::text as created_at,
       date_trunc('second', b.updated_at)::text as updated_at
  from storage.buckets b
 order by b.id";

/// One page of a bucket's objects, in name order.
///
/// Name order because that is what a listing of files is, and because
/// `bucketid_objname` is an index on exactly that, so a page deep into
/// a large bucket is read rather than sorted. Newest first would be the
/// other defensible order and would cost a sort of the whole bucket to
/// produce the first fifty rows.
///
/// The size and the content type come out of the metadata the storage
/// api wrote when the object was uploaded, so both are whatever is
/// there and neither is guaranteed. The size is matched against digits
/// before it is cast: an object whose metadata somebody wrote by hand
/// should read as a size nobody knows, not take the listing down with a
/// failed cast.
const OBJECTS: &str = "\
select o.id::text as id,
       o.name::text as name,
       case when o.metadata->>'size' ~ '^[0-9]+$'
            then (o.metadata->>'size')::bigint end as size,
       o.metadata->>'mimetype' as mimetype,
       o.version::text as version,
       date_trunc('second', o.created_at)::text as created_at,
       date_trunc('second', o.updated_at)::text as updated_at
  from storage.objects o
 where o.bucket_id = $1::text
   and ($2::text = '' or o.name ilike $3::text)
 order by o.name
 limit $4::bigint offset $5::bigint";

/// One page of the audit trail, newest first.
///
/// The payload is a json object with the actor and the event in it
/// rather than columns, because that is the shape GoTrue writes and the
/// console reads what is there rather than a shape of its own. What it
/// does do is lift the four fields somebody scans a log for into their
/// own keys, and hand the traits back whole underneath: an entry's
/// traits are whatever the flow that wrote it thought were worth
/// keeping, so there is nothing general to say about them beyond
/// showing them.
///
/// Sorted rather than read in order, because the only index upstream
/// puts on this table is on the instance id and this schema is theirs.
/// The sweep that deletes entries older than the project's retention is
/// what keeps that sort honest.
const AUDIT: &str = "\
select l.id::text as id,
       date_trunc('second', l.created_at)::text as at,
       l.payload->>'action' as action,
       l.payload->>'log_type' as kind,
       coalesce(nullif(l.payload->>'actor_username', ''),
                l.payload->>'actor_id') as actor,
       nullif(l.ip_address, '') as ip,
       nullif((l.payload->'traits')::text, 'null') as traits
  from auth.audit_log_entries l
 where $1::text = ''
    or l.payload->>'action' ilike $2::text
    or l.payload->>'actor_username' ilike $2::text
    or l.ip_address ilike $2::text
 order by l.created_at desc nulls last, l.id
 limit $3::bigint offset $4::bigint";

/// How large this database is and what has gone through it.
///
/// Two different kinds of number in one row, on purpose. The size is
/// what the database occupies right now, which is what a disk bill is
/// made of. The counters are totals since the last reset, which is what
/// a rate is made of: nobody reads `xact_commit` as a number, they read
/// two of them a minute apart. So `counting_since` is in the row too,
/// because a total with no start to it is not a measurement, and on a
/// node that has been up an hour it is a very different number than on
/// one that has been up a year.
///
/// The size is the whole database, catalog included, rather than the sum
/// of the relations listed below. Those two do not match and should not:
/// what postgres keeps about a project is part of what the project
/// costs.
const USAGE_DATABASE: &str = "\
select current_database()::text as name,
       pg_database_size(current_database()) as bytes,
       d.numbackends::bigint as connections,
       d.xact_commit as commits,
       d.xact_rollback as rollbacks,
       d.blks_read as blocks_read,
       d.blks_hit as blocks_hit,
       d.tup_returned as rows_returned,
       d.tup_fetched as rows_fetched,
       d.tup_inserted as rows_inserted,
       d.tup_updated as rows_updated,
       d.tup_deleted as rows_deleted,
       d.deadlocks as deadlocks,
       d.temp_bytes as temp_bytes,
       date_trunc('second', d.stats_reset)::text as counting_since
  from pg_stat_database d
 where d.datname = current_database()";

/// The largest relations in the project, with the table and the indexes
/// on it counted apart.
///
/// Apart because they are two different answers to why something is
/// large. A table that is mostly index is one somebody has indexed twice
/// over, which is a thing to go and look at, and a table that is mostly
/// table is just a table with rows in it.
///
/// The two window functions are the count and the total over everything
/// that matched, computed before the limit cuts the list, so a project
/// with four thousand relations gets an honest total under a list of the
/// fifty worth reading rather than a total of the fifty. They cost
/// another size lookup a relation, which is a stat of a directory and
/// the reason this is a page somebody opens rather than one that is
/// always up.
///
/// Row counts are the planner's estimate for the same reason the sidebar
/// uses it: counting them is reading the whole project to draw a table
/// of how large the project is.
const USAGE_RELATIONS: &str = "\
select n.nspname::text as schema,
       c.relname::text as name,
       c.relkind::text as kind,
       pg_total_relation_size(c.oid) as total_bytes,
       pg_table_size(c.oid) as table_bytes,
       pg_indexes_size(c.oid) as index_bytes,
       c.reltuples::float8 as rows,
       count(*) over () as relations,
       (sum(pg_total_relation_size(c.oid)) over ())::bigint as all_bytes
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
 where c.relkind in ('r', 'm', 'p')
   and n.nspname not in ('pg_catalog', 'information_schema')
   and n.nspname not like 'pg_toast%'
   and n.nspname not like 'pg_temp%'
 order by pg_total_relation_size(c.oid) desc, n.nspname, c.relname
 limit $1::bigint";

/// What the console will not run in one request, in bytes. Large
/// enough for a migration somebody pasted in, small enough that a
/// runaway client cannot make the server hold a request body it will
/// never finish reading.
const MAX_QUERY: usize = 1 << 20;

/// GET /_zou, the page itself.
///
/// Served with no cache headers of its own, so a browser revalidates
/// it after an upgrade rather than running last release's console
/// against this release's endpoints.
pub async fn page(axum::extract::State(app): axum::extract::State<Arc<App>>) -> Response {
    if !app.cfg.console {
        return crate::kong_no_route();
    }
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        PAGE,
    )
        .into_response()
}

/// Whoever is asking is the service role, or this is not their surface.
///
/// The bearer token and nothing else, the same way the storage surface
/// reads its caller and for the same reason: these routes sit outside
/// the apikey gate, because a browser navigating to a url carries no
/// apikey and cannot be made to.
fn service_role(app: &App, req: &Request<Body>) -> Result<(), Box<Response>> {
    let raw = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = match raw.get(..7) {
        Some(prefix) if prefix.eq_ignore_ascii_case("bearer ") => &raw[7..],
        _ => raw,
    };
    // A key that does not verify and a key that verifies as somebody
    // else are two different answers. The first is a typo or an
    // expired token and the second is the anon key, which is the one
    // that ships inside a web page and must never open this.
    let verified =
        crate::jwt::verify_any(token, &app.cfg.jwt_secret, app.jwks.as_ref()).map_err(|why| {
            Box::new(refused(
                StatusCode::UNAUTHORIZED,
                &format!(
                    "That key was not accepted: {}",
                    crate::storage::jose_words(&why)
                ),
            ))
        })?;
    match verified.role.as_deref() {
        Some("service_role") | Some("supabase_admin") => Ok(()),
        _ => Err(Box::new(refused(
            StatusCode::FORBIDDEN,
            "This console needs the project's service role key.",
        ))),
    }
}

fn refused(status: StatusCode, message: &str) -> Response {
    json_body(status, serde_json::json!({ "error": message }))
}

/// The three things every endpoint below settles before it looks at
/// what was actually asked for: the console is switched on, the caller
/// is the service role, and there is a database behind this server.
fn admitted<'a>(app: &'a App, req: &Request<Body>) -> Result<&'a crate::sql::Pool, Box<Response>> {
    if !app.cfg.console {
        return Err(Box::new(crate::kong_no_route()));
    }
    service_role(app, req)?;
    match &app.pool {
        Some(pool) => Ok(pool),
        None => Err(Box::new(refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "This server has no database attached.",
        ))),
    }
}

/// What a listing takes off the query string: something to search for,
/// the pattern that becomes, and which page of the answer to cut.
struct Asked {
    term: String,
    like: String,
    page: i64,
}

fn asked(query: &serde_json::Value) -> Asked {
    let term = query["q"].as_str().unwrap_or("").trim().to_string();
    // The term is a term and not a pattern: somebody searching for
    // `a_b@example.com` means that address and not any address with a
    // character where the underscore is.
    let like = format!(
        "%{}%",
        term.replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    );
    let page = query["page"]
        .as_str()
        .and_then(|p| p.parse().ok())
        .filter(|p| *p >= 0)
        .unwrap_or(0);
    Asked { term, like, page }
}

/// Whether a relation is there to be read.
///
/// Bootstrap makes every relation asked about below on each database
/// this server opens, so a no here means somebody dropped the schema,
/// which is a strange state to be in and a worse one to read a 42P01
/// about. The lists answer it as an empty page that says so.
async fn present(
    sess: &crate::sql::Session,
    relation: &str,
) -> Result<bool, tokio_postgres::Error> {
    let rows = sess
        .query(
            "select to_regclass($1::text) is not null as present",
            &[&relation],
        )
        .await?;
    Ok(rows
        .first()
        .is_some_and(|row| row.get::<_, bool>("present")))
}

/// GET /_zou/api/catalog, every relation the connecting role can read.
pub async fn catalog(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let pool = match admitted(&app, &req) {
        Ok(pool) => pool,
        Err(res) => return *res,
    };
    let sess = match pool.unscoped().await {
        Ok(sess) => sess,
        Err(e) => return refused(StatusCode::BAD_GATEWAY, &e.to_string()),
    };
    let rows = match sess.query(CATALOG, &[]).await {
        Ok(rows) => rows,
        Err(e) => {
            let _ = sess.rollback().await;
            return refused(StatusCode::BAD_GATEWAY, &e.to_string());
        }
    };
    if let Err(e) = sess.commit().await {
        return refused(StatusCode::BAD_GATEWAY, &e.to_string());
    }
    json_body(StatusCode::OK, group(&rows))
}

/// The flat catalog query folded into the shape the sidebar draws:
/// schemas, each with its relations, each with its columns in
/// attribute order.
fn group(rows: &[tokio_postgres::Row]) -> serde_json::Value {
    let mut schemas: Vec<serde_json::Value> = Vec::new();
    for row in rows {
        let schema: String = row.get("schema");
        let name: String = row.get("name");
        let kind: String = row.get("kind");
        let estimate: f64 = row.get("rows");
        let column = serde_json::json!({
            "name": row.get::<_, String>("column"),
            "type": row.get::<_, String>("type"),
            "required": row.get::<_, bool>("required"),
            "default": row.get::<_, Option<String>>("default"),
            "pkey": row.get::<_, bool>("pkey"),
        });
        // Both loops walk backwards over one element in the ordinary
        // case, because the query is ordered by schema and then by
        // relation, so the row being placed almost always belongs to
        // the last group made. The search is still a search rather
        // than an assumption, since a relation with no columns at all
        // is legal and would otherwise put the next one in the wrong
        // place.
        let schema_entry = match schemas
            .iter_mut()
            .rfind(|s| s["name"].as_str() == Some(schema.as_str()))
        {
            Some(entry) => entry,
            None => {
                schemas.push(serde_json::json!({"name": schema, "relations": []}));
                schemas.last_mut().expect("just pushed")
            }
        };
        let relations = schema_entry["relations"]
            .as_array_mut()
            .expect("relations is an array");
        let relation = match relations
            .iter_mut()
            .rfind(|r| r["name"].as_str() == Some(name.as_str()))
        {
            Some(entry) => entry,
            None => {
                relations.push(serde_json::json!({
                    "name": name,
                    "kind": kind,
                    // -1 is postgres for never analysed, which is not
                    // an estimate of zero and is not shown as one.
                    "rows": if estimate < 0.0 { serde_json::Value::Null }
                            else { serde_json::Value::from(estimate) },
                    "columns": [],
                }));
                relations.last_mut().expect("just pushed")
            }
        };
        relation["columns"]
            .as_array_mut()
            .expect("columns is an array")
            .push(column);
    }
    serde_json::json!({ "schemas": schemas })
}

/// GET /_zou/api/users, a page of the project's people.
///
/// A list rather than a query somebody has to write, because "who is
/// this person and did they ever confirm their email" is the question
/// asked most often about a project after "what is in this table", and
/// answering it in the sql editor means knowing that `auth.users` and
/// `auth.identities` exist and how they join.
///
/// A database with no `auth.users` on it answers with an empty list
/// that says so rather than with postgres's words about a relation that
/// does not exist.
pub async fn users(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let pool = match admitted(&app, &req) {
        Ok(pool) => pool,
        Err(res) => return *res,
    };
    let query = crate::auth::query_object(req.uri().query().unwrap_or_default());
    let Asked { term, like, page } = asked(&query);
    let sess = match pool.unscoped().await {
        Ok(sess) => sess,
        Err(e) => return refused(StatusCode::BAD_GATEWAY, &e.to_string()),
    };
    match present(&sess, "auth.users").await {
        Ok(true) => {}
        Ok(false) => {
            let _ = sess.commit().await;
            return json_body(
                StatusCode::OK,
                serde_json::json!({ "users": [], "more": false, "absent": true }),
            );
        }
        Err(e) => {
            let _ = sess.rollback().await;
            return refused(StatusCode::BAD_GATEWAY, &e.to_string());
        }
    }
    // One more than a page, so the page knows whether there is a next
    // one without counting a table it does not otherwise read.
    let rows = match sess
        .query(
            USERS,
            &[&term, &like, &(LIST_PAGE + 1), &(page * LIST_PAGE)],
        )
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            let _ = sess.rollback().await;
            return refused(StatusCode::BAD_GATEWAY, &e.to_string());
        }
    };
    if let Err(e) = sess.commit().await {
        return refused(StatusCode::BAD_GATEWAY, &e.to_string());
    }
    let more = rows.len() as i64 > LIST_PAGE;
    let listed: Vec<serde_json::Value> = rows
        .iter()
        .take(LIST_PAGE as usize)
        .map(|row| {
            serde_json::json!({
                "id": row.get::<_, String>("id"),
                "email": row.get::<_, Option<String>>("email"),
                "phone": row.get::<_, Option<String>>("phone"),
                "created_at": row.get::<_, Option<String>>("created_at"),
                "last_sign_in_at": row.get::<_, Option<String>>("last_sign_in_at"),
                "confirmed": row.get::<_, Option<bool>>("confirmed").unwrap_or(false),
                "anonymous": row.get::<_, Option<bool>>("anonymous").unwrap_or(false),
                "banned": row.get::<_, Option<bool>>("banned").unwrap_or(false),
                "providers": row.get::<_, Vec<String>>("providers"),
            })
        })
        .collect();
    json_body(
        StatusCode::OK,
        serde_json::json!({ "users": listed, "page": page, "more": more }),
    )
}

/// GET /_zou/api/buckets, what the project keeps files in.
///
/// The other half of "what is in this project" that the sql editor
/// answers badly. A bucket and its objects are two rows in two tables
/// somebody has to know the shape of, and the question underneath is
/// usually one of two: is this bucket public, and did that upload
/// actually land.
pub async fn buckets(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let pool = match admitted(&app, &req) {
        Ok(pool) => pool,
        Err(res) => return *res,
    };
    let sess = match pool.unscoped().await {
        Ok(sess) => sess,
        Err(e) => return refused(StatusCode::BAD_GATEWAY, &e.to_string()),
    };
    match present(&sess, "storage.buckets").await {
        Ok(true) => {}
        Ok(false) => {
            let _ = sess.commit().await;
            return json_body(
                StatusCode::OK,
                serde_json::json!({ "buckets": [], "absent": true }),
            );
        }
        Err(e) => {
            let _ = sess.rollback().await;
            return refused(StatusCode::BAD_GATEWAY, &e.to_string());
        }
    }
    let rows = match sess.query(BUCKETS, &[]).await {
        Ok(rows) => rows,
        Err(e) => {
            let _ = sess.rollback().await;
            return refused(StatusCode::BAD_GATEWAY, &e.to_string());
        }
    };
    if let Err(e) = sess.commit().await {
        return refused(StatusCode::BAD_GATEWAY, &e.to_string());
    }
    let listed: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            serde_json::json!({
                "id": row.get::<_, String>("id"),
                "name": row.get::<_, String>("name"),
                "public": row.get::<_, Option<bool>>("public").unwrap_or(false),
                "type": row.get::<_, Option<String>>("type"),
                "file_size_limit": row.get::<_, Option<i64>>("file_size_limit"),
                "allowed_mime_types": row.get::<_, Option<Vec<String>>>("allowed_mime_types"),
                "created_at": row.get::<_, Option<String>>("created_at"),
                "updated_at": row.get::<_, Option<String>>("updated_at"),
            })
        })
        .collect();
    json_body(StatusCode::OK, serde_json::json!({ "buckets": listed }))
}

/// GET /_zou/api/objects, a page of one bucket.
///
/// One bucket and not all of them, because there is no such thing as a
/// useful listing of every object a project has: the buckets are the
/// only division the storage api gives, and a person looking for a file
/// already knows which one they put it in.
pub async fn objects(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let pool = match admitted(&app, &req) {
        Ok(pool) => pool,
        Err(res) => return *res,
    };
    let query = crate::auth::query_object(req.uri().query().unwrap_or_default());
    let bucket = query["bucket"].as_str().unwrap_or("").trim().to_string();
    if bucket.is_empty() {
        return refused(
            StatusCode::BAD_REQUEST,
            "Say which bucket to list, as ?bucket=name.",
        );
    }
    let Asked { term, like, page } = asked(&query);
    let sess = match pool.unscoped().await {
        Ok(sess) => sess,
        Err(e) => return refused(StatusCode::BAD_GATEWAY, &e.to_string()),
    };
    match present(&sess, "storage.objects").await {
        Ok(true) => {}
        Ok(false) => {
            let _ = sess.commit().await;
            return json_body(
                StatusCode::OK,
                serde_json::json!({ "objects": [], "more": false, "absent": true }),
            );
        }
        Err(e) => {
            let _ = sess.rollback().await;
            return refused(StatusCode::BAD_GATEWAY, &e.to_string());
        }
    }
    let rows = match sess
        .query(
            OBJECTS,
            &[&bucket, &term, &like, &(LIST_PAGE + 1), &(page * LIST_PAGE)],
        )
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            let _ = sess.rollback().await;
            return refused(StatusCode::BAD_GATEWAY, &e.to_string());
        }
    };
    if let Err(e) = sess.commit().await {
        return refused(StatusCode::BAD_GATEWAY, &e.to_string());
    }
    let more = rows.len() as i64 > LIST_PAGE;
    let listed: Vec<serde_json::Value> = rows
        .iter()
        .take(LIST_PAGE as usize)
        .map(|row| {
            serde_json::json!({
                "id": row.get::<_, String>("id"),
                "name": row.get::<_, Option<String>>("name"),
                "size": row.get::<_, Option<i64>>("size"),
                "mimetype": row.get::<_, Option<String>>("mimetype"),
                "version": row.get::<_, Option<String>>("version"),
                "created_at": row.get::<_, Option<String>>("created_at"),
                "updated_at": row.get::<_, Option<String>>("updated_at"),
            })
        })
        .collect();
    json_body(
        StatusCode::OK,
        serde_json::json!({
            "bucket": bucket,
            "objects": listed,
            "page": page,
            "more": more,
        }),
    )
}

/// GET /_zou/api/audit, what the project's people have been doing.
///
/// The log a project keeps about itself. Every sign in, sign out,
/// signup, token refresh and admin action lands in this table with the
/// address it came from, which makes it the first thing worth reading
/// when an account has done something nobody expected, and the only
/// record of it once the process that served the request is gone.
///
/// The answer says whether the trail is being written at all, because
/// an empty list means two very different things: a project where
/// nothing has happened yet, and a project that has switched the
/// postgres copy off and is writing only to its log stream. A page that
/// could not tell them apart would report the second as silence.
pub async fn audit(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let pool = match admitted(&app, &req) {
        Ok(pool) => pool,
        Err(res) => return *res,
    };
    let writing = !app.cfg.audit.disable_postgres;
    let query = crate::auth::query_object(req.uri().query().unwrap_or_default());
    let Asked { term, like, page } = asked(&query);
    let sess = match pool.unscoped().await {
        Ok(sess) => sess,
        Err(e) => return refused(StatusCode::BAD_GATEWAY, &e.to_string()),
    };
    match present(&sess, "auth.audit_log_entries").await {
        Ok(true) => {}
        Ok(false) => {
            let _ = sess.commit().await;
            return json_body(
                StatusCode::OK,
                serde_json::json!({
                    "entries": [], "more": false, "absent": true, "writing": writing,
                }),
            );
        }
        Err(e) => {
            let _ = sess.rollback().await;
            return refused(StatusCode::BAD_GATEWAY, &e.to_string());
        }
    }
    let rows = match sess
        .query(
            AUDIT,
            &[&term, &like, &(LIST_PAGE + 1), &(page * LIST_PAGE)],
        )
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            let _ = sess.rollback().await;
            return refused(StatusCode::BAD_GATEWAY, &e.to_string());
        }
    };
    if let Err(e) = sess.commit().await {
        return refused(StatusCode::BAD_GATEWAY, &e.to_string());
    }
    let more = rows.len() as i64 > LIST_PAGE;
    let listed: Vec<serde_json::Value> = rows
        .iter()
        .take(LIST_PAGE as usize)
        .map(|row| {
            serde_json::json!({
                "id": row.get::<_, String>("id"),
                "at": row.get::<_, Option<String>>("at"),
                "action": row.get::<_, Option<String>>("action"),
                "kind": row.get::<_, Option<String>>("kind"),
                "actor": row.get::<_, Option<String>>("actor"),
                "ip": row.get::<_, Option<String>>("ip"),
                "traits": row.get::<_, Option<String>>("traits"),
            })
        })
        .collect();
    json_body(
        StatusCode::OK,
        serde_json::json!({
            "entries": listed,
            "page": page,
            "more": more,
            "writing": writing,
        }),
    )
}

/// GET /_zou/api/usage, how large this project is and how busy it has
/// been.
///
/// The question behind it is always one of two, and they arrive
/// together: the disk is filling up and nobody knows which table is
/// doing it, or something is slow and nobody knows whether this database
/// is reading from memory or from the disk. Both are one query against
/// the catalog and neither is a thing somebody should have to remember
/// the name of at three in the morning.
///
/// No probe, no sampling and nothing kept between requests. Every number
/// here is one postgres already had: the sizes are the files on disk and
/// the counters are the ones the statistics collector has been keeping
/// since it was last reset. A console that kept its own history would be
/// a monitoring system, and the node already exports one to a scraper on
/// the ops port.
pub async fn usage(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let pool = match admitted(&app, &req) {
        Ok(pool) => pool,
        Err(res) => return *res,
    };
    let sess = match pool.unscoped().await {
        Ok(sess) => sess,
        Err(e) => return refused(StatusCode::BAD_GATEWAY, &e.to_string()),
    };
    let database = match sess.query(USAGE_DATABASE, &[]).await {
        Ok(rows) => rows,
        Err(e) => {
            let _ = sess.rollback().await;
            return refused(StatusCode::BAD_GATEWAY, &e.to_string());
        }
    };
    let relations = match sess.query(USAGE_RELATIONS, &[&LIST_PAGE]).await {
        Ok(rows) => rows,
        Err(e) => {
            let _ = sess.rollback().await;
            return refused(StatusCode::BAD_GATEWAY, &e.to_string());
        }
    };
    if let Err(e) = sess.commit().await {
        return refused(StatusCode::BAD_GATEWAY, &e.to_string());
    }
    let about = database.first().map(|row| {
        serde_json::json!({
            "name": row.get::<_, String>("name"),
            "bytes": row.get::<_, Option<i64>>("bytes"),
            "connections": row.get::<_, Option<i64>>("connections"),
            "commits": row.get::<_, Option<i64>>("commits"),
            "rollbacks": row.get::<_, Option<i64>>("rollbacks"),
            "blocks_read": row.get::<_, Option<i64>>("blocks_read"),
            "blocks_hit": row.get::<_, Option<i64>>("blocks_hit"),
            "rows_returned": row.get::<_, Option<i64>>("rows_returned"),
            "rows_fetched": row.get::<_, Option<i64>>("rows_fetched"),
            "rows_inserted": row.get::<_, Option<i64>>("rows_inserted"),
            "rows_updated": row.get::<_, Option<i64>>("rows_updated"),
            "rows_deleted": row.get::<_, Option<i64>>("rows_deleted"),
            "deadlocks": row.get::<_, Option<i64>>("deadlocks"),
            "temp_bytes": row.get::<_, Option<i64>>("temp_bytes"),
            // Null on a node whose counters have never been reset, which
            // is most of them: the totals then run from when the cluster
            // was created.
            "counting_since": row.get::<_, Option<String>>("counting_since"),
        })
    });
    // Both come off the first row, since the window functions put the
    // same pair on every one of them. An empty answer is a project with
    // no relations of its own, which is a new database and not an error.
    let counted: i64 = relations
        .first()
        .and_then(|row| row.get::<_, Option<i64>>("relations"))
        .unwrap_or(0);
    let occupied: i64 = relations
        .first()
        .and_then(|row| row.get::<_, Option<i64>>("all_bytes"))
        .unwrap_or(0);
    let listed: Vec<serde_json::Value> = relations
        .iter()
        .map(|row| {
            let estimate: f64 = row.get("rows");
            serde_json::json!({
                "schema": row.get::<_, String>("schema"),
                "name": row.get::<_, String>("name"),
                "kind": row.get::<_, String>("kind"),
                "total_bytes": row.get::<_, Option<i64>>("total_bytes"),
                "table_bytes": row.get::<_, Option<i64>>("table_bytes"),
                "index_bytes": row.get::<_, Option<i64>>("index_bytes"),
                "rows": if estimate < 0.0 { serde_json::Value::Null }
                        else { serde_json::Value::from(estimate) },
            })
        })
        .collect();
    json_body(
        StatusCode::OK,
        serde_json::json!({
            "database": about,
            "relations": listed,
            "relation_count": counted,
            "relation_bytes": occupied,
        }),
    )
}

/// POST /_zou/api/sql, run what the editor holds.
///
/// The simple protocol, so a person can paste several statements and
/// get an answer for each, which is what a migration looks like. It
/// also means every value arrives as text in postgres's own output
/// format, which is what a console wants: no type is rendered through
/// a driver's idea of it, a numeric is the digits postgres printed and
/// a timestamp is the string it would have written to a dump.
pub async fn run(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let pool = match admitted(&app, &req) {
        Ok(pool) => pool,
        Err(res) => return *res,
    };
    let body = match axum::body::to_bytes(req.into_body(), MAX_QUERY).await {
        Ok(body) => body,
        Err(_) => {
            return refused(
                StatusCode::PAYLOAD_TOO_LARGE,
                "That is more sql than this console will run in one go.",
            );
        }
    };
    let query = match serde_json::from_slice::<serde_json::Value>(&body) {
        Ok(v) => v
            .get("query")
            .and_then(|q| q.as_str())
            .unwrap_or("")
            .to_string(),
        Err(e) => return refused(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    if query.trim().is_empty() {
        return refused(StatusCode::BAD_REQUEST, "There is nothing to run.");
    }
    let sess = match pool.unscoped().await {
        Ok(sess) => sess,
        Err(e) => return refused(StatusCode::BAD_GATEWAY, &e.to_string()),
    };
    let started = std::time::Instant::now();
    let answer = sess.simple_rows(&query).await;
    let elapsed = started.elapsed();
    // Whatever happened, the connection goes back with no transaction
    // on it. A batch that opened one and did not close it is rolled
    // back here rather than left for whoever checks this connection
    // out next, and a batch that closed its own makes this a no op
    // that postgres answers with a warning nobody reads.
    let _ = sess.simple_rows("rollback").await;
    let statements = match answer {
        Ok(messages) => messages,
        Err(e) => {
            let _ = sess.commit().await;
            return json_body(StatusCode::OK, serde_json::json!({ "error": pg_words(&e) }));
        }
    };
    if let Err(e) = sess.commit().await {
        return refused(StatusCode::BAD_GATEWAY, &e.to_string());
    }
    json_body(
        StatusCode::OK,
        serde_json::json!({
            "results": results(statements),
            "ms": elapsed.as_secs_f64() * 1000.0,
        }),
    )
}

/// A failed statement in the words a person can act on.
///
/// The message, and the detail and hint when postgres wrote them,
/// which between them are usually the whole answer: "column x does not
/// exist" with "Perhaps you meant to reference the column y" is a fix
/// rather than a report. Everything else postgres carries, the file
/// and line of the C source that raised it, is for a postgres hacker
/// and is left off.
fn pg_words(e: &tokio_postgres::Error) -> serde_json::Value {
    let Some(db) = e.as_db_error() else {
        return serde_json::json!({ "message": e.to_string() });
    };
    serde_json::json!({
        "message": db.message(),
        "code": db.code().code(),
        "detail": db.detail(),
        "hint": db.hint(),
        "position": db.position().map(|p| match p {
            tokio_postgres::error::ErrorPosition::Original(n) => *n,
            tokio_postgres::error::ErrorPosition::Internal { position, .. } => *position,
        }),
    })
}

/// One entry per statement postgres answered, in the order it answered
/// them, each carrying either a grid or a tag.
///
/// The row description arrives before the rows it describes and a
/// statement that returns nothing has no description at all, so the
/// entry is opened by whichever of the two comes first and closed by
/// the command tag that always follows.
fn results(messages: Vec<tokio_postgres::SimpleQueryMessage>) -> serde_json::Value {
    use tokio_postgres::SimpleQueryMessage as M;
    let mut out: Vec<serde_json::Value> = Vec::new();
    let mut columns: Vec<String> = Vec::new();
    let mut rows: Vec<serde_json::Value> = Vec::new();
    for message in messages {
        match message {
            M::RowDescription(description) => {
                columns = description.iter().map(|c| c.name().to_string()).collect();
            }
            M::Row(row) => {
                let cells: Vec<serde_json::Value> = (0..row.len())
                    // A null is a null and not the empty string, which
                    // is a different value and prints differently.
                    .map(|i| match row.get(i) {
                        Some(v) => serde_json::Value::from(v),
                        None => serde_json::Value::Null,
                    })
                    .collect();
                rows.push(serde_json::Value::Array(cells));
            }
            M::CommandComplete(touched) => {
                out.push(serde_json::json!({
                    "columns": columns,
                    "rows": rows,
                    "touched": touched,
                }));
                columns = Vec::new();
                rows = Vec::new();
            }
            // The enum is not exhaustive upstream, so a message this
            // build has never heard of is skipped rather than fatal.
            _ => {}
        }
    }
    serde_json::Value::Array(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_catalog_folds_into_schemas_then_relations_then_columns() {
        // Built by hand rather than from a database, because what is
        // being checked is the fold and not the query.
        let json = group(&[]);
        assert_eq!(json["schemas"], serde_json::json!([]));
    }

    #[test]
    fn a_statement_with_no_rows_still_gets_an_entry() {
        let out = results(Vec::new());
        assert_eq!(out, serde_json::json!([]));
    }

    #[test]
    fn the_page_is_one_file_with_nothing_fetched_from_anywhere() {
        // A console that pulled a script or a font off the internet
        // would be a console that does not work on the box it is most
        // wanted on, which is the one with no route to it.
        assert!(!PAGE.contains("//cdn."));
        assert!(!PAGE.contains("https://"));
        assert!(PAGE.contains("/_zou/api/sql"));
        assert!(PAGE.contains("/_zou/api/users?q="));
        assert!(PAGE.contains("/_zou/api/buckets"));
        assert!(PAGE.contains("/_zou/api/objects?bucket="));
        assert!(PAGE.contains("/_zou/api/audit?q="));
        assert!(PAGE.contains("/_zou/api/usage"));
    }

    #[test]
    fn a_search_term_is_escaped_into_a_pattern_that_matches_it_literally() {
        let for_a_discount = asked(&serde_json::json!({ "q": " 50%_off ", "page": "2" }));
        assert_eq!(for_a_discount.term, "50%_off");
        assert_eq!(for_a_discount.like, "%50\\%\\_off%");
        assert_eq!(for_a_discount.page, 2);
        // A page nobody named is the first one, and a page below the
        // first one does not exist, so both read as zero rather than as
        // a negative offset postgres would refuse.
        assert_eq!(asked(&serde_json::json!({})).page, 0);
        assert_eq!(asked(&serde_json::json!({ "page": "-3" })).page, 0);
        assert_eq!(asked(&serde_json::json!({ "page": "many" })).page, 0);
    }
}
