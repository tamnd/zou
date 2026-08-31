//! The web admin at `/_zou`.
//!
//! One page and three endpoints, served by the same binary that serves
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

/// One page of the user list.
///
/// Fifty is what fits on a screen without scrolling past what a person
/// came to look at, and the list is ordered newest first, so the ones
/// worth looking at are almost always on the first page.
const USERS_PAGE: i64 = 50;

/// A page of `auth.users` with the providers each of them signed in
/// with.
///
/// Every timestamp is cast to text in the query rather than decoded
/// here, for the reason the sql editor returns text: what a console
/// should print is what postgres would have written, not a driver's
/// rendering of a type and a timezone it had to pick.
///
/// The search is one term against the email, the phone and the id,
/// because those are the three things somebody has in front of them
/// when they come here, usually pasted out of a support ticket.
const USERS: &str = "\
select u.id::text as id,
       u.email::text as email,
       u.phone::text as phone,
       u.created_at::text as created_at,
       u.last_sign_in_at::text as last_sign_in_at,
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

/// GET /_zou/api/catalog, every relation the connecting role can read.
pub async fn catalog(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    if !app.cfg.console {
        return crate::kong_no_route();
    }
    if let Err(res) = service_role(&app, &req) {
        return *res;
    }
    let Some(pool) = &app.pool else {
        return refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "This server has no database attached.",
        );
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
/// A node with no auth schema on it is not an error. Auth is one of the
/// surfaces a project can be running without, so the answer is an empty
/// list that says so, and the page prints a sentence rather than a
/// stack of postgres words about a relation that does not exist.
pub async fn users(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    if !app.cfg.console {
        return crate::kong_no_route();
    }
    if let Err(res) = service_role(&app, &req) {
        return *res;
    }
    let Some(pool) = &app.pool else {
        return refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "This server has no database attached.",
        );
    };
    let query = crate::auth::query_object(req.uri().query().unwrap_or_default());
    let term = query["q"].as_str().unwrap_or("").trim().to_string();
    let page: i64 = query["page"]
        .as_str()
        .and_then(|p| p.parse().ok())
        .filter(|p| *p >= 0)
        .unwrap_or(0);
    let sess = match pool.unscoped().await {
        Ok(sess) => sess,
        Err(e) => return refused(StatusCode::BAD_GATEWAY, &e.to_string()),
    };
    let present = match sess
        .query(
            "select to_regclass('auth.users') is not null as present",
            &[],
        )
        .await
    {
        Ok(rows) => rows
            .first()
            .is_some_and(|row| row.get::<_, bool>("present")),
        Err(e) => {
            let _ = sess.rollback().await;
            return refused(StatusCode::BAD_GATEWAY, &e.to_string());
        }
    };
    if !present {
        let _ = sess.commit().await;
        return json_body(
            StatusCode::OK,
            serde_json::json!({ "users": [], "more": false, "absent": true }),
        );
    }
    // One more than a page, so the page knows whether there is a next
    // one without counting a table it does not otherwise read.
    // The term is a term and not a pattern: somebody searching for
    // `a_b@example.com` means that address and not any address with a
    // character where the underscore is.
    let like = format!(
        "%{}%",
        term.replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    );
    let rows = match sess
        .query(
            USERS,
            &[&term, &like, &(USERS_PAGE + 1), &(page * USERS_PAGE)],
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
    let more = rows.len() as i64 > USERS_PAGE;
    let listed: Vec<serde_json::Value> = rows
        .iter()
        .take(USERS_PAGE as usize)
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
    if !app.cfg.console {
        return crate::kong_no_route();
    }
    if let Err(res) = service_role(&app, &req) {
        return *res;
    }
    let Some(pool) = &app.pool else {
        return refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "This server has no database attached.",
        );
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
    }
}
