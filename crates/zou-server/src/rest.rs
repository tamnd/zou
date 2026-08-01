//! The REST table surface: GET, HEAD, POST, PATCH, and DELETE on
//! /rest/v1/{table}, and function calls on /rest/v1/rpc/{fn}.
//!
//! A request's query string becomes a [`zou_rest::plan::Query`], the
//! relationship catalog loads through INTROSPECT_SQL on the same
//! transaction the query will run in, the planner emits one
//! statement, and the rows come back as jsonb text the handler joins
//! into the response array. Everything runs inside a transaction
//! carrying the full request context, read only for reads, so RLS
//! policies see role, request.jwt.claims, request.headers,
//! request.cookies, request.method, and request.path exactly like
//! they do behind Supabase.
//!
//! Writes go through the zou-rest mutation builders: the body binds
//! whole as one json parameter, root filters become the mutation's
//! WHERE, and Prefer picks the response. return=minimal answers 201
//! or 204 empty, headers-only adds a Location built from the
//! returned primary key, and representation mounts the mutation as a
//! CTE and runs the request's select tree over the returned rows, so
//! embeds work on writes exactly as on reads. Prefer resolution
//! turns a POST into an upsert, targeting the on_conflict columns or
//! the table's primary key.
//!
//! RPC resolves the function like named notation would: pg_proc
//! introspection finds every overload, the supplied argument names
//! pick one, a json body binds whole and unpacks per argument while
//! query string values bind one text parameter each. GET runs read
//! only so a volatile function fails with pg's 25006, which maps to
//! 405 exactly as PostgREST has it. What comes back follows the
//! return type: void is 204, a scalar is the bare json value, a set
//! of rows goes through the planner over a CTE so select, filters,
//! order, and embeds apply to the result.
//!
//! Errors speak PostgREST throughout: the body is always the four
//! key code, details, hint, message object, grammar problems are
//! PGRST100, a missing relationship is PGRST200 and an ambiguous one
//! PGRST201 with status 300, and SQL errors map SQLSTATE to status
//! with the same table PostgREST's Error.hs uses, so a client that
//! branches on a 409 unique violation or a 404 missing table sees
//! the code it expects.

use std::sync::Arc;

use axum::body::Body;
use axum::http::request::Parts;
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use tokio_postgres::types::{Format, IsNull, ToSql, Type, to_sql_checked};
use zou_rest::catalog::{Catalog, FkRow, INTROSPECT_SQL};
use zou_rest::filter::{self, Node, Parsed};
use zou_rest::mutate::{self, Conflict, Returning};
use zou_rest::plan::{self, PlanError, Query};
use zou_rest::{order, page, rpc, select};

use crate::sql::{RequestContext, Session};
use crate::{App, AuthContext, json_body};

/// The one schema the REST surface serves until Accept-Profile
/// lands, the same default a fresh Supabase project exposes.
const SCHEMA: &str = "public";

/// The most body a write accepts, 16 MiB like a generous PostgREST
/// deployment; past it the response is 413.
const BODY_LIMIT: usize = 1 << 24;

/// The table's primary key columns in constraint order, for the
/// default upsert target and the Location header.
const PK_SQL: &str = "select a.attname::text \
     from pg_constraint c \
     join pg_class t on t.oid = c.conrelid \
     join pg_namespace n on n.oid = t.relnamespace \
     join pg_attribute a on a.attrelid = t.oid and a.attnum = any(c.conkey) \
     where c.contype = 'p' and n.nspname = $1 and t.relname = $2 \
     order by array_position(c.conkey, a.attnum)";

/// A PostgREST shaped error: a status and the four body keys, with
/// details and hint rendered as json null when absent, which is what
/// supabase-js expects to destructure.
#[derive(Debug)]
pub struct RestError {
    pub status: StatusCode,
    pub code: String,
    pub message: String,
    pub details: Option<String>,
    pub hint: Option<String>,
}

impl RestError {
    fn into_response(self) -> Response {
        json_body(
            self.status,
            serde_json::json!({
                "code": self.code,
                "details": self.details,
                "hint": self.hint,
                "message": self.message,
            }),
        )
    }
}

fn bad_grammar(message: impl Into<String>) -> RestError {
    RestError {
        status: StatusCode::BAD_REQUEST,
        code: "PGRST100".to_string(),
        message: message.into(),
        details: None,
        hint: None,
    }
}

fn no_database() -> RestError {
    RestError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "PGRST000".to_string(),
        message: "Database connection error. Retrying the connection.".to_string(),
        details: None,
        hint: None,
    }
}

/// A body that is not the json a write needs, PostgREST's PGRST102
/// carrying the parser's own message.
fn invalid_body(message: impl Into<String>) -> RestError {
    RestError {
        status: StatusCode::BAD_REQUEST,
        code: "PGRST102".to_string(),
        message: message.into(),
        details: None,
        hint: None,
    }
}

/// A parameter sent in text format and accepted for any type, which
/// hands the parse to the server exactly like an inline unknown
/// literal, minus the injection surface. The compiler's type stance
/// depends on this: every client value binds as one of these.
#[derive(Debug)]
struct Text(String);

impl ToSql for Text {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        Format::Text
    }

    to_sql_checked!();
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Percent decoding plus the form flavor's plus-for-space, applied
/// to each side of a pair after splitting on & and =, so encoded
/// separators land inside values instead of splitting them.
fn decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => match (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                (Some(hi), Some(lo)) => {
                    out.push(hi << 4 | lo);
                    i += 3;
                }
                _ => {
                    out.push(b'%');
                    i += 1;
                }
            },
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The route an `orders.items.order` style key addresses, the
/// segments before the reserved word.
fn route_of(prefix: &str) -> Vec<String> {
    if prefix.is_empty() {
        Vec::new()
    } else {
        prefix.split('.').map(str::to_string).collect()
    }
}

/// The write only query parameters: on_conflict names the upsert
/// target and columns narrows which body keys insert.
#[derive(Debug, Default)]
struct Extras {
    on_conflict: Option<Vec<String>>,
    columns: Option<Vec<String>>,
}

fn csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// One query string into the planner's input. Reserved words are
/// select, order, limit, offset, on_conflict, and columns, the
/// pagination three optionally behind an embed route, apikey is the
/// gate's and skipped, and every other key is a filter. A quoted
/// column named "order" stays a filter because the quote survives
/// into the key.
fn parse_query(table: &str, raw: Option<&str>) -> Result<(Query, Extras), RestError> {
    let mut q = Query {
        table: table.to_string(),
        ..Query::default()
    };
    let mut extras = Extras::default();
    let mut selected = false;
    for pair in raw.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode(k);
        let value = decode(v);
        if key == "apikey" {
            continue;
        }
        let (prefix, word) = match key.rsplit_once('.') {
            Some((p, w)) => (p, w),
            None => ("", key.as_str()),
        };
        match word {
            "select" if prefix.is_empty() => {
                q.select = select::parse(&value).map_err(|e| bad_grammar(e.to_string()))?;
                selected = true;
            }
            "on_conflict" if prefix.is_empty() => {
                extras.on_conflict = Some(csv(&value));
            }
            "columns" if prefix.is_empty() => {
                extras.columns = Some(csv(&value));
            }
            "order" => {
                let terms = order::parse(&value).map_err(|e| bad_grammar(e.to_string()))?;
                q.order.push((route_of(prefix), terms));
            }
            "limit" => {
                let n = page::parse_limit(&value).map_err(|e| bad_grammar(e.to_string()))?;
                q.limit.push((route_of(prefix), n));
            }
            "offset" => {
                let n = page::parse_offset(&value).map_err(|e| bad_grammar(e.to_string()))?;
                q.offset.push((route_of(prefix), n));
            }
            _ => match filter::parse_pair(&key, &value).map_err(|e| bad_grammar(e.to_string()))? {
                Parsed::Filter(cond) => q.filters.push((Vec::new(), Node::Cond(cond))),
                Parsed::Logic {
                    embed,
                    op,
                    negated,
                    kids,
                } => q.filters.push((embed, Node::Group { op, negated, kids })),
            },
        }
    }
    if !selected {
        q.select = select::parse("*").expect("* always parses");
    }
    Ok((q, extras))
}

/// What a write returns, the Prefer return token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ret {
    Minimal,
    HeadersOnly,
    Representation,
}

/// How Prefer count= wants the total computed: count(*) beside the
/// read, the planner's row estimate, or exact bounded by the
/// estimate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Count {
    Exact,
    Planned,
    Estimated,
}

/// The Prefer tokens the surface honors. Unknown preferences pass
/// through silently until handling=strict lands, and `applied`
/// keeps the recognized ones in arrival order for the
/// Preference-Applied header.
#[derive(Debug)]
struct Prefer {
    ret: Ret,
    /// Some(true) merges duplicates, Some(false) ignores them.
    merge: Option<bool>,
    count: Option<Count>,
    applied: Vec<&'static str>,
}

fn parse_prefer(headers: &HeaderMap) -> Prefer {
    let mut p = Prefer {
        ret: Ret::Minimal,
        merge: None,
        count: None,
        applied: Vec::new(),
    };
    for value in headers.get_all("prefer") {
        let Ok(line) = value.to_str() else { continue };
        for item in line.split(',') {
            let token = match item.trim() {
                "return=minimal" => {
                    p.ret = Ret::Minimal;
                    "return=minimal"
                }
                "return=headers-only" => {
                    p.ret = Ret::HeadersOnly;
                    "return=headers-only"
                }
                "return=representation" => {
                    p.ret = Ret::Representation;
                    "return=representation"
                }
                "resolution=merge-duplicates" => {
                    p.merge = Some(true);
                    "resolution=merge-duplicates"
                }
                "resolution=ignore-duplicates" => {
                    p.merge = Some(false);
                    "resolution=ignore-duplicates"
                }
                "count=exact" => {
                    p.count = Some(Count::Exact);
                    "count=exact"
                }
                "count=planned" => {
                    p.count = Some(Count::Planned);
                    "count=planned"
                }
                "count=estimated" => {
                    p.count = Some(Count::Estimated);
                    "count=estimated"
                }
                _ => continue,
            };
            if !p.applied.contains(&token) {
                p.applied.push(token);
            }
        }
    }
    p
}

/// An insert body into its column list and normalized payload: one
/// object wraps into a single element array, the columns are the
/// union of every element's keys so a key absent from one row
/// inserts null there, and the columns parameter narrows the list
/// when given, PostgREST's shapes.
fn insert_payload(
    bytes: &[u8],
    columns: Option<&[String]>,
) -> Result<(Vec<String>, String), RestError> {
    let v: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| invalid_body(e.to_string()))?;
    let arr = match v {
        serde_json::Value::Object(_) => vec![v],
        serde_json::Value::Array(a) => a,
        _ => {
            return Err(invalid_body(
                "the insert body must be a json object or array",
            ));
        }
    };
    let mut cols: Vec<String> = Vec::new();
    for item in &arr {
        let Some(obj) = item.as_object() else {
            return Err(invalid_body(
                "every element of an insert array must be an object",
            ));
        };
        for key in obj.keys() {
            if !cols.contains(key) {
                cols.push(key.clone());
            }
        }
    }
    if let Some(list) = columns {
        cols = list.to_vec();
    }
    Ok((cols, serde_json::Value::Array(arr).to_string()))
}

/// An update body into its column list and payload, one json object
/// whose keys are the columns to set.
fn update_payload(bytes: &[u8]) -> Result<(Vec<String>, String), RestError> {
    let v: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| invalid_body(e.to_string()))?;
    let Some(obj) = v.as_object() else {
        return Err(invalid_body("the update body must be a json object"));
    };
    Ok((obj.keys().cloned().collect(), v.to_string()))
}

/// The Range header as a root limit and offset, only when the query
/// string did not already page the root, and silently ignored when
/// it is not a parseable items range, both PostgREST's stance.
fn apply_range(q: &mut Query, headers: &HeaderMap) {
    let already =
        q.limit.iter().any(|(r, _)| r.is_empty()) || q.offset.iter().any(|(r, _)| r.is_empty());
    if already {
        return;
    }
    let Some(range) = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| page::parse_range(v).ok())
    else {
        return;
    };
    if range.offset() > 0 {
        q.offset.push((Vec::new(), range.offset()));
    }
    if let Some(limit) = range.limit() {
        q.limit.push((Vec::new(), limit));
    }
}

/// PostgREST's SQLSTATE to status table, transcribed from Error.hs.
/// `authed` decides 401 versus 403 on insufficient privilege, and a
/// PT prefixed state smuggles its own status the way a RAISE
/// sqlstate 'PT404' does in PostgREST.
fn status_for(code: &str, message: &str, authed: bool) -> StatusCode {
    let s = |n: u16| StatusCode::from_u16(n).expect("status codes in the table are valid");
    match code {
        "23503" | "23505" => s(409),
        "25006" => s(405),
        "21000" if message.ends_with("requires a WHERE clause") => s(400),
        "21000" => s(500),
        "22023" if message.starts_with("role") && message.ends_with("does not exist") => s(401),
        "53400" => s(500),
        "57P01" => s(503),
        "P0001" => s(400),
        "42883" | "42P01" => s(404),
        "42P17" => s(500),
        "42501" if authed => s(403),
        "42501" => s(401),
        c if c.starts_with("08") || c.starts_with("53") => s(503),
        c if c.starts_with("0L") || c.starts_with("0P") || c.starts_with("28") => s(403),
        c if c.starts_with("09")
            || c.starts_with("25")
            || c.starts_with("2D")
            || c.starts_with("38")
            || c.starts_with("39")
            || c.starts_with("3B")
            || c.starts_with("40")
            || c.starts_with("54")
            || c.starts_with("55")
            || c.starts_with("57")
            || c.starts_with("58")
            || c.starts_with("F0")
            || c.starts_with("HV")
            || c.starts_with("P0")
            || c.starts_with("XX") =>
        {
            s(500)
        }
        c if c.starts_with("PT") => c[2..]
            .parse()
            .ok()
            .and_then(|n| StatusCode::from_u16(n).ok())
            .unwrap_or(s(500)),
        _ => s(400),
    }
}

/// A pool or SQL error into the PostgREST body. A real database
/// error carries its SQLSTATE through the table above, anything
/// else, a dropped connection or a refused dial, is the connection
/// error shape with status 503.
fn pg_error(e: &tokio_postgres::Error, authed: bool) -> RestError {
    match e.as_db_error() {
        Some(db) => RestError {
            status: status_for(db.code().code(), db.message(), authed),
            code: db.code().code().to_string(),
            message: db.message().to_string(),
            details: db.detail().map(str::to_string),
            hint: db.hint().map(str::to_string),
        },
        None => RestError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "PGRST001".to_string(),
            message: "Database connection error. Retrying the connection.".to_string(),
            details: Some(e.to_string()),
            hint: None,
        },
    }
}

fn plan_error(e: PlanError) -> RestError {
    match e {
        PlanError::Embed(e) => RestError {
            status: if e.code == "PGRST201" {
                StatusCode::MULTIPLE_CHOICES
            } else {
                StatusCode::BAD_REQUEST
            },
            code: e.code.to_string(),
            message: e.message,
            details: e.details,
            hint: e.hint,
        },
        PlanError::Compile(c) => bad_grammar(c.message),
        PlanError::Other(m) => RestError {
            status: StatusCode::BAD_REQUEST,
            // The one refusal PostgREST names separately: paging or
            // filtering a route the select tree does not embed.
            code: if m.contains("is not an embedded resource") {
                "PGRST108".to_string()
            } else {
                "PGRST100".to_string()
            },
            message: m,
            details: None,
            hint: None,
        },
    }
}

/// The request identity and shape as the six settings the session
/// pool injects, which is the whole per request contract RLS
/// policies read.
fn request_context(auth: &AuthContext, req: &Parts) -> RequestContext {
    let mut headers = serde_json::Map::new();
    for (name, value) in &req.headers {
        if let Ok(v) = value.to_str() {
            headers.insert(name.as_str().to_string(), serde_json::Value::from(v));
        }
    }
    let mut cookies = serde_json::Map::new();
    if let Some(line) = req
        .headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
    {
        for item in line.split(';') {
            if let Some((name, value)) = item.trim().split_once('=') {
                cookies.insert(name.to_string(), serde_json::Value::from(value));
            }
        }
    }
    RequestContext {
        role: auth.role.clone(),
        claims: auth.claims.to_string(),
        method: req.method.as_str().to_string(),
        path: req.uri.path().to_string(),
        headers: serde_json::Value::Object(headers).to_string(),
        cookies: serde_json::Value::Object(cookies).to_string(),
    }
}

/// PostgREST's Content-Range: the served window over the total. The
/// window collapses to * when no rows came back or the total is
/// known to be zero, the total is * when nobody asked for one.
fn content_range(offset: u64, rows: usize, total: Option<i64>) -> String {
    let lower = offset as i64;
    let upper = lower + rows as i64 - 1;
    let window = if total == Some(0) || lower > upper {
        "*".to_string()
    } else {
        format!("{lower}-{upper}")
    };
    match total {
        Some(t) => format!("{window}/{t}"),
        None => format!("{window}/*"),
    }
}

/// The read status PostgREST derives from the window and the total:
/// without a total everything is 200, a window past the total is
/// 416, a window smaller than the total is 206.
fn range_status(offset: u64, rows: usize, total: Option<i64>) -> StatusCode {
    let lower = offset as i64;
    let upper = lower + rows as i64 - 1;
    match total {
        Some(t) if lower > t => StatusCode::RANGE_NOT_SATISFIABLE,
        Some(t) if 1 + upper - lower < t => StatusCode::PARTIAL_CONTENT,
        _ => StatusCode::OK,
    }
}

/// The 416 body, PostgREST's PGRST103 out of bounds spelling.
fn out_of_bounds(offset: u64, total: i64) -> serde_json::Value {
    serde_json::json!({
        "code": "PGRST103",
        "details": format!("An offset of {offset} was requested, but there are only {total} rows."),
        "hint": null,
        "message": "Requested range not satisfiable",
    })
}

/// The fk graph on the request's own transaction, one pg_constraint
/// query; caching belongs to the catalog epoch work.
async fn load_catalog(sess: &Session, authed: bool) -> Result<Catalog, RestError> {
    let rows = sess
        .query(INTROSPECT_SQL, &[&SCHEMA])
        .await
        .map_err(|e| pg_error(&e, authed))?;
    Ok(Catalog::new(
        rows.iter()
            .map(|r| FkRow {
                constraint: r.get(0),
                table: r.get(1),
                columns: r.get(2),
                ref_table: r.get(3),
                ref_columns: r.get(4),
                unique: r.get(5),
                in_pk: r.get(6),
            })
            .collect(),
    ))
}

/// Rows of jsonb text joined into the response array by hand, which
/// hands back the row count for Content-Range for free.
fn json_array(rows: &[tokio_postgres::Row]) -> String {
    let mut body = String::from("[");
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            body.push(',');
        }
        body.push_str(&row.get::<_, String>(0));
    }
    body.push(']');
    body
}

fn param_refs(params: &[Text]) -> Vec<&(dyn ToSql + Sync)> {
    params.iter().map(|p| p as _).collect()
}

async fn read(
    app: &App,
    table: &str,
    auth: &AuthContext,
    req: &Parts,
) -> Result<Response, RestError> {
    let (mut q, _) = parse_query(table, req.uri.query())?;
    apply_range(&mut q, &req.headers);
    let prefer = parse_prefer(&req.headers);

    let pool = app.pool.as_ref().ok_or_else(no_database)?;
    let authed = auth.role != "anon";
    let ctx = request_context(auth, req);
    let sess = pool
        .session(&ctx, true)
        .await
        .map_err(|e| pg_error(&e, authed))?;

    // An early return past this point drops the session, which
    // forfeits the connection instead of pooling a dirty one, the
    // containment the pool promises.
    let catalog = load_catalog(&sess, authed).await?;

    let sql = plan::plan(&catalog, &q).map_err(plan_error)?;
    let wrapped = format!(
        "select to_jsonb(\"_zou_row\")::text from ({}) as \"_zou_row\"",
        sql.text
    );
    let params: Vec<Text> = sql.params.into_iter().map(Text).collect();
    let rows = sess
        .query(&wrapped, &param_refs(&params))
        .await
        .map_err(|e| pg_error(&e, authed))?;

    // The total, when count= asked for one, on the same transaction
    // so it sees the same snapshot. An unpaged exact total is just
    // the page, PostgREST's shortcut; a paged one runs the count
    // query, planned EXPLAINs it, and estimated takes exact bounded
    // below by the plan's guess, which is PostgREST's arithmetic
    // when no max-rows cap is configured.
    let paged =
        q.limit.iter().any(|(r, _)| r.is_empty()) || q.offset.iter().any(|(r, _)| r.is_empty());
    let needs_exact = matches!(prefer.count, Some(Count::Exact | Count::Estimated));
    let needs_planned = matches!(prefer.count, Some(Count::Planned | Count::Estimated));
    let count_sql = if (needs_exact && paged) || needs_planned {
        Some(plan::count(&catalog, &q).map_err(plan_error)?)
    } else {
        None
    };
    let count_params: Vec<Text> = count_sql
        .as_ref()
        .map(|c| c.params.iter().cloned().map(Text).collect())
        .unwrap_or_default();
    let exact: Option<i64> = if needs_exact {
        if paged {
            let c = count_sql.as_ref().expect("a paged exact built the count");
            let text = format!("select count(*) from ({}) as \"_zou_count\"", c.text);
            let crows = sess
                .query(&text, &param_refs(&count_params))
                .await
                .map_err(|e| pg_error(&e, authed))?;
            Some(crows[0].get(0))
        } else {
            Some(rows.len() as i64)
        }
    } else {
        None
    };
    let planned: Option<i64> = if needs_planned {
        let c = count_sql.as_ref().expect("planned built the count");
        let text = format!("explain (format json) {}", c.text);
        let erows = sess
            .query(&text, &param_refs(&count_params))
            .await
            .map_err(|e| pg_error(&e, authed))?;
        erows.first().and_then(|r| {
            r.get::<_, serde_json::Value>(0)
                .pointer("/0/Plan/Plan Rows")
                .and_then(serde_json::Value::as_i64)
        })
    } else {
        None
    };
    sess.commit().await.map_err(|e| pg_error(&e, authed))?;
    let total = match prefer.count {
        None => None,
        Some(Count::Exact) => exact,
        Some(Count::Planned) => planned,
        Some(Count::Estimated) => exact.zip(planned).map(|(e, p)| e.max(p)),
    };

    let offset = q
        .offset
        .iter()
        .find(|(r, _)| r.is_empty())
        .map(|(_, n)| *n)
        .unwrap_or(0);
    let status = range_status(offset, rows.len(), total);
    let mut res = if status == StatusCode::RANGE_NOT_SATISFIABLE {
        json_body(status, out_of_bounds(offset, total.unwrap_or(0)))
    } else {
        (
            status,
            [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
            json_array(&rows),
        )
            .into_response()
    };
    if let Ok(v) = content_range(offset, rows.len(), total).parse() {
        res.headers_mut().insert(header::CONTENT_RANGE, v);
    }
    let applied = match prefer.count {
        Some(Count::Exact) => Some("count=exact"),
        Some(Count::Planned) => Some("count=planned"),
        Some(Count::Estimated) => Some("count=estimated"),
        None => None,
    };
    if let Some(token) = applied
        && let Ok(v) = token.parse()
    {
        res.headers_mut().insert("preference-applied", v);
    }
    Ok(res)
}

/// The headers every write response carries: content type always,
/// Preference-Applied when any Prefer token was honored.
fn write_headers(prefer: &Prefer, res: &mut Response) {
    if !prefer.applied.is_empty() {
        let joined = prefer.applied.join(", ");
        if let Ok(v) = joined.parse() {
            res.headers_mut().insert("preference-applied", v);
        }
    }
}

async fn write(
    app: &App,
    table: &str,
    auth: &AuthContext,
    req: &Parts,
    bytes: &[u8],
) -> Result<Response, RestError> {
    let method = &req.method;
    let prefer = parse_prefer(&req.headers);
    let (mut q, extras) = parse_query(table, req.uri.query())?;

    // Root filters belong to the mutation's WHERE. A condition whose
    // field reaches into an embed, or anything routed at one, stays
    // with the representation select, where the planner will accept
    // or refuse it.
    let mut root: Vec<Node> = Vec::new();
    q.filters.retain(|(route, node)| {
        let embedded = matches!(node, Node::Cond(c) if !c.field.embed.is_empty());
        if route.is_empty() && !embedded {
            root.push(node.clone());
            return false;
        }
        true
    });

    let payload = match *method {
        Method::POST => Some(insert_payload(bytes, extras.columns.as_deref())?),
        Method::PATCH => Some(update_payload(bytes)?),
        _ => None,
    };

    // A PATCH that carries no keys touches nothing: PostgREST's
    // no-op answer, no database round trip.
    if *method == Method::PATCH && payload.as_ref().is_some_and(|(c, _)| c.is_empty()) {
        let mut res = if prefer.ret == Ret::Representation {
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
                "[]",
            )
                .into_response()
        } else {
            StatusCode::NO_CONTENT.into_response()
        };
        write_headers(&prefer, &mut res);
        return Ok(res);
    }

    let pool = app.pool.as_ref().ok_or_else(no_database)?;
    let authed = auth.role != "anon";
    let ctx = request_context(auth, req);
    let sess = pool
        .session(&ctx, false)
        .await
        .map_err(|e| pg_error(&e, authed))?;

    // The primary key, only fetched when the upsert needs a default
    // target or the Location header wants the columns.
    let wants_location = *method == Method::POST && prefer.ret == Ret::HeadersOnly;
    let needs_pk = wants_location
        || (*method == Method::POST && prefer.merge.is_some() && extras.on_conflict.is_none());
    let pk: Vec<String> = if needs_pk {
        sess.query(PK_SQL, &[&SCHEMA, &table])
            .await
            .map_err(|e| pg_error(&e, authed))?
            .iter()
            .map(|r| r.get(0))
            .collect()
    } else {
        Vec::new()
    };

    let conflict = match (*method == Method::POST, prefer.merge) {
        (true, Some(merge)) => {
            let target = extras.on_conflict.clone().unwrap_or_else(|| pk.clone());
            if target.is_empty() {
                return Err(bad_grammar(
                    "an upsert needs a primary key or on_conflict columns",
                ));
            }
            Some(if merge {
                Conflict::Merge {
                    target,
                    set: payload.as_ref().map(|(c, _)| c.clone()).unwrap_or_default(),
                }
            } else {
                Conflict::Ignore { target }
            })
        }
        _ => None,
    };

    let returning = match prefer.ret {
        Ret::Representation => Returning::Star,
        // Location only makes sense for an insert; headers-only on
        // an update or delete degrades to minimal like PostgREST.
        Ret::HeadersOnly if wants_location => Returning::Cols(pk.clone()),
        Ret::HeadersOnly | Ret::Minimal => Returning::None,
    };

    let m = match *method {
        Method::POST => {
            let (cols, body) = payload.expect("post parsed a payload");
            mutate::insert(table, &cols, body, conflict.as_ref(), &returning)
        }
        Method::PATCH => {
            let (cols, body) = payload.expect("patch parsed a payload");
            mutate::update(table, &cols, body, &root, &returning)
        }
        Method::DELETE => mutate::delete(table, &root, &returning),
        _ => unreachable!("the dispatcher only sends writes here"),
    }
    .map_err(|e| bad_grammar(e.message))?;

    let created = *method == Method::POST;
    let affected: u64;
    let mut res = match prefer.ret {
        Ret::Representation => {
            let catalog = load_catalog(&sess, authed).await?;
            let r = mutate::representation(&catalog, m, &mut q).map_err(plan_error)?;
            let text = format!(
                "with {} select to_jsonb(\"_zou_row\")::text from ({}) as \"_zou_row\"",
                r.cte, r.select.text
            );
            let params: Vec<Text> = r.select.params.into_iter().map(Text).collect();
            let rows = sess
                .query(&text, &param_refs(&params))
                .await
                .map_err(|e| pg_error(&e, authed))?;
            sess.commit().await.map_err(|e| pg_error(&e, authed))?;
            affected = rows.len() as u64;
            (
                if created {
                    StatusCode::CREATED
                } else {
                    StatusCode::OK
                },
                [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
                json_array(&rows),
            )
                .into_response()
        }
        Ret::HeadersOnly if wants_location && !pk.is_empty() => {
            // The returned key rides out through a CTE as jsonb text,
            // no type juggling, and a single row becomes Location.
            let text = format!(
                "with \"{src}\" as ({}) select to_jsonb(\"_zou_row\")::text from \"{src}\" as \"_zou_row\"",
                m.text,
                src = mutate::SOURCE,
            );
            let params: Vec<Text> = m.params.into_iter().map(Text).collect();
            let rows = sess
                .query(&text, &param_refs(&params))
                .await
                .map_err(|e| pg_error(&e, authed))?;
            sess.commit().await.map_err(|e| pg_error(&e, authed))?;
            affected = rows.len() as u64;
            let mut res = StatusCode::CREATED.into_response();
            if rows.len() == 1
                && let Ok(row) =
                    serde_json::from_str::<serde_json::Value>(&rows[0].get::<_, String>(0))
            {
                let mut pairs = Vec::new();
                for col in &pk {
                    let v = match &row[col.as_str()] {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    pairs.push(format!("{col}=eq.{v}"));
                }
                let location = format!("/rest/v1/{table}?{}", pairs.join("&"));
                if let Ok(v) = location.parse() {
                    res.headers_mut().insert(header::LOCATION, v);
                }
            }
            res
        }
        _ => {
            let params: Vec<Text> = m.params.into_iter().map(Text).collect();
            affected = sess
                .execute(&m.text, &param_refs(&params))
                .await
                .map_err(|e| pg_error(&e, authed))?;
            sess.commit().await.map_err(|e| pg_error(&e, authed))?;
            if created {
                StatusCode::CREATED.into_response()
            } else {
                StatusCode::NO_CONTENT.into_response()
            }
        }
    };
    // PostgREST's write Content-Range: an update shows the window it
    // touched from zero, insert and delete collapse the window, and
    // the total is the affected count only when count= asked.
    let total =
        matches!(prefer.count, Some(Count::Exact | Count::Estimated)).then_some(affected as i64);
    let window = if *method == Method::PATCH {
        content_range(0, affected as usize, total)
    } else {
        content_range(1, 0, total)
    };
    if let Ok(v) = window.parse() {
        res.headers_mut().insert(header::CONTENT_RANGE, v);
    }
    write_headers(&prefer, &mut res);
    Ok(res)
}

/// The /rest/v1/{table} handler. Reads and writes are live, anything
/// else answers the honest 501.
pub async fn table(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::extract::Path(table): axum::extract::Path<String>,
    axum::Extension(auth): axum::Extension<AuthContext>,
    req: Request<Body>,
) -> Response {
    let method = req.method().clone();
    // Holding the unsync Body across an await would unsend the
    // future, so it is consumed or dropped before any other await.
    let (parts, body) = req.into_parts();
    let result = match method {
        Method::GET | Method::HEAD => read(&app, &table, &auth, &parts).await,
        Method::POST | Method::PATCH | Method::DELETE => {
            match axum::body::to_bytes(body, BODY_LIMIT).await {
                Ok(bytes) => write(&app, &table, &auth, &parts, &bytes).await,
                Err(_) => Err(RestError {
                    status: StatusCode::PAYLOAD_TOO_LARGE,
                    code: "PGRST102".to_string(),
                    message: "The request body is too large".to_string(),
                    details: None,
                    hint: None,
                }),
            }
        }
        _ => return crate::not_yet("this REST method"),
    };
    match result {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

fn rpc_error(e: rpc::RpcError) -> RestError {
    RestError {
        status: match e.code {
            "PGRST202" => StatusCode::NOT_FOUND,
            "PGRST203" => StatusCode::MULTIPLE_CHOICES,
            _ => StatusCode::BAD_REQUEST,
        },
        code: e.code.to_string(),
        message: e.message,
        details: e.details,
        hint: None,
    }
}

async fn invoke(
    app: &App,
    func: &str,
    auth: &AuthContext,
    req: &Parts,
    body: Option<&[u8]>,
) -> Result<Response, RestError> {
    let is_post = body.is_some();
    let pool = app.pool.as_ref().ok_or_else(no_database)?;
    let authed = auth.role != "anon";
    let ctx = request_context(auth, req);
    // GET and HEAD run read only, so a volatile function fails with
    // pg's 25006, which the status table maps to PostgREST's 405.
    let sess = pool
        .session(&ctx, !is_post)
        .await
        .map_err(|e| pg_error(&e, authed))?;

    let rows = sess
        .query(rpc::INTROSPECT_SQL, &[&SCHEMA, &func])
        .await
        .map_err(|e| pg_error(&e, authed))?;
    let overloads: Vec<rpc::Routine> = rows
        .iter()
        .map(|r| {
            rpc::routine(rpc::RoutineRow {
                arg_names: r.get(0),
                arg_types: r.get(1),
                arg_variadic: r.get(2),
                defaults: r.get(3),
                returns_set: r.get(4),
                volatile: r.get(5),
                rettype: r.get(6),
                return_table: r.get(7),
            })
        })
        .collect();

    // Which query pairs are arguments: on a GET any key naming an
    // argument of some overload, everything else stays with the
    // result grammar. On a POST the body keys are the arguments and
    // the whole query string is grammar.
    let raw = req.uri.query().unwrap_or("");
    let mut get_args: Vec<(String, String)> = Vec::new();
    let mut residual: Vec<&str> = Vec::new();
    let mut supplied: Vec<String> = Vec::new();
    if is_post {
        residual = raw.split('&').filter(|p| !p.is_empty()).collect();
    } else {
        for pair in raw.split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let key = decode(k);
            // With no overloads at all everything non reserved
            // counts as a supplied name, so the PGRST202 message
            // spells the call the client attempted.
            let named = if overloads.is_empty() {
                !key.contains('.')
                    && !matches!(
                        key.as_str(),
                        "select" | "order" | "limit" | "offset" | "apikey"
                    )
            } else {
                overloads
                    .iter()
                    .any(|r| r.args.iter().any(|a| !a.name.is_empty() && a.name == key))
            };
            if named {
                if !supplied.contains(&key) {
                    supplied.push(key.clone());
                }
                get_args.push((key, decode(v)));
            } else {
                residual.push(pair);
            }
        }
    }
    let payload = match body {
        Some(bytes) => {
            let text = if bytes.is_empty() {
                "{}".to_string()
            } else {
                String::from_utf8(bytes.to_vec())
                    .map_err(|_| invalid_body("invalid utf-8 in the request body"))?
            };
            let v: serde_json::Value =
                serde_json::from_str(&text).map_err(|e| invalid_body(e.to_string()))?;
            if let Some(o) = v.as_object() {
                supplied = o.keys().cloned().collect();
            }
            Some(text)
        }
        None => None,
    };

    let choice = rpc::choose(SCHEMA, func, &overloads, &supplied, is_post).map_err(rpc_error)?;
    let kind = choice.routine.kind.clone();
    let returns_set = choice.routine.returns_set;
    let m = match payload {
        Some(text) => rpc::call_json(SCHEMA, func, &choice, &supplied, text),
        None => rpc::call_get(SCHEMA, func, choice.routine, &get_args),
    };

    match kind {
        rpc::RetKind::Void => {
            let params: Vec<Text> = m.params.into_iter().map(Text).collect();
            sess.execute(&m.text, &param_refs(&params))
                .await
                .map_err(|e| pg_error(&e, authed))?;
            sess.commit().await.map_err(|e| pg_error(&e, authed))?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        rpc::RetKind::Scalar => {
            let wrapped = rpc::scalar_wrap(func, m, returns_set);
            let params: Vec<Text> = wrapped.params.into_iter().map(Text).collect();
            let rows = sess
                .query(&wrapped.text, &param_refs(&params))
                .await
                .map_err(|e| pg_error(&e, authed))?;
            sess.commit().await.map_err(|e| pg_error(&e, authed))?;
            let out = rows
                .first()
                .and_then(|r| r.get::<_, Option<String>>(0))
                .unwrap_or_else(|| if returns_set { "[]" } else { "null" }.to_string());
            Ok((
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
                out,
            )
                .into_response())
        }
        rpc::RetKind::Composite { table } => {
            // Rows go through the planner over the call's CTE, so
            // the whole select grammar applies, and when the return
            // type is a real table's rowtype embeds resolve on it.
            let logical = table.unwrap_or_else(|| func.to_string());
            let joined = residual.join("&");
            let (mut q, _) = parse_query(
                &logical,
                if joined.is_empty() {
                    None
                } else {
                    Some(&joined)
                },
            )?;
            apply_range(&mut q, &req.headers);
            let catalog = load_catalog(&sess, authed).await?;
            let r = rpc::representation(&catalog, m, &mut q).map_err(plan_error)?;
            let text = format!(
                "with {} select to_jsonb(\"_zou_row\")::text from ({}) as \"_zou_row\"",
                r.cte, r.select.text
            );
            let params: Vec<Text> = r.select.params.into_iter().map(Text).collect();
            let rows = sess
                .query(&text, &param_refs(&params))
                .await
                .map_err(|e| pg_error(&e, authed))?;
            sess.commit().await.map_err(|e| pg_error(&e, authed))?;
            if returns_set {
                let offset = q
                    .offset
                    .iter()
                    .find(|(route, _)| route.is_empty())
                    .map(|(_, n)| *n)
                    .unwrap_or(0);
                Ok((
                    StatusCode::OK,
                    [
                        (header::CONTENT_TYPE, "application/json; charset=utf-8"),
                        (
                            header::CONTENT_RANGE,
                            &content_range(offset, rows.len(), None),
                        ),
                    ],
                    json_array(&rows),
                )
                    .into_response())
            } else {
                // A non set function is one row, and PostgREST hands
                // it back as a bare object, not a one element array.
                let out = rows
                    .first()
                    .map(|r| r.get::<_, String>(0))
                    .unwrap_or_else(|| "null".to_string());
                Ok((
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
                    out,
                )
                    .into_response())
            }
        }
    }
}

/// The /rest/v1/rpc/{func} handler. GET, HEAD, and POST call the
/// function, anything else is PostgREST's PGRST101.
pub async fn rpc(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::extract::Path(func): axum::extract::Path<String>,
    axum::Extension(auth): axum::Extension<AuthContext>,
    req: Request<Body>,
) -> Response {
    let method = req.method().clone();
    let (parts, body) = req.into_parts();
    let result = match method {
        Method::GET | Method::HEAD => invoke(&app, &func, &auth, &parts, None).await,
        Method::POST => match axum::body::to_bytes(body, BODY_LIMIT).await {
            Ok(bytes) => invoke(&app, &func, &auth, &parts, Some(&bytes)).await,
            Err(_) => Err(RestError {
                status: StatusCode::PAYLOAD_TOO_LARGE,
                code: "PGRST102".to_string(),
                message: "The request body is too large".to_string(),
                details: None,
                hint: None,
            }),
        },
        m => Err(RestError {
            status: StatusCode::METHOD_NOT_ALLOWED,
            code: "PGRST101".to_string(),
            message: format!("Cannot use the {m} method on RPC"),
            details: None,
            hint: None,
        }),
    };
    match result {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bare_table_defaults_to_select_star() {
        let (q, _) = parse_query("todos", None).unwrap();
        assert_eq!(zou_rest::select::render(&q.select), "*");
        assert!(q.filters.is_empty());
    }

    #[test]
    fn reserved_words_route_and_everything_else_filters() {
        let (q, _) = parse_query(
            "todos",
            Some("select=id,orders(total)&orders.order=total.desc&orders.limit=2&status=eq.done&or=(id.eq.1,id.eq.2)&apikey=xyz"),
        )
        .unwrap();
        assert_eq!(q.order.len(), 1);
        assert_eq!(q.order[0].0, vec!["orders".to_string()]);
        assert_eq!(q.limit, vec![(vec!["orders".to_string()], 2)]);
        assert_eq!(q.filters.len(), 2, "the apikey pair is the gate's");
        assert!(q.filters.iter().all(|(route, _)| route.is_empty()));
    }

    #[test]
    fn a_percent_encoded_value_survives_the_split() {
        let (q, _) = parse_query("t", Some("name=eq.a%26b+c")).unwrap();
        match &q.filters[0].1 {
            Node::Cond(c) => match &c.value {
                filter::Value::Lit(v) => assert_eq!(v, "a&b c"),
                other => panic!("expected a literal, got {other:?}"),
            },
            other => panic!("expected a condition, got {other:?}"),
        }
    }

    #[test]
    fn a_broken_filter_is_pgrst100() {
        let e = parse_query("t", Some("name=zzz.1")).unwrap_err();
        assert_eq!(e.status, StatusCode::BAD_REQUEST);
        assert_eq!(e.code, "PGRST100");
    }

    #[test]
    fn a_quoted_column_named_order_stays_a_filter() {
        let (q, _) = parse_query("t", Some("%22my.order%22=eq.1")).unwrap();
        assert!(q.order.is_empty());
        assert_eq!(q.filters.len(), 1);
    }

    #[test]
    fn the_range_header_pages_the_root_unless_params_already_did() {
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, "5-9".parse().unwrap());

        let (mut q, _) = parse_query("t", None).unwrap();
        apply_range(&mut q, &headers);
        assert_eq!(q.offset, vec![(Vec::new(), 5)]);
        assert_eq!(q.limit, vec![(Vec::new(), 5)]);

        let (mut q, _) = parse_query("t", Some("limit=1")).unwrap();
        apply_range(&mut q, &headers);
        assert_eq!(q.limit, vec![(Vec::new(), 1)]);
        assert!(q.offset.is_empty());
    }

    #[test]
    fn the_sqlstate_table_matches_postgrest() {
        let cases = [
            ("23505", "", false, 409),
            ("23503", "", false, 409),
            ("23502", "", false, 400),
            ("42P01", "", false, 404),
            ("42883", "", false, 404),
            ("42501", "", false, 401),
            ("42501", "", true, 403),
            ("42601", "", false, 400),
            ("P0001", "", false, 400),
            ("P0002", "", false, 500),
            ("PT302", "", false, 302),
            ("PTxyz", "", false, 500),
            ("08006", "", false, 503),
            ("53300", "", false, 503),
            ("53400", "", false, 500),
            ("57P01", "", false, 503),
            ("28000", "", false, 403),
            ("22012", "", false, 400),
            ("XX000", "", false, 500),
            ("21000", "UPDATE requires a WHERE clause", false, 400),
            ("21000", "more than one row returned", false, 500),
            ("22023", "role \"ghost\" does not exist", false, 401),
            ("22023", "unrecognized parameter", false, 400),
        ];
        for (code, message, authed, want) in cases {
            assert_eq!(
                status_for(code, message, authed).as_u16(),
                want,
                "sqlstate {code} authed {authed}"
            );
        }
    }

    #[test]
    fn content_range_speaks_postgrest() {
        assert_eq!(content_range(0, 0, None), "*/*");
        assert_eq!(content_range(0, 3, None), "0-2/*");
        assert_eq!(content_range(10, 5, None), "10-14/*");
        assert_eq!(content_range(0, 3, Some(7)), "0-2/7");
        assert_eq!(content_range(5, 0, Some(5)), "*/5");
        assert_eq!(content_range(0, 0, Some(0)), "*/0");
        // The write shapes: insert and delete collapse the window.
        assert_eq!(content_range(1, 0, Some(4)), "*/4");
        assert_eq!(content_range(0, 4, Some(4)), "0-3/4");
    }

    #[test]
    fn range_status_speaks_postgrest() {
        use axum::http::StatusCode;
        assert_eq!(range_status(0, 3, None), StatusCode::OK);
        assert_eq!(range_status(0, 7, Some(7)), StatusCode::OK);
        assert_eq!(range_status(0, 3, Some(7)), StatusCode::PARTIAL_CONTENT);
        assert_eq!(range_status(5, 0, Some(5)), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            range_status(6, 0, Some(5)),
            StatusCode::RANGE_NOT_SATISFIABLE
        );
        // limit=0 with rows in the table is a partial answer.
        assert_eq!(range_status(0, 0, Some(5)), StatusCode::PARTIAL_CONTENT);
    }

    #[test]
    fn the_embed_errors_keep_their_statuses() {
        use zou_rest::catalog::EmbedError;
        let none = plan_error(PlanError::Embed(EmbedError {
            code: "PGRST200",
            message: "no".to_string(),
            details: None,
            hint: None,
        }));
        assert_eq!(none.status, StatusCode::BAD_REQUEST);
        let many = plan_error(PlanError::Embed(EmbedError {
            code: "PGRST201",
            message: "which".to_string(),
            details: None,
            hint: None,
        }));
        assert_eq!(many.status, StatusCode::MULTIPLE_CHOICES);
        let unrouted = plan_error(PlanError::Other(
            "'orders' is not an embedded resource in this request".to_string(),
        ));
        assert_eq!(unrouted.code, "PGRST108");
    }

    #[test]
    fn prefer_tokens_parse_and_the_rest_pass_through() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "prefer",
            "return=representation, resolution=merge-duplicates, count=exact"
                .parse()
                .unwrap(),
        );
        let p = parse_prefer(&headers);
        assert_eq!(p.ret, Ret::Representation);
        assert_eq!(p.merge, Some(true));
        assert_eq!(p.count, Some(Count::Exact));
        assert_eq!(
            p.applied,
            vec![
                "return=representation",
                "resolution=merge-duplicates",
                "count=exact"
            ]
        );

        let p = parse_prefer(&HeaderMap::new());
        assert_eq!(p.ret, Ret::Minimal);
        assert_eq!(p.merge, None);
        assert!(p.applied.is_empty());
    }

    #[test]
    fn insert_bodies_normalize_to_an_array_and_a_column_union() {
        let (cols, payload) = insert_payload(br#"{"a":1,"b":2}"#, None).unwrap();
        assert_eq!(cols, vec!["a", "b"]);
        assert_eq!(payload, r#"[{"a":1,"b":2}]"#);

        let (cols, _) = insert_payload(br#"[{"a":1},{"b":2,"a":3}]"#, None).unwrap();
        assert_eq!(cols, vec!["a", "b"], "the union keeps first sight order");

        let narrowing = vec!["a".to_string()];
        let (cols, _) = insert_payload(br#"[{"a":1,"b":2}]"#, Some(&narrowing)).unwrap();
        assert_eq!(cols, vec!["a"], "the columns parameter narrows");

        assert_eq!(insert_payload(b"", None).unwrap_err().code, "PGRST102");
        assert_eq!(insert_payload(b"42", None).unwrap_err().code, "PGRST102");
        assert_eq!(insert_payload(b"[1,2]", None).unwrap_err().code, "PGRST102");
    }

    #[test]
    fn update_bodies_are_one_object() {
        let (cols, payload) = update_payload(br#"{"title":"x"}"#).unwrap();
        assert_eq!(cols, vec!["title"]);
        assert_eq!(payload, r#"{"title":"x"}"#);
        assert_eq!(update_payload(b"[]").unwrap_err().code, "PGRST102");
    }

    #[test]
    fn write_parameters_come_out_of_the_query_string() {
        let (_, extras) =
            parse_query("t", Some("on_conflict=id,tenant&columns=a,b&select=*")).unwrap();
        assert_eq!(
            extras.on_conflict,
            Some(vec!["id".to_string(), "tenant".to_string()])
        );
        assert_eq!(extras.columns, Some(vec!["a".to_string(), "b".to_string()]));
    }
}
