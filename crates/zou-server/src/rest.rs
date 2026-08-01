//! The REST read path: GET and HEAD on /rest/v1/{table}.
//!
//! A request's query string becomes a [`zou_rest::plan::Query`], the
//! relationship catalog loads through INTROSPECT_SQL on the same
//! transaction the query will run in, the planner emits one
//! statement, and the rows come back as jsonb text the handler joins
//! into the response array. Everything runs inside a read only
//! transaction carrying the full request context, so RLS policies
//! see role, request.jwt.claims, request.headers, request.cookies,
//! request.method, and request.path exactly like they do behind
//! Supabase.
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
use zou_rest::plan::{self, PlanError, Query};
use zou_rest::{order, page, select};

use crate::sql::RequestContext;
use crate::{App, AuthContext, json_body};

/// The one schema the REST surface serves until Accept-Profile
/// lands, the same default a fresh Supabase project exposes.
const SCHEMA: &str = "public";

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

/// One query string into the planner's input. Reserved words are
/// select, order, limit, and offset, the last three optionally
/// behind an embed route, apikey is the gate's and skipped, and
/// every other key is a filter. A quoted column named "order" stays
/// a filter because the quote survives into the key.
fn parse_query(table: &str, raw: Option<&str>) -> Result<Query, RestError> {
    let mut q = Query {
        table: table.to_string(),
        ..Query::default()
    };
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
    Ok(q)
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

/// PostgREST's Content-Range for an uncounted read: the served
/// window when rows came back, */* when none did. Exact and
/// estimated totals arrive with the counts work.
fn content_range(offset: u64, rows: usize) -> String {
    if rows == 0 {
        "*/*".to_string()
    } else {
        format!("{}-{}/*", offset, offset + rows as u64 - 1)
    }
}

async fn read(
    app: &App,
    table: &str,
    auth: &AuthContext,
    req: &Parts,
) -> Result<Response, RestError> {
    let mut q = parse_query(table, req.uri.query())?;
    apply_range(&mut q, &req.headers);

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
    let rows = sess
        .query(INTROSPECT_SQL, &[&SCHEMA])
        .await
        .map_err(|e| pg_error(&e, authed))?;
    let catalog = Catalog::new(
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
    );

    let sql = plan::plan(&catalog, &q).map_err(plan_error)?;
    let wrapped = format!(
        "select to_jsonb(\"_zou_row\")::text from ({}) as \"_zou_row\"",
        sql.text
    );
    let params: Vec<Text> = sql.params.into_iter().map(Text).collect();
    let refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p as _).collect();
    let rows = sess
        .query(&wrapped, &refs)
        .await
        .map_err(|e| pg_error(&e, authed))?;
    sess.commit().await.map_err(|e| pg_error(&e, authed))?;

    let offset = q
        .offset
        .iter()
        .find(|(r, _)| r.is_empty())
        .map(|(_, n)| *n)
        .unwrap_or(0);
    let mut body = String::from("[");
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            body.push(',');
        }
        body.push_str(&row.get::<_, String>(0));
    }
    body.push(']');

    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/json; charset=utf-8"),
            (header::CONTENT_RANGE, &content_range(offset, rows.len())),
        ],
        body,
    )
        .into_response())
}

/// The /rest/v1/{table} handler. Reads are live, everything else
/// answers the honest 501 until the mutations work lands.
pub async fn table(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::extract::Path(table): axum::extract::Path<String>,
    axum::Extension(auth): axum::Extension<AuthContext>,
    req: Request<Body>,
) -> Response {
    if req.method() != Method::GET && req.method() != Method::HEAD {
        return crate::not_yet("this REST method");
    }
    // The body plays no part in a read, and holding the unsync Body
    // across an await would unsend the future, so only the parts
    // travel.
    let (parts, _body) = req.into_parts();
    match read(&app, &table, &auth, &parts).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bare_table_defaults_to_select_star() {
        let q = parse_query("todos", None).unwrap();
        assert_eq!(zou_rest::select::render(&q.select), "*");
        assert!(q.filters.is_empty());
    }

    #[test]
    fn reserved_words_route_and_everything_else_filters() {
        let q = parse_query(
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
        let q = parse_query("t", Some("name=eq.a%26b+c")).unwrap();
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
        let q = parse_query("t", Some("%22my.order%22=eq.1")).unwrap();
        assert!(q.order.is_empty());
        assert_eq!(q.filters.len(), 1);
    }

    #[test]
    fn the_range_header_pages_the_root_unless_params_already_did() {
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, "5-9".parse().unwrap());

        let mut q = parse_query("t", None).unwrap();
        apply_range(&mut q, &headers);
        assert_eq!(q.offset, vec![(Vec::new(), 5)]);
        assert_eq!(q.limit, vec![(Vec::new(), 5)]);

        let mut q = parse_query("t", Some("limit=1")).unwrap();
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
        assert_eq!(content_range(0, 0), "*/*");
        assert_eq!(content_range(0, 3), "0-2/*");
        assert_eq!(content_range(10, 5), "10-14/*");
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
}
