//! The REST table surface: GET, HEAD, POST, PATCH, and DELETE on
//! /rest/v1/{table}, and function calls on /rest/v1/rpc/{fn}.
//!
//! A request's query string becomes a [`zou_rest::plan::Query`], the
//! relationship catalog loads through INTROSPECT_SQL on the same
//! transaction the query will run in, the planner emits one
//! statement, and the rows come back as json text the handler joins
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
use std::sync::atomic::Ordering;

use axum::body::Body;
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use tokio_postgres::types::{Format, IsNull, ToSql, Type, to_sql_checked};
use zou_rest::catalog::{
    COLUMNS_SQL, COMPUTED_SQL, Catalog, Column, ColumnRow, ComputedRow, Details, FkRow,
    INTROSPECT_SQL, KEYS_SQL, RELATIONS_SQL,
};
use zou_rest::filter::{self, Error as GrammarError, Failure, Node, Op, Parsed};
use zou_rest::media::MediaType;
use zou_rest::mutate::{self, Conflict, Missing, Returning};
use zou_rest::origin::{self, KeyRow, PkRow, VIEW_KEYS_SQL, VIEW_PKS_SQL, VIEWS_SQL, ViewRow};
use zou_rest::plan::{self, PlanError, Query};
use zou_rest::{csv as csvbody, media, order, page, rpc, select};

use crate::sql::{RequestContext, Session};
use crate::{App, AuthContext, Config, openapi};

/// PostgREST's getSchema: writes pick their schema through
/// Content-Profile, everything else through Accept-Profile, a header
/// naming an unexposed schema is the PGRST106 406, and no header
/// means the first exposed schema. The bool says whether the
/// response echoes Content-Profile back, which happens when a header
/// chose and also headerless when more than one schema is exposed,
/// because the default then counts as negotiated.
fn profile<'a>(
    schemas: &'a [String],
    method: &Method,
    headers: &HeaderMap,
) -> Result<(&'a str, bool), RestError> {
    let name = match *method {
        Method::POST | Method::PATCH | Method::PUT | Method::DELETE => "content-profile",
        _ => "accept-profile",
    };
    match headers.get(name).and_then(|v| v.to_str().ok()) {
        Some(p) => match schemas.iter().find(|s| s.as_str() == p) {
            Some(s) => Ok((s.as_str(), true)),
            None => Err(RestError {
                status: StatusCode::NOT_ACCEPTABLE,
                code: "PGRST106".to_string(),
                message: format!("Invalid schema: {p}"),
                details: None,
                hint: Some(format!(
                    "Only the following schemas are exposed: {}",
                    schemas.join(", ")
                )),
                headers: None,
            }),
        },
        None => Ok((schemas[0].as_str(), schemas.len() != 1)),
    }
}

/// The Content-Profile response header rides wherever a Content-Type
/// does once a profile negotiated the schema, upstream's
/// profileHeader inside contentTypeHeaders.
fn profile_header(res: &mut Response, schema: &str, negotiated: bool) {
    if negotiated && let Ok(v) = schema.parse() {
        res.headers_mut().insert("content-profile", v);
    }
}

/// The most body a write accepts, 16 MiB like a generous PostgREST
/// deployment; past it the response is 413.
const BODY_LIMIT: usize = 1 << 24;

/// A PostgREST shaped error: a status and the four body keys, with
/// details and hint rendered as json null when absent, which is what
/// supabase-js expects to destructure.
///
/// The details are a json value rather than a string because one
/// error's are not a sentence: an ambiguous embed answers with the
/// list of relationships it found, an object each.
#[derive(Debug)]
pub struct RestError {
    pub status: StatusCode,
    pub code: String,
    pub message: String,
    pub details: Option<serde_json::Value>,
    pub hint: Option<String>,
    /// Headers the answer carries. Absent for every refusal this server
    /// decided on its own, and only ever set by a function that raised
    /// its own response with `RAISE SQLSTATE 'PGRST'`, which is allowed
    /// to name headers because it is naming the whole answer. Boxed
    /// because this error is the Err of nearly every function on this
    /// surface and it is not worth two dozen bytes on all of them for
    /// the one case that fills it.
    pub headers: Option<Box<Vec<(String, String)>>>,
}

/// The details of an embed or rpc error as the body carries them.
fn detail_json(details: Details) -> serde_json::Value {
    match details {
        Details::Text(s) => serde_json::Value::String(s),
        Details::Rels(rels) => rels
            .into_iter()
            .map(|r| {
                serde_json::json!({
                    "cardinality": r.cardinality,
                    "embedding": r.embedding,
                    "relationship": r.relationship,
                })
            })
            .collect(),
    }
}

impl RestError {
    fn into_response(self) -> Response {
        let status = self.status;
        let mut res = error_body(
            status,
            serde_json::json!({
                "code": self.code,
                "details": self.details,
                "hint": self.hint,
                "message": self.message,
            }),
        );
        // A 401 has to say what it wants instead, or it is a refusal
        // with no way back in. Upstream sends the bare challenge for
        // everything a row level security policy turned away.
        if status == StatusCode::UNAUTHORIZED {
            res.headers_mut()
                .insert(header::WWW_AUTHENTICATE, "Bearer".parse().expect("a token"));
        }
        // Whatever the function that wrote this answer asked to be sent
        // with it. A name or a value http will not hold is dropped
        // rather than fatal: the rest of the answer is still the answer.
        for (name, value) in self.headers.map(|h| *h).unwrap_or_default() {
            if let (Ok(name), Ok(value)) = (
                header::HeaderName::try_from(&name),
                header::HeaderValue::try_from(&value),
            ) {
                res.headers_mut().insert(name, value);
            }
        }
        res
    }
}

/// What this surface says about a bearer token it will not take.
///
/// Not the one line the edge says about the same token. Upstream's
/// gateway passes a bearer through without reading it, so the refusal a
/// client sees on `/rest/v1` is PostgREST's own: a code, a challenge
/// naming the reason, and the charset every answer of its carries.
///
/// The mapping was measured against 16.1 rather than read off the
/// source. The three claims about time are PGRST303 and the message is
/// the claim that failed, and everything about the signature is
/// PGRST301 under one message with the detail line saying which way it
/// failed. A key that decoded nothing is "none of the keys" and a token
/// no key could even be chosen for is "no suitable key was found",
/// which is the same sentence twice until you have both in front of
/// you.
pub fn token_refused(why: &crate::jwt::Reject, token: &str) -> Response {
    use crate::jwt::Reject;
    let (code, message, details) = match why {
        Reject::Expired | Reject::TooEarly | Reject::IssuedLater => {
            ("PGRST303", why.as_str().to_string(), None)
        }
        Reject::BadSignature => (
            "PGRST301",
            "No suitable key or wrong key type".to_string(),
            Some("None of the keys was able to decode the JWT"),
        ),
        // A header naming an algorithm this server does not sign with,
        // and a kid naming a key it does not have, are the same thing
        // from here: there was no key to try. Only the first of the two
        // was probed, since the reference has no key set configured in
        // the run that recorded these.
        Reject::WrongAlgorithm(_) | Reject::UnknownKey => (
            "PGRST301",
            "No suitable key or wrong key type".to_string(),
            Some("No suitable key was found to decode the JWT"),
        ),
        // Two shapes under the one variant here, and the reference tells
        // them apart: a bearer that is not three parts is counted and
        // named as such before anything is decoded, and three parts that
        // do not decode are the operation failing. A client that sent a
        // session id where a token goes gets the first, which is the one
        // worth being told plainly.
        Reject::Malformed => match token.split('.').count() {
            3 => (
                "PGRST301",
                "JWT cryptographic operation failed".to_string(),
                None,
            ),
            parts => (
                "PGRST301",
                format!("Expected 3 parts in JWT; got {parts}"),
                None,
            ),
        },
    };
    let mut res = error_body(
        StatusCode::UNAUTHORIZED,
        serde_json::json!({
            "code": code,
            "details": details,
            "hint": serde_json::Value::Null,
            "message": message,
        }),
    );
    // The challenge carries the reason where the bare one this surface
    // sends for a policy refusal does not, which is upstream saying the
    // difference between "you may not" and "this token is no good".
    if let Ok(value) = header::HeaderValue::try_from(format!(
        "Bearer error=\"invalid_token\", error_description=\"{message}\""
    )) {
        res.headers_mut().insert(header::WWW_AUTHENTICATE, value);
    }
    res
}

/// An error on the REST surface. PostgREST names the charset on every
/// answer it sends, including the ones that went wrong, and GoTrue does
/// not, so this is not the same builder the auth surface uses.
fn error_body(status: StatusCode, body: serde_json::Value) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
        body.to_string(),
    )
        .into_response()
}

/// What a url under the rest prefix that names no route gets.
///
/// The router already turns away a path with the wrong number of
/// segments, and a path that only looks like it has the right number:
/// `/rest/v1tsearch_to_tsvector` is not the table `tsearch_to_tsvector`
/// under the prefix, it is a path that happens to start with the same
/// letters. Either way the surface it missed is this one, so the answer
/// is this surface's, in this surface's shape, rather than the
/// gateway's line about no route matching.
pub(crate) fn invalid_path() -> Response {
    RestError {
        status: StatusCode::NOT_FOUND,
        code: "PGRST125".to_string(),
        message: "Invalid path specified in request URL".to_string(),
        details: None,
        hint: None,
        headers: None,
    }
    .into_response()
}

/// A parameter the grammar could not read, in the two halves
/// upstream answers with.
///
/// The message is parsec's source position, which names the parameter
/// and quotes the value it was given, and the details are parsec's
/// own: the token that stopped it and the set of things that would
/// have fit there instead. The set is the useful half, so it is the
/// one the parsers carry.
fn bad_grammar(f: &Failure) -> RestError {
    let at = f.error.at.min(f.input.len());
    let column = f.input[..at].chars().count() + 1 + f.skew;
    RestError {
        status: StatusCode::BAD_REQUEST,
        code: "PGRST100".to_string(),
        message: format!(
            "\"failed to parse {} ({})\" (line 1, column {column})",
            f.what, f.input
        ),
        details: Some(f.error.to_string().into()),
        hint: None,
        headers: None,
    }
}

/// The same for one of the parameters that is read on its own, where
/// the whole value is what the parser was handed.
fn failed(what: &'static str, input: &str) -> impl Fn(GrammarError) -> RestError {
    let input = input.to_string();
    move |error| {
        bad_grammar(&Failure {
            what,
            input: input.clone(),
            skew: 0,
            error,
        })
    }
}

/// A request the grammar read and the planner refused, which carries
/// a sentence rather than a position.
fn bad_request(message: impl Into<String>) -> RestError {
    RestError {
        status: StatusCode::BAD_REQUEST,
        code: "PGRST100".to_string(),
        message: message.into(),
        details: None,
        hint: None,
        headers: None,
    }
}

/// A window that ends before it starts. It is the same code and the
/// same status as an offset past the last row, because upstream reads
/// both of them the same way: a range the request asked for and the
/// table cannot answer.
fn negative_limit() -> RestError {
    RestError {
        status: StatusCode::RANGE_NOT_SATISFIABLE,
        code: "PGRST103".to_string(),
        message: "Requested range not satisfiable".to_string(),
        details: Some("Limit should be greater than or equal to zero.".into()),
        hint: None,
        headers: None,
    }
}

fn no_database() -> RestError {
    RestError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "PGRST000".to_string(),
        message: "Database connection error. Retrying the connection.".to_string(),
        details: None,
        hint: None,
        headers: None,
    }
}

/// A body that is not the json a write needs, PostgREST's PGRST102.
/// Upstream says the same sentence for every way a body can be
/// wrong, so the parser's own account of it stays out of the
/// response and goes nowhere near a client.
fn invalid_body(message: impl Into<String>) -> RestError {
    RestError {
        status: StatusCode::BAD_REQUEST,
        code: "PGRST102".to_string(),
        message: message.into(),
        details: None,
        hint: None,
        headers: None,
    }
}

/// Preferences the request asked for that nobody has, refused
/// because it also asked to be told, PostgREST's PGRST122. The
/// details name them in the order the header listed them.
fn invalid_prefs(tokens: &[String]) -> RestError {
    RestError {
        status: StatusCode::BAD_REQUEST,
        code: "PGRST122".to_string(),
        message: "Invalid preferences given with handling=strict".to_string(),
        details: Some(format!("Invalid preferences: {}", tokens.join(", ")).into()),
        hint: None,
        headers: None,
    }
}

/// A mutation that touched more rows than max-affected allowed,
/// PostgREST's PGRST124. The rows are already written when this is
/// raised and the transaction is what takes them back, so nothing
/// reaches this without a rollback behind it.
fn too_many_affected(rows: u64) -> RestError {
    RestError {
        status: StatusCode::BAD_REQUEST,
        code: "PGRST124".to_string(),
        message: "Query result exceeds max-affected preference constraint".to_string(),
        details: Some(format!("The query affects {rows} rows").into()),
        hint: None,
        headers: None,
    }
}

/// max-affected asked of a function that does not return rows,
/// PostgREST's PGRST128. There is nothing to count, and counting the
/// call itself as one row would let a function that deleted a
/// thousand of them pass.
fn not_set_returning() -> RestError {
    RestError {
        status: StatusCode::BAD_REQUEST,
        code: "PGRST128".to_string(),
        message: "Function must return SETOF or TABLE when max-affected preference is used with \
                  handling=strict"
            .to_string(),
        details: None,
        hint: None,
        headers: None,
    }
}

/// What a function said about the response it wanted, left behind in
/// the transaction it ran in.
///
/// This is the settings half of what a function can say about its own
/// response. The other half is `RAISE SQLSTATE 'PGRST'`, which
/// replaces the response outright and gives up the transaction to do
/// it; these two amend a response the handler built and let the work
/// stand.
#[derive(Debug, Default)]
struct Gucs {
    headers: Option<String>,
    status: Option<String>,
}

/// Both settings, read on the commit. They are transaction local, so
/// they have to be read before it, and upstream selects them beside
/// the body in the one statement it builds. zou's statements are
/// shaped by the route rather than by one template, so they ride out
/// on the commit instead, which is a round trip that was being spent
/// anyway. `nullif` is upstream's too: a setting reset to the empty
/// string is a setting nobody made.
const GUCS_SQL: &str = "select nullif(current_setting('response.headers', true), ''), \
                        nullif(current_setting('response.status', true), '')";

impl Gucs {
    /// What `GUCS_SQL` came back with, in the order it selected them.
    fn read(columns: Vec<Option<String>>) -> Gucs {
        let mut columns = columns.into_iter();
        Gucs {
            headers: columns.next().flatten(),
            status: columns.next().flatten(),
        }
    }
}

/// A response.headers that is not a list of one key objects,
/// PostgREST's PGRST111. Upstream decodes the setting into a list of
/// its GucHeader, whose parser takes an object of exactly one key
/// holding a string and nothing else, so every way of being wrong
/// gets this one sentence.
fn bad_guc_headers() -> RestError {
    RestError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        code: "PGRST111".to_string(),
        message: "response.headers guc must be a JSON array composed of objects with a single key \
                  and a string value"
            .to_string(),
        details: None,
        hint: None,
        headers: None,
    }
}

/// A response.status that is not a status code, PostgREST's PGRST112.
fn bad_guc_status() -> RestError {
    RestError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        code: "PGRST112".to_string(),
        message: "response.status guc must be a valid status code".to_string(),
        details: None,
        hint: None,
        headers: None,
    }
}

/// The status a function asked for. Upstream reads it with `decimal`
/// and keeps only the number, discarding whatever followed, so `205`
/// and `205 Reset Content` are the same answer and a word is no
/// answer at all.
fn guc_status(raw: &str) -> Result<StatusCode, RestError> {
    let digits: String = raw.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits
        .parse::<u16>()
        .ok()
        .and_then(|n| StatusCode::from_u16(n).ok())
        .ok_or_else(bad_guc_status)
}

/// The headers a function asked for, in the order it wrote them. Each
/// element is one header, which is what makes a name repeated across
/// elements a name sent twice.
fn guc_headers(raw: &str) -> Result<Vec<(HeaderName, HeaderValue)>, RestError> {
    let parsed: serde_json::Value = serde_json::from_str(raw).map_err(|_| bad_guc_headers())?;
    let list = parsed.as_array().ok_or_else(bad_guc_headers)?;
    let mut out = Vec::new();
    for item in list {
        let object = item
            .as_object()
            .filter(|o| o.len() == 1)
            .ok_or_else(bad_guc_headers)?;
        let (name, value) = object.iter().next().expect("one key");
        let text = value.as_str().ok_or_else(bad_guc_headers)?;
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| bad_guc_headers())?;
        let value = HeaderValue::from_str(text).map_err(|_| bad_guc_headers())?;
        out.push((name, value));
    }
    Ok(out)
}

/// Upstream's addHeadersIfNotIncluded, read from the other end: a
/// header the function set stands, and the one the handler computed
/// under that name goes. Everything the function did not mention is
/// left where it was, so a status of its own does not cost it the
/// Content-Range.
///
/// The status is decoded first, which is upstream's order and decides
/// which of the two errors a response that got both wrong answers
/// with.
fn override_response(gucs: &Gucs, res: &mut Response) -> Result<(), RestError> {
    let status = gucs.status.as_deref().map(guc_status).transpose()?;
    let headers = match gucs.headers.as_deref() {
        Some(raw) => guc_headers(raw)?,
        None => Vec::new(),
    };
    if let Some(status) = status {
        *res.status_mut() = status;
    }
    for (name, _) in &headers {
        res.headers_mut().remove(name);
    }
    for (name, value) in headers {
        res.headers_mut().append(name, value);
    }
    Ok(())
}

/// The table the schema does not have, PostgREST's PGRST205. It is a
/// 404 and not a 400 because the url named a resource and there is
/// no such resource, and the message names the schema as well as the
/// table, since the same name in another profile may well exist and
/// that is the likelier mistake.
///
/// This is the answer instead of postgres's own 42P01 because the
/// statement is never written: the check is a lookup in a list zou
/// already holds, so a caller asking for a table nobody has costs a
/// round trip to nothing.
fn no_table(catalog: &Catalog, schema: &str, table: &str) -> RestError {
    RestError {
        status: StatusCode::NOT_FOUND,
        code: "PGRST205".to_string(),
        message: format!("Could not find the table '{schema}.{table}' in the schema cache"),
        details: None,
        hint: catalog
            .nearest(table)
            .map(|near| format!("Perhaps you meant the table '{schema}.{near}'")),
        headers: None,
    }
}

/// A column a write named that the table does not have, PostgREST's
/// PGRST204. Unlike a column in `select=` or `order=`, which reaches
/// postgres and comes back as a 42703, a write's columns are known
/// before the statement is built and upstream refuses them there.
fn no_column(table: &str, column: &str) -> RestError {
    RestError {
        status: StatusCode::BAD_REQUEST,
        code: "PGRST204".to_string(),
        message: format!("Could not find the '{column}' column of '{table}' in the schema cache"),
        details: None,
        hint: None,
        headers: None,
    }
}

/// The three ways a PUT is refused. PostgREST reads the url of a PUT
/// as naming one row, so paging it is meaningless, filtering it by
/// anything but the whole primary key names something else, and a
/// body carrying a different key names a second row.
fn put_paged() -> RestError {
    RestError {
        status: StatusCode::BAD_REQUEST,
        code: "PGRST114".to_string(),
        message: "limit/offset querystring parameters are not allowed for PUT".to_string(),
        details: None,
        hint: None,
        headers: None,
    }
}

fn put_filters() -> RestError {
    RestError {
        status: StatusCode::METHOD_NOT_ALLOWED,
        code: "PGRST105".to_string(),
        message: "Filters must include all and only primary key columns with 'eq' operators"
            .to_string(),
        details: None,
        hint: None,
        headers: None,
    }
}

fn put_mismatch() -> RestError {
    RestError {
        status: StatusCode::BAD_REQUEST,
        code: "PGRST115".to_string(),
        message: "Payload values do not match URL in primary key column(s)".to_string(),
        details: None,
        hint: None,
        headers: None,
    }
}

/// Whether the url's filters are exactly the primary key, each one an
/// unnegated `eq` with no quantifier, and no logic tree anywhere. A
/// table with no primary key fails this the moment it is asked, which
/// is the same answer PostgREST gives it.
fn keys_the_row(filters: &[Node], pk: &[String]) -> bool {
    if pk.is_empty() || filters.len() != pk.len() {
        return false;
    }
    let mut named: Vec<&str> = Vec::new();
    for node in filters {
        let Node::Cond(c) = node else { return false };
        if c.negated || c.op != Op::Eq || c.quant.is_some() || !c.field.path.is_empty() {
            return false;
        }
        named.push(&c.field.column);
    }
    pk.iter().all(|col| named.contains(&col.as_str()))
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
pub(crate) fn decode(s: &str) -> String {
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

/// Percent encoding the other way, for the query string zou writes
/// rather than the one it was sent: a `Location` header.
///
/// Unreserved is the url standard's own set, letters, digits and
/// `-_.~`, and everything else is a pair of hex digits. That is
/// narrow on purpose, and it is upstream's, whose `renderSimpleQuery`
/// encodes each side of each pair the same way. A space becomes %20
/// rather than a plus, since the plus is a decoding courtesy and not
/// something anything has to write.
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
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
    // Held back until the whole query string has been read, because a
    // limit and an offset on the same route are one window and it
    // takes both of them to say where that window is.
    let mut limits: Vec<(Vec<String>, i64)> = Vec::new();
    let mut offsets: Vec<(Vec<String>, i64)> = Vec::new();
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
                q.select = select::parse(&value).map_err(failed("select parameter", &value))?;
                selected = true;
            }
            "on_conflict" if prefix.is_empty() => {
                let names =
                    select::columns(&value).map_err(failed("on_conflict parameter", &value))?;
                extras.on_conflict = Some(names);
            }
            "columns" if prefix.is_empty() => {
                let names = select::columns(&value).map_err(failed("columns parameter", &value))?;
                extras.columns = Some(names);
            }
            "order" => {
                let terms = order::parse(&value).map_err(failed("order", &value))?;
                q.order.push((route_of(prefix), terms));
            }
            "limit" => {
                let n = page::parse_limit(&value).map_err(failed("limit", &value))?;
                limits.push((route_of(prefix), n));
            }
            "offset" => {
                let n = page::parse_offset(&value).map_err(failed("offset", &value))?;
                offsets.push((route_of(prefix), n));
            }
            _ => match filter::parse_pair(&key, &value).map_err(|f| bad_grammar(&f))? {
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
    window(&mut q, &limits, &offsets)?;
    Ok((q, extras))
}

/// The limits and offsets of one query string turned into the ones
/// the planner gets.
///
/// PostgREST does not carry a limit and an offset, it carries the
/// window they describe, `[offset, offset + limit - 1]`, and then
/// intersects it with everything from row zero on. That intersection
/// is the whole behaviour of a negative one of either. An offset
/// below zero is taken back to zero, and the rows it would have
/// skipped come off the limit, so `limit=5&offset=-1` is four rows
/// from the start. A window ending before it starts is a limit under
/// zero, which is refused as a range nobody can satisfy, and a window
/// ending exactly one before it starts is the way to ask for no rows
/// at all.
fn window(
    q: &mut Query,
    limits: &[(Vec<String>, i64)],
    offsets: &[(Vec<String>, i64)],
) -> Result<(), RestError> {
    for (route, limit) in limits {
        let offset = offsets
            .iter()
            .find(|(r, _)| r == route)
            .map(|(_, o)| *o)
            .unwrap_or(0);
        let first = offset.max(0);
        let last = offset.saturating_add(*limit).saturating_sub(1);
        let rows = last.saturating_sub(first).saturating_add(1);
        if rows < 0 {
            return Err(negative_limit());
        }
        q.limit.push((route.clone(), rows as u64));
    }
    for (route, offset) in offsets {
        // A negative offset is where the window would have started,
        // and it starts at zero instead, which is the same request as
        // one that named no offset at all.
        if *offset >= 0 {
            q.offset.push((route.clone(), *offset as u64));
        }
    }
    Ok(())
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

/// What an unrecognized preference costs. Strict refuses the whole
/// request, lenient carries on and says so.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Handling {
    Strict,
    Lenient,
}

/// The Prefer tokens the surface honors. `applied` keeps the
/// recognized ones in arrival order and `invalid` the rest, since
/// under handling=strict the refusal has to name them.
#[derive(Debug)]
struct Prefer {
    ret: Ret,
    /// Some(true) merges duplicates, Some(false) ignores them.
    merge: Option<bool>,
    count: Option<Count>,
    /// What a write puts in a column its body said nothing about.
    missing: Missing,
    handling: Option<Handling>,
    /// The timezone as the request spelled it. Nothing here decides
    /// whether postgres has one by that name: it is set, and postgres
    /// says.
    timezone: Option<String>,
    /// The most rows a mutation may touch. It only binds under
    /// handling=strict, which is upstream's rule and not a shortcut:
    /// asked for on its own the token is not applied at all.
    max_affected: Option<i64>,
    applied: Vec<String>,
    invalid: Vec<String>,
}

fn parse_prefer(headers: &HeaderMap) -> Prefer {
    let mut p = Prefer {
        ret: Ret::Minimal,
        merge: None,
        count: None,
        missing: Missing::Null,
        handling: None,
        timezone: None,
        max_affected: None,
        applied: Vec::new(),
        invalid: Vec::new(),
    };
    for value in headers.get_all("prefer") {
        let Ok(line) = value.to_str() else { continue };
        for item in line.split(',') {
            let item = item.trim();
            // A timezone is applied without being judged, because
            // postgres is the only thing that knows the list and it
            // says so itself. A max-affected that is not a number is
            // neither applied nor refused: upstream reads it with a
            // parser that gives up quietly, and a caller who wrote
            // max-affected=lots gets the request they would have got
            // without it.
            if let Some(tz) = item.strip_prefix("timezone=") {
                p.timezone = Some(tz.to_string());
                continue;
            }
            if let Some(n) = item.strip_prefix("max-affected=") {
                if let Ok(n) = n.parse::<i64>()
                    && n >= 0
                {
                    p.max_affected = Some(n);
                }
                continue;
            }
            let token = match item {
                "handling=strict" => {
                    p.handling = Some(Handling::Strict);
                    "handling=strict"
                }
                "handling=lenient" => {
                    p.handling = Some(Handling::Lenient);
                    "handling=lenient"
                }
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
                "missing=default" => {
                    p.missing = Missing::Default;
                    "missing=default"
                }
                "missing=null" => {
                    p.missing = Missing::Null;
                    "missing=null"
                }
                other => {
                    if !other.is_empty() && !p.invalid.iter().any(|t| t == other) {
                        p.invalid.push(other.to_string());
                    }
                    continue;
                }
            };
            if !p.applied.iter().any(|t| t == token) {
                p.applied.push(token.to_string());
            }
        }
    }
    p
}

impl Prefer {
    /// The preferences settled: a timezone is applied as written, and
    /// under handling=strict anything unrecognized refuses the
    /// request.
    ///
    /// A timezone is not one of the things that can be unrecognized
    /// here. Upstream used to check it against the names postgres has
    /// and call an unknown one an invalid preference, and stopped:
    /// from PostgREST 16 the name goes to `set_config` as written and
    /// what comes back for a name postgres does not have is postgres's
    /// own 22023, whatever the handling said. That is the better
    /// reading anyway, since a list of zone names read out of the
    /// server is a copy of something the server will consult again a
    /// moment later.
    fn check(&mut self) -> Result<(), RestError> {
        if let Some(tz) = &self.timezone {
            self.applied.push(format!("timezone={tz}"));
        }
        if self.handling == Some(Handling::Strict) && !self.invalid.is_empty() {
            return Err(invalid_prefs(&self.invalid));
        }
        Ok(())
    }

    /// Whether max-affected binds this request, which takes both the
    /// token and handling=strict.
    fn cap(&self) -> Option<i64> {
        match self.handling {
            Some(Handling::Strict) => self.max_affected,
            _ => None,
        }
    }
}

/// The response shape the Accept header negotiated. Plain json
/// covers application/json, */*, and the vendored array+json name,
/// which PostgREST folds into plain json unless nulls=stripped
/// rides along.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Media {
    Json {
        stripped: bool,
    },
    Single {
        stripped: bool,
    },
    Csv,
    /// A media type one function answers in, named by the domain it
    /// returns. The value goes out as its own text with nothing
    /// wrapped around it, which is the whole point: a function can
    /// hand back an xml document or a png rather than a json string
    /// holding one.
    Custom(String),
}

impl Media {
    fn content_type(&self) -> String {
        match self {
            Media::Json { stripped: false } => media::content_type(&MediaType::Json),
            Media::Json { stripped: true } => media::content_type(&MediaType::ArrayStripped),
            Media::Single { stripped } => media::content_type(&MediaType::Single {
                stripped: *stripped,
            }),
            Media::Csv => media::content_type(&MediaType::Csv),
            Media::Custom(name) => media::content_type(&media::decode(name)),
        }
    }

    fn is_single(&self) -> bool {
        matches!(self, Media::Single { .. })
    }

    fn stripped(&self) -> bool {
        matches!(
            self,
            Media::Json { stripped: true } | Media::Single { stripped: true }
        )
    }
}

/// One Accept item into its handler, or into the name the 406
/// message echoes, which is the name of what was asked for rather
/// than the string that asked: a request for
/// `application/vnd.pgrst.plan` is refused by its full name,
/// options and all.
fn decode_media(item: &str) -> Result<Media, String> {
    match media::decode(item) {
        MediaType::Json | MediaType::Any => Ok(Media::Json { stripped: false }),
        MediaType::ArrayStripped => Ok(Media::Json { stripped: true }),
        MediaType::Single { stripped } => Ok(Media::Single { stripped }),
        MediaType::Csv => Ok(Media::Csv),
        other => Err(media::mime(&other)),
    }
}

/// The Accept header the way wai reads it: every space stripped,
/// each item cut at ";q=" keeping the parameters before it, sorted by
/// quality then by semicolons minus stars. None means no Accept at
/// all, which every handler treats as its own default.
fn accept_items(headers: &HeaderMap) -> Option<Vec<String>> {
    let raw = headers.get(header::ACCEPT).and_then(|v| v.to_str().ok())?;
    let mut items: Vec<(String, f64)> = Vec::new();
    for part in raw.split(',') {
        let s: String = part.chars().filter(|c| *c != ' ').collect();
        let (mime, q) = match s.find(";q=") {
            Some(i) => {
                let tail = &s[i + 3..];
                let digits = &tail[..tail.find(';').unwrap_or(tail.len())];
                (s[..i].to_string(), digits.parse().unwrap_or(1.0))
            }
            None => (s, 1.0),
        };
        items.push((mime, q));
    }
    let spec = |m: &str| m.matches(';').count() as i64 - m.matches('*').count() as i64;
    items.sort_by(|a, b| {
        (b.1, spec(&b.0))
            .partial_cmp(&(a.1, spec(&a.0)))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Some(items.into_iter().map(|(mime, _)| mime).collect())
}

/// The PGRST107 406, which lists the items in the order they were
/// weighed.
fn unacceptable(offered: Vec<String>) -> RestError {
    RestError {
        status: StatusCode::NOT_ACCEPTABLE,
        code: "PGRST107".to_string(),
        message: format!(
            "None of these media types are available: {}",
            offered.join(", ")
        ),
        details: None,
        hint: None,
        headers: None,
    }
}

/// Content negotiation for a table: the first weighed item with a
/// handler wins.
fn negotiate(headers: &HeaderMap) -> Result<Media, RestError> {
    negotiate_call(headers, None)
}

/// Content negotiation for a call, which is a table's plus whatever
/// media type the function itself answers in.
///
/// The function's own handler is looked up first, so a function
/// returning a domain named `text/csv` writes the csv rather than
/// the csv aggregate. It is not looked up under `*/*` though: a
/// client that stated no preference gets json, and the only way to
/// the function's own type is to name it.
fn negotiate_call(headers: &HeaderMap, custom: Option<&str>) -> Result<Media, RestError> {
    let Some(items) = accept_items(headers) else {
        return Ok(Media::Json { stripped: false });
    };
    let mut offered: Vec<String> = Vec::new();
    for mime in &items {
        if let Some(name) = custom
            && media::decode(mime) == media::decode(name)
        {
            return Ok(Media::Custom(name.to_string()));
        }
        match decode_media(mime) {
            Ok(m) => return Ok(m),
            Err(name) => offered.push(name),
        }
    }
    Err(unacceptable(offered))
}

/// Content negotiation for the root, whose producible list is the
/// openapi type, plain json and the star. Anything else is the same
/// PGRST107 a table raises, which is why `/items` cannot ask for
/// openapi+json and the root cannot ask for csv.
fn negotiate_openapi(headers: &HeaderMap) -> Result<(), RestError> {
    let Some(items) = accept_items(headers) else {
        return Ok(());
    };
    let mut offered: Vec<String> = Vec::new();
    for mime in &items {
        let kind = media::decode(mime);
        match kind {
            MediaType::OpenApi | MediaType::Json | MediaType::Any => return Ok(()),
            _ => offered.push(media::mime(&kind)),
        }
    }
    Err(unacceptable(offered))
}

/// The PGRST116 refusal for a singular request that did not land on
/// exactly one row. Writes raise it before commit, so the mutation
/// rolls back the way PostgREST condemns the transaction.
fn not_single(rows: usize) -> RestError {
    RestError {
        status: StatusCode::NOT_ACCEPTABLE,
        code: "PGRST116".to_string(),
        message: "Cannot coerce the result to a single JSON object".to_string(),
        details: Some(format!("The result contains {rows} rows").into()),
        hint: None,
        headers: None,
    }
}

/// The per-row json expression, stripping nulls when the vendored
/// nulls=stripped parameter asked for it.
///
/// json and not jsonb, because jsonb is a parsed value and this is
/// the last step before the bytes go out: jsonb drops a duplicate
/// key, sorts the keys by length, and spaces out the colons, so a
/// row built with it answers a question about the value rather than
/// about what the caller sent. Embeds stay jsonb one level in, where
/// grouping needs an equality operator json does not have.
fn row_json(media: &Media) -> &'static str {
    if media.stripped() {
        "json_strip_nulls(to_json(\"_zou_row\"))"
    } else {
        "to_json(\"_zou_row\")"
    }
}

/// PostgREST's csv shaping, transcribed from asCsvF: the header is
/// the first row's json keys, each line is the row's record text
/// with the parens shaved off, and the row count rides in a second
/// column. An empty result comes out as a lone newline, same as
/// upstream. Callers prepend `with ... "_zou_source" as (...)`.
const CSV_AGG: &str = "select (select coalesce(string_agg(a.k, ','), '') from (select json_object_keys(r)::text as k from (select row_to_json(hh) as r from \"_zou_source\" as hh limit 1) s) a) || E'\\n' || coalesce(string_agg(substring(\"_zou_t\"::text, 2, length(\"_zou_t\"::text) - 2), E'\\n'), ''), count(*) from (select * from \"_zou_source\") as \"_zou_t\"";

/// What the body is written as, which is the Content-Type header and
/// nothing else: no sniffing, and a request that says nothing is
/// saying json.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Content {
    Json,
    Csv,
    Form,
    /// A body that is one value, which only a call can take and only
    /// into a single unnamed parameter.
    Value(rpc::Payload),
    /// A media type no body on this surface may be written in, held
    /// by its name for the refusal.
    Other(String),
}

impl Content {
    /// What the body said it was, which is what a refusal names.
    fn name(&self) -> String {
        let kind = match self {
            Content::Json => MediaType::Json,
            Content::Csv => MediaType::Csv,
            Content::Form => MediaType::Form,
            Content::Value(rpc::Payload::Xml) => MediaType::Xml,
            Content::Value(rpc::Payload::Bytes) => MediaType::Bytes,
            Content::Value(_) => MediaType::Text,
            Content::Other(name) => return name.clone(),
        };
        media::mime(&kind)
    }
}

fn content_of(headers: &HeaderMap) -> Content {
    let raw = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if raw.trim().is_empty() {
        return Content::Json;
    }
    match media::decode(raw) {
        MediaType::Json => Content::Json,
        MediaType::Csv => Content::Csv,
        MediaType::Form => Content::Form,
        MediaType::Text => Content::Value(rpc::Payload::Text),
        MediaType::Xml => Content::Value(rpc::Payload::Xml),
        MediaType::Bytes => Content::Value(rpc::Payload::Bytes),
        other => Content::Other(media::mime(&other)),
    }
}

/// What resolution makes of the body: which single unnamed parameter
/// it could pass to, if any.
fn payload_of(content: &Content) -> rpc::Payload {
    match content {
        Content::Json => rpc::Payload::Json,
        Content::Value(p) => *p,
        _ => rpc::Payload::Other,
    }
}

/// The PGRST102 for a body written in a media type nothing here
/// reads. It is the content type that is refused, not the body, so
/// nobody looks at the bytes.
fn unreadable(name: &str) -> RestError {
    invalid_body(format!("Content-Type not acceptable: {name}"))
}

/// A form body as its pairs, in the order they were written and with
/// repeats kept, which is what fills a variadic argument.
fn form_pairs(bytes: &[u8]) -> Vec<(String, String)> {
    String::from_utf8_lossy(bytes)
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (decode(k), decode(v)),
            None => (decode(p), String::new()),
        })
        .collect()
}

/// A csv body as the rows it carries, the header naming the keys.
fn csv_rows(bytes: &[u8]) -> Result<(Vec<String>, Vec<serde_json::Value>), RestError> {
    let table = csvbody::read(bytes).map_err(invalid_body)?;
    let rows: Vec<serde_json::Value> = table
        .rows
        .into_iter()
        .map(|row| {
            serde_json::Value::Object(
                table
                    .header
                    .iter()
                    .cloned()
                    .zip(row.into_iter().map(|f| match f {
                        Some(text) => serde_json::Value::String(text),
                        None => serde_json::Value::Null,
                    }))
                    .collect(),
            )
        })
        .collect();
    // A csv with nothing under its header writes no columns, the
    // same as the empty json array it amounts to.
    let cols = if rows.is_empty() {
        Vec::new()
    } else {
        table.header
    };
    Ok((cols, rows))
}

/// A write's body, read.
///
/// `raw` is the json text exactly as it arrived, and it is the thing
/// that goes on to postgres when there is one. `rows` is the parse of
/// it, which is only ever consulted for the column names and for the
/// checks the names need: re-encoding the parse would quietly drop a
/// duplicate key the sender wrote on purpose. A csv and a form have
/// no json text of their own, so they leave `raw` empty and the parse
/// is all there is.
#[derive(Debug)]
struct Written<'a> {
    cols: Vec<String>,
    rows: Vec<serde_json::Value>,
    raw: Option<&'a str>,
}

impl<'a> Written<'a> {
    /// A body zou built out of something that was not json.
    fn read(cols: Vec<String>, rows: Vec<serde_json::Value>) -> Self {
        Written {
            cols,
            rows,
            raw: None,
        }
    }

    /// A body that arrived as json, keeping the bytes it arrived as.
    fn sent(cols: Vec<String>, rows: Vec<serde_json::Value>, raw: &'a str) -> Self {
        Written {
            cols,
            rows,
            raw: Some(raw),
        }
    }
}

/// A write's body into the rows it carries.
///
/// A json body is one object counted as a single row, and anything
/// that is not an object carries no keys at all rather than being
/// refused, which is why a body of `42` inserts one row of defaults.
/// A csv is its records, a form is one row of text, and every other
/// content type is refused unread.
///
/// The columns are the first row's keys and every other row has to
/// agree with them, because upstream unpacks the whole array against
/// one column list and a row with different keys would silently lose
/// some. ?columns= says the list outright, and then the body is not
/// inspected at all: an array of numbers reaches postgres and fails
/// there, which is upstream's answer too. It only speaks for a json
/// body: a csv and a form name their own columns, and upstream lets
/// them.
fn body_rows<'a>(
    content: &Content,
    bytes: &'a [u8],
    columns: Option<&[String]>,
) -> Result<Written<'a>, RestError> {
    match content {
        Content::Json => {}
        Content::Csv => {
            let (cols, rows) = csv_rows(bytes)?;
            return Ok(Written::read(cols, rows));
        }
        Content::Form => {
            let mut row = serde_json::Map::new();
            for (k, v) in form_pairs(bytes) {
                row.insert(k, serde_json::Value::String(v));
            }
            let cols: Vec<String> = row.keys().cloned().collect();
            return Ok(Written::read(cols, vec![serde_json::Value::Object(row)]));
        }
        Content::Value(_) | Content::Other(_) => return Err(unreadable(&content.name())),
    }
    let text = std::str::from_utf8(bytes).map_err(|_| invalid_body("Empty or invalid json"))?;
    let v: serde_json::Value =
        serde_json::from_str(text).map_err(|_| invalid_body("Empty or invalid json"))?;
    let bulk = v.is_array();
    let rows = match v {
        serde_json::Value::Array(a) => a,
        other => vec![other],
    };
    if let Some(list) = columns {
        return Ok(Written::sent(list.to_vec(), rows, text));
    }
    let cols: Vec<String> = rows
        .first()
        .and_then(|row| row.as_object())
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default();
    // A lone object never disagrees with itself, and neither does a
    // scalar, so only an array is held to this.
    if bulk {
        for row in &rows {
            let Some(obj) = row.as_object() else {
                return Err(invalid_body("All object keys must match"));
            };
            if obj.len() != cols.len() || !cols.iter().all(|c| obj.contains_key(c)) {
                return Err(invalid_body("All object keys must match"));
            }
        }
    }
    Ok(Written::sent(cols, rows, text))
}

/// Whether a json body is an array, read off the text rather than
/// off the parse so that the text is what goes on. Upstream asks the
/// same question the same way, of the first byte that is not json's
/// insignificant whitespace.
fn sent_an_array(raw: &str) -> bool {
    raw.trim_start_matches([' ', '\t', '\n', '\r'])
        .starts_with('[')
}

/// An insert body into its column list and the payload it binds,
/// which is always a json array so that one shape unpacks it.
///
/// A json body goes on as the bytes that arrived rather than as
/// anything zou wrote back out, which is upstream's rule and is
/// observable: postgres keeps what a `json` column was sent, so
/// `{"a": 1, "a": 2}` is stored with both keys, and re-encoding the
/// parse would have dropped one of them. The parse here is only for
/// the column names and the check that the rows agree on them. One
/// object is an array of one, spelled by putting brackets around the
/// text rather than around the parse, for the same reason.
fn insert_payload(
    content: &Content,
    bytes: &[u8],
    columns: Option<&[String]>,
) -> Result<(Vec<String>, String), RestError> {
    let body = body_rows(content, bytes, columns)?;
    let text = match body.raw {
        Some(raw) if sent_an_array(raw) => raw.to_string(),
        Some(raw) => format!("[{raw}]"),
        None => serde_json::Value::Array(body.rows).to_string(),
    };
    Ok((body.cols, text))
}

/// An update body into its column list and payload. An update writes
/// one row, so an array of them is not refused, it is read down to
/// its first element the way upstream's LIMIT 1 reads it, and an
/// empty one leaves no columns to set at all.
///
/// The one object case is the body itself, for the reason
/// [`insert_payload`] gives, and so is the element an array is read
/// down to: it is cut out of the text rather than taken out of the
/// parse, so that a duplicate key and the spelling of a number survive
/// a PATCH the same way they survive a POST.
fn update_payload(
    content: &Content,
    bytes: &[u8],
    columns: Option<&[String]>,
) -> Result<(Vec<String>, String), RestError> {
    let body = body_rows(content, bytes, columns)?;
    if let Some(raw) = body.raw {
        if !sent_an_array(raw) {
            return Ok((body.cols, raw.to_string()));
        }
        return match first_element(raw) {
            Some(first) => Ok((body.cols, first.to_string())),
            None => Ok((Vec::new(), "null".to_string())),
        };
    }
    // A csv or a form, which zou built out of text of its own, so
    // there is no arrival to preserve.
    let Some(first) = body.rows.into_iter().next() else {
        return Ok((Vec::new(), "null".to_string()));
    };
    Ok((body.cols, first.to_string()))
}

/// The text of the first element of a json array, or nothing when the
/// array is empty.
///
/// Cut rather than re-encoded. A parse and a re-encode drops one of
/// `{"a": 1, "a": 2}`'s two keys and can respell a number, and both are
/// visible in the answer when the column is `json`, which keeps what it
/// was sent. The scan is a depth count that knows about strings and
/// their escapes, which is all it takes to find where one element ends:
/// the text has already parsed, so it does not have to be validated
/// again on the way past.
fn first_element(raw: &str) -> Option<&str> {
    const BLANK: [char; 4] = [' ', '\t', '\n', '\r'];
    let inside = raw.trim_start_matches(BLANK).strip_prefix('[')?;
    let start = inside.len() - inside.trim_start_matches(BLANK).len();
    let bytes = inside.as_bytes();
    let mut at = start;
    let mut depth = 0u32;
    let mut in_string = false;
    let mut escaped = false;
    while at < bytes.len() {
        match (in_string, escaped, bytes[at]) {
            (true, true, _) => escaped = false,
            (true, false, b'\\') => escaped = true,
            (true, false, b'"') => in_string = false,
            (true, false, _) => {}
            (false, _, b'"') => in_string = true,
            (false, _, b'[' | b'{') => depth += 1,
            // The end of this element, whether it is the end of the
            // array or only the comma before the next one.
            (false, _, b']' | b'}' | b',') if depth == 0 => break,
            (false, _, b']' | b'}') => depth -= 1,
            (false, _, _) => {}
        }
        at += 1;
    }
    let element = inside[start..at].trim_end_matches(BLANK);
    (!element.is_empty()).then_some(element)
}

/// The Range header as a root limit and offset, only when the query
/// string did not already page the root, and silently ignored when
/// it is not a parseable items range, both PostgREST's stance.
///
/// It is also only read on a GET. Upstream says so in one line, "the
/// Range header must be ignored for all methods other than GET", and
/// means the method literally: a HEAD carrying a range gets the whole
/// set, and so does a POST to a function.
fn apply_range(q: &mut Query, method: &Method, headers: &HeaderMap) {
    if method != Method::GET {
        return;
    }
    let already =
        q.limit.iter().any(|(r, _)| r.is_empty()) || q.offset.iter().any(|(r, _)| r.is_empty());
    if already {
        return;
    }
    let Some(range) = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(page::parse_range)
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
///
/// One SQLSTATE is not an error at all. `PGRST` is how a function
/// writes the whole answer itself, so it is taken apart rather than
/// reported.
fn pg_error(e: &tokio_postgres::Error, authed: bool) -> RestError {
    match e.as_db_error() {
        Some(db) if db.code().code() == "PGRST" => raised(db.message(), db.detail()),
        Some(db) => RestError {
            status: status_for(db.code().code(), db.message(), authed),
            code: db.code().code().to_string(),
            message: db.message().to_string(),
            details: db.detail().map(Into::into),
            hint: db.hint().map(str::to_string),
            headers: None,
        },
        None => RestError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "PGRST001".to_string(),
            message: "Database connection error. Retrying the connection.".to_string(),
            details: Some(e.to_string().into()),
            hint: None,
            headers: None,
        },
    }
}

/// A `RAISE SQLSTATE 'PGRST'` taken apart.
///
/// A function raising this is not reporting a failure, it is writing
/// the answer: the status, the headers and the body are all its own,
/// which is how a function answers with a redirect or a 402 without
/// this server knowing anything about either. MESSAGE is the body, the
/// same four keys every other error here has, and DETAIL is the
/// envelope. Both have to be json objects with the keys upstream
/// names, all of them strings but the status, and when either is not,
/// the answer is this server's 500 saying which half and what it
/// should have looked like: a function that meant to write an answer
/// and wrote nothing readable is a bug in the function rather than
/// something to pass on.
fn raised(message: &str, detail: Option<&str>) -> RestError {
    const BODY_HINT: &str = "MESSAGE must be a JSON object with obligatory keys: 'code', 'message' and optional keys: 'details', 'hint'.";
    const ENVELOPE_HINT: &str = "DETAIL must be a JSON object with obligatory keys: 'status', 'headers' and optional key: 'status_text'.";
    let unreadable = |details: String, hint: &str| RestError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        code: "PGRST121".to_string(),
        message: "Could not parse JSON in the \"RAISE SQLSTATE 'PGRST'\" error".to_string(),
        details: Some(details.into()),
        hint: Some(hint.to_string()),
        headers: None,
    };
    let bad_body = || {
        unreadable(
            format!("Invalid JSON value for MESSAGE: '{message}'"),
            BODY_HINT,
        )
    };
    let body = match serde_json::from_str::<serde_json::Value>(message) {
        Ok(serde_json::Value::Object(body)) => body,
        _ => return bad_body(),
    };
    // A key that has to be there and has to be a string, and one that
    // may be missing but may not be anything else. Absent and null are
    // the same absence, which is what the json library upstream reads
    // through does.
    let need = |map: &serde_json::Map<String, serde_json::Value>, key: &str| {
        map.get(key).and_then(|v| v.as_str()).map(str::to_string)
    };
    let may = |map: &serde_json::Map<String, serde_json::Value>, key: &str| match map.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(()),
    };
    let (Some(code), Some(text)) = (need(&body, "code"), need(&body, "message")) else {
        return bad_body();
    };
    let (Ok(details), Ok(hint)) = (may(&body, "details"), may(&body, "hint")) else {
        return bad_body();
    };
    let Some(detail) = detail else {
        return unreadable(
            "DETAIL is missing in the RAISE statement".to_string(),
            ENVELOPE_HINT,
        );
    };
    let bad_envelope = || {
        unreadable(
            format!("Invalid JSON value for DETAIL: '{detail}'"),
            ENVELOPE_HINT,
        )
    };
    let envelope = match serde_json::from_str::<serde_json::Value>(detail) {
        Ok(serde_json::Value::Object(envelope)) => envelope,
        _ => return bad_envelope(),
    };
    // status_text is read so that a DETAIL naming it wrongly is the
    // parse error it is upstream, and then dropped: the words on the
    // status line are hyper's, and nothing above this server picks
    // them.
    if may(&envelope, "status_text").is_err() {
        return bad_envelope();
    }
    let (Some(status), Some(serde_json::Value::Object(headers))) = (
        envelope.get("status").and_then(serde_json::Value::as_u64),
        envelope.get("headers"),
    ) else {
        return bad_envelope();
    };
    let mut named = Vec::new();
    for (name, value) in headers {
        match value.as_str() {
            Some(value) => named.push((name.clone(), value.to_string())),
            None => return bad_envelope(),
        }
    }
    RestError {
        // A status the http crate will not hold is not worth guessing
        // at, and a function that wrote one has the same bug as a
        // function that wrote no json.
        status: u16::try_from(status)
            .ok()
            .and_then(|n| StatusCode::from_u16(n).ok())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        code,
        message: text,
        details: details.map(Into::into),
        hint,
        headers: Some(Box::new(named)),
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
            details: e.details.map(detail_json),
            hint: e.hint,
            headers: None,
        },
        PlanError::Compile(c) => bad_request(c.message),
        PlanError::Other(m) => RestError {
            status: StatusCode::BAD_REQUEST,
            code: "PGRST100".to_string(),
            message: m,
            details: None,
            hint: None,
            headers: None,
        },
    }
}

/// The request identity and shape as the settings the session pool
/// injects, which is the whole per request contract RLS policies
/// read, plus the search_path scoping the transaction to the
/// negotiated schema.
///
/// Fallible for one reason: the role. A `role` claim naming a role
/// this server does not expose is refused here rather than passed to
/// `set_config`, because the connection role is whatever the dsn
/// names and a dev loop's dsn is usually the superuser, so a token
/// claiming `postgres` would otherwise be a superuser session. See
/// #92, and [`Config::exposes`] for what the set is.
fn request_context(
    cfg: &Config,
    auth: &AuthContext,
    req: &Parts,
    schema: &str,
) -> Result<RequestContext, RestError> {
    if !cfg.exposes(&auth.role) {
        return Err(RestError {
            status: StatusCode::UNAUTHORIZED,
            // The state postgres itself raises for a role it will not
            // enter, so a client that already handles that answer
            // handles this one.
            code: "22023".to_string(),
            message: format!("role \"{}\" is not exposed", auth.role),
            details: None,
            hint: Some(format!(
                "Only the following roles are exposed: {}",
                cfg.exposed().join(", ")
            )),
            headers: None,
        });
    }
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
    Ok(RequestContext {
        role: auth.role.clone(),
        claims: auth.claims.to_string(),
        method: req.method.as_str().to_string(),
        path: mounted_at(req.uri.path()),
        headers: serde_json::Value::Object(headers).to_string(),
        cookies: serde_json::Value::Object(cookies).to_string(),
        search_path: format!("\"{}\"", schema.replace('"', "\"\"")),
    })
}

/// The path as PostgREST would have seen it, which is the path with
/// this gateway's mount prefix taken off.
///
/// `request.path` is a setting a function reads, and what it is for
/// is telling the function which url it was called over. PostgREST
/// serves at the root, so the answer there is `/rpc/name`, and the
/// hosted Supabase edge strips `/rest/v1` before proxying, so the
/// answer there is the same. A path that is not under the prefix at
/// all is left alone rather than trimmed to nothing.
fn mounted_at(path: &str) -> String {
    path.strip_prefix(crate::REST_PREFIX)
        .unwrap_or(path)
        .to_string()
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

/// The fk graph and the relation list on the request's own
/// transaction, two catalog queries. They go together because they
/// are cached together and expire together: one epoch moved the
/// schema, and a graph that has been rebuilt against a table list
/// that has not is worse than either being stale.
async fn introspect(sess: &Session, authed: bool, schema: &str) -> Result<Catalog, RestError> {
    let rows = sess
        .query(INTROSPECT_SQL, &[&schema])
        .await
        .map_err(|e| pg_error(&e, authed))?;
    let mut fks: Vec<FkRow> = rows
        .iter()
        .map(|r| FkRow {
            constraint: r.get(0),
            table: r.get(1),
            columns: r.get(2),
            ref_table: r.get(3),
            ref_columns: r.get(4),
            unique: r.get(5),
            in_pk: r.get(6),
        })
        .collect();
    let (view_fks, views, mut keys) = view_keys(sess, authed, schema).await?;
    fks.extend(view_fks);
    let rows = sess
        .query(RELATIONS_SQL, &[&schema])
        .await
        .map_err(|e| pg_error(&e, authed))?;
    let names = rows.iter().map(|r| r.get(0)).collect();
    let rows = sess
        .query(KEYS_SQL, &[&schema])
        .await
        .map_err(|e| pg_error(&e, authed))?;
    keys.extend(rows.iter().map(|r| {
        (
            r.get(0),
            r.get::<_, Option<Vec<String>>>(1).unwrap_or_default(),
        )
    }));
    let rows = sess
        .query(COLUMNS_SQL, &[&schema])
        .await
        .map_err(|e| pg_error(&e, authed))?;
    let cols = rows
        .iter()
        .map(|r| ColumnRow {
            table: r.get(0),
            column: Column {
                name: r.get(1),
                to_json: r.get(2),
                from_text: r.get(3),
                from_json: r.get(4),
                type_name: r.get(5),
                base_type: r.get(6),
                default_expr: r.get(7),
            },
        })
        .collect();
    let rows = sess
        .query(COMPUTED_SQL, &[&schema])
        .await
        .map_err(|e| pg_error(&e, authed))?;
    let computed = rows
        .iter()
        .map(|r| ComputedRow {
            function: r.get(0),
            table: r.get(1),
            ftable: r.get(2),
            single: r.get(3),
        })
        .collect();
    Ok(Catalog::new(fks)
        .with_computed(computed)
        .with_relations(names, cols)
        .with_keys(keys)
        .with_views(views)
        .with_schema(schema))
}

/// The keys the schema's views inherit from the tables under them,
/// foreign and primary alike, and the names of the views
/// themselves, three more catalog queries on the same transaction.
///
/// A view has no key of its own, so both kinds have to be traced
/// back through the columns it selects. The queries reach outside
/// the exposed schema on purpose: the view underneath and the table
/// holding the key may be somewhere nobody can name, and the key is
/// still the view's from here. The names come back with them
/// because resolution treats a relationship to a view more narrowly
/// than one to a table and has to be able to tell them apart.
async fn view_keys(
    sess: &Session,
    authed: bool,
    schema: &str,
) -> Result<(Vec<FkRow>, Vec<String>, Vec<(String, Vec<String>)>), RestError> {
    let rows = sess
        .query(VIEWS_SQL, &[&schema])
        .await
        .map_err(|e| pg_error(&e, authed))?;
    let views: Vec<ViewRow> = rows
        .iter()
        .map(|r| ViewRow {
            oid: r.get(0),
            schema: r.get(1),
            name: r.get(2),
            attnums: r.get::<_, Option<Vec<i32>>>(3).unwrap_or_default(),
            columns: r.get::<_, Option<Vec<String>>>(4).unwrap_or_default(),
            tree: r.get(5),
        })
        .collect();
    // Only the ones a request can name: the rest were read for the
    // sake of the chain that runs through them.
    let names: Vec<String> = views
        .iter()
        .filter(|v| v.schema == schema)
        .map(|v| v.name.clone())
        .collect();
    if views.is_empty() {
        return Ok((Vec::new(), names, Vec::new()));
    }
    let rows = sess
        .query(VIEW_KEYS_SQL, &[&schema])
        .await
        .map_err(|e| pg_error(&e, authed))?;
    let keys: Vec<KeyRow> = rows
        .iter()
        .map(|r| KeyRow {
            constraint: r.get(0),
            schema: r.get(1),
            table: r.get(2),
            oid: r.get(3),
            attnums: r.get(4),
            columns: r.get(5),
            ref_schema: r.get(6),
            ref_table: r.get(7),
            ref_oid: r.get(8),
            ref_attnums: r.get(9),
            ref_columns: r.get(10),
            unique: r.get(11),
            in_pk: r.get(12),
        })
        .collect();
    let rows = sess
        .query(VIEW_PKS_SQL, &[&schema])
        .await
        .map_err(|e| pg_error(&e, authed))?;
    let pks: Vec<PkRow> = rows
        .iter()
        .map(|r| PkRow {
            oid: r.get(0),
            attnums: r.get::<_, Option<Vec<i32>>>(1).unwrap_or_default(),
        })
        .collect();
    Ok((
        origin::derive(schema, &views, &keys),
        names,
        origin::primary_keys(schema, &views, &pks),
    ))
}

/// The preferences settled and put to work: a strict request carrying
/// anything unrecognized is refused here, and a timezone is set for
/// the length of the transaction, which is also where a name postgres
/// does not have becomes a 22023.
///
/// Local like everything else the request injects, so the connection
/// goes back to the pool with the timezone it came out with.
async fn settle_prefs(prefer: &mut Prefer, sess: &Session, authed: bool) -> Result<(), RestError> {
    prefer.check()?;
    if let Some(tz) = &prefer.timezone {
        sess.query("select set_config('timezone', $1, true)", &[tz])
            .await
            .map_err(|e| pg_error(&e, authed))?;
    }
    Ok(())
}

/// The catalog for the profiled schema, introspected once per epoch
/// instead of once per request. PostgREST holds the same graph in its
/// schema cache and rebuilds it when told the schema changed; here the
/// telling is the DDL watch, and the epoch it moves is what a cached
/// entry is checked against.
///
/// Two requests racing a cold cache both introspect and both write,
/// which is a wasted query and never a wrong answer, so the lock is
/// never held across the query.
async fn load_catalog(
    app: &App,
    sess: &Session,
    authed: bool,
    schema: &str,
) -> Result<Arc<Catalog>, RestError> {
    // A router can be built outside a runtime, so the watch starts on
    // the first request that needs a catalog rather than at boot.
    app.watching
        .get_or_init(|| async {
            if let Some(pool) = &app.pool {
                pool.watch(Arc::clone(&app.epoch));
            }
        })
        .await;

    let epoch = app.epoch.load(Ordering::Relaxed);
    if let Some((at, cat)) = app.catalog.read().await.get(schema)
        && *at == epoch
    {
        return Ok(Arc::clone(cat));
    }
    let fresh = Arc::new(introspect(sess, authed, schema).await?);
    app.catalog
        .write()
        .await
        .insert(schema.to_string(), (epoch, Arc::clone(&fresh)));
    Ok(fresh)
}

/// Rows of json text joined into the response array by hand, which
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
    apply_range(&mut q, &req.method, &req.headers);
    let mut prefer = parse_prefer(&req.headers);

    let (schema, negotiated) = profile(&app.cfg.schemas, &req.method, &req.headers)?;
    let pool = app.pool.as_ref().ok_or_else(no_database)?;
    let authed = auth.role != app.cfg.anon_role;
    let ctx = request_context(&app.cfg, auth, req, schema)?;
    let sess = pool
        .session(&ctx, true)
        .await
        .map_err(|e| pg_error(&e, authed))?;

    // An early return past this point drops the session, which
    // forfeits the connection instead of pooling a dirty one, the
    // containment the pool promises.
    let catalog = load_catalog(app, &sess, authed, schema).await?;
    settle_prefs(&mut prefer, &sess, authed).await?;
    if catalog.relation(table).is_none() {
        return Err(no_table(&catalog, schema, table));
    }

    let sql = plan::plan(&catalog, &q).map_err(plan_error)?;
    let media = negotiate(&req.headers)?;
    let params: Vec<Text> = sql.params.into_iter().map(Text).collect();
    let (body, returned) = if media == Media::Csv {
        let text = format!("with \"_zou_source\" as ({}) {}", sql.text, CSV_AGG);
        let rows = sess
            .query(&text, &param_refs(&params))
            .await
            .map_err(|e| pg_error(&e, authed))?;
        (
            rows[0].get::<_, String>(0),
            rows[0].get::<_, i64>(1) as usize,
        )
    } else {
        let wrapped = format!(
            "select {}::text from ({}) as \"_zou_row\"",
            row_json(&media),
            sql.text
        );
        let rows = sess
            .query(&wrapped, &param_refs(&params))
            .await
            .map_err(|e| pg_error(&e, authed))?;
        if media.is_single() && rows.len() != 1 {
            return Err(not_single(rows.len()));
        }
        let body = if media.is_single() {
            rows[0].get::<_, String>(0)
        } else {
            json_array(&rows)
        };
        (body, rows.len())
    };

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
            Some(returned as i64)
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
    let gucs = Gucs::read(
        sess.commit_reading(GUCS_SQL)
            .await
            .map_err(|e| pg_error(&e, authed))?,
    );
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
    let status = range_status(offset, returned, total);
    let mut res = if status == StatusCode::RANGE_NOT_SATISFIABLE {
        error_body(status, out_of_bounds(offset, total.unwrap_or(0)))
    } else {
        (status, [(header::CONTENT_TYPE, media.content_type())], body).into_response()
    };
    if let Ok(v) = content_range(offset, returned, total).parse() {
        res.headers_mut().insert(header::CONTENT_RANGE, v);
    }
    // Upstream builds the read headers once, so even the 416 keeps
    // its Content-Profile.
    profile_header(&mut res, schema, negotiated);
    applied_header(&prefer, Surface::Read, &req.method, false, &mut res);
    override_response(&gucs, &mut res)?;
    Ok(res)
}

/// Which handler is answering. A preference is echoed only where it
/// is honored, and what honors what is not guessable from the method
/// alone: a POST to a table applies resolution and missing, a POST to
/// a function applies neither, and a PATCH applies missing but not
/// resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Surface {
    Read,
    Write,
    Rpc,
}

/// Preference-Applied, the tokens that were honored in the order
/// upstream writes them, which is its own and not the order the
/// request listed them.
///
/// `capped` says the request actually held itself to max-affected,
/// which takes handling=strict and a method that counts rows.
fn applied_header(
    prefer: &Prefer,
    surface: Surface,
    method: &Method,
    capped: bool,
    res: &mut Response,
) {
    let write = surface == Surface::Write;
    let cap = prefer
        .cap()
        .filter(|_| capped)
        .map(|n| format!("max-affected={n}"));
    let mut applied: Vec<&str> = prefer
        .applied
        .iter()
        .map(String::as_str)
        .filter(|t| match t.split('=').next().unwrap_or("") {
            "return" => write,
            "resolution" => write && matches!(*method, Method::POST | Method::PUT),
            // The missing preference is about reading a body, so a
            // method that has no body neither applies it nor says it
            // did, which a DELETE carrying the token shows.
            "missing" => write && matches!(*method, Method::POST | Method::PATCH),
            _ => true,
        })
        .collect();
    applied.extend(cap.as_deref());
    applied.sort_by_key(|t| rank(t));
    if applied.is_empty() {
        return;
    }
    if let Ok(v) = applied.join(", ").parse() {
        res.headers_mut().insert("preference-applied", v);
    }
}

/// Where a preference sits in Preference-Applied.
fn rank(token: &str) -> u8 {
    match token.split('=').next().unwrap_or("") {
        "resolution" => 0,
        "missing" => 1,
        "return" => 2,
        "count" => 3,
        "handling" => 4,
        "timezone" => 5,
        _ => 6,
    }
}

/// The affected count judged against max-affected. Over it the write
/// is refused, and since the rows are written by then the only thing
/// that takes them back is the transaction not landing, which is
/// upstream's ABORT where a COMMIT would have gone.
fn over_cap(cap: Option<i64>, affected: u64) -> Option<RestError> {
    match cap {
        Some(n) if affected as i64 > n => Some(too_many_affected(affected)),
        _ => None,
    }
}

/// The status a write carries, and the point a PUT's row count is
/// judged.
///
/// A creation is a row that was not there before, so both of the
/// statements that can find one already there ask what they wrote:
/// a POST resolving duplicates by merging them, and a PUT, which is
/// an upsert of one row. Everything else is a creation on its face.
/// The count is the net one the statement kept as it ran, up per row
/// written and down again per row the conflict clause turned into an
/// update, so nothing new leaves it at zero.
async fn written(
    sess: &Session,
    method: &Method,
    merge: bool,
    affected: u64,
    authed: bool,
) -> Result<StatusCode, RestError> {
    let put = *method == Method::PUT;
    if *method == Method::POST && !merge {
        return Ok(StatusCode::CREATED);
    }
    if !put && *method != Method::POST {
        return Ok(StatusCode::OK);
    }
    if put && affected != 1 {
        return Err(put_mismatch());
    }
    let rows = sess
        .query(mutate::INSERTED_SQL, &[])
        .await
        .map_err(|e| pg_error(&e, authed))?;
    Ok(if rows[0].get::<_, i32>(0) > 0 {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    })
}

async fn write(
    app: &App,
    table: &str,
    auth: &AuthContext,
    req: &Parts,
    bytes: &[u8],
) -> Result<Response, RestError> {
    let method = &req.method;
    let mut prefer = parse_prefer(&req.headers);
    // The schema check runs before the payload parse, upstream's
    // getSchema order, so PGRST106 beats a bad body.
    let (schema, negotiated) = profile(&app.cfg.schemas, method, &req.headers)?;
    let (mut q, extras) = parse_query(table, req.uri.query())?;

    // A PUT names one row, so a window over it is a contradiction and
    // upstream refuses it while it is still reading the request, ahead
    // of the body and ahead of the primary key.
    let put = *method == Method::PUT;
    if put
        && (q.limit.iter().any(|(r, _)| r.is_empty()) || q.offset.iter().any(|(r, _)| r.is_empty()))
    {
        return Err(put_paged());
    }

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

    // A body zou cannot read is refused before a connection is taken
    // out of the pool, which is where it stays: the schema cache
    // decides the rest and it needs a session to be filled.
    //
    // ?columns= is a POST and a PATCH parameter. A PUT reads its
    // columns off the body whatever the url says, which is worth
    // stating because it is not obviously deliberate upstream and it
    // is observable: a PUT with ?columns= naming one column writes
    // every column the body carries.
    let content = content_of(&req.headers);
    let payload = match *method {
        Method::POST => Some(insert_payload(&content, bytes, extras.columns.as_deref())?),
        Method::PUT => Some(insert_payload(&content, bytes, None)?),
        Method::PATCH => Some(update_payload(&content, bytes, extras.columns.as_deref())?),
        _ => None,
    };

    let media = negotiate(&req.headers)?;

    let pool = app.pool.as_ref().ok_or_else(no_database)?;
    let authed = auth.role != app.cfg.anon_role;
    let ctx = request_context(&app.cfg, auth, req, schema)?;
    let sess = pool
        .session(&ctx, false)
        .await
        .map_err(|e| pg_error(&e, authed))?;

    // The table the url named, before anything it might have said
    // about that table's columns. Everything downstream of here can
    // assume the relation exists.
    let catalog = load_catalog(app, &sess, authed, schema).await?;
    settle_prefs(&mut prefer, &sess, authed).await?;
    let Some(relation) = catalog.relation(table) else {
        return Err(no_table(&catalog, schema, table));
    };

    // What max-affected binds. An insert is not counted at all,
    // upstream's own reading: the rows a POST writes are the rows the
    // caller sent, and a PUT writes exactly the one the url named, so
    // the only counts worth capping are the ones the url decided.
    let cap = match *method {
        Method::PATCH | Method::DELETE => prefer.cap(),
        _ => None,
    };

    // The primary key, which an upsert with no target of its own
    // conflicts on, a PUT is filtered by, and a Location header is
    // written out of. It comes off the catalog rather than out of a
    // query here, so a view has one as surely as a table does.
    let wants_location = *method == Method::POST && prefer.ret == Ret::HeadersOnly;
    let pk: Vec<String> = relation.keys.clone();

    // What a PUT is allowed to be filtered by, which is the primary
    // key and nothing else. The answer is 405 rather than 400 because
    // upstream reads it as the url naming a set, and a set is not a
    // thing PUT knows how to replace.
    if put && !keys_the_row(&root, &pk) {
        return Err(put_filters());
    }

    // The write's own columns, which are ?columns= when the url said
    // so and the body's keys otherwise. A name the table does not
    // have is answered here rather than by postgres, which would
    // have said 42703 about a statement nobody meant to write. It
    // comes after the PUT check because a PUT the url has already
    // ruled out is not a write whose columns anybody is waiting on.
    if let Some((cols, _)) = &payload
        && let Some(missing) = cols.iter().find(|c| !relation.has(c))
    {
        return Err(no_column(table, missing));
    }

    // What the upsert conflicts on: the on_conflict parameter's
    // columns, or the primary key. A table with neither has nothing
    // to conflict on, which is not an error, it is an insert. Upstream
    // drops the clause and, since it did not resolve anything, also
    // drops the preference from the answer.
    let target = extras.on_conflict.clone().unwrap_or_else(|| pk.clone());
    if target.is_empty() {
        prefer.applied.retain(|t| !t.starts_with("resolution="));
    }
    let conflict = match (*method == Method::POST, prefer.merge) {
        (true, Some(true)) => Some(Conflict::Merge {
            target,
            set: payload.as_ref().map(|(c, _)| c.clone()).unwrap_or_default(),
        }),
        (true, Some(false)) => Some(Conflict::Ignore { target }),
        _ => None,
    };
    let merging = matches!(conflict, Some(Conflict::Merge { .. }));

    let returning = match prefer.ret {
        // Named columns rather than a star, because returning a
        // column is reading it and a role can be allowed to write a
        // row it is not allowed to read all of.
        Ret::Representation => mutate::needed(&catalog, table, &q.select, &pk),
        // Location only makes sense for an insert; headers-only on
        // an update or delete degrades to minimal like PostgREST.
        Ret::HeadersOnly if wants_location => Returning::Cols(pk.clone()),
        Ret::HeadersOnly | Ret::Minimal => Returning::None,
    };

    let m = match *method {
        Method::POST => {
            let (cols, body) = payload.expect("post parsed a payload");
            mutate::insert(
                table,
                Some(relation),
                &cols,
                body,
                prefer.missing,
                conflict.as_ref(),
                &returning,
            )
        }
        Method::PATCH => {
            let (cols, body) = payload.expect("patch parsed a payload");
            mutate::update(
                table,
                Some(relation),
                &cols,
                body,
                prefer.missing,
                &root,
                &returning,
            )
        }
        Method::PUT => {
            let (cols, body) = payload.expect("put parsed a payload");
            mutate::upsert_one(table, Some(relation), &cols, body, &pk, &root, &returning)
        }
        Method::DELETE => mutate::delete(table, Some(relation), &root, &returning),
        _ => unreachable!("the dispatcher only sends writes here"),
    }
    .map_err(|e| bad_request(e.message))?;

    let affected: u64;
    // Filled in by whichever arm commits, since each of them commits
    // at a different point and a setting lives only as long as the
    // transaction that made it.
    let gucs: Gucs;
    let mut res = match prefer.ret {
        Ret::Representation => {
            let r = mutate::representation(&catalog, m, &mut q).map_err(plan_error)?;
            let params: Vec<Text> = r.select.params.into_iter().map(Text).collect();
            if media == Media::Csv {
                let text = format!(
                    "with {}, \"_zou_source\" as ({}) {}",
                    r.cte, r.select.text, CSV_AGG
                );
                let rows = sess
                    .query(&text, &param_refs(&params))
                    .await
                    .map_err(|e| pg_error(&e, authed))?;
                affected = rows[0].get::<_, i64>(1) as u64;
                let status = written(&sess, method, merging, affected, authed).await?;
                if let Some(e) = over_cap(cap, affected) {
                    sess.rollback().await.map_err(|x| pg_error(&x, authed))?;
                    return Err(e);
                }
                gucs = Gucs::read(
                    sess.commit_reading(GUCS_SQL)
                        .await
                        .map_err(|e| pg_error(&e, authed))?,
                );
                (
                    status,
                    [(header::CONTENT_TYPE, media.content_type())],
                    rows[0].get::<_, String>(0),
                )
                    .into_response()
            } else {
                let text = format!(
                    "with {} select {}::text from ({}) as \"_zou_row\"",
                    r.cte,
                    row_json(&media),
                    r.select.text
                );
                let rows = sess
                    .query(&text, &param_refs(&params))
                    .await
                    .map_err(|e| pg_error(&e, authed))?;
                affected = rows.len() as u64;
                let status = written(&sess, method, merging, affected, authed).await?;
                if media.is_single() && rows.len() != 1 {
                    return Err(not_single(rows.len()));
                }
                if let Some(e) = over_cap(cap, affected) {
                    sess.rollback().await.map_err(|x| pg_error(&x, authed))?;
                    return Err(e);
                }
                gucs = Gucs::read(
                    sess.commit_reading(GUCS_SQL)
                        .await
                        .map_err(|e| pg_error(&e, authed))?,
                );
                let body = if media.is_single() {
                    rows[0].get::<_, String>(0)
                } else {
                    json_array(&rows)
                };
                (status, [(header::CONTENT_TYPE, media.content_type())], body).into_response()
            }
        }
        Ret::HeadersOnly if wants_location && !pk.is_empty() => {
            // The returned key rides out through a CTE as json text,
            // no type juggling, and a single row becomes Location.
            let text = format!(
                "with \"{src}\" as ({}) select to_json(\"_zou_row\")::text from \"{src}\" as \"_zou_row\"",
                m.text,
                src = mutate::SOURCE,
            );
            let params: Vec<Text> = m.params.into_iter().map(Text).collect();
            let rows = sess
                .query(&text, &param_refs(&params))
                .await
                .map_err(|e| pg_error(&e, authed))?;
            if media.is_single() && rows.len() != 1 {
                return Err(not_single(rows.len()));
            }
            affected = rows.len() as u64;
            // Before the commit, since the count the status turns on
            // is transaction local.
            let status = written(&sess, method, merging, affected, authed).await?;
            gucs = Gucs::read(
                sess.commit_reading(GUCS_SQL)
                    .await
                    .map_err(|e| pg_error(&e, authed))?,
            );
            let mut res = status.into_response();
            if rows.len() == 1
                && let Ok(row) =
                    serde_json::from_str::<serde_json::Value>(&rows[0].get::<_, String>(0))
            {
                let mut pairs = Vec::new();
                for col in &pk {
                    // Upstream reads the row with json_each_text and
                    // coalesces, so a key column that came back null
                    // is written as the filter that would find it
                    // again rather than as the word null.
                    let filter = match &row[col.as_str()] {
                        serde_json::Value::Null => "is.null".to_string(),
                        serde_json::Value::String(s) => format!("eq.{s}"),
                        other => format!("eq.{other}"),
                    };
                    pairs.push(format!("{}={}", escape(col), escape(&filter)));
                }
                // No mount prefix. PostgREST serves at the root and
                // writes the table's own name, and the prefix here
                // is this gateway's, which the hosted one strips
                // before the request ever arrives.
                let location = format!("/{table}?{}", pairs.join("&"));
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
            // A PUT that wrote no row still has to be judged, and it
            // answers 204 either way once it has been.
            if put && affected != 1 {
                return Err(put_mismatch());
            }
            if media.is_single() && affected != 1 {
                return Err(not_single(affected as usize));
            }
            if let Some(e) = over_cap(cap, affected) {
                sess.rollback().await.map_err(|x| pg_error(&x, authed))?;
                return Err(e);
            }
            // A minimal answer still says whether it created anything,
            // so the insert count is read here too, and before the
            // commit takes it away. A PUT is the one write that does
            // not: it answers 204 either way.
            let status = if *method == Method::POST {
                written(&sess, method, merging, affected, authed).await?
            } else {
                StatusCode::NO_CONTENT
            };
            gucs = Gucs::read(
                sess.commit_reading(GUCS_SQL)
                    .await
                    .map_err(|e| pg_error(&e, authed))?,
            );
            status.into_response()
        }
    };
    // PostgREST's write Content-Range: an update shows the window it
    // touched from zero, insert and delete collapse the window, and
    // the total is the affected count only when count= asked. A PUT
    // carries none, because the row it wrote is the row the url named
    // and a window over one named row says nothing.
    let total =
        matches!(prefer.count, Some(Count::Exact | Count::Estimated)).then_some(affected as i64);
    let window = if *method == Method::PATCH {
        content_range(0, affected as usize, total)
    } else {
        content_range(1, 0, total)
    };
    if !put && let Ok(v) = window.parse() {
        res.headers_mut().insert(header::CONTENT_RANGE, v);
    }
    // Content-Profile rides with Content-Type, so only the
    // representation arm carries it, never a 204 or the bare 201.
    if prefer.ret == Ret::Representation {
        profile_header(&mut res, schema, negotiated);
    }
    applied_header(&prefer, Surface::Write, method, cap.is_some(), &mut res);
    override_response(&gucs, &mut res)?;
    Ok(res)
}

/// What a table will take. The list is the same for every table
/// upstream serves, which is worth saying out loud: it is not a
/// permission check and a caller who reads it as one will be
/// surprised by the first 401. The only thing it answers is whether
/// the table is there, and that answer is the 404.
async fn options(
    app: &App,
    table: &str,
    auth: &AuthContext,
    req: &Parts,
) -> Result<Response, RestError> {
    let (schema, _) = profile(&app.cfg.schemas, &req.method, &req.headers)?;
    let pool = app.pool.as_ref().ok_or_else(no_database)?;
    let authed = auth.role != app.cfg.anon_role;
    let ctx = request_context(&app.cfg, auth, req, schema)?;
    let sess = pool
        .session(&ctx, true)
        .await
        .map_err(|e| pg_error(&e, authed))?;
    let catalog = load_catalog(app, &sess, authed, schema).await?;
    if catalog.relation(table).is_none() {
        return Err(no_table(&catalog, schema, table));
    }
    sess.commit().await.map_err(|e| pg_error(&e, authed))?;
    let mut res = StatusCode::OK.into_response();
    res.headers_mut().insert(
        header::ALLOW,
        header::HeaderValue::from_static("OPTIONS,GET,HEAD,POST,PUT,PATCH,DELETE"),
    );
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
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE => {
            match axum::body::to_bytes(body, BODY_LIMIT).await {
                Ok(bytes) => write(&app, &table, &auth, &parts, &bytes).await,
                Err(_) => Err(RestError {
                    status: StatusCode::PAYLOAD_TOO_LARGE,
                    code: "PGRST102".to_string(),
                    message: "The request body is too large".to_string(),
                    details: None,
                    hint: None,
                    headers: None,
                }),
            }
        }
        Method::OPTIONS => options(&app, &table, &auth, &parts).await,
        _ => return crate::not_yet("this REST method", crate::tracked::REST),
    };
    match result {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

/// The introspection behind the document, all of it on the request's
/// own transaction under the request's own role, which is what makes
/// the document show a caller only what that caller may reach.
async fn describe(app: &App, auth: &AuthContext, req: &Parts) -> Result<Response, RestError> {
    // getSchema runs before the plan upstream, so an unexposed
    // profile is the 406 even when the Accept is hopeless too.
    let (schema, negotiated) = profile(&app.cfg.schemas, &req.method, &req.headers)?;
    negotiate_openapi(&req.headers)?;

    let pool = app.pool.as_ref().ok_or_else(no_database)?;
    let authed = auth.role != app.cfg.anon_role;
    let ctx = request_context(&app.cfg, auth, req, schema)?;
    let sess = pool
        .session(&ctx, true)
        .await
        .map_err(|e| pg_error(&e, authed))?;

    let catalog = load_catalog(app, &sess, authed, schema).await?;
    let table_rows = sess
        .query(openapi::TABLES_SQL, &[&schema])
        .await
        .map_err(|e| pg_error(&e, authed))?;
    let column_rows = sess
        .query(openapi::COLUMNS_SQL, &[&schema])
        .await
        .map_err(|e| pg_error(&e, authed))?;
    let func_rows = sess
        .query(openapi::FUNCS_SQL, &[&schema])
        .await
        .map_err(|e| pg_error(&e, authed))?;
    let comment: Option<String> = sess
        .query(openapi::SCHEMA_SQL, &[&schema])
        .await
        .map_err(|e| pg_error(&e, authed))?
        .first()
        .and_then(|r| r.get(0));
    sess.commit().await.map_err(|e| pg_error(&e, authed))?;

    let mut tables: Vec<openapi::Table> = table_rows
        .iter()
        .map(|r| openapi::Table {
            name: r.get(0),
            description: r.get(1),
            insertable: r.get(2),
            updatable: r.get(3),
            deletable: r.get(4),
            pk: r.get(5),
            columns: Vec::new(),
        })
        .collect();
    // Both queries order by relation name, so one walk fills every
    // column list without a map in between.
    for row in &column_rows {
        let table: String = row.get(0);
        if let Some(t) = tables.iter_mut().find(|t| t.name == table) {
            t.columns.push(openapi::Column {
                name: row.get(1),
                description: row.get(2),
                nullable: row.get(3),
                data_type: row.get(4),
                max_len: row.get(5),
                default: row.get(6),
                enum_labels: row.get(7),
            });
        }
    }
    let funcs: Vec<openapi::Func> = func_rows
        .iter()
        .map(|r| {
            let names: Vec<String> = r.get(3);
            let types: Vec<String> = r.get(4);
            let required: Vec<bool> = r.get(5);
            let variadic: Vec<bool> = r.get(6);
            openapi::Func {
                name: r.get(0),
                description: r.get(1),
                volatile: r.get(2),
                params: names
                    .into_iter()
                    .zip(types)
                    .zip(required)
                    .zip(variadic)
                    .map(|(((name, type_name), required), variadic)| openapi::Param {
                        name,
                        type_name,
                        required,
                        variadic,
                    })
                    .collect(),
            }
        })
        .collect();

    // Upstream takes the host and the base path from its own config
    // and serves the document at the server root. Zou is mounted
    // under the Supabase prefix and has no such config, so the site
    // is read off the request instead: whatever name the client used
    // to get here is a name that works.
    let site = openapi::Site {
        scheme: req
            .headers
            .get("x-forwarded-proto")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("http")
            .to_string(),
        host: req
            .headers
            .get(header::HOST)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("localhost")
            .to_string(),
        base_path: "/rest/v1/".to_string(),
        version: openapi::version(),
    };
    let doc = openapi::document(&site, comment.as_ref(), &tables, &funcs, &catalog);

    let mut res = (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "application/openapi+json; charset=utf-8",
        )],
        doc.to_string(),
    )
        .into_response();
    profile_header(&mut res, schema, negotiated);
    Ok(res)
}

/// The /rest/v1/ handler, which is the OpenAPI document and nothing
/// else.
pub async fn root(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Extension(auth): axum::Extension<AuthContext>,
    req: Request<Body>,
) -> Response {
    let (parts, _body) = req.into_parts();
    match parts.method {
        Method::GET | Method::HEAD => match describe(&app, &auth, &parts).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        },
        _ => crate::not_yet("this REST method", crate::tracked::REST),
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
        details: e.details.map(Into::into),
        hint: e.hint,
        headers: None,
    }
}

/// Whether a query key belongs to the result grammar rather than to
/// the function's arguments. Upstream's list of reserved words, plus
/// the gate's apikey, which is nobody's argument.
fn grammar_key(key: &str) -> bool {
    let word = key.rsplit_once('.').map_or(key, |(_, w)| w);
    matches!(word, "order" | "limit" | "offset" | "and" | "or")
        || matches!(key, "select" | "columns" | "on_conflict" | "apikey")
}

async fn invoke(
    app: &App,
    func: &str,
    auth: &AuthContext,
    req: &Parts,
    body: Option<&[u8]>,
) -> Result<Response, RestError> {
    let is_post = body.is_some();
    let mut prefer = parse_prefer(&req.headers);
    let (schema, negotiated) = profile(&app.cfg.schemas, &req.method, &req.headers)?;
    let pool = app.pool.as_ref().ok_or_else(no_database)?;
    let authed = auth.role != app.cfg.anon_role;
    let ctx = request_context(&app.cfg, auth, req, schema)?;
    // GET and HEAD run read only, so a volatile function fails with
    // pg's 25006, which the status table maps to PostgREST's 405.
    let sess = pool
        .session(&ctx, !is_post)
        .await
        .map_err(|e| pg_error(&e, authed))?;

    // A call reads the schema cache for the same reasons a table
    // does, and it is cached per epoch either way, so the preferences
    // are settled here rather than in the one arm that would have
    // loaded it anyway.
    let catalog = load_catalog(app, &sess, authed, schema).await?;
    settle_prefs(&mut prefer, &sess, authed).await?;

    let rows = sess
        .query(rpc::INTROSPECT_SQL, &[&schema, &func])
        .await
        .map_err(|e| pg_error(&e, authed))?;
    let overloads: Vec<rpc::Routine> = rows
        .iter()
        .map(|r| {
            rpc::routine(rpc::RoutineRow {
                arg_names: r.get(0),
                arg_types: r.get(1),
                arg_casts: r.get(2),
                arg_variadic: r.get(3),
                defaults: r.get(4),
                returns_set: r.get(5),
                volatile: r.get(6),
                rettype: r.get(7),
                return_table: r.get(8),
                composite: r.get(9),
                media: r.get(10),
            })
        })
        .collect();

    // Which query pairs are arguments: on a GET every pair the
    // result grammar has no word for and whose value is a bare one,
    // since a value with an operator in front of it is a filter over
    // what the call returns. The two can share a name, so `?id=5`
    // calls the function and `?id=gt.2` narrows its answer. On a
    // POST the body keys are the arguments and the whole query
    // string is grammar.
    //
    // A name no overload has is an argument all the same. Upstream
    // resolves the call against everything the client wrote, so a
    // typo is a function nobody has rather than an argument quietly
    // dropped.
    let raw = req.uri.query().unwrap_or("");
    let mut args: Vec<(String, String)> = Vec::new();
    let mut residual: Vec<&str> = Vec::new();
    let mut supplied: Vec<String> = Vec::new();
    let mut default_select = true;
    if is_post {
        residual = raw.split('&').filter(|p| !p.is_empty()).collect();
    }
    for pair in raw.split('&').filter(|p| !p.is_empty()) {
        let Some((k, v)) = pair.split_once('=') else {
            if !is_post {
                residual.push(pair);
            }
            continue;
        };
        let key = decode(k);
        let value = decode(v);
        if key == "select" {
            default_select = false;
        }
        if is_post {
            continue;
        }
        if grammar_key(&key) || filter::is_operator(&value) {
            residual.push(pair);
            continue;
        }
        if !supplied.contains(&key) {
            supplied.push(key.clone());
        }
        args.push((key, value));
    }

    // Whatever is left is grammar over what the call returns, and it
    // is read here rather than in the one arm that goes on to use it,
    // because a query string nobody can read is a bad request no
    // matter what the function returns or what the body says. It is
    // read before the body for the same reason: a POST that gets both
    // wrong is answered about the half the client wrote first.
    let joined = residual.join("&");
    let (query, extras) = parse_query(func, (!joined.is_empty()).then_some(joined.as_str()))?;

    // What the body says, which is the content type and then the
    // bytes. A form is argument pairs the same way a query string
    // is, so both go on to bind one text parameter per value; a json
    // body and a csv read down to one object of arguments; and a
    // body that is one value carries no argument names at all, only
    // itself.
    let content = content_of(&req.headers);
    let mut payload: Option<String> = None;
    if let Some(bytes) = body {
        match &content {
            Content::Json => {
                let text = if bytes.is_empty() {
                    "{}".to_string()
                } else {
                    String::from_utf8(bytes.to_vec())
                        .map_err(|_| invalid_body("invalid utf-8 in the request body"))?
                };
                let v: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|_| invalid_body("Empty or invalid json"))?;
                // An array of objects calls the function once, on the
                // first of them, which is what upstream's LIMIT 1 over
                // the unpacked body comes to.
                let first = match &v {
                    serde_json::Value::Array(a) => a.first(),
                    other => Some(other),
                };
                if let Some(o) = first.and_then(|f| f.as_object()) {
                    supplied = o.keys().cloned().collect();
                }
                payload = Some(match &v {
                    serde_json::Value::Array(_) => match first {
                        Some(f) => f.to_string(),
                        None => "{}".to_string(),
                    },
                    _ => text,
                });
            }
            Content::Csv => {
                // A call takes one row of arguments, so a csv is read
                // down to its first record the way an update body is.
                let (cols, rows) = csv_rows(bytes)?;
                supplied = cols;
                payload = Some(match rows.into_iter().next() {
                    Some(row) => row.to_string(),
                    None => "{}".to_string(),
                });
            }
            Content::Form => {
                for (k, v) in form_pairs(bytes) {
                    if !supplied.contains(&k) {
                        supplied.push(k.clone());
                    }
                    args.push((k, v));
                }
            }
            Content::Value(_) => {
                payload = Some(String::from_utf8_lossy(bytes).into_owned());
            }
            Content::Other(name) => return Err(unreadable(name)),
        }
    }

    // ?columns= names the arguments outright, and then the body's own
    // keys are not read at all. It is the same rule a write has, and
    // it decides the call as well as the arguments: a key the list
    // leaves out is not something the function is asked for, so a
    // body carrying more than the function takes still resolves.
    // Only a json body has keys of its own to overrule, which is why
    // a form and a csv are left alone here.
    if matches!(content, Content::Json)
        && body.is_some()
        && let Some(list) = &extras.columns
    {
        supplied = list.clone();
    }

    let choice = match rpc::choose(
        schema,
        func,
        &overloads,
        &supplied,
        payload_of(&content),
        is_post,
    ) {
        Ok(c) => c,
        Err(mut e) => {
            // A name the schema has no function of at all is the one
            // case worth a second query: the suggestion is the
            // nearest name it does have, and nothing loaded so far
            // knows the others.
            if e.unknown_name {
                let rows = sess
                    .query(rpc::NAMES_SQL, &[&schema])
                    .await
                    .map_err(|x| pg_error(&x, authed))?;
                let names: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
                e.hint = rpc::name_hint(schema, func, &names);
            }
            return Err(rpc_error(e));
        }
    };
    // The function's own media type is only on offer when the call
    // hands back what the function returns. A select list of its own
    // makes the answer a shape the request invented, and nothing the
    // function declared speaks for that.
    let custom = choice.routine.media.as_deref().filter(|_| default_select);
    let media = negotiate_call(&req.headers, custom)?;
    let kind = choice.routine.kind.clone();
    let returns_set = choice.routine.returns_set;
    // A cap on rows counts the rows a call returns, so a function
    // that returns none has nothing to be judged on and is refused
    // outright rather than passing for free.
    let cap = prefer.cap();
    if cap.is_some() && !returns_set {
        return Err(not_set_returning());
    }
    // Only an exact total is answered on a call. Upstream's planned
    // and estimated ones come off the plan of the count query, which
    // is a second statement, and nothing asks a function for one.
    let exact = matches!(prefer.count, Some(Count::Exact));
    let m = match payload {
        Some(text) => rpc::call_json(schema, func, &choice, &supplied, text),
        None => rpc::call_get(schema, func, choice.routine, &args),
    };

    match kind {
        rpc::RetKind::Void => {
            let params: Vec<Text> = m.params.into_iter().map(Text).collect();
            sess.execute(&m.text, &param_refs(&params))
                .await
                .map_err(|e| pg_error(&e, authed))?;
            let gucs = Gucs::read(
                sess.commit_reading(GUCS_SQL)
                    .await
                    .map_err(|e| pg_error(&e, authed))?,
            );
            // Nothing to send and a range all the same, because
            // upstream counts the one row the call was.
            let mut res = (
                StatusCode::NO_CONTENT,
                [(
                    header::CONTENT_RANGE,
                    content_range(0, 1, exact.then_some(1)),
                )],
            )
                .into_response();
            applied_header(&prefer, Surface::Rpc, &req.method, false, &mut res);
            override_response(&gucs, &mut res)?;
            Ok(res)
        }
        rpc::RetKind::Scalar => {
            // Three ways to write one value out, all of them two
            // columns: what to send and how many rows it was. json
            // wraps, csv tabulates under the column the value
            // travelled in, and the function's own media type sends
            // the value itself with nothing around it.
            let wrapped = match &media {
                Media::Custom(_) => rpc::value_wrap(m),
                Media::Csv => zou_rest::sql::Sql {
                    text: format!("with \"_zou_source\" as ({}) {CSV_AGG}", m.text),
                    params: m.params,
                },
                _ => rpc::scalar_wrap(m, returns_set),
            };
            let params: Vec<Text> = wrapped.params.into_iter().map(Text).collect();
            let rows = sess
                .query(&wrapped.text, &param_refs(&params))
                .await
                .map_err(|e| pg_error(&e, authed))?;
            // The wrap counts the rows it folded, which is the only
            // place the count survives once a set is one json array.
            let affected = rows.first().map(|r| r.get::<_, i64>(1) as u64).unwrap_or(0);
            if let Some(e) = over_cap(cap, affected) {
                sess.rollback().await.map_err(|x| pg_error(&x, authed))?;
                return Err(e);
            }
            let gucs = Gucs::read(
                sess.commit_reading(GUCS_SQL)
                    .await
                    .map_err(|e| pg_error(&e, authed))?,
            );
            // Nothing is the empty body everywhere except json, where
            // nothing is still something to write.
            let out = rows
                .first()
                .and_then(|r| r.get::<_, Option<String>>(0))
                .unwrap_or_else(|| match &media {
                    Media::Json { .. } | Media::Single { .. } => {
                        if returns_set { "[]" } else { "null" }.to_string()
                    }
                    _ => String::new(),
                });
            // One value is one row as far as the range goes and a
            // folded set is as many rows as it folded, which is how
            // upstream counts them. Nothing here is paged, so an
            // exact total is the page it just built.
            let page = if returns_set { affected as usize } else { 1 };
            let total = exact.then_some(page as i64);
            let mut res = (
                range_status(0, page, total),
                [
                    (header::CONTENT_TYPE, media.content_type()),
                    (header::CONTENT_RANGE, content_range(0, page, total)),
                ],
                out,
            )
                .into_response();
            profile_header(&mut res, schema, negotiated);
            applied_header(&prefer, Surface::Rpc, &req.method, cap.is_some(), &mut res);
            override_response(&gucs, &mut res)?;
            Ok(res)
        }
        rpc::RetKind::Composite { table } => {
            // Rows go through the planner over the call's CTE, so
            // the whole select grammar applies, and when the return
            // type is a real table's rowtype embeds resolve on it.
            let mut q = query;
            q.table = table.unwrap_or_else(|| func.to_string());
            apply_range(&mut q, &req.method, &req.headers);
            let r = rpc::representation(&catalog, m, &mut q).map_err(plan_error)?;
            if !returns_set {
                let text = if media == Media::Csv {
                    format!(
                        "with {}, \"_zou_source\" as ({}) {CSV_AGG}",
                        r.cte, r.select.text
                    )
                } else {
                    format!(
                        "with {} select to_json(\"_zou_row\")::text from ({}) as \"_zou_row\"",
                        r.cte, r.select.text
                    )
                };
                let params: Vec<Text> = r.select.params.into_iter().map(Text).collect();
                let rows = sess
                    .query(&text, &param_refs(&params))
                    .await
                    .map_err(|e| pg_error(&e, authed))?;
                let gucs = Gucs::read(
                    sess.commit_reading(GUCS_SQL)
                        .await
                        .map_err(|e| pg_error(&e, authed))?,
                );
                // A non set function is one row, and PostgREST hands
                // it back as a bare object, not a one element array.
                // One row is also what it counts, whatever came back.
                let out = rows
                    .first()
                    .map(|r| r.get::<_, String>(0))
                    .unwrap_or_else(|| "null".to_string());
                let mut res = (
                    StatusCode::OK,
                    [
                        (header::CONTENT_TYPE, media.content_type()),
                        (
                            header::CONTENT_RANGE,
                            content_range(0, 1, exact.then_some(1)),
                        ),
                    ],
                    out,
                )
                    .into_response();
                profile_header(&mut res, schema, negotiated);
                applied_header(&prefer, Surface::Rpc, &req.method, false, &mut res);
                override_response(&gucs, &mut res)?;
                return Ok(res);
            }
            // A page that is the whole set is its own total, and a
            // paged one needs counting. The count reads the rows the
            // call already left in the CTE rather than calling the
            // function a second time, which is what upstream's second
            // CTE is for and the only way a volatile call stays one
            // call. Both totals ride out of the one statement,
            // because a window past the end has no row to carry it.
            let paged = q.limit.iter().any(|(r, _)| r.is_empty())
                || q.offset.iter().any(|(r, _)| r.is_empty());
            let counted = if exact && paged {
                Some(plan::count_from(&catalog, &q, r.select.params.clone()).map_err(plan_error)?)
            } else {
                None
            };
            let (cte, total_sql, params) = match counted {
                Some(c) => (
                    format!("{}, \"_zou_count\" as ({})", r.cte, c.text),
                    "(select count(*) from \"_zou_count\")::bigint",
                    c.params,
                ),
                None => (r.cte, "null::bigint", r.select.params),
            };
            // The array is joined out of the row texts rather than
            // built by json_agg, which is the same bytes a read
            // sends: an aggregate would space the commas out. csv
            // goes through the same aggregate a read uses, one CTE
            // further along so the total can ride out beside it.
            let text = if media == Media::Csv {
                format!(
                    "with {cte}, \"_zou_source\" as ({}), \"_zou_csv\" as ({CSV_AGG}) \
                     select \"_zou_csv\".*, {total_sql} from \"_zou_csv\"",
                    r.select.text
                )
            } else {
                format!(
                    "with {cte} select '[' || coalesce(string_agg(to_json(\"_zou_row\")::text, ','), '') || ']', \
                     count(*)::bigint, {total_sql} from ({}) as \"_zou_row\"",
                    r.select.text
                )
            };
            let params: Vec<Text> = params.into_iter().map(Text).collect();
            let rows = sess
                .query(&text, &param_refs(&params))
                .await
                .map_err(|e| pg_error(&e, authed))?;
            let page = rows
                .first()
                .map(|r| r.get::<_, i64>(1) as usize)
                .unwrap_or(0);
            if let Some(e) = over_cap(cap, page as u64) {
                sess.rollback().await.map_err(|x| pg_error(&x, authed))?;
                return Err(e);
            }
            let gucs = Gucs::read(
                sess.commit_reading(GUCS_SQL)
                    .await
                    .map_err(|e| pg_error(&e, authed))?,
            );
            let out = rows
                .first()
                .and_then(|r| r.get::<_, Option<String>>(0))
                .unwrap_or_else(|| if media == Media::Csv { "" } else { "[]" }.to_string());
            let total = if exact {
                Some(
                    rows.first()
                        .and_then(|r| r.get::<_, Option<i64>>(2))
                        .unwrap_or(page as i64),
                )
            } else {
                None
            };
            let offset = q
                .offset
                .iter()
                .find(|(route, _)| route.is_empty())
                .map(|(_, n)| *n)
                .unwrap_or(0);
            let status = range_status(offset, page, total);
            let mut res = if status == StatusCode::RANGE_NOT_SATISFIABLE {
                error_body(status, out_of_bounds(offset, total.unwrap_or(0)))
            } else {
                (status, [(header::CONTENT_TYPE, media.content_type())], out).into_response()
            };
            if let Ok(v) = content_range(offset, page, total).parse() {
                res.headers_mut().insert(header::CONTENT_RANGE, v);
            }
            profile_header(&mut res, schema, negotiated);
            applied_header(&prefer, Surface::Rpc, &req.method, cap.is_some(), &mut res);
            override_response(&gucs, &mut res)?;
            Ok(res)
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
                headers: None,
            }),
        },
        m => Err(RestError {
            status: StatusCode::METHOD_NOT_ALLOWED,
            code: "PGRST101".to_string(),
            message: format!("Cannot use the {m} method on RPC"),
            details: None,
            hint: None,
            headers: None,
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

    /// A refusal taken apart: the status, the body, and the challenge
    /// header, which together are everything a conformance case reads.
    async fn refusal(
        why: crate::jwt::Reject,
        token: &str,
    ) -> (u16, serde_json::Value, Option<String>) {
        let res = token_refused(&why, token);
        let status = res.status().as_u16();
        let challenge = res
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .expect("a refusal has a body");
        (
            status,
            serde_json::from_slice(&bytes).expect("and it is json"),
            challenge,
        )
    }

    #[tokio::test]
    async fn a_claim_about_time_is_its_own_code_and_the_message_is_the_claim() {
        for (why, said) in [
            (crate::jwt::Reject::Expired, "JWT expired"),
            (crate::jwt::Reject::TooEarly, "JWT not yet valid"),
            (crate::jwt::Reject::IssuedLater, "JWT issued at future"),
        ] {
            let (status, body, challenge) = refusal(why, "a.b.c").await;
            assert_eq!(status, 401);
            assert_eq!(body["code"], "PGRST303");
            assert_eq!(body["message"], said);
            assert_eq!(body["details"], serde_json::Value::Null);
            // The challenge carries the reason, which is the half of
            // this a client reads without opening the body.
            assert_eq!(
                challenge.unwrap(),
                format!("Bearer error=\"invalid_token\", error_description=\"{said}\"")
            );
        }
    }

    #[tokio::test]
    async fn a_signature_and_an_algorithm_read_alike_until_the_detail_line() {
        for (why, detail) in [
            (
                crate::jwt::Reject::BadSignature,
                "None of the keys was able to decode the JWT",
            ),
            (
                crate::jwt::Reject::WrongAlgorithm("HS512".to_string()),
                "No suitable key was found to decode the JWT",
            ),
            (
                crate::jwt::Reject::UnknownKey,
                "No suitable key was found to decode the JWT",
            ),
        ] {
            let (status, body, _) = refusal(why, "a.b.c").await;
            assert_eq!(status, 401);
            assert_eq!(body["code"], "PGRST301");
            assert_eq!(body["message"], "No suitable key or wrong key type");
            assert_eq!(body["details"], detail);
        }
    }

    #[tokio::test]
    async fn a_bearer_that_is_not_three_parts_is_counted_and_told_so() {
        let (_, one, _) = refusal(crate::jwt::Reject::Malformed, "not-a-token").await;
        assert_eq!(one["code"], "PGRST301");
        assert_eq!(one["message"], "Expected 3 parts in JWT; got 1");
        let (_, two, _) = refusal(crate::jwt::Reject::Malformed, "a.b").await;
        assert_eq!(two["message"], "Expected 3 parts in JWT; got 2");
        // Three parts that do not decode are the other shape, where
        // there was something to try and it did not work.
        let (_, three, _) = refusal(crate::jwt::Reject::Malformed, "a.b.c").await;
        assert_eq!(three["message"], "JWT cryptographic operation failed");
    }

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
    fn a_window_is_where_it_starts_and_how_far_it_reaches() {
        let (q, _) = parse_query("t", Some("limit=5&offset=10")).unwrap();
        assert_eq!(q.limit, vec![(vec![], 5)]);
        assert_eq!(q.offset, vec![(vec![], 10)]);

        // Nobody starts before the first row. The offset goes back to
        // zero and the rows it would have skipped come off the limit.
        let (q, _) = parse_query("t", Some("limit=5&offset=-1")).unwrap();
        assert_eq!(q.limit, vec![(vec![], 4)]);
        assert!(q.offset.is_empty());

        // On its own it is the request that named no offset at all.
        let (q, _) = parse_query("t", Some("select=id&offset=-4")).unwrap();
        assert!(q.limit.is_empty() && q.offset.is_empty());

        // Asking for no rows is a window ending one before it starts,
        // which is the one empty window that is not an error.
        let (q, _) = parse_query("t", Some("limit=0")).unwrap();
        assert_eq!(q.limit, vec![(vec![], 0)]);
        let (q, _) = parse_query("t", Some("limit=4&offset=-4")).unwrap();
        assert_eq!(q.limit, vec![(vec![], 0)]);

        // And one that ends any earlier is a range nobody can answer.
        for query in ["limit=-1", "limit=3&offset=-4", "orders.limit=-1"] {
            let e = parse_query("t", Some(query)).unwrap_err();
            assert_eq!(e.status, StatusCode::RANGE_NOT_SATISFIABLE, "{query}");
            assert_eq!(e.code, "PGRST103", "{query}");
        }
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
    fn the_grammar_words_are_the_ones_a_call_keeps_for_itself() {
        for key in [
            "select",
            "columns",
            "on_conflict",
            "apikey",
            "order",
            "limit",
            "offset",
            "and",
            "or",
            "orders.order",
            "orders.and",
        ] {
            assert!(grammar_key(key), "{key} is grammar");
        }
        // A word that only looks like one of them is an argument.
        for key in ["id", "ordering", "selected", "my.select", "limits"] {
            assert!(!grammar_key(key), "{key} is an argument");
        }
    }

    #[test]
    fn the_content_type_says_what_the_body_could_pass_as() {
        let of = |ct: Option<&str>| {
            let mut h = HeaderMap::new();
            if let Some(ct) = ct {
                h.insert(header::CONTENT_TYPE, ct.parse().unwrap());
            }
            payload_of(&content_of(&h))
        };
        assert_eq!(of(None), rpc::Payload::Json);
        assert_eq!(of(Some("application/json")), rpc::Payload::Json);
        assert_eq!(
            of(Some("application/json; charset=utf-8")),
            rpc::Payload::Json
        );
        assert_eq!(of(Some("text/plain")), rpc::Payload::Text);
        assert_eq!(of(Some("text/xml")), rpc::Payload::Xml);
        assert_eq!(of(Some("application/octet-stream")), rpc::Payload::Bytes);
        assert_eq!(
            of(Some("application/x-www-form-urlencoded")),
            rpc::Payload::Other
        );
        assert_eq!(of(Some("text/csv")), rpc::Payload::Other);
        assert_eq!(of(Some("audio/mpeg3")), rpc::Payload::Other);
    }

    /// An empty Content-Type is no Content-Type: hyper sends the
    /// header on a body-less request and the value is the empty
    /// string, which is not a media type and must not be refused as
    /// one.
    #[test]
    fn a_body_that_named_nothing_is_read_as_json() {
        let mut h = HeaderMap::new();
        h.insert(header::CONTENT_TYPE, "".parse().unwrap());
        assert_eq!(content_of(&h), Content::Json);
        assert_eq!(content_of(&HeaderMap::new()), Content::Json);
        assert_eq!(
            content_of(&h.clone()).name(),
            "application/json".to_string()
        );
        h.insert(header::CONTENT_TYPE, "audio/mpeg3".parse().unwrap());
        assert_eq!(
            content_of(&h),
            Content::Other("audio/mpeg3".to_string()),
            "a type nothing reads keeps its name for the refusal"
        );
        assert_eq!(
            unreadable(&content_of(&h).name()).message,
            "Content-Type not acceptable: audio/mpeg3"
        );
    }

    /// A function's own media type is only reached by naming it. No
    /// Accept at all, or a star, is json, because the handler is not
    /// registered under either.
    #[test]
    fn a_functions_own_media_type_has_to_be_asked_for_by_name() {
        let accept = |v: Option<&str>| {
            let mut h = HeaderMap::new();
            if let Some(v) = v {
                h.insert(header::ACCEPT, v.parse().unwrap());
            }
            negotiate_call(&h, Some("text/plain"))
        };
        assert_eq!(accept(None).unwrap(), Media::Json { stripped: false });
        assert_eq!(
            accept(Some("*/*")).unwrap(),
            Media::Json { stripped: false }
        );
        assert_eq!(
            accept(Some("text/plain")).unwrap(),
            Media::Custom("text/plain".to_string())
        );
        assert_eq!(
            accept(Some("text/plain; charset=utf-8")).unwrap(),
            Media::Custom("text/plain".to_string()),
            "a parameter nobody reads does not hide the type"
        );
        // Without the function's own type on offer the same request
        // has nothing left to be answered in.
        let mut h = HeaderMap::new();
        h.insert(header::ACCEPT, "text/plain".parse().unwrap());
        let e = negotiate_call(&h, None).unwrap_err();
        assert_eq!(e.code, "PGRST107");
        assert_eq!(
            e.message,
            "None of these media types are available: text/plain"
        );
    }

    /// The 406 names what was asked for the way this surface spells
    /// it, not the way the request wrote it, which is why a bare plan
    /// comes back with the media type it is a plan of on it.
    #[test]
    fn the_refusal_spells_the_name_back_in_full() {
        let mut h = HeaderMap::new();
        h.insert(
            header::ACCEPT,
            "application/vnd.pgrst.plan".parse().unwrap(),
        );
        assert_eq!(
            negotiate(&h).unwrap_err().message,
            "None of these media types are available: \
             application/vnd.pgrst.plan+text; for=\"application/json\""
        );
    }

    /// A csv body is rows of text under the header's names, and the
    /// word NULL is the only null in it.
    #[test]
    fn a_csv_body_is_its_records_under_its_header() {
        let body = body_rows(&Content::Csv, b"id,name\n1,foo\n2,NULL", None).unwrap();
        let (cols, rows) = (body.cols, body.rows);
        assert_eq!(cols, ["id", "name"]);
        assert_eq!(
            rows,
            vec![
                serde_json::json!({"id": "1", "name": "foo"}),
                serde_json::json!({"id": "2", "name": null}),
            ]
        );
        assert_eq!(
            body_rows(&Content::Csv, b"id,name\n1", None)
                .unwrap_err()
                .message,
            "All lines must have same number of fields"
        );
    }

    /// A form body is one row of text whatever it says, so nothing in
    /// it is a number and nothing in it is null.
    #[test]
    fn a_form_body_is_one_row_of_text() {
        let body = body_rows(&Content::Form, b"id=1&name=foo+bar", None).unwrap();
        let (cols, rows) = (body.cols, body.rows);
        assert_eq!(cols, ["id", "name"]);
        assert_eq!(
            rows,
            vec![serde_json::json!({"id": "1", "name": "foo bar"})]
        );
        // A table takes one row, so a repeated key is the last one
        // written rather than a list.
        let rows = body_rows(&Content::Form, b"a=1&a=2", None).unwrap().rows;
        assert_eq!(rows, vec![serde_json::json!({"a": "2"})]);
        // A call takes the pairs instead, repeats and order kept,
        // which is what fills a variadic argument.
        assert_eq!(
            form_pairs(b"v=hi&v=there"),
            vec![
                ("v".to_string(), "hi".to_string()),
                ("v".to_string(), "there".to_string()),
            ]
        );
    }

    /// A body nothing here reads is refused on its content type, and
    /// the bytes are never looked at.
    #[test]
    fn a_body_in_a_type_nothing_reads_is_refused_unread() {
        let e = body_rows(&Content::Other("audio/mpeg3".to_string()), b"", None).unwrap_err();
        assert_eq!(e.status, StatusCode::BAD_REQUEST);
        assert_eq!(e.code, "PGRST102");
        assert_eq!(e.message, "Content-Type not acceptable: audio/mpeg3");
        // A value body is one value, which a table has no column for.
        assert_eq!(
            body_rows(&Content::Value(rpc::Payload::Text), b"hi", None)
                .unwrap_err()
                .message,
            "Content-Type not acceptable: text/plain"
        );
    }

    /// The two halves of a parse error: the message names the
    /// parameter and quotes what it was given, with a position that
    /// counts characters from the front of it, and the details are
    /// what the grammar would have taken there instead.
    #[test]
    fn a_broken_filter_is_pgrst100() {
        let e = parse_query("t", Some("name=zzz.1")).unwrap_err();
        assert_eq!(e.status, StatusCode::BAD_REQUEST);
        assert_eq!(e.code, "PGRST100");
        assert_eq!(
            e.message,
            "\"failed to parse filter (zzz.1)\" (line 1, column 1)"
        );
        assert_eq!(
            e.details,
            Some("unexpected \"z\" expecting \"not\" or operator (eq, gt, ...)".into())
        );

        // A logic tree counts from the front of the operator, which
        // upstream reads out of the key and parses with the value.
        let e = parse_query("t", Some("or=()")).unwrap_err();
        assert_eq!(
            e.message,
            "\"failed to parse logic tree (())\" (line 1, column 4)"
        );

        // And a position counts characters rather than bytes.
        let e = parse_query("t", Some("order=\u{e9}\u{e9}.ascc")).unwrap_err();
        assert_eq!(
            e.message,
            "\"failed to parse order (\u{e9}\u{e9}.ascc)\" (line 1, column 7)"
        );
    }

    /// PostgREST names the charset on everything it sends, the errors
    /// as much as the rows. The auth surface copies GoTrue instead,
    /// which does not, so this is the one place it is decided.
    #[test]
    fn an_error_says_which_charset_it_is_in() {
        let res = bad_request("no").into_response();
        assert_eq!(
            res.headers()[header::CONTENT_TYPE],
            "application/json; charset=utf-8"
        );
        assert!(res.headers().get(header::WWW_AUTHENTICATE).is_none());
    }

    /// A refusal that does not say what it wants instead is a door
    /// with no handle on it.
    #[test]
    fn a_refusal_carries_the_challenge_and_the_others_do_not() {
        let refused = RestError {
            status: StatusCode::UNAUTHORIZED,
            code: "42501".to_string(),
            message: "no".to_string(),
            details: None,
            hint: None,
            headers: None,
        };
        let res = refused.into_response();
        assert_eq!(res.headers()[header::WWW_AUTHENTICATE], "Bearer");
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
        apply_range(&mut q, &Method::GET, &headers);
        assert_eq!(q.offset, vec![(Vec::new(), 5)]);
        assert_eq!(q.limit, vec![(Vec::new(), 5)]);

        let (mut q, _) = parse_query("t", Some("limit=1")).unwrap();
        apply_range(&mut q, &Method::GET, &headers);
        assert_eq!(q.limit, vec![(Vec::new(), 1)]);
        assert!(q.offset.is_empty());
    }

    #[test]
    fn only_a_get_reads_the_range_header() {
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, "1-1".parse().unwrap());

        // A HEAD asks the same question a GET does and still gets
        // the whole set, which is upstream reading the method and
        // not the shape of the request.
        for method in [Method::HEAD, Method::POST] {
            let (mut q, _) = parse_query("t", None).unwrap();
            apply_range(&mut q, &method, &headers);
            assert!(q.limit.is_empty(), "{method} took the range");
            assert!(q.offset.is_empty(), "{method} took the range");
        }
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
        let unrouted = plan_error(PlanError::Embed(EmbedError {
            code: "PGRST108",
            message: "'orders' is not an embedded resource in this request".to_string(),
            details: None,
            hint: Some("Verify that 'orders' is included in the 'select' query parameter.".into()),
        }));
        assert_eq!(unrouted.code, "PGRST108");
        assert_eq!(unrouted.status, StatusCode::BAD_REQUEST);
    }

    /// A sentence goes out as a string and the relationships of an
    /// ambiguous embed go out as a list of objects, which is the one
    /// place the details key is not a sentence.
    #[test]
    fn the_details_of_an_ambiguity_go_out_as_a_list() {
        use zou_rest::catalog::RelDetail;
        assert_eq!(
            detail_json(Details::Text("a sentence".into())),
            serde_json::json!("a sentence")
        );
        assert_eq!(
            detail_json(Details::Rels(vec![RelDetail {
                cardinality: "many-to-one",
                embedding: "orders with addresses".into(),
                relationship: "k using orders(a) and addresses(id)".into(),
            }])),
            serde_json::json!([{
                "cardinality": "many-to-one",
                "embedding": "orders with addresses",
                "relationship": "k using orders(a) and addresses(id)",
            }])
        );
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

    fn prefer(line: &str) -> Prefer {
        let mut h = HeaderMap::new();
        h.insert("prefer", line.parse().unwrap());
        parse_prefer(&h)
    }

    #[test]
    fn a_preference_nobody_has_is_refused_only_when_strict_asked_to_be_told() {
        let mut p = prefer("handling=strict, anything");
        assert_eq!(p.invalid, vec!["anything"]);
        let e = p.check().unwrap_err();
        assert_eq!(e.code, "PGRST122");
        assert_eq!(
            e.details.as_ref().and_then(|d| d.as_str()),
            Some("Invalid preferences: anything")
        );
        assert_eq!(e.status, StatusCode::BAD_REQUEST);

        // Lenient carries on, and so does saying nothing about it.
        let mut p = prefer("handling=lenient, anything");
        assert!(p.check().is_ok());
        let mut p = prefer("anything");
        assert!(p.check().is_ok());
        assert!(p.applied.is_empty());
    }

    #[test]
    fn a_timezone_is_postgres_business_and_nothing_judges_it_here() {
        let mut p = prefer("timezone=Asia/Bangkok");
        assert!(p.check().is_ok());
        assert_eq!(p.timezone.as_deref(), Some("Asia/Bangkok"));
        assert_eq!(p.applied, vec!["timezone=Asia/Bangkok"]);

        // A name postgres does not have is carried just the same,
        // under strict and without it, and the refusal comes from the
        // set_config that follows rather than from here. Upstream read
        // the name list itself until PostgREST 16 and stopped.
        for line in [
            "timezone=Nowhere/Special",
            "handling=strict, timezone=Nowhere/Special",
            "handling=lenient, timezone=utc",
        ] {
            let mut p = prefer(line);
            assert!(p.check().is_ok(), "{line}");
            assert!(p.timezone.is_some(), "{line}");
            assert!(p.invalid.is_empty(), "{line}");
        }
    }

    #[test]
    fn max_affected_binds_only_alongside_strict() {
        assert_eq!(prefer("handling=strict, max-affected=10").cap(), Some(10));
        assert_eq!(prefer("handling=lenient, max-affected=10").cap(), None);
        assert_eq!(prefer("max-affected=10").cap(), None);
        // A count that is not one is neither applied nor refused.
        let p = prefer("handling=strict, max-affected=lots");
        assert_eq!(p.cap(), None);
        assert!(p.invalid.is_empty());
        assert_eq!(prefer("handling=strict, max-affected=-1").cap(), None);
        assert_eq!(
            over_cap(Some(2), 3).map(|e| e.details).unwrap(),
            Some("The query affects 3 rows".into())
        );
        assert!(over_cap(Some(2), 2).is_none());
        assert!(over_cap(None, 9000).is_none());
    }

    #[test]
    fn preference_applied_says_what_the_request_honored_in_upstreams_order() {
        let applied = |line: &str, surface, method: Method, capped| {
            let mut p = prefer(line);
            p.check().unwrap();
            let mut res = StatusCode::OK.into_response();
            applied_header(&p, surface, &method, capped, &mut res);
            res.headers()
                .get("preference-applied")
                .map(|v| v.to_str().unwrap().to_string())
        };

        // Not the order the request listed them in.
        assert_eq!(
            applied(
                "timezone=UTC, handling=strict, count=exact, return=representation, \
                 missing=default, resolution=merge-duplicates",
                Surface::Write,
                Method::POST,
                false
            )
            .as_deref(),
            Some(
                "resolution=merge-duplicates, missing=default, return=representation, \
                 count=exact, handling=strict, timezone=UTC"
            )
        );

        // A read applies none of the three a write does.
        assert_eq!(
            applied(
                "return=representation, missing=default, resolution=merge-duplicates, count=exact",
                Surface::Read,
                Method::GET,
                false
            )
            .as_deref(),
            Some("count=exact")
        );
        // A patch fills defaults but does not resolve duplicates.
        assert_eq!(
            applied(
                "missing=default, resolution=merge-duplicates",
                Surface::Write,
                Method::PATCH,
                false
            )
            .as_deref(),
            Some("missing=default")
        );
        // A function call applies neither, and a cap it held itself
        // to is said last.
        assert_eq!(
            applied(
                "return=representation, handling=strict, max-affected=20",
                Surface::Rpc,
                Method::POST,
                true
            )
            .as_deref(),
            Some("handling=strict, max-affected=20")
        );
        assert_eq!(
            applied(
                "handling=strict, max-affected=20",
                Surface::Read,
                Method::GET,
                false
            )
            .as_deref(),
            Some("handling=strict")
        );
        assert_eq!(applied("", Surface::Read, Method::GET, false), None);
    }

    #[test]
    fn accept_negotiation_speaks_postgrest() {
        let m = |accept: Option<&str>| {
            let mut h = HeaderMap::new();
            if let Some(a) = accept {
                h.insert("accept", a.parse().unwrap());
            }
            negotiate(&h)
        };
        assert_eq!(m(None).unwrap(), Media::Json { stripped: false });
        assert_eq!(m(Some("*/*")).unwrap(), Media::Json { stripped: false });
        assert_eq!(
            m(Some("Application/JSON")).unwrap(),
            Media::Json { stripped: false }
        );
        assert_eq!(m(Some("text/csv")).unwrap(), Media::Csv);
        assert_eq!(
            m(Some("application/vnd.pgrst.object+json")).unwrap(),
            Media::Single { stripped: false }
        );
        assert_eq!(
            m(Some("application/vnd.pgrst.object+json;nulls=stripped")).unwrap(),
            Media::Single { stripped: true }
        );
        assert_eq!(
            m(Some("application/vnd.pgrst.array+json")).unwrap(),
            Media::Json { stripped: false },
            "the plain array vendored name folds into plain json"
        );
        assert_eq!(
            m(Some("application/vnd.pgrst.array+json;nulls=stripped")).unwrap(),
            Media::Json { stripped: true }
        );
        assert_eq!(
            m(Some("text/html, text/csv")).unwrap(),
            Media::Csv,
            "an unhandled type is skipped, not fatal"
        );
        assert_eq!(
            m(Some("text/csv;q=0.1, application/json;q=0.9")).unwrap(),
            Media::Json { stripped: false },
            "quality reorders"
        );
        assert_eq!(
            m(Some("*/*, text/csv")).unwrap(),
            Media::Csv,
            "the stars lose the specificity tiebreak"
        );

        let e = m(Some("text/html;level=1, image/png")).unwrap_err();
        assert_eq!(e.status, StatusCode::NOT_ACCEPTABLE);
        assert_eq!(e.code, "PGRST107");
        assert_eq!(
            e.message,
            "None of these media types are available: text/html;level=1, image/png"
        );
        let e = m(Some("TEXT/Plain;foo=1")).unwrap_err();
        assert_eq!(
            e.message, "None of these media types are available: text/plain",
            "a known but unproducible type echoes canonically"
        );

        assert_eq!(Media::Csv.content_type(), "text/csv; charset=utf-8");
        assert_eq!(
            Media::Single { stripped: true }.content_type(),
            "application/vnd.pgrst.object+json;nulls=stripped; charset=utf-8"
        );

        let e = not_single(2);
        assert_eq!(e.status, StatusCode::NOT_ACCEPTABLE);
        assert_eq!(e.code, "PGRST116");
        assert_eq!(
            e.message,
            "Cannot coerce the result to a single JSON object"
        );
        assert_eq!(
            e.details.as_ref().and_then(|d| d.as_str()),
            Some("The result contains 2 rows")
        );
    }

    #[test]
    fn schema_profiles_pick_the_header_by_method() {
        let exposed = vec!["public".to_string(), "other".to_string()];
        let p = |method: Method, name: &'static str, value: &str| {
            let mut h = HeaderMap::new();
            if !name.is_empty() {
                h.insert(name, value.parse().unwrap());
            }
            profile(&exposed, &method, &h)
        };

        assert_eq!(
            p(Method::GET, "accept-profile", "other").unwrap(),
            ("other", true)
        );
        assert_eq!(
            p(Method::GET, "content-profile", "other").unwrap(),
            ("public", true),
            "a read ignores Content-Profile, and two schemas make the default negotiated"
        );
        assert_eq!(
            p(Method::POST, "content-profile", "other").unwrap(),
            ("other", true)
        );
        assert_eq!(
            p(Method::POST, "accept-profile", "other").unwrap(),
            ("public", true),
            "a write ignores Accept-Profile"
        );
        for m in [Method::PATCH, Method::PUT, Method::DELETE] {
            assert_eq!(p(m, "content-profile", "other").unwrap(), ("other", true));
        }
        assert_eq!(p(Method::GET, "", "").unwrap(), ("public", true));
        assert_eq!(
            profile(&["public".to_string()], &Method::GET, &HeaderMap::new()).unwrap(),
            ("public", false),
            "one exposed schema means nothing was negotiated"
        );

        let e = p(Method::GET, "accept-profile", "secret").unwrap_err();
        assert_eq!(e.status, StatusCode::NOT_ACCEPTABLE);
        assert_eq!(e.code, "PGRST106");
        assert_eq!(e.message, "Invalid schema: secret");
        assert_eq!(e.details, None);
        assert_eq!(
            e.hint.as_deref(),
            Some("Only the following schemas are exposed: public, other")
        );
    }

    #[test]
    fn the_search_path_is_a_quoted_ident() {
        let parts = Request::builder()
            .uri("/rest/v1/t")
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let auth = AuthContext {
            role: "anon".to_string(),
            claims: Arc::new(serde_json::json!({})),
        };
        let cfg = Config::default();
        assert_eq!(
            request_context(&cfg, &auth, &parts, "public")
                .unwrap()
                .search_path,
            "\"public\""
        );
        assert_eq!(
            request_context(&cfg, &auth, &parts, "we\"ird")
                .unwrap()
                .search_path,
            "\"we\"\"ird\"",
            "a quote in the name doubles instead of escaping the list"
        );
    }

    /// The fence from #92, without a database in the way: a claim
    /// naming a role the project does not expose never becomes a
    /// context, so there is nothing to hand `set_config`.
    #[test]
    fn a_role_the_project_does_not_expose_is_refused_before_the_session() {
        let parts = Request::builder()
            .uri("/rest/v1/t")
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let claiming = |role: &str| AuthContext {
            role: role.to_string(),
            claims: Arc::new(serde_json::json!({})),
        };
        let cfg = Config::default();
        for role in ["anon", "authenticated", "service_role"] {
            assert!(
                request_context(&cfg, &claiming(role), &parts, "public").is_ok(),
                "{role} is one of the three a project has"
            );
        }
        let Err(e) = request_context(&cfg, &claiming("postgres"), &parts, "public") else {
            panic!("the superuser is not an api role");
        };
        assert_eq!(e.status, StatusCode::UNAUTHORIZED);
        assert_eq!(e.code, "22023");
        assert_eq!(e.message, "role \"postgres\" is not exposed");
        assert_eq!(
            e.hint.as_deref(),
            Some("Only the following roles are exposed: anon, authenticated, service_role")
        );

        // And a project that named its own set gets its own set, plus
        // whatever it calls the anonymous role.
        let cfg = Config {
            anon_role: "web_anon".to_string(),
            exposed_roles: vec!["reporting".to_string()],
            ..Config::default()
        };
        assert!(request_context(&cfg, &claiming("reporting"), &parts, "public").is_ok());
        assert!(request_context(&cfg, &claiming("web_anon"), &parts, "public").is_ok());
        assert!(
            request_context(&cfg, &claiming("authenticated"), &parts, "public").is_err(),
            "a named set replaces the default rather than adding to it"
        );
    }

    /// An update writes one row, so an array is read down to its first
    /// element. What survives is the text of it, which is the only way a
    /// duplicate key or the spelling of a number reaches a json column
    /// the way it was sent.
    #[test]
    fn an_update_takes_the_first_element_out_of_the_text() {
        let (cols, payload) =
            update_payload(&Content::Json, br#"[{"a":1.0,"a":2}]"#, None).unwrap();
        assert_eq!(cols, vec!["a"]);
        assert_eq!(payload, r#"{"a":1.0,"a":2}"#);

        let (_, payload) =
            update_payload(&Content::Json, br#" [ {"a": 1e2} , {"a": 3} ] "#, None).unwrap();
        assert_eq!(payload, r#"{"a": 1e2}"#);

        // A lone object is the body, and always was.
        let (_, payload) = update_payload(&Content::Json, br#"{"a":1.0}"#, None).unwrap();
        assert_eq!(payload, r#"{"a":1.0}"#);

        // An empty array leaves no columns to set at all.
        let (cols, payload) = update_payload(&Content::Json, b"[]", None).unwrap();
        assert!(cols.is_empty());
        assert_eq!(payload, "null");
    }

    /// The scan has to know where a string ends, because a bracket or a
    /// comma inside one ends nothing.
    #[test]
    fn a_bracket_inside_a_string_is_not_the_end_of_an_element() {
        assert_eq!(
            first_element(r#"[{"a":"],[","b":[1,2]},{"c":3}]"#),
            Some(r#"{"a":"],[","b":[1,2]}"#)
        );
        assert_eq!(first_element(r#"["a\"],b","c"]"#), Some(r#""a\"],b""#));
        assert_eq!(first_element("[1,2]"), Some("1"));
        assert_eq!(first_element("[  ]"), None);
        assert_eq!(first_element("{}"), None);
    }

    #[test]
    fn insert_bodies_normalize_to_an_array_and_the_first_row_names_the_columns() {
        let (cols, payload) = insert_payload(&Content::Json, br#"{"a":1,"b":2}"#, None).unwrap();
        assert_eq!(cols, vec!["a", "b"]);
        assert_eq!(payload, r#"[{"a":1,"b":2}]"#);

        let (cols, _) =
            insert_payload(&Content::Json, br#"[{"a":1,"b":2},{"b":3,"a":4}]"#, None).unwrap();
        assert_eq!(cols, vec!["a", "b"], "order across rows is not a mismatch");

        let narrowing = vec!["a".to_string()];
        let (cols, _) =
            insert_payload(&Content::Json, br#"[{"a":1,"b":2}]"#, Some(&narrowing)).unwrap();
        assert_eq!(cols, vec!["a"], "the columns parameter narrows");
        assert!(
            insert_payload(&Content::Json, b"[1,2]", Some(&narrowing)).is_ok(),
            "and it stops the body being read, so postgres refuses this one"
        );

        // A body that is not json is one sentence, whatever serde
        // thought of it, and a body that is not an object at all is
        // a row of defaults rather than a refusal.
        let e = insert_payload(&Content::Json, b"", None).unwrap_err();
        assert_eq!(
            (e.code.as_str(), e.message.as_str()),
            ("PGRST102", "Empty or invalid json")
        );
        assert_eq!(
            insert_payload(&Content::Json, b"}{", None)
                .unwrap_err()
                .message,
            "Empty or invalid json"
        );
        let (cols, payload) = insert_payload(&Content::Json, b"42", None).unwrap();
        assert!(cols.is_empty());
        assert_eq!(payload, "[42]");

        for body in [
            &br#"[1,2]"#[..],
            br#"[{"a":1},{"b":2}]"#,
            br#"[{"a":1},{"a":2,"b":3}]"#,
        ] {
            let e = insert_payload(&Content::Json, body, None).unwrap_err();
            assert_eq!(e.message, "All object keys must match", "for {body:?}");
        }
    }

    #[test]
    fn update_bodies_are_read_down_to_one_row() {
        let (cols, payload) = update_payload(&Content::Json, br#"{"title":"x"}"#, None).unwrap();
        assert_eq!(cols, vec!["title"]);
        assert_eq!(payload, r#"{"title":"x"}"#);

        let (cols, payload) =
            update_payload(&Content::Json, br#"[{"a":1},{"a":2}]"#, None).unwrap();
        assert_eq!(cols, vec!["a"]);
        assert_eq!(
            payload, r#"{"a":1}"#,
            "the first row is the one that writes"
        );

        // Nothing to set, which is a statement rather than an error.
        for body in [&b"[]"[..], b"{}", b"[{}]", b"42"] {
            let (cols, _) = update_payload(&Content::Json, body, None).unwrap();
            assert!(cols.is_empty(), "for {body:?}");
        }
        let named = vec!["a".to_string()];
        let (cols, _) = update_payload(&Content::Json, b"[]", Some(&named)).unwrap();
        assert!(
            cols.is_empty(),
            "an empty array has no row to take them from"
        );
    }

    #[test]
    fn a_put_is_filtered_by_its_key_or_it_is_refused() {
        fn root(query: &str) -> Vec<Node> {
            let (q, _) = parse_query("t", Some(query)).unwrap();
            q.filters.into_iter().map(|(_, node)| node).collect()
        }
        let one = vec!["id".to_string()];
        let two = vec!["first".to_string(), "last".to_string()];

        assert!(keys_the_row(&root("id=eq.1"), &one));
        assert!(keys_the_row(&root("last=eq.roe&first=eq.frances"), &two));

        // Everything else is the same refusal: a column that is not
        // the key, an operator that is not eq, a negation, a tree, a
        // key only half named, no filter at all, and no key at all.
        for query in [
            "rank=eq.19",
            "id=in.(1)",
            "id=not.eq.1",
            "and=(id.eq.1)",
            "id=eq.1&rank=eq.19",
            "",
        ] {
            assert!(!keys_the_row(&root(query), &one), "{query}");
        }
        assert!(!keys_the_row(&root("first=eq.frances"), &two));
        assert!(!keys_the_row(&root("id=eq.1"), &[]));
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

    #[test]
    fn a_function_that_raises_pgrst_writes_the_whole_answer() {
        let e = raised(
            r#"{"code":"123","message":"ABC","details":"DEF","hint":"XYZ"}"#,
            Some(r#"{"status":332,"status_text":"My Custom Status","headers":{"X-Header":"str"}}"#),
        );
        assert_eq!(e.status.as_u16(), 332);
        assert_eq!(e.code, "123");
        assert_eq!(e.message, "ABC");
        assert_eq!(e.details, Some("DEF".into()));
        assert_eq!(e.hint, Some("XYZ".to_string()));
        assert_eq!(
            e.headers.as_deref(),
            Some(&vec![("X-Header".to_string(), "str".to_string())]),
        );
        // The two optional keys of the body really are optional.
        let e = raised(
            r#"{"code":"123","message":"ABC"}"#,
            Some(r#"{"status":404,"headers":{}}"#),
        );
        assert_eq!(e.status, StatusCode::NOT_FOUND);
        assert_eq!(e.details, None);
        assert_eq!(e.hint, None);
        assert!(e.headers.is_none_or(|h| h.is_empty()));
    }

    #[test]
    fn a_raise_that_wrote_no_answer_is_this_servers_own_five_hundred() {
        let detail = r#"{"status":332,"headers":{}}"#;
        for (message, detail, details) in [
            (
                "INVALID",
                Some(detail),
                "Invalid JSON value for MESSAGE: 'INVALID'",
            ),
            // Obligatory means obligatory, and a key of the wrong type
            // is as unreadable as no key at all.
            (
                r#"{"message":"ABC"}"#,
                Some(detail),
                "Invalid JSON value for MESSAGE: '{\"message\":\"ABC\"}'",
            ),
            (
                r#"{"code":"123","message":"ABC","hint":7}"#,
                Some(detail),
                "Invalid JSON value for MESSAGE: '{\"code\":\"123\",\"message\":\"ABC\",\"hint\":7}'",
            ),
            (
                r#"{"code":"123","message":"ABC"}"#,
                None,
                "DETAIL is missing in the RAISE statement",
            ),
            (
                r#"{"code":"123","message":"ABC"}"#,
                Some("INVALID"),
                "Invalid JSON value for DETAIL: 'INVALID'",
            ),
            (
                r#"{"code":"123","message":"ABC"}"#,
                Some(r#"{"status":332}"#),
                "Invalid JSON value for DETAIL: '{\"status\":332}'",
            ),
            (
                r#"{"code":"123","message":"ABC"}"#,
                Some(r#"{"status":332,"headers":{"X-Header":7}}"#),
                "Invalid JSON value for DETAIL: '{\"status\":332,\"headers\":{\"X-Header\":7}}'",
            ),
        ] {
            let e = raised(message, detail);
            assert_eq!(e.status, StatusCode::INTERNAL_SERVER_ERROR, "{message}");
            assert_eq!(e.code, "PGRST121", "{message}");
            assert_eq!(
                e.message,
                "Could not parse JSON in the \"RAISE SQLSTATE 'PGRST'\" error",
            );
            assert_eq!(e.details, Some(details.into()), "{message}");
            assert!(e.hint.is_some(), "{message}");
        }
    }

    /// Upstream reads the status with `decimal`, which keeps the
    /// leading digits and drops what followed, so a reason phrase
    /// after the number costs nothing and a word alone is nothing.
    #[test]
    fn a_status_is_the_number_it_starts_with() {
        for (raw, want) in [
            ("205", Some(StatusCode::RESET_CONTENT)),
            ("403", Some(StatusCode::FORBIDDEN)),
            ("205 Reset Content", Some(StatusCode::RESET_CONTENT)),
            ("unknown", None),
            ("", None),
            ("-1", None),
            (" 200", None),
            ("99", None),
            ("70000", None),
        ] {
            assert_eq!(guc_status(raw).ok(), want, "{raw}");
        }
    }

    /// A single key object holding a string, and every other shape is
    /// the one sentence upstream says about all of them.
    #[test]
    fn a_header_is_one_key_holding_a_string() {
        assert_eq!(
            guc_headers(r#"[{"X-Test": "one"}, {"X-Test": "two"}]"#)
                .unwrap()
                .len(),
            2,
        );
        for raw in [
            r#"{"X-Test": "one"}"#,
            r#"[{"X-Test": "one", "X-Other": "two"}]"#,
            r#"[{"X-Test": 7}]"#,
            r#"[{}]"#,
            r#"["X-Test"]"#,
            "not json at all",
        ] {
            let e = guc_headers(raw).expect_err(raw);
            assert_eq!(e.code, "PGRST111", "{raw}");
            assert_eq!(e.status, StatusCode::INTERNAL_SERVER_ERROR, "{raw}");
        }
    }

    /// What the function set stands where the handler computed the
    /// same name, and everything else it did not mention stays. A
    /// name it wrote twice is sent twice, which is the only reason
    /// the list is a list of objects rather than one object.
    #[test]
    fn what_a_function_set_wins_and_the_rest_stays() {
        let mut res = (
            StatusCode::CREATED,
            [
                (header::LOCATION, "/stuff?id=eq.1"),
                (header::CONTENT_RANGE, "*/*"),
            ],
        )
            .into_response();
        let gucs = Gucs {
            headers: Some(
                r#"[{"Location": "/stuff?id=eq.2"}, {"X-Two": "a"}, {"X-Two": "b"}]"#.to_string(),
            ),
            status: Some("205".to_string()),
        };
        override_response(&gucs, &mut res).unwrap();
        assert_eq!(res.status(), StatusCode::RESET_CONTENT);
        assert_eq!(res.headers()[header::LOCATION], "/stuff?id=eq.2");
        assert_eq!(res.headers()[header::CONTENT_RANGE], "*/*");
        let two: Vec<&str> = res
            .headers()
            .get_all("x-two")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(two, vec!["a", "b"]);
    }

    /// A response that got both wrong answers with the status, which
    /// is the order upstream decodes them in.
    #[test]
    fn the_status_is_decoded_first() {
        let mut res = StatusCode::OK.into_response();
        let gucs = Gucs {
            headers: Some("not json".to_string()),
            status: Some("unknown".to_string()),
        };
        assert_eq!(
            override_response(&gucs, &mut res).unwrap_err().code,
            "PGRST112"
        );
    }

    /// Nothing set is nothing changed, which is every request that
    /// never calls a function that says anything.
    #[test]
    fn a_response_nobody_spoke_for_is_left_alone() {
        let mut res = StatusCode::NO_CONTENT.into_response();
        override_response(&Gucs::default(), &mut res).unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        assert!(res.headers().is_empty());
    }

    #[test]
    fn a_status_no_answer_can_carry_is_the_bug_it_is() {
        let e = raised(
            r#"{"code":"123","message":"ABC"}"#,
            Some(r#"{"status":99,"headers":{}}"#),
        );
        assert_eq!(e.status, StatusCode::INTERNAL_SERVER_ERROR);
        // The body is still the function's own, because it wrote one.
        assert_eq!(e.code, "123");
    }
}
