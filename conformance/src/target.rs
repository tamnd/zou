//! Something that answers requests, and how to get a suite's schema
//! into the database behind it.
//!
//! A target is a url and the keys to ask it with. Hosted Supabase,
//! `supabase start`,
//! a bare PostgREST, and zou are all the same thing from here, which is
//! the point: the runner has no idea which it is talking to, so it
//! cannot accidentally be kinder to one of them.
//!
//! The keys are JWTs signed with the target's own secret. Give every
//! target the same secret and the same keys go to all of them, and then
//! the requests really are the same requests rather than requests that
//! happen to mean the same thing.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::sigv4::{self, Credentials};
use crate::suite::{Answer, Case, Key};

/// What a case can be asked as.
///
/// One struct rather than four arguments because they are chosen
/// together and always all four: a target is given every key the suite
/// might name, and which one a request goes out with is the case's
/// decision rather than the target's.
#[derive(Default)]
pub struct Keys {
    pub anon: Option<String>,
    pub authenticated: Option<String>,
    pub service: Option<String>,
    /// An access token for the person the suite seeded, which is how a
    /// case reaches an endpoint that wants one without signing in
    /// first. Signing in would answer with a token, and a case cannot
    /// read the answer to the case before it.
    pub user: Option<String>,
    /// The pair the S3 surface is asked with, which is not a token and
    /// is not interchangeable with one. A target without it cannot be
    /// asked the S3 cases at all, which is the hosted case until
    /// somebody makes a pair on a throwaway project.
    pub s3: Option<Credentials>,
}

pub struct Target {
    /// What the report calls it.
    pub name: String,
    /// No trailing slash.
    pub url: String,
    pub keys: Keys,
    /// The database behind it, when the suite's schema can be applied
    /// from here. A target with no dsn has to be set up by hand, which
    /// is the hosted case.
    pub dsn: Option<String>,
    /// A path prefix this target does not have. Cases are written with
    /// the paths a Supabase project answers on, `/rest/v1/todos`, and a
    /// bare PostgREST serves that table at `/todos`, so the prefix is
    /// taken off on the way out rather than written twice in the case.
    pub strip: Option<String>,
    agent: ureq::Agent,
}

/// The headers a difference is a difference in. Everything else is
/// transport or bookkeeping: `date` and `server` are never equal, the
/// request id is meant to differ, and `content-length` is the body
/// again in a form that says nothing the body did not.
///
/// `content-location` is left out for a duller reason. It echoes the
/// path the request came in on, and a bare PostgREST is asked on a path
/// with `/rest/v1` taken off it, so it would differ on every case
/// without anything having differed.
///
/// The nine at the bottom are the resumable protocol, where the answer
/// is the headers. A creation answers 201 with an empty body and says
/// everything in `location`, a head answers 200 with an empty body and
/// says everything in `upload-offset` and `upload-length`, and the
/// capabilities of the endpoint are `tus-version`, `tus-extension` and
/// `tus-max-size` and nothing else. Comparing bodies alone would say
/// every one of those cases matched.
///
/// `etag` is here for the same reason, and it is the one entry that is
/// not free: it is the whole answer to a put on the S3 surface, which
/// sends an empty body, and it is a value both targets compute rather
/// than echo. It is safe to compare because it is the md5 of the bytes
/// in quotes on both, which is a value a fixture can pin, and it is
/// not compared as a date is: an object with different bytes has a
/// different one and an object with the same bytes has the same one on
/// every run and every machine.
pub const COMPARED: [&str; 18] = [
    "allow",
    "content-profile",
    "content-range",
    "content-type",
    "etag",
    "location",
    "preference-applied",
    "retry-after",
    "www-authenticate",
    "tus-extension",
    "tus-max-size",
    "tus-resumable",
    "tus-version",
    "upload-defer-length",
    "upload-expires",
    "upload-length",
    "upload-metadata",
    "upload-offset",
];

impl Target {
    pub fn new(
        name: &str,
        url: &str,
        keys: Keys,
        dsn: Option<String>,
        strip: Option<String>,
    ) -> Target {
        // Statuses are the answer here, not an error, and a slow target
        // should say so rather than hang a CI job for ten minutes.
        // A 3xx is an answer like any other and gets compared like one.
        // Following it would ask a second question and record what came
        // back to that instead, and a 300 with no location, which is
        // what PostgREST says when an embed is ambiguous, would not even
        // be followable.
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .max_redirects(0)
            .timeout_global(Some(Duration::from_secs(30)))
            .build()
            .into();
        Target {
            name: name.to_string(),
            url: url.trim_end_matches('/').to_string(),
            keys,
            dsn,
            strip: strip.filter(|s| !s.is_empty()),
            agent,
        }
    }

    fn key_for(&self, key: Key) -> Option<&str> {
        match key {
            Key::Anon => self.keys.anon.as_deref(),
            Key::Authenticated => self.keys.authenticated.as_deref(),
            Key::Service => self.keys.service.as_deref(),
            Key::User => self.keys.user.as_deref(),
            // Signed rather than carried, and signed after the case's
            // own headers are on the request, so there is nothing to
            // put in front here.
            Key::S3
            | Key::S3WrongSecret
            | Key::S3WrongId
            | Key::S3WrongRegion
            | Key::S3ProjectRegion
            | Key::None => None,
        }
    }

    /// The path this target answers `case` on.
    pub fn path_of(&self, case: &Case) -> String {
        match &self.strip {
            // Only as a prefix, and only leaving something behind: a
            // target that strips /rest/v1 should still be asked for
            // /auth/v1/health as it stands.
            Some(prefix) => match case.path.strip_prefix(prefix.as_str()) {
                Some("") => "/".to_string(),
                Some(rest) if rest.starts_with('/') || rest.starts_with('?') => rest.to_string(),
                _ => case.path.clone(),
            },
            None => case.path.clone(),
        }
    }

    /// One case, once. Transport failures come back as an error rather
    /// than as a status, because a target that did not answer is not a
    /// target that answered differently.
    pub fn send(&self, case: &Case) -> Result<Sent, String> {
        let url = format!("{}{}", self.url, encoded(&self.path_of(case)));
        let mut request = ureq::http::Request::builder()
            .method(case.method.as_str())
            .uri(&url);
        if let Some(key) = self.key_for(case.key) {
            // Both, the same as every Supabase client sends: the apikey
            // is what the gate reads and the bearer is what postgres
            // gets its role from.
            request = request
                .header("apikey", key)
                .header("authorization", format!("Bearer {key}"));
        }
        let body = case.body.clone().unwrap_or_default();
        let mut request = request
            .body(body)
            .map_err(|e| format!("{}: building {} {}: {e}", self.name, case.method, url))?;
        // Replacing rather than adding, so that a case that writes its
        // own authorization is asked with that one and not with two.
        // The apikey the key put on stays, which is the point: a case
        // about a missing or malformed token has to get past the
        // gateway to be a case about the token at all, and the hosted
        // gateway is in front of the reference too even though the
        // reference here is a bare binary with nothing in front of it.
        for (name, value) in &case.headers {
            let name: ureq::http::HeaderName = name
                .parse()
                .map_err(|e| format!("{}: {}: header {name}: {e}", self.name, case.name))?;
            let value: ureq::http::HeaderValue = value
                .parse()
                .map_err(|e| format!("{}: {}: header {name}: {e}", self.name, case.name))?;
            request.headers_mut().insert(name, value);
        }
        if case.key.signs() {
            self.sign(&mut request, case)?;
        }
        let response = self
            .agent
            .run(request)
            .map_err(|e| format!("{}: {} {}: {e}", self.name, case.method, url))?;

        let status = response.status().as_u16();
        let mut headers = BTreeMap::new();
        // Every header rather than the compared ones, because a case can
        // hold a value out of one this suite does not compare and a hold
        // that silently found nothing is the worst way to learn that.
        let mut all = BTreeMap::new();
        for (name, value) in response.headers() {
            if let Ok(value) = value.to_str() {
                all.insert(name.as_str().to_ascii_lowercase(), value.to_string());
            }
        }
        for name in COMPARED {
            if let Some(value) = all.get(name) {
                headers.insert(name.to_string(), value.clone());
            }
        }
        let raw = response
            .into_body()
            .read_to_string()
            .map_err(|e| format!("{}: reading the body of {url}: {e}", self.name))?;
        // Before anything is named, because the case after this one is
        // sent to the id this one answered rather than to the shape of
        // it.
        let held = holding(case, &all, &raw);
        let mut answer = answer(&case.name, status, headers, &raw);
        if !case.volatile.is_empty() {
            crate::volatile::redact(&mut answer.body, &case.volatile);
            crate::volatile::redact_headers(&mut answer.headers, &case.volatile);
            // The kept bytes are the bytes the volatile values were in,
            // so there is nothing left in them to compare: a case that
            // names a token gives up the byte for byte verdict on that
            // answer and keeps the json one.
            answer.raw = None;
        }
        Ok(Sent { answer, held })
    }

    /// Sign a request the way an S3 client would, in place.
    ///
    /// After the case's own headers rather than before, for two
    /// reasons. A case that sets an `x-amz-` header means it to be
    /// covered by the signature, since that is what a client would do.
    /// And a case that writes its own `authorization` is a case about a
    /// signature that is wrong on purpose, so there is nothing to sign.
    fn sign(&self, request: &mut ureq::http::Request<String>, case: &Case) -> Result<(), String> {
        let credentials = self.keys.s3.as_ref().ok_or_else(|| {
            format!(
                "{}: {}: asked to be signed and this target has no S3 key pair",
                self.name, case.name
            )
        })?;
        // A wrong pair is the target's own with one field replaced, so
        // the request is correct in every other way and the answer is
        // about the credential rather than about the shape of the
        // header. Which field is replaced is the question: an id
        // nobody has is a lookup that finds nothing, and a secret that
        // is not the right one is a signature that does not match.
        let credentials = match case.key {
            Key::S3WrongId => &Credentials {
                access: "0000000000000000000000000000000a".to_string(),
                ..credentials.clone()
            },
            Key::S3WrongSecret => &Credentials {
                secret: "0".repeat(credentials.secret.len()),
                ..credentials.clone()
            },
            _ => credentials,
        };
        if request.headers().contains_key("authorization") {
            return Ok(());
        }
        let host = self
            .url
            .split_once("://")
            .map(|(_, rest)| rest)
            .unwrap_or(&self.url)
            .trim_end_matches('/')
            .to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| format!("{}: reading the clock: {e}", self.name))?
            .as_secs();
        let stamp = match request
            .headers()
            .get("x-amz-date")
            .and_then(|v| v.to_str().ok())
        {
            Some(given) => given.to_string(),
            None => sigv4::stamp(now),
        };
        let payload = match request
            .headers()
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
        {
            Some(given) => given.to_string(),
            None => sigv4::hex(&sigv4::sha256(request.body().as_bytes())),
        };
        put(request, "x-amz-date", &stamp)?;
        put(request, "x-amz-content-sha256", &payload)?;
        let (path, query) = match self.path_of(case).split_once('?') {
            Some((path, query)) => (encoded(path), query.to_string()),
            None => (encoded(&self.path_of(case)), String::new()),
        };
        // Host and every x-amz- header on the request, which is the set
        // an unadorned client signs. Lowercased and sorted, because the
        // server rebuilds this list from what arrived and compares the
        // two strings.
        let mut headers = vec![("host".to_string(), host)];
        for (name, value) in request.headers() {
            let name = name.as_str().to_ascii_lowercase();
            if name.starts_with("x-amz-")
                && let Ok(value) = value.to_str()
            {
                headers.push((name, value.to_string()));
            }
        }
        headers.sort();
        let signed = sigv4::authorization(
            &sigv4::Request {
                method: case.method.as_str(),
                path: &path,
                query: &query,
                headers,
                payload: &payload,
                stamp: &stamp,
                region: match case.key {
                    Key::S3WrongRegion => "eu-west-1",
                    // Where a local project says it is, which is the
                    // one region other than the default that could be
                    // taken. Written out here for the same reason the
                    // wrong one is: a case cannot read it out of the
                    // answer to the case before it.
                    Key::S3ProjectRegion => sigv4::PROJECT,
                    _ => sigv4::REGION,
                },
            },
            credentials,
        );
        put(request, "authorization", &signed)
    }

    /// The suite's schema, applied to whatever database this target
    /// reads. Statements run in one batch, so the file can hold plpgsql
    /// blocks and dollar quoting without being split up here.
    pub fn set_up(&self, setup: &str) -> Result<(), String> {
        let dsn = match &self.dsn {
            Some(dsn) => dsn,
            None => return Ok(()),
        };
        apply(dsn, setup, &self.name)
    }

    /// Refuse a database that would answer a timestamp differently here
    /// than it does anywhere else.
    ///
    /// A timestamptz is rendered in the session's timezone, and the
    /// session gets the server's, so `2023-10-18 12:37:59.611+00` comes
    /// back as `+00:00` on one machine and `+07:00` on another. That is
    /// the same binary answering the same question two ways, which is
    /// exactly what a recording cannot survive: recorded on a +07
    /// laptop, every later run on a UTC server reads as a difference
    /// nobody introduced. Upstream's own expectations are written in
    /// UTC, so UTC is what the suites are recorded against.
    ///
    /// Checked against a connection opened here, since that is the same
    /// thing the pools opened.
    pub fn utc(&self) -> Result<(), String> {
        let dsn = match &self.dsn {
            Some(dsn) => dsn,
            None => return Ok(()),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("tokio: {e}"))?;
        runtime.block_on(async {
            let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
                .await
                .map_err(|e| format!("{}: connecting to read the timezone: {e}", self.name))?;
            let task = tokio::spawn(connection);
            let row = client
                .query_one(
                    "select current_setting('TimeZone'), \
                     extract(timezone from now())::int",
                    &[],
                )
                .await
                .map_err(|e| format!("{}: reading the timezone: {}", self.name, because(&e)));
            drop(client);
            let _ = task.await;
            let row = row?;
            let zone: String = row.get(0);
            let offset: i32 = row.get(1);
            match offset {
                0 => Ok(()),
                _ => Err(format!(
                    "{}: the database is on {zone} and the suites are recorded \
                     against a server on UTC, so every case that reads a \
                     timestamptz would differ. Set it with \"alter database \
                     <name> set timezone to 'UTC'\" and start whatever holds a \
                     pool on it again.",
                    self.name
                )),
            }
        })
    }
}

/// What one case came back with: the answer as it is recorded, and the
/// values the cases after it were told to read out of it.
///
/// The two are not the same string. A creation answers with a url and an
/// id that were made a moment ago, so the case naming them as volatile
/// has them replaced by the shape of them, and the case after it still
/// has to be sent to the real ones.
pub struct Sent {
    pub answer: Answer,
    pub held: BTreeMap<String, String>,
}

/// The one value every answer offers whether or not a case asked for it,
/// which is the url a creation pointed at.
pub const LOCATION: &str = "location";

/// Every name the cases in a suite hold something under, which is every
/// name a placeholder in one of them can mean.
///
/// Collected up front so that a placeholder can be told from a brace
/// that is part of a body. A json body is full of braces and none of
/// them are placeholders, and the difference is that a placeholder names
/// something a case in this suite said it would hold.
pub fn held_names(cases: &[Case]) -> BTreeSet<String> {
    let mut names = BTreeSet::from([LOCATION.to_string()]);
    for case in cases {
        names.extend(case.holds.keys().cloned());
    }
    names
}

/// `case`, with every placeholder in it replaced by what an earlier case
/// held under that name.
///
/// Placeholders are replaced in the path, in the body and in the header
/// values, because the three requests that need this need all three: a
/// part goes to a url carrying the upload's id, a completion sends the
/// parts' etags in a document, and there is nothing about a header that
/// makes it the exception.
///
/// A case naming something nothing held is an error rather than a
/// request with braces in it, since the case before it having failed is
/// the usual reason for it and a second failure that says something else
/// would only hide the first.
pub fn filled(
    case: &Case,
    held: &BTreeMap<String, String>,
    names: &BTreeSet<String>,
) -> Result<Case, String> {
    let mut case = case.clone();
    for name in names {
        let named = format!("{{{name}}}");
        let inside = case.path.contains(&named)
            || case.body.as_deref().is_some_and(|it| it.contains(&named))
            || case.headers.values().any(|it| it.contains(&named));
        if !inside {
            continue;
        }
        let Some(value) = held.get(name) else {
            return Err(format!(
                "{}: names {named}, and nothing before it held one",
                case.name
            ));
        };
        case.path = case.path.replace(&named, value);
        if let Some(body) = case.body.as_mut() {
            *body = body.replace(&named, value);
        }
        for header in case.headers.values_mut() {
            *header = header.replace(&named, value);
        }
    }
    Ok(case)
}

/// What this case said to hold, read out of its answer before anything
/// in it was named as volatile.
///
/// The path of a location and not the whole url, because the reference
/// answers an absolute one built out of the host the request arrived on
/// and every target is asked on a host of its own.
fn holding(
    case: &Case,
    headers: &BTreeMap<String, String>,
    body: &str,
) -> BTreeMap<String, String> {
    let mut held = BTreeMap::new();
    if let Some(url) = headers.get(LOCATION) {
        let path = match url.split_once("://") {
            Some((_, rest)) => match rest.split_once('/') {
                Some((_, path)) => format!("/{path}"),
                None => "/".to_string(),
            },
            None => url.to_string(),
        };
        held.insert(LOCATION.to_string(), path);
    }
    for (name, from) in &case.holds {
        // A hold that finds nothing is left unheld rather than held
        // empty, so the case that names it says the chain broke instead
        // of asking about an id that is the empty string.
        if let Some(value) = read_out(headers, body, from) {
            held.insert(name.clone(), value);
        }
    }
    held
}

/// One value out of an answer, by a name saying which half of it to look
/// in.
fn read_out(headers: &BTreeMap<String, String>, body: &str, from: &str) -> Option<String> {
    if let Some(name) = from.strip_prefix("header:") {
        return headers.get(&name.trim().to_ascii_lowercase()).cloned();
    }
    let name = from.strip_prefix("element:")?.trim();
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let after = &body[body.find(&open)? + open.len()..];
    Some(after[..after.find(&close)?].to_string())
}

/// A sql file, applied to a database over one connection.
///
/// Its own connection every time rather than a pool: this runs once
/// before anything is asked, and a setup that leaves a session with a
/// changed search_path or a held advisory lock behind should not be able
/// to hand that to a case.
///
/// One batch, so the file can hold plpgsql blocks and dollar quoting
/// without being split up here. `whose` only ever appears in the error,
/// to say which target's database refused it.
pub fn apply(dsn: &str, sql: &str, whose: &str) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio: {e}"))?;
    runtime.block_on(async {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| format!("{whose}: connecting to set up: {e}"))?;
        let task = tokio::spawn(connection);
        let result = client
            .batch_execute(sql)
            .await
            .map_err(|e| format!("{whose}: setup.sql: {}", because(&e)));
        drop(client);
        let _ = task.await;
        result
    })
}

/// An error and everything under it, on one line.
///
/// tokio_postgres prints "db error" and keeps the statement that failed
/// and the position in it in the source, which is the only part anybody
/// reading a setup failure wants.
fn because(error: &dyn std::error::Error) -> String {
    let mut out = error.to_string();
    let mut under = error.source();
    while let Some(error) = under {
        out.push_str(": ");
        out.push_str(&error.to_string());
        under = error.source();
    }
    out
}

/// The characters a url cannot carry, escaped, and nothing else.
///
/// PostgREST's own syntax is full of them: `profile->>city`, `tags=cs.{a}`,
/// `select=id::text`. A case is written the way it is written in the
/// documentation and encoded on the way out, which is what every client
/// does and what makes the cases readable. Percent signs are left alone,
/// so a case that means to send an escape can.
/// One header onto a request that is already built, by the two names
/// the http crate wants it under.
fn put(request: &mut ureq::http::Request<String>, name: &str, value: &str) -> Result<(), String> {
    let name: ureq::http::HeaderName = name.parse().map_err(|e| format!("header {name}: {e}"))?;
    let value: ureq::http::HeaderValue = value
        .parse()
        .map_err(|e| format!("header {name} value: {e}"))?;
    request.headers_mut().insert(name, value);
    Ok(())
}

fn encoded(path: &str) -> String {
    const ESCAPED: &str = " \"<>{}|\\^`";
    let mut out = String::with_capacity(path.len());
    for c in path.chars() {
        match ESCAPED.contains(c) || !c.is_ascii() {
            true => {
                let mut bytes = [0u8; 4];
                for byte in c.encode_utf8(&mut bytes).as_bytes() {
                    out.push_str(&format!("%{byte:02X}"));
                }
            }
            false => out.push(c),
        }
    }
    out
}

/// A body is parsed when it is json and kept as text when it is not,
/// and the bytes are kept alongside only when re-serializing loses
/// something, which is whitespace or key order.
pub fn answer(name: &str, status: u16, headers: BTreeMap<String, String>, raw: &str) -> Answer {
    let (body, kept) = match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(value) => {
            let round_trip = serde_json::to_string(&value).unwrap_or_default();
            let kept = match round_trip == raw {
                true => None,
                false => Some(raw.to_string()),
            };
            (value, kept)
        }
        Err(_) if raw.is_empty() => (serde_json::Value::Null, None),
        Err(_) => (serde_json::Value::String(raw.to_string()), None),
    };
    Answer {
        name: name.to_string(),
        status,
        headers,
        body,
        raw: kept,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers() -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    #[test]
    fn a_json_body_is_kept_as_json() {
        let answer = answer("n", 200, headers(), r#"[{"id":1}]"#);
        assert_eq!(answer.body, serde_json::json!([{"id": 1}]));
        assert!(answer.raw.is_none(), "nothing was lost, so nothing is kept");
    }

    /// The bytes are kept when they say something the parsed body does
    /// not, so that a difference in spacing or key order can be seen
    /// even though it is not a difference in what was said.
    #[test]
    fn bytes_that_do_not_round_trip_are_kept() {
        let answer = answer("n", 200, headers(), "[ {\"id\": 1} ]");
        assert_eq!(answer.body, serde_json::json!([{"id": 1}]));
        assert_eq!(answer.raw.as_deref(), Some("[ {\"id\": 1} ]"));
    }

    #[test]
    fn a_body_that_is_not_json_is_kept_as_text() {
        let answer = answer("n", 200, headers(), "id,title\n1,walk\n");
        assert_eq!(
            answer.body,
            serde_json::Value::String("id,title\n1,walk\n".to_string())
        );
    }

    #[test]
    fn no_body_at_all_is_null_rather_than_an_empty_string() {
        let answer = answer("n", 204, headers(), "");
        assert_eq!(answer.body, serde_json::Value::Null);
    }

    fn case(path: &str) -> Case {
        Case {
            name: "n".to_string(),
            feature: "f".to_string(),
            method: "GET".to_string(),
            path: path.to_string(),
            key: Key::Anon,
            headers: BTreeMap::new(),
            body: None,
            note: None,
            writes: false,
            chained: false,
            holds: BTreeMap::new(),
            volatile: Vec::new(),
        }
    }

    fn bare(strip: Option<&str>) -> Target {
        Target::new(
            "n",
            "http://127.0.0.1:3000",
            Keys::default(),
            None,
            strip.map(str::to_string),
        )
    }

    #[test]
    fn a_target_that_has_the_prefix_is_asked_for_the_path_as_written() {
        assert_eq!(
            bare(None).path_of(&case("/rest/v1/todos")),
            "/rest/v1/todos"
        );
    }

    #[test]
    fn a_target_without_the_prefix_is_asked_for_what_is_left() {
        let target = bare(Some("/rest/v1"));
        assert_eq!(
            target.path_of(&case("/rest/v1/todos?id=eq.1")),
            "/todos?id=eq.1"
        );
        assert_eq!(target.path_of(&case("/rest/v1/")), "/");
        assert_eq!(target.path_of(&case("/rest/v1")), "/");
    }

    #[test]
    fn the_characters_a_url_cannot_carry_are_escaped_and_the_rest_are_not() {
        assert_eq!(
            encoded("/rest/v1/tasks?id=eq.1&order=id"),
            "/rest/v1/tasks?id=eq.1&order=id"
        );
        assert_eq!(
            encoded("/p?profile->>city=eq.a"),
            "/p?profile-%3E%3Ecity=eq.a"
        );
        assert_eq!(encoded("/p?tags=cs.{roads}"), "/p?tags=cs.%7Broads%7D");
        assert_eq!(encoded("/p?a=b c"), "/p?a=b%20c");
    }

    /// Otherwise a case could never send an escape of its own.
    #[test]
    fn a_percent_that_is_already_there_is_left_alone() {
        assert_eq!(encoded("/p?title=like.%2A"), "/p?title=like.%2A");
    }

    /// What a location is held as: the path of it. The reference
    /// answers an absolute url and every target is asked on a host of
    /// its own, so only the path survives.
    #[test]
    fn a_case_that_follows_a_url_is_sent_to_the_path_of_it() {
        let at = "http://127.0.0.1:54321/storage/v1/upload/resumable/bm90ZXM";
        let held = holding(
            &case("n"),
            &BTreeMap::from([(LOCATION.to_string(), at.to_string())]),
            "",
        );
        assert_eq!(
            filled(&case("{location}"), &held, &names())
                .expect("follows")
                .path,
            "/storage/v1/upload/resumable/bm90ZXM"
        );
    }

    /// A location that is already a path is one, since nothing says a
    /// server has to send the other kind.
    #[test]
    fn a_location_that_is_already_a_path_is_used_as_it_stands() {
        let at = "/storage/v1/upload/resumable/bm90ZXM";
        let held = holding(
            &case("n"),
            &BTreeMap::from([(LOCATION.to_string(), at.to_string())]),
            "",
        );
        assert_eq!(held.get(LOCATION).map(String::as_str), Some(at));
    }

    #[test]
    fn a_case_that_names_something_nothing_held_is_an_error_rather_than_a_request() {
        let nothing = BTreeMap::new();
        assert!(filled(&case("{location}"), &nothing, &names()).is_err());
        assert_eq!(
            filled(&case("/storage/v1/bucket"), &nothing, &names())
                .expect("nothing to follow")
                .path,
            "/storage/v1/bucket"
        );
    }

    /// A body full of braces is a json body rather than a body full of
    /// placeholders, so only the names some case said it would hold are
    /// replaced.
    #[test]
    fn a_brace_that_is_not_a_name_a_case_holds_is_left_where_it_is() {
        let mut asking = case("/storage/v1/s3/notes/big?uploadId={upload}");
        asking.body = Some("{\"prefix\":\"{unheld}\"} {tag}".to_string());
        let held = BTreeMap::from([
            ("upload".to_string(), "abc123".to_string()),
            ("tag".to_string(), "\"5eb63bbb\"".to_string()),
        ]);
        let names = BTreeSet::from(["upload".to_string(), "tag".to_string()]);
        let filled = filled(&asking, &held, &names).expect("held both");
        assert_eq!(filled.path, "/storage/v1/s3/notes/big?uploadId=abc123");
        assert_eq!(
            filled.body.as_deref(),
            Some("{\"prefix\":\"{unheld}\"} \"5eb63bbb\"")
        );
    }

    /// Where a hold reads from, which is one of two halves of an answer.
    #[test]
    fn a_hold_reads_a_header_or_an_element() {
        let headers = BTreeMap::from([("etag".to_string(), "\"5eb63bbb\"".to_string())]);
        let body = "<InitiateMultipartUploadResult><UploadId>abc/123</UploadId>\
                    </InitiateMultipartUploadResult>";
        assert_eq!(
            read_out(&headers, body, "header:etag"),
            Some("\"5eb63bbb\"".to_string())
        );
        assert_eq!(
            read_out(&headers, body, "element:UploadId"),
            Some("abc/123".to_string())
        );
        assert_eq!(read_out(&headers, body, "element:Missing"), None);
        assert_eq!(read_out(&headers, body, "header:missing"), None);
    }

    /// A case that holds nothing holds nothing, and a hold that found
    /// nothing is left unheld rather than held empty.
    #[test]
    fn a_hold_that_finds_nothing_holds_nothing() {
        let mut asking = case("n");
        asking.holds = BTreeMap::from([("upload".to_string(), "element:UploadId".to_string())]);
        assert!(holding(&asking, &BTreeMap::new(), "<Error/>").is_empty());
    }

    fn names() -> BTreeSet<String> {
        BTreeSet::from([LOCATION.to_string()])
    }

    /// Prefixes come off whole. A path that merely starts with the same
    /// letters is a different path.
    #[test]
    fn a_path_that_only_looks_like_the_prefix_is_left_alone() {
        let target = bare(Some("/rest/v1"));
        assert_eq!(
            target.path_of(&case("/rest/v100/todos")),
            "/rest/v100/todos"
        );
        assert_eq!(target.path_of(&case("/auth/v1/health")), "/auth/v1/health");
    }
}
