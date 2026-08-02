//! Something that answers requests, and how to get a suite's schema
//! into the database behind it.
//!
//! A target is a url and two keys. Hosted Supabase, `supabase start`,
//! a bare PostgREST, and zou are all the same thing from here, which is
//! the point: the runner has no idea which it is talking to, so it
//! cannot accidentally be kinder to one of them.
//!
//! The keys are JWTs signed with the target's own secret. Give every
//! target the same secret and the same keys go to all of them, and then
//! the requests really are the same requests rather than requests that
//! happen to mean the same thing.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::suite::{Answer, Case, Key};

pub struct Target {
    /// What the report calls it.
    pub name: String,
    /// No trailing slash.
    pub url: String,
    pub anon_key: Option<String>,
    pub authenticated_key: Option<String>,
    pub service_key: Option<String>,
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
pub const COMPARED: [&str; 8] = [
    "allow",
    "content-profile",
    "content-range",
    "content-type",
    "location",
    "preference-applied",
    "retry-after",
    "www-authenticate",
];

impl Target {
    pub fn new(
        name: &str,
        url: &str,
        anon_key: Option<String>,
        authenticated_key: Option<String>,
        service_key: Option<String>,
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
            anon_key,
            authenticated_key,
            service_key,
            dsn,
            strip: strip.filter(|s| !s.is_empty()),
            agent,
        }
    }

    fn key_for(&self, key: Key) -> Option<&str> {
        match key {
            Key::Anon => self.anon_key.as_deref(),
            Key::Authenticated => self.authenticated_key.as_deref(),
            Key::Service => self.service_key.as_deref(),
            Key::None => None,
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
    pub fn send(&self, case: &Case) -> Result<Answer, String> {
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
        for (name, value) in &case.headers {
            request = request.header(name, value);
        }
        let body = case.body.clone().unwrap_or_default();
        let request = request
            .body(body)
            .map_err(|e| format!("{}: building {} {}: {e}", self.name, case.method, url))?;
        let response = self
            .agent
            .run(request)
            .map_err(|e| format!("{}: {} {}: {e}", self.name, case.method, url))?;

        let status = response.status().as_u16();
        let mut headers = BTreeMap::new();
        for name in COMPARED {
            if let Some(value) = response.headers().get(name)
                && let Ok(value) = value.to_str()
            {
                headers.insert(name.to_string(), value.to_string());
            }
        }
        let raw = response
            .into_body()
            .read_to_string()
            .map_err(|e| format!("{}: reading the body of {url}: {e}", self.name))?;
        Ok(answer(&case.name, status, headers, &raw))
    }

    /// The suite's schema, applied to whatever database this target
    /// reads. Statements run in one batch, so the file can hold plpgsql
    /// blocks and dollar quoting without being split up here.
    pub fn set_up(&self, setup: &str) -> Result<(), String> {
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
                .map_err(|e| format!("{}: connecting to set up: {e}", self.name))?;
            let task = tokio::spawn(connection);
            let result = client
                .batch_execute(setup)
                .await
                .map_err(|e| format!("{}: setup.sql: {}", self.name, because(&e)));
            drop(client);
            let _ = task.await;
            result
        })
    }
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
        }
    }

    fn bare(strip: Option<&str>) -> Target {
        Target::new(
            "n",
            "http://127.0.0.1:3000",
            None,
            None,
            None,
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
