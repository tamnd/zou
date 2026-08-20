//! S3 compatible CAS backend, behind the `s3` feature.
//!
//! One client covers AWS S3, MinIO, R2, and GCS. The first three share
//! the wire API and, since 2025, conditional writes: `If-None-Match: *`
//! creates and `If-Match: <etag>` swaps, which is exactly the primitive
//! the manifest CAS needs, so no coordination service enters the picture
//! here either. GCS accepts the same SigV4 signing with HMAC interop
//! keys on its XML API and spells the same preconditions
//! `x-goog-if-generation-match`, selected via [`Dialect`].
//!
//! The client is hand rolled on ureq and rustls rather than aws-sdk-s3.
//! The SDK drags in tokio, and zou-store stays runtime free so the
//! embedded core links like SQLite. SigV4 plus three request shapes is a
//! few hundred lines, and the signer is checked against the AWS
//! documented test vectors below.
//!
//! Versions are the backend's ETags, opaque as the trait demands. Do not
//! expect them to equal the content hashes the local backend produces.

use std::io::Read;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

use crate::cas::{CasError, CasStore, Version};

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// How the endpoint expresses conditional writes and versions. The wire
/// format and signing are otherwise identical, GCS accepts SigV4 with
/// HMAC interop keys on its XML API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Dialect {
    /// ETag versions, If-Match and If-None-Match preconditions. AWS S3,
    /// MinIO, R2.
    #[default]
    S3,
    /// Generation number versions, x-goog-if-generation-match with 0
    /// meaning "must not exist". Google Cloud Storage.
    Gcs,
}

/// Connection settings for one bucket. Path style addressing is used
/// throughout because MinIO requires it and AWS, R2, and GCS accept it.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Scheme plus authority, no trailing slash: `https://s3.us-east-1.amazonaws.com`
    /// for AWS, `http://127.0.0.1:9000` for a local MinIO,
    /// `https://<account>.r2.cloudflarestorage.com` for R2,
    /// `https://storage.googleapis.com` for GCS.
    pub endpoint: String,
    /// `us-east-1` style region. MinIO accepts anything, R2 and GCS want
    /// `auto`.
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    /// The third part of a temporary credential, from a role rather
    /// than a user: an instance profile, an ECS task role, or the
    /// environment a Lambda function is handed. The key pair alone is
    /// not enough to sign with, the token has to travel with it, in the
    /// signature on a request and in the query of a presigned url.
    pub session: Option<String>,
    pub dialect: Dialect,
}

/// Total attempts per request, so up to three retries after the first.
const MAX_ATTEMPTS: u32 = 4;

/// Statuses worth retrying: throttling and transient server trouble.
/// Everything else means what it says and goes straight to the caller.
fn retryable(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

pub struct S3Store {
    agent: ureq::Agent,
    cfg: S3Config,
    host: String,
    retry_base: Duration,
}

impl S3Store {
    pub fn new(cfg: S3Config) -> Self {
        let host = cfg
            .endpoint
            .split_once("://")
            .map_or(cfg.endpoint.as_str(), |(_, rest)| rest)
            .trim_end_matches('/')
            .to_string();
        // Non 2xx statuses carry CAS meaning here, so they must come back
        // as responses, not transport errors.
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        // First backoff step, doubling per retry. The env knob exists so
        // fault injection tests can run a fast schedule.
        let retry_base = crate::setting::number("ZOU_S3_RETRY_BASE_MS", "a number of milliseconds")
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(100));
        Self {
            agent,
            cfg,
            host,
            retry_base,
        }
    }

    fn io(key: &str, msg: String) -> CasError {
        CasError::Io {
            key: key.to_string(),
            source: std::io::Error::other(msg),
        }
    }

    /// A request that never got an answer. The underlying error says
    /// what the socket did and nothing about where it was pointed, and
    /// where it was pointed is the whole question, so the endpoint goes
    /// in. Retries are named too, since "it failed" and "it failed four
    /// times over a couple of seconds" call for different next moves,
    /// and a PUT says plainly that it is not known whether the write
    /// landed, because that is the one thing a caller must not assume.
    fn transport(&self, key: &str, method: &str, e: &str) -> CasError {
        let retried = match method == "GET" || method == "DELETE" {
            true => format!(", after {MAX_ATTEMPTS} attempts"),
            false => String::new(),
        };
        let ambiguity = match method == "PUT" {
            true => {
                ", and a PUT that died on the wire may or may not have landed, so this is not retried here and a reader deciding what to do next has to look at the object rather than assume either way"
            }
            false => "",
        };
        Self::io(
            key,
            format!(
                "transport: {e}{retried}, talking to {}{ambiguity}, so check that the endpoint is reachable from this node and that ZOU_S3_ENDPOINT names the one you meant",
                self.cfg.endpoint
            ),
        )
    }

    /// The caller's headers plus the session token, when there is one.
    ///
    /// A temporary credential's token is a header like any other, so it
    /// rides along with the conditions and is signed and sent by the
    /// same code rather than by a special case in every method.
    fn with_token<'a>(&'a self, extra: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
        let mut headers = extra.to_vec();
        if let Some(token) = self.cfg.session.as_deref() {
            headers.push(("x-amz-security-token", token));
        }
        headers
    }

    fn object_path(&self, key: &str) -> String {
        format!(
            "/{}",
            uri_encode(&format!("{}/{key}", self.cfg.bucket), false)
        )
    }

    /// One signed request. `path` and `query` must already be in canonical
    /// encoded form, the same bytes go on the wire and into the signature.
    fn request(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Option<&[u8]>,
        extra_headers: &[(&str, &str)],
        err_key: &str,
    ) -> Result<(u16, Option<String>, Vec<u8>), CasError> {
        let payload_hash = body.map_or_else(|| EMPTY_SHA256.to_string(), sha256_hex);
        let url = if query.is_empty() {
            format!("{}{path}", self.cfg.endpoint.trim_end_matches('/'))
        } else {
            format!("{}{path}?{query}", self.cfg.endpoint.trim_end_matches('/'))
        };
        // GET and DELETE are idempotent, so transport errors retry, which
        // also absorbs a stale pooled connection that died between
        // requests. A PUT that dies on the wire is not retried at this
        // layer: the outcome is ambiguous and callers own that decision.
        // Throttling and 5xx answers retry for every method with
        // exponential backoff, bounded so commits stall visibly rather
        // than hang. Signing happens per attempt to keep the timestamp
        // fresh.
        let idempotent = method == "GET" || method == "DELETE";
        let mut attempt = 0;
        loop {
            attempt += 1;
            let retry = |what: &str| {
                if attempt >= MAX_ATTEMPTS {
                    return None;
                }
                let wait = self.retry_base * 2u32.pow(attempt - 1);
                log::warn!("s3 {method} {err_key}: {what}, retrying in {wait:?}");
                Some(wait)
            };
            let (status, version, data) = match self.attempt(
                method,
                path,
                query,
                &url,
                body,
                extra_headers,
                &payload_hash,
            ) {
                Ok(answer) => answer,
                Err(e) if idempotent => match retry(&format!("transport: {e}")) {
                    // The first transport retry goes immediately, a dead
                    // pooled connection needs no backoff.
                    Some(wait) => {
                        if attempt > 1 {
                            std::thread::sleep(wait);
                        }
                        continue;
                    }
                    None => return Err(self.transport(err_key, method, &e)),
                },
                Err(e) => return Err(self.transport(err_key, method, &e)),
            };
            if retryable(status)
                && let Some(wait) = retry(&format!("status {status}"))
            {
                std::thread::sleep(wait);
                continue;
            }
            return Ok((status, version, data));
        }
    }

    /// One signed attempt: build the signature, send, read the body.
    /// Transport and body read failures come back as one error kind so
    /// the retry loop above can treat them alike.
    #[allow(clippy::too_many_arguments)]
    fn attempt(
        &self,
        method: &str,
        path: &str,
        query: &str,
        url: &str,
        body: Option<&[u8]>,
        extra_headers: &[(&str, &str)],
        payload_hash: &str,
    ) -> Result<(u16, Option<String>, Vec<u8>), String> {
        let (amz_date, datestamp) = amz_timestamp(SystemTime::now());
        let extra = self.with_token(extra_headers);
        let extra_headers = &extra[..];
        // The condition headers are signed too. SigV4 allows signing any
        // header, and GCS requires its x-goog-* headers in the signature.
        let mut signed_headers = vec![
            ("host".to_string(), self.host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        for (k, v) in extra_headers {
            signed_headers.push((k.to_string(), v.to_string()));
        }
        signed_headers.sort();
        let auth = authorization(
            &self.cfg,
            method,
            path,
            query,
            &signed_headers,
            payload_hash,
            &amz_date,
            &datestamp,
        );
        let result = match method {
            "GET" => {
                let mut req = self.agent.get(url);
                for (k, v) in extra_headers {
                    req = req.header(*k, *v);
                }
                req.header("authorization", &auth)
                    .header("x-amz-content-sha256", payload_hash)
                    .header("x-amz-date", &amz_date)
                    .call()
            }
            "PUT" => {
                let mut req = self.agent.put(url);
                for (k, v) in extra_headers {
                    req = req.header(*k, *v);
                }
                // Servers answering a conditional PUT with 412 tend to
                // close the connection early, and a pooled dead socket
                // then resets whatever request reuses it. Every PUT here
                // is conditional, so ask for a fresh close instead of
                // poisoning the pool.
                req.header("connection", "close")
                    .header("authorization", &auth)
                    .header("x-amz-content-sha256", payload_hash)
                    .header("x-amz-date", &amz_date)
                    .send(body.unwrap_or_default())
            }
            "DELETE" => {
                let mut req = self.agent.delete(url);
                for (k, v) in extra_headers {
                    req = req.header(*k, *v);
                }
                req.header("authorization", &auth)
                    .header("x-amz-content-sha256", payload_hash)
                    .header("x-amz-date", &amz_date)
                    .call()
            }
            other => unreachable!("unsupported method {other}"),
        };
        let res = result.map_err(|e| e.to_string())?;

        let status = res.status().as_u16();
        let version_header = match self.cfg.dialect {
            Dialect::S3 => "etag",
            Dialect::Gcs => "x-goog-generation",
        };
        let version = res
            .headers()
            .get(version_header)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let mut data = Vec::new();
        res.into_body()
            .into_reader()
            .read_to_end(&mut data)
            .map_err(|e| format!("reading body: {e}"))?;
        Ok((status, version, data))
    }
}

fn error_snippet(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    text.chars().take(300).collect()
}

/// What a status that got this far means for whoever is reading the log.
///
/// The statuses a caller handles never reach here: a 404 on a GET is an
/// absent object, a 412 is a lost race, a 5xx has already been retried
/// to the attempt limit. What is left is a request the store understood
/// and refused, and for those the body is written for whoever operates
/// the store rather than for whoever configured this one. So the reading
/// goes next to it, because the difference between a wrong key, a wrong
/// bucket and a wrong region is invisible in an XML snippet and is the
/// only thing the reader needs.
///
/// An empty reading is the honest answer for a status with no single
/// cause worth naming, and the caller leaves the sentence where it was.
fn what_it_means(status: u16) -> &'static str {
    match status {
        400 => {
            ", the store understood the request and rejected its shape, which usually means it does not speak this s3 dialect, and ZOU_STORE_DIALECT picks another"
        }
        401 | 403 => {
            ", the signature was refused, so it is AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or the pair is right and lacks this permission on this bucket, or the clock on this node is more than a few minutes off, since a signature carries its timestamp"
        }
        404 => {
            ", a 404 on this path is the bucket and not the object, since an absent object is an ordinary answer and never reaches here, so ZOU_S3_ENDPOINT and the bucket named in the target are pointing at something that is not there"
        }
        301 | 307 => {
            ", the bucket lives in another region, which ZOU_S3_REGION sets and which has to match the endpoint"
        }
        413 => ", the store refused the size of this object rather than the request itself",
        _ => "",
    }
}

/// The message for a status that reached a caller, with the reading
/// appended.
fn refused(what: &str, status: u16, body: &[u8]) -> String {
    format!(
        "{what} returned {status}: {}{}",
        error_snippet(body),
        what_it_means(status)
    )
}

impl CasStore for S3Store {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        let path = self.object_path(key);
        let (status, version, body) = self.request("GET", &path, "", None, &[], key)?;
        match status {
            200 => {
                let version =
                    version.ok_or_else(|| Self::io(key, "response without a version".into()))?;
                Ok(Some((body, Version::from_backend(version))))
            }
            404 => Ok(None),
            s => Err(Self::io(key, refused("GET", s, &body))),
        }
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
        if len == 0 {
            return Ok(self.get(key)?.map(|_| Vec::new()));
        }
        let path = self.object_path(key);
        let range = format!("bytes={}-{}", offset, offset.saturating_add(len) - 1);
        let (status, _, body) =
            self.request("GET", &path, "", None, &[("range", range.as_str())], key)?;
        match status {
            // 200 means the backend ignored the range and sent it all.
            206 => Ok(Some(body)),
            200 => {
                let start = (offset as usize).min(body.len());
                let end = (offset.saturating_add(len) as usize).min(body.len());
                Ok(Some(body[start..end].to_vec()))
            }
            404 => Ok(None),
            // The range starts at or past the object's end.
            416 => Ok(Some(Vec::new())),
            s => Err(Self::io(key, refused("ranged GET", s, &body))),
        }
    }

    /// A query signed GET, which is the same signature the header
    /// signer makes over a canonical request that carries the
    /// credentials in the query string and reads `UNSIGNED-PAYLOAD` for
    /// its body hash.
    ///
    /// Only host is signed. Whoever follows this url is a browser or a
    /// curl and sends whatever headers it likes, and a url that only
    /// works from one client is not a url.
    ///
    /// No request is made, so this answers for a key that is not there
    /// too, and the url then produces the backend's own not found when
    /// it is followed.
    fn presigned_get(
        &self,
        key: &str,
        ttl: Duration,
        response: &[(&str, &str)],
    ) -> Result<Option<String>, CasError> {
        let (amz_date, datestamp) = amz_timestamp(SystemTime::now());
        let scope = format!("{datestamp}/{}/s3/aws4_request", self.cfg.region);
        // A week is the longest S3 signs for, and a zero second url is
        // one nobody can spend, so both ends are pulled into range
        // rather than refused: the caller asked for a download, not for
        // an argument about durations.
        let seconds = ttl.as_secs().clamp(1, 604_800);
        let mut params = vec![
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_string()),
            (
                "X-Amz-Credential",
                format!("{}/{scope}", self.cfg.access_key),
            ),
            ("X-Amz-Date", amz_date.clone()),
            ("X-Amz-Expires", seconds.to_string()),
            ("X-Amz-SignedHeaders", "host".to_string()),
        ];
        // Temporary credentials carry their token in the query here,
        // since a browser following the url sends no headers of ours.
        if let Some(token) = self.cfg.session.as_deref() {
            params.push(("X-Amz-Security-Token", token.to_string()));
        }
        params.extend(response.iter().map(|(k, v)| (*k, v.to_string())));
        // Canonical order is by encoded name, and every pair is encoded
        // once, here, so what goes on the wire is what was signed.
        let mut encoded: Vec<(String, String)> = params
            .iter()
            .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
            .collect();
        encoded.sort();
        let query = encoded
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let path = self.object_path(key);
        let canonical = format!(
            "GET\n{path}\n{query}\nhost:{}\n\nhost\nUNSIGNED-PAYLOAD",
            self.host
        );
        let signature = signature(&self.cfg, &canonical, &amz_date, &datestamp, &scope);
        Ok(Some(format!(
            "{}{path}?{query}&X-Amz-Signature={signature}",
            self.cfg.endpoint.trim_end_matches('/')
        )))
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        let path = self.object_path(key);
        let cond: (&str, &str) = match (self.cfg.dialect, expected) {
            (Dialect::S3, Some(v)) => ("if-match", v.as_str()),
            (Dialect::S3, None) => ("if-none-match", "*"),
            (Dialect::Gcs, Some(v)) => ("x-goog-if-generation-match", v.as_str()),
            (Dialect::Gcs, None) => ("x-goog-if-generation-match", "0"),
        };
        let (status, version, body) = self.request("PUT", &path, "", Some(data), &[cond], key)?;
        match status {
            200 => {
                let version = version
                    .ok_or_else(|| Self::io(key, "put response without a version".into()))?;
                Ok(Version::from_backend(version))
            }
            // 412 is the precondition failing, 409 is S3 reporting a
            // concurrent conditional writer, 404 is If-Match against a key
            // that no longer exists. All mean the same thing to CAS: the
            // caller's view is stale, re-read and decide again.
            409 | 412 => Err(CasError::Conflict {
                key: key.to_string(),
            }),
            404 if expected.is_some() => Err(CasError::Conflict {
                key: key.to_string(),
            }),
            s => Err(Self::io(key, refused("PUT", s, &body))),
        }
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        let path = self.object_path(key);
        let (status, version, body) = self.request("PUT", &path, "", Some(data), &[], key)?;
        match status {
            200 => {
                let version = version
                    .ok_or_else(|| Self::io(key, "put response without a version".into()))?;
                Ok(Version::from_backend(version))
            }
            s => Err(Self::io(key, refused("PUT", s, &body))),
        }
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        let path = self.object_path(key);
        let (status, _, body) = self.request("DELETE", &path, "", None, &[], key)?;
        match status {
            // 204 on both dialects, and 404 when already gone, which
            // delete treats as success.
            204 | 404 => Ok(()),
            s => Err(Self::io(key, refused("DELETE", s, &body))),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        let path = format!("/{}", uri_encode(&self.cfg.bucket, false));
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            // Built in canonical order: query params sorted by name.
            let mut query = String::new();
            if let Some(t) = &token {
                query.push_str(&format!("continuation-token={}&", uri_encode(t, true)));
            }
            query.push_str(&format!("list-type=2&prefix={}", uri_encode(prefix, true)));
            let (status, _, body) = self.request("GET", &path, &query, None, &[], prefix)?;
            if status != 200 {
                return Err(Self::io(prefix, refused("LIST", status, &body)));
            }
            let text = String::from_utf8_lossy(&body);
            keys.extend(xml_values(&text, "Key"));
            token = if xml_value(&text, "IsTruncated").as_deref() == Some("true") {
                xml_value(&text, "NextContinuationToken")
            } else {
                None
            };
            if token.is_none() {
                break;
            }
        }
        keys.sort();
        Ok(keys)
    }
}

/// AWS Signature Version 4 over the headers in `signed` (which must be
/// sorted by name and include host, x-amz-content-sha256, x-amz-date).
#[allow(clippy::too_many_arguments)]
fn authorization(
    cfg: &S3Config,
    method: &str,
    path: &str,
    query: &str,
    signed: &[(String, String)],
    payload_hash: &str,
    amz_date: &str,
    datestamp: &str,
) -> String {
    let signed_names: Vec<&str> = signed.iter().map(|(k, _)| k.as_str()).collect();
    let signed_names = signed_names.join(";");
    let canonical_headers: String = signed.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let canonical =
        format!("{method}\n{path}\n{query}\n{canonical_headers}\n{signed_names}\n{payload_hash}");
    let scope = format!("{datestamp}/{}/s3/aws4_request", cfg.region);
    let signature = signature(cfg, &canonical, amz_date, datestamp, &scope);
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope},SignedHeaders={signed_names},Signature={signature}",
        cfg.access_key
    )
}

/// The signature over a canonical request.
///
/// The header signer and the query signer build different canonical
/// requests and write the answer in different places. Everything
/// between the two is this.
fn signature(
    cfg: &S3Config,
    canonical: &str,
    amz_date: &str,
    datestamp: &str,
    scope: &str,
) -> String {
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical.as_bytes())
    );
    let key = signing_key(&cfg.secret_key, datestamp, &cfg.region);
    hex(&hmac(&key, to_sign.as_bytes()))
}

fn signing_key(secret: &str, datestamp: &str, region: &str) -> Vec<u8> {
    let k = hmac(format!("AWS4{secret}").as_bytes(), datestamp.as_bytes());
    let k = hmac(&k, region.as_bytes());
    let k = hmac(&k, b"s3");
    hmac(&k, b"aws4_request")
}

fn hmac(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(msg);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex(&h.finalize())
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// S3's strict percent encoding: unreserved characters pass, everything
/// else becomes %XX, and '/' passes only in paths.
fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// `20130524T000000Z` and `20130524` from a wall clock instant, days to
/// civil date via the standard Gregorian era arithmetic.
fn amz_timestamp(now: SystemTime) -> (String, String) {
    let secs = now
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the unix epoch")
        .as_secs() as i64;
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let datestamp = format!("{y:04}{m:02}{d:02}");
    let amz_date = format!(
        "{datestamp}T{:02}{:02}{:02}Z",
        tod / 3600,
        (tod % 3600) / 60,
        tod % 60
    );
    (amz_date, datestamp)
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (y + i64::from(m <= 2), m as u32, d as u32)
}

fn xml_values(body: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(i) = rest.find(&open) {
        rest = &rest[i + open.len()..];
        let Some(j) = rest.find(&close) else { break };
        out.push(xml_unescape(&rest[..j]));
        rest = &rest[j + close.len()..];
    }
    out
}

fn xml_value(body: &str, tag: &str) -> Option<String> {
    xml_values(body, tag).into_iter().next()
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example_cfg() -> S3Config {
        S3Config {
            endpoint: "https://examplebucket.s3.amazonaws.com".into(),
            region: "us-east-1".into(),
            bucket: "examplebucket".into(),
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session: None,
            dialect: Dialect::S3,
        }
    }

    fn signed_headers(extra: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut h = vec![
            (
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            ),
            ("x-amz-content-sha256".to_string(), EMPTY_SHA256.to_string()),
            ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
        ];
        for (k, v) in extra {
            h.push((k.to_string(), v.to_string()));
        }
        h.sort();
        h
    }

    /// The GET object example from the AWS SigV4 documentation.
    #[test]
    fn signer_matches_the_aws_get_object_vector() {
        let auth = authorization(
            &example_cfg(),
            "GET",
            "/test.txt",
            "",
            &signed_headers(&[("range", "bytes=0-9")]),
            EMPTY_SHA256,
            "20130524T000000Z",
            "20130524",
        );
        assert!(
            auth.ends_with("f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"),
            "{auth}"
        );
        assert!(auth.contains("SignedHeaders=host;range;x-amz-content-sha256;x-amz-date"));
    }

    /// The GET bucket listing example, which exercises the query string
    /// canonicalization.
    #[test]
    fn signer_matches_the_aws_list_objects_vector() {
        let auth = authorization(
            &example_cfg(),
            "GET",
            "/",
            "max-keys=2&prefix=J",
            &signed_headers(&[]),
            EMPTY_SHA256,
            "20130524T000000Z",
            "20130524",
        );
        assert!(
            auth.ends_with("34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"),
            "{auth}"
        );
    }

    /// GCS requires its x-goog-* headers inside the signature, so the
    /// condition header must appear in SignedHeaders in sorted position.
    #[test]
    fn gcs_condition_headers_are_signed() {
        let auth = authorization(
            &example_cfg(),
            "PUT",
            "/obj",
            "",
            &signed_headers(&[("x-goog-if-generation-match", "42")]),
            EMPTY_SHA256,
            "20130524T000000Z",
            "20130524",
        );
        assert!(auth.contains(
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-goog-if-generation-match"
        ));
    }

    /// A role's token has to be in the signature, not just on the wire,
    /// or S3 answers 403 to every request a function makes.
    #[test]
    fn a_session_token_is_signed_along_with_the_condition_headers() {
        let mut cfg = example_cfg();
        cfg.session = Some("IQoJb3JpZ2luX2VjEXAMPLE".into());
        let store = S3Store::new(cfg);
        let sent = store.with_token(&[("if-none-match", "*")]);
        assert_eq!(
            sent,
            vec![
                ("if-none-match", "*"),
                ("x-amz-security-token", "IQoJb3JpZ2luX2VjEXAMPLE"),
            ]
        );
        let auth = authorization(
            &store.cfg,
            "PUT",
            "/obj",
            "",
            &signed_headers(&sent),
            EMPTY_SHA256,
            "20130524T000000Z",
            "20130524",
        );
        assert!(
            auth.contains(
                "SignedHeaders=host;if-none-match;x-amz-content-sha256;x-amz-date;x-amz-security-token"
            ),
            "{auth}"
        );
    }

    /// A static key pair sends no token at all, which is what every
    /// MinIO and every laptop is.
    #[test]
    fn a_static_key_pair_sends_no_token() {
        let store = S3Store::new(example_cfg());
        assert_eq!(store.with_token(&[("range", "bytes=0-9")]).len(), 1);
        let url = store
            .presigned_get("a/b", Duration::from_secs(60), &[])
            .expect("a url")
            .expect("a url");
        assert!(!url.contains("X-Amz-Security-Token"), "{url}");
    }

    /// Whoever follows a presigned url sends none of our headers, so
    /// the token has to be in the query, and signed there.
    #[test]
    fn a_presigned_url_carries_the_session_token() {
        let mut cfg = example_cfg();
        cfg.session = Some("IQoJb3JpZ2luX2VjEXAMPLE".into());
        let store = S3Store::new(cfg);
        let url = store
            .presigned_get("a/b", Duration::from_secs(60), &[])
            .expect("a url")
            .expect("a url");
        assert!(
            url.contains("X-Amz-Security-Token=IQoJb3JpZ2luX2VjEXAMPLE"),
            "{url}"
        );
        assert!(url.contains("X-Amz-Signature="), "{url}");
    }

    /// The query string authentication example from the same
    /// documentation, asked of the shared signer with the canonical
    /// request the docs print. Presigning builds that string from a key
    /// and a ttl, and this pins the arithmetic under it.
    #[test]
    fn signer_matches_the_aws_presigned_get_vector() {
        let canonical = concat!(
            "GET\n",
            "/test.txt\n",
            "X-Amz-Algorithm=AWS4-HMAC-SHA256",
            "&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request",
            "&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\n",
            "host:examplebucket.s3.amazonaws.com\n",
            "\n",
            "host\n",
            "UNSIGNED-PAYLOAD"
        );
        assert_eq!(
            signature(
                &example_cfg(),
                canonical,
                "20130524T000000Z",
                "20130524",
                "20130524/us-east-1/s3/aws4_request",
            ),
            "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
        );
    }

    /// What a presigned url is made of, asked of the real one.
    ///
    /// The signature itself moves with the clock, so what is pinned
    /// here is everything else: the five parameters in canonical order,
    /// the response overrides signed alongside them, and the fact that
    /// changing one of those changes the answer, which is what stops a
    /// url from being edited into a different download.
    #[test]
    fn a_presigned_url_carries_what_it_signed() {
        let store = S3Store::new(example_cfg());
        let plain = store
            .presigned_get("files/a.png", Duration::from_secs(60), &[])
            .unwrap()
            .expect("s3 can presign");
        assert!(
            plain.starts_with("https://examplebucket.s3.amazonaws.com/examplebucket/files/a.png?"),
            "{plain}"
        );
        let query = plain.split_once('?').unwrap().1;
        let names: Vec<&str> = query
            .split('&')
            .map(|p| p.split_once('=').unwrap().0)
            .collect();
        assert_eq!(
            names,
            [
                "X-Amz-Algorithm",
                "X-Amz-Credential",
                "X-Amz-Date",
                "X-Amz-Expires",
                "X-Amz-SignedHeaders",
                "X-Amz-Signature",
            ],
            "canonical order, with the signature last because it is over the rest"
        );
        assert!(query.contains("X-Amz-Expires=60"), "{query}");

        let named = store
            .presigned_get(
                "files/a.png",
                Duration::from_secs(60),
                &[(
                    "response-content-disposition",
                    "attachment; filename=\"a b\"",
                )],
            )
            .unwrap()
            .unwrap();
        assert!(
            named.contains("response-content-disposition=attachment%3B%20filename%3D%22a%20b%22"),
            "{named}"
        );
        assert_ne!(
            plain.split("X-Amz-Signature=").nth(1),
            named.split("X-Amz-Signature=").nth(1),
            "the override is signed, so it cannot be added to a url afterwards"
        );
    }

    /// A week is as long as S3 signs for, and a url nobody can spend is
    /// not worth refusing a download over.
    #[test]
    fn a_ttl_out_of_range_is_pulled_into_it() {
        let store = S3Store::new(example_cfg());
        let with = |ttl| {
            store
                .presigned_get("k", ttl, &[])
                .unwrap()
                .unwrap()
                .split('&')
                .find(|p| p.starts_with("X-Amz-Expires="))
                .unwrap()
                .to_string()
        };
        assert_eq!(with(Duration::from_secs(0)), "X-Amz-Expires=1");
        assert_eq!(with(Duration::from_secs(999_999)), "X-Amz-Expires=604800");
    }

    #[test]
    fn timestamps_format_as_utc_including_leap_years() {
        let at = |secs: u64| UNIX_EPOCH + std::time::Duration::from_secs(secs);
        assert_eq!(
            amz_timestamp(at(1_369_353_600)),
            ("20130524T000000Z".to_string(), "20130524".to_string())
        );
        assert_eq!(
            amz_timestamp(at(951_782_400)),
            ("20000229T000000Z".to_string(), "20000229".to_string())
        );
        assert_eq!(
            amz_timestamp(at(1_769_903_999)),
            ("20260131T235959Z".to_string(), "20260131".to_string())
        );
    }

    #[test]
    fn only_throttling_and_transient_server_errors_retry() {
        for s in [429, 500, 502, 503, 504] {
            assert!(retryable(s), "{s} should retry");
        }
        // CAS statuses and client errors carry meaning, never retry them.
        for s in [200, 204, 206, 304, 400, 403, 404, 409, 412, 416, 501] {
            assert!(!retryable(s), "{s} must not retry");
        }
    }

    #[test]
    fn uri_encoding_follows_the_s3_rules() {
        assert_eq!(uri_encode("a b/c*!", false), "a%20b/c%2A%21");
        assert_eq!(uri_encode("a b/c*!", true), "a%20b%2Fc%2A%21");
        assert_eq!(uri_encode("A-z_0.9~", true), "A-z_0.9~");
    }

    #[test]
    fn list_responses_parse_keys_truncation_and_escapes() {
        let body = r#"<?xml version="1.0"?>
<ListBucketResult>
  <IsTruncated>true</IsTruncated>
  <Contents><Key>a/plain.wal</Key><Size>1</Size></Contents>
  <Contents><Key>b/with&amp;amp.wal</Key><Size>2</Size></Contents>
  <NextContinuationToken>tok+1/2=</NextContinuationToken>
</ListBucketResult>"#;
        assert_eq!(
            xml_values(body, "Key"),
            vec!["a/plain.wal", "b/with&amp.wal"]
        );
        assert_eq!(xml_value(body, "IsTruncated").as_deref(), Some("true"));
        assert_eq!(
            xml_value(body, "NextContinuationToken").as_deref(),
            Some("tok+1/2=")
        );
    }

    /// The status a store refuses with is the one thing that separates a
    /// wrong key from a wrong bucket from a wrong region, and the body
    /// it sends is written for whoever runs the store rather than
    /// whoever configured this one. So each of these has to name the
    /// setting the reader would go change, and naming the wrong one is
    /// worse than naming none.
    #[test]
    fn a_refusal_names_the_setting_behind_that_status() {
        let body = b"<Error><Code>SignatureDoesNotMatch</Code></Error>";

        let denied = refused("PUT", 403, body);
        assert!(denied.contains("PUT returned 403"), "{denied}");
        assert!(denied.contains("SignatureDoesNotMatch"), "{denied}");
        assert!(denied.contains("AWS_ACCESS_KEY_ID"), "{denied}");
        // The clock belongs in this one. A signature carries its
        // timestamp, so a node hours out of step gets a 403 that reads
        // exactly like a bad key and sends the reader rotating
        // credentials that were never wrong.
        assert!(denied.contains("clock"), "{denied}");

        let missing = refused("LIST", 404, body);
        assert!(missing.contains("ZOU_S3_ENDPOINT"), "{missing}");
        assert!(
            missing.contains("not the object"),
            "a 404 that reaches a caller is the bucket, and saying so is the point: {missing}"
        );

        let elsewhere = refused("GET", 301, body);
        assert!(elsewhere.contains("ZOU_S3_REGION"), "{elsewhere}");

        let dialect = refused("PUT", 400, body);
        assert!(dialect.contains("ZOU_STORE_DIALECT"), "{dialect}");

        // A status with no single cause worth naming gets no invented
        // one. The sentence ends where the body ends.
        let odd = refused("GET", 418, body);
        assert!(odd.ends_with("</Error>"), "{odd}");
    }

    /// A transport failure says nothing about where it was pointed, and
    /// where it was pointed is the entire question.
    #[test]
    fn a_transport_failure_names_the_endpoint_and_whether_it_was_retried() {
        let store = S3Store::new(example_cfg());

        let get = store
            .transport("k", "GET", "connection refused")
            .to_string();
        assert!(get.contains("connection refused"), "{get}");
        assert!(get.contains("examplebucket.s3.amazonaws.com"), "{get}");
        assert!(
            get.contains(&format!("after {MAX_ATTEMPTS} attempts")),
            "a reader deciding whether to wait needs to know this already waited: {get}"
        );

        // The one thing a caller must not assume either way. A PUT is
        // not retried here precisely because the outcome is unknown, so
        // the message that reports it has to say so.
        let put = store.transport("k", "PUT", "broken pipe").to_string();
        assert!(put.contains("may or may not have landed"), "{put}");
        assert!(
            !put.contains("attempts"),
            "a PUT is not retried at this layer and must not claim to have been: {put}"
        );
    }
}
