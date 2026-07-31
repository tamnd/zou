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
use std::time::{SystemTime, UNIX_EPOCH};

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
    pub dialect: Dialect,
}

pub struct S3Store {
    agent: ureq::Agent,
    cfg: S3Config,
    host: String,
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
        Self { agent, cfg, host }
    }

    fn io(key: &str, msg: String) -> CasError {
        CasError::Io {
            key: key.to_string(),
            source: std::io::Error::other(msg),
        }
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
        let (amz_date, datestamp) = amz_timestamp(SystemTime::now());
        // The condition headers are signed too. SigV4 allows signing any
        // header, and GCS requires its x-goog-* headers in the signature.
        let mut signed_headers = vec![
            ("host".to_string(), self.host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
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
            &payload_hash,
            &amz_date,
            &datestamp,
        );

        let url = if query.is_empty() {
            format!("{}{path}", self.cfg.endpoint.trim_end_matches('/'))
        } else {
            format!("{}{path}?{query}", self.cfg.endpoint.trim_end_matches('/'))
        };
        let send = || match method {
            "GET" => {
                let mut req = self.agent.get(&url);
                for (k, v) in extra_headers {
                    req = req.header(*k, *v);
                }
                req.header("authorization", &auth)
                    .header("x-amz-content-sha256", &payload_hash)
                    .header("x-amz-date", &amz_date)
                    .call()
            }
            "PUT" => {
                let mut req = self.agent.put(&url);
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
                    .header("x-amz-content-sha256", &payload_hash)
                    .header("x-amz-date", &amz_date)
                    .send(body.unwrap_or_default())
            }
            other => unreachable!("unsupported method {other}"),
        };
        // GET is idempotent, so one retry absorbs a stale pooled
        // connection that died between requests. PUT is not retried at
        // this layer: callers own that decision.
        let result = match send() {
            Err(_) if method == "GET" => send(),
            other => other,
        };
        let res = result.map_err(|e| Self::io(err_key, format!("transport: {e}")))?;

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
            .map_err(|e| Self::io(err_key, format!("reading body: {e}")))?;
        Ok((status, version, data))
    }
}

fn error_snippet(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    text.chars().take(300).collect()
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
            s => Err(Self::io(
                key,
                format!("GET returned {s}: {}", error_snippet(&body)),
            )),
        }
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
            s => Err(Self::io(
                key,
                format!("PUT returned {s}: {}", error_snippet(&body)),
            )),
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
                return Err(Self::io(
                    prefix,
                    format!("LIST returned {status}: {}", error_snippet(&body)),
                ));
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
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical.as_bytes())
    );
    let key = signing_key(&cfg.secret_key, datestamp, &cfg.region);
    let signature = hex(&hmac(&key, to_sign.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope},SignedHeaders={signed_names},Signature={signature}",
        cfg.access_key
    )
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
}
