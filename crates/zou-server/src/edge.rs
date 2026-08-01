//! Edge concerns shared by every surface: CORS the way PostgREST
//! answers it behind Supabase, a request id on every response, and
//! the rate limit skeleton the per endpoint budgets will hang off
//! when the auth surface lands.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use axum::body::Body;
use axum::http::{HeaderValue, Method, Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// The methods PostgREST advertises on preflight.
const ALLOW_METHODS: &str = "GET, POST, PATCH, PUT, DELETE, OPTIONS, HEAD";

/// The headers PostgREST exposes to browser javascript on actual
/// responses, Content-Range being the one supabase-js reads for counts.
const EXPOSE_HEADERS: &str = "Content-Encoding, Content-Location, Content-Range, \
    Content-Type, Date, Location, Server, Transfer-Encoding, Range-Unit";

/// CORS with PostgREST's policy: mirror the Origin, allow credentials,
/// echo whatever headers the preflight asks for, cache it a day.
///
/// This sits outside the apikey gate on purpose. Browsers strip custom
/// headers from preflight requests, so an OPTIONS carrying an Origin
/// and a requested method must be answered here, never handed to a
/// gate that would 401 it for the missing apikey.
pub async fn cors(req: Request<Body>, next: Next) -> Response {
    let origin = req.headers().get(header::ORIGIN).cloned();
    let preflight = req.method() == Method::OPTIONS
        && origin.is_some()
        && req
            .headers()
            .contains_key(header::ACCESS_CONTROL_REQUEST_METHOD);
    if preflight {
        let allow_headers = req
            .headers()
            .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
            .cloned()
            .unwrap_or_else(|| HeaderValue::from_static("Authorization, Content-Type, apikey"));
        let mut res = StatusCode::NO_CONTENT.into_response();
        let h = res.headers_mut();
        h.insert(
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            origin.expect("preflight checked origin"),
        );
        h.insert(
            header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
            HeaderValue::from_static("true"),
        );
        h.insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static(ALLOW_METHODS),
        );
        h.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, allow_headers);
        h.insert(
            header::ACCESS_CONTROL_MAX_AGE,
            HeaderValue::from_static("86400"),
        );
        return res;
    }
    let mut res = next.run(req).await;
    if let Some(origin) = origin {
        let h = res.headers_mut();
        h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        h.insert(
            header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
            HeaderValue::from_static("true"),
        );
        h.insert(
            header::ACCESS_CONTROL_EXPOSE_HEADERS,
            HeaderValue::from_static(EXPOSE_HEADERS),
        );
    }
    res
}

/// The id of the request in flight, deposited in extensions so
/// handlers and logs can correlate with the x-request-id the client
/// got back.
#[derive(Clone)]
pub struct RequestId(pub std::sync::Arc<str>);

/// Tag every request: honor a sane x-request-id from the client so
/// ids survive proxies, mint a uuid otherwise, and always send it
/// back on the response.
pub async fn request_id(mut req: Request<Body>, next: Next) -> Response {
    let id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty() && v.len() <= 128 && v.chars().all(|c| c.is_ascii_graphic()))
        .map(str::to_string)
        .unwrap_or_else(fresh_id);
    req.extensions_mut()
        .insert(RequestId(std::sync::Arc::from(id.as_str())));
    let mut res = next.run(req).await;
    if let Ok(v) = HeaderValue::from_str(&id) {
        res.headers_mut().insert("x-request-id", v);
    }
    res
}

fn fresh_id() -> String {
    let mut b = [0u8; 16];
    getrandom::fill(&mut b).expect("the os rng never fails");
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: Vec<String> = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        hex[..4].join(""),
        hex[4..6].join(""),
        hex[6..8].join(""),
        hex[8..10].join(""),
        hex[10..].join("")
    )
}

/// A rate as tokens: `burst` requests can land at once, refilled at
/// `per_second`. Plain data so it can sit in Config.
#[derive(Clone, Copy)]
pub struct Rate {
    pub burst: u32,
    pub per_second: f64,
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

/// A token bucket per key. This is the skeleton the GoTrue per
/// endpoint budgets will configure later, today zou dev runs without
/// one and nothing is limited.
pub struct RateLimit {
    rate: Rate,
    buckets: Mutex<HashMap<String, Bucket>>,
}

impl RateLimit {
    pub fn new(rate: Rate) -> RateLimit {
        RateLimit {
            rate,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Spend a token for `key`, refilling first. The mutex is held for
    /// arithmetic only, never across an await.
    pub fn allow(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut buckets = self.buckets.lock().expect("rate limit mutex");
        let bucket = buckets.entry(key.to_string()).or_insert(Bucket {
            tokens: f64::from(self.rate.burst),
            last: now,
        });
        let refill = now.duration_since(bucket.last).as_secs_f64() * self.rate.per_second;
        bucket.tokens = (bucket.tokens + refill).min(f64::from(self.rate.burst));
        bucket.last = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Whole seconds until the next token, for Retry-After. At least
    /// one, because zero would tell the client to hammer.
    pub fn retry_after(&self) -> u64 {
        if self.rate.per_second > 0.0 {
            (1.0 / self.rate.per_second).ceil().max(1.0) as u64
        } else {
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_id_is_a_v4_uuid() {
        let id = fresh_id();
        assert_eq!(id.len(), 36);
        let parts: Vec<&str> = id.split('-').collect();
        assert_eq!(
            parts.iter().map(|p| p.len()).collect::<Vec<_>>(),
            vec![8, 4, 4, 4, 12]
        );
        assert!(parts[2].starts_with('4'));
        assert_ne!(fresh_id(), id);
    }

    #[test]
    fn the_bucket_spends_and_refuses() {
        let limit = RateLimit::new(Rate {
            burst: 2,
            per_second: 0.0,
        });
        assert!(limit.allow("k"));
        assert!(limit.allow("k"));
        assert!(!limit.allow("k"));
        // A different key has its own bucket.
        assert!(limit.allow("other"));
    }

    #[test]
    fn the_bucket_refills_over_time() {
        let limit = RateLimit::new(Rate {
            burst: 1,
            per_second: 1000.0,
        });
        assert!(limit.allow("k"));
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(limit.allow("k"));
    }
}
