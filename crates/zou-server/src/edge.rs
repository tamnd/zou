//! Edge concerns shared by every surface: CORS the way PostgREST
//! answers it behind Supabase, a request id on every response, and
//! the rate limit skeleton the per endpoint budgets will hang off
//! when the auth surface lands.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

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
///
/// The functions surface is the exception, and it is measured rather
/// than chosen. On `supabase start` the gateway answers every preflight
/// itself, function or not, and a function's own OPTIONS handler never
/// runs. The runtime behind that gateway does the opposite: asked
/// directly it hands the OPTIONS to the function, adds no header of its
/// own, and refuses nothing, which is why the hosted platform's
/// documented pattern is a `_shared/cors.ts` every function imports. A
/// server that answered the preflight here would make that pattern
/// dead code, so a preflight to `/functions/v1/` is handed to the
/// function, and only if what comes back says nothing about CORS does
/// the answer above stand in for it. Then a project that never wrote a
/// cors.ts still works the way it does against the local stack.
pub async fn cors(req: Request<Body>, next: Next) -> Response {
    let origin = req.headers().get(header::ORIGIN).cloned();
    let preflight = req.method() == Method::OPTIONS
        && origin.is_some()
        && req
            .headers()
            .contains_key(header::ACCESS_CONTROL_REQUEST_METHOD);
    if preflight {
        let origin = origin.expect("preflight checked origin");
        let answer = preflight_answer(req.headers(), origin);
        if !req.uri().path().starts_with(crate::functions::PREFIX) {
            return answer;
        }
        let res = next.run(req).await;
        return if res
            .headers()
            .contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN)
        {
            res
        } else {
            answer
        };
    }
    let mut res = next.run(req).await;
    if let Some(origin) = origin {
        let h = res.headers_mut();
        // A handler that named the origins it may be read from has
        // said the whole thing, so nothing is written over it and
        // nothing is added beside it. A function that allows one
        // origin means one origin, and this is the layer that would
        // otherwise quietly widen it to whoever asked: the gateway on
        // the local stack does exactly that, replacing what the
        // function set with `*`, and it is the one part of that
        // gateway's behaviour worth not copying.
        if h.contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN) {
            return res;
        }
        h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        h.insert(
            header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
            HeaderValue::from_static("true"),
        );
        // Unless the handler already said which of its headers a
        // browser may read. The functions surface does: its refusals
        // carry sb-error-code and say so, the way upstream's runtime
        // does, and a list written here over the top of that one would
        // hide the header the client is meant to read.
        if !h.contains_key(header::ACCESS_CONTROL_EXPOSE_HEADERS) {
            h.insert(
                header::ACCESS_CONTROL_EXPOSE_HEADERS,
                HeaderValue::from_static(EXPOSE_HEADERS),
            );
        }
    }
    res
}

/// What this edge tells a browser a preflight is allowed to do.
///
/// Built before the request is handed on, because on the functions
/// surface it is only sent if the function had nothing to say.
fn preflight_answer(asked: &header::HeaderMap, origin: HeaderValue) -> Response {
    let allow_headers = asked
        .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
        .cloned()
        .unwrap_or_else(|| HeaderValue::from_static("Authorization, Content-Type, apikey"));
    let mut res = StatusCode::NO_CONTENT.into_response();
    let h = res.headers_mut();
    h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
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

pub(crate) fn fresh_id() -> String {
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

/// The bucket map and when it was last swept, under one lock because
/// they are read together.
struct Buckets {
    map: HashMap<String, Bucket>,
    swept: Instant,
}

/// A token bucket per key. The apikey limiter uses one of these, and
/// the GoTrue per endpoint budgets in `limit` use eleven.
pub struct RateLimit {
    rate: Rate,
    /// How long an idle bucket is kept, upstream's DefaultExpirationTTL.
    /// It is memory management rather than policy: a bucket nobody has
    /// touched for its whole ttl has refilled to full by then, and a
    /// full bucket is the same thing as a missing one.
    ttl: Option<Duration>,
    buckets: Mutex<Buckets>,
}

impl RateLimit {
    pub fn new(rate: Rate) -> RateLimit {
        RateLimit::keep(rate, None)
    }

    /// The same, dropping buckets nobody has spent from in `ttl`.
    pub fn keep(rate: Rate, ttl: impl Into<Option<Duration>>) -> RateLimit {
        RateLimit {
            rate,
            ttl: ttl.into(),
            buckets: Mutex::new(Buckets {
                map: HashMap::new(),
                swept: Instant::now(),
            }),
        }
    }

    /// Spend a token for `key`, refilling first. The mutex is held for
    /// arithmetic only, never across an await.
    pub fn allow(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut buckets = self.buckets.lock().expect("rate limit mutex");
        // Sweeping once a ttl rather than once a request keeps this off
        // the hot path: between sweeps the map holds at most one ttl's
        // worth of callers, which is what it would hold anyway.
        if let Some(ttl) = self.ttl
            && now.duration_since(buckets.swept) >= ttl
        {
            buckets.map.retain(|_, b| now.duration_since(b.last) < ttl);
            buckets.swept = now;
        }
        let bucket = buckets.map.entry(key.to_string()).or_insert(Bucket {
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

    /// How many keys are being tracked, which is the only thing the
    /// sweep changes and so the only way to see it happen.
    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.buckets.lock().expect("rate limit mutex").map.len()
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
    fn an_idle_bucket_is_swept_and_a_busy_one_is_kept() {
        let limit = RateLimit::keep(
            Rate {
                burst: 5,
                per_second: 0.0,
            },
            std::time::Duration::from_millis(10),
        );
        assert!(limit.allow("gone"));
        assert_eq!(limit.tracked(), 1);
        std::thread::sleep(std::time::Duration::from_millis(15));
        // The sweep runs on the next spend, and the key that spent it
        // is the key that survives.
        assert!(limit.allow("here"));
        assert_eq!(limit.tracked(), 1);
        assert!(limit.allow("here"));
        assert_eq!(limit.tracked(), 1);
    }

    #[test]
    fn without_a_ttl_nothing_is_swept() {
        let limit = RateLimit::new(Rate {
            burst: 5,
            per_second: 0.0,
        });
        assert!(limit.allow("a"));
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(limit.allow("b"));
        assert_eq!(limit.tracked(), 2);
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
