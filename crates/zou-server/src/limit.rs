//! GoTrue's per endpoint rate limits: how often one caller may ask for
//! a token, a code, or a second factor, and how often this whole server
//! will post mail or text messages on anybody's behalf.
//!
//! Two different things live under the one name upstream, and they stay
//! two different things here because they are configured differently
//! and they refuse differently:
//!
//! - The endpoint limits are token buckets keyed on the caller. Every
//!   endpoint has its own bucket set even where several of them are
//!   configured by one number, so a project that spends its otp budget
//!   on /recover still has its /magiclink budget. A refusal is a 429
//!   with `over_request_rate_limit`.
//! - The send limits are one counter each for mail and for text, not
//!   keyed on anything, because they are there to protect the transport
//!   the whole project shares rather than to be fair between callers.
//!   They refuse with `over_email_send_rate_limit` and
//!   `over_sms_send_rate_limit`.
//!
//! Who a request counts against is the part worth reading before
//! turning any of this on. Upstream keys on an address it was told
//! about, never on the socket, because it is always behind something:
//! Sb-Forwarded-For when the platform sets it, otherwise the first
//! value of whatever header `ZOU_RATE_LIMIT_HEADER` names, otherwise
//! **nothing at all, and the request is not limited**. A GoTrue with no
//! proxy in front of it and no header configured does not rate limit,
//! and neither does this. That is the correct default for a server
//! behind a load balancer, and the wrong one for the case upstream does
//! not have, which is zou embedded in an application and listening on a
//! socket itself. `ZOU_RATE_LIMIT_PEER` is the one rung this end adds
//! for that case, off by default, and it is only safe to switch on when
//! nothing is forwarding for you.
//!
//! The endpoints upstream limits that this server does not serve yet,
//! and so does not configure: sso, saml assertions, web3, passkeys, and
//! dynamic oauth client registration.

use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{HeaderMap, Method, Request, StatusCode};
use axum::response::Response;

use crate::edge;

/// A rate as GoTrue's config writes one: either a bare number, which
/// counts events per hour and is reset rather than refilled, or
/// `events/duration`, which is a bucket of that many refilled one at a
/// time. Only the two send limits are configured this way, and the
/// syntax is theirs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rate {
    pub events: f64,
    pub over_time: Duration,
    pub kind: Kind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Count within a window, and start the count again in the next
    /// one. Upstream's IntervalLimiter, which is what a bare number
    /// gets.
    Interval,
    /// A bucket of `events` refilled one per `over_time`. Upstream's
    /// BurstLimiter, which is x/time/rate underneath.
    Burst,
}

/// The window a bare number counts over, upstream's defaultOverTime.
const HOUR: Duration = Duration::from_secs(60 * 60);

impl Rate {
    /// Upstream's conf.Rate.Decode. A number is an interval rate over
    /// an hour, anything else has to be `events/duration` where the
    /// duration is written the way Go writes one.
    pub fn parse(value: &str) -> Result<Rate, String> {
        if let Ok(events) = value.trim().parse::<f64>() {
            return Ok(Rate {
                events,
                over_time: HOUR,
                kind: Kind::Interval,
            });
        }
        let Some((events, over)) = value.split_once('/') else {
            return Err(format!("{value:?} does not match rate syntax"));
        };
        let events: u64 = events
            .parse()
            .map_err(|_| format!("the events part of {value:?} is not a whole number"))?;
        let over_time = duration(over)
            .ok_or_else(|| format!("the over-time part of {value:?} is not a duration"))?;
        Ok(Rate {
            events: events as f64,
            over_time,
            kind: Kind::Burst,
        })
    }
}

/// Go's time.ParseDuration, as much of it as an env var ever holds: a
/// run of number-and-unit pairs, each unit one of the six Go accepts.
/// Anything else is None, which is a refusal at startup rather than a
/// limit nobody configured the way they thought they had.
fn duration(text: &str) -> Option<Duration> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let mut total = 0.0f64;
    let mut rest = text;
    while !rest.is_empty() {
        let digits = rest
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(rest.len());
        let number: f64 = rest[..digits].parse().ok()?;
        rest = &rest[digits..];
        let unit = rest
            .find(|c: char| c.is_ascii_digit())
            .unwrap_or(rest.len());
        let seconds = match &rest[..unit] {
            "ns" => 1e-9,
            "us" | "µs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            _ => return None,
        };
        rest = &rest[unit..];
        total += number * seconds;
    }
    Some(Duration::from_secs_f64(total))
}

/// What the environment says the budgets are, GoTrue's GOTRUE_RATE_LIMIT_*
/// with GOTRUE_ swapped for ZOU_.
#[derive(Debug, Clone, PartialEq)]
pub struct Settings {
    /// `ZOU_RATE_LIMIT_HEADER`, the header a caller is identified by
    /// when the platform is not setting Sb-Forwarded-For. Empty, and
    /// there is no key and so no limiting.
    pub header: String,
    /// `ZOU_SECURITY_SB_FORWARDED_FOR_ENABLED`, whether the platform's
    /// own header is trusted. It takes precedence over the header
    /// above, as it does upstream.
    pub sb_forwarded_for: bool,
    /// `ZOU_RATE_LIMIT_PEER`, this server's own rung: key on the socket
    /// the request arrived on when nothing was forwarded. Off by
    /// default because trusting the socket behind a proxy would put
    /// every caller in one bucket.
    pub peer: bool,
    /// `ZOU_RATE_LIMIT_TOKEN_REFRESH`, over five minutes.
    pub token_refresh: f64,
    /// `ZOU_RATE_LIMIT_VERIFY`, over five minutes.
    pub verify: f64,
    /// `ZOU_RATE_LIMIT_OTP`, over five minutes, and it is the budget
    /// for six endpoints rather than one.
    pub otp: f64,
    /// `ZOU_RATE_LIMIT_ANONYMOUS_USERS`, over an hour.
    pub anonymous_users: f64,
    /// `ZOU_MFA_RATE_LIMIT_CHALLENGE_AND_VERIFY`, over a minute.
    pub mfa: f64,
    /// `ZOU_RATE_LIMIT_EMAIL_SENT`, for the whole server.
    pub email_sent: Rate,
    /// `ZOU_RATE_LIMIT_SMS_SENT`, for the whole server.
    pub sms_sent: Rate,
}

impl Default for Settings {
    /// GoTrue's defaults, every one of them.
    fn default() -> Settings {
        Settings {
            header: String::new(),
            sb_forwarded_for: false,
            peer: false,
            token_refresh: 150.0,
            verify: 30.0,
            otp: 30.0,
            anonymous_users: 30.0,
            mfa: 15.0,
            email_sent: PER_HOUR_30,
            sms_sent: PER_HOUR_30,
        }
    }
}

/// The default both send limits carry, thirty an hour.
const PER_HOUR_30: Rate = Rate {
    events: 30.0,
    over_time: HOUR,
    kind: Kind::Interval,
};

/// The budgets the environment asks for.
pub fn from_env() -> Result<Settings, String> {
    configured(&|name| std::env::var(name).unwrap_or_default())
}

pub fn configured(var: &dyn Fn(&str) -> String) -> Result<Settings, String> {
    let fallback = Settings::default();
    Ok(Settings {
        header: var("ZOU_RATE_LIMIT_HEADER").trim().to_string(),
        sb_forwarded_for: switch(var, "ZOU_SECURITY_SB_FORWARDED_FOR_ENABLED")?,
        peer: switch(var, "ZOU_RATE_LIMIT_PEER")?,
        token_refresh: number(var, "ZOU_RATE_LIMIT_TOKEN_REFRESH", fallback.token_refresh)?,
        verify: number(var, "ZOU_RATE_LIMIT_VERIFY", fallback.verify)?,
        otp: number(var, "ZOU_RATE_LIMIT_OTP", fallback.otp)?,
        anonymous_users: number(
            var,
            "ZOU_RATE_LIMIT_ANONYMOUS_USERS",
            fallback.anonymous_users,
        )?,
        mfa: number(var, "ZOU_MFA_RATE_LIMIT_CHALLENGE_AND_VERIFY", fallback.mfa)?,
        email_sent: rate(var, "ZOU_RATE_LIMIT_EMAIL_SENT", fallback.email_sent)?,
        sms_sent: rate(var, "ZOU_RATE_LIMIT_SMS_SENT", fallback.sms_sent)?,
    })
}

fn switch(var: &dyn Fn(&str) -> String, name: &str) -> Result<bool, String> {
    match var(name).trim() {
        "" | "false" | "0" => Ok(false),
        "true" | "1" => Ok(true),
        other => Err(format!("{name} is {other:?}, which is not true or false")),
    }
}

fn number(var: &dyn Fn(&str) -> String, name: &str, fallback: f64) -> Result<f64, String> {
    let text = var(name);
    let text = text.trim();
    if text.is_empty() {
        return Ok(fallback);
    }
    match text.parse::<f64>() {
        Ok(n) if n >= 0.0 && n.is_finite() => Ok(n),
        _ => Err(format!("{name} is {text:?}, which is not a rate")),
    }
}

fn rate(var: &dyn Fn(&str) -> String, name: &str, fallback: Rate) -> Result<Rate, String> {
    let text = var(name);
    let text = text.trim();
    if text.is_empty() {
        return Ok(fallback);
    }
    Rate::parse(text).map_err(|why| format!("{name}: {why}"))
}

/// Which budget an endpoint spends from. There is one of these per
/// endpoint rather than per configured number, because upstream builds
/// a separate limiter per endpoint too and a shared one would be a
/// tighter limit than the operator asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Point {
    Token,
    Signup,
    Anonymous,
    Verify,
    Otp,
    MagicLink,
    Recover,
    Resend,
    User,
    FactorChallenge,
    FactorVerify,
}

/// Every endpoint's buckets and the two send counters, built once at
/// startup and shared by every request.
pub struct Limits {
    settings: Settings,
    token: edge::RateLimit,
    signup: edge::RateLimit,
    anonymous: edge::RateLimit,
    verify: edge::RateLimit,
    otp: edge::RateLimit,
    magic_link: edge::RateLimit,
    recover: edge::RateLimit,
    resend: edge::RateLimit,
    user: edge::RateLimit,
    factor_challenge: edge::RateLimit,
    factor_verify: edge::RateLimit,
    email: Counter,
    sms: Counter,
}

/// Five minutes, which is the window most of the endpoint budgets are
/// spread over upstream.
const FIVE_MINUTES: f64 = 60.0 * 5.0;

/// The burst every endpoint limiter but the anonymous one carries.
/// Thirty requests may land at once however slow the refill is, which
/// is what makes a page that fires several requests on load work at all.
const BURST: u32 = 30;

impl Limits {
    pub fn new(settings: Settings) -> Limits {
        // Upstream spreads all of these over five minutes with a burst
        // of thirty and keeps an idle bucket for an hour, except the
        // anonymous one, which is an hour's worth all at once, and the
        // two factor ones, which are a minute's.
        let per_5m = |rate: f64| {
            edge::RateLimit::keep(
                edge::Rate {
                    burst: BURST,
                    per_second: rate / FIVE_MINUTES,
                },
                HOUR,
            )
        };
        let per_minute = |rate: f64| {
            edge::RateLimit::keep(
                edge::Rate {
                    burst: BURST,
                    per_second: rate / 60.0,
                },
                Duration::from_secs(60),
            )
        };
        Limits {
            token: per_5m(settings.token_refresh),
            verify: per_5m(settings.verify),
            signup: per_5m(settings.otp),
            otp: per_5m(settings.otp),
            magic_link: per_5m(settings.otp),
            recover: per_5m(settings.otp),
            resend: per_5m(settings.otp),
            user: per_5m(settings.otp),
            anonymous: edge::RateLimit::keep(
                edge::Rate {
                    burst: settings.anonymous_users as u32,
                    per_second: settings.anonymous_users / 3600.0,
                },
                HOUR,
            ),
            factor_challenge: per_minute(settings.mfa),
            factor_verify: per_minute(settings.mfa),
            email: Counter::new(settings.email_sent),
            sms: Counter::new(settings.sms_sent),
            settings,
        }
    }

    fn bucket(&self, point: Point) -> &edge::RateLimit {
        match point {
            Point::Token => &self.token,
            Point::Signup => &self.signup,
            Point::Anonymous => &self.anonymous,
            Point::Verify => &self.verify,
            Point::Otp => &self.otp,
            Point::MagicLink => &self.magic_link,
            Point::Recover => &self.recover,
            Point::Resend => &self.resend,
            Point::User => &self.user,
            Point::FactorChallenge => &self.factor_challenge,
            Point::FactorVerify => &self.factor_verify,
        }
    }

    /// Who this request counts against, or nobody. Nobody is the
    /// unconfigured answer and it means the request is not limited,
    /// which is upstream's behaviour and worth saying twice.
    pub fn who(&self, headers: &HeaderMap, peer: Option<IpAddr>) -> Option<String> {
        if self.settings.sb_forwarded_for
            && let Some(ip) = leading_ip(header(headers, SB_FORWARDED_FOR))
        {
            return Some(ip);
        }
        if !self.settings.header.is_empty() {
            let first = leading(header(headers, &self.settings.header));
            if !first.is_empty() {
                return Some(first.to_string());
            }
        }
        // The rung upstream does not have. It is last on purpose: a
        // request that was forwarded is keyed on who it came from, not
        // on whoever forwarded it.
        if self.settings.peer {
            return peer.map(|ip| ip.to_string());
        }
        None
    }

    /// Spend one request's worth of `point`'s budget. An unkeyed
    /// request is always allowed, because there is nothing to count it
    /// against.
    pub fn allow(&self, point: Point, who: Option<&str>) -> bool {
        match who {
            Some(key) => self.bucket(point).allow(key),
            None => true,
        }
    }

    /// GoTrue's sendEmail, the part of it that is a limit: the whole
    /// server may post so much mail an hour whoever asked for it.
    ///
    /// Two upstream quirks are kept because a project's monitoring will
    /// have been written against them. A budget of zero refuses before
    /// anything else is considered, autoconfirm or not. And a project
    /// that confirms its own signups skips the limit entirely, which is
    /// a TODO in upstream's own source rather than a decision, but it
    /// is the behaviour today.
    pub fn email_sent(&self, autoconfirm: bool) -> Result<(), crate::auth::Error> {
        if self.settings.email_sent.events == 0.0 {
            return Err(over_send(OVER_EMAIL, "email rate limit exceeded"));
        }
        if autoconfirm || self.email.allow() {
            return Ok(());
        }
        Err(over_send(OVER_EMAIL, "email rate limit exceeded"))
    }

    /// The same for text messages, with the same autoconfirm quirk and
    /// without the zero shortcut, because upstream does not have one
    /// there: a budget of zero refuses anyway, one call later.
    pub fn sms_sent(&self, autoconfirm: bool) -> Result<(), crate::auth::Error> {
        if autoconfirm || self.sms.allow() {
            return Ok(());
        }
        Err(over_send(OVER_SMS, "SMS rate limit exceeded"))
    }
}

/// The header the platform sets in front of a hosted project, and the
/// first place a caller is looked for.
const SB_FORWARDED_FOR: &str = "sb-forwarded-for";

const OVER_REQUEST: &str = "over_request_rate_limit";
const OVER_EMAIL: &str = "over_email_send_rate_limit";
const OVER_SMS: &str = "over_sms_send_rate_limit";

fn over_send(code: &'static str, msg: &str) -> crate::auth::Error {
    crate::auth::refused(StatusCode::TOO_MANY_REQUESTS, code, msg)
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
}

/// The first value of a comma separated header, which is how every
/// forwarding header is written and how upstream reads all of them.
fn leading(value: &str) -> &str {
    value.split(',').next().unwrap_or_default().trim()
}

/// The same, but it has to be an address. Sb-Forwarded-For is parsed
/// rather than trusted, and a header that holds something else is
/// treated as a header that is not there.
fn leading_ip(value: &str) -> Option<String> {
    leading(value)
        .parse::<IpAddr>()
        .ok()
        .map(|ip| ip.to_string())
}

/// What a caller over its budget is told. There is no Retry-After on
/// it, because upstream does not send one and a client that starts
/// honouring a header the hosted service never sends is a client that
/// behaves differently against the two.
pub fn refused() -> Response {
    crate::auth::error_body(
        StatusCode::TOO_MANY_REQUESTS,
        OVER_REQUEST,
        "Request rate limit reached",
    )
}

/// Which budget a request spends from, by where it is going. Signup is
/// missing on purpose: which of its two budgets a request spends
/// depends on what is in the body, so the handler spends it once it
/// knows.
fn point_of(method: &Method, path: &str) -> Option<Point> {
    let post = method == Method::POST;
    match path {
        "/auth/v1/token" if post => Some(Point::Token),
        // Both halves of verify are limited upstream, because the link
        // in an email is a GET and is exactly what gets hammered.
        "/auth/v1/verify" if post || method == Method::GET => Some(Point::Verify),
        "/auth/v1/otp" if post => Some(Point::Otp),
        "/auth/v1/magiclink" if post => Some(Point::MagicLink),
        "/auth/v1/recover" if post => Some(Point::Recover),
        "/auth/v1/resend" if post => Some(Point::Resend),
        // Reading the user is not limited upstream, only writing it.
        "/auth/v1/user" if method == Method::PUT => Some(Point::User),
        _ if post && factor(path, "/challenge") => Some(Point::FactorChallenge),
        _ if post && factor(path, "/verify") => Some(Point::FactorVerify),
        _ => None,
    }
}

/// Whether a path is one of the two limited factor endpoints, which
/// have an id in the middle of them.
fn factor(path: &str, tail: &str) -> bool {
    let Some(rest) = path.strip_prefix("/auth/v1/factors/") else {
        return false;
    };
    match rest.strip_suffix(tail) {
        Some(id) => !id.is_empty() && !id.contains('/'),
        None => false,
    }
}

/// The address the request arrived from, when axum was told to keep it.
pub(crate) fn peer(req: &Request<Body>) -> Option<IpAddr> {
    req.extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|info| info.0.ip())
}

/// The endpoint limits as one layer, sitting where upstream's per route
/// middleware sits: after the request has been matched to an endpoint
/// and before the endpoint does any work.
///
/// The token endpoint is limited here rather than inside the handler,
/// which is where upstream does it. Upstream has to, because a web3
/// grant spends a different budget, and zou does not serve web3 grants,
/// so the two are the same thing here.
pub async fn guard(
    axum::extract::State(app): axum::extract::State<std::sync::Arc<crate::App>>,
    req: Request<Body>,
    next: axum::middleware::Next,
) -> Response {
    let Some(point) = point_of(req.method(), req.uri().path()) else {
        return next.run(req).await;
    };
    let who = app.limits.who(req.headers(), peer(&req));
    if !app.limits.allow(point, who.as_deref()) {
        return refused();
    }
    next.run(req).await
}

/// One of the two send counters. Neither is keyed, because neither is
/// about fairness between callers: they are the last thing between a
/// misconfigured project and its mail provider's own limits.
struct Counter {
    rate: Rate,
    tally: Mutex<Tally>,
}

enum Tally {
    /// Upstream's IntervalLimiter: a count and the window it belongs to.
    Interval { since: Instant, count: f64 },
    /// Upstream's BurstLimiter, which is a token bucket that starts
    /// full and refills one per over_time.
    Burst { tokens: f64, last: Instant },
}

impl Counter {
    fn new(rate: Rate) -> Counter {
        let tally = match rate.kind {
            Kind::Interval => Tally::Interval {
                since: Instant::now(),
                count: 0.0,
            },
            Kind::Burst => Tally::Burst {
                tokens: rate.events.max(0.0),
                last: Instant::now(),
            },
        };
        Counter {
            rate,
            tally: Mutex::new(tally),
        }
    }

    fn allow(&self) -> bool {
        self.allow_at(Instant::now())
    }

    /// The same at a given moment, which is upstream's AllowAt and is
    /// here for the same reason it is there: a limit written in hours
    /// cannot be tested by waiting for one.
    fn allow_at(&self, now: Instant) -> bool {
        let mut tally = self.tally.lock().expect("send limit mutex");
        match &mut *tally {
            Tally::Interval { since, count } => {
                // Upstream moves the window forward by whole windows
                // rather than to now, so the windows keep their
                // original edges.
                let window = self.rate.over_time;
                if !window.is_zero() {
                    let elapsed = now.duration_since(*since);
                    let whole = elapsed.as_secs_f64() / window.as_secs_f64();
                    if whole >= 1.0 {
                        *since += window.mul_f64(whole.floor());
                        *count = 0.0;
                    }
                }
                if *count < self.rate.events {
                    *count += 1.0;
                    return true;
                }
                false
            }
            Tally::Burst { tokens, last } => {
                let window = match self.rate.over_time.is_zero() {
                    true => HOUR,
                    false => self.rate.over_time,
                };
                let refill = now.duration_since(*last).as_secs_f64() / window.as_secs_f64();
                *tokens = (*tokens + refill).min(self.rate.events.max(0.0));
                *last = now;
                if *tokens >= 1.0 {
                    *tokens -= 1.0;
                    return true;
                }
                false
            }
        }
    }
}

/// Kept so the endpoint limiters can be reasoned about in tests without
/// a request: what a given endpoint's bucket is holding right now.
#[cfg(test)]
impl Limits {
    fn spend(&self, point: Point, key: &str, times: usize) -> usize {
        (0..times).filter(|_| self.allow(point, Some(key))).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Settings {
        read(pairs).expect("these settings parse")
    }

    fn read(pairs: &[(&str, &str)]) -> Result<Settings, String> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        configured(&|name| {
            pairs
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        })
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).expect("a header name"),
                value.parse().expect("a header value"),
            );
        }
        headers
    }

    #[test]
    fn nothing_configured_is_gotrues_own_numbers() {
        let settings = env(&[]);
        assert_eq!(settings, Settings::default());
        assert_eq!(settings.token_refresh, 150.0);
        assert_eq!(settings.verify, 30.0);
        assert_eq!(settings.otp, 30.0);
        assert_eq!(settings.anonymous_users, 30.0);
        assert_eq!(settings.mfa, 15.0);
        assert_eq!(settings.email_sent, PER_HOUR_30);
        assert_eq!(settings.sms_sent, PER_HOUR_30);
        assert_eq!(settings.header, "");
        assert!(!settings.sb_forwarded_for);
        assert!(!settings.peer);
    }

    #[test]
    fn a_number_that_is_not_one_is_refused_by_name() {
        let why = read(&[("ZOU_RATE_LIMIT_OTP", "lots")]).expect_err("this is not a rate");
        assert!(why.contains("ZOU_RATE_LIMIT_OTP"), "{why}");
        assert!(why.contains("lots"), "{why}");
        assert!(read(&[("ZOU_RATE_LIMIT_VERIFY", "-1")]).is_err());
        assert!(read(&[("ZOU_RATE_LIMIT_TOKEN_REFRESH", "0")]).is_ok());
    }

    #[test]
    fn a_switch_is_true_or_false_and_says_so() {
        assert!(env(&[("ZOU_RATE_LIMIT_PEER", "true")]).peer);
        assert!(env(&[("ZOU_RATE_LIMIT_PEER", "1")]).peer);
        assert!(!env(&[("ZOU_RATE_LIMIT_PEER", "false")]).peer);
        let why = read(&[("ZOU_RATE_LIMIT_PEER", "yes")]).expect_err("this is not a switch");
        assert!(why.contains("ZOU_RATE_LIMIT_PEER"), "{why}");
    }

    #[test]
    fn a_bare_number_is_events_an_hour() {
        let rate = Rate::parse("30").expect("a bare number parses");
        assert_eq!(
            rate,
            Rate {
                events: 30.0,
                over_time: HOUR,
                kind: Kind::Interval,
            }
        );
    }

    #[test]
    fn a_slashed_rate_is_a_burst_over_its_own_window() {
        let rate = Rate::parse("10/1m").expect("a burst rate parses");
        assert_eq!(rate.events, 10.0);
        assert_eq!(rate.over_time, Duration::from_secs(60));
        assert_eq!(rate.kind, Kind::Burst);
        assert_eq!(
            Rate::parse("1/1h30m")
                .expect("go durations add up")
                .over_time,
            Duration::from_secs(5400)
        );
        assert_eq!(
            Rate::parse("2/500ms")
                .expect("sub second durations parse")
                .over_time,
            Duration::from_millis(500)
        );
    }

    #[test]
    fn a_rate_that_is_not_one_is_refused() {
        for value in ["", "10/", "/1m", "10/1x", "ten/1m", "10/1m/2s", "-1/1m"] {
            assert!(Rate::parse(value).is_err(), "{value:?} should not parse");
        }
        let why = read(&[("ZOU_RATE_LIMIT_EMAIL_SENT", "10/1x")]).expect_err("not a duration");
        assert!(why.contains("ZOU_RATE_LIMIT_EMAIL_SENT"), "{why}");
    }

    #[test]
    fn with_nothing_to_key_on_nothing_is_limited() {
        let limits = Limits::new(Settings::default());
        let who = limits.who(&headers(&[("x-forwarded-for", "1.2.3.4")]), None);
        assert_eq!(who, None);
        // A thousand of them, all allowed, because there is nobody to
        // count them against.
        assert!((0..1000).all(|_| limits.allow(Point::Token, None)));
    }

    #[test]
    fn the_configured_header_is_the_key() {
        let limits = Limits::new(env(&[("ZOU_RATE_LIMIT_HEADER", "x-forwarded-for")]));
        let who = limits.who(&headers(&[("x-forwarded-for", "1.2.3.4, 5.6.7.8")]), None);
        assert_eq!(who.as_deref(), Some("1.2.3.4"));
        // A header with nothing in it is a request with no key, which
        // upstream warns about and lets through.
        assert_eq!(
            limits.who(&headers(&[("x-forwarded-for", " ")]), None),
            None
        );
        assert_eq!(limits.who(&headers(&[]), None), None);
    }

    #[test]
    fn the_platform_header_wins_and_has_to_be_an_address() {
        let limits = Limits::new(env(&[
            ("ZOU_RATE_LIMIT_HEADER", "x-forwarded-for"),
            ("ZOU_SECURITY_SB_FORWARDED_FOR_ENABLED", "true"),
        ]));
        let both = headers(&[
            ("sb-forwarded-for", "9.9.9.9"),
            ("x-forwarded-for", "1.2.3.4"),
        ]);
        assert_eq!(limits.who(&both, None).as_deref(), Some("9.9.9.9"));
        // Not an address, so it is not a key, and the request falls
        // through to the header behind it.
        let junk = headers(&[
            ("sb-forwarded-for", "nonsense"),
            ("x-forwarded-for", "1.2.3.4"),
        ]);
        assert_eq!(limits.who(&junk, None).as_deref(), Some("1.2.3.4"));
    }

    #[test]
    fn the_platform_header_is_ignored_until_it_is_trusted() {
        let limits = Limits::new(Settings::default());
        assert_eq!(
            limits.who(&headers(&[("sb-forwarded-for", "9.9.9.9")]), None),
            None
        );
    }

    #[test]
    fn the_socket_is_the_last_rung_and_only_when_asked_for() {
        let off = Limits::new(Settings::default());
        let peer: IpAddr = "10.0.0.1".parse().expect("an address");
        assert_eq!(off.who(&headers(&[]), Some(peer)), None);
        let on = Limits::new(env(&[
            ("ZOU_RATE_LIMIT_PEER", "true"),
            ("ZOU_RATE_LIMIT_HEADER", "x-forwarded-for"),
        ]));
        assert_eq!(
            on.who(&headers(&[]), Some(peer)).as_deref(),
            Some("10.0.0.1")
        );
        // Forwarded beats the socket, because the socket is the proxy.
        let forwarded = headers(&[("x-forwarded-for", "1.2.3.4")]);
        assert_eq!(on.who(&forwarded, Some(peer)).as_deref(), Some("1.2.3.4"));
    }

    #[test]
    fn every_endpoint_has_its_own_bucket() {
        let limits = Limits::new(Settings::default());
        // The otp number configures six endpoints, and spending one of
        // them dry leaves the other five with a full burst.
        assert_eq!(limits.spend(Point::Recover, "1.2.3.4", 60), 30);
        assert!(!limits.allow(Point::Recover, Some("1.2.3.4")));
        for point in [
            Point::Otp,
            Point::MagicLink,
            Point::Resend,
            Point::User,
            Point::Signup,
        ] {
            assert!(limits.allow(point, Some("1.2.3.4")), "{point:?} was spent");
        }
    }

    #[test]
    fn one_callers_budget_is_not_anothers() {
        let limits = Limits::new(Settings::default());
        assert_eq!(limits.spend(Point::Verify, "1.2.3.4", 40), 30);
        assert!(limits.allow(Point::Verify, Some("5.6.7.8")));
    }

    #[test]
    fn the_anonymous_budget_is_an_hours_worth_at_once() {
        // Upstream gives this one a burst of the whole number rather
        // than thirty, so a project that allows two anonymous sign ins
        // an hour allows two at once and not thirty.
        let limits = Limits::new(env(&[("ZOU_RATE_LIMIT_ANONYMOUS_USERS", "2")]));
        assert_eq!(limits.spend(Point::Anonymous, "1.2.3.4", 5), 2);
    }

    #[test]
    fn a_budget_of_zero_refuses_everything() {
        let limits = Limits::new(env(&[
            ("ZOU_RATE_LIMIT_ANONYMOUS_USERS", "0"),
            ("ZOU_RATE_LIMIT_OTP", "0"),
        ]));
        assert!(!limits.allow(Point::Anonymous, Some("1.2.3.4")));
        // The otp endpoints keep their burst of thirty whatever the
        // refill is, which is upstream's arithmetic and not a bug here.
        assert_eq!(limits.spend(Point::Otp, "1.2.3.4", 40), 30);
    }

    #[test]
    fn the_paths_that_are_limited_are_the_ones_upstream_limits() {
        let cases = [
            ("POST", "/auth/v1/token", Some(Point::Token)),
            ("POST", "/auth/v1/verify", Some(Point::Verify)),
            ("GET", "/auth/v1/verify", Some(Point::Verify)),
            ("POST", "/auth/v1/otp", Some(Point::Otp)),
            ("POST", "/auth/v1/magiclink", Some(Point::MagicLink)),
            ("POST", "/auth/v1/recover", Some(Point::Recover)),
            ("POST", "/auth/v1/resend", Some(Point::Resend)),
            ("PUT", "/auth/v1/user", Some(Point::User)),
            ("GET", "/auth/v1/user", None),
            (
                "POST",
                "/auth/v1/factors/abc/challenge",
                Some(Point::FactorChallenge),
            ),
            (
                "POST",
                "/auth/v1/factors/abc/verify",
                Some(Point::FactorVerify),
            ),
            ("POST", "/auth/v1/factors", None),
            ("POST", "/auth/v1/factors//challenge", None),
            ("POST", "/auth/v1/factors/a/b/challenge", None),
            // Signup is spent by the handler, once it knows which of
            // its two budgets the body is asking for.
            ("POST", "/auth/v1/signup", None),
            ("POST", "/auth/v1/logout", None),
            ("GET", "/auth/v1/settings", None),
            ("POST", "/auth/v1/token/extra", None),
        ];
        for (method, path, want) in cases {
            let method = Method::from_bytes(method.as_bytes()).expect("a method");
            assert_eq!(point_of(&method, path), want, "{method} {path}");
        }
    }

    #[test]
    fn the_send_counter_counts_a_window_at_a_time() {
        let counter = Counter::new(Rate {
            events: 2.0,
            over_time: Duration::from_secs(60),
            kind: Kind::Interval,
        });
        let start = Instant::now();
        let at = |seconds: u64| start + Duration::from_secs(seconds);
        assert!(counter.allow_at(at(0)));
        assert!(counter.allow_at(at(0)));
        assert!(!counter.allow_at(at(0)));
        assert!(!counter.allow_at(at(59)), "still the same window");
        assert!(counter.allow_at(at(60)), "the next window starts again");
        assert!(counter.allow_at(at(60)));
        // The window moved by a whole window rather than to the moment
        // it was noticed, so the next edge is where it always was.
        assert!(!counter.allow_at(at(119)));
        assert!(counter.allow_at(at(120)));
    }

    #[test]
    fn a_burst_send_rate_refills_one_at_a_time() {
        let counter = Counter::new(Rate {
            events: 2.0,
            over_time: Duration::from_secs(10),
            kind: Kind::Burst,
        });
        let start = Instant::now();
        let at = |seconds: u64| start + Duration::from_secs(seconds);
        assert!(counter.allow_at(at(0)));
        assert!(counter.allow_at(at(0)), "it starts full");
        assert!(!counter.allow_at(at(0)));
        assert!(!counter.allow_at(at(9)));
        assert!(counter.allow_at(at(10)));
        assert!(!counter.allow_at(at(10)), "one back, not the whole bucket");
        // And it never fills past the burst however long it idles.
        assert!(counter.allow_at(at(600)));
        assert!(counter.allow_at(at(600)));
        assert!(!counter.allow_at(at(600)));
    }

    #[test]
    fn the_mail_budget_is_the_whole_servers() {
        let limits = Limits::new(env(&[("ZOU_RATE_LIMIT_EMAIL_SENT", "2")]));
        assert!(limits.email_sent(false).is_ok());
        assert!(limits.email_sent(false).is_ok());
        let refused = limits.email_sent(false).expect_err("the third is refused");
        match refused {
            crate::auth::Error::Denied { status, code, msg } => {
                assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
                assert_eq!(code, "over_email_send_rate_limit");
                assert_eq!(msg, "email rate limit exceeded");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[test]
    fn a_project_that_confirms_its_own_signups_is_not_limited() {
        let limits = Limits::new(env(&[
            ("ZOU_RATE_LIMIT_EMAIL_SENT", "1"),
            ("ZOU_RATE_LIMIT_SMS_SENT", "1"),
        ]));
        assert!((0..10).all(|_| limits.email_sent(true).is_ok()));
        assert!((0..10).all(|_| limits.sms_sent(true).is_ok()));
    }

    #[test]
    fn a_mail_budget_of_zero_refuses_even_under_autoconfirm() {
        // Upstream checks this one before it checks autoconfirm, which
        // is the one way to turn the mail off altogether.
        let limits = Limits::new(env(&[("ZOU_RATE_LIMIT_EMAIL_SENT", "0")]));
        assert!(limits.email_sent(true).is_err());
        assert!(limits.email_sent(false).is_err());
        // The sms side has no such shortcut, so autoconfirm still wins.
        let texts = Limits::new(env(&[("ZOU_RATE_LIMIT_SMS_SENT", "0")]));
        assert!(texts.sms_sent(true).is_ok());
        assert!(texts.sms_sent(false).is_err());
    }

    #[test]
    fn the_text_budget_refuses_in_upstreams_words() {
        let limits = Limits::new(env(&[("ZOU_RATE_LIMIT_SMS_SENT", "1")]));
        assert!(limits.sms_sent(false).is_ok());
        match limits.sms_sent(false).expect_err("the second is refused") {
            crate::auth::Error::Denied { status, code, msg } => {
                assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
                assert_eq!(code, "over_sms_send_rate_limit");
                assert_eq!(msg, "SMS rate limit exceeded");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }
}
