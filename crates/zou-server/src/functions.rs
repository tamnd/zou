//! `/functions/v1/<name>`: the front of the edge functions surface.
//!
//! What is here is everything that happens to a call before and after
//! whatever runs the function, which is the part that has an upstream
//! to be exact about. The runtime itself is [`zou_functions::Runtime`],
//! and this file does not know or care whether the thing on the other
//! side of it is a javascript isolate or a closure in the application
//! that embedded this server.
//!
//! Every refusal below was taken off supabase-edge-runtime 1.74.2 on a
//! real `supabase start`, header for header and byte for byte, rather
//! than derived from the docs:
//!
//! ```text
//! 404 text/plain; charset=UTF-8   Function not found
//! 500 text/plain;charset=UTF-8    Internal Server Error
//! 401 application/json            {"code":"UNAUTHORIZED_NO_AUTH_HEADER","message":"Missing authorization header","msg":"Missing authorization header"}
//! 401 application/json            {"code":"UNAUTHORIZED_INVALID_JWT_FORMAT","message":"Invalid JWT format","msg":"Invalid JWT format"}
//! 401 application/json            {"code":"UNAUTHORIZED_LEGACY_JWT","message":"Invalid JWT","msg":"Invalid JWT"}
//! 401 application/json            {"code":"UNAUTHORIZED_ASYMMETRIC_JWT","message":"Invalid JWT","msg":"Invalid JWT"}
//! 401 application/json            {"code":"UNAUTHORIZED_UNSUPPORTED_TOKEN_ALGORITHM","message":"Unsupported JWT algorithm HS512","msg":"Unsupported JWT algorithm HS512"}
//! ```
//!
//! Three of those five have a message twice, once as `message` and once
//! as `msg`, which is a serializer upstream wrote around an older shape
//! rather than a decision. It is copied because a client matching on
//! either field is matching on what it was given.
//!
//! The order matters as much as the wording. A name nothing is deployed
//! under is answered 404 whether or not the caller carried a token, so
//! the lookup happens before the check and an unauthenticated stranger
//! learns nothing from the difference between a 401 and a 404. That is
//! upstream's order and it is the safe one anyway.
//!
//! This surface sits outside the apikey gate, for the same reason the
//! storage one does: the gateway in front of upstream's runtime has no
//! key check on this route, so a request carrying no key at all reaches
//! the runtime and is answered in the runtime's words. Behind the gate
//! it would be refused a layer too early and in the wrong sentence.

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::{Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use zou_functions::{Answer, Call};

use crate::{App, jwt, limit};

/// Where the surface lives, with the trailing slash, which is also
/// what upstream sends the function as `x-forwarded-prefix`.
pub(crate) const PREFIX: &str = "/functions/v1/";

/// How much of a request body this server will collect before handing
/// it over.
///
/// This server's own number rather than upstream's: what the reference
/// answers a body larger than it will take was not recorded, so there
/// is nothing to copy yet. Twenty mebibytes is well past what a
/// function is normally posted and well short of what would let one
/// caller take the process down.
const BODY_LIMIT: usize = 20 * 1024 * 1024;

/// The two algorithms the reference answered `UNAUTHORIZED_ASYMMETRIC_JWT`
/// for, as opposed to the `UNAUTHORIZED_UNSUPPORTED_TOKEN_ALGORITHM` it
/// answers for an algorithm it does not verify at all.
///
/// Probed rather than assumed, and the list is exactly two: RS256 and
/// ES256 came back asymmetric, while PS256, RS512, EdDSA, HS512 and
/// `none` all came back unsupported and named themselves in the
/// message. So this is a list of two algorithms rather than a rule
/// about which family an algorithm belongs to.
const ASYMMETRIC: &[&str] = &["RS256", "ES256"];

/// One call to one function, whatever method it arrived on.
///
/// Both routes land here, the bare `/functions/v1/<name>` and the one
/// with a path after it, because the name and the rest are read off
/// the url either way.
pub async fn call(State(app): State<Arc<App>>, req: Request<Body>) -> Response {
    let Some(rest) = req.uri().path().strip_prefix(PREFIX) else {
        // Only reachable if the routes and this constant disagree.
        return not_found();
    };
    let (name, tail) = match rest.split_once('/') {
        Some((name, tail)) => (name.to_string(), format!("/{tail}")),
        None => (rest.to_string(), String::new()),
    };
    let Some(registry) = app.cfg.functions.as_ref() else {
        return not_found();
    };
    // The lookup first, so that whether a name exists is not something
    // a caller can learn by watching a refusal change shape.
    let Some(function) = registry.lookup(&name).cloned() else {
        return not_found();
    };
    if function.verify_jwt
        && let Some(refusal) = verified(&app, &req)
    {
        return refusal;
    }
    let peer = limit::peer(&req);
    let (parts, body) = req.into_parts();
    let body = match to_bytes(body, BODY_LIMIT).await {
        Ok(body) => body.to_vec(),
        Err(_) => return too_large(),
    };
    let call = Call {
        method: parts.method.as_str().to_string(),
        url: url_for(&parts, &name, &tail),
        headers: forwarded(&parts, &name, &tail, peer),
        body,
        execution_id: crate::edge::fresh_id(),
    };
    let registry = Arc::clone(app.cfg.functions.as_ref().expect("looked up above"));
    // Blocking on purpose: an isolate is a thread's worth of state and
    // a host handler is ordinary Rust, so neither belongs on the
    // executor that is also answering everything else.
    let ran = tokio::task::spawn_blocking(move || registry.invoke(&function, call)).await;
    match ran {
        Ok(Ok(answer)) => answered(answer),
        Ok(Err(why)) => {
            log::error!("functions: {name} failed: {why}");
            failed()
        }
        Err(e) => {
            log::error!("functions: {name} died: {e}");
            failed()
        }
    }
}

/// `/functions/v1/` with no name after it, which upstream answers with
/// the same 404 as a name nobody deployed rather than with the
/// gateway's line about routes.
///
/// `/functions/v1` without the slash is a different url and gets the
/// gateway's json 404 from the router's fallback, which is what the
/// reference does too.
pub async fn unnamed() -> Response {
    not_found()
}

/// The refusal the caller has earned, or none, meaning it carried
/// something this project signed.
///
/// The header is read the way the reference reads it, which is not the
/// way the gate reads an apikey. `Authorization` wins whenever it is
/// there at all: a request with a good apikey and a broken bearer is
/// refused for the bearer. A `Bearer ` prefix is stripped if present
/// and the rest of the value is the token, so an `Authorization` that
/// is just the token works, which is a thing the reference accepts and
/// some clients send. With no `Authorization`, the `apikey` header is
/// the token instead. An `apikey` in the query string is not, and that
/// was probed rather than assumed: the reference answers a call
/// carrying `?apikey=...` and nothing else with the missing header
/// refusal.
///
/// An `Authorization` that is there and empty is the missing header
/// refusal, and a `Bearer ` with nothing after it is the format one.
/// Both were probed, and telling them apart is why the emptiness is
/// tested on the raw value rather than on what is left after the
/// prefix has been taken off.
fn verified(app: &App, req: &Request<Body>) -> Option<Response> {
    let raw = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = if raw.trim().is_empty() {
        match req
            .headers()
            .get("apikey")
            .and_then(|v| v.to_str().ok())
            .filter(|key| !key.trim().is_empty())
        {
            Some(key) => key,
            None => {
                return Some(unauthorized(
                    "UNAUTHORIZED_NO_AUTH_HEADER",
                    "Missing authorization header",
                ));
            }
        }
    } else {
        match raw.get(..7) {
            Some(prefix) if prefix.eq_ignore_ascii_case("bearer ") => &raw[7..],
            _ => raw,
        }
    };
    match jwt::verify_any(token, &app.cfg.jwt_secret, app.jwks.as_ref()) {
        Ok(_) => None,
        Err(_) => Some(refused(token)),
    }
}

/// A refusal in the reference's words, which are chosen by the token's
/// header and by nothing else.
///
/// That is worth stating plainly, because the obvious design is the
/// other one: refuse in the words the failure earns, a bad signature
/// saying one thing and an expiry another. The reference does not do
/// that, and it was probed rather than assumed. A token whose header
/// says HS256 gets `UNAUTHORIZED_LEGACY_JWT` whether its signature is
/// wrong, unreadable, or absent, and whether its payload is json at
/// all: `eyJhbGciOiJIUzI1NiJ9.bbb.ccc` and
/// `eyJhbGciOiJIUzI1NiJ9..AAAA` both earn it. Only a token whose
/// header cannot be read is a format error, and the header cannot be
/// read when the token is not three segments, when the first segment
/// is empty or is not json, or when the json has no `alg` in it.
///
/// Two algorithms are asymmetric here, exactly the two the reference
/// names. PS256, RS512 and EdDSA are not among them: they earn the
/// unsupported refusal with their own name quoted back, and so does
/// `none`.
fn refused(token: &str) -> Response {
    match jwt::algorithm(token) {
        None => unauthorized("UNAUTHORIZED_INVALID_JWT_FORMAT", "Invalid JWT format"),
        Some(alg) if alg == "HS256" => unauthorized("UNAUTHORIZED_LEGACY_JWT", "Invalid JWT"),
        Some(alg) if ASYMMETRIC.contains(&alg.as_str()) => {
            unauthorized("UNAUTHORIZED_ASYMMETRIC_JWT", "Invalid JWT")
        }
        Some(alg) => unauthorized(
            "UNAUTHORIZED_UNSUPPORTED_TOKEN_ALGORITHM",
            &format!("Unsupported JWT algorithm {alg}"),
        ),
    }
}

/// The 401 shape: the code in a header a browser is allowed to read,
/// and the same sentence twice in the body.
fn unauthorized(code: &str, message: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::ACCESS_CONTROL_EXPOSE_HEADERS, "sb-error-code"),
            (header::HeaderName::from_static("sb-error-code"), code),
        ],
        serde_json::json!({"code": code, "message": message, "msg": message}).to_string(),
    )
        .into_response()
}

/// A name nothing is deployed under, as text rather than as json,
/// which is worth knowing before writing a json 404.
fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        [(header::CONTENT_TYPE, "text/plain; charset=UTF-8")],
        "Function not found",
    )
        .into_response()
}

/// A function that threw. What went wrong goes to the log and never to
/// the caller, which is upstream's rule and the right one: the caller
/// is somebody else's browser and the stack trace is the operator's.
///
/// The content type has no space in it, and the 404's does. Both are
/// copied as they were recorded, because they come from two different
/// places in the reference and a suite that compares headers will see
/// the difference.
fn failed() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        [(header::CONTENT_TYPE, "text/plain;charset=UTF-8")],
        "Internal Server Error",
    )
        .into_response()
}

/// A body past what this server will collect. Not upstream's answer,
/// because upstream's was never recorded, and said in the same plain
/// text the rest of this surface says everything in.
fn too_large() -> Response {
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        [(header::CONTENT_TYPE, "text/plain;charset=UTF-8")],
        "Payload Too Large",
    )
        .into_response()
}

/// What the function answered, turned back into a response.
///
/// A header the function cannot legally send is dropped rather than
/// taking the whole answer down with it, because a function that put a
/// newline in a header value has made a mistake the caller cannot fix
/// and should still get the body it was sent.
fn answered(answer: Answer) -> Response {
    let mut res = Response::new(Body::from(answer.body));
    *res.status_mut() = StatusCode::from_u16(answer.status).unwrap_or(StatusCode::OK);
    for (name, value) in answer.headers {
        if let (Ok(name), Ok(value)) = (
            header::HeaderName::try_from(name.as_str()),
            header::HeaderValue::try_from(value.as_str()),
        ) {
            res.headers_mut().append(name, value);
        }
    }
    res
}

/// What the handler reads as `req.url`.
///
/// A decision rather than a copy, and the one place this surface
/// deliberately answers differently from the local reference. Upstream
/// hands the isolate the url of its own internal hop,
/// `http://127.0.0.1:8081/probe/a/b?x=1&y=2`: the container's address,
/// and the path with `/functions/v1` already taken off. There is no
/// internal hop here, so what goes in its place is the address the
/// caller actually reached, with the same path shape. A function doing
/// `new URL(req.url).pathname` sees `/probe/a/b` on both, which is what
/// every routing example in the documentation is written against, and
/// the host is the truth here rather than a container name that means
/// nothing outside a docker network.
fn url_for(parts: &axum::http::request::Parts, name: &str, tail: &str) -> String {
    let scheme = header_of(parts, "x-forwarded-proto").unwrap_or("http");
    let host = header_of(parts, header::HOST.as_str()).unwrap_or("localhost");
    let query = match parts.uri.query() {
        Some(q) => format!("?{q}"),
        None => String::new(),
    };
    format!("{scheme}://{host}/{name}{tail}{query}")
}

fn header_of<'a>(parts: &'a axum::http::request::Parts, name: &str) -> Option<&'a str> {
    parts.headers.get(name).and_then(|v| v.to_str().ok())
}

/// Every header the function is told about.
///
/// The caller's own headers go through as they arrived, lowercased,
/// which is what a `Headers` object gives javascript back. On top of
/// them go the six the reference's gateway adds, because a function
/// that reconstructs its public url reads them and would otherwise
/// find nothing there. A value a proxy in front of this server already
/// set is kept rather than overwritten, since that proxy knows what it
/// forwarded and this server only knows what it received.
///
/// `x-forwarded-path` and `x-forwarded-prefix` are always this
/// server's, because they describe this hop and no other, and the
/// prefix carries the trailing slash the reference sends.
fn forwarded(
    parts: &axum::http::request::Parts,
    name: &str,
    tail: &str,
    peer: Option<std::net::IpAddr>,
) -> Vec<(String, String)> {
    let mut headers: Vec<(String, String)> = parts
        .headers
        .iter()
        .filter_map(|(k, v)| {
            v.to_str()
                .ok()
                .map(|v| (k.as_str().to_lowercase(), v.to_string()))
        })
        .collect();
    let host = header_of(parts, header::HOST.as_str()).unwrap_or("localhost");
    let (hostname, port) = match host.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) => (h, p.to_string()),
        _ => (host, "80".to_string()),
    };
    let mut fill = |name: &str, value: String| {
        if !headers.iter().any(|(k, _)| k == name) {
            headers.push((name.to_string(), value));
        }
    };
    fill("x-forwarded-proto", "http".to_string());
    fill("x-forwarded-host", hostname.to_string());
    fill("x-forwarded-port", port);
    if let Some(peer) = peer {
        fill("x-forwarded-for", peer.to_string());
        fill("x-real-ip", peer.to_string());
    }
    let path = format!("{PREFIX}{name}{tail}");
    headers.retain(|(k, _)| k != "x-forwarded-path" && k != "x-forwarded-prefix");
    headers.push(("x-forwarded-path".to_string(), path));
    headers.push(("x-forwarded-prefix".to_string(), PREFIX.to_string()));
    headers
}

/// The urls a server answers, the way `supabase functions serve`
/// prints them at boot.
pub fn served(registry: &zou_functions::Registry) -> Vec<String> {
    registry
        .names()
        .map(|name| format!("{PREFIX}{name}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use axum::Router;
    use axum::body::to_bytes;
    use tower::ServiceExt;
    use zou_functions::{Hosted, Registry};

    const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

    /// The last call any function was made, so a test can ask what the
    /// function was told rather than only what the caller was answered.
    #[derive(Default)]
    struct Seen(Mutex<Option<Call>>);

    fn server(seen: &Arc<Seen>) -> Router {
        let seen = Arc::clone(seen);
        let recorder = Arc::clone(&seen);
        let hosted = Hosted::new()
            .at("hello", move |call| {
                *recorder.0.lock().expect("seen") = Some(call.clone());
                Ok(Answer::new(
                    "application/json",
                    br#"{"hello":"world"}"#.to_vec(),
                ))
            })
            .at("boom", |_| Err("kaboom".to_string()));
        let mut functions = vec![
            zou_functions::Function::new("hello", std::path::PathBuf::new()),
            zou_functions::Function::new("boom", std::path::PathBuf::new()),
        ];
        // The one function with `verify_jwt = false` in its block, the
        // way the probe project had it.
        functions.push(zou_functions::Function {
            verify_jwt: false,
            ..zou_functions::Function::new("open", std::path::PathBuf::new())
        });
        let hosted = hosted.at("open", |_| Ok(Answer::new("text/plain", b"open".to_vec())));
        crate::router(crate::Config {
            jwt_secret: SECRET.to_vec(),
            functions: Some(Arc::new(Registry::new(functions, Arc::new(hosted)))),
            ..crate::Config::default()
        })
        .expect("router")
    }

    fn bare() -> Router {
        crate::router(crate::Config {
            jwt_secret: SECRET.to_vec(),
            ..crate::Config::default()
        })
        .expect("router")
    }

    fn anon() -> String {
        jwt::mint(&jwt::key_claims("anon"), SECRET)
    }

    fn post(uri: &str) -> axum::http::request::Builder {
        Request::builder().method("POST").uri(uri)
    }

    async fn text(res: Response) -> String {
        let bytes = to_bytes(res.into_body(), 1 << 20).await.expect("body");
        String::from_utf8_lossy(&bytes).to_string()
    }

    async fn json(res: Response) -> serde_json::Value {
        let bytes = to_bytes(res.into_body(), 1 << 20).await.expect("body");
        serde_json::from_slice(&bytes).expect("json")
    }

    #[tokio::test]
    async fn a_name_nothing_is_deployed_under_is_text_and_not_json() {
        let seen = Arc::new(Seen::default());
        let res = server(&seen)
            .oneshot(
                post("/functions/v1/nosuch")
                    .header("authorization", format!("Bearer {}", anon()))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("answer");
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert_eq!(res.headers()["content-type"], "text/plain; charset=UTF-8");
        assert_eq!(text(res).await, "Function not found");
    }

    #[tokio::test]
    async fn the_lookup_happens_before_the_check() {
        // No token at all, on a name nobody deployed. Upstream answers
        // the 404 rather than the 401, so a stranger cannot tell which
        // names exist by watching the refusal change shape.
        let seen = Arc::new(Seen::default());
        let res = server(&seen)
            .oneshot(
                post("/functions/v1/nosuch")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("answer");
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert_eq!(text(res).await, "Function not found");
    }

    #[tokio::test]
    async fn a_server_with_no_functions_answers_every_name_the_same_way() {
        let res = bare()
            .oneshot(
                post("/functions/v1/hello")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("answer");
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert_eq!(text(res).await, "Function not found");
    }

    #[tokio::test]
    async fn the_prefix_with_no_name_is_the_functions_404_and_without_the_slash_the_gateways() {
        let seen = Arc::new(Seen::default());
        let res = server(&seen)
            .oneshot(post("/functions/v1/").body(Body::empty()).expect("request"))
            .await
            .expect("answer");
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert_eq!(text(res).await, "Function not found");

        let seen = Arc::new(Seen::default());
        let res = server(&seen)
            .oneshot(post("/functions/v1").body(Body::empty()).expect("request"))
            .await
            .expect("answer");
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            json(res).await,
            serde_json::json!({"message": "no Route matched with those values"})
        );
    }

    #[tokio::test]
    async fn a_call_carrying_nothing_is_the_missing_header_refusal() {
        let seen = Arc::new(Seen::default());
        let res = server(&seen)
            .oneshot(
                post("/functions/v1/hello")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("answer");
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            res.headers()["sb-error-code"],
            "UNAUTHORIZED_NO_AUTH_HEADER"
        );
        assert_eq!(
            res.headers()["access-control-expose-headers"],
            "sb-error-code"
        );
        assert_eq!(res.headers()["content-type"], "application/json");
        assert_eq!(
            json(res).await,
            serde_json::json!({
                "code": "UNAUTHORIZED_NO_AUTH_HEADER",
                "message": "Missing authorization header",
                "msg": "Missing authorization header",
            })
        );
    }

    /// The four refusals a token can earn, each with the code the
    /// reference answered for exactly this token.
    #[tokio::test]
    async fn a_token_is_refused_in_the_words_its_own_algorithm_earns() {
        let bad_signature = format!("{}.WRONG", anon().rsplit_once('.').expect("a jwt").0);
        let expired = jwt::mint(
            &serde_json::json!({"role": "anon", "exp": 1_500_000_000u64}),
            SECRET,
        );
        const FORMAT: (&str, &str) = ("UNAUTHORIZED_INVALID_JWT_FORMAT", "Invalid JWT format");
        const LEGACY: (&str, &str) = ("UNAUTHORIZED_LEGACY_JWT", "Invalid JWT");
        const ASYMMETRIC: (&str, &str) = ("UNAUTHORIZED_ASYMMETRIC_JWT", "Invalid JWT");
        // Every line below is a token that was posted to a real
        // `supabase functions serve` and the answer it came back with.
        let cases = [
            // Nothing that could be read as a header on the front.
            ("not-a-jwt", FORMAT),
            ("aaa.bbb.ccc", FORMAT),
            ("", FORMAT),
            ("e30.eyJyb2xlIjoiYW5vbiJ9.AAAA", FORMAT),
            ("eyJ0eXAiOiJKV1QifQ.eyJyb2xlIjoiYW5vbiJ9.AAAA", FORMAT),
            (".eyJyb2xlIjoiYW5vbiJ9.AAAA", FORMAT),
            // Two segments, and four. A readable header on the front of
            // either is still a format error, so the shape is checked
            // before the header is read.
            ("eyJhbGciOiJIUzI1NiJ9.eyJyb2xlIjoiYW5vbiJ9", FORMAT),
            (
                "eyJhbGciOiJIUzI1NiJ9.eyJyb2xlIjoiYW5vbiJ9.AAAA.BBBB",
                FORMAT,
            ),
            // A header that says HS256, however it fails after that.
            (bad_signature.as_str(), LEGACY),
            (expired.as_str(), LEGACY),
            ("eyJhbGciOiJIUzI1NiJ9.bbb.ccc", LEGACY),
            ("eyJhbGciOiJIUzI1NiJ9..AAAA", LEGACY),
            (
                "eyJhbGciOiJIUzI1NiIsImtpZCI6Im5vcGUifQ.eyJyb2xlIjoiYW5vbiJ9.AAAA",
                LEGACY,
            ),
            // The two that are asymmetric, and nothing else is.
            ("eyJhbGciOiJSUzI1NiJ9.eyJyb2xlIjoiYW5vbiJ9.AAAA", ASYMMETRIC),
            ("eyJhbGciOiJFUzI1NiJ9.eyJyb2xlIjoiYW5vbiJ9.AAAA", ASYMMETRIC),
            (
                "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYW5vbiJ9.AAAA",
                (
                    "UNAUTHORIZED_UNSUPPORTED_TOKEN_ALGORITHM",
                    "Unsupported JWT algorithm HS512",
                ),
            ),
            (
                "eyJhbGciOiJQUzI1NiJ9.eyJyb2xlIjoiYW5vbiJ9.AAAA",
                (
                    "UNAUTHORIZED_UNSUPPORTED_TOKEN_ALGORITHM",
                    "Unsupported JWT algorithm PS256",
                ),
            ),
            (
                "eyJhbGciOiJFZERTQSJ9.eyJyb2xlIjoiYW5vbiJ9.AAAA",
                (
                    "UNAUTHORIZED_UNSUPPORTED_TOKEN_ALGORITHM",
                    "Unsupported JWT algorithm EdDSA",
                ),
            ),
            // `none` is an algorithm like any other here, which is the
            // only safe reading of it and also the reference's.
            (
                "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJyb2xlIjoiYW5vbiJ9.",
                (
                    "UNAUTHORIZED_UNSUPPORTED_TOKEN_ALGORITHM",
                    "Unsupported JWT algorithm none",
                ),
            ),
            // An alg that is not a string still has a value, and the
            // reference quotes it back rather than calling the token
            // shapeless.
            (
                "eyJhbGciOjEyM30.eyJyb2xlIjoiYW5vbiJ9.AAAA",
                (
                    "UNAUTHORIZED_UNSUPPORTED_TOKEN_ALGORITHM",
                    "Unsupported JWT algorithm 123",
                ),
            ),
        ];
        for (token, (code, message)) in cases {
            let seen = Arc::new(Seen::default());
            let res = server(&seen)
                .oneshot(
                    post("/functions/v1/hello")
                        .header("authorization", format!("Bearer {token}"))
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("answer");
            assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "for {token}");
            assert_eq!(res.headers()["sb-error-code"], code, "for {token}");
            assert_eq!(
                json(res).await,
                serde_json::json!({"code": code, "message": message, "msg": message}),
                "for {token}"
            );
            assert!(
                seen.0.lock().expect("seen").is_none(),
                "a refused call never reaches the function"
            );
        }
    }

    #[tokio::test]
    async fn the_token_can_arrive_three_ways_and_the_authorization_header_wins() {
        let seen = Arc::new(Seen::default());
        // With the Bearer prefix, without it, and as the apikey when
        // there is no authorization at all. All three are accepted by
        // the reference and all three are here.
        for header in [
            ("authorization", format!("Bearer {}", anon())),
            ("authorization", anon()),
            ("apikey", anon()),
        ] {
            let res = server(&seen)
                .oneshot(
                    post("/functions/v1/hello")
                        .header(header.0, header.1.clone())
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("answer");
            assert_eq!(res.status(), StatusCode::OK, "for {}", header.0);
        }
        // A good apikey does not rescue a broken bearer, and a broken
        // apikey does not spoil a good one.
        let res = server(&seen)
            .oneshot(
                post("/functions/v1/hello")
                    .header("apikey", anon())
                    .header("authorization", "Bearer nope")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("answer");
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        let res = server(&seen)
            .oneshot(
                post("/functions/v1/hello")
                    .header("apikey", "nope")
                    .header("authorization", format!("Bearer {}", anon()))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("answer");
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn an_apikey_in_the_query_string_is_not_a_token_here() {
        let seen = Arc::new(Seen::default());
        let res = server(&seen)
            .oneshot(
                post(&format!("/functions/v1/hello?apikey={}", anon()))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("answer");
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            res.headers()["sb-error-code"],
            "UNAUTHORIZED_NO_AUTH_HEADER"
        );
    }

    #[tokio::test]
    async fn verify_jwt_off_lets_a_stranger_in() {
        let seen = Arc::new(Seen::default());
        let res = server(&seen)
            .oneshot(
                post("/functions/v1/open")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("answer");
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(text(res).await, "open");
    }

    #[tokio::test]
    async fn the_function_is_told_the_path_after_its_name_and_the_query_with_it() {
        let seen = Arc::new(Seen::default());
        let res = server(&seen)
            .oneshot(
                post("/functions/v1/hello/a/b?x=1&y=2")
                    .header("authorization", format!("Bearer {}", anon()))
                    .header("host", "127.0.0.1:54321")
                    .header("content-type", "application/json")
                    .body(Body::from("{\"n\":1}"))
                    .expect("request"),
            )
            .await
            .expect("answer");
        assert_eq!(res.status(), StatusCode::OK);
        let call = seen.0.lock().expect("seen").clone().expect("the call");
        assert_eq!(call.method, "POST");
        // The name is still on the path, the prefix is not, and the
        // query came with it. That is the shape upstream hands the
        // isolate, on this server's own host rather than a container's.
        assert_eq!(call.url, "http://127.0.0.1:54321/hello/a/b?x=1&y=2");
        assert_eq!(call.body, b"{\"n\":1}");
        assert_eq!(call.header("content-type"), Some("application/json"));
        assert_eq!(
            call.header("x-forwarded-path"),
            Some("/functions/v1/hello/a/b")
        );
        assert_eq!(call.header("x-forwarded-prefix"), Some("/functions/v1/"));
        assert_eq!(call.header("x-forwarded-host"), Some("127.0.0.1"));
        assert_eq!(call.header("x-forwarded-port"), Some("54321"));
        assert_eq!(call.header("x-forwarded-proto"), Some("http"));
        assert!(!call.execution_id.is_empty(), "one id per call");
    }

    #[tokio::test]
    async fn what_a_proxy_in_front_already_said_is_kept() {
        let seen = Arc::new(Seen::default());
        let res = server(&seen)
            .oneshot(
                post("/functions/v1/hello")
                    .header("authorization", format!("Bearer {}", anon()))
                    .header("host", "zou.internal:8000")
                    .header("x-forwarded-proto", "https")
                    .header("x-forwarded-host", "api.example.com")
                    .header("x-forwarded-port", "443")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("answer");
        assert_eq!(res.status(), StatusCode::OK);
        let call = seen.0.lock().expect("seen").clone().expect("the call");
        assert_eq!(call.header("x-forwarded-proto"), Some("https"));
        assert_eq!(call.header("x-forwarded-host"), Some("api.example.com"));
        assert_eq!(call.header("x-forwarded-port"), Some("443"));
        // The url is built from what the caller reached and what the
        // proxy said about the scheme, since that is the pair that
        // makes an absolute url a function can fetch itself with.
        assert_eq!(call.url, "https://zou.internal:8000/hello");
    }

    #[tokio::test]
    async fn a_function_that_throws_tells_the_caller_nothing() {
        let seen = Arc::new(Seen::default());
        let res = server(&seen)
            .oneshot(
                post("/functions/v1/boom")
                    .header("authorization", format!("Bearer {}", anon()))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("answer");
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(res.headers()["content-type"], "text/plain;charset=UTF-8");
        let body = text(res).await;
        assert_eq!(body, "Internal Server Error");
        assert!(!body.contains("kaboom"), "the reason is the operator's");
    }

    #[tokio::test]
    async fn what_the_function_answered_is_what_the_caller_gets() {
        let seen = Arc::new(Seen::default());
        let res = server(&seen)
            .oneshot(
                post("/functions/v1/hello")
                    .header("authorization", format!("Bearer {}", anon()))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("answer");
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()["content-type"], "application/json");
        assert_eq!(json(res).await, serde_json::json!({"hello": "world"}));
    }

    #[tokio::test]
    async fn a_browser_can_read_the_error_code_it_was_sent() {
        // The cors layer has its own list of headers it exposes, and a
        // response that already said which of its own to expose keeps
        // it rather than losing the one the client is meant to read.
        let seen = Arc::new(Seen::default());
        let res = server(&seen)
            .oneshot(
                post("/functions/v1/hello")
                    .header("origin", "http://localhost:3000")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("answer");
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            res.headers()["access-control-expose-headers"],
            "sb-error-code"
        );
        assert_eq!(
            res.headers()["access-control-allow-origin"],
            "http://localhost:3000"
        );
    }
}
