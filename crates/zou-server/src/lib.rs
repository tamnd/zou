//! The HTTP front door for zou's Supabase compatible surface.
//!
//! One router carries the four Supabase path prefixes: /rest/v1,
//! /auth/v1, /storage/v1, and /realtime/v1. Every request under them
//! passes the same gate Supabase's edge applies: an apikey header or
//! url parameter that must be a JWT signed with the project secret,
//! then an optional Authorization bearer token whose claims become the
//! request identity. The gate deposits an [`AuthContext`] in request
//! extensions, which is what the REST and auth handlers will turn into
//! SET LOCAL role and request.jwt.claims when they land. Endpoints
//! that do not exist yet answer 501 with a plain message instead of
//! pretending, and the error bodies the gate does send match the
//! shapes supabase-js already tolerates from the hosted edge.
//!
//! The server runs on its own tokio runtime behind [`serve_blocking`],
//! so the sync callers in zou dev just park a thread on it.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Router, middleware};

pub mod jwt;
pub mod sql;

/// What the front door needs to know: the secret every key and token
/// must verify against, and where postgres lives when there is one to
/// talk to. `pg` is a dsn like "host=127.0.0.1 port=5432 user=x
/// dbname=postgres", None runs the router without a pool, which is
/// what the pure routing tests use.
pub struct Config {
    pub jwt_secret: Vec<u8>,
    pub pg: Option<String>,
}

/// Everything the handlers share: the config and, when postgres is
/// reachable, the session pool. The pool dials lazily, so building
/// this never blocks on the database.
pub struct App {
    pub cfg: Config,
    pub pool: Option<sql::Pool>,
}

/// PostgREST's default db-pool size, a sane dev loop default here too.
const POOL_SIZE: usize = 10;

fn app_state(cfg: Config) -> Result<Arc<App>, String> {
    let pool = match &cfg.pg {
        Some(dsn) => Some(sql::Pool::new(dsn, POOL_SIZE).map_err(|e| format!("pg dsn: {e}"))?),
        None => None,
    };
    Ok(Arc::new(App { cfg, pool }))
}

/// The verified identity of a request, deposited in request extensions
/// by the gate. role is the bearer token's role claim when a bearer
/// was sent, otherwise the apikey's, otherwise anon, the same
/// precedence PostgREST applies behind Supabase.
#[derive(Clone)]
pub struct AuthContext {
    pub role: String,
    pub claims: Arc<serde_json::Value>,
}

fn json_body(status: StatusCode, body: serde_json::Value) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

/// The apikey from the header or, failing that, the url query string.
fn apikey_of(req: &Request<Body>) -> Option<String> {
    if let Some(v) = req.headers().get("apikey") {
        return v.to_str().ok().map(str::to_string);
    }
    let query = req.uri().query()?;
    query.split('&').find_map(|pair| {
        pair.strip_prefix("apikey=")
            .map(|v| v.split('#').next().unwrap_or(v).to_string())
    })
}

/// The gate every Supabase prefix sits behind. The error bodies for a
/// missing or invalid apikey are the ones the hosted edge sends, so a
/// misconfigured client sees the exact message it would see against
/// Supabase.
async fn gate(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let Some(apikey) = apikey_of(&req) else {
        return json_body(
            StatusCode::UNAUTHORIZED,
            serde_json::json!({
                "message": "No API key found in request",
                "hint": "No `apikey` request header or url param was found.",
            }),
        );
    };
    let key = match jwt::verify(&apikey, &app.cfg.jwt_secret) {
        Ok(v) => v,
        Err(_) => {
            return json_body(
                StatusCode::UNAUTHORIZED,
                serde_json::json!({"message": "Invalid API key"}),
            );
        }
    };

    let bearer = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_string);
    let identity = match bearer {
        Some(token) => match jwt::verify(&token, &app.cfg.jwt_secret) {
            Ok(v) => v,
            Err(why) => {
                return json_body(
                    StatusCode::UNAUTHORIZED,
                    serde_json::json!({"message": why.as_str()}),
                );
            }
        },
        None => key,
    };

    req.extensions_mut().insert(AuthContext {
        role: identity.role.unwrap_or_else(|| "anon".to_string()),
        claims: Arc::new(identity.claims),
    });
    next.run(req).await
}

/// GoTrue's health shape, served locally: the auth surface is going to
/// be a reimplementation, and health is its first endpoint.
async fn auth_health() -> Response {
    json_body(
        StatusCode::OK,
        serde_json::json!({
            "version": concat!("zou-", env!("CARGO_PKG_VERSION")),
            "name": "GoTrue",
            "description": "GoTrue is a user registration and authentication API",
        }),
    )
}

/// An honest placeholder for surfaces that exist in the router but not
/// yet in the code, with the milestone that will fill them in.
fn not_yet(surface: &str) -> Response {
    json_body(
        StatusCode::NOT_IMPLEMENTED,
        serde_json::json!({
            "message": format!("{surface} is not implemented yet, tracked in tamnd/zou milestones"),
        }),
    )
}

async fn rest_stub() -> Response {
    not_yet("the REST surface")
}
async fn auth_stub() -> Response {
    not_yet("this auth endpoint")
}
async fn storage_stub() -> Response {
    not_yet("the storage surface")
}
async fn realtime_stub() -> Response {
    not_yet("the realtime surface")
}

/// Anything outside the four prefixes, in the words the hosted edge
/// uses for an unmatched route.
async fn no_route() -> Response {
    json_body(
        StatusCode::NOT_FOUND,
        serde_json::json!({"message": "no Route matched with those values"}),
    )
}

/// The whole front door as one axum router.
pub fn router(cfg: Config) -> Result<Router, String> {
    let app = app_state(cfg)?;
    let gated = Router::new()
        .route("/auth/v1/health", get(auth_health))
        .route("/auth/v1/{*rest}", any(auth_stub))
        .route("/rest/v1/", any(rest_stub))
        .route("/rest/v1/{*rest}", any(rest_stub))
        .route("/storage/v1/{*rest}", any(storage_stub))
        .route("/realtime/v1/{*rest}", any(realtime_stub))
        .layer(middleware::from_fn_with_state(Arc::clone(&app), gate))
        .with_state(app);
    Ok(Router::new().merge(gated).fallback(no_route))
}

/// Serve `router(cfg)` on `listener` forever. Builds a private tokio
/// runtime, so a plain thread can own the whole server: the listener
/// is bound by the caller while it can still report errors cheaply,
/// this end only converts and serves.
pub fn serve_blocking(listener: std::net::TcpListener, cfg: Config) -> Result<(), String> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;
    rt.block_on(async move {
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("nonblocking: {e}"))?;
        let listener =
            tokio::net::TcpListener::from_std(listener).map_err(|e| format!("listener: {e}"))?;
        axum::serve(listener, router(cfg)?)
            .await
            .map_err(|e| format!("serve: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use tower::ServiceExt;

    const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

    fn app() -> Router {
        router(Config {
            jwt_secret: SECRET.to_vec(),
            pg: None,
        })
        .unwrap()
    }

    fn anon_key() -> String {
        jwt::mint(&jwt::key_claims("anon"), SECRET)
    }

    async fn body_json(res: Response) -> serde_json::Value {
        let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn get_req(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn no_apikey_gets_the_edge_error_shape() {
        let res = app().oneshot(get_req("/rest/v1/todos")).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        let body = body_json(res).await;
        assert_eq!(body["message"], "No API key found in request");
        assert_eq!(
            body["hint"],
            "No `apikey` request header or url param was found."
        );
    }

    #[tokio::test]
    async fn a_garbage_apikey_is_invalid() {
        let req = Request::builder()
            .uri("/rest/v1/todos")
            .header("apikey", "not-a-jwt")
            .body(Body::empty())
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(body_json(res).await["message"], "Invalid API key");
    }

    #[tokio::test]
    async fn the_apikey_works_from_the_query_string_too() {
        let res = app()
            .oneshot(get_req(&format!("/auth/v1/health?apikey={}", anon_key())))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_json(res).await["name"], "GoTrue");
    }

    #[tokio::test]
    async fn health_answers_behind_a_valid_key() {
        let req = Request::builder()
            .uri("/auth/v1/health")
            .header("apikey", anon_key())
            .body(Body::empty())
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn rest_is_a_501_stub_for_now() {
        let req = Request::builder()
            .uri("/rest/v1/todos?select=*")
            .header("apikey", anon_key())
            .body(Body::empty())
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn an_expired_bearer_is_rejected_even_with_a_good_key() {
        let expired = jwt::mint(
            &serde_json::json!({"role": "authenticated", "exp": 1}),
            SECRET,
        );
        let req = Request::builder()
            .uri("/rest/v1/todos")
            .header("apikey", anon_key())
            .header("authorization", format!("Bearer {expired}"))
            .body(Body::empty())
            .unwrap();
        let res = app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(body_json(res).await["message"], "JWT expired");
    }

    #[tokio::test]
    async fn outside_the_prefixes_is_the_edge_404() {
        let res = app().oneshot(get_req("/nope")).await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body = body_json(res).await;
        assert_eq!(body["message"], "no Route matched with those values");
    }

    /// A router with the real gate in front of a handler that echoes
    /// the deposited role, so precedence is observed, not inferred.
    fn echo_app() -> Router {
        let app = app_state(Config {
            jwt_secret: SECRET.to_vec(),
            pg: None,
        })
        .unwrap();
        Router::new()
            .route(
                "/echo",
                get(|axum::Extension(ctx): axum::Extension<AuthContext>| async move { ctx.role }),
            )
            .layer(middleware::from_fn_with_state(Arc::clone(&app), gate))
            .with_state(app)
    }

    #[tokio::test]
    async fn without_a_bearer_the_key_role_is_the_identity() {
        let service_key = jwt::mint(&jwt::key_claims("service_role"), SECRET);
        let req = Request::builder()
            .uri("/echo")
            .header("apikey", service_key)
            .body(Body::empty())
            .unwrap();
        let res = echo_app().oneshot(req).await.unwrap();
        let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        assert_eq!(&bytes[..], b"service_role");
    }

    #[tokio::test]
    async fn the_gate_prefers_the_bearer_identity() {
        let bearer = jwt::mint(
            &serde_json::json!({"role": "authenticated", "sub": "user-1"}),
            SECRET,
        );
        let req = Request::builder()
            .uri("/echo")
            .header("apikey", anon_key())
            .header("authorization", format!("Bearer {bearer}"))
            .body(Body::empty())
            .unwrap();
        let res = echo_app().oneshot(req).await.unwrap();
        let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        assert_eq!(&bytes[..], b"authenticated");
    }
}
